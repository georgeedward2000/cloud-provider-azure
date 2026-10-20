/*
Copyright 2026 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package difftracker

import (
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-logr/logr"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/record"

	"sigs.k8s.io/cloud-provider-azure/pkg/azclient"
	utilsets "sigs.k8s.io/cloud-provider-azure/pkg/util/sets"
)

// ================================================================================================
// ERRORS
// ================================================================================================

var (
	// ErrServiceUIDEmpty is returned when a service UID is empty
	ErrServiceUIDEmpty = errors.New("service UID cannot be empty")
)

// ================================================================================================
// ENUMS
// ================================================================================================
type Operation int

const (
	UnknownOperation Operation = iota
	Add
	Remove
	Update
)

// ResourceState represents the lifecycle state of a service in the Engine
type ResourceState int

const (
	StateNotStarted ResourceState = iota
	StateCreationInProgress
	StateCreated
	StateDeletionPending
	StateDeletionInProgress
	StateUpdateInProgress
)

type UpdateAction int

const (
	UnknownUpdateAction UpdateAction = iota
	PartialUpdate
	FullUpdate
)

// operationKind is the kind of run the ServiceUpdater dispatched for an operation.
type operationKind int

const (
	operationNone                 operationKind = iota // no run in flight
	operationCreate                                    // a creation
	operationUpdate                                    // an update; for a managed egress identity an apply or an observe-and-clean
	operationEgressResidueCleanup                      // the residue cleanup of a BYO egress identity
	operationDelete                                    // a deletion
)

type SyncStatus int

const (
	UnknownSyncStatus SyncStatus = iota
	AlreadyInSync
	Success
)

// ================================================================================================
// SERVICE CONFIGURATION TYPES
// ================================================================================================

// PortMapping represents a port mapping configuration
type PortMapping struct {
	Port     int32
	Protocol string // TCP or UDP
}

// HealthProbeConfig represents health probe configuration
type HealthProbeConfig struct {
	Protocol          string // TCP, HTTP, or HTTPS
	Port              int32
	IntervalInSeconds int32
	NumberOfProbes    int32
	RequestPath       *string // For HTTP/HTTPS probes
}

// InboundConfig contains Load Balancer configuration for inbound services
type InboundConfig struct {
	FrontendPorts      []PortMapping      // nullable for future use
	BackendPorts       []PortMapping      // nullable for future use
	Protocol           *string            // TCP/UDP, nullable
	IdleTimeoutMinutes *int32             // nullable
	SessionPersistence *string            // nullable
	HealthProbe        *HealthProbeConfig // nullable
	// IPFamilies mirrors Service.Spec.IPFamilies ("IPv4"/"IPv6"). A single entry selects the
	// Public IP version; more than one (dual-stack) is unsupported for PodIP backend pools and
	// is rejected as a terminal error in buildInboundServiceResources.
	IPFamilies []string
	// InvalidIdleTimeout holds the raw idle-timeout annotation value when it could not be parsed
	// or fell outside the supported range. Recorded rather than dropped so ValidateInboundConfig
	// can reject the Service with a visible reason instead of silently applying the default.
	InvalidIdleTimeout *string
	// NamedTargetPorts holds any string (named) Service targetPort values. Named target ports
	// cannot be resolved to a PodIP backend port here, so their presence is rejected as a
	// terminal error in buildInboundServiceResources rather than silently mis-routing traffic.
	NamedTargetPorts []string
}

// OutboundConfig contains NAT Gateway configuration for outbound services. A published value is never
// modified: a change assigns a new value, and snapshots are taken with DeepCopy.
type OutboundConfig struct {
	// IPFamilies holds the address families this NAT Gateway must provide a public path for
	// ("IPv4"/"IPv6"), decided once when the service is created. Empty means IPv4 only.
	//
	// It is derived from the cluster's families rather than from the pods of one identity: a dual-stack
	// cluster can run single-stack pods, so the first pod does not predict the rest.
	IPFamilies []string
	// Egress is the NAT Gateway configuration: the desired one in ServiceOperationState.Config and the
	// applied one in LastAppliedConfig.
	Egress EgressConfig
}

// DeepCopy returns a copy of c that shares no memory with it, or nil for a nil c.
func (c *OutboundConfig) DeepCopy() *OutboundConfig {
	if c == nil {
		return nil
	}
	out := *c
	out.IPFamilies = slices.Clone(c.IPFamilies)
	return &out
}

// outboundEgress returns the NAT Gateway configuration of c; a nil c has the zero value.
func outboundEgress(c *OutboundConfig) EgressConfig {
	if c == nil {
		return EgressConfig{}
	}
	return c.Egress
}

// outboundFamilies returns the IP families of c; a nil c has none, which means IPv4 only.
func outboundFamilies(c *OutboundConfig) []string {
	if c == nil {
		return nil
	}
	return c.IPFamilies
}

// Equals returns true if two InboundConfigs describe the same desired LB shape.
// Used by UpdateService to short-circuit no-op reconciles.
// Comparison is order-sensitive for FrontendPorts/BackendPorts because the
// position of a port determines its pairing with a backend port in
// buildInboundServiceResources.
func (c *InboundConfig) Equals(other *InboundConfig) bool {
	if c == nil && other == nil {
		return true
	}
	if c == nil || other == nil {
		return false
	}
	if !portMappingsEqual(c.FrontendPorts, other.FrontendPorts) {
		return false
	}
	if !portMappingsEqual(c.BackendPorts, other.BackendPorts) {
		return false
	}
	if !strPtrEqual(c.Protocol, other.Protocol) {
		return false
	}
	if !int32PtrEqual(c.IdleTimeoutMinutes, other.IdleTimeoutMinutes) {
		return false
	}
	if !strPtrEqual(c.SessionPersistence, other.SessionPersistence) {
		return false
	}
	if !healthProbeEqual(c.HealthProbe, other.HealthProbe) {
		return false
	}
	if !stringSlicesEqual(c.IPFamilies, other.IPFamilies) {
		return false
	}
	if !stringSlicesEqual(c.NamedTargetPorts, other.NamedTargetPorts) {
		return false
	}
	return true
}

func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func portMappingsEqual(a, b []PortMapping) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func strPtrEqual(a, b *string) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}

func int32PtrEqual(a, b *int32) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}

func healthProbeEqual(a, b *HealthProbeConfig) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	if a.Protocol != b.Protocol || a.Port != b.Port ||
		a.IntervalInSeconds != b.IntervalInSeconds || a.NumberOfProbes != b.NumberOfProbes {
		return false
	}
	return strPtrEqual(a.RequestPath, b.RequestPath)
}

// ServiceConfig encapsulates all configuration needed for a service
type ServiceConfig struct {
	UID            string
	IsInbound      bool
	InboundConfig  *InboundConfig  // nil allowed for defaults
	OutboundConfig *OutboundConfig // nil allowed for defaults

	// Namespace and Name identify the originating Kubernetes Service. They let the engine
	// resolve a UID to a cached serviceLister read instead of scanning every Service. They
	// are empty for entries recovered from NRP without a Service object.
	Namespace string
	Name      string
}

// snapshot returns a copy of c with its outbound part deep-copied, so a snapshot of a published
// configuration (dispatched, in flight or last applied) cannot change with it.
func (c ServiceConfig) snapshot() ServiceConfig {
	c.OutboundConfig = c.OutboundConfig.DeepCopy()
	return c
}

// Validate checks if the ServiceConfig is valid
func (c *ServiceConfig) Validate() error {
	if c.UID == "" {
		return ErrServiceUIDEmpty
	}
	// A nil InboundConfig or OutboundConfig is allowed; defaults are applied downstream.
	return nil
}

// NewInboundServiceConfig creates a ServiceConfig for an inbound service
func NewInboundServiceConfig(uid string, inboundConfig *InboundConfig) ServiceConfig {
	return ServiceConfig{
		UID:           uid,
		IsInbound:     true,
		InboundConfig: inboundConfig,
	}
}

// NewOutboundServiceConfig creates a ServiceConfig for an outbound service
func NewOutboundServiceConfig(uid string, outboundConfig *OutboundConfig) ServiceConfig {
	return ServiceConfig{
		UID:            uid,
		IsInbound:      false,
		OutboundConfig: outboundConfig,
	}
}

// --------------------------------------------------------------------------------
// DiffTracker keeps track of the state of the K8s cluster and NRP
// --------------------------------------------------------------------------------
// NRPAddress holds the NRP-side state for a single pod address (pod IP).
type NRPAddress struct {
	// Services holds the SGW service identities (LBs for inbound, NATGWs for
	// outbound) currently associated with this address on the NRP side.
	// These are SGW service identities, not Kubernetes Service names.
	Services *utilsets.IgnoreCaseSet
}

// NRPLocation holds the NRP-side state for a single node/VM and groups the
// pod addresses running on it.
type NRPLocation struct {
	// Addresses is keyed by pod IP. Each pod IP is added to the ServiceGateway
	// as an address under this location once the pod is created.
	Addresses map[string]NRPAddress
}

type NRPState struct {
	// LoadBalancers holds the UIDs of inbound services that have a LoadBalancer
	// registered on the NRP side. These are SGW service identities, not Azure
	// LoadBalancer resource names.
	LoadBalancers *utilsets.IgnoreCaseSet
	// NATGateways holds the UIDs of outbound/egress services that have a NAT
	// Gateway registered on the NRP side (SGW service identities, not Azure
	// resource names).
	NATGateways *utilsets.IgnoreCaseSet
	// OutboundNATGatewayIDs maps each registered egress identity (lower-cased) to its NAT Gateway ID,
	// including one whose unlink was interrupted (recordLinkedBYONATGateways). Start-up snapshot only,
	// not kept up to date; the live BYO record is DiffTracker.byoNATGateways. Optional.
	OutboundNATGatewayIDs map[string]string
	// UnmanagedNATGateways holds names this controller must never manage: cluster-resource-group NAT
	// Gateways it did not create, the default outbound service's name, and its gateway when that is in
	// the cluster resource group. They are never swept as orphans, and an egress identity with such a
	// name is rejected. Start-up snapshot only, not kept up to date. Optional.
	UnmanagedNATGateways *utilsets.IgnoreCaseSet
	// Locations is keyed by node/VM IP (e.g. "10.0.0.1"). "Location" here is
	// an SGW concept identifying a node, not an Azure region (e.g. "eastus2").
	Locations map[string]NRPLocation
}

type Pod struct {
	// InboundIdentities holds the UIDs of the inbound ServiceGateway services
	// (LoadBalancers) this pod backs. A pod may back several, hence a set.
	InboundIdentities *utilsets.IgnoreCaseSet
	// PublicOutboundIdentity is the UID of the single outbound/egress ServiceGateway
	// service (NAT Gateway) this pod uses for egress; empty if the pod has no egress.
	PublicOutboundIdentity string
	// OutboundPodKey is the namespace/name of the Kubernetes pod that owns this outbound address.
	// It allows a terminating pod first observed without PodIPs to remove its stale live entries.
	OutboundPodKey string
	// OutboundPodUID distinguishes same-name replacement pods.
	OutboundPodUID string
}

// newPod returns a Pod with its InboundIdentities set initialized.
func newPod() Pod {
	return Pod{InboundIdentities: utilsets.NewString()}
}

type Node struct {
	Pods map[string]Pod
}

// newNode returns a Node with its Pods map initialized.
func newNode() Node {
	return Node{Pods: make(map[string]Pod)}
}

type K8sState struct {
	Services *utilsets.IgnoreCaseSet
	Egresses *utilsets.IgnoreCaseSet
	Nodes    map[string]Node
}

// ================================================================================================
// ENGINE STATE TRACKING TYPES
// ================================================================================================

// ServiceOperationState tracks the lifecycle state of a service being created or deleted
type ServiceOperationState struct {
	ServiceUID string
	// Config is the desired configuration. For update flows it is overwritten by
	// the latest Update call before the updater goroutine picks it up.
	Config ServiceConfig
	// InFlightConfig is the configuration snapshot that the currently-running
	// (or last-dispatched) updater goroutine is applying. Set by the dispatcher
	// when it picks up the work; consumed by OnServiceCreationComplete to
	// (a) populate LastAppliedConfig on success and (b) detect whether a newer
	// Config arrived during the in-flight operation so we can reschedule.
	InFlightConfig *ServiceConfig
	// LastAppliedConfig is the configuration last successfully applied to Azure.
	// Used by UpdateService to short-circuit no-op updates.
	LastAppliedConfig *ServiceConfig
	State             ResourceState
	RetryCount        int
	LastAttempt       string // timestamp as string for serialization

	// NextRetryAt is the earliest time the dispatcher may re-dispatch this operation after a
	// transient failure. Set in the failure branches of OnServiceCreationComplete to now+backoff;
	// the dispatcher skips (and schedules a revisit) while now is before it. Prevents the
	// no-backoff hot-loop on a persistent transient ARM/NRP error or a recovered worker panic.
	NextRetryAt time.Time

	// RetriesExhausted is set once the operation has been retried maxServiceRetries times; the
	// dispatcher then stops re-dispatching it (a "gave up" park) instead of looping unbounded.
	RetriesExhausted bool

	// CreationFailedTerminal is set when creation failed with a non-retryable
	// (deterministic) error such as an invalid Service spec (unsupported protocol,
	// port out of range). The dispatcher skips such entries instead of retrying
	// forever; a later UpdateService with a changed spec clears it.
	CreationFailedTerminal bool

	// CreatedAt is when this operation was first enqueued. Never overwritten on retry.
	// Used by the oldest-age metric to detect stuck operations.
	CreatedAt time.Time

	// OperationStartedAt is when the current Azure operation was dispatched in
	// processBatch (refreshed on every dispatch, including retries). Used to measure
	// real operation latency rather than the time spent in the completion callback.
	OperationStartedAt time.Time

	// IsOrphan indicates this is an orphaned Azure resource being cleaned up at startup.
	IsOrphan bool

	// CorrelationID is a unique ID for tracing all logs related to this operation.
	CorrelationID string

	// TriggeringPodNamespace and TriggeringPodName identify which pod triggered
	// a NAT Gateway creation (outbound services only). While that pod is a member of the
	// egress identity with TriggeringPodUID, its vote seeds the creation configuration (creationConfigLocked).
	TriggeringPodNamespace string
	TriggeringPodName      string
	// TriggeringPodUID is the UID of the triggering pod, so a same-name replacement pod is not taken for it.
	// It is cleared when the identity is re-created after a completed deletion: its oldest member then seeds
	// the creation configuration.
	TriggeringPodUID string

	// RecreateAfterDeletion is set when an UpdateService re-create arrives while the
	// service is being deleted; the deletion-success path replays it as a fresh create.
	RecreateAfterDeletion bool

	// InFlightKind is the kind of the run the dispatcher started, recorded with InFlightConfig and
	// Egress.DispatchedObservationGen and consumed by the run's completion, which branches on it rather than on the
	// state the operation was moved to meanwhile. operationNone while no run is in flight.
	InFlightKind operationKind

	// Egress is what the operation of an egress identity knows about the effect of its runs on Azure.
	Egress egressOperationState
}

// egressOperationState is what the operation of an egress identity knows about the effect of its runs on Azure.
type egressOperationState struct {
	// MutationStarted is set before the first Azure write of a managed outbound run, cleared on its completion.
	MutationStarted bool
	// NeedsObservation is set while Azure must be re-read and swept for this egress identity.
	NeedsObservation bool
	// ObservationGen counts how often NeedsObservation was set (requireObservation). A run clears the obligation
	// only when the count is unchanged since its dispatch, so an obligation raised while it was in flight outlives it.
	ObservationGen uint64
	// DispatchedObservationGen is ObservationGen when the current or last run was dispatched.
	DispatchedObservationGen uint64
	// QuietUntil holds back managed outbound runs after a write whose outcome is unknown; retry resets keep it.
	QuietUntil time.Time
	// StartupBaseline is set by startup on an egress identity it found in Azure. Until a run decides the agreed
	// configuration, a rejection of it is one the previous process already reported, so it is recorded without
	// events. Cleared by that decision and by any vote change of the identity.
	StartupBaseline bool
	// LastKnownApplied is, while the applied configuration is egressUnknown, the one last known applied before it,
	// which a configuration found applied later is announced against; nil when none was known, as for an identity
	// this process adopted (4.5 rule 2).
	LastKnownApplied *EgressConfig
	// UnclearWrites are, while the applied configuration is egressUnknown, the configurations of the applies whose
	// writes Azure did not answer clearly, newest first, without repeats and at most maxUnclearEgressWrites: an
	// observation reports one the NAT Gateway satisfies, and finding one applied counts as a write of this process
	// (4.5 rules 0 and 2).
	UnclearWrites []EgressConfig
}

// maxUnclearEgressWrites bounds Egress.UnclearWrites. Each entry needs a write that Azure did not answer clearly and
// the quiet period after it, so more than a few in a row before the applied configuration is known again are rare.
const maxUnclearEgressWrites = 4

// rememberUnclearWrite records cfg as the configuration of the latest apply whose write Azure did not answer clearly:
// first in UnclearWrites, without an older repeat of it, keeping at most maxUnclearEgressWrites. It builds a new slice,
// so a copy taken before is left as it was.
func (e *egressOperationState) rememberUnclearWrite(cfg EgressConfig) {
	writes := make([]EgressConfig, 0, maxUnclearEgressWrites)
	writes = append(writes, cfg)
	for _, w := range e.UnclearWrites {
		if len(writes) == maxUnclearEgressWrites {
			break
		}
		if w != cfg {
			writes = append(writes, w)
		}
	}
	e.UnclearWrites = writes
}

// requireObservation records that the NAT Gateway of the identity must be re-read and swept by a run. Every call is a
// new obligation, which a run dispatched before it cannot settle.
func (e *egressOperationState) requireObservation() {
	e.NeedsObservation = true
	e.ObservationGen++
}

// noteDispatched records, for the run being dispatched, the obligations raised so far: the ones it can settle.
func (e *egressOperationState) noteDispatched() {
	e.DispatchedObservationGen = e.ObservationGen
}

// raisedSinceDispatch reports whether an obligation was raised after the current or last run was dispatched, so that
// the run cannot settle it.
func (e *egressOperationState) raisedSinceDispatch() bool {
	return e.ObservationGen != e.DispatchedObservationGen
}

// egressMode is how the NAT Gateway of an egress identity is provided.
type egressMode int

const (
	egressModeUnknown egressMode = iota // not resolved yet: the identity's creation resolves it
	egressModeManaged                   // created and configured by this controller
	egressModeBYO                       // the user's NAT Gateway: only linked, accepts no configuration
)

// egressRejection is an egress configuration that was refused when it was applied.
type egressRejection struct {
	Config EgressConfig
	Reason string
	// Retryable is set when the refusal can clear without a configuration change.
	Retryable bool
}

// egressOutcome is what an outbound run reports with its completion.
type egressOutcome struct {
	Mode egressMode
	// Applied is the configuration the settled NAT Gateway satisfies; nil for BYO or when not determined.
	Applied *EgressConfig
	// NATWritten is set when the run's NAT Gateway PUT applied Applied, rather than finding it satisfied.
	NATWritten bool
	// Rejected is a refused configuration; it may come with Applied when a creation fell back to the default.
	Rejected *egressRejection
	// CommitUnknown is set when the run ended with a write that Azure may still apply.
	CommitUnknown bool
	// Swept is set when every reserved name was read settled and every needed delete was conclusive.
	Swept bool
}

// egressPodRef identifies the pod an egress membership call is about.
type egressPodRef struct {
	Namespace, Name string
	UID             types.UID
	Created         time.Time
}

// key returns the "namespace/name" key of the pod.
func (p egressPodRef) key() string {
	return p.Namespace + "/" + p.Name
}

// voteFor returns vote as the pod's vote for identity: its identity, pod UID and creation time are the pod's.
func (p egressPodRef) voteFor(identity string, vote egressVote) egressVote {
	vote.Identity, vote.PodUID, vote.PodCreated = identity, p.UID, p.Created
	return vote
}

// egressAddress is a pod address accepted for egress, with the node location of its IP family.
type egressAddress struct {
	IP, Location string
}

// valid reports whether a names both the address and its location.
func (a egressAddress) valid() bool {
	return a.IP != "" && a.Location != ""
}

// PendingEndpointUpdate represents endpoints waiting for their service to be created
type PendingEndpointUpdate struct {
	OldPodIPToNodeIP map[string]string // podIP -> nodeIP removed by this event
	PodIPToNodeIP    map[string]string // podIP -> nodeIP added/present by this event
	Timestamp        string            // When buffered
}

// PendingServiceDeletion tracks a service waiting for locations to clear before deletion
type PendingServiceDeletion struct {
	ServiceUID string
	IsInbound  bool
	Timestamp  string
}

// DiffTracker is the main struct that contains the state of the K8s and NRP services
type DiffTracker struct {
	mu sync.Mutex // Protects concurrent access to DiffTracker

	K8sResources K8sState
	NRPResources NRPState

	// outboundIdentityPodRefCount counts how many pods reference each outbound
	// (egress) identity, keyed by lowercased PublicOutboundIdentity. It lets the
	// engine delete a NAT Gateway when its last egress pod is removed. Inbound
	// (LoadBalancer) services are not tracked here; their lifecycle follows the
	// Kubernetes Service object.
	outboundIdentityPodRefCount sync.Map

	InitialSyncDone bool

	// Configuration and clients
	config               Config
	networkClientFactory azclient.ClientFactory
	kubeClient           kubernetes.Interface

	// serviceLister is the provider's shared-informer-backed Service lister, published via
	// SetServiceLister once SetInformers has run. It is nil until then (and in tests), in which
	// case getServiceByUID falls back to a direct apiserver read. Guarded by mu.
	serviceLister corelisters.ServiceLister
	// nodeLister is the provider's shared-informer-backed Node lister used to resolve EndpointSlice
	// node names to InternalIPs. Guarded by mu.
	nodeLister corelisters.NodeLister

	// eventRecorder emits Service Gateway pod events; set post-init before the egress informer starts.
	eventRecorder record.EventRecorder
	// endpointSlicesCache is owned by difftracker and stores snapshots from forwarded informer
	// events. It is replayed when a service is registered and by ReconcileNodeIPChange.
	endpointSlicesCache sync.Map

	// Engine state management
	pendingServiceOps       map[string]*ServiceOperationState
	pendingEndpoints        map[string][]PendingEndpointUpdate
	pendingPods             map[string][]PendingPodUpdate
	pendingServiceDeletions map[string]*PendingServiceDeletion
	pendingPodDeletions     map[string]*PendingPodDeletion // key = "namespace/name"

	// recoveredServiceFinalizers holds the UIDs of Services whose stuck finalizer startup left to
	// the diff. An entry is cleared when that finalizer is actually removed, which is what closes
	// the finalizers_recovery_scheduled_total / finalizers_recovered_total gap.
	recoveredServiceFinalizers map[string]struct{}

	// byoNATGateways maps an egress identity to the BYO NAT Gateway ID it uses.
	// Such gateways are only linked and unlinked, never created or deleted. Guarded by mu.
	byoNATGateways map[string]string

	// egress holds the votes, modes, rejections and pending pod events of the egress identities, and the members
	// startup admitted until the pod informer reconciles them. Guarded by mu.
	egress egressTrackerState

	// Communication channels
	serviceUpdaterTrigger   chan bool
	locationsUpdaterTrigger chan bool

	// Initialization tracking
	// clusterHasIPv6 caches the cluster's IPv6-ness once observed. Nodes never lose a family in
	// practice, so a positive result is permanent and the node walk runs at most once per process.
	// Seeded during initialization from the Node list fetched there, because the Node lister is
	// published only afterwards and the startup create path must not mistake a dual-stack cluster
	// for an IPv4-only one.
	clusterHasIPv6 atomic.Bool

	isInitializing         int32 // Atomic: 1 during initialization, 0 after
	initCompletionChecker  chan struct{}
	pendingUpdaterTriggers int32 // Atomic counter for in-flight updater triggers
	initCompletionOnce     sync.Once
	// initPlanning is set while startup is still enqueueing its work: initialization cannot complete before
	// every piece of it is enqueued, however fast the work enqueued first completes. Guarded by mu.
	initPlanning bool

	// Updater references (started during initialization, kept running)
	serviceUpdater   *ServiceUpdater
	locationsUpdater *LocationsUpdater

	// logger is the component logger, backed by klog via pkg/log. It is supplied
	// to New and enriched with WithName("difftracker").
	logger logr.Logger
}

// egressTrackerState is what the engine knows about egress identities beyond their operations. Guarded by dt.mu.
type egressTrackerState struct {
	// votes holds the NAT Gateway configuration vote of every egress member pod.
	votes *egressVotes
	// mode, rejected and warned are keyed by egress identity: how its NAT Gateway is provided, the configuration
	// rejected terminally for it, and the retryable rejection already warned about.
	mode     map[string]egressMode
	rejected map[string]egressRejection
	warned   map[string]egressRejection
	// pendingEvents holds the egress pod events raised before a recorder was published, oldest first, until
	// SetEventRecorder emits them.
	pendingEvents []egressEvent
	// admitted holds, by pod key, trimmed copies of the pods startup admitted as members (seedEgressVotes,
	// admittedEgressPod), until the ADD of that key reconciles them (reconcileEgressPodAdd), or until
	// removeUnlistedEgressPods removes those whose key the pod informer's synced store, listed consistently
	// (egressPodListOptions), does not hold.
	admitted map[string]*v1.Pod
}

// forgetRejections forgets the rejections of identity, terminal and warned, whose configuration forget selects.
func (e *egressTrackerState) forgetRejections(identity string, forget func(EgressConfig) bool) {
	if rejection, ok := e.rejected[identity]; ok && forget(rejection.Config) {
		delete(e.rejected, identity)
	}
	if rejection, ok := e.warned[identity]; ok && forget(rejection.Config) {
		delete(e.warned, identity)
	}
}

// --------------------------------------------------------------------------------
// Types that are used while events are received and processed in order to update K8s state
// --------------------------------------------------------------------------------

// UpdateK8sResource represents input for K8s service or egress updates
type UpdateK8sResource struct {
	Operation Operation
	ID        string
}

// UpdateK8sEndpointsInputType represents input for K8s endpoints updates
type UpdateK8sEndpointsInputType struct {
	InboundIdentity string
	OldAddresses    map[string]string // address -> location
	NewAddresses    map[string]string // address -> location
}

// UpdatePodInputType represents input for K8s pod updates (egress assignments)
type UpdatePodInputType struct {
	PodOperation           Operation
	PublicOutboundIdentity string
	PodKey                 string
	PodUID                 string
	Location               string
	Address                string
}

// PendingPodUpdate represents a pod waiting for its service to be created
type PendingPodUpdate struct {
	PodKey    string // namespace/name for logging
	PodUID    string // Kubernetes UID; distinguishes same-name replacement pods
	Location  string // HostIP
	Address   string // PodIP
	Timestamp string // When buffered (for debugging/metrics)
}

// --------------------------------------------------------------------------------
// Types that are used while syncing NRP state to K8s state
// --------------------------------------------------------------------------------
type Address struct {
	ServiceRef *utilsets.IgnoreCaseSet
}

// Update Location to use a map for Addresses
type Location struct {
	AddressUpdateAction UpdateAction
	Addresses           map[string]Address // key is the pod IP
}

type LocationData struct {
	Action    UpdateAction
	Locations map[string]Location // key is the node IP
}

type SyncServicesReturnType struct {
	Additions *utilsets.IgnoreCaseSet
	Removals  *utilsets.IgnoreCaseSet
}

type SyncDiffTrackerReturnType struct {
	SyncStatus          SyncStatus
	LoadBalancerUpdates SyncServicesReturnType
	NATGatewayUpdates   SyncServicesReturnType
	LocationData        LocationData
}

// --------------------------------------------------------------------------------
// Data Transfer Objects (DTOs) for LocationData (following the ServiceGateway API documentation)
// --------------------------------------------------------------------------------

// AddressDTO represents the DTO for Address
type AddressDTO struct {
	Address      string                  `json:"Address"`
	ServiceNames *utilsets.IgnoreCaseSet `json:"ServiceNames"`
}

// LocationDTO represents the DTO for Location
type LocationDTO struct {
	Location            string       `json:"Location"`
	AddressUpdateAction UpdateAction `json:"AddressUpdateAction"`
	Addresses           []AddressDTO `json:"Addresses"`
}

// LocationsDataDTO represents the DTO for LocationData
type LocationsDataDTO struct {
	Action    UpdateAction  `json:"Action"`
	Locations []LocationDTO `json:"Locations"`
}

// ================================================================================================
// Data Transfer Objects (DTOs) for ServiceData (following the ServiceGateway API documentation)
// ================================================================================================

type ServiceType string

const (
	Inbound  ServiceType = "Inbound"
	Outbound ServiceType = "Outbound"
)

type LoadBalancerBackendPoolDTO struct {
	ID string `json:"Id"`
}

type NatGatewayDTO struct {
	ID string `json:"Id"`
}

type ServiceDTO struct {
	Service                  string                       `json:"Service"`
	ServiceType              ServiceType                  `json:"ServiceType"`
	LoadBalancerBackendPools []LoadBalancerBackendPoolDTO `json:"LoadBalancerBackendPools"`
	PublicNatGateway         NatGatewayDTO                `json:"PublicNatGateway"`
	IsDelete                 bool
}

type ServicesDataDTO struct {
	Action   UpdateAction `json:"Action"`
	Services []ServiceDTO `json:"Services"`
}
