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

// Startup of the egress identities found in Azure (4.6).
//
// Startup lists the NAT Gateways, Public IPs and Public IP prefixes of the node resource group with the Service
// Gateway's registrations (egressListing) and adopts every registered egress identity from that listing: its mode, and
// for a managed identity with members a Created operation recording the IP families its NAT Gateway serves, the
// configuration its settled NAT Gateway satisfies and whether it must be observed. Only differences start runs. Writes
// of the previous process may land after that listing, within the egress settle delay (the platform bound, sign-off
// 11), so a confirmation pass lists them again once the delay has passed and owes an observation to every identity the
// new listing shows changed. Leftovers of the managed past of a BYO identity get a residue cleanup, and those of an
// identity nothing tracks go through the orphan path.

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork/v12"
	"github.com/google/uuid"
	"k8s.io/utils/ptr"

	utilsets "sigs.k8s.io/cloud-provider-azure/pkg/util/sets"
)

// egressConfirmationRetryDelay is the first wait of the confirmation pass after a listing that could not be read
// completely. It doubles after each failure, up to the egress settle delay.
const egressConfirmationRetryDelay = 5 * time.Second

// egressListing is a listing of the egress resources, taken as one unit: the NAT Gateways, Public IPs and Public IP
// prefixes of the node resource group, and the NAT Gateways the Service Gateway's services reference.
type egressListing struct {
	natGateways map[string]*armnetwork.NatGateway // by lower-cased name
	resources   egressManagedResources            // every listed Public IP and prefix, by name
	publicIPs   []*armnetwork.PublicIPAddress
	prefixes    []*armnetwork.PublicIPPrefix
	// registrations maps each egress identity registered with the Service Gateway (lower-cased) to the ID of the NAT
	// Gateway its service references, "" for none. The default outbound service is not an egress identity.
	registrations map[string]string
	// usedNATGateways holds the canonical IDs (canonicalResourceID) of the NAT Gateways Service Gateway services
	// reference, the default outbound service's included.
	usedNATGateways map[string]bool
}

func newEgressListingOf(natGateways []*armnetwork.NatGateway, publicIPs []*armnetwork.PublicIPAddress, prefixes []*armnetwork.PublicIPPrefix) *egressListing {
	l := &egressListing{
		natGateways:     make(map[string]*armnetwork.NatGateway, len(natGateways)),
		resources:       newEgressManagedResources(publicIPs, prefixes),
		publicIPs:       publicIPs,
		prefixes:        prefixes,
		registrations:   map[string]string{},
		usedNATGateways: map[string]bool{},
	}
	for _, nat := range natGateways {
		if nat != nil && nat.Name != nil {
			l.natGateways[strings.ToLower(*nat.Name)] = nat
		}
	}
	return l
}

// newStartupEgressListing returns the listing startup took: the NAT Gateways, Public IPs and prefixes it listed, and
// the registrations of the NRP state, which reads them from the Service Gateway (the default outbound service, which
// it leaves out, is recorded as unmanaged instead).
func newStartupEgressListing(nrp NRPState, natGateways []*armnetwork.NatGateway, publicIPs []*armnetwork.PublicIPAddress, prefixes []*armnetwork.PublicIPPrefix) *egressListing {
	l := newEgressListingOf(natGateways, publicIPs, prefixes)
	if nrp.NATGateways != nil {
		for _, identity := range nrp.NATGateways.UnsortedList() {
			l.register(identity, nrp.OutboundNATGatewayIDs[strings.ToLower(identity)])
		}
	}
	return l
}

// newEgressListing returns the listing of the NAT Gateways, Public IPs and prefixes listed with the Service Gateway's
// services. Like startup, it refuses a service whose type it does not recognise: the listing would be incomplete.
func newEgressListing(services []*armnetwork.ServiceGatewayService, natGateways []*armnetwork.NatGateway, publicIPs []*armnetwork.PublicIPAddress, prefixes []*armnetwork.PublicIPPrefix) (*egressListing, error) {
	l := newEgressListingOf(natGateways, publicIPs, prefixes)
	for _, service := range services {
		if service == nil || service.Name == nil || service.Properties == nil || service.Properties.ServiceType == nil {
			continue
		}
		serviceType := string(*service.Properties.ServiceType)
		outbound := strings.EqualFold(serviceType, string(armnetwork.ServiceTypeOutbound)) ||
			strings.EqualFold(serviceType, string(armnetwork.ServiceTypeInboundOutbound))
		if !outbound && !strings.EqualFold(serviceType, string(armnetwork.ServiceTypeInbound)) {
			return nil, fmt.Errorf("unrecognized type %q of Service Gateway service %q", serviceType, *service.Name)
		}
		if !outbound {
			continue
		}
		natID := derefString(service.Properties.PublicNatGatewayID)
		if IsReservedEgressIdentity(*service.Name) || ptr.Deref(service.Properties.IsDefault, false) {
			l.use(natID)
			continue
		}
		l.register(*service.Name, natID)
	}
	return l, nil
}

// register records that egress identity is registered with the Service Gateway, its service referencing natID.
func (l *egressListing) register(identity, natID string) {
	l.registrations[strings.ToLower(identity)] = natID
	l.use(natID)
}

// use records that a Service Gateway service references the NAT Gateway natID.
func (l *egressListing) use(natID string) {
	if natID != "" {
		l.usedNATGateways[canonicalResourceID(natID)] = true
	}
}

// natGateway returns the listed NAT Gateway named after egress identity id, or nil.
func (l *egressListing) natGateway(id string) *armnetwork.NatGateway {
	return l.natGateways[strings.ToLower(id)]
}

// natGatewayUsed reports whether a Service Gateway service references the node resource group's NAT Gateway named after
// egress identity id.
func (l *egressListing) natGatewayUsed(id string, dt Config) bool {
	return l.usedNATGateways[canonicalResourceID(natGatewayID(id, dt))]
}

// reservedReads returns the listed resources of the reserved managed names of egress identity id in the served IP
// families (IPv4, and IPv6 when ipv6 is set), as the reads of a sweep, one for each name that exists.
func (l *egressListing) reservedReads(id string, ipv6 bool) []*egressReservedRead {
	var reads []*egressReservedRead
	for _, m := range reservedManaged(ipv6) {
		r := &egressReservedRead{managed: m, name: m.name(id)}
		if m.Kind == egressManagedPrefix {
			r.prefix = l.resources.prefix(r.name)
			r.exists = r.prefix != nil
		} else {
			r.pip = l.resources.publicIP(r.name)
			r.exists = r.pip != nil
		}
		if r.exists {
			reads = append(reads, r)
		}
	}
	return reads
}

// registeredIdentities returns the registered egress identities, sorted.
func (l *egressListing) registeredIdentities() []string {
	return slices.Sorted(maps.Keys(l.registrations))
}

// leftoverCandidates returns the egress identities the listing may hold resources of, lower-cased and sorted: the
// names of the NAT Gateways, and the identities in the ownership tags of Public IPs and prefixes with one of their
// reserved managed names.
func (l *egressListing) leftoverCandidates() []string {
	candidates := map[string]bool{}
	for name := range l.natGateways {
		candidates[name] = true
	}
	tagged := func(name string, tags map[string]*string) {
		if owner := tags[EgressIdentityTagKey]; owner != nil && *owner != "" {
			if _, ok := parseManagedName(*owner, name); ok {
				candidates[strings.ToLower(*owner)] = true
			}
		}
	}
	for _, pip := range l.publicIPs {
		if pip != nil {
			tagged(derefString(pip.Name), pip.Tags)
		}
	}
	for _, prefix := range l.prefixes {
		if prefix != nil {
			tagged(derefString(prefix.Name), prefix.Tags)
		}
	}
	return slices.Sorted(maps.Keys(candidates))
}

// natGatewayFamilies returns the IP families the NAT Gateway nat serves: IPv4, and IPv6 when it references an IPv6
// Public IP or prefix. They are those of its identity's creation, which no write changes: a NAT Gateway being written
// is listed with the goal of the write (4.4), so it is read alike.
func natGatewayFamilies(nat *armnetwork.NatGateway) []string {
	if props := nat.Properties; props != nil && (len(props.PublicIPAddressesV6) > 0 || len(props.PublicIPPrefixesV6) > 0) {
		return []string{string(armnetwork.IPVersionIPv4), string(armnetwork.IPVersionIPv6)}
	}
	return []string{string(armnetwork.IPVersionIPv4)}
}

// egressClassification is what a listing shows of the NAT Gateway of a managed egress identity.
type egressClassification struct {
	// applied is the configuration the settled NAT Gateway satisfies: the agreed one, else the default, else
	// egressUnknown.
	applied EgressConfig
	// owed says why the NAT Gateway must be observed, "" when it need not be.
	owed string
}

// classifyManagedEgress classifies the NAT Gateway of managed egress identity id, which serves IPv6 when ipv6 is set,
// from listing l (4.6 step 3). agreed is the configuration the identity's members agree on, nil without consensus, and
// fallback the default configuration. The applied configuration is decided from settled reads only: when the NAT
// Gateway or a listed reserved resource of the identity is not settled it is egressUnknown, and an observation is owed.
// An observation is also owed for a listed reserved name that is the controller's and that the NAT Gateway does not
// reference (the sweep deletes it), and when either side of the link between the NAT Gateway and the identity's
// Service Gateway service is missing, as after an interrupted unlink (the link repair restores both).
func classifyManagedEgress(id string, l *egressListing, agreed *EgressConfig, fallback EgressConfig, ipv6 bool, dt Config) egressClassification {
	nat := l.natGateway(id)
	if !natReadSettled(nat) {
		return egressClassification{applied: egressUnknown,
			owed: fmt.Sprintf("NAT Gateway %q is %s", derefString(nat.Name), ptr.Deref(nat.Properties.ProvisioningState, ""))}
	}
	reads := l.reservedReads(id, ipv6)
	for _, r := range reads {
		if !r.settled() {
			return egressClassification{applied: egressUnknown, owed: fmt.Sprintf("%q is not settled", r.name)}
		}
	}

	c := egressClassification{applied: observedEgressConfig(nat, l.resources, id, ipv6, dt, agreed, &fallback)}
	referenced := sweepReferencedIDs(nat)
	for _, r := range reads {
		if ownsManagedName(id, r.name, r.tags()) && !referenced[canonicalResourceID(r.id(dt))] {
			c.owed = fmt.Sprintf("%q is not referenced by the NAT Gateway", r.name)
			return c
		}
	}
	switch {
	case natNeedsLink(nat, dt):
		c.owed = "the NAT Gateway is not linked to the Service Gateway"
	case canonicalResourceID(l.registrations[strings.ToLower(id)]) != canonicalResourceID(natGatewayID(id, dt)):
		c.owed = "the Service Gateway service does not reference the NAT Gateway"
	}
	return c
}

// egressLeftover returns a resource egress identity id has left in listing l that removing what the controller created
// for it deletes, "" for none: its same-name NAT Gateway, when the controller may delete it (ownsNATGateway, not
// recorded as unmanaged, referenced by no Service Gateway service); else, without a same-name NAT Gateway, an existing
// reserved name of either family that is the controller's (ownsManagedName). A NAT Gateway the controller may not
// delete is left alone with its resources, as the residue cleanup and the deletion leave it (4.4).
func egressLeftover(id string, l *egressListing, unmanaged *utilsets.IgnoreCaseSet, dt Config) string {
	if nat := l.natGateway(id); nat != nil {
		if ownsNATGateway(nat, dt) && (unmanaged == nil || !unmanaged.Has(id)) && !l.natGatewayUsed(id, dt) {
			return fmt.Sprintf("NAT Gateway %q", derefString(nat.Name))
		}
		return ""
	}
	for _, r := range l.reservedReads(id, true) {
		if ownsManagedName(id, r.name, r.tags()) {
			return fmt.Sprintf("%q", r.name)
		}
	}
	return ""
}

// managedEgressLeftover returns a resource of egress identity id that the controller owns and that listing l holds, ""
// for none: its NAT Gateway when the controller owns it, or a reserved name of either family that is the controller's.
// The deletion of a managed identity deletes them all.
func managedEgressLeftover(id string, l *egressListing, dt Config) string {
	if nat := l.natGateway(id); nat != nil && ownsNATGateway(nat, dt) {
		return fmt.Sprintf("NAT Gateway %q", derefString(nat.Name))
	}
	for _, r := range l.reservedReads(id, true) {
		if ownsManagedName(id, r.name, r.tags()) {
			return fmt.Sprintf("%q", r.name)
		}
	}
	return ""
}

// adoptStartupEgressIdentities adopts the egress identities registered with the Service Gateway from listing l, taken
// at startup (4.6 step 3); the votes seedEgressVotes seeded are parsed again, from their own annotations, for the IP
// families each identity's NAT Gateway serves. It runs once the updaters started, so a run it starts is tracked, and
// before reconcileServices, so an identity without members that owes a residue cleanup is deleted with it.
func (dt *DiffTracker) adoptStartupEgressIdentities(l *egressListing) {
	if l == nil {
		return
	}
	dt.mu.Lock()
	defer dt.mu.Unlock()
	for _, id := range l.registeredIdentities() {
		dt.adoptStartupEgressIdentityLocked(id, l)
	}
}

// adoptStartupEgressIdentityLocked adopts the registered egress identity id from startup's listing l:
//   - BYO: its mode is recorded; leftovers of its managed past (egressLeftover) are owed a residue cleanup.
//   - Managed, when the same-name NAT Gateway is the controller's by the existing rules (ownsNATGateway, not recorded
//     as unmanaged): its mode is recorded. Without members it is deleted as before. With members it gets a Created
//     operation for the IP families its NAT Gateway serves (natGatewayFamilies), for which its members' votes are
//     parsed, recording the configuration its NAT Gateway satisfies and owing an observation when the listing calls
//     for one (classifyManagedEgress), which the evaluation turns into a run.
//   - Otherwise (its NAT Gateway is missing, listed without its properties or not the controller's) nothing changes.
//
// Must be called with dt.mu held.
func (dt *DiffTracker) adoptStartupEgressIdentityLocked(id string, l *egressListing) {
	if _, tracked := dt.pendingServiceOps[id]; tracked || dt.K8sResources.Services.Has(id) || dt.NRPResources.LoadBalancers.Has(id) {
		return
	}
	desired := dt.K8sResources.Egresses.Has(id)
	if dt.byoNATGateways[id] != "" {
		dt.egress.mode[id] = egressModeBYO
		if leftover := egressLeftover(id, l, dt.NRPResources.UnmanagedNATGateways, dt.config); leftover != "" {
			dt.owesBYOResidueLocked(id, leftover, desired)
		}
		return
	}
	nat := l.natGateway(id)
	if nat == nil || nat.Properties == nil || !ownsNATGateway(nat, dt.config) || dt.unmanagedNATGatewayLocked(id) {
		return
	}
	dt.egress.mode[id] = egressModeManaged
	if !desired {
		return
	}
	families := natGatewayFamilies(nat)
	// seedEgressVotes parsed the votes for the families of a new identity, the cluster's. The unchanged baseline owes
	// no event.
	dt.reparseEgressVotesLocked(id, dt.egressParseContextFor(families))
	c := dt.classifyManagedEgressLocked(id, l, families)
	op := dt.trackStartupEgressLocked(id, families, c.applied)
	if c.owed != "" {
		op.Egress.requireObservation()
	}
	dt.logger.V(2).Info("Adopted managed egress identity found in Azure", "egressIdentity", id, "ipFamilies", families,
		"applied", c.applied.Summary(), "observationOwed", c.owed)
	dt.evaluateEgressConsensusLocked(id)
}

// classifyManagedEgressLocked is classifyManagedEgress for managed egress identity id, whose NAT Gateway serves
// families, with its members' agreement and the default configuration of its families. Must be called with dt.mu
// held.
func (dt *DiffTracker) classifyManagedEgressLocked(id string, l *egressListing, families []string) egressClassification {
	var agreed *EgressConfig
	if cfg, ok := dt.egress.votes.agreed(id); ok {
		agreed = &cfg
	}
	pc := dt.egressParseContextFor(families)
	return classifyManagedEgress(id, l, agreed, defaultEgressConfig(pc), pc.WantsIPv6, dt.config)
}

// trackStartupEgressLocked tracks egress identity id, found in Azure, with a Created operation whose NAT Gateway serves
// families and has configuration applied, as the startup baseline. Must be called with dt.mu held.
func (dt *DiffTracker) trackStartupEgressLocked(id string, families []string, applied EgressConfig) *ServiceOperationState {
	config := NewOutboundServiceConfig(id, &OutboundConfig{IPFamilies: families, Egress: applied})
	lastApplied := config.snapshot()
	now := time.Now()
	op := &ServiceOperationState{
		ServiceUID:        id,
		Config:            config,
		LastAppliedConfig: &lastApplied,
		State:             StateCreated,
		LastAttempt:       now.Format(time.RFC3339),
		CreatedAt:         now,
		CorrelationID:     uuid.NewString(),
		Egress:            egressOperationState{StartupBaseline: true},
	}
	dt.pendingServiceOps[id] = op
	return op
}

// owesBYOResidueLocked records that BYO egress identity id has leftover of a managed past: its operation, tracked
// Created when it has none, is owed an observation, which for a BYO identity is a residue cleanup (4.4); evaluate starts
// it. Must be called with dt.mu held.
func (dt *DiffTracker) owesBYOResidueLocked(id, leftover string, evaluate bool) {
	op, tracked := dt.pendingServiceOps[id]
	if !tracked {
		op = dt.trackStartupEgressLocked(id, dt.outboundIPFamiliesLocked(), egressUnknown)
	}
	op.Egress.requireObservation()
	dt.logger.V(2).Info("Found a leftover of the managed past of a BYO egress identity", "egressIdentity", id, "leftover", leftover)
	if evaluate {
		dt.evaluateEgressConsensusLocked(id)
	}
}

// unmanagedNATGatewayLocked reports whether startup recorded the NAT Gateway or service name id as one the controller
// must never manage. Must be called with dt.mu held.
func (dt *DiffTracker) unmanagedNATGatewayLocked(id string) bool {
	return dt.NRPResources.UnmanagedNATGateways != nil && dt.NRPResources.UnmanagedNATGateways.Has(id)
}

// ================================================================================================
// Confirmation pass (4.6 step 3a)
// ================================================================================================

// runEgressConfirmationPass is startup's confirmation pass. Once the egress settle delay has passed, writes of the
// previous process have landed or been dropped (sign-off 11): the pass lists the egress resources again as one
// snapshot, retried with a bounded backoff until a complete listing is read, never acting on a partial one, and owes an
// observation to every identity the listing shows changed (confirmEgressListing). Startup starts it once
// initialization completed, so it never holds initialization open. It ends with ctx.
func (dt *DiffTracker) runEgressConfirmationPass(ctx context.Context) {
	if !waitEgressDelay(ctx, getEgressSettleDelay()) {
		return
	}
	listing, ok := dt.readEgressConfirmationListing(ctx)
	if !ok {
		return
	}
	dt.confirmEgressListing(listing)
}

// readEgressConfirmationListing reads the listing of the confirmation pass, again after every failure, waiting
// egressConfirmationRetryDelay and then twice as long each time, up to the egress settle delay. It reports false once
// ctx ended.
func (dt *DiffTracker) readEgressConfirmationListing(ctx context.Context) (*egressListing, bool) {
	maxDelay := max(getEgressSettleDelay(), time.Millisecond)
	delay := min(egressConfirmationRetryDelay, maxDelay)
	for attempt := 1; ; attempt++ {
		listing, err := dt.readEgressListing(ctx)
		if err == nil {
			return listing, true
		}
		if ctx.Err() != nil {
			return nil, false
		}
		dt.logger.V(2).Info("Could not list the egress resources for the startup confirmation pass; retrying",
			"attempt", attempt, "retryAfter", delay, "err", err)
		if !waitEgressDelay(ctx, delay) {
			return nil, false
		}
		delay = min(2*delay, maxDelay)
	}
}

// readEgressListing reads the egress listing: the Service Gateway's services and the NAT Gateways, Public IPs and
// Public IP prefixes of the node resource group, each read with its own budget. A read that fails fails the listing.
func (dt *DiffTracker) readEgressListing(ctx context.Context) (*egressListing, error) {
	read := func(what string, call func(ctx context.Context) error) error {
		callCtx, cancel := egressReadContext(ctx)
		defer cancel()
		if err := call(callCtx); err != nil {
			return fmt.Errorf("listing %s: %w", what, err)
		}
		return nil
	}
	factory, rg := dt.networkClientFactory, dt.config.ResourceGroup
	var (
		services    []*armnetwork.ServiceGatewayService
		natGateways []*armnetwork.NatGateway
		publicIPs   []*armnetwork.PublicIPAddress
		prefixes    []*armnetwork.PublicIPPrefix
	)
	if err := read("Service Gateway services", func(ctx context.Context) (err error) {
		services, err = factory.GetServiceGatewayClient().GetServices(ctx, rg, dt.config.ServiceGatewayResourceName)
		return err
	}); err != nil {
		return nil, err
	}
	if err := read("NAT Gateways", func(ctx context.Context) (err error) {
		natGateways, err = factory.GetNatGatewayClient().List(ctx, rg)
		return err
	}); err != nil {
		return nil, err
	}
	if err := read("Public IPs", func(ctx context.Context) (err error) {
		publicIPs, err = factory.GetPublicIPAddressClient().List(ctx, rg)
		return err
	}); err != nil {
		return nil, err
	}
	if err := read("Public IP prefixes", func(ctx context.Context) (err error) {
		prefixes, err = factory.GetPublicIPPrefixClient().List(ctx, rg)
		return err
	}); err != nil {
		return nil, err
	}
	return newEgressListing(services, natGateways, publicIPs, prefixes)
}

// confirmEgressListing compares listing l, read by the confirmation pass, with what the engine holds:
//   - an identity that is not BYO and that the listing shows changed (egressChangedLocked) is owed an observation;
//   - a BYO identity with leftovers of its managed past is owed a residue cleanup, its operation tracked Created when
//     it has none;
//   - an identity with leftovers that has no operation, is not registered and is not BYO goes through the orphan path:
//     its deletion as an orphan, which deletes every resource of it the controller owns.
//
// The evaluation of each identity starts what it is owed, or the completion of its creation or deletion does.
func (dt *DiffTracker) confirmEgressListing(l *egressListing) {
	dt.mu.Lock()
	orphans := 0
	for _, id := range slices.Sorted(maps.Keys(dt.pendingServiceOps)) {
		op := dt.pendingServiceOps[id]
		if op.Config.IsInbound || dt.egress.mode[id] == egressModeBYO {
			continue
		}
		if change := dt.egressChangedLocked(id, op, l); change != "" {
			op.Egress.requireObservation()
			dt.logger.V(2).Info("Found an egress identity changed since the startup listing; it is owed an observation",
				"egressIdentity", id, "change", change)
			dt.evaluateEgressConsensusLocked(id)
		}
	}
	for _, id := range dt.byoEgressIdentitiesLocked() {
		if leftover := egressLeftover(id, l, dt.NRPResources.UnmanagedNATGateways, dt.config); leftover != "" {
			dt.owesBYOResidueLocked(id, leftover, true)
		}
	}
	for _, id := range l.leftoverCandidates() {
		if dt.scheduleOrphanEgressDeletionLocked(id, l) {
			orphans++
		}
	}
	dt.mu.Unlock()

	if orphans > 0 {
		dt.triggerLocationsUpdater()
	}
}

// egressChangedLocked returns how listing l shows that Azure changed for egress identity id, whose operation is op and
// which is not BYO, "" when it did not. A creation or deletion, waiting or in flight, may have read Azure before a
// write of the previous process landed, whether its mode is resolved or not: a listed resource of the identity that the
// controller owns (managedEgressLeftover) is a change. For a managed identity that is created or being updated, either
// an observation is owed (classifyManagedEgress), or the NAT Gateway no longer satisfies the applied configuration, or
// the applied configuration is unknown and the NAT Gateway now satisfies the agreed or the default one. Must be called
// with dt.mu held.
func (dt *DiffTracker) egressChangedLocked(id string, op *ServiceOperationState, l *egressListing) string {
	switch op.State {
	case StateNotStarted, StateCreationInProgress, StateDeletionPending, StateDeletionInProgress:
		if leftover := managedEgressLeftover(id, l, dt.config); leftover != "" {
			return leftover + " is listed"
		}
		return ""
	}
	if dt.egress.mode[id] != egressModeManaged {
		return ""
	}
	families := outboundFamilies(op.Config.OutboundConfig)
	c := dt.classifyManagedEgressLocked(id, l, families)
	if c.owed != "" {
		return c.owed
	}
	applied := appliedEgress(op)
	if applied == egressUnknown {
		if c.applied != egressUnknown {
			return "the NAT Gateway satisfies " + c.applied.Summary()
		}
		return ""
	}
	return natMismatch(l.natGateway(id), l.resources, id, applied, dt.egressParseContextFor(families).WantsIPv6, dt.config)
}

// byoEgressIdentitiesLocked returns the egress identities whose mode is BYO, sorted: startup records it for every
// registered BYO identity, and a creation for the identity it resolves to BYO. Must be called with dt.mu held.
func (dt *DiffTracker) byoEgressIdentitiesLocked() []string {
	var identities []string
	for id, mode := range dt.egress.mode {
		if mode == egressModeBYO {
			identities = append(identities, id)
		}
	}
	slices.Sort(identities)
	return identities
}

// scheduleOrphanEgressDeletionLocked schedules, through the orphan path, the deletion of egress identity id when
// listing l holds a leftover of it (egressLeftover) and nothing accounts for it: the identity has no operation, is not
// registered with the Service Gateway (in the listing or by this process), is not BYO and is not a name the controller
// must never manage. The check and the scheduling are one step under dt.mu, so a pod that arrives later finds the
// deletion and is handled as for any deletion. It reports whether the deletion was scheduled. Must be called with
// dt.mu held.
func (dt *DiffTracker) scheduleOrphanEgressDeletionLocked(id string, l *egressListing) bool {
	if _, tracked := dt.pendingServiceOps[id]; tracked {
		return false
	}
	if _, registered := l.registrations[id]; registered || dt.NRPResources.NATGateways.Has(id) {
		return false
	}
	if dt.byoNATGateways[id] != "" || IsReservedEgressIdentity(id) || dt.unmanagedNATGatewayLocked(id) {
		return false
	}
	leftover := egressLeftover(id, l, dt.NRPResources.UnmanagedNATGateways, dt.config)
	if leftover == "" {
		return false
	}
	dt.markOutboundServiceForDeletionLocked(id)
	dt.pendingServiceOps[id].IsOrphan = true
	dt.logger.V(2).Info("Scheduled deletion of an egress identity that left resources behind", "egressIdentity", id, "leftover", leftover)
	return true
}

// waitEgressDelay waits for d and reports true, or false once ctx ends first.
func waitEgressDelay(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// ================================================================================================
// Orphan sweep of managed Public IP prefixes (4.6 step 6)
// ================================================================================================

// taggedManagedNameOwner returns the egress identity a Public IP or prefix named name belongs to when it carries the
// identity's ownership tag and name is one of that identity's reserved managed names of kind, except a base Public IP,
// which is recognised by its name alone (identityFromPublicIPName).
func taggedManagedNameOwner(name string, tags map[string]*string, kind egressManagedKind) (string, bool) {
	owner := tags[EgressIdentityTagKey]
	if owner == nil || *owner == "" {
		return "", false
	}
	m, ok := parseManagedName(*owner, name)
	if !ok || m.Kind != kind || m.basePublicIP() {
		return "", false
	}
	return *owner, true
}

// cleanupOrphanedPublicIPPrefixes deletes the managed Public IP prefixes of egress identities that no service accounts
// for, as cleanupOrphanedPublicIPs deletes Public IPs: a prefix is recognised by its reserved name and the ownership tag
// (taggedManagedNameOwner), and is kept while its identity is desired in Kubernetes or registered with the Service
// Gateway, or while it is in use.
func (dt *DiffTracker) cleanupOrphanedPublicIPPrefixes(ctx context.Context, prefixes []*armnetwork.PublicIPPrefix) error {
	logger := dt.logger
	var orphaned []string
	dt.mu.Lock()
	for _, prefix := range prefixes {
		if prefix == nil {
			continue
		}
		name := derefString(prefix.Name)
		identity, ok := taggedManagedNameOwner(name, prefix.Tags, egressManagedPrefix)
		if !ok || IsReservedEgressIdentity(identity) {
			continue
		}
		if props := prefix.Properties; props != nil &&
			(props.NatGateway != nil || len(props.PublicIPAddresses) > 0 || props.LoadBalancerFrontendIPConfiguration != nil) {
			logger.V(5).Info("Skipped attached Public IP prefix", "prefix", name)
			continue
		}
		if dt.K8sResources.Services.Has(identity) || dt.K8sResources.Egresses.Has(identity) ||
			dt.NRPResources.LoadBalancers.Has(identity) || dt.NRPResources.NATGateways.Has(identity) {
			continue
		}
		orphaned = append(orphaned, name)
	}
	dt.mu.Unlock()

	if len(orphaned) == 0 {
		return nil
	}
	logger.V(2).Info("Found orphaned Public IP prefixes", "prefixes", len(orphaned))
	pool := NewWorkerPool(ctx, workerPoolSize)
	var deleted int32
	for _, name := range orphaned {
		pool.Submit(func() error {
			if err := dt.deleteEgressPrefix(ctx, name); err != nil {
				return fmt.Errorf("deleting orphaned Public IP prefix %s: %w", name, err)
			}
			atomic.AddInt32(&deleted, 1)
			return nil
		})
	}
	if err := pool.Wait(); err != nil {
		return fmt.Errorf("waiting for orphaned Public IP prefix cleanup: %w", err)
	}
	logger.V(2).Info("Deleted orphaned Public IP prefixes", "prefixes", atomic.LoadInt32(&deleted))
	return nil
}
