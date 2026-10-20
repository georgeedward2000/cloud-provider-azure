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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork/v12"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/component-base/metrics/testutil"

	"sigs.k8s.io/cloud-provider-azure/pkg/azclient"
	"sigs.k8s.io/cloud-provider-azure/pkg/azclient/natgatewayclient"
	"sigs.k8s.io/cloud-provider-azure/pkg/consts"
	"sigs.k8s.io/cloud-provider-azure/pkg/log"
)

// egressRunsID is the egress identity the end-to-end runs work on: the one the fake ARM checks the writes of.
const egressRunsID = egressCleanupTestID

const (
	egressRunsPutPIP     = "PUT publicIPAddresses/egress-a-pip"
	egressRunsPutPIP2    = "PUT publicIPAddresses/egress-a-pip-2"
	egressRunsBYONATCall = egressCleanupTestBYORG + "/natGateways/" + egressRunsID
)

// egressRunsHarness runs the operations of egress identity egressRunsID end to end: the dispatcher (processBatch)
// starts the run the operation records, the run calls the fake ARM (fakeARM) through the real azclient
// pipelines, and its completion updates the engine (onServiceOperationComplete), as in production.
type egressRunsHarness struct {
	t     *testing.T
	cloud *fakeARM
	dt    *DiffTracker
	su    *ServiceUpdater
}

func newEgressRunsHarness(t *testing.T, config Config) *egressRunsHarness {
	t.Helper()
	dt := newTestDiffTracker()
	dt.config = config
	dt.logger = log.Noop()
	cloud := newFakeARM(t, dt)
	su := NewServiceUpdater(context.Background(), dt, dt.OnServiceCreationComplete, dt.onServiceOperationComplete, dt.serviceUpdaterTrigger)
	t.Cleanup(su.Stop)
	return &egressRunsHarness{t: t, cloud: cloud, dt: dt, su: su}
}

// dispatch lets the dispatcher start every run that is due and waits until they have completed.
func (h *egressRunsHarness) dispatch() {
	h.t.Helper()
	drainServiceUpdaterTrigger(h.dt)
	h.su.processBatch()
	h.su.wg.Wait()
}

// op returns the operation of the test identity; nil once it is no longer tracked.
func (h *egressRunsHarness) op() *ServiceOperationState {
	h.dt.mu.Lock()
	defer h.dt.mu.Unlock()
	return h.dt.pendingServiceOps[egressRunsID]
}

// track tracks the test identity as an operation in state, whose NAT Gateway serves families (IPv4 when none) and is
// provided as mode says, with desired as its desired configuration and applied as its applied one (nil: the identity
// was never created).
func (h *egressRunsHarness) track(state ResourceState, mode egressMode, desired EgressConfig, applied *EgressConfig,
	families ...string) *ServiceOperationState {
	if len(families) == 0 {
		families = ipv4Families()
	}
	op := &ServiceOperationState{
		ServiceUID:    egressRunsID,
		Config:        NewOutboundServiceConfig(egressRunsID, &OutboundConfig{IPFamilies: slices.Clone(families), Egress: desired}),
		State:         state,
		CorrelationID: "corr",
	}
	if applied != nil {
		config := NewOutboundServiceConfig(egressRunsID, &OutboundConfig{IPFamilies: slices.Clone(families), Egress: *applied})
		op.LastAppliedConfig = &config
	}
	h.dt.mu.Lock()
	defer h.dt.mu.Unlock()
	h.dt.pendingServiceOps[egressRunsID] = op
	h.dt.egress.mode[egressRunsID] = mode
	if applied != nil {
		h.dt.NRPResources.NATGateways.Insert(egressRunsID)
	}
	return op
}

// owe makes the test identity owed an observation, as startup or an earlier run does.
func (h *egressRunsHarness) owe() {
	h.dt.mu.Lock()
	defer h.dt.mu.Unlock()
	h.dt.pendingServiceOps[egressRunsID].Egress.requireObservation()
}

// vote registers the vote of pod ns/<pod> of the test identity for cfg.
func (h *egressRunsHarness) vote(pod string, cfg EgressConfig) {
	h.dt.mu.Lock()
	defer h.dt.mu.Unlock()
	castEgressVote(h.dt, egressRunsID, "ns/"+pod, &cfg, consensusEpoch)
}

// allowBYOWrites accepts the writes of a BYO identity's link and registration, which are not managed writes and mark
// nothing.
func (h *egressRunsHarness) allowBYOWrites() {
	h.cloud.update(func(c *fakeARM) {
		c.unnotedAllowed = func(call string) bool { return call == "PUT "+egressRunsBYONATCall || call == cleanupCallRegister }
	})
}

// evaluate lets the engine align the test identity's operation with its votes, as a vote change does.
func (h *egressRunsHarness) evaluate() {
	h.dt.mu.Lock()
	defer h.dt.mu.Unlock()
	h.dt.evaluateEgressConsensusLocked(egressRunsID)
}

// addPod adds pod ns/<pod>, voting for cfg, to the test identity through the engine's entry point.
func (h *egressRunsHarness) addPod(pod string, cfg EgressConfig, annotated bool) {
	ref := egressPodRef{Namespace: "ns", Name: pod, UID: types.UID(pod + "-uid"), Created: consensusEpoch}
	h.dt.AddEgressPod(egressRunsID, ref, egressVote{Config: cfg, HasAnnotations: annotated},
		[]egressAddress{{IP: "10.244.0.5", Location: "10.0.0.1"}})
}

// events returns the egress events of the given reason raised so far, as "reason ns/name", and empties the queue.
func (h *egressRunsHarness) events(reason string) []string {
	var events []string
	for _, event := range takeEgressEvents(h.dt) {
		if strings.HasPrefix(event, reason+" ") {
			events = append(events, event)
		}
	}
	return events
}

// assertSettled checks that the test identity is Created with applied as its applied configuration and owes nothing.
func (h *egressRunsHarness) assertSettled(applied EgressConfig) {
	h.t.Helper()
	op := h.op()
	if !assert.NotNil(h.t, op) {
		return
	}
	assert.Equal(h.t, StateCreated, op.State)
	assert.Equal(h.t, &applied, appliedEgressOf(op))
	assert.False(h.t, op.Egress.NeedsObservation, "nothing is owed")
	assert.False(h.t, op.Egress.QuietUntil.After(time.Now()), "no quiet period")
	assert.False(h.t, op.Egress.MutationStarted, "the completion clears the write-ahead flag")
	assert.Nil(h.t, op.InFlightConfig)
	assert.Zero(h.t, op.RetryCount)
}

// managedNATReferences returns the names of the Public IPs and prefixes the stored NAT Gateway of the test identity
// references, sorted.
func (h *egressRunsHarness) managedNATReferences() []string {
	nat := h.cloud.nat(h.dt.config.ResourceGroup, egressRunsID)
	if nat == nil || nat.Properties == nil {
		return nil
	}
	var names []string
	for _, ref := range slices.Concat(nat.Properties.PublicIPAddresses, nat.Properties.PublicIPPrefixes,
		nat.Properties.PublicIPAddressesV6, nat.Properties.PublicIPPrefixesV6) {
		id := derefString(ref.ID)
		names = append(names, id[strings.LastIndex(id, "/")+1:])
	}
	slices.Sort(names)
	return names
}

func egressRunsJSON(t *testing.T, v any) string {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

// egressRunsRegistration returns the body of the request that registered an egress identity with its managed NAT
// Gateway before configuration existed: the registration DTO of the identity, converted as updateNRPSGWServices does.
func egressRunsRegistration(t *testing.T, config Config) string {
	t.Helper()
	dto := MapLoadBalancerAndNATGatewayUpdatesToServicesDataDTO(SyncServicesReturnType{},
		SyncServicesReturnType{Additions: newIgnoreCaseSetFromSlice([]string{egressRunsID})}, config.networkResourceSubscriptionID(), config.ResourceGroup)
	requests, err := convertServiceDTOsToServiceRequests(dto.Services, config)
	if err != nil {
		t.Fatal(err)
	}
	return egressRunsJSON(t, armnetwork.ServiceGatewayUpdateServicesRequest{
		Action: convertServicesUpdateActionToARM(dto.Action), ServiceRequests: requests})
}

// TestEgressRuns_CreateWithTheDefault: a new identity whose pods have no egress annotations gets exactly the requests
// it got before configuration existed, byte for byte and in the same order: its Public IP, its NAT Gateway, then its
// registration. The identity is Created with the default applied, recorded in NRP, and owes nothing.
func TestEgressRuns_CreateWithTheDefault(t *testing.T) {
	h := newEgressRunsHarness(t, testConfig())
	defaultV4 := defaultEgressConfig(egressParseContext{})
	h.addPod("pod-a", defaultV4, false)

	h.dispatch()

	h.assertSettled(defaultV4)
	assert.Equal(t, egressModeManaged, h.dt.egress.mode[egressRunsID])
	assert.True(t, h.dt.NRPResources.NATGateways.Has(egressRunsID), "the created identity is recorded in NRP")
	assert.Empty(t, h.dt.pendingPods[egressRunsID], "the buffered pod is promoted")

	pips, nat := egressCreationRequests(t, egressRunsID, defaultV4, false, h.dt.config)
	if !assert.Equal(t, []string{egressRunsPutPIP, cleanupCallPutNAT, cleanupCallRegister}, h.cloud.writes(), "today's writes, in today's order") {
		return
	}
	assert.Equal(t, egressRunsJSON(t, pips[0]), string(h.cloud.requestsOf(egressRunsPutPIP)[0].Body), "today's Public IP request")
	assert.Equal(t, egressRunsJSON(t, nat), string(h.cloud.requestsOf(cleanupCallPutNAT)[0].Body), "today's NAT Gateway request")
	assert.Equal(t, egressRunsRegistration(t, h.dt.config), string(h.cloud.requestsOf(cleanupCallRegister)[0].Body), "today's registration")
	assert.Equal(t, []string{"egress-a-pip"}, h.managedNATReferences())
	assert.Empty(t, h.events(consts.ServiceEgressGatewayReasonConfigApplied), "the default at first creation is not announced")
}

// TestEgressRuns_CreateWithTheDualStackDefault: a new identity of a dual-stack cluster whose pods have no egress
// annotations is created serving both families with their default: its NAT Gateway references one managed Public IP
// of each family. The identity is Created with the dual-stack default applied, which is not announced.
func TestEgressRuns_CreateWithTheDualStackDefault(t *testing.T) {
	h := newEgressRunsHarness(t, testConfig())
	h.dt.clusterHasIPv6.Store(true)
	defaultDual := defaultEgressConfig(egressParseContext{WantsIPv6: true})
	h.addPod("pod-a", defaultDual, false)

	h.dispatch()

	h.assertSettled(defaultDual)
	assert.Equal(t, dualStackFamilies(), outboundFamilies(h.op().Config.OutboundConfig), "control: the identity serves both families")
	assert.Equal(t, []string{"egress-a-pip", "egress-a-pip-v6"}, h.managedNATReferences(), "one managed Public IP of each family")
	assert.Empty(t, h.events(consts.ServiceEgressGatewayReasonConfigApplied), "the default at first creation is not announced")
}

// TestEgressRuns_CreateWithAConfiguration: a new identity whose pods agree on two managed Public IPs and a managed /31
// prefix is created with them: the managed resources first, then the NAT Gateway referencing them, then the
// registration, each the projection of the configuration.
func TestEgressRuns_CreateWithAConfiguration(t *testing.T) {
	h := newEgressRunsHarness(t, testConfig())
	cfg := egressCleanupConfig(t, false, map[string]string{
		consts.PodAnnotationServiceEgressGatewayPublicIPCount:        "2",
		consts.PodAnnotationServiceEgressGatewayPublicIPPrefixLength: "31",
	})
	h.addPod("pod-a", cfg, true)

	h.dispatch()

	h.assertSettled(cfg)
	assert.True(t, h.dt.NRPResources.NATGateways.Has(egressRunsID))
	writes := h.cloud.writes()
	if !assert.Len(t, writes, 5, "writes: %v", writes) {
		return
	}
	assert.ElementsMatch(t, []string{egressRunsPutPIP, egressRunsPutPIP2, "PUT publicIPPrefixes/egress-a-prefix-31"}, writes[:3],
		"the managed resources are written in one phase, before the NAT Gateway")
	assert.Equal(t, []string{cleanupCallPutNAT, cleanupCallRegister}, writes[3:])
	config := h.dt.config
	nat, err := natRequest(nil, egressRunsID, cfg, false, config)
	assert.NoError(t, err)
	assert.Equal(t, egressRunsJSON(t, nat), string(h.cloud.requestsOf(cleanupCallPutNAT)[0].Body))
	for n, call := range map[int]string{1: egressRunsPutPIP, 2: egressRunsPutPIP2} {
		pip, err := managedPublicIPRequest(nil, egressRunsID, false, n, cfg, config)
		assert.NoError(t, err)
		assert.Equal(t, egressRunsJSON(t, pip), string(h.cloud.requestsOf(call)[0].Body))
	}
	prefix, err := managedPrefixRequest(nil, egressRunsID, false, 31, cfg, config)
	assert.NoError(t, err)
	assert.Equal(t, egressRunsJSON(t, prefix), string(h.cloud.requestsOf("PUT publicIPPrefixes/egress-a-prefix-31")[0].Body))
	assert.Equal(t, []string{"egress-a-pip", "egress-a-pip-2", "egress-a-prefix-31"}, h.managedNATReferences())
}

// TestEgressRuns_CreateFallsBackToTheDefault: a creation configuration that is rejected gives the identity the default
// in the same run, so it gets a working NAT Gateway, and the rejection is recorded and announced to the pods voting for
// it: a terminal one keeps the identity Created on the default; a retryable one is retried in the background, after a
// backoff, while the pods agree on it.
func TestEgressRuns_CreateFallsBackToTheDefault(t *testing.T) {
	const userPIP = "/subscriptions/sub/resourceGroups/user-rg/providers/Microsoft.Network/publicIPAddresses/user-pip"
	defaultV4 := defaultEgressConfig(egressParseContext{})

	for _, tc := range []struct {
		name      string
		userPIP   func() *armnetwork.PublicIPAddress // nil: the user Public IP does not exist
		retryable bool
	}{
		{name: "terminal: the user Public IP has the wrong SKU", userPIP: func() *armnetwork.PublicIPAddress {
			pip := egressApplyTestUserPIP(userPIP, false)
			pip.SKU.Name = to.Ptr(armnetwork.PublicIPAddressSKUNameStandard)
			return pip
		}},
		{name: "retryable: the user Public IP does not exist", retryable: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newEgressRunsHarness(t, testConfig())
			cfg := egressCleanupConfig(t, false, map[string]string{consts.PodAnnotationServiceEgressGatewayPublicIPIDs: userPIP})
			if tc.userPIP != nil {
				h.cloud.update(func(c *fakeARM) { c.pips[fakeARMKey("user-rg", "user-pip")] = tc.userPIP() })
			}
			h.addPod("pod-a", cfg, true)

			h.dispatch()

			op := h.op()
			assert.Equal(t, &defaultV4, appliedEgressOf(op), "the identity got the default")
			assert.True(t, h.dt.NRPResources.NATGateways.Has(egressRunsID))
			assert.Equal(t, []string{egressRunsPutPIP, cleanupCallPutNAT, cleanupCallRegister}, h.cloud.writes(),
				"the default is written; nothing of the rejected configuration")
			assert.Equal(t, []string{"egress-a-pip"}, h.managedNATReferences())
			assert.Equal(t, []string{consts.ServiceEgressGatewayReasonConfigRejected + " ns/pod-a"},
				h.events(consts.ServiceEgressGatewayReasonConfigRejected))
			rejected, terminal := h.dt.egress.rejected[egressRunsID]
			warned, retryable := h.dt.egress.warned[egressRunsID]
			assert.Equal(t, !tc.retryable, terminal, "terminal rejection recorded")
			assert.Equal(t, tc.retryable, retryable, "retryable rejection recorded")
			if tc.retryable {
				assert.Equal(t, cfg, warned.Config)
				assert.Equal(t, StateUpdateInProgress, op.State, "the requested configuration is retried in the background")
				assert.Equal(t, cfg, outboundEgress(op.Config.OutboundConfig))
				assertBackoff(t, op, true)
				return
			}
			assert.Equal(t, cfg, rejected.Config)
			h.assertSettled(defaultV4)
		})
	}
}

// TestEgressRuns_CreateFallsBackToTheDualStackDefault: a dual-stack identity whose creation configuration is rejected
// falls back, in the same run, to the default of both its families, so its NAT Gateway references one managed Public
// IP of each family.
func TestEgressRuns_CreateFallsBackToTheDualStackDefault(t *testing.T) {
	const userPIP = "/subscriptions/sub/resourceGroups/user-rg/providers/Microsoft.Network/publicIPAddresses/user-pip"
	h := newEgressRunsHarness(t, testConfig())
	h.dt.clusterHasIPv6.Store(true)
	defaultDual := defaultEgressConfig(egressParseContext{WantsIPv6: true})
	cfg := egressCleanupConfig(t, true, map[string]string{consts.PodAnnotationServiceEgressGatewayPublicIPIDs: userPIP})
	h.addPod("pod-a", cfg, true)

	h.dispatch()

	assert.Equal(t, cfg, h.dt.egress.warned[egressRunsID].Config, "control: the creation configuration was rejected (its user Public IP does not exist)")
	assert.Equal(t, &defaultDual, appliedEgressOf(h.op()), "the identity got the default of both families")
	assert.Equal(t, []string{"egress-a-pip", "egress-a-pip-v6"}, h.managedNATReferences())
}

// TestEgressRuns_CreateFallbackSweepsTheRejectedConfiguration: when Azure refuses one write of a creation configuration
// after the other writes of its phase succeeded, the identity gets the default in the same run and the sweep deletes
// what the rejected configuration created, so nothing of it is left and nothing is owed.
func TestEgressRuns_CreateFallbackSweepsTheRejectedConfiguration(t *testing.T) {
	const putPrefix = "PUT publicIPPrefixes/egress-a-prefix-31"
	defaultV4 := defaultEgressConfig(egressParseContext{})
	for _, tc := range []struct {
		name      string
		code      string
		retryable bool
	}{
		{name: "terminal: Azure refuses the prefix", code: "InvalidParameter"},
		{name: "retryable: no prefix quota left", code: "PublicIPPrefixCountLimitReached", retryable: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newEgressRunsHarness(t, testConfig())
			cfg := egressCleanupConfig(t, false, map[string]string{
				consts.PodAnnotationServiceEgressGatewayPublicIPCount:        "2",
				consts.PodAnnotationServiceEgressGatewayPublicIPPrefixLength: "31",
			})
			h.cloud.fault(putPrefix, armReplyError(http.StatusBadRequest, tc.code))
			h.addPod("pod-a", cfg, true)

			h.dispatch()

			writes := h.cloud.writes()
			if !assert.Len(t, writes, 6, "writes: %v", writes) {
				return
			}
			assert.ElementsMatch(t, []string{egressRunsPutPIP, egressRunsPutPIP2, putPrefix}, writes[:3])
			assert.Equal(t, []string{cleanupCallPutNAT, cleanupCallRegister, "DELETE publicIPAddresses/egress-a-pip-2"}, writes[3:],
				"the default is applied, then the Public IP the rejected configuration created is swept")
			assert.Equal(t, []string{"egress-a-pip"}, h.managedNATReferences())
			var left bool
			h.cloud.update(func(c *fakeARM) { _, left = c.pips[fakeARMKey(h.dt.config.ResourceGroup, "egress-a-pip-2")] })
			assert.False(t, left, "nothing the rejected configuration created is left")
			op := h.op()
			assert.Equal(t, &defaultV4, appliedEgressOf(op))
			assert.False(t, op.Egress.NeedsObservation, "the sweep completed: nothing is owed")
			if tc.retryable {
				assert.Equal(t, cfg, h.dt.egress.warned[egressRunsID].Config)
				assert.Equal(t, StateUpdateInProgress, op.State, "the requested configuration is retried in the background")
				return
			}
			assert.Equal(t, cfg, h.dt.egress.rejected[egressRunsID].Config)
			h.assertSettled(defaultV4)
		})
	}
}

// TestEgressRuns_UpdateAppliesInPlace: a new agreement is applied to the identity's NAT Gateway in place, by the update
// run that replaces the old skip: two Public IPs, then back to one, whose leftover is swept. The NAT Gateway is never
// deleted, and every real update is recorded like an inbound one.
func TestEgressRuns_UpdateAppliesInPlace(t *testing.T) {
	RegisterMetrics()
	serviceOperationTotal.Reset()
	serviceOperationRetries.Reset()

	h := newEgressRunsHarness(t, testConfig())
	defaultV4 := defaultEgressConfig(egressParseContext{})
	two := egressCleanupConfig(t, false, map[string]string{consts.PodAnnotationServiceEgressGatewayPublicIPCount: "2"})
	h.cloud.seedApplied(defaultV4, false)
	h.cloud.register(egressRunsID, natGatewayID(egressRunsID, h.dt.config))
	h.track(StateCreated, egressModeManaged, defaultV4, &defaultV4)
	h.vote("pod-a", two)
	h.vote("pod-b", two)
	h.evaluate()
	before := derefString(h.cloud.nat(h.dt.config.ResourceGroup, egressRunsID).Etag)

	h.dispatch()

	h.assertSettled(two)
	assert.Equal(t, []string{egressRunsPutPIP2, cleanupCallPutNAT}, h.cloud.writes(), "only what differs is written")
	assert.NotEqual(t, before, derefString(h.cloud.nat(h.dt.config.ResourceGroup, egressRunsID).Etag), "control: the NAT Gateway was written")
	assert.Equal(t, []string{"egress-a-pip", "egress-a-pip-2"}, h.managedNATReferences())
	assert.ElementsMatch(t, []string{consts.ServiceEgressGatewayReasonConfigApplied + " ns/pod-a", consts.ServiceEgressGatewayReasonConfigApplied + " ns/pod-b"},
		h.events(consts.ServiceEgressGatewayReasonConfigApplied))

	// Back to one Public IP: the NAT Gateway drops the second, which the sweep then deletes.
	h.cloud.resetRequests()
	h.vote("pod-a", defaultV4)
	h.vote("pod-b", defaultV4)
	h.evaluate()
	h.dispatch()

	h.assertSettled(defaultV4)
	assert.Equal(t, []string{cleanupCallPutNAT, "DELETE publicIPAddresses/egress-a-pip-2"}, h.cloud.writes())
	assert.Equal(t, []string{"egress-a-pip"}, h.managedNATReferences())
	pips, _ := h.cloud.resourceNames()
	assert.Equal(t, []string{"egress-a-pip"}, pips)
	assert.Empty(t, h.cloud.requestsOf(cleanupCallDeleteNAT), "the NAT Gateway is changed in place")

	testutil.AssertVectorCount(t, "difftracker_engine_service_operations_total",
		map[string]string{"operation": "update", "service_type": "outbound", "result": "success"}, 2)
	retries, err := testutil.GetHistogramMetricCount(serviceOperationRetries.WithLabelValues("update", "outbound"))
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), retries, "each update observes its retries once")
}

// TestEgressRuns_UpdateAppliesBothFamilies: an update of a dual-stack identity applies the agreed configuration to both
// families of its NAT Gateway, in place: from two IPv6 Public IPs back to one, the NAT Gateway keeps the Public IP of
// each family and drops the second IPv6 one, which the sweep then deletes.
func TestEgressRuns_UpdateAppliesBothFamilies(t *testing.T) {
	h := newEgressRunsHarness(t, testConfig())
	defaultDual := defaultEgressConfig(egressParseContext{WantsIPv6: true})
	twoV6 := egressCleanupConfig(t, true, map[string]string{consts.PodAnnotationServiceEgressGatewayPublicIPCountIPv6: "2"})
	h.cloud.seedApplied(twoV6, true)
	h.cloud.register(egressRunsID, natGatewayID(egressRunsID, h.dt.config))
	h.track(StateCreated, egressModeManaged, twoV6, &twoV6, dualStackFamilies()...)
	h.vote("pod-a", defaultDual)
	h.evaluate()

	h.dispatch()

	h.assertSettled(defaultDual)
	assert.Equal(t, []string{cleanupCallPutNAT, "DELETE publicIPAddresses/egress-a-pip-v6-2"}, h.cloud.writes())
	assert.Equal(t, []string{"egress-a-pip", "egress-a-pip-v6"}, h.managedNATReferences())
	pips, _ := h.cloud.resourceNames()
	assert.Equal(t, []string{"egress-a-pip", "egress-a-pip-v6"}, pips)
}

// TestEgressRuns_UpdateSkipsWhenSatisfied: an update whose NAT Gateway already satisfies the agreed configuration (here
// of an identity whose applied configuration is unknown with none known before, as for one adopted at startup) writes
// nothing, records it applied and does not announce it: the previous process did, if anything.
func TestEgressRuns_UpdateSkipsWhenSatisfied(t *testing.T) {
	h := newEgressRunsHarness(t, testConfig())
	two := egressCleanupConfig(t, false, map[string]string{consts.PodAnnotationServiceEgressGatewayPublicIPCount: "2"})
	h.cloud.seedApplied(two, false)
	h.cloud.register(egressRunsID, natGatewayID(egressRunsID, h.dt.config))
	h.track(StateCreated, egressModeManaged, egressUnknown, &egressUnknown)
	h.vote("pod-a", two)
	h.evaluate()
	assert.Equal(t, StateUpdateInProgress, h.op().State, "control: the agreement is dispatchable")

	h.dispatch()

	h.assertSettled(two)
	assert.Empty(t, h.cloud.writes(), "a satisfied NAT Gateway gets no write")
	assert.Subset(t, h.cloud.reservedCalls(http.MethodGet), cleanupReservedReads(false), "the reserved names are still swept")
	assert.Empty(t, h.events(consts.ServiceEgressGatewayReasonConfigApplied), "a configuration found applied when none was known before is not announced")
}

// TestEgressObservationTargets pins what an observation looks for before the default, in that order: the agreed
// configuration; the configurations of the writes Azure did not answer clearly, newest first; and the one last known
// applied, which is the applied one while it is known, else the one remembered before it became unknown, and none for
// an identity adopted or never applied. The targets are copies.
func TestEgressObservationTargets(t *testing.T) {
	agreeA := []*EgressConfig{&consensusA, &consensusA}
	disagree := []*EgressConfig{&consensusA, &consensusB}
	for _, tc := range []struct {
		name string
		run  *egressRun
		want []*EgressConfig // agreed, every unclear write newest first, known
	}{
		{name: "not tracked", want: []*EgressConfig{nil, nil}},
		{name: "the applied configuration is known",
			run:  &egressRun{state: StateCreated, mode: egressModeManaged, desired: consensusA, applied: &consensusA, votes: agreeA},
			want: []*EgressConfig{&consensusA, &consensusA}},
		{name: "unknown after a write Azure did not answer clearly",
			run: &egressRun{state: StateUpdateInProgress, mode: egressModeManaged, desired: egressUnknown, applied: &egressUnknown,
				lastKnown: &consensusApplied, unclear: []EgressConfig{consensusB}, votes: disagree},
			want: []*EgressConfig{nil, &consensusB, &consensusApplied}},
		{name: "two writes Azure did not answer clearly: both are looked for, the newest first",
			run: &egressRun{state: StateUpdateInProgress, mode: egressModeManaged, desired: egressUnknown, applied: &egressUnknown,
				lastKnown: &consensusApplied, unclear: []EgressConfig{consensusB, consensusA}, votes: disagree},
			want: []*EgressConfig{nil, &consensusB, &consensusA, &consensusApplied}},
		{name: "an agreement and a write Azure did not answer clearly: both are looked for",
			run: &egressRun{state: StateUpdateInProgress, mode: egressModeManaged, desired: consensusB, applied: &egressUnknown,
				lastKnown: &consensusApplied, unclear: []EgressConfig{consensusA}, votes: []*EgressConfig{&consensusB, &consensusB}},
			want: []*EgressConfig{&consensusB, &consensusA, &consensusApplied}},
		{name: "unknown with nothing known before it, as after an adoption",
			run: &egressRun{state: StateUpdateInProgress, mode: egressModeManaged, desired: egressUnknown, applied: &egressUnknown,
				votes: disagree},
			want: []*EgressConfig{nil, nil}},
		{name: "never applied",
			run:  &egressRun{state: StateCreationInProgress, mode: egressModeManaged, desired: consensusA, votes: agreeA},
			want: []*EgressConfig{&consensusA, nil}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dt := newTestDiffTracker()
			var op *ServiceOperationState
			if tc.run != nil {
				op = trackEgressRun(dt, *tc.run)
			}

			targets := dt.egressObservationTargets(egressRunID)

			assert.Equal(t, tc.want, targets, "agreed, unclear writes, known, in that order")
			if op == nil {
				return
			}
			// The targets are copies, never pointers into the operation state.
			for i := range op.Egress.UnclearWrites {
				assert.NotSame(t, &op.Egress.UnclearWrites[i], targets[1+i], "unclear write %d", i)
			}
			known := targets[len(targets)-1]
			if op.Egress.LastKnownApplied != nil {
				assert.NotSame(t, op.Egress.LastKnownApplied, known, "the last known configuration")
			}
			if op.LastAppliedConfig != nil && op.LastAppliedConfig.OutboundConfig != nil {
				assert.NotSame(t, &op.LastAppliedConfig.OutboundConfig.Egress, known, "the applied configuration")
			}
		})
	}
}

// TestEgressRuns_ObservationFindsTheRightConfiguration: an observation, which runs while the members disagree, records
// the configuration the NAT Gateway still has rather than the default, which a NAT Gateway whose only difference is tags
// also satisfies, so that nothing is announced that did not happen: after a write Azure did not answer clearly and that
// did not land, and for an owed observation of a known configuration. A write that landed is found, and announced,
// although the NAT Gateway also satisfies the configuration known before it.
func TestEgressRuns_ObservationFindsTheRightConfiguration(t *testing.T) {
	applied := consts.ServiceEgressGatewayReasonConfigApplied
	tagsKey := consts.PodAnnotationServiceEgressGatewayTags
	for _, tc := range []struct {
		name       string
		next       map[string]string // the annotations of the agreement whose write Azure does not answer; nil: none
		landed     bool
		wantNext   bool // the observation finds the next configuration, else the one known before it
		wantEvents bool
	}{
		{name: "a write that did not land: the configuration known before it, not announced",
			next: map[string]string{tagsKey: "team=net", consts.PodAnnotationServiceEgressGatewayIdleTimeout: "10"}},
		{name: "a write that landed and added a tag: that configuration, announced", next: map[string]string{tagsKey: "team=net,env=prod"},
			landed: true, wantNext: true, wantEvents: true},
		{name: "an owed observation of a known configuration: kept, not announced"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newEgressRunsHarness(t, testConfig())
			tagged := egressCleanupConfig(t, false, map[string]string{tagsKey: "team=net"})
			other := egressCleanupConfig(t, false, map[string]string{consts.PodAnnotationServiceEgressGatewayPublicIPCount: "2"})
			assert.NotEqual(t, defaultEgressConfig(egressParseContext{}), tagged, "control: the configuration differs from the default")
			h.cloud.seedApplied(tagged, false)
			h.cloud.register(egressRunsID, natGatewayID(egressRunsID, h.dt.config))
			op := h.track(StateCreated, egressModeManaged, tagged, &tagged)
			want := tagged
			if tc.next == nil {
				h.owe()
			} else {
				next := egressCleanupConfig(t, false, tc.next)
				if tc.wantNext {
					want = next
				}
				h.vote("pod-a", next)
				h.vote("pod-b", next)
				h.evaluate()
				h.cloud.fault(cleanupCallPutNAT, func(w http.ResponseWriter, r *http.Request) {
					if tc.landed {
						h.cloud.seedApplied(next, false)
					}
					armReplyError(http.StatusInternalServerError, "InternalServerError")(w, r)
				})
				h.dispatch()
				assert.Equal(t, &egressUnknown, appliedEgressOf(op), "control: the outcome of the write is unknown")
			}
			h.vote("pod-a", tagged)
			h.vote("pod-b", other)
			h.evaluate()
			assert.Equal(t, egressUnknown, outboundEgress(op.Config.OutboundConfig), "control: without agreement an observation runs")
			h.cloud.resetRequests()
			h.dt.mu.Lock()
			op.Egress.QuietUntil, op.NextRetryAt = time.Now().Add(-time.Second), time.Time{}
			h.dt.mu.Unlock()
			h.dispatch()

			assert.Equal(t, &want, appliedEgressOf(op))
			assert.Empty(t, h.cloud.writes(), "an observation writes no configuration")
			var wantEvents []string
			if tc.wantEvents {
				wantEvents = []string{applied + " ns/pod-a", applied + " ns/pod-b"}
			}
			assert.Equal(t, wantEvents, h.events(applied))
		})
	}
}

// TestEgressRuns_UpdateFromUnknownAnnouncesWhatItWrites: an update of an identity whose applied configuration is
// unknown (a drift found at startup, or a write whose outcome was unknown) that writes the agreed configuration to the
// NAT Gateway announces it to the members, as any change of the applied configuration.
func TestEgressRuns_UpdateFromUnknownAnnouncesWhatItWrites(t *testing.T) {
	h := newEgressRunsHarness(t, testConfig())
	two := egressCleanupConfig(t, false, map[string]string{consts.PodAnnotationServiceEgressGatewayPublicIPCount: "2"})
	h.cloud.seedApplied(defaultEgressConfig(egressParseContext{}), false)
	h.cloud.register(egressRunsID, natGatewayID(egressRunsID, h.dt.config))
	h.track(StateCreated, egressModeManaged, egressUnknown, &egressUnknown)
	h.vote("pod-a", two)
	h.evaluate()
	assert.Equal(t, StateUpdateInProgress, h.op().State, "control: the agreement is dispatchable")

	h.dispatch()

	h.assertSettled(two)
	assert.Equal(t, []string{egressRunsPutPIP2, cleanupCallPutNAT}, h.cloud.writes(), "control: the NAT Gateway is written")
	assert.Equal(t, []string{consts.ServiceEgressGatewayReasonConfigApplied + " ns/pod-a"}, h.events(consts.ServiceEgressGatewayReasonConfigApplied))
}

// TestEgressRuns_SatisfiedUpdateOwesWhatItLeftUndone: an update whose NAT Gateway already satisfies the agreed
// configuration writes no PUT, yet what its link repair or sweep could not do is owed all the same. The configuration
// is recorded applied and an observation follows after a backoff, which repairs the registration or sweeps the
// leftover.
func TestEgressRuns_SatisfiedUpdateOwesWhatItLeftUndone(t *testing.T) {
	const leftover = "egress-a-pip-9"
	leftoverDeleted := []string{"DELETE publicIPAddresses/" + leftover}
	refuseRegistrations := func(h *egressRunsHarness, answer http.HandlerFunc) {
		h.cloud.update(func(c *fakeARM) { c.failures[strings.ToLower(cleanupCallRegister)] = answer })
	}
	for _, tc := range []struct {
		name string
		// setup makes the link repair or the sweep fail; recover lets the observation succeed.
		setup, recover func(h *egressRunsHarness)
		wantWrites     []string
	}{
		{name: "the registration is refused", setup: func(h *egressRunsHarness) {
			h.cloud.register(egressRunsID, "")
			refuseRegistrations(h, armReplyError(http.StatusConflict, "Conflict"))
		}, recover: func(h *egressRunsHarness) { refuseRegistrations(h, nil) }, wantWrites: []string{cleanupCallRegister}},
		{name: "a reserved name cannot be read", setup: func(h *egressRunsHarness) {
			h.cloud.seedPublicIP(egressCleanupPIP(leftover, cleanupOwnTag(), armnetwork.ProvisioningStateSucceeded))
			h.cloud.failEvery(http.MethodGet, publicIPAddressID(leftover, h.dt.config), armReplyError(http.StatusServiceUnavailable, "ServiceUnavailable"))
		}, recover: func(h *egressRunsHarness) {
			h.cloud.failEvery(http.MethodGet, publicIPAddressID(leftover, h.dt.config), nil)
		}, wantWrites: leftoverDeleted},
		{name: "a reserved name is not settled", setup: func(h *egressRunsHarness) {
			h.cloud.seedPublicIP(egressCleanupPIP(leftover, cleanupOwnTag(), armnetwork.ProvisioningStateUpdating))
		}, recover: func(h *egressRunsHarness) {
			h.cloud.update(func(c *fakeARM) {
				c.pips[fakeARMKey(h.dt.config.ResourceGroup, leftover)].Properties.ProvisioningState = to.Ptr(armnetwork.ProvisioningStateSucceeded)
			})
		}, wantWrites: leftoverDeleted},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newEgressRunsHarness(t, testConfig())
			defaultV4 := defaultEgressConfig(egressParseContext{})
			two := egressCleanupConfig(t, false, map[string]string{consts.PodAnnotationServiceEgressGatewayPublicIPCount: "2"})
			h.cloud.seedApplied(two, false)
			h.cloud.register(egressRunsID, natGatewayID(egressRunsID, h.dt.config))
			h.track(StateCreated, egressModeManaged, defaultV4, &defaultV4)
			h.vote("pod-a", two)
			tc.setup(h)
			h.evaluate()

			h.dispatch()

			op := h.op()
			assert.Empty(t, slices.DeleteFunc(h.cloud.writes(), func(call string) bool { return !strings.HasPrefix(call, http.MethodPut+" ") }),
				"control: the satisfied NAT Gateway gets no PUT")
			assert.Equal(t, &two, appliedEgressOf(op))
			assert.True(t, op.Egress.NeedsObservation, "what the run left undone is owed")
			assert.Equal(t, StateUpdateInProgress, op.State)
			assert.Equal(t, egressUnknown, outboundEgress(op.Config.OutboundConfig), "an observation follows")
			assert.Equal(t, 1, op.RetryCount)
			assertBackoff(t, op, true)

			h.cloud.resetRequests()
			h.dispatch()
			assert.Empty(t, h.cloud.calls(), "the observation waits out the backoff")

			tc.recover(h)
			h.dt.mu.Lock()
			op.NextRetryAt = time.Time{}
			h.dt.mu.Unlock()
			h.dispatch()

			h.assertSettled(two)
			assert.Equal(t, tc.wantWrites, h.cloud.writes())
		})
	}
}

// TestEgressRuns_ObserveAndClean: an identity whose applied configuration is unknown and that is owed an observation
// (as startup leaves one) gets an observe-and-clean run: both links are repaired, the NAT Gateway is classified (the
// default, without consensus), the garbage is swept, and nothing else is written.
func TestEgressRuns_ObserveAndClean(t *testing.T) {
	h := newEgressRunsHarness(t, testConfig())
	config := h.dt.config
	defaultV4 := defaultEgressConfig(egressParseContext{})
	h.cloud.seedApplied(defaultV4, false)
	h.cloud.register(egressRunsID, "")
	h.cloud.update(func(c *fakeARM) { c.natLocked(config.ResourceGroup, egressRunsID).Properties.ServiceGateway = nil })
	h.cloud.seedPublicIP(egressCleanupPIP("egress-a-pip-9", cleanupOwnTag(), armnetwork.ProvisioningStateSucceeded))
	h.cloud.seedPublicIP(egressCleanupPIP("egress-a-pip-10", nil, armnetwork.ProvisioningStateSucceeded))
	unlinked := h.cloud.nat(config.ResourceGroup, egressRunsID)
	h.track(StateCreated, egressModeManaged, egressUnknown, &egressUnknown)
	h.owe()
	h.evaluate()
	if !assert.Equal(t, egressUnknown, outboundEgress(h.op().Config.OutboundConfig), "control: an observe-and-clean run is due") {
		return
	}

	h.dispatch()

	h.assertSettled(defaultV4)
	assert.Equal(t, []string{cleanupCallPutNAT, cleanupCallRegister, "DELETE publicIPAddresses/egress-a-pip-9"}, h.cloud.writes(),
		"the link, the registration and the garbage: no configuration write")
	assert.JSONEq(t, egressRunsJSON(t, natLinkRequest(unlinked, config)), string(h.cloud.requestsOf(cleanupCallPutNAT)[0].Body), "a link-only PUT")
	registered, _ := h.cloud.registration(egressRunsID)
	assert.Equal(t, natGatewayID(egressRunsID, config), registered, "the registration references the NAT Gateway again")
	pips, _ := h.cloud.resourceNames()
	assert.Equal(t, []string{"egress-a-pip", "egress-a-pip-10"}, pips, "an untagged numbered Public IP is never deleted")
}

// TestEgressRuns_ObserveAndCleanBothFamilies: an observe-and-clean run of a dual-stack identity classifies its NAT
// Gateway for both families (the dual-stack default, without consensus) and sweeps the garbage of both: an owned
// numbered IPv6 Public IP is deleted, and nothing else is written.
func TestEgressRuns_ObserveAndCleanBothFamilies(t *testing.T) {
	h := newEgressRunsHarness(t, testConfig())
	defaultDual := defaultEgressConfig(egressParseContext{WantsIPv6: true})
	h.cloud.seedApplied(defaultDual, true)
	h.cloud.register(egressRunsID, natGatewayID(egressRunsID, h.dt.config))
	h.cloud.seedPublicIP(egressCleanupPIP("egress-a-pip-v6-9", cleanupOwnTag(), armnetwork.ProvisioningStateSucceeded))
	h.track(StateCreated, egressModeManaged, egressUnknown, &egressUnknown, dualStackFamilies()...)
	h.owe()
	h.evaluate()

	h.dispatch()

	h.assertSettled(defaultDual)
	assert.Equal(t, []string{"DELETE publicIPAddresses/egress-a-pip-v6-9"}, h.cloud.writes(), "the garbage alone: no configuration write")
	pips, _ := h.cloud.resourceNames()
	assert.Equal(t, []string{"egress-a-pip", "egress-a-pip-v6"}, pips)
}

// TestEgressRuns_BYOResidue: a BYO identity owed the cleanup of its managed past gets a residue run, chosen by the kind
// recorded at dispatch: the NAT Gateway it left in the cluster resource group is unlinked and deleted and its Public IPs
// swept, while the customer's NAT Gateway and the identity's registration are never touched.
func TestEgressRuns_BYOResidue(t *testing.T) {
	h := newEgressRunsHarness(t, byoNATConfig())
	config := h.dt.config
	byoNATID := fakeARMID(egressCleanupTestBYORG, "natGateways", egressRunsID)
	h.cloud.seedApplied(defaultEgressConfig(egressParseContext{}), false)
	h.cloud.seedPublicIP(egressCleanupPIP("egress-a-pip-2", cleanupOwnTag(), armnetwork.ProvisioningStateSucceeded))
	h.cloud.seedNAT(egressCleanupTestBYORG, armnetwork.NatGateway{
		Name: to.Ptr(egressRunsID),
		Properties: &armnetwork.NatGatewayPropertiesFormat{
			ServiceGateway:    &armnetwork.SubResource{ID: to.Ptr(config.ServiceGatewayResourceID())},
			PublicIPAddresses: []*armnetwork.SubResource{{ID: to.Ptr(fakeARMID(egressCleanupTestBYORG, "publicIPAddresses", "customer-pip"))}},
		},
	})
	h.cloud.register(egressRunsID, byoNATID)
	customer := egressRunsJSON(t, h.cloud.nat(egressCleanupTestBYORG, egressRunsID))
	h.dt.setBYONATGatewayID(egressRunsID, byoNATID)
	h.track(StateCreated, egressModeBYO, egressUnknown, &egressUnknown)
	h.owe()
	h.vote("pod-a", egressCleanupConfig(t, false, map[string]string{consts.PodAnnotationServiceEgressGatewayPublicIPCount: "2"}))
	h.evaluate()

	h.dispatch()

	h.assertSettled(egressUnknown)
	assert.Equal(t, egressModeBYO, h.dt.egress.mode[egressRunsID])
	assert.Equal(t, []string{cleanupCallGetNAT, cleanupCallGetServices, cleanupCallPutNAT, cleanupCallGetNAT, cleanupCallDeleteNAT},
		h.cloud.sequence(), "the residue is unlinked and deleted: no configuration is applied")
	assert.Equal(t, cleanupDeletes("egress-a-pip", "egress-a-pip-2"), h.cloud.reservedCalls(http.MethodDelete), "then swept")
	assert.Empty(t, h.cloud.reservedCalls(http.MethodPut))
	assert.Nil(t, h.cloud.nat(config.ResourceGroup, egressRunsID))
	for _, call := range h.cloud.calls() {
		assert.NotContains(t, call, egressCleanupTestBYORG, "the customer's NAT Gateway is never read or written")
	}
	assert.Equal(t, customer, egressRunsJSON(t, h.cloud.nat(egressCleanupTestBYORG, egressRunsID)))
	registered, _ := h.cloud.registration(egressRunsID)
	assert.Equal(t, byoNATID, registered, "the registration is never written")
}

// TestEgressRuns_BYOResidueNotComplete: a residue run that leaves residue behind (its NAT Gateway DELETE refused) fails,
// and is recorded as a failed update: the residue stays owed and the run is retried after a backoff.
func TestEgressRuns_BYOResidueNotComplete(t *testing.T) {
	RegisterMetrics()
	serviceOperationTotal.Reset()
	h := newEgressRunsHarness(t, byoNATConfig())
	h.cloud.seedApplied(defaultEgressConfig(egressParseContext{}), false)
	h.cloud.update(func(c *fakeARM) {
		c.natLocked(h.dt.config.ResourceGroup, egressRunsID).Properties.ServiceGateway = nil
	})
	h.cloud.fault(cleanupCallDeleteNAT, armReplyError(http.StatusConflict, "Conflict"))
	h.dt.setBYONATGatewayID(egressRunsID, fakeARMID(egressCleanupTestBYORG, "natGateways", egressRunsID))
	h.track(StateCreated, egressModeBYO, egressUnknown, &egressUnknown)
	h.owe()
	h.evaluate()

	h.dispatch()

	op := h.op()
	assert.Equal(t, []string{cleanupCallDeleteNAT}, h.cloud.writes(), "control: the DELETE was refused")
	assert.True(t, op.Egress.NeedsObservation, "the residue stays owed")
	assert.Equal(t, StateUpdateInProgress, op.State)
	assert.Equal(t, 1, op.RetryCount)
	assertBackoff(t, op, true)
	testutil.AssertVectorCount(t, "difftracker_engine_service_operations_total",
		map[string]string{"operation": "update", "service_type": "outbound", "result": "error"}, 1)
	testutil.AssertVectorCount(t, "difftracker_engine_service_operations_total",
		map[string]string{"operation": "update", "service_type": "outbound", "result": "success"}, 0)
}

// TestEgressRuns_EachCallHasABudgetOfItsOwn pins design 4.4 for the creation, update, observe-and-clean and BYO residue
// runs: every Azure call has a budget of its own, taken when it starts, never one the run shares. Every answer here
// takes most of a budget, so no two calls fit into one, and each run still settles.
func TestEgressRuns_EachCallHasABudgetOfItsOwn(t *testing.T) {
	setEgressCallTimeoutForTest(t, 600*time.Millisecond)
	setEgressConcurrencyForTest(t, len(reservedManaged(true)))
	defaultV4 := defaultEgressConfig(egressParseContext{})
	for _, tc := range []struct {
		name   string
		config Config
		setup  func(t *testing.T, h *egressRunsHarness) EgressConfig
	}{
		{name: "creation of two Public IPs and a prefix", config: testConfig(), setup: func(t *testing.T, h *egressRunsHarness) EgressConfig {
			cfg := egressCleanupConfig(t, false, map[string]string{
				consts.PodAnnotationServiceEgressGatewayPublicIPCount:        "2",
				consts.PodAnnotationServiceEgressGatewayPublicIPPrefixLength: "31",
			})
			h.addPod("pod-a", cfg, true)
			return cfg
		}},
		{name: "update to two Public IPs", config: testConfig(), setup: func(t *testing.T, h *egressRunsHarness) EgressConfig {
			two := egressCleanupConfig(t, false, map[string]string{consts.PodAnnotationServiceEgressGatewayPublicIPCount: "2"})
			h.cloud.seedApplied(defaultV4, false)
			h.cloud.register(egressRunsID, natGatewayID(egressRunsID, h.dt.config))
			h.track(StateCreated, egressModeManaged, defaultV4, &defaultV4)
			h.vote("pod-a", two)
			h.evaluate()
			return two
		}},
		{name: "observe-and-clean", config: testConfig(), setup: func(_ *testing.T, h *egressRunsHarness) EgressConfig {
			h.cloud.seedApplied(defaultV4, false)
			h.cloud.register(egressRunsID, "")
			h.cloud.update(func(c *fakeARM) { c.natLocked(h.dt.config.ResourceGroup, egressRunsID).Properties.ServiceGateway = nil })
			h.cloud.seedPublicIP(egressCleanupPIP("egress-a-pip-9", cleanupOwnTag(), armnetwork.ProvisioningStateSucceeded))
			h.track(StateCreated, egressModeManaged, egressUnknown, &egressUnknown)
			h.owe()
			h.evaluate()
			return defaultV4
		}},
		{name: "BYO residue cleanup", config: byoNATConfig(), setup: func(_ *testing.T, h *egressRunsHarness) EgressConfig {
			byoNATID := fakeARMID(egressCleanupTestBYORG, "natGateways", egressRunsID)
			h.cloud.seedApplied(defaultV4, false)
			h.cloud.seedNAT(egressCleanupTestBYORG, armnetwork.NatGateway{Name: to.Ptr(egressRunsID), Properties: &armnetwork.NatGatewayPropertiesFormat{
				ServiceGateway: &armnetwork.SubResource{ID: to.Ptr(h.dt.config.ServiceGatewayResourceID())},
			}})
			h.cloud.register(egressRunsID, byoNATID)
			h.dt.setBYONATGatewayID(egressRunsID, byoNATID)
			h.track(StateCreated, egressModeBYO, egressUnknown, &egressUnknown)
			h.owe()
			h.evaluate()
			return egressUnknown
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newEgressRunsHarness(t, tc.config)
			applied := tc.setup(t, h)
			h.cloud.update(func(c *fakeARM) { c.delay = 350 * time.Millisecond })

			h.dispatch()

			h.assertSettled(applied)
			assert.NotEmpty(t, h.cloud.writes(), "control: the run wrote")
		})
	}
}

// TestEgressRuns_ManagedDelete: deleting a managed identity keeps today's order (the unlink of both sides, then the
// unregistration, each write sent once with today's request), then deletes the NAT Gateway on the etag of a read taken
// after the unlink, and sweeps every reserved name of both families that is the controller's. The identity is untracked
// once everything is gone.
func TestEgressRuns_ManagedDelete(t *testing.T) {
	h := newEgressRunsHarness(t, testConfig())
	config := h.dt.config
	two := egressCleanupConfig(t, false, map[string]string{consts.PodAnnotationServiceEgressGatewayPublicIPCount: "2"})
	h.cloud.seedApplied(two, false)
	h.cloud.register(egressRunsID, natGatewayID(egressRunsID, config))
	h.cloud.seedPublicIP(egressCleanupPIP("egress-a-pip-9", cleanupOwnTag(), armnetwork.ProvisioningStateSucceeded))
	h.cloud.seedPublicIP(egressCleanupPIP("egress-a-pip-v6", cleanupOwnTag(), armnetwork.ProvisioningStateSucceeded))
	h.cloud.seedPublicIP(egressCleanupPIP("egress-a-pip-3", nil, armnetwork.ProvisioningStateSucceeded))
	unlinked := ""
	h.cloud.hook(cleanupCallPutNAT, func(c *fakeARM) { unlinked = derefString(c.natLocked(config.ResourceGroup, egressRunsID).Etag) })
	h.track(StateDeletionInProgress, egressModeManaged, two, &two)
	read := h.cloud.nat(config.ResourceGroup, egressRunsID)

	h.dispatch()

	assert.Nil(t, h.op(), "the deleted identity is no longer tracked")
	assert.False(t, h.dt.NRPResources.NATGateways.Has(egressRunsID))
	assert.Equal(t, []string{
		cleanupCallGetNAT, cleanupCallGetServices, // owned and used by no other service
		cleanupCallGetServices, cleanupCallRegister, cleanupCallGetNAT, cleanupCallPutNAT, // the unlink of both sides
		cleanupCallRegister,                     // the unregistration
		cleanupCallGetNAT, cleanupCallDeleteNAT, // the NAT Gateway, read right before its DELETE
	}, h.cloud.sequence())
	writes := slices.DeleteFunc(h.cloud.writeRequests(), func(req fakeARMRequest) bool { return isReservedCall(req.Call) })
	calls := make([]string, len(writes))
	for i, req := range writes {
		calls[i] = req.Call
	}
	if assert.Equal(t, []string{cleanupCallRegister, cleanupCallPutNAT, cleanupCallRegister, cleanupCallDeleteNAT}, calls, "one write each") {
		assert.Contains(t, string(writes[0].Body), `"isDelete":false`, "the registration is kept")
		assert.Contains(t, string(writes[0].Body), `"publicNatGatewayId":null`, "its NAT Gateway is cleared")
		assert.Empty(t, writes[1].IfMatch, "the unlink PUT carries no precondition")
		assert.JSONEq(t, string(fakeARMJSON(t, natUnlinkRequest(read))), string(writes[1].Body), "the unlink PUT changes nothing but the link")
		assert.Contains(t, string(writes[2].Body), `"isDelete":true`, "then the registration is removed")
	}
	if deletes := h.cloud.requestsOf(cleanupCallDeleteNAT); assert.Len(t, deletes, 1) && assert.NotEmpty(t, unlinked) {
		assert.Equal(t, unlinked, deletes[0].IfMatch, "the etag after the unlink")
	}
	assert.Equal(t, cleanupReservedReads(true), h.cloud.reservedCalls(http.MethodGet), "every reserved name of both families")
	assert.Equal(t, cleanupDeletes("egress-a-pip", "egress-a-pip-2", "egress-a-pip-9", "egress-a-pip-v6"), h.cloud.reservedCalls(http.MethodDelete))
	assert.Nil(t, h.cloud.nat(config.ResourceGroup, egressRunsID))
	pips, _ := h.cloud.resourceNames()
	assert.Equal(t, []string{"egress-a-pip-3"}, pips, "an untagged numbered Public IP is never deleted")
	_, registered := h.cloud.registration(egressRunsID)
	assert.False(t, registered)
}

// TestEgressRuns_ManagedDeleteStopsAtAnUnknownWrite: whichever write of a managed deletion's unlink or unregistration
// ends with an unknown outcome (the registration's NAT Gateway cleared, the NAT Gateway unlinked, the registration
// removed), it was sent once and the run ends there: no NAT Gateway DELETE, no sweep. The deletion is not complete: an
// observation is owed and the next pass waits out the quiet period, without an Azure call; the pass after it completes
// the deletion from settled reads.
func TestEgressRuns_ManagedDeleteStopsAtAnUnknownWrite(t *testing.T) {
	defaultV4 := defaultEgressConfig(egressParseContext{})
	unlinkThenRemove := []string{cleanupCallRegister, cleanupCallPutNAT, cleanupCallRegister}
	for _, step := range []struct {
		name   string
		fault  func(h *egressRunsHarness, answer http.HandlerFunc)
		writes []string
	}{
		{name: "clearing the registration's NAT Gateway", fault: func(h *egressRunsHarness, answer http.HandlerFunc) {
			h.cloud.fault(cleanupCallRegister, answer)
		}, writes: unlinkThenRemove[:1]},
		{name: "unlinking the NAT Gateway", fault: func(h *egressRunsHarness, answer http.HandlerFunc) {
			h.cloud.fault(cleanupCallPutNAT, answer)
		}, writes: unlinkThenRemove[:2]},
		{name: "removing the registration", fault: func(h *egressRunsHarness, answer http.HandlerFunc) {
			// The registration is written twice: its NAT Gateway cleared, then removed.
			h.cloud.hook(cleanupCallRegister, func(c *fakeARM) { c.faults[cleanupCallRegister] = append(c.faults[cleanupCallRegister], answer) })
		}, writes: unlinkThenRemove},
	} {
		for answerName, answer := range map[string]http.HandlerFunc{
			"500":       armReplyError(http.StatusInternalServerError, "InternalServerError"),
			"no answer": armDropConnection,
		} {
			t.Run(step.name+"/"+answerName, func(t *testing.T) {
				h := newEgressRunsHarness(t, testConfig())
				config := h.dt.config
				h.cloud.seedApplied(defaultV4, false)
				h.cloud.register(egressRunsID, natGatewayID(egressRunsID, config))
				h.cloud.seedPublicIP(egressCleanupPIP("egress-a-pip-9", cleanupOwnTag(), armnetwork.ProvisioningStateSucceeded))
				h.track(StateDeletionInProgress, egressModeManaged, defaultV4, &defaultV4)
				step.fault(h, answer)

				before := time.Now()
				h.dispatch()

				assert.Equal(t, step.writes, h.cloud.writes(), "each write sent once, none after the unknown one")
				assert.Empty(t, h.cloud.reservedCalls(http.MethodGet), "no sweep")
				assert.NotNil(t, h.cloud.nat(config.ResourceGroup, egressRunsID), "no NAT Gateway DELETE")
				op := h.op()
				if !assert.NotNil(t, op, "the deletion is not complete") {
					return
				}
				assert.Equal(t, StateDeletionInProgress, op.State)
				assert.True(t, h.dt.NRPResources.NATGateways.Has(egressRunsID))
				assert.True(t, op.Egress.NeedsObservation, "an observation is owed")
				assert.False(t, op.Egress.QuietUntil.Before(before.Add(getEgressSettleDelay())), "the quiet period started")
				assert.False(t, op.Egress.MutationStarted)
				assert.Equal(t, 1, op.RetryCount)

				h.cloud.resetRequests()
				h.dt.mu.Lock()
				op.NextRetryAt = time.Time{} // the backoff is over; the quiet period is not
				h.dt.mu.Unlock()
				h.dispatch()
				assert.Empty(t, h.cloud.calls(), "no Azure call during the quiet period")

				h.dt.mu.Lock()
				op.Egress.QuietUntil = time.Now().Add(-time.Second)
				h.dt.mu.Unlock()
				h.dispatch()
				assert.Nil(t, h.op(), "the next pass completes the deletion")
				assert.Nil(t, h.cloud.nat(config.ResourceGroup, egressRunsID))
				pips, _ := h.cloud.resourceNames()
				assert.Empty(t, pips)
			})
		}
	}
}

// TestEgressRuns_ManagedDeleteRefusals: a write of a managed deletion's unlink or unregistration that Azure refuses is
// handled as before, without a quiet period. A refused unlink (the registration's NAT Gateway, or the NAT Gateway's
// link; a refused registration write ends the unlink) is logged, counted and passed: the identity is still
// unregistered and its NAT Gateway deleted, which Azure refuses while the NAT Gateway is linked. A refused
// unregistration fails the run, which still deletes the NAT Gateway and sweeps; a 404 means it is already done. A
// deletion that does not complete is retried, and the retry completes it.
func TestEgressRuns_ManagedDeleteRefusals(t *testing.T) {
	defaultV4 := defaultEgressConfig(egressParseContext{})
	refused := armReplyError(http.StatusConflict, "Conflict")
	removal := func(answer http.HandlerFunc) func(h *egressRunsHarness) {
		return func(h *egressRunsHarness) {
			h.cloud.hook(cleanupCallRegister, func(c *fakeARM) { c.faults[cleanupCallRegister] = append(c.faults[cleanupCallRegister], answer) })
		}
	}
	for _, tc := range []struct {
		name           string
		setup          func(h *egressRunsHarness)
		writes         []string
		wantCounted    float64
		wantNATDeleted bool
		wantComplete   bool
	}{
		{name: "clearing the registration's NAT Gateway refused, the NAT Gateway not linked: completes",
			setup: func(h *egressRunsHarness) {
				h.cloud.update(func(c *fakeARM) { c.natLocked(h.dt.config.ResourceGroup, egressRunsID).Properties.ServiceGateway = nil })
				h.cloud.fault(cleanupCallRegister, refused)
			},
			writes: []string{cleanupCallRegister, cleanupCallRegister, cleanupCallDeleteNAT}, wantCounted: 1, wantNATDeleted: true, wantComplete: true},
		{name: "clearing the registration's NAT Gateway refused: the NAT Gateway is not unlinked, the deletion goes on",
			setup:  func(h *egressRunsHarness) { h.cloud.fault(cleanupCallRegister, refused) },
			writes: []string{cleanupCallRegister, cleanupCallRegister, cleanupCallDeleteNAT}, wantCounted: 1},
		{name: "unlinking the NAT Gateway refused: the deletion goes on",
			setup:  func(h *egressRunsHarness) { h.cloud.fault(cleanupCallPutNAT, refused) },
			writes: []string{cleanupCallRegister, cleanupCallPutNAT, cleanupCallRegister, cleanupCallDeleteNAT}, wantCounted: 1},
		{name: "unregistration refused: the NAT Gateway is still deleted and swept, the run fails",
			setup:  removal(refused),
			writes: []string{cleanupCallRegister, cleanupCallPutNAT, cleanupCallRegister, cleanupCallDeleteNAT}, wantNATDeleted: true},
		{name: "unregistration answered 404: already done",
			setup:  removal(armReplyError(http.StatusNotFound, "NotFound")),
			writes: []string{cleanupCallRegister, cleanupCallPutNAT, cleanupCallRegister, cleanupCallDeleteNAT}, wantNATDeleted: true, wantComplete: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			RegisterMetrics()
			deleteSubstepFailuresTotal.Reset()
			h := newEgressRunsHarness(t, testConfig())
			config := h.dt.config
			h.cloud.seedApplied(defaultV4, false)
			h.cloud.register(egressRunsID, natGatewayID(egressRunsID, config))
			h.track(StateDeletionInProgress, egressModeManaged, defaultV4, &defaultV4)
			tc.setup(h)

			h.dispatch()

			assert.Equal(t, tc.writes, slices.DeleteFunc(h.cloud.writes(), isReservedCall), "each write sent once")
			counted, err := testutil.GetCounterMetricValue(deleteSubstepFailuresTotal.WithLabelValues(deleteStepDisassociateNAT))
			assert.NoError(t, err)
			assert.Equal(t, tc.wantCounted, counted, "a refused unlink is counted")
			assert.Equal(t, tc.wantNATDeleted, h.cloud.nat(config.ResourceGroup, egressRunsID) == nil, "NAT Gateway deleted")
			if tc.wantNATDeleted {
				assert.Equal(t, cleanupDeletes("egress-a-pip"), h.cloud.reservedCalls(http.MethodDelete), "swept")
			}
			if tc.wantComplete {
				assert.Nil(t, h.op(), "the deletion completes")
				return
			}
			op := h.op()
			if !assert.NotNil(t, op, "the deletion is retried") {
				return
			}
			assert.Equal(t, StateDeletionInProgress, op.State)
			assert.True(t, op.Egress.QuietUntil.IsZero(), "a refusal starts no quiet period")
			assert.Equal(t, 1, op.RetryCount)

			h.dt.mu.Lock()
			op.NextRetryAt = time.Time{}
			h.dt.mu.Unlock()
			h.dispatch()
			assert.Nil(t, h.op(), "the retry completes the deletion")
			assert.Nil(t, h.cloud.nat(config.ResourceGroup, egressRunsID))
		})
	}
}

// TestEgressRuns_ManagedDeleteNotComplete: a managed deletion that cannot complete yet (its NAT Gateway is updating when
// the unlink reads it, which ends the deletion before the identity is unregistered, or when it is read again right
// before the DELETE; or a reserved Public IP is updating once the NAT Gateway is deleted) fails: the operation stays
// tracked, the identity stays in NRP and its last pod keeps its finalizer. The deletion is retried after a backoff and
// completes once Azure has settled.
func TestEgressRuns_ManagedDeleteNotComplete(t *testing.T) {
	defaultV4 := defaultEgressConfig(egressParseContext{})
	rg := testConfig().ResourceGroup
	natState := func(state armnetwork.ProvisioningState) func(c *fakeARM) {
		return func(c *fakeARM) { c.natLocked(rg, egressRunsID).Properties.ProvisioningState = to.Ptr(state) }
	}
	for _, tc := range []struct {
		name string
		// setup makes a resource the deletion reads update; settle lets it settle.
		setup, settle func(h *egressRunsHarness)
		// writes are the writes of the first pass, but the sweep's.
		writes     []string
		natDeleted bool
	}{
		{name: "the NAT Gateway is updating when the unlink reads it", setup: func(h *egressRunsHarness) {
			// The unlink clears the registration's NAT Gateway, then finds the NAT Gateway updating: the deletion ends there.
			h.cloud.update(natState(armnetwork.ProvisioningStateUpdating))
		}, settle: func(h *egressRunsHarness) { h.cloud.update(natState(armnetwork.ProvisioningStateSucceeded)) },
			writes: []string{cleanupCallRegister}},
		{name: "the NAT Gateway read before its DELETE is updating", setup: func(h *egressRunsHarness) {
			// Settled for the unlink, updating when it is read again right before the DELETE.
			h.cloud.hook(cleanupCallPutNAT, natState(armnetwork.ProvisioningStateUpdating))
		}, settle: func(h *egressRunsHarness) { h.cloud.update(natState(armnetwork.ProvisioningStateSucceeded)) },
			writes: []string{cleanupCallRegister, cleanupCallPutNAT, cleanupCallRegister}},
		{name: "a leftover Public IP is updating after the NAT Gateway DELETE", setup: func(h *egressRunsHarness) {
			h.cloud.seedPublicIP(egressCleanupPIP("egress-a-pip-9", cleanupOwnTag(), armnetwork.ProvisioningStateUpdating))
		}, settle: func(h *egressRunsHarness) {
			h.cloud.update(func(c *fakeARM) {
				c.pips[fakeARMKey(rg, "egress-a-pip-9")].Properties.ProvisioningState = to.Ptr(armnetwork.ProvisioningStateSucceeded)
			})
		}, writes: []string{cleanupCallRegister, cleanupCallPutNAT, cleanupCallRegister, cleanupCallDeleteNAT}, natDeleted: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newEgressRunsHarness(t, testConfig())
			config := h.dt.config
			h.cloud.seedApplied(defaultV4, false)
			h.cloud.register(egressRunsID, natGatewayID(egressRunsID, config))
			tc.setup(h)
			h.track(StateDeletionInProgress, egressModeManaged, defaultV4, &defaultV4)
			kube := fake.NewSimpleClientset(&v1.Pod{ObjectMeta: metav1.ObjectMeta{
				Namespace: "ns", Name: "last", UID: "last-uid", Finalizers: []string{ServiceGatewayPodCleanupFinalizer}}})
			h.dt.kubeClient = kube
			h.dt.pendingPodDeletions["ns/last"] = &PendingPodDeletion{Namespace: "ns", Name: "last", UID: "last-uid",
				ServiceUID: egressRunsID, Addresses: []string{"10.244.0.5"}, IsLastPod: true}
			lastPod := func() (pending bool, finalizers []string) {
				h.dt.mu.Lock()
				_, pending = h.dt.pendingPodDeletions["ns/last"]
				h.dt.mu.Unlock()
				pod, err := kube.CoreV1().Pods("ns").Get(context.Background(), "last", metav1.GetOptions{})
				if err != nil {
					t.Fatal(err)
				}
				return pending, pod.Finalizers
			}

			h.dispatch()

			op := h.op()
			if !assert.NotNil(t, op, "the deletion is not complete") {
				return
			}
			assert.Equal(t, StateDeletionInProgress, op.State)
			assert.Equal(t, 1, op.RetryCount)
			assertBackoff(t, op, true)
			assert.True(t, h.dt.NRPResources.NATGateways.Has(egressRunsID), "the identity stays in NRP")
			pending, finalizers := lastPod()
			assert.True(t, pending, "the last pod is still tracked")
			assert.Equal(t, []string{ServiceGatewayPodCleanupFinalizer}, finalizers, "the last pod keeps its finalizer")
			assert.Equal(t, tc.natDeleted, h.cloud.nat(config.ResourceGroup, egressRunsID) == nil, "NAT Gateway deleted")
			assert.Equal(t, tc.writes, slices.DeleteFunc(h.cloud.writes(), isReservedCall), "the deletion stops at the resource found updating")

			tc.settle(h)
			h.dt.mu.Lock()
			op.NextRetryAt = time.Time{}
			h.dt.mu.Unlock()
			h.dispatch()

			assert.Nil(t, h.op(), "the retry completes the deletion")
			assert.False(t, h.dt.NRPResources.NATGateways.Has(egressRunsID))
			pending, finalizers = lastPod()
			assert.False(t, pending)
			assert.Empty(t, finalizers, "the last pod's finalizer is released")
			assert.Nil(t, h.cloud.nat(config.ResourceGroup, egressRunsID))
			_, registered := h.cloud.registration(egressRunsID)
			assert.False(t, registered, "the identity is unregistered")
			pips, _ := h.cloud.resourceNames()
			assert.Empty(t, pips)
		})
	}
}

// TestEgressRuns_ThrottledServiceGatewayWritesAreRetried: a Service Gateway write that Azure throttles (a creation's
// registration, an update's re-registration, a deletion's clearing of the registration's NAT Gateway or its
// unregistration) was not applied, since the write is synchronous. The run fails with a plain error, retried after the
// usual backoff, without a quiet period and without rejecting the configuration, and the retry completes it. Live, a
// throttled registration held a creation, and with it the CCM's initial sync, back for the quiet period.
func TestEgressRuns_ThrottledServiceGatewayWritesAreRetried(t *testing.T) {
	defaultV4 := defaultEgressConfig(egressParseContext{})
	two := egressCleanupConfig(t, false, map[string]string{consts.PodAnnotationServiceEgressGatewayPublicIPCount: "2"})
	throttled := armReplyError(http.StatusTooManyRequests, "TooManyRequests")
	for _, tc := range []struct {
		name  string
		setup func(h *egressRunsHarness)
		// wantState is the state of the identity's operation after the throttled run.
		wantState ResourceState
		// settled is the configuration the retry leaves applied; nil when the retry deletes the identity.
		settled *EgressConfig
	}{
		{name: "creation: the registration", setup: func(h *egressRunsHarness) {
			h.addPod("pod-a", defaultV4, false)
			h.cloud.fault(cleanupCallRegister, throttled)
		}, wantState: StateNotStarted, settled: &defaultV4},
		{name: "update: the re-registration", setup: func(h *egressRunsHarness) {
			h.cloud.seedApplied(defaultV4, false)
			h.track(StateCreated, egressModeManaged, defaultV4, &defaultV4)
			h.vote("pod-a", two)
			h.evaluate()
			h.cloud.fault(cleanupCallRegister, throttled)
		}, wantState: StateUpdateInProgress, settled: &two},
		{name: "deletion: clearing the registration's NAT Gateway", setup: func(h *egressRunsHarness) {
			h.cloud.seedApplied(defaultV4, false)
			h.cloud.register(egressRunsID, natGatewayID(egressRunsID, h.dt.config))
			h.track(StateDeletionInProgress, egressModeManaged, defaultV4, &defaultV4)
			h.cloud.fault(cleanupCallRegister, throttled)
		}, wantState: StateDeletionInProgress},
		{name: "deletion: the unregistration", setup: func(h *egressRunsHarness) {
			h.cloud.seedApplied(defaultV4, false)
			h.cloud.register(egressRunsID, natGatewayID(egressRunsID, h.dt.config))
			h.track(StateDeletionInProgress, egressModeManaged, defaultV4, &defaultV4)
			// The registration is written twice: its NAT Gateway cleared, then removed.
			h.cloud.hook(cleanupCallRegister, func(c *fakeARM) { c.faults[cleanupCallRegister] = append(c.faults[cleanupCallRegister], throttled) })
		}, wantState: StateDeletionInProgress},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newEgressRunsHarness(t, testConfig())
			tc.setup(h)

			h.dispatch()

			op := h.op()
			if !assert.NotNil(t, op) {
				return
			}
			assert.Equal(t, tc.wantState, op.State, "the run failed")
			assert.True(t, op.Egress.QuietUntil.IsZero(), "a throttled Service Gateway write starts no quiet period")
			assert.Equal(t, 1, op.RetryCount)
			assertBackoff(t, op, true)
			assert.Empty(t, h.dt.egress.rejected, "no configuration is rejected")
			assert.Empty(t, h.dt.egress.warned)
			assert.Empty(t, h.events(consts.ServiceEgressGatewayReasonConfigRejected))

			h.dt.mu.Lock()
			op.NextRetryAt = time.Time{}
			h.dt.mu.Unlock()
			h.dispatch()
			if tc.settled == nil {
				assert.Nil(t, h.op(), "the retry completes the deletion")
				assert.Nil(t, h.cloud.nat(h.dt.config.ResourceGroup, egressRunsID))
				return
			}
			h.assertSettled(*tc.settled)
			assert.True(t, h.dt.NRPResources.NATGateways.Has(egressRunsID))
			natID, registered := h.cloud.registration(egressRunsID)
			assert.True(t, registered, "the retry registers the identity")
			assert.Equal(t, natGatewayID(egressRunsID, h.dt.config), natID)
		})
	}
}

// TestEgressRuns_BYODelete: deleting a BYO identity unlinks the customer's NAT Gateway and unregisters the identity as
// before; when the identity is owed the cleanup of its managed past, the same run removes that residue, so the
// deletion completes in one pass. Without one, nothing in the cluster resource group is touched.
func TestEgressRuns_BYODelete(t *testing.T) {
	for _, tc := range []struct {
		name string
		owed bool
		// residueFails refuses the first DELETE of the residue NAT Gateway: the deletion completes for the BYO NAT
		// Gateway, and one more pass, once its backoff is over, removes the residue.
		residueFails bool
	}{
		{name: "residue owed", owed: true},
		{name: "no residue owed"},
		{name: "residue owed, its cleanup failing once", owed: true, residueFails: true},
	} {
		owed := tc.owed
		t.Run(tc.name, func(t *testing.T) {
			h := newEgressRunsHarness(t, byoNATConfig())
			config := h.dt.config
			byoNATID := fakeARMID(egressCleanupTestBYORG, "natGateways", egressRunsID)
			h.cloud.seedApplied(defaultEgressConfig(egressParseContext{}), false)
			h.cloud.seedPublicIP(egressCleanupPIP("egress-a-pip-2", cleanupOwnTag(), armnetwork.ProvisioningStateSucceeded))
			h.cloud.seedNAT(egressCleanupTestBYORG, armnetwork.NatGateway{
				Name: to.Ptr(egressRunsID),
				Properties: &armnetwork.NatGatewayPropertiesFormat{
					ServiceGateway:    &armnetwork.SubResource{ID: to.Ptr(config.ServiceGatewayResourceID())},
					PublicIPAddresses: []*armnetwork.SubResource{{ID: to.Ptr(fakeARMID(egressCleanupTestBYORG, "publicIPAddresses", "customer-pip"))}},
				},
			})
			h.cloud.register(egressRunsID, byoNATID)
			h.allowBYOWrites()
			h.dt.setBYONATGatewayID(egressRunsID, byoNATID)
			h.track(StateDeletionInProgress, egressModeBYO, egressUnknown, &egressUnknown)
			if owed {
				h.owe()
			}
			if tc.residueFails {
				h.cloud.fault(cleanupCallDeleteNAT, armReplyError(http.StatusConflict, "Conflict"))
			}

			h.dispatch()

			if tc.residueFails {
				op := h.op()
				if !assert.NotNil(t, op, "the residue is still owed") {
					return
				}
				assert.True(t, op.Egress.NeedsObservation)
				assert.Equal(t, StateDeletionInProgress, op.State, "one more deletion pass")
				assert.Empty(t, h.dt.byoNATGatewayID(egressRunsID), "the BYO NAT Gateway is released")
				h.dt.mu.Lock()
				op.NextRetryAt = time.Time{}
				h.dt.mu.Unlock()
				h.dispatch()
			}

			assert.Nil(t, h.op(), "the deletion completes")
			assert.Empty(t, h.dt.byoNATGatewayID(egressRunsID))
			customer := h.cloud.nat(egressCleanupTestBYORG, egressRunsID)
			if assert.NotNil(t, customer, "the customer's NAT Gateway is never deleted") {
				assert.Empty(t, natGatewayServiceGatewayID(customer), "it is unlinked")
				assert.Len(t, customer.Properties.PublicIPAddresses, 1, "its addresses are kept")
			}
			_, registered := h.cloud.registration(egressRunsID)
			assert.False(t, registered)
			pips, _ := h.cloud.resourceNames()
			if !owed {
				assert.NotNil(t, h.cloud.nat(config.ResourceGroup, egressRunsID))
				assert.Equal(t, []string{"egress-a-pip", "egress-a-pip-2"}, pips)
				for _, call := range h.cloud.calls() {
					assert.Contains(t, []string{cleanupCallGetServices, cleanupCallRegister, "GET " + egressRunsBYONATCall, "PUT " + egressRunsBYONATCall}, call,
						"nothing in the cluster resource group is touched")
				}
				return
			}
			assert.Nil(t, h.cloud.nat(config.ResourceGroup, egressRunsID), "the residue NAT Gateway is deleted")
			assert.Empty(t, pips, "the residue Public IPs are swept")
		})
	}
}

// TestEgressRuns_CommitUnknown: an update whose NAT Gateway PUT has an unknown outcome leaves the applied configuration
// unknown, owes an observation and holds every run of the identity back for the quiet period, which makes no Azure call
// and counts no retry. Once it is over, the next run decides from settled reads and settles what was owed.
func TestEgressRuns_CommitUnknown(t *testing.T) {
	h := newEgressRunsHarness(t, testConfig())
	defaultV4 := defaultEgressConfig(egressParseContext{})
	two := egressCleanupConfig(t, false, map[string]string{consts.PodAnnotationServiceEgressGatewayPublicIPCount: "2"})
	h.cloud.seedApplied(defaultV4, false)
	h.cloud.register(egressRunsID, natGatewayID(egressRunsID, h.dt.config))
	h.track(StateCreated, egressModeManaged, defaultV4, &defaultV4)
	h.vote("pod-a", two)
	h.evaluate()
	h.cloud.fault(cleanupCallPutNAT, armReplyError(http.StatusInternalServerError, "InternalServerError"))

	before := time.Now()
	h.dispatch()

	op := h.op()
	assert.Equal(t, &egressUnknown, appliedEgressOf(op), "the applied configuration is unknown")
	assert.True(t, op.Egress.NeedsObservation, "an observation is owed")
	assert.False(t, op.Egress.QuietUntil.Before(before.Add(getEgressSettleDelay())), "the quiet period lasts the settle delay")
	assert.Equal(t, StateUpdateInProgress, op.State)
	assert.Equal(t, 1, op.RetryCount)
	assert.Empty(t, h.cloud.requestsOf("PUT publicIPAddresses/egress-a-pip-3"), "control")

	h.cloud.resetRequests()
	h.dt.mu.Lock()
	op.NextRetryAt = time.Time{} // the backoff is over; the quiet period is not
	h.dt.mu.Unlock()
	h.dispatch()
	assert.Empty(t, h.cloud.calls(), "no Azure call during the quiet period")
	assert.Equal(t, 1, op.RetryCount, "waiting is not a retry")

	h.dt.mu.Lock()
	op.Egress.QuietUntil = time.Now().Add(-time.Second)
	h.dt.mu.Unlock()
	h.dispatch()
	h.assertSettled(two)
	assert.Equal(t, []string{cleanupCallPutNAT}, h.cloud.writes(), "the Public IP written before is reused; the NAT Gateway is written again")
	assert.Equal(t, []string{consts.ServiceEgressGatewayReasonConfigApplied + " ns/pod-a"}, h.events(consts.ServiceEgressGatewayReasonConfigApplied),
		"the configuration is announced once it is applied")
}

// TestEgressRuns_UnclearWriteFoundAppliedIsAnnounced: an update whose NAT Gateway PUT Azure did not answer clearly, but
// applied, is announced once a run after the quiet period finds it applied, as the update would have announced it: an
// apply that writes nothing while the members still agree on it, or an observation while they no longer do, also for
// an identity adopted at startup, of which nothing was known before. A write that did not land is written again by the
// next apply, which announces it.
func TestEgressRuns_UnclearWriteFoundAppliedIsAnnounced(t *testing.T) {
	applied := consts.ServiceEgressGatewayReasonConfigApplied
	for _, tc := range []struct {
		name       string
		landed     bool
		disagree   bool
		adopted    bool // the identity's applied configuration is unknown with none known before, as after a restart
		wantWrites []string
	}{
		{name: "the members still agree: the apply finds it and writes nothing", landed: true},
		{name: "an identity adopted at startup: the apply finds it and writes nothing", landed: true, adopted: true},
		{name: "an identity adopted at startup whose members no longer agree: an observation finds it", landed: true, adopted: true, disagree: true},
		{name: "the members no longer agree: an observation finds it", landed: true, disagree: true},
		{name: "the write did not land: the next apply writes it", wantWrites: []string{cleanupCallPutNAT}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newEgressRunsHarness(t, testConfig())
			defaultV4 := defaultEgressConfig(egressParseContext{})
			two := egressCleanupConfig(t, false, map[string]string{consts.PodAnnotationServiceEgressGatewayPublicIPCount: "2"})
			three := egressCleanupConfig(t, false, map[string]string{consts.PodAnnotationServiceEgressGatewayPublicIPCount: "3"})
			h.cloud.seedApplied(defaultV4, false)
			h.cloud.register(egressRunsID, natGatewayID(egressRunsID, h.dt.config))
			if tc.adopted {
				h.track(StateCreated, egressModeManaged, egressUnknown, &egressUnknown)
			} else {
				h.track(StateCreated, egressModeManaged, defaultV4, &defaultV4)
			}
			h.vote("pod-a", two)
			h.vote("pod-b", two)
			h.evaluate()
			h.cloud.fault(cleanupCallPutNAT, func(w http.ResponseWriter, r *http.Request) {
				if tc.landed {
					h.cloud.seedApplied(two, false)
				}
				armReplyError(http.StatusInternalServerError, "InternalServerError")(w, r)
			})

			h.dispatch()

			op := h.op()
			assert.Equal(t, &egressUnknown, appliedEgressOf(op), "control: the outcome of the write is unknown")
			assert.Empty(t, h.events(applied), "nothing is announced while the outcome is unknown")
			if tc.disagree {
				h.vote("pod-b", three)
				h.evaluate()
				assert.Equal(t, egressUnknown, outboundEgress(op.Config.OutboundConfig), "control: without agreement an observation follows")
			}
			h.cloud.resetRequests()
			h.dt.mu.Lock()
			op.Egress.QuietUntil, op.NextRetryAt = time.Now().Add(-time.Second), time.Time{}
			h.dt.mu.Unlock()
			h.dispatch()

			assert.Equal(t, &two, appliedEgressOf(op))
			assert.ElementsMatch(t, tc.wantWrites, h.cloud.writes())
			assert.Equal(t, []string{applied + " ns/pod-a", applied + " ns/pod-b"}, h.events(applied), "every member is told")
			assert.Nil(t, op.Egress.LastKnownApplied)
			assert.Nil(t, op.Egress.UnclearWrites)
		})
	}
}

// TestEgressRuns_AnOlderUnclearWriteFoundAppliedIsAnnounced: of two applies in a row whose writes Azure did not answer
// clearly, the first landed and the second did not. The configuration of the first is found applied all the same:
// by an observation once the members no longer agree, or by an apply when they agree on it again. It is announced
// even when no configuration was known before, as for an identity adopted at startup, since this process wrote it. The
// NAT Gateway is not written again; only the Public IP the second apply created is swept.
func TestEgressRuns_AnOlderUnclearWriteFoundAppliedIsAnnounced(t *testing.T) {
	applied := consts.ServiceEgressGatewayReasonConfigApplied
	for _, tc := range []struct {
		name     string
		adopted  bool // the identity's applied configuration is unknown with none known before, as after a restart
		disagree bool // the members then disagree, so an observation reads the NAT Gateway; else they agree on it again
	}{
		{name: "the members disagree: an observation finds it", disagree: true},
		{name: "the members agree on it again: the apply finds it"},
		{name: "an identity adopted at startup whose members disagree: an observation finds it", adopted: true, disagree: true},
		{name: "an identity adopted at startup whose members agree on it again: the apply finds it", adopted: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newEgressRunsHarness(t, testConfig())
			defaultV4 := defaultEgressConfig(egressParseContext{})
			two := egressCleanupConfig(t, false, map[string]string{consts.PodAnnotationServiceEgressGatewayPublicIPCount: "2"})
			three := egressCleanupConfig(t, false, map[string]string{consts.PodAnnotationServiceEgressGatewayPublicIPCount: "3"})
			h.cloud.seedApplied(defaultV4, false)
			h.cloud.register(egressRunsID, natGatewayID(egressRunsID, h.dt.config))
			if tc.adopted {
				h.track(StateCreated, egressModeManaged, egressUnknown, &egressUnknown)
			} else {
				h.track(StateCreated, egressModeManaged, defaultV4, &defaultV4)
			}
			h.cloud.fault(cleanupCallPutNAT, func(w http.ResponseWriter, r *http.Request) {
				h.cloud.seedApplied(two, false)
				armReplyError(http.StatusInternalServerError, "InternalServerError")(w, r)
			}, armReplyError(http.StatusInternalServerError, "InternalServerError"))
			op := h.op()
			endQuiet := func() {
				h.dt.mu.Lock()
				op.Egress.QuietUntil, op.NextRetryAt = time.Now().Add(-time.Second), time.Time{}
				h.dt.mu.Unlock()
			}
			h.vote("pod-a", two)
			h.vote("pod-b", two)
			h.evaluate()
			h.dispatch()
			endQuiet()
			h.vote("pod-a", three)
			h.vote("pod-b", three)
			h.evaluate()
			h.dispatch()
			endQuiet()
			if !assert.Equal(t, &egressUnknown, appliedEgressOf(op), "control: the outcomes of both writes are unknown") ||
				!assert.Equal(t, []EgressConfig{three, two}, op.Egress.UnclearWrites, "control: both writes are remembered") ||
				!assert.Empty(t, h.events(applied), "control: nothing is announced while the outcome is unknown") {
				return
			}
			if tc.disagree {
				h.vote("pod-b", two)
			} else {
				h.vote("pod-a", two)
			}
			h.evaluate()
			h.cloud.resetRequests()

			h.dispatch()

			assert.Equal(t, &two, appliedEgressOf(op))
			assert.Equal(t, []string{"DELETE publicIPAddresses/egress-a-pip-3"}, h.cloud.writes(),
				"the configuration is found, not written: only the Public IP the second apply created is swept")
			assert.Equal(t, []string{applied + " ns/pod-a", applied + " ns/pod-b"}, h.events(applied), "every member is told")
			assert.Nil(t, op.Egress.UnclearWrites)
		})
	}
}

// TestEgressRuns_Busy: an update, apply or observation, that finds the NAT Gateway unsettled writes nothing, records
// nothing (no applied configuration, no change to the observation owed, no quiet period), counts as a failed update
// and is retried after a backoff.
func TestEgressRuns_Busy(t *testing.T) {
	defaultV4 := defaultEgressConfig(egressParseContext{})
	two := egressCleanupConfig(t, false, map[string]string{consts.PodAnnotationServiceEgressGatewayPublicIPCount: "2"})

	for _, tc := range []struct {
		name        string
		applied     EgressConfig
		owed        bool
		votes       []EgressConfig
		wantDesired EgressConfig
	}{
		{name: "apply", applied: defaultV4, votes: []EgressConfig{two}, wantDesired: two},
		{name: "observation", applied: egressUnknown, owed: true, wantDesired: egressUnknown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			RegisterMetrics()
			serviceOperationTotal.Reset()
			h := newEgressRunsHarness(t, testConfig())
			h.cloud.seedApplied(defaultV4, false)
			h.cloud.update(func(c *fakeARM) {
				c.natLocked(h.dt.config.ResourceGroup, egressRunsID).Properties.ProvisioningState = to.Ptr(armnetwork.ProvisioningStateUpdating)
			})
			h.track(StateCreated, egressModeManaged, tc.applied, &tc.applied)
			if tc.owed {
				h.owe()
			}
			for i, vote := range tc.votes {
				h.vote(fmt.Sprintf("pod-%d", i), vote)
			}
			h.evaluate()

			h.dispatch()

			op := h.op()
			assert.Empty(t, h.cloud.writes())
			assert.Equal(t, &tc.applied, appliedEgressOf(op), "nothing is recorded")
			assert.Equal(t, tc.owed, op.Egress.NeedsObservation, "the observation owed is unchanged")
			assert.True(t, op.Egress.QuietUntil.IsZero(), "no quiet period")
			assert.Equal(t, StateUpdateInProgress, op.State, "the update is retried")
			assert.Equal(t, tc.wantDesired, outboundEgress(op.Config.OutboundConfig))
			assert.Equal(t, 1, op.RetryCount)
			assertBackoff(t, op, true)
			testutil.AssertVectorCount(t, "difftracker_engine_service_operations_total",
				map[string]string{"operation": "update", "service_type": "outbound", "result": "error"}, 1)
			testutil.AssertVectorCount(t, "difftracker_engine_service_operations_total",
				map[string]string{"operation": "update", "service_type": "outbound", "result": "success"}, 0)
		})
	}
}

// TestEgressRuns_UnknownOutcomes: whatever write of a run ends with an unknown outcome (an apply's NAT Gateway PUT, also
// a throttled one, its re-registration or a DELETE of its sweep; an observation's link PUT; a deletion's throttled NAT
// Gateway unlink PUT or its NAT Gateway DELETE), the run reports it: an observation is owed and the identity's runs wait
// out the quiet period. The applied configuration is unknown, unless the NAT Gateway PUT that satisfies it succeeded
// before.
func TestEgressRuns_UnknownOutcomes(t *testing.T) {
	defaultV4 := defaultEgressConfig(egressParseContext{})
	two := egressCleanupConfig(t, false, map[string]string{consts.PodAnnotationServiceEgressGatewayPublicIPCount: "2"})
	unknown := armReplyError(http.StatusInternalServerError, "InternalServerError")

	for _, tc := range []struct {
		name        string
		setup       func(h *egressRunsHarness)
		wantApplied EgressConfig
		wantState   ResourceState
	}{
		{name: "apply: the NAT Gateway PUT", setup: func(h *egressRunsHarness) {
			h.track(StateCreated, egressModeManaged, defaultV4, &defaultV4)
			h.vote("pod-a", two)
			h.cloud.fault(cleanupCallPutNAT, unknown)
		}, wantApplied: egressUnknown, wantState: StateUpdateInProgress},
		{name: "apply: the NAT Gateway PUT throttled", setup: func(h *egressRunsHarness) {
			h.track(StateCreated, egressModeManaged, defaultV4, &defaultV4)
			h.vote("pod-a", two)
			h.cloud.fault(cleanupCallPutNAT, armReplyError(http.StatusTooManyRequests, "TooManyRequests"))
		}, wantApplied: egressUnknown, wantState: StateUpdateInProgress},
		{name: "apply: the re-registration after the NAT Gateway PUT", setup: func(h *egressRunsHarness) {
			h.track(StateCreated, egressModeManaged, defaultV4, &defaultV4)
			h.vote("pod-a", two)
			h.cloud.register(egressRunsID, "")
			h.cloud.fault(cleanupCallRegister, unknown)
		}, wantApplied: two, wantState: StateUpdateInProgress},
		{name: "apply: a DELETE of the sweep", setup: func(h *egressRunsHarness) {
			h.track(StateCreated, egressModeManaged, defaultV4, &defaultV4)
			h.vote("pod-a", two)
			h.cloud.seedPublicIP(egressCleanupPIP("egress-a-pip-9", cleanupOwnTag(), armnetwork.ProvisioningStateSucceeded))
			h.cloud.fault("DELETE publicIPAddresses/egress-a-pip-9", unknown)
		}, wantApplied: two, wantState: StateUpdateInProgress},
		{name: "observation: the link PUT", setup: func(h *egressRunsHarness) {
			h.track(StateCreated, egressModeManaged, egressUnknown, &egressUnknown)
			h.owe()
			h.cloud.update(func(c *fakeARM) {
				c.natLocked(h.dt.config.ResourceGroup, egressRunsID).Properties.ServiceGateway = nil
			})
			h.cloud.fault(cleanupCallPutNAT, unknown)
		}, wantApplied: egressUnknown, wantState: StateUpdateInProgress},
		{name: "deletion: the NAT Gateway DELETE", setup: func(h *egressRunsHarness) {
			h.track(StateDeletionInProgress, egressModeManaged, defaultV4, &defaultV4)
			h.cloud.fault(cleanupCallDeleteNAT, unknown)
		}, wantApplied: egressUnknown, wantState: StateDeletionInProgress},
		{name: "deletion: the NAT Gateway unlink PUT throttled", setup: func(h *egressRunsHarness) {
			h.track(StateDeletionInProgress, egressModeManaged, defaultV4, &defaultV4)
			h.cloud.fault(cleanupCallPutNAT, armReplyError(http.StatusTooManyRequests, "TooManyRequests"))
		}, wantApplied: egressUnknown, wantState: StateDeletionInProgress},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newEgressRunsHarness(t, testConfig())
			h.cloud.seedApplied(defaultV4, false)
			h.cloud.register(egressRunsID, natGatewayID(egressRunsID, h.dt.config))
			tc.setup(h)
			h.evaluate()

			before := time.Now()
			h.dispatch()

			op := h.op()
			if !assert.NotNil(t, op) {
				return
			}
			assert.NotEmpty(t, h.cloud.writes(), "control: the run wrote")
			assert.Equal(t, &tc.wantApplied, appliedEgressOf(op))
			assert.True(t, op.Egress.NeedsObservation, "an observation is owed")
			assert.False(t, op.Egress.QuietUntil.Before(before.Add(getEgressSettleDelay())), "the quiet period started")
			assert.False(t, op.Egress.MutationStarted)
			assert.Equal(t, tc.wantState, op.State)
		})
	}
}

// TestEgressRuns_UpdateRejected: an update whose agreed configuration is rejected changes nothing. A terminal rejection
// is reported as a terminal error (a validation error, never a park): the update is cancelled and the identity stays
// Created until its pods agree on another configuration. A retryable one is retried after a backoff.
func TestEgressRuns_UpdateRejected(t *testing.T) {
	const userPIP = "/subscriptions/sub/resourceGroups/user-rg/providers/Microsoft.Network/publicIPAddresses/user-pip"
	defaultV4 := defaultEgressConfig(egressParseContext{})

	for _, tc := range []struct {
		name      string
		retryable bool
	}{
		{name: "terminal"},
		{name: "retryable", retryable: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			RegisterMetrics()
			serviceOperationTotal.Reset()
			h := newEgressRunsHarness(t, testConfig())
			h.cloud.seedApplied(defaultV4, false)
			h.cloud.register(egressRunsID, natGatewayID(egressRunsID, h.dt.config))
			cfg := egressCleanupConfig(t, false, map[string]string{consts.PodAnnotationServiceEgressGatewayPublicIPIDs: userPIP})
			if !tc.retryable {
				h.cloud.update(func(c *fakeARM) {
					pip := egressApplyTestUserPIP(userPIP, false)
					pip.SKU.Name = to.Ptr(armnetwork.PublicIPAddressSKUNameStandard)
					c.pips[fakeARMKey("user-rg", "user-pip")] = pip
				})
			}
			h.track(StateCreated, egressModeManaged, defaultV4, &defaultV4)
			h.vote("pod-a", cfg)
			h.evaluate()

			h.dispatch()

			op := h.op()
			assert.Empty(t, h.cloud.writes(), "nothing is written")
			assert.Equal(t, &defaultV4, appliedEgressOf(op), "the NAT Gateway keeps its configuration")
			assert.Equal(t, []string{consts.ServiceEgressGatewayReasonConfigRejected + " ns/pod-a"},
				h.events(consts.ServiceEgressGatewayReasonConfigRejected))
			terminalErrors := 1
			if tc.retryable {
				terminalErrors = 0
				assert.Equal(t, cfg, h.dt.egress.warned[egressRunsID].Config)
				assert.Equal(t, StateUpdateInProgress, op.State)
				assertBackoff(t, op, true)
			} else {
				assert.Equal(t, cfg, h.dt.egress.rejected[egressRunsID].Config)
				h.assertSettled(defaultV4)
				assert.Equal(t, defaultV4, outboundEgress(op.Config.OutboundConfig), "the update is cancelled")
			}
			assert.False(t, op.CreationFailedTerminal, "an update is never parked")
			testutil.AssertVectorCount(t, "difftracker_engine_service_operations_total",
				map[string]string{"operation": "update", "service_type": "outbound", "result": "error", "error_code": "ValidationError"}, terminalErrors)
		})
	}
}

// TestEgressRuns_NATGatewayUsedByAnotherService: while another Service Gateway service uses the NAT Gateway of an
// identity, an update writes nothing and fails with a plain error: no rejection, no event, no quiet period, a retry
// after a backoff. Once the other service no longer uses the NAT Gateway, the retry applies the configuration.
func TestEgressRuns_NATGatewayUsedByAnotherService(t *testing.T) {
	h := newEgressRunsHarness(t, testConfig())
	defaultV4 := defaultEgressConfig(egressParseContext{})
	two := egressCleanupConfig(t, false, map[string]string{consts.PodAnnotationServiceEgressGatewayPublicIPCount: "2"})
	h.cloud.seedApplied(defaultV4, false)
	h.cloud.register(egressRunsID, natGatewayID(egressRunsID, h.dt.config))
	h.cloud.register("default-outbound", natGatewayID(egressRunsID, h.dt.config))
	h.track(StateCreated, egressModeManaged, defaultV4, &defaultV4)
	h.vote("pod-a", two)
	h.evaluate()
	takeEgressEvents(h.dt)

	h.dispatch()

	op := h.op()
	assert.Empty(t, h.cloud.writes(), "nothing is written")
	assert.Equal(t, &defaultV4, appliedEgressOf(op))
	assert.Equal(t, StateUpdateInProgress, op.State, "the update is retried")
	assert.Equal(t, two, outboundEgress(op.Config.OutboundConfig))
	assert.Equal(t, 1, op.RetryCount)
	assertBackoff(t, op, true)
	assert.True(t, op.Egress.QuietUntil.IsZero(), "no quiet period")
	assert.False(t, op.Egress.NeedsObservation)
	assert.Empty(t, h.dt.egress.rejected, "no rejection")
	assert.Empty(t, h.dt.egress.warned)
	assert.Empty(t, takeEgressEvents(h.dt), "no event")

	h.cloud.update(func(c *fakeARM) { delete(c.services, "default-outbound") })
	h.dt.mu.Lock()
	op.NextRetryAt = time.Time{}
	h.dt.mu.Unlock()
	h.dispatch()

	h.assertSettled(two)
	assert.Equal(t, []string{egressRunsPutPIP2, cleanupCallPutNAT}, h.cloud.writes())
}

// TestEgressRuns_CreationRejectedIsRetried: a creation whose configuration, the default, is rejected terminally has no
// fallback; it is retried after a backoff, never parked, since a pod-driven identity has no spec to change.
func TestEgressRuns_CreationRejectedIsRetried(t *testing.T) {
	h := newEgressRunsHarness(t, testConfig())
	h.track(StateNotStarted, egressModeUnknown, defaultEgressConfig(egressParseContext{}), nil)
	h.cloud.fault(egressRunsPutPIP, armReplyError(http.StatusBadRequest, "InvalidRequest"))

	h.dispatch()

	op := h.op()
	assert.Equal(t, defaultEgressConfig(egressParseContext{}), h.dt.egress.rejected[egressRunsID].Config, "control: rejected terminally")
	assert.Equal(t, StateNotStarted, op.State)
	assert.False(t, op.CreationFailedTerminal, "not parked")
	assert.Equal(t, 1, op.RetryCount)
	assertBackoff(t, op, true)
	assert.False(t, h.dt.NRPResources.NATGateways.Has(egressRunsID))
}

// TestEgressRuns_CreationSweepFailureDoesNotHoldBackEgress: a creation whose NAT Gateway is applied and registered
// completes even when its sweep does not, so the identity's pods get egress at once; the leftover is owed to an
// observation, which runs after a backoff and sweeps it.
func TestEgressRuns_CreationSweepFailureDoesNotHoldBackEgress(t *testing.T) {
	h := newEgressRunsHarness(t, testConfig())
	defaultV4 := defaultEgressConfig(egressParseContext{})
	h.cloud.seedPublicIP(egressCleanupPIP("egress-a-pip-9", cleanupOwnTag(), armnetwork.ProvisioningStateSucceeded))
	h.cloud.fault("DELETE publicIPAddresses/egress-a-pip-9", armReplyError(http.StatusConflict, "Conflict"))
	h.addPod("pod-a", defaultV4, false)

	h.dispatch()

	op := h.op()
	assert.Equal(t, &defaultV4, appliedEgressOf(op))
	assert.True(t, h.dt.NRPResources.NATGateways.Has(egressRunsID), "the identity is created")
	assert.Empty(t, h.dt.pendingPods[egressRunsID], "its pod is promoted")
	assert.True(t, op.Egress.NeedsObservation, "the leftover is owed")
	assert.Equal(t, StateUpdateInProgress, op.State)
	assert.Equal(t, egressUnknown, outboundEgress(op.Config.OutboundConfig), "an observation follows")
	assertBackoff(t, op, true)

	h.dt.mu.Lock()
	op.NextRetryAt = time.Time{}
	h.dt.mu.Unlock()
	h.dispatch()

	h.assertSettled(defaultV4)
	pips, _ := h.cloud.resourceNames()
	assert.Equal(t, []string{"egress-a-pip"}, pips, "the observation swept the leftover")
}

// TestEgressRuns_DeletionOfNothingOfOurs: a deletion that may delete nothing (the NAT Gateway of the identity's name is
// used by another service, or was recorded at startup as not the controller's) completes in one pass, without a further
// pass for an observation, also one that was owed. Only the identity's own registration is removed, when it may exist.
func TestEgressRuns_DeletionOfNothingOfOurs(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(h *egressRunsHarness)
		// removal: the registration of the identity is removed, its only write.
		removal bool
	}{
		{name: "used by another service", setup: func(h *egressRunsHarness) {
			h.cloud.seedApplied(defaultEgressConfig(egressParseContext{}), false)
			h.cloud.register("default-outbound", natGatewayID(egressRunsID, h.dt.config))
		}, removal: true},
		{name: "recorded at startup as not the controller's", setup: func(h *egressRunsHarness) {
			h.dt.NRPResources.UnmanagedNATGateways = newIgnoreCaseSetFromSlice([]string{egressRunsID})
			h.dt.NRPResources.NATGateways.Delete(egressRunsID)
			h.dt.mu.Lock()
			h.dt.egress.mode[egressRunsID] = egressModeBYO
			h.dt.mu.Unlock()
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newEgressRunsHarness(t, testConfig())
			h.track(StateDeletionInProgress, egressModeManaged, egressUnknown, &egressUnknown)
			h.owe()
			tc.setup(h)

			h.dispatch()

			assert.Nil(t, h.op(), "the deletion completes in one pass")
			writes := h.cloud.writeRequests()
			if !tc.removal {
				assert.Empty(t, writes, "nothing is written")
				return
			}
			if assert.Len(t, writes, 1, "nothing but the identity's own registration is written") {
				assert.Equal(t, cleanupCallRegister, writes[0].Call)
				assert.Contains(t, string(writes[0].Body), `"isDelete":true`, "the registration is removed")
			}
		})
	}
}

// TestEgressRuns_ObserveClassifiesTheAgreedConfiguration: an observation finds the configuration the pods agree on
// when the NAT Gateway satisfies it, so an owed observation of an identity whose agreement was applied before keeps
// that configuration applied and dispatches nothing more.
func TestEgressRuns_ObserveClassifiesTheAgreedConfiguration(t *testing.T) {
	h := newEgressRunsHarness(t, testConfig())
	two := egressCleanupConfig(t, false, map[string]string{consts.PodAnnotationServiceEgressGatewayPublicIPCount: "2"})
	h.cloud.seedApplied(two, false)
	h.cloud.register(egressRunsID, natGatewayID(egressRunsID, h.dt.config))
	h.cloud.seedPublicIP(egressCleanupPIP("egress-a-pip-9", cleanupOwnTag(), armnetwork.ProvisioningStateSucceeded))
	h.track(StateCreated, egressModeManaged, two, &two)
	h.vote("pod-a", two)
	h.owe()
	h.evaluate()
	if !assert.Equal(t, egressUnknown, outboundEgress(h.op().Config.OutboundConfig), "control: an observation is due") {
		return
	}

	h.dispatch()

	h.assertSettled(two)
	assert.Equal(t, []string{"DELETE publicIPAddresses/egress-a-pip-9"}, h.cloud.writes())
}

// panickingNATFactory is a client factory whose NAT Gateway client panics on a PUT.
type panickingNATFactory struct{ azclient.ClientFactory }

func (f panickingNATFactory) GetNatGatewayClient() natgatewayclient.Interface {
	return panickingNATClient{f.ClientFactory.GetNatGatewayClient()}
}

type panickingNATClient struct{ natgatewayclient.Interface }

func (panickingNATClient) CreateOrUpdate(context.Context, string, string, armnetwork.NatGateway) (*armnetwork.NatGateway, error) {
	panic("NAT Gateway client panicked")
}

// TestEgressRuns_PanicAfterTheFirstWrite: a run that panics after its first write (a Public IP PUT) is recovered and,
// although it reports no outcome, treated like a write with an unknown outcome: applied unknown, observation owed,
// quiet period.
func TestEgressRuns_PanicAfterTheFirstWrite(t *testing.T) {
	h := newEgressRunsHarness(t, testConfig())
	defaultV4 := defaultEgressConfig(egressParseContext{})
	two := egressCleanupConfig(t, false, map[string]string{consts.PodAnnotationServiceEgressGatewayPublicIPCount: "2"})
	h.cloud.seedApplied(defaultV4, false)
	h.dt.networkClientFactory = panickingNATFactory{h.dt.networkClientFactory}
	h.track(StateCreated, egressModeManaged, defaultV4, &defaultV4)
	h.vote("pod-a", two)
	h.evaluate()

	assert.NotPanics(t, h.dispatch)

	op := h.op()
	assert.Equal(t, []string{egressRunsPutPIP2}, h.cloud.writes(), "control: the run wrote before it panicked")
	assert.Equal(t, &egressUnknown, appliedEgressOf(op))
	assert.True(t, op.Egress.NeedsObservation)
	assert.True(t, op.Egress.QuietUntil.After(time.Now()), "the quiet period started")
	assert.False(t, op.Egress.MutationStarted)
	assert.Equal(t, StateUpdateInProgress, op.State)
}

// TestEgressRuns_SweepOfResourcesInUseElsewhere: a leftover that Azure refuses to delete (400) while its read shows a
// use other than the identity's NAT Gateway is done for the sweep, so the observation completes and is not run again
// and again. A refusal the read does not explain (the leftover still names the identity's NAT Gateway, which has not
// released it yet, or names nothing) and a conflict (409) leave the observation owed: it waits out a backoff, without
// any Azure call, and its retry deletes the leftover once the use has ended.
func TestEgressRuns_SweepOfResourcesInUseElsewhere(t *testing.T) {
	const leftover = "egress-a-pip-9"
	deleteLeftover := "DELETE publicIPAddresses/" + leftover
	ownNAT := natGatewayID(egressRunsID, testConfig())
	userNAT := fakeARMID("user-rg", "natGateways", "user-nat")
	inUse := armReplyError(http.StatusBadRequest, "PublicIPAddressInUse")
	for _, tc := range []struct {
		name string
		// uses is the NAT Gateway the leftover's read names, "" for none; answer, when set, answers its first DELETE.
		uses     string
		answer   http.HandlerFunc
		wantOwed bool
	}{
		{name: "in use by another NAT Gateway: done", uses: userNAT},
		{name: "conflict: still owed", uses: userNAT, answer: armReplyError(http.StatusConflict, "AnotherOperationInProgress"), wantOwed: true},
		{name: "refused while it names the identity's NAT Gateway: still owed", uses: ownNAT, answer: inUse, wantOwed: true},
		{name: "refused without a known use: still owed", answer: inUse, wantOwed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newEgressRunsHarness(t, testConfig())
			config := h.dt.config
			defaultV4 := defaultEgressConfig(egressParseContext{})
			h.cloud.seedApplied(defaultV4, false)
			h.cloud.register(egressRunsID, ownNAT)
			pip := egressCleanupPIP(leftover, cleanupOwnTag(), armnetwork.ProvisioningStateSucceeded)
			if tc.uses != "" {
				pip.Properties.NatGateway = &armnetwork.NatGateway{ID: to.Ptr(tc.uses)}
			}
			h.cloud.seedPublicIP(pip)
			if tc.uses == userNAT {
				h.cloud.seedNAT("user-rg", armnetwork.NatGateway{Name: to.Ptr("user-nat"), Properties: &armnetwork.NatGatewayPropertiesFormat{
					PublicIPAddresses: []*armnetwork.SubResource{{ID: to.Ptr(publicIPAddressID(leftover, config))}},
				}})
			}
			if tc.answer != nil {
				h.cloud.fault(deleteLeftover, tc.answer)
			}
			h.track(StateCreated, egressModeManaged, egressUnknown, &egressUnknown)
			h.owe()
			h.evaluate()

			h.dispatch()

			assert.Equal(t, []string{deleteLeftover}, h.cloud.writes())
			pips, _ := h.cloud.resourceNames()
			assert.Equal(t, []string{"egress-a-pip", leftover}, pips, "the leftover is still there")
			if !tc.wantOwed {
				h.assertSettled(defaultV4)
				return
			}
			op := h.op()
			assert.True(t, op.Egress.NeedsObservation)
			assert.Equal(t, StateUpdateInProgress, op.State)
			assert.Equal(t, 1, op.RetryCount)
			assertBackoff(t, op, true)

			h.cloud.resetRequests()
			h.dispatch()
			assert.Empty(t, h.cloud.calls(), "the observation waits out the backoff")

			h.cloud.update(func(c *fakeARM) {
				c.pips[fakeARMKey(config.ResourceGroup, leftover)].Properties.NatGateway = nil
				delete(c.nats, fakeARMKey("user-rg", "user-nat"))
			})
			h.dt.mu.Lock()
			op.NextRetryAt = time.Time{}
			h.dt.mu.Unlock()
			h.dispatch()

			h.assertSettled(defaultV4)
			assert.Equal(t, []string{deleteLeftover}, h.cloud.writes(), "the retry deletes the leftover")
			pips, _ = h.cloud.resourceNames()
			assert.Equal(t, []string{"egress-a-pip"}, pips)
		})
	}
}

// TestEgressRuns_ObserveWithoutNATGateway: an observation of a registered managed identity whose NAT Gateway was
// deleted out of band (out of scope) completes with nothing owed, so it is not run again and again.
func TestEgressRuns_ObserveWithoutNATGateway(t *testing.T) {
	h := newEgressRunsHarness(t, testConfig())
	h.cloud.register(egressRunsID, natGatewayID(egressRunsID, h.dt.config))
	h.cloud.seedPublicIP(egressCleanupPIP("egress-a-pip", cleanupOwnTag(), armnetwork.ProvisioningStateSucceeded))
	h.track(StateCreated, egressModeManaged, egressUnknown, &egressUnknown)
	h.owe()
	h.evaluate()

	h.dispatch()

	h.assertSettled(egressUnknown)
	assert.Equal(t, []string{cleanupCallGetNAT}, h.cloud.calls(), "nothing but the NAT Gateway read")

	h.cloud.resetRequests()
	h.dispatch()
	assert.Empty(t, h.cloud.calls(), "the identity is not observed again")
}

// TestEgressRuns_UndecidedConfigurations: an operation without a configuration (the zero value, from before
// configuration existed, or the unknown one) is created with the default, which the dispatch decides, and updated by
// the observation its evaluation dispatches, never by applying the zero value.
func TestEgressRuns_UndecidedConfigurations(t *testing.T) {
	defaultV4 := defaultEgressConfig(egressParseContext{})

	t.Run("creation", func(t *testing.T) {
		h := newEgressRunsHarness(t, testConfig())
		h.track(StateNotStarted, egressModeUnknown, EgressConfig{}, nil)

		h.dispatch()

		h.assertSettled(defaultV4)
		assert.Empty(t, h.dt.egress.rejected, "the zero value is never applied, so never rejected")
		assert.Equal(t, []string{egressRunsPutPIP, cleanupCallPutNAT, cleanupCallRegister}, h.cloud.writes())
	})

	t.Run("creation with the unknown configuration", func(t *testing.T) {
		h := newEgressRunsHarness(t, testConfig())
		h.track(StateNotStarted, egressModeUnknown, egressUnknown, nil)

		h.dispatch()

		h.assertSettled(defaultV4)
		assert.Empty(t, h.dt.egress.rejected, "the unknown configuration is never applied, so never rejected")
		assert.Equal(t, []string{egressRunsPutPIP, cleanupCallPutNAT, cleanupCallRegister}, h.cloud.writes())
	})

	t.Run("update", func(t *testing.T) {
		h := newEgressRunsHarness(t, testConfig())
		h.cloud.seedApplied(defaultV4, false)
		h.cloud.register(egressRunsID, natGatewayID(egressRunsID, h.dt.config))
		h.track(StateCreated, egressModeManaged, EgressConfig{}, &egressUnknown)
		h.owe()

		h.evaluate()
		assert.Equal(t, egressUnknown, outboundEgress(h.op().Config.OutboundConfig), "an observation is dispatched, never the zero value")
		h.dispatch()

		h.assertSettled(defaultV4)
		assert.Empty(t, h.dt.egress.rejected)
		assert.Empty(t, h.cloud.writes(), "an observation: nothing to write")
		assert.Equal(t, cleanupCallGetNAT, h.cloud.calls()[0], "observed, not applied: no preflight of a configuration")
	})
}

// TestServiceUpdaterRunServiceWork_UnknownKind: a run of a kind the dispatcher never records fails the operation, so it
// is retried rather than left in flight, inbound or outbound, and an outbound one reports an outcome.
func TestServiceUpdaterRunServiceWork_UnknownKind(t *testing.T) {
	for _, inbound := range []bool{true, false} {
		dt := newTestDiffTracker()
		su := newTestServiceUpdater(dt)
		var plain, withOutcome int
		var gotErr error
		su.onComplete = func(_ string, success bool, err error) {
			assert.False(t, success)
			plain++
			gotErr = err
		}
		su.onOutboundComplete = func(_ string, success bool, err error, outcome *egressOutcome) {
			assert.False(t, success)
			assert.NotNil(t, outcome)
			withOutcome++
			gotErr = err
		}
		config := NewOutboundServiceConfig(egressRunsID, &OutboundConfig{})
		if inbound {
			config = NewInboundServiceConfig(egressRunsID, makeInboundConfig(80))
		}

		su.runServiceWork(serviceWork{serviceUID: egressRunsID, config: config, kind: operationNone})

		assert.Error(t, gotErr)
		if inbound {
			assert.Equal(t, []int{1, 0}, []int{plain, withOutcome})
		} else {
			assert.Equal(t, []int{0, 1}, []int{plain, withOutcome})
		}
	}
}

// TestEgressRuns_EveryOutboundCompletionReportsAnOutcome: every outbound run completes with an outcome, whatever way it
// ends: the managed ones, a BYO creation (with its mode, also when rejected), the residue cleanup and every deletion.
func TestEgressRuns_EveryOutboundCompletionReportsAnOutcome(t *testing.T) {
	defaultV4 := defaultEgressConfig(egressParseContext{})
	for _, tc := range []struct {
		name     string
		config   Config
		setup    func(h *egressRunsHarness)
		wantMode egressMode
	}{
		{name: "managed creation", config: testConfig(), setup: func(h *egressRunsHarness) {
			h.track(StateNotStarted, egressModeUnknown, defaultV4, nil)
		}, wantMode: egressModeManaged},
		{name: "managed creation that fails", config: testConfig(), setup: func(h *egressRunsHarness) {
			h.track(StateNotStarted, egressModeUnknown, defaultV4, nil)
			h.cloud.fault(cleanupCallPutNAT, armReplyError(http.StatusConflict, "Conflict"))
		}, wantMode: egressModeManaged},
		{name: "creation whose NAT Gateway cannot be resolved", config: testConfig(), setup: func(h *egressRunsHarness) {
			h.track(StateNotStarted, egressModeUnknown, defaultV4, nil)
			h.cloud.fault(cleanupCallGetNAT, armReplyError(http.StatusForbidden, "AuthorizationFailed"))
		}},
		{name: "BYO creation", config: byoNATConfig(), setup: func(h *egressRunsHarness) {
			h.track(StateNotStarted, egressModeUnknown, defaultV4, nil)
			h.cloud.seedNAT(egressCleanupTestBYORG, armnetwork.NatGateway{Name: to.Ptr(egressRunsID),
				SKU: &armnetwork.NatGatewaySKU{Name: to.Ptr(armnetwork.NatGatewaySKUNameStandardV2)},
				Properties: &armnetwork.NatGatewayPropertiesFormat{
					PublicIPAddresses: []*armnetwork.SubResource{{ID: to.Ptr(fakeARMID(egressCleanupTestBYORG, "publicIPAddresses", "customer-pip"))}},
				}})
			h.allowBYOWrites()
		}, wantMode: egressModeBYO},
		{name: "BYO creation that is rejected", config: byoNATConfig(), setup: func(h *egressRunsHarness) {
			h.track(StateNotStarted, egressModeUnknown, defaultV4, nil)
			h.cloud.seedNAT(egressCleanupTestBYORG, armnetwork.NatGateway{Name: to.Ptr(egressRunsID),
				SKU: &armnetwork.NatGatewaySKU{Name: to.Ptr(armnetwork.NatGatewaySKUNameStandard)}})
		}, wantMode: egressModeBYO},
		{name: "observation", config: testConfig(), setup: func(h *egressRunsHarness) {
			h.track(StateUpdateInProgress, egressModeManaged, egressUnknown, &egressUnknown)
		}, wantMode: egressModeManaged},
		{name: "residue cleanup", config: byoNATConfig(), setup: func(h *egressRunsHarness) {
			h.track(StateUpdateInProgress, egressModeBYO, egressUnknown, &egressUnknown)
		}, wantMode: egressModeBYO},
		{name: "managed deletion", config: testConfig(), setup: func(h *egressRunsHarness) {
			h.track(StateDeletionInProgress, egressModeManaged, defaultV4, &defaultV4)
		}},
		{name: "managed deletion that fails", config: testConfig(), setup: func(h *egressRunsHarness) {
			h.cloud.seedApplied(defaultV4, false)
			h.track(StateDeletionInProgress, egressModeManaged, defaultV4, &defaultV4)
			h.cloud.fault(cleanupCallDeleteNAT, armReplyError(http.StatusConflict, "Conflict"))
		}},
		{name: "deletion whose finalizer release fails", config: testConfig(), setup: func(h *egressRunsHarness) {
			h.track(StateDeletionInProgress, egressModeManaged, defaultV4, &defaultV4)
			kube := fake.NewSimpleClientset(&v1.Pod{ObjectMeta: metav1.ObjectMeta{
				Namespace: "ns", Name: "last", UID: "last-uid", Finalizers: []string{ServiceGatewayPodCleanupFinalizer}}})
			kube.PrependReactor("update", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
				return true, nil, errors.New("apiserver down")
			})
			h.dt.kubeClient = kube
			h.dt.pendingPodDeletions["ns/last"] = &PendingPodDeletion{Namespace: "ns", Name: "last", UID: "last-uid",
				ServiceUID: egressRunsID, Addresses: []string{"10.244.0.5"}, IsLastPod: true}
		}},
		{name: "deletion whose ownership cannot be read", config: testConfig(), setup: func(h *egressRunsHarness) {
			h.track(StateDeletionInProgress, egressModeManaged, defaultV4, &defaultV4)
			h.cloud.fault(cleanupCallGetNAT, armReplyError(http.StatusForbidden, "AuthorizationFailed"))
		}},
		{name: "deletion of an identity whose NAT Gateway is not the controller's", config: testConfig(), setup: func(h *egressRunsHarness) {
			h.track(StateDeletionInProgress, egressModeManaged, defaultV4, &defaultV4)
			h.dt.NRPResources.UnmanagedNATGateways = newIgnoreCaseSetFromSlice([]string{egressRunsID})
			h.dt.NRPResources.NATGateways.Delete(egressRunsID)
		}},
		{name: "BYO deletion", config: byoNATConfig(), setup: func(h *egressRunsHarness) {
			h.track(StateDeletionInProgress, egressModeBYO, egressUnknown, &egressUnknown)
			h.dt.setBYONATGatewayID(egressRunsID, fakeARMID(egressCleanupTestBYORG, "natGateways", egressRunsID))
			h.allowBYOWrites()
		}},
		{name: "BYO deletion whose unlink fails", config: byoNATConfig(), setup: func(h *egressRunsHarness) {
			h.track(StateDeletionInProgress, egressModeBYO, egressUnknown, &egressUnknown)
			h.dt.setBYONATGatewayID(egressRunsID, fakeARMID(egressCleanupTestBYORG, "natGateways", egressRunsID))
			h.cloud.fault(cleanupCallGetServices, armReplyError(http.StatusConflict, "Conflict"))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newEgressRunsHarness(t, tc.config)
			tc.setup(h)
			var outcomes []*egressOutcome
			h.su.onOutboundComplete = func(uid string, success bool, err error, outcome *egressOutcome) {
				outcomes = append(outcomes, outcome)
				h.dt.onServiceOperationComplete(uid, success, err, outcome)
			}
			h.su.onComplete = func(uid string, success bool, err error) {
				t.Errorf("an outbound run completed without an outcome: success %v, err %v", success, err)
				h.dt.OnServiceCreationComplete(uid, success, err)
			}

			h.dispatch()

			if assert.Len(t, outcomes, 1) && assert.NotNil(t, outcomes[0]) {
				assert.Equal(t, tc.wantMode, outcomes[0].Mode)
			}
		})
	}
}

// TestEgressRuns_DeletionPreemptsTheDispatchedRun: the run started is the one recorded at dispatch: an update preempted
// by a deletion while it waited for its worker still runs as the update, and the deletion follows once it completes.
func TestEgressRuns_DeletionPreemptsTheDispatchedRun(t *testing.T) {
	h := newEgressRunsHarness(t, testConfig())
	defaultV4 := defaultEgressConfig(egressParseContext{})
	two := egressCleanupConfig(t, false, map[string]string{consts.PodAnnotationServiceEgressGatewayPublicIPCount: "2"})
	h.cloud.seedApplied(defaultV4, false)
	h.cloud.register(egressRunsID, natGatewayID(egressRunsID, h.dt.config))
	h.track(StateCreated, egressModeManaged, defaultV4, &defaultV4)
	h.vote("pod-a", two)
	h.evaluate()
	// The dispatcher's only slot is taken, so the update it dispatches waits for its worker.
	h.su.semaphore = make(chan struct{}, 1)
	h.su.semaphore <- struct{}{}
	drainServiceUpdaterTrigger(h.dt)
	h.su.processBatch()
	h.dt.mu.Lock()
	h.dt.pendingServiceOps[egressRunsID].State = StateDeletionInProgress
	h.dt.mu.Unlock()
	<-h.su.semaphore
	h.su.wg.Wait()

	assert.Equal(t, []string{egressRunsPutPIP2, cleanupCallPutNAT}, h.cloud.writes(), "the dispatched update ran")
	op := h.op()
	assert.Equal(t, &two, appliedEgressOf(op), "and was recorded")
	assert.Equal(t, StateDeletionInProgress, op.State, "the deletion follows")

	h.cloud.resetRequests()
	h.dispatch()
	assert.Nil(t, h.op(), "then the identity is deleted")
	assert.Nil(t, h.cloud.nat(h.dt.config.ResourceGroup, egressRunsID))
	assert.Contains(t, h.cloud.writes(), cleanupCallDeleteNAT)
}
