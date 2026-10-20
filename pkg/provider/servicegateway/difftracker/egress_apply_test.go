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

	"sigs.k8s.io/cloud-provider-azure/pkg/consts"
	"sigs.k8s.io/cloud-provider-azure/pkg/log"
)

const (
	egressApplyTestID     = egressAzureTestIdentity
	egressApplyTestUserRG = "/subscriptions/sub/resourceGroups/user-rg/providers/Microsoft.Network"
	egressApplyTestNAT    = "natGateways/" + egressApplyTestID
)

func egressApplyTestUserPIPID(name string) string {
	return egressApplyTestUserRG + "/publicIPAddresses/" + name
}

func egressApplyTestUserPrefixID(name string) string {
	return egressApplyTestUserRG + "/publicIPPrefixes/" + name
}

// newApplyARM returns a tracker whose test identity's update is in progress and the fake ARM it calls, which records
// and clears the write-ahead mark of the identity's operation at every write (consumeMarks).
func newApplyARM(t *testing.T) (*DiffTracker, *fakeARM) {
	t.Helper()
	dt := &DiffTracker{
		config:            testConfig(),
		logger:            log.Noop(),
		pendingServiceOps: map[string]*ServiceOperationState{egressApplyTestID: {State: StateUpdateInProgress}},
	}
	f := newFakeARM(t, dt)
	f.update(func(f *fakeARM) { f.requireMarks, f.consumeMarks = false, true })
	return dt, f
}

// assertMarkedOnce checks that the identity's operation was marked as writing exactly once, right before the first
// write, and not at all when nothing was written.
func (f *fakeARM) assertMarkedOnce(t *testing.T) {
	t.Helper()
	f.dt.mu.Lock()
	marked := slices.Clone(f.marks)
	markedAtEnd := f.dt.pendingServiceOps[egressApplyTestID].Egress.MutationStarted
	f.dt.mu.Unlock()

	assert.False(t, markedAtEnd, "the operation was marked as writing after its last write, or without writing")
	if len(marked) == 0 {
		return
	}
	assert.True(t, marked[0], "the first write was sent before the operation was marked as writing")
	assert.Equal(t, 1, len(slices.DeleteFunc(slices.Clone(marked), func(m bool) bool { return !m })),
		"the operation was marked as writing again before a later write: %v", marked)
}

// assertReadsBeforeWrites checks that no GET followed the first write and that the NAT Gateway PUT, when sent, was
// the last write, after every Public IP and prefix PUT.
func (f *fakeARM) assertReadsBeforeWrites(t *testing.T) {
	t.Helper()
	calls := f.received()
	firstWrite := slices.IndexFunc(calls, func(c fakeARMRequest) bool { return c.Method != http.MethodGet })
	if firstWrite < 0 {
		return
	}
	for _, c := range calls[firstWrite:] {
		assert.NotEqual(t, http.MethodGet, c.Method, "%s after the first write: %v", c, calls)
	}
	natPUT := slices.IndexFunc(calls, func(c fakeARMRequest) bool { return c.String() == "PUT "+egressApplyTestNAT })
	if natPUT >= 0 {
		assert.Equal(t, len(calls)-1, natPUT, "the NAT Gateway PUT was not the last write: %v", calls)
	}
}

// egressApplyTestConfig parses annotations for the test identity in the cluster of testConfig.
func egressApplyTestConfig(t *testing.T, ipv6 bool, ann map[string]string) EgressConfig {
	t.Helper()
	cfg, _, err := parseEgressConfig(ann, egressParseContext{WantsIPv6: ipv6, NetworkSubscription: "sub", NodeResourceGroup: "rg"})
	if err != nil {
		t.Fatalf("parsing %v: %v", ann, err)
	}
	return cfg
}

func egressApplyTestDefault(ipv6 bool) EgressConfig {
	return defaultEgressConfig(egressParseContext{WantsIPv6: ipv6})
}

func egressApplyTestTags(n int) map[string]*string {
	tags := map[string]*string{}
	for i := range n {
		tags[fmt.Sprintf("foreign-%d", i)] = to.Ptr("x")
	}
	return tags
}

// egressApplyTestUserPIP is a user Public IP a NAT Gateway of the test cluster can use.
func egressApplyTestUserPIP(id string, ipv6 bool) *armnetwork.PublicIPAddress {
	return &armnetwork.PublicIPAddress{
		ID:       to.Ptr(id),
		Name:     to.Ptr(id[strings.LastIndex(id, "/")+1:]),
		Location: to.Ptr("eastus"),
		SKU:      &armnetwork.PublicIPAddressSKU{Name: to.Ptr(armnetwork.PublicIPAddressSKUNameStandardV2)},
		Properties: &armnetwork.PublicIPAddressPropertiesFormat{
			ProvisioningState:        to.Ptr(armnetwork.ProvisioningStateSucceeded),
			PublicIPAddressVersion:   to.Ptr(ipVersion(ipv6)),
			PublicIPAllocationMethod: to.Ptr(armnetwork.IPAllocationMethodStatic),
		},
	}
}

// egressApplyTestUserPrefix is a user Public IP prefix a NAT Gateway of the test cluster can use.
func egressApplyTestUserPrefix(id string, ipv6 bool, length int32) *armnetwork.PublicIPPrefix {
	return &armnetwork.PublicIPPrefix{
		ID:       to.Ptr(id),
		Name:     to.Ptr(id[strings.LastIndex(id, "/")+1:]),
		Location: to.Ptr("eastus"),
		SKU:      &armnetwork.PublicIPPrefixSKU{Name: to.Ptr(armnetwork.PublicIPPrefixSKUNameStandardV2)},
		Properties: &armnetwork.PublicIPPrefixPropertiesFormat{
			ProvisioningState:      to.Ptr(armnetwork.ProvisioningStateSucceeded),
			PublicIPAddressVersion: to.Ptr(ipVersion(ipv6)),
			PrefixLength:           to.Ptr(length),
		},
	}
}

// egressApplyState is the NAT Gateway of the test identity and the resources it references, as Azure returns them.
// Public IPs and prefixes are keyed by ID, managed and user ones alike (user ones by their canonical, lower-cased ID).
type egressApplyState struct {
	nat      *armnetwork.NatGateway
	pips     map[string]*armnetwork.PublicIPAddress
	prefixes map[string]*armnetwork.PublicIPPrefix
}

// egressApplyTestApplied returns what applying cfg leaves in Azure: the NAT Gateway and the managed resources as
// their projections create them, provisioned, and the user resources cfg references attached to the NAT Gateway
// (user prefixes /31 and /127).
func egressApplyTestApplied(t *testing.T, cfg EgressConfig, ipv6 bool) *egressApplyState {
	t.Helper()
	dt := testConfig()
	nat, err := natRequest(nil, egressApplyTestID, cfg, ipv6, dt)
	if err != nil {
		t.Fatal(err)
	}
	nat.Etag = to.Ptr(`W/"seed"`)
	nat.Properties.ProvisioningState = to.Ptr(armnetwork.ProvisioningStateSucceeded)
	s := &egressApplyState{nat: &nat, pips: map[string]*armnetwork.PublicIPAddress{}, prefixes: map[string]*armnetwork.PublicIPPrefix{}}
	attached := &armnetwork.NatGateway{ID: to.Ptr(natGatewayID(egressApplyTestID, dt))}
	for _, v6 := range egressServedFamilies(ipv6) {
		family := cfg.familyConfig(v6)
		for _, m := range family.managed(v6) {
			if m.Kind == egressManagedPrefix {
				prefix, err := managedPrefixRequest(nil, egressApplyTestID, v6, m.N, cfg, dt)
				if err != nil {
					t.Fatal(err)
				}
				prefix.Properties.ProvisioningState = to.Ptr(armnetwork.ProvisioningStateSucceeded)
				s.prefixes[*prefix.ID] = &prefix
				continue
			}
			pip, err := managedPublicIPRequest(nil, egressApplyTestID, v6, m.N, cfg, dt)
			if err != nil {
				t.Fatal(err)
			}
			pip.Properties.ProvisioningState = to.Ptr(armnetwork.ProvisioningStateSucceeded)
			s.pips[*pip.ID] = &pip
		}
		length := int32(31)
		if v6 {
			length = 127
		}
		for _, id := range egressIDList(family.PublicIPIDs) {
			pip := egressApplyTestUserPIP(id, v6)
			pip.Properties.NatGateway = attached
			s.pips[id] = pip
		}
		for _, id := range egressIDList(family.PublicIPPrefixIDs) {
			prefix := egressApplyTestUserPrefix(id, v6, length)
			prefix.Properties.NatGateway = attached
			s.prefixes[id] = prefix
		}
	}
	return s
}

func (s *egressApplyState) managedPIP(name string) *armnetwork.PublicIPAddress {
	return s.pips[publicIPAddressID(name, testConfig())]
}

// untag removes the ownership tag from the NAT Gateway and the named managed Public IPs: the shape of an identity
// created before the controller tagged its resources.
func (s *egressApplyState) untag(names ...string) {
	delete(s.nat.Tags, EgressIdentityTagKey)
	for _, name := range names {
		delete(s.managedPIP(name).Tags, EgressIdentityTagKey)
	}
}

func (f *fakeARM) seedState(t *testing.T, s *egressApplyState) {
	t.Helper()
	if s.nat != nil {
		f.seed(t, natGatewayID(egressApplyTestID, testConfig()), s.nat)
	}
	for id, pip := range s.pips {
		f.seed(t, id, pip)
	}
	for id, prefix := range s.prefixes {
		f.seed(t, id, prefix)
	}
}

// egressApplyOutcome names how a single apply ended, and checks that only the fields of that end are set.
func egressApplyOutcome(t *testing.T, result egressRunResult) string {
	t.Helper()
	outcome := "inconsistent"
	switch {
	case result.Applied != nil && result.Rejected == nil && !result.CommitUnknown && !result.Busy && result.Err == nil:
		outcome = "applied"
	case result.Rejected != nil && result.Applied == nil && !result.CommitUnknown && !result.Busy && result.Err == nil:
		outcome = "terminal"
		if result.Rejected.Retryable {
			outcome = "retryable"
		}
	case result.CommitUnknown && result.Err != nil && result.Applied == nil && result.Rejected == nil && !result.Busy && result.NAT == nil:
		outcome = "unknown"
	case result.Busy && result.Err != nil && result.Applied == nil && result.Rejected == nil && !result.CommitUnknown && result.NAT == nil:
		outcome = "busy"
	case result.Err != nil && result.Applied == nil && result.Rejected == nil && !result.CommitUnknown && !result.Busy:
		outcome = "error"
	}
	if outcome == "inconsistent" {
		t.Errorf("inconsistent result: %+v", result)
	}
	return outcome
}

// egressApplyReason returns what explains how an apply ended: the rejection reason or the error.
func egressApplyReason(result egressRunResult) string {
	if result.Rejected != nil {
		return result.Rejected.Reason
	}
	if result.Err != nil {
		return result.Err.Error()
	}
	return ""
}

// TestApplyEgressConfig_CreatesInOrder: on an identity without a NAT Gateway, every preflight read comes before the
// first write, the managed Public IPs and prefixes are written before the NAT Gateway, each write is the projection of
// the configuration, and the result carries the NAT Gateway as written.
func TestApplyEgressConfig_CreatesInOrder(t *testing.T) {
	userPIP, userPIPv6 := egressApplyTestUserPIPID("user-pip"), egressApplyTestUserPIPID("user-pip-v6")
	userPrefix := egressApplyTestUserPrefixID("user-prefix")
	cfg := egressApplyTestConfig(t, true, map[string]string{
		consts.PodAnnotationServiceEgressGatewayIdleTimeout:          "10",
		consts.PodAnnotationServiceEgressGatewayPublicIPCount:        "2",
		consts.PodAnnotationServiceEgressGatewayPublicIPPrefixLength: "31",
		consts.PodAnnotationServiceEgressGatewayPublicIPIDs:          userPIP,
		consts.PodAnnotationServiceEgressGatewayPublicIPPrefixIDs:    userPrefix,
		consts.PodAnnotationServiceEgressGatewayPublicIPIDsIPv6:      userPIPv6,
		consts.PodAnnotationServiceEgressGatewayTags:                 "team=net",
	})
	dt, a := newApplyARM(t)
	a.seed(t, userPIP, egressApplyTestUserPIP(userPIP, false))
	a.seed(t, userPIPv6, egressApplyTestUserPIP(userPIPv6, true))
	a.seed(t, userPrefix, egressApplyTestUserPrefix(userPrefix, false, 30))

	result := dt.applyEgressConfig(context.Background(), egressApplyTestID, cfg, true)

	if !assert.Equal(t, "applied", egressApplyOutcome(t, result), "result: %+v", result) {
		t.FailNow()
	}
	assert.Equal(t, cfg, *result.Applied)
	assert.True(t, result.NATWritten, "the NAT Gateway PUT applied the configuration")
	if assert.NotNil(t, result.NAT) {
		var stored armnetwork.NatGateway
		a.stored(t, natGatewayID(egressApplyTestID, testConfig()), &stored)
		assert.Equal(t, derefString(stored.Etag), derefString(result.NAT.Etag), "the result carries the NAT Gateway as written")
	}
	a.assertReadsBeforeWrites(t)
	a.assertMarkedOnce(t)
	reads := 0
	for _, c := range a.received() {
		if c.Method == http.MethodGet {
			reads++
		}
	}
	assert.Equal(t, 1+1+3+3, reads, "the NAT Gateway, the Service Gateway's services, 3 user resources and 3 managed resources were each read once")

	want := map[string]any{}
	for _, v6 := range egressServedFamilies(true) {
		for _, m := range cfg.familyConfig(v6).managed(v6) {
			var request any
			var err error
			if m.Kind == egressManagedPrefix {
				request, err = managedPrefixRequest(nil, egressApplyTestID, v6, m.N, cfg, testConfig())
				want["PUT publicIPPrefixes/"+m.name(egressApplyTestID)] = request
			} else {
				request, err = managedPublicIPRequest(nil, egressApplyTestID, v6, m.N, cfg, testConfig())
				want["PUT publicIPAddresses/"+m.name(egressApplyTestID)] = request
			}
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	nat, err := natRequest(nil, egressApplyTestID, cfg, true, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	want["PUT "+egressApplyTestNAT] = nat
	writes := a.writeRequests()
	assert.Len(t, writes, len(want))
	for _, w := range writes {
		if expected, ok := want[w.String()]; assert.True(t, ok, "unexpected %s", w) {
			assert.JSONEq(t, egressNATTestJSON(t, expected), string(w.Body), "%s sends the projection", w)
		}
	}
}

// TestApplyEgressConfig_SkipsWhenSatisfied: a settled NAT Gateway that already satisfies the configuration gets no
// write, and the operation is not marked as writing.
func TestApplyEgressConfig_SkipsWhenSatisfied(t *testing.T) {
	rich := egressApplyTestConfig(t, true, map[string]string{
		consts.PodAnnotationServiceEgressGatewayIdleTimeout:              "10",
		consts.PodAnnotationServiceEgressGatewayPublicIPCount:            "2",
		consts.PodAnnotationServiceEgressGatewayPublicIPPrefixLength:     "30",
		consts.PodAnnotationServiceEgressGatewayPublicIPIDs:              egressApplyTestUserPIPID("user-pip"),
		consts.PodAnnotationServiceEgressGatewayPublicIPPrefixIDs:        egressApplyTestUserPrefixID("user-prefix"),
		consts.PodAnnotationServiceEgressGatewayPublicIPCountIPv6:        "1",
		consts.PodAnnotationServiceEgressGatewayPublicIPPrefixLengthIPv6: "127",
		consts.PodAnnotationServiceEgressGatewayPublicIPIDsIPv6:          egressApplyTestUserPIPID("user-pip-v6"),
		consts.PodAnnotationServiceEgressGatewayPublicIPPrefixIDsIPv6:    egressApplyTestUserPrefixID("user-prefix-v6"),
		consts.PodAnnotationServiceEgressGatewayTags:                     "Env=Prod,team=net",
	})
	for _, tc := range []struct {
		name   string
		cfg    EgressConfig
		ipv6   bool
		mutate func(s *egressApplyState)
	}{
		{name: "default IPv4", cfg: egressApplyTestDefault(false)},
		{name: "default dual-stack", cfg: egressApplyTestDefault(true), ipv6: true},
		{name: "legacy untagged IPv4 identity", cfg: egressApplyTestDefault(false), mutate: func(s *egressApplyState) {
			s.untag(PublicIPName(egressApplyTestID))
		}},
		{name: "legacy untagged dual-stack identity", cfg: egressApplyTestDefault(true), ipv6: true, mutate: func(s *egressApplyState) {
			s.untag(PublicIPName(egressApplyTestID), PublicIPNameV6(egressApplyTestID))
		}},
		{name: "legacy identity with as many tags as Azure accepts: no tag limit for a write that is not needed", cfg: egressApplyTestDefault(false),
			mutate: func(s *egressApplyState) {
				s.untag(PublicIPName(egressApplyTestID))
				s.nat.Tags = egressApplyTestTags(consts.ServiceEgressGatewayMaxTags)
				s.managedPIP(PublicIPName(egressApplyTestID)).Tags = egressApplyTestTags(consts.ServiceEgressGatewayMaxTags)
			}},
		{name: "Azure reports the default idle timeout", cfg: egressApplyTestDefault(false), mutate: func(s *egressApplyState) {
			s.nat.Properties.IdleTimeoutInMinutes = to.Ptr[int32](4)
		}},
		{name: "every setting, user resources attached to this NAT Gateway", cfg: rich, ipv6: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := egressApplyTestApplied(t, tc.cfg, tc.ipv6)
			if tc.mutate != nil {
				tc.mutate(state)
			}
			dt, a := newApplyARM(t)
			a.seedState(t, state)

			result := dt.applyEgressConfig(context.Background(), egressApplyTestID, tc.cfg, tc.ipv6)

			if !assert.Equal(t, "applied", egressApplyOutcome(t, result), "result: %+v", result) {
				t.FailNow()
			}
			assert.Equal(t, tc.cfg, *result.Applied)
			assert.False(t, result.NATWritten, "the NAT Gateway already satisfied the configuration")
			assert.Empty(t, a.writes(), "nothing is written")
			a.assertMarkedOnce(t)
			if assert.NotNil(t, result.NAT) {
				assert.Equal(t, `W/"seed"`, derefString(result.NAT.Etag), "the result carries the NAT Gateway as read")
			}
		})
	}
}

// TestApplyEgressConfig_ChecksComeBeforeTheSkip pins the design order (4.4: preflight, then the skip): a NAT Gateway
// that satisfies the configuration is not recorded as applied while a user resource it references has a problem;
// the problem is reported (retryable) and nothing is written.
func TestApplyEgressConfig_ChecksComeBeforeTheSkip(t *testing.T) {
	userPIP := egressApplyTestUserPIPID("user-pip")
	cfg := egressApplyTestConfig(t, false, map[string]string{consts.PodAnnotationServiceEgressGatewayPublicIPIDs: userPIP})
	for _, tc := range []struct {
		name   string
		mutate func(s *egressApplyState, a *fakeARM)
		reason string
	}{
		{name: "user Public IP failed", mutate: func(s *egressApplyState, _ *fakeARM) {
			s.pips[strings.ToLower(userPIP)].Properties.ProvisioningState = to.Ptr(armnetwork.ProvisioningStateFailed)
		}, reason: `has provisioning state "Failed"`},
		{name: "user Public IP no longer readable", mutate: func(_ *egressApplyState, a *fakeARM) {
			a.failEvery(http.MethodGet, userPIP, armReplyError(http.StatusForbidden, "AuthorizationFailed"))
		}, reason: "cannot be read: access denied"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := egressApplyTestApplied(t, cfg, false)
			dt, a := newApplyARM(t)
			tc.mutate(state, a)
			a.seedState(t, state)

			result := dt.applyEgressConfig(context.Background(), egressApplyTestID, cfg, false)

			assert.Equal(t, "retryable", egressApplyOutcome(t, result), "result: %+v", result)
			assert.Contains(t, egressApplyReason(result), tc.reason)
			assert.Empty(t, a.writes(), "nothing is written")
		})
	}
}

// TestApplyEgressConfig_WritesWhatDiffers: only the managed resources that are missing or differ are written, and the
// NAT Gateway whenever it does not satisfy the configuration, including drift the configuration does not cause.
func TestApplyEgressConfig_WritesWhatDiffers(t *testing.T) {
	pip, pip2 := "PUT publicIPAddresses/egress-a-pip", "PUT publicIPAddresses/egress-a-pip-2"
	nat := "PUT " + egressApplyTestNAT
	def := egressApplyTestDefault(false)
	two := egressApplyTestConfig(t, false, map[string]string{consts.PodAnnotationServiceEgressGatewayPublicIPCount: "2"})
	prefix31 := egressApplyTestConfig(t, false, map[string]string{consts.PodAnnotationServiceEgressGatewayPublicIPPrefixLength: "31"})
	prefix30 := egressApplyTestConfig(t, false, map[string]string{consts.PodAnnotationServiceEgressGatewayPublicIPPrefixLength: "30"})
	userOnly := egressApplyTestConfig(t, false, map[string]string{consts.PodAnnotationServiceEgressGatewayPublicIPIDs: egressApplyTestUserPIPID("user-pip")})
	tagged := egressApplyTestConfig(t, false, map[string]string{consts.PodAnnotationServiceEgressGatewayTags: "team=net"})
	for _, tc := range []struct {
		name     string
		from, to EgressConfig
		mutate   func(s *egressApplyState)
		want     []string
	}{
		{name: "NAT Gateway unlinked from the Service Gateway", from: def, to: def, mutate: func(s *egressApplyState) {
			s.nat.Properties.ServiceGateway = nil
		}, want: []string{nat}},
		{name: "NAT Gateway idle timeout drift", from: def, to: def, mutate: func(s *egressApplyState) {
			s.nat.Properties.IdleTimeoutInMinutes = to.Ptr[int32](30)
		}, want: []string{nat}},
		{name: "NAT Gateway SKU drift", from: def, to: def, mutate: func(s *egressApplyState) {
			s.nat.SKU.Name = to.Ptr(armnetwork.NatGatewaySKUNameStandard)
		}, want: []string{nat}},
		{name: "NAT Gateway failed", from: def, to: def, mutate: func(s *egressApplyState) {
			s.nat.Properties.ProvisioningState = to.Ptr(armnetwork.ProvisioningStateFailed)
		}, want: []string{nat}},
		{name: "managed Public IP canceled", from: def, to: def, mutate: func(s *egressApplyState) {
			s.managedPIP("egress-a-pip").Properties.ProvisioningState = to.Ptr(armnetwork.ProvisioningStateCanceled)
		}, want: []string{pip, nat}},
		{name: "tags added", from: def, to: tagged, want: []string{pip, nat}},
		{name: "scale up 1 -> 2", from: def, to: two, want: []string{pip2, nat}},
		{name: "scale down 2 -> 1: the extra Public IP is left to the sweep", from: two, to: def, want: []string{nat}},
		{name: "NAT Gateway unlinked: the satisfied managed prefix is not written", from: prefix31, to: prefix31, mutate: func(s *egressApplyState) {
			s.nat.Properties.ServiceGateway = nil
		}, want: []string{nat}},
		{name: "managed prefix length change creates the new prefix", from: prefix31, to: prefix30, want: []string{"PUT publicIPPrefixes/egress-a-prefix-30", nat}},
		{name: "managed Public IP replaced by a user Public IP", from: def, to: userOnly, want: []string{nat}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := egressApplyTestApplied(t, tc.from, false)
			if tc.mutate != nil {
				tc.mutate(state)
			}
			dt, a := newApplyARM(t)
			a.seedState(t, state)
			if id := egressApplyTestUserPIPID("user-pip"); tc.to == userOnly {
				a.seed(t, id, egressApplyTestUserPIP(id, false))
			}

			result := dt.applyEgressConfig(context.Background(), egressApplyTestID, tc.to, false)

			if !assert.Equal(t, "applied", egressApplyOutcome(t, result), "result: %+v", result) {
				t.FailNow()
			}
			got := a.writes()
			if len(got) > 1 {
				slices.Sort(got[:len(got)-1])
			}
			assert.Equal(t, tc.want, got)
			a.assertReadsBeforeWrites(t)
			a.assertMarkedOnce(t)
		})
	}
}

// TestApplyEgressConfig_LegacyBasePublicIPGetsTheOwnershipTag: an identity created before the controller tagged its
// resources satisfies the default untagged, but a run that writes its NAT Gateway (which adds the ownership tag, so
// the legacy rule no longer applies) writes the untagged base Public IPs with the tag as well (legacyNAT false).
func TestApplyEgressConfig_LegacyBasePublicIPGetsTheOwnershipTag(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  EgressConfig
		ipv6 bool
		want []string
	}{
		{name: "idle timeout change", cfg: egressApplyTestConfig(t, false, map[string]string{
			consts.PodAnnotationServiceEgressGatewayIdleTimeout: "10",
		}), want: []string{"PUT publicIPAddresses/egress-a-pip"}},
		{name: "dual-stack idle timeout change", ipv6: true, cfg: egressApplyTestConfig(t, true, map[string]string{
			consts.PodAnnotationServiceEgressGatewayIdleTimeout: "10",
		}), want: []string{"PUT publicIPAddresses/egress-a-pip", "PUT publicIPAddresses/egress-a-pip-v6"}},
		{name: "relink only", cfg: egressApplyTestDefault(false), want: []string{"PUT publicIPAddresses/egress-a-pip"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := egressApplyTestApplied(t, egressApplyTestDefault(tc.ipv6), tc.ipv6)
			names := []string{PublicIPName(egressApplyTestID)}
			if tc.ipv6 {
				names = append(names, PublicIPNameV6(egressApplyTestID))
			}
			state.untag(names...)
			state.nat.Properties.ServiceGateway = nil
			dt, a := newApplyARM(t)
			a.seedState(t, state)

			result := dt.applyEgressConfig(context.Background(), egressApplyTestID, tc.cfg, tc.ipv6)

			if !assert.Equal(t, "applied", egressApplyOutcome(t, result), "result: %+v", result) {
				t.FailNow()
			}
			got := a.writes()
			if len(got) > 1 {
				slices.Sort(got[:len(got)-1])
			}
			assert.Equal(t, append(slices.Clone(tc.want), "PUT "+egressApplyTestNAT), got)
			for _, name := range names {
				var pip armnetwork.PublicIPAddress
				a.stored(t, publicIPAddressID(name, testConfig()), &pip)
				assert.True(t, hasEgressIdentityTag(pip.Tags, egressApplyTestID), "%s is written with the ownership tag", name)
			}
			var written armnetwork.NatGateway
			a.stored(t, natGatewayID(egressApplyTestID, testConfig()), &written)
			assert.True(t, hasEgressIdentityTag(written.Tags, egressApplyTestID), "the NAT Gateway is written with the ownership tag")
			var pips []*armnetwork.PublicIPAddress
			for _, name := range names {
				var pip armnetwork.PublicIPAddress
				a.stored(t, publicIPAddressID(name, testConfig()), &pip)
				pips = append(pips, &pip)
			}
			assert.Empty(t, natMismatch(&written, newEgressManagedResources(pips, nil), egressApplyTestID, tc.cfg, tc.ipv6, testConfig()),
				"after the run the identity satisfies the configuration without the legacy rule")
		})
	}
}

// TestApplyEgressConfig_WritesProjectionsOverTheReads: the PUTs are the projections of the configuration over the
// resources as read, so what the controller does not set (foreign tags, zones) is kept, and no etag is sent.
func TestApplyEgressConfig_WritesProjectionsOverTheReads(t *testing.T) {
	state := egressApplyTestApplied(t, egressApplyTestDefault(false), false)
	base := state.managedPIP("egress-a-pip")
	base.Tags["owner"] = to.Ptr("someone")
	base.Zones = []*string{to.Ptr("1")}
	base.Etag = to.Ptr(`W/"pip"`)
	state.nat.Tags["owner"] = to.Ptr("someone")
	state.nat.Zones = []*string{to.Ptr("1")}
	cfg := egressApplyTestConfig(t, false, map[string]string{consts.PodAnnotationServiceEgressGatewayTags: "team=net"})
	dt, a := newApplyARM(t)
	a.seedState(t, state)
	// What the controller reads back: the seeded resources after a round trip through JSON.
	var readNAT armnetwork.NatGateway
	var readPIP armnetwork.PublicIPAddress
	a.stored(t, natGatewayID(egressApplyTestID, testConfig()), &readNAT)
	a.stored(t, *base.ID, &readPIP)

	result := dt.applyEgressConfig(context.Background(), egressApplyTestID, cfg, false)

	if !assert.Equal(t, "applied", egressApplyOutcome(t, result), "result: %+v", result) {
		t.FailNow()
	}
	wantNAT, err := natRequest(&readNAT, egressApplyTestID, cfg, false, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	wantPIP, err := managedPublicIPRequest(&readPIP, egressApplyTestID, false, 1, cfg, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	writes := a.writeRequests()
	if !assert.Len(t, writes, 2) {
		t.FailNow()
	}
	assert.JSONEq(t, egressNATTestJSON(t, wantPIP), string(writes[0].Body))
	assert.JSONEq(t, egressNATTestJSON(t, wantNAT), string(writes[1].Body))
	for _, w := range writes {
		assert.NotContains(t, string(w.Body), `"etag"`, "%s carries no etag", w)
		assert.Contains(t, string(w.Body), `"owner":"someone"`, "%s keeps the foreign tag", w)
		assert.Contains(t, string(w.Body), `"zones":["1"]`, "%s keeps the zones", w)
	}
}

// TestApplyEgressConfig_Preflight: every way the preflight ends the apply, before any write: terminal and retryable
// rejections (design 2.3), busy on an unsettled read, a plain error on a failed read or a NAT Gateway the controller
// does not own, and which of them wins when several apply. User resources whose SKU and IP version Azure returns in
// another case pass it, and the apply goes on.
func TestApplyEgressConfig_Preflight(t *testing.T) {
	userPIP, userPIPv6 := egressApplyTestUserPIPID("user-pip"), egressApplyTestUserPIPID("user-pip-v6")
	userPrefix, userPrefixV6 := egressApplyTestUserPrefixID("user-prefix"), egressApplyTestUserPrefixID("user-prefix-v6")
	cfg := egressApplyTestConfig(t, true, map[string]string{
		consts.PodAnnotationServiceEgressGatewayPublicIPCount:         "2",
		consts.PodAnnotationServiceEgressGatewayPublicIPPrefixLength:  "31",
		consts.PodAnnotationServiceEgressGatewayPublicIPIDs:           userPIP,
		consts.PodAnnotationServiceEgressGatewayPublicIPPrefixIDs:     userPrefix,
		consts.PodAnnotationServiceEgressGatewayPublicIPCountIPv6:     "1",
		consts.PodAnnotationServiceEgressGatewayPublicIPIDsIPv6:       userPIPv6,
		consts.PodAnnotationServiceEgressGatewayPublicIPPrefixIDsIPv6: userPrefixV6,
	})
	tagged := egressApplyTestConfig(t, true, map[string]string{consts.PodAnnotationServiceEgressGatewayTags: "team=net"})
	taggedPrefix := egressApplyTestConfig(t, true, map[string]string{
		consts.PodAnnotationServiceEgressGatewayTags:                 "team=net",
		consts.PodAnnotationServiceEgressGatewayPublicIPPrefixLength: "31",
	})
	taggedUser := egressApplyTestConfig(t, true, map[string]string{
		consts.PodAnnotationServiceEgressGatewayTags:        "team=net",
		consts.PodAnnotationServiceEgressGatewayPublicIPIDs: userPIP,
	})
	// A user prefix is the only address source of a family.
	prefixOnly := egressApplyTestConfig(t, false, map[string]string{consts.PodAnnotationServiceEgressGatewayPublicIPPrefixIDs: userPrefix})
	prefixOnlyV6 := egressApplyTestConfig(t, true, map[string]string{consts.PodAnnotationServiceEgressGatewayPublicIPPrefixIDsIPv6: userPrefixV6})
	prefixAndPIP := egressApplyTestConfig(t, false, map[string]string{
		consts.PodAnnotationServiceEgressGatewayPublicIPCount:     "1",
		consts.PodAnnotationServiceEgressGatewayPublicIPPrefixIDs: userPrefix,
	})
	// 14 managed Public IPs and a user prefix: 16 addresses as parsing counts them, the most a NAT Gateway accepts.
	prefixAndFourteen := egressApplyTestConfig(t, false, map[string]string{
		consts.PodAnnotationServiceEgressGatewayPublicIPCount:     "14",
		consts.PodAnnotationServiceEgressGatewayPublicIPPrefixIDs: userPrefix,
	})
	otherNAT := &armnetwork.NatGateway{ID: to.Ptr("/subscriptions/sub/resourceGroups/user-rg/providers/Microsoft.Network/natGateways/other")}
	state := func(s armnetwork.ProvisioningState) *armnetwork.ProvisioningState { return &s }
	serverError := armReplyError(http.StatusInternalServerError, "InternalServerError")
	type setup struct {
		s *egressApplyState
		a *fakeARM
	}
	pip := func(x setup, id string) *armnetwork.PublicIPAddress { return x.s.pips[id] }
	prefix := func(x setup, id string) *armnetwork.PublicIPPrefix { return x.s.prefixes[id] }
	for _, tc := range []struct {
		name string
		// cfg and ipv6 override the configuration applied.
		cfg     *EgressConfig
		ipv4    bool
		mutate  func(x setup)
		outcome string
		reason  string
		absent  string
	}{
		// No problem: applied. SKUs and IP versions compare case-insensitively, as in Azure.
		{name: "user resources' SKU and IP version in another case", mutate: func(x setup) {
			pip(x, userPIP).SKU.Name = to.Ptr(armnetwork.PublicIPAddressSKUName("standardv2"))
			pip(x, userPIP).Properties.PublicIPAddressVersion = to.Ptr(armnetwork.IPVersion("ipv4"))
			prefix(x, userPrefix).SKU.Name = to.Ptr(armnetwork.PublicIPPrefixSKUName("STANDARDV2"))
			prefix(x, userPrefix).Properties.PublicIPAddressVersion = to.Ptr(armnetwork.IPVersion("IPV4"))
		}, outcome: "applied"},

		// Deterministic: terminal.
		{name: "user Public IP of SKU Standard", mutate: func(x setup) {
			pip(x, userPIP).SKU.Name = to.Ptr(armnetwork.PublicIPAddressSKUNameStandard)
		}, outcome: "terminal", reason: `user Public IP "` + strings.ToLower(userPIP) + `" has SKU "Standard", not StandardV2`},
		{name: "user prefix of SKU Standard", mutate: func(x setup) {
			prefix(x, userPrefixV6).SKU.Name = to.Ptr(armnetwork.PublicIPPrefixSKUNameStandard)
		}, outcome: "terminal", reason: `has SKU "Standard", not StandardV2`},
		{name: "user Public IP in another region", mutate: func(x setup) {
			pip(x, userPIP).Location = to.Ptr("westus")
		}, outcome: "terminal", reason: `is in location "westus", not "eastus"`},
		{name: "user prefix in another region", mutate: func(x setup) {
			prefix(x, userPrefix).Location = to.Ptr("westus")
		}, outcome: "terminal", reason: `is in location "westus"`},
		{name: "IPv4 user Public IP listed for IPv6", mutate: func(x setup) {
			pip(x, userPIPv6).Properties.PublicIPAddressVersion = to.Ptr(armnetwork.IPVersionIPv4)
		}, outcome: "terminal", reason: `is IPv4, but it is listed for IPv6`},
		{name: "IPv6 user prefix listed for IPv4", mutate: func(x setup) {
			prefix(x, userPrefix).Properties.PublicIPAddressVersion = to.Ptr(armnetwork.IPVersionIPv6)
		}, outcome: "terminal", reason: `is IPv6, but it is listed for IPv4`},
		{name: "exact address count over 16 with the user prefix size as read", mutate: func(x setup) {
			prefix(x, userPrefix).Properties.PrefixLength = to.Ptr[int32](28)
		}, outcome: "terminal", reason: "IPv4: 21 public addresses, more than the 16 a NAT Gateway accepts"},
		{name: "exact IPv6 address count over 16", mutate: func(x setup) {
			prefix(x, userPrefixV6).Properties.PrefixLength = to.Ptr[int32](124)
		}, outcome: "terminal", reason: "IPv6: 18 public addresses"},
		{name: "IPv4 user prefix listed for IPv6 is not counted for IPv6", mutate: func(x setup) {
			prefix(x, userPrefixV6).Properties.PublicIPAddressVersion = to.Ptr(armnetwork.IPVersionIPv4)
			prefix(x, userPrefixV6).Properties.PrefixLength = to.Ptr[int32](28)
		}, outcome: "terminal", reason: "is IPv4, but it is listed for IPv6", absent: "public addresses"},
		{name: "IPv6 user prefix far larger than 16 addresses", mutate: func(x setup) {
			prefix(x, userPrefixV6).Properties.PrefixLength = to.Ptr[int32](64)
		}, outcome: "terminal", reason: "IPv6: 1073741826 public addresses"},
		{name: "a user prefix as read counts for its exact size", cfg: &prefixAndPIP, ipv4: true, mutate: func(x setup) {
			prefix(x, userPrefix).Properties.PrefixLength = to.Ptr[int32](28)
		}, outcome: "terminal", reason: "IPv4: 17 public addresses, more than the 16"},
		{name: "managed prefix length out of range", cfg: &EgressConfig{IdleTimeoutMinutes: 4, IPv4: egressFamilyConfig{ManagedPrefixLength: 40}},
			ipv4: true, outcome: "terminal", reason: `managed resource "egress-a-prefix-40" is not a reserved name`},
		{name: "served family without a public address", cfg: to.Ptr(egressApplyTestDefault(false)), outcome: "terminal", reason: "IPv6: no public address"},
		{name: "user resource ID that is not one", cfg: &EgressConfig{IdleTimeoutMinutes: 4, IPv4: egressFamilyConfig{PublicIPIDs: "not-an-id"}},
			ipv4: true, outcome: "terminal", reason: `user Public IP "not-an-id" is not a resource ID`},
		{name: "more merged tags on the NAT Gateway than Azure accepts", cfg: &tagged, mutate: func(x setup) {
			for k, v := range egressApplyTestTags(49) {
				x.s.nat.Tags[k] = v
			}
		}, outcome: "terminal", reason: `egress NAT Gateway "egress-a": 51 tags, more than the 50`},
		{name: "more merged tags on a managed Public IP than Azure accepts", cfg: &tagged, mutate: func(x setup) {
			for k, v := range egressApplyTestTags(49) {
				x.s.managedPIP("egress-a-pip-v6").Tags[k] = v
			}
		}, outcome: "terminal", reason: `egress Public IP "egress-a-pip-v6": 51 tags`},

		{name: "more merged tags on a managed prefix than Azure accepts", cfg: &taggedPrefix, mutate: func(x setup) {
			p := egressApplyTestUserPrefix(publicIPPrefixID("egress-a-prefix-31", testConfig()), false, 31)
			p.Tags = egressApplyTestTags(49)
			p.Tags[EgressIdentityTagKey] = to.Ptr(egressApplyTestID)
			x.s.prefixes[*p.ID] = p
		}, outcome: "terminal", reason: `egress Public IP prefix "egress-a-prefix-31": 51 tags`},

		// Mutable: retryable.
		{name: "user Public IP missing", mutate: func(x setup) { delete(x.s.pips, userPIP) },
			outcome: "retryable", reason: `user Public IP "` + strings.ToLower(userPIP) + `" does not exist`},
		{name: "user prefix missing", mutate: func(x setup) { delete(x.s.prefixes, userPrefixV6) },
			outcome: "retryable", reason: `user Public IP prefix "` + strings.ToLower(userPrefixV6) + `" does not exist`},
		// A user prefix that cannot be read counts as parsing counted it, so the address count decides nothing yet.
		{name: "the only address source, a user prefix, missing", cfg: &prefixOnly, ipv4: true, mutate: func(x setup) { delete(x.s.prefixes, userPrefix) },
			outcome: "retryable", reason: `user Public IP prefix "` + strings.ToLower(userPrefix) + `" does not exist`, absent: "public address"},
		{name: "the only address source, a user prefix, unreadable", cfg: &prefixOnly, ipv4: true, mutate: func(x setup) {
			x.a.failEvery(http.MethodGet, userPrefix, armReplyError(http.StatusForbidden, "AuthorizationFailed"))
		}, outcome: "retryable", reason: "cannot be read: access denied", absent: "public address"},
		{name: "the only IPv6 address source, a user prefix, missing", cfg: &prefixOnlyV6, mutate: func(x setup) { delete(x.s.prefixes, userPrefixV6) },
			outcome: "retryable", reason: "does not exist", absent: "public address"},
		{name: "the only IPv6 address source, a user prefix, unreadable", cfg: &prefixOnlyV6, mutate: func(x setup) {
			x.a.failEvery(http.MethodGet, userPrefixV6, armReplyError(http.StatusForbidden, "AuthorizationFailed"))
		}, outcome: "retryable", reason: "cannot be read: access denied", absent: "public address"},
		{name: "14 managed Public IPs and a missing user prefix are 16 addresses", cfg: &prefixAndFourteen, ipv4: true,
			mutate:  func(x setup) { delete(x.s.prefixes, userPrefix) },
			outcome: "retryable", reason: "does not exist", absent: "more than the 16"},
		{name: "14 managed Public IPs and an unreadable user prefix are 16 addresses", cfg: &prefixAndFourteen, ipv4: true, mutate: func(x setup) {
			x.a.failEvery(http.MethodGet, userPrefix, armReplyError(http.StatusForbidden, "AuthorizationFailed"))
		}, outcome: "retryable", reason: "cannot be read: access denied", absent: "more than the 16"},
		{name: "user Public IP unreadable", mutate: func(x setup) {
			x.a.failEvery(http.MethodGet, userPIP, armReplyError(http.StatusForbidden, "AuthorizationFailed"))
		}, outcome: "retryable", reason: "cannot be read: access denied"},
		{name: "user Public IP failed", mutate: func(x setup) {
			pip(x, userPIP).Properties.ProvisioningState = state(armnetwork.ProvisioningStateFailed)
		}, outcome: "retryable", reason: `has provisioning state "Failed"`},
		{name: "user prefix canceled", mutate: func(x setup) {
			prefix(x, userPrefix).Properties.ProvisioningState = state(armnetwork.ProvisioningStateCanceled)
		}, outcome: "retryable", reason: `has provisioning state "Canceled"`},
		{name: "user Public IP attached to another NAT Gateway", mutate: func(x setup) {
			pip(x, userPIP).Properties.NatGateway = otherNAT
		}, outcome: "retryable", reason: "is attached to NAT Gateway"},
		{name: "user Public IP used by an IP configuration", mutate: func(x setup) {
			pip(x, userPIPv6).Properties.IPConfiguration = &armnetwork.IPConfiguration{ID: to.Ptr("/subscriptions/sub/resourceGroups/user-rg/providers/Microsoft.Network/loadBalancers/lb/frontendIPConfigurations/fe")}
		}, outcome: "retryable", reason: `is in use by IP configuration`},
		{name: "user prefix attached to another NAT Gateway", mutate: func(x setup) {
			prefix(x, userPrefix).Properties.NatGateway = otherNAT
		}, outcome: "retryable", reason: "is attached to NAT Gateway"},
		{name: "user prefix used by a load balancer frontend", mutate: func(x setup) {
			prefix(x, userPrefix).Properties.LoadBalancerFrontendIPConfiguration = &armnetwork.SubResource{ID: to.Ptr("/subscriptions/sub/resourceGroups/user-rg/providers/Microsoft.Network/loadBalancers/lb/frontendIPConfigurations/fe")}
		}, outcome: "retryable", reason: "is in use by load balancer frontend"},
		{name: "user prefix with Public IPs allocated from it", mutate: func(x setup) {
			prefix(x, userPrefixV6).Properties.PublicIPAddresses = []*armnetwork.ReferencedPublicIPAddress{{ID: to.Ptr(egressApplyTestUserPIPID("from-prefix"))}}
		}, outcome: "retryable", reason: "is in use by 1 Public IPs allocated from it"},

		// Unsettled: busy.
		{name: "NAT Gateway updating", mutate: func(x setup) {
			x.s.nat.Properties.ProvisioningState = state(armnetwork.ProvisioningStateUpdating)
		}, outcome: "busy", reason: `NAT Gateway "egress-a" is Updating`},
		{name: "user Public IP updating", mutate: func(x setup) {
			pip(x, userPIP).Properties.ProvisioningState = state(armnetwork.ProvisioningStateUpdating)
		}, outcome: "busy", reason: "is Updating"},
		{name: "user prefix creating", mutate: func(x setup) {
			prefix(x, userPrefixV6).Properties.ProvisioningState = state(armnetwork.ProvisioningStateCreating)
		}, outcome: "busy", reason: "is Creating"},
		{name: "managed Public IP deleting", mutate: func(x setup) {
			x.s.managedPIP("egress-a-pip").Properties.ProvisioningState = state(armnetwork.ProvisioningStateDeleting)
		}, outcome: "busy", reason: `Public IP "egress-a-pip" is Deleting`},
		{name: "managed prefix creating", mutate: func(x setup) {
			p := egressApplyTestUserPrefix(publicIPPrefixID("egress-a-prefix-31", testConfig()), false, 31)
			p.Tags = egressIdentityTags(egressApplyTestID)
			p.Properties.ProvisioningState = state(armnetwork.ProvisioningStateCreating)
			x.s.prefixes[*p.ID] = p
		}, outcome: "busy", reason: `Public IP prefix "egress-a-prefix-31" is Creating`},

		// Failed reads and a NAT Gateway that is not the controller's: plain errors.
		{name: "NAT Gateway read failed", mutate: func(x setup) {
			x.a.failEvery(http.MethodGet, natGatewayID(egressApplyTestID, testConfig()), serverError)
		}, outcome: "error", reason: "InternalServerError"},
		{name: "Service Gateway services read failed", mutate: func(x setup) {
			x.a.failEvery(http.MethodGet, egressAzureTestServicesPath, serverError)
		}, outcome: "error", reason: "getting Service Gateway services"},
		{name: "user Public IP read failed", mutate: func(x setup) { x.a.failEvery(http.MethodGet, userPIP, serverError) },
			outcome: "error", reason: "InternalServerError"},
		{name: "managed Public IP read failed", mutate: func(x setup) {
			x.a.failEvery(http.MethodGet, publicIPAddressID("egress-a-pip-2", testConfig()), serverError)
		}, outcome: "error", reason: "InternalServerError"},
		{name: "managed Public IP unreadable is no user problem", mutate: func(x setup) {
			x.a.failEvery(http.MethodGet, publicIPAddressID("egress-a-pip-2", testConfig()), armReplyError(http.StatusForbidden, "AuthorizationFailed"))
		}, outcome: "error", reason: "AuthorizationFailed"},
		{name: "NAT Gateway of another owner", mutate: func(x setup) {
			delete(x.s.nat.Tags, EgressIdentityTagKey)
			x.s.nat.Properties.PublicIPAddresses = []*armnetwork.SubResource{{ID: to.Ptr(egressApplyTestUserPIPID("theirs"))}}
		}, outcome: "error", reason: "not managed by this controller"},
		{name: "NAT Gateway tagged for another identity", mutate: func(x setup) {
			x.s.nat.Tags[EgressIdentityTagKey] = to.Ptr("other")
		}, outcome: "error", reason: "not managed by this controller"},
		{name: "NAT Gateway used by another Service Gateway service", mutate: func(x setup) {
			x.a.register("default-outbound", natGatewayID(egressApplyTestID, testConfig()))
		}, outcome: "error", reason: `NAT Gateway "egress-a" is used by Service Gateway service "default-outbound"`},

		// Precedence.
		{name: "a failed read wins over an unsettled read", mutate: func(x setup) {
			x.a.failEvery(http.MethodGet, userPrefixV6, serverError)
			x.s.nat.Properties.ProvisioningState = state(armnetwork.ProvisioningStateUpdating)
		}, outcome: "error", reason: "InternalServerError"},
		{name: "an unsettled read wins over a deterministic problem", mutate: func(x setup) {
			pip(x, userPIPv6).Properties.ProvisioningState = state(armnetwork.ProvisioningStateUpdating)
			prefix(x, userPrefix).SKU.Name = to.Ptr(armnetwork.PublicIPPrefixSKUNameStandard)
		}, outcome: "busy", reason: "is Updating"},
		{name: "an unsettled read wins over the NAT Gateway owner", mutate: func(x setup) {
			x.s.nat.Tags[EgressIdentityTagKey] = to.Ptr("other")
			pip(x, userPIP).Properties.ProvisioningState = state(armnetwork.ProvisioningStateCreating)
		}, outcome: "busy", reason: "is Creating"},
		{name: "the NAT Gateway owner wins over a deterministic problem", mutate: func(x setup) {
			x.s.nat.Tags[EgressIdentityTagKey] = to.Ptr("other")
			prefix(x, userPrefix).SKU.Name = to.Ptr(armnetwork.PublicIPPrefixSKUNameStandard)
		}, outcome: "error", reason: "not managed by this controller"},
		{name: "another service's use of the NAT Gateway wins over a deterministic problem", mutate: func(x setup) {
			x.a.register("default-outbound", natGatewayID(egressApplyTestID, testConfig()))
			prefix(x, userPrefix).SKU.Name = to.Ptr(armnetwork.PublicIPPrefixSKUNameStandard)
		}, outcome: "error", reason: "is used by Service Gateway service"},
		{name: "a deterministic problem wins over a mutable one and is the reason", mutate: func(x setup) {
			delete(x.s.pips, userPIP)
			prefix(x, userPrefix).SKU.Name = to.Ptr(armnetwork.PublicIPPrefixSKUNameStandard)
		}, outcome: "terminal", reason: `has SKU "Standard"`, absent: "does not exist"},
		{name: "a deterministic tag problem wins over a mutable one", cfg: &taggedUser, mutate: func(x setup) {
			for k, v := range egressApplyTestTags(49) {
				x.s.nat.Tags[k] = v
			}
			delete(x.s.pips, userPIP)
		}, outcome: "terminal", reason: "51 tags", absent: "does not exist"},
		{name: "every mutable problem is the reason", mutate: func(x setup) {
			delete(x.s.pips, userPIP)
			prefix(x, userPrefix).Properties.NatGateway = otherNAT
		}, outcome: "retryable", reason: `does not exist; user Public IP prefix "` + strings.ToLower(userPrefix) + `" is attached to NAT Gateway`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := egressApplyTestApplied(t, egressApplyTestDefault(true), true)
			s.pips[userPIP] = egressApplyTestUserPIP(userPIP, false)
			s.pips[userPIPv6] = egressApplyTestUserPIP(userPIPv6, true)
			s.prefixes[userPrefix] = egressApplyTestUserPrefix(userPrefix, false, 31)
			s.prefixes[userPrefixV6] = egressApplyTestUserPrefix(userPrefixV6, true, 127)
			dt, a := newApplyARM(t)
			if tc.mutate != nil {
				tc.mutate(setup{s: s, a: a})
			}
			a.seedState(t, s)
			apply, ipv6 := cfg, !tc.ipv4
			if tc.cfg != nil {
				apply = *tc.cfg
			}

			result := dt.applyEgressConfig(context.Background(), egressApplyTestID, apply, ipv6)

			assert.Equal(t, tc.outcome, egressApplyOutcome(t, result), "result: %+v", result)
			assert.Contains(t, egressApplyReason(result), tc.reason)
			if tc.absent != "" {
				assert.NotContains(t, egressApplyReason(result), tc.absent)
			}
			if tc.outcome == "applied" {
				assert.True(t, result.NATWritten, "the preflight passed and the NAT Gateway PUT applied the configuration")
				a.assertMarkedOnce(t)
				return
			}
			if result.Rejected != nil {
				assert.Equal(t, apply, result.Rejected.Config)
				if assert.NotNil(t, result.NAT, "a rejection carries the settled NAT Gateway") {
					assert.Equal(t, `W/"seed"`, derefString(result.NAT.Etag))
				}
			} else {
				assert.Nil(t, result.NAT, "no NAT Gateway without settled reads")
			}
			assert.Empty(t, a.writes(), "nothing is written")
			a.assertMarkedOnce(t)
		})
	}
}

// TestEgressApplyCheckAddresses_UnreadUserPrefix: a user prefix whose size is not known, missing or unreadable, counts
// for the 2 addresses parsing counted, the fewest a prefix has. With 14 managed Public IPs that is 16 addresses, the
// most a NAT Gateway accepts; with 15 it is 17, over the limit whatever the prefix's size.
func TestEgressApplyCheckAddresses_UnreadUserPrefix(t *testing.T) {
	userPrefix := strings.ToLower(egressApplyTestUserPrefixID("user-prefix"))
	run := &egressApplyRun{identity: egressApplyTestID}
	for _, tc := range []struct {
		managed int
		want    []string
	}{
		{managed: 14},
		{managed: 15, want: []string{"IPv4: 17 public addresses, more than the 16 a NAT Gateway accepts " +
			"(managed Public IPs + managed prefix size + user Public IPs + user prefix sizes)"}},
	} {
		for _, read := range []*egressApplyUserRead{
			{id: userPrefix, isPrefix: true},
			{id: userPrefix, isPrefix: true, err: errors.New("access denied")},
		} {
			var problems egressApplyProblems
			cfg := EgressConfig{IdleTimeoutMinutes: 4, IPv4: egressFamilyConfig{ManagedPublicIPs: tc.managed, PublicIPPrefixIDs: userPrefix}}

			run.checkAddresses(&problems, cfg, []*egressApplyUserRead{read})

			assert.Equal(t, tc.want, problems.terminal, "%d managed Public IPs, read error %v", tc.managed, read.err)
			assert.Empty(t, problems.retryable)
		}
	}
}

// TestApplyEgressConfig_OwnershipCollision: a desired numbered Public IP or prefix that exists without the
// identity's ownership tag is never overwritten, in either IP family: the configuration is rejected retryably and
// nothing is written, even when the resource carries every tag Azure accepts (a resource never written is never
// projected, so adding the ownership tag cannot make the rejection terminal). A base Public IP is the identity's by its
// name.
func TestApplyEgressConfig_OwnershipCollision(t *testing.T) {
	cfgs := map[bool]EgressConfig{
		false: egressApplyTestConfig(t, false, map[string]string{
			consts.PodAnnotationServiceEgressGatewayPublicIPCount:        "2",
			consts.PodAnnotationServiceEgressGatewayPublicIPPrefixLength: "31",
		}),
		true: egressApplyTestConfig(t, true, map[string]string{
			consts.PodAnnotationServiceEgressGatewayPublicIPCountIPv6:        "2",
			consts.PodAnnotationServiceEgressGatewayPublicIPPrefixLengthIPv6: "127",
		}),
	}
	theirs := func(ipv6 bool, tags map[string]*string) (*armnetwork.PublicIPAddress, *armnetwork.PublicIPPrefix) {
		pipName, prefixName, length := "egress-a-pip-2", "egress-a-prefix-31", int32(31)
		if ipv6 {
			pipName, prefixName, length = "egress-a-pip-v6-2", "egress-a-prefix-v6-127", 127
		}
		pip := egressApplyTestUserPIP(publicIPAddressID(pipName, testConfig()), ipv6)
		pip.Tags = tags
		prefix := egressApplyTestUserPrefix(publicIPPrefixID(prefixName, testConfig()), ipv6, length)
		prefix.Tags = tags
		return pip, prefix
	}
	fullTags := egressApplyTestTags(consts.ServiceEgressGatewayMaxTags)
	for _, tc := range []struct {
		name        string
		ipv6        bool
		pip, prefix bool
		tags        map[string]*string
		untagBase   bool
		outcome     string
		reasons     []string
	}{
		{name: "untagged numbered Public IP", pip: true, outcome: "retryable",
			reasons: []string{`Public IP "egress-a-pip-2" exists without the ownership tag of egress identity "egress-a"`}},
		{name: "untagged managed prefix", prefix: true, outcome: "retryable",
			reasons: []string{`Public IP prefix "egress-a-prefix-31" exists without the ownership tag`}},
		{name: "numbered Public IP and prefix of another identity", pip: true, prefix: true, tags: egressIdentityTags("other"), outcome: "retryable",
			reasons: []string{`Public IP "egress-a-pip-2" exists without`, `Public IP prefix "egress-a-prefix-31" exists without`}},
		{name: "numbered Public IP and prefix of nobody with every tag Azure accepts: still retryable", pip: true, prefix: true,
			tags: fullTags, outcome: "retryable",
			reasons: []string{`Public IP "egress-a-pip-2" exists without`, `Public IP prefix "egress-a-prefix-31" exists without`}},
		{name: "IPv6 numbered Public IP and prefix of another identity", ipv6: true, pip: true, prefix: true,
			tags: egressIdentityTags("other"), outcome: "retryable",
			reasons: []string{`Public IP "egress-a-pip-v6-2" exists without`, `Public IP prefix "egress-a-prefix-v6-127" exists without`}},
		{name: "IPv6 numbered Public IP and prefix of nobody with every tag Azure accepts: still retryable", ipv6: true, pip: true,
			prefix: true, tags: fullTags, outcome: "retryable",
			reasons: []string{`Public IP "egress-a-pip-v6-2" exists without`, `Public IP prefix "egress-a-prefix-v6-127" exists without`}},
		{name: "untagged base Public IP is the identity's by its name", untagBase: true, outcome: "applied"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := egressApplyTestApplied(t, egressApplyTestDefault(tc.ipv6), tc.ipv6)
			pip, prefix := theirs(tc.ipv6, tc.tags)
			if tc.pip {
				s.pips[*pip.ID] = pip
			}
			if tc.prefix {
				s.prefixes[*prefix.ID] = prefix
			}
			if tc.untagBase {
				delete(s.managedPIP("egress-a-pip").Tags, EgressIdentityTagKey)
			}
			dt, a := newApplyARM(t)
			a.seedState(t, s)

			result := dt.applyEgressConfig(context.Background(), egressApplyTestID, cfgs[tc.ipv6], tc.ipv6)

			if !assert.Equal(t, tc.outcome, egressApplyOutcome(t, result), "result: %+v", result) {
				t.FailNow()
			}
			for _, reason := range tc.reasons {
				assert.Contains(t, egressApplyReason(result), reason)
			}
			assert.NotContains(t, egressApplyReason(result), "tags, more than", "a resource never written is never projected")
			if tc.outcome != "applied" {
				assert.Empty(t, a.writes(), "nothing is written")
				return
			}
			got := a.writes()
			if len(got) > 1 {
				slices.Sort(got[:len(got)-1])
			}
			assert.Equal(t, []string{"PUT publicIPAddresses/egress-a-pip", "PUT publicIPAddresses/egress-a-pip-2",
				"PUT publicIPPrefixes/egress-a-prefix-31", "PUT " + egressApplyTestNAT}, got, "the base Public IP gets the tag")
			a.assertMarkedOnce(t)
		})
	}
}

// TestApplyEgressConfig_NATGatewayUsedByAnotherService: the NAT Gateway of an identity is the controller's only while
// no other Service Gateway service uses it. While one does, an apply writes nothing, whether the NAT Gateway satisfies
// the configuration or not, and ends with a plain error, never a rejection; once the other service no longer uses it,
// the same apply goes through. The identity's own registration is no other use, and a NAT Gateway that does not exist
// yet has no user.
func TestApplyEgressConfig_NATGatewayUsedByAnotherService(t *testing.T) {
	natID := natGatewayID(egressApplyTestID, testConfig())
	two := egressApplyTestConfig(t, false, map[string]string{consts.PodAnnotationServiceEgressGatewayPublicIPCount: "2"})
	for _, tc := range []struct {
		name string
		cfg  EgressConfig
		// want are the writes once no other service uses the NAT Gateway.
		want []string
	}{
		{name: "a configuration that needs writes", cfg: two, want: []string{"PUT publicIPAddresses/egress-a-pip-2", "PUT " + egressApplyTestNAT}},
		{name: "a configuration the NAT Gateway satisfies", cfg: egressApplyTestDefault(false)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dt, a := newApplyARM(t)
			a.seedState(t, egressApplyTestApplied(t, egressApplyTestDefault(false), false))
			a.register(egressApplyTestID, natID)
			a.register("default-outbound", strings.ToUpper(natID))

			result := dt.applyEgressConfig(context.Background(), egressApplyTestID, tc.cfg, false)

			assert.Equal(t, "error", egressApplyOutcome(t, result), "result: %+v", result)
			assert.Contains(t, egressApplyReason(result), `is used by Service Gateway service "default-outbound"`)
			assert.Nil(t, result.NAT)
			assert.Empty(t, a.writes(), "nothing is written")
			a.assertMarkedOnce(t)

			a.update(func(f *fakeARM) { delete(f.services, "default-outbound") })
			a.resetRequests()
			result = dt.applyEgressConfig(context.Background(), egressApplyTestID, tc.cfg, false)

			if !assert.Equal(t, "applied", egressApplyOutcome(t, result), "result: %+v", result) {
				t.FailNow()
			}
			assert.ElementsMatch(t, tc.want, a.writes())
			if assert.Len(t, result.Services, 1, "the services as read, for the link repair") {
				assert.Equal(t, egressApplyTestID, derefString(result.Services[0].Name))
			}
		})
	}

	t.Run("a NAT Gateway that does not exist yet has no other user", func(t *testing.T) {
		dt, a := newApplyARM(t)
		a.register("default-outbound", natID)

		result := dt.applyEgressConfig(context.Background(), egressApplyTestID, egressApplyTestDefault(false), false)

		assert.Equal(t, "applied", egressApplyOutcome(t, result), "result: %+v", result)
		assert.Equal(t, []string{"PUT publicIPAddresses/egress-a-pip", "PUT " + egressApplyTestNAT}, a.writes())
	})
}

// TestApplyEgressConfig_SixteenAndSixteen: 16 addresses per family with managed and user prefixes. The reads and the
// writes of the managed resources run at most egressPublicIPConcurrency at a time, the NAT Gateway is written after
// all of them, and a phase waits for every call and reports the outcome of all: commit unknown wins over a refusal.
func TestApplyEgressConfig_SixteenAndSixteen(t *testing.T) {
	userPrefix, userPrefixV6 := egressApplyTestUserPrefixID("user-prefix"), egressApplyTestUserPrefixID("user-prefix-v6")
	cfg := egressApplyTestConfig(t, true, map[string]string{
		consts.PodAnnotationServiceEgressGatewayPublicIPCount:            "12",
		consts.PodAnnotationServiceEgressGatewayPublicIPPrefixLength:     "31",
		consts.PodAnnotationServiceEgressGatewayPublicIPPrefixIDs:        userPrefix,
		consts.PodAnnotationServiceEgressGatewayPublicIPCountIPv6:        "12",
		consts.PodAnnotationServiceEgressGatewayPublicIPPrefixLengthIPv6: "127",
		consts.PodAnnotationServiceEgressGatewayPublicIPPrefixIDsIPv6:    userPrefixV6,
	})
	managed := desiredManagedNames(egressApplyTestID, cfg)
	if !assert.Len(t, managed, 26) {
		t.FailNow()
	}
	for _, tc := range []struct {
		name    string
		faults  map[string]http.HandlerFunc
		outcome string
	}{
		{name: "applied", outcome: "applied"},
		{name: "a refusal and a commit-unknown write", outcome: "unknown", faults: map[string]http.HandlerFunc{
			publicIPAddressID("egress-a-pip-3", testConfig()):     armReplyError(http.StatusConflict, "AnotherOperationInProgress"),
			publicIPAddressID("egress-a-pip-v6-12", testConfig()): armReplyError(http.StatusInternalServerError, "InternalServerError"),
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dt, a := newApplyARM(t)
			a.seed(t, userPrefix, egressApplyTestUserPrefix(userPrefix, false, 31))
			a.seed(t, userPrefixV6, egressApplyTestUserPrefix(userPrefixV6, true, 127))
			a.delay = 20 * time.Millisecond
			for id, h := range tc.faults {
				a.failEvery(http.MethodPut, id, h)
			}

			result := dt.applyEgressConfig(context.Background(), egressApplyTestID, cfg, true)

			if !assert.Equal(t, tc.outcome, egressApplyOutcome(t, result), "result: %+v", result) {
				t.FailNow()
			}
			for _, name := range managed {
				kind := "publicIPAddresses/"
				if strings.Contains(name, "-prefix-") {
					kind = "publicIPPrefixes/"
				}
				assert.Equal(t, 1, a.count(http.MethodGet, kind+name), "%s read once", name)
				assert.Equal(t, 1, a.count(http.MethodPut, kind+name), "%s written once", name)
			}
			concurrency := getEgressPublicIPConcurrency()
			assert.LessOrEqual(t, a.peakInFlight(http.MethodGet), concurrency, "reads in flight")
			assert.LessOrEqual(t, a.peakInFlight(http.MethodPut), concurrency, "writes in flight")
			assert.Greater(t, a.peakInFlight(http.MethodPut), 1, "the writes ran concurrently")
			a.assertReadsBeforeWrites(t)
			a.assertMarkedOnce(t)
			if tc.outcome != "applied" {
				assert.Zero(t, a.count(http.MethodPut, egressApplyTestNAT), "no NAT Gateway PUT after a commit-unknown write")
				return
			}
			var nat armnetwork.NatGateway
			a.stored(t, natGatewayID(egressApplyTestID, testConfig()), &nat)
			assert.Len(t, nat.Properties.PublicIPAddresses, 12)
			assert.Len(t, nat.Properties.PublicIPAddressesV6, 12)
			assert.Len(t, nat.Properties.PublicIPPrefixes, 2)
			assert.Len(t, nat.Properties.PublicIPPrefixesV6, 2)
		})
	}
}

// TestApplyEgressConfig_WriteFailures: how a write that does not succeed ends the apply. Commit unknown wins and
// stops the run before the NAT Gateway PUT; a refusal is a retryable rejection for a quota, 403 and 404, a terminal
// rejection for any other 400 and a plain error otherwise (409, 412).
func TestApplyEgressConfig_WriteFailures(t *testing.T) {
	cfg := egressApplyTestConfig(t, false, map[string]string{
		consts.PodAnnotationServiceEgressGatewayPublicIPCount:        "2",
		consts.PodAnnotationServiceEgressGatewayPublicIPPrefixLength: "31",
	})
	pip2, prefix := publicIPAddressID("egress-a-pip-2", testConfig()), publicIPPrefixID("egress-a-prefix-31", testConfig())
	nat := natGatewayID(egressApplyTestID, testConfig())
	staged := []string{"PUT publicIPAddresses/egress-a-pip-2", "PUT publicIPPrefixes/egress-a-prefix-31"}
	all := append(slices.Clone(staged), "PUT "+egressApplyTestNAT)
	refuse := armReplyError
	for _, tc := range []struct {
		name    string
		faults  map[string]http.HandlerFunc
		outcome string
		reason  string
		writes  []string
	}{
		{name: "Public IP quota", faults: map[string]http.HandlerFunc{pip2: refuse(http.StatusBadRequest, "PublicIPCountLimitReached")}, outcome: "retryable",
			reason: `Azure refused Public IP "egress-a-pip-2": HTTP 400 PublicIPCountLimitReached: PublicIPCountLimitReached from the fake ARM`, writes: staged},
		{name: "prefix quota", faults: map[string]http.HandlerFunc{prefix: refuse(http.StatusBadRequest, "PublicIPPrefixCountLimitReached")}, outcome: "retryable",
			reason: `Azure refused Public IP prefix "egress-a-prefix-31": HTTP 400 PublicIPPrefixCountLimitReached`, writes: staged},
		{name: "SKU quota", faults: map[string]http.HandlerFunc{pip2: refuse(http.StatusBadRequest, "IPv4StandardSkuPublicIpCountLimitReached")}, outcome: "retryable",
			reason: "IPv4StandardSkuPublicIpCountLimitReached", writes: staged},
		{name: "quota answered with 403", faults: map[string]http.HandlerFunc{pip2: refuse(http.StatusForbidden, "QuotaExceeded")}, outcome: "retryable",
			reason: "HTTP 403 QuotaExceeded", writes: staged},
		{name: "other 400", faults: map[string]http.HandlerFunc{pip2: refuse(http.StatusBadRequest, "InvalidRequestFormat")}, outcome: "terminal",
			reason: "HTTP 400 InvalidRequestFormat", writes: staged},
		{name: "403", faults: map[string]http.HandlerFunc{pip2: refuse(http.StatusForbidden, "LinkedAuthorizationFailed")}, outcome: "retryable",
			reason: "HTTP 403 LinkedAuthorizationFailed", writes: staged},
		{name: "404", faults: map[string]http.HandlerFunc{prefix: refuse(http.StatusNotFound, "ResourceGroupNotFound")}, outcome: "retryable",
			reason: "HTTP 404 ResourceGroupNotFound", writes: staged},
		{name: "409", faults: map[string]http.HandlerFunc{pip2: refuse(http.StatusConflict, "AnotherOperationInProgress")}, outcome: "error",
			reason: "AnotherOperationInProgress", writes: staged},
		{name: "412", faults: map[string]http.HandlerFunc{prefix: refuse(http.StatusPreconditionFailed, "PreconditionFailed")}, outcome: "error",
			reason: "PreconditionFailed", writes: staged},
		{name: "500", faults: map[string]http.HandlerFunc{pip2: refuse(http.StatusInternalServerError, "InternalServerError")}, outcome: "unknown",
			reason: "InternalServerError", writes: staged},
		{name: "dropped connection", faults: map[string]http.HandlerFunc{prefix: armDropConnection}, outcome: "unknown", writes: staged},
		{name: "refused and commit unknown", faults: map[string]http.HandlerFunc{
			pip2:   refuse(http.StatusBadRequest, "InvalidRequestFormat"),
			prefix: refuse(http.StatusServiceUnavailable, "ServiceUnavailable"),
		}, outcome: "unknown", reason: "ServiceUnavailable", writes: staged},
		{name: "refused and succeeded", faults: map[string]http.HandlerFunc{pip2: refuse(http.StatusBadRequest, "InvalidRequestFormat")}, outcome: "terminal",
			reason: "InvalidRequestFormat", writes: staged},
		{name: "NAT Gateway 400", faults: map[string]http.HandlerFunc{nat: refuse(http.StatusBadRequest, "InvalidResourceReference")}, outcome: "terminal",
			reason: `Azure refused NAT Gateway "egress-a": HTTP 400 InvalidResourceReference`, writes: all},
		{name: "NAT Gateway quota", faults: map[string]http.HandlerFunc{nat: refuse(http.StatusBadRequest, "QuotaExceeded")}, outcome: "retryable",
			reason: "QuotaExceeded", writes: all},
		{name: "NAT Gateway 409", faults: map[string]http.HandlerFunc{nat: refuse(http.StatusConflict, "AnotherOperationInProgress")}, outcome: "error",
			reason: "AnotherOperationInProgress", writes: all},
		{name: "NAT Gateway 500", faults: map[string]http.HandlerFunc{nat: refuse(http.StatusInternalServerError, "InternalServerError")}, outcome: "unknown",
			writes: all},
		{name: "NAT Gateway 429", faults: map[string]http.HandlerFunc{nat: refuse(http.StatusTooManyRequests, "TooManyRequests")}, outcome: "unknown",
			writes: all},
		{name: "NAT Gateway dropped connection", faults: map[string]http.HandlerFunc{nat: armDropConnection}, outcome: "unknown", writes: all},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dt, a := newApplyARM(t)
			a.seedState(t, egressApplyTestApplied(t, egressApplyTestDefault(false), false))
			for id, h := range tc.faults {
				a.failEvery(http.MethodPut, id, h)
			}

			result := dt.applyEgressConfig(context.Background(), egressApplyTestID, cfg, false)

			if !assert.Equal(t, tc.outcome, egressApplyOutcome(t, result), "result: %+v", result) {
				t.FailNow()
			}
			assert.Contains(t, egressApplyReason(result), tc.reason)
			assert.Equal(t, egressApplySorted(tc.writes), egressApplySorted(a.writes()), "writes sent")
			a.assertReadsBeforeWrites(t)
			a.assertMarkedOnce(t)
			if result.Rejected != nil {
				assert.Equal(t, cfg, result.Rejected.Config)
			}
			if tc.outcome != "unknown" {
				if assert.NotNil(t, result.NAT) {
					assert.Equal(t, `W/"seed"`, derefString(result.NAT.Etag), "a refused or unsent NAT Gateway PUT leaves the NAT Gateway as read")
				}
			}
		})
	}
}

// TestApplyEgressCreation: a rejected creation configuration is followed in the same run by the default, and both
// are reported; a creation that ended commit unknown, busy or with an error is not; nor is a rejected default.
func TestApplyEgressCreation(t *testing.T) {
	def := egressApplyTestDefault(false)
	userPIP := egressApplyTestUserPIPID("user-pip")
	userOnly := egressApplyTestConfig(t, false, map[string]string{consts.PodAnnotationServiceEgressGatewayPublicIPIDs: userPIP})
	two := egressApplyTestConfig(t, false, map[string]string{consts.PodAnnotationServiceEgressGatewayPublicIPCount: "2"})
	pip, pip2 := publicIPAddressID("egress-a-pip", testConfig()), publicIPAddressID("egress-a-pip-2", testConfig())
	nat := natGatewayID(egressApplyTestID, testConfig())
	updating := func(a *fakeARM, t *testing.T, id string) {
		p := egressApplyTestUserPIP(id, false)
		p.Tags = egressIdentityTags(egressApplyTestID)
		p.Properties.ProvisioningState = to.Ptr(armnetwork.ProvisioningStateUpdating)
		a.seed(t, id, p)
	}
	for _, tc := range []struct {
		name     string
		creation EgressConfig
		setup    func(t *testing.T, a *fakeARM)
		// applied is the configuration applied; rejected the retryability of the creation's rejection, nil for none.
		applied     *EgressConfig
		rejected    *bool
		unknown     bool
		busy        bool
		err         string
		writes      []string
		natReads    int
		natWritesOK bool
	}{
		{name: "creation applied", creation: two, applied: &two, natReads: 1,
			writes: []string{"PUT publicIPAddresses/egress-a-pip", "PUT publicIPAddresses/egress-a-pip-2", "PUT " + egressApplyTestNAT}},
		{name: "creation rejected retryably by the preflight", creation: userOnly, applied: &def, rejected: to.Ptr(true), natReads: 2,
			writes: []string{"PUT publicIPAddresses/egress-a-pip", "PUT " + egressApplyTestNAT}},
		{name: "creation rejected terminally by the preflight", creation: userOnly, setup: func(t *testing.T, a *fakeARM) {
			p := egressApplyTestUserPIP(userPIP, false)
			p.SKU.Name = to.Ptr(armnetwork.PublicIPAddressSKUNameStandard)
			a.seed(t, userPIP, p)
		}, applied: &def, rejected: to.Ptr(false), natReads: 2, writes: []string{"PUT publicIPAddresses/egress-a-pip", "PUT " + egressApplyTestNAT}},
		{name: "creation refused after staging", creation: two, setup: func(_ *testing.T, a *fakeARM) {
			a.failEvery(http.MethodPut, pip2, armReplyError(http.StatusBadRequest, "PublicIPCountLimitReached"))
		}, applied: &def, rejected: to.Ptr(true), natReads: 2,
			writes: []string{"PUT publicIPAddresses/egress-a-pip", "PUT publicIPAddresses/egress-a-pip-2", "PUT " + egressApplyTestNAT}},
		{name: "creation commit unknown", creation: two, setup: func(_ *testing.T, a *fakeARM) {
			a.failEvery(http.MethodPut, pip2, armReplyError(http.StatusInternalServerError, "InternalServerError"))
		}, unknown: true, natReads: 1, writes: []string{"PUT publicIPAddresses/egress-a-pip", "PUT publicIPAddresses/egress-a-pip-2"}},
		{name: "creation busy", creation: userOnly, setup: func(t *testing.T, a *fakeARM) {
			updating(a, t, userPIP)
		}, busy: true, err: "is Updating", natReads: 1},
		{name: "creation read failed", creation: userOnly, setup: func(_ *testing.T, a *fakeARM) {
			a.failEvery(http.MethodGet, userPIP, armReplyError(http.StatusInternalServerError, "InternalServerError"))
		}, err: "InternalServerError", natReads: 1},
		{name: "creation is the default and rejected", creation: def, setup: func(_ *testing.T, a *fakeARM) {
			a.failEvery(http.MethodPut, nat, armReplyError(http.StatusBadRequest, "InvalidRequestFormat"))
		}, rejected: to.Ptr(false), natReads: 1, writes: []string{"PUT publicIPAddresses/egress-a-pip", "PUT " + egressApplyTestNAT}},
		{name: "default rejected as well", creation: userOnly, setup: func(_ *testing.T, a *fakeARM) {
			a.failEvery(http.MethodPut, nat, armReplyError(http.StatusBadRequest, "InvalidRequestFormat"))
		}, rejected: to.Ptr(true), err: "the default configuration was rejected as well", natReads: 2,
			writes: []string{"PUT publicIPAddresses/egress-a-pip", "PUT " + egressApplyTestNAT}},
		{name: "default busy", creation: userOnly, setup: func(t *testing.T, a *fakeARM) {
			updating(a, t, pip)
		}, rejected: to.Ptr(true), busy: true, err: "is Updating", natReads: 2},
		{name: "default commit unknown", creation: userOnly, setup: func(_ *testing.T, a *fakeARM) {
			a.failEvery(http.MethodPut, nat, armDropConnection)
		}, rejected: to.Ptr(true), unknown: true, natReads: 2, writes: []string{"PUT publicIPAddresses/egress-a-pip", "PUT " + egressApplyTestNAT}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dt, a := newApplyARM(t)
			if tc.setup != nil {
				tc.setup(t, a)
			}

			result := dt.applyEgressCreation(context.Background(), egressApplyTestID, tc.creation, def, false)

			assert.Equal(t, tc.applied, result.Applied, "applied")
			if tc.rejected == nil {
				assert.Nil(t, result.Rejected, "rejected")
			} else if assert.NotNil(t, result.Rejected, "rejected") {
				assert.Equal(t, tc.creation, result.Rejected.Config, "the creation configuration is the one reported rejected")
				assert.Equal(t, *tc.rejected, result.Rejected.Retryable, "retryable")
			}
			assert.Equal(t, tc.unknown, result.CommitUnknown, "commit unknown")
			assert.Equal(t, tc.busy, result.Busy, "busy")
			if tc.err == "" && !tc.unknown {
				assert.NoError(t, result.Err)
			} else {
				assert.ErrorContains(t, result.Err, tc.err)
			}
			assert.Equal(t, egressApplySorted(tc.writes), egressApplySorted(a.writes()), "writes sent")
			assert.Equal(t, tc.natReads, a.count(http.MethodGet, egressApplyTestNAT), "applies made")
			a.assertMarkedOnce(t)
			if tc.applied != nil {
				var written armnetwork.NatGateway
				a.stored(t, nat, &written)
				if assert.NotNil(t, result.NAT) {
					assert.Equal(t, derefString(written.Etag), derefString(result.NAT.Etag), "the result carries the NAT Gateway as written")
				}
				var ids []string
				for _, name := range desiredManagedNames(egressApplyTestID, *tc.applied) {
					ids = append(ids, publicIPAddressID(name, testConfig()))
				}
				assert.Empty(t, natMismatch(&written, newEgressManagedResources(egressApplyTestStoredPIPs(t, a, ids...), nil),
					egressApplyTestID, *tc.applied, false, testConfig()), "the NAT Gateway satisfies the applied configuration")
			}
		})
	}
}

// egressApplySorted returns a sorted copy of calls, for comparing the calls of concurrent phases.
func egressApplySorted(calls []string) []string {
	return slices.Sorted(slices.Values(calls))
}

func egressApplyTestStoredPIPs(t *testing.T, a *fakeARM, ids ...string) []*armnetwork.PublicIPAddress {
	t.Helper()
	var pips []*armnetwork.PublicIPAddress
	for _, id := range ids {
		var pip armnetwork.PublicIPAddress
		a.stored(t, id, &pip)
		pips = append(pips, &pip)
	}
	return pips
}

func TestEgressQuotaErrorCode(t *testing.T) {
	for _, tc := range []struct {
		code  string
		quota bool
	}{
		{"QuotaExceeded", true},
		{"quotaexceeded", true},
		{"PublicIPCountLimitReached", true},
		{"MaxPublicIpCountLimitReached", true},
		{"PublicIPPrefixCountLimitReached", true},
		{"StandardSkuPublicIpCountLimitReached", true},
		{"IPv4StandardSkuPublicIpCountLimitReached", true},
		{"InvalidRequestFormat", false},
		{"AnotherOperationInProgress", false},
		{"LinkedAuthorizationFailed", false},
		{"", false},
	} {
		assert.Equal(t, tc.quota, egressQuotaErrorCode(tc.code), "%q", tc.code)
	}
}

// TestEgressAzureError: the reason of a refusal names Azure's error code and message when Azure's answer has them.
func TestEgressAzureError(t *testing.T) {
	for _, tc := range []struct {
		name          string
		err           error
		code, message string
	}{
		{name: "not an Azure answer", err: errors.New("transport")},
		{name: "code only", err: classifyEgressWrite(nil).Err},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, message := egressAzureError(tc.err)
			assert.Equal(t, tc.code, code)
			assert.Equal(t, tc.message, message)
		})
	}
	dt, a := newApplyARM(t)
	a.failEvery(http.MethodPut, publicIPAddressID("egress-a-pip", testConfig()), armReplyError(http.StatusBadRequest, "InvalidRequestFormat"))
	_, err := dt.putEgressPublicIP(context.Background(), armnetwork.PublicIPAddress{Name: to.Ptr("egress-a-pip"), Location: to.Ptr("eastus")})
	code, message := egressAzureError(err)
	assert.Equal(t, "InvalidRequestFormat", code)
	assert.Equal(t, "InvalidRequestFormat from the fake ARM", message)
}
