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
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork/v12"
	"github.com/stretchr/testify/assert"

	"sigs.k8s.io/cloud-provider-azure/pkg/consts"
	"sigs.k8s.io/cloud-provider-azure/pkg/log"
)

const (
	egressCleanupTestID    = "egress-a"
	egressCleanupTestBYORG = "vnet-rg"
)

// newCleanupARM returns the fake ARM of a tracker that tracks the operation of the test identity.
func newCleanupARM(t *testing.T) *fakeARM {
	t.Helper()
	return newFakeARM(t, &DiffTracker{
		config:            testConfig(),
		logger:            log.Noop(),
		pendingServiceOps: map[string]*ServiceOperationState{egressCleanupTestID: {ServiceUID: egressCleanupTestID}},
	})
}

// cleanupReservedReads returns the reads of every reserved name of the test identity in the served families, sorted.
func cleanupReservedReads(ipv6 bool) []string {
	var calls []string
	for _, m := range reservedManaged(ipv6) {
		kind := "publicIPAddresses/"
		if m.Kind == egressManagedPrefix {
			kind = "publicIPPrefixes/"
		}
		calls = append(calls, http.MethodGet+" "+kind+m.name(egressCleanupTestID))
	}
	slices.Sort(calls)
	return calls
}

func cleanupDeletes(names ...string) []string {
	var calls []string
	for _, name := range names {
		kind := "publicIPAddresses/"
		if strings.Contains(name, publicIPPrefixNameSuffix) {
			kind = "publicIPPrefixes/"
		}
		calls = append(calls, http.MethodDelete+" "+kind+name)
	}
	slices.Sort(calls)
	return calls
}

func egressCleanupConfig(t *testing.T, ipv6 bool, ann map[string]string) EgressConfig {
	t.Helper()
	cfg, _, err := parseEgressConfig(ann, egressParseContext{WantsIPv6: ipv6, NetworkSubscription: "sub", NodeResourceGroup: "rg"})
	if err != nil {
		t.Fatalf("parsing %v: %v", ann, err)
	}
	return cfg
}

func egressCleanupPIP(name string, tags map[string]*string, state armnetwork.ProvisioningState) armnetwork.PublicIPAddress {
	return armnetwork.PublicIPAddress{
		Name:       to.Ptr(name),
		Location:   to.Ptr(testConfig().Location),
		SKU:        &armnetwork.PublicIPAddressSKU{Name: to.Ptr(armnetwork.PublicIPAddressSKUNameStandardV2)},
		Tags:       tags,
		Properties: &armnetwork.PublicIPAddressPropertiesFormat{ProvisioningState: to.Ptr(state)},
	}
}

func egressCleanupPrefix(name string, tags map[string]*string, state armnetwork.ProvisioningState) armnetwork.PublicIPPrefix {
	return armnetwork.PublicIPPrefix{
		Name:       to.Ptr(name),
		Location:   to.Ptr(testConfig().Location),
		SKU:        &armnetwork.PublicIPPrefixSKU{Name: to.Ptr(armnetwork.PublicIPPrefixSKUNameStandardV2)},
		Tags:       tags,
		Properties: &armnetwork.PublicIPPrefixPropertiesFormat{ProvisioningState: to.Ptr(state)},
	}
}

// cleanupOwnTag is the ownership tag of the test identity; cleanupOtherTag that of another identity.
func cleanupOwnTag() map[string]*string {
	return map[string]*string{EgressIdentityTagKey: to.Ptr(egressCleanupTestID)}
}

func cleanupOtherTag() map[string]*string {
	return map[string]*string{EgressIdentityTagKey: to.Ptr("egress-b")}
}

const (
	cleanupCallGetNAT      = "GET natGateways/egress-a"
	cleanupCallPutNAT      = "PUT natGateways/egress-a"
	cleanupCallDeleteNAT   = "DELETE natGateways/egress-a"
	cleanupCallGetServices = "GET serviceGateways/sgw/services"
	cleanupCallRegister    = "POST serviceGateways/sgw/updateServices"
)

func TestSweepEgressReserved(t *testing.T) {
	ctx := context.Background()
	rg := testConfig().ResourceGroup
	succeeded := armnetwork.ProvisioningStateSucceeded
	defaultV4 := defaultEgressConfig(egressParseContext{})
	two := egressCleanupConfig(t, false, map[string]string{consts.PodAnnotationServiceEgressGatewayPublicIPCount: "2"})

	t.Run("deletes the unreferenced names of the controller and nothing else", func(t *testing.T) {
		c := newCleanupARM(t)
		c.seedApplied(two, false)
		// The NAT Gateway also references a user Public IP of a reserved name, in another resource group.
		c.update(func(c *fakeARM) {
			nat := c.natLocked(rg, egressCleanupTestID)
			nat.Properties.PublicIPAddresses = append(nat.Properties.PublicIPAddresses,
				&armnetwork.SubResource{ID: to.Ptr(fakeARMID("user-rg", "publicIPAddresses", "egress-a-pip-8"))})
		})
		c.seedPublicIP(egressCleanupPIP("egress-a-pip-3", cleanupOwnTag(), succeeded))
		c.seedPrefix(egressCleanupPrefix("egress-a-prefix-30", cleanupOwnTag(), succeeded))
		c.seedPublicIP(egressCleanupPIP("egress-a-pip-4", nil, succeeded))
		c.seedPrefix(egressCleanupPrefix("egress-a-prefix-31", nil, succeeded))
		c.seedPublicIP(egressCleanupPIP("egress-a-pip-5", cleanupOtherTag(), succeeded))
		c.seedPublicIP(egressCleanupPIP("egress-a-pip-6", cleanupOwnTag(), armnetwork.ProvisioningStateFailed))
		c.seedPublicIP(egressCleanupPIP("egress-a-pip-7", cleanupOwnTag(), armnetwork.ProvisioningStateCanceled))
		c.seedPublicIP(egressCleanupPIP("egress-a-pip-8", cleanupOwnTag(), succeeded))
		c.seedPublicIP(egressCleanupPIP("egress-a-pip-v6", cleanupOwnTag(), succeeded))
		c.seedPrefix(egressCleanupPrefix("egress-a-prefix-v6-127", cleanupOwnTag(), succeeded))

		swept, unknown, err := c.dt.sweepEgressReserved(ctx, egressCleanupTestID, false, c.nat(rg, egressCleanupTestID))
		assert.NoError(t, err)
		assert.True(t, swept)
		assert.False(t, unknown)
		assert.Len(t, c.reservedCalls(http.MethodGet), 20)
		assert.Equal(t, cleanupReservedReads(false), c.reservedCalls(http.MethodGet), "every reserved name of the served family is read")
		assert.Equal(t, cleanupDeletes("egress-a-pip-3", "egress-a-prefix-30", "egress-a-pip-6", "egress-a-pip-7", "egress-a-pip-8"),
			c.reservedCalls(http.MethodDelete), "referenced, untagged numbered, untagged prefix, foreign and unserved names are kept")
		assert.Empty(t, c.sequence(), "no other resource is called")
		pips, prefixes := c.resourceNames()
		assert.Equal(t, []string{"egress-a-pip", "egress-a-pip-2", "egress-a-pip-4", "egress-a-pip-5", "egress-a-pip-v6"}, pips)
		assert.Equal(t, []string{"egress-a-prefix-31", "egress-a-prefix-v6-127"}, prefixes)
	})

	t.Run("both families when the NAT Gateway serves IPv6", func(t *testing.T) {
		c := newCleanupARM(t)
		c.seedApplied(defaultEgressConfig(egressParseContext{WantsIPv6: true}), true)
		c.seedPublicIP(egressCleanupPIP("egress-a-pip-v6-2", cleanupOwnTag(), succeeded))
		c.seedPrefix(egressCleanupPrefix("egress-a-prefix-v6-124", cleanupOwnTag(), succeeded))
		c.seedPublicIP(egressCleanupPIP("egress-a-pip-v6-3", nil, succeeded))

		swept, unknown, err := c.dt.sweepEgressReserved(ctx, egressCleanupTestID, true, c.nat(rg, egressCleanupTestID))
		assert.NoError(t, err)
		assert.True(t, swept && !unknown)
		assert.Len(t, c.reservedCalls(http.MethodGet), 40)
		assert.Equal(t, cleanupReservedReads(true), c.reservedCalls(http.MethodGet))
		assert.Equal(t, cleanupDeletes("egress-a-pip-v6-2", "egress-a-prefix-v6-124"), c.reservedCalls(http.MethodDelete))
	})

	t.Run("what the NAT Gateway references is kept in every list of both families", func(t *testing.T) {
		c := newCleanupARM(t)
		c.seedApplied(egressCleanupConfig(t, true, map[string]string{
			consts.PodAnnotationServiceEgressGatewayPublicIPCount:            "2",
			consts.PodAnnotationServiceEgressGatewayPublicIPPrefixLength:     "30",
			consts.PodAnnotationServiceEgressGatewayPublicIPCountIPv6:        "1",
			consts.PodAnnotationServiceEgressGatewayPublicIPPrefixLengthIPv6: "127",
		}), true)
		c.seedPrefix(egressCleanupPrefix("egress-a-prefix-31", cleanupOwnTag(), succeeded))
		c.seedPrefix(egressCleanupPrefix("egress-a-prefix-v6-126", cleanupOwnTag(), succeeded))
		c.seedPublicIP(egressCleanupPIP("egress-a-pip-v6-2", cleanupOwnTag(), succeeded))

		swept, unknown, err := c.dt.sweepEgressReserved(ctx, egressCleanupTestID, true, c.nat(rg, egressCleanupTestID))
		assert.NoError(t, err)
		assert.True(t, swept && !unknown)
		assert.Equal(t, cleanupDeletes("egress-a-prefix-31", "egress-a-prefix-v6-126", "egress-a-pip-v6-2"), c.reservedCalls(http.MethodDelete))
		pips, prefixes := c.resourceNames()
		assert.Equal(t, []string{"egress-a-pip", "egress-a-pip-2", "egress-a-pip-v6"}, pips)
		assert.Equal(t, []string{"egress-a-prefix-30", "egress-a-prefix-v6-127"}, prefixes)
	})

	t.Run("every name of the controller once the NAT Gateway is gone", func(t *testing.T) {
		c := newCleanupARM(t)
		c.seedApplied(defaultEgressConfig(egressParseContext{WantsIPv6: true}), true)
		c.update(func(c *fakeARM) { delete(c.nats, fakeARMKey(rg, egressCleanupTestID)) })
		c.seedPublicIP(egressCleanupPIP("egress-a-pip-2", cleanupOwnTag(), succeeded))
		c.seedPublicIP(egressCleanupPIP("egress-a-pip-3", nil, succeeded))
		c.seedPrefix(egressCleanupPrefix("egress-a-prefix-29", nil, succeeded))

		swept, unknown, err := c.dt.sweepEgressReserved(ctx, egressCleanupTestID, true, nil)
		assert.NoError(t, err)
		assert.True(t, swept && !unknown)
		assert.Equal(t, cleanupDeletes("egress-a-pip", "egress-a-pip-v6", "egress-a-pip-2"), c.reservedCalls(http.MethodDelete))
		pips, prefixes := c.resourceNames()
		assert.Equal(t, []string{"egress-a-pip-3"}, pips)
		assert.Equal(t, []string{"egress-a-prefix-29"}, prefixes)
	})

	t.Run("legacy base Public IPs are the controller's by their name only", func(t *testing.T) {
		c := newCleanupARM(t)
		c.seedApplied(defaultV4, false)
		c.update(func(c *fakeARM) {
			delete(c.natLocked(rg, egressCleanupTestID).Tags, EgressIdentityTagKey)
			delete(c.pips[fakeARMKey(rg, "egress-a-pip")].Tags, EgressIdentityTagKey)
		})
		c.seedPublicIP(egressCleanupPIP("egress-a-pip-2", nil, succeeded))
		nat := c.nat(rg, egressCleanupTestID)
		assert.True(t, legacyEgressNAT(nat, egressCleanupTestID, false, c.dt.config), "control: a NAT Gateway created before tagging")

		swept, _, err := c.dt.sweepEgressReserved(ctx, egressCleanupTestID, false, nat)
		assert.NoError(t, err)
		assert.True(t, swept)
		assert.Empty(t, c.reservedCalls(http.MethodDelete), "the referenced base Public IP and an untagged numbered one are kept")

		c.update(func(c *fakeARM) { delete(c.nats, fakeARMKey(rg, egressCleanupTestID)) })
		swept, _, err = c.dt.sweepEgressReserved(ctx, egressCleanupTestID, false, nil)
		assert.NoError(t, err)
		assert.True(t, swept)
		assert.Equal(t, cleanupDeletes("egress-a-pip"), c.reservedCalls(http.MethodDelete), "the untagged base Public IP is deleted, the numbered one kept")
	})

	t.Run("an unsettled name is kept and the sweep is incomplete", func(t *testing.T) {
		for _, state := range []armnetwork.ProvisioningState{
			armnetwork.ProvisioningStateCreating, armnetwork.ProvisioningStateUpdating, armnetwork.ProvisioningStateDeleting,
		} {
			c := newCleanupARM(t)
			c.seedApplied(defaultV4, false)
			c.seedPublicIP(egressCleanupPIP("egress-a-pip-2", cleanupOwnTag(), state))
			c.seedPublicIP(egressCleanupPIP("egress-a-pip-3", cleanupOwnTag(), succeeded))

			swept, unknown, err := c.dt.sweepEgressReserved(ctx, egressCleanupTestID, false, c.nat(rg, egressCleanupTestID))
			assert.NoError(t, err, state)
			assert.False(t, swept, state)
			assert.False(t, unknown, state)
			assert.Equal(t, cleanupDeletes("egress-a-pip-3"), c.reservedCalls(http.MethodDelete), state)
		}
	})

	t.Run("an unsettled name that is not the controller's leaves the sweep incomplete too", func(t *testing.T) {
		c := newCleanupARM(t)
		c.seedApplied(defaultV4, false)
		c.seedPrefix(egressCleanupPrefix("egress-a-prefix-28", nil, armnetwork.ProvisioningStateCreating))

		swept, _, err := c.dt.sweepEgressReserved(ctx, egressCleanupTestID, false, c.nat(rg, egressCleanupTestID))
		assert.NoError(t, err)
		assert.False(t, swept, "it may be a late create of the controller, tagged once it settles")
		assert.Empty(t, c.writes())
	})

	t.Run("a failed read leaves the sweep incomplete", func(t *testing.T) {
		c := newCleanupARM(t)
		c.seedApplied(defaultV4, false)
		c.seedPublicIP(egressCleanupPIP("egress-a-pip-3", cleanupOwnTag(), succeeded))
		c.fault("GET publicIPAddresses/egress-a-pip-9", armReplyError(http.StatusForbidden, "AuthorizationFailed"))

		swept, unknown, err := c.dt.sweepEgressReserved(ctx, egressCleanupTestID, false, c.nat(rg, egressCleanupTestID))
		assert.ErrorContains(t, err, "egress-a-pip-9")
		assert.False(t, swept)
		assert.False(t, unknown, "a failed read is not a write")
		assert.Equal(t, cleanupDeletes("egress-a-pip-3"), c.reservedCalls(http.MethodDelete))
		pips, _ := c.resourceNames()
		assert.NotContains(t, pips, "egress-a-pip-3", "the other leftovers are still deleted")
	})

	for _, tc := range []struct {
		name                string
		answer              http.HandlerFunc
		swept, unknown, err bool
	}{
		{name: "a refused DELETE leaves the sweep incomplete", answer: armReplyError(http.StatusConflict, "Conflict"), err: true},
		{name: "a DELETE refused with 400 while the read shows no use leaves the sweep incomplete",
			answer: armReplyError(http.StatusBadRequest, "PublicIPAddressInUse"), err: true},
		{name: "a DELETE refused for its precondition leaves the sweep incomplete", answer: armReplyError(http.StatusPreconditionFailed, "PreconditionFailed"),
			err: true},
		{name: "a DELETE refused for lack of access leaves the sweep incomplete", answer: armReplyError(http.StatusForbidden, "ScopeLocked"), err: true},
		{name: "a DELETE with an unknown outcome is reported", answer: armReplyError(http.StatusInternalServerError, "InternalServerError"),
			unknown: true, err: true},
		{name: "a DELETE whose connection drops is reported unknown", answer: armDropConnection, unknown: true, err: true},
		{name: "a DELETE answered 404 found the name gone", answer: armReplyError(http.StatusNotFound, "NotFound"), swept: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newCleanupARM(t)
			c.seedApplied(defaultV4, false)
			c.seedPublicIP(egressCleanupPIP("egress-a-pip-3", cleanupOwnTag(), succeeded))
			c.fault("DELETE publicIPAddresses/egress-a-pip-3", tc.answer)

			swept, unknown, err := c.dt.sweepEgressReserved(ctx, egressCleanupTestID, false, c.nat(rg, egressCleanupTestID))
			assert.Equal(t, tc.err, err != nil, "err: %v", err)
			assert.Equal(t, tc.swept, swept)
			assert.Equal(t, tc.unknown, unknown)
			assert.Len(t, c.requestsOf("DELETE publicIPAddresses/egress-a-pip-3"), 1, "a single attempt")
		})
	}

	t.Run("leftovers in use elsewhere are kept and the sweep is complete; other leftovers are still deleted", func(t *testing.T) {
		c := newCleanupARM(t)
		c.seedApplied(defaultV4, false)
		userNAT := &armnetwork.NatGateway{ID: to.Ptr(fakeARMID("user-rg", "natGateways", "user-nat"))}
		pip3 := egressCleanupPIP("egress-a-pip-3", cleanupOwnTag(), succeeded)
		pip3.Properties.NatGateway = userNAT
		c.seedPublicIP(pip3)
		prefix30 := egressCleanupPrefix("egress-a-prefix-30", cleanupOwnTag(), succeeded)
		prefix30.Properties.NatGateway = userNAT
		c.seedPrefix(prefix30)
		c.seedPublicIP(egressCleanupPIP("egress-a-pip-4", cleanupOwnTag(), succeeded))
		// A user attached two of the leftovers to a NAT Gateway of their own: Azure refuses to delete them (400).
		c.seedNAT("user-rg", armnetwork.NatGateway{Name: to.Ptr("user-nat"), Properties: &armnetwork.NatGatewayPropertiesFormat{
			PublicIPAddresses: []*armnetwork.SubResource{{ID: to.Ptr(publicIPAddressID("egress-a-pip-3", testConfig()))}},
			PublicIPPrefixes:  []*armnetwork.SubResource{{ID: to.Ptr(publicIPPrefixID("egress-a-prefix-30", testConfig()))}},
		}})

		swept, unknown, err := c.dt.sweepEgressReserved(ctx, egressCleanupTestID, false, c.nat(rg, egressCleanupTestID))
		assert.NoError(t, err)
		assert.True(t, swept, "a leftover Azure refuses to delete because it is in use is not retried")
		assert.False(t, unknown)
		assert.Equal(t, cleanupDeletes("egress-a-pip-3", "egress-a-pip-4", "egress-a-prefix-30"), c.reservedCalls(http.MethodDelete),
			"every leftover gets a single DELETE")
		pips, prefixes := c.resourceNames()
		assert.Equal(t, []string{"egress-a-pip", "egress-a-pip-3"}, pips, "the leftover that is not in use is deleted")
		assert.Equal(t, []string{"egress-a-prefix-30"}, prefixes)
	})

	t.Run("a DELETE refused with 400 is done only when the read shows a use other than the identity's NAT Gateway", func(t *testing.T) {
		ownNAT := strings.ToUpper(natGatewayID(egressCleanupTestID, testConfig()))
		userNAT := fakeARMID("user-rg", "natGateways", "user-nat")
		frontend := fakeARMID("user-rg", "loadBalancers", "lb") + "/frontendIPConfigurations/fe"
		nic := fakeARMID("user-rg", "networkInterfaces", "nic") + "/ipConfigurations/ipconfig1"
		for _, tc := range []struct {
			name   string
			pip    func(p *armnetwork.PublicIPAddressPropertiesFormat)
			prefix func(p *armnetwork.PublicIPPrefixPropertiesFormat)
			swept  bool
		}{
			{name: "Public IP attached to another NAT Gateway", swept: true, pip: func(p *armnetwork.PublicIPAddressPropertiesFormat) {
				p.NatGateway = &armnetwork.NatGateway{ID: to.Ptr(userNAT)}
			}},
			{name: "Public IP of a network interface", swept: true, pip: func(p *armnetwork.PublicIPAddressPropertiesFormat) {
				p.IPConfiguration = &armnetwork.IPConfiguration{ID: to.Ptr(nic)}
			}},
			{name: "Public IP of a load balancer", swept: true, pip: func(p *armnetwork.PublicIPAddressPropertiesFormat) {
				p.IPConfiguration = &armnetwork.IPConfiguration{ID: to.Ptr(frontend)}
			}},
			{name: "prefix attached to another NAT Gateway", swept: true, prefix: func(p *armnetwork.PublicIPPrefixPropertiesFormat) {
				p.NatGateway = &armnetwork.NatGateway{ID: to.Ptr(userNAT)}
			}},
			{name: "prefix of a load balancer frontend", swept: true, prefix: func(p *armnetwork.PublicIPPrefixPropertiesFormat) {
				p.LoadBalancerFrontendIPConfiguration = &armnetwork.SubResource{ID: to.Ptr(frontend)}
			}},
			{name: "prefix with Public IPs allocated from it", swept: true, prefix: func(p *armnetwork.PublicIPPrefixPropertiesFormat) {
				p.PublicIPAddresses = []*armnetwork.ReferencedPublicIPAddress{{ID: to.Ptr(fakeARMID("user-rg", "publicIPAddresses", "from-prefix"))}}
			}},
			{name: "Public IP still naming the identity's NAT Gateway", pip: func(p *armnetwork.PublicIPAddressPropertiesFormat) {
				p.NatGateway = &armnetwork.NatGateway{ID: to.Ptr(ownNAT)}
			}},
			{name: "prefix still naming the identity's NAT Gateway", prefix: func(p *armnetwork.PublicIPPrefixPropertiesFormat) {
				p.NatGateway = &armnetwork.NatGateway{ID: to.Ptr(ownNAT)}
			}},
			{name: "Public IP without a known use", pip: func(*armnetwork.PublicIPAddressPropertiesFormat) {}},
			{name: "prefix without a known use", prefix: func(*armnetwork.PublicIPPrefixPropertiesFormat) {}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				c := newCleanupARM(t)
				c.seedApplied(defaultV4, false)
				call := "DELETE publicIPAddresses/egress-a-pip-3"
				if tc.pip != nil {
					pip := egressCleanupPIP("egress-a-pip-3", cleanupOwnTag(), succeeded)
					tc.pip(pip.Properties)
					c.seedPublicIP(pip)
				} else {
					call = "DELETE publicIPPrefixes/egress-a-prefix-30"
					prefix := egressCleanupPrefix("egress-a-prefix-30", cleanupOwnTag(), succeeded)
					tc.prefix(prefix.Properties)
					c.seedPrefix(prefix)
				}
				c.fault(call, armReplyError(http.StatusBadRequest, "InUseCannotBeDeleted"))

				swept, unknown, err := c.dt.sweepEgressReserved(ctx, egressCleanupTestID, false, c.nat(rg, egressCleanupTestID))
				assert.Equal(t, tc.swept, swept)
				assert.Equal(t, !tc.swept, err != nil, "err: %v", err)
				assert.False(t, unknown, "a refused DELETE is conclusive")
				assert.Len(t, c.requestsOf(call), 1, "a single attempt")
				pips, prefixes := c.resourceNames()
				assert.Len(t, append(pips, prefixes...), 2, "the leftover is kept")
			})
		}
	})

	t.Run("nothing to sweep: no write and no mutation noted", func(t *testing.T) {
		c := newCleanupARM(t)
		c.seedApplied(two, false)

		swept, unknown, err := c.dt.sweepEgressReserved(ctx, egressCleanupTestID, false, c.nat(rg, egressCleanupTestID))
		assert.NoError(t, err)
		assert.True(t, swept)
		assert.False(t, unknown)
		assert.Empty(t, c.writes())
		assert.False(t, c.mutationNoted())
	})
}

func TestRepairEgressLinks(t *testing.T) {
	ctx := context.Background()
	cfg := testConfig()
	rg := cfg.ResourceGroup
	sgw := cfg.ServiceGatewayResourceID()
	natID := natGatewayID(egressCleanupTestID, cfg)
	byoNATID := fakeARMID(egressCleanupTestBYORG, "natGateways", egressCleanupTestID)
	unlink := func(c *fakeARM) { c.natLocked(rg, egressCleanupTestID).Properties.ServiceGateway = nil }
	unregister := func(c *fakeARM) { delete(c.services, egressCleanupTestID) }
	registerWith := func(natID string) func(c *fakeARM) {
		return func(c *fakeARM) {
			c.services[egressCleanupTestID].Properties.PublicNatGatewayID = to.Ptr(natID)
			if natID == "" {
				c.services[egressCleanupTestID].Properties.PublicNatGatewayID = nil
			}
		}
	}
	fault := func(call string, answer http.HandlerFunc) func(c *fakeARM) {
		return func(c *fakeARM) { c.faults[call] = append(c.faults[call], answer) }
	}
	both := func(setups ...func(c *fakeARM)) func(c *fakeARM) {
		return func(c *fakeARM) {
			for _, setup := range setups {
				setup(c)
			}
		}
	}

	for _, tc := range []struct {
		name                       string
		setup                      func(c *fakeARM)
		calls                      []string
		err, unknown               bool
		wantLinked, wantRegistered bool
	}{
		{name: "both sides linked: no write", wantLinked: true, wantRegistered: true},
		{name: "NAT Gateway not linked", setup: unlink, calls: []string{cleanupCallPutNAT}, wantLinked: true, wantRegistered: true},
		{name: "NAT Gateway linked to another Service Gateway", setup: func(c *fakeARM) {
			c.natLocked(rg, egressCleanupTestID).Properties.ServiceGateway.ID = to.Ptr(fakeARMID(rg, "serviceGateways", "other"))
		}, calls: []string{cleanupCallPutNAT}, wantLinked: true, wantRegistered: true},
		{name: "NAT Gateway linked with the Service Gateway ID in another case: no write", setup: func(c *fakeARM) {
			c.natLocked(rg, egressCleanupTestID).Properties.ServiceGateway.ID = to.Ptr(strings.ToUpper(sgw))
		}, wantLinked: true, wantRegistered: true},
		{name: "identity not registered", setup: unregister, calls: []string{cleanupCallRegister}, wantLinked: true, wantRegistered: true},
		{name: "identity registered without its NAT Gateway (interrupted unlink)", setup: registerWith(""),
			calls: []string{cleanupCallRegister}, wantLinked: true, wantRegistered: true},
		{name: "identity registered with another NAT Gateway", setup: registerWith(byoNATID),
			calls: []string{cleanupCallRegister}, wantLinked: true, wantRegistered: true},
		{name: "identity registered with its NAT Gateway spelled in another case", setup: registerWith(strings.ToUpper(natID)),
			wantLinked: true, wantRegistered: true},
		{name: "both sides lost", setup: both(unlink, unregister),
			calls: []string{cleanupCallPutNAT, cleanupCallRegister}, wantLinked: true, wantRegistered: true},
		{name: "NAT Gateway used by another service: nothing written", setup: both(unlink, unregister, func(c *fakeARM) {
			c.services["default-outbound"] = &armnetwork.ServiceGatewayService{Name: to.Ptr("default-outbound"),
				Properties: &armnetwork.ServiceGatewayServicePropertiesFormat{PublicNatGatewayID: to.Ptr(strings.ToUpper(natID))}}
		}), err: true},
		{name: "link refused: the repair stops", setup: both(unlink, unregister, fault(cleanupCallPutNAT, armReplyError(http.StatusBadRequest, "InvalidRequest"))),
			calls: []string{cleanupCallPutNAT}, err: true},
		{name: "link with an unknown outcome: the repair stops", setup: both(unlink, unregister,
			fault(cleanupCallPutNAT, armReplyError(http.StatusInternalServerError, "InternalServerError"))),
			calls: []string{cleanupCallPutNAT}, err: true, unknown: true},
		{name: "registration refused", setup: both(unregister, fault(cleanupCallRegister, armReplyError(http.StatusBadRequest, "InvalidRequest"))),
			calls: []string{cleanupCallRegister}, err: true, wantLinked: true},
		{name: "registration with an unknown outcome", setup: both(unregister, fault(cleanupCallRegister, armDropConnection)),
			calls: []string{cleanupCallRegister}, err: true, unknown: true, wantLinked: true},
		{name: "link written, registration refused", setup: both(unlink, unregister, fault(cleanupCallRegister, armReplyError(http.StatusConflict, "Conflict"))),
			calls: []string{cleanupCallPutNAT, cleanupCallRegister}, err: true, wantLinked: true},
		{name: "Service Gateway gone", setup: both(unregister, func(c *fakeARM) { c.noServiceGateway = true }),
			calls: []string{cleanupCallRegister}, err: true, wantLinked: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newCleanupARM(t)
			c.seedApplied(defaultEgressConfig(egressParseContext{}), false)
			c.register(egressCleanupTestID, natID)
			if tc.setup != nil {
				c.update(tc.setup)
			}
			read := c.nat(rg, egressCleanupTestID)
			services, err := c.dt.getEgressServices(ctx)
			if err != nil {
				t.Fatal(err)
			}
			c.resetRequests()

			result := c.dt.repairEgressLinks(ctx, egressCleanupTestID, read, services)
			assert.Equal(t, tc.calls, c.calls(), "nothing is read")
			assert.Equal(t, tc.err, result.Err != nil, "err: %v", result.Err)
			assert.Equal(t, tc.unknown, result.CommitUnknown)
			assert.Equal(t, len(c.writes()) > 0, c.mutationNoted(), "a mutation is noted exactly when a write is sent")

			after := c.nat(rg, egressCleanupTestID)
			assert.Equal(t, tc.wantLinked, strings.EqualFold(natGatewayServiceGatewayID(after), sgw), "NAT Gateway linked afterwards")
			c.mu.Lock()
			registration := c.services[egressCleanupTestID]
			c.mu.Unlock()
			registered := registration != nil && registration.Properties != nil &&
				strings.EqualFold(derefString(registration.Properties.PublicNatGatewayID), natID)
			assert.Equal(t, tc.wantRegistered, registered, "registered with the NAT Gateway afterwards")

			for _, put := range c.requestsOf(cleanupCallPutNAT) {
				assert.Empty(t, put.IfMatch, "the link PUT carries no precondition")
				assert.JSONEq(t, string(fakeARMJSON(t, natLinkRequest(read, cfg))), string(put.Body), "the link PUT changes nothing but the link")
			}
			if slices.Contains(tc.calls, cleanupCallPutNAT) && !tc.err {
				assert.Equal(t, sgw, natGatewayServiceGatewayID(result.NAT), "the NAT Gateway as the link PUT returned it")
				assert.Equal(t, derefString(after.Etag), derefString(result.NAT.Etag))
			} else if !slices.Contains(tc.calls, cleanupCallPutNAT) {
				assert.Same(t, read, result.NAT, "the NAT Gateway as given")
			}
		})
	}

	t.Run("a legacy NAT Gateway is linked and stays untagged", func(t *testing.T) {
		c := newCleanupARM(t)
		c.seedApplied(defaultEgressConfig(egressParseContext{}), false)
		c.register(egressCleanupTestID, natID)
		c.update(func(c *fakeARM) {
			unlink(c)
			delete(c.natLocked(rg, egressCleanupTestID).Tags, EgressIdentityTagKey)
			delete(c.pips[fakeARMKey(rg, "egress-a-pip")].Tags, EgressIdentityTagKey)
		})

		services, err := c.dt.getEgressServices(ctx)
		if err != nil {
			t.Fatal(err)
		}
		result := c.dt.repairEgressLinks(ctx, egressCleanupTestID, c.nat(rg, egressCleanupTestID), services)
		assert.NoError(t, result.Err)
		assert.Equal(t, []string{cleanupCallPutNAT}, c.writes(), "the link PUT")
		after := c.nat(rg, egressCleanupTestID)
		assert.Equal(t, sgw, natGatewayServiceGatewayID(after))
		assert.NotContains(t, after.Tags, EgressIdentityTagKey)
	})

	for name, nat := range map[string]*armnetwork.NatGateway{
		"no NAT Gateway": nil,
		"an unsettled NAT Gateway": {Name: to.Ptr(egressCleanupTestID), Tags: cleanupOwnTag(), Properties: &armnetwork.NatGatewayPropertiesFormat{
			ProvisioningState: to.Ptr(armnetwork.ProvisioningStateUpdating)}},
		"a NAT Gateway the controller does not own": {Name: to.Ptr(egressCleanupTestID), Properties: &armnetwork.NatGatewayPropertiesFormat{
			ProvisioningState: to.Ptr(armnetwork.ProvisioningStateSucceeded),
			PublicIPAddresses: []*armnetwork.SubResource{{ID: to.Ptr(fakeARMID("user-rg", "publicIPAddresses", "user-pip"))}}}},
		"a NAT Gateway of another identity": {Name: to.Ptr(egressCleanupTestID), Tags: cleanupOtherTag(), Properties: &armnetwork.NatGatewayPropertiesFormat{
			ProvisioningState: to.Ptr(armnetwork.ProvisioningStateSucceeded)}},
	} {
		t.Run(name+" is never written", func(t *testing.T) {
			c := newCleanupARM(t)
			result := c.dt.repairEgressLinks(ctx, egressCleanupTestID, nat, nil)
			assert.Error(t, result.Err)
			assert.False(t, result.CommitUnknown)
			assert.Empty(t, c.calls())
			assert.False(t, c.mutationNoted())
		})
	}
}

// egressObserveSummary is the result of an observation with its error reduced to whether there is one.
type egressObserveSummary struct {
	Applied                                  *EgressConfig
	NATGone, Swept, CommitUnknown, Busy, Err bool
}

func summarizeObserve(r egressRunResult) egressObserveSummary {
	return egressObserveSummary{Applied: r.Applied, NATGone: r.NATGone, Swept: r.Swept,
		CommitUnknown: r.CommitUnknown, Busy: r.Busy, Err: r.Err != nil}
}

func TestObserveEgress(t *testing.T) {
	ctx := context.Background()
	cfg := testConfig()
	rg := cfg.ResourceGroup
	natID := natGatewayID(egressCleanupTestID, cfg)
	succeeded := armnetwork.ProvisioningStateSucceeded
	defaultV4 := defaultEgressConfig(egressParseContext{})
	defaultDual := defaultEgressConfig(egressParseContext{WantsIPv6: true})
	two := egressCleanupConfig(t, false, map[string]string{consts.PodAnnotationServiceEgressGatewayPublicIPCount: "2"})
	three := egressCleanupConfig(t, false, map[string]string{consts.PodAnnotationServiceEgressGatewayPublicIPCount: "3"})
	tagged := egressCleanupConfig(t, false, map[string]string{consts.PodAnnotationServiceEgressGatewayTags: "team=net"})
	rich := egressCleanupConfig(t, true, map[string]string{
		consts.PodAnnotationServiceEgressGatewayPublicIPCount:            "2",
		consts.PodAnnotationServiceEgressGatewayPublicIPPrefixLength:     "29",
		consts.PodAnnotationServiceEgressGatewayPublicIPCountIPv6:        "2",
		consts.PodAnnotationServiceEgressGatewayPublicIPPrefixLengthIPv6: "126",
		consts.PodAnnotationServiceEgressGatewayIdleTimeout:              "10",
	})
	unknown := egressUnknown
	assert.NotEqual(t, defaultV4, tagged, "control: the tagged configuration differs from the default")

	unlink := func(c *fakeARM) { c.natLocked(rg, egressCleanupTestID).Properties.ServiceGateway = nil }
	unregister := func(c *fakeARM) { delete(c.services, egressCleanupTestID) }
	pip := func(name string, tags map[string]*string, state armnetwork.ProvisioningState) func(c *fakeARM) {
		return func(c *fakeARM) {
			p := egressCleanupPIP(name, tags, state)
			p.ID, p.Etag = to.Ptr(publicIPAddressID(name, cfg)), c.nextEtagLocked()
			c.pips[fakeARMKey(rg, name)] = &p
		}
	}
	prefix := func(name string, tags map[string]*string) func(c *fakeARM) {
		return func(c *fakeARM) {
			p := egressCleanupPrefix(name, tags, succeeded)
			p.ID, p.Etag = to.Ptr(publicIPPrefixID(name, cfg)), c.nextEtagLocked()
			c.prefixes[fakeARMKey(rg, name)] = &p
		}
	}
	fault := func(call string, answer http.HandlerFunc) func(c *fakeARM) {
		return func(c *fakeARM) { c.faults[call] = append(c.faults[call], answer) }
	}
	garbage := pip("egress-a-pip-9", cleanupOwnTag(), succeeded)

	for _, tc := range []struct {
		name     string
		applied  *EgressConfig // what the NAT Gateway was created with; nil: no NAT Gateway
		ipv6     bool
		setups   []func(c *fakeARM)
		agreed   *EgressConfig
		unclear  *EgressConfig // the configuration of a write Azure did not answer clearly
		known    *EgressConfig // the configuration last known applied
		want     egressObserveSummary
		sequence []string
		reads    bool
		deletes  []string
	}{
		{name: "NAT Gateway unsettled: busy", applied: &defaultV4, setups: []func(c *fakeARM){unlink, garbage, func(c *fakeARM) {
			c.natLocked(rg, egressCleanupTestID).Properties.ProvisioningState = to.Ptr(armnetwork.ProvisioningStateUpdating)
		}}, want: egressObserveSummary{Busy: true}, sequence: []string{cleanupCallGetNAT}},
		{name: "NAT Gateway absent: reported, nothing else, nothing owed", setups: []func(c *fakeARM){garbage},
			want: egressObserveSummary{NATGone: true, Swept: true}, sequence: []string{cleanupCallGetNAT}},
		{name: "NAT Gateway cannot be read", applied: &defaultV4, setups: []func(c *fakeARM){
			fault(cleanupCallGetNAT, armReplyError(http.StatusForbidden, "AuthorizationFailed"))},
			want: egressObserveSummary{Err: true}, sequence: []string{cleanupCallGetNAT}},
		{name: "NAT Gateway not owned: neither written nor swept against", setups: []func(c *fakeARM){garbage, func(c *fakeARM) {
			c.storeNATLocked(rg, egressCleanupTestID, &armnetwork.NatGateway{Properties: &armnetwork.NatGatewayPropertiesFormat{
				PublicIPAddresses: []*armnetwork.SubResource{{ID: to.Ptr(fakeARMID("user-rg", "publicIPAddresses", "user-pip"))}}}})
		}}, want: egressObserveSummary{Applied: &unknown, Err: true}, sequence: []string{cleanupCallGetNAT}},

		{name: "the agreed configuration is applied", applied: &two, agreed: &two,
			want: egressObserveSummary{Applied: &two, Swept: true}, sequence: []string{cleanupCallGetNAT, cleanupCallGetServices}, reads: true},
		{name: "the agreed configuration comes before a satisfied fallback", applied: &tagged, agreed: &tagged,
			want: egressObserveSummary{Applied: &tagged, Swept: true}, sequence: []string{cleanupCallGetNAT, cleanupCallGetServices}, reads: true},
		{name: "the fallback without consensus", applied: &tagged,
			want: egressObserveSummary{Applied: &defaultV4, Swept: true}, sequence: []string{cleanupCallGetNAT, cleanupCallGetServices}, reads: true},
		{name: "the fallback when the agreed configuration is not applied", applied: &defaultV4, agreed: &two,
			want: egressObserveSummary{Applied: &defaultV4, Swept: true}, sequence: []string{cleanupCallGetNAT, cleanupCallGetServices}, reads: true},
		{name: "unknown when neither is applied; what the NAT Gateway references is kept", applied: &three, agreed: &two,
			want: egressObserveSummary{Applied: &unknown, Swept: true}, sequence: []string{cleanupCallGetNAT, cleanupCallGetServices}, reads: true},
		{name: "without consensus, the configuration of a write Azure did not answer clearly", applied: &three, unclear: &three,
			want: egressObserveSummary{Applied: &three, Swept: true}, sequence: []string{cleanupCallGetNAT, cleanupCallGetServices}, reads: true},
		{name: "the configuration of an unclear write comes before a satisfied fallback", applied: &tagged, unclear: &tagged,
			want: egressObserveSummary{Applied: &tagged, Swept: true}, sequence: []string{cleanupCallGetNAT, cleanupCallGetServices}, reads: true},
		{name: "the agreed configuration comes before that of an unclear write", applied: &tagged, agreed: &defaultV4, unclear: &tagged,
			want: egressObserveSummary{Applied: &defaultV4, Swept: true}, sequence: []string{cleanupCallGetNAT, cleanupCallGetServices}, reads: true},
		{name: "the fallback when an unclear write is not applied", applied: &defaultV4, unclear: &three,
			want: egressObserveSummary{Applied: &defaultV4, Swept: true}, sequence: []string{cleanupCallGetNAT, cleanupCallGetServices}, reads: true},
		{name: "unknown when an unclear write is not applied, nor the fallback", applied: &two, unclear: &three,
			want: egressObserveSummary{Applied: &unknown, Swept: true}, sequence: []string{cleanupCallGetNAT, cleanupCallGetServices}, reads: true},
		{name: "the configuration last known applied comes before a satisfied fallback", applied: &tagged, known: &tagged,
			want: egressObserveSummary{Applied: &tagged, Swept: true}, sequence: []string{cleanupCallGetNAT, cleanupCallGetServices}, reads: true},
		{name: "the configuration of an unclear write comes before the last known one", applied: &tagged, unclear: &tagged, known: &defaultV4,
			want: egressObserveSummary{Applied: &tagged, Swept: true}, sequence: []string{cleanupCallGetNAT, cleanupCallGetServices}, reads: true},
		{name: "the agreed configuration comes before the last known one", applied: &tagged, agreed: &defaultV4, known: &tagged,
			want: egressObserveSummary{Applied: &defaultV4, Swept: true}, sequence: []string{cleanupCallGetNAT, cleanupCallGetServices}, reads: true},
		{name: "the fallback when the last known configuration is not applied", applied: &defaultV4, known: &three,
			want: egressObserveSummary{Applied: &defaultV4, Swept: true}, sequence: []string{cleanupCallGetNAT, cleanupCallGetServices}, reads: true},
		{name: "the agreed configuration with managed prefixes in both families", applied: &rich, agreed: &rich, ipv6: true,
			want: egressObserveSummary{Applied: &rich, Swept: true}, sequence: []string{cleanupCallGetNAT, cleanupCallGetServices}, reads: true},
		{name: "a legacy identity satisfies the default", applied: &defaultV4, setups: []func(c *fakeARM){func(c *fakeARM) {
			delete(c.natLocked(rg, egressCleanupTestID).Tags, EgressIdentityTagKey)
			delete(c.pips[fakeARMKey(rg, "egress-a-pip")].Tags, EgressIdentityTagKey)
		}}, want: egressObserveSummary{Applied: &defaultV4, Swept: true}, sequence: []string{cleanupCallGetNAT, cleanupCallGetServices}, reads: true},
		{name: "dual-stack", applied: &defaultDual, ipv6: true, setups: []func(c *fakeARM){pip("egress-a-pip-v6-2", cleanupOwnTag(), succeeded)},
			want:     egressObserveSummary{Applied: &defaultDual, Swept: true},
			sequence: []string{cleanupCallGetNAT, cleanupCallGetServices}, reads: true, deletes: []string{"egress-a-pip-v6-2"}},

		{name: "link repair without consensus: both sides", applied: &defaultV4, setups: []func(c *fakeARM){unlink, unregister},
			want:     egressObserveSummary{Applied: &defaultV4, Swept: true},
			sequence: []string{cleanupCallGetNAT, cleanupCallGetServices, cleanupCallPutNAT, cleanupCallRegister}, reads: true},
		{name: "link repair: NAT Gateway side only", applied: &two, agreed: &two, setups: []func(c *fakeARM){unlink},
			want:     egressObserveSummary{Applied: &two, Swept: true},
			sequence: []string{cleanupCallGetNAT, cleanupCallGetServices, cleanupCallPutNAT}, reads: true},
		{name: "link repair: registration side only", applied: &defaultV4, setups: []func(c *fakeARM){func(c *fakeARM) {
			c.services[egressCleanupTestID].Properties.PublicNatGatewayID = nil
		}}, want: egressObserveSummary{Applied: &defaultV4, Swept: true},
			sequence: []string{cleanupCallGetNAT, cleanupCallGetServices, cleanupCallRegister}, reads: true},
		{name: "leftovers swept", applied: &defaultV4, setups: []func(c *fakeARM){garbage, prefix("egress-a-prefix-28", cleanupOwnTag()),
			pip("egress-a-pip-10", nil, succeeded), prefix("egress-a-prefix-29", nil)},
			want:     egressObserveSummary{Applied: &defaultV4, Swept: true},
			sequence: []string{cleanupCallGetNAT, cleanupCallGetServices}, reads: true, deletes: []string{"egress-a-pip-9", "egress-a-prefix-28"}},

		{name: "a referenced resource unsettled: busy before any write", applied: &two, agreed: &two, setups: []func(c *fakeARM){unlink, garbage,
			func(c *fakeARM) {
				c.pips[fakeARMKey(rg, "egress-a-pip-2")].Properties.ProvisioningState = to.Ptr(armnetwork.ProvisioningStateUpdating)
			}}, want: egressObserveSummary{Busy: true}, sequence: []string{cleanupCallGetNAT}, reads: true},
		{name: "a referenced resource cannot be read: no write", applied: &two, agreed: &two, setups: []func(c *fakeARM){unlink, garbage,
			fault("GET publicIPAddresses/egress-a-pip-2", armReplyError(http.StatusForbidden, "AuthorizationFailed"))},
			want: egressObserveSummary{Err: true}, sequence: []string{cleanupCallGetNAT}, reads: true},
		{name: "an unreferenced resource unsettled: kept, sweep incomplete", applied: &defaultV4,
			setups: []func(c *fakeARM){unlink, garbage, pip("egress-a-pip-10", cleanupOwnTag(), armnetwork.ProvisioningStateCreating)},
			want:   egressObserveSummary{Applied: &defaultV4}, sequence: []string{cleanupCallGetNAT, cleanupCallGetServices, cleanupCallPutNAT},
			reads: true, deletes: []string{"egress-a-pip-9"}},
		{name: "an unreferenced resource cannot be read: sweep incomplete", applied: &defaultV4, setups: []func(c *fakeARM){garbage,
			fault("GET publicIPAddresses/egress-a-pip-10", armReplyError(http.StatusForbidden, "AuthorizationFailed"))},
			want:     egressObserveSummary{Applied: &defaultV4, Err: true},
			sequence: []string{cleanupCallGetNAT, cleanupCallGetServices}, reads: true, deletes: []string{"egress-a-pip-9"}},

		{name: "link with an unknown outcome: the run ends, nothing swept", applied: &defaultV4, setups: []func(c *fakeARM){unlink, garbage,
			fault(cleanupCallPutNAT, armReplyError(http.StatusServiceUnavailable, "ServiceUnavailable"))},
			want:     egressObserveSummary{Applied: &unknown, CommitUnknown: true, Err: true},
			sequence: []string{cleanupCallGetNAT, cleanupCallGetServices, cleanupCallPutNAT}, reads: true},
		{name: "registration with an unknown outcome: the run ends, nothing swept", applied: &defaultV4, setups: []func(c *fakeARM){unregister, garbage,
			fault(cleanupCallRegister, armDropConnection)},
			want:     egressObserveSummary{Applied: &unknown, CommitUnknown: true, Err: true},
			sequence: []string{cleanupCallGetNAT, cleanupCallGetServices, cleanupCallRegister}, reads: true},
		{name: "link refused: the run ends, nothing swept", applied: &defaultV4, setups: []func(c *fakeARM){unlink, garbage,
			fault(cleanupCallPutNAT, armReplyError(http.StatusBadRequest, "InvalidRequest"))},
			want:     egressObserveSummary{Applied: &unknown, Err: true},
			sequence: []string{cleanupCallGetNAT, cleanupCallGetServices, cleanupCallPutNAT}, reads: true},
		{name: "NAT Gateway used by another service: nothing written, nothing swept", applied: &defaultV4, setups: []func(c *fakeARM){unlink, garbage,
			func(c *fakeARM) {
				c.services["default-outbound"] = &armnetwork.ServiceGatewayService{Name: to.Ptr("default-outbound"),
					Properties: &armnetwork.ServiceGatewayServicePropertiesFormat{PublicNatGatewayID: to.Ptr(natID)}}
			}},
			want:     egressObserveSummary{Applied: &unknown, Err: true},
			sequence: []string{cleanupCallGetNAT, cleanupCallGetServices}, reads: true},
		{name: "registration refused: applied reported, nothing swept", applied: &defaultV4, setups: []func(c *fakeARM){unregister, garbage,
			fault(cleanupCallRegister, armReplyError(http.StatusBadRequest, "InvalidRequest"))},
			want:     egressObserveSummary{Applied: &defaultV4, Err: true},
			sequence: []string{cleanupCallGetNAT, cleanupCallGetServices, cleanupCallRegister}, reads: true},
		{name: "registrations cannot be read: nothing written, nothing reported", applied: &defaultV4, setups: []func(c *fakeARM){unlink, garbage,
			fault(cleanupCallGetServices, armReplyError(http.StatusForbidden, "AuthorizationFailed"))},
			want:     egressObserveSummary{Err: true},
			sequence: []string{cleanupCallGetNAT, cleanupCallGetServices}, reads: true},
		{name: "a leftover DELETE with an unknown outcome", applied: &defaultV4, setups: []func(c *fakeARM){garbage,
			fault("DELETE publicIPAddresses/egress-a-pip-9", armReplyError(http.StatusInternalServerError, "InternalServerError"))},
			want:     egressObserveSummary{Applied: &unknown, CommitUnknown: true, Err: true},
			sequence: []string{cleanupCallGetNAT, cleanupCallGetServices}, reads: true, deletes: []string{"egress-a-pip-9"}},
		{name: "the link PUT returns a NAT Gateway without references: what the read references is kept", applied: &defaultV4,
			setups: []func(c *fakeARM){unlink, fault(cleanupCallPutNAT,
				armReply(http.StatusOK, `{"name":"egress-a","properties":{"provisioningState":"Succeeded"}}`))},
			want:     egressObserveSummary{Applied: &unknown, Swept: true},
			sequence: []string{cleanupCallGetNAT, cleanupCallGetServices, cleanupCallPutNAT}, reads: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newCleanupARM(t)
			if tc.applied != nil {
				c.seedApplied(*tc.applied, tc.ipv6)
			}
			c.register(egressCleanupTestID, natID)
			for _, setup := range tc.setups {
				c.update(setup)
			}
			read := c.nat(rg, egressCleanupTestID)
			fallback := defaultEgressConfig(egressParseContext{WantsIPv6: tc.ipv6})

			result := c.dt.observeEgress(ctx, egressCleanupTestID, tc.ipv6, tc.agreed, tc.unclear, tc.known, &fallback)
			assert.Equal(t, tc.want, summarizeObserve(result), "err: %v", result.Err)
			assert.Equal(t, tc.sequence, c.sequence())
			var reads []string
			if tc.reads {
				reads = cleanupReservedReads(tc.ipv6)
			}
			assert.Equal(t, reads, c.reservedCalls(http.MethodGet))
			assert.Equal(t, cleanupDeletes(tc.deletes...), c.reservedCalls(http.MethodDelete))
			assert.Equal(t, len(c.writes()) > 0, c.mutationNoted(), "a mutation is noted exactly when a write is sent")
			for _, write := range c.writes() {
				leftover := isReservedCall(write) && strings.HasPrefix(write, http.MethodDelete+" ")
				assert.True(t, leftover || write == cleanupCallPutNAT || write == cleanupCallRegister, "no configuration write: %s", write)
			}
			for _, put := range c.requestsOf(cleanupCallPutNAT) {
				assert.Empty(t, put.IfMatch)
				assert.JSONEq(t, string(fakeARMJSON(t, natLinkRequest(read, cfg))), string(put.Body), "a link-only PUT")
			}
		})
	}
}

// egressCleanupSummary is the result of a deletion or a BYO residue cleanup with its error reduced to whether there is
// one.
type egressCleanupSummary struct {
	NotApplicable, NATGone, Swept, CommitUnknown, Busy, Err, Complete bool
}

func summarizeCleanup(r egressRunResult) egressCleanupSummary {
	return egressCleanupSummary{NotApplicable: r.NotApplicable, NATGone: r.NATGone, Swept: r.Swept,
		CommitUnknown: r.CommitUnknown, Busy: r.Busy, Err: r.Err != nil, Complete: r.Complete()}
}

func TestEgressRunResultComplete(t *testing.T) {
	err := errors.New("failed")
	for _, tc := range []struct {
		result egressRunResult
		want   bool
	}{
		{result: egressRunResult{}},
		{result: egressRunResult{NATGone: true, Swept: true}, want: true},
		{result: egressRunResult{NotApplicable: true, Swept: true}, want: true},
		{result: egressRunResult{NATGone: true}},
		{result: egressRunResult{NotApplicable: true}},
		{result: egressRunResult{Swept: true}},
		{result: egressRunResult{NATGone: true, Swept: true, Err: err}},
		{result: egressRunResult{NATGone: true, Swept: true, Busy: true}},
		{result: egressRunResult{NATGone: true, Swept: true, CommitUnknown: true}},
	} {
		assert.Equal(t, tc.want, tc.result.Complete(), "%+v", tc.result)
	}
}

// TestUnlinkManagedEgress pins the unlink step of a managed deletion (design 4.4): the identity's registration loses its
// NAT Gateway with a partial update, then the NAT Gateway, read settled, is unlinked from the cluster's Service Gateway
// with a PUT without precondition, unless another service uses it or it is no longer the controller's. Each write is
// sent once and classified; the unlink stops at the first read or write that does not succeed: an unknown outcome
// (CommitUnknown), an unsettled NAT Gateway (Busy), or any other failure, after which nothing else is written.
func TestUnlinkManagedEgress(t *testing.T) {
	ctx := context.Background()
	cfg := testConfig()
	rg := cfg.ResourceGroup
	sgw := cfg.ServiceGatewayResourceID()
	natID := natGatewayID(egressCleanupTestID, cfg)
	fault := func(call string, answer http.HandlerFunc) func(c *fakeARM) {
		return func(c *fakeARM) { c.faults[call] = append(c.faults[call], answer) }
	}
	unknown := armReplyError(http.StatusInternalServerError, "InternalServerError")
	refused := armReplyError(http.StatusConflict, "Conflict")
	unreadable := armReplyError(http.StatusForbidden, "AuthorizationFailed")
	unregistered := func(c *fakeARM) { delete(c.services, egressCleanupTestID) }
	natSide := []string{cleanupCallGetServices, cleanupCallGetNAT, cleanupCallPutNAT}
	clearOnly := []string{cleanupCallGetServices, cleanupCallRegister, cleanupCallGetNAT}
	both := []string{cleanupCallGetServices, cleanupCallRegister, cleanupCallGetNAT, cleanupCallPutNAT}

	for _, tc := range []struct {
		name   string
		setups []func(c *fakeARM)
		calls  []string
		want   egressCleanupSummary
		// wantCleared: the identity's registration references no NAT Gateway afterwards; wantUnlinked: the NAT Gateway
		// is not linked to the cluster's Service Gateway afterwards.
		wantCleared, wantUnlinked bool
	}{
		{name: "both sides linked: the registration is cleared, then the NAT Gateway unlinked", calls: both,
			want: egressCleanupSummary{}, wantCleared: true, wantUnlinked: true},
		{name: "registered without a NAT Gateway: the NAT Gateway side only", setups: []func(c *fakeARM){func(c *fakeARM) {
			c.services[egressCleanupTestID].Properties.PublicNatGatewayID = nil
		}}, calls: natSide, want: egressCleanupSummary{}, wantCleared: true, wantUnlinked: true},
		{name: "not registered: the NAT Gateway side only", setups: []func(c *fakeARM){unregistered},
			calls: natSide, want: egressCleanupSummary{}, wantCleared: true, wantUnlinked: true},
		{name: "no Service Gateway: the NAT Gateway side only", setups: []func(c *fakeARM){unregistered, func(c *fakeARM) { c.noServiceGateway = true }},
			calls: natSide, want: egressCleanupSummary{}, wantCleared: true, wantUnlinked: true},
		{name: "NAT Gateway not linked: the registration only", setups: []func(c *fakeARM){func(c *fakeARM) {
			c.natLocked(rg, egressCleanupTestID).Properties.ServiceGateway = nil
		}}, calls: clearOnly, want: egressCleanupSummary{}, wantCleared: true, wantUnlinked: true},
		{name: "NAT Gateway linked to another Service Gateway: left alone", setups: []func(c *fakeARM){func(c *fakeARM) {
			c.natLocked(rg, egressCleanupTestID).Properties.ServiceGateway.ID = to.Ptr(fakeARMID(rg, "serviceGateways", "other"))
		}}, calls: clearOnly, want: egressCleanupSummary{}, wantCleared: true, wantUnlinked: true},
		{name: "NAT Gateway linked to the cluster's Service Gateway spelled in another case: unlinked", setups: []func(c *fakeARM){func(c *fakeARM) {
			c.natLocked(rg, egressCleanupTestID).Properties.ServiceGateway.ID = to.Ptr(strings.ToUpper(sgw))
		}}, calls: both, want: egressCleanupSummary{}, wantCleared: true, wantUnlinked: true},
		{name: "NAT Gateway gone", setups: []func(c *fakeARM){func(c *fakeARM) { delete(c.nats, fakeARMKey(rg, egressCleanupTestID)) }},
			calls: clearOnly, want: egressCleanupSummary{}, wantCleared: true, wantUnlinked: true},
		{name: "NAT Gateway used by another service: left linked", setups: []func(c *fakeARM){func(c *fakeARM) {
			c.services["default-outbound"] = &armnetwork.ServiceGatewayService{Name: to.Ptr("default-outbound"),
				Properties: &armnetwork.ServiceGatewayServicePropertiesFormat{PublicNatGatewayID: to.Ptr(strings.ToUpper(natID))}}
		}}, calls: []string{cleanupCallGetServices, cleanupCallRegister}, want: egressCleanupSummary{}, wantCleared: true},
		{name: "NAT Gateway no longer the controller's: left linked", setups: []func(c *fakeARM){func(c *fakeARM) {
			c.natLocked(rg, egressCleanupTestID).Tags = cleanupOtherTag()
		}}, calls: clearOnly, want: egressCleanupSummary{}, wantCleared: true},
		{name: "registrations cannot be read: nothing written", setups: []func(c *fakeARM){fault(cleanupCallGetServices, unreadable)},
			calls: []string{cleanupCallGetServices}, want: egressCleanupSummary{Err: true}},
		{name: "registration refused: the NAT Gateway is left linked", setups: []func(c *fakeARM){fault(cleanupCallRegister, refused)},
			calls: []string{cleanupCallGetServices, cleanupCallRegister}, want: egressCleanupSummary{Err: true}},
		{name: "registration with an unknown outcome: sent once, nothing after it", setups: []func(c *fakeARM){fault(cleanupCallRegister, unknown)},
			calls: []string{cleanupCallGetServices, cleanupCallRegister}, want: egressCleanupSummary{CommitUnknown: true, Err: true}},
		{name: "registration without an answer: sent once, nothing after it", setups: []func(c *fakeARM){fault(cleanupCallRegister, armDropConnection)},
			calls: []string{cleanupCallGetServices, cleanupCallRegister}, want: egressCleanupSummary{CommitUnknown: true, Err: true}},
		{name: "NAT Gateway cannot be read", setups: []func(c *fakeARM){fault(cleanupCallGetNAT, unreadable)},
			calls: clearOnly, want: egressCleanupSummary{Err: true}, wantCleared: true},
		{name: "NAT Gateway unsettled: busy, left linked", setups: []func(c *fakeARM){func(c *fakeARM) {
			c.natLocked(rg, egressCleanupTestID).Properties.ProvisioningState = to.Ptr(armnetwork.ProvisioningStateUpdating)
		}}, calls: clearOnly, want: egressCleanupSummary{Busy: true, Err: true}, wantCleared: true},
		{name: "NAT Gateway PUT refused", setups: []func(c *fakeARM){fault(cleanupCallPutNAT, refused)},
			calls: both, want: egressCleanupSummary{Err: true}, wantCleared: true},
		{name: "NAT Gateway PUT with an unknown outcome: sent once", setups: []func(c *fakeARM){fault(cleanupCallPutNAT, unknown)},
			calls: both, want: egressCleanupSummary{CommitUnknown: true, Err: true}, wantCleared: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newCleanupARM(t)
			c.seedApplied(defaultEgressConfig(egressParseContext{}), false)
			c.register(egressCleanupTestID, natID)
			for _, setup := range tc.setups {
				c.update(setup)
			}
			read := c.nat(rg, egressCleanupTestID)

			result := c.dt.unlinkManagedEgress(ctx, egressCleanupTestID)
			assert.Equal(t, tc.want, summarizeCleanup(result), "err: %v", result.Err)
			assert.Equal(t, tc.calls, c.calls())
			assert.Equal(t, len(c.writes()) > 0, c.mutationNoted(), "a mutation is noted exactly when a write is sent")

			c.mu.Lock()
			registration := c.services[egressCleanupTestID]
			c.mu.Unlock()
			cleared := registration == nil || registration.Properties == nil || registration.Properties.PublicNatGatewayID == nil
			assert.Equal(t, tc.wantCleared, cleared, "registration cleared afterwards")
			assert.Equal(t, tc.wantUnlinked, !strings.EqualFold(natGatewayServiceGatewayID(c.nat(rg, egressCleanupTestID)), sgw),
				"NAT Gateway unlinked afterwards")
			for _, post := range c.requestsOf(cleanupCallRegister) {
				assert.Contains(t, string(post.Body), `"isDelete":false`, "the registration is kept")
				assert.Contains(t, string(post.Body), `"publicNatGatewayId":null`, "its NAT Gateway is cleared")
			}
			for _, put := range c.requestsOf(cleanupCallPutNAT) {
				assert.Empty(t, put.IfMatch, "the unlink PUT carries no precondition")
				assert.JSONEq(t, string(fakeARMJSON(t, natUnlinkRequest(read))), string(put.Body), "the unlink PUT changes nothing but the link")
			}
		})
	}
}

func TestDeleteManagedEgress(t *testing.T) {
	ctx := context.Background()
	cfg := testConfig()
	rg := cfg.ResourceGroup
	natID := natGatewayID(egressCleanupTestID, cfg)
	succeeded := armnetwork.ProvisioningStateSucceeded
	defaultV4 := defaultEgressConfig(egressParseContext{})

	t.Run("after the unlink: deleted on the etag of the read right before, then every reserved name swept", func(t *testing.T) {
		c := newCleanupARM(t)
		c.seedApplied(egressCleanupConfig(t, true, map[string]string{
			consts.PodAnnotationServiceEgressGatewayPublicIPCount:        "2",
			consts.PodAnnotationServiceEgressGatewayPublicIPPrefixLength: "30",
			consts.PodAnnotationServiceEgressGatewayPublicIPCountIPv6:    "1",
		}), true)
		c.register(egressCleanupTestID, natID)
		c.seedPublicIP(egressCleanupPIP("egress-a-pip-3", cleanupOwnTag(), succeeded))
		c.seedPublicIP(egressCleanupPIP("egress-a-pip-4", nil, succeeded))
		c.seedPrefix(egressCleanupPrefix("egress-a-prefix-v6-124", nil, succeeded))
		created := derefString(c.nat(rg, egressCleanupTestID).Etag)

		// The existing order unlinks both sides before the NAT Gateway and its Public IPs are deleted.
		unlink := c.dt.unlinkManagedEgress(ctx, egressCleanupTestID)
		assert.Equal(t, egressCleanupSummary{}, summarizeCleanup(unlink), "err: %v", unlink.Err)
		assert.Equal(t, []string{cleanupCallRegister, cleanupCallPutNAT}, c.writes(), "control: both sides are unlinked")
		unlinked := derefString(c.nat(rg, egressCleanupTestID).Etag)
		assert.NotEqual(t, created, unlinked, "control: the unlink PUT changed the etag")
		c.setMutationNoted(false)
		c.resetRequests()

		result := c.dt.deleteManagedEgress(ctx, egressCleanupTestID)
		assert.Equal(t, egressCleanupSummary{NATGone: true, Swept: true, Complete: true}, summarizeCleanup(result), "err: %v", result.Err)
		assert.Equal(t, []string{cleanupCallGetNAT, cleanupCallDeleteNAT}, c.calls()[:2], "the NAT Gateway is read right before its DELETE")
		assert.Equal(t, []string{cleanupCallGetNAT, cleanupCallDeleteNAT}, c.sequence())
		if deletes := c.requestsOf(cleanupCallDeleteNAT); assert.Len(t, deletes, 1) {
			assert.Equal(t, unlinked, deletes[0].IfMatch, "the etag after the unlink, not an older one")
		}
		assert.Equal(t, cleanupReservedReads(true), c.reservedCalls(http.MethodGet))
		assert.Equal(t, cleanupDeletes("egress-a-pip", "egress-a-pip-2", "egress-a-pip-3", "egress-a-pip-v6", "egress-a-prefix-30"),
			c.reservedCalls(http.MethodDelete))
		assert.Nil(t, c.nat(rg, egressCleanupTestID))
		pips, prefixes := c.resourceNames()
		assert.Equal(t, []string{"egress-a-pip-4"}, pips)
		assert.Equal(t, []string{"egress-a-prefix-v6-124"}, prefixes)
	})

	t.Run("a NAT Gateway written after the read is not deleted", func(t *testing.T) {
		c := newCleanupARM(t)
		c.seedApplied(defaultV4, false)
		c.update(func(c *fakeARM) { c.natLocked(rg, egressCleanupTestID).Properties.ServiceGateway = nil })
		c.seedPublicIP(egressCleanupPIP("egress-a-pip-9", cleanupOwnTag(), succeeded))
		read := ""
		c.hook(cleanupCallGetNAT, func(c *fakeARM) {
			nat := c.natLocked(rg, egressCleanupTestID)
			read, nat.Etag = derefString(nat.Etag), c.nextEtagLocked()
		})

		result := c.dt.deleteManagedEgress(ctx, egressCleanupTestID)
		assert.Equal(t, egressCleanupSummary{Err: true}, summarizeCleanup(result))
		assert.Equal(t, []string{cleanupCallGetNAT, cleanupCallDeleteNAT}, c.calls(), "nothing is swept")
		assert.Equal(t, read, c.requestsOf(cleanupCallDeleteNAT)[0].IfMatch)
		assert.NotNil(t, c.nat(rg, egressCleanupTestID), "Azure refused the DELETE (412)")
	})

	unlink := func(c *fakeARM) { c.natLocked(rg, egressCleanupTestID).Properties.ServiceGateway = nil }
	pip := func(name string, tags map[string]*string, state armnetwork.ProvisioningState) func(c *fakeARM) {
		return func(c *fakeARM) {
			p := egressCleanupPIP(name, tags, state)
			p.ID, p.Etag = to.Ptr(publicIPAddressID(name, cfg)), c.nextEtagLocked()
			c.pips[fakeARMKey(rg, name)] = &p
		}
	}
	fault := func(call string, answer http.HandlerFunc) func(c *fakeARM) {
		return func(c *fakeARM) { c.faults[call] = append(c.faults[call], answer) }
	}
	garbage := pip("egress-a-pip-9", cleanupOwnTag(), succeeded)
	for _, tc := range []struct {
		name     string
		setups   []func(c *fakeARM)
		want     egressCleanupSummary
		sequence []string
		reads    bool
		deletes  []string
		natKept  bool
	}{
		{name: "NAT Gateway already gone: every reserved name of the controller, both families",
			setups: []func(c *fakeARM){func(c *fakeARM) { delete(c.nats, fakeARMKey(rg, egressCleanupTestID)) }, garbage,
				pip("egress-a-pip-3", nil, succeeded), pip("egress-a-pip-v6", nil, succeeded), pip("egress-a-pip-v6-2", nil, succeeded)},
			want:     egressCleanupSummary{NATGone: true, Swept: true, Complete: true},
			sequence: []string{cleanupCallGetNAT}, reads: true, deletes: []string{"egress-a-pip", "egress-a-pip-9", "egress-a-pip-v6"}},
		{name: "deleted with nothing left to sweep", setups: []func(c *fakeARM){unlink, func(c *fakeARM) {
			c.natLocked(rg, egressCleanupTestID).Properties.PublicIPAddresses = nil
			delete(c.pips, fakeARMKey(rg, "egress-a-pip"))
		}}, want: egressCleanupSummary{NATGone: true, Swept: true, Complete: true},
			sequence: []string{cleanupCallGetNAT, cleanupCallDeleteNAT}, reads: true},
		{name: "DELETE answered 404: gone", setups: []func(c *fakeARM){unlink, func(c *fakeARM) {
			// Deleted by someone else between the read and the DELETE.
			c.faults[cleanupCallDeleteNAT] = append(c.faults[cleanupCallDeleteNAT], func(w http.ResponseWriter, r *http.Request) {
				c.update(func(c *fakeARM) { delete(c.nats, fakeARMKey(rg, egressCleanupTestID)) })
				armReplyError(http.StatusNotFound, "NotFound")(w, r)
			})
		}}, want: egressCleanupSummary{NATGone: true, Swept: true, Complete: true},
			sequence: []string{cleanupCallGetNAT, cleanupCallDeleteNAT}, reads: true, deletes: []string{"egress-a-pip"}},
		{name: "DELETE with an unknown outcome: nothing swept", setups: []func(c *fakeARM){unlink, garbage,
			fault(cleanupCallDeleteNAT, armReplyError(http.StatusInternalServerError, "InternalServerError"))},
			want:     egressCleanupSummary{CommitUnknown: true, Err: true},
			sequence: []string{cleanupCallGetNAT, cleanupCallDeleteNAT}, natKept: true},
		{name: "DELETE refused, the NAT Gateway still linked: nothing swept", setups: []func(c *fakeARM){garbage},
			want: egressCleanupSummary{Err: true}, sequence: []string{cleanupCallGetNAT, cleanupCallDeleteNAT}, natKept: true},
		{name: "NAT Gateway unsettled: busy", setups: []func(c *fakeARM){unlink, garbage, func(c *fakeARM) {
			c.natLocked(rg, egressCleanupTestID).Properties.ProvisioningState = to.Ptr(armnetwork.ProvisioningStateDeleting)
		}}, want: egressCleanupSummary{Busy: true}, sequence: []string{cleanupCallGetNAT}, natKept: true},
		{name: "NAT Gateway no longer the controller's: left alone with its resources", setups: []func(c *fakeARM){garbage, func(c *fakeARM) {
			c.storeNATLocked(rg, egressCleanupTestID, &armnetwork.NatGateway{Properties: &armnetwork.NatGatewayPropertiesFormat{
				PublicIPAddresses: []*armnetwork.SubResource{{ID: to.Ptr(fakeARMID("user-rg", "publicIPAddresses", "user-pip"))}}}})
		}}, want: egressCleanupSummary{NotApplicable: true, Swept: true, Complete: true}, sequence: []string{cleanupCallGetNAT}, natKept: true},
		{name: "NAT Gateway cannot be read", setups: []func(c *fakeARM){unlink, fault(cleanupCallGetNAT, armReplyError(http.StatusForbidden, "AuthorizationFailed"))},
			want: egressCleanupSummary{Err: true}, sequence: []string{cleanupCallGetNAT}, natKept: true},
		{name: "NAT Gateway read without an etag: never deleted unconditionally", setups: []func(c *fakeARM){unlink, func(c *fakeARM) {
			c.natLocked(rg, egressCleanupTestID).Etag = nil
		}}, want: egressCleanupSummary{Err: true}, sequence: []string{cleanupCallGetNAT}, natKept: true},
		{name: "deleted, a leftover DELETE refused: incomplete", setups: []func(c *fakeARM){unlink, garbage,
			fault("DELETE publicIPAddresses/egress-a-pip-9", armReplyError(http.StatusConflict, "Conflict"))},
			want:     egressCleanupSummary{NATGone: true, Err: true},
			sequence: []string{cleanupCallGetNAT, cleanupCallDeleteNAT}, reads: true, deletes: []string{"egress-a-pip", "egress-a-pip-9"}},
		{name: "deleted, a leftover unsettled: incomplete", setups: []func(c *fakeARM){unlink, pip("egress-a-pip-9", cleanupOwnTag(), armnetwork.ProvisioningStateUpdating)},
			want:     egressCleanupSummary{NATGone: true},
			sequence: []string{cleanupCallGetNAT, cleanupCallDeleteNAT}, reads: true, deletes: []string{"egress-a-pip"}},
		{name: "deleted, a leftover DELETE with an unknown outcome", setups: []func(c *fakeARM){unlink, garbage,
			fault("DELETE publicIPAddresses/egress-a-pip-9", armDropConnection)},
			want:     egressCleanupSummary{NATGone: true, CommitUnknown: true, Err: true},
			sequence: []string{cleanupCallGetNAT, cleanupCallDeleteNAT}, reads: true, deletes: []string{"egress-a-pip", "egress-a-pip-9"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newCleanupARM(t)
			c.seedApplied(defaultV4, false)
			c.register(egressCleanupTestID, natID)
			for _, setup := range tc.setups {
				c.update(setup)
			}

			result := c.dt.deleteManagedEgress(ctx, egressCleanupTestID)
			assert.Equal(t, tc.want, summarizeCleanup(result), "err: %v", result.Err)
			assert.Equal(t, tc.sequence, c.sequence())
			var reads []string
			if tc.reads {
				reads = cleanupReservedReads(true)
			}
			assert.Equal(t, reads, c.reservedCalls(http.MethodGet))
			assert.Equal(t, cleanupDeletes(tc.deletes...), c.reservedCalls(http.MethodDelete))
			assert.Equal(t, tc.natKept, c.nat(rg, egressCleanupTestID) != nil, "NAT Gateway kept")
			assert.Equal(t, len(c.writes()) > 0, c.mutationNoted(), "a mutation is noted exactly when a write is sent")
			for _, del := range c.requestsOf(cleanupCallDeleteNAT) {
				assert.NotEmpty(t, del.IfMatch, "the NAT Gateway DELETE is conditional")
			}
		})
	}
}

func TestCleanupBYOResidue(t *testing.T) {
	ctx := context.Background()
	cfg := testConfig()
	rg := cfg.ResourceGroup
	natID := natGatewayID(egressCleanupTestID, cfg)
	byoNATID := fakeARMID(egressCleanupTestBYORG, "natGateways", egressCleanupTestID)
	succeeded := armnetwork.ProvisioningStateSucceeded
	defaultV4 := defaultEgressConfig(egressParseContext{})

	// newResidue returns a cloud where the test identity is BYO, registered with its NAT Gateway in the VNet resource
	// group, while its managed past left a NAT Gateway in the cluster resource group, linked to the Service Gateway as
	// well, with its Public IP and a leftover Public IP. The default outbound service uses a NAT Gateway of its own.
	newResidue := func(t *testing.T) *fakeARM {
		c := newCleanupARM(t)
		c.seedApplied(defaultV4, false)
		c.seedPublicIP(egressCleanupPIP("egress-a-pip-2", cleanupOwnTag(), succeeded))
		c.seedPublicIP(egressCleanupPIP("egress-a-pip-3", nil, succeeded))
		c.seedNAT(egressCleanupTestBYORG, armnetwork.NatGateway{
			Name:     to.Ptr(egressCleanupTestID),
			Location: to.Ptr(cfg.Location),
			SKU:      &armnetwork.NatGatewaySKU{Name: to.Ptr(armnetwork.NatGatewaySKUNameStandardV2)},
			Properties: &armnetwork.NatGatewayPropertiesFormat{
				ServiceGateway:    &armnetwork.SubResource{ID: to.Ptr(cfg.ServiceGatewayResourceID())},
				PublicIPAddresses: []*armnetwork.SubResource{{ID: to.Ptr(fakeARMID(egressCleanupTestBYORG, "publicIPAddresses", "customer-pip"))}},
			},
		})
		c.register(egressCleanupTestID, byoNATID)
		c.register("default-outbound", fakeARMID(rg, "natGateways", "aks-default"))
		return c
	}
	// untouched checks that the run sent nothing to the VNet resource group and no registration, and that the BYO NAT
	// Gateway and the registrations are as they were.
	type snapshot struct{ byo, services string }
	snap := func(c *fakeARM) snapshot {
		c.mu.Lock()
		defer c.mu.Unlock()
		return snapshot{
			byo:      string(fakeARMJSON(t, c.nats[fakeARMKey(egressCleanupTestBYORG, egressCleanupTestID)])),
			services: string(fakeARMJSON(t, c.services)),
		}
	}
	untouched := func(t *testing.T, c *fakeARM, before snapshot) {
		t.Helper()
		for _, call := range c.calls() {
			assert.NotContains(t, call, egressCleanupTestBYORG, "the BYO NAT Gateway is never read or written")
			assert.NotEqual(t, cleanupCallRegister, call, "no registration is written")
		}
		assert.Equal(t, before, snap(c))
	}

	t.Run("owned and unused: unlinked without precondition, read again, deleted on the fresh etag, then swept", func(t *testing.T) {
		c := newResidue(t)
		before := snap(c)
		first := c.nat(rg, egressCleanupTestID)
		unlinked := ""
		c.hook(cleanupCallPutNAT, func(c *fakeARM) { unlinked = derefString(c.natLocked(rg, egressCleanupTestID).Etag) })

		result := c.dt.cleanupBYOResidue(ctx, egressCleanupTestID)
		assert.Equal(t, egressCleanupSummary{NATGone: true, Swept: true, Complete: true}, summarizeCleanup(result), "err: %v", result.Err)
		sequence := []string{cleanupCallGetNAT, cleanupCallGetServices, cleanupCallPutNAT, cleanupCallGetNAT, cleanupCallDeleteNAT}
		assert.Equal(t, sequence, c.sequence())
		assert.Equal(t, sequence, c.calls()[:len(sequence)], "the reserved names are swept once the NAT Gateway is gone")
		if puts := c.requestsOf(cleanupCallPutNAT); assert.Len(t, puts, 1) {
			assert.Empty(t, puts[0].IfMatch, "the unlink PUT carries no precondition")
			assert.JSONEq(t, string(fakeARMJSON(t, natUnlinkRequest(first))), string(puts[0].Body), "a NAT-side unlink only")
			assert.Contains(t, string(puts[0].Body), `"serviceGateway":null`)
			assert.NotContains(t, string(puts[0].Body), "etag")
		}
		if deletes := c.requestsOf(cleanupCallDeleteNAT); assert.Len(t, deletes, 1) {
			assert.Equal(t, unlinked, deletes[0].IfMatch, "the etag of the read after the unlink")
			assert.NotEqual(t, derefString(first.Etag), deletes[0].IfMatch)
		}
		assert.Equal(t, cleanupReservedReads(true), c.reservedCalls(http.MethodGet), "both families")
		assert.Equal(t, cleanupDeletes("egress-a-pip", "egress-a-pip-2"), c.reservedCalls(http.MethodDelete))
		assert.Nil(t, c.nat(rg, egressCleanupTestID))
		pips, _ := c.resourceNames()
		assert.Equal(t, []string{"egress-a-pip-3"}, pips)
		untouched(t, c, before)
	})

	t.Run("owned, unused and not linked: deleted on the etag of the first read", func(t *testing.T) {
		c := newResidue(t)
		c.update(func(c *fakeARM) { c.natLocked(rg, egressCleanupTestID).Properties.ServiceGateway = nil })
		before := snap(c)
		first := c.nat(rg, egressCleanupTestID)

		result := c.dt.cleanupBYOResidue(ctx, egressCleanupTestID)
		assert.Equal(t, egressCleanupSummary{NATGone: true, Swept: true, Complete: true}, summarizeCleanup(result), "err: %v", result.Err)
		assert.Equal(t, []string{cleanupCallGetNAT, cleanupCallGetServices, cleanupCallDeleteNAT}, c.sequence())
		assert.Equal(t, derefString(first.Etag), c.requestsOf(cleanupCallDeleteNAT)[0].IfMatch)
		assert.Equal(t, cleanupDeletes("egress-a-pip", "egress-a-pip-2"), c.reservedCalls(http.MethodDelete))
		untouched(t, c, before)
	})

	setState := func(state armnetwork.ProvisioningState) func(c *fakeARM) {
		return func(c *fakeARM) {
			c.natLocked(rg, egressCleanupTestID).Properties.ProvisioningState = to.Ptr(state)
		}
	}
	disown := func(c *fakeARM) {
		nat := c.natLocked(rg, egressCleanupTestID)
		delete(nat.Tags, EgressIdentityTagKey)
		nat.Properties.PublicIPPrefixes = []*armnetwork.SubResource{{ID: to.Ptr(fakeARMID("user-rg", "publicIPPrefixes", "user-prefix"))}}
	}
	fault := func(call string, answer http.HandlerFunc) func(c *fakeARM) {
		return func(c *fakeARM) { c.faults[call] = append(c.faults[call], answer) }
	}
	after := func(call string, fn func(c *fakeARM)) func(c *fakeARM) {
		return func(c *fakeARM) { c.hooks[call] = fn }
	}
	unlinkAndDelete := []string{cleanupCallGetNAT, cleanupCallGetServices, cleanupCallPutNAT, cleanupCallGetNAT, cleanupCallDeleteNAT}
	for _, tc := range []struct {
		name     string
		setups   []func(c *fakeARM)
		want     egressCleanupSummary
		sequence []string
		reads    bool
		deletes  []string
		natKept  bool
	}{
		{name: "used by another service: left alone with its resources", setups: []func(c *fakeARM){func(c *fakeARM) {
			c.services["other"] = &armnetwork.ServiceGatewayService{Name: to.Ptr("other"),
				Properties: &armnetwork.ServiceGatewayServicePropertiesFormat{PublicNatGatewayID: to.Ptr(strings.ToUpper(natID))}}
		}}, want: egressCleanupSummary{NotApplicable: true, Swept: true, Complete: true},
			sequence: []string{cleanupCallGetNAT, cleanupCallGetServices}, natKept: true},
		{name: "used by a service without a name: left alone", setups: []func(c *fakeARM){func(c *fakeARM) {
			c.services["unnamed"] = &armnetwork.ServiceGatewayService{
				Properties: &armnetwork.ServiceGatewayServicePropertiesFormat{PublicNatGatewayID: to.Ptr(natID)}}
		}}, want: egressCleanupSummary{NotApplicable: true, Swept: true, Complete: true},
			sequence: []string{cleanupCallGetNAT, cleanupCallGetServices}, natKept: true},
		{name: "used by the identity's own registration: left alone", setups: []func(c *fakeARM){func(c *fakeARM) {
			c.services[egressCleanupTestID].Properties.PublicNatGatewayID = to.Ptr(natID)
		}}, want: egressCleanupSummary{NotApplicable: true, Swept: true, Complete: true},
			sequence: []string{cleanupCallGetNAT, cleanupCallGetServices}, natKept: true},
		{name: "not owned: left alone with its resources", setups: []func(c *fakeARM){disown},
			want: egressCleanupSummary{NotApplicable: true, Swept: true, Complete: true}, sequence: []string{cleanupCallGetNAT}, natKept: true},
		{name: "another identity's: left alone", setups: []func(c *fakeARM){func(c *fakeARM) {
			c.natLocked(rg, egressCleanupTestID).Tags = cleanupOtherTag()
		}}, want: egressCleanupSummary{NotApplicable: true, Swept: true, Complete: true}, sequence: []string{cleanupCallGetNAT}, natKept: true},
		{name: "legacy NAT Gateway (untagged default shape) is the controller's", setups: []func(c *fakeARM){func(c *fakeARM) {
			delete(c.natLocked(rg, egressCleanupTestID).Tags, EgressIdentityTagKey)
			delete(c.pips[fakeARMKey(rg, "egress-a-pip")].Tags, EgressIdentityTagKey)
		}}, want: egressCleanupSummary{NATGone: true, Swept: true, Complete: true},
			sequence: unlinkAndDelete, reads: true, deletes: []string{"egress-a-pip", "egress-a-pip-2"}},
		{name: "no NAT Gateway: the reserved names only", setups: []func(c *fakeARM){func(c *fakeARM) {
			delete(c.nats, fakeARMKey(rg, egressCleanupTestID))
		}}, want: egressCleanupSummary{NATGone: true, Swept: true, Complete: true},
			sequence: []string{cleanupCallGetNAT}, reads: true, deletes: []string{"egress-a-pip", "egress-a-pip-2"}},
		{name: "no Service Gateway: no service uses it", setups: []func(c *fakeARM){func(c *fakeARM) { c.noServiceGateway = true }},
			want:     egressCleanupSummary{NATGone: true, Swept: true, Complete: true},
			sequence: unlinkAndDelete, reads: true, deletes: []string{"egress-a-pip", "egress-a-pip-2"}},
		{name: "NAT Gateway unsettled: busy", setups: []func(c *fakeARM){setState(armnetwork.ProvisioningStateUpdating)},
			want: egressCleanupSummary{Busy: true}, sequence: []string{cleanupCallGetNAT}, natKept: true},
		{name: "NAT Gateway cannot be read", setups: []func(c *fakeARM){fault(cleanupCallGetNAT, armReplyError(http.StatusForbidden, "AuthorizationFailed"))},
			want: egressCleanupSummary{Err: true}, sequence: []string{cleanupCallGetNAT}, natKept: true},
		{name: "registrations cannot be read", setups: []func(c *fakeARM){fault(cleanupCallGetServices, armReplyError(http.StatusForbidden, "AuthorizationFailed"))},
			want: egressCleanupSummary{Err: true}, sequence: []string{cleanupCallGetNAT, cleanupCallGetServices}, natKept: true},
		{name: "unlink with an unknown outcome: the run ends", setups: []func(c *fakeARM){fault(cleanupCallPutNAT, armDropConnection)},
			want:     egressCleanupSummary{CommitUnknown: true, Err: true},
			sequence: []string{cleanupCallGetNAT, cleanupCallGetServices, cleanupCallPutNAT}, natKept: true},
		{name: "unlink refused: the run ends", setups: []func(c *fakeARM){fault(cleanupCallPutNAT, armReplyError(http.StatusForbidden, "AuthorizationFailed"))},
			want: egressCleanupSummary{Err: true}, sequence: []string{cleanupCallGetNAT, cleanupCallGetServices, cleanupCallPutNAT}, natKept: true},
		{name: "fresh read unsettled: busy", setups: []func(c *fakeARM){after(cleanupCallPutNAT, setState(armnetwork.ProvisioningStateUpdating))},
			want:     egressCleanupSummary{Busy: true},
			sequence: []string{cleanupCallGetNAT, cleanupCallGetServices, cleanupCallPutNAT, cleanupCallGetNAT}, natKept: true},
		{name: "fresh read no longer the controller's: left alone", setups: []func(c *fakeARM){after(cleanupCallPutNAT, disown)},
			want:     egressCleanupSummary{NotApplicable: true, Swept: true, Complete: true},
			sequence: []string{cleanupCallGetNAT, cleanupCallGetServices, cleanupCallPutNAT, cleanupCallGetNAT}, natKept: true},
		{name: "fresh read fails", setups: []func(c *fakeARM){after(cleanupCallPutNAT,
			fault(cleanupCallGetNAT, armReplyError(http.StatusForbidden, "AuthorizationFailed")))},
			want:     egressCleanupSummary{Err: true},
			sequence: []string{cleanupCallGetNAT, cleanupCallGetServices, cleanupCallPutNAT, cleanupCallGetNAT}, natKept: true},
		{name: "gone at the fresh read: the reserved names only", setups: []func(c *fakeARM){after(cleanupCallPutNAT, func(c *fakeARM) {
			delete(c.nats, fakeARMKey(rg, egressCleanupTestID))
		})}, want: egressCleanupSummary{NATGone: true, Swept: true, Complete: true},
			sequence: []string{cleanupCallGetNAT, cleanupCallGetServices, cleanupCallPutNAT, cleanupCallGetNAT}, reads: true,
			deletes: []string{"egress-a-pip", "egress-a-pip-2"}},
		{name: "written after the fresh read: the DELETE is refused (412)", setups: []func(c *fakeARM){after(cleanupCallPutNAT,
			after(cleanupCallGetNAT, func(c *fakeARM) { c.natLocked(rg, egressCleanupTestID).Etag = c.nextEtagLocked() }))},
			want: egressCleanupSummary{Err: true}, sequence: unlinkAndDelete, natKept: true},
		{name: "DELETE with an unknown outcome: nothing swept", setups: []func(c *fakeARM){
			fault(cleanupCallDeleteNAT, armReplyError(http.StatusInternalServerError, "InternalServerError"))},
			want: egressCleanupSummary{CommitUnknown: true, Err: true}, sequence: unlinkAndDelete, natKept: true},
		{name: "deleted, a leftover DELETE refused: incomplete", setups: []func(c *fakeARM){
			fault("DELETE publicIPAddresses/egress-a-pip-2", armReplyError(http.StatusConflict, "Conflict"))},
			want:     egressCleanupSummary{NATGone: true, Err: true},
			sequence: unlinkAndDelete, reads: true, deletes: []string{"egress-a-pip", "egress-a-pip-2"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newResidue(t)
			for _, setup := range tc.setups {
				c.update(setup)
			}
			before := snap(c)

			result := c.dt.cleanupBYOResidue(ctx, egressCleanupTestID)
			assert.Equal(t, tc.want, summarizeCleanup(result), "err: %v", result.Err)
			assert.Equal(t, tc.sequence, c.sequence())
			var reads []string
			if tc.reads {
				reads = cleanupReservedReads(true)
			}
			assert.Equal(t, reads, c.reservedCalls(http.MethodGet))
			assert.Equal(t, cleanupDeletes(tc.deletes...), c.reservedCalls(http.MethodDelete))
			assert.Equal(t, tc.natKept, c.nat(rg, egressCleanupTestID) != nil, "NAT Gateway kept")
			assert.Equal(t, len(c.writes()) > 0, c.mutationNoted(), "a mutation is noted exactly when a write is sent")
			for _, put := range c.requestsOf(cleanupCallPutNAT) {
				assert.Empty(t, put.IfMatch)
				assert.Contains(t, string(put.Body), `"serviceGateway":null`)
			}
			if tc.natKept && tc.want.NotApplicable {
				pips, _ := c.resourceNames()
				assert.Equal(t, []string{"egress-a-pip", "egress-a-pip-2", "egress-a-pip-3"}, pips, "its resources are left alone")
			}
			untouched(t, c, before)
		})
	}
}
