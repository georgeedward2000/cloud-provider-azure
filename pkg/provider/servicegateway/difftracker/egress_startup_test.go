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
	"maps"
	"net/http"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork/v12"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"

	"sigs.k8s.io/cloud-provider-azure/pkg/azclient/loadbalancerclient/mock_loadbalancerclient"
	"sigs.k8s.io/cloud-provider-azure/pkg/azclient/mock_azclient"
	"sigs.k8s.io/cloud-provider-azure/pkg/azclient/natgatewayclient/mock_natgatewayclient"
	"sigs.k8s.io/cloud-provider-azure/pkg/azclient/publicipaddressclient/mock_publicipaddressclient"
	"sigs.k8s.io/cloud-provider-azure/pkg/azclient/publicipprefixclient/mock_publicipprefixclient"
	"sigs.k8s.io/cloud-provider-azure/pkg/azclient/servicegatewayclient/mock_servicegatewayclient"
	"sigs.k8s.io/cloud-provider-azure/pkg/consts"
	utilsets "sigs.k8s.io/cloud-provider-azure/pkg/util/sets"
)

const (
	startupIdentity = "egress-a"
	startupBYO      = "egress-byo"
	startupGone     = "egress-gone"
	startupOther    = "egress-other"
)

// egressCloud is what Azure holds of the egress identities, as its listings return it: the NAT Gateways, Public IPs
// and Public IP prefixes of the node resource group and the Service Gateway's services. Listings return copies.
type egressCloud struct {
	t        *testing.T
	nats     map[string]*armnetwork.NatGateway
	pips     map[string]*armnetwork.PublicIPAddress
	prefixes map[string]*armnetwork.PublicIPPrefix
	services map[string]*armnetwork.ServiceGatewayService
}

func newEgressCloud(t *testing.T) *egressCloud {
	return &egressCloud{
		t:        t,
		nats:     map[string]*armnetwork.NatGateway{},
		pips:     map[string]*armnetwork.PublicIPAddress{},
		prefixes: map[string]*armnetwork.PublicIPPrefix{},
		services: map[string]*armnetwork.ServiceGatewayService{},
	}
}

// applied stores what applying cfg to egress identity id creates, as the projections write it and Azure lists it
// (Succeeded): its NAT Gateway, linked to the cluster's Service Gateway, and its managed Public IPs and prefixes. The
// identity is registered with its NAT Gateway.
func (c *egressCloud) applied(id string, cfg EgressConfig, ipv6 bool) *egressCloud {
	c.t.Helper()
	dt := testConfig()
	nat, err := natRequest(nil, id, cfg, ipv6, dt)
	if err != nil {
		c.t.Fatal(err)
	}
	nat.Etag = to.Ptr(`W/"1"`)
	nat.Properties.ProvisioningState = to.Ptr(armnetwork.ProvisioningStateSucceeded)
	c.nats[strings.ToLower(id)] = &nat
	for _, v6 := range egressServedFamilies(ipv6) {
		for _, m := range cfg.familyConfig(v6).managed(v6) {
			if m.Kind == egressManagedPrefix {
				prefix, err := managedPrefixRequest(nil, id, v6, m.N, cfg, dt)
				if err != nil {
					c.t.Fatal(err)
				}
				prefix.Properties.ProvisioningState = to.Ptr(armnetwork.ProvisioningStateSucceeded)
				c.prefixes[strings.ToLower(*prefix.Name)] = &prefix
				continue
			}
			pip, err := managedPublicIPRequest(nil, id, v6, m.N, cfg, dt)
			if err != nil {
				c.t.Fatal(err)
			}
			pip.Properties.ProvisioningState = to.Ptr(armnetwork.ProvisioningStateSucceeded)
			c.pips[strings.ToLower(*pip.Name)] = &pip
		}
	}
	c.register(id, natGatewayID(id, dt))
	return c
}

// legacy stores the NAT Gateway of egress identity id as created before the controller tagged its resources: untagged,
// referencing only its untagged base Public IP. The identity is registered with it.
func (c *egressCloud) legacy(id string) *egressCloud {
	dt := testConfig()
	pip := egressStartupPIP(PublicIPName(id), "", armnetwork.ProvisioningStateSucceeded)
	c.pips[strings.ToLower(*pip.Name)] = pip
	c.nats[strings.ToLower(id)] = &armnetwork.NatGateway{
		ID:       to.Ptr(natGatewayID(id, dt)),
		Name:     to.Ptr(id),
		Location: to.Ptr(dt.Location),
		SKU:      &armnetwork.NatGatewaySKU{Name: to.Ptr(armnetwork.NatGatewaySKUNameStandardV2)},
		Etag:     to.Ptr(`W/"1"`),
		Properties: &armnetwork.NatGatewayPropertiesFormat{
			ProvisioningState: to.Ptr(armnetwork.ProvisioningStateSucceeded),
			PublicIPAddresses: []*armnetwork.SubResource{{ID: pip.ID}},
			ServiceGateway:    &armnetwork.SubResource{ID: to.Ptr(dt.ServiceGatewayResourceID())},
		},
	}
	c.register(id, natGatewayID(id, dt))
	return c
}

// register records the Service Gateway service of egress identity id, referencing natID ("" for none).
func (c *egressCloud) register(id, natID string) {
	service := &armnetwork.ServiceGatewayService{
		Name:       to.Ptr(id),
		Properties: &armnetwork.ServiceGatewayServicePropertiesFormat{ServiceType: to.Ptr(armnetwork.ServiceTypeOutbound)},
	}
	if natID != "" {
		service.Properties.PublicNatGatewayID = to.Ptr(natID)
	}
	c.services[strings.ToLower(id)] = service
}

// nat returns the stored NAT Gateway of id, to be changed in place.
func (c *egressCloud) nat(id string) *armnetwork.NatGateway {
	c.t.Helper()
	nat := c.nats[strings.ToLower(id)]
	if nat == nil {
		c.t.Fatalf("no NAT Gateway %q", id)
	}
	return nat
}

// ownedNAT stores a NAT Gateway named id that carries its ownership tag and references nothing, unlinked.
func (c *egressCloud) ownedNAT(id string) {
	c.nats[strings.ToLower(id)] = &armnetwork.NatGateway{
		ID:         to.Ptr(natGatewayID(id, testConfig())),
		Name:       to.Ptr(id),
		Tags:       egressIdentityTags(id),
		Etag:       to.Ptr(`W/"1"`),
		Properties: &armnetwork.NatGatewayPropertiesFormat{ProvisioningState: to.Ptr(armnetwork.ProvisioningStateSucceeded)},
	}
}

func (c *egressCloud) addPIP(name, owner string, state armnetwork.ProvisioningState) {
	c.pips[strings.ToLower(name)] = egressStartupPIP(name, owner, state)
}

func (c *egressCloud) addPrefix(name, owner string, length int32, state armnetwork.ProvisioningState) {
	c.prefixes[strings.ToLower(name)] = egressStartupPrefix(name, owner, length, state)
}

// egressStartupPIP returns a StandardV2 static IPv4 Public IP of the node resource group, tagged as owned by owner
// unless it is "".
func egressStartupPIP(name, owner string, state armnetwork.ProvisioningState) *armnetwork.PublicIPAddress {
	pip := &armnetwork.PublicIPAddress{
		ID:       to.Ptr(publicIPAddressID(name, testConfig())),
		Name:     to.Ptr(name),
		Location: to.Ptr(testConfig().Location),
		SKU:      &armnetwork.PublicIPAddressSKU{Name: to.Ptr(armnetwork.PublicIPAddressSKUNameStandardV2)},
		Properties: &armnetwork.PublicIPAddressPropertiesFormat{
			ProvisioningState:        to.Ptr(state),
			PublicIPAllocationMethod: to.Ptr(armnetwork.IPAllocationMethodStatic),
			PublicIPAddressVersion:   to.Ptr(armnetwork.IPVersionIPv4),
		},
	}
	if owner != "" {
		pip.Tags = egressIdentityTags(owner)
	}
	return pip
}

// egressStartupPrefix returns a StandardV2 IPv4 Public IP prefix of the node resource group, tagged as owned by owner
// unless it is "".
func egressStartupPrefix(name, owner string, length int32, state armnetwork.ProvisioningState) *armnetwork.PublicIPPrefix {
	prefix := &armnetwork.PublicIPPrefix{
		ID:       to.Ptr(publicIPPrefixID(name, testConfig())),
		Name:     to.Ptr(name),
		Location: to.Ptr(testConfig().Location),
		SKU:      &armnetwork.PublicIPPrefixSKU{Name: to.Ptr(armnetwork.PublicIPPrefixSKUNameStandardV2)},
		Properties: &armnetwork.PublicIPPrefixPropertiesFormat{
			ProvisioningState:      to.Ptr(state),
			PrefixLength:           to.Ptr(length),
			PublicIPAddressVersion: to.Ptr(armnetwork.IPVersionIPv4),
		},
	}
	if owner != "" {
		prefix.Tags = egressIdentityTags(owner)
	}
	return prefix
}

// cloneListed returns a copy of a listed resource, as a new listing returns it.
func cloneListed[T any](t *testing.T, v *T) *T {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	var out T
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatal(err)
	}
	return &out
}

func listedSorted[T any](t *testing.T, resources map[string]*T) []*T {
	var out []*T
	for _, key := range slices.Sorted(maps.Keys(resources)) {
		out = append(out, cloneListed(t, resources[key]))
	}
	return out
}

func (c *egressCloud) natList() []*armnetwork.NatGateway {
	return listedSorted(c.t, c.nats)
}

func (c *egressCloud) pipList() []*armnetwork.PublicIPAddress {
	return listedSorted(c.t, c.pips)
}

func (c *egressCloud) prefixList() []*armnetwork.PublicIPPrefix {
	return listedSorted(c.t, c.prefixes)
}

func (c *egressCloud) serviceList() []*armnetwork.ServiceGatewayService {
	return listedSorted(c.t, c.services)
}

// seedNRP records the cloud's registrations in the NRP state of dt as startup reads them (fetchServiceGatewayServices,
// recordLinkedBYONATGateways and New).
func (c *egressCloud) seedNRP(dt *DiffTracker) {
	for _, service := range c.serviceList() {
		name := *service.Name
		dt.NRPResources.NATGateways.Insert(name)
		natID := derefString(service.Properties.PublicNatGatewayID)
		if natID == "" {
			continue
		}
		if dt.NRPResources.OutboundNATGatewayIDs == nil {
			dt.NRPResources.OutboundNATGatewayIDs = map[string]string{}
		}
		dt.NRPResources.OutboundNATGatewayIDs[strings.ToLower(name)] = natID
		if isBYONATGatewayID(natID, dt.config) {
			dt.byoNATGateways[strings.ToLower(name)] = natID
		}
	}
}

// startupListing returns the listing startup takes of the cloud, with the registrations of dt's NRP state.
func (c *egressCloud) startupListing(dt *DiffTracker) *egressListing {
	return newStartupEgressListing(dt.NRPResources, c.natList(), c.pipList(), c.prefixList())
}

// newStartupTracker returns a tracker of an IPv4 cluster, or a dual-stack one, initializing. As on every Service
// Gateway cluster, the default outbound service is recorded as unmanaged.
func newStartupTracker(ipv6 bool) *DiffTracker {
	dt := newTestDiffTracker()
	dt.config = testConfig()
	dt.NRPResources.UnmanagedNATGateways = utilsets.NewString(DefaultOutboundNATGatewayName)
	dt.byoNATGateways = map[string]string{}
	dt.clusterHasIPv6.Store(ipv6)
	atomic.StoreInt32(&dt.isInitializing, 1)
	dt.initCompletionChecker = make(chan struct{})
	return dt
}

// startupMembers makes one pod a member of identity for each vote, created in order (nil: an invalid vote), as
// startup admits and seeds them.
func startupMembers(dt *DiffTracker, identity string, votes ...*EgressConfig) {
	for i, cfg := range votes {
		castEgressVote(dt, identity, fmt.Sprintf("ns/%s-p%d", identity, i), cfg, consensusEpoch.Add(time.Duration(i)*time.Minute))
	}
	if len(votes) > 0 {
		dt.K8sResources.Egresses.Insert(identity)
	}
}

// egressStartupOp describes the operation of an egress identity for comparison.
type egressStartupOp struct {
	State    ResourceState
	Applied  EgressConfig
	Desired  EgressConfig
	Owed     bool
	Baseline bool
}

func describeEgressStartupOp(op *ServiceOperationState) egressStartupOp {
	return egressStartupOp{State: op.State, Applied: appliedEgress(op), Desired: outboundEgress(op.Config.OutboundConfig),
		Owed: op.Egress.NeedsObservation, Baseline: op.Egress.StartupBaseline}
}

// TestAdoptStartupEgressIdentities pins how startup adopts the registered egress identities from its listing (4.6 step
// 3): the mode by ownership, and for a managed identity with members a Created operation recording the configuration
// its settled NAT Gateway satisfies (the agreed one, else the default, else unknown) and owing an observation for
// unsettled reads, garbage and a missing side of the link; only what is actionable starts a run, through the tracked
// trigger. Nothing raises an event.
func TestAdoptStartupEgressIdentities(t *testing.T) {
	dflt := defaultEgressConfig(egressParseContext{})
	dfltDual := defaultEgressConfig(egressParseContext{WantsIPv6: true})
	a, b := consensusA, consensusB
	tagged := dflt
	tagged.Tags = "team=net"
	byoID := byoNATGatewayARMID(startupIdentity)

	type want struct {
		mode    *egressMode
		op      *egressStartupOp
		trigger bool
	}
	managed, byo := egressModeManaged, egressModeBYO
	created := func(applied, desired EgressConfig, owed bool) *egressStartupOp {
		return &egressStartupOp{State: StateCreated, Applied: applied, Desired: desired, Owed: owed, Baseline: true}
	}
	updating := func(applied, desired EgressConfig, owed bool) *egressStartupOp {
		return &egressStartupOp{State: StateUpdateInProgress, Applied: applied, Desired: desired, Owed: owed, Baseline: true}
	}

	cases := []struct {
		name  string
		ipv6  bool
		setup func(c *egressCloud, dt *DiffTracker)
		want  want
	}{
		{
			name: "satisfied agreed configuration: no run",
			setup: func(c *egressCloud, dt *DiffTracker) {
				c.applied(startupIdentity, a, false)
				startupMembers(dt, startupIdentity, &a, &a)
			},
			want: want{mode: &managed, op: created(a, a, false)},
		},
		{
			name: "satisfied agreed configuration, registered with its NAT Gateway spelled in another case: no run",
			setup: func(c *egressCloud, dt *DiffTracker) {
				c.applied(startupIdentity, a, false)
				c.register(startupIdentity, strings.ToUpper(natGatewayID(startupIdentity, dt.config)))
				startupMembers(dt, startupIdentity, &a, &a)
			},
			want: want{mode: &managed, op: created(a, a, false)},
		},
		{
			// natSatisfies matches tags by presence, so the NAT Gateway satisfies the default too: the agreed one comes first.
			name: "satisfied agreed configuration that differs from the default only by its tags: no run",
			setup: func(c *egressCloud, dt *DiffTracker) {
				c.applied(startupIdentity, tagged, false)
				startupMembers(dt, startupIdentity, &tagged, &tagged)
			},
			want: want{mode: &managed, op: created(tagged, tagged, false)},
		},
		{
			name: "satisfied default, another configuration agreed: applied",
			setup: func(c *egressCloud, dt *DiffTracker) {
				c.applied(startupIdentity, dflt, false)
				startupMembers(dt, startupIdentity, &a, &a)
			},
			want: want{mode: &managed, op: updating(dflt, a, false), trigger: true},
		},
		{
			name: "satisfied default without consensus: no run",
			setup: func(c *egressCloud, dt *DiffTracker) {
				c.applied(startupIdentity, dflt, false)
				startupMembers(dt, startupIdentity, &a, &b)
			},
			want: want{mode: &managed, op: created(dflt, dflt, false)},
		},
		{
			name: "dual-stack satisfied default without consensus: no run",
			ipv6: true,
			setup: func(c *egressCloud, dt *DiffTracker) {
				c.applied(startupIdentity, dfltDual, true)
				startupMembers(dt, startupIdentity, &a, &b)
			},
			want: want{mode: &managed, op: created(dfltDual, dfltDual, false)},
		},
		{
			name: "unknown without consensus: no run",
			setup: func(c *egressCloud, dt *DiffTracker) {
				c.applied(startupIdentity, a, false)
				c.nat(startupIdentity).Properties.IdleTimeoutInMinutes = to.Ptr[int32](7)
				startupMembers(dt, startupIdentity, &a, &b)
			},
			want: want{mode: &managed, op: created(egressUnknown, egressUnknown, false)},
		},
		{
			name: "drift from the agreed configuration: applied",
			setup: func(c *egressCloud, dt *DiffTracker) {
				c.applied(startupIdentity, a, false)
				c.nat(startupIdentity).Properties.IdleTimeoutInMinutes = to.Ptr[int32](7)
				startupMembers(dt, startupIdentity, &a, &a)
			},
			want: want{mode: &managed, op: updating(egressUnknown, a, false), trigger: true},
		},
		{
			name: "unsettled NAT Gateway: unknown and observed",
			setup: func(c *egressCloud, dt *DiffTracker) {
				c.applied(startupIdentity, a, false)
				c.nat(startupIdentity).Properties.ProvisioningState = to.Ptr(armnetwork.ProvisioningStateUpdating)
				startupMembers(dt, startupIdentity, &a, &b)
			},
			want: want{mode: &managed, op: updating(egressUnknown, egressUnknown, true), trigger: true},
		},
		{
			name: "unsettled reserved Public IP: unknown and owed",
			setup: func(c *egressCloud, dt *DiffTracker) {
				c.applied(startupIdentity, a, false)
				c.addPIP(managedPublicIPName(startupIdentity, false, 3), startupIdentity, armnetwork.ProvisioningStateCreating)
				startupMembers(dt, startupIdentity, &a, &b)
			},
			want: want{mode: &managed, op: updating(egressUnknown, egressUnknown, true), trigger: true},
		},
		{
			name: "unsettled referenced Public IP: unknown and owed",
			setup: func(c *egressCloud, dt *DiffTracker) {
				c.applied(startupIdentity, a, false)
				c.pips[strings.ToLower(managedPublicIPName(startupIdentity, false, 2))].Properties.ProvisioningState = to.Ptr(armnetwork.ProvisioningStateUpdating)
				startupMembers(dt, startupIdentity, &a, &a)
			},
			want: want{mode: &managed, op: updating(egressUnknown, a, true), trigger: true},
		},
		{
			name: "unsettled reserved prefix: unknown and owed",
			setup: func(c *egressCloud, dt *DiffTracker) {
				c.applied(startupIdentity, a, false)
				c.addPrefix(managedPrefixName(startupIdentity, false, 30), startupIdentity, 30, armnetwork.ProvisioningStateDeleting)
				startupMembers(dt, startupIdentity, &a, &a)
			},
			want: want{mode: &managed, op: updating(egressUnknown, a, true), trigger: true},
		},
		{
			name: "listed garbage Public IP: observe-and-clean",
			setup: func(c *egressCloud, dt *DiffTracker) {
				c.applied(startupIdentity, a, false)
				c.addPIP(managedPublicIPName(startupIdentity, false, 3), startupIdentity, armnetwork.ProvisioningStateSucceeded)
				startupMembers(dt, startupIdentity, &a, &a)
			},
			want: want{mode: &managed, op: updating(a, egressUnknown, true), trigger: true},
		},
		{
			name: "listed garbage prefix: observe-and-clean",
			setup: func(c *egressCloud, dt *DiffTracker) {
				c.applied(startupIdentity, a, false)
				c.addPrefix(managedPrefixName(startupIdentity, false, 30), startupIdentity, 30, armnetwork.ProvisioningStateFailed)
				startupMembers(dt, startupIdentity, &a, &a)
			},
			want: want{mode: &managed, op: updating(a, egressUnknown, true), trigger: true},
		},
		{
			name: "unreferenced untagged base Public IP is the identity's by its name: observe-and-clean",
			setup: func(c *egressCloud, dt *DiffTracker) {
				c.applied(startupIdentity, b, false)
				c.addPIP(PublicIPName(startupIdentity), "", armnetwork.ProvisioningStateSucceeded)
				startupMembers(dt, startupIdentity, &b, &b)
			},
			want: want{mode: &managed, op: updating(b, egressUnknown, true), trigger: true},
		},
		{
			name: "untagged numbered Public IP is not the controller's: no run",
			setup: func(c *egressCloud, dt *DiffTracker) {
				c.applied(startupIdentity, a, false)
				c.addPIP(managedPublicIPName(startupIdentity, false, 3), "", armnetwork.ProvisioningStateSucceeded)
				c.addPrefix(managedPrefixName(startupIdentity, false, 29), startupOther, 29, armnetwork.ProvisioningStateSucceeded)
				startupMembers(dt, startupIdentity, &a, &a)
			},
			want: want{mode: &managed, op: created(a, a, false)},
		},
		{
			name: "managed prefix configuration read from the prefix listing",
			setup: func(c *egressCloud, dt *DiffTracker) {
				c.applied(startupIdentity, b, false)
				startupMembers(dt, startupIdentity, &b, &b)
			},
			want: want{mode: &managed, op: created(b, b, false)},
		},
		{
			name: "managed prefix missing from the listing: applied again",
			setup: func(c *egressCloud, dt *DiffTracker) {
				c.applied(startupIdentity, b, false)
				clear(c.prefixes)
				startupMembers(dt, startupIdentity, &b, &b)
			},
			want: want{mode: &managed, op: updating(egressUnknown, b, false), trigger: true},
		},
		{
			name: "legacy untagged default identity: no write",
			setup: func(c *egressCloud, dt *DiffTracker) {
				c.legacy(startupIdentity)
				startupMembers(dt, startupIdentity, &dflt, &dflt)
			},
			want: want{mode: &managed, op: created(dflt, dflt, false)},
		},
		{
			name: "dual-stack default identity: no write",
			ipv6: true,
			setup: func(c *egressCloud, dt *DiffTracker) {
				c.applied(startupIdentity, dfltDual, true)
				startupMembers(dt, startupIdentity, &dfltDual)
			},
			want: want{mode: &managed, op: created(dfltDual, dfltDual, false)},
		},
		{
			name: "interrupted unlink, registration without its NAT Gateway: link repair",
			setup: func(c *egressCloud, dt *DiffTracker) {
				c.applied(startupIdentity, a, false)
				c.register(startupIdentity, "")
				startupMembers(dt, startupIdentity, &a, &a)
			},
			want: want{mode: &managed, op: updating(a, egressUnknown, true), trigger: true},
		},
		{
			name: "interrupted unlink, NAT Gateway unlinked too, agreeing votes",
			setup: func(c *egressCloud, dt *DiffTracker) {
				c.applied(startupIdentity, a, false)
				c.register(startupIdentity, "")
				c.nat(startupIdentity).Properties.ServiceGateway = nil
				startupMembers(dt, startupIdentity, &a, &a)
			},
			want: want{mode: &managed, op: updating(egressUnknown, a, true), trigger: true},
		},
		{
			name: "interrupted unlink, NAT Gateway unlinked too, conflicting votes",
			setup: func(c *egressCloud, dt *DiffTracker) {
				c.applied(startupIdentity, a, false)
				c.register(startupIdentity, "")
				c.nat(startupIdentity).Properties.ServiceGateway = nil
				startupMembers(dt, startupIdentity, &a, &b)
			},
			want: want{mode: &managed, op: updating(egressUnknown, egressUnknown, true), trigger: true},
		},
		{
			name: "interrupted unlink, NAT Gateway unlinked too, invalid vote",
			setup: func(c *egressCloud, dt *DiffTracker) {
				c.applied(startupIdentity, a, false)
				c.register(startupIdentity, "")
				c.nat(startupIdentity).Properties.ServiceGateway = nil
				startupMembers(dt, startupIdentity, &a, nil)
			},
			want: want{mode: &managed, op: updating(egressUnknown, egressUnknown, true), trigger: true},
		},
		{
			name: "NAT Gateway unlinked, registration intact: link repair",
			setup: func(c *egressCloud, dt *DiffTracker) {
				c.applied(startupIdentity, a, false)
				c.nat(startupIdentity).Properties.ServiceGateway = nil
				startupMembers(dt, startupIdentity, &a, &b)
			},
			want: want{mode: &managed, op: updating(egressUnknown, egressUnknown, true), trigger: true},
		},
		{
			name: "interrupted unlink without members: deleted as before",
			setup: func(c *egressCloud, _ *DiffTracker) {
				c.applied(startupIdentity, a, false)
				c.register(startupIdentity, "")
				c.nat(startupIdentity).Properties.ServiceGateway = nil
			},
			want: want{mode: &managed},
		},
		{
			name: "registered identity without members: deleted as before",
			setup: func(c *egressCloud, _ *DiffTracker) {
				c.applied(startupIdentity, a, false)
			},
			want: want{mode: &managed},
		},
		{
			name: "NAT Gateway missing: unchanged",
			setup: func(c *egressCloud, dt *DiffTracker) {
				c.register(startupIdentity, natGatewayID(startupIdentity, testConfig()))
				startupMembers(dt, startupIdentity, &a)
			},
		},
		{
			name: "NAT Gateway not the controller's: unchanged",
			setup: func(c *egressCloud, dt *DiffTracker) {
				c.applied(startupIdentity, a, false)
				nat := c.nat(startupIdentity)
				nat.Tags = nil
				nat.Properties.PublicIPAddresses = []*armnetwork.SubResource{{ID: to.Ptr(publicIPAddressID("corp-ip", testConfig()))}}
				startupMembers(dt, startupIdentity, &a)
			},
		},
		{
			name: "NAT Gateway recorded as unmanaged: unchanged",
			setup: func(c *egressCloud, dt *DiffTracker) {
				c.applied(startupIdentity, a, false)
				dt.NRPResources.UnmanagedNATGateways.Insert(startupIdentity)
				startupMembers(dt, startupIdentity, &a)
			},
		},
		{
			name: "NAT Gateway listed without its properties, its families unknown: unchanged",
			setup: func(c *egressCloud, dt *DiffTracker) {
				c.applied(startupIdentity, a, false)
				c.nat(startupIdentity).Properties = nil
				startupMembers(dt, startupIdentity, &a)
			},
		},
		{
			name: "identity colliding with an inbound service: unchanged",
			setup: func(c *egressCloud, dt *DiffTracker) {
				c.applied(startupIdentity, a, false)
				startupMembers(dt, startupIdentity, &a)
				dt.K8sResources.Services.Insert(startupIdentity)
			},
		},
		{
			name: "BYO identity: its mode only",
			setup: func(c *egressCloud, dt *DiffTracker) {
				c.register(startupIdentity, byoID)
				startupMembers(dt, startupIdentity, &a)
			},
			want: want{mode: &byo},
		},
		{
			name: "BYO identity with a leftover numbered Public IP: residue cleanup",
			setup: func(c *egressCloud, dt *DiffTracker) {
				c.register(startupIdentity, byoID)
				c.addPIP(managedPublicIPName(startupIdentity, false, 2), startupIdentity, armnetwork.ProvisioningStateSucceeded)
				startupMembers(dt, startupIdentity, &a)
			},
			want: want{mode: &byo, op: updating(egressUnknown, egressUnknown, true), trigger: true},
		},
		{
			name: "BYO identity with a leftover base Public IP: residue cleanup",
			setup: func(c *egressCloud, dt *DiffTracker) {
				c.register(startupIdentity, byoID)
				c.addPIP(PublicIPName(startupIdentity), "", armnetwork.ProvisioningStateSucceeded)
				startupMembers(dt, startupIdentity, &a)
			},
			want: want{mode: &byo, op: updating(egressUnknown, egressUnknown, true), trigger: true},
		},
		{
			name: "BYO identity with a leftover NAT Gateway: residue cleanup",
			setup: func(c *egressCloud, dt *DiffTracker) {
				c.register(startupIdentity, byoID)
				c.ownedNAT(startupIdentity)
				startupMembers(dt, startupIdentity, &a)
			},
			want: want{mode: &byo, op: updating(egressUnknown, egressUnknown, true), trigger: true},
		},
		{
			name: "BYO identity whose same-name NAT Gateway a service uses: left alone",
			setup: func(c *egressCloud, dt *DiffTracker) {
				c.register(startupIdentity, byoID)
				c.ownedNAT(startupIdentity)
				c.addPIP(managedPublicIPName(startupIdentity, false, 2), startupIdentity, armnetwork.ProvisioningStateSucceeded)
				c.register(startupOther, natGatewayID(startupIdentity, testConfig()))
				startupMembers(dt, startupIdentity, &a)
			},
			want: want{mode: &byo},
		},
		{
			name: "BYO identity with leftovers and no members: owed by its deletion",
			setup: func(c *egressCloud, _ *DiffTracker) {
				c.register(startupIdentity, byoID)
				c.addPrefix(managedPrefixName(startupIdentity, false, 31), startupIdentity, 31, armnetwork.ProvisioningStateSucceeded)
			},
			want: want{mode: &byo, op: created(egressUnknown, egressUnknown, true)},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dt := newStartupTracker(tc.ipv6)
			c := newEgressCloud(t)
			tc.setup(c, dt)
			c.seedNRP(dt)

			dt.adoptStartupEgressIdentities(c.startupListing(dt))

			mode, known := dt.egress.mode[startupIdentity]
			if tc.want.mode == nil {
				assert.False(t, known, "the mode must stay unresolved, got %v", mode)
			} else {
				assert.Equal(t, *tc.want.mode, mode)
			}
			op := dt.pendingServiceOps[startupIdentity]
			if tc.want.op == nil {
				assert.Nil(t, op, "no operation must be tracked")
			} else if assert.NotNil(t, op, "an operation must be tracked") {
				assert.Equal(t, *tc.want.op, describeEgressStartupOp(op))
				wantGen := uint64(0)
				if tc.want.op.Owed {
					wantGen = 1
				}
				assert.Equal(t, wantGen, op.Egress.ObservationGen, "an owed observation is a new obligation")
				if assert.NotNil(t, op.Config.OutboundConfig) && assert.NotNil(t, op.LastAppliedConfig) {
					assert.Equal(t, dt.outboundIPFamilies(), op.Config.OutboundConfig.IPFamilies, "the families its NAT Gateway serves, here the cluster's")
					assert.NotSame(t, op.Config.OutboundConfig, op.LastAppliedConfig.OutboundConfig, "published configs are never shared")
				}
			}
			wantTriggers := 0
			if tc.want.trigger {
				wantTriggers = 1
			}
			assert.Len(t, dt.serviceUpdaterTrigger, wantTriggers)
			assert.Equal(t, int32(wantTriggers), atomic.LoadInt32(&dt.pendingUpdaterTriggers), "a run started at startup is tracked")
			assert.Empty(t, dt.egress.pendingEvents, "the unchanged baseline raises no event")
		})
	}
}

// TestNATGatewayFamilies pins the IP families a NAT Gateway serves: IPv6 as well when it references an IPv6 Public IP or
// prefix, IPv4 alone otherwise, also when the listing shows nothing of it.
func TestNATGatewayFamilies(t *testing.T) {
	ref := []*armnetwork.SubResource{{ID: to.Ptr("ip")}}
	cases := []struct {
		name  string
		props *armnetwork.NatGatewayPropertiesFormat
		want  []string
	}{
		{name: "no properties", want: ipv4Families()},
		{name: "IPv4 Public IPs and prefixes", props: &armnetwork.NatGatewayPropertiesFormat{PublicIPAddresses: ref, PublicIPPrefixes: ref},
			want: ipv4Families()},
		{name: "an IPv6 Public IP", props: &armnetwork.NatGatewayPropertiesFormat{PublicIPAddresses: ref, PublicIPAddressesV6: ref},
			want: dualStackFamilies()},
		{name: "an IPv6 prefix", props: &armnetwork.NatGatewayPropertiesFormat{PublicIPAddresses: ref, PublicIPPrefixesV6: ref},
			want: dualStackFamilies()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, natGatewayFamilies(&armnetwork.NatGateway{Properties: tc.props}))
		})
	}
}

// startupPods returns the egress pods ns/<identity>-p<i> of identity, Running on an IPv4 node and created in order, with
// the egress annotations annotations[i], as startup lists them.
func startupPods(identity string, annotations ...map[string]string) *v1.PodList {
	pods := &v1.PodList{}
	for i, ann := range annotations {
		name := fmt.Sprintf("%s-p%d", identity, i)
		pods.Items = append(pods.Items, v1.Pod{
			ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: name, UID: types.UID(name + "-uid"),
				CreationTimestamp: metav1.NewTime(consensusEpoch.Add(time.Duration(i) * time.Minute)),
				Labels:            map[string]string{consts.PodLabelServiceEgressGateway: identity}, Annotations: ann},
			Status: v1.PodStatus{Phase: v1.PodRunning, HostIP: "10.0.0.1", PodIP: fmt.Sprintf("10.244.0.%d", i+1)},
		})
	}
	return pods
}

// TestAdoptStartupEgressIdentities_KeepsTheFamiliesOfTheNATGateway pins that an identity found in Azure keeps the IP
// families its NAT Gateway serves (IPv6 when it references an IPv6 Public IP or prefix, also while it is being written),
// whatever the Node snapshot of the restart shows, and that its members' votes are parsed for them, as its pods cast
// them from then on: a NAT Gateway that satisfies them gets no run, so none of its addresses is added or removed. A new
// identity takes the cluster's families.
func TestAdoptStartupEgressIdentities_KeepsTheFamiliesOfTheNATGateway(t *testing.T) {
	ipv4, dual := ipv4Families(), dualStackFamilies()
	dflt := defaultEgressConfig(egressParseContext{})
	dfltDual := defaultEgressConfig(egressParseContext{WantsIPv6: true})
	dualA := EgressConfig{IdleTimeoutMinutes: 10, IPv4: egressFamilyConfig{ManagedPublicIPs: 2}, IPv6: egressFamilyConfig{ManagedPublicIPs: 2}}
	dualAAnnotations := map[string]string{
		consts.PodAnnotationServiceEgressGatewayIdleTimeout:       "10",
		consts.PodAnnotationServiceEgressGatewayPublicIPCount:     "2",
		consts.PodAnnotationServiceEgressGatewayPublicIPCountIPv6: "2",
	}
	v6Prefix := EgressConfig{IdleTimeoutMinutes: 4, IPv4: egressFamilyConfig{ManagedPublicIPs: 1}, IPv6: egressFamilyConfig{ManagedPrefixLength: 127}}
	v6PrefixAnnotations := map[string]string{consts.PodAnnotationServiceEgressGatewayPublicIPPrefixLengthIPv6: "127"}
	created := func(applied EgressConfig) egressStartupOp {
		return egressStartupOp{State: StateCreated, Applied: applied, Desired: applied, Baseline: true}
	}

	cases := []struct {
		name         string
		clusterIPv6  bool                 // what the Node snapshot of the restart shows
		nat          func(c *egressCloud) // the identity's resources in Azure
		annotations  map[string]string    // of each of its two members
		wantFamilies []string
		wantVote     *EgressConfig // nil: invalid
		want         egressStartupOp
		trigger      bool // a run was started
	}{
		{
			name:         "dual-stack NAT Gateway, IPv4-only Node snapshot: both families kept, no run",
			nat:          func(c *egressCloud) { c.applied(startupIdentity, dfltDual, true) },
			wantFamilies: dual, wantVote: &dfltDual, want: created(dfltDual),
		},
		{
			name:         "IPv4 NAT Gateway, dual-stack Node snapshot: no IPv6 added, no run",
			clusterIPv6:  true,
			nat:          func(c *egressCloud) { c.applied(startupIdentity, dflt, false) },
			wantFamilies: ipv4, wantVote: &dflt, want: created(dflt),
		},
		{
			name:         "same families as the Node snapshot",
			clusterIPv6:  true,
			nat:          func(c *egressCloud) { c.applied(startupIdentity, dfltDual, true) },
			wantFamilies: dual, wantVote: &dfltDual, want: created(dfltDual),
		},
		{
			name:         "IPv6 settings of a dual-stack NAT Gateway, IPv4-only Node snapshot: valid",
			nat:          func(c *egressCloud) { c.applied(startupIdentity, dualA, true) },
			annotations:  dualAAnnotations,
			wantFamilies: dual, wantVote: &dualA, want: created(dualA),
		},
		{
			name:         "IPv6 served by a managed prefix alone, IPv4-only Node snapshot",
			nat:          func(c *egressCloud) { c.applied(startupIdentity, v6Prefix, true) },
			annotations:  v6PrefixAnnotations,
			wantFamilies: dual, wantVote: &v6Prefix, want: created(v6Prefix),
		},
		{
			name:         "IPv6 settings on an IPv4 NAT Gateway, dual-stack Node snapshot: invalid",
			clusterIPv6:  true,
			nat:          func(c *egressCloud) { c.applied(startupIdentity, dflt, false) },
			annotations:  dualAAnnotations,
			wantFamilies: ipv4, want: created(dflt),
		},
		{
			name: "dual-stack NAT Gateway being written, IPv4-only Node snapshot: both families kept",
			nat: func(c *egressCloud) {
				c.applied(startupIdentity, dfltDual, true)
				c.nat(startupIdentity).Properties.ProvisioningState = to.Ptr(armnetwork.ProvisioningStateUpdating)
			},
			wantFamilies: dual, wantVote: &dfltDual,
			want:    egressStartupOp{State: StateUpdateInProgress, Applied: egressUnknown, Desired: dfltDual, Owed: true, Baseline: true},
			trigger: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dt := newStartupTracker(tc.clusterIPv6)
			c := newEgressCloud(t)
			tc.nat(c)
			c.seedNRP(dt)
			pods := startupPods(startupIdentity, tc.annotations, tc.annotations, tc.annotations)
			pods.Items[2].Status.Phase = v1.PodFailed
			dt.K8sResources.Egresses.Insert(startupIdentity)
			dt.seedEgressVotes(pods)

			dt.adoptStartupEgressIdentities(c.startupListing(dt))

			op := dt.pendingServiceOps[startupIdentity]
			if !assert.NotNil(t, op, "the identity must be adopted") {
				return
			}
			assert.Equal(t, tc.wantFamilies, op.Config.OutboundConfig.IPFamilies, "the families its NAT Gateway serves")
			assert.Equal(t, tc.wantFamilies, op.LastAppliedConfig.OutboundConfig.IPFamilies)
			assert.Equal(t, tc.want, describeEgressStartupOp(op))
			assert.Equal(t, tc.trigger, len(dt.serviceUpdaterTrigger) > 0, "a run was started")
			for i := range pods.Items[:2] {
				pod := &pods.Items[i]
				vote, ok := dt.egress.votes.get(pod.Namespace + "/" + pod.Name)
				if !assert.True(t, ok, "%s votes", pod.Name) {
					continue
				}
				if tc.wantVote == nil {
					assert.NotEmpty(t, vote.Invalid, "%s: invalid for the families of the NAT Gateway", pod.Name)
				} else {
					assert.Empty(t, vote.Invalid, "%s", pod.Name)
					assert.Equal(t, *tc.wantVote, vote.Config, "%s", pod.Name)
				}
				dt.mu.Lock()
				pc := dt.egressParseContextOfLocked(startupIdentity)
				dt.mu.Unlock()
				cast := egressPodRefOf(pod).voteFor(startupIdentity, parseEgressVote(pod, pc))
				assert.True(t, sameEgressVote(vote, cast), "%s: the vote is the one its pod casts from now on", pod.Name)
			}
			_, voted := dt.egress.votes.get(pods.Items[2].Namespace + "/" + pods.Items[2].Name)
			assert.False(t, voted, "a pod that is no member does not vote")
			assertRecounted(t, dt.egress.votes)
			assert.Empty(t, dt.egress.pendingEvents, "the unchanged baseline raises no event")
		})
	}

	for _, clusterIPv6 := range []bool{false, true} {
		t.Run(fmt.Sprintf("a new identity takes the cluster's families (IPv6 nodes: %v)", clusterIPv6), func(t *testing.T) {
			dt := newStartupTracker(clusterIPv6)
			c := newEgressCloud(t)
			c.applied(startupIdentity, defaultEgressConfig(egressParseContext{WantsIPv6: !clusterIPv6}), !clusterIPv6)
			c.seedNRP(dt)
			pods := startupPods(startupIdentity, nil)
			pods.Items = append(pods.Items, startupPods(startupOther, nil).Items...)
			dt.K8sResources.Egresses.Insert(startupIdentity, startupOther)
			dt.seedEgressVotes(pods)

			dt.adoptStartupEgressIdentities(c.startupListing(dt))
			dt.reconcileServices(&SyncDiffTrackerReturnType{
				LoadBalancerUpdates: SyncServicesReturnType{Additions: utilsets.NewString(), Removals: utilsets.NewString()},
				NATGatewayUpdates:   SyncServicesReturnType{Additions: utilsets.NewString(startupOther), Removals: utilsets.NewString()},
			}, nil)

			cluster := dt.outboundIPFamilies()
			clusterDefault := defaultEgressConfig(egressParseContext{WantsIPv6: clusterIPv6})
			adopted, created := dt.pendingServiceOps[startupIdentity], dt.pendingServiceOps[startupOther]
			if assert.NotNil(t, adopted) && assert.NotNil(t, created) {
				assert.NotEqual(t, cluster, adopted.Config.OutboundConfig.IPFamilies, "the adopted identity keeps its own")
				assert.Equal(t, StateNotStarted, created.State)
				assert.Equal(t, cluster, created.Config.OutboundConfig.IPFamilies)
				dispatchWithoutRunning(t, dt)
				if assert.NotNil(t, created.InFlightConfig, "the creation is dispatched") {
					assert.Equal(t, clusterDefault, outboundEgress(created.InFlightConfig.OutboundConfig), "the creation configuration")
				}
			}
			vote, _ := dt.egress.votes.get("ns/" + startupOther + "-p0")
			assert.Equal(t, clusterDefault, vote.Config, "the new identity's votes are parsed for the cluster's families")
		})
	}
}

// TestEgressListings pins what the listings record of the Service Gateway's services: every outbound service but the
// default one is a registered egress identity, with the NAT Gateway it references; every referenced NAT Gateway, the
// default service's included, is used. Service types compare case-insensitively, and a service of an unknown type
// makes the confirmation listing incomplete. Leftover candidates are lower-cased.
func TestEgressListings(t *testing.T) {
	cfg := testConfig()
	service := func(name string, serviceType armnetwork.ServiceType, natID string, isDefault bool) *armnetwork.ServiceGatewayService {
		s := &armnetwork.ServiceGatewayService{Name: to.Ptr(name), Properties: &armnetwork.ServiceGatewayServicePropertiesFormat{
			ServiceType: to.Ptr(serviceType), IsDefault: to.Ptr(isDefault)}}
		if natID != "" {
			s.Properties.PublicNatGatewayID = to.Ptr(natID)
		}
		return s
	}
	services := []*armnetwork.ServiceGatewayService{
		service("Egress-A", armnetwork.ServiceType("outbound"), natGatewayID("egress-a", cfg), false),
		service("egress-unlinked", armnetwork.ServiceTypeOutbound, "", false),
		service("both", armnetwork.ServiceType("INBOUNDOUTBOUND"), natGatewayID("both", cfg), false),
		service("custom-default", armnetwork.ServiceTypeOutbound, natGatewayID("default-nat", cfg), true),
		service(DefaultOutboundNATGatewayName, armnetwork.ServiceTypeOutbound, "", false),
		service("inbound", armnetwork.ServiceType("inbound"), "", false),
		nil,
	}

	l, err := newEgressListing(services, nil, nil, nil)
	if assert.NoError(t, err) {
		assert.Equal(t, map[string]string{"egress-a": natGatewayID("egress-a", cfg), "egress-unlinked": "", "both": natGatewayID("both", cfg)},
			l.registrations)
		assert.True(t, l.natGatewayUsed("EGRESS-A", cfg))
		assert.True(t, l.natGatewayUsed("default-nat", cfg), "the default service uses its NAT Gateway")
		assert.False(t, l.natGatewayUsed("egress-unlinked", cfg))
	}

	_, err = newEgressListing([]*armnetwork.ServiceGatewayService{service("odd", armnetwork.ServiceType("Sideways"), "", false)}, nil, nil, nil)
	assert.Error(t, err, "a service of an unknown type makes the listing incomplete")

	nrp := emptyNRPState()
	nrp.NATGateways.Insert("Egress-A", "egress-unlinked")
	nrp.OutboundNATGatewayIDs = map[string]string{"egress-a": natGatewayID("egress-a", cfg)}
	startup := newStartupEgressListing(nrp, []*armnetwork.NatGateway{{Name: to.Ptr("Egress-A")}}, nil, nil)
	assert.Equal(t, map[string]string{"egress-a": natGatewayID("egress-a", cfg), "egress-unlinked": ""}, startup.registrations)
	assert.True(t, startup.natGatewayUsed("egress-a", cfg))
	assert.NotNil(t, startup.natGateway("EGRESS-A"), "NAT Gateways are found by name, case-insensitively")

	orphan := newEgressListingOf([]*armnetwork.NatGateway{{Name: to.Ptr("Orphan")}}, []*armnetwork.PublicIPAddress{{
		Name: to.Ptr(managedPublicIPName("orphan", false, 2)), Tags: map[string]*string{EgressIdentityTagKey: to.Ptr("ORPHAN")},
	}}, nil)
	assert.Equal(t, []string{"orphan"}, orphan.leftoverCandidates(), "the NAT Gateway name and the ownership tag name one candidate, lower-cased")
}

// TestBuildNRPState_ListsTheEgressResources pins step 1 of 4.6: startup keeps the NAT Gateways, Public IPs and (a new
// listing) Public IP prefixes of the node resource group, with the registrations, for the egress identities.
func TestBuildNRPState_ListsTheEgressResources(t *testing.T) {
	cfg := testConfig()
	c := newEgressCloud(t)
	c.applied(startupIdentity, consensusB, false)
	c.addPIP(managedPublicIPName(startupIdentity, false, 2), startupIdentity, armnetwork.ProvisioningStateSucceeded)
	m := newStartupReaders(gomock.NewController(t))
	m.serve(c)

	_, _, _, listing, _, err := buildNRPState(context.Background(), cfg, m.factory)
	if assert.NoError(t, err) && assert.NotNil(t, listing) {
		assert.NotNil(t, listing.natGateway(startupIdentity), "the NAT Gateway is kept as listed")
		assert.NotNil(t, listing.resources.publicIP(managedPublicIPName(startupIdentity, false, 2)), "the Public IPs are kept as listed")
		assert.NotNil(t, listing.resources.prefix(managedPrefixName(startupIdentity, false, 31)), "the prefixes are listed and kept")
		assert.Len(t, listing.publicIPs, 1)
		assert.Equal(t, map[string]string{startupIdentity: natGatewayID(startupIdentity, cfg)}, listing.registrations)
		assert.Equal(t, consensusB, classifyManagedEgress(startupIdentity, listing, &consensusB, consensusApplied, false, cfg).applied,
			"the listed prefix is what satisfies a managed prefix configuration")
	}
}

// TestInitializeFromCluster_AbortsWhenAnEgressListingFails pins that the egress listings of step 1 of 4.6 are one
// snapshot: when any of its reads fails, startup returns that error before planning anything. No tracker is returned,
// so nothing is adopted or reconciled, and nothing but reads reaches Azure or Kubernetes: no write, no orphan deletion.
func TestInitializeFromCluster_AbortsWhenAnEgressListingFails(t *testing.T) {
	failure := errors.New("transient ARM list failure")
	// What a startup that went on would act on: a registered identity with a member, and an orphan Public IP and prefix.
	c := newEgressCloud(t)
	c.applied(startupIdentity, defaultEgressConfig(egressParseContext{}), false)
	c.addPIP(managedPublicIPName(startupGone, false, 2), startupGone, armnetwork.ProvisioningStateSucceeded)
	c.addPrefix(managedPrefixName(startupGone, false, 31), startupGone, 31, armnetwork.ProvisioningStateSucceeded)

	for _, tc := range []struct {
		listing string
		fail    func(m *startupClients)
	}{
		{"Service Gateway services", func(m *startupClients) { m.sgw.EXPECT().GetServices(gomock.Any(), "rg", "sgw").Return(nil, failure) }},
		{"NAT Gateways", func(m *startupClients) { m.nat.EXPECT().List(gomock.Any(), "rg").Return(nil, failure) }},
		{"Public IPs", func(m *startupClients) { m.pip.EXPECT().List(gomock.Any(), "rg").Return(nil, failure) }},
		{"Public IP prefixes", func(m *startupClients) { m.prefix.EXPECT().List(gomock.Any(), "rg").Return(nil, failure) }},
	} {
		t.Run(tc.listing, func(t *testing.T) {
			// Only the listings are expected: a write, an orphan deletion or any other read fails the test.
			m := newStartupReaders(gomock.NewController(t))
			tc.fail(m)
			m.sgw.EXPECT().GetServices(gomock.Any(), "rg", "sgw").Return(c.serviceList(), nil).AnyTimes()
			m.nat.EXPECT().List(gomock.Any(), "rg").Return(c.natList(), nil).AnyTimes()
			m.pip.EXPECT().List(gomock.Any(), "rg").Return(c.pipList(), nil).AnyTimes()
			m.prefix.EXPECT().List(gomock.Any(), "rg").Return(c.prefixList(), nil).AnyTimes()
			kube := fake.NewSimpleClientset(newEgressPod("member", "ns", startupIdentity, "node-1", "10.244.0.5", "10.0.0.1", v1.PodRunning))
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			dt, err := InitializeFromCluster(ctx, testConfig(), m.factory, kube)

			assert.ErrorIs(t, err, failure)
			assert.Nil(t, dt, "a startup that failed returns no tracker")
			for _, action := range kube.Actions() {
				assert.Equal(t, "list", action.GetVerb(), "Kubernetes is only listed: %s %s", action.GetVerb(), action.GetResource().Resource)
			}
		})
	}
}

// setStartupSettleDelay sets the egress settle delay for the test.
func setStartupSettleDelay(t *testing.T, d time.Duration) {
	t.Helper()
	old := egressSettleDelay.Load()
	egressSettleDelay.Store(int64(d))
	t.Cleanup(func() { egressSettleDelay.Store(old) })
}

// egressPassClients are the clients the confirmation pass reads with.
type egressPassClients struct {
	factory *mock_azclient.MockClientFactory
	sgw     *mock_servicegatewayclient.MockInterface
	nat     *mock_natgatewayclient.MockInterface
	pip     *mock_publicipaddressclient.MockInterface
	prefix  *mock_publicipprefixclient.MockInterface
}

func newEgressPassClients(ctrl *gomock.Controller) *egressPassClients {
	m := &egressPassClients{
		factory: mock_azclient.NewMockClientFactory(ctrl),
		sgw:     mock_servicegatewayclient.NewMockInterface(ctrl),
		nat:     mock_natgatewayclient.NewMockInterface(ctrl),
		pip:     mock_publicipaddressclient.NewMockInterface(ctrl),
		prefix:  mock_publicipprefixclient.NewMockInterface(ctrl),
	}
	m.factory.EXPECT().GetServiceGatewayClient().Return(m.sgw).AnyTimes()
	m.factory.EXPECT().GetNatGatewayClient().Return(m.nat).AnyTimes()
	m.factory.EXPECT().GetPublicIPAddressClient().Return(m.pip).AnyTimes()
	m.factory.EXPECT().GetPublicIPPrefixClient().Return(m.prefix).AnyTimes()
	return m
}

// serve expects the four reads of one listing of the cloud, each answered with what the cloud holds then.
func (m *egressPassClients) serve(c *egressCloud) {
	m.sgw.EXPECT().GetServices(gomock.Any(), "rg", "sgw").DoAndReturn(
		func(context.Context, string, string) ([]*armnetwork.ServiceGatewayService, error) {
			return c.serviceList(), nil
		})
	m.nat.EXPECT().List(gomock.Any(), "rg").DoAndReturn(
		func(context.Context, string) ([]*armnetwork.NatGateway, error) { return c.natList(), nil })
	m.pip.EXPECT().List(gomock.Any(), "rg").DoAndReturn(
		func(context.Context, string) ([]*armnetwork.PublicIPAddress, error) { return c.pipList(), nil })
	m.prefix.EXPECT().List(gomock.Any(), "rg").DoAndReturn(
		func(context.Context, string) ([]*armnetwork.PublicIPPrefix, error) { return c.prefixList(), nil })
}

// adoptedTracker returns a tracker that adopted the identities of the cloud at startup, with the members members
// seeds, once initialization completed: the state the confirmation pass finds when nothing ran meanwhile.
func adoptedTracker(c *egressCloud, members func(dt *DiffTracker)) *DiffTracker {
	dt := newStartupTracker(false)
	members(dt)
	c.seedNRP(dt)
	dt.adoptStartupEgressIdentities(c.startupListing(dt))
	drainTrigger(dt.serviceUpdaterTrigger)
	atomic.StoreInt32(&dt.isInitializing, 0)
	atomic.StoreInt32(&dt.pendingUpdaterTriggers, 0)
	return dt
}

// TestEgressConfirmationPass pins the confirmation pass (4.6 step 3a): after the settle delay it reads the four
// listings once and owes an observation to every managed identity whose NAT Gateway changed since the startup listing
// (a late NAT Gateway write, unlink, Public IP or prefix create) and to every identity being created or deleted, its
// mode resolved or not, with a resource of it the controller owns listed, a residue cleanup to a BYO identity with
// leftovers of its managed past (tracking a Created operation for it), and schedules the deletion of the leftovers of
// an identity nothing tracks through the orphan path. Without a difference, also for an identity whose NAT Gateway
// serves both families, it makes no Azure call beyond the four reads and starts nothing.
func TestEgressConfirmationPass(t *testing.T) {
	a, b := consensusA, consensusB
	dflt := defaultEgressConfig(egressParseContext{})
	dfltDual := defaultEgressConfig(egressParseContext{WantsIPv6: true})
	cfg := testConfig()

	// The default startup: a managed identity whose NAT Gateway satisfies its agreed configuration, and a BYO one.
	stable := func(c *egressCloud) func(dt *DiffTracker) {
		c.applied(startupIdentity, a, false)
		c.register(startupBYO, byoNATGatewayARMID(startupBYO))
		return func(dt *DiffTracker) {
			startupMembers(dt, startupIdentity, &a, &a)
			startupMembers(dt, startupBYO, &dflt)
		}
	}
	// A dual-stack managed identity whose NAT Gateway satisfies the dual-stack default its member votes for.
	stableDual := func(c *egressCloud) func(dt *DiffTracker) {
		c.applied(startupIdentity, dfltDual, true)
		return func(dt *DiffTracker) { startupMembers(dt, startupIdentity, &dfltDual) }
	}
	// A managed identity whose NAT Gateway drifted from the default without consensus: applied unknown, nothing owed.
	drifted := func(c *egressCloud) func(dt *DiffTracker) {
		c.applied(startupIdentity, dflt, false)
		c.nat(startupIdentity).Properties.IdleTimeoutInMinutes = to.Ptr[int32](7)
		return func(dt *DiffTracker) { startupMembers(dt, startupIdentity, &a, &b) }
	}

	type want struct {
		owed     []string                 // operations that gain an obligation
		states   map[string]ResourceState // the state of these operations after the pass
		orphans  []string                 // identities scheduled for deletion as orphans
		untraced []string                 // identities that must have no operation
		trigger  bool                     // a run was started
	}
	cases := []struct {
		name    string
		startup func(c *egressCloud) func(dt *DiffTracker)
		late    func(c *egressCloud, dt *DiffTracker)
		want    want
	}{
		{
			name:    "no difference",
			startup: stable,
			want:    want{states: map[string]ResourceState{startupIdentity: StateCreated}, untraced: []string{startupBYO}},
		},
		{
			name:    "no difference, dual-stack identity",
			startup: stableDual,
			want:    want{states: map[string]ResourceState{startupIdentity: StateCreated}},
		},
		{
			name:    "late IPv6 Public IP create, dual-stack identity",
			startup: stableDual,
			late: func(c *egressCloud, _ *DiffTracker) {
				c.addPIP(managedPublicIPName(startupIdentity, true, 2), startupIdentity, armnetwork.ProvisioningStateSucceeded)
			},
			want: want{owed: []string{startupIdentity}, states: map[string]ResourceState{startupIdentity: StateUpdateInProgress}, trigger: true},
		},
		{
			name:    "late NAT Gateway change",
			startup: stable,
			late: func(c *egressCloud, _ *DiffTracker) {
				c.nat(startupIdentity).Properties.IdleTimeoutInMinutes = to.Ptr[int32](7)
			},
			want: want{owed: []string{startupIdentity}, states: map[string]ResourceState{startupIdentity: StateUpdateInProgress}, trigger: true},
		},
		{
			name:    "late NAT Gateway unlink",
			startup: stable,
			late: func(c *egressCloud, _ *DiffTracker) {
				c.nat(startupIdentity).Properties.ServiceGateway = nil
			},
			want: want{owed: []string{startupIdentity}, states: map[string]ResourceState{startupIdentity: StateUpdateInProgress}, trigger: true},
		},
		{
			name:    "late unlink of the registration",
			startup: stable,
			late: func(c *egressCloud, _ *DiffTracker) {
				c.register(startupIdentity, "")
			},
			want: want{owed: []string{startupIdentity}, states: map[string]ResourceState{startupIdentity: StateUpdateInProgress}, trigger: true},
		},
		{
			name:    "late Public IP create",
			startup: stable,
			late: func(c *egressCloud, _ *DiffTracker) {
				c.addPIP(managedPublicIPName(startupIdentity, false, 3), startupIdentity, armnetwork.ProvisioningStateSucceeded)
			},
			want: want{owed: []string{startupIdentity}, states: map[string]ResourceState{startupIdentity: StateUpdateInProgress}, trigger: true},
		},
		{
			name:    "late prefix create",
			startup: stable,
			late: func(c *egressCloud, _ *DiffTracker) {
				c.addPrefix(managedPrefixName(startupIdentity, false, 30), startupIdentity, 30, armnetwork.ProvisioningStateSucceeded)
			},
			want: want{owed: []string{startupIdentity}, states: map[string]ResourceState{startupIdentity: StateUpdateInProgress}, trigger: true},
		},
		{
			name:    "late write still in progress",
			startup: stable,
			late: func(c *egressCloud, _ *DiffTracker) {
				c.nat(startupIdentity).Properties.ProvisioningState = to.Ptr(armnetwork.ProvisioningStateUpdating)
			},
			want: want{owed: []string{startupIdentity}, states: map[string]ResourceState{startupIdentity: StateUpdateInProgress}, trigger: true},
		},
		{
			name:    "unknown NAT Gateway now satisfies the default",
			startup: drifted,
			late: func(c *egressCloud, _ *DiffTracker) {
				c.nat(startupIdentity).Properties.IdleTimeoutInMinutes = nil
			},
			want: want{owed: []string{startupIdentity}, states: map[string]ResourceState{startupIdentity: StateUpdateInProgress}, trigger: true},
		},
		{
			name:    "unknown NAT Gateway still unknown",
			startup: drifted,
			want:    want{states: map[string]ResourceState{startupIdentity: StateCreated}},
		},
		{
			name:    "BYO identity with a late leftover Public IP",
			startup: stable,
			late: func(c *egressCloud, _ *DiffTracker) {
				c.addPIP(managedPublicIPName(startupBYO, false, 2), startupBYO, armnetwork.ProvisioningStateSucceeded)
			},
			want: want{owed: []string{startupBYO}, states: map[string]ResourceState{startupBYO: StateUpdateInProgress}, trigger: true},
		},
		{
			name:    "BYO identity with a late leftover IPv6 Public IP",
			startup: stable,
			late: func(c *egressCloud, _ *DiffTracker) {
				c.addPIP(managedPublicIPName(startupBYO, true, 2), startupBYO, armnetwork.ProvisioningStateSucceeded)
			},
			want: want{owed: []string{startupBYO}, states: map[string]ResourceState{startupBYO: StateUpdateInProgress}, trigger: true},
		},
		{
			name:    "BYO identity re-created inside the window with late NAT Gateway, Public IP and prefix writes",
			startup: stable,
			late: func(c *egressCloud, _ *DiffTracker) {
				c.ownedNAT(startupBYO)
				c.addPIP(PublicIPName(startupBYO), startupBYO, armnetwork.ProvisioningStateCreating)
				c.addPrefix(managedPrefixName(startupBYO, false, 31), startupBYO, 31, armnetwork.ProvisioningStateSucceeded)
			},
			want: want{owed: []string{startupBYO}, states: map[string]ResourceState{startupBYO: StateUpdateInProgress}, trigger: true},
		},
		{
			name:    "BYO identity whose same-name NAT Gateway a service uses",
			startup: stable,
			late: func(c *egressCloud, _ *DiffTracker) {
				c.ownedNAT(startupBYO)
				c.register(startupOther, natGatewayID(startupBYO, cfg))
			},
			want: want{untraced: []string{startupBYO, startupOther}},
		},
		{
			name:    "leftover Public IP of an identity nothing tracks",
			startup: stable,
			late: func(c *egressCloud, _ *DiffTracker) {
				c.addPIP(managedPublicIPName(startupGone, false, 2), startupGone, armnetwork.ProvisioningStateSucceeded)
			},
			want: want{orphans: []string{startupGone}},
		},
		{
			name:    "leftover NAT Gateway of an identity nothing tracks",
			startup: stable,
			late: func(c *egressCloud, _ *DiffTracker) {
				c.ownedNAT(startupGone)
			},
			want: want{orphans: []string{startupGone}},
		},
		{
			name:    "leftover prefix of an identity nothing tracks",
			startup: stable,
			late: func(c *egressCloud, _ *DiffTracker) {
				c.addPrefix(managedPrefixName(startupGone, false, 31), startupGone, 31, armnetwork.ProvisioningStateSucceeded)
			},
			want: want{orphans: []string{startupGone}},
		},
		{
			name:    "untagged numbered Public IP of an identity nothing tracks is not a leftover",
			startup: stable,
			late: func(c *egressCloud, _ *DiffTracker) {
				c.addPIP(managedPublicIPName(startupGone, false, 2), "", armnetwork.ProvisioningStateSucceeded)
			},
			want: want{untraced: []string{startupGone}},
		},
		{
			name:    "NAT Gateway the controller does not own is not a leftover",
			startup: stable,
			late: func(c *egressCloud, _ *DiffTracker) {
				c.ownedNAT(startupGone)
				c.nat(startupGone).Tags = nil
			},
			want: want{untraced: []string{startupGone}},
		},
		{
			name:    "a registered identity is not an orphan",
			startup: stable,
			late: func(c *egressCloud, _ *DiffTracker) {
				c.ownedNAT(startupOther)
				c.register(startupOther, natGatewayID(startupOther, cfg))
			},
			want: want{untraced: []string{startupOther}},
		},
		{
			name:    "a registered identity is not an orphan, whatever it left",
			startup: stable,
			late: func(c *egressCloud, _ *DiffTracker) {
				c.register(startupOther, "")
				c.addPIP(managedPublicIPName(startupOther, false, 2), startupOther, armnetwork.ProvisioningStateSucceeded)
			},
			want: want{untraced: []string{startupOther}},
		},
		{
			name:    "the default outbound identity is never an orphan",
			startup: stable,
			late: func(c *egressCloud, dt *DiffTracker) {
				// Not recorded as unmanaged, so the orphan path's own refusal of the reserved name is what keeps it out.
				dt.NRPResources.UnmanagedNATGateways.Delete(DefaultOutboundNATGatewayName)
				c.addPIP(managedPublicIPName(DefaultOutboundNATGatewayName, false, 2), DefaultOutboundNATGatewayName, armnetwork.ProvisioningStateSucceeded)
			},
			want: want{untraced: []string{DefaultOutboundNATGatewayName}},
		},
		{
			name:    "an identity this process registered is not an orphan",
			startup: stable,
			late: func(c *egressCloud, dt *DiffTracker) {
				c.addPIP(managedPublicIPName(startupGone, false, 2), startupGone, armnetwork.ProvisioningStateSucceeded)
				dt.NRPResources.NATGateways.Insert(startupGone)
			},
			want: want{untraced: []string{startupGone}},
		},
		{
			name:    "a name the controller must never manage is not an orphan",
			startup: stable,
			late: func(c *egressCloud, dt *DiffTracker) {
				c.addPIP(managedPublicIPName(startupGone, false, 2), startupGone, armnetwork.ProvisioningStateSucceeded)
				dt.NRPResources.UnmanagedNATGateways.Insert(startupGone)
			},
			want: want{untraced: []string{startupGone}},
		},
		{
			name:    "creation in flight, its mode not resolved yet, with a late leftover: owed, not an orphan",
			startup: stable,
			late: func(c *egressCloud, dt *DiffTracker) {
				c.addPIP(managedPublicIPName(startupGone, false, 2), startupGone, armnetwork.ProvisioningStateSucceeded)
				dt.byoNATGateways[startupGone] = byoNATGatewayARMID(startupGone)
				dt.pendingServiceOps[startupGone] = &ServiceOperationState{ServiceUID: startupGone, State: StateCreationInProgress,
					Config: NewOutboundServiceConfig(startupGone, &OutboundConfig{IPFamilies: ipv4Families(), Egress: a})}
			},
			want: want{owed: []string{startupGone}, states: map[string]ResourceState{startupGone: StateCreationInProgress}},
		},
		{
			name:    "creation in flight with a late IPv6 leftover: owed, not an orphan",
			startup: stable,
			late: func(c *egressCloud, dt *DiffTracker) {
				c.addPIP(managedPublicIPName(startupGone, true, 2), startupGone, armnetwork.ProvisioningStateSucceeded)
				dt.pendingServiceOps[startupGone] = &ServiceOperationState{ServiceUID: startupGone, State: StateCreationInProgress,
					Config: NewOutboundServiceConfig(startupGone, &OutboundConfig{IPFamilies: dualStackFamilies(), Egress: a})}
			},
			want: want{owed: []string{startupGone}, states: map[string]ResourceState{startupGone: StateCreationInProgress}},
		},
		{
			name:    "creation waiting for a worker, with a late NAT Gateway: owed, not an orphan",
			startup: stable,
			late: func(c *egressCloud, dt *DiffTracker) {
				c.ownedNAT(startupGone)
				dt.pendingServiceOps[startupGone] = &ServiceOperationState{ServiceUID: startupGone, State: StateNotStarted,
					Config: NewOutboundServiceConfig(startupGone, &OutboundConfig{IPFamilies: ipv4Families(), Egress: a})}
			},
			want: want{owed: []string{startupGone}, states: map[string]ResourceState{startupIdentity: StateCreated, startupGone: StateNotStarted}},
		},
		{
			name:    "creation with nothing of it listed that the controller owns: nothing owed",
			startup: stable,
			late: func(c *egressCloud, dt *DiffTracker) {
				c.addPIP(managedPublicIPName(startupGone, false, 2), "", armnetwork.ProvisioningStateSucceeded)
				dt.pendingServiceOps[startupGone] = &ServiceOperationState{ServiceUID: startupGone, State: StateCreationInProgress,
					Config: NewOutboundServiceConfig(startupGone, &OutboundConfig{IPFamilies: ipv4Families(), Egress: a})}
			},
			want: want{states: map[string]ResourceState{startupGone: StateCreationInProgress}},
		},
		{
			name:    "managed creation in flight whose NAT Gateway is unlinked: owed",
			startup: stable,
			late: func(c *egressCloud, dt *DiffTracker) {
				dt.pendingServiceOps[startupIdentity].State = StateCreationInProgress
				c.nat(startupIdentity).Properties.ServiceGateway = nil
			},
			want: want{owed: []string{startupIdentity}, states: map[string]ResourceState{startupIdentity: StateCreationInProgress}},
		},
		{
			name:    "BYO creation whose same-name NAT Gateway a service uses: left alone",
			startup: stable,
			late: func(c *egressCloud, dt *DiffTracker) {
				c.ownedNAT(startupGone)
				c.register(startupOther, natGatewayID(startupGone, cfg))
				dt.egress.mode[startupGone] = egressModeBYO
				dt.pendingServiceOps[startupGone] = &ServiceOperationState{ServiceUID: startupGone, State: StateNotStarted,
					Config: NewOutboundServiceConfig(startupGone, &OutboundConfig{IPFamilies: ipv4Families(), Egress: a})}
			},
			want: want{states: map[string]ResourceState{startupGone: StateNotStarted}, untraced: []string{startupOther}},
		},
		{
			name:    "created identity whose mode is not resolved: left alone",
			startup: stable,
			late: func(c *egressCloud, dt *DiffTracker) {
				delete(dt.egress.mode, startupIdentity)
				c.nat(startupIdentity).Properties.IdleTimeoutInMinutes = to.Ptr[int32](7)
			},
			want: want{states: map[string]ResourceState{startupIdentity: StateCreated}},
		},
		{
			name:    "identity being deleted with a leftover",
			startup: stable,
			late: func(_ *egressCloud, dt *DiffTracker) {
				dt.pendingServiceOps[startupIdentity].State = StateDeletionInProgress
			},
			want: want{owed: []string{startupIdentity}, states: map[string]ResourceState{startupIdentity: StateDeletionInProgress}},
		},
		{
			name:    "deletion, its mode not resolved, with a late leftover: owed",
			startup: stable,
			late: func(c *egressCloud, dt *DiffTracker) {
				c.addPrefix(managedPrefixName(startupGone, false, 30), startupGone, 30, armnetwork.ProvisioningStateSucceeded)
				dt.pendingServiceOps[startupGone] = &ServiceOperationState{ServiceUID: startupGone, State: StateDeletionInProgress,
					IsOrphan: true, Config: NewOutboundServiceConfig(startupGone, nil)}
			},
			want: want{owed: []string{startupGone}, states: map[string]ResourceState{startupGone: StateDeletionInProgress}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			setStartupSettleDelay(t, time.Millisecond)
			c := newEgressCloud(t)
			dt := adoptedTracker(c, tc.startup(c))
			if tc.late != nil {
				tc.late(c, dt)
			}
			generations := map[string]uint64{}
			for id, op := range dt.pendingServiceOps {
				generations[id] = op.Egress.ObservationGen
			}
			opsBefore := slices.Sorted(maps.Keys(dt.pendingServiceOps))
			m := newEgressPassClients(gomock.NewController(t))
			m.serve(c)
			dt.networkClientFactory = m.factory

			dt.runEgressConfirmationPass(context.Background())

			for _, id := range slices.Sorted(maps.Keys(dt.pendingServiceOps)) {
				op := dt.pendingServiceOps[id]
				owed := slices.Contains(tc.want.owed, id)
				assert.Equal(t, owed, op.Egress.ObservationGen > generations[id], "%s: a new obligation", id)
				if owed {
					assert.True(t, op.Egress.NeedsObservation, "%s: owed an observation", id)
				}
				if state, ok := tc.want.states[id]; ok {
					assert.Equal(t, state, op.State, "%s", id)
				}
			}
			var orphans, created []string
			for _, id := range slices.Sorted(maps.Keys(dt.pendingServiceOps)) {
				if slices.Contains(opsBefore, id) {
					continue
				}
				if op := dt.pendingServiceOps[id]; op.IsOrphan {
					orphans = append(orphans, id)
					assert.Equal(t, StateDeletionPending, op.State, "an orphan deletion waits for the locations of %s to drain", id)
					assert.Contains(t, dt.pendingServiceDeletions, id)
				} else {
					created = append(created, id)
				}
			}
			assert.Equal(t, tc.want.orphans, orphans, "orphan deletions")
			for _, id := range created {
				assert.Contains(t, tc.want.owed, id, "only an owed BYO identity gets a new operation")
				assert.Equal(t, egressModeBYO, dt.egress.mode[id])
				assert.True(t, dt.pendingServiceOps[id].Egress.StartupBaseline)
			}
			for _, id := range tc.want.untraced {
				assert.NotContains(t, dt.pendingServiceOps, id)
			}
			if op := dt.pendingServiceOps[startupGone]; op != nil && op.State == StateCreationInProgress {
				assert.NotContains(t, dt.egress.mode, startupGone, "its creation resolves its mode")
			}
			wantTriggers := 0
			if tc.want.trigger {
				wantTriggers = 1
			}
			assert.Len(t, dt.serviceUpdaterTrigger, wantTriggers, "runs started")
			wantLocationTriggers := 0
			if len(tc.want.orphans) > 0 {
				wantLocationTriggers = 1
			}
			assert.Len(t, dt.locationsUpdaterTrigger, wantLocationTriggers, "the orphan deletion waits for a location sync")
			assert.Empty(t, dt.egress.pendingEvents)
		})
	}
}

// TestEgressConfirmationPass_ObligationSurvivesInFlightRuns pins that an obligation the confirmation pass raises while
// a run of the identity is in flight (an observe-and-clean of a managed identity, the residue cleanup of a BYO one)
// survives that run's completion: the generation advanced since its dispatch, so another run follows.
func TestEgressConfirmationPass_ObligationSurvivesInFlightRuns(t *testing.T) {
	setStartupSettleDelay(t, time.Millisecond)
	a := consensusA
	c := newEgressCloud(t)
	c.applied(startupIdentity, a, false)
	c.addPIP(managedPublicIPName(startupIdentity, false, 3), startupIdentity, armnetwork.ProvisioningStateSucceeded)
	c.register(startupBYO, byoNATGatewayARMID(startupBYO))
	c.addPIP(managedPublicIPName(startupBYO, false, 2), startupBYO, armnetwork.ProvisioningStateSucceeded)
	dt := adoptedTracker(c, func(dt *DiffTracker) {
		startupMembers(dt, startupIdentity, &a, &a)
		startupMembers(dt, startupBYO, &a)
	})
	for _, id := range []string{startupIdentity, startupBYO} {
		op := dt.pendingServiceOps[id]
		if !assert.NotNil(t, op) || !assert.Equal(t, StateUpdateInProgress, op.State, "%s: startup started its run", id) {
			return
		}
	}
	dispatchWithoutRunning(t, dt)

	m := newEgressPassClients(gomock.NewController(t))
	m.serve(c)
	dt.networkClientFactory = m.factory
	dt.runEgressConfirmationPass(context.Background())

	dt.onServiceOperationComplete(startupIdentity, true, nil, &egressOutcome{Mode: egressModeManaged, Applied: &a, Swept: true})
	dt.onServiceOperationComplete(startupBYO, true, nil, &egressOutcome{Mode: egressModeBYO, Swept: true})

	for _, id := range []string{startupIdentity, startupBYO} {
		op := dt.pendingServiceOps[id]
		assert.True(t, op.Egress.NeedsObservation, "%s: the obligation raised while the run was in flight must survive it", id)
		assert.Equal(t, StateUpdateInProgress, op.State, "%s: another run follows", id)
	}
}

// TestEgressConfirmationPass_LateLeftoverOfACreation pins that the confirmation pass owes an observation to an
// identity whose creation, dispatched before the pass and its mode not resolved yet, may have read Azure before a write
// of the previous process landed: the listing shows a resource of it the controller owns. The creation's completion,
// managed or BYO, keeps the obligation, and the run that settles it follows: an observe-and-clean, which sweeps, or a
// residue cleanup. With nothing of it listed nothing is owed and nothing follows.
func TestEgressConfirmationPass_LateLeftoverOfACreation(t *testing.T) {
	dflt := defaultEgressConfig(egressParseContext{})
	cases := []struct {
		name     string
		leftover bool
		outcome  egressOutcome
		wantRun  operationKind // the run that follows the creation; operationNone for none
	}{
		{name: "managed creation, late Public IP: observe-and-clean", leftover: true,
			outcome: egressOutcome{Mode: egressModeManaged, Applied: &dflt, Swept: true}, wantRun: operationUpdate},
		{name: "BYO creation, late Public IP: residue cleanup", leftover: true,
			outcome: egressOutcome{Mode: egressModeBYO}, wantRun: operationEgressResidueCleanup},
		{name: "managed creation, nothing listed", outcome: egressOutcome{Mode: egressModeManaged, Applied: &dflt, Swept: true}},
		{name: "BYO creation, nothing listed", outcome: egressOutcome{Mode: egressModeBYO}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			setStartupSettleDelay(t, time.Millisecond)
			c := newEgressCloud(t)
			dt := adoptedTracker(c, func(*DiffTracker) {})
			op := &ServiceOperationState{ServiceUID: startupGone, State: StateNotStarted,
				Config: NewOutboundServiceConfig(startupGone, &OutboundConfig{IPFamilies: ipv4Families(), Egress: dflt})}
			dt.pendingServiceOps[startupGone] = op
			dispatchWithoutRunning(t, dt)
			if !assert.Equal(t, StateCreationInProgress, op.State, "the creation is in flight") {
				return
			}
			if tc.leftover {
				c.addPIP(managedPublicIPName(startupGone, false, 2), startupGone, armnetwork.ProvisioningStateSucceeded)
			}
			m := newEgressPassClients(gomock.NewController(t))
			m.serve(c)
			dt.networkClientFactory = m.factory

			dt.runEgressConfirmationPass(context.Background())
			assert.Equal(t, tc.leftover, op.Egress.NeedsObservation, "an observation is owed")
			assert.NotContains(t, dt.egress.mode, startupGone, "its creation resolves its mode")
			assert.NotContains(t, dt.pendingServiceDeletions, startupGone, "not an orphan")

			dt.onServiceOperationComplete(startupGone, true, nil, &tc.outcome)
			assert.Equal(t, tc.leftover, op.Egress.NeedsObservation, "the creation read Azure before the obligation was raised")
			op.NextRetryAt = time.Time{} // the backoff of the run that follows has passed
			dispatchWithoutRunning(t, dt)
			assert.Equal(t, tc.wantRun, op.InFlightKind, "the run that follows the creation")
			if tc.wantRun == operationNone {
				assert.Equal(t, StateCreated, op.State)
			} else if assert.NotNil(t, op.InFlightConfig) {
				assert.Equal(t, egressUnknown, outboundEgress(op.InFlightConfig.OutboundConfig), "it changes no configuration")
			}
		})
	}
}

// egressPassState describes what the confirmation pass may change: the operations (state and obligations), the
// modes, the deletions and the triggers.
func egressPassState(dt *DiffTracker) string {
	dt.mu.Lock()
	defer dt.mu.Unlock()
	var b strings.Builder
	for _, id := range slices.Sorted(maps.Keys(dt.pendingServiceOps)) {
		op := dt.pendingServiceOps[id]
		fmt.Fprintf(&b, "%s: %v, observations %d, owed %t; ", id, op.State, op.Egress.ObservationGen, op.Egress.NeedsObservation)
	}
	fmt.Fprintf(&b, "modes %v; deletions %v; triggers %d/%d", dt.egress.mode, slices.Sorted(maps.Keys(dt.pendingServiceDeletions)),
		len(dt.serviceUpdaterTrigger), len(dt.locationsUpdaterTrigger))
	return b.String()
}

// TestEgressConfirmationPass_RetriesUntilACompleteListing pins that the confirmation pass never acts on a partial
// listing: whichever of its four reads fails, or when the Service Gateway lists a service of an unknown type, the
// listing is dropped and read again after a backoff, and nothing changes before a complete listing is read.
func TestEgressConfirmationPass_RetriesUntilACompleteListing(t *testing.T) {
	// The identity's NAT Gateway is checked against all four lists: a listing without one of them would owe it an
	// observation.
	cfg := EgressConfig{IdleTimeoutMinutes: 10, IPv4: egressFamilyConfig{ManagedPublicIPs: 2, ManagedPrefixLength: 31}}
	listFailure := errors.New("transient ARM list failure")
	// The reads of a failed listing that succeed show a changed NAT Gateway and a leftover NAT Gateway of an identity
	// nothing tracks: acting on them would owe an observation and schedule a deletion.
	partial := newEgressCloud(t)
	partial.applied(startupIdentity, cfg, false)
	partial.nat(startupIdentity).Properties.IdleTimeoutInMinutes = to.Ptr[int32](7)
	partial.ownedNAT(startupGone)
	oddService := []*armnetwork.ServiceGatewayService{{Name: to.Ptr("odd"),
		Properties: &armnetwork.ServiceGatewayServicePropertiesFormat{ServiceType: to.Ptr(armnetwork.ServiceType("Sideways"))}}}

	cases := []struct {
		name   string
		failed func(m *egressPassClients) []any // the reads of the failed listing, in order
	}{
		{name: "Service Gateway services", failed: func(m *egressPassClients) []any {
			return []any{m.sgw.EXPECT().GetServices(gomock.Any(), "rg", "sgw").Return(nil, listFailure)}
		}},
		{name: "NAT Gateways", failed: func(m *egressPassClients) []any {
			return []any{
				m.sgw.EXPECT().GetServices(gomock.Any(), "rg", "sgw").Return(partial.serviceList(), nil),
				m.nat.EXPECT().List(gomock.Any(), "rg").Return(nil, listFailure),
			}
		}},
		{name: "Public IPs", failed: func(m *egressPassClients) []any {
			return []any{
				m.sgw.EXPECT().GetServices(gomock.Any(), "rg", "sgw").Return(partial.serviceList(), nil),
				m.nat.EXPECT().List(gomock.Any(), "rg").Return(partial.natList(), nil),
				m.pip.EXPECT().List(gomock.Any(), "rg").Return(nil, listFailure),
			}
		}},
		{name: "Public IP prefixes", failed: func(m *egressPassClients) []any {
			return []any{
				m.sgw.EXPECT().GetServices(gomock.Any(), "rg", "sgw").Return(partial.serviceList(), nil),
				m.nat.EXPECT().List(gomock.Any(), "rg").Return(partial.natList(), nil),
				m.pip.EXPECT().List(gomock.Any(), "rg").Return(partial.pipList(), nil),
				m.prefix.EXPECT().List(gomock.Any(), "rg").Return(nil, listFailure),
			}
		}},
		{name: "a Service Gateway service of an unknown type", failed: func(m *egressPassClients) []any {
			return []any{
				m.sgw.EXPECT().GetServices(gomock.Any(), "rg", "sgw").Return(oddService, nil),
				m.nat.EXPECT().List(gomock.Any(), "rg").Return(partial.natList(), nil),
				m.pip.EXPECT().List(gomock.Any(), "rg").Return(partial.pipList(), nil),
				m.prefix.EXPECT().List(gomock.Any(), "rg").Return(partial.prefixList(), nil),
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			setStartupSettleDelay(t, time.Millisecond)
			c := newEgressCloud(t)
			c.applied(startupIdentity, cfg, false)
			dt := adoptedTracker(c, func(dt *DiffTracker) { startupMembers(dt, startupIdentity, &cfg, &cfg) })
			before := egressPassState(dt)

			m := newEgressPassClients(gomock.NewController(t))
			retried := m.sgw.EXPECT().GetServices(gomock.Any(), "rg", "sgw").DoAndReturn(
				func(context.Context, string, string) ([]*armnetwork.ServiceGatewayService, error) {
					assert.Equal(t, before, egressPassState(dt), "nothing changes before a complete listing is read")
					return c.serviceList(), nil
				})
			gomock.InOrder(append(tc.failed(m), retried,
				m.nat.EXPECT().List(gomock.Any(), "rg").Return(c.natList(), nil),
				m.pip.EXPECT().List(gomock.Any(), "rg").Return(c.pipList(), nil),
				m.prefix.EXPECT().List(gomock.Any(), "rg").Return(c.prefixList(), nil))...)
			dt.networkClientFactory = m.factory

			dt.runEgressConfirmationPass(context.Background())

			assert.Equal(t, before, egressPassState(dt), "the complete listing shows no difference")
		})
	}
}

// TestEgressConfirmationPass_EndsWithItsContext pins that the confirmation pass ends with its context and reads nothing
// more: while it waits out the settle delay, when its context ends during a listing that fails, and when it ends while
// the pass waits to read a failed listing again.
func TestEgressConfirmationPass_EndsWithItsContext(t *testing.T) {
	run := func(t *testing.T, dt *DiffTracker, ctx context.Context) {
		t.Helper()
		done := make(chan struct{})
		go func() {
			defer close(done)
			dt.runEgressConfirmationPass(ctx)
		}()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatal("the confirmation pass did not end with its context")
		}
	}

	t.Run("during the settle delay", func(t *testing.T) {
		setStartupSettleDelay(t, time.Hour)
		dt := newStartupTracker(false)
		dt.networkClientFactory = newEgressPassClients(gomock.NewController(t)).factory // no read is expected
		ctx, cancel := context.WithCancel(context.Background())
		time.AfterFunc(20*time.Millisecond, cancel)
		run(t, dt, ctx)
	})

	t.Run("during a listing that fails", func(t *testing.T) {
		setStartupSettleDelay(t, 5*time.Millisecond)
		dt := newStartupTracker(false)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		m := newEgressPassClients(gomock.NewController(t))
		var reads atomic.Int32
		m.sgw.EXPECT().GetServices(gomock.Any(), "rg", "sgw").DoAndReturn(
			func(context.Context, string, string) ([]*armnetwork.ServiceGatewayService, error) {
				if reads.Add(1) == 3 {
					cancel()
				}
				return nil, errors.New("transient ARM list failure")
			}).Times(3)
		dt.networkClientFactory = m.factory
		run(t, dt, ctx)
		assert.Empty(t, dt.pendingServiceOps)
	})

	t.Run("while waiting to read a failed listing again", func(t *testing.T) {
		// The wait before the next read (egressConfirmationRetryDelay) is long: only the end of the context ends it at once.
		setStartupSettleDelay(t, time.Hour)
		dt := newStartupTracker(false)
		parent, cancel := context.WithCancel(context.Background())
		defer cancel()
		ctx := &cancelledAfterErrCheck{Context: parent, cancel: cancel}
		m := newEgressPassClients(gomock.NewController(t))
		m.sgw.EXPECT().GetServices(gomock.Any(), "rg", "sgw").Return(nil, errors.New("transient ARM list failure")).Times(1)
		dt.networkClientFactory = m.factory

		read := make(chan bool, 1)
		go func() {
			_, ok := dt.readEgressConfirmationListing(ctx)
			read <- ok
		}()
		select {
		case ok := <-read:
			assert.False(t, ok, "no listing is read")
		case <-time.After(egressConfirmationRetryDelay):
			t.Fatal("the wait to read the listing again did not end with the context")
		}
		assert.Error(t, parent.Err(), "control: the context ended after the failed listing was checked")
	})
}

// cancelledAfterErrCheck is a context that ends right after the first check of its Err, which still finds it alive:
// only what its holder does after that check sees it end.
type cancelledAfterErrCheck struct {
	context.Context
	cancel context.CancelFunc
	once   sync.Once
}

func (c *cancelledAfterErrCheck) Err() error {
	err := c.Context.Err()
	c.once.Do(c.cancel)
	return err
}

// TestReadEgressListing_EachReadHasABudget pins that every read of the egress listing ends within its own
// getNRPOperationTimeout() budget, even when Azure never answers it: the confirmation pass runs with the runtime's
// context, which has no deadline, so an unanswered read would otherwise hold the pass forever. The budget is the read's
// own: reads that each take most of one still complete the listing.
func TestReadEgressListing_EachReadHasABudget(t *testing.T) {
	setEgressCallTimeoutForTest(t, 200*time.Millisecond)
	for what, path := range map[string]string{
		"Service Gateway services": egressAzureTestServicesPath,
		"NAT Gateways":             egressAzureTestRG + "/natGateways",
		"Public IPs":               egressAzureTestRG + "/publicIPAddresses",
		"Public IP prefixes":       egressAzureTestRG + "/publicIPPrefixes",
	} {
		t.Run(what+" never answered", func(t *testing.T) {
			gate := newArmGate()
			t.Cleanup(gate.open)
			dt, _ := newScriptedARM(t, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != path {
					armReply(http.StatusOK, `{"value":[]}`)(w, r)
					return
				}
				select {
				case <-r.Context().Done():
				case <-gate.ch:
				}
			})
			done := make(chan error, 1)
			go func() {
				_, err := dt.readEgressListing(context.Background())
				done <- err
			}()
			select {
			case err := <-done:
				assert.ErrorIs(t, err, context.DeadlineExceeded)
				assert.ErrorContains(t, err, "listing "+what)
			case <-time.After(5 * time.Second):
				t.Errorf("the read did not end within its budget")
				gate.open()
				<-done
			}
		})
	}
	t.Run("every read answers after most of a budget", func(t *testing.T) {
		dt, _ := newScriptedARM(t, armAfter(120*time.Millisecond, armReply(http.StatusOK, `{"value":[]}`)))
		_, err := dt.readEgressListing(context.Background())
		assert.NoError(t, err, "each read has a budget of its own, not one the listing shares")
	})
}

// TestReconcileServices_StartupCreationConfiguration pins step 4 of 4.6: an egress identity created at startup is
// dispatched with its creation configuration, the agreed one, else the vote of its oldest member when it is valid, else
// the default, for the families of the cluster.
func TestReconcileServices_StartupCreationConfiguration(t *testing.T) {
	a, b := consensusA, consensusB
	const identity = "egress-new"
	cases := []struct {
		name  string
		ipv6  bool
		votes []*EgressConfig // in creation order
		want  EgressConfig
	}{
		{name: "agreed configuration", votes: []*EgressConfig{&a, &a}, want: a},
		{name: "the oldest member seeds it", votes: []*EgressConfig{&b, &a}, want: b},
		{name: "an invalid oldest member gives the default", votes: []*EgressConfig{nil, &a}, want: defaultEgressConfig(egressParseContext{})},
		{name: "the default of a dual-stack cluster", ipv6: true, want: defaultEgressConfig(egressParseContext{WantsIPv6: true})},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dt := newStartupTracker(tc.ipv6)
			startupMembers(dt, identity, tc.votes...)
			dt.reconcileServices(&SyncDiffTrackerReturnType{
				LoadBalancerUpdates: SyncServicesReturnType{Additions: utilsets.NewString(), Removals: utilsets.NewString()},
				NATGatewayUpdates:   SyncServicesReturnType{Additions: utilsets.NewString(identity), Removals: utilsets.NewString()},
			}, nil)

			op := dt.pendingServiceOps[identity]
			if assert.NotNil(t, op) && assert.NotNil(t, op.Config.OutboundConfig) {
				assert.Equal(t, StateNotStarted, op.State)
				assert.Equal(t, dt.outboundIPFamilies(), op.Config.OutboundConfig.IPFamilies)
				dispatchWithoutRunning(t, dt)
				if assert.NotNil(t, op.InFlightConfig, "the creation is dispatched") {
					assert.Equal(t, tc.want, outboundEgress(op.InFlightConfig.OutboundConfig))
				}
			}
		})
	}
}

// TestInitializationPlanningLatch pins the planning latch (4.6 step 8): while startup is still enqueueing its work,
// the completion of the work enqueued first cannot complete initialization, however little is left; releasing the
// latch checks completion.
func TestInitializationPlanningLatch(t *testing.T) {
	const uid = "11111111-1111-1111-1111-111111111111"
	dt := newTestDiffTracker()
	dt.networkClientFactory = mock_azclient.NewMockClientFactory(gomock.NewController(t))
	if !assert.NoError(t, startInitialization(context.Background(), dt)) {
		return
	}
	// Stop the updaters: the test plays the first worker's completion itself.
	dt.serviceUpdater.Stop()
	dt.locationsUpdater.Stop()

	dt.mu.Lock()
	dt.pendingServiceOps[uid] = &ServiceOperationState{ServiceUID: uid, Config: NewInboundServiceConfig(uid, nil),
		State: StateDeletionInProgress, InFlightKind: operationDelete}
	dt.mu.Unlock()
	dt.onServiceOperationComplete(uid, true, nil, nil)

	dt.mu.Lock()
	_, tracked := dt.pendingServiceOps[uid]
	dt.mu.Unlock()
	assert.False(t, tracked, "the first startup work completed")
	select {
	case <-dt.initCompletionChecker:
		t.Fatal("initialization must not complete while startup is still enqueueing its work")
	default:
	}

	dt.endInitializationPlanning()
	select {
	case <-dt.initCompletionChecker:
	default:
		t.Fatal("releasing the planning latch must complete an initialization whose work is done")
	}
}

// startupClients mocks the Azure clients InitializeFromCluster uses, for an empty Service Gateway.
type startupClients struct {
	*egressPassClients
	lb *mock_loadbalancerclient.MockInterface
}

// newStartupReaders returns the clients of a startup that only reads, its egress listings left to the test.
func newStartupReaders(ctrl *gomock.Controller) *startupClients {
	m := &startupClients{egressPassClients: newEgressPassClients(ctrl), lb: mock_loadbalancerclient.NewMockInterface(ctrl)}
	m.factory.EXPECT().GetLoadBalancerClient().Return(m.lb).AnyTimes()
	m.sgw.EXPECT().GetAddressLocations(gomock.Any(), "rg", "sgw").Return(nil, nil).AnyTimes()
	m.lb.EXPECT().List(gomock.Any(), "rg").Return(nil, nil).AnyTimes()
	return m
}

func newStartupClients(ctrl *gomock.Controller) *startupClients {
	m := newStartupReaders(ctrl)
	m.sgw.EXPECT().UpdateAddressLocations(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	m.sgw.EXPECT().UpdateServices(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	return m
}

// TestInitializeFromCluster_CompletesWhenStartupDispatchesNothing pins that a startup whose only work is declined
// before anything is dispatched completes: the release of the planning latch checks completion, as nothing else
// would. The only Service is an internal load balancer, which admission declines.
func TestInitializeFromCluster_CompletesWhenStartupDispatchesNothing(t *testing.T) {
	m := newStartupClients(gomock.NewController(t))
	m.sgw.EXPECT().GetServices(gomock.Any(), "rg", "sgw").Return(nil, nil).AnyTimes()
	m.nat.EXPECT().List(gomock.Any(), "rg").Return(nil, nil).AnyTimes()
	m.pip.EXPECT().List(gomock.Any(), "rg").Return(nil, nil).AnyTimes()
	m.prefix.EXPECT().List(gomock.Any(), "rg").Return(nil, nil).AnyTimes()

	internal := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "internal", Namespace: "ns", UID: "11111111-1111-1111-1111-111111111111",
			Annotations: map[string]string{consts.ServiceAnnotationLoadBalancerInternal: "true"}},
		Spec: v1.ServiceSpec{Type: v1.ServiceTypeLoadBalancer, Ports: []v1.ServicePort{{Port: 80, Protocol: v1.ProtocolTCP}}},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dt, err := InitializeFromCluster(ctx, testConfig(), m.factory, fake.NewSimpleClientset(internal))
	assert.NoError(t, err, "startup must complete although it dispatched nothing")
	assert.NotNil(t, dt)
}

// TestInitializeFromCluster_ConfirmationPassNeverHoldsInitialization pins that startup runs the confirmation pass once
// initialization completed, in the background: initialization returns before the pass reads anything, and the pass
// then reads the four listings.
func TestInitializeFromCluster_ConfirmationPassNeverHoldsInitialization(t *testing.T) {
	setStartupSettleDelay(t, time.Millisecond)
	m := newStartupClients(gomock.NewController(t))
	release, passRead := make(chan struct{}), make(chan struct{})
	var services atomic.Int32
	m.sgw.EXPECT().GetServices(gomock.Any(), "rg", "sgw").DoAndReturn(
		func(context.Context, string, string) ([]*armnetwork.ServiceGatewayService, error) {
			if services.Add(1) > 1 {
				<-release // the confirmation pass
			}
			return nil, nil
		}).Times(2)
	m.nat.EXPECT().List(gomock.Any(), "rg").Return(nil, nil).Times(2)
	// Not nil: the orphan sweep lists the Public IPs again when startup's listing is nil.
	m.pip.EXPECT().List(gomock.Any(), "rg").Return([]*armnetwork.PublicIPAddress{}, nil).Times(2)
	var prefixes atomic.Int32
	m.prefix.EXPECT().List(gomock.Any(), "rg").DoAndReturn(func(context.Context, string) ([]*armnetwork.PublicIPPrefix, error) {
		if prefixes.Add(1) > 1 {
			close(passRead)
		}
		return nil, nil
	}).Times(2)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	initialized := make(chan error, 1)
	go func() {
		_, err := InitializeFromCluster(ctx, testConfig(), m.factory, fake.NewSimpleClientset())
		initialized <- err
	}()
	select {
	case err := <-initialized:
		assert.NoError(t, err)
	case <-time.After(10 * time.Second):
		close(release)
		t.Fatal("initialization waited for the confirmation pass")
	}

	close(release)
	select {
	case <-passRead:
	case <-time.After(10 * time.Second):
		t.Fatal("the confirmation pass did not read the listings")
	}
}

// TestInitializeFromCluster_SweepsOrphanedManagedPublicIPsAndPrefixes pins step 6 of 4.6 through startup: the orphan
// sweep recognises the numbered managed Public IPs and the managed prefixes of an identity nothing accounts for by
// their reserved name and the ownership tag, and deletes them; untagged ones, prefixes in use (by a NAT Gateway, by
// Public IPs allocated from them or by a load balancer frontend) and names that are not reserved are kept. Base Public
// IPs keep being recognised by their name alone.
func TestInitializeFromCluster_SweepsOrphanedManagedPublicIPsAndPrefixes(t *testing.T) {
	cfg := testConfig()
	c := newEgressCloud(t)
	c.addPIP(PublicIPName(startupGone), "", armnetwork.ProvisioningStateSucceeded)
	c.addPIP(managedPublicIPName(startupGone, false, 2), startupGone, armnetwork.ProvisioningStateSucceeded)
	c.addPIP(managedPublicIPName(startupGone, false, 3), "", armnetwork.ProvisioningStateSucceeded)
	c.addPIP(managedPublicIPName(startupGone, true, 2), startupGone, armnetwork.ProvisioningStateSucceeded)
	c.addPIP(managedPublicIPName(startupGone, false, 4), startupOther, armnetwork.ProvisioningStateSucceeded)
	c.addPIP(managedPrefixName(startupGone, false, 28), startupGone, armnetwork.ProvisioningStateSucceeded)
	c.addPrefix(managedPrefixName(startupGone, false, 31), startupGone, 31, armnetwork.ProvisioningStateSucceeded)
	c.addPrefix(managedPrefixName(startupGone, true, 124), startupGone, 124, armnetwork.ProvisioningStateSucceeded)
	c.addPrefix(managedPrefixName(startupGone, false, 30), "", 30, armnetwork.ProvisioningStateSucceeded)
	c.addPrefix(startupGone+publicIPPrefixNameSuffix+"-27", startupGone, 27, armnetwork.ProvisioningStateSucceeded)
	c.addPrefix(managedPrefixName(startupGone, false, 29), startupGone, 29, armnetwork.ProvisioningStateSucceeded)
	c.prefixes[strings.ToLower(managedPrefixName(startupGone, false, 29))].Properties.NatGateway = &armnetwork.NatGateway{ID: to.Ptr("elsewhere")}
	c.addPrefix(managedPrefixName(startupGone, true, 125), startupGone, 125, armnetwork.ProvisioningStateSucceeded)
	c.prefixes[strings.ToLower(managedPrefixName(startupGone, true, 125))].Properties.PublicIPAddresses =
		[]*armnetwork.ReferencedPublicIPAddress{{ID: to.Ptr("elsewhere")}}
	c.addPrefix(managedPrefixName(startupGone, true, 126), startupGone, 126, armnetwork.ProvisioningStateSucceeded)
	c.prefixes[strings.ToLower(managedPrefixName(startupGone, true, 126))].Properties.LoadBalancerFrontendIPConfiguration =
		&armnetwork.SubResource{ID: to.Ptr("elsewhere")}

	m := newStartupClients(gomock.NewController(t))
	m.sgw.EXPECT().GetServices(gomock.Any(), "rg", "sgw").Return(nil, nil).AnyTimes()
	m.nat.EXPECT().List(gomock.Any(), "rg").Return(nil, nil).AnyTimes()
	m.pip.EXPECT().List(gomock.Any(), "rg").Return(c.pipList(), nil).AnyTimes()
	m.prefix.EXPECT().List(gomock.Any(), "rg").Return(c.prefixList(), nil).AnyTimes()
	var (
		mu      sync.Mutex
		deleted []string
	)
	recordDelete := func(_ context.Context, _, name string) error {
		mu.Lock()
		defer mu.Unlock()
		deleted = append(deleted, name)
		return nil
	}
	m.pip.EXPECT().Delete(gomock.Any(), cfg.ResourceGroup, gomock.Any()).DoAndReturn(recordDelete).AnyTimes()
	m.prefix.EXPECT().Delete(gomock.Any(), cfg.ResourceGroup, gomock.Any()).DoAndReturn(recordDelete).AnyTimes()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err := InitializeFromCluster(ctx, cfg, m.factory, fake.NewSimpleClientset())
	assert.NoError(t, err)

	mu.Lock()
	defer mu.Unlock()
	assert.ElementsMatch(t, []string{
		PublicIPName(startupGone),
		managedPublicIPName(startupGone, false, 2),
		managedPublicIPName(startupGone, true, 2),
		managedPrefixName(startupGone, false, 31),
		managedPrefixName(startupGone, true, 124),
	}, deleted)
}

// TestCleanupOrphanedManagedPublicIPsAndPrefixes pins that the orphan sweep keeps the numbered managed Public IPs and
// the managed prefixes of an identity something accounts for: desired in Kubernetes or registered with the Service
// Gateway, as for base Public IPs.
func TestCleanupOrphanedManagedPublicIPsAndPrefixes(t *testing.T) {
	const desired, registered = "egress-desired", "egress-registered"
	cfg := testConfig()
	c := newEgressCloud(t)
	for _, id := range []string{desired, registered, startupGone} {
		c.addPIP(managedPublicIPName(id, false, 2), id, armnetwork.ProvisioningStateSucceeded)
		c.addPrefix(managedPrefixName(id, false, 31), id, 31, armnetwork.ProvisioningStateSucceeded)
	}

	m := newEgressPassClients(gomock.NewController(t))
	var (
		mu      sync.Mutex
		deleted []string
	)
	recordDelete := func(_ context.Context, _, name string) error {
		mu.Lock()
		defer mu.Unlock()
		deleted = append(deleted, name)
		return nil
	}
	m.pip.EXPECT().Delete(gomock.Any(), cfg.ResourceGroup, gomock.Any()).DoAndReturn(recordDelete).AnyTimes()
	m.prefix.EXPECT().Delete(gomock.Any(), cfg.ResourceGroup, gomock.Any()).DoAndReturn(recordDelete).AnyTimes()
	dt := newStartupTracker(false)
	dt.networkClientFactory = m.factory
	dt.K8sResources.Egresses.Insert(desired)
	dt.NRPResources.NATGateways.Insert(registered)

	assert.NoError(t, dt.cleanupOrphanedPublicIPs(context.Background(), c.pipList()))
	assert.NoError(t, dt.cleanupOrphanedPublicIPPrefixes(context.Background(), c.prefixList()))

	assert.ElementsMatch(t, []string{managedPublicIPName(startupGone, false, 2), managedPrefixName(startupGone, false, 31)}, deleted)
}

// TestCleanupOrphanedPublicIPPrefixes_EndsOnceEveryDeletionEnded pins how the orphan prefix sweep ends when its
// deletions end at different times: it returns only once every DELETE it started has ended, with the error of the one
// that failed and the other orphans deleted. The failing DELETE is answered while another is still in flight.
func TestCleanupOrphanedPublicIPPrefixes_EndsOnceEveryDeletionEnded(t *testing.T) {
	prefix := func(length int) *armnetwork.PublicIPPrefix {
		return egressStartupPrefix(managedPrefixName(startupGone, false, length), startupGone, int32(length), armnetwork.ProvisioningStateSucceeded)
	}
	failing, quick, blocked := prefix(31), prefix(30), prefix(29)
	deleteAnswer := func(status int, code string) error {
		req, _ := http.NewRequestWithContext(context.Background(), http.MethodDelete, "https://management.azure.com"+*failing.ID, nil)
		return &azcore.ResponseError{StatusCode: status, ErrorCode: code, RawResponse: &http.Response{StatusCode: status, Request: req}}
	}

	for _, tc := range []struct {
		name    string
		failure error // the answer to the DELETE of failing
	}{
		{name: "every deletion succeeds"},
		{name: "a deletion refused while another is in flight", failure: deleteAnswer(http.StatusConflict, "Conflict")},
		{name: "a deletion with an unknown outcome while another is in flight", failure: deleteAnswer(http.StatusInternalServerError, "InternalServerError")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			blockedStarted, failingAnswered, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var (
				inFlight atomic.Int32
				mu       sync.Mutex
				deleted  []string
			)
			track := func(answer func() error) func(context.Context, string, string) error {
				return func(_ context.Context, _, name string) error {
					inFlight.Add(1)
					defer inFlight.Add(-1)
					err := answer()
					if err == nil {
						mu.Lock()
						deleted = append(deleted, name)
						mu.Unlock()
					}
					return err
				}
			}
			m := newEgressPassClients(gomock.NewController(t))
			m.prefix.EXPECT().Delete(gomock.Any(), "rg", *blocked.Name).DoAndReturn(track(func() error {
				close(blockedStarted)
				<-release
				return nil
			}))
			m.prefix.EXPECT().Delete(gomock.Any(), "rg", *failing.Name).DoAndReturn(track(func() error {
				<-blockedStarted
				defer close(failingAnswered)
				return tc.failure
			}))
			m.prefix.EXPECT().Delete(gomock.Any(), "rg", *quick.Name).DoAndReturn(track(func() error { return nil }))
			dt := newStartupTracker(false)
			dt.networkClientFactory = m.factory

			type result struct {
				err      error
				inFlight int32
			}
			returned := make(chan result, 1)
			go func() {
				err := dt.cleanupOrphanedPublicIPPrefixes(context.Background(), []*armnetwork.PublicIPPrefix{failing, quick, blocked})
				returned <- result{err, inFlight.Load()}
			}()
			select {
			case <-failingAnswered:
			case <-time.After(10 * time.Second):
				t.Fatal("the failing DELETE was not answered while the blocked one was in flight")
			}
			close(release)
			var r result
			select {
			case r = <-returned:
			case <-time.After(10 * time.Second):
				t.Fatal("the sweep did not return once its deletions ended")
			}

			assert.Zero(t, r.inFlight, "the sweep returned before every DELETE it started ended")
			wantDeleted := []string{*quick.Name, *blocked.Name}
			if tc.failure == nil {
				assert.NoError(t, r.err)
				wantDeleted = append(wantDeleted, *failing.Name)
			} else {
				assert.ErrorIs(t, r.err, tc.failure)
				assert.ErrorContains(t, r.err, *failing.Name)
			}
			mu.Lock()
			defer mu.Unlock()
			assert.ElementsMatch(t, wantDeleted, deleted)
		})
	}
}

// TestInitializeFromCluster_AdoptsRegisteredEgressIdentities pins the startup wiring of 4.6 steps 1-3: a registered
// managed identity with a member gets a Created operation for the IP families its listed NAT Gateway serves, whatever
// the Node list shows, recording the configuration it satisfies, and nothing is run for it when nothing is actionable.
func TestInitializeFromCluster_AdoptsRegisteredEgressIdentities(t *testing.T) {
	cases := []struct {
		name    string
		natIPv6 bool
		nodeIPs []string
	}{
		{name: "IPv4 NAT Gateway, IPv4 nodes", nodeIPs: []string{"10.0.0.1"}},
		{name: "dual-stack NAT Gateway, nodes listed without their IPv6 address", natIPv6: true, nodeIPs: []string{"10.0.0.1"}},
		{name: "IPv4 NAT Gateway, dual-stack nodes", nodeIPs: []string{"10.0.0.1", "fd00::1"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			applied := defaultEgressConfig(egressParseContext{WantsIPv6: tc.natIPv6})
			c := newEgressCloud(t)
			c.applied(startupIdentity, applied, tc.natIPv6)

			m := newStartupClients(gomock.NewController(t))
			m.sgw.EXPECT().GetServices(gomock.Any(), "rg", "sgw").Return(c.serviceList(), nil).AnyTimes()
			m.nat.EXPECT().List(gomock.Any(), "rg").Return(c.natList(), nil).AnyTimes()
			m.pip.EXPECT().List(gomock.Any(), "rg").Return(c.pipList(), nil).AnyTimes()
			m.prefix.EXPECT().List(gomock.Any(), "rg").Return(c.prefixList(), nil).AnyTimes()

			node := &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}}
			for _, ip := range tc.nodeIPs {
				node.Status.Addresses = append(node.Status.Addresses, v1.NodeAddress{Type: v1.NodeInternalIP, Address: ip})
			}
			pod := newEgressPod("member", "ns", startupIdentity, "node-1", "10.244.0.5", "10.0.0.1", v1.PodRunning)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			dt, err := InitializeFromCluster(ctx, testConfig(), m.factory, fake.NewSimpleClientset(node, pod))
			if !assert.NoError(t, err) {
				return
			}
			dt.mu.Lock()
			defer dt.mu.Unlock()
			assert.Equal(t, egressModeManaged, dt.egress.mode[startupIdentity])
			if op := dt.pendingServiceOps[startupIdentity]; assert.NotNil(t, op, "the identity must be adopted") {
				assert.Equal(t, egressStartupOp{State: StateCreated, Applied: applied, Desired: applied, Baseline: true}, describeEgressStartupOp(op))
				wantFamilies := ipv4Families()
				if tc.natIPv6 {
					wantFamilies = dualStackFamilies()
				}
				assert.Equal(t, wantFamilies, op.Config.OutboundConfig.IPFamilies, "the families its NAT Gateway serves")
			}
		})
	}
}
