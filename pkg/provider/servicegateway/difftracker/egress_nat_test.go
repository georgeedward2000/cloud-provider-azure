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
	"encoding/json"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork/v12"
	"github.com/stretchr/testify/assert"

	"sigs.k8s.io/cloud-provider-azure/pkg/consts"
)

const egressNATTestID = "team-egress"

// egressNATTestDT keeps network resources in their own subscription, so managed resource IDs must use it.
func egressNATTestDT() Config {
	return Config{
		SubscriptionID:                "sub",
		NetworkResourceSubscriptionID: "netsub",
		ResourceGroup:                 "MC_rg",
		Location:                      "westus2",
		ServiceGatewayResourceName:    "sgw",
		VNetName:                      "vnet",
	}
}

func egressNATTestUserID(kind, name string) string {
	return "/subscriptions/netsub/resourceGroups/User-RG/providers/Microsoft.Network/" + kind + "/" + name
}

func egressNATTestManagedID(kind, name string) string {
	return "/subscriptions/netsub/resourceGroups/MC_rg/providers/Microsoft.Network/" + kind + "/" + name
}

// egressNATTestConfig parses annotations for the test identity; its NAT Gateway serves IPv6 when ipv6 is set.
func egressNATTestConfig(t *testing.T, ipv6 bool, ann map[string]string) EgressConfig {
	t.Helper()
	cfg, _, err := parseEgressConfig(ann, egressParseContext{WantsIPv6: ipv6, NetworkSubscription: "netsub", NodeResourceGroup: "MC_rg"})
	if err != nil {
		t.Fatalf("parsing %v: %v", ann, err)
	}
	return cfg
}

// egressNATTestRichConfig is a dual-stack configuration that uses every setting.
func egressNATTestRichConfig(t *testing.T) EgressConfig {
	return egressNATTestConfig(t, true, map[string]string{
		consts.PodAnnotationServiceEgressGatewayIdleTimeout:              "10",
		consts.PodAnnotationServiceEgressGatewayPublicIPCount:            "2",
		consts.PodAnnotationServiceEgressGatewayPublicIPPrefixLength:     "30",
		consts.PodAnnotationServiceEgressGatewayPublicIPIDs:              egressNATTestUserID("publicIPAddresses", "user-pip"),
		consts.PodAnnotationServiceEgressGatewayPublicIPPrefixIDs:        egressNATTestUserID("publicIPPrefixes", "user-prefix"),
		consts.PodAnnotationServiceEgressGatewayPublicIPCountIPv6:        "1",
		consts.PodAnnotationServiceEgressGatewayPublicIPPrefixLengthIPv6: "127",
		consts.PodAnnotationServiceEgressGatewayPublicIPIDsIPv6:          egressNATTestUserID("publicIPAddresses", "user-pip-v6"),
		consts.PodAnnotationServiceEgressGatewayPublicIPPrefixIDsIPv6:    egressNATTestUserID("publicIPPrefixes", "user-prefix-v6"),
		consts.PodAnnotationServiceEgressGatewayTags:                     "Env=Prod,team=net",
	})
}

func egressNATTestJSON(t *testing.T, v any) string {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshalling %T: %v", v, err)
	}
	return string(data)
}

// egressNATTestMust returns v and fails the test when err is set: egressNATTestMust(f())(t).
func egressNATTestMust[T any](v T, err error) func(t *testing.T) T {
	return func(t *testing.T) T {
		t.Helper()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		return v
	}
}

func TestManagedResourceNames(t *testing.T) {
	cases := []struct {
		name, got, want string
	}{
		{"first IPv4 Public IP keeps today's name", managedPublicIPName("id", false, 1), PublicIPName("id")},
		{"first IPv6 Public IP keeps today's name", managedPublicIPName("id", true, 1), PublicIPNameV6("id")},
		{"second IPv4 Public IP", managedPublicIPName("id", false, 2), "id-pip-2"},
		{"last IPv4 Public IP", managedPublicIPName("id", false, 16), "id-pip-16"},
		{"second IPv6 Public IP", managedPublicIPName("id", true, 2), "id-pip-v6-2"},
		{"last IPv6 Public IP", managedPublicIPName("id", true, 16), "id-pip-v6-16"},
		{"shortest IPv4 prefix", managedPrefixName("id", false, 28), "id-prefix-28"},
		{"longest IPv4 prefix", managedPrefixName("id", false, 31), "id-prefix-31"},
		{"shortest IPv6 prefix", managedPrefixName("id", true, 124), "id-prefix-v6-124"},
		{"longest IPv6 prefix", managedPrefixName("id", true, 127), "id-prefix-v6-127"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.got)
		})
	}
	assert.Equal(t, "id-pip", managedPublicIPName("id", false, 1), "control: today's IPv4 name")
	assert.Equal(t, "id-pip-v6", managedPublicIPName("id", true, 1), "control: today's IPv6 name")
}

func TestParseManagedName(t *testing.T) {
	t.Run("every reserved name round-trips in any case", func(t *testing.T) {
		for _, m := range reservedManaged(true) {
			name := m.name(egressNATTestID)
			spellings := []string{name, strings.ToUpper(name), "Team-Egress" + name[len(egressNATTestID):]}
			for _, spelling := range spellings {
				got, ok := parseManagedName(egressNATTestID, spelling)
				assert.True(t, ok, spelling)
				assert.Equal(t, m, got, spelling)
			}
			got, ok := parseManagedName("TEAM-EGRESS", name)
			assert.True(t, ok, "the identity compares case-insensitively too: %s", name)
			assert.Equal(t, m, got, name)
		}
	})

	t.Run("kinds, families and numbers", func(t *testing.T) {
		for name, want := range map[string]egressManagedName{
			"team-egress-pip":           {Kind: egressManagedPublicIP, N: 1},
			"team-egress-pip-16":        {Kind: egressManagedPublicIP, N: 16},
			"team-egress-pip-v6":        {Kind: egressManagedPublicIP, IPv6: true, N: 1},
			"team-egress-pip-v6-2":      {Kind: egressManagedPublicIP, IPv6: true, N: 2},
			"team-egress-prefix-28":     {Kind: egressManagedPrefix, N: 28},
			"team-egress-prefix-v6-127": {Kind: egressManagedPrefix, IPv6: true, N: 127},
		} {
			got, ok := parseManagedName(egressNATTestID, name)
			assert.True(t, ok, name)
			assert.Equal(t, want, got, name)
		}
	})

	t.Run("names that are not reserved", func(t *testing.T) {
		for _, name := range []string{
			"", "team-egress", "team-egress-", "team-egress-pip-1", "team-egress-pip-01", "team-egress-pip-02",
			"team-egress-pip-0", "team-egress-pip-17", "team-egress-pip-v6-1", "team-egress-pip-v6-17", "team-egress-pip-2x",
			"team-egress-pip-+2", "team-egress-pip- 2", "team-egress-pipx", "team-egress-pip-pip", "team-egress-prefix",
			"team-egress-prefix-27", "team-egress-prefix-32", "team-egress-prefix-124", "team-egress-prefix-v6-123",
			"team-egress-prefix-v6-128", "team-egress-prefix-v6-28", "team-egress-prefix-v6", "other-pip", "team-egres-pip",
			"xteam-egress-pip", " team-egress-pip",
		} {
			_, ok := parseManagedName(egressNATTestID, name)
			assert.False(t, ok, "%q must not be a reserved name of %q", name, egressNATTestID)
		}
	})

	t.Run("identities sharing a stem do not share names", func(t *testing.T) {
		_, ok := parseManagedName("a", "a-pip-pip")
		assert.False(t, ok)
		got, ok := parseManagedName("a-pip", "a-pip-pip")
		assert.True(t, ok)
		assert.Equal(t, egressManagedName{Kind: egressManagedPublicIP, N: 1}, got)
		_, ok = parseManagedName("a-pip", "a-pip-2")
		assert.False(t, ok, "the second Public IP of a is not a name of a-pip")
	})
}

// reservedManagedNames returns every name egress identity id reserves for the Public IPs and prefixes the controller
// manages in the served families (IPv4, and IPv6 when the NAT Gateway serves it), as reservedManaged lists them: Public
// IPs first, at most 20 names per family.
func reservedManagedNames(id string, ipv6 bool) []string {
	reserved := reservedManaged(ipv6)
	names := make([]string, 0, len(reserved))
	for _, m := range reserved {
		names = append(names, m.name(id))
	}
	return names
}

// desiredManagedNames returns the names of the managed Public IPs and prefixes cfg asks for, as each family's
// egressFamilyConfig.managed lists them: Public IPs 1..count and the prefix of each family, IPv4 first.
func desiredManagedNames(id string, cfg EgressConfig) []string {
	var names []string
	for _, v6 := range egressServedFamilies(true) {
		for _, m := range cfg.familyConfig(v6).managed(v6) {
			names = append(names, m.name(id))
		}
	}
	return names
}

func TestReservedManagedNames(t *testing.T) {
	ipv4 := reservedManagedNames("id", false)
	dual := reservedManagedNames("id", true)

	wantIPv4 := []string{"id-pip"}
	for n := 2; n <= 16; n++ {
		wantIPv4 = append(wantIPv4, fmt.Sprintf("id-pip-%d", n))
	}
	wantIPv4 = append(wantIPv4, "id-prefix-28", "id-prefix-29", "id-prefix-30", "id-prefix-31")
	wantIPv6 := []string{"id-pip-v6"}
	for n := 2; n <= 16; n++ {
		wantIPv6 = append(wantIPv6, fmt.Sprintf("id-pip-v6-%d", n))
	}
	wantIPv6 = append(wantIPv6, "id-prefix-v6-124", "id-prefix-v6-125", "id-prefix-v6-126", "id-prefix-v6-127")

	assert.Equal(t, wantIPv4, ipv4, "an IPv4-only identity reserves 20 names")
	assert.Equal(t, append(slices.Clone(wantIPv4), wantIPv6...), dual, "a dual-stack identity reserves 20 more for IPv6")
	assert.Subset(t, dual, []string{PublicIPName("id"), PublicIPNameV6("id")}, "the names deleted before configuration existed stay reserved")

	seen := map[string]bool{}
	for _, name := range dual {
		assert.False(t, seen[strings.ToLower(name)], "%s is reserved twice", name)
		seen[strings.ToLower(name)] = true
		m, ok := parseManagedName("id", name)
		assert.True(t, ok, name)
		assert.Equal(t, name, m.name("id"))
		assert.True(t, m.reserved(), name)
	}
}

func TestDesiredManagedNames(t *testing.T) {
	cases := []struct {
		name string
		cfg  EgressConfig
		want []string
	}{
		{"default IPv4", defaultEgressConfig(egressParseContext{}), []string{"team-egress-pip"}},
		{"default dual-stack", defaultEgressConfig(egressParseContext{WantsIPv6: true}), []string{"team-egress-pip", "team-egress-pip-v6"}},
		{"every setting", egressNATTestRichConfig(t), []string{
			"team-egress-pip", "team-egress-pip-2", "team-egress-prefix-30", "team-egress-pip-v6", "team-egress-prefix-v6-127",
		}},
		{"managed prefix only", egressNATTestConfig(t, false, map[string]string{
			consts.PodAnnotationServiceEgressGatewayPublicIPPrefixLength: "31",
		}), []string{"team-egress-prefix-31"}},
		{"user resources only", egressNATTestConfig(t, false, map[string]string{
			consts.PodAnnotationServiceEgressGatewayPublicIPIDs: egressNATTestUserID("publicIPAddresses", "user-pip"),
		}), nil},
		{"sixteen Public IPs", egressNATTestConfig(t, false, map[string]string{
			consts.PodAnnotationServiceEgressGatewayPublicIPCount: "16",
		}), reservedManagedNames(egressNATTestID, false)[:16]},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, desiredManagedNames(egressNATTestID, tc.cfg))
		})
	}
}

func egressNATTestTags(kv ...string) map[string]*string {
	tags := map[string]*string{}
	for i := 0; i+1 < len(kv); i += 2 {
		tags[kv[i]] = to.Ptr(kv[i+1])
	}
	return tags
}

func TestOwnsManagedName(t *testing.T) {
	const own = EgressIdentityTagKey
	cases := []struct {
		name, resource string
		tags           map[string]*string
		want           bool
	}{
		{"base IPv4 Public IP by its name", "team-egress-pip", nil, true},
		{"base IPv6 Public IP by its name", "team-egress-pip-v6", nil, true},
		{"base Public IP named in another case", "TEAM-EGRESS-PIP", nil, true},
		{"base Public IP tagged for another identity, as today", "team-egress-pip", egressNATTestTags(own, "other"), true},
		{"numbered Public IP with the ownership tag", "team-egress-pip-2", egressNATTestTags(own, "team-egress"), true},
		{"numbered Public IP, tag value in another case", "team-egress-pip-v6-16", egressNATTestTags(own, "TEAM-EGRESS"), true},
		{"numbered Public IP without tags", "team-egress-pip-2", nil, false},
		{"numbered Public IP with foreign tags only", "team-egress-pip-2", egressNATTestTags("env", "team-egress"), false},
		{"numbered Public IP tagged for another identity", "team-egress-pip-2", egressNATTestTags(own, "other"), false},
		{"numbered Public IP, tag key in another case", "team-egress-pip-2", egressNATTestTags(strings.ToUpper(own), "team-egress"), false},
		{"numbered Public IP, tag without value", "team-egress-pip-2", map[string]*string{own: nil}, false},
		{"prefix with the ownership tag", "team-egress-prefix-31", egressNATTestTags(own, "team-egress"), true},
		{"IPv6 prefix with the ownership tag", "team-egress-prefix-v6-124", egressNATTestTags(own, "team-egress"), true},
		{"prefix without the ownership tag", "team-egress-prefix-31", nil, false},
		{"prefix tagged for another identity", "team-egress-prefix-31", egressNATTestTags(own, "other"), false},
		{"tagged name that is not reserved", "team-egress-pip-17", egressNATTestTags(own, "team-egress"), false},
		{"base name of another identity", "other-pip", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, ownsManagedName(egressNATTestID, tc.resource, tc.tags))
		})
	}
}

func TestManagedOwnershipRecorded(t *testing.T) {
	const own = EgressIdentityTagKey
	cases := []struct {
		name, resource string
		tags           map[string]*string
		legacyNAT      bool
		want           bool
	}{
		{"tagged numbered Public IP", "team-egress-pip-2", egressNATTestTags(own, "team-egress"), false, true},
		{"tagged base Public IP", "team-egress-pip", egressNATTestTags(own, "Team-Egress"), false, true},
		{"tagged prefix", "team-egress-prefix-30", egressNATTestTags(own, "team-egress"), false, true},
		{"untagged base Public IP of a legacy NAT Gateway", "team-egress-pip", nil, true, true},
		{"untagged base IPv6 Public IP of a legacy NAT Gateway", "team-egress-pip-v6", egressNATTestTags("env", "x"), true, true},
		{"untagged base Public IP otherwise", "team-egress-pip", nil, false, false},
		{"base Public IP tagged for another identity", "team-egress-pip", egressNATTestTags(own, "other"), true, false},
		{"base Public IP with an empty ownership tag", "team-egress-pip", map[string]*string{own: nil}, true, false},
		{"untagged numbered Public IP of a legacy NAT Gateway", "team-egress-pip-2", nil, true, false},
		{"untagged prefix of a legacy NAT Gateway", "team-egress-prefix-30", nil, true, false},
		{"tagged name that is not reserved", "team-egress-pip-17", egressNATTestTags(own, "team-egress"), true, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, managedOwnershipRecorded(egressNATTestID, tc.resource, tc.tags, tc.legacyNAT))
		})
	}
}

func TestLegacyEgressNAT(t *testing.T) {
	dt := egressNATTestDT()
	pip := func(name string) *armnetwork.SubResource {
		return &armnetwork.SubResource{ID: to.Ptr(egressNATTestManagedID("publicIPAddresses", name))}
	}
	nat := func(mutate func(*armnetwork.NatGateway)) *armnetwork.NatGateway {
		n := &armnetwork.NatGateway{
			Name: to.Ptr(egressNATTestID),
			Properties: &armnetwork.NatGatewayPropertiesFormat{
				PublicIPAddresses: []*armnetwork.SubResource{pip("team-egress-pip")},
			},
		}
		if mutate != nil {
			mutate(n)
		}
		return n
	}
	dual := func(n *armnetwork.NatGateway) {
		n.Properties.PublicIPAddressesV6 = []*armnetwork.SubResource{pip("team-egress-pip-v6")}
	}
	cases := []struct {
		name string
		nat  *armnetwork.NatGateway
		ipv6 bool
		want bool
	}{
		{"untagged IPv4 NAT Gateway with its base Public IP", nat(nil), false, true},
		{"untagged dual-stack NAT Gateway with both base Public IPs", nat(dual), true, true},
		{"reference IDs in another case", nat(func(n *armnetwork.NatGateway) {
			n.Properties.PublicIPAddresses[0].ID = to.Ptr(strings.ToUpper(*n.Properties.PublicIPAddresses[0].ID))
			n.Name = to.Ptr("TEAM-EGRESS")
		}), false, true},
		{"foreign tags only", nat(func(n *armnetwork.NatGateway) { n.Tags = egressNATTestTags("env", "x") }), false, true},
		{"tagged NAT Gateway", nat(func(n *armnetwork.NatGateway) { n.Tags = egressIdentityTags(egressNATTestID) }), false, false},
		{"empty ownership tag", nat(func(n *armnetwork.NatGateway) { n.Tags = map[string]*string{EgressIdentityTagKey: nil} }), false, false},
		{"dual-stack NAT Gateway of an IPv4-only identity", nat(dual), false, false},
		{"IPv4 NAT Gateway of a dual-stack identity", nat(nil), true, false},
		{"numbered Public IP", nat(func(n *armnetwork.NatGateway) {
			n.Properties.PublicIPAddresses = append(n.Properties.PublicIPAddresses, pip("team-egress-pip-2"))
		}), false, false},
		{"user prefix", nat(func(n *armnetwork.NatGateway) {
			n.Properties.PublicIPPrefixes = []*armnetwork.SubResource{{ID: to.Ptr(egressNATTestUserID("publicIPPrefixes", "p"))}}
		}), false, false},
		{"user IPv6 prefix", nat(func(n *armnetwork.NatGateway) {
			dual(n)
			n.Properties.PublicIPPrefixesV6 = []*armnetwork.SubResource{{ID: to.Ptr(egressNATTestUserID("publicIPPrefixes", "p6"))}}
		}), true, false},
		{"base Public IP in another resource group", nat(func(n *armnetwork.NatGateway) {
			n.Properties.PublicIPAddresses = []*armnetwork.SubResource{{ID: to.Ptr(egressNATTestUserID("publicIPAddresses", "team-egress-pip"))}}
		}), false, false},
		{"base Public IP in another subscription", nat(func(n *armnetwork.NatGateway) {
			n.Properties.PublicIPAddresses = []*armnetwork.SubResource{{ID: to.Ptr(
				"/subscriptions/sub/resourceGroups/MC_rg/providers/Microsoft.Network/publicIPAddresses/team-egress-pip")}}
		}), false, false},
		{"IPv6 base Public IP in the IPv4 list", nat(func(n *armnetwork.NatGateway) {
			n.Properties.PublicIPAddresses = []*armnetwork.SubResource{pip("team-egress-pip-v6")}
		}), false, false},
		{"NAT Gateway of another name", nat(func(n *armnetwork.NatGateway) { n.Name = to.Ptr("other") }), false, false},
		{"no properties", nat(func(n *armnetwork.NatGateway) { n.Properties = nil }), false, false},
		{"no NAT Gateway", nil, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := legacyEgressNAT(tc.nat, egressNATTestID, tc.ipv6, dt)
			assert.Equal(t, tc.want, got)
			if got {
				assert.True(t, ownsNATGateway(tc.nat, dt), "a legacy NAT Gateway is one the legacy ownership rule owns")
			}
		})
	}
}

func TestMergeTags(t *testing.T) {
	const own = EgressIdentityTagKey
	cases := []struct {
		name       string
		existing   map[string]*string
		configured string
		want       map[string]*string
	}{
		{"from scratch", nil, "", egressNATTestTags(own, "team-egress")},
		{"configured tags keep the user's spelling", nil, "Env=Prod,team=net", egressNATTestTags("Env", "Prod", "team", "net", own, "team-egress")},
		{"foreign tags are kept", egressNATTestTags("Owner", "alice"), "env=prod", egressNATTestTags("Owner", "alice", "env", "prod", own, "team-egress")},
		{"tags no longer configured are kept", egressNATTestTags("old", "1", own, "team-egress"), "new=2", egressNATTestTags("old", "1", "new", "2", own, "team-egress")},
		{"a configured key updates an existing key in another case and keeps its spelling", egressNATTestTags("Env", "dev"), "ENV=prod", egressNATTestTags("Env", "prod", own, "team-egress")},
		{"values are updated exactly", egressNATTestTags("env", "Prod"), "env=prod", egressNATTestTags("env", "prod", own, "team-egress")},
		{"empty configured value", egressNATTestTags("env", "dev"), "env=", egressNATTestTags("env", "", own, "team-egress")},
		{"the ownership tag of another identity is replaced", egressNATTestTags(own, "other"), "", egressNATTestTags(own, "team-egress")},
		{"the ownership tag gets the spelling the controller reads", egressNATTestTags(strings.ToUpper(own), "team-egress", "x", "y"), "", egressNATTestTags("x", "y", own, "team-egress")},
		{"a tag without value is kept", map[string]*string{"empty": nil}, "", map[string]*string{"empty": nil, own: to.Ptr("team-egress")}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := mergeTags(tc.existing, tc.configured, egressNATTestID)
			assert.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}

	t.Run("keys that differ only in case, which Azure never returns, merge deterministically", func(t *testing.T) {
		for range 100 {
			got, err := mergeTags(egressNATTestTags("ENV", "b", "env", "a"), "Env=c", egressNATTestID)
			assert.NoError(t, err)
			if !assert.Equal(t, egressNATTestTags("ENV", "b", "env", "c", own, egressNATTestID), got) {
				break
			}
		}
	})

	t.Run("the existing tags are neither changed nor shared", func(t *testing.T) {
		existing := egressNATTestTags("env", "dev", "Owner", "alice", strings.ToUpper(own), "other")
		before := egressNATTestTags("env", "dev", "Owner", "alice", strings.ToUpper(own), "other")
		got, err := mergeTags(existing, "env=prod", egressNATTestID)
		assert.NoError(t, err)
		assert.Equal(t, before, existing)
		*got["Owner"] = "mallory"
		assert.Equal(t, "alice", *existing["Owner"], "the merged tags must not share values with the existing ones")
	})

	t.Run("Azure accepts 50 tags", func(t *testing.T) {
		foreign := func(n int) map[string]*string {
			tags := map[string]*string{}
			for i := range n {
				tags[fmt.Sprintf("foreign-%02d", i)] = to.Ptr("x")
			}
			return tags
		}
		got, err := mergeTags(foreign(48), "env=prod", egressNATTestID)
		assert.NoError(t, err)
		assert.Len(t, got, consts.ServiceEgressGatewayMaxTags, "48 foreign + 1 configured + ownership")

		got, err = mergeTags(foreign(49), "", egressNATTestID)
		assert.NoError(t, err)
		assert.Len(t, got, consts.ServiceEgressGatewayMaxTags, "49 foreign + ownership")

		withOwnership := foreign(49)
		withOwnership[own] = to.Ptr("team-egress")
		got, err = mergeTags(withOwnership, "foreign-00=y", egressNATTestID)
		assert.NoError(t, err, "updating a tag adds none")
		assert.Len(t, got, consts.ServiceEgressGatewayMaxTags)

		_, err = mergeTags(foreign(49), "env=prod", egressNATTestID)
		assert.ErrorContains(t, err, "51 tags, more than the 50")
		_, err = mergeTags(foreign(50), "", egressNATTestID)
		assert.ErrorContains(t, err, "51 tags")
		_, err = mergeTags(nil, egressTestTags(50), egressNATTestID)
		assert.ErrorContains(t, err, "51 tags")
	})
}

func TestMissingEgressTags(t *testing.T) {
	tags := egressNATTestTags("Env", "Prod", "team", "net")
	tags["empty"] = nil
	cases := []struct{ configured, want string }{
		{"", ""},
		{"Env=Prod,team=net", ""},
		{"ENV=Prod", ""},
		{"Env=prod", "Env=prod"},
		{"empty=", "empty="},
		{"a=1,Env=Prod,z=2", "a=1,z=2"},
	}
	for _, tc := range cases {
		assert.Equal(t, tc.want, missingEgressTags(tags, tc.configured), tc.configured)
	}
	assert.Equal(t, "a=1", missingEgressTags(nil, "a=1"))
}

func TestEgressManagedResources(t *testing.T) {
	pip := &armnetwork.PublicIPAddress{Name: to.Ptr("Team-Egress-PIP-2")}
	prefix := &armnetwork.PublicIPPrefix{Name: to.Ptr("team-egress-prefix-30")}
	r := newEgressManagedResources(
		[]*armnetwork.PublicIPAddress{nil, {}, pip},
		[]*armnetwork.PublicIPPrefix{nil, {}, prefix},
	)
	assert.Same(t, pip, r.publicIP("team-egress-pip-2"), "names compare case-insensitively")
	assert.Same(t, pip, r.publicIP("TEAM-EGRESS-PIP-2"))
	assert.Same(t, prefix, r.prefix("TEAM-EGRESS-PREFIX-30"))
	assert.Nil(t, r.publicIP("team-egress-prefix-30"), "Public IPs and prefixes are separate")
	assert.Nil(t, r.prefix("team-egress-pip-2"))
	assert.Nil(t, r.publicIP(""))
	assert.Nil(t, egressManagedResources{}.publicIP("team-egress-pip"), "the zero value holds nothing")
	assert.Nil(t, egressManagedResources{}.prefix("team-egress-prefix-30"))
}

// TestEgressRequests_Wire pins the exact JSON of the projections built from scratch on armnetwork v12 for a
// configuration that uses every setting: managed IDs as before configuration existed, user IDs in canonical form.
func TestEgressRequests_Wire(t *testing.T) {
	dt, cfg := egressNATTestDT(), egressNATTestRichConfig(t)
	const (
		managed = `/subscriptions/netsub/resourceGroups/MC_rg/providers/Microsoft.Network/`
		user    = `/subscriptions/netsub/resourcegroups/user-rg/providers/microsoft.network/`
		tags    = `"tags":{"Env":"Prod","k8s-azure-egress-identity":"team-egress","team":"net"}`
	)
	wantNAT := `{"id":"` + managed + `natGateways/team-egress","location":"westus2","name":"team-egress","properties":{` +
		`"idleTimeoutInMinutes":10,` +
		`"publicIpAddresses":[{"id":"` + managed + `publicIPAddresses/team-egress-pip"},{"id":"` + managed + `publicIPAddresses/team-egress-pip-2"},` +
		`{"id":"` + user + `publicipaddresses/user-pip"}],` +
		`"publicIpAddressesV6":[{"id":"` + managed + `publicIPAddresses/team-egress-pip-v6"},{"id":"` + user + `publicipaddresses/user-pip-v6"}],` +
		`"publicIpPrefixes":[{"id":"` + managed + `publicIPPrefixes/team-egress-prefix-30"},{"id":"` + user + `publicipprefixes/user-prefix"}],` +
		`"publicIpPrefixesV6":[{"id":"` + managed + `publicIPPrefixes/team-egress-prefix-v6-127"},{"id":"` + user + `publicipprefixes/user-prefix-v6"}],` +
		`"serviceGateway":{"id":"` + managed + `serviceGateways/sgw"}},"sku":{"name":"StandardV2"},` + tags + `}`
	assert.Equal(t, wantNAT, egressNATTestJSON(t, egressNATTestMust(natRequest(nil, egressNATTestID, cfg, true, dt))(t)))

	wantPIP := `{"id":"` + managed + `publicIPAddresses/team-egress-pip-2","location":"westus2","name":"team-egress-pip-2",` +
		`"properties":{"publicIPAddressVersion":"IPv4","publicIPAllocationMethod":"Static"},"sku":{"name":"StandardV2"},` + tags + `}`
	assert.Equal(t, wantPIP, egressNATTestJSON(t, egressNATTestMust(managedPublicIPRequest(nil, egressNATTestID, false, 2, cfg, dt))(t)))
	wantPIPv6 := `{"id":"` + managed + `publicIPAddresses/team-egress-pip-v6-16","location":"westus2","name":"team-egress-pip-v6-16",` +
		`"properties":{"publicIPAddressVersion":"IPv6","publicIPAllocationMethod":"Static"},"sku":{"name":"StandardV2"},` + tags + `}`
	assert.Equal(t, wantPIPv6, egressNATTestJSON(t, egressNATTestMust(managedPublicIPRequest(nil, egressNATTestID, true, 16, cfg, dt))(t)))

	wantPrefix := `{"id":"` + managed + `publicIPPrefixes/team-egress-prefix-30","location":"westus2","name":"team-egress-prefix-30",` +
		`"properties":{"prefixLength":30,"publicIPAddressVersion":"IPv4"},"sku":{"name":"StandardV2"},` + tags + `}`
	assert.Equal(t, wantPrefix, egressNATTestJSON(t, egressNATTestMust(managedPrefixRequest(nil, egressNATTestID, false, 30, cfg, dt))(t)))
	wantPrefixV6 := `{"id":"` + managed + `publicIPPrefixes/team-egress-prefix-v6-127","location":"westus2","name":"team-egress-prefix-v6-127",` +
		`"properties":{"prefixLength":127,"publicIPAddressVersion":"IPv6"},"sku":{"name":"StandardV2"},` + tags + `}`
	assert.Equal(t, wantPrefixV6, egressNATTestJSON(t, egressNATTestMust(managedPrefixRequest(nil, egressNATTestID, true, 127, cfg, dt))(t)))

	t.Run("IPv6 settings of a NAT Gateway that does not serve IPv6 are not written", func(t *testing.T) {
		nat := egressNATTestMust(natRequest(nil, egressNATTestID, cfg, false, dt))(t)
		assert.Nil(t, nat.Properties.PublicIPAddressesV6)
		assert.Nil(t, nat.Properties.PublicIPPrefixesV6)
		assert.Len(t, nat.Properties.PublicIPAddresses, 3)
	})
}

// egressCreationRequests returns the requests that create the NAT Gateway of identity id with cfg from scratch: the
// managed Public IPs of the served families, in order, and the NAT Gateway.
func egressCreationRequests(t *testing.T, id string, cfg EgressConfig, ipv6 bool, dt Config) ([]armnetwork.PublicIPAddress, armnetwork.NatGateway) {
	t.Helper()
	var pips []armnetwork.PublicIPAddress
	for _, v6 := range egressServedFamilies(ipv6) {
		for n := 1; n <= cfg.familyConfig(v6).ManagedPublicIPs; n++ {
			pips = append(pips, egressNATTestMust(managedPublicIPRequest(nil, id, v6, n, cfg, dt))(t))
		}
	}
	return pips, egressNATTestMust(natRequest(nil, id, cfg, ipv6, dt))(t)
}

// TestEgressRequests_DefaultWireUnchanged pins the exact JSON of the requests that create an identity with the default
// egress configuration: byte-identical to the requests made before configuration existed (captured at 72c561c3b), for
// IPv4-only and dual-stack identities, so existing and unannotated identities see no change.
func TestEgressRequests_DefaultWireUnchanged(t *testing.T) {
	dtConfig := Config{
		SubscriptionID:                "sub",
		NetworkResourceSubscriptionID: "netsub",
		ResourceGroup:                 "MC_rg",
		Location:                      "westus2",
		ServiceGatewayResourceName:    "sgw",
	}
	const (
		pipV4 = `{"id":"/subscriptions/netsub/resourceGroups/MC_rg/providers/Microsoft.Network/publicIPAddresses/team-egress-pip",` +
			`"location":"westus2","name":"team-egress-pip","properties":{"publicIPAddressVersion":"IPv4","publicIPAllocationMethod":"Static"},` +
			`"sku":{"name":"StandardV2"},"tags":{"k8s-azure-egress-identity":"team-egress"}}`
		pipV6 = `{"id":"/subscriptions/netsub/resourceGroups/MC_rg/providers/Microsoft.Network/publicIPAddresses/team-egress-pip-v6",` +
			`"location":"westus2","name":"team-egress-pip-v6","properties":{"publicIPAddressVersion":"IPv6","publicIPAllocationMethod":"Static"},` +
			`"sku":{"name":"StandardV2"},"tags":{"k8s-azure-egress-identity":"team-egress"}}`
		natIPv4 = `{"id":"/subscriptions/netsub/resourceGroups/MC_rg/providers/Microsoft.Network/natGateways/team-egress",` +
			`"location":"westus2","name":"team-egress","properties":{` +
			`"publicIpAddresses":[{"id":"/subscriptions/netsub/resourceGroups/MC_rg/providers/Microsoft.Network/publicIPAddresses/team-egress-pip"}],` +
			`"serviceGateway":{"id":"/subscriptions/netsub/resourceGroups/MC_rg/providers/Microsoft.Network/serviceGateways/sgw"}},` +
			`"sku":{"name":"StandardV2"},"tags":{"k8s-azure-egress-identity":"team-egress"}}`
		natDualStack = `{"id":"/subscriptions/netsub/resourceGroups/MC_rg/providers/Microsoft.Network/natGateways/team-egress",` +
			`"location":"westus2","name":"team-egress","properties":{` +
			`"publicIpAddresses":[{"id":"/subscriptions/netsub/resourceGroups/MC_rg/providers/Microsoft.Network/publicIPAddresses/team-egress-pip"}],` +
			`"publicIpAddressesV6":[{"id":"/subscriptions/netsub/resourceGroups/MC_rg/providers/Microsoft.Network/publicIPAddresses/team-egress-pip-v6"}],` +
			`"serviceGateway":{"id":"/subscriptions/netsub/resourceGroups/MC_rg/providers/Microsoft.Network/serviceGateways/sgw"}},` +
			`"sku":{"name":"StandardV2"},"tags":{"k8s-azure-egress-identity":"team-egress"}}`
	)
	for _, tc := range []struct {
		name string
		ipv6 bool
		pips []string
		nat  string
	}{
		{name: "IPv4-only", pips: []string{pipV4}, nat: natIPv4},
		{name: "dual-stack", ipv6: true, pips: []string{pipV4, pipV6}, nat: natDualStack},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pips, nat := egressCreationRequests(t, "team-egress", defaultEgressConfig(egressParseContext{WantsIPv6: tc.ipv6}), tc.ipv6, dtConfig)
			var got []string
			for _, pip := range pips {
				got = append(got, egressNATTestJSON(t, pip))
			}
			assert.Equal(t, tc.pips, got)
			assert.Equal(t, tc.nat, egressNATTestJSON(t, nat))
		})
	}
}

// egressFillAll sets every exported field reachable from v to a non-zero value, following pointers and slices
// through at most depth levels, so that a projection sees every field of the SDK model, read-only ones included.
func egressFillAll(v reflect.Value, depth int) {
	switch v.Kind() {
	case reflect.Pointer:
		if depth > 0 {
			v.Set(reflect.New(v.Type().Elem()))
			egressFillAll(v.Elem(), depth-1)
		}
	case reflect.Struct:
		for i := range v.NumField() {
			if v.Type().Field(i).IsExported() {
				egressFillAll(v.Field(i), depth)
			}
		}
	case reflect.Slice:
		if depth > 0 {
			v.Set(reflect.MakeSlice(v.Type(), 1, 1))
			egressFillAll(v.Index(0), depth-1)
		}
	case reflect.Map:
		key, value := reflect.New(v.Type().Key()).Elem(), reflect.New(v.Type().Elem()).Elem()
		egressFillAll(key, depth)
		egressFillAll(value, depth)
		v.Set(reflect.MakeMap(v.Type()))
		v.SetMapIndex(key, value)
	case reflect.String:
		v.SetString("filled")
	case reflect.Bool:
		v.SetBool(true)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v.SetInt(7)
	case reflect.Float32, reflect.Float64:
		v.SetFloat(7)
	}
}

// egressJSONObject returns the JSON object v marshals to.
func egressJSONObject(t *testing.T, v any) map[string]any {
	t.Helper()
	var object map[string]any
	if err := json.Unmarshal([]byte(egressNATTestJSON(t, v)), &object); err != nil {
		t.Fatalf("unmarshalling %T: %v", v, err)
	}
	return object
}

func egressJSONKeys(object any) []string {
	m, _ := object.(map[string]any)
	return slices.Sorted(maps.Keys(m))
}

// egressFieldClasses classifies the JSON fields of one object of a projected resource: kept from the existing
// resource, set by the controller, or never written.
type egressFieldClasses struct{ kept, controlled, dropped []string }

func (c egressFieldClasses) all() []string {
	return slices.Sorted(slices.Values(slices.Concat(c.kept, c.controlled, c.dropped)))
}

func (c egressFieldClasses) written() []string {
	return slices.Sorted(slices.Values(slices.Concat(c.kept, c.controlled)))
}

// TestEgressRequests_FieldsOverExisting builds each projection over an existing resource with every field of its
// v12 model set and checks every field is classified, so an SDK update adding one fails here until it is decided:
// kept fields are copied (references reduced to their IDs) so that changing the request's values never changes the
// existing resource, controlled fields are the controller's, read-only and derived fields are never written.
func TestEgressRequests_FieldsOverExisting(t *testing.T) {
	dt, cfg := egressNATTestDT(), defaultEgressConfig(egressParseContext{})
	top := egressFieldClasses{
		controlled: []string{"id", "location", "name", "properties", "sku", "tags"},
		dropped:    []string{"etag", "type"},
	}

	t.Run("NAT Gateway", func(t *testing.T) {
		existing := &armnetwork.NatGateway{}
		egressFillAll(reflect.ValueOf(existing).Elem(), 6)
		before := egressNATTestJSON(t, existing)
		request := egressNATTestMust(natRequest(existing, egressNATTestID, cfg, false, dt))(t)
		assert.Equal(t, before, egressNATTestJSON(t, existing), "the existing NAT Gateway must not change")

		natTop := top
		natTop.kept = []string{"zones"}
		properties := egressFieldClasses{
			kept: []string{"nat64", "sourceVirtualNetwork"},
			controlled: []string{"idleTimeoutInMinutes", "publicIpAddresses", "publicIpAddressesV6", "publicIpPrefixes",
				"publicIpPrefixesV6", "serviceGateway"},
			dropped: []string{"provisioningState", "resourceGuid", "subnets"},
		}
		sku := egressFieldClasses{controlled: []string{"name"}}
		have, got := egressJSONObject(t, existing), egressJSONObject(t, request)
		assert.Equal(t, natTop.all(), egressJSONKeys(have))
		assert.Equal(t, properties.all(), egressJSONKeys(have["properties"]))
		assert.Equal(t, sku.all(), egressJSONKeys(have["sku"]))
		assert.Equal(t, natTop.written(), egressJSONKeys(got))
		assert.Equal(t, properties.written(), egressJSONKeys(got["properties"]))
		assert.Equal(t, sku.written(), egressJSONKeys(got["sku"]))

		assert.Equal(t, existing.Zones, request.Zones)
		assert.Equal(t, existing.Properties.Nat64, request.Properties.Nat64)
		assert.Equal(t, existing.Properties.SourceVirtualNetwork, request.Properties.SourceVirtualNetwork)
		assert.Equal(t, egressNATTestTags("filled", "filled", EgressIdentityTagKey, egressNATTestID), request.Tags)
		assert.Equal(t, egressNATTestID, *request.Name)
		assert.Equal(t, egressNATTestManagedID("natGateways", egressNATTestID), *request.ID)
		assert.Equal(t, "westus2", *request.Location)
		assert.Equal(t, armnetwork.NatGatewaySKUNameStandardV2, *request.SKU.Name)
		assert.Equal(t, dt.ServiceGatewayResourceID(), *request.Properties.ServiceGateway.ID)

		*request.Zones[0] = "changed"
		*request.Properties.Nat64 = "changed"
		*request.Properties.SourceVirtualNetwork.ID = "changed"
		assert.Equal(t, before, egressNATTestJSON(t, existing), "the request must share no memory with the existing NAT Gateway")
	})

	t.Run("Public IP", func(t *testing.T) {
		existing := &armnetwork.PublicIPAddress{}
		egressFillAll(reflect.ValueOf(existing).Elem(), 6)
		before := egressNATTestJSON(t, existing)
		request := egressNATTestMust(managedPublicIPRequest(existing, egressNATTestID, true, 3, cfg, dt))(t)
		assert.Equal(t, before, egressNATTestJSON(t, existing), "the existing Public IP must not change")

		pipTop := top
		pipTop.kept = []string{"extendedLocation", "zones"}
		properties := egressFieldClasses{
			kept: []string{"ddosSettings", "deleteOption", "dnsSettings", "idleTimeoutInMinutes", "ipTags", "linkedPublicIPAddress",
				"migrationPhase", "publicIPPrefix", "servicePublicIPAddress"},
			controlled: []string{"publicIPAddressVersion", "publicIPAllocationMethod"},
			dropped:    []string{"ipAddress", "ipConfiguration", "natGateway", "provisioningState", "resourceGuid", "upgradedToV2"},
		}
		sku := egressFieldClasses{kept: []string{"tier"}, controlled: []string{"name"}}
		have, got := egressJSONObject(t, existing), egressJSONObject(t, request)
		assert.Equal(t, pipTop.all(), egressJSONKeys(have))
		assert.Equal(t, properties.all(), egressJSONKeys(have["properties"]))
		assert.Equal(t, sku.all(), egressJSONKeys(have["sku"]))
		assert.Equal(t, pipTop.written(), egressJSONKeys(got))
		assert.Equal(t, properties.written(), egressJSONKeys(got["properties"]))
		assert.Equal(t, sku.written(), egressJSONKeys(got["sku"]))

		p, e := request.Properties, existing.Properties
		assert.Equal(t, existing.Zones, request.Zones)
		assert.Equal(t, existing.ExtendedLocation, request.ExtendedLocation)
		assert.Equal(t, existing.SKU.Tier, request.SKU.Tier)
		assert.Equal(t, e.DNSSettings, p.DNSSettings)
		assert.Equal(t, e.DdosSettings, p.DdosSettings)
		assert.Equal(t, e.DeleteOption, p.DeleteOption)
		assert.Equal(t, e.IPTags, p.IPTags)
		assert.Equal(t, e.IdleTimeoutInMinutes, p.IdleTimeoutInMinutes)
		assert.Equal(t, e.MigrationPhase, p.MigrationPhase)
		assert.Equal(t, e.PublicIPPrefix, p.PublicIPPrefix)
		assert.Equal(t, &armnetwork.PublicIPAddress{ID: e.LinkedPublicIPAddress.ID}, p.LinkedPublicIPAddress, "only the reference is kept")
		assert.Equal(t, &armnetwork.PublicIPAddress{ID: e.ServicePublicIPAddress.ID}, p.ServicePublicIPAddress, "only the reference is kept")
		assert.Equal(t, "team-egress-pip-v6-3", *request.Name)
		assert.Equal(t, egressNATTestManagedID("publicIPAddresses", "team-egress-pip-v6-3"), *request.ID)
		assert.Equal(t, "westus2", *request.Location)
		assert.Equal(t, armnetwork.PublicIPAddressSKUNameStandardV2, *request.SKU.Name)
		assert.Equal(t, armnetwork.IPAllocationMethodStatic, *p.PublicIPAllocationMethod)
		assert.Equal(t, armnetwork.IPVersionIPv6, *p.PublicIPAddressVersion)
		assert.Equal(t, egressNATTestTags("filled", "filled", EgressIdentityTagKey, egressNATTestID), request.Tags)

		*request.Zones[0] = "changed"
		request.ExtendedLocation.Name = to.Ptr("changed")
		*request.SKU.Tier = "changed"
		p.DNSSettings.Fqdn = to.Ptr("changed")
		p.DdosSettings.ProtectionMode = nil
		p.IPTags[0].Tag = to.Ptr("changed")
		*p.IdleTimeoutInMinutes = 1
		*p.DeleteOption = "changed"
		*p.MigrationPhase = "changed"
		*p.PublicIPPrefix.ID = "changed"
		*p.LinkedPublicIPAddress.ID = "changed"
		*p.ServicePublicIPAddress.ID = "changed"
		assert.Equal(t, before, egressNATTestJSON(t, existing), "changing the request's values must not change the existing Public IP")
	})

	t.Run("Public IP prefix", func(t *testing.T) {
		existing := &armnetwork.PublicIPPrefix{}
		egressFillAll(reflect.ValueOf(existing).Elem(), 6)
		before := egressNATTestJSON(t, existing)
		request := egressNATTestMust(managedPrefixRequest(existing, egressNATTestID, false, 29, cfg, dt))(t)
		assert.Equal(t, before, egressNATTestJSON(t, existing), "the existing prefix must not change")

		prefixTop := top
		prefixTop.kept = []string{"extendedLocation", "zones"}
		properties := egressFieldClasses{
			kept:       []string{"customIPPrefix", "ipTags"},
			controlled: []string{"prefixLength", "publicIPAddressVersion"},
			dropped: []string{"ipPrefix", "loadBalancerFrontendIpConfiguration", "natGateway", "provisioningState", "publicIPAddresses",
				"resourceGuid", "upgradedToV2"},
		}
		sku := egressFieldClasses{kept: []string{"tier"}, controlled: []string{"name"}}
		have, got := egressJSONObject(t, existing), egressJSONObject(t, request)
		assert.Equal(t, prefixTop.all(), egressJSONKeys(have))
		assert.Equal(t, properties.all(), egressJSONKeys(have["properties"]))
		assert.Equal(t, sku.all(), egressJSONKeys(have["sku"]))
		assert.Equal(t, prefixTop.written(), egressJSONKeys(got))
		assert.Equal(t, properties.written(), egressJSONKeys(got["properties"]))
		assert.Equal(t, sku.written(), egressJSONKeys(got["sku"]))

		assert.Equal(t, existing.Zones, request.Zones)
		assert.Equal(t, existing.ExtendedLocation, request.ExtendedLocation)
		assert.Equal(t, existing.SKU.Tier, request.SKU.Tier)
		assert.Equal(t, existing.Properties.IPTags, request.Properties.IPTags)
		assert.Equal(t, existing.Properties.CustomIPPrefix, request.Properties.CustomIPPrefix)
		assert.Equal(t, "team-egress-prefix-29", *request.Name)
		assert.Equal(t, egressNATTestManagedID("publicIPPrefixes", "team-egress-prefix-29"), *request.ID)
		assert.Equal(t, "westus2", *request.Location)
		assert.Equal(t, armnetwork.PublicIPPrefixSKUNameStandardV2, *request.SKU.Name)
		assert.Equal(t, int32(29), *request.Properties.PrefixLength)
		assert.Equal(t, armnetwork.IPVersionIPv4, *request.Properties.PublicIPAddressVersion)
		assert.Equal(t, egressNATTestTags("filled", "filled", EgressIdentityTagKey, egressNATTestID), request.Tags)

		*request.Zones[0] = "changed"
		request.ExtendedLocation.Name = to.Ptr("changed")
		*request.SKU.Tier = "changed"
		request.Properties.IPTags[0].Tag = to.Ptr("changed")
		*request.Properties.CustomIPPrefix.ID = "changed"
		assert.Equal(t, before, egressNATTestJSON(t, existing), "changing the request's values must not change the existing prefix")
	})
}

func TestNATRequest_IdleTimeout(t *testing.T) {
	dt := egressNATTestDT()
	existing := func(idle *int32) *armnetwork.NatGateway {
		return &armnetwork.NatGateway{Properties: &armnetwork.NatGatewayPropertiesFormat{IdleTimeoutInMinutes: idle}}
	}
	cases := []struct {
		name     string
		existing *armnetwork.NatGateway
		idle     int32
		want     *int32
	}{
		{"default from scratch stays unset, as before configuration existed", nil, 4, nil},
		{"default over an unset idle timeout", existing(nil), 4, nil},
		{"default over the default Azure reports", existing(to.Ptr[int32](4)), 4, nil},
		{"default over another idle timeout", existing(to.Ptr[int32](10)), 4, to.Ptr[int32](4)},
		{"configured from scratch", nil, 10, to.Ptr[int32](10)},
		{"configured over an unset idle timeout", existing(nil), 10, to.Ptr[int32](10)},
		{"configured over the default", existing(to.Ptr[int32](4)), 120, to.Ptr[int32](120)},
		{"configured and unchanged", existing(to.Ptr[int32](10)), 10, to.Ptr[int32](10)},
		{"existing NAT Gateway without properties", &armnetwork.NatGateway{}, 4, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := defaultEgressConfig(egressParseContext{})
			cfg.IdleTimeoutMinutes = tc.idle
			request := egressNATTestMust(natRequest(tc.existing, egressNATTestID, cfg, false, dt))(t)
			assert.Equal(t, tc.want, request.Properties.IdleTimeoutInMinutes)
		})
	}
}

func TestNATRequest_OverExisting(t *testing.T) {
	dt := egressNATTestDT()
	ref := func(id string) *armnetwork.SubResource { return &armnetwork.SubResource{ID: to.Ptr(id)} }
	ids := func(refs []*armnetwork.SubResource) []string {
		var out []string
		for _, r := range refs {
			out = append(out, *r.ID)
		}
		return out
	}
	existing := &armnetwork.NatGateway{
		Name:     to.Ptr("TEAM-EGRESS"),
		ID:       to.Ptr("/subscriptions/netsub/resourcegroups/mc_rg/providers/microsoft.network/natgateways/team-egress"),
		Location: to.Ptr("West US 2"),
		Etag:     to.Ptr(`W/"etag"`),
		Type:     to.Ptr("Microsoft.Network/natGateways"),
		SKU:      &armnetwork.NatGatewaySKU{Name: to.Ptr(armnetwork.NatGatewaySKUNameStandard)},
		Tags:     egressNATTestTags("Owner", "alice", "env", "dev", EgressIdentityTagKey, egressNATTestID),
		Zones:    []*string{to.Ptr("1")},
		Properties: &armnetwork.NatGatewayPropertiesFormat{
			ProvisioningState:    to.Ptr(armnetwork.ProvisioningStateSucceeded),
			ResourceGUID:         to.Ptr("guid"),
			IdleTimeoutInMinutes: to.Ptr[int32](4),
			PublicIPAddresses: []*armnetwork.SubResource{
				ref(egressNATTestManagedID("publicIPAddresses", "team-egress-pip")),
				ref(egressNATTestManagedID("publicIPAddresses", "team-egress-pip-5")),
			},
			PublicIPAddressesV6: []*armnetwork.SubResource{ref(egressNATTestManagedID("publicIPAddresses", "team-egress-pip-v6"))},
			PublicIPPrefixes:    []*armnetwork.SubResource{ref(egressNATTestUserID("publicIPPrefixes", "old"))},
			ServiceGateway:      ref("/subscriptions/netsub/resourceGroups/MC_rg/providers/Microsoft.Network/serviceGateways/other"),
			Subnets:             []*armnetwork.SubResource{ref("subnet")},
		},
	}

	t.Run("a new configuration replaces the references and keeps foreign tags", func(t *testing.T) {
		cfg := egressNATTestConfig(t, false, map[string]string{
			consts.PodAnnotationServiceEgressGatewayPublicIPCount: "2",
			consts.PodAnnotationServiceEgressGatewayPublicIPIDs:   egressNATTestUserID("publicIPAddresses", "user-pip"),
			consts.PodAnnotationServiceEgressGatewayTags:          "ENV=prod",
		})
		request := egressNATTestMust(natRequest(existing, egressNATTestID, cfg, false, dt))(t)
		assert.Equal(t, []string{
			egressNATTestManagedID("publicIPAddresses", "team-egress-pip"),
			egressNATTestManagedID("publicIPAddresses", "team-egress-pip-2"),
			strings.ToLower(egressNATTestUserID("publicIPAddresses", "user-pip")),
		}, ids(request.Properties.PublicIPAddresses))
		assert.Equal(t, []*armnetwork.SubResource{}, request.Properties.PublicIPAddressesV6,
			"references a NAT Gateway that does not serve IPv6 has are removed explicitly")
		assert.Equal(t, []*armnetwork.SubResource{}, request.Properties.PublicIPPrefixes, "a prefix no longer configured is removed explicitly")
		assert.Nil(t, request.Properties.PublicIPPrefixesV6, "an empty list without existing references stays unset")
		assert.Equal(t, egressNATTestTags("Owner", "alice", "env", "prod", EgressIdentityTagKey, egressNATTestID), request.Tags)
		assert.Equal(t, dt.ServiceGatewayResourceID(), *request.Properties.ServiceGateway.ID, "the link is the cluster's Service Gateway")
		assert.Equal(t, armnetwork.NatGatewaySKUNameStandardV2, *request.SKU.Name)
		assert.Equal(t, "westus2", *request.Location)
		assert.Equal(t, egressNATTestID, *request.Name)
		assert.Equal(t, []*string{to.Ptr("1")}, request.Zones)
		assert.Nil(t, request.Etag)
		assert.Nil(t, request.Type)
		assert.Nil(t, request.Properties.ProvisioningState)
		assert.Nil(t, request.Properties.ResourceGUID)
		assert.Nil(t, request.Properties.Subnets)
		assert.Nil(t, request.Properties.IdleTimeoutInMinutes)
	})

	t.Run("a dual-stack configuration with prefixes", func(t *testing.T) {
		cfg := egressNATTestRichConfig(t)
		request := egressNATTestMust(natRequest(existing, egressNATTestID, cfg, true, dt))(t)
		fromScratch := egressNATTestMust(natRequest(nil, egressNATTestID, cfg, true, dt))(t)
		assert.Equal(t, fromScratch.Properties.PublicIPAddresses, request.Properties.PublicIPAddresses)
		assert.Equal(t, fromScratch.Properties.PublicIPAddressesV6, request.Properties.PublicIPAddressesV6)
		assert.Equal(t, fromScratch.Properties.PublicIPPrefixes, request.Properties.PublicIPPrefixes)
		assert.Equal(t, fromScratch.Properties.PublicIPPrefixesV6, request.Properties.PublicIPPrefixesV6)
		assert.Equal(t, to.Ptr[int32](10), request.Properties.IdleTimeoutInMinutes)
		assert.Equal(t, egressNATTestTags("Owner", "alice", "env", "Prod", "team", "net", EgressIdentityTagKey, egressNATTestID),
			request.Tags, "the configured Env updates the existing env, which keeps its spelling")
	})
}

func TestEgressRequests_RejectInvalidInput(t *testing.T) {
	dt := egressNATTestDT()
	valid := defaultEgressConfig(egressParseContext{WantsIPv6: true})
	with := func(mutate func(*EgressConfig)) EgressConfig {
		cfg := valid
		mutate(&cfg)
		return cfg
	}

	t.Run("NAT Gateway", func(t *testing.T) {
		cases := []struct {
			name string
			cfg  EgressConfig
			ipv6 bool
			want string
		}{
			{"configuration not set", EgressConfig{}, true, "idle timeout 0"},
			{"unknown configuration", egressUnknown, true, "idle timeout -1"},
			{"idle timeout too short", with(func(c *EgressConfig) { c.IdleTimeoutMinutes = 3 }), true, "idle timeout 3"},
			{"idle timeout too long", with(func(c *EgressConfig) { c.IdleTimeoutMinutes = 121 }), true, "idle timeout 121"},
			{"more Public IPs than reserved", with(func(c *EgressConfig) { c.IPv4.ManagedPublicIPs = 17 }), true, `"team-egress-pip-17"`},
			{"IPv4 prefix too long", with(func(c *EgressConfig) { c.IPv4.ManagedPrefixLength = 32 }), true, `"team-egress-prefix-32"`},
			{"IPv4 prefix too short", with(func(c *EgressConfig) { c.IPv4.ManagedPrefixLength = 27 }), true, `"team-egress-prefix-27"`},
			{"IPv6 prefix of IPv4 length", with(func(c *EgressConfig) { c.IPv6.ManagedPrefixLength = 31 }), true, `"team-egress-prefix-v6-31"`},
			{"more IPv6 Public IPs than reserved", with(func(c *EgressConfig) { c.IPv6.ManagedPublicIPs = 17 }), true, `"team-egress-pip-v6-17"`},
			{"too many tags", with(func(c *EgressConfig) { c.Tags = egressTestTags(50) }), true, "51 tags"},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				request, err := natRequest(nil, egressNATTestID, tc.cfg, tc.ipv6, dt)
				assert.ErrorContains(t, err, tc.want)
				assert.ErrorContains(t, err, `egress NAT Gateway "team-egress"`)
				assert.Equal(t, armnetwork.NatGateway{}, request)
			})
		}
		for _, idle := range []int32{4, 120} {
			_, err := natRequest(nil, egressNATTestID, with(func(c *EgressConfig) { c.IdleTimeoutMinutes = idle }), true, dt)
			assert.NoError(t, err, "idle timeout %d is in range", idle)
		}
		_, err := natRequest(nil, egressNATTestID, with(func(c *EgressConfig) { c.IPv6.ManagedPublicIPs = 17 }), false, dt)
		assert.NoError(t, err, "the IPv6 settings of a NAT Gateway that does not serve IPv6 are not used")
		_, err = natRequest(nil, egressNATTestID, with(func(c *EgressConfig) { c.Tags = egressTestTags(49) }), true, dt)
		assert.NoError(t, err, "49 configured tags fit beside the ownership tag")
	})

	t.Run("Public IP", func(t *testing.T) {
		for _, n := range []int{0, 17} {
			for _, ipv6 := range []bool{false, true} {
				request, err := managedPublicIPRequest(nil, egressNATTestID, ipv6, n, valid, dt)
				assert.ErrorContains(t, err, "not a reserved name", "n=%d ipv6=%v", n, ipv6)
				assert.Equal(t, armnetwork.PublicIPAddress{}, request)
			}
		}
		for _, n := range []int{1, 16} {
			_, err := managedPublicIPRequest(nil, egressNATTestID, true, n, valid, dt)
			assert.NoError(t, err, "n=%d", n)
		}
		_, err := managedPublicIPRequest(nil, egressNATTestID, false, 2, with(func(c *EgressConfig) { c.Tags = egressTestTags(50) }), dt)
		assert.ErrorContains(t, err, `egress Public IP "team-egress-pip-2": 51 tags`)
		crowded := &armnetwork.PublicIPAddress{Tags: map[string]*string{}}
		for i := range 49 {
			crowded.Tags[fmt.Sprintf("foreign-%02d", i)] = to.Ptr("x")
		}
		_, err = managedPublicIPRequest(crowded, egressNATTestID, false, 1, valid, dt)
		assert.NoError(t, err, "49 foreign tags and the ownership tag fit")
		_, err = managedPublicIPRequest(crowded, egressNATTestID, false, 1, with(func(c *EgressConfig) { c.Tags = "env=prod" }), dt)
		assert.ErrorContains(t, err, "51 tags", "a configured tag does not fit beside 49 foreign tags")
	})

	t.Run("Public IP prefix", func(t *testing.T) {
		for _, tc := range []struct {
			ipv6   bool
			length int
		}{{false, 27}, {false, 32}, {false, 124}, {true, 123}, {true, 128}, {true, 28}, {false, 0}} {
			request, err := managedPrefixRequest(nil, egressNATTestID, tc.ipv6, tc.length, valid, dt)
			assert.ErrorContains(t, err, "not a reserved name", "%+v", tc)
			assert.Equal(t, armnetwork.PublicIPPrefix{}, request)
		}
		for _, tc := range []struct {
			ipv6   bool
			length int
		}{{false, 28}, {false, 31}, {true, 124}, {true, 127}} {
			_, err := managedPrefixRequest(nil, egressNATTestID, tc.ipv6, tc.length, valid, dt)
			assert.NoError(t, err, "%+v", tc)
		}
		_, err := managedPrefixRequest(nil, egressNATTestID, false, 31, with(func(c *EgressConfig) { c.Tags = egressTestTags(50) }), dt)
		assert.ErrorContains(t, err, `egress Public IP prefix "team-egress-prefix-31": 51 tags`)
	})
}

// egressNATTestState is the NAT Gateway of the test identity and its managed resources, by name, as Azure returns them.
type egressNATTestState struct {
	nat      *armnetwork.NatGateway
	pips     map[string]*armnetwork.PublicIPAddress
	prefixes map[string]*armnetwork.PublicIPPrefix
}

// egressNATTestApplied returns what writing the projections of cfg creates, once Azure has provisioned it.
func egressNATTestApplied(t *testing.T, cfg EgressConfig, ipv6 bool) *egressNATTestState {
	t.Helper()
	dt := egressNATTestDT()
	nat := egressNATTestMust(natRequest(nil, egressNATTestID, cfg, ipv6, dt))(t)
	nat.Properties.ProvisioningState = to.Ptr(armnetwork.ProvisioningStateSucceeded)
	state := &egressNATTestState{nat: &nat, pips: map[string]*armnetwork.PublicIPAddress{}, prefixes: map[string]*armnetwork.PublicIPPrefix{}}
	for _, v6 := range egressServedFamilies(ipv6) {
		for _, m := range cfg.familyConfig(v6).managed(v6) {
			name := m.name(egressNATTestID)
			if m.Kind == egressManagedPrefix {
				prefix := egressNATTestMust(managedPrefixRequest(nil, egressNATTestID, v6, m.N, cfg, dt))(t)
				prefix.Properties.ProvisioningState = to.Ptr(armnetwork.ProvisioningStateSucceeded)
				state.prefixes[name] = &prefix
				continue
			}
			pip := egressNATTestMust(managedPublicIPRequest(nil, egressNATTestID, v6, m.N, cfg, dt))(t)
			pip.Properties.ProvisioningState = to.Ptr(armnetwork.ProvisioningStateSucceeded)
			state.pips[name] = &pip
		}
	}
	return state
}

// mismatch returns natMismatch for the state, checking natSatisfies agrees with it.
func (s *egressNATTestState) mismatch(t *testing.T, cfg EgressConfig, ipv6 bool) string {
	t.Helper()
	managed := newEgressManagedResources(slices.Collect(maps.Values(s.pips)), slices.Collect(maps.Values(s.prefixes)))
	mismatch := natMismatch(s.nat, managed, egressNATTestID, cfg, ipv6, egressNATTestDT())
	assert.Equal(t, mismatch == "", natSatisfies(s.nat, managed, egressNATTestID, cfg, ipv6, egressNATTestDT()),
		"natSatisfies must agree with natMismatch %q", mismatch)
	return mismatch
}

func egressNATTestWithout(refs []*armnetwork.SubResource, name string) []*armnetwork.SubResource {
	return slices.DeleteFunc(slices.Clone(refs), func(r *armnetwork.SubResource) bool {
		return strings.HasSuffix(strings.ToLower(*r.ID), "/"+name)
	})
}

func TestNATSatisfies_AppliedConfigurations(t *testing.T) {
	userOnly := egressNATTestConfig(t, false, map[string]string{
		consts.PodAnnotationServiceEgressGatewayPublicIPIDs:       egressNATTestUserID("publicIPAddresses", "user-pip"),
		consts.PodAnnotationServiceEgressGatewayPublicIPPrefixIDs: egressNATTestUserID("publicIPPrefixes", "user-prefix"),
	})
	cases := []struct {
		name string
		cfg  EgressConfig
		ipv6 bool
	}{
		{"default IPv4", defaultEgressConfig(egressParseContext{}), false},
		{"default dual-stack", defaultEgressConfig(egressParseContext{WantsIPv6: true}), true},
		{"every setting", egressNATTestRichConfig(t), true},
		{"user resources only", userOnly, false},
		{"managed prefix only", egressNATTestConfig(t, false, map[string]string{
			consts.PodAnnotationServiceEgressGatewayPublicIPPrefixLength: "28",
		}), false},
		{"sixteen Public IPs", egressNATTestConfig(t, false, map[string]string{
			consts.PodAnnotationServiceEgressGatewayPublicIPCount: "16",
		}), false},
		{"IPv6 settings of a NAT Gateway that does not serve IPv6", defaultEgressConfig(egressParseContext{WantsIPv6: true}), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Empty(t, egressNATTestApplied(t, tc.cfg, tc.ipv6).mismatch(t, tc.cfg, tc.ipv6))
		})
	}

	t.Run("an unset idle timeout is the default", func(t *testing.T) {
		cfg := defaultEgressConfig(egressParseContext{})
		state := egressNATTestApplied(t, cfg, false)
		assert.Nil(t, state.nat.Properties.IdleTimeoutInMinutes)
		assert.Empty(t, state.mismatch(t, cfg, false))
		state.nat.Properties.IdleTimeoutInMinutes = to.Ptr[int32](4)
		assert.Empty(t, state.mismatch(t, cfg, false), "Azure reports the default it applied")
	})

	t.Run("no NAT Gateway satisfies an unknown or unset configuration", func(t *testing.T) {
		state := egressNATTestApplied(t, defaultEgressConfig(egressParseContext{}), false)
		assert.Contains(t, state.mismatch(t, egressUnknown, false), "idle timeout")
		assert.Contains(t, state.mismatch(t, EgressConfig{}, false), "idle timeout")
	})

	t.Run("a NAT Gateway that does not serve IPv6 references no IPv6 resource", func(t *testing.T) {
		cfg := defaultEgressConfig(egressParseContext{})
		state := egressNATTestApplied(t, cfg, false)
		state.nat.Properties.PublicIPAddressesV6 = []*armnetwork.SubResource{
			{ID: to.Ptr(egressNATTestManagedID("publicIPAddresses", "team-egress-pip-v6"))},
		}
		assert.Equal(t, "the NAT Gateway's IPv6 Public IPs are [team-egress-pip-v6], not []", state.mismatch(t, cfg, false))
		state.nat.Properties.PublicIPAddressesV6 = nil
		state.nat.Properties.PublicIPPrefixesV6 = []*armnetwork.SubResource{{ID: to.Ptr(egressNATTestUserID("publicIPPrefixes", "p6"))}}
		assert.Contains(t, state.mismatch(t, cfg, false), "IPv6 Public IP prefixes are [p6], not []")
	})
}

func TestNATSatisfies_ControlledFields(t *testing.T) {
	const (
		own      = EgressIdentityTagKey
		pip      = "team-egress-pip"
		pip2     = "team-egress-pip-2"
		pipV6    = "team-egress-pip-v6"
		prefix   = "team-egress-prefix-30"
		prefixV6 = "team-egress-prefix-v6-127"
	)
	natState := func(state armnetwork.ProvisioningState) func(*egressNATTestState) {
		return func(s *egressNATTestState) { s.nat.Properties.ProvisioningState = to.Ptr(state) }
	}
	pipState := func(state armnetwork.ProvisioningState) func(*egressNATTestState) {
		return func(s *egressNATTestState) { s.pips[pip2].Properties.ProvisioningState = to.Ptr(state) }
	}
	prefixState := func(state armnetwork.ProvisioningState) func(*egressNATTestState) {
		return func(s *egressNATTestState) { s.prefixes[prefix].Properties.ProvisioningState = to.Ptr(state) }
	}
	userPIP := strings.ToLower(egressNATTestUserID("publicIPAddresses", "user-pip"))
	cases := []struct {
		name   string
		mutate func(s *egressNATTestState)
		want   string
	}{
		{"no NAT Gateway", func(s *egressNATTestState) { s.nat = nil }, "the NAT Gateway does not exist"},
		{"NAT Gateway of another name", func(s *egressNATTestState) { s.nat.Name = to.Ptr("other") }, `the NAT Gateway is named "other"`},
		{"NAT Gateway Failed", natState(armnetwork.ProvisioningStateFailed), `provisioning state is "Failed"`},
		{"NAT Gateway Canceled", natState(armnetwork.ProvisioningStateCanceled), `provisioning state is "Canceled"`},
		{"NAT Gateway Updating", natState(armnetwork.ProvisioningStateUpdating), `provisioning state is "Updating"`},
		{"NAT Gateway Creating", natState(armnetwork.ProvisioningStateCreating), `provisioning state is "Creating"`},
		{"NAT Gateway Deleting", natState(armnetwork.ProvisioningStateDeleting), `provisioning state is "Deleting"`},
		{"NAT Gateway without provisioning state", func(s *egressNATTestState) { s.nat.Properties.ProvisioningState = nil }, `provisioning state is ""`},
		{"NAT Gateway without properties", func(s *egressNATTestState) { s.nat.Properties = nil }, `provisioning state is ""`},
		{"Standard NAT Gateway", func(s *egressNATTestState) { s.nat.SKU.Name = to.Ptr(armnetwork.NatGatewaySKUNameStandard) }, `SKU is "Standard"`},
		{"NAT Gateway without SKU", func(s *egressNATTestState) { s.nat.SKU = nil }, `SKU is ""`},
		{"NAT Gateway in another location", func(s *egressNATTestState) { s.nat.Location = to.Ptr("eastus") }, `location is "eastus"`},
		{"NAT Gateway without location", func(s *egressNATTestState) { s.nat.Location = nil }, `location is ""`},
		{"NAT Gateway not linked", func(s *egressNATTestState) { s.nat.Properties.ServiceGateway = nil }, "not linked to a Service Gateway"},
		{"NAT Gateway linked to another Service Gateway", func(s *egressNATTestState) {
			s.nat.Properties.ServiceGateway.ID = to.Ptr(egressNATTestManagedID("serviceGateways", "other"))
		}, "linked to Service Gateway"},
		{"NAT Gateway tagged for another identity", func(s *egressNATTestState) { s.nat.Tags[own] = to.Ptr("other") }, "NAT Gateway does not carry the identity's ownership tag"},
		{"NAT Gateway without the ownership tag", func(s *egressNATTestState) { delete(s.nat.Tags, own) }, "NAT Gateway does not carry the identity's ownership tag"},
		{"NAT Gateway lacks a configured tag", func(s *egressNATTestState) { delete(s.nat.Tags, "team") }, "the NAT Gateway lacks tags team=net"},
		{"NAT Gateway tag value in another case", func(s *egressNATTestState) { s.nat.Tags["Env"] = to.Ptr("prod") }, "the NAT Gateway lacks tags Env=Prod"},
		{"another idle timeout", func(s *egressNATTestState) { s.nat.Properties.IdleTimeoutInMinutes = to.Ptr[int32](11) }, "idle timeout is 11 minutes"},
		{"idle timeout unset while 10 is configured", func(s *egressNATTestState) { s.nat.Properties.IdleTimeoutInMinutes = nil }, "idle timeout is 4 minutes"},
		{"managed IPv4 Public IP not referenced", func(s *egressNATTestState) {
			s.nat.Properties.PublicIPAddresses = egressNATTestWithout(s.nat.Properties.PublicIPAddresses, pip2)
		}, "IPv4 Public IPs are [team-egress-pip user-pip], not [team-egress-pip team-egress-pip-2 user-pip]"},
		{"extra managed IPv4 Public IP referenced", func(s *egressNATTestState) {
			s.nat.Properties.PublicIPAddresses = append(s.nat.Properties.PublicIPAddresses,
				&armnetwork.SubResource{ID: to.Ptr(egressNATTestManagedID("publicIPAddresses", "team-egress-pip-3"))})
		}, "IPv4 Public IPs are"},
		{"user IPv4 Public IP not referenced", func(s *egressNATTestState) {
			s.nat.Properties.PublicIPAddresses = egressNATTestWithout(s.nat.Properties.PublicIPAddresses, "user-pip")
		}, "IPv4 Public IPs are"},
		{"another user IPv4 Public IP", func(s *egressNATTestState) {
			s.nat.Properties.PublicIPAddresses[2].ID = to.Ptr(egressNATTestUserID("publicIPAddresses", "other-pip"))
		}, "IPv4 Public IPs are [team-egress-pip team-egress-pip-2 other-pip]"},
		{"user IPv4 Public IP in another subscription", func(s *egressNATTestState) {
			s.nat.Properties.PublicIPAddresses[2].ID = to.Ptr(strings.Replace(userPIP, "/netsub/", "/sub/", 1))
		}, "IPv4 Public IPs are"},
		{"IPv4 Public IP referenced twice", func(s *egressNATTestState) {
			s.nat.Properties.PublicIPAddresses = append(s.nat.Properties.PublicIPAddresses, s.nat.Properties.PublicIPAddresses[0])
		}, "IPv4 Public IPs are"},
		{"empty IPv4 reference", func(s *egressNATTestState) {
			s.nat.Properties.PublicIPAddresses = append(s.nat.Properties.PublicIPAddresses, nil)
		}, "IPv4 Public IPs are"},
		{"IPv6 Public IP referenced as IPv4", func(s *egressNATTestState) {
			p := s.nat.Properties
			p.PublicIPAddresses = append(p.PublicIPAddresses, p.PublicIPAddressesV6[0])
			p.PublicIPAddressesV6 = p.PublicIPAddressesV6[1:]
		}, "IPv4 Public IPs are"},
		{"managed IPv4 prefix of another length", func(s *egressNATTestState) {
			s.nat.Properties.PublicIPPrefixes[0].ID = to.Ptr(egressNATTestManagedID("publicIPPrefixes", "team-egress-prefix-31"))
		}, "IPv4 Public IP prefixes are [team-egress-prefix-31 user-prefix], not [team-egress-prefix-30 user-prefix]"},
		{"user IPv4 prefix not referenced", func(s *egressNATTestState) {
			s.nat.Properties.PublicIPPrefixes = egressNATTestWithout(s.nat.Properties.PublicIPPrefixes, "user-prefix")
		}, "IPv4 Public IP prefixes are"},
		{"managed IPv6 Public IP not referenced", func(s *egressNATTestState) {
			s.nat.Properties.PublicIPAddressesV6 = egressNATTestWithout(s.nat.Properties.PublicIPAddressesV6, pipV6)
		}, "IPv6 Public IPs are [user-pip-v6], not [team-egress-pip-v6 user-pip-v6]"},
		{"user IPv6 prefix not referenced", func(s *egressNATTestState) {
			s.nat.Properties.PublicIPPrefixesV6 = egressNATTestWithout(s.nat.Properties.PublicIPPrefixesV6, "user-prefix-v6")
		}, "IPv6 Public IP prefixes are"},

		{"managed Public IP missing", func(s *egressNATTestState) { delete(s.pips, pip2) }, `Public IP "team-egress-pip-2" does not exist`},
		{"managed Public IP Failed", pipState(armnetwork.ProvisioningStateFailed), `Public IP "team-egress-pip-2" has provisioning state "Failed"`},
		{"managed Public IP Canceled", pipState(armnetwork.ProvisioningStateCanceled), `has provisioning state "Canceled"`},
		{"managed Public IP Updating", pipState(armnetwork.ProvisioningStateUpdating), `has provisioning state "Updating"`},
		{"managed Public IP without properties", func(s *egressNATTestState) { s.pips[pip2].Properties = nil }, `Public IP "team-egress-pip-2" has provisioning state ""`},
		{"Standard managed Public IP", func(s *egressNATTestState) {
			s.pips[pip2].SKU.Name = to.Ptr(armnetwork.PublicIPAddressSKUNameStandard)
		}, `Public IP "team-egress-pip-2" has SKU "Standard"`},
		{"managed Public IP without SKU", func(s *egressNATTestState) { s.pips[pip2].SKU = nil }, `Public IP "team-egress-pip-2" has SKU ""`},
		{"managed Public IP in another location", func(s *egressNATTestState) { s.pips[pip2].Location = to.Ptr("eastus") }, `Public IP "team-egress-pip-2" is in location "eastus"`},
		{"dynamic managed Public IP", func(s *egressNATTestState) {
			s.pips[pip2].Properties.PublicIPAllocationMethod = to.Ptr(armnetwork.IPAllocationMethodDynamic)
		}, `Public IP "team-egress-pip-2" has allocation method "Dynamic"`},
		{"managed Public IP without allocation method", func(s *egressNATTestState) { s.pips[pip2].Properties.PublicIPAllocationMethod = nil }, `has allocation method ""`},
		{"IPv6 Public IP in the IPv4 family", func(s *egressNATTestState) {
			s.pips[pip2].Properties.PublicIPAddressVersion = to.Ptr(armnetwork.IPVersionIPv6)
		}, `Public IP "team-egress-pip-2" has IP version "IPv6"`},
		{"IPv4 Public IP in the IPv6 family", func(s *egressNATTestState) {
			s.pips[pipV6].Properties.PublicIPAddressVersion = to.Ptr(armnetwork.IPVersionIPv4)
		}, `Public IP "team-egress-pip-v6" has IP version "IPv4"`},
		{"IPv6 Public IP without version, which means IPv4", func(s *egressNATTestState) { s.pips[pipV6].Properties.PublicIPAddressVersion = nil }, `Public IP "team-egress-pip-v6" has IP version "IPv4"`},
		{"numbered Public IP without the ownership tag", func(s *egressNATTestState) { delete(s.pips[pip2].Tags, own) }, `Public IP "team-egress-pip-2" does not carry the identity's ownership tag`},
		{"numbered Public IP tagged for another identity", func(s *egressNATTestState) { s.pips[pip2].Tags[own] = to.Ptr("other") }, `Public IP "team-egress-pip-2" does not carry`},
		{"base Public IP of a tagged NAT Gateway without the ownership tag", func(s *egressNATTestState) { delete(s.pips[pip].Tags, own) }, `Public IP "team-egress-pip" does not carry`},
		{"managed Public IP lacks a configured tag", func(s *egressNATTestState) { delete(s.pips[pip2].Tags, "Env") }, `Public IP "team-egress-pip-2" lacks tags Env=Prod`},

		{"managed prefix missing", func(s *egressNATTestState) { delete(s.prefixes, prefix) }, `Public IP prefix "team-egress-prefix-30" does not exist`},
		{"managed prefix Failed", prefixState(armnetwork.ProvisioningStateFailed), `Public IP prefix "team-egress-prefix-30" has provisioning state "Failed"`},
		{"managed prefix Canceled", prefixState(armnetwork.ProvisioningStateCanceled), `has provisioning state "Canceled"`},
		{"managed prefix Updating", prefixState(armnetwork.ProvisioningStateUpdating), `has provisioning state "Updating"`},
		{"managed prefix without properties", func(s *egressNATTestState) { s.prefixes[prefix].Properties = nil }, `prefix "team-egress-prefix-30" has provisioning state ""`},
		{"Standard managed prefix", func(s *egressNATTestState) {
			s.prefixes[prefix].SKU.Name = to.Ptr(armnetwork.PublicIPPrefixSKUNameStandard)
		}, `Public IP prefix "team-egress-prefix-30" has SKU "Standard"`},
		{"managed prefix without SKU", func(s *egressNATTestState) { s.prefixes[prefix].SKU = nil }, `prefix "team-egress-prefix-30" has SKU ""`},
		{"managed prefix in another location", func(s *egressNATTestState) { s.prefixes[prefix].Location = to.Ptr("eastus") }, `prefix "team-egress-prefix-30" is in location "eastus"`},
		{"managed prefix of another length", func(s *egressNATTestState) { s.prefixes[prefix].Properties.PrefixLength = to.Ptr[int32](31) }, `prefix "team-egress-prefix-30" has length 31`},
		{"managed prefix without length", func(s *egressNATTestState) { s.prefixes[prefix].Properties.PrefixLength = nil }, `prefix "team-egress-prefix-30" has length 0`},
		{"IPv6 prefix in the IPv4 family", func(s *egressNATTestState) {
			s.prefixes[prefix].Properties.PublicIPAddressVersion = to.Ptr(armnetwork.IPVersionIPv6)
		}, `prefix "team-egress-prefix-30" has IP version "IPv6"`},
		{"IPv4 prefix in the IPv6 family", func(s *egressNATTestState) {
			s.prefixes[prefixV6].Properties.PublicIPAddressVersion = to.Ptr(armnetwork.IPVersionIPv4)
		}, `prefix "team-egress-prefix-v6-127" has IP version "IPv4"`},
		{"managed prefix without the ownership tag", func(s *egressNATTestState) { delete(s.prefixes[prefix].Tags, own) }, `prefix "team-egress-prefix-30" does not carry`},
		{"managed prefix tagged for another identity", func(s *egressNATTestState) { s.prefixes[prefix].Tags[own] = to.Ptr("other") }, `prefix "team-egress-prefix-30" does not carry`},
		{"managed prefix lacks a configured tag", func(s *egressNATTestState) { delete(s.prefixes[prefix].Tags, "team") }, `prefix "team-egress-prefix-30" lacks tags team=net`},
	}
	cfg := egressNATTestRichConfig(t)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			state := egressNATTestApplied(t, cfg, true)
			assert.Empty(t, state.mismatch(t, cfg, true), "control: the applied state satisfies the configuration")
			tc.mutate(state)
			got := state.mismatch(t, cfg, true)
			assert.NotEmpty(t, got)
			assert.Contains(t, got, tc.want)
		})
	}
}

// TestNATSatisfies_EquivalentShapes pins what natSatisfies ignores: fields the controller does not control, and
// differences in spelling that Azure treats as the same, so that a read never looks drifted from what was written.
func TestNATSatisfies_EquivalentShapes(t *testing.T) {
	cfg := egressNATTestRichConfig(t)
	references := func(s *egressNATTestState, edit func([]*armnetwork.SubResource) []*armnetwork.SubResource) {
		p := s.nat.Properties
		p.PublicIPAddresses, p.PublicIPAddressesV6 = edit(p.PublicIPAddresses), edit(p.PublicIPAddressesV6)
		p.PublicIPPrefixes, p.PublicIPPrefixesV6 = edit(p.PublicIPPrefixes), edit(p.PublicIPPrefixesV6)
	}
	allTags := func(s *egressNATTestState, edit func(map[string]*string)) {
		edit(s.nat.Tags)
		for _, pip := range s.pips {
			edit(pip.Tags)
		}
		for _, prefix := range s.prefixes {
			edit(prefix.Tags)
		}
	}
	cases := []struct {
		name   string
		mutate func(s *egressNATTestState)
	}{
		{"read-only, derived and foreign fields", func(s *egressNATTestState) {
			s.nat.Etag, s.nat.Type, s.nat.Zones = to.Ptr("etag"), to.Ptr("Microsoft.Network/natGateways"), []*string{to.Ptr("1")}
			s.nat.Properties.ResourceGUID = to.Ptr("guid")
			s.nat.Properties.Subnets = []*armnetwork.SubResource{{ID: to.Ptr("subnet")}}
			s.nat.Properties.Nat64 = to.Ptr(armnetwork.Nat64State("Disabled"))
			for _, pip := range s.pips {
				pip.Etag, pip.SKU.Tier = to.Ptr("etag"), to.Ptr(armnetwork.PublicIPAddressSKUTierRegional)
				pip.Properties.IPAddress = to.Ptr("20.1.2.3")
				pip.Properties.NatGateway = &armnetwork.NatGateway{ID: s.nat.ID}
				pip.Properties.IdleTimeoutInMinutes = to.Ptr[int32](4)
			}
			for _, prefix := range s.prefixes {
				prefix.Etag, prefix.SKU.Tier = to.Ptr("etag"), to.Ptr(armnetwork.PublicIPPrefixSKUTierRegional)
				prefix.Properties.IPPrefix = to.Ptr("20.1.2.0/30")
				prefix.Properties.NatGateway = &armnetwork.NatGateway{ID: s.nat.ID}
			}
			allTags(s, func(tags map[string]*string) { tags["Owner"] = to.Ptr("alice") })
		}},
		{"reference IDs in another case", func(s *egressNATTestState) {
			references(s, func(refs []*armnetwork.SubResource) []*armnetwork.SubResource {
				for _, ref := range refs {
					ref.ID = to.Ptr(strings.ToUpper(*ref.ID))
				}
				return refs
			})
		}},
		{"references in another order", func(s *egressNATTestState) {
			references(s, func(refs []*armnetwork.SubResource) []*armnetwork.SubResource {
				slices.Reverse(refs)
				return refs
			})
		}},
		{"configured tag keys in another case", func(s *egressNATTestState) {
			allTags(s, func(tags map[string]*string) {
				tags["ENV"] = tags["Env"]
				delete(tags, "Env")
			})
		}},
		{"ownership tag value in another case", func(s *egressNATTestState) {
			allTags(s, func(tags map[string]*string) { tags[EgressIdentityTagKey] = to.Ptr("Team-Egress") })
		}},
		{"names in another case", func(s *egressNATTestState) {
			s.nat.Name = to.Ptr("Team-Egress")
			for _, pip := range s.pips {
				pip.Name = to.Ptr(strings.ToUpper(*pip.Name))
			}
			for _, prefix := range s.prefixes {
				prefix.Name = to.Ptr(strings.ToUpper(*prefix.Name))
			}
		}},
		{"location in display form", func(s *egressNATTestState) {
			s.nat.Location = to.Ptr("West US 2")
			s.pips["team-egress-pip-2"].Location = to.Ptr("WestUS2")
			s.prefixes["team-egress-prefix-30"].Location = to.Ptr("West US 2")
		}},
		{"SKU and enum values in another case", func(s *egressNATTestState) {
			s.nat.SKU.Name = to.Ptr(armnetwork.NatGatewaySKUName("standardv2"))
			s.nat.Properties.ProvisioningState = to.Ptr(armnetwork.ProvisioningState("succeeded"))
			s.pips["team-egress-pip-2"].SKU.Name = to.Ptr(armnetwork.PublicIPAddressSKUName("STANDARDV2"))
			s.pips["team-egress-pip-2"].Properties.PublicIPAllocationMethod = to.Ptr(armnetwork.IPAllocationMethod("static"))
			s.pips["team-egress-pip-v6"].Properties.PublicIPAddressVersion = to.Ptr(armnetwork.IPVersion("ipv6"))
			s.prefixes["team-egress-prefix-30"].SKU.Name = to.Ptr(armnetwork.PublicIPPrefixSKUName("standardV2"))
			s.prefixes["team-egress-prefix-30"].Properties.PublicIPAddressVersion = to.Ptr(armnetwork.IPVersion("ipv4"))
		}},
		{"IPv4 version left unset", func(s *egressNATTestState) {
			s.pips["team-egress-pip-2"].Properties.PublicIPAddressVersion = nil
			s.prefixes["team-egress-prefix-30"].Properties.PublicIPAddressVersion = nil
		}},
		{"Service Gateway link in another case", func(s *egressNATTestState) {
			s.nat.Properties.ServiceGateway.ID = to.Ptr(strings.ToLower(*s.nat.Properties.ServiceGateway.ID))
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			state := egressNATTestApplied(t, cfg, true)
			tc.mutate(state)
			assert.Empty(t, state.mismatch(t, cfg, true))
		})
	}
}

// TestNATSatisfies_LegacyOwnership pins that identities created before the controller tagged its resources satisfy
// the default without a write, and that nothing else is accepted untagged.
func TestNATSatisfies_LegacyOwnership(t *testing.T) {
	const own = EgressIdentityTagKey
	defaultIPv4 := defaultEgressConfig(egressParseContext{})
	defaultDualStack := defaultEgressConfig(egressParseContext{WantsIPv6: true})
	untag := func(s *egressNATTestState, names ...string) {
		delete(s.nat.Tags, own)
		for _, name := range names {
			delete(s.pips[name].Tags, own)
		}
	}
	cases := []struct {
		name   string
		cfg    EgressConfig
		ipv6   bool
		mutate func(s *egressNATTestState)
		want   string
	}{
		{"legacy IPv4 identity", defaultIPv4, false, func(s *egressNATTestState) { untag(s, "team-egress-pip") }, ""},
		{"legacy IPv4 identity with Azure's view of it", defaultIPv4, false, func(s *egressNATTestState) {
			untag(s, "team-egress-pip")
			s.nat.Tags, s.pips["team-egress-pip"].Tags = nil, nil
			s.nat.Properties.IdleTimeoutInMinutes = to.Ptr[int32](4)
		}, ""},
		{"legacy dual-stack identity", defaultDualStack, true, func(s *egressNATTestState) { untag(s, "team-egress-pip", "team-egress-pip-v6") }, ""},
		{"legacy NAT Gateway with a base Public IP tagged for another identity", defaultIPv4, false, func(s *egressNATTestState) {
			untag(s)
			s.pips["team-egress-pip"].Tags[own] = to.Ptr("other")
		}, `Public IP "team-egress-pip" does not carry`},
		{"tagged NAT Gateway with an untagged base Public IP", defaultIPv4, false, func(s *egressNATTestState) {
			delete(s.pips["team-egress-pip"].Tags, own)
		}, `Public IP "team-egress-pip" does not carry`},
		{"untagged NAT Gateway with a numbered Public IP", egressNATTestConfig(t, false, map[string]string{
			consts.PodAnnotationServiceEgressGatewayPublicIPCount: "2",
		}), false, func(s *egressNATTestState) { untag(s, "team-egress-pip", "team-egress-pip-2") }, "NAT Gateway does not carry"},
		{"untagged NAT Gateway with a managed prefix", egressNATTestConfig(t, false, map[string]string{
			consts.PodAnnotationServiceEgressGatewayPublicIPCount:        "1",
			consts.PodAnnotationServiceEgressGatewayPublicIPPrefixLength: "31",
		}), false, func(s *egressNATTestState) { untag(s, "team-egress-pip") }, "NAT Gateway does not carry"},
		{"legacy IPv4 NAT Gateway of a dual-stack identity", defaultDualStack, true, func(s *egressNATTestState) {
			untag(s, "team-egress-pip", "team-egress-pip-v6")
			s.nat.Properties.PublicIPAddressesV6 = nil
		}, "IPv6 Public IPs are [], not [team-egress-pip-v6]"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			state := egressNATTestApplied(t, tc.cfg, tc.ipv6)
			tc.mutate(state)
			got := state.mismatch(t, tc.cfg, tc.ipv6)
			if tc.want == "" {
				assert.Empty(t, got)
				return
			}
			assert.Contains(t, got, tc.want)
		})
	}
}

// TestNATLinkRequest_Wire pins the exact JSON of a link repair on armnetwork v12 for a NAT Gateway as Azure returns
// it: the read with its read-only fields removed and the link added.
func TestNATLinkRequest_Wire(t *testing.T) {
	const (
		managed = `/subscriptions/netsub/resourceGroups/MC_rg/providers/Microsoft.Network/`
		prefix  = `/subscriptions/netsub/resourceGroups/User-RG/providers/Microsoft.Network/publicIPPrefixes/user-prefix`
	)
	read := `{"etag":"W/\"5e2f\"","id":"` + managed + `natGateways/team-egress","location":"westus2","name":"team-egress",` +
		`"type":"Microsoft.Network/natGateways","sku":{"name":"StandardV2"},"zones":["1","2","3"],` +
		`"tags":{"Owner":"alice","k8s-azure-egress-identity":"team-egress"},"properties":{"idleTimeoutInMinutes":10,` +
		`"provisioningState":"Succeeded","resourceGuid":"bc852ddb-3006-4380-8885-16f99117761a",` +
		`"publicIpAddresses":[{"id":"` + managed + `publicIPAddresses/team-egress-pip"},{"id":"` + managed + `publicIPAddresses/team-egress-pip-2"}],` +
		`"publicIpPrefixes":[{"id":"` + prefix + `"}],"subnets":[{"id":"` + managed + `virtualNetworks/vnet/subnets/s"}]}}`
	var existing armnetwork.NatGateway
	if err := json.Unmarshal([]byte(read), &existing); err != nil {
		t.Fatal(err)
	}

	want := `{"id":"` + managed + `natGateways/team-egress","location":"westus2","name":"team-egress","properties":{` +
		`"idleTimeoutInMinutes":10,` +
		`"publicIpAddresses":[{"id":"` + managed + `publicIPAddresses/team-egress-pip"},{"id":"` + managed + `publicIPAddresses/team-egress-pip-2"}],` +
		`"publicIpPrefixes":[{"id":"` + prefix + `"}],"serviceGateway":{"id":"` + managed + `serviceGateways/sgw"}},` +
		`"sku":{"name":"StandardV2"},"tags":{"Owner":"alice","k8s-azure-egress-identity":"team-egress"},"zones":["1","2","3"]}`
	assert.Equal(t, want, egressNATTestJSON(t, natLinkRequest(&existing, egressNATTestDT())))
}

// TestNATLinkRequest_WritableFieldsOnly builds a link repair over a NAT Gateway with every field of its v12 model set:
// the request is the read without its read-only fields, and with the link.
func TestNATLinkRequest_WritableFieldsOnly(t *testing.T) {
	dt := egressNATTestDT()
	existing := &armnetwork.NatGateway{}
	egressFillAll(reflect.ValueOf(existing).Elem(), 6)
	before := egressNATTestJSON(t, existing)
	request := natLinkRequest(existing, dt)
	assert.Equal(t, before, egressNATTestJSON(t, existing), "the existing NAT Gateway must not change")

	want := egressJSONObject(t, existing)
	for _, readOnly := range []string{"etag", "type"} {
		assert.Contains(t, want, readOnly, "control: v12 marshals %s", readOnly)
		delete(want, readOnly)
	}
	properties := want["properties"].(map[string]any)
	for _, readOnly := range []string{"provisioningState", "resourceGuid", "subnets"} {
		assert.Contains(t, properties, readOnly, "control: v12 marshals %s", readOnly)
		delete(properties, readOnly)
	}
	properties["serviceGateway"] = map[string]any{"id": dt.ServiceGatewayResourceID()}
	assert.Equal(t, egressNATTestJSON(t, want), egressNATTestJSON(t, request))

	*request.Name = "changed"
	*request.ID = "changed"
	*request.Location = "changed"
	*request.SKU.Name = armnetwork.NatGatewaySKUNameStandard
	*request.Tags["filled"] = "changed"
	*request.Zones[0] = "changed"
	*request.Properties.IdleTimeoutInMinutes = 99
	*request.Properties.Nat64 = "changed"
	*request.Properties.PublicIPAddresses[0].ID = "changed"
	*request.Properties.PublicIPAddressesV6[0].ID = "changed"
	*request.Properties.PublicIPPrefixes[0].ID = "changed"
	*request.Properties.PublicIPPrefixesV6[0].ID = "changed"
	*request.Properties.SourceVirtualNetwork.ID = "changed"
	assert.Equal(t, before, egressNATTestJSON(t, existing), "the request must share no memory with the existing NAT Gateway")

	linked := &armnetwork.NatGateway{Properties: &armnetwork.NatGatewayPropertiesFormat{ServiceGateway: &armnetwork.SubResource{ID: to.Ptr("sgw")}}}
	*natWritable(linked).Properties.ServiceGateway.ID = "changed"
	assert.Equal(t, "sgw", *linked.Properties.ServiceGateway.ID, "the link as read is copied too")
}

// TestNATLinkRequest_SameWritableFieldsAsNATRequest pins that a link repair and a configuration apply write the same
// fields of a NAT Gateway, and the same values for every field the configuration does not control, so the two can
// never drift apart.
func TestNATLinkRequest_SameWritableFieldsAsNATRequest(t *testing.T) {
	dt := egressNATTestDT()
	existing := &armnetwork.NatGateway{}
	egressFillAll(reflect.ValueOf(existing).Elem(), 6)
	link := egressJSONObject(t, natLinkRequest(existing, dt))
	apply := egressJSONObject(t, egressNATTestMust(natRequest(existing, egressNATTestID, egressNATTestRichConfig(t), true, dt))(t))

	assert.Equal(t, egressJSONKeys(apply), egressJSONKeys(link))
	assert.Equal(t, egressJSONKeys(apply["properties"]), egressJSONKeys(link["properties"]))
	assert.Equal(t, egressJSONKeys(apply["sku"]), egressJSONKeys(link["sku"]))
	assert.Equal(t, apply["zones"], link["zones"])
	linkProperties, applyProperties := link["properties"].(map[string]any), apply["properties"].(map[string]any)
	for _, kept := range []string{"nat64", "sourceVirtualNetwork"} {
		assert.Equal(t, applyProperties[kept], linkProperties[kept], kept)
	}
	assert.Equal(t, applyProperties["serviceGateway"], linkProperties["serviceGateway"], "both link the cluster's Service Gateway")
}

func TestNATLinkRequest_ChangesOnlyTheLink(t *testing.T) {
	dt := egressNATTestDT()
	sgw := dt.ServiceGatewayResourceID()
	settle := func(request armnetwork.NatGateway) *armnetwork.NatGateway {
		request.Properties.ProvisioningState = to.Ptr(armnetwork.ProvisioningStateSucceeded)
		return &request
	}

	t.Run("an applied configuration is kept", func(t *testing.T) {
		cfg := egressNATTestRichConfig(t)
		for name, unlink := range map[string]func(*armnetwork.NatGateway){
			"unlinked": func(n *armnetwork.NatGateway) { n.Properties.ServiceGateway = nil },
			"linked to another": func(n *armnetwork.NatGateway) {
				n.Properties.ServiceGateway.ID = to.Ptr(egressNATTestManagedID("serviceGateways", "other"))
			},
			"link without ID":              func(n *armnetwork.NatGateway) { n.Properties.ServiceGateway = &armnetwork.SubResource{} },
			"already linked, link is kept": func(*armnetwork.NatGateway) {},
		} {
			state := egressNATTestApplied(t, cfg, true)
			unlink(state.nat)
			read := natWritable(state.nat)
			request := natLinkRequest(state.nat, dt)
			assert.Equal(t, sgw, *request.Properties.ServiceGateway.ID, name)
			request.Properties.ServiceGateway = read.Properties.ServiceGateway
			assert.Equal(t, read, request, "%s: nothing but the link changes", name)

			state.nat = settle(natLinkRequest(state.nat, dt))
			assert.Empty(t, state.mismatch(t, cfg, true), "%s: the repaired NAT Gateway satisfies the configuration it had", name)
		}
	})

	t.Run("a legacy identity stays untagged", func(t *testing.T) {
		cfg := defaultEgressConfig(egressParseContext{})
		state := egressNATTestApplied(t, cfg, false)
		delete(state.nat.Tags, EgressIdentityTagKey)
		delete(state.pips["team-egress-pip"].Tags, EgressIdentityTagKey)
		state.nat.Properties.ServiceGateway = nil
		assert.Equal(t, "the NAT Gateway is not linked to a Service Gateway", state.mismatch(t, cfg, false))

		request := natLinkRequest(state.nat, dt)
		assert.NotContains(t, request.Tags, EgressIdentityTagKey, "a link repair adds no tag")
		state.nat = settle(request)
		assert.Empty(t, state.mismatch(t, cfg, false))
	})

	t.Run("no NAT Gateway", func(t *testing.T) {
		request := natLinkRequest(nil, dt)
		assert.Nil(t, request.Name, "without a name the NAT Gateway client refuses to write the request")
		assert.Equal(t, armnetwork.NatGateway{Properties: &armnetwork.NatGatewayPropertiesFormat{
			ServiceGateway: &armnetwork.SubResource{ID: to.Ptr(sgw)},
		}}, request)
	})
}

// TestNATUnlinkRequest_Wire pins the exact JSON of the BYO residue unlink on armnetwork v12 for a linked NAT Gateway as
// Azure returns it: the read with its read-only fields removed and the link as JSON null, the only value that clears it.
func TestNATUnlinkRequest_Wire(t *testing.T) {
	const (
		managed = `/subscriptions/netsub/resourceGroups/MC_rg/providers/Microsoft.Network/`
		prefix  = `/subscriptions/netsub/resourceGroups/User-RG/providers/Microsoft.Network/publicIPPrefixes/user-prefix`
	)
	read := `{"etag":"W/\"5e2f\"","id":"` + managed + `natGateways/team-egress","location":"westus2","name":"team-egress",` +
		`"type":"Microsoft.Network/natGateways","sku":{"name":"StandardV2"},"zones":["1","2","3"],` +
		`"tags":{"Owner":"alice","k8s-azure-egress-identity":"team-egress"},"properties":{"idleTimeoutInMinutes":10,` +
		`"provisioningState":"Succeeded","resourceGuid":"bc852ddb-3006-4380-8885-16f99117761a",` +
		`"publicIpAddresses":[{"id":"` + managed + `publicIPAddresses/team-egress-pip"},{"id":"` + managed + `publicIPAddresses/team-egress-pip-2"}],` +
		`"publicIpPrefixes":[{"id":"` + prefix + `"}],"serviceGateway":{"id":"` + managed + `serviceGateways/sgw"},` +
		`"subnets":[{"id":"` + managed + `virtualNetworks/vnet/subnets/s"}]}}`
	var existing armnetwork.NatGateway
	if err := json.Unmarshal([]byte(read), &existing); err != nil {
		t.Fatal(err)
	}

	want := `{"id":"` + managed + `natGateways/team-egress","location":"westus2","name":"team-egress","properties":{` +
		`"idleTimeoutInMinutes":10,` +
		`"publicIpAddresses":[{"id":"` + managed + `publicIPAddresses/team-egress-pip"},{"id":"` + managed + `publicIPAddresses/team-egress-pip-2"}],` +
		`"publicIpPrefixes":[{"id":"` + prefix + `"}],"serviceGateway":null},` +
		`"sku":{"name":"StandardV2"},"tags":{"Owner":"alice","k8s-azure-egress-identity":"team-egress"},"zones":["1","2","3"]}`
	assert.Equal(t, want, egressNATTestJSON(t, natUnlinkRequest(&existing)))
}

// TestNATUnlinkRequest_WritableFieldsOnly builds the unlink over a NAT Gateway with every field of its v12 model set:
// the request is the read without its read-only fields and with the link cleared, and differs from the writable fields
// of the read (natWritable) in the link only.
func TestNATUnlinkRequest_WritableFieldsOnly(t *testing.T) {
	existing := &armnetwork.NatGateway{}
	egressFillAll(reflect.ValueOf(existing).Elem(), 6)
	before := egressNATTestJSON(t, existing)
	request := natUnlinkRequest(existing)
	assert.Equal(t, before, egressNATTestJSON(t, existing), "the existing NAT Gateway must not change")

	want := egressJSONObject(t, existing)
	for _, readOnly := range []string{"etag", "type"} {
		assert.Contains(t, want, readOnly, "control: v12 marshals %s", readOnly)
		delete(want, readOnly)
	}
	properties := want["properties"].(map[string]any)
	for _, readOnly := range []string{"provisioningState", "resourceGuid", "subnets"} {
		assert.Contains(t, properties, readOnly, "control: v12 marshals %s", readOnly)
		delete(properties, readOnly)
	}
	assert.Contains(t, properties, "serviceGateway", "control: the read is linked")
	properties["serviceGateway"] = nil
	assert.Equal(t, egressNATTestJSON(t, want), egressNATTestJSON(t, request))

	assert.True(t, azcore.IsNullValue(request.Properties.ServiceGateway), "the link is the null value that clears it")
	writable := natWritable(existing)
	request.Properties.ServiceGateway = writable.Properties.ServiceGateway
	assert.Equal(t, writable, request, "nothing but the link differs from the writable fields of the read")

	*request.Name = "changed"
	*request.ID = "changed"
	*request.Location = "changed"
	*request.SKU.Name = armnetwork.NatGatewaySKUNameStandard
	*request.Tags["filled"] = "changed"
	*request.Zones[0] = "changed"
	*request.Properties.IdleTimeoutInMinutes = 99
	*request.Properties.Nat64 = "changed"
	*request.Properties.PublicIPAddresses[0].ID = "changed"
	*request.Properties.PublicIPAddressesV6[0].ID = "changed"
	*request.Properties.PublicIPPrefixes[0].ID = "changed"
	*request.Properties.PublicIPPrefixesV6[0].ID = "changed"
	*request.Properties.SourceVirtualNetwork.ID = "changed"
	assert.Equal(t, before, egressNATTestJSON(t, existing), "the request must share no memory with the existing NAT Gateway")
}

// TestNATUnlinkRequest_ChangesOnlyTheLink pins that the unlink and the link repair write the same fields, and that an
// unlinked NAT Gateway linked again satisfies the configuration it had.
func TestNATUnlinkRequest_ChangesOnlyTheLink(t *testing.T) {
	dt := egressNATTestDT()
	t.Run("same fields as the link repair", func(t *testing.T) {
		existing := &armnetwork.NatGateway{}
		egressFillAll(reflect.ValueOf(existing).Elem(), 6)
		unlink := egressJSONObject(t, natUnlinkRequest(existing))
		link := egressJSONObject(t, natLinkRequest(existing, dt))
		assert.Equal(t, egressJSONKeys(link), egressJSONKeys(unlink))
		assert.Equal(t, egressJSONKeys(link["properties"]), egressJSONKeys(unlink["properties"]))
		assert.Nil(t, unlink["properties"].(map[string]any)["serviceGateway"])
	})

	t.Run("an applied configuration is kept", func(t *testing.T) {
		cfg := egressNATTestRichConfig(t)
		state := egressNATTestApplied(t, cfg, true)
		read := natWritable(state.nat)
		request := natUnlinkRequest(state.nat)
		assert.True(t, azcore.IsNullValue(request.Properties.ServiceGateway))
		request.Properties.ServiceGateway = read.Properties.ServiceGateway
		assert.Equal(t, read, request, "nothing but the link changes")

		// What Azure stores for the unlink: the request, settled, without a link.
		unlinked := natUnlinkRequest(state.nat)
		unlinked.Properties.ServiceGateway = nil
		unlinked.Properties.ProvisioningState = to.Ptr(armnetwork.ProvisioningStateSucceeded)
		state.nat = &unlinked
		assert.Equal(t, "the NAT Gateway is not linked to a Service Gateway", state.mismatch(t, cfg, true))
		relinked := natLinkRequest(state.nat, dt)
		relinked.Properties.ProvisioningState = to.Ptr(armnetwork.ProvisioningStateSucceeded)
		state.nat = &relinked
		assert.Empty(t, state.mismatch(t, cfg, true), "linked again, it satisfies the configuration it had")
	})

	t.Run("an unlinked NAT Gateway is sent unlinked", func(t *testing.T) {
		state := egressNATTestApplied(t, defaultEgressConfig(egressParseContext{}), false)
		state.nat.Properties.ServiceGateway = nil
		assert.Contains(t, egressNATTestJSON(t, natUnlinkRequest(state.nat)), `"serviceGateway":null`)
	})

	t.Run("no NAT Gateway", func(t *testing.T) {
		request := natUnlinkRequest(nil)
		assert.Nil(t, request.Name, "without a name the NAT Gateway client refuses to write the request")
		assert.True(t, azcore.IsNullValue(request.Properties.ServiceGateway))
		request.Properties.ServiceGateway = nil
		assert.Equal(t, armnetwork.NatGateway{Properties: &armnetwork.NatGatewayPropertiesFormat{}}, request)
	})
}
