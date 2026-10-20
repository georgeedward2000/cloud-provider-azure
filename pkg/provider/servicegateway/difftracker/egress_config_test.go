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
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"sigs.k8s.io/cloud-provider-azure/pkg/consts"
)

// TestEgressUnknown_DiffersFromEveryConfiguration pins that the unknown applied value can never be
// mistaken for a configuration: not for the zero value an unconfigured identity carries, nor for one a
// pod can ask for. An unknown NAT Gateway equal to a vote would look converged and never be observed.
func TestEgressUnknown_DiffersFromEveryConfiguration(t *testing.T) {
	cases := []struct {
		name string
		cfg  EgressConfig
	}{
		{"zero value", EgressConfig{}},
		{"default IPv4", EgressConfig{IdleTimeoutMinutes: 4, IPv4: egressFamilyConfig{ManagedPublicIPs: 1}}},
		{"default dual-stack", EgressConfig{
			IdleTimeoutMinutes: 4,
			IPv4:               egressFamilyConfig{ManagedPublicIPs: 1},
			IPv6:               egressFamilyConfig{ManagedPublicIPs: 1},
		}},
		{"idle timeout only", EgressConfig{IdleTimeoutMinutes: 4}},
		{"user resources only", EgressConfig{
			IdleTimeoutMinutes: 120,
			IPv4:               egressFamilyConfig{PublicIPIDs: "pip", PublicIPPrefixIDs: "prefix"},
		}},
		{"managed prefix", EgressConfig{IdleTimeoutMinutes: 30, IPv4: egressFamilyConfig{ManagedPrefixLength: 31}, Tags: "a=b"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.NotEqual(t, egressUnknown, tc.cfg)
		})
	}

	assert.True(t, egressUnknown.IdleTimeoutMinutes < consts.ServiceEgressGatewayIdleTimeoutMinMinutes ||
		egressUnknown.IdleTimeoutMinutes > consts.ServiceEgressGatewayIdleTimeoutMaxMinutes,
		"the sentinel's idle timeout must be one parsing rejects")
	applied := map[EgressConfig]bool{egressUnknown: true}
	assert.True(t, applied[EgressConfig{IdleTimeoutMinutes: -1}], "control: the sentinel is a plain comparable value")
}

const (
	egressAnnIdle      = consts.PodAnnotationServiceEgressGatewayIdleTimeout
	egressAnnCount     = consts.PodAnnotationServiceEgressGatewayPublicIPCount
	egressAnnCount6    = consts.PodAnnotationServiceEgressGatewayPublicIPCountIPv6
	egressAnnLength    = consts.PodAnnotationServiceEgressGatewayPublicIPPrefixLength
	egressAnnLength6   = consts.PodAnnotationServiceEgressGatewayPublicIPPrefixLengthIPv6
	egressAnnPIPs      = consts.PodAnnotationServiceEgressGatewayPublicIPIDs
	egressAnnPIPs6     = consts.PodAnnotationServiceEgressGatewayPublicIPIDsIPv6
	egressAnnPrefixes  = consts.PodAnnotationServiceEgressGatewayPublicIPPrefixIDs
	egressAnnPrefixes6 = consts.PodAnnotationServiceEgressGatewayPublicIPPrefixIDsIPv6
	egressAnnTags      = consts.PodAnnotationServiceEgressGatewayTags

	egressTestSubscription = "8c4ab6f6-8a4e-4a3c-9f5e-5a7d7f2b9e01"
	egressTestNodeRG       = "MC_cluster-rg_eastus"
)

var (
	egressTestPC     = egressParseContext{NetworkSubscription: egressTestSubscription, NodeResourceGroup: egressTestNodeRG}
	egressTestPCDual = egressParseContext{WantsIPv6: true, NetworkSubscription: egressTestSubscription, NodeResourceGroup: egressTestNodeRG}
)

func egressTestPIPID(name string) string {
	return "/subscriptions/" + egressTestSubscription + "/resourceGroups/user-rg/providers/Microsoft.Network/publicIPAddresses/" + name
}

func egressTestPrefixID(name string) string {
	return "/subscriptions/" + egressTestSubscription + "/resourceGroups/user-rg/providers/Microsoft.Network/publicIPPrefixes/" + name
}

// egressTestIDs returns n IDs as an annotation value, listed in reverse order, and in canonical form.
func egressTestIDs(id func(string) string, stem string, n int) (annotation, canonical string) {
	listed := make([]string, n)
	ids := make([]string, n)
	for i := 0; i < n; i++ {
		listed[n-1-i] = id(fmt.Sprintf("%s-%02d", stem, i+1))
		ids[i] = strings.ToLower(id(fmt.Sprintf("%s-%02d", stem, i+1)))
	}
	return strings.Join(listed, ","), strings.Join(ids, ",")
}

// egressTestTags returns n distinct tags in canonical form.
func egressTestTags(n int) string {
	tags := make([]string, n)
	for i := range tags {
		tags[i] = fmt.Sprintf("k%02d=v", i+1)
	}
	return strings.Join(tags, ",")
}

func TestDefaultEgressConfig(t *testing.T) {
	assert.Equal(t, EgressConfig{IdleTimeoutMinutes: 4, IPv4: egressFamilyConfig{ManagedPublicIPs: 1}},
		defaultEgressConfig(egressTestPC))
	assert.Equal(t, EgressConfig{
		IdleTimeoutMinutes: 4,
		IPv4:               egressFamilyConfig{ManagedPublicIPs: 1},
		IPv6:               egressFamilyConfig{ManagedPublicIPs: 1},
	}, defaultEgressConfig(egressTestPCDual))
}

// TestParseEgressConfig_WithoutAnnotationsIsTheDefault pins that a pod without egress annotations votes
// for exactly the configuration every existing identity has, so upgrading changes nothing.
func TestParseEgressConfig_WithoutAnnotationsIsTheDefault(t *testing.T) {
	cases := []struct {
		name string
		ann  map[string]string
	}{
		{"nil", nil},
		{"empty", map[string]string{}},
		{"unrelated", map[string]string{
			"example.com/owner":                  "team-a",
			consts.PodLabelServiceEgressGateway:  "identity",
			consts.ServiceAnnotationAzurePIPTags: "a=b",
		}},
	}
	for _, tc := range cases {
		for _, pc := range []egressParseContext{egressTestPC, egressTestPCDual} {
			t.Run(fmt.Sprintf("%s/ipv6=%t", tc.name, pc.WantsIPv6), func(t *testing.T) {
				cfg, hasAnnotations, err := parseEgressConfig(tc.ann, pc)
				assert.NoError(t, err)
				assert.False(t, hasAnnotations)
				assert.Equal(t, defaultEgressConfig(pc), cfg)
			})
		}
	}
}

func TestParseEgressConfig_Valid(t *testing.T) {
	pipA, pipB, pip6 := egressTestPIPID("pip-a"), egressTestPIPID("pip-b"), egressTestPIPID("pip-v6")
	pfxA, pfxB, pfx6 := egressTestPrefixID("pfx-a"), egressTestPrefixID("pfx-b"), egressTestPrefixID("pfx-v6")
	pips16, pips16Canonical := egressTestIDs(egressTestPIPID, "pip", 16)
	prefixes8, prefixes8Canonical := egressTestIDs(egressTestPrefixID, "pfx", 8)
	prefixes8v6, prefixes8v6Canonical := egressTestIDs(egressTestPrefixID, "pfx-v6", 8)
	lower := strings.ToLower
	one := egressFamilyConfig{ManagedPublicIPs: 1}
	v4 := func(f egressFamilyConfig) EgressConfig { return EgressConfig{IdleTimeoutMinutes: 4, IPv4: f} }
	dual := func(f4, f6 egressFamilyConfig) EgressConfig {
		return EgressConfig{IdleTimeoutMinutes: 4, IPv4: f4, IPv6: f6}
	}

	cases := []struct {
		name string
		pc   egressParseContext
		ann  map[string]string
		want EgressConfig
	}{
		{"idle timeout minimum", egressTestPC, map[string]string{egressAnnIdle: "4"}, v4(one)},
		{"idle timeout maximum", egressTestPC, map[string]string{egressAnnIdle: "120"},
			EgressConfig{IdleTimeoutMinutes: 120, IPv4: one}},
		{"values are trimmed", egressTestPC, map[string]string{egressAnnIdle: " 10\n"},
			EgressConfig{IdleTimeoutMinutes: 10, IPv4: one}},
		{"explicit count 1 is the default", egressTestPC, map[string]string{egressAnnCount: "1"}, v4(one)},
		{"16 managed IPs", egressTestPC, map[string]string{egressAnnCount: "16"},
			v4(egressFamilyConfig{ManagedPublicIPs: 16})},
		{"count 0 with a managed prefix", egressTestPC, map[string]string{egressAnnCount: "0", egressAnnLength: "31"},
			v4(egressFamilyConfig{ManagedPrefixLength: 31})},
		{"managed prefix /31 replaces the default IP", egressTestPC, map[string]string{egressAnnLength: "31"},
			v4(egressFamilyConfig{ManagedPrefixLength: 31})},
		{"16 from a managed prefix /28", egressTestPC, map[string]string{egressAnnLength: "28"},
			v4(egressFamilyConfig{ManagedPrefixLength: 28})},
		{"user IP replaces the default IP", egressTestPC, map[string]string{egressAnnPIPs: pipA},
			v4(egressFamilyConfig{PublicIPIDs: lower(pipA)})},
		{"user prefix replaces the default IP", egressTestPC, map[string]string{egressAnnPrefixes: pfxA},
			v4(egressFamilyConfig{PublicIPPrefixIDs: lower(pfxA)})},
		{"user IDs are lower-cased, trimmed and sorted", egressTestPC,
			map[string]string{egressAnnPIPs: " " + strings.ToUpper(pipB) + " ,\n" + pipA, egressAnnPrefixes: pfxB + "," + pfxA},
			v4(egressFamilyConfig{PublicIPIDs: lower(pipA) + "," + lower(pipB), PublicIPPrefixIDs: lower(pfxA) + "," + lower(pfxB)})},
		{"user ID subscription is case-insensitive", egressTestPC,
			map[string]string{egressAnnPIPs: strings.Replace(pipA, egressTestSubscription, strings.ToUpper(egressTestSubscription), 1)},
			v4(egressFamilyConfig{PublicIPIDs: lower(pipA)})},
		{"user ID in a resource group named like the node resource group", egressTestPC,
			map[string]string{egressAnnPIPs: strings.Replace(pipA, "user-rg", egressTestNodeRG+"-user", 1)},
			v4(egressFamilyConfig{PublicIPIDs: lower(strings.Replace(pipA, "user-rg", egressTestNodeRG+"-user", 1))})},
		{"explicit count adds to every other source", egressTestPC,
			map[string]string{egressAnnCount: "2", egressAnnLength: "30", egressAnnPIPs: pipA, egressAnnPrefixes: pfxA},
			v4(egressFamilyConfig{ManagedPublicIPs: 2, ManagedPrefixLength: 30, PublicIPIDs: lower(pipA), PublicIPPrefixIDs: lower(pfxA)})},
		{"16 user IPs", egressTestPC, map[string]string{egressAnnPIPs: pips16},
			v4(egressFamilyConfig{PublicIPIDs: pips16Canonical})},
		{"16 from 8 user prefixes", egressTestPC, map[string]string{egressAnnPrefixes: prefixes8},
			v4(egressFamilyConfig{PublicIPPrefixIDs: prefixes8Canonical})},
		{"16 from every source", egressTestPC,
			map[string]string{egressAnnCount: "2", egressAnnLength: "29", egressAnnPIPs: pipA + "," + pipB, egressAnnPrefixes: pfxA + "," + pfxB},
			v4(egressFamilyConfig{ManagedPublicIPs: 2, ManagedPrefixLength: 29,
				PublicIPIDs: lower(pipA) + "," + lower(pipB), PublicIPPrefixIDs: lower(pfxA) + "," + lower(pfxB)})},
		{"16 from managed IPs and a managed prefix /31", egressTestPC, map[string]string{egressAnnCount: "14", egressAnnLength: "31"},
			v4(egressFamilyConfig{ManagedPublicIPs: 14, ManagedPrefixLength: 31})},
		{"dual-stack default", egressTestPCDual, map[string]string{egressAnnIdle: "4"}, dual(one, one)},
		{"16 managed IPv6 IPs", egressTestPCDual, map[string]string{egressAnnCount6: "16"},
			dual(one, egressFamilyConfig{ManagedPublicIPs: 16})},
		{"IPv6 count 0 with a managed prefix", egressTestPCDual, map[string]string{egressAnnCount6: "0", egressAnnLength6: "127"},
			dual(one, egressFamilyConfig{ManagedPrefixLength: 127})},
		{"16 from a managed IPv6 prefix /124", egressTestPCDual, map[string]string{egressAnnLength6: "124"},
			dual(one, egressFamilyConfig{ManagedPrefixLength: 124})},
		{"16 from managed IPv6 IPs and a managed prefix /127", egressTestPCDual, map[string]string{egressAnnCount6: "14", egressAnnLength6: "127"},
			dual(one, egressFamilyConfig{ManagedPublicIPs: 14, ManagedPrefixLength: 127})},
		{"16 from 8 IPv6 user prefixes", egressTestPCDual, map[string]string{egressAnnPrefixes6: prefixes8v6},
			dual(one, egressFamilyConfig{PublicIPPrefixIDs: prefixes8v6Canonical})},
		{"IPv6 user resources", egressTestPCDual, map[string]string{egressAnnPIPs6: pip6, egressAnnPrefixes6: pfx6},
			dual(one, egressFamilyConfig{PublicIPIDs: lower(pip6), PublicIPPrefixIDs: lower(pfx6)})},
		{"IPv4 sources keep the IPv6 default", egressTestPCDual, map[string]string{egressAnnLength: "31", egressAnnPIPs: pipA},
			dual(egressFamilyConfig{ManagedPrefixLength: 31, PublicIPIDs: lower(pipA)}, one)},
		{"IPv6 sources keep the IPv4 default", egressTestPCDual, map[string]string{egressAnnLength6: "126"},
			dual(one, egressFamilyConfig{ManagedPrefixLength: 126})},
		{"tags", egressTestPC, map[string]string{egressAnnTags: "env=prod"},
			EgressConfig{IdleTimeoutMinutes: 4, IPv4: one, Tags: "env=prod"}},
		{"tags are trimmed and ordered by lower-cased key, keeping the spelling", egressTestPC,
			map[string]string{egressAnnTags: " team = a b , Env=Prod,cost center=  "},
			EgressConfig{IdleTimeoutMinutes: 4, IPv4: one, Tags: "cost center=,Env=Prod,team=a b"}},
		{"tag key and value at their length limits", egressTestPC,
			map[string]string{egressAnnTags: strings.Repeat("k", 512) + "=" + strings.Repeat("v", 256)},
			EgressConfig{IdleTimeoutMinutes: 4, IPv4: one, Tags: strings.Repeat("k", 512) + "=" + strings.Repeat("v", 256)}},
		{"tag lengths count characters, not bytes", egressTestPC,
			map[string]string{egressAnnTags: strings.Repeat("é", 512) + "=" + strings.Repeat("ü", 256)},
			EgressConfig{IdleTimeoutMinutes: 4, IPv4: one, Tags: strings.Repeat("é", 512) + "=" + strings.Repeat("ü", 256)}},
		{"tag value containing null", egressTestPC, map[string]string{egressAnnTags: "a=nullable"},
			EgressConfig{IdleTimeoutMinutes: 4, IPv4: one, Tags: "a=nullable"}},
		{"49 tags", egressTestPC, map[string]string{egressAnnTags: egressTestTags(49)},
			EgressConfig{IdleTimeoutMinutes: 4, IPv4: one, Tags: egressTestTags(49)}},
		{"every setting", egressTestPCDual, map[string]string{
			egressAnnIdle: "30", egressAnnCount: "3", egressAnnLength: "30", egressAnnPIPs: pipA, egressAnnPrefixes: pfxA,
			egressAnnCount6: "2", egressAnnLength6: "126", egressAnnPIPs6: pip6, egressAnnPrefixes6: pfx6, egressAnnTags: "b=2,a=1",
		}, EgressConfig{
			IdleTimeoutMinutes: 30,
			IPv4:               egressFamilyConfig{ManagedPublicIPs: 3, ManagedPrefixLength: 30, PublicIPIDs: lower(pipA), PublicIPPrefixIDs: lower(pfxA)},
			IPv6:               egressFamilyConfig{ManagedPublicIPs: 2, ManagedPrefixLength: 126, PublicIPIDs: lower(pip6), PublicIPPrefixIDs: lower(pfx6)},
			Tags:               "a=1,b=2",
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, hasAnnotations, err := parseEgressConfig(tc.ann, tc.pc)
			assert.NoError(t, err)
			assert.True(t, hasAnnotations)
			assert.Equal(t, tc.want, cfg)
			assert.NotEqual(t, egressUnknown, cfg)
		})
	}
}

func TestParseEgressConfig_Invalid(t *testing.T) {
	pipA, pfxA := egressTestPIPID("pip-a"), egressTestPrefixID("pfx-a")
	pips16, _ := egressTestIDs(egressTestPIPID, "pip", 16)
	prefixes8, _ := egressTestIDs(egressTestPrefixID, "pfx", 8)
	prefixes9, _ := egressTestIDs(egressTestPrefixID, "pfx", 9)
	otherSubscription := strings.Replace(pipA, egressTestSubscription, "11111111-2222-3333-4444-555555555555", 1)
	inNodeRG := strings.Replace(pipA, "user-rg", strings.ToLower(egressTestNodeRG), 1)
	prefixInNodeRG := strings.Replace(pfxA, "user-rg", egressTestNodeRG, 1)
	resourceGroup := "/subscriptions/" + egressTestSubscription + "/resourceGroups/user-rg"
	natGateway := "/subscriptions/" + egressTestSubscription + "/resourceGroups/user-rg/providers/Microsoft.Network/natGateways/nat"
	child := pipA + "/children/x"
	extension := "/subscriptions/" + egressTestSubscription +
		"/resourceGroups/user-rg/providers/Microsoft.Compute/virtualMachines/vm/providers/Microsoft.Network/publicIPAddresses/x"
	subscriptionLevel := "/subscriptions/" + egressTestSubscription + "/providers/Microsoft.Network/publicIPAddresses/x"
	noAddress := func(family string) string {
		return family + ": no public address (the Public IP count is 0 and no prefix or user Public IP is set)"
	}
	tooMany := func(family string, n int) string {
		return fmt.Sprintf("%s: %d public addresses, more than 16 (managed Public IPs + managed prefix size + "+
			"user Public IPs + 2 per user prefix)", family, n)
	}
	longKey, longValue := strings.Repeat("k", 513), strings.Repeat("v", 257)
	reservedKeys := []string{
		"k8s-azure-egress-identity", "K8S-Azure-Egress-Identity", "k8s-azure-cluster-name", "kubernetes-cluster-name",
		"k8s-azure-service", "service", "Service", "k8s-azure-dns-label-service", "kubernetes-dns-label-service",
	}

	type testCase struct {
		name    string
		pc      egressParseContext
		ann     map[string]string
		wantErr string
	}
	cases := []testCase{
		{"unknown annotation", egressTestPC, map[string]string{consts.PodAnnotationServiceEgressGatewayPrefix + "foo": "1"},
			consts.PodAnnotationServiceEgressGatewayPrefix + "foo: unknown annotation"},
		{"bare prefix", egressTestPC, map[string]string{consts.PodAnnotationServiceEgressGatewayPrefix: "1"},
			consts.PodAnnotationServiceEgressGatewayPrefix + ": unknown annotation"},
		{"unknown IPv6 idle timeout", egressTestPCDual, map[string]string{egressAnnIdle + "-ipv6": "10"},
			egressAnnIdle + "-ipv6: unknown annotation"},
		{"idle timeout below the minimum", egressTestPC, map[string]string{egressAnnIdle: "3"},
			egressAnnIdle + `: "3" is not an integer from 4 to 120`},
		{"idle timeout above the maximum", egressTestPC, map[string]string{egressAnnIdle: "121"},
			egressAnnIdle + `: "121" is not an integer from 4 to 120`},
		{"idle timeout of the unknown sentinel", egressTestPC, map[string]string{egressAnnIdle: "-1"},
			egressAnnIdle + `: "-1" is not an integer from 4 to 120`},
		{"idle timeout not a number", egressTestPC, map[string]string{egressAnnIdle: "10m"},
			egressAnnIdle + `: "10m" is not an integer from 4 to 120`},
		{"idle timeout not an integer", egressTestPC, map[string]string{egressAnnIdle: "4.5"},
			egressAnnIdle + `: "4.5" is not an integer from 4 to 120`},
		{"idle timeout overflowing", egressTestPC, map[string]string{egressAnnIdle: "99999999999999999999"},
			egressAnnIdle + `: "99999999999999999999" is not an integer from 4 to 120`},
		{"count above the maximum", egressTestPC, map[string]string{egressAnnCount: "17"},
			egressAnnCount + `: "17" is not an integer from 0 to 16`},
		{"count negative", egressTestPC, map[string]string{egressAnnCount: "-1"},
			egressAnnCount + `: "-1" is not an integer from 0 to 16`},
		{"IPv6 count above the maximum", egressTestPCDual, map[string]string{egressAnnCount6: "17"},
			egressAnnCount6 + `: "17" is not an integer from 0 to 16`},
		{"prefix length 27", egressTestPC, map[string]string{egressAnnLength: "27"},
			egressAnnLength + `: "27" is not an integer from 28 to 31`},
		{"prefix length 32", egressTestPC, map[string]string{egressAnnLength: "32"},
			egressAnnLength + `: "32" is not an integer from 28 to 31`},
		{"prefix length 0 is not none", egressTestPC, map[string]string{egressAnnLength: "0"},
			egressAnnLength + `: "0" is not an integer from 28 to 31`},
		{"IPv6 prefix length of IPv4", egressTestPCDual, map[string]string{egressAnnLength6: "31"},
			egressAnnLength6 + `: "31" is not an integer from 124 to 127`},
		{"IPv6 prefix length 123", egressTestPCDual, map[string]string{egressAnnLength6: "123"},
			egressAnnLength6 + `: "123" is not an integer from 124 to 127`},
		{"IPv6 prefix length 128", egressTestPCDual, map[string]string{egressAnnLength6: "128"},
			egressAnnLength6 + `: "128" is not an integer from 124 to 127`},
		{"IPv4 prefix length for IPv6", egressTestPC, map[string]string{egressAnnLength: "127"},
			egressAnnLength + `: "127" is not an integer from 28 to 31`},
		{"not a resource ID", egressTestPC, map[string]string{egressAnnPIPs: "pip-a"},
			egressAnnPIPs + `: "pip-a" is not a Public IP address ID`},
		{"resource ID without the leading slash", egressTestPC, map[string]string{egressAnnPIPs: pipA[1:]},
			egressAnnPIPs + fmt.Sprintf(": %q is not a Public IP address ID", pipA[1:])},
		{"resource group ID", egressTestPC, map[string]string{egressAnnPrefixes: resourceGroup},
			egressAnnPrefixes + fmt.Sprintf(": %q is not a Public IP prefix ID", resourceGroup)},
		{"NAT Gateway ID", egressTestPC, map[string]string{egressAnnPIPs: natGateway},
			egressAnnPIPs + fmt.Sprintf(": %q is not a Public IP address ID", natGateway)},
		{"prefix ID as a Public IP", egressTestPC, map[string]string{egressAnnPIPs: pfxA},
			egressAnnPIPs + fmt.Sprintf(": %q is not a Public IP address ID", pfxA)},
		{"Public IP ID as a prefix", egressTestPC, map[string]string{egressAnnPrefixes: pipA},
			egressAnnPrefixes + fmt.Sprintf(": %q is not a Public IP prefix ID", pipA)},
		{"child resource ID", egressTestPC, map[string]string{egressAnnPIPs: child},
			egressAnnPIPs + fmt.Sprintf(": %q is not a Public IP address ID", child)},
		{"extension resource ID", egressTestPC, map[string]string{egressAnnPIPs: extension},
			egressAnnPIPs + fmt.Sprintf(": %q is not a Public IP address ID", extension)},
		{"ID without a resource group", egressTestPC, map[string]string{egressAnnPIPs: subscriptionLevel},
			egressAnnPIPs + fmt.Sprintf(": %q is not a Public IP address ID", subscriptionLevel)},
		{"ID in another subscription", egressTestPC, map[string]string{egressAnnPIPs: otherSubscription},
			egressAnnPIPs + fmt.Sprintf(": %q is not in subscription %q", otherSubscription, egressTestSubscription)},
		{"prefix in the node resource group", egressTestPC, map[string]string{egressAnnPrefixes: prefixInNodeRG},
			egressAnnPrefixes + fmt.Sprintf(": %q is in the node resource group %q, which the cluster manages", prefixInNodeRG, egressTestNodeRG)},
		{"Public IP in the node resource group, any case", egressTestPC, map[string]string{egressAnnPIPs: inNodeRG},
			egressAnnPIPs + fmt.Sprintf(": %q is in the node resource group %q, which the cluster manages", inNodeRG, egressTestNodeRG)},
		{"duplicate ID", egressTestPC, map[string]string{egressAnnPIPs: pipA + "," + pipA},
			egressAnnPIPs + fmt.Sprintf(": %q is listed more than once", pipA)},
		{"duplicate ID in another case", egressTestPC, map[string]string{egressAnnPrefixes: pfxA + "," + strings.ToUpper(pfxA)},
			egressAnnPrefixes + fmt.Sprintf(": %q is listed more than once", strings.ToUpper(pfxA))},
		{"ID listed for both families", egressTestPCDual, map[string]string{egressAnnPIPs: pipA, egressAnnPIPs6: pipA},
			egressAnnPIPs6 + fmt.Sprintf(": %q is also listed in %s", pipA, egressAnnPIPs)},
		{"prefix listed for both families", egressTestPCDual, map[string]string{egressAnnPrefixes: pfxA, egressAnnPrefixes6: pfxA},
			egressAnnPrefixes6 + fmt.Sprintf(": %q is also listed in %s", pfxA, egressAnnPrefixes)},
		{"empty entry in an ID list", egressTestPC, map[string]string{egressAnnPIPs: pipA + ","},
			egressAnnPIPs + ": empty entry in the list"},
		{"IPv4 without an address", egressTestPC, map[string]string{egressAnnCount: "0"}, noAddress("IPv4")},
		{"IPv6 without an address", egressTestPCDual, map[string]string{egressAnnCount6: "0"}, noAddress("IPv6")},
		{"17: 16 managed IPs and a user IP", egressTestPC, map[string]string{egressAnnCount: "16", egressAnnPIPs: pipA},
			tooMany("IPv4", 17)},
		{"17: a managed prefix /28 and a managed IP", egressTestPC, map[string]string{egressAnnLength: "28", egressAnnCount: "1"},
			tooMany("IPv4", 17)},
		{"17: 15 managed IPs and a managed prefix /31", egressTestPC, map[string]string{egressAnnCount: "15", egressAnnLength: "31"},
			tooMany("IPv4", 17)},
		{"17: 16 user IPs and a managed IP", egressTestPC, map[string]string{egressAnnPIPs: pips16, egressAnnCount: "1"},
			tooMany("IPv4", 17)},
		{"17: 8 user prefixes and a user IP", egressTestPC, map[string]string{egressAnnPrefixes: prefixes8, egressAnnPIPs: pipA},
			tooMany("IPv4", 17)},
		{"18: 9 user prefixes", egressTestPC, map[string]string{egressAnnPrefixes: prefixes9}, tooMany("IPv4", 18)},
		{"17: a managed IPv6 prefix /124 and a managed IP", egressTestPCDual,
			map[string]string{egressAnnLength6: "124", egressAnnCount6: "1"}, tooMany("IPv6", 17)},
		{"17: 8 IPv6 user prefixes and a managed IP", egressTestPCDual,
			map[string]string{egressAnnPrefixes6: prefixes8, egressAnnCount6: "1"}, tooMany("IPv6", 17)},
		{"tag without a value", egressTestPC, map[string]string{egressAnnTags: "a"},
			egressAnnTags + `: "a" is not key=value`},
		{"tag with two values", egressTestPC, map[string]string{egressAnnTags: "a=b=c"},
			egressAnnTags + `: "a=b=c" is not key=value`},
		{"tag with an empty key", egressTestPC, map[string]string{egressAnnTags: " = v"},
			egressAnnTags + `: "= v" has an empty key`},
		{"tag key too long", egressTestPC, map[string]string{egressAnnTags: longKey + "=v"},
			egressAnnTags + fmt.Sprintf(": key %q is longer than 512 characters", longKey)},
		{"tag value too long", egressTestPC, map[string]string{egressAnnTags: "a=" + longValue},
			egressAnnTags + `: value of "a" is longer than 256 characters`},
		{"tag value null", egressTestPC, map[string]string{egressAnnTags: "a= Null "},
			egressAnnTags + `: value of "a" is "Null", which Azure reserves`},
		{"tag key set twice in another case", egressTestPC, map[string]string{egressAnnTags: "env=a,Env=b"},
			egressAnnTags + `: key "Env" is set more than once (keys are case-insensitive)`},
		{"empty entry in the tags", egressTestPC, map[string]string{egressAnnTags: "a=b,,c=d"},
			egressAnnTags + ": empty entry in the list"},
		{"50 tags", egressTestPC, map[string]string{egressAnnTags: egressTestTags(50)},
			egressAnnTags + ": 50 tags, more than 49 (Azure accepts 50 and the cluster adds its own)"},
		{"invalid integer does not add a family problem", egressTestPC, map[string]string{egressAnnCount: "x"},
			egressAnnCount + `: "x" is not an integer from 0 to 16`},
		{"invalid ID does not add a family problem", egressTestPC, map[string]string{egressAnnCount: "0", egressAnnPIPs: "x"},
			egressAnnPIPs + `: "x" is not a Public IP address ID`},
	}
	for _, key := range []string{
		egressAnnIdle, egressAnnCount, egressAnnCount6, egressAnnLength, egressAnnLength6,
		egressAnnPIPs, egressAnnPIPs6, egressAnnPrefixes, egressAnnPrefixes6, egressAnnTags,
	} {
		cases = append(cases,
			testCase{"empty " + key, egressTestPCDual, map[string]string{key: ""}, key + ": empty value"},
			testCase{"blank " + key, egressTestPCDual, map[string]string{key: " \t"}, key + ": empty value"})
	}
	for _, setting := range [][2]string{
		{egressAnnCount6, "1"}, {egressAnnLength6, "127"}, {egressAnnPIPs6, pipA}, {egressAnnPrefixes6, pfxA},
	} {
		cases = append(cases, testCase{"IPv6 setting without IPv6: " + setting[0], egressTestPC,
			map[string]string{setting[0]: setting[1]}, setting[0] + ": this egress identity does not use IPv6"})
	}
	for _, c := range []string{"<", ">", "%", "&", `\`, "?", "/"} {
		tagKey := "a" + c + "b"
		cases = append(cases, testCase{"tag key with " + c, egressTestPC, map[string]string{egressAnnTags: tagKey + "=v"},
			egressAnnTags + fmt.Sprintf(`: key %q contains one of <>%%&\?/`, tagKey)})
	}
	for _, reserved := range reservedKeys {
		cases = append(cases, testCase{"reserved tag key " + reserved, egressTestPC, map[string]string{egressAnnTags: reserved + "=x"},
			egressAnnTags + fmt.Sprintf(": key %q is reserved for the cluster", reserved)})
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, hasAnnotations, err := parseEgressConfig(tc.ann, tc.pc)
			if assert.Error(t, err) {
				assert.Equal(t, tc.wantErr, err.Error())
			}
			assert.True(t, hasAnnotations)
			assert.Equal(t, EgressConfig{}, cfg)
		})
	}
}

// TestParseEgressConfig_ReportsEveryProblemInKeyOrder pins that one event lists every problem of a pod,
// identically on every parse, although annotations come from a map.
func TestParseEgressConfig_ReportsEveryProblemInKeyOrder(t *testing.T) {
	ann := map[string]string{
		egressAnnTags: "a,k8s-azure-service=x",
		egressAnnIdle: "x",
		consts.PodAnnotationServiceEgressGatewayPrefix + "zzz": "1",
		consts.PodAnnotationServiceEgressGatewayPrefix + "aaa": "1",
		egressAnnCount:  "0",
		egressAnnPIPs6:  egressTestPIPID("a"),
		egressAnnPIPs:   "b-bad, a-bad",
		egressAnnCount6: "",
	}
	want := strings.Join([]string{
		consts.PodAnnotationServiceEgressGatewayPrefix + "aaa: unknown annotation",
		egressAnnIdle + `: "x" is not an integer from 4 to 120`,
		egressAnnCount6 + ": this egress identity does not use IPv6",
		egressAnnPIPs + `: "b-bad" is not a Public IP address ID`,
		egressAnnPIPs + `: "a-bad" is not a Public IP address ID`,
		egressAnnPIPs6 + ": this egress identity does not use IPv6",
		egressAnnTags + `: "a" is not key=value`,
		egressAnnTags + `: key "k8s-azure-service" is reserved for the cluster`,
		consts.PodAnnotationServiceEgressGatewayPrefix + "zzz: unknown annotation",
	}, "; ")
	for i := 0; i < 20; i++ {
		cfg, hasAnnotations, err := parseEgressConfig(ann, egressTestPC)
		if assert.Error(t, err) {
			assert.Equal(t, want, err.Error())
		}
		assert.True(t, hasAnnotations)
		assert.Equal(t, EgressConfig{}, cfg)
	}

	_, _, err := parseEgressConfig(map[string]string{egressAnnCount: "0", egressAnnTags: "a"}, egressTestPC)
	if assert.Error(t, err) {
		assert.Equal(t, "IPv4: no public address (the Public IP count is 0 and no prefix or user Public IP is set); "+
			egressAnnTags+`: "a" is not key=value`, err.Error(), "a family problem sorts by family name, before the annotations")
	}
}

// TestParseEgressConfig_IntegerRanges sweeps every integer setting across its bounds: only values in range
// are accepted, and no value, in range or not, yields the unknown sentinel.
func TestParseEgressConfig_IntegerRanges(t *testing.T) {
	cases := []struct {
		key    string
		lo, hi int
	}{
		{egressAnnIdle, 4, 120},
		{egressAnnCount, 0, 16},
		{egressAnnCount6, 0, 16},
		{egressAnnLength, 28, 31},
		{egressAnnLength6, 124, 127},
	}
	for _, tc := range cases {
		t.Run(tc.key, func(t *testing.T) {
			for n := tc.lo - 5; n <= tc.hi+5; n++ {
				value := strconv.Itoa(n)
				cfg, _, err := parseEgressConfig(map[string]string{tc.key: value}, egressTestPCDual)
				rangeProblem := fmt.Sprintf("%s: %q is not an integer from %d to %d", tc.key, value, tc.lo, tc.hi)
				inRange := n >= tc.lo && n <= tc.hi
				assert.Equal(t, !inRange, err != nil && strings.Contains(err.Error(), rangeProblem), "value %d", n)
				assert.NotEqual(t, egressUnknown, cfg, "value %d", n)
				if err == nil {
					assert.GreaterOrEqual(t, cfg.IdleTimeoutMinutes, int32(consts.ServiceEgressGatewayIdleTimeoutMinMinutes))
					assert.LessOrEqual(t, cfg.IdleTimeoutMinutes, int32(consts.ServiceEgressGatewayIdleTimeoutMaxMinutes))
				}
				if tc.key == egressAnnIdle && inRange {
					assert.Equal(t, int32(n), cfg.IdleTimeoutMinutes)
				}
			}
		})
	}
}

// TestParseEgressConfig_HasAnnotations pins that any annotation with the egress prefix counts, valid or
// not and even with the default value: a BYO identity rejects every such pod.
func TestParseEgressConfig_HasAnnotations(t *testing.T) {
	cases := []struct {
		name string
		ann  map[string]string
		want bool
	}{
		{"none", nil, false},
		{"the egress label as an annotation", map[string]string{consts.PodLabelServiceEgressGateway: "identity"}, false},
		{"explicit default idle timeout", map[string]string{egressAnnIdle: "4"}, true},
		{"explicit default count", map[string]string{egressAnnCount: "1"}, true},
		{"invalid value", map[string]string{egressAnnIdle: "x"}, true},
		{"empty value", map[string]string{egressAnnTags: ""}, true},
		{"unknown annotation", map[string]string{consts.PodAnnotationServiceEgressGatewayPrefix + "foo": "1"}, true},
		{"IPv6 setting without IPv6", map[string]string{egressAnnCount6: "1"}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, hasAnnotations, _ := parseEgressConfig(tc.ann, egressTestPC)
			assert.Equal(t, tc.want, hasAnnotations)
		})
	}
}

// TestParseEgressConfig_EquivalentSpellingsAreEqual pins the canonical form: pods that ask for the same
// configuration in different words vote together, and pods that ask for different ones do not.
func TestParseEgressConfig_EquivalentSpellingsAreEqual(t *testing.T) {
	pipA, pipB, pfxA := egressTestPIPID("pip-a"), egressTestPIPID("pip-b"), egressTestPrefixID("pfx-a")
	cases := []struct {
		name  string
		pc    egressParseContext
		a, b  map[string]string
		equal bool
	}{
		{"explicit default and no annotation", egressTestPCDual,
			map[string]string{egressAnnIdle: "4", egressAnnCount: "1", egressAnnCount6: "1"}, nil, true},
		{"integer spacing", egressTestPC, map[string]string{egressAnnIdle: " 10"}, map[string]string{egressAnnIdle: "10 "}, true},
		{"explicit count 0 next to a managed prefix", egressTestPC,
			map[string]string{egressAnnCount: "0", egressAnnLength: "30"}, map[string]string{egressAnnLength: "30"}, true},
		{"user ID order, case, spacing and trailing slash", egressTestPC,
			map[string]string{egressAnnPIPs: pipA + "," + pipB, egressAnnPrefixes: pfxA},
			map[string]string{egressAnnPIPs: " " + strings.ToUpper(pipB) + "/ , " + strings.ToLower(pipA), egressAnnPrefixes: strings.ToUpper(pfxA)},
			true},
		{"tag order and spacing", egressTestPC,
			map[string]string{egressAnnTags: "a=1,b=2"}, map[string]string{egressAnnTags: " b = 2 ,a=1 "}, true},
		{"explicit count 1 next to a managed prefix", egressTestPC,
			map[string]string{egressAnnCount: "1", egressAnnLength: "30"}, map[string]string{egressAnnLength: "30"}, false},
		{"other user ID", egressTestPC, map[string]string{egressAnnPIPs: pipA}, map[string]string{egressAnnPIPs: pipB}, false},
		{"tag key spelling", egressTestPC, map[string]string{egressAnnTags: "Env=prod"}, map[string]string{egressAnnTags: "env=prod"}, false},
		{"tag value case", egressTestPC, map[string]string{egressAnnTags: "env=Prod"}, map[string]string{egressAnnTags: "env=prod"}, false},
		{"IPv4 and IPv6 prefix", egressTestPCDual,
			map[string]string{egressAnnLength: "31"}, map[string]string{egressAnnLength6: "127"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a, _, errA := parseEgressConfig(tc.a, tc.pc)
			b, _, errB := parseEgressConfig(tc.b, tc.pc)
			assert.NoError(t, errA)
			assert.NoError(t, errB)
			assert.Equal(t, tc.equal, a == b, "%+v vs %+v", a, b)
			assert.Equal(t, tc.equal, a.Summary() == b.Summary(), "%q vs %q", a.Summary(), b.Summary())
		})
	}
}

func TestEgressConfigSummary(t *testing.T) {
	everything, _, err := parseEgressConfig(map[string]string{
		egressAnnIdle: "10", egressAnnCount: "2", egressAnnLength: "30",
		egressAnnPIPs:     egressTestPIPID("pip-b") + "," + egressTestPIPID("PIP-A"),
		egressAnnPrefixes: egressTestPrefixID("pfx-a"),
		egressAnnLength6:  "127", egressAnnTags: "b=2,A=1",
	}, egressTestPCDual)
	assert.NoError(t, err)

	cases := []struct {
		name string
		cfg  EgressConfig
		want string
	}{
		{"unknown", egressUnknown, "unknown"},
		{"default", defaultEgressConfig(egressTestPC), "idle timeout 4m; IPv4: 1 managed IP"},
		{"dual-stack default", defaultEgressConfig(egressTestPCDual), "idle timeout 4m; IPv4: 1 managed IP; IPv6: 1 managed IP"},
		{"user resources only", EgressConfig{IdleTimeoutMinutes: 4, IPv4: egressFamilyConfig{
			PublicIPIDs: strings.ToLower(egressTestPIPID("pip-a")), PublicIPPrefixIDs: strings.ToLower(egressTestPrefixID("pfx-a")),
		}}, "idle timeout 4m; IPv4: user IPs (pip-a), user prefixes (pfx-a)"},
		{"every setting", everything, "idle timeout 10m; IPv4: 2 managed IPs, managed prefix /30, user IPs (pip-a, pip-b), " +
			"user prefixes (pfx-a); IPv6: managed prefix /127; tags: A=1,b=2"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.cfg.Summary())
		})
	}
}

// FuzzParseEgressConfig checks the parser's guarantees on any annotations: no panic, never the unknown
// sentinel, a valid configuration within the address limits, and a canonical form that parses to itself.
func FuzzParseEgressConfig(f *testing.F) {
	f.Add(false, "10", "2", "31", egressTestPIPID("a"), egressTestPrefixID("b"), "", "", "", "", " b=2, A=1")
	f.Add(true, "", "", "", "", "", "0", "125", egressTestPIPID("B")+","+egressTestPIPID("a"), "", "")
	f.Add(true, "x", "17", "27", "not-an-id", ",", "-1", "128", egressTestPIPID("a"), egressTestPrefixID("a"), "a,service=x")
	f.Fuzz(func(t *testing.T, wantsIPv6 bool, idle, count, length, pips, prefixes, count6, length6, pips6, prefixes6, tags string) {
		pc := egressTestPC
		pc.WantsIPv6 = wantsIPv6
		ann := map[string]string{}
		for _, setting := range [][2]string{
			{egressAnnIdle, idle}, {egressAnnCount, count}, {egressAnnLength, length}, {egressAnnPIPs, pips},
			{egressAnnPrefixes, prefixes}, {egressAnnCount6, count6}, {egressAnnLength6, length6}, {egressAnnPIPs6, pips6},
			{egressAnnPrefixes6, prefixes6}, {egressAnnTags, tags},
		} {
			if setting[1] != "" {
				ann[setting[0]] = setting[1]
			}
		}

		cfg, hasAnnotations, err := parseEgressConfig(ann, pc)
		assert.NotEqual(t, egressUnknown, cfg)
		assert.Equal(t, len(ann) > 0, hasAnnotations)
		if err != nil {
			assert.Equal(t, EgressConfig{}, cfg)
			return
		}
		assert.True(t, cfg.IdleTimeoutMinutes >= 4 && cfg.IdleTimeoutMinutes <= 120, "idle timeout %d", cfg.IdleTimeoutMinutes)
		addresses := func(family egressFamilyConfig, bits int) int {
			n := family.ManagedPublicIPs + len(egressTestSplit(family.PublicIPIDs)) + 2*len(egressTestSplit(family.PublicIPPrefixIDs))
			if family.ManagedPrefixLength > 0 {
				n += 1 << (bits - family.ManagedPrefixLength)
			}
			return n
		}
		assert.True(t, addresses(cfg.IPv4, 32) >= 1 && addresses(cfg.IPv4, 32) <= 16, "%+v", cfg.IPv4)
		if wantsIPv6 {
			assert.True(t, addresses(cfg.IPv6, 128) >= 1 && addresses(cfg.IPv6, 128) <= 16, "%+v", cfg.IPv6)
		} else {
			assert.Equal(t, egressFamilyConfig{}, cfg.IPv6)
		}

		canonical := map[string]string{egressAnnIdle: strconv.Itoa(int(cfg.IdleTimeoutMinutes)), egressAnnTags: cfg.Tags}
		render := func(a egressFamilyAnnotations, family egressFamilyConfig) {
			canonical[a.count] = strconv.Itoa(family.ManagedPublicIPs)
			if family.ManagedPrefixLength > 0 {
				canonical[a.prefixLength] = strconv.Itoa(family.ManagedPrefixLength)
			}
			canonical[a.publicIPIDs], canonical[a.prefixIDs] = family.PublicIPIDs, family.PublicIPPrefixIDs
		}
		render(egressIPv4Annotations, cfg.IPv4)
		if wantsIPv6 {
			render(egressIPv6Annotations, cfg.IPv6)
		}
		for key, value := range canonical {
			if value == "" {
				delete(canonical, key)
			}
		}
		again, _, err := parseEgressConfig(canonical, pc)
		assert.NoError(t, err)
		assert.Equal(t, cfg, again)
	})
}

func egressTestSplit(ids string) []string {
	if ids == "" {
		return nil
	}
	return strings.Split(ids, ",")
}
