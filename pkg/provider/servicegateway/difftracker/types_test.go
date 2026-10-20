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
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/utils/ptr"
)

// TestInboundConfigEquals_ComparesEveryField pins that config drift detection sees every field it
// carries. Equals decides whether a user's spec change is dispatched to Azure at all, so a field it
// ignores is a field the user can edit with no effect.
func TestInboundConfigEquals_ComparesEveryField(t *testing.T) {
	base := func() *InboundConfig {
		return &InboundConfig{
			FrontendPorts:      []PortMapping{{Port: 80, Protocol: "TCP"}},
			BackendPorts:       []PortMapping{{Port: 8080, Protocol: "TCP"}},
			IdleTimeoutMinutes: ptr.To[int32](10),
			IPFamilies:         []string{"IPv4"},
			NamedTargetPorts:   []string{"http"},
		}
	}

	cases := []struct {
		field  string
		mutate func(*InboundConfig)
	}{
		{"IdleTimeoutMinutes", func(c *InboundConfig) { c.IdleTimeoutMinutes = ptr.To[int32](20) }},
		{"IdleTimeoutMinutes cleared", func(c *InboundConfig) { c.IdleTimeoutMinutes = nil }},
		{"IPFamilies", func(c *InboundConfig) { c.IPFamilies = []string{"IPv6"} }},
		{"NamedTargetPorts", func(c *InboundConfig) { c.NamedTargetPorts = []string{"metrics"} }},
		{"FrontendPorts", func(c *InboundConfig) { c.FrontendPorts = []PortMapping{{Port: 443, Protocol: "TCP"}} }},
		{"BackendPorts", func(c *InboundConfig) { c.BackendPorts = []PortMapping{{Port: 9090, Protocol: "TCP"}} }},
	}

	for _, tc := range cases {
		t.Run(tc.field, func(t *testing.T) {
			changed := base()
			tc.mutate(changed)
			assert.False(t, base().Equals(changed), "a change to %s must be detected as drift", tc.field)
		})
	}

	assert.True(t, base().Equals(base()), "control: identical configs must compare equal")
}

// TestOutboundConfigDeepCopy pins that a copy shares no memory with its source, so a snapshot of a
// published outbound config cannot change when the source is edited in place.
func TestOutboundConfigDeepCopy(t *testing.T) {
	cases := []struct {
		name string
		cfg  *OutboundConfig
	}{
		{"nil", nil},
		{"no families", &OutboundConfig{}},
		{"dual-stack with an egress configuration", &OutboundConfig{
			IPFamilies: []string{"IPv4", "IPv6"},
			Egress: EgressConfig{
				IdleTimeoutMinutes: 10,
				IPv4:               egressFamilyConfig{ManagedPublicIPs: 2},
				IPv6:               egressFamilyConfig{ManagedPrefixLength: 127},
				Tags:               "team=a",
			},
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.cfg.DeepCopy()
			if tc.cfg == nil {
				assert.Nil(t, got)
				return
			}
			want := &OutboundConfig{IPFamilies: append([]string(nil), tc.cfg.IPFamilies...), Egress: tc.cfg.Egress}
			assert.Equal(t, want.IPFamilies, got.IPFamilies)
			assert.Equal(t, want.Egress, got.Egress)

			for i := range got.IPFamilies {
				got.IPFamilies[i] = "mutated"
			}
			got.Egress = egressUnknown
			assert.Equal(t, want.IPFamilies, tc.cfg.IPFamilies, "editing the copy must not change the source")
			assert.Equal(t, want.Egress, tc.cfg.Egress, "editing the copy must not change the source")
		})
	}
}

// TestServiceConfigSnapshot_CopiesTheOutboundPart pins that a snapshot of a service config owns its
// outbound config, while the rest of the config is carried over unchanged.
func TestServiceConfigSnapshot_CopiesTheOutboundPart(t *testing.T) {
	inbound := NewInboundServiceConfig("svc", &InboundConfig{FrontendPorts: []PortMapping{{Port: 80, Protocol: "TCP"}}})
	inbound.Namespace, inbound.Name = "ns", "svc"

	cases := []struct {
		name string
		cfg  ServiceConfig
	}{
		{"inbound", inbound},
		{"outbound without a config", NewOutboundServiceConfig("egress", nil)},
		{"outbound", NewOutboundServiceConfig("egress", &OutboundConfig{
			IPFamilies: []string{"IPv4"},
			Egress:     EgressConfig{IdleTimeoutMinutes: 4, IPv4: egressFamilyConfig{ManagedPublicIPs: 1}},
		})},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			snap := tc.cfg.snapshot()
			assert.Equal(t, tc.cfg, snap)
			if tc.cfg.OutboundConfig != nil {
				assert.NotSame(t, tc.cfg.OutboundConfig, snap.OutboundConfig, "the snapshot must own its outbound config")
			}
		})
	}
}
