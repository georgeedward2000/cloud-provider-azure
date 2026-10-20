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
	"maps"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/component-base/metrics/testutil"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/cloud-provider-azure/pkg/consts"
	utilsets "sigs.k8s.io/cloud-provider-azure/pkg/util/sets"
)

// TestNoteEgressMutationStarted pins that the write-ahead flag is set on the identity's operation only,
// and that an identity without an operation is left untracked.
func TestNoteEgressMutationStarted(t *testing.T) {
	const uid, other = "egress-write", "egress-other"

	cases := []struct {
		name    string
		tracked bool
	}{
		{"tracked operation", true},
		{"untracked identity", false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dt := newTestDiffTracker()
			dt.pendingServiceOps[other] = &ServiceOperationState{
				ServiceUID: other, Config: NewOutboundServiceConfig(other, &OutboundConfig{}), State: StateUpdateInProgress,
			}
			if tc.tracked {
				dt.pendingServiceOps[uid] = &ServiceOperationState{
					ServiceUID: uid, Config: NewOutboundServiceConfig(uid, &OutboundConfig{}), State: StateUpdateInProgress,
				}
			}

			dt.noteEgressMutationStarted(uid)

			op, exists := dt.pendingServiceOps[uid]
			assert.Equal(t, tc.tracked, exists, "the setter must not create an operation")
			if exists {
				assert.True(t, op.Egress.MutationStarted)
			}
			assert.False(t, dt.pendingServiceOps[other].Egress.MutationStarted, "another identity's operation must not be marked")
			if assert.True(t, dt.mu.TryLock(), "the setter must release dt.mu") {
				dt.mu.Unlock()
			}
		})
	}
}

// ================================================================================================
// Egress NAT Gateway configuration by pod consensus
// ================================================================================================

var (
	consensusEpoch = time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	// consensusApplied is the applied configuration of the evaluation cases (the IPv4 default); the
	// others are votes.
	consensusApplied  = EgressConfig{IdleTimeoutMinutes: 4, IPv4: egressFamilyConfig{ManagedPublicIPs: 1}}
	consensusA        = EgressConfig{IdleTimeoutMinutes: 10, IPv4: egressFamilyConfig{ManagedPublicIPs: 2}}
	consensusB        = EgressConfig{IdleTimeoutMinutes: 20, IPv4: egressFamilyConfig{ManagedPrefixLength: 31}}
	consensusRejected = EgressConfig{IdleTimeoutMinutes: 30, IPv4: egressFamilyConfig{ManagedPublicIPs: 16}}
)

// ipv4Families and dualStackFamilies return a fresh slice on every call, so a case can never share one.
func ipv4Families() []string      { return []string{"IPv4"} }
func dualStackFamilies() []string { return []string{"IPv4", "IPv6"} }

// castEgressVote registers the vote of podKey for identity, cast by a pod created at created and parsed for the
// identity's IP families: cfg, or an invalid vote when cfg is nil.
func castEgressVote(dt *DiffTracker, identity, podKey string, cfg *EgressConfig, created time.Time) {
	vote := egressVote{Identity: identity, HasAnnotations: true, PodUID: types.UID(podKey + "-uid"), PodCreated: created,
		ParsedForIPv6: dt.egressParseContextOfLocked(identity).WantsIPv6}
	if cfg == nil {
		vote.Invalid = "idle-timeout: not an integer"
	} else {
		vote.Config = *cfg
	}
	dt.egress.votes.set(podKey, vote)
}

// TestEgressParseContextFor pins what parsing is told about an identity: whether its NAT Gateway serves
// IPv6, from its families, and the cluster's network subscription and node resource group.
func TestEgressParseContextFor(t *testing.T) {
	sameSubscription := Config{SubscriptionID: "sub", ResourceGroup: "mc-rg"}
	separateNetwork := Config{SubscriptionID: "sub", NetworkResourceSubscriptionID: "network-sub", ResourceGroup: "mc-rg"}
	ipv4 := egressParseContext{NetworkSubscription: "sub", NodeResourceGroup: "mc-rg"}
	ipv6 := egressParseContext{WantsIPv6: true, NetworkSubscription: "sub", NodeResourceGroup: "mc-rg"}

	cases := []struct {
		name     string
		config   Config
		families []string
		want     egressParseContext
	}{
		{"IPv4", sameSubscription, ipv4Families(), ipv4},
		{"no families means IPv4", sameSubscription, nil, ipv4},
		{"dual-stack", sameSubscription, dualStackFamilies(), ipv6},
		{"families are case-insensitive", sameSubscription, []string{"ipv4", "ipv6"}, ipv6},
		{"separate network subscription", separateNetwork, ipv4Families(),
			egressParseContext{NetworkSubscription: "network-sub", NodeResourceGroup: "mc-rg"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dt := newTestDiffTracker()
			dt.config = tc.config
			assert.Equal(t, tc.want, dt.egressParseContextFor(tc.families))
		})
	}
}

// TestCreationConfigLocked pins the configuration a NAT Gateway is created with: agreed, else the seed's
// vote when it is valid, else the default for the identity's families. The seed is the pod that
// triggered the operation (same name and UID) while it is a member of the identity, else the oldest member.
func TestCreationConfigLocked(t *testing.T) {
	const identity, other = "egress-create", "egress-other"
	older, newer := consensusEpoch, consensusEpoch.Add(time.Minute)
	defaultIPv4 := defaultEgressConfig(egressParseContext{})
	defaultDual := defaultEgressConfig(egressParseContext{WantsIPv6: true})
	if !assert.NotEqual(t, defaultIPv4, defaultDual, "control: the defaults must tell the families apart") {
		return
	}

	type vote struct {
		pod      string
		identity string // empty: the identity under test
		cfg      *EgressConfig
		created  time.Time
		noUID    bool // the vote carries no pod UID
	}
	cases := []struct {
		name         string
		untracked    bool
		triggeredBy  string
		triggeredUID string // the recorded UID of the triggering pod; empty: the UID of its vote
		noUID        bool   // no UID was recorded for the triggering pod
		votes        []vote
		families     []string
		want         EgressConfig
	}{
		{name: "agreed configuration", triggeredBy: "ns/seed",
			votes: []vote{{pod: "ns/seed", cfg: &consensusA, created: newer}, {pod: "ns/old", cfg: &consensusA, created: older}},
			want:  consensusA},
		{name: "valid seed vote without agreement", triggeredBy: "ns/seed",
			votes: []vote{{pod: "ns/seed", cfg: &consensusB, created: newer}, {pod: "ns/old", cfg: &consensusA, created: older}},
			want:  consensusB},
		{name: "invalid seed vote gives the default", triggeredBy: "ns/seed",
			votes: []vote{{pod: "ns/seed", created: newer}, {pod: "ns/old", cfg: &consensusA, created: older}},
			want:  defaultIPv4},
		{name: "same-name replacement of the seed: oldest member", triggeredBy: "ns/seed", triggeredUID: "ns/seed-replaced-uid",
			votes: []vote{{pod: "ns/seed", cfg: &consensusB, created: newer}, {pod: "ns/old", cfg: &consensusA, created: older}},
			want:  consensusA},
		{name: "seed without a recorded UID: oldest member", triggeredBy: "ns/seed", noUID: true,
			votes: []vote{{pod: "ns/seed", cfg: &consensusB, created: newer}, {pod: "ns/old", cfg: &consensusA, created: older}},
			want:  consensusA},
		{name: "seed without a recorded UID and a vote without one: oldest member", triggeredBy: "ns/seed", noUID: true,
			votes: []vote{{pod: "ns/seed", cfg: &consensusB, created: newer, noUID: true}, {pod: "ns/old", cfg: &consensusA, created: older}},
			want:  consensusA},
		{name: "seed gone: oldest member", triggeredBy: "ns/gone",
			votes: []vote{{pod: "ns/new", cfg: &consensusB, created: newer}, {pod: "ns/old", cfg: &consensusA, created: older}},
			want:  consensusA},
		{name: "seed now a member of another identity: oldest member", triggeredBy: "ns/seed",
			votes: []vote{
				{pod: "ns/seed", identity: other, cfg: &consensusRejected, created: older.Add(-time.Hour)},
				{pod: "ns/new", cfg: &consensusB, created: newer}, {pod: "ns/old", cfg: &consensusA, created: older},
			},
			want: consensusA},
		{name: "no triggering pod (startup): oldest member",
			votes: []vote{{pod: "ns/new", cfg: &consensusB, created: newer}, {pod: "ns/old", cfg: &consensusA, created: older}},
			want:  consensusA},
		{name: "untracked identity: oldest member", untracked: true,
			votes: []vote{{pod: "ns/new", cfg: &consensusB, created: newer}, {pod: "ns/old", cfg: &consensusA, created: older}},
			want:  consensusA},
		{name: "invalid oldest member gives the default",
			votes: []vote{{pod: "ns/new", cfg: &consensusB, created: newer}, {pod: "ns/old", created: older}},
			want:  defaultIPv4},
		{name: "no members: default", triggeredBy: "ns/seed", want: defaultIPv4},
		{name: "members of other identities do not count",
			votes: []vote{{pod: "ns/other", identity: other, cfg: &consensusA, created: older}},
			want:  defaultIPv4},
		{name: "dual-stack default serves IPv6", families: dualStackFamilies(), want: defaultDual},
		{name: "invalid seed on a dual-stack identity gives its default", triggeredBy: "ns/seed", families: dualStackFamilies(),
			votes: []vote{{pod: "ns/seed", created: newer}},
			want:  defaultDual},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dt := newTestDiffTracker()
			for _, v := range tc.votes {
				voteIdentity := v.identity
				if voteIdentity == "" {
					voteIdentity = identity
				}
				castEgressVote(dt, voteIdentity, v.pod, v.cfg, v.created)
				if v.noUID {
					vote, _ := dt.egress.votes.get(v.pod)
					vote.PodUID = ""
					dt.egress.votes.set(v.pod, vote)
				}
			}
			if !tc.untracked {
				op := &ServiceOperationState{
					ServiceUID: identity,
					Config:     NewOutboundServiceConfig(identity, &OutboundConfig{IPFamilies: tc.families}),
					State:      StateNotStarted,
				}
				op.TriggeringPodNamespace, op.TriggeringPodName, _ = strings.Cut(tc.triggeredBy, "/")
				switch {
				case tc.noUID:
				case tc.triggeredUID != "":
					op.TriggeringPodUID = tc.triggeredUID
				case tc.triggeredBy != "":
					op.TriggeringPodUID = tc.triggeredBy + "-uid"
				}
				dt.pendingServiceOps[identity] = op
			}

			dt.mu.Lock()
			got := dt.creationConfigLocked(identity, tc.families)
			dt.mu.Unlock()

			assert.Equal(t, tc.want, got)
			_, tracked := dt.pendingServiceOps[identity]
			assert.Equal(t, !tc.untracked, tracked, "deciding the configuration must not create an operation")
		})
	}
}

// TestEvaluateEgressConsensusLocked pins how the votes of a managed identity drive its operation in
// every state. Dispatchable: agreed, different from the applied configuration and not rejected
// terminally. Actionable: dispatchable or owed an observation. Every case also checks that the
// evaluation publishes a new desired config instead of modifying the published one, keeps the families
// decided at creation, the applied and in-flight configs, the owed observation and the quiet period, and
// resets the retry state only when it cancels an update or a queued update takes a new agreement.
func TestEvaluateEgressConsensusLocked(t *testing.T) {
	const identity = "egress-consensus"
	zero := EgressConfig{}

	type row struct {
		name             string
		state            ResourceState
		nilDesired       bool // the operation has no outbound config
		desired          EgressConfig
		applied          *EgressConfig // nil: nothing recorded
		inFlight         *EgressConfig
		votes            []*EgressConfig // pod-i is created i minutes after the epoch; nil is an invalid vote
		triggeredBy      string
		rejected         *EgressConfig
		needsObservation bool
		families         []string

		wantState      ResourceState
		wantDesired    EgressConfig
		wantTrigger    bool
		wantRetryReset bool
	}
	cases := []row{
		// Created, nothing actionable: stay.
		{name: "created: agreement equals the applied configuration", state: StateCreated, desired: consensusApplied, applied: &consensusApplied,
			votes: []*EgressConfig{&consensusApplied, &consensusApplied}, wantState: StateCreated, wantDesired: consensusApplied},
		{name: "created: no agreement", state: StateCreated, desired: consensusApplied, applied: &consensusApplied,
			votes: []*EgressConfig{&consensusA, &consensusB}, wantState: StateCreated, wantDesired: consensusApplied},
		{name: "created: an invalid vote blocks the agreement", state: StateCreated, desired: consensusApplied, applied: &consensusApplied,
			votes: []*EgressConfig{&consensusA, nil}, wantState: StateCreated, wantDesired: consensusApplied},
		{name: "created: no members", state: StateCreated, desired: consensusApplied, applied: &consensusApplied,
			wantState: StateCreated, wantDesired: consensusApplied},
		{name: "created: unknown applied configuration without agreement or owed observation", state: StateCreated,
			desired: egressUnknown, applied: &egressUnknown, votes: []*EgressConfig{&consensusA, &consensusB},
			wantState: StateCreated, wantDesired: egressUnknown},
		{name: "created: agreed configuration rejected", state: StateCreated, desired: consensusApplied, applied: &consensusApplied,
			votes: []*EgressConfig{&consensusRejected, &consensusRejected}, rejected: &consensusRejected,
			wantState: StateCreated, wantDesired: consensusApplied},

		// Created, actionable: update, desired = agreed if dispatchable else egressUnknown, trigger.
		{name: "created: dispatchable agreement", state: StateCreated, desired: consensusApplied, applied: &consensusApplied,
			votes: []*EgressConfig{&consensusA, &consensusA}, wantState: StateUpdateInProgress, wantDesired: consensusA, wantTrigger: true},
		{name: "created: agreement for an unknown applied configuration", state: StateCreated, desired: egressUnknown, applied: &egressUnknown,
			votes: []*EgressConfig{&consensusA}, wantState: StateUpdateInProgress, wantDesired: consensusA, wantTrigger: true},
		{name: "created: another configuration rejected", state: StateCreated, desired: consensusApplied, applied: &consensusApplied,
			votes: []*EgressConfig{&consensusA}, rejected: &consensusB, wantState: StateUpdateInProgress, wantDesired: consensusA, wantTrigger: true},
		{name: "created: only a recorded rejection blocks (zero configuration agreed)", state: StateCreated, desired: consensusApplied,
			applied: &consensusApplied, votes: []*EgressConfig{&zero}, wantState: StateUpdateInProgress, wantDesired: zero, wantTrigger: true},
		{name: "created: nothing applied recorded", state: StateCreated, desired: consensusApplied,
			votes: []*EgressConfig{&consensusA}, wantState: StateUpdateInProgress, wantDesired: consensusA, wantTrigger: true},
		{name: "created: owed observation without agreement: observe-and-clean", state: StateCreated, desired: consensusApplied,
			applied: &consensusApplied, votes: []*EgressConfig{&consensusA, &consensusB}, needsObservation: true,
			wantState: StateUpdateInProgress, wantDesired: egressUnknown, wantTrigger: true},
		{name: "created: owed observation, agreement equals the applied configuration: observe-and-clean", state: StateCreated,
			desired: consensusApplied, applied: &consensusApplied, votes: []*EgressConfig{&consensusApplied}, needsObservation: true,
			wantState: StateUpdateInProgress, wantDesired: egressUnknown, wantTrigger: true},
		{name: "created: owed observation, agreement rejected: observe-and-clean", state: StateCreated, desired: consensusApplied,
			applied: &consensusApplied, votes: []*EgressConfig{&consensusRejected}, rejected: &consensusRejected, needsObservation: true,
			wantState: StateUpdateInProgress, wantDesired: egressUnknown, wantTrigger: true},
		{name: "created: owed observation and dispatchable agreement: converge-and-clean", state: StateCreated, desired: consensusApplied,
			applied: &consensusApplied, votes: []*EgressConfig{&consensusA}, needsObservation: true,
			wantState: StateUpdateInProgress, wantDesired: consensusA, wantTrigger: true},
		{name: "created without an outbound config: observe-and-clean gets one", state: StateCreated, nilDesired: true,
			applied: &egressUnknown, needsObservation: true, wantState: StateUpdateInProgress, wantDesired: egressUnknown, wantTrigger: true},

		// UpdateInProgress, not dispatched (queued, backing off or quiet): follow the votes or cancel. A new agreement is
		// dispatched at once, without the backoff or park of the configuration it replaces.
		{name: "update queued: a new dispatchable agreement becomes desired, at once", state: StateUpdateInProgress, desired: consensusB,
			applied: &consensusApplied, votes: []*EgressConfig{&consensusA}, wantState: StateUpdateInProgress, wantDesired: consensusA,
			wantTrigger: true, wantRetryReset: true},
		{name: "observe-and-clean queued: a dispatchable agreement becomes desired, at once", state: StateUpdateInProgress, desired: egressUnknown,
			applied: &consensusApplied, votes: []*EgressConfig{&consensusA}, needsObservation: true, wantState: StateUpdateInProgress,
			wantDesired: consensusA, wantTrigger: true, wantRetryReset: true},
		{name: "update backing off: unchanged agreement stays desired", state: StateUpdateInProgress, desired: consensusA,
			applied: &consensusApplied, votes: []*EgressConfig{&consensusA, &consensusA}, wantState: StateUpdateInProgress, wantDesired: consensusA},
		{name: "update queued: owed observation without agreement: observe-and-clean", state: StateUpdateInProgress, desired: consensusA,
			applied: &consensusApplied, votes: []*EgressConfig{&consensusA, &consensusB}, needsObservation: true,
			wantState: StateUpdateInProgress, wantDesired: egressUnknown},
		{name: "update queued: owed observation, agreement rejected: observe-and-clean", state: StateUpdateInProgress, desired: consensusRejected,
			applied: &consensusApplied, votes: []*EgressConfig{&consensusRejected}, rejected: &consensusRejected, needsObservation: true,
			wantState: StateUpdateInProgress, wantDesired: egressUnknown},
		{name: "update queued: no agreement: cancelled", state: StateUpdateInProgress, desired: consensusA, applied: &consensusApplied,
			votes: []*EgressConfig{&consensusA, &consensusB}, wantState: StateCreated, wantDesired: consensusApplied, wantRetryReset: true},
		{name: "update queued: agreement back to the applied configuration: cancelled", state: StateUpdateInProgress, desired: consensusA,
			applied: &consensusApplied, votes: []*EgressConfig{&consensusApplied}, wantState: StateCreated, wantDesired: consensusApplied, wantRetryReset: true},
		{name: "update queued: agreement rejected: cancelled", state: StateUpdateInProgress, desired: consensusRejected, applied: &consensusApplied,
			votes: []*EgressConfig{&consensusRejected}, rejected: &consensusRejected, wantState: StateCreated, wantDesired: consensusApplied, wantRetryReset: true},
		{name: "update queued: no members: cancelled", state: StateUpdateInProgress, desired: consensusA, applied: &consensusApplied,
			wantState: StateCreated, wantDesired: consensusApplied, wantRetryReset: true},
		{name: "observe-and-clean queued, nothing owed any more: cancelled", state: StateUpdateInProgress, desired: egressUnknown,
			applied: &egressUnknown, votes: []*EgressConfig{&consensusA, nil}, wantState: StateCreated, wantDesired: egressUnknown, wantRetryReset: true},

		// UpdateInProgress, dispatched: desired = agreed if it exists, else the in-flight value.
		{name: "update in flight: new agreement becomes desired", state: StateUpdateInProgress, desired: consensusA, applied: &consensusApplied,
			inFlight: &consensusA, votes: []*EgressConfig{&consensusB}, wantState: StateUpdateInProgress, wantDesired: consensusB},
		{name: "update in flight: agreement back to the applied configuration", state: StateUpdateInProgress, desired: consensusA,
			applied: &consensusApplied, inFlight: &consensusA, votes: []*EgressConfig{&consensusApplied},
			wantState: StateUpdateInProgress, wantDesired: consensusApplied},
		{name: "update in flight: rejected agreement becomes desired", state: StateUpdateInProgress, desired: consensusA, applied: &consensusApplied,
			inFlight: &consensusA, votes: []*EgressConfig{&consensusRejected}, rejected: &consensusRejected,
			wantState: StateUpdateInProgress, wantDesired: consensusRejected},
		{name: "update in flight: agreement lost: in-flight value, no revert", state: StateUpdateInProgress, desired: consensusB,
			applied: &consensusApplied, inFlight: &consensusA, votes: []*EgressConfig{&consensusA, &consensusB},
			wantState: StateUpdateInProgress, wantDesired: consensusA},
		{name: "update in flight: agreement lost with an owed observation: in-flight value", state: StateUpdateInProgress, desired: consensusB,
			applied: &consensusApplied, inFlight: &consensusA, votes: []*EgressConfig{&consensusA, nil}, needsObservation: true,
			wantState: StateUpdateInProgress, wantDesired: consensusA},
		{name: "observe-and-clean in flight: agreement lost: stays unknown", state: StateUpdateInProgress, desired: consensusA,
			applied: &egressUnknown, inFlight: &egressUnknown, votes: []*EgressConfig{&consensusA, &consensusB},
			wantState: StateUpdateInProgress, wantDesired: egressUnknown},

		// NotStarted / CreationInProgress: nothing (the creation configuration is decided at dispatch).
		{name: "creation queued: agreement", state: StateNotStarted, desired: consensusApplied,
			votes: []*EgressConfig{&consensusA, &consensusA}, wantState: StateNotStarted, wantDesired: consensusApplied},
		{name: "creation queued: triggering pod's valid vote", state: StateNotStarted, desired: consensusApplied, triggeredBy: "ns/pod-1",
			votes: []*EgressConfig{&consensusA, &consensusB}, families: dualStackFamilies(), wantState: StateNotStarted, wantDesired: consensusApplied},
		{name: "creation in flight", state: StateCreationInProgress, desired: consensusApplied,
			inFlight: &consensusApplied, votes: []*EgressConfig{&consensusA, &consensusA}, wantState: StateCreationInProgress, wantDesired: consensusApplied},

		// Deletion states: nothing.
		{name: "deletion pending", state: StateDeletionPending, desired: consensusApplied, applied: &consensusApplied,
			votes: []*EgressConfig{&consensusA}, needsObservation: true, wantState: StateDeletionPending, wantDesired: consensusApplied},
		{name: "deletion in progress", state: StateDeletionInProgress, desired: consensusApplied, applied: &consensusApplied,
			votes: []*EgressConfig{&consensusA}, needsObservation: true, wantState: StateDeletionInProgress, wantDesired: consensusApplied},
		{name: "deletion in progress preempting an update in flight", state: StateDeletionInProgress, desired: consensusA,
			applied: &consensusApplied, inFlight: &consensusA, votes: []*EgressConfig{&consensusB},
			wantState: StateDeletionInProgress, wantDesired: consensusA},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dt := newTestDiffTracker()
			dt.egress.mode[identity] = egressModeManaged
			for i, cfg := range tc.votes {
				castEgressVote(dt, identity, fmt.Sprintf("ns/pod-%d", i), cfg, consensusEpoch.Add(time.Duration(i)*time.Minute))
			}
			if tc.rejected != nil {
				dt.egress.rejected[identity] = egressRejection{Config: *tc.rejected, Reason: "rejected by Azure"}
			}
			families := tc.families
			if families == nil {
				families = ipv4Families()
			}
			outbound := func(cfg EgressConfig) *ServiceConfig {
				c := NewOutboundServiceConfig(identity, &OutboundConfig{IPFamilies: slices.Clone(families), Egress: cfg})
				return &c
			}

			quietUntil := time.Now().Add(2 * time.Minute)
			op := &ServiceOperationState{
				ServiceUID: identity,
				Config:     NewOutboundServiceConfig(identity, nil),
				State:      tc.state,
				Egress:     egressOperationState{NeedsObservation: tc.needsObservation, QuietUntil: quietUntil},
			}
			if !tc.nilDesired {
				op.Config = *outbound(tc.desired)
			}
			if tc.state != StateCreated {
				op.RetryCount, op.RetriesExhausted, op.NextRetryAt = 3, true, time.Now().Add(time.Minute)
			}
			if tc.applied != nil {
				op.LastAppliedConfig = outbound(*tc.applied)
			}
			if tc.inFlight != nil {
				op.InFlightConfig = outbound(*tc.inFlight)
			}
			op.TriggeringPodNamespace, op.TriggeringPodName, _ = strings.Cut(tc.triggeredBy, "/")
			if tc.triggeredBy != "" {
				op.TriggeringPodUID = tc.triggeredBy + "-uid"
			}
			dt.pendingServiceOps[identity] = op

			published, lastApplied, inFlight := op.Config.OutboundConfig, op.LastAppliedConfig, op.InFlightConfig
			publishedBefore := published.DeepCopy()
			var lastAppliedBefore, inFlightBefore *ServiceConfig
			if lastApplied != nil {
				c := lastApplied.snapshot()
				lastAppliedBefore = &c
			}
			if inFlight != nil {
				c := inFlight.snapshot()
				inFlightBefore = &c
			}
			retryCount, retriesExhausted, nextRetryAt := op.RetryCount, op.RetriesExhausted, op.NextRetryAt

			dt.mu.Lock()
			dt.evaluateEgressConsensusLocked(identity)
			dt.mu.Unlock()

			assert.Equal(t, tc.wantState, op.State)
			if assert.NotNil(t, op.Config.OutboundConfig) {
				assert.Equal(t, tc.wantDesired, op.Config.OutboundConfig.Egress)
				assert.Equal(t, outboundFamilies(publishedBefore), op.Config.OutboundConfig.IPFamilies, "families are decided at creation")
			}
			assert.Equal(t, publishedBefore, published, "a published desired config must never be modified")
			if tc.state == tc.wantState && tc.state != StateUpdateInProgress {
				assert.Same(t, published, op.Config.OutboundConfig, "nothing to do must leave the desired config alone")
			}
			assert.Same(t, lastApplied, op.LastAppliedConfig)
			assert.Equal(t, lastAppliedBefore, op.LastAppliedConfig, "the evaluation must not touch the applied config")
			assert.Same(t, inFlight, op.InFlightConfig)
			assert.Equal(t, inFlightBefore, op.InFlightConfig, "the evaluation must not touch the in-flight snapshot")
			if op.InFlightConfig != nil {
				assert.NotSame(t, op.InFlightConfig.OutboundConfig, op.Config.OutboundConfig, "the desired config must not share the snapshot")
			}
			assert.Equal(t, tc.wantTrigger, len(dt.serviceUpdaterTrigger) == 1, "the updater is triggered only when an update starts or takes a new agreement")
			assert.Equal(t, tc.needsObservation, op.Egress.NeedsObservation, "only a run settles an owed observation")
			assert.Equal(t, quietUntil, op.Egress.QuietUntil, "the evaluation must keep the quiet period")
			if tc.wantRetryReset {
				assert.Zero(t, op.RetryCount)
				assert.False(t, op.RetriesExhausted)
				assert.Zero(t, op.NextRetryAt)
			} else {
				assert.Equal(t, retryCount, op.RetryCount, "the evaluation must keep the retry budget")
				assert.Equal(t, retriesExhausted, op.RetriesExhausted)
				assert.Equal(t, nextRetryAt, op.NextRetryAt, "the evaluation must keep the backoff")
			}
		})
	}
}

// TestEvaluateEgressConsensusLocked_OnlyManagedIdentities pins that consensus is only evaluated for a
// managed egress identity: a BYO NAT Gateway accepts no configuration, an identity whose mode is not
// resolved waits for its creation, even when it is owed an observation, and an inbound service is never
// touched. A BYO identity owed an observation gets a residue cleanup instead
// (TestEvaluateEgressConsensusLocked_BYOResidue).
func TestEvaluateEgressConsensusLocked_OnlyManagedIdentities(t *testing.T) {
	const identity = "egress-mode"

	cases := []struct {
		name    string
		mode    *egressMode
		inbound bool
		tracked bool
		owed    bool
	}{
		{"BYO NAT Gateway", ptr.To(egressModeBYO), false, true, false},
		{"mode not resolved", nil, false, true, true},
		{"inbound service under a managed key", ptr.To(egressModeManaged), true, true, true},
		{"untracked managed identity", ptr.To(egressModeManaged), false, false, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dt := newTestDiffTracker()
			if tc.mode != nil {
				dt.egress.mode[identity] = *tc.mode
			}
			castEgressVote(dt, identity, "ns/pod", &consensusA, consensusEpoch)
			var op *ServiceOperationState
			if tc.tracked {
				config := NewOutboundServiceConfig(identity, &OutboundConfig{IPFamilies: ipv4Families(), Egress: consensusApplied})
				if tc.inbound {
					config = NewInboundServiceConfig(identity, makeInboundConfig(80))
				}
				applied := config.snapshot()
				op = &ServiceOperationState{
					ServiceUID: identity, Config: config, LastAppliedConfig: &applied, State: StateCreated,
					Egress: egressOperationState{NeedsObservation: tc.owed},
				}
				dt.pendingServiceOps[identity] = op
			}
			before := dt.pendingServiceOps[identity]
			var configBefore ServiceConfig
			if op != nil {
				configBefore = op.Config.snapshot()
			}

			dt.mu.Lock()
			dt.evaluateEgressConsensusLocked(identity)
			dt.mu.Unlock()

			assert.Same(t, before, dt.pendingServiceOps[identity], "the evaluation must not create or replace an operation")
			if op != nil {
				assert.Equal(t, StateCreated, op.State)
				assert.Equal(t, configBefore, op.Config)
			}
			assert.Empty(t, dt.serviceUpdaterTrigger)
		})
	}
}

// TestEvaluateEgressConsensusLocked_TracksItsTrigger pins that an evaluation starting a run uses the
// tracked trigger, so a run started during initialization is accounted for until it is dispatched.
func TestEvaluateEgressConsensusLocked_TracksItsTrigger(t *testing.T) {
	const identity = "egress-tracked-trigger"
	dt := newTestDiffTracker()
	dt.initCompletionChecker = make(chan struct{})
	atomic.StoreInt32(&dt.isInitializing, 1)
	dt.egress.mode[identity] = egressModeManaged
	castEgressVote(dt, identity, "ns/pod", &consensusA, consensusEpoch)
	applied := NewOutboundServiceConfig(identity, &OutboundConfig{IPFamilies: ipv4Families(), Egress: consensusApplied})
	dt.pendingServiceOps[identity] = &ServiceOperationState{
		ServiceUID: identity, Config: applied.snapshot(), LastAppliedConfig: &applied, State: StateCreated,
	}

	dt.mu.Lock()
	dt.evaluateEgressConsensusLocked(identity)
	dt.mu.Unlock()

	assert.Equal(t, StateUpdateInProgress, dt.pendingServiceOps[identity].State)
	assert.Len(t, dt.serviceUpdaterTrigger, 1)
	assert.Equal(t, int32(1), atomic.LoadInt32(&dt.pendingUpdaterTriggers), "the trigger must be tracked during initialization")
}

// TestEvaluateEgressConsensusLocked_NeverModifiesAPublishedConfig pins under -race that a reader of the
// published desired config and of a dispatched snapshot (as a run reads its snapshot, without dt.mu)
// never races the evaluations that follow, because an evaluation replaces the desired config.
func TestEvaluateEgressConsensusLocked_NeverModifiesAPublishedConfig(t *testing.T) {
	const identity = "egress-published"
	dt := newTestDiffTracker()
	dt.egress.mode[identity] = egressModeManaged
	applied := NewOutboundServiceConfig(identity, &OutboundConfig{IPFamilies: dualStackFamilies(), Egress: consensusApplied})
	op := &ServiceOperationState{ServiceUID: identity, Config: applied.snapshot(), LastAppliedConfig: &applied, State: StateCreated}
	dt.pendingServiceOps[identity] = op
	published := op.Config.OutboundConfig
	dispatched := op.Config.snapshot()

	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
			}
			_ = published.Egress.Summary() + dispatched.OutboundConfig.Egress.Summary()
			_ = slices.Clone(published.IPFamilies)
			_ = slices.Clone(dispatched.OutboundConfig.IPFamilies)
		}
	}()

	votes := []EgressConfig{consensusA, consensusB}
	for i := 0; i < 300; i++ {
		dt.mu.Lock()
		castEgressVote(dt, identity, "ns/pod", &votes[i%2], consensusEpoch)
		switch i % 3 {
		case 0:
			op.State, op.InFlightConfig = StateCreated, nil
		case 1:
			op.State, op.InFlightConfig = StateUpdateInProgress, nil
		case 2:
			op.State, op.InFlightConfig = StateUpdateInProgress, &dispatched
		}
		dt.evaluateEgressConsensusLocked(identity)
		dt.mu.Unlock()
	}
	close(stop)
	<-done

	assert.Equal(t, consensusApplied, published.Egress, "the published desired config must never be modified")
	assert.Equal(t, dualStackFamilies(), published.IPFamilies)
	assert.Equal(t, consensusApplied, dispatched.OutboundConfig.Egress, "a dispatched snapshot must never be modified")
	assert.Equal(t, consensusApplied, applied.OutboundConfig.Egress, "the applied config must never be modified")
}

// TestCheckInitializationComplete_EgressConfigurationRuns pins that an update of an egress identity whose
// NAT Gateway already serves egress (apply or observe-and-clean of a managed one, residue cleanup of a BYO
// one) never holds initialization open, dispatched or not. Creations, deletions and every other update still do.
func TestCheckInitializationComplete_EgressConfigurationRuns(t *testing.T) {
	const uid = "egress-init"

	cases := []struct {
		name         string
		inbound      bool
		mode         egressMode
		state        ResourceState
		desired      EgressConfig
		inFlight     bool
		wantComplete bool
	}{
		{"managed update queued", false, egressModeManaged, StateUpdateInProgress, consensusA, false, true},
		{"managed update in flight", false, egressModeManaged, StateUpdateInProgress, consensusA, true, true},
		{"managed observe-and-clean", false, egressModeManaged, StateUpdateInProgress, egressUnknown, false, true},
		{"managed creation queued", false, egressModeManaged, StateNotStarted, consensusA, false, false},
		{"managed creation in flight", false, egressModeManaged, StateCreationInProgress, consensusA, true, false},
		{"managed deletion pending", false, egressModeManaged, StateDeletionPending, consensusA, false, false},
		{"managed deletion in progress", false, egressModeManaged, StateDeletionInProgress, consensusA, false, false},
		{"BYO residue cleanup", false, egressModeBYO, StateUpdateInProgress, egressUnknown, false, true},
		{"BYO residue cleanup in flight", false, egressModeBYO, StateUpdateInProgress, egressUnknown, true, true},
		{"BYO creation queued", false, egressModeBYO, StateNotStarted, consensusA, false, false},
		{"BYO deletion in progress", false, egressModeBYO, StateDeletionInProgress, consensusA, false, false},
		{"update of an identity whose mode is not resolved", false, egressModeUnknown, StateUpdateInProgress, consensusA, false, false},
		{"inbound update under a managed key", true, egressModeManaged, StateUpdateInProgress, consensusA, false, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dt := newTestDiffTracker()
			atomic.StoreInt32(&dt.isInitializing, 1)
			dt.initCompletionChecker = make(chan struct{})
			dt.egress.mode[uid] = tc.mode
			config := NewOutboundServiceConfig(uid, &OutboundConfig{IPFamilies: ipv4Families(), Egress: tc.desired})
			if tc.inbound {
				config = NewInboundServiceConfig(uid, makeInboundConfig(80))
			}
			op := &ServiceOperationState{ServiceUID: uid, Config: config, State: tc.state}
			if tc.inFlight {
				snapshot := config.snapshot()
				op.InFlightConfig = &snapshot
			}
			dt.pendingServiceOps[uid] = op

			dt.checkInitializationComplete()

			select {
			case <-dt.initCompletionChecker:
				assert.True(t, tc.wantComplete, "initialization must not complete while this operation is pending")
			default:
				assert.False(t, tc.wantComplete, "an egress configuration run must not hold initialization open")
			}
		})
	}
}

// TestRecreateAfterDeletion_RebuildsOutboundConfig pins that an egress identity re-created after its
// deletion is created as a new identity is: with the cluster's families, and dispatched with its creation
// configuration. The deletion of an identity known only from Azure carries no outbound config, which re-created
// its NAT Gateway IPv4-only on a dual-stack cluster. The quiet period survives the re-creation.
func TestRecreateAfterDeletion_RebuildsOutboundConfig(t *testing.T) {
	const identity = "egress-recreated"
	defaultIPv4 := defaultEgressConfig(egressParseContext{})
	defaultDual := defaultEgressConfig(egressParseContext{WantsIPv6: true})

	cases := []struct {
		name         string
		dualStack    bool
		config       *OutboundConfig // the deletion's outbound config
		triggeredBy  string
		votes        []*EgressConfig // pod-i is created i minutes after the epoch; nil is an invalid vote
		wantFamilies []string
		wantEgress   EgressConfig
	}{
		{name: "known only from Azure, dual-stack cluster", dualStack: true, wantFamilies: dualStackFamilies(), wantEgress: defaultDual},
		{name: "known only from Azure, IPv4 cluster", wantFamilies: ipv4Families(), wantEgress: defaultIPv4},
		{name: "configured identity starts afresh", dualStack: true,
			config:       &OutboundConfig{IPFamilies: ipv4Families(), Egress: consensusB},
			wantFamilies: dualStackFamilies(), wantEgress: defaultDual},
		{name: "agreed configuration", votes: []*EgressConfig{&consensusA, &consensusA}, wantFamilies: ipv4Families(), wantEgress: consensusA},
		{name: "creator gone: oldest member's valid vote", triggeredBy: "ns/creator",
			config: &OutboundConfig{IPFamilies: ipv4Families(), Egress: consensusB},
			votes:  []*EgressConfig{&consensusA, &consensusB}, wantFamilies: ipv4Families(), wantEgress: consensusA},
		{name: "creator a member again: oldest member's valid vote, not the creator's", triggeredBy: "ns/pod-1",
			config: &OutboundConfig{IPFamilies: ipv4Families(), Egress: consensusB},
			votes:  []*EgressConfig{&consensusA, &consensusB}, wantFamilies: ipv4Families(), wantEgress: consensusA},
		{name: "invalid oldest member: default", votes: []*EgressConfig{nil, &consensusB}, dualStack: true,
			wantFamilies: dualStackFamilies(), wantEgress: defaultDual},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dt := newTestDiffTracker()
			if tc.dualStack {
				dt.clusterHasIPv6.Store(true)
			}
			for i, cfg := range tc.votes {
				castEgressVote(dt, identity, fmt.Sprintf("ns/pod-%d", i), cfg, consensusEpoch.Add(time.Duration(i)*time.Minute))
			}
			quietUntil := time.Now().Add(2 * time.Minute)
			op := &ServiceOperationState{
				ServiceUID: identity,
				Config:     NewOutboundServiceConfig(identity, tc.config),
				State:      StateDeletionInProgress,
				Egress:     egressOperationState{QuietUntil: quietUntil},
			}
			op.TriggeringPodNamespace, op.TriggeringPodName, _ = strings.Cut(tc.triggeredBy, "/")
			if tc.triggeredBy != "" {
				op.TriggeringPodUID = tc.triggeredBy + "-uid"
			}
			dt.pendingServiceOps[identity] = op
			dt.pendingPods[identity] = []PendingPodUpdate{{PodKey: "ns/pod-0", PodUID: "ns/pod-0-uid", Location: "10.0.0.1", Address: "10.244.0.5"}}
			configBefore := tc.config.DeepCopy()

			dt.OnServiceCreationComplete(identity, true, nil)

			assert.Equal(t, StateNotStarted, op.State, "a buffered pod must re-create the identity")
			if assert.NotNil(t, op.Config.OutboundConfig) {
				assert.Equal(t, tc.wantFamilies, op.Config.OutboundConfig.IPFamilies)
			}
			assert.False(t, op.Config.IsInbound)
			assert.Equal(t, identity, op.Config.UID)
			if tc.config != nil {
				assert.NotSame(t, tc.config, op.Config.OutboundConfig, "a re-creation must publish a new outbound config")
				assert.Equal(t, configBefore, tc.config, "the published outbound config must never be modified")
			}
			assert.Nil(t, op.LastAppliedConfig)
			assert.Empty(t, op.TriggeringPodUID, "the first creator must not seed the re-created identity")
			assert.Equal(t, quietUntil, op.Egress.QuietUntil, "the quiet period must survive a re-creation")
			assert.Len(t, dt.serviceUpdaterTrigger, 1)

			op.Egress.QuietUntil = time.Time{} // the quiet period has passed
			dispatchWithoutRunning(t, dt)
			if assert.NotNil(t, op.InFlightConfig, "control: the creation is dispatched") {
				assert.Equal(t, tc.wantFamilies, outboundFamilies(op.InFlightConfig.OutboundConfig))
				assert.Equal(t, tc.wantEgress, outboundEgress(op.InFlightConfig.OutboundConfig), "the creation configuration")
			}
		})
	}
}

// TestRecreateAfterDeletion_InboundKeepsItsConfig is the control of the outbound rebuild: an inbound
// re-creation replays the config its UpdateService recorded.
func TestRecreateAfterDeletion_InboundKeepsItsConfig(t *testing.T) {
	const uid = "svc-recreated"
	dt := newTestDiffTracker()
	dt.clusterHasIPv6.Store(true)
	config := NewInboundServiceConfig(uid, makeInboundConfig(8080))
	dt.pendingServiceOps[uid] = &ServiceOperationState{
		ServiceUID: uid, Config: config, State: StateDeletionInProgress, RecreateAfterDeletion: true,
	}

	dt.OnServiceCreationComplete(uid, true, nil)

	op := dt.pendingServiceOps[uid]
	assert.Equal(t, StateNotStarted, op.State)
	assert.Equal(t, config, op.Config)
}

// joinWithAnnotations makes the membership pod name a member of identity with the egress annotations ann, through the
// engine call of the pod handler, which parses its vote under dt.mu for the IP families of the identity.
func joinWithAnnotations(dt *DiffTracker, identity, name string, ann map[string]string) {
	pod := membershipPod(name, identity, ann)
	dt.addEgressPod(identity, egressPodRefOf(pod), memberAccepted(memberPod(name)), egressVoteParser(pod))
}

// TestRecreateAfterDeletion_ReparsesTheVotesForItsFamilies pins the re-creation of an identity whose votes were parsed
// for other IP families: a pod with an IPv4 setting joins while the deletion of the identity is in progress on an IPv4
// cluster, which gains IPv6 before the deletion completes. The dual-stack re-creation counts the pod's vote parsed
// again for both families, so it is created, and dispatched, with the IPv4 setting and an IPv6 address; the pod, which
// did not change, is owed no event. Without a change of families the vote is left as it is. A second pod whose IPv6
// setting, invalid on the IPv4 cluster, becomes valid without agreeing with the first is told of the conflict, and the
// identity is created with the vote of the first, its oldest member.
func TestRecreateAfterDeletion_ReparsesTheVotesForItsFamilies(t *testing.T) {
	for _, tc := range []struct {
		name       string
		gainsIPv6  bool
		p2         map[string]string // the egress annotations of a second member, which joins after p1; nil: none joins
		wantEvents []string
	}{
		{name: "the cluster gains IPv6", gainsIPv6: true},
		{name: "the cluster stays IPv4"},
		{name: "the cluster gains IPv6: the IPv6 setting of a second member becomes valid without agreement", gainsIPv6: true,
			p2:         map[string]string{consts.PodAnnotationServiceEgressGatewayPublicIPCountIPv6: "2"},
			wantEvents: []string{memberEvent(consts.ServiceEgressGatewayReasonConfigConflict, "p2")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dt := newTestDiffTracker()
			dt.config = testConfig()
			op := trackManagedEgressIdentity(dt, memberIdentity, consensusApplied)
			op.State, op.InFlightKind = StateDeletionInProgress, operationDelete
			joinWithAnnotations(dt, memberIdentity, "p1", annotationsA())
			if tc.p2 != nil {
				joinWithAnnotations(dt, memberIdentity, "p2", tc.p2)
			}
			cast, _ := dt.egress.votes.get("ns/p1")
			if !assert.Equal(t, consensusA, cast.Config, "control: the vote is parsed for the IPv4 cluster") {
				return
			}
			settleMembership(dt)
			families := ipv4Families()
			if tc.gainsIPv6 {
				dt.clusterHasIPv6.Store(true)
				families = dualStackFamilies()
			}

			dt.onServiceOperationComplete(memberIdentity, true, nil, &egressOutcome{Mode: egressModeManaged, Swept: true})

			if !assert.Equal(t, StateNotStarted, op.State, "control: the buffered pod re-creates the identity") {
				return
			}
			pod := membershipPod("p1", memberIdentity, annotationsA())
			want := egressPodRefOf(pod).voteFor(memberIdentity, parseEgressVote(pod, dt.egressParseContextFor(families)))
			vote, _ := dt.egress.votes.get("ns/p1")
			assert.Equal(t, want, vote, "the vote counts for the families of the re-creation")
			if tc.gainsIPv6 {
				assert.Equal(t, consensusA.IPv4, vote.Config.IPv4, "the IPv4 setting")
				assert.Equal(t, egressFamilyConfig{ManagedPublicIPs: 1}, vote.Config.IPv6, "and an IPv6 address")
			} else {
				assert.Equal(t, cast, vote, "nothing is parsed again")
			}
			assert.Equal(t, families, op.Config.OutboundConfig.IPFamilies)
			assert.Equal(t, tc.wantEvents, takeEgressEvents(dt), "only a pod whose vote became valid or invalid is owed an event")
			assertEgressInvariants(t, dt)

			dispatchWithoutRunning(t, dt)
			if assert.NotNil(t, op.InFlightConfig, "control: the creation is dispatched") {
				assert.Equal(t, want.Config, outboundEgress(op.InFlightConfig.OutboundConfig), "the dispatch snapshot")
				assert.Equal(t, families, outboundFamilies(op.InFlightConfig.OutboundConfig))
			}
		})
	}
}

// TestRecreateAfterDeletion_AppliesTheVoteParsedForItsFamilies runs the dual-stack re-creation of
// TestRecreateAfterDeletion_ReparsesTheVotesForItsFamilies end to end: the creation applies the pod's IPv4 setting with
// an IPv6 address, rejects nothing and announces the configuration applied.
func TestRecreateAfterDeletion_AppliesTheVoteParsedForItsFamilies(t *testing.T) {
	h := newEgressRunsHarness(t, testConfig())
	dt := h.dt
	dt.mu.Lock()
	dt.egress.mode[egressRunsID] = egressModeManaged
	dt.pendingServiceOps[egressRunsID] = &ServiceOperationState{ServiceUID: egressRunsID, State: StateDeletionInProgress,
		InFlightKind: operationDelete, Config: NewOutboundServiceConfig(egressRunsID,
			&OutboundConfig{IPFamilies: ipv4Families(), Egress: defaultEgressConfig(dt.egressParseContextFor(ipv4Families()))})}
	dt.mu.Unlock()
	joinWithAnnotations(dt, egressRunsID, "p1", annotationsA())
	takeEgressEvents(dt)
	dt.clusterHasIPv6.Store(true)
	dt.onServiceOperationComplete(egressRunsID, true, nil, &egressOutcome{Mode: egressModeManaged, Swept: true})
	assert.Empty(t, takeEgressEvents(dt), "the pod did not change")

	h.dispatch()

	want, _, err := parseEgressConfig(annotationsA(), dt.egressParseContextFor(dualStackFamilies()))
	assert.NoError(t, err)
	op := h.op()
	if !assert.NotNil(t, op) {
		return
	}
	assert.Equal(t, StateCreated, op.State)
	assert.Equal(t, dualStackFamilies(), outboundFamilies(op.Config.OutboundConfig))
	assert.Equal(t, &want, appliedEgressOf(op), "the IPv4 setting is applied, with an IPv6 address")
	assert.Empty(t, dt.egress.rejected, "no terminal rejection")
	assert.Empty(t, dt.egress.warned, "no retryable rejection")
	assert.Equal(t, []string{consts.ServiceEgressGatewayReasonConfigApplied + " ns/p1"}, takeEgressEvents(dt))
}

// TestAddPod_RevivedManagedIdentityHasUnknownAppliedConfig pins that a managed identity whose deletion is
// cancelled before this process applied anything to it (it was known only from Azure) records egressUnknown
// as applied, so LastAppliedConfig is set as for every managed identity, and owes an observation of its NAT
// Gateway. Other revivals are left as they are.
func TestAddPod_RevivedManagedIdentityHasUnknownAppliedConfig(t *testing.T) {
	const identity = "egress-revived"

	cases := []struct {
		name         string
		mode         egressMode
		dualStack    bool
		config       *OutboundConfig
		applied      bool
		wantAdopted  bool
		wantFamilies []string
	}{
		{name: "managed, known only from Azure, dual-stack cluster", mode: egressModeManaged, dualStack: true,
			wantAdopted: true, wantFamilies: dualStackFamilies()},
		{name: "managed, known only from Azure, IPv4 cluster", mode: egressModeManaged,
			wantAdopted: true, wantFamilies: ipv4Families()},
		{name: "managed with a desired config but nothing applied", mode: egressModeManaged, dualStack: true,
			config:      &OutboundConfig{IPFamilies: ipv4Families(), Egress: consensusA},
			wantAdopted: true, wantFamilies: ipv4Families()},
		{name: "managed with an applied config", mode: egressModeManaged,
			config: &OutboundConfig{IPFamilies: ipv4Families(), Egress: consensusA}, applied: true},
		{name: "BYO NAT Gateway", mode: egressModeBYO},
		{name: "mode not resolved", mode: egressModeUnknown},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dt := newTestDiffTracker()
			if tc.dualStack {
				dt.clusterHasIPv6.Store(true)
			}
			dt.egress.mode[identity] = tc.mode
			dt.NRPResources.NATGateways.Insert(identity)
			op := &ServiceOperationState{ServiceUID: identity, Config: NewOutboundServiceConfig(identity, tc.config), State: StateDeletionPending}
			if tc.applied {
				applied := op.Config.snapshot()
				op.LastAppliedConfig = &applied
			}
			// Left by an earlier life of the identity in this process: an adopted NAT Gateway is announced against nothing.
			op.Egress.LastKnownApplied, op.Egress.UnclearWrites = &consensusApplied, []EgressConfig{consensusB}
			dt.pendingServiceOps[identity] = op
			dt.pendingServiceDeletions[identity] = &PendingServiceDeletion{ServiceUID: identity}
			configBefore, lastAppliedBefore := op.Config, op.LastAppliedConfig
			publishedBefore := tc.config.DeepCopy()

			dt.AddPod(identity, "ns/pod", "10.0.0.1", "10.244.0.5")

			assert.Equal(t, StateCreated, op.State, "a pod arriving before the deletion is dispatched revives the identity")
			if tc.config != nil {
				assert.Equal(t, publishedBefore, tc.config, "the published outbound config must never be modified")
			}
			if !tc.wantAdopted {
				assert.Equal(t, configBefore, op.Config)
				assert.Same(t, lastAppliedBefore, op.LastAppliedConfig)
				assert.False(t, op.Egress.NeedsObservation)
				return
			}
			assert.True(t, op.Egress.NeedsObservation, "a NAT Gateway nobody applied must be observed")
			assert.Equal(t, uint64(1), op.Egress.ObservationGen, "the obligation must be a new generation")
			assert.Nil(t, op.Egress.LastKnownApplied, "nothing is known of what an adopted NAT Gateway had before")
			assert.Nil(t, op.Egress.UnclearWrites)
			if !assert.NotNil(t, op.Config.OutboundConfig) || !assert.NotNil(t, op.LastAppliedConfig) ||
				!assert.NotNil(t, op.LastAppliedConfig.OutboundConfig) {
				return
			}
			assert.Equal(t, NewOutboundServiceConfig(identity, &OutboundConfig{IPFamilies: tc.wantFamilies, Egress: egressUnknown}), *op.LastAppliedConfig)
			assert.Equal(t, *op.LastAppliedConfig, op.Config, "the desired config of a Created identity is the applied one")
			assert.NotSame(t, op.Config.OutboundConfig, op.LastAppliedConfig.OutboundConfig, "the applied config must be a copy")
			op.Config.OutboundConfig.IPFamilies[0] = "mutated"
			assert.Equal(t, tc.wantFamilies, op.LastAppliedConfig.OutboundConfig.IPFamilies, "the applied config must share nothing")
		})
	}
}

// TestAddPod_NewIdentityIsDispatchedWithItsCreationConfig pins that a new egress identity is tracked with the
// cluster's families and dispatched with its creation configuration, seeded by the pod that creates it.
func TestAddPod_NewIdentityIsDispatchedWithItsCreationConfig(t *testing.T) {
	const identity, creator = "egress-new", "ns/creator"
	older, newer := consensusEpoch, consensusEpoch.Add(time.Minute)

	cases := []struct {
		name         string
		dualStack    bool
		creatorVote  *EgressConfig // nil with creatorVotes: an invalid vote
		creatorVotes bool
		replaced     bool          // the creator's vote was cast by a same-name pod with another UID
		olderVote    *EgressConfig // the vote of an older member; nil: none
		wantFamilies []string
		wantEgress   EgressConfig
	}{
		{name: "no votes, IPv4 cluster", wantFamilies: ipv4Families(), wantEgress: defaultEgressConfig(egressParseContext{})},
		{name: "no votes, dual-stack cluster", dualStack: true, wantFamilies: dualStackFamilies(),
			wantEgress: defaultEgressConfig(egressParseContext{WantsIPv6: true})},
		{name: "the creating pod seeds it", creatorVote: &consensusB, creatorVotes: true, olderVote: &consensusA,
			wantFamilies: ipv4Families(), wantEgress: consensusB},
		{name: "an invalid vote of the creating pod gives the default", creatorVotes: true, olderVote: &consensusA,
			wantFamilies: ipv4Families(), wantEgress: defaultEgressConfig(egressParseContext{})},
		{name: "a same-name pod with another UID does not seed it", creatorVote: &consensusB, creatorVotes: true, replaced: true,
			olderVote: &consensusA, wantFamilies: ipv4Families(), wantEgress: consensusA},
		{name: "agreement", creatorVote: &consensusA, creatorVotes: true, olderVote: &consensusA,
			wantFamilies: ipv4Families(), wantEgress: consensusA},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dt := newTestDiffTracker()
			if tc.dualStack {
				dt.clusterHasIPv6.Store(true)
			}
			if tc.olderVote != nil {
				castEgressVote(dt, identity, "ns/older", tc.olderVote, older)
			}
			if tc.creatorVotes {
				castEgressVote(dt, identity, creator, tc.creatorVote, newer)
			}
			creatorUID := creator + "-uid"
			if tc.replaced {
				creatorUID = "replacement-uid"
			}

			dt.AddPodWithUID(identity, creator, creatorUID, "10.0.0.1", "10.244.0.5")

			op := dt.pendingServiceOps[identity]
			if !assert.NotNil(t, op) || !assert.NotNil(t, op.Config.OutboundConfig) {
				return
			}
			assert.Equal(t, StateNotStarted, op.State)
			assert.Equal(t, "ns", op.TriggeringPodNamespace)
			assert.Equal(t, "creator", op.TriggeringPodName)
			assert.Equal(t, creatorUID, op.TriggeringPodUID)
			assert.Equal(t, tc.wantFamilies, op.Config.OutboundConfig.IPFamilies)
			assert.Len(t, dt.serviceUpdaterTrigger, 1)

			dispatchWithoutRunning(t, dt)
			if assert.NotNil(t, op.InFlightConfig, "control: the creation is dispatched") {
				assert.Equal(t, tc.wantFamilies, outboundFamilies(op.InFlightConfig.OutboundConfig))
				assert.Equal(t, tc.wantEgress, outboundEgress(op.InFlightConfig.OutboundConfig))
			}
		})
	}
}

// TestProcessBatch_CreationTakesTheCreationConfigOfTheMembers pins that a creation is dispatched with the creation
// configuration of the identity's members at dispatch, also when no evaluation followed their votes because the
// identity's mode is not resolved: a creation seeded by its creator with A fails before its NAT Gateway is resolved,
// the creator leaves and the remaining members agree on B, and the retry creates B, not A. An inbound creation is
// dispatched with its own config.
func TestProcessBatch_CreationTakesTheCreationConfigOfTheMembers(t *testing.T) {
	t.Run("retry after a failure before the mode is resolved", func(t *testing.T) {
		creator, other := memberPod("p2"), memberPod("p3")
		dt := newTestDiffTracker()
		joinMember(dt, memberIdentity, creator, &consensusA)
		joinMember(dt, memberIdentity, other, &consensusB)
		op := dt.pendingServiceOps[memberIdentity]
		dispatchWithoutRunning(t, dt)
		if !assert.NotNil(t, op.InFlightConfig) {
			return
		}
		assert.Equal(t, consensusA, outboundEgress(op.InFlightConfig.OutboundConfig), "control: the creator seeds the creation")

		dt.onServiceOperationComplete(memberIdentity, false, errors.New("resolving the NAT Gateway"), &egressOutcome{})
		assert.Equal(t, StateNotStarted, op.State, "control: the creation is retried")
		assert.Equal(t, egressModeUnknown, dt.egress.mode[memberIdentity], "control: the mode is not resolved")
		dt.DeletePod(memberIdentity, memberNode, []string{memberIP(creator)}, creator.Namespace, creator.Name, string(creator.UID))
		joinMember(dt, memberIdentity, memberPod("p4"), &consensusB)

		op.NextRetryAt = time.Time{}
		dispatchWithoutRunning(t, dt)
		if assert.NotNil(t, op.InFlightConfig, "the retry is dispatched") {
			assert.Equal(t, consensusB, outboundEgress(op.InFlightConfig.OutboundConfig), "the members' agreement, not the creator's vote")
			assert.Equal(t, ipv4Families(), outboundFamilies(op.InFlightConfig.OutboundConfig), "the families decided at creation")
		}
	})

	t.Run("inbound creation", func(t *testing.T) {
		const uid = "inbound-create"
		dt := newTestDiffTracker()
		castEgressVote(dt, uid, "ns/pod", &consensusA, consensusEpoch)
		config := NewInboundServiceConfig(uid, makeInboundConfig(80))
		op := &ServiceOperationState{ServiceUID: uid, Config: config, State: StateNotStarted}
		dt.pendingServiceOps[uid] = op

		dispatchWithoutRunning(t, dt)
		if assert.NotNil(t, op.InFlightConfig) {
			assert.Nil(t, op.InFlightConfig.OutboundConfig, "an inbound creation gets no egress configuration")
			assert.Equal(t, config.InboundConfig, op.InFlightConfig.InboundConfig)
		}
		assert.Nil(t, op.Config.OutboundConfig)
	})
}

// ================================================================================================
// Egress completion: the outcome of an outbound run (4.5)
// ================================================================================================

const egressRunID = "egress-run"

// egressRun describes the operation of an egress identity whose run completes. The run was dispatched at
// observation generation 7.
type egressRun struct {
	state    ResourceState // at completion; StateDeletionPending with an in-flight config is a preempted run
	mode     egressMode
	desired  EgressConfig
	applied  *EgressConfig // nil: nothing applied, the identity was never created
	inFlight *EgressConfig // nil: no in-flight snapshot, as for a deletion run
	wrote    bool          // Egress.MutationStarted: the run started writing
	owed     bool          // Egress.NeedsObservation when the run was dispatched
	raised   bool          // an observation obligation was raised while the run was in flight
	baseline bool          // Egress.StartupBaseline
	retries  int
	// lastKnown and unclear are Egress.LastKnownApplied and Egress.UnclearWrites, kept while applied is egressUnknown.
	lastKnown *EgressConfig
	unclear   []EgressConfig
	votes     []*EgressConfig // ns/pod-i, created i minutes after the epoch; nil is an invalid vote
}

// egressRunConfig returns a new outbound config of the identity of egressRun with configuration cfg.
func egressRunConfig(cfg EgressConfig) *ServiceConfig {
	c := NewOutboundServiceConfig(egressRunID, &OutboundConfig{IPFamilies: ipv4Families(), Egress: cfg})
	return &c
}

// trackEgressRun tracks the operation r describes on dt and returns it.
func trackEgressRun(dt *DiffTracker, r egressRun) *ServiceOperationState {
	dt.egress.mode[egressRunID] = r.mode
	for i, cfg := range r.votes {
		castEgressVote(dt, egressRunID, fmt.Sprintf("ns/pod-%d", i), cfg, consensusEpoch.Add(time.Duration(i)*time.Minute))
	}
	op := &ServiceOperationState{
		ServiceUID: egressRunID,
		Config:     *egressRunConfig(r.desired),
		State:      r.state,
		RetryCount: r.retries,
		Egress: egressOperationState{
			MutationStarted: r.wrote, NeedsObservation: r.owed, ObservationGen: 7, DispatchedObservationGen: 7,
			StartupBaseline: r.baseline, LastKnownApplied: r.lastKnown, UnclearWrites: r.unclear,
		},
	}
	if r.raised {
		op.Egress.requireObservation()
	}
	if r.applied != nil {
		op.LastAppliedConfig = egressRunConfig(*r.applied)
	}
	if r.inFlight != nil {
		op.InFlightConfig = egressRunConfig(*r.inFlight)
	}
	dt.pendingServiceOps[egressRunID] = op
	return op
}

// takeEgressEvents returns the egress events queued on dt, which has no recorder, as "reason ns/name" in the
// order they were raised, and empties the queue.
func takeEgressEvents(dt *DiffTracker) []string {
	dt.mu.Lock()
	defer dt.mu.Unlock()
	var events []string
	for _, event := range dt.egress.pendingEvents {
		events = append(events, event.reason+" "+event.pod.Namespace+"/"+event.pod.Name)
	}
	dt.egress.pendingEvents = nil
	return events
}

// appliedEgressOf returns the applied configuration of op; nil when nothing was applied.
func appliedEgressOf(op *ServiceOperationState) *EgressConfig {
	if op.LastAppliedConfig == nil || op.LastAppliedConfig.OutboundConfig == nil {
		return nil
	}
	applied := op.LastAppliedConfig.OutboundConfig.Egress
	return &applied
}

// dispatchWithoutRunning lets the dispatcher pick up the operations of dt, as it does before a run starts,
// without running them: their context is done and the semaphore never frees. The trigger the finished
// workers send is drained.
func dispatchWithoutRunning(t *testing.T, dt *DiffTracker) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	su := newTestServiceUpdater(dt)
	su.ctx = ctx
	su.semaphore = make(chan struct{})
	defer stopRetryTimers(su)
	su.processBatch()
	su.wg.Wait()
	drainServiceUpdaterTrigger(dt)
}

// drainServiceUpdaterTrigger empties the updater trigger of dt.
func drainServiceUpdaterTrigger(dt *DiffTracker) {
	select {
	case <-dt.serviceUpdaterTrigger:
	default:
	}
}

// assertBackoff checks that op waits out a retry backoff, or that it does not.
func assertBackoff(t *testing.T, op *ServiceOperationState, want bool) {
	t.Helper()
	if want {
		assert.True(t, op.NextRetryAt.After(time.Now()), "the next run must wait out a retry backoff")
		return
	}
	assert.False(t, op.NextRetryAt.After(time.Now()), "the next run must not wait")
}

// TestOnOutboundComplete_UnknownCommit pins rule 0: a run that ended with a write Azure may still apply
// (CommitUnknown), or panicked after its first write (no outcome, Egress.MutationStarted set), leaves the
// applied configuration unknown, owes an observation with a new generation and holds the identity's runs back
// for the quiet period. A run that failed before the identity was ever created records nothing applied. Every
// completion clears the write-ahead flag.
func TestOnOutboundComplete_UnknownCommit(t *testing.T) {
	const managed = egressModeManaged
	update := egressRun{state: StateUpdateInProgress, mode: managed, desired: consensusA, applied: &consensusApplied, inFlight: &consensusA, wrote: true}
	creation := egressRun{state: StateCreationInProgress, desired: consensusA, inFlight: &consensusA, wrote: true}
	deletion := egressRun{state: StateDeletionInProgress, mode: managed, desired: consensusApplied, applied: &consensusApplied, wrote: true}
	unknownCommit := &egressOutcome{Mode: managed, CommitUnknown: true}
	without := func(r egressRun, change func(*egressRun)) egressRun {
		change(&r)
		return r
	}

	cases := []struct {
		name        string
		run         egressRun
		success     bool
		outcome     *egressOutcome
		wantApplied *EgressConfig
		wantUnknown bool // owed, new generation, quiet period
		wantState   ResourceState
	}{
		{name: "panic after the first write of an update", run: update,
			wantApplied: &egressUnknown, wantUnknown: true, wantState: StateUpdateInProgress},
		{name: "panic before any write of an update", run: without(update, func(r *egressRun) { r.wrote = false }),
			wantApplied: &consensusApplied, wantState: StateCreated},
		{name: "busy update records nothing", run: without(update, func(r *egressRun) { r.wrote = false }),
			outcome: &egressOutcome{Mode: managed}, wantApplied: &consensusApplied, wantState: StateCreated},
		{name: "update ended commit unknown", run: update, outcome: unknownCommit,
			wantApplied: &egressUnknown, wantUnknown: true, wantState: StateUpdateInProgress},
		{name: "update ended commit unknown after its NAT Gateway PUT applied", run: update,
			outcome:     &egressOutcome{Mode: managed, Applied: &consensusA, CommitUnknown: true},
			wantApplied: &consensusA, wantUnknown: true, wantState: StateUpdateInProgress},
		{name: "panic after the first write of a creation", run: creation,
			wantUnknown: true, wantState: StateNotStarted},
		{name: "creation ended commit unknown", run: creation, outcome: unknownCommit,
			wantUnknown: true, wantState: StateNotStarted},
		{name: "deletion ended commit unknown", run: deletion, outcome: unknownCommit,
			wantApplied: &egressUnknown, wantUnknown: true, wantState: StateDeletionInProgress},
		{name: "deletion of an identity known only from Azure ended commit unknown",
			run: without(deletion, func(r *egressRun) { r.applied = nil }), outcome: unknownCommit,
			wantUnknown: true, wantState: StateDeletionInProgress},
		{name: "conclusive update", run: update, success: true,
			outcome:     &egressOutcome{Mode: managed, Applied: &consensusA, Swept: true},
			wantApplied: &consensusA, wantState: StateCreated},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dt := newTestDiffTracker()
			op := trackEgressRun(dt, tc.run)
			var err error
			if !tc.success {
				err = errors.New("run failed")
			}

			before := time.Now()
			dt.onServiceOperationComplete(egressRunID, tc.success, err, tc.outcome)
			after := time.Now()

			assert.False(t, op.Egress.MutationStarted, "every completion clears the write-ahead flag")
			assert.Equal(t, tc.wantApplied, appliedEgressOf(op))
			assert.Equal(t, tc.wantUnknown, op.Egress.NeedsObservation)
			if tc.wantUnknown {
				assert.Equal(t, uint64(8), op.Egress.ObservationGen, "an unknown write is a new obligation")
				assert.False(t, op.Egress.QuietUntil.Before(before.Add(getEgressSettleDelay())), "the quiet period must last the settle delay")
				assert.False(t, op.Egress.QuietUntil.After(after.Add(getEgressSettleDelay())))
			} else {
				assert.Equal(t, uint64(7), op.Egress.ObservationGen)
				assert.True(t, op.Egress.QuietUntil.IsZero(), "a conclusive run starts no quiet period")
			}
			if op.LastAppliedConfig != nil {
				assert.Equal(t, ipv4Families(), op.LastAppliedConfig.OutboundConfig.IPFamilies, "the applied config keeps the families")
			}
			assert.Equal(t, tc.wantState, op.State)
			assert.Nil(t, op.InFlightConfig)
			assert.False(t, op.CreationFailedTerminal)
		})
	}
}

// TestOnOutboundComplete_RecordsTheMode pins rule 1: the mode a run resolved is recorded and a run that
// resolved none keeps the recorded one. An identity found to be BYO tells its pods with egress annotations,
// valid or not, once, that they are invalid there; that invalidity is derived from the mode, so the votes
// are left as they were cast.
func TestOnOutboundComplete_RecordsTheMode(t *testing.T) {
	invalid := consts.ServiceEgressGatewayReasonInvalidConfig
	cases := []struct {
		name       string
		mode       egressMode
		state      ResourceState
		success    bool
		outcome    *egressOutcome
		wantMode   egressMode
		wantEvents []string
	}{
		{name: "creation resolves a managed NAT Gateway", state: StateCreationInProgress, success: true,
			outcome: &egressOutcome{Mode: egressModeManaged, Applied: &consensusApplied, Swept: true}, wantMode: egressModeManaged},
		{name: "creation resolves a BYO NAT Gateway", state: StateCreationInProgress, success: true,
			outcome: &egressOutcome{Mode: egressModeBYO}, wantMode: egressModeBYO,
			wantEvents: []string{invalid + " ns/annotated", invalid + " ns/invalid"}},
		{name: "a failed creation resolves a BYO NAT Gateway", state: StateCreationInProgress,
			outcome: &egressOutcome{Mode: egressModeBYO}, wantMode: egressModeBYO,
			wantEvents: []string{invalid + " ns/annotated", invalid + " ns/invalid"}},
		{name: "a BYO identity found to be BYO again", mode: egressModeBYO, state: StateUpdateInProgress, success: true,
			outcome: &egressOutcome{Mode: egressModeBYO, Swept: true}, wantMode: egressModeBYO},
		{name: "a run that resolved no mode keeps the recorded one", mode: egressModeManaged, state: StateCreationInProgress,
			outcome: &egressOutcome{}, wantMode: egressModeManaged},
		{name: "a panic keeps the recorded one", mode: egressModeBYO, state: StateCreationInProgress, wantMode: egressModeBYO},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dt := newTestDiffTracker()
			trackEgressRun(dt, egressRun{state: tc.state, mode: tc.mode, desired: egressUnknown, inFlight: &egressUnknown})
			castEgressVote(dt, egressRunID, "ns/annotated", &consensusA, consensusEpoch)
			castEgressVote(dt, egressRunID, "ns/invalid", nil, consensusEpoch)
			dt.egress.votes.set("ns/plain", egressVote{Identity: egressRunID, Config: consensusApplied, PodUID: "plain-uid", PodCreated: consensusEpoch})
			castEgressVote(dt, "egress-other", "ns/other", &consensusA, consensusEpoch)
			votesBefore := maps.Clone(dt.egress.votes.byPod)
			var err error
			if !tc.success {
				err = errors.New("run failed")
			}

			dt.onServiceOperationComplete(egressRunID, tc.success, err, tc.outcome)

			assert.Equal(t, tc.wantMode, dt.egress.mode[egressRunID])
			assert.Equal(t, tc.wantEvents, takeEgressEvents(dt))
			assert.Equal(t, votesBefore, dt.egress.votes.byPod, "the votes must be left as they were cast")
			_, agreedOther := dt.egress.votes.agreed("egress-other")
			assert.True(t, agreedOther, "control: another identity's votes are untouched")
		})
	}
}

// TestOnOutboundComplete_RecordsTheAppliedConfiguration pins rule 2: the configuration the NAT Gateway satisfies
// is recorded as a new applied config, also after an update error past the commit point and for a creation now
// preempted by a deletion, and every member is told when it changed: not for the default at the identity's
// first creation, not to the unknown configuration, and from it when it differs from the configuration last known
// applied, or, when none is known, only when this process wrote the configuration: the run's NAT Gateway PUT, or an
// earlier write whose outcome was unknown. A creation that failed records nothing. An applied configuration is no
// longer rejected.
func TestOnOutboundComplete_RecordsTheAppliedConfiguration(t *testing.T) {
	const managed = egressModeManaged
	reason := consts.ServiceEgressGatewayReasonConfigApplied
	members := []string{reason + " ns/pod-0", reason + " ns/pod-1"}
	defaults := defaultEgressConfig(egressParseContext{})
	votesA := []*EgressConfig{&consensusA, &consensusB}
	appliedA := &egressOutcome{Mode: managed, Applied: &consensusA, Swept: true}

	cases := []struct {
		name          string
		run           egressRun
		success       bool
		outcome       *egressOutcome
		rejected      *EgressConfig // recorded terminal rejection
		warned        *EgressConfig // recorded retryable rejection
		wantApplied   *EgressConfig
		wantEvents    []string
		wantRejection bool // the recorded rejections are kept
	}{
		{name: "first creation with the default is not announced",
			run:     egressRun{state: StateCreationInProgress, desired: defaults, inFlight: &defaults, votes: votesA},
			success: true, outcome: &egressOutcome{Mode: managed, Applied: &defaults, Swept: true}, wantApplied: &defaults},
		{name: "first creation with another configuration is announced",
			run:     egressRun{state: StateCreationInProgress, desired: consensusA, inFlight: &consensusA, votes: votesA},
			success: true, outcome: appliedA, wantApplied: &consensusA, wantEvents: members},
		{name: "an update that changes it is announced",
			run:     egressRun{state: StateUpdateInProgress, mode: managed, desired: consensusA, applied: &defaults, inFlight: &consensusA, votes: votesA},
			success: true, outcome: appliedA, wantApplied: &consensusA, wantEvents: members},
		{name: "an update that keeps it is not announced",
			run:     egressRun{state: StateUpdateInProgress, mode: managed, desired: consensusA, applied: &consensusA, inFlight: &consensusA, votes: votesA},
			success: true, outcome: appliedA, wantApplied: &consensusA},
		{name: "a configuration found applied after the unknown one is not announced when none was known before it",
			run:     egressRun{state: StateUpdateInProgress, mode: managed, desired: consensusA, applied: &egressUnknown, inFlight: &consensusA, votes: votesA},
			success: true, outcome: appliedA, wantApplied: &consensusA},
		{name: "a configuration found applied after the unknown one is announced, when none was known before it, if it is that of an unknown write",
			run: egressRun{state: StateUpdateInProgress, mode: managed, desired: consensusA, applied: &egressUnknown, unclear: []EgressConfig{consensusA},
				inFlight: &consensusA, votes: votesA},
			success: true, outcome: appliedA, wantApplied: &consensusA, wantEvents: members},
		{name: "a configuration found applied after the unknown one is announced, when none was known before it, if it is that of an older unknown write",
			run: egressRun{state: StateUpdateInProgress, mode: managed, desired: consensusA, applied: &egressUnknown,
				unclear: []EgressConfig{consensusB, consensusA}, inFlight: &consensusA, votes: votesA},
			success: true, outcome: appliedA, wantApplied: &consensusA, wantEvents: members},
		{name: "a configuration found applied after the unknown one is not announced, when none was known before it, if it is not that of an unknown write",
			run: egressRun{state: StateUpdateInProgress, mode: managed, desired: consensusA, applied: &egressUnknown, unclear: []EgressConfig{consensusB},
				inFlight: &consensusA, votes: votesA},
			success: true, outcome: appliedA, wantApplied: &consensusA},
		{name: "a configuration found applied after the unknown one is announced when it differs from the last known one",
			run: egressRun{state: StateUpdateInProgress, mode: managed, desired: consensusA, applied: &egressUnknown, lastKnown: &defaults,
				unclear: []EgressConfig{consensusB}, inFlight: &consensusA, votes: votesA},
			success: true, outcome: appliedA, wantApplied: &consensusA, wantEvents: members},
		{name: "a configuration found applied after the unknown one is not announced when it is the last known one",
			run: egressRun{state: StateUpdateInProgress, mode: managed, desired: consensusA, applied: &egressUnknown, lastKnown: &consensusA,
				unclear: []EgressConfig{consensusB}, inFlight: &consensusA, votes: votesA},
			success: true, outcome: appliedA, wantApplied: &consensusA},
		{name: "an apply of an identity with none known that found its configuration applied is not announced, also when its sweep ended unknown",
			run:     egressRun{state: StateUpdateInProgress, mode: managed, desired: consensusA, applied: &egressUnknown, inFlight: &consensusA, votes: votesA},
			success: true, outcome: &egressOutcome{Mode: managed, Applied: &consensusA, CommitUnknown: true}, wantApplied: &consensusA},
		// Rule 0 remembers the run's own write before rule 2 reads the earlier ones: with room in the list, rebuilding it
		// in place would make the run's write count as an earlier one.
		{name: "an apply that found its configuration applied is not announced when its sweep ended unknown after another unknown write",
			run: egressRun{state: StateUpdateInProgress, mode: managed, desired: consensusA, applied: &egressUnknown,
				unclear: append(make([]EgressConfig, 0, maxUnclearEgressWrites), consensusB), inFlight: &consensusA, votes: votesA},
			success: true, outcome: &egressOutcome{Mode: managed, Applied: &consensusA, CommitUnknown: true}, wantApplied: &consensusA},
		{name: "a change found applied over a known configuration is announced, also when the sweep of the run ended unknown",
			run:     egressRun{state: StateUpdateInProgress, mode: managed, desired: consensusA, applied: &defaults, inFlight: &consensusA, votes: votesA},
			success: true, outcome: &egressOutcome{Mode: managed, Applied: &consensusA, CommitUnknown: true}, wantApplied: &consensusA,
			wantEvents: members},
		{name: "a configuration found applied by a run whose sweep ended unknown is announced against the last known one",
			run: egressRun{state: StateUpdateInProgress, mode: managed, desired: consensusA, applied: &egressUnknown, lastKnown: &defaults,
				inFlight: &consensusA, votes: votesA},
			success: true, outcome: &egressOutcome{Mode: managed, Applied: &consensusA, CommitUnknown: true}, wantApplied: &consensusA,
			wantEvents: members},
		{name: "a NAT Gateway PUT from the unknown configuration that restores the last known one is not announced",
			run: egressRun{state: StateUpdateInProgress, mode: managed, desired: consensusA, applied: &egressUnknown, lastKnown: &consensusA,
				unclear: []EgressConfig{consensusB}, inFlight: &consensusA, votes: votesA},
			success: true, outcome: &egressOutcome{Mode: managed, Applied: &consensusA, NATWritten: true, Swept: true}, wantApplied: &consensusA},
		{name: "a change from the unknown configuration that the run's NAT Gateway PUT applied is announced",
			run:     egressRun{state: StateUpdateInProgress, mode: managed, desired: consensusA, applied: &egressUnknown, inFlight: &consensusA, votes: votesA},
			success: true, outcome: &egressOutcome{Mode: managed, Applied: &consensusA, NATWritten: true, Swept: true}, wantApplied: &consensusA,
			wantEvents: members},
		{name: "an update back to the default is announced",
			run:     egressRun{state: StateUpdateInProgress, mode: managed, desired: defaults, applied: &consensusA, inFlight: &defaults, votes: votesA},
			success: true, outcome: &egressOutcome{Mode: managed, Applied: &defaults, NATWritten: true, Swept: true}, wantApplied: &defaults,
			wantEvents: members},
		{name: "the default found after the unknown one is announced when the last known one was another",
			run: egressRun{state: StateUpdateInProgress, mode: managed, desired: egressUnknown, applied: &egressUnknown, lastKnown: &consensusA,
				unclear: []EgressConfig{defaults}, inFlight: &egressUnknown, votes: votesA},
			success: true, outcome: &egressOutcome{Mode: managed, Applied: &defaults, Swept: true}, wantApplied: &defaults, wantEvents: members},
		{name: "the default written from the unknown one is announced when none was known before it",
			run:     egressRun{state: StateUpdateInProgress, mode: managed, desired: defaults, applied: &egressUnknown, inFlight: &defaults, votes: votesA},
			success: true, outcome: &egressOutcome{Mode: managed, Applied: &defaults, NATWritten: true, Swept: true}, wantApplied: &defaults,
			wantEvents: members},
		{name: "a NAT Gateway PUT from the unknown one is announced when none was known before it, whatever unclear write is remembered",
			run: egressRun{state: StateUpdateInProgress, mode: managed, desired: consensusA, applied: &egressUnknown, unclear: []EgressConfig{consensusB},
				inFlight: &consensusA, votes: votesA},
			success: true, outcome: &egressOutcome{Mode: managed, Applied: &consensusA, NATWritten: true, Swept: true}, wantApplied: &consensusA,
			wantEvents: members},
		{name: "a NAT Gateway PUT that keeps the configuration is not announced",
			run:     egressRun{state: StateUpdateInProgress, mode: managed, desired: consensusA, applied: &consensusA, inFlight: &consensusA, votes: votesA},
			success: true, outcome: &egressOutcome{Mode: managed, Applied: &consensusA, NATWritten: true, Swept: true}, wantApplied: &consensusA},
		{name: "first creation with the default written by the run is not announced",
			run:     egressRun{state: StateCreationInProgress, desired: defaults, inFlight: &defaults, votes: votesA},
			success: true, outcome: &egressOutcome{Mode: managed, Applied: &defaults, NATWritten: true, Swept: true}, wantApplied: &defaults},
		{name: "a change to the unknown configuration is not announced",
			run:     egressRun{state: StateUpdateInProgress, mode: managed, desired: egressUnknown, applied: &consensusA, inFlight: &egressUnknown, votes: votesA},
			success: true, outcome: &egressOutcome{Mode: managed, Applied: &egressUnknown, Swept: true}, wantApplied: &egressUnknown},
		{name: "an update error after the commit point is recorded and announced",
			run:     egressRun{state: StateUpdateInProgress, mode: managed, desired: consensusA, applied: &defaults, inFlight: &consensusA, votes: votesA},
			outcome: &egressOutcome{Mode: managed, Applied: &consensusA}, wantApplied: &consensusA, wantEvents: members},
		{name: "a creation that failed after the commit point records nothing",
			run:     egressRun{state: StateCreationInProgress, desired: consensusA, inFlight: &consensusA, votes: votesA},
			outcome: &egressOutcome{Mode: managed, Applied: &consensusA}},
		{name: "a creation that resolved a BYO NAT Gateway records the unknown configuration",
			run:     egressRun{state: StateCreationInProgress, desired: consensusA, inFlight: &consensusA, votes: votesA},
			success: true, outcome: &egressOutcome{Mode: egressModeBYO}, wantApplied: &egressUnknown,
			wantEvents: []string{consts.ServiceEgressGatewayReasonInvalidConfig + " ns/pod-0", consts.ServiceEgressGatewayReasonInvalidConfig + " ns/pod-1"}},
		{name: "an update preempted by a deletion is recorded and announced",
			run:     egressRun{state: StateDeletionInProgress, mode: managed, desired: consensusA, applied: &defaults, inFlight: &consensusA, votes: votesA},
			success: true, outcome: appliedA, wantApplied: &consensusA, wantEvents: members},
		{name: "a creation preempted by a deletion is recorded and announced",
			run:     egressRun{state: StateDeletionPending, mode: managed, desired: consensusA, inFlight: &consensusA, votes: votesA},
			success: true, outcome: appliedA, wantApplied: &consensusA, wantEvents: members},
		{name: "an applied configuration is no longer rejected",
			run:     egressRun{state: StateUpdateInProgress, mode: managed, desired: consensusA, applied: &defaults, inFlight: &consensusA, votes: votesA},
			success: true, outcome: appliedA, rejected: &consensusA, warned: &consensusA, wantApplied: &consensusA, wantEvents: members},
		{name: "the rejections of other configurations are kept",
			run:     egressRun{state: StateUpdateInProgress, mode: managed, desired: consensusA, applied: &defaults, inFlight: &consensusA, votes: votesA},
			success: true, outcome: appliedA, rejected: &consensusB, warned: &consensusRejected, wantApplied: &consensusA, wantEvents: members,
			wantRejection: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dt := newTestDiffTracker()
			op := trackEgressRun(dt, tc.run)
			if tc.rejected != nil {
				dt.egress.rejected[egressRunID] = egressRejection{Config: *tc.rejected, Reason: "rejected"}
			}
			if tc.warned != nil {
				dt.egress.warned[egressRunID] = egressRejection{Config: *tc.warned, Reason: "quota", Retryable: true}
			}
			published := op.Config.OutboundConfig
			publishedBefore := published.DeepCopy()
			var err error
			if !tc.success {
				err = errors.New("run failed")
			}

			dt.onServiceOperationComplete(egressRunID, tc.success, err, tc.outcome)

			assert.Equal(t, tc.wantApplied, appliedEgressOf(op))
			assert.Equal(t, tc.wantEvents, takeEgressEvents(dt))
			_, rejected := dt.egress.rejected[egressRunID]
			_, warned := dt.egress.warned[egressRunID]
			assert.Equal(t, tc.rejected != nil && tc.wantRejection, rejected)
			assert.Equal(t, tc.warned != nil && tc.wantRejection, warned)
			assert.Equal(t, publishedBefore, published, "a published config must never be modified")
			if op.LastAppliedConfig != nil {
				assert.Equal(t, ipv4Families(), op.LastAppliedConfig.OutboundConfig.IPFamilies)
				assert.NotSame(t, op.Config.OutboundConfig, op.LastAppliedConfig.OutboundConfig, "the applied config must be a new value")
				op.Config.OutboundConfig.IPFamilies[0] = "mutated"
				assert.Equal(t, ipv4Families(), op.LastAppliedConfig.OutboundConfig.IPFamilies, "the applied config must share nothing")
			}
		})
	}
}

// TestOnOutboundComplete_RemembersWhatTheUnknownConfigurationReplaced pins what rules 0 and 2 keep while the applied
// configuration is unknown: the configuration last known applied before it, from a write whose outcome is unknown or
// an observation that found no configuration it knows, and the configuration of an apply whose write is unknown,
// which a later unknown write of an apply replaces and of an observation keeps; a known configuration recorded
// forgets both, and a run of an identity never applied remembers neither.
func TestOnOutboundComplete_RemembersWhatTheUnknownConfigurationReplaced(t *testing.T) {
	const managed = egressModeManaged
	unknownWrite := &egressOutcome{Mode: managed, CommitUnknown: true}
	// An observation reports a link or sweep write of unknown outcome with the unknown configuration (observeEgress).
	observedUnknownWrite := &egressOutcome{Mode: managed, Applied: &egressUnknown, CommitUnknown: true}
	fifth := EgressConfig{IdleTimeoutMinutes: 40, IPv4: egressFamilyConfig{ManagedPublicIPs: 3}}
	cases := []struct {
		name          string
		run           egressRun
		success       bool
		outcome       *egressOutcome
		wantApplied   *EgressConfig
		wantLastKnown *EgressConfig
		wantUnclear   []EgressConfig
	}{
		{name: "an apply whose write is unknown remembers the configuration known before and its own",
			run:     egressRun{state: StateUpdateInProgress, mode: managed, desired: consensusA, applied: &consensusApplied, inFlight: &consensusA, wrote: true},
			outcome: unknownWrite, wantApplied: &egressUnknown, wantLastKnown: &consensusApplied, wantUnclear: []EgressConfig{consensusA}},
		{name: "an apply whose write is unknown remembers its own configuration, not one agreed while it was in flight",
			run:     egressRun{state: StateUpdateInProgress, mode: managed, desired: consensusB, applied: &consensusApplied, inFlight: &consensusA, wrote: true},
			outcome: unknownWrite, wantApplied: &egressUnknown, wantLastKnown: &consensusApplied, wantUnclear: []EgressConfig{consensusA}},
		{name: "an apply that panicked after its first write is remembered alike",
			run:         egressRun{state: StateUpdateInProgress, mode: managed, desired: consensusA, applied: &consensusApplied, inFlight: &consensusA, wrote: true},
			wantApplied: &egressUnknown, wantLastKnown: &consensusApplied, wantUnclear: []EgressConfig{consensusA}},
		{name: "an observation whose write is unknown remembers the configuration known before only",
			run:     egressRun{state: StateUpdateInProgress, mode: managed, desired: egressUnknown, applied: &consensusApplied, inFlight: &egressUnknown, wrote: true},
			outcome: observedUnknownWrite, wantApplied: &egressUnknown, wantLastKnown: &consensusApplied},
		{name: "a deletion whose write is unknown remembers the configuration known before only",
			run:     egressRun{state: StateDeletionInProgress, mode: managed, desired: consensusApplied, applied: &consensusApplied, wrote: true},
			outcome: unknownWrite, wantApplied: &egressUnknown, wantLastKnown: &consensusApplied},
		{name: "a second apply whose write is unknown keeps the configuration known before the first",
			run: egressRun{state: StateUpdateInProgress, mode: managed, desired: consensusB, applied: &egressUnknown, lastKnown: &consensusApplied,
				unclear: []EgressConfig{consensusA}, inFlight: &consensusB, wrote: true},
			outcome: unknownWrite, wantApplied: &egressUnknown, wantLastKnown: &consensusApplied, wantUnclear: []EgressConfig{consensusB, consensusA}},
		{name: "an unknown write of a configuration remembered before moves it first, without a repeat",
			run: egressRun{state: StateUpdateInProgress, mode: managed, desired: consensusB, applied: &egressUnknown, lastKnown: &consensusApplied,
				unclear: []EgressConfig{consensusA, consensusB}, inFlight: &consensusB, wrote: true},
			outcome: unknownWrite, wantApplied: &egressUnknown, wantLastKnown: &consensusApplied, wantUnclear: []EgressConfig{consensusB, consensusA}},
		{name: "an unknown write remembered before in a full list moves first, and the other three are kept",
			run: egressRun{state: StateUpdateInProgress, mode: managed, desired: consensusRejected, applied: &egressUnknown, lastKnown: &consensusApplied,
				unclear: []EgressConfig{consensusA, consensusB, consensusRejected, consensusApplied}, inFlight: &consensusRejected, wrote: true},
			outcome: unknownWrite, wantApplied: &egressUnknown, wantLastKnown: &consensusApplied,
			wantUnclear: []EgressConfig{consensusRejected, consensusA, consensusB, consensusApplied}},
		{name: "at most four unknown writes are remembered, the oldest dropped",
			run: egressRun{state: StateUpdateInProgress, mode: managed, desired: fifth, applied: &egressUnknown, lastKnown: &consensusApplied,
				unclear: []EgressConfig{consensusA, consensusB, consensusRejected, consensusApplied}, inFlight: &fifth, wrote: true},
			outcome: unknownWrite, wantApplied: &egressUnknown, wantLastKnown: &consensusApplied,
			wantUnclear: []EgressConfig{fifth, consensusA, consensusB, consensusRejected}},
		{name: "an observation whose write is unknown keeps the configuration of the apply before it",
			run: egressRun{state: StateUpdateInProgress, mode: managed, desired: egressUnknown, applied: &egressUnknown, lastKnown: &consensusApplied,
				unclear: []EgressConfig{consensusA}, inFlight: &egressUnknown, wrote: true},
			outcome: observedUnknownWrite, wantApplied: &egressUnknown, wantLastKnown: &consensusApplied, wantUnclear: []EgressConfig{consensusA}},
		{name: "an apply preempted by a deletion whose write is unknown is remembered alike",
			run: egressRun{state: StateDeletionPending, mode: managed, desired: consensusA, applied: &consensusApplied, inFlight: &consensusA,
				wrote: true},
			outcome: unknownWrite, wantApplied: &egressUnknown, wantLastKnown: &consensusApplied, wantUnclear: []EgressConfig{consensusA}},
		{name: "an observation that finds no configuration it knows remembers the configuration known before",
			run:     egressRun{state: StateUpdateInProgress, mode: managed, desired: egressUnknown, applied: &consensusA, inFlight: &egressUnknown},
			success: true, outcome: &egressOutcome{Mode: managed, Applied: &egressUnknown, Swept: true},
			wantApplied: &egressUnknown, wantLastKnown: &consensusA},
		{name: "a known configuration recorded forgets both",
			run: egressRun{state: StateUpdateInProgress, mode: managed, desired: egressUnknown, applied: &egressUnknown, lastKnown: &consensusApplied,
				unclear: []EgressConfig{consensusA}, inFlight: &egressUnknown},
			success: true, outcome: &egressOutcome{Mode: managed, Applied: &consensusB, Swept: true}, wantApplied: &consensusB},
		{name: "a creation whose write is unknown records nothing",
			run:     egressRun{state: StateCreationInProgress, desired: consensusA, inFlight: &consensusA, wrote: true},
			outcome: unknownWrite},
		{name: "a creation that resolved a BYO NAT Gateway remembers nothing",
			run:     egressRun{state: StateCreationInProgress, desired: consensusA, inFlight: &consensusA},
			success: true, outcome: &egressOutcome{Mode: egressModeBYO}, wantApplied: &egressUnknown},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dt := newTestDiffTracker()
			op := trackEgressRun(dt, tc.run)

			dt.onServiceOperationComplete(egressRunID, tc.success, errors.New("run ended"), tc.outcome)

			assert.Equal(t, tc.wantApplied, appliedEgressOf(op))
			assert.Equal(t, tc.wantLastKnown, op.Egress.LastKnownApplied)
			assert.Equal(t, tc.wantUnclear, op.Egress.UnclearWrites)
		})
	}
}

// TestOnOutboundComplete_TellsTheFinalReasonOfARetriedRejection pins the event of rule 4 for a configuration warned
// about that a later try rejects terminally: the pods voting for it are told the final reason and that it will not
// be retried, once.
func TestOnOutboundComplete_TellsTheFinalReasonOfARetriedRejection(t *testing.T) {
	dt := newTestDiffTracker()
	trackEgressRun(dt, egressRun{state: StateUpdateInProgress, mode: egressModeManaged, desired: consensusRejected,
		applied: &consensusApplied, inFlight: &consensusRejected, votes: []*EgressConfig{&consensusRejected, &consensusRejected}})
	dt.egress.warned[egressRunID] = egressRejection{Config: consensusRejected, Reason: "user prefix does not exist", Retryable: true}
	final := egressRejection{Config: consensusRejected, Reason: "IPv4: 17 public addresses, more than 16"}

	dt.onServiceOperationComplete(egressRunID, false, errors.New("rejected"), &egressOutcome{Mode: egressModeManaged, Rejected: &final})

	dt.mu.Lock()
	events := dt.egress.pendingEvents
	dt.egress.pendingEvents = nil
	dt.mu.Unlock()
	if assert.Len(t, events, 2, "every pod voting for the configuration is told") {
		for _, event := range events {
			assert.Equal(t, consts.ServiceEgressGatewayReasonConfigRejected, event.reason)
			assert.Equal(t, v1.EventTypeWarning, event.eventType)
			assert.Equal(t, egressRejectedMessage(egressRunID, final), event.message)
			assert.Contains(t, event.message, "it will not be retried until the members of the identity agree on another configuration")
		}
	}

	dt.mu.Lock()
	dt.pendingServiceOps[egressRunID].State = StateUpdateInProgress
	dt.pendingServiceOps[egressRunID].InFlightConfig = egressRunConfig(consensusRejected)
	dt.mu.Unlock()
	dt.onServiceOperationComplete(egressRunID, false, errors.New("rejected"), &egressOutcome{Mode: egressModeManaged, Rejected: &final})
	assert.Empty(t, takeEgressEvents(dt), "a configuration rejected terminally is told once")
}

// TestOnOutboundComplete_SettlesTheOwedObservation pins rule 3: the observation is owed, as a new obligation,
// after a run that wrote, or that reports an applied configuration, without completing its link repair and sweep;
// a complete sweep settles it unless an obligation was raised since the run was dispatched; a busy run and a
// refusal before any write leave it as it is.
func TestOnOutboundComplete_SettlesTheOwedObservation(t *testing.T) {
	const managed = egressModeManaged
	refused := &egressRejection{Config: consensusA, Reason: "user prefix in another region"}

	cases := []struct {
		name     string
		wrote    bool
		owed     bool
		raised   bool
		success  bool
		outcome  *egressOutcome
		wantOwed bool
		wantGen  uint64
	}{
		{name: "a write without a complete sweep owes it", wrote: true, success: true,
			outcome: &egressOutcome{Mode: managed, Applied: &consensusA}, wantOwed: true, wantGen: 8},
		{name: "a write without a complete sweep owes it again", wrote: true, owed: true,
			outcome: &egressOutcome{Mode: managed, Applied: &consensusA}, wantOwed: true, wantGen: 8},
		{name: "an applied configuration without a write and without a complete sweep owes it", success: true,
			outcome: &egressOutcome{Mode: managed, Applied: &consensusA}, wantOwed: true, wantGen: 8},
		{name: "a link repair that failed after an applied configuration without a write owes it",
			outcome: &egressOutcome{Mode: managed, Applied: &consensusA}, wantOwed: true, wantGen: 8},
		{name: "a complete sweep settles it", wrote: true, owed: true, success: true,
			outcome: &egressOutcome{Mode: managed, Applied: &consensusA, Swept: true}, wantGen: 7},
		{name: "a complete sweep without a write settles it", owed: true, success: true,
			outcome: &egressOutcome{Mode: managed, Applied: &consensusApplied, Swept: true}, wantGen: 7},
		{name: "a complete sweep of a failed run settles it", owed: true,
			outcome: &egressOutcome{Mode: managed, Swept: true}, wantGen: 7},
		{name: "an obligation raised in flight survives a complete sweep", wrote: true, raised: true, success: true,
			outcome: &egressOutcome{Mode: managed, Applied: &consensusA, Swept: true}, wantOwed: true, wantGen: 8},
		{name: "a busy run leaves an owed observation owed", owed: true,
			outcome: &egressOutcome{Mode: managed}, wantOwed: true, wantGen: 7},
		{name: "a busy run owes nothing new",
			outcome: &egressOutcome{Mode: managed}, wantGen: 7},
		{name: "a refusal before any write leaves an owed observation owed", owed: true,
			outcome: &egressOutcome{Mode: managed, Rejected: refused}, wantOwed: true, wantGen: 7},
		{name: "a refusal before any write owes nothing new",
			outcome: &egressOutcome{Mode: managed, Rejected: refused}, wantGen: 7},
		{name: "a write ending commit unknown owes it even after a complete sweep", wrote: true,
			outcome: &egressOutcome{Mode: managed, CommitUnknown: true, Swept: true}, wantOwed: true, wantGen: 8},
		{name: "a panic before any write leaves it as it is", owed: true, wantOwed: true, wantGen: 7},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dt := newTestDiffTracker()
			op := trackEgressRun(dt, egressRun{state: StateUpdateInProgress, mode: managed, desired: consensusA,
				applied: &consensusApplied, inFlight: &consensusA, wrote: tc.wrote, owed: tc.owed, raised: tc.raised})
			var err error
			if !tc.success {
				err = errors.New("run failed")
			}

			dt.onServiceOperationComplete(egressRunID, tc.success, err, tc.outcome)

			assert.Equal(t, tc.wantOwed, op.Egress.NeedsObservation)
			assert.Equal(t, tc.wantGen, op.Egress.ObservationGen)
		})
	}
}

// TestOnOutboundComplete_RecordsRejections pins rule 4 and decision H for rejections: a terminal rejection is
// recorded as such, a retryable one as warned about, and the pods voting for the rejected configuration (only
// those) are told when that configuration is first rejected and once more when a later try rejects terminally a
// configuration warned about: never again for a reason-only change, never for a configuration already rejected
// terminally, and not for the startup baseline (the agreed configuration of an identity startup found in Azure,
// which the previous process already reported). After a creation fell back to the default, the creation
// configuration it was dispatched with stays desired.
func TestOnOutboundComplete_RecordsRejections(t *testing.T) {
	const managed = egressModeManaged
	reason := consts.ServiceEgressGatewayReasonConfigRejected
	voters := []string{reason + " ns/pod-0", reason + " ns/pod-1"}
	mixed := []*EgressConfig{&consensusRejected, &consensusRejected, &consensusA, nil}
	agreed := []*EgressConfig{&consensusRejected, &consensusRejected}
	terminal := egressRejection{Config: consensusRejected, Reason: "too many tags"}
	retryable := egressRejection{Config: consensusRejected, Reason: "quota", Retryable: true}
	update := func(votes []*EgressConfig, baseline bool) egressRun {
		return egressRun{state: StateUpdateInProgress, mode: managed, desired: consensusRejected, applied: &consensusApplied,
			inFlight: &consensusRejected, votes: votes, baseline: baseline}
	}
	creation := egressRun{state: StateCreationInProgress, desired: consensusRejected, inFlight: &consensusRejected, votes: mixed}
	other := &egressRejection{Config: consensusB, Reason: "user prefix in another region"}

	cases := []struct {
		name         string
		run          egressRun
		success      bool
		outcome      *egressOutcome
		rejected     *egressRejection // recorded before
		warned       *egressRejection // recorded before
		wantRejected *egressRejection
		wantWarned   *egressRejection
		wantDesired  EgressConfig
		wantEvents   []string
		wantBaseline bool
	}{
		{name: "terminal rejection of an update is told to its voters", run: update(mixed, false),
			outcome:      &egressOutcome{Mode: managed, Rejected: &terminal},
			wantRejected: &terminal, wantDesired: consensusApplied, wantEvents: voters},
		{name: "retryable rejection of an update is warned to its voters", run: update(mixed, false),
			outcome:    &egressOutcome{Mode: managed, Rejected: &retryable},
			wantWarned: &retryable, wantDesired: consensusApplied, wantEvents: voters},
		{name: "the same configuration rejected for another reason is not told again", run: update(mixed, false),
			rejected: &egressRejection{Config: consensusRejected, Reason: "older reason"},
			outcome:  &egressOutcome{Mode: managed, Rejected: &terminal}, wantRejected: &terminal, wantDesired: consensusApplied},
		{name: "a warned configuration rejected terminally is told once more", run: update(mixed, false),
			warned:  &retryable,
			outcome: &egressOutcome{Mode: managed, Rejected: &terminal}, wantRejected: &terminal, wantWarned: &retryable,
			wantDesired: consensusApplied, wantEvents: voters},
		{name: "a warned configuration warned about again for another reason is not told again", run: update(mixed, false),
			warned:  &egressRejection{Config: consensusRejected, Reason: "older reason", Retryable: true},
			outcome: &egressOutcome{Mode: managed, Rejected: &retryable}, wantWarned: &retryable, wantDesired: consensusApplied},
		{name: "a configuration rejected terminally after a warning is not told a third time", run: update(mixed, false),
			rejected: &terminal, warned: &retryable,
			outcome:      &egressOutcome{Mode: managed, Rejected: &egressRejection{Config: consensusRejected, Reason: "user prefix too small"}},
			wantRejected: &egressRejection{Config: consensusRejected, Reason: "user prefix too small"}, wantWarned: &retryable,
			wantDesired: consensusApplied},
		{name: "startup baseline: a warned configuration rejected terminally is recorded silently", run: update(agreed, true),
			warned:  &retryable,
			outcome: &egressOutcome{Mode: managed, Rejected: &terminal}, wantRejected: &terminal, wantWarned: &retryable,
			wantDesired: consensusApplied},
		{name: "a terminally rejected configuration warned about is not told again", run: update(mixed, false),
			rejected: &terminal,
			outcome:  &egressOutcome{Mode: managed, Rejected: &retryable}, wantRejected: &terminal, wantWarned: &retryable,
			wantDesired: consensusApplied},
		{name: "a newly rejected configuration is told", run: update(mixed, false),
			rejected: other, warned: other,
			outcome: &egressOutcome{Mode: managed, Rejected: &terminal}, wantRejected: &terminal, wantWarned: other,
			wantDesired: consensusApplied, wantEvents: voters},
		{name: "a creation that fell back to the default keeps the creation configuration it was dispatched with desired", run: creation, success: true,
			outcome:    &egressOutcome{Mode: managed, Applied: &consensusApplied, Rejected: &retryable, Swept: true},
			wantWarned: &retryable, wantDesired: consensusRejected, wantEvents: voters},
		{name: "a creation that fell back to the default records a terminal rejection", run: creation, success: true,
			outcome:      &egressOutcome{Mode: managed, Applied: &consensusApplied, Rejected: &terminal, Swept: true},
			wantRejected: &terminal, wantDesired: consensusRejected, wantEvents: voters},
		{name: "startup baseline: the rejection of the agreed configuration is recorded silently", run: update(agreed, true),
			outcome:      &egressOutcome{Mode: managed, Rejected: &terminal},
			wantRejected: &terminal, wantDesired: consensusApplied},
		{name: "startup baseline: a rejection without agreement is told", run: update(mixed, true),
			outcome:      &egressOutcome{Mode: managed, Rejected: &terminal},
			wantRejected: &terminal, wantDesired: consensusApplied, wantEvents: voters},
		{name: "startup baseline: a busy run keeps it", run: update(agreed, true),
			outcome: &egressOutcome{Mode: managed}, wantDesired: consensusRejected, wantBaseline: true},
		{name: "startup baseline: an applied configuration ends it", run: update(agreed, true), success: true,
			outcome: &egressOutcome{Mode: managed, Applied: &consensusRejected, Swept: true}, wantDesired: consensusRejected},
		{name: "an update reporting another configuration rejected keeps its desired configuration",
			run: egressRun{state: StateUpdateInProgress, mode: managed, desired: consensusA, applied: &consensusApplied,
				inFlight: &consensusA, votes: mixed},
			success: true, outcome: &egressOutcome{Mode: managed, Applied: &consensusA, Rejected: &retryable, Swept: true},
			wantWarned: &retryable, wantDesired: consensusA, wantEvents: voters},
		{name: "a run without a rejection keeps the recorded ones", run: update(mixed, false), success: true,
			rejected: other, warned: other, outcome: &egressOutcome{Mode: managed, Applied: &consensusA, Swept: true},
			wantRejected: other, wantWarned: other, wantDesired: consensusRejected},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dt := newTestDiffTracker()
			op := trackEgressRun(dt, tc.run)
			if tc.rejected != nil {
				dt.egress.rejected[egressRunID] = *tc.rejected
			}
			if tc.warned != nil {
				dt.egress.warned[egressRunID] = *tc.warned
			}
			var err error
			if !tc.success {
				err = newTerminalError(errors.New("rejected"))
			}

			dt.onServiceOperationComplete(egressRunID, tc.success, err, tc.outcome)

			rejected, hasRejected := dt.egress.rejected[egressRunID]
			if assert.Equal(t, tc.wantRejected != nil, hasRejected, "terminal rejection recorded") && hasRejected {
				assert.Equal(t, *tc.wantRejected, rejected)
			}
			warned, hasWarned := dt.egress.warned[egressRunID]
			if assert.Equal(t, tc.wantWarned != nil, hasWarned, "retryable rejection recorded") && hasWarned {
				assert.Equal(t, *tc.wantWarned, warned)
			}
			assert.Equal(t, tc.wantDesired, outboundEgress(op.Config.OutboundConfig))
			var rejectedEvents []string
			for _, event := range takeEgressEvents(dt) {
				if strings.HasPrefix(event, reason+" ") {
					rejectedEvents = append(rejectedEvents, event)
				}
			}
			assert.Equal(t, tc.wantEvents, rejectedEvents)
			assert.Equal(t, tc.wantBaseline, op.Egress.StartupBaseline)
		})
	}
}

// TestOnOutboundComplete_UpdateNeverParks pins rule 5 for failed updates: the identity's NAT Gateway exists,
// so a failed update, terminal or not, never parks it in StateNotStarted. The in-flight snapshot is cleared,
// the failure counts as a retry with a backoff, and the evaluation decides what follows: a retry of the
// agreed configuration, a cancellation, or an observe-and-clean.
func TestOnOutboundComplete_UpdateNeverParks(t *testing.T) {
	const managed = egressModeManaged
	transient := errors.New("409 conflict")
	terminalErr := newTerminalError(errors.New("rejected"))

	cases := []struct {
		name        string
		votes       []*EgressConfig
		wrote       bool
		err         error
		outcome     *egressOutcome
		wantState   ResourceState
		wantDesired EgressConfig
	}{
		{name: "retryable error: the agreed configuration is retried after a backoff", votes: []*EgressConfig{&consensusA, &consensusA},
			err: transient, outcome: &egressOutcome{Mode: managed}, wantState: StateUpdateInProgress, wantDesired: consensusA},
		{name: "terminal error without a rejection: retried after a backoff, not parked", votes: []*EgressConfig{&consensusA},
			err: terminalErr, outcome: &egressOutcome{Mode: managed}, wantState: StateUpdateInProgress, wantDesired: consensusA},
		{name: "panic: retried after a backoff, not parked", votes: []*EgressConfig{&consensusA},
			err: errors.New("panic in service worker"), wantState: StateUpdateInProgress, wantDesired: consensusA},
		{name: "retryable rejection: retried in the background", votes: []*EgressConfig{&consensusA},
			err:       transient,
			outcome:   &egressOutcome{Mode: managed, Rejected: &egressRejection{Config: consensusA, Reason: "quota", Retryable: true}},
			wantState: StateUpdateInProgress, wantDesired: consensusA},
		{name: "terminal rejection of the agreed configuration: cancelled", votes: []*EgressConfig{&consensusA},
			err: terminalErr, outcome: &egressOutcome{Mode: managed, Rejected: &egressRejection{Config: consensusA, Reason: "too many tags"}},
			wantState: StateCreated, wantDesired: consensusApplied},
		{name: "consensus lost while the update ran: cancelled", votes: []*EgressConfig{&consensusA, &consensusB},
			err: transient, outcome: &egressOutcome{Mode: managed}, wantState: StateCreated, wantDesired: consensusApplied},
		{name: "a write without a complete sweep: observe-and-clean after a backoff", votes: []*EgressConfig{&consensusA, &consensusB},
			wrote: true, err: transient, outcome: &egressOutcome{Mode: managed}, wantState: StateUpdateInProgress, wantDesired: egressUnknown},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dt := newTestDiffTracker()
			op := trackEgressRun(dt, egressRun{state: StateUpdateInProgress, mode: managed, desired: consensusA,
				applied: &consensusApplied, inFlight: &consensusA, wrote: tc.wrote, retries: 2, votes: tc.votes})

			dt.onServiceOperationComplete(egressRunID, false, tc.err, tc.outcome)

			assert.Equal(t, tc.wantState, op.State, "a failed update never parks its identity in StateNotStarted")
			assert.False(t, op.CreationFailedTerminal)
			assert.Nil(t, op.InFlightConfig, "a failed update clears its in-flight snapshot")
			assert.Equal(t, tc.wantDesired, outboundEgress(op.Config.OutboundConfig))
			if tc.wantState == StateUpdateInProgress {
				assert.Equal(t, 3, op.RetryCount, "the failure counts as a retry")
				assertBackoff(t, op, true)
				assert.Len(t, dt.serviceUpdaterTrigger, 1, "the retry must be scheduled")
			} else {
				assert.Zero(t, op.RetryCount, "a cancelled update starts the next one with a clean budget")
				assertBackoff(t, op, false)
			}
		})
	}
}

// TestOnOutboundComplete_TerminalRejectionWaitsForAVoteChange pins that an identity whose agreed configuration
// was rejected terminally stays Created with its NAT Gateway unchanged: new pods register at once, the same
// votes never dispatch the rejected configuration again, and a new agreement does.
func TestOnOutboundComplete_TerminalRejectionWaitsForAVoteChange(t *testing.T) {
	dt := newTestDiffTracker()
	op := trackEgressRun(dt, egressRun{state: StateUpdateInProgress, mode: egressModeManaged, desired: consensusRejected,
		applied: &consensusApplied, inFlight: &consensusRejected, votes: []*EgressConfig{&consensusRejected, &consensusRejected}})
	dt.NRPResources.NATGateways.Insert(egressRunID)

	dt.onServiceOperationComplete(egressRunID, false, newTerminalError(errors.New("rejected")), &egressOutcome{
		Mode: egressModeManaged, Rejected: &egressRejection{Config: consensusRejected, Reason: "too many tags"}})

	assert.Equal(t, StateCreated, op.State)
	assert.Equal(t, consensusApplied, *appliedEgressOf(op), "the NAT Gateway keeps its configuration")
	drainServiceUpdaterTrigger(dt)

	dt.AddPodWithUID(egressRunID, "ns/new", "new-uid", "10.0.0.1", "10.244.0.9")
	assert.True(t, dt.outboundPodExistsLocked(egressRunID, "10.0.0.1", "10.244.0.9"), "a new pod must register at once")

	dt.mu.Lock()
	dt.evaluateEgressConsensusLocked(egressRunID)
	dt.mu.Unlock()
	assert.Equal(t, StateCreated, op.State, "the same votes must not dispatch the rejected configuration again")
	assert.Empty(t, dt.serviceUpdaterTrigger)

	dt.mu.Lock()
	castEgressVote(dt, egressRunID, "ns/pod-0", &consensusB, consensusEpoch)
	castEgressVote(dt, egressRunID, "ns/pod-1", &consensusB, consensusEpoch)
	dt.evaluateEgressConsensusLocked(egressRunID)
	dt.mu.Unlock()
	assert.Equal(t, StateUpdateInProgress, op.State, "a new agreement must be applied")
	assert.Equal(t, consensusB, outboundEgress(op.Config.OutboundConfig))
}

// TestEvaluateEgressConsensusLocked_NewAgreementSkipsTheOldBackoff pins that an agreement on another configuration is
// dispatched at once, not after the backoff or the park of the configuration it replaces: one backing off after a
// failure, one parked after its retry burst, or one whose run failed while the new agreement was reached. The quiet
// period of a write whose outcome is unknown still holds the new agreement back, and the same agreement again keeps
// the backoff of the configuration that fails.
func TestEvaluateEgressConsensusLocked_NewAgreementSkipsTheOldBackoff(t *testing.T) {
	const managed = egressModeManaged
	transient := errors.New("409 conflict")
	for _, tc := range []struct {
		name string
		// retries is the retry count before the run of A fails; maxServiceRetries-1 parks it.
		retries int
		// inFlight: the members agree on the next configuration while the failing run of A is in flight.
		inFlight bool
		// unknown: the run of A fails with a write whose outcome is unknown, which starts the quiet period.
		unknown bool
		// same: after the failure of A a new member agrees on A too, instead of the members agreeing on B.
		same bool
		// wantDispatched: the next dispatch starts a run, before the quiet period ends when there is one.
		wantDispatched bool
	}{
		{name: "A backing off, then B agreed: B at once", wantDispatched: true},
		{name: "A parked after its retry burst, then B agreed: B at once", retries: maxServiceRetries - 1, wantDispatched: true},
		{name: "B agreed while the run of A was in flight, A failed: B at once", inFlight: true, wantDispatched: true},
		{name: "A in its quiet period, then B agreed: B waits for its end", unknown: true},
		{name: "A backing off, a new member agrees on A too: A keeps its backoff", same: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dt := newTestDiffTracker()
			op := trackManagedEgressIdentity(dt, memberIdentity, consensusApplied)
			p1 := memberPod("p1")
			joinMember(dt, memberIdentity, p1, &consensusA)
			settleMembership(dt)
			op.RetryCount = tc.retries
			dispatchWithoutRunning(t, dt)
			if !assert.NotNil(t, op.InFlightConfig) || !assert.Equal(t, consensusA, outboundEgress(op.InFlightConfig.OutboundConfig), "control: A is dispatched") {
				return
			}

			if tc.inFlight {
				dt.SetEgressVote(memberIdentity, p1, memberVote(&consensusB))
			}
			dt.onServiceOperationComplete(memberIdentity, false, transient, &egressOutcome{Mode: managed, CommitUnknown: tc.unknown})
			quietUntil := op.Egress.QuietUntil
			assert.Equal(t, tc.unknown, quietUntil.After(time.Now()), "control: quiet period")
			if !tc.inFlight {
				drainServiceUpdaterTrigger(dt)
				dispatchWithoutRunning(t, dt)
				assert.Nil(t, op.InFlightConfig, "control: A waits")
				assert.Equal(t, tc.retries == maxServiceRetries-1, op.RetriesExhausted, "control: parked")
			}
			retries, nextRetryAt := op.RetryCount, op.NextRetryAt
			switch {
			case tc.same:
				joinMember(dt, memberIdentity, memberPod("p2"), &consensusA)
			case !tc.inFlight:
				dt.SetEgressVote(memberIdentity, p1, memberVote(&consensusB))
			}

			want := consensusB
			if tc.same {
				want = consensusA
				assert.Equal(t, retries, op.RetryCount, "the same agreement keeps the retry budget")
				assert.Equal(t, nextRetryAt, op.NextRetryAt, "and the backoff")
			} else {
				assert.Zero(t, op.RetryCount, "a new agreement starts with a clean retry budget")
				assert.False(t, op.RetriesExhausted)
				assert.Zero(t, op.NextRetryAt)
				assert.Len(t, dt.serviceUpdaterTrigger, 1, "and is dispatched without waiting for a revisit")
			}
			assert.Equal(t, want, desiredEgressOf(dt, memberIdentity))
			assert.Equal(t, quietUntil, op.Egress.QuietUntil, "the quiet period is kept")

			dispatchWithoutRunning(t, dt)
			if !tc.wantDispatched {
				assert.Nil(t, op.InFlightConfig, "not dispatched yet")
				if !tc.unknown {
					return
				}
				op.Egress.QuietUntil = time.Now().Add(-time.Second)
				dispatchWithoutRunning(t, dt)
			}
			if assert.NotNil(t, op.InFlightConfig, "dispatched") {
				assert.Equal(t, consensusB, outboundEgress(op.InFlightConfig.OutboundConfig))
			}
		})
	}
}

// TestEvaluateEgressConsensusLocked_RejectionIsForgottenOnAnotherAgreement pins how long a rejection is remembered
// (design 5: retried when the agreed configuration changes): while the members keep agreeing on the rejected
// configuration, also across a loss of agreement, it is not dispatched again and only a pod that votes for it anew is
// told; once they agree on another configuration it is forgotten, terminal or warned, so agreeing on it again tries it
// once more, without a stale event, and its next rejection is reported to every pod that votes for it.
func TestEvaluateEgressConsensusLocked_RejectionIsForgottenOnAnotherAgreement(t *testing.T) {
	const identity = "egress-rejected"
	rejectedEvent := func(pods ...string) []string {
		var events []string
		for _, pod := range pods {
			events = append(events, memberEvent(consts.ServiceEgressGatewayReasonConfigRejected, pod))
		}
		return events
	}
	conflictEvent := func(pod string) []string {
		return []string{memberEvent(consts.ServiceEgressGatewayReasonConfigConflict, pod)}
	}

	for _, retryable := range []bool{false, true} {
		t.Run(fmt.Sprintf("retryable=%t", retryable), func(t *testing.T) {
			dt := newTestDiffTracker()
			op := trackManagedEgressIdentity(dt, identity, consensusApplied)
			p1, p2 := memberPod("p1"), memberPod("p2")
			joinMember(dt, identity, p1, &consensusRejected)
			joinMember(dt, identity, p2, &consensusRejected)
			settleMembership(dt)
			vote := func(pod egressPodRef, cfg EgressConfig) []string {
				t.Helper()
				dt.SetEgressVote(identity, pod, memberVote(&cfg))
				drainServiceUpdaterTrigger(dt)
				return takeEgressEvents(dt)
			}
			reject := func() []string {
				t.Helper()
				if !assert.Equal(t, StateUpdateInProgress, op.State, "control: the agreed configuration is dispatched") ||
					!assert.Equal(t, consensusRejected, desiredEgressOf(dt, identity)) {
					t.FailNow()
				}
				dispatchWithoutRunning(t, dt)
				dt.onServiceOperationComplete(identity, false, errors.New("rejected"), &egressOutcome{Mode: egressModeManaged,
					Rejected: &egressRejection{Config: consensusRejected, Reason: "rejected", Retryable: retryable}})
				drainServiceUpdaterTrigger(dt)
				if !retryable {
					assert.Equal(t, StateCreated, op.State, "a terminal rejection is not retried while it is agreed")
				}
				return takeEgressEvents(dt)
			}
			remembered := func() bool {
				dt.mu.Lock()
				defer dt.mu.Unlock()
				_, known := dt.egressRejectionOfLocked(identity, consensusRejected)
				return known
			}

			assert.Equal(t, rejectedEvent("p1", "p2"), reject(), "the first rejection is reported to every pod voting for it")
			assert.True(t, remembered())

			// Agreement lost, then found again on the rejected configuration: it is still remembered.
			assert.Equal(t, conflictEvent("p1"), vote(p1, consensusB))
			assert.True(t, remembered(), "losing the agreement alone does not forget the rejection")
			assert.Equal(t, rejectedEvent("p1"), vote(p1, consensusRejected), "a pod voting anew for it is told")
			if !retryable {
				assert.Equal(t, StateCreated, op.State, "the same configuration is not tried again")
			}

			// Another agreement forgets the rejection.
			assert.Equal(t, conflictEvent("p1"), vote(p1, consensusB))
			assert.Empty(t, vote(p2, consensusB))
			assert.False(t, remembered(), "agreeing on another configuration forgets the rejection")
			assert.Equal(t, StateUpdateInProgress, op.State)
			assert.Equal(t, consensusB, desiredEgressOf(dt, identity))

			// Agreeing on it again tries it once more, without a stale event.
			assert.Equal(t, conflictEvent("p1"), vote(p1, consensusRejected), "no event for a configuration no longer rejected")
			assert.Empty(t, vote(p2, consensusRejected))
			assert.Equal(t, rejectedEvent("p1", "p2"), reject(), "the next rejection is reported to every pod voting for it")
			assert.True(t, remembered())

			if retryable {
				assert.Empty(t, reject(), "a retryable rejection already warned about is not reported again")
				return
			}
			dt.mu.Lock()
			dt.evaluateEgressConsensusLocked(identity)
			dt.mu.Unlock()
			assert.Equal(t, StateCreated, op.State, "repeated agreement on it does not dispatch it again")
			assert.Empty(t, dt.serviceUpdaterTrigger)
		})
	}
}

// TestOnOutboundComplete_SuccessFollowUp pins how a successful run is followed up. A run that leaves its own
// work undone (an observation still owed, or the configuration it was dispatched with not applied) is
// followed by a run that waits out a retry backoff and counts as a retry, so a run that cannot make progress
// cannot loop; a run for a configuration agreed while it was in flight is new intent and is dispatched at
// once; nothing left to do leaves the identity Created with a clean retry budget.
func TestOnOutboundComplete_SuccessFollowUp(t *testing.T) {
	const managed = egressModeManaged
	agreeA := []*EgressConfig{&consensusA, &consensusA}
	agreeB := []*EgressConfig{&consensusB, &consensusB}
	agreeRejected := []*EgressConfig{&consensusRejected, &consensusRejected}
	noAgreement := []*EgressConfig{&consensusA, &consensusB}
	update := func(votes []*EgressConfig, wrote, raised bool) egressRun {
		return egressRun{state: StateUpdateInProgress, mode: managed, desired: consensusA, applied: &consensusApplied,
			inFlight: &consensusA, wrote: wrote, raised: raised, retries: 2, votes: votes}
	}
	creation := func(desired EgressConfig, votes []*EgressConfig, wrote bool) egressRun {
		return egressRun{state: StateCreationInProgress, desired: desired, inFlight: &desired, wrote: wrote, retries: 3, votes: votes}
	}

	cases := []struct {
		name        string
		run         egressRun
		outcome     *egressOutcome
		wantState   ResourceState
		wantDesired EgressConfig // checked for an update
		wantRetries int
		wantBackoff bool
	}{
		{name: "converged update", run: update(agreeA, true, false),
			outcome: &egressOutcome{Mode: managed, Applied: &consensusA, Swept: true}, wantState: StateCreated},
		{name: "incomplete sweep: observation after a backoff", run: update(agreeA, true, false),
			outcome:   &egressOutcome{Mode: managed, Applied: &consensusA},
			wantState: StateUpdateInProgress, wantDesired: egressUnknown, wantRetries: 3, wantBackoff: true},
		{name: "obligation raised while in flight: observation after a backoff", run: update(agreeA, true, true),
			outcome:   &egressOutcome{Mode: managed, Applied: &consensusA, Swept: true},
			wantState: StateUpdateInProgress, wantDesired: egressUnknown, wantRetries: 3, wantBackoff: true},
		{name: "configuration not applied: retried after a backoff", run: update(agreeA, false, false),
			outcome:   &egressOutcome{Mode: managed, Rejected: &egressRejection{Config: consensusA, Reason: "quota", Retryable: true}},
			wantState: StateUpdateInProgress, wantDesired: consensusA, wantRetries: 3, wantBackoff: true},
		{name: "agreement changed while in flight: applied at once", run: update(agreeB, true, false),
			outcome:   &egressOutcome{Mode: managed, Applied: &consensusA, Swept: true},
			wantState: StateUpdateInProgress, wantDesired: consensusB},
		{name: "agreement changed and the sweep is incomplete: converge-and-clean at once", run: update(agreeB, true, false),
			outcome:   &egressOutcome{Mode: managed, Applied: &consensusA},
			wantState: StateUpdateInProgress, wantDesired: consensusB},
		{name: "observe-and-clean that settled the observation",
			run: egressRun{state: StateUpdateInProgress, mode: managed, desired: egressUnknown, applied: &consensusApplied,
				inFlight: &egressUnknown, owed: true, retries: 2, votes: noAgreement},
			outcome: &egressOutcome{Mode: managed, Applied: &consensusApplied, Swept: true}, wantState: StateCreated},
		{name: "creation whose desired configuration changed meanwhile, mode not resolved: Created",
			run:       egressRun{state: StateCreationInProgress, desired: consensusB, inFlight: &consensusA, votes: agreeB},
			wantState: StateCreated},
		{name: "converged creation", run: creation(consensusA, agreeA, true),
			outcome: &egressOutcome{Mode: managed, Applied: &consensusA, Swept: true}, wantState: StateCreated},
		{name: "creation with an incomplete sweep: observation after a backoff", run: creation(consensusA, agreeA, true),
			outcome:   &egressOutcome{Mode: managed, Applied: &consensusA},
			wantState: StateUpdateInProgress, wantDesired: egressUnknown, wantRetries: 1, wantBackoff: true},
		{name: "creation fell back to the default: the agreed configuration is retried in the background",
			run: creation(consensusRejected, agreeRejected, true),
			outcome: &egressOutcome{Mode: managed, Applied: &consensusApplied, Swept: true,
				Rejected: &egressRejection{Config: consensusRejected, Reason: "quota", Retryable: true}},
			wantState: StateUpdateInProgress, wantDesired: consensusRejected, wantRetries: 1, wantBackoff: true},
		{name: "BYO residue cleanup that swept: done",
			run: egressRun{state: StateUpdateInProgress, mode: egressModeBYO, desired: egressUnknown, applied: &egressUnknown,
				inFlight: &egressUnknown, wrote: true, owed: true, retries: 2, votes: agreeA},
			outcome: &egressOutcome{Mode: egressModeBYO, Swept: true}, wantState: StateCreated},
		{name: "incomplete BYO residue cleanup: again after a backoff",
			run: egressRun{state: StateUpdateInProgress, mode: egressModeBYO, desired: egressUnknown, applied: &egressUnknown,
				inFlight: &egressUnknown, wrote: true, owed: true, retries: 2, votes: agreeA},
			outcome:   &egressOutcome{Mode: egressModeBYO},
			wantState: StateUpdateInProgress, wantDesired: egressUnknown, wantRetries: 3, wantBackoff: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dt := newTestDiffTracker()
			op := trackEgressRun(dt, tc.run)

			dt.onServiceOperationComplete(egressRunID, true, nil, tc.outcome)

			assert.Equal(t, tc.wantState, op.State)
			assert.Equal(t, tc.wantRetries, op.RetryCount)
			assertBackoff(t, op, tc.wantBackoff)
			if tc.wantState != StateUpdateInProgress {
				assert.Empty(t, dt.serviceUpdaterTrigger, "nothing left to do must not trigger a run")
				return
			}
			assert.Equal(t, tc.wantDesired, outboundEgress(op.Config.OutboundConfig))
			assert.Len(t, dt.serviceUpdaterTrigger, 1)

			drainServiceUpdaterTrigger(dt)
			dispatchWithoutRunning(t, dt)
			assert.Equal(t, !tc.wantBackoff, op.InFlightConfig != nil, "only new intent is dispatched at once")
		})
	}
}

// TestOnOutboundComplete_UnconvergedSuccessesExhaustTheBudget pins that runs that keep succeeding without
// completing their sweep cannot loop: every follow-up counts as a retry, so the retry budget parks the identity.
func TestOnOutboundComplete_UnconvergedSuccessesExhaustTheBudget(t *testing.T) {
	dt := newTestDiffTracker()
	op := trackEgressRun(dt, egressRun{state: StateUpdateInProgress, mode: egressModeManaged, desired: egressUnknown,
		applied: &consensusApplied, owed: true})

	for attempt := 1; attempt <= maxServiceRetries; attempt++ {
		op.NextRetryAt = time.Time{}
		dispatchWithoutRunning(t, dt)
		if !assert.NotNil(t, op.InFlightConfig, "attempt %d must be dispatched", attempt) {
			return
		}
		dt.noteEgressMutationStarted(egressRunID)
		dt.onServiceOperationComplete(egressRunID, true, nil, &egressOutcome{Mode: egressModeManaged, Applied: &consensusApplied})
		assert.Equal(t, attempt, op.RetryCount)
		assert.Equal(t, StateUpdateInProgress, op.State)
		drainServiceUpdaterTrigger(dt)
	}

	op.NextRetryAt = time.Time{}
	dispatchWithoutRunning(t, dt)
	assert.Nil(t, op.InFlightConfig, "the exhausted budget must park the identity")
	assert.True(t, op.RetriesExhausted)
}

// TestOnOutboundComplete_ObligationRaisedInFlightSurvives pins the observation generation the dispatcher records
// with a run: an obligation raised while an apply, an observe-and-clean or a deletion is in flight (as the
// startup confirmation pass raises one) survives that run's completion, even when the run swept everything,
// because the run was dispatched before it. The apply and the observe-and-clean are followed by an observation;
// the deletion makes one more pass before it completes, and only that pass forgets the identity, also when the
// identity is re-created afterwards.
func TestOnOutboundComplete_ObligationRaisedInFlightSurvives(t *testing.T) {
	const managed = egressModeManaged
	apply := egressRun{state: StateUpdateInProgress, mode: managed, desired: consensusA, applied: &consensusApplied,
		votes: []*EgressConfig{&consensusA}}
	observe := egressRun{state: StateUpdateInProgress, mode: managed, desired: egressUnknown, applied: &consensusApplied,
		owed: true, votes: []*EgressConfig{&consensusA, &consensusB}}
	deletion := egressRun{state: StateDeletionInProgress, mode: managed, desired: consensusApplied, applied: &consensusApplied}

	cases := []struct {
		name     string
		run      egressRun
		recreate bool
		outcome  *egressOutcome
	}{
		{"apply", apply, false, &egressOutcome{Mode: managed, Applied: &consensusA, Swept: true}},
		{"observe-and-clean", observe, false, &egressOutcome{Mode: managed, Applied: &consensusApplied, Swept: true}},
		{"deletion", deletion, false, &egressOutcome{Mode: managed, Swept: true}},
		{"deletion followed by a re-creation", deletion, true, &egressOutcome{Mode: managed, Swept: true}},
	}

	for _, tc := range cases {
		for _, raised := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s, obligation raised in flight: %v", tc.name, raised), func(t *testing.T) {
				dt := newTestDiffTracker()
				op := trackEgressRun(dt, tc.run)
				op.Egress.ObservationGen, op.Egress.DispatchedObservationGen = 0, 0
				if tc.recreate {
					dt.pendingPods[egressRunID] = []PendingPodUpdate{{PodKey: "ns/pod-0", PodUID: "ns/pod-0-uid", Location: "10.0.0.1", Address: "10.244.0.5"}}
				}
				dispatchWithoutRunning(t, dt)
				if !assert.False(t, op.OperationStartedAt.IsZero(), "control: the run must be dispatched") {
					return
				}
				if raised {
					dt.mu.Lock()
					op.Egress.requireObservation()
					dt.mu.Unlock()
				}

				dt.onServiceOperationComplete(egressRunID, true, nil, tc.outcome)

				if tc.run.state != StateDeletionInProgress {
					assert.Equal(t, raised, op.Egress.NeedsObservation, "only an obligation raised in flight survives the sweep")
					if raised {
						assert.Equal(t, StateUpdateInProgress, op.State)
						assert.Equal(t, egressUnknown, outboundEgress(op.Config.OutboundConfig), "the next run observes")
						assertBackoff(t, op, true)
					} else {
						assert.Equal(t, StateCreated, op.State)
					}
					return
				}

				if raised {
					assert.Same(t, op, dt.pendingServiceOps[egressRunID], "the deletion must make one more pass")
					assert.Equal(t, StateDeletionInProgress, op.State)
					assert.True(t, op.Egress.NeedsObservation)
					assert.Equal(t, egressModeManaged, dt.egress.mode[egressRunID], "the identity is forgotten only when the deletion completes")
					assert.Equal(t, 1, op.RetryCount, "the extra pass goes through the retry backoff")
					assertBackoff(t, op, true)
					assert.Len(t, dt.serviceUpdaterTrigger, 1)

					op.NextRetryAt = time.Time{}
					dispatchWithoutRunning(t, dt)
					dt.onServiceOperationComplete(egressRunID, true, nil, tc.outcome)
				}
				_, known := dt.egress.mode[egressRunID]
				assert.False(t, known, "a completed deletion forgets the identity's mode")
				if tc.recreate {
					assert.Equal(t, StateNotStarted, dt.pendingServiceOps[egressRunID].State, "the identity is re-created afterwards")
					assert.False(t, op.Egress.NeedsObservation)
					assert.Zero(t, op.Egress.ObservationGen)
				} else {
					assert.NotContains(t, dt.pendingServiceOps, egressRunID, "the deletion completes")
				}
			})
		}
	}
}

// TestOnOutboundComplete_DeletionForgetsTheIdentity pins what a deletion that really completed drops, so that a
// re-creation starts afresh: the mode, the rejections and warnings, the owed observation with its generation, the
// startup baseline, and what was remembered while the applied configuration was unknown. The votes are kept, as the pods may still exist, and so is the quiet period. An inbound
// service under the same key leaves the egress records alone.
func TestOnOutboundComplete_DeletionForgetsTheIdentity(t *testing.T) {
	cases := []struct {
		name     string
		inbound  bool
		recreate bool
	}{
		{name: "deletion completes"},
		{name: "deletion followed by a re-creation", recreate: true},
		{name: "inbound service under the same key", inbound: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dt := newTestDiffTracker()
			op := trackEgressRun(dt, egressRun{state: StateDeletionInProgress, mode: egressModeManaged, desired: consensusA,
				applied: &egressUnknown, lastKnown: &consensusApplied, unclear: []EgressConfig{consensusA}, owed: true, baseline: true,
				votes: []*EgressConfig{&consensusA, &consensusB}})
			quietUntil := time.Now().Add(time.Minute)
			op.Egress.QuietUntil = quietUntil
			if tc.inbound {
				op.Config = NewInboundServiceConfig(egressRunID, makeInboundConfig(80))
			}
			if tc.recreate {
				dt.pendingPods[egressRunID] = []PendingPodUpdate{{PodKey: "ns/pod-0", PodUID: "ns/pod-0-uid", Location: "10.0.0.1", Address: "10.244.0.5"}}
			}
			dt.egress.rejected[egressRunID] = egressRejection{Config: consensusRejected, Reason: "too many tags"}
			dt.egress.warned[egressRunID] = egressRejection{Config: consensusB, Reason: "quota", Retryable: true}

			// No outcome, as from a deletion that reports none: what is forgotten does not depend on rule 3.
			dt.OnServiceCreationComplete(egressRunID, true, nil)

			_, hasMode := dt.egress.mode[egressRunID]
			_, hasRejected := dt.egress.rejected[egressRunID]
			_, hasWarned := dt.egress.warned[egressRunID]
			assert.Equal(t, tc.inbound, hasMode)
			assert.Equal(t, tc.inbound, hasRejected)
			assert.Equal(t, tc.inbound, hasWarned)
			assert.Equal(t, 2, dt.egress.votes.members(egressRunID), "the votes must be kept")
			if !tc.recreate {
				assert.NotContains(t, dt.pendingServiceOps, egressRunID)
				return
			}
			assert.Equal(t, StateNotStarted, op.State)
			assert.False(t, op.Egress.NeedsObservation)
			assert.Zero(t, op.Egress.ObservationGen)
			assert.Zero(t, op.Egress.DispatchedObservationGen)
			assert.False(t, op.Egress.StartupBaseline)
			assert.Nil(t, op.Egress.LastKnownApplied, "the re-created identity is announced against nothing it had before")
			assert.Nil(t, op.Egress.UnclearWrites)
			assert.Equal(t, quietUntil, op.Egress.QuietUntil, "the quiet period must survive a re-creation")
		})
	}
}

// TestAddPod_RevivalKeepsWhatIsKnownAboutTheIdentity pins that a revival from a deletion that was not dispatched
// keeps the identity's mode, rejections, warnings and owed observation: its NAT Gateway was never deleted.
func TestAddPod_RevivalKeepsWhatIsKnownAboutTheIdentity(t *testing.T) {
	dt := newTestDiffTracker()
	op := trackEgressRun(dt, egressRun{state: StateDeletionPending, mode: egressModeManaged, desired: consensusA,
		applied: &consensusA, owed: true})
	dt.pendingServiceDeletions[egressRunID] = &PendingServiceDeletion{ServiceUID: egressRunID}
	dt.NRPResources.NATGateways.Insert(egressRunID)
	rejected := egressRejection{Config: consensusRejected, Reason: "too many tags"}
	warned := egressRejection{Config: consensusB, Reason: "quota", Retryable: true}
	dt.egress.rejected[egressRunID] = rejected
	dt.egress.warned[egressRunID] = warned

	dt.AddPodWithUID(egressRunID, "ns/pod", "pod-uid", "10.0.0.1", "10.244.0.5")

	assert.Equal(t, StateCreated, op.State, "control: the identity is revived")
	assert.Equal(t, egressModeManaged, dt.egress.mode[egressRunID])
	assert.Equal(t, rejected, dt.egress.rejected[egressRunID])
	assert.Equal(t, warned, dt.egress.warned[egressRunID])
	assert.True(t, op.Egress.NeedsObservation)
	assert.Equal(t, uint64(7), op.Egress.ObservationGen)
}

// TestAddPod_RevivalKeepsWhatWasRememberedWhileUnknown pins that a revival from a deletion that was not dispatched,
// of an identity this process applied before, keeps what it remembered while its applied configuration is unknown:
// the write Azure did not answer clearly is still found and announced.
func TestAddPod_RevivalKeepsWhatWasRememberedWhileUnknown(t *testing.T) {
	dt := newTestDiffTracker()
	op := trackEgressRun(dt, egressRun{state: StateDeletionPending, mode: egressModeManaged, desired: egressUnknown,
		applied: &egressUnknown, lastKnown: &consensusApplied, unclear: []EgressConfig{consensusA}, owed: true})
	dt.pendingServiceDeletions[egressRunID] = &PendingServiceDeletion{ServiceUID: egressRunID}
	dt.NRPResources.NATGateways.Insert(egressRunID)

	dt.AddPodWithUID(egressRunID, "ns/pod", "pod-uid", "10.0.0.1", "10.244.0.5")

	assert.Equal(t, StateCreated, op.State, "control: the identity is revived")
	assert.Equal(t, &consensusApplied, op.Egress.LastKnownApplied)
	assert.Equal(t, []EgressConfig{consensusA}, op.Egress.UnclearWrites)
}

// TestOnOutboundComplete_BYOIdentityRecreatedAsManagedCountsItsVotes pins that annotated votes, invalid while
// their identity is BYO, count again once the identity is deleted and re-created as a managed one: the
// invalidity was derived from the mode, which the deletion forgot.
func TestOnOutboundComplete_BYOIdentityRecreatedAsManagedCountsItsVotes(t *testing.T) {
	dt := newTestDiffTracker()
	op := trackEgressRun(dt, egressRun{state: StateDeletionInProgress, mode: egressModeBYO, desired: egressUnknown,
		applied: &egressUnknown, votes: []*EgressConfig{&consensusA, &consensusA}})
	dt.pendingPods[egressRunID] = []PendingPodUpdate{{PodKey: "ns/pod-0", PodUID: "ns/pod-0-uid", Location: "10.0.0.1", Address: "10.244.0.5"}}

	dt.onServiceOperationComplete(egressRunID, true, nil, &egressOutcome{Mode: egressModeBYO, Swept: true})

	if !assert.Equal(t, StateNotStarted, op.State, "control: the identity is re-created") {
		return
	}

	dispatchWithoutRunning(t, dt)
	if !assert.NotNil(t, op.InFlightConfig, "control: the creation is dispatched") {
		return
	}
	assert.Equal(t, consensusA, outboundEgress(op.InFlightConfig.OutboundConfig), "the votes decide the creation configuration again")
	dt.onServiceOperationComplete(egressRunID, true, nil, &egressOutcome{Mode: egressModeManaged, Applied: &consensusA, Swept: true})

	assert.Equal(t, egressModeManaged, dt.egress.mode[egressRunID])
	assert.Equal(t, StateCreated, op.State)
	assert.Equal(t, []string{consts.ServiceEgressGatewayReasonConfigApplied + " ns/pod-0", consts.ServiceEgressGatewayReasonConfigApplied + " ns/pod-1"},
		takeEgressEvents(dt), "the members are told about the configuration they agreed on")

	dt.mu.Lock()
	castEgressVote(dt, egressRunID, "ns/pod-0", &consensusB, consensusEpoch)
	castEgressVote(dt, egressRunID, "ns/pod-1", &consensusB, consensusEpoch)
	dt.evaluateEgressConsensusLocked(egressRunID)
	dt.mu.Unlock()
	assert.Equal(t, StateUpdateInProgress, op.State, "a new agreement of the former BYO members is applied")
	assert.Equal(t, consensusB, outboundEgress(op.Config.OutboundConfig))
}

// TestOnOutboundComplete_PreemptedRunsCountAsWhatTheyWere pins that a run preempted by a deletion is counted as
// the operation it was, judged before its outcome is recorded: a first creation whose outcome records an applied
// configuration is still counted as a creation.
func TestOnOutboundComplete_PreemptedRunsCountAsWhatTheyWere(t *testing.T) {
	cases := []struct {
		name      string
		applied   *EgressConfig
		operation string
	}{
		{"first creation", nil, "create"},
		{"update", &consensusApplied, "update"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			RegisterMetrics()
			serviceOperationTotal.Reset()
			dt := newTestDiffTracker()
			op := trackEgressRun(dt, egressRun{state: StateDeletionPending, mode: egressModeManaged, desired: consensusA,
				applied: tc.applied, inFlight: &consensusA, wrote: true})

			dt.onServiceOperationComplete(egressRunID, true, nil, &egressOutcome{Mode: egressModeManaged, Applied: &consensusA, Swept: true})

			assert.Equal(t, &consensusA, appliedEgressOf(op), "control: the outcome recorded an applied configuration first")
			testutil.AssertVectorCount(t, "difftracker_engine_service_operations_total",
				map[string]string{"operation": tc.operation, "service_type": "outbound", "result": "success"}, 1)
			testutil.AssertVectorCount(t, "difftracker_engine_service_operations_total", map[string]string{}, 1)
		})
	}
}

// TestOnOutboundComplete_RecordsRealUpdates pins the outbound update metrics (4.8): an update run that reports an
// outcome made real Azure calls, so it is recorded like an inbound update, success or error, and observes its
// retries once, when the update ends.
func TestOnOutboundComplete_RecordsRealUpdates(t *testing.T) {
	const managed = egressModeManaged
	cases := []struct {
		name        string
		votes       []*EgressConfig
		success     bool
		outcome     *egressOutcome
		wantResult  string // empty: nothing recorded
		wantRetries uint64
		wantApplied EgressConfig
	}{
		{name: "success", votes: []*EgressConfig{&consensusA}, success: true,
			outcome: &egressOutcome{Mode: managed, Applied: &consensusA, Swept: true}, wantResult: "success", wantRetries: 1,
			wantApplied: consensusA},
		{name: "failure that is retried", votes: []*EgressConfig{&consensusA},
			outcome: &egressOutcome{Mode: managed}, wantResult: "error", wantApplied: consensusApplied},
		{name: "failure that ends the update", votes: []*EgressConfig{&consensusA, &consensusB},
			outcome: &egressOutcome{Mode: managed}, wantResult: "error", wantRetries: 1, wantApplied: consensusApplied},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			RegisterMetrics()
			serviceOperationTotal.Reset()
			serviceOperationRetries.Reset()
			dt := newTestDiffTracker()
			op := trackEgressRun(dt, egressRun{state: StateUpdateInProgress, mode: managed, desired: consensusA,
				applied: &consensusApplied, inFlight: &consensusA, votes: tc.votes})
			var err error
			if !tc.success {
				err = errors.New("run failed")
			}

			dt.onServiceOperationComplete(egressRunID, tc.success, err, tc.outcome)

			assert.Equal(t, &tc.wantApplied, appliedEgressOf(op))
			wantRecorded := 0
			if tc.wantResult != "" {
				wantRecorded = 1
				testutil.AssertVectorCount(t, "difftracker_engine_service_operations_total",
					map[string]string{"operation": "update", "service_type": "outbound", "result": tc.wantResult}, 1)
			}
			testutil.AssertVectorCount(t, "difftracker_engine_service_operations_total", map[string]string{}, wantRecorded)
			retries, metricErr := testutil.GetHistogramMetricCount(serviceOperationRetries.WithLabelValues("update", "outbound"))
			assert.NoError(t, metricErr)
			assert.Equal(t, tc.wantRetries, retries, "the retries are observed once, when the update ends")
		})
	}
}

// TestEvaluateEgressConsensusLocked_BYOResidue pins the residue rule: a BYO identity accepts no
// configuration, whatever its members agree on, but when it is owed an observation (startup found managed
// resources of its managed past) it gets a residue cleanup: an update whose desired configuration is the marker
// egressUnknown. A residue cleanup not dispatched yet is cancelled once nothing is owed; one in flight, a
// creation and a deletion are left alone.
func TestEvaluateEgressConsensusLocked_BYOResidue(t *testing.T) {
	cases := []struct {
		name           string
		state          ResourceState
		owed           bool
		inFlight       bool
		desired        EgressConfig
		wantState      ResourceState
		wantDesired    EgressConfig
		wantTrigger    bool
		wantRetryReset bool
	}{
		{name: "created and owed: residue cleanup", state: StateCreated, owed: true, desired: egressUnknown,
			wantState: StateUpdateInProgress, wantDesired: egressUnknown, wantTrigger: true},
		{name: "created, nothing owed: nothing, whatever the agreement", state: StateCreated, desired: egressUnknown,
			wantState: StateCreated, wantDesired: egressUnknown},
		{name: "residue cleanup queued and still owed", state: StateUpdateInProgress, owed: true, desired: egressUnknown,
			wantState: StateUpdateInProgress, wantDesired: egressUnknown},
		{name: "another update queued while owed becomes a residue cleanup", state: StateUpdateInProgress, owed: true, desired: consensusA,
			wantState: StateUpdateInProgress, wantDesired: egressUnknown},
		{name: "residue cleanup queued, nothing owed any more: cancelled", state: StateUpdateInProgress, desired: egressUnknown,
			wantState: StateCreated, wantDesired: egressUnknown, wantRetryReset: true},
		{name: "residue cleanup in flight runs to completion", state: StateUpdateInProgress, inFlight: true, desired: egressUnknown,
			wantState: StateUpdateInProgress, wantDesired: egressUnknown},
		{name: "creation queued", state: StateNotStarted, owed: true, desired: consensusA,
			wantState: StateNotStarted, wantDesired: consensusA},
		{name: "deletion in progress", state: StateDeletionInProgress, owed: true, desired: consensusA,
			wantState: StateDeletionInProgress, wantDesired: consensusA},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dt := newTestDiffTracker()
			r := egressRun{state: tc.state, mode: egressModeBYO, desired: tc.desired, applied: &egressUnknown, owed: tc.owed,
				retries: 3, votes: []*EgressConfig{&consensusA, &consensusA}}
			if tc.inFlight {
				r.inFlight = &egressUnknown
			}
			op := trackEgressRun(dt, r)
			published := op.Config.OutboundConfig
			publishedBefore := published.DeepCopy()

			dt.mu.Lock()
			dt.evaluateEgressConsensusLocked(egressRunID)
			dt.mu.Unlock()

			assert.Equal(t, tc.wantState, op.State)
			assert.Equal(t, tc.wantDesired, outboundEgress(op.Config.OutboundConfig), "a BYO identity never desires a configuration")
			assert.Equal(t, ipv4Families(), op.Config.OutboundConfig.IPFamilies)
			assert.Equal(t, publishedBefore, published, "a published config must never be modified")
			assert.Equal(t, tc.wantTrigger, len(dt.serviceUpdaterTrigger) == 1)
			assert.Equal(t, tc.owed, op.Egress.NeedsObservation, "only a run settles an owed observation")
			if tc.wantRetryReset {
				assert.Zero(t, op.RetryCount)
			} else {
				assert.Equal(t, 3, op.RetryCount)
			}
		})
	}
}

// eventLockRecorder records whether dt.mu was held while an event was emitted.
type eventLockRecorder struct {
	record.FakeRecorder
	dt         *DiffTracker
	heldByEmit bool
}

func (r *eventLockRecorder) Event(object runtime.Object, eventType, reason, message string) {
	if r.dt.mu.TryLock() {
		r.dt.mu.Unlock()
	} else {
		r.heldByEmit = true
	}
	r.FakeRecorder.Event(object, eventType, reason, message)
}

// TestOnOutboundComplete_EmitsEgressEventsWithoutTheLock pins that the egress events of a completion are emitted
// once dt.mu is released, as the recorder performs I/O, in the order they were raised.
func TestOnOutboundComplete_EmitsEgressEventsWithoutTheLock(t *testing.T) {
	dt := newTestDiffTracker()
	recorder := &eventLockRecorder{FakeRecorder: *record.NewFakeRecorder(10), dt: dt}
	dt.eventRecorder = recorder
	trackEgressRun(dt, egressRun{state: StateUpdateInProgress, mode: egressModeManaged, desired: consensusRejected,
		applied: &consensusApplied, inFlight: &consensusRejected, votes: []*EgressConfig{&consensusRejected, &consensusRejected}})

	dt.onServiceOperationComplete(egressRunID, false, newTerminalError(errors.New("rejected")), &egressOutcome{
		Mode: egressModeManaged, Rejected: &egressRejection{Config: consensusRejected, Reason: "too many tags"}})

	events := drainEvents(&recorder.FakeRecorder)
	if assert.Len(t, events, 2, "one event per voter") {
		assert.True(t, strings.HasPrefix(events[0], v1.EventTypeWarning+" "+consts.ServiceEgressGatewayReasonConfigRejected+" "))
	}
	assert.False(t, recorder.heldByEmit, "an event must not be emitted while dt.mu is held")
	assert.Empty(t, dt.egress.pendingEvents, "nothing is queued once a recorder is published")
}

// TestOnOutboundComplete_NeverModifiesASnapshot pins under -race that a run reading its dispatched snapshot and an
// earlier applied config without dt.mu never races the completions and evaluations that record new applied and
// desired configs, because those are new values.
func TestOnOutboundComplete_NeverModifiesASnapshot(t *testing.T) {
	dt := newTestDiffTracker()
	op := trackEgressRun(dt, egressRun{state: StateUpdateInProgress, mode: egressModeManaged, desired: consensusA,
		applied: &consensusApplied, inFlight: &consensusA, votes: []*EgressConfig{&consensusA}})
	dispatched, applied := op.InFlightConfig, op.LastAppliedConfig
	appliedOutcome := consensusB

	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
			}
			_ = dispatched.OutboundConfig.Egress.Summary() + applied.OutboundConfig.Egress.Summary() + appliedOutcome.Summary()
			_ = slices.Clone(dispatched.OutboundConfig.IPFamilies)
			_ = slices.Clone(applied.OutboundConfig.IPFamilies)
		}
	}()

	outcomes := []*egressOutcome{
		{Mode: egressModeManaged, Applied: &appliedOutcome, Swept: true},
		{Mode: egressModeManaged, CommitUnknown: true},
		{Mode: egressModeManaged, Applied: &appliedOutcome, Rejected: &egressRejection{Config: consensusA, Reason: "quota", Retryable: true}},
	}
	for i := 0; i < 300; i++ {
		dt.mu.Lock()
		op.State, op.InFlightConfig, op.Egress.QuietUntil = StateUpdateInProgress, dispatched, time.Time{}
		dt.mu.Unlock()
		var err error
		if i%2 == 1 {
			err = errors.New("run failed")
		}
		dt.onServiceOperationComplete(egressRunID, err == nil, err, outcomes[i%len(outcomes)])
		drainServiceUpdaterTrigger(dt)
	}
	close(stop)
	<-done

	assert.Equal(t, consensusA, dispatched.OutboundConfig.Egress, "a dispatched snapshot must never be modified")
	assert.Equal(t, ipv4Families(), dispatched.OutboundConfig.IPFamilies)
	assert.Equal(t, consensusApplied, applied.OutboundConfig.Egress, "an earlier applied config must never be modified")
	assert.Equal(t, consensusB, appliedOutcome, "the outcome must never be modified")
}

// TestOnOutboundComplete_QuietPeriodSurvivesPreemptionAndLateLastPod pins that the quiet period a run started
// holds back whatever follows it: the deletion that preempted the run, and the deletion pass re-dispatched for a
// late last-pod record.
func TestOnOutboundComplete_QuietPeriodSurvivesPreemptionAndLateLastPod(t *testing.T) {
	t.Run("deletion preemption", func(t *testing.T) {
		dt := newTestDiffTracker()
		op := trackEgressRun(dt, egressRun{state: StateDeletionPending, mode: egressModeManaged, desired: consensusA,
			applied: &consensusApplied, inFlight: &consensusA, wrote: true})

		dt.onServiceOperationComplete(egressRunID, false, errors.New("poll timed out"),
			&egressOutcome{Mode: egressModeManaged, CommitUnknown: true})

		if !assert.Equal(t, StateDeletionInProgress, op.State, "control: the deletion takes over") {
			return
		}
		assert.True(t, op.Egress.QuietUntil.After(time.Now()))
		dispatchWithoutRunning(t, dt)
		assert.True(t, op.OperationStartedAt.IsZero(), "the deletion must wait for the end of the quiet period")
	})

	t.Run("late last-pod re-dispatch", func(t *testing.T) {
		dt := newTestDiffTracker()
		op := trackEgressRun(dt, egressRun{state: StateDeletionInProgress, mode: egressModeManaged, desired: consensusApplied,
			applied: &consensusApplied})
		quietUntil := time.Now().Add(time.Hour)
		op.Egress.QuietUntil = quietUntil
		dt.pendingPodDeletions["ns/pod"] = &PendingPodDeletion{Namespace: "ns", Name: "pod", ServiceUID: egressRunID, IsLastPod: true}

		dt.onServiceOperationComplete(egressRunID, true, nil, &egressOutcome{Mode: egressModeManaged, Swept: true})

		assert.Equal(t, StateDeletionInProgress, op.State, "control: the deletion is re-dispatched for the late record")
		assert.Equal(t, quietUntil, op.Egress.QuietUntil, "the re-dispatch must keep the quiet period")
	})
}

// TestOnOutboundComplete_LateCommitIsObservedAfterTheQuietPeriod pins the engine side of a late commit: an update
// whose NAT Gateway PUT ended commit unknown leaves the applied configuration unknown, remembers the configuration
// known before and the one it was writing, and holds the identity back for the quiet period; afterwards an
// observe-and-clean runs, which looks for the configuration of that write although the members no longer agree on
// it, a busy observation records nothing and waits out a backoff, and the next one records what the NAT Gateway
// satisfies, announces it to every member, as the update would have, and settles the observation.
func TestOnOutboundComplete_LateCommitIsObservedAfterTheQuietPeriod(t *testing.T) {
	dt := newTestDiffTracker()
	op := trackEgressRun(dt, egressRun{state: StateCreated, mode: egressModeManaged, desired: consensusApplied,
		applied: &consensusApplied, votes: []*EgressConfig{&consensusA, &consensusA}})
	op.Egress.ObservationGen, op.Egress.DispatchedObservationGen = 0, 0

	dt.mu.Lock()
	dt.evaluateEgressConsensusLocked(egressRunID)
	dt.mu.Unlock()
	dispatchWithoutRunning(t, dt)
	if !assert.NotNil(t, op.InFlightConfig, "control: the agreed configuration is dispatched") {
		return
	}
	dt.noteEgressMutationStarted(egressRunID)
	dt.mu.Lock()
	castEgressVote(dt, egressRunID, "ns/pod-1", &consensusB, consensusEpoch)
	dt.evaluateEgressConsensusLocked(egressRunID)
	dt.mu.Unlock()
	dt.onServiceOperationComplete(egressRunID, false, errors.New("poll timed out"), &egressOutcome{Mode: egressModeManaged, CommitUnknown: true})

	assert.Equal(t, egressUnknown, *appliedEgressOf(op))
	assert.Equal(t, &consensusApplied, op.Egress.LastKnownApplied, "the configuration known before the write is remembered")
	assert.Equal(t, []EgressConfig{consensusA}, op.Egress.UnclearWrites, "and the configuration of the write")
	assert.Equal(t, egressUnknown, outboundEgress(op.Config.OutboundConfig), "without agreement the next run observes")
	assert.Equal(t, []*EgressConfig{nil, &consensusA, &consensusApplied}, dt.egressObservationTargets(egressRunID),
		"without agreement, the observation looks for the configuration of the write, then for the one known before it")
	dispatchWithoutRunning(t, dt)
	assert.Nil(t, op.InFlightConfig, "nothing is dispatched during the quiet period")

	op.Egress.QuietUntil, op.NextRetryAt = time.Now().Add(-time.Second), time.Time{}
	dispatchWithoutRunning(t, dt)
	if !assert.NotNil(t, op.InFlightConfig, "the observation is dispatched once the quiet period is over") {
		return
	}
	dt.onServiceOperationComplete(egressRunID, false, errors.New("NAT Gateway is Updating"), &egressOutcome{Mode: egressModeManaged})
	assert.Equal(t, egressUnknown, *appliedEgressOf(op), "a busy run records nothing")
	assert.True(t, op.Egress.NeedsObservation)
	assertBackoff(t, op, true)

	op.NextRetryAt = time.Time{}
	dispatchWithoutRunning(t, dt)
	if !assert.NotNil(t, op.InFlightConfig) {
		return
	}
	dt.onServiceOperationComplete(egressRunID, true, nil, &egressOutcome{Mode: egressModeManaged, Applied: &consensusA, Swept: true})
	assert.Equal(t, consensusA, *appliedEgressOf(op), "the late commit is classified")
	assert.False(t, op.Egress.NeedsObservation, "and swept")
	assert.Equal(t, StateCreated, op.State)
	assert.Nil(t, op.Egress.LastKnownApplied, "a known configuration forgets what was remembered")
	assert.Nil(t, op.Egress.UnclearWrites)
	reason := consts.ServiceEgressGatewayReasonConfigApplied
	assert.Equal(t, []string{reason + " ns/pod-0", reason + " ns/pod-1"}, takeEgressEvents(dt),
		"the late commit is announced to every member, as the update would have been")
}

// TestAddPod_RevivalDuringAnInFlightRun pins a revival from a pending deletion while a run of the identity is
// in flight: the deletion is cancelled, and the identity returns to the state of the run in flight, so an
// apply, an observe-and-clean or a BYO residue cleanup is still an update in flight (not Created) and its
// completion is processed as that update. Without a run in flight the identity is Created, as before; a
// creation in flight is not revived, nor an identity that is not registered, whose creation never ran: the pod
// waits. Earlier failed attempts of an update, in flight or backing off, do not prevent a revival: the deletion
// started with a fresh retry budget.
func TestAddPod_RevivalDuringAnInFlightRun(t *testing.T) {
	const managed = egressModeManaged
	cases := []struct {
		name         string
		run          egressRun
		dispatch     bool
		backingOff   bool
		unregistered bool // the identity was never created
		outcome      *egressOutcome
		wantKind     operationKind
		wantRevived  ResourceState // StateDeletionPending: not revived
	}{
		{name: "apply in flight", dispatch: true,
			run: egressRun{state: StateUpdateInProgress, mode: managed, desired: consensusA, applied: &consensusApplied,
				votes: []*EgressConfig{&consensusA}},
			outcome: &egressOutcome{Mode: managed, Applied: &consensusA, Swept: true}, wantKind: operationUpdate,
			wantRevived: StateUpdateInProgress},
		{name: "apply in flight after failed attempts", dispatch: true,
			run: egressRun{state: StateUpdateInProgress, mode: managed, desired: consensusA, applied: &consensusApplied,
				votes: []*EgressConfig{&consensusA}, retries: 2},
			outcome: &egressOutcome{Mode: managed, Applied: &consensusA, Swept: true}, wantKind: operationUpdate,
			wantRevived: StateUpdateInProgress},
		{name: "observe-and-clean in flight", dispatch: true,
			run: egressRun{state: StateUpdateInProgress, mode: managed, desired: egressUnknown, applied: &consensusApplied, owed: true,
				votes: []*EgressConfig{&consensusA, &consensusB}},
			outcome: &egressOutcome{Mode: managed, Applied: &consensusApplied, Swept: true}, wantKind: operationUpdate,
			wantRevived: StateUpdateInProgress},
		{name: "BYO residue cleanup in flight", dispatch: true,
			run: egressRun{state: StateUpdateInProgress, mode: egressModeBYO, desired: egressUnknown, applied: &egressUnknown, owed: true,
				votes: []*EgressConfig{&consensusA}},
			outcome: &egressOutcome{Mode: egressModeBYO, Swept: true}, wantKind: operationEgressResidueCleanup,
			wantRevived: StateUpdateInProgress},
		{name: "BYO residue cleanup in flight after a failed attempt", dispatch: true,
			run: egressRun{state: StateUpdateInProgress, mode: egressModeBYO, desired: egressUnknown, applied: &egressUnknown, owed: true,
				votes: []*EgressConfig{&consensusA}, retries: 1},
			outcome: &egressOutcome{Mode: egressModeBYO, Swept: true}, wantKind: operationEgressResidueCleanup,
			wantRevived: StateUpdateInProgress},
		{name: "no run in flight",
			run:         egressRun{state: StateCreated, mode: managed, desired: consensusApplied, applied: &consensusApplied},
			wantRevived: StateCreated},
		{name: "update backing off after a failed attempt", backingOff: true,
			run: egressRun{state: StateUpdateInProgress, mode: managed, desired: consensusA, applied: &consensusApplied,
				votes: []*EgressConfig{&consensusA}, retries: 1},
			wantRevived: StateCreated},
		{name: "creation in flight", dispatch: true, unregistered: true,
			run:      egressRun{state: StateNotStarted, mode: managed, desired: consensusA},
			wantKind: operationCreate, wantRevived: StateDeletionPending},
		{name: "creation not dispatched yet", unregistered: true,
			run:         egressRun{state: StateNotStarted, mode: managed, desired: consensusA},
			wantRevived: StateDeletionPending},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			RegisterMetrics()
			serviceOperationTotal.Reset()
			dt := newTestDiffTracker()
			op := trackEgressRun(dt, tc.run)
			op.Egress.ObservationGen, op.Egress.DispatchedObservationGen = 0, 0
			if !tc.unregistered {
				dt.NRPResources.NATGateways.Insert(egressRunID)
			}
			if tc.dispatch {
				dispatchWithoutRunning(t, dt)
			}
			if tc.backingOff {
				op.NextRetryAt = time.Now().Add(time.Minute)
			}
			if !assert.Equal(t, tc.wantKind, op.InFlightKind, "control: the dispatcher records the kind of the run") {
				return
			}
			inFlight := op.InFlightConfig
			dt.mu.Lock()
			dt.markOutboundServiceForDeletionLocked(egressRunID)
			dt.mu.Unlock()
			if !assert.Equal(t, StateDeletionPending, op.State, "control: the last pod left") {
				return
			}

			dt.AddPodWithUID(egressRunID, "ns/back", "back-uid", "10.0.0.1", "10.244.0.9")

			assert.Equal(t, tc.wantRevived, op.State)
			assert.Same(t, inFlight, op.InFlightConfig, "the snapshot of the run in flight must be kept")
			assert.Equal(t, tc.wantKind, op.InFlightKind, "a revival must not change the run in flight")
			if tc.wantRevived == StateDeletionPending {
				assert.Len(t, dt.pendingPods[egressRunID], 1, "the pod must wait")
				assert.False(t, dt.outboundPodExistsLocked(egressRunID, "10.0.0.1", "10.244.0.9"), "the pod must not be registered")
				assert.Contains(t, dt.pendingServiceDeletions, egressRunID)
				return
			}
			assert.NotContains(t, dt.pendingServiceDeletions, egressRunID, "the deletion must be cancelled")
			assert.True(t, dt.outboundPodExistsLocked(egressRunID, "10.0.0.1", "10.244.0.9"), "the pod must be registered")
			assert.Zero(t, op.RetryCount, "the retries of the update must not outlive the deletion")
			assertBackoff(t, op, false)
			if tc.outcome == nil {
				return
			}

			dt.onServiceOperationComplete(egressRunID, true, nil, tc.outcome)

			testutil.AssertVectorCount(t, "difftracker_engine_service_operations_total",
				map[string]string{"operation": "update", "service_type": "outbound", "result": "success"}, 1)
			testutil.AssertVectorCount(t, "difftracker_engine_service_operations_total", map[string]string{}, 1)
			assert.Equal(t, StateCreated, op.State, "the update completed")
			assert.False(t, op.Egress.NeedsObservation)
			assert.Equal(t, operationNone, op.InFlightKind, "the completion consumes the kind")
			assert.Nil(t, op.InFlightConfig)
		})
	}
}

// TestAddPod_RevivalPublishesTheBufferedPods pins that a revival publishes the pods that waited for the identity,
// with the pod that revives it, as a creation does. p1, admitted at startup, leaves while the identity's creation
// is in flight; p2 waits for that creation, which the deletion preempts while the address of p1 drains from NRP;
// p3 then revives the identity. The refused revival publishes nothing.
func TestAddPod_RevivalPublishesTheBufferedPods(t *testing.T) {
	p1, p2, p3 := memberPod("p1"), memberPod("p2"), memberPod("p3")
	defaults := defaultEgressConfig(egressParseContext{})
	dt := newTestDiffTracker()
	dt.mu.Lock()
	err := dt.updateK8sPodLocked(UpdatePodInputType{PodOperation: Add, PublicOutboundIdentity: memberIdentity,
		PodKey: p1.key(), PodUID: string(p1.UID), Location: memberNode, Address: memberIP(p1)})
	dt.mu.Unlock()
	if !assert.NoError(t, err) {
		return
	}
	dt.NRPResources.Locations[memberNode] = NRPLocation{Addresses: map[string]NRPAddress{
		memberIP(p1): {Services: utilsets.NewString(memberIdentity)}}}
	dt.AddService(NewOutboundServiceConfig(memberIdentity, &OutboundConfig{IPFamilies: ipv4Families(), Egress: defaults}))
	dispatchWithoutRunning(t, dt)
	dt.DeletePod(memberIdentity, memberNode, []string{memberIP(p1)}, p1.Namespace, p1.Name, string(p1.UID))
	joinMember(dt, memberIdentity, p2, &defaults)
	dt.NRPResources.NATGateways.Insert(memberIdentity) // the ServiceUpdater registers a created identity before its completion
	dt.onServiceOperationComplete(memberIdentity, true, nil, &egressOutcome{Mode: egressModeManaged, Applied: &defaults, Swept: true})

	op := dt.pendingServiceOps[memberIdentity]
	if !assert.Equal(t, StateDeletionPending, op.State, "control: the preempted creation waits for the address of p1 to drain") {
		return
	}
	assert.False(t, dt.outboundPodExistsLocked(memberIdentity, memberNode, memberIP(p2)), "a refused revival must publish nothing")
	assert.Len(t, dt.pendingPods[memberIdentity], 1)

	joinMember(dt, memberIdentity, p3, &defaults)

	assert.Equal(t, StateCreated, op.State, "control: p3 revives the identity")
	assert.True(t, dt.outboundPodExistsLocked(memberIdentity, memberNode, memberIP(p2)), "the pod that waited must be registered")
	assert.True(t, dt.outboundPodExistsLocked(memberIdentity, memberNode, memberIP(p3)), "the reviving pod must be registered")
	assert.Empty(t, dt.pendingPods[memberIdentity])
	count, _ := dt.outboundIdentityPodRefCount.Load(memberIdentity)
	assert.Equal(t, 2, count)
	assertEgressInvariants(t, dt)
}

// TestAddPod_DoesNotReviveAnIdentityWhoseCreationFailed pins that a pod joining an identity whose creation failed
// after its last pod left, before the deletion is dispatched, waits for the deletion and the re-creation: the
// identity is not registered and the failed creation may have left no NAT Gateway, so a revival would publish the
// pod to nothing. p1, admitted at startup, leaves while the creation is in flight; the deletion waits for the
// address of p1 to drain from NRP.
func TestAddPod_DoesNotReviveAnIdentityWhoseCreationFailed(t *testing.T) {
	p1, p2 := memberPod("p1"), memberPod("p2")
	defaults := defaultEgressConfig(egressParseContext{})
	dt := newTestDiffTracker()
	dt.mu.Lock()
	err := dt.updateK8sPodLocked(UpdatePodInputType{PodOperation: Add, PublicOutboundIdentity: memberIdentity,
		PodKey: p1.key(), PodUID: string(p1.UID), Location: memberNode, Address: memberIP(p1)})
	dt.mu.Unlock()
	if !assert.NoError(t, err) {
		return
	}
	dt.NRPResources.Locations[memberNode] = NRPLocation{Addresses: map[string]NRPAddress{
		memberIP(p1): {Services: utilsets.NewString(memberIdentity)}}}
	dt.AddService(NewOutboundServiceConfig(memberIdentity, &OutboundConfig{IPFamilies: ipv4Families(), Egress: defaults}))
	dispatchWithoutRunning(t, dt)
	dt.DeletePod(memberIdentity, memberNode, []string{memberIP(p1)}, p1.Namespace, p1.Name, string(p1.UID))
	dt.onServiceOperationComplete(memberIdentity, false, errors.New("creation failed"), &egressOutcome{Mode: egressModeManaged})

	op := dt.pendingServiceOps[memberIdentity]
	if !assert.Equal(t, StateDeletionPending, op.State, "control: the failed creation is routed to the deletion") ||
		!assert.Equal(t, operationNone, op.InFlightKind, "control: no run is in flight") ||
		!assert.Zero(t, op.RetryCount, "control: the retries alone would allow a revival") {
		return
	}

	joinMember(dt, memberIdentity, p2, &defaults)

	assert.Equal(t, StateDeletionPending, op.State, "the identity must not be revived")
	assert.Contains(t, dt.pendingServiceDeletions, memberIdentity, "the deletion must still run")
	assert.False(t, dt.outboundPodExistsLocked(memberIdentity, memberNode, memberIP(p2)), "the pod must not be published")
	assert.Len(t, dt.pendingPods[memberIdentity], 1, "the pod must wait for the re-creation")
	assertEgressInvariants(t, dt)
}

// TestMarkOutboundServiceForDeletionLocked_StartsAFreshRetryBudget pins that the deletion after the last pod of an
// identity whose update was parked does not inherit the update's retries, backoff or park, and keeps the quiet
// period, the owed observation and its generation: it waits for the end of the quiet period only.
func TestMarkOutboundServiceForDeletionLocked_StartsAFreshRetryBudget(t *testing.T) {
	dt := newTestDiffTracker()
	op := trackEgressRun(dt, egressRun{state: StateUpdateInProgress, mode: egressModeManaged, desired: consensusA,
		applied: &consensusApplied, owed: true, retries: maxServiceRetries})
	op.RetriesExhausted = true
	op.NextRetryAt = time.Now().Add(parkReArmCooldown)
	quietUntil := time.Now().Add(time.Hour)
	op.Egress.QuietUntil = quietUntil

	dt.mu.Lock()
	dt.markOutboundServiceForDeletionLocked(egressRunID)
	dt.mu.Unlock()

	assert.Equal(t, quietUntil, op.Egress.QuietUntil)
	assert.True(t, op.Egress.NeedsObservation)
	assert.Equal(t, uint64(7), op.Egress.ObservationGen)
	assert.Equal(t, uint64(7), op.Egress.DispatchedObservationGen)
	dt.CheckPendingServiceDeletions()
	if !assert.Equal(t, StateDeletionInProgress, op.State, "control: no address of the identity is in NRP") {
		return
	}
	dispatchWithoutRunning(t, dt)
	assert.Equal(t, operationNone, op.InFlightKind, "the deletion must wait for the end of the quiet period")

	op.Egress.QuietUntil = time.Now().Add(-time.Second)
	dispatchWithoutRunning(t, dt)
	assert.Equal(t, operationDelete, op.InFlightKind, "the deletion must not wait for the park of the update")
}

// TestOnServiceOperationComplete_BranchesOnTheDispatchedKind pins that a completion is processed as the run that
// was dispatched, not by the state the operation was moved to meanwhile. Inbound operations, whose state follows
// their runs, behave as before: a creation, update or deletion completes as such, and a deletion preempts the
// creation or update in flight.
func TestOnServiceOperationComplete_BranchesOnTheDispatchedKind(t *testing.T) {
	const uid = "svc-kind"
	inbound := NewInboundServiceConfig(uid, makeInboundConfig(80))
	outbound := NewOutboundServiceConfig(uid, &OutboundConfig{IPFamilies: ipv4Families(), Egress: consensusA})
	failure := errors.New("transient")

	cases := []struct {
		name          string
		config        ServiceConfig
		state         ResourceState
		applied       bool
		between       func(dt *DiffTracker, op *ServiceOperationState)
		success       bool
		outcome       *egressOutcome
		wantOperation string
		wantState     ResourceState // StateNotStarted with wantRemoved: the operation is gone
		wantRemoved   bool
	}{
		{name: "inbound creation", config: inbound, state: StateNotStarted, success: true,
			wantOperation: "create", wantState: StateCreated},
		{name: "inbound failed creation", config: inbound, state: StateNotStarted,
			wantOperation: "create", wantState: StateNotStarted},
		{name: "inbound update", config: inbound, state: StateUpdateInProgress, applied: true, success: true,
			wantOperation: "update", wantState: StateCreated},
		{name: "inbound failed update", config: inbound, state: StateUpdateInProgress, applied: true,
			wantOperation: "update", wantState: StateUpdateInProgress},
		{name: "inbound deletion", config: inbound, state: StateDeletionInProgress, applied: true, success: true,
			wantOperation: "delete", wantRemoved: true},
		{name: "inbound creation preempted by a deletion", config: inbound, state: StateNotStarted, success: true,
			between:       func(dt *DiffTracker, _ *ServiceOperationState) { dt.DeleteService(uid, true, false) },
			wantOperation: "create", wantState: StateDeletionInProgress},
		{name: "inbound update preempted by a deletion", config: inbound, state: StateUpdateInProgress, applied: true, success: true,
			between:       func(dt *DiffTracker, _ *ServiceOperationState) { dt.DeleteService(uid, true, false) },
			wantOperation: "update", wantState: StateDeletionInProgress},
		{name: "inbound deletion completing while its state reads pending", config: inbound, state: StateDeletionInProgress, applied: true,
			success:       true,
			between:       func(_ *DiffTracker, op *ServiceOperationState) { op.State = StateDeletionPending },
			wantOperation: "delete", wantRemoved: true},
		{name: "outbound update completing after its state was set to Created", config: outbound, state: StateUpdateInProgress,
			applied: true, success: true, outcome: &egressOutcome{Mode: egressModeManaged, Applied: &consensusA, Swept: true},
			between:       func(_ *DiffTracker, op *ServiceOperationState) { op.State = StateCreated },
			wantOperation: "update", wantState: StateCreated},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			RegisterMetrics()
			serviceOperationTotal.Reset()
			dt := newTestDiffTracker()
			dt.egress.mode[uid] = egressModeManaged
			op := &ServiceOperationState{ServiceUID: uid, Config: tc.config.snapshot(), State: tc.state}
			if tc.applied {
				applied := tc.config.snapshot()
				op.LastAppliedConfig = &applied
			}
			dt.pendingServiceOps[uid] = op
			dispatchWithoutRunning(t, dt)
			if tc.between != nil {
				tc.between(dt, op)
			}
			var err error
			if !tc.success {
				err = failure
			}

			dt.onServiceOperationComplete(uid, tc.success, err, tc.outcome)

			testutil.AssertVectorCount(t, "difftracker_engine_service_operations_total",
				map[string]string{"operation": tc.wantOperation}, 1)
			testutil.AssertVectorCount(t, "difftracker_engine_service_operations_total", map[string]string{}, 1)
			if tc.wantRemoved {
				assert.NotContains(t, dt.pendingServiceOps, uid)
				return
			}
			assert.Equal(t, tc.wantState, op.State)
			assert.Equal(t, operationNone, op.InFlightKind, "the completion consumes the kind")
		})
	}
}

// TestOnOutboundComplete_BYODeletionKeepsAnOwedResidueCleanup pins that the deletion of a BYO identity owed a
// residue cleanup keeps the obligation until a run reports the residue swept: a deletion whose outcome did not
// report it swept makes one more pass (through the retry backoff) instead of forgetting the identity, and when
// pods wait for the identity it is re-created with the obligation, which its first run then settles (a BYO
// identity by a residue cleanup).
func TestOnOutboundComplete_BYODeletionKeepsAnOwedResidueCleanup(t *testing.T) {
	cases := []struct {
		name         string
		mode         egressMode
		owed         bool
		recreate     bool
		outcome      *egressOutcome
		wantPass     bool // one more deletion pass
		wantRecreate bool
	}{
		{name: "residue swept: the deletion completes", mode: egressModeBYO, owed: true,
			outcome: &egressOutcome{Mode: egressModeBYO, Swept: true}},
		{name: "residue not swept: one more deletion pass", mode: egressModeBYO, owed: true,
			outcome: &egressOutcome{Mode: egressModeBYO}, wantPass: true},
		{name: "residue not swept, pods waiting: re-created with the obligation", mode: egressModeBYO, owed: true, recreate: true,
			outcome: &egressOutcome{Mode: egressModeBYO}, wantRecreate: true},
		{name: "nothing owed: the deletion completes", mode: egressModeBYO,
			outcome: &egressOutcome{Mode: egressModeBYO}},
		{name: "managed identity: the deletion completes once swept", mode: egressModeManaged, owed: true,
			outcome: &egressOutcome{Mode: egressModeManaged, Swept: true}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dt := newTestDiffTracker()
			op := trackEgressRun(dt, egressRun{state: StateDeletionInProgress, mode: tc.mode, desired: egressUnknown,
				applied: &egressUnknown, owed: tc.owed, votes: []*EgressConfig{&consensusA}})
			if tc.recreate {
				dt.pendingPods[egressRunID] = []PendingPodUpdate{{PodKey: "ns/pod-0", PodUID: "ns/pod-0-uid", Location: "10.0.0.1", Address: "10.244.0.5"}}
			}

			dt.onServiceOperationComplete(egressRunID, true, nil, tc.outcome)

			switch {
			case tc.wantPass:
				assert.Same(t, op, dt.pendingServiceOps[egressRunID], "the deletion must not complete")
				assert.Equal(t, StateDeletionInProgress, op.State)
				assert.True(t, op.Egress.NeedsObservation, "the residue cleanup is still owed")
				assert.Equal(t, egressModeBYO, dt.egress.mode[egressRunID])
				assert.Equal(t, 1, op.RetryCount)
				assertBackoff(t, op, true)
			case tc.wantRecreate:
				assert.Equal(t, StateNotStarted, op.State, "the identity is re-created")
				assert.True(t, op.Egress.NeedsObservation, "the re-created identity keeps the residue cleanup")
				assert.Equal(t, uint64(7), op.Egress.ObservationGen)
				_, known := dt.egress.mode[egressRunID]
				assert.False(t, known, "the re-creation resolves the mode again")

				dispatchWithoutRunning(t, dt)
				dt.onServiceOperationComplete(egressRunID, true, nil, &egressOutcome{Mode: egressModeBYO})
				if !assert.Equal(t, StateUpdateInProgress, op.State, "the re-created BYO identity gets its residue cleanup") {
					return
				}
				assert.Equal(t, egressUnknown, outboundEgress(op.Config.OutboundConfig))
				op.NextRetryAt = time.Time{}
				dispatchWithoutRunning(t, dt)
				assert.Equal(t, operationEgressResidueCleanup, op.InFlightKind)
			default:
				assert.NotContains(t, dt.pendingServiceOps, egressRunID, "the deletion completes")
				_, known := dt.egress.mode[egressRunID]
				assert.False(t, known)
			}
		})
	}
}

// TestEvaluateEgressConsensusLocked_TrackedBYOResidueOp pins the residue cleanup of the op startup's
// confirmation pass keeps for a BYO identity with leftovers of its managed past: a Created BYO op owed an
// observation, even without an applied or desired outbound config, gets a residue cleanup run, dispatched as
// such; once the run reports the residue swept nothing more is dispatched.
func TestEvaluateEgressConsensusLocked_TrackedBYOResidueOp(t *testing.T) {
	dt := newTestDiffTracker()
	dt.egress.mode[egressRunID] = egressModeBYO
	op := &ServiceOperationState{
		ServiceUID: egressRunID,
		Config:     NewOutboundServiceConfig(egressRunID, nil),
		State:      StateCreated,
		Egress:     egressOperationState{NeedsObservation: true, ObservationGen: 1},
	}
	dt.pendingServiceOps[egressRunID] = op

	dt.mu.Lock()
	dt.evaluateEgressConsensusLocked(egressRunID)
	dt.mu.Unlock()
	if !assert.Equal(t, StateUpdateInProgress, op.State) {
		return
	}
	assert.Equal(t, egressUnknown, outboundEgress(op.Config.OutboundConfig), "the residue cleanup marker")
	dispatchWithoutRunning(t, dt)
	if !assert.Equal(t, operationEgressResidueCleanup, op.InFlightKind, "the run is dispatched as a residue cleanup") {
		return
	}

	dt.onServiceOperationComplete(egressRunID, true, nil, &egressOutcome{Mode: egressModeBYO, Swept: true})

	assert.Equal(t, StateCreated, op.State)
	assert.False(t, op.Egress.NeedsObservation)
	dt.mu.Lock()
	dt.evaluateEgressConsensusLocked(egressRunID)
	dt.mu.Unlock()
	assert.Equal(t, StateCreated, op.State, "nothing more is dispatched")
	assert.Empty(t, dt.serviceUpdaterTrigger)
}

// TestSetAppliedEgressLocked pins how an applied configuration is recorded: as a new value built from the
// config the run was dispatched with, else from the applied or the desired one, so the applied config keeps
// the families of the run; a missing outbound config takes the cluster's families. Nothing published changes.
func TestSetAppliedEgressLocked(t *testing.T) {
	dual, ipv4 := dualStackFamilies(), ipv4Families()
	config := func(families []string, cfg EgressConfig) *ServiceConfig {
		c := NewOutboundServiceConfig(egressRunID, &OutboundConfig{IPFamilies: families, Egress: cfg})
		return &c
	}
	cases := []struct {
		name         string
		inFlight     *ServiceConfig
		applied      *ServiceConfig
		desired      ServiceConfig
		wantFamilies []string
	}{
		{name: "the dispatched config first", inFlight: config(dual, consensusA), applied: config(ipv4, consensusApplied),
			desired: *config(ipv4, consensusB), wantFamilies: dual},
		{name: "else the applied config", applied: config(dual, consensusApplied), desired: *config(ipv4, consensusB), wantFamilies: dual},
		{name: "else the desired config", desired: *config(dual, consensusB), wantFamilies: dual},
		{name: "a missing outbound config takes the cluster's families", desired: NewOutboundServiceConfig(egressRunID, nil),
			wantFamilies: dual},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dt := newTestDiffTracker()
			dt.clusterHasIPv6.Store(true)
			op := &ServiceOperationState{ServiceUID: egressRunID, Config: tc.desired, InFlightConfig: tc.inFlight, LastAppliedConfig: tc.applied}
			var before []*ServiceConfig
			for _, c := range []*ServiceConfig{tc.inFlight, tc.applied, &tc.desired} {
				if c != nil {
					snapshot := c.snapshot()
					before = append(before, &snapshot)
				}
			}

			dt.mu.Lock()
			dt.setAppliedEgressLocked(op, consensusRejected)
			dt.mu.Unlock()

			if !assert.NotNil(t, op.LastAppliedConfig) || !assert.NotNil(t, op.LastAppliedConfig.OutboundConfig) {
				return
			}
			assert.Equal(t, consensusRejected, op.LastAppliedConfig.OutboundConfig.Egress)
			assert.Equal(t, tc.wantFamilies, op.LastAppliedConfig.OutboundConfig.IPFamilies)
			assert.False(t, op.LastAppliedConfig.IsInbound)
			assert.Equal(t, egressRunID, op.LastAppliedConfig.UID)
			var after []*ServiceConfig
			for _, c := range []*ServiceConfig{tc.inFlight, tc.applied, &tc.desired} {
				if c != nil {
					after = append(after, c)
				}
			}
			assert.Equal(t, before, after, "nothing published may change")
		})
	}
}

// ================================================================================================
// Egress membership: every member pod of an egress identity casts one vote (4.1)
// ================================================================================================

const (
	memberIdentity = "egress-members"
	otherIdentity  = "egress-elsewhere"
	memberNode     = "10.0.0.1"
)

// memberPod returns the test pod ns/p<n>, with UID "p<n>-uid", created n minutes after the consensus epoch.
func memberPod(name string) egressPodRef {
	n, _ := strconv.Atoi(strings.TrimPrefix(name, "p"))
	return egressPodRef{Namespace: "ns", Name: name, UID: types.UID(name + "-uid"),
		Created: consensusEpoch.Add(time.Duration(n) * time.Minute)}
}

// replacementPod returns a pod with the name of pod and another UID, as a same-name replacement.
func replacementPod(pod egressPodRef) egressPodRef {
	pod.UID += "-replacement"
	return pod
}

// memberIP returns the address of the test pod ns/p<n>.
func memberIP(pod egressPodRef) string {
	return "10.244.1." + strings.TrimPrefix(pod.Name, "p")
}

// memberAccepted returns the address accepted for the test pod, on the test node.
func memberAccepted(pod egressPodRef) []egressAddress {
	return []egressAddress{{IP: memberIP(pod), Location: memberNode}}
}

// memberVote returns the vote of a pod with egress annotations for cfg, or an invalid one when cfg is nil, as
// the pod handler parses it: its identity, UID and creation time are for the engine to fill in.
func memberVote(cfg *EgressConfig) egressVote {
	if cfg == nil {
		return egressVote{Invalid: "idle-timeout: not an integer", HasAnnotations: true}
	}
	return egressVote{Config: *cfg, HasAnnotations: true}
}

// plainVote returns the vote of a pod without egress annotations for cfg, which is the default configuration of a
// real pod.
func plainVote(cfg EgressConfig) egressVote {
	return egressVote{Config: cfg}
}

// joinMember makes pod a member of identity voting cfg (nil: invalid), as the pod handler does.
func joinMember(dt *DiffTracker, identity string, pod egressPodRef, cfg *EgressConfig) {
	dt.AddEgressPod(identity, pod, memberVote(cfg), memberAccepted(pod))
}

// trackManagedEgressIdentity tracks a created managed egress identity whose NAT Gateway, in NRP, has
// configuration applied.
func trackManagedEgressIdentity(dt *DiffTracker, identity string, applied EgressConfig) *ServiceOperationState {
	dt.egress.mode[identity] = egressModeManaged
	dt.NRPResources.NATGateways.Insert(identity)
	op := &ServiceOperationState{ServiceUID: identity, State: StateCreated,
		Config: NewOutboundServiceConfig(identity, &OutboundConfig{IPFamilies: ipv4Families(), Egress: applied})}
	lastApplied := op.Config.snapshot()
	op.LastAppliedConfig = &lastApplied
	dt.pendingServiceOps[identity] = op
	return op
}

// trackInboundService tracks an inbound service whose UID is identity.
func trackInboundService(dt *DiffTracker, identity string) *ServiceOperationState {
	op := &ServiceOperationState{ServiceUID: identity, Config: NewInboundServiceConfig(identity, nil), State: StateCreated}
	dt.pendingServiceOps[identity] = op
	return op
}

// settleMembership drops the triggers and events raised so far, so a case observes only its own.
func settleMembership(dt *DiffTracker) {
	drainTrigger(dt.serviceUpdaterTrigger)
	drainTrigger(dt.locationsUpdaterTrigger)
	takeEgressEvents(dt)
}

// memberEvent returns the event reason on the test pod as takeEgressEvents describes it.
func memberEvent(reason, pod string) string {
	return reason + " ns/" + pod
}

// votersOf returns the keys of the pods voting for each identity.
func votersOf(dt *DiffTracker) map[string][]string {
	dt.mu.Lock()
	defer dt.mu.Unlock()
	voters := map[string][]string{}
	for podKey, vote := range dt.egress.votes.byPod {
		voters[vote.Identity] = append(voters[vote.Identity], podKey)
	}
	for _, keys := range voters {
		slices.Sort(keys)
	}
	return voters
}

// assertEgressInvariants asserts I2, the vote counters equal a recount of the votes, and I1, the pods voting for
// an identity are its members: the pods (name and UID) with a live or buffered address in it.
func assertEgressInvariants(t *testing.T, dt *DiffTracker) {
	t.Helper()
	dt.mu.Lock()
	defer dt.mu.Unlock()
	assertRecounted(t, dt.egress.votes)

	members := map[string][]string{}
	addMember := func(identity, podKey, uid string) {
		identity = strings.ToLower(identity)
		member := podKey + "@" + uid
		if !slices.Contains(members[identity], member) {
			members[identity] = append(members[identity], member)
		}
	}
	for _, node := range dt.K8sResources.Nodes {
		for _, pod := range node.Pods {
			if pod.PublicOutboundIdentity != "" {
				addMember(pod.PublicOutboundIdentity, pod.OutboundPodKey, pod.OutboundPodUID)
			}
		}
	}
	for identity, pods := range dt.pendingPods {
		for _, pod := range pods {
			addMember(identity, pod.PodKey, pod.PodUID)
		}
	}
	voters := map[string][]string{}
	for podKey, vote := range dt.egress.votes.byPod {
		voters[vote.Identity] = append(voters[vote.Identity], podKey+"@"+string(vote.PodUID))
	}
	for identity := range members {
		slices.Sort(members[identity])
	}
	for identity := range voters {
		slices.Sort(voters[identity])
	}
	assert.Equal(t, members, voters, "I1: the pods voting for an identity must be its members")
}

// desiredEgressOf returns the desired configuration of the operation of identity.
func desiredEgressOf(dt *DiffTracker, identity string) EgressConfig {
	return outboundEgress(dt.pendingServiceOps[identity].Config.OutboundConfig)
}

// TestAddEgressPod pins the row "pod becomes a member" (4.1): one call records the pod's vote and its addresses
// (a new operation is seeded by the pod, name and UID), withdraws the vote from an identity the pod left or from
// any identity when the new one collides with an inbound service, evaluates every identity whose votes changed,
// clears the startup baseline on a vote change and owes the pod the events of decision H, none when its vote did
// not change.
func TestAddEgressPod(t *testing.T) {
	p1, p2 := memberPod("p1"), memberPod("p2")
	defaultIPv4 := defaultEgressConfig(egressParseContext{})
	invalid := consts.ServiceEgressGatewayReasonInvalidConfig
	conflict := consts.ServiceEgressGatewayReasonConfigConflict
	rejected := consts.ServiceEgressGatewayReasonConfigRejected

	cases := []struct {
		name     string
		setup    func(dt *DiffTracker)
		identity string // empty: memberIdentity
		pod      egressPodRef
		vote     egressVote
		accepted []egressAddress // nil: the pod's address

		wantVoters   map[string][]string
		wantStates   map[string]ResourceState // of the identities listed
		wantDesired  map[string]EgressConfig  // of the identities listed
		wantTrigger  bool                     // the ServiceUpdater was triggered
		wantEvents   []string
		wantCreation map[string]EgressConfig // the creation configuration each identity listed is dispatched with
		check        func(t *testing.T, dt *DiffTracker)
	}{
		{name: "a pod creating an identity seeds it with its vote (name and UID)", pod: p1, vote: memberVote(&consensusB),
			wantVoters:   map[string][]string{memberIdentity: {"ns/p1"}},
			wantStates:   map[string]ResourceState{memberIdentity: StateNotStarted},
			wantCreation: map[string]EgressConfig{memberIdentity: consensusB}, wantTrigger: true,
			check: func(t *testing.T, dt *DiffTracker) {
				op := dt.pendingServiceOps[memberIdentity]
				assert.Equal(t, []string{"ns", "p1", "p1-uid"}, []string{op.TriggeringPodNamespace, op.TriggeringPodName, op.TriggeringPodUID})
				assert.Equal(t, []PendingPodUpdate{{PodKey: "ns/p1", PodUID: "p1-uid", Location: memberNode, Address: memberIP(p1)}},
					withoutTimestamps(dt.pendingPods[memberIdentity]))
			}},
		{name: "an invalid vote of the pod creating an identity: created with the default, InvalidEgressConfig", pod: p1,
			vote:         memberVote(nil),
			wantVoters:   map[string][]string{memberIdentity: {"ns/p1"}},
			wantStates:   map[string]ResourceState{memberIdentity: StateNotStarted},
			wantCreation: map[string]EgressConfig{memberIdentity: defaultIPv4}, wantTrigger: true,
			wantEvents: []string{memberEvent(invalid, "p1")}},
		{name: "a pod joining with the applied configuration changes nothing", pod: p2, vote: memberVote(&consensusA),
			setup: func(dt *DiffTracker) {
				trackManagedEgressIdentity(dt, memberIdentity, consensusA)
				joinMember(dt, memberIdentity, p1, &consensusA)
			},
			wantVoters: map[string][]string{memberIdentity: {"ns/p1", "ns/p2"}},
			wantStates: map[string]ResourceState{memberIdentity: StateCreated}},
		{name: "a pod joining with another configuration: EgressConfigConflict, nothing applied", pod: p2, vote: memberVote(&consensusB),
			setup: func(dt *DiffTracker) {
				trackManagedEgressIdentity(dt, memberIdentity, consensusA)
				joinMember(dt, memberIdentity, p1, &consensusA)
			},
			wantVoters: map[string][]string{memberIdentity: {"ns/p1", "ns/p2"}},
			wantStates: map[string]ResourceState{memberIdentity: StateCreated},
			wantEvents: []string{memberEvent(conflict, "p2")}},
		{name: "a re-added pod whose vote changed completes an agreement: update", pod: p2, vote: memberVote(&consensusA),
			setup: func(dt *DiffTracker) {
				trackManagedEgressIdentity(dt, memberIdentity, consensusApplied)
				joinMember(dt, memberIdentity, p1, &consensusA)
				joinMember(dt, memberIdentity, p2, &consensusB)
			},
			wantVoters:  map[string][]string{memberIdentity: {"ns/p1", "ns/p2"}},
			wantStates:  map[string]ResourceState{memberIdentity: StateUpdateInProgress},
			wantDesired: map[string]EgressConfig{memberIdentity: consensusA}, wantTrigger: true},
		{name: "a pod joining with a rejected configuration: EgressConfigConflict and EgressConfigRejected", pod: p2,
			vote: memberVote(&consensusRejected),
			setup: func(dt *DiffTracker) {
				trackManagedEgressIdentity(dt, memberIdentity, consensusA)
				joinMember(dt, memberIdentity, p1, &consensusA)
				// Recorded after the agreement on another configuration, which would forget it.
				dt.egress.rejected[memberIdentity] = egressRejection{Config: consensusRejected, Reason: "too many tags"}
			},
			wantVoters: map[string][]string{memberIdentity: {"ns/p1", "ns/p2"}},
			wantStates: map[string]ResourceState{memberIdentity: StateCreated},
			wantEvents: []string{memberEvent(conflict, "p2"), memberEvent(rejected, "p2")}},
		{name: "a pod completing an agreement on a rejected configuration: EgressConfigRejected, nothing dispatched", pod: p2,
			vote: memberVote(&consensusRejected),
			setup: func(dt *DiffTracker) {
				trackManagedEgressIdentity(dt, memberIdentity, consensusA)
				dt.egress.rejected[memberIdentity] = egressRejection{Config: consensusRejected, Reason: "too many tags"}
				joinMember(dt, memberIdentity, p1, &consensusRejected)
			},
			wantVoters: map[string][]string{memberIdentity: {"ns/p1", "ns/p2"}},
			wantStates: map[string]ResourceState{memberIdentity: StateCreated},
			wantEvents: []string{memberEvent(rejected, "p2")}},
		{name: "a pod with egress annotations joining a BYO identity: InvalidEgressConfig", pod: p1, vote: memberVote(&consensusA),
			setup: func(dt *DiffTracker) {
				trackManagedEgressIdentity(dt, memberIdentity, consensusApplied)
				dt.egress.mode[memberIdentity] = egressModeBYO
			},
			wantVoters: map[string][]string{memberIdentity: {"ns/p1"}},
			wantStates: map[string]ResourceState{memberIdentity: StateCreated},
			wantEvents: []string{memberEvent(invalid, "p1")}},
		{name: "a pod without egress annotations joining a BYO identity: no event", pod: p1, vote: plainVote(consensusApplied),
			setup: func(dt *DiffTracker) {
				trackManagedEgressIdentity(dt, memberIdentity, consensusApplied)
				dt.egress.mode[memberIdentity] = egressModeBYO
				joinMember(dt, memberIdentity, p2, &consensusA)
			},
			wantVoters: map[string][]string{memberIdentity: {"ns/p1", "ns/p2"}},
			wantStates: map[string]ResourceState{memberIdentity: StateCreated}},
		{name: "an identity colliding with an inbound service records nothing and withdraws the vote the pod cast elsewhere",
			pod: p1, vote: memberVote(&consensusA),
			setup: func(dt *DiffTracker) {
				trackManagedEgressIdentity(dt, otherIdentity, consensusApplied)
				joinMember(dt, otherIdentity, p1, &consensusB)
				joinMember(dt, otherIdentity, p2, &consensusA)
				trackInboundService(dt, memberIdentity)
				// The label move drains the addresses the pod leaves first.
				dt.DeletePod(otherIdentity, memberNode, []string{memberIP(p1)}, "", "", "")
			},
			wantVoters:  map[string][]string{otherIdentity: {"ns/p2"}},
			wantStates:  map[string]ResourceState{otherIdentity: StateUpdateInProgress, memberIdentity: StateCreated},
			wantDesired: map[string]EgressConfig{otherIdentity: consensusA}, wantTrigger: true,
			check: func(t *testing.T, dt *DiffTracker) {
				assert.True(t, dt.pendingServiceOps[memberIdentity].Config.IsInbound, "the inbound operation must be left alone")
				assert.Empty(t, dt.pendingPods[memberIdentity])
			}},
		{name: "an identity colliding with an inbound service keeps the vote of another pod with the same name",
			pod: replacementPod(p1), vote: memberVote(&consensusB),
			setup: func(dt *DiffTracker) {
				trackManagedEgressIdentity(dt, otherIdentity, consensusA)
				joinMember(dt, otherIdentity, p1, &consensusA)
				trackInboundService(dt, memberIdentity)
			},
			wantVoters: map[string][]string{otherIdentity: {"ns/p1"}},
			wantStates: map[string]ResourceState{otherIdentity: StateCreated, memberIdentity: StateCreated},
			check: func(t *testing.T, dt *DiffTracker) {
				vote, _ := dt.egress.votes.get("ns/p1")
				assert.Equal(t, p1.UID, vote.PodUID)
			}},
		{name: "a label move moves the vote, and both identities are evaluated", pod: p1, vote: memberVote(&consensusB),
			setup: func(dt *DiffTracker) {
				trackManagedEgressIdentity(dt, otherIdentity, consensusApplied)
				joinMember(dt, otherIdentity, p1, &consensusB)
				joinMember(dt, otherIdentity, p2, &consensusA)
				dt.DeletePod(otherIdentity, memberNode, []string{memberIP(p1)}, "", "", "")
			},
			wantVoters:  map[string][]string{memberIdentity: {"ns/p1"}, otherIdentity: {"ns/p2"}},
			wantStates:  map[string]ResourceState{memberIdentity: StateNotStarted, otherIdentity: StateUpdateInProgress},
			wantDesired: map[string]EgressConfig{otherIdentity: consensusA}, wantTrigger: true,
			wantCreation: map[string]EgressConfig{memberIdentity: consensusB}},
		{name: "an unchanged vote owes no event and keeps the startup baseline", pod: p2, vote: memberVote(&consensusB),
			setup: func(dt *DiffTracker) {
				trackManagedEgressIdentity(dt, memberIdentity, consensusApplied)
				joinMember(dt, memberIdentity, p1, &consensusA)
				joinMember(dt, memberIdentity, p2, &consensusB)
				dt.pendingServiceOps[memberIdentity].Egress.StartupBaseline = true
			},
			wantVoters: map[string][]string{memberIdentity: {"ns/p1", "ns/p2"}},
			wantStates: map[string]ResourceState{memberIdentity: StateCreated},
			check: func(t *testing.T, dt *DiffTracker) {
				assert.True(t, dt.pendingServiceOps[memberIdentity].Egress.StartupBaseline)
			}},
		{name: "the add of a pod whose vote was seeded at startup owes no event, and the identity is evaluated", pod: p1,
			vote: memberVote(&consensusA),
			setup: func(dt *DiffTracker) {
				trackManagedEgressIdentity(dt, memberIdentity, consensusApplied)
				addOutboundIdentityToPodForTest(dt, memberNode, memberIP(p1), memberIdentity, p1)
				dt.mu.Lock()
				dt.seedEgressVoteLocked(memberIdentity, p1, memberVote(&consensusA))
				dt.mu.Unlock()
			},
			wantVoters:  map[string][]string{memberIdentity: {"ns/p1"}},
			wantStates:  map[string]ResourceState{memberIdentity: StateUpdateInProgress},
			wantDesired: map[string]EgressConfig{memberIdentity: consensusA}, wantTrigger: true},
		{name: "a re-added pod whose annotations alone changed owes no event", pod: p2, vote: memberVote(&consensusB),
			setup: func(dt *DiffTracker) {
				trackManagedEgressIdentity(dt, memberIdentity, consensusA)
				joinMember(dt, memberIdentity, p1, &consensusA)
				dt.AddEgressPod(memberIdentity, p2, plainVote(consensusB), memberAccepted(p2))
			},
			wantVoters: map[string][]string{memberIdentity: {"ns/p1", "ns/p2"}},
			wantStates: map[string]ResourceState{memberIdentity: StateCreated},
			check: func(t *testing.T, dt *DiffTracker) {
				vote, _ := dt.egress.votes.get("ns/p2")
				assert.True(t, vote.HasAnnotations, "the changed vote is recorded")
			}},
		{name: "a changed vote clears the startup baseline", pod: p2, vote: memberVote(nil),
			setup: func(dt *DiffTracker) {
				trackManagedEgressIdentity(dt, memberIdentity, consensusApplied)
				joinMember(dt, memberIdentity, p1, &consensusA)
				joinMember(dt, memberIdentity, p2, &consensusB)
				dt.pendingServiceOps[memberIdentity].Egress.StartupBaseline = true
			},
			wantVoters: map[string][]string{memberIdentity: {"ns/p1", "ns/p2"}},
			wantStates: map[string]ResourceState{memberIdentity: StateCreated},
			wantEvents: []string{memberEvent(invalid, "p2")},
			check: func(t *testing.T, dt *DiffTracker) {
				assert.False(t, dt.pendingServiceOps[memberIdentity].Egress.StartupBaseline)
			}},
		{name: "a same-name replacement pod takes over the vote", pod: replacementPod(p1), vote: memberVote(&consensusA),
			setup: func(dt *DiffTracker) {
				trackManagedEgressIdentity(dt, memberIdentity, consensusA)
				joinMember(dt, memberIdentity, p1, &consensusA)
			},
			wantVoters: map[string][]string{memberIdentity: {"ns/p1"}},
			wantStates: map[string]ResourceState{memberIdentity: StateCreated},
			check: func(t *testing.T, dt *DiffTracker) {
				vote, _ := dt.egress.votes.get("ns/p1")
				assert.Equal(t, replacementPod(p1).UID, vote.PodUID)
			}},
		{name: "a same-name replacement pod is told of its invalid annotations", pod: replacementPod(p1), vote: memberVote(nil),
			setup: func(dt *DiffTracker) {
				trackManagedEgressIdentity(dt, memberIdentity, consensusA)
				joinMember(dt, memberIdentity, p1, nil)
			},
			wantVoters: map[string][]string{memberIdentity: {"ns/p1"}},
			wantStates: map[string]ResourceState{memberIdentity: StateCreated},
			wantEvents: []string{memberEvent(invalid, "p1")}},
		{name: "no accepted address: nothing is recorded", pod: p1, vote: memberVote(&consensusA), accepted: []egressAddress{},
			wantVoters: map[string][]string{},
			check: func(t *testing.T, dt *DiffTracker) {
				assert.Empty(t, dt.pendingServiceOps)
			}},
		{name: "addresses without a location or an IP are not accepted", pod: p1, vote: memberVote(&consensusA),
			accepted:   []egressAddress{{IP: memberIP(p1)}, {Location: memberNode}},
			wantVoters: map[string][]string{},
			check: func(t *testing.T, dt *DiffTracker) {
				assert.Empty(t, dt.pendingServiceOps)
			}},
		{name: "an address without a location is skipped", pod: p1, vote: memberVote(&consensusA),
			accepted:     []egressAddress{{IP: "10.244.9.9"}, {IP: memberIP(p1), Location: memberNode}},
			wantVoters:   map[string][]string{memberIdentity: {"ns/p1"}},
			wantStates:   map[string]ResourceState{memberIdentity: StateNotStarted},
			wantCreation: map[string]EgressConfig{memberIdentity: consensusA}, wantTrigger: true,
			check: func(t *testing.T, dt *DiffTracker) {
				assert.Equal(t, []PendingPodUpdate{{PodKey: "ns/p1", PodUID: "p1-uid", Location: memberNode, Address: memberIP(p1)}},
					withoutTimestamps(dt.pendingPods[memberIdentity]))
			}},
		{name: "an unnamed pod is not added", pod: egressPodRef{Namespace: "ns", UID: "p1-uid"}, vote: memberVote(&consensusA),
			accepted: memberAccepted(p1), wantVoters: map[string][]string{},
			check: func(t *testing.T, dt *DiffTracker) {
				assert.Empty(t, dt.pendingServiceOps)
			}},
		{name: "a pod without a namespace is not added", pod: egressPodRef{Name: "p1", UID: "p1-uid"}, vote: memberVote(&consensusA),
			accepted: memberAccepted(p1), wantVoters: map[string][]string{},
			check: func(t *testing.T, dt *DiffTracker) {
				assert.Empty(t, dt.pendingServiceOps)
			}},
		{name: "a pod without an identity is not added", identity: "-", pod: p1, vote: memberVote(&consensusA),
			wantVoters: map[string][]string{},
			check: func(t *testing.T, dt *DiffTracker) {
				assert.Empty(t, dt.pendingServiceOps)
			}},
		{name: "the vote's identity, pod UID and creation time are the pod's", pod: p1,
			vote: egressVote{Identity: otherIdentity, Config: consensusA, HasAnnotations: true, PodUID: "other-uid",
				PodCreated: consensusEpoch.Add(time.Hour)},
			wantVoters: map[string][]string{memberIdentity: {"ns/p1"}}, wantTrigger: true,
			check: func(t *testing.T, dt *DiffTracker) {
				vote, _ := dt.egress.votes.get("ns/p1")
				assert.Equal(t, egressVote{Identity: memberIdentity, Config: consensusA, HasAnnotations: true, PodUID: p1.UID,
					PodCreated: p1.Created}, vote)
			}},
		{name: "the vote counts for the IP families of the identity: those of a new one, the cluster's", pod: p2,
			vote: memberVote(&consensusA),
			setup: func(dt *DiffTracker) {
				dt.clusterHasIPv6.Store(true)
				joinMember(dt, memberIdentity, p1, &consensusA)
			},
			wantVoters:   map[string][]string{memberIdentity: {"ns/p1", "ns/p2"}},
			wantStates:   map[string]ResourceState{memberIdentity: StateNotStarted},
			wantCreation: map[string]EgressConfig{memberIdentity: consensusA},
			check: func(t *testing.T, dt *DiffTracker) {
				for _, podKey := range []string{"ns/p1", "ns/p2"} {
					vote, _ := dt.egress.votes.get(podKey)
					assert.Equal(t, consensusA, vote.Config, "%s is not parsed again", podKey)
					assert.True(t, vote.ParsedForIPv6, podKey)
				}
			}},
		{name: "a pod arriving before the deletion is dispatched revives the identity, which applies the agreement", pod: p1,
			vote: memberVote(&consensusA),
			setup: func(dt *DiffTracker) {
				trackManagedEgressIdentity(dt, memberIdentity, consensusApplied).State = StateDeletionPending
				dt.pendingServiceDeletions[memberIdentity] = &PendingServiceDeletion{ServiceUID: memberIdentity}
			},
			wantVoters:  map[string][]string{memberIdentity: {"ns/p1"}},
			wantStates:  map[string]ResourceState{memberIdentity: StateUpdateInProgress},
			wantDesired: map[string]EgressConfig{memberIdentity: consensusA}, wantTrigger: true,
			check: func(t *testing.T, dt *DiffTracker) {
				assert.NotContains(t, dt.pendingServiceDeletions, memberIdentity)
			}},
		{name: "a dual-stack pod casts one vote for its two addresses", pod: p1, vote: memberVote(&consensusA),
			accepted:   []egressAddress{{IP: memberIP(p1), Location: memberNode}, {IP: "fd00::1", Location: "fd00::10"}},
			wantVoters: map[string][]string{memberIdentity: {"ns/p1"}}, wantTrigger: true,
			check: func(t *testing.T, dt *DiffTracker) {
				assert.Len(t, dt.pendingPods[memberIdentity], 2)
			}},
		{name: "an identity known only from NRP takes the pod's addresses at once", pod: p1, vote: memberVote(&consensusA),
			setup: func(dt *DiffTracker) {
				dt.NRPResources.NATGateways.Insert(memberIdentity)
			},
			wantVoters: map[string][]string{memberIdentity: {"ns/p1"}},
			check: func(t *testing.T, dt *DiffTracker) {
				assert.Empty(t, dt.pendingServiceOps)
				assert.Equal(t, memberIdentity, dt.K8sResources.Nodes[memberNode].Pods[memberIP(p1)].PublicOutboundIdentity)
			}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dt := newTestDiffTracker()
			if tc.setup != nil {
				tc.setup(dt)
			}
			settleMembership(dt)
			identity, accepted := tc.identity, tc.accepted
			switch identity {
			case "":
				identity = memberIdentity
			case "-":
				identity = ""
			}
			if accepted == nil {
				accepted = memberAccepted(tc.pod)
			}

			dt.AddEgressPod(identity, tc.pod, tc.vote, accepted)

			assert.Equal(t, tc.wantVoters, votersOf(dt))
			for id, state := range tc.wantStates {
				if assert.Contains(t, dt.pendingServiceOps, id) {
					assert.Equal(t, state, dt.pendingServiceOps[id].State, id)
				}
			}
			for id, desired := range tc.wantDesired {
				assert.Equal(t, desired, desiredEgressOf(dt, id), id)
			}
			assert.Equal(t, tc.wantTrigger, len(dt.serviceUpdaterTrigger) == 1, "ServiceUpdater trigger")
			assert.Equal(t, tc.wantEvents, takeEgressEvents(dt))
			if tc.check != nil {
				tc.check(t, dt)
			}
			assertEgressInvariants(t, dt)
			if len(tc.wantCreation) > 0 {
				dispatchWithoutRunning(t, dt)
			}
			for id, creation := range tc.wantCreation {
				if op := dt.pendingServiceOps[id]; assert.NotNil(t, op, id) && assert.NotNil(t, op.InFlightConfig, "%s: the creation is dispatched", id) {
					assert.Equal(t, creation, outboundEgress(op.InFlightConfig.OutboundConfig), id)
				}
			}
		})
	}
}

// withoutTimestamps returns pods with their timestamps cleared.
func withoutTimestamps(pods []PendingPodUpdate) []PendingPodUpdate {
	var out []PendingPodUpdate
	for _, pod := range pods {
		pod.Timestamp = ""
		out = append(out, pod)
	}
	return out
}

// TestSetEgressVote pins the row "annotation-only update" (4.1): the vote of a member pod (the same name and
// UID, registered for the identity) is replaced; anything else is ignored. A changed vote clears the startup
// baseline, owes the pod the events of decision H and re-evaluates the identity; an unchanged one does nothing.
func TestSetEgressVote(t *testing.T) {
	p1, p2 := memberPod("p1"), memberPod("p2")
	invalid := consts.ServiceEgressGatewayReasonInvalidConfig
	conflict := consts.ServiceEgressGatewayReasonConfigConflict
	rejected := consts.ServiceEgressGatewayReasonConfigRejected

	cases := []struct {
		name     string
		setup    func(dt *DiffTracker)
		identity string // empty: memberIdentity
		pod      egressPodRef
		vote     egressVote

		wantVote     *egressVote // the vote registered for the pod's key afterwards; nil: none
		wantState    ResourceState
		wantDesired  *EgressConfig
		wantTrigger  bool
		wantEvents   []string
		wantBaseline bool
	}{
		{name: "a changed vote completing an agreement: update", pod: p2, vote: memberVote(&consensusA),
			setup: func(dt *DiffTracker) {
				trackManagedEgressIdentity(dt, memberIdentity, consensusApplied)
				joinMember(dt, memberIdentity, p1, &consensusA)
				joinMember(dt, memberIdentity, p2, &consensusB)
			},
			wantVote:  &egressVote{Identity: memberIdentity, Config: consensusA, HasAnnotations: true, PodUID: p2.UID, PodCreated: p2.Created},
			wantState: StateUpdateInProgress, wantDesired: &consensusA, wantTrigger: true},
		{name: "a changed vote leaving the members in disagreement: EgressConfigConflict", pod: p2, vote: memberVote(&consensusB),
			setup: func(dt *DiffTracker) {
				trackManagedEgressIdentity(dt, memberIdentity, consensusA)
				joinMember(dt, memberIdentity, p1, &consensusA)
				joinMember(dt, memberIdentity, p2, &consensusA)
			},
			wantVote:  &egressVote{Identity: memberIdentity, Config: consensusB, HasAnnotations: true, PodUID: p2.UID, PodCreated: p2.Created},
			wantState: StateCreated, wantDesired: &consensusA, wantEvents: []string{memberEvent(conflict, "p2")}},
		{name: "the vote counts for the IP families of the identity", pod: p2, vote: memberVote(&consensusB),
			setup: func(dt *DiffTracker) {
				trackManagedEgressIdentity(dt, memberIdentity, consensusA).Config.OutboundConfig.IPFamilies = dualStackFamilies()
				joinMember(dt, memberIdentity, p1, &consensusA)
				joinMember(dt, memberIdentity, p2, &consensusA)
			},
			wantVote: &egressVote{Identity: memberIdentity, Config: consensusB, HasAnnotations: true, ParsedForIPv6: true,
				PodUID: p2.UID, PodCreated: p2.Created},
			wantState: StateCreated, wantDesired: &consensusA, wantEvents: []string{memberEvent(conflict, "p2")}},
		{name: "a changed vote for a rejected configuration: EgressConfigRejected", pod: p2, vote: memberVote(&consensusRejected),
			setup: func(dt *DiffTracker) {
				trackManagedEgressIdentity(dt, memberIdentity, consensusA)
				dt.egress.rejected[memberIdentity] = egressRejection{Config: consensusRejected, Reason: "too many tags"}
				joinMember(dt, memberIdentity, p1, &consensusRejected)
				joinMember(dt, memberIdentity, p2, &consensusA)
			},
			wantVote: &egressVote{Identity: memberIdentity, Config: consensusRejected, HasAnnotations: true, PodUID: p2.UID,
				PodCreated: p2.Created},
			wantState: StateCreated, wantDesired: &consensusA, wantEvents: []string{memberEvent(rejected, "p2")}},
		{name: "a vote becoming invalid: InvalidEgressConfig, and the update not dispatched yet is cancelled", pod: p2,
			vote: memberVote(nil),
			setup: func(dt *DiffTracker) {
				trackManagedEgressIdentity(dt, memberIdentity, consensusApplied)
				joinMember(dt, memberIdentity, p1, &consensusA)
				joinMember(dt, memberIdentity, p2, &consensusA)
			},
			wantVote: &egressVote{Identity: memberIdentity, Invalid: "idle-timeout: not an integer", HasAnnotations: true,
				PodUID: p2.UID, PodCreated: p2.Created},
			wantState: StateCreated, wantDesired: &consensusApplied, wantEvents: []string{memberEvent(invalid, "p2")}},
		{name: "an unchanged vote does nothing: no event, the startup baseline kept", pod: p2, vote: memberVote(nil),
			setup: func(dt *DiffTracker) {
				trackManagedEgressIdentity(dt, memberIdentity, consensusA)
				joinMember(dt, memberIdentity, p1, &consensusA)
				joinMember(dt, memberIdentity, p2, nil)
				dt.pendingServiceOps[memberIdentity].Egress.StartupBaseline = true
			},
			wantVote: &egressVote{Identity: memberIdentity, Invalid: "idle-timeout: not an integer", HasAnnotations: true,
				PodUID: p2.UID, PodCreated: p2.Created},
			wantState: StateCreated, wantDesired: &consensusA, wantBaseline: true},
		{name: "invalid annotations changed while invalid: InvalidEgressConfig", pod: p2,
			vote: egressVote{Invalid: "tags: duplicate key", HasAnnotations: true},
			setup: func(dt *DiffTracker) {
				trackManagedEgressIdentity(dt, memberIdentity, consensusA)
				joinMember(dt, memberIdentity, p1, &consensusA)
				joinMember(dt, memberIdentity, p2, nil)
			},
			wantVote: &egressVote{Identity: memberIdentity, Invalid: "tags: duplicate key", HasAnnotations: true,
				PodUID: p2.UID, PodCreated: p2.Created},
			wantState: StateCreated, wantDesired: &consensusA, wantEvents: []string{memberEvent(invalid, "p2")}},
		{name: "egress annotations added on a BYO identity: InvalidEgressConfig, the startup baseline cleared", pod: p1,
			vote: memberVote(&consensusApplied),
			setup: func(dt *DiffTracker) {
				trackManagedEgressIdentity(dt, memberIdentity, consensusApplied)
				dt.egress.mode[memberIdentity] = egressModeBYO
				dt.AddEgressPod(memberIdentity, p1, plainVote(consensusApplied), memberAccepted(p1))
				dt.pendingServiceOps[memberIdentity].Egress.StartupBaseline = true
			},
			wantVote: &egressVote{Identity: memberIdentity, Config: consensusApplied, HasAnnotations: true, PodUID: p1.UID,
				PodCreated: p1.Created},
			wantState: StateCreated, wantDesired: &consensusApplied, wantEvents: []string{memberEvent(invalid, "p1")}},
		{name: "annotations changed without changing the managed configuration: no event, the startup baseline cleared", pod: p1,
			vote: memberVote(&consensusApplied),
			setup: func(dt *DiffTracker) {
				trackManagedEgressIdentity(dt, memberIdentity, consensusA)
				dt.AddEgressPod(memberIdentity, p1, plainVote(consensusApplied), memberAccepted(p1))
				joinMember(dt, memberIdentity, p2, &consensusA)
				dt.pendingServiceOps[memberIdentity].Egress.StartupBaseline = true
			},
			wantVote: &egressVote{Identity: memberIdentity, Config: consensusApplied, HasAnnotations: true, PodUID: p1.UID,
				PodCreated: p1.Created},
			wantState: StateCreated, wantDesired: &consensusA},
		{name: "a same-name pod with another UID is ignored", pod: replacementPod(p1), vote: memberVote(&consensusB),
			setup: func(dt *DiffTracker) {
				trackManagedEgressIdentity(dt, memberIdentity, consensusA)
				joinMember(dt, memberIdentity, p1, &consensusA)
				dt.pendingServiceOps[memberIdentity].Egress.StartupBaseline = true
			},
			wantVote:  &egressVote{Identity: memberIdentity, Config: consensusA, HasAnnotations: true, PodUID: p1.UID, PodCreated: p1.Created},
			wantState: StateCreated, wantDesired: &consensusA, wantBaseline: true},
		{name: "a vote registered for another identity is ignored", identity: otherIdentity, pod: p1, vote: memberVote(&consensusB),
			setup: func(dt *DiffTracker) {
				trackManagedEgressIdentity(dt, memberIdentity, consensusA)
				joinMember(dt, memberIdentity, p1, &consensusA)
				dt.pendingServiceOps[memberIdentity].Egress.StartupBaseline = true
			},
			wantVote:  &egressVote{Identity: memberIdentity, Config: consensusA, HasAnnotations: true, PodUID: p1.UID, PodCreated: p1.Created},
			wantState: StateCreated, wantDesired: &consensusA, wantBaseline: true},
		{name: "a pod that is not a member does not vote", pod: p2, vote: memberVote(&consensusB),
			setup: func(dt *DiffTracker) {
				trackManagedEgressIdentity(dt, memberIdentity, consensusA)
				joinMember(dt, memberIdentity, p1, &consensusA)
			},
			wantState: StateCreated, wantDesired: &consensusA},
		{name: "a pod that is not a member does not vote, whatever the identity and UID", identity: "-",
			pod: egressPodRef{Namespace: "ns", Name: "p2"}, vote: memberVote(&consensusB),
			setup: func(dt *DiffTracker) {
				trackManagedEgressIdentity(dt, memberIdentity, consensusA)
				joinMember(dt, memberIdentity, p1, &consensusA)
			},
			wantState: StateCreated, wantDesired: &consensusA},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dt := newTestDiffTracker()
			tc.setup(dt)
			settleMembership(dt)
			identity := tc.identity
			switch identity {
			case "":
				identity = memberIdentity
			case "-":
				identity = ""
			}

			dt.SetEgressVote(identity, tc.pod, tc.vote)

			vote, registered := dt.egress.votes.get(tc.pod.key())
			if assert.Equal(t, tc.wantVote != nil, registered) && registered {
				assert.Equal(t, *tc.wantVote, vote)
			}
			op := dt.pendingServiceOps[memberIdentity]
			assert.Equal(t, tc.wantState, op.State)
			if tc.wantDesired != nil {
				assert.Equal(t, *tc.wantDesired, desiredEgressOf(dt, memberIdentity))
			}
			assert.Equal(t, tc.wantTrigger, len(dt.serviceUpdaterTrigger) == 1, "ServiceUpdater trigger")
			assert.Equal(t, tc.wantEvents, takeEgressEvents(dt))
			assert.Equal(t, tc.wantBaseline, op.Egress.StartupBaseline, "startup baseline")
			assertEgressInvariants(t, dt)
		})
	}
}

// TestReparseEgressVotesLocked pins how the votes of an identity parsed for other IP families are parsed again for
// those of its NAT Gateway, from the annotations they were parsed from: none is when every vote was parsed for them;
// only the votes parsed for other families are, and no vote of another identity; a pod whose vote becomes valid or
// invalid is owed the events of a changed vote, raised once every vote is parsed again, and no other pod any.
func TestReparseEgressVotesLocked(t *testing.T) {
	ipv6Two := map[string]string{consts.PodAnnotationServiceEgressGatewayPublicIPCountIPv6: "2"}
	ipv6One := map[string]string{consts.PodAnnotationServiceEgressGatewayPublicIPCountIPv6: "1"}
	twiceInvalid := map[string]string{consts.PodAnnotationServiceEgressGatewayIdleTimeout: "ten",
		consts.PodAnnotationServiceEgressGatewayPublicIPCountIPv6: "2"}
	invalid := consts.ServiceEgressGatewayReasonInvalidConfig
	conflict := consts.ServiceEgressGatewayReasonConfigConflict

	type member struct {
		name string
		ann  map[string]string
		ipv6 bool // its vote was parsed for IPv6
		// forged registers consensusB as its vote, which its annotations do not cast: a vote parsed again loses it.
		forged bool
	}
	cases := []struct {
		name         string
		mode         egressMode
		members      []member
		ipv6         bool     // the votes are parsed again for IPv6
		wantReparsed []string // the members whose vote is parsed again
		wantEvents   []string
	}{
		{name: "every vote parsed for the families: none is parsed again",
			members: []member{{name: "p1", ann: annotationsA(), forged: true}, {name: "p2"}}},
		{name: "votes parsed for IPv4, again for IPv6", members: []member{{name: "p1", ann: annotationsA()}, {name: "p2"}},
			ipv6: true, wantReparsed: []string{"p1", "p2"}},
		{name: "only the votes parsed for other families", members: []member{{name: "p1", ann: annotationsA(), ipv6: true, forged: true}, {name: "p2"}},
			ipv6: true, wantReparsed: []string{"p2"}},
		{name: "a vote becoming valid without agreement: EgressConfigConflict", members: []member{{name: "p1", ann: ipv6Two}, {name: "p2"}},
			ipv6: true, wantReparsed: []string{"p1", "p2"}, wantEvents: []string{memberEvent(conflict, "p1")}},
		{name: "a vote becoming valid in agreement: no event", members: []member{{name: "p1", ann: ipv6One}, {name: "p2"}},
			ipv6: true, wantReparsed: []string{"p1", "p2"}},
		{name: "a vote becoming invalid: InvalidEgressConfig", members: []member{{name: "p1", ann: ipv6Two, ipv6: true}, {name: "p2", ipv6: true}},
			wantReparsed: []string{"p1", "p2"}, wantEvents: []string{memberEvent(invalid, "p1")}},
		{name: "a vote staying invalid: no event", members: []member{{name: "p1", ann: twiceInvalid}},
			ipv6: true, wantReparsed: []string{"p1"}},
		{name: "a BYO identity refuses annotations whatever the families: no event", mode: egressModeBYO,
			members: []member{{name: "p1", ann: ipv6Two}}, ipv6: true, wantReparsed: []string{"p1"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dt := newTestDiffTracker()
			dt.config = testConfig()
			dt.egress.mode[memberIdentity] = tc.mode
			parseContext := func(ipv6 bool) egressParseContext {
				if ipv6 {
					return dt.egressParseContextFor(dualStackFamilies())
				}
				return dt.egressParseContextFor(ipv4Families())
			}
			pods := map[string]*v1.Pod{}
			for _, m := range tc.members {
				pod := membershipPod(m.name, memberIdentity, m.ann)
				vote := egressPodRefOf(pod).voteFor(memberIdentity, parseEgressVote(pod, parseContext(m.ipv6)))
				if m.forged {
					vote.Config = consensusB
				}
				dt.egress.votes.set("ns/"+m.name, vote)
				pods["ns/"+m.name] = pod
			}
			other := membershipPod("p9", otherIdentity, nil)
			dt.egress.votes.set("ns/p9", egressPodRefOf(other).voteFor(otherIdentity, parseEgressVote(other, parseContext(!tc.ipv6))))
			before := maps.Clone(dt.egress.votes.byPod)

			dt.mu.Lock()
			flips, reparsed := dt.reparseEgressVotesLocked(memberIdentity, parseContext(tc.ipv6))
			events := dt.egressFlipEventsLocked(flips, "")
			dt.mu.Unlock()

			assert.Equal(t, len(tc.wantReparsed) > 0, reparsed)
			for podKey, vote := range before {
				if slices.Contains(tc.wantReparsed, strings.TrimPrefix(podKey, "ns/")) {
					vote = egressPodRefOf(pods[podKey]).voteFor(memberIdentity, parseEgressVote(pods[podKey], parseContext(tc.ipv6)))
				}
				got, _ := dt.egress.votes.get(podKey)
				assert.Equal(t, vote, got, podKey)
			}
			var got []string
			for _, event := range events {
				got = append(got, event.reason+" "+event.pod.Namespace+"/"+event.pod.Name)
			}
			assert.Equal(t, tc.wantEvents, got)
			assertRecounted(t, dt.egress.votes)
		})
	}
}

// TestEgressVoteCasts_ReparseTheVotesOfOtherFamilies pins that a pod casting its vote first has the votes of its
// identity parsed again for the identity's IP families, and that a pod whose vote becomes valid or invalid there is told.
// A pod joins an identity whose deletion is in progress on an IPv4 cluster, which then gains IPv6, the families of the
// re-creation: when another pod joins, the first vote counts for them too, so the pods without annotations agree and
// the joining pod is told of no conflict; when a member's annotations change, the other votes count for them too, and
// a member whose IPv6 setting becomes valid is told of the conflict. A pod with an IPv6 setting joins an identity
// created IPv4-only, whose deletion waits for its creation in flight, on a cluster that has IPv6: when the next pod
// revives the identity, the first vote counts for IPv4 only, as its NAT Gateway serves it, so the pod is told that its
// IPv6 setting is invalid. Every event judges the votes as the call leaves them, the cast included: a member whose
// IPv6 setting becomes valid is told of a conflict only when the votes disagree once the cast is registered, and the
// pod casting is judged once, from its vote before the re-parse.
func TestEgressVoteCasts_ReparseTheVotesOfOtherFamilies(t *testing.T) {
	ipv6Two := map[string]string{consts.PodAnnotationServiceEgressGatewayPublicIPCountIPv6: "2"}
	idleKey := consts.PodAnnotationServiceEgressGatewayIdleTimeout
	invalid := consts.ServiceEgressGatewayReasonInvalidConfig
	conflict := consts.ServiceEgressGatewayReasonConfigConflict
	deleting := func(dt *DiffTracker) {
		trackManagedEgressIdentity(dt, memberIdentity, consensusApplied).State = StateDeletionInProgress
	}
	annotateP2With := func(ann map[string]string) func(dt *DiffTracker) {
		return func(dt *DiffTracker) {
			pod := membershipPod("p2", memberIdentity, ann)
			dt.setEgressVote(memberIdentity, egressPodRefOf(pod), egressVoteParser(pod))
		}
	}
	annotateP2 := annotateP2With(annotationsA())
	cases := []struct {
		name        string
		setup       func(dt *DiffTracker)
		before      []string          // the members that joined before the cluster gained IPv6
		annotations map[string]string // the egress annotations of p1
		others      map[string]string // the egress annotations of the other members in before
		cast        func(dt *DiffTracker)
		wantIPv6    bool // the families the vote of p1 counts for
		wantState   ResourceState
		wantEvents  []string
	}{
		{name: "a pod joins a deletion", setup: deleting, before: []string{"p1"},
			cast:     func(dt *DiffTracker) { joinWithAnnotations(dt, memberIdentity, "p2", nil) },
			wantIPv6: true, wantState: StateDeletionInProgress},
		{name: "the annotations of a member of a deletion change", setup: deleting, before: []string{"p1", "p2"}, cast: annotateP2,
			wantIPv6: true, wantState: StateDeletionInProgress, wantEvents: []string{memberEvent(conflict, "p2")}},
		{name: "the annotations of a member of a deletion change, and the IPv6 setting of another becomes valid", setup: deleting,
			before: []string{"p1", "p2"}, annotations: ipv6Two, cast: annotateP2, wantIPv6: true, wantState: StateDeletionInProgress,
			wantEvents: []string{memberEvent(conflict, "p1"), memberEvent(conflict, "p2")}},
		{name: "a member takes the IPv6 setting of another, whose vote becomes valid: they agree, no conflict is told",
			setup: deleting, before: []string{"p1", "p2"}, annotations: ipv6Two, cast: annotateP2With(ipv6Two), wantIPv6: true,
			wantState: StateDeletionInProgress},
		{name: "a member drops the IPv6 setting both votes count for once valid: both are told of the conflict", setup: deleting,
			before: []string{"p1", "p2"}, annotations: ipv6Two, others: ipv6Two, cast: annotateP2With(nil), wantIPv6: true,
			wantState: StateDeletionInProgress, wantEvents: []string{memberEvent(conflict, "p1"), memberEvent(conflict, "p2")}},
		{name: "a pod joins a deletion and the IPv6 setting of a member becomes valid: both are told of the conflict",
			setup: deleting, before: []string{"p1"}, annotations: ipv6Two,
			cast:     func(dt *DiffTracker) { joinWithAnnotations(dt, memberIdentity, "p2", nil) },
			wantIPv6: true, wantState: StateDeletionInProgress, wantEvents: []string{memberEvent(conflict, "p1"), memberEvent(conflict, "p2")}},
		{name: "a member is added again unchanged and its IPv6 setting becomes valid: it is told of the conflict", setup: deleting,
			before: []string{"p1", "p2"}, annotations: ipv6Two,
			cast:     func(dt *DiffTracker) { joinWithAnnotations(dt, memberIdentity, "p1", ipv6Two) },
			wantIPv6: true, wantState: StateDeletionInProgress, wantEvents: []string{memberEvent(conflict, "p1")}},
		{name: "a member is added again unchanged: only the member whose IPv6 setting becomes valid is told of the conflict",
			setup: deleting, before: []string{"p1", "p2"}, annotations: ipv6Two,
			cast:     func(dt *DiffTracker) { joinWithAnnotations(dt, memberIdentity, "p2", nil) },
			wantIPv6: true, wantState: StateDeletionInProgress, wantEvents: []string{memberEvent(conflict, "p1")}},
		{name: "an annotation edit that leaves a member's vote the same still tells the member whose IPv6 setting becomes valid",
			setup: deleting, before: []string{"p1", "p2"}, annotations: ipv6Two, others: map[string]string{idleKey: "4"},
			cast: annotateP2With(map[string]string{idleKey: " 4"}), wantIPv6: true, wantState: StateDeletionInProgress,
			wantEvents: []string{memberEvent(conflict, "p1")}},
		{name: "an annotation edit that leaves an invalid vote the same is not told again", setup: deleting, before: []string{"p1"},
			annotations: map[string]string{idleKey: "ten"},
			cast: func(dt *DiffTracker) {
				pod := membershipPod("p1", memberIdentity, map[string]string{idleKey: "ten"})
				dt.setEgressVote(memberIdentity, egressPodRefOf(pod), egressVoteParser(pod))
			},
			wantIPv6: true, wantState: StateDeletionInProgress},
		{name: "an invalid member added again unchanged is not told again", setup: deleting, before: []string{"p1"},
			annotations: map[string]string{idleKey: "ten"},
			cast: func(dt *DiffTracker) {
				joinWithAnnotations(dt, memberIdentity, "p1", map[string]string{idleKey: "ten"})
			},
			wantIPv6: true, wantState: StateDeletionInProgress},
		{name: "a pod revives an identity created IPv4-only",
			setup: func(dt *DiffTracker) {
				dt.clusterHasIPv6.Store(true)
				op := trackManagedEgressIdentity(dt, memberIdentity, consensusApplied)
				op.State, op.InFlightKind = StateDeletionPending, operationCreate
				dt.pendingServiceDeletions[memberIdentity] = &PendingServiceDeletion{ServiceUID: memberIdentity}
				joinWithAnnotations(dt, memberIdentity, "p1", ipv6Two)
				op.InFlightKind = operationNone
			},
			annotations: ipv6Two,
			cast:        func(dt *DiffTracker) { joinWithAnnotations(dt, memberIdentity, "p2", nil) },
			wantState:   StateCreated, wantEvents: []string{memberEvent(invalid, "p1")}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dt := newTestDiffTracker()
			dt.config = testConfig()
			tc.setup(dt)
			for _, name := range tc.before {
				ann := tc.others
				if name == "p1" {
					ann = tc.annotations
				}
				joinWithAnnotations(dt, memberIdentity, name, ann)
			}
			settleMembership(dt)
			dt.clusterHasIPv6.Store(true)

			tc.cast(dt)

			families := ipv4Families()
			if tc.wantIPv6 {
				families = dualStackFamilies()
			}
			pc := dt.egressParseContextFor(families)
			vote, _ := dt.egress.votes.get("ns/p1")
			assert.Equal(t, egressPodRefOf(membershipPod("p1", memberIdentity, nil)).voteFor(memberIdentity, egressVoteOf(tc.annotations, pc)), vote,
				"the vote of p1 counts for %v", families)
			assert.True(t, dt.egress.votes.parsedFor(memberIdentity, tc.wantIPv6))
			assert.Equal(t, tc.wantState, egressStateOf(dt, memberIdentity))
			assert.Equal(t, tc.wantEvents, takeEgressEvents(dt))
			assertEgressInvariants(t, dt)
		})
	}
}

// TestEgressVoteCasts_KeepTheAnnotationsOfAnUnchangedVote pins that a vote cast again unchanged is registered with the
// annotations it was cast from, so that it is parsed again for other IP families from the pod's current annotations:
// an IPv6 setting changed on an IPv4 identity leaves the vote invalid alike, owing no event, and the vote parsed again
// for both families counts the current setting.
func TestEgressVoteCasts_KeepTheAnnotationsOfAnUnchangedVote(t *testing.T) {
	stale := map[string]string{consts.PodAnnotationServiceEgressGatewayPublicIPCountIPv6: "x"}
	current := map[string]string{consts.PodAnnotationServiceEgressGatewayPublicIPCountIPv6: "2"}
	cases := []struct {
		name   string
		recast func(dt *DiffTracker, pod *v1.Pod)
	}{
		{name: "the annotations alone change", recast: func(dt *DiffTracker, pod *v1.Pod) {
			dt.setEgressVote(memberIdentity, egressPodRefOf(pod), egressVoteParser(pod))
		}},
		{name: "the pod is added again", recast: func(dt *DiffTracker, pod *v1.Pod) {
			dt.addEgressPod(memberIdentity, egressPodRefOf(pod), memberAccepted(memberPod("p1")), egressVoteParser(pod))
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dt := newTestDiffTracker()
			dt.config = testConfig()
			trackManagedEgressIdentity(dt, memberIdentity, consensusApplied)
			joinWithAnnotations(dt, memberIdentity, "p1", stale)
			cast, _ := dt.egress.votes.get("ns/p1")
			settleMembership(dt)
			pod := membershipPod("p1", memberIdentity, current)

			tc.recast(dt, pod)

			vote, _ := dt.egress.votes.get("ns/p1")
			assert.True(t, sameEgressVote(cast, vote), "control: the vote is unchanged")
			assert.Empty(t, takeEgressEvents(dt), "an unchanged vote owes no event")
			pc := dt.egressParseContextFor(dualStackFamilies())
			dt.mu.Lock()
			dt.reparseEgressVotesLocked(memberIdentity, pc)
			vote, _ = dt.egress.votes.get("ns/p1")
			dt.mu.Unlock()
			assert.Equal(t, egressPodRefOf(pod).voteFor(memberIdentity, parseEgressVote(pod, pc)), vote, "parsed again from the current annotations")
			assert.Empty(t, vote.Invalid)
		})
	}
}

// TestRemoveEgressVote pins the rows "label move to an identity the pod is not accepted for" and "unlabelled
// delete with our finalizer" (4.1): the vote of the pod (name and UID) is withdrawn from the identity, or from
// whichever identity it was cast for when none is given, the identity is re-evaluated and its startup baseline
// cleared. The pod's addresses are left alone: the handler drains them first.
func TestRemoveEgressVote(t *testing.T) {
	p1, p2 := memberPod("p1"), memberPod("p2")
	// p1 votes A and p2 B on an identity whose applied configuration is the default, so p2 leaving is an agreement.
	disagreeing := func(dt *DiffTracker) {
		trackManagedEgressIdentity(dt, memberIdentity, consensusApplied)
		joinMember(dt, memberIdentity, p1, &consensusA)
		joinMember(dt, memberIdentity, p2, &consensusB)
		dt.pendingServiceOps[memberIdentity].Egress.StartupBaseline = true
	}
	drain := func(dt *DiffTracker, pod egressPodRef) {
		dt.DeletePod(memberIdentity, memberNode, []string{memberIP(pod)}, "", "", "")
	}

	cases := []struct {
		name     string
		setup    func(dt *DiffTracker)
		identity string
		podKey   string
		uid      types.UID

		want         bool
		wantVoters   map[string][]string
		wantState    ResourceState
		wantBaseline bool
	}{
		{name: "the vote is withdrawn and the identity re-evaluated",
			setup: func(dt *DiffTracker) { disagreeing(dt); drain(dt, p2) }, identity: memberIdentity, podKey: "ns/p2", uid: p2.UID,
			want: true, wantVoters: map[string][]string{memberIdentity: {"ns/p1"}}, wantState: StateUpdateInProgress},
		{name: "without an identity the vote is withdrawn from the identity it was cast for",
			setup: func(dt *DiffTracker) { disagreeing(dt); drain(dt, p2) }, podKey: "ns/p2", uid: p2.UID,
			want: true, wantVoters: map[string][]string{memberIdentity: {"ns/p1"}}, wantState: StateUpdateInProgress},
		{name: "the vote of a same-name pod with another UID is kept",
			setup: disagreeing, identity: memberIdentity, podKey: "ns/p2", uid: replacementPod(p2).UID,
			wantVoters: map[string][]string{memberIdentity: {"ns/p1", "ns/p2"}}, wantState: StateCreated, wantBaseline: true},
		{name: "without an identity, the vote of a same-name pod with another UID is kept",
			setup: disagreeing, podKey: "ns/p2", uid: replacementPod(p2).UID,
			wantVoters: map[string][]string{memberIdentity: {"ns/p1", "ns/p2"}}, wantState: StateCreated, wantBaseline: true},
		{name: "a vote cast for another identity is kept",
			setup: disagreeing, identity: otherIdentity, podKey: "ns/p2", uid: p2.UID,
			wantVoters: map[string][]string{memberIdentity: {"ns/p1", "ns/p2"}}, wantState: StateCreated, wantBaseline: true},
		{name: "a pod without a vote",
			setup: disagreeing, podKey: "ns/p3", uid: "p3-uid",
			wantVoters: map[string][]string{memberIdentity: {"ns/p1", "ns/p2"}}, wantState: StateCreated, wantBaseline: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dt := newTestDiffTracker()
			tc.setup(dt)
			settleMembership(dt)

			got := dt.RemoveEgressVote(tc.identity, tc.podKey, tc.uid)

			assert.Equal(t, tc.want, got)
			assert.Equal(t, tc.wantVoters, votersOf(dt))
			op := dt.pendingServiceOps[memberIdentity]
			assert.Equal(t, tc.wantState, op.State)
			assert.Equal(t, tc.want, len(dt.serviceUpdaterTrigger) == 1, "ServiceUpdater trigger")
			assert.Equal(t, tc.wantBaseline, op.Egress.StartupBaseline, "startup baseline")
			assert.Empty(t, takeEgressEvents(dt), "leaving an identity owes no event")
			assertEgressInvariants(t, dt)
		})
	}

	t.Run("the pod's addresses are left alone", func(t *testing.T) {
		dt := newTestDiffTracker()
		disagreeing(dt)

		assert.True(t, dt.RemoveEgressVote(memberIdentity, "ns/p2", p2.UID))

		assert.Equal(t, memberIdentity, dt.K8sResources.Nodes[memberNode].Pods[memberIP(p2)].PublicOutboundIdentity)
	})
}

// TestDeletePod_EgressVote pins the rows "pod removed" and "live re-registration drain" (4.1) for the votes: a
// removed pod (DeletePod or DeletePodWithoutAddresses with the pod's name and UID) withdraws its vote, which
// re-evaluates the identity, also when the removal of its addresses ends early; a stale event (another UID,
// another identity) and a drain for a re-registration (DeletePodForReplacement, or an unnamed drain of a label
// move) keep it.
func TestDeletePod_EgressVote(t *testing.T) {
	p1, p2 := memberPod("p1"), memberPod("p2")
	disagreeing := func(dt *DiffTracker) {
		trackManagedEgressIdentity(dt, memberIdentity, consensusApplied)
		joinMember(dt, memberIdentity, p1, &consensusA)
		joinMember(dt, memberIdentity, p2, &consensusB)
		dt.pendingServiceOps[memberIdentity].Egress.StartupBaseline = true
	}
	deletePod := func(pod egressPodRef) func(dt *DiffTracker) {
		return func(dt *DiffTracker) {
			dt.DeletePod(memberIdentity, memberNode, []string{memberIP(pod)}, pod.Namespace, pod.Name, string(pod.UID))
		}
	}

	cases := []struct {
		name      string
		setup     func(dt *DiffTracker)
		call      func(dt *DiffTracker)
		transient bool // the handler adds the pod right after (the vote is kept for that add)

		wantVoters   map[string][]string
		wantUID      types.UID // of the vote of ns/p1, when set
		wantState    ResourceState
		wantBaseline bool
	}{
		{name: "DeletePod withdraws the vote and re-evaluates the identity", setup: disagreeing, call: deletePod(p2),
			wantVoters: map[string][]string{memberIdentity: {"ns/p1"}}, wantState: StateUpdateInProgress},
		{name: "DeletePodWithoutAddresses withdraws the vote and re-evaluates the identity", setup: disagreeing,
			call:       func(dt *DiffTracker) { dt.DeletePodWithoutAddresses(memberIdentity, "ns", "p2", string(p2.UID)) },
			wantVoters: map[string][]string{memberIdentity: {"ns/p1"}}, wantState: StateUpdateInProgress},
		{name: "DeletePodWithoutAddresses of a pod whose drain is recorded withdraws the vote",
			setup: func(dt *DiffTracker) {
				disagreeing(dt)
				dt.DeletePodForReplacement(memberIdentity, memberNode, []string{memberIP(p2)}, "", "", "")
				dt.pendingPodDeletions["ns/p2"] = &PendingPodDeletion{Namespace: "ns", Name: "p2", UID: string(p2.UID),
					ServiceUID: memberIdentity, Addresses: []string{memberIP(p2)}}
			},
			call:       func(dt *DiffTracker) { dt.DeletePodWithoutAddresses(memberIdentity, "ns", "p2", string(p2.UID)) },
			wantVoters: map[string][]string{memberIdentity: {"ns/p1"}}, wantState: StateUpdateInProgress},
		{name: "DeletePodWithoutAddresses of a same-name pod with another UID keeps the vote", setup: disagreeing,
			call: func(dt *DiffTracker) {
				dt.DeletePodWithoutAddresses(memberIdentity, "ns", "p2", string(replacementPod(p2).UID))
			},
			wantVoters: map[string][]string{memberIdentity: {"ns/p1", "ns/p2"}}, wantState: StateCreated, wantBaseline: true},
		{name: "a stale DeletePod of a replaced pod keeps the vote of its same-name replacement",
			setup: func(dt *DiffTracker) {
				disagreeing(dt)
				joinMember(dt, memberIdentity, replacementPod(p1), &consensusA)
				dt.pendingServiceOps[memberIdentity].Egress.StartupBaseline = true
			},
			call:       deletePod(p1),
			wantVoters: map[string][]string{memberIdentity: {"ns/p1", "ns/p2"}}, wantUID: replacementPod(p1).UID,
			wantState: StateCreated, wantBaseline: true},
		{name: "a stale DeletePod of the identity the pod left keeps its vote",
			setup: func(dt *DiffTracker) {
				disagreeing(dt)
				trackManagedEgressIdentity(dt, otherIdentity, consensusApplied)
				dt.DeletePod(memberIdentity, memberNode, []string{memberIP(p1)}, "", "", "")
				joinMember(dt, otherIdentity, p1, &consensusA)
				dt.pendingServiceOps[memberIdentity].Egress.StartupBaseline = true
			},
			call:       deletePod(p1),
			wantVoters: map[string][]string{memberIdentity: {"ns/p2"}, otherIdentity: {"ns/p1"}}, wantUID: p1.UID,
			wantState: StateUpdateInProgress, wantBaseline: true},
		{name: "an unnamed drain of a label move keeps the vote for the add that follows", setup: disagreeing,
			call: func(dt *DiffTracker) {
				dt.DeletePod(memberIdentity, memberNode, []string{memberIP(p1)}, "", "", "")
			},
			transient:  true,
			wantVoters: map[string][]string{memberIdentity: {"ns/p1", "ns/p2"}}, wantUID: p1.UID, wantState: StateCreated,
			wantBaseline: true},
		{name: "DeletePodForReplacement keeps the vote of the pod it re-registers", setup: disagreeing,
			call: func(dt *DiffTracker) {
				dt.DeletePodForReplacement(memberIdentity, memberNode, []string{memberIP(p1)}, "ns", "p1", string(p1.UID))
			},
			transient:  true,
			wantVoters: map[string][]string{memberIdentity: {"ns/p1", "ns/p2"}}, wantUID: p1.UID, wantState: StateCreated,
			wantBaseline: true},
		{name: "DeletePodForReplacement of the sole pod keeps the identity and the vote",
			setup: func(dt *DiffTracker) {
				trackManagedEgressIdentity(dt, memberIdentity, consensusA)
				joinMember(dt, memberIdentity, p1, &consensusA)
			},
			call: func(dt *DiffTracker) {
				dt.DeletePodForReplacement(memberIdentity, memberNode, []string{memberIP(p1)}, "", "", "")
			},
			transient:  true,
			wantVoters: map[string][]string{memberIdentity: {"ns/p1"}}, wantUID: p1.UID, wantState: StateCreated},
		{name: "DeletePod of an identity now colliding with an inbound service withdraws the vote",
			setup: func(dt *DiffTracker) {
				disagreeing(dt)
				dt.DeletePod(memberIdentity, memberNode, []string{memberIP(p2)}, "", "", "")
				trackInboundService(dt, memberIdentity)
			},
			call:       deletePod(p2),
			wantVoters: map[string][]string{memberIdentity: {"ns/p1"}}, wantState: StateCreated},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dt := newTestDiffTracker()
			tc.setup(dt)
			settleMembership(dt)

			tc.call(dt)

			assert.Equal(t, tc.wantVoters, votersOf(dt))
			if tc.wantUID != "" {
				vote, _ := dt.egress.votes.get("ns/p1")
				assert.Equal(t, tc.wantUID, vote.PodUID)
			}
			op := dt.pendingServiceOps[memberIdentity]
			assert.Equal(t, tc.wantState, op.State)
			assert.Equal(t, tc.wantBaseline, op.Egress.StartupBaseline, "startup baseline")
			assert.Empty(t, takeEgressEvents(dt), "leaving an identity owes no event")
			if tc.transient {
				assertRecounted(t, dt.egress.votes)
				return
			}
			assertEgressInvariants(t, dt)
		})
	}
}

// TestLivePodLeftEgress pins the row "live re-registration drain, same identity, no accepted address left"
// (4.1, I12): the addresses the live pod leaves are drained where this pod (name and UID) holds them, its vote is
// withdrawn, and a sole pod's identity is deleted like after its last pod, while nothing is recorded to remove the
// live pod's finalizer. The pod's later deletion takes the removal path of a pod, before and after the identity's
// deletion was dispatched; a pod arriving before that dispatch revives the identity.
func TestLivePodLeftEgress(t *testing.T) {
	p1, p2 := memberPod("p1"), memberPod("p2")
	// newAddress is an address the pod changed to, which has no node location of its family, so it was never accepted.
	const newAddress = "fd00::99"
	leave := func(dt *DiffTracker, pod egressPodRef) {
		dt.LivePodLeftEgress(memberIdentity, memberNode, []string{memberIP(pod)}, pod)
	}
	sole := func(dt *DiffTracker) {
		trackManagedEgressIdentity(dt, memberIdentity, consensusA)
		joinMember(dt, memberIdentity, p1, &consensusA)
		settleMembership(dt)
	}
	assertNoFinalizerRecord := func(t *testing.T, dt *DiffTracker, pod egressPodRef) {
		t.Helper()
		assert.NotContains(t, dt.pendingPodDeletions, pod.key(), "nothing may be recorded to remove a live pod's finalizer")
	}

	t.Run("the sole pod: the identity is deleted, and the live pod keeps its finalizer", func(t *testing.T) {
		dt := newTestDiffTracker()
		sole(dt)

		leave(dt, p1)

		op := dt.pendingServiceOps[memberIdentity]
		assert.Equal(t, StateDeletionPending, op.State)
		assert.Contains(t, dt.pendingServiceDeletions, memberIdentity)
		assertNoFinalizerRecord(t, dt, p1)
		assert.Empty(t, votersOf(dt))
		assert.NotContains(t, dt.K8sResources.Nodes, memberNode, "the addresses the pod left are drained")
		assert.Len(t, dt.locationsUpdaterTrigger, 1, "the drain is synced to NRP")
		assertEgressInvariants(t, dt)
	})

	t.Run("2 -> 1: the identity is kept and re-evaluated", func(t *testing.T) {
		dt := newTestDiffTracker()
		trackManagedEgressIdentity(dt, memberIdentity, consensusApplied)
		joinMember(dt, memberIdentity, p1, &consensusA)
		joinMember(dt, memberIdentity, p2, &consensusB)
		settleMembership(dt)

		leave(dt, p2)

		op := dt.pendingServiceOps[memberIdentity]
		assert.Equal(t, StateUpdateInProgress, op.State, "the remaining member agrees")
		assert.Equal(t, consensusA, desiredEgressOf(dt, memberIdentity))
		assert.NotContains(t, dt.pendingServiceDeletions, memberIdentity)
		assertNoFinalizerRecord(t, dt, p2)
		assert.Equal(t, map[string][]string{memberIdentity: {"ns/p1"}}, votersOf(dt))
		assertEgressInvariants(t, dt)
	})

	t.Run("the pod's later deletion before the identity's deletion is dispatched releases its finalizer", func(t *testing.T) {
		dt := newTestDiffTracker()
		sole(dt)
		leave(dt, p1)

		result := dt.DeletePod(memberIdentity, memberNode, []string{newAddress}, "ns", "p1", string(p1.UID))

		assert.Equal(t, PodFinalizerDecisionReleaseNoDrain, result.FinalizerDecision)
		assert.Equal(t, StateDeletionPending, dt.pendingServiceOps[memberIdentity].State)
		assertNoFinalizerRecord(t, dt, p1)
		assertEgressInvariants(t, dt)
	})

	t.Run("the pod's later deletion after the identity's deletion was dispatched releases its finalizer", func(t *testing.T) {
		dt := newTestDiffTracker()
		sole(dt)
		leave(dt, p1)
		dt.CheckPendingServiceDeletions()
		if !assert.Equal(t, StateDeletionInProgress, dt.pendingServiceOps[memberIdentity].State, "control: dispatched") {
			return
		}

		result := dt.DeletePod(memberIdentity, memberNode, []string{newAddress}, "ns", "p1", string(p1.UID))

		assert.Equal(t, PodFinalizerDecisionReleaseNoDrain, result.FinalizerDecision)
		assertNoFinalizerRecord(t, dt, p1)
		dt.OnServiceCreationComplete(memberIdentity, true, nil)
		assert.NotContains(t, dt.pendingServiceOps, memberIdentity, "the identity's deletion completes")
		assertEgressInvariants(t, dt)
	})

	t.Run("the pod's later deletion while NRP still maps an address it left holds its finalizer until the identity is deleted",
		func(t *testing.T) {
			dt := newTestDiffTracker()
			sole(dt)
			dt.NRPResources.Locations[memberNode] = NRPLocation{Addresses: map[string]NRPAddress{
				memberIP(p1): {Services: utilsets.NewString(memberIdentity)},
			}}
			leave(dt, p1)
			assertNoFinalizerRecord(t, dt, p1)

			result := dt.DeletePod(memberIdentity, memberNode, []string{memberIP(p1)}, "ns", "p1", string(p1.UID))

			assert.Equal(t, PodFinalizerDecisionHoldForServiceDeletion, result.FinalizerDecision)
			assert.Equal(t, StateDeletionPending, dt.pendingServiceOps[memberIdentity].State)
		})

	t.Run("a pod arriving before the identity's deletion is dispatched revives it", func(t *testing.T) {
		dt := newTestDiffTracker()
		sole(dt)
		leave(dt, p1)

		joinMember(dt, memberIdentity, p1, &consensusA)

		assert.Equal(t, StateCreated, dt.pendingServiceOps[memberIdentity].State)
		assert.NotContains(t, dt.pendingServiceDeletions, memberIdentity)
		assert.Equal(t, map[string][]string{memberIdentity: {"ns/p1"}}, votersOf(dt))
		assertNoFinalizerRecord(t, dt, p1)
		assertEgressInvariants(t, dt)
	})

	t.Run("only the pod's own addresses and vote: a same-name replacement keeps them", func(t *testing.T) {
		dt := newTestDiffTracker()
		trackManagedEgressIdentity(dt, memberIdentity, consensusA)
		joinMember(dt, memberIdentity, replacementPod(p1), &consensusA)
		settleMembership(dt)

		leave(dt, p1)

		assert.Equal(t, StateCreated, dt.pendingServiceOps[memberIdentity].State)
		assert.Equal(t, map[string][]string{memberIdentity: {"ns/p1"}}, votersOf(dt))
		assert.Equal(t, memberIdentity, dt.K8sResources.Nodes[memberNode].Pods[memberIP(p1)].PublicOutboundIdentity)
		assertEgressInvariants(t, dt)
	})

	t.Run("a pod without a name drains nothing", func(t *testing.T) {
		dt := newTestDiffTracker()
		sole(dt)

		dt.LivePodLeftEgress(memberIdentity, memberNode, []string{memberIP(p1)}, egressPodRef{Namespace: "ns", UID: p1.UID})

		assert.Equal(t, StateCreated, dt.pendingServiceOps[memberIdentity].State)
		assert.Equal(t, map[string][]string{memberIdentity: {"ns/p1"}}, votersOf(dt))
		assert.Equal(t, memberIdentity, dt.K8sResources.Nodes[memberNode].Pods[memberIP(p1)].PublicOutboundIdentity)
	})
}

// TestSeedEgressVoteLocked pins the row "startup admission" (4.1, 4.6 step 2): the vote of an admitted pod is
// registered as the pod's, without evaluating or triggering anything and without events; a pod whose identity
// collides with an inbound service (tracked, or known from the cluster or NRP) is not seeded.
func TestSeedEgressVoteLocked(t *testing.T) {
	p1 := memberPod("p1")

	cases := []struct {
		name     string
		setup    func(dt *DiffTracker)
		identity string
		want     bool
	}{
		{name: "an admitted pod", identity: memberIdentity, want: true},
		{name: "an identity colliding with a tracked inbound service", identity: memberIdentity,
			setup: func(dt *DiffTracker) { trackInboundService(dt, memberIdentity) }},
		{name: "an identity colliding with a LoadBalancer Service of the cluster", identity: memberIdentity,
			setup: func(dt *DiffTracker) { dt.K8sResources.Services.Insert(memberIdentity) }},
		{name: "an identity colliding with a LoadBalancer in NRP", identity: memberIdentity,
			setup: func(dt *DiffTracker) { dt.NRPResources.LoadBalancers.Insert(memberIdentity) }},
		{name: "no identity"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dt := newTestDiffTracker()
			// A managed identity found at startup whose seeded votes agree on another configuration: evaluating it
			// would start an update.
			op := trackManagedEgressIdentity(dt, otherIdentity, consensusApplied)
			op.Egress.StartupBaseline = true
			if tc.setup != nil {
				tc.setup(dt)
			}
			addOutboundIdentityToPodForTest(dt, memberNode, memberIP(p1), tc.identity, p1)

			dt.mu.Lock()
			got := dt.seedEgressVoteLocked(tc.identity, p1,
				egressVote{Identity: otherIdentity, Config: consensusA, HasAnnotations: true, PodUID: "spoofed"})
			seededOther := dt.seedEgressVoteLocked(otherIdentity, memberPod("p2"), memberVote(&consensusA))
			dt.mu.Unlock()

			assert.Equal(t, tc.want, got)
			assert.True(t, seededOther, "control")
			vote, registered := dt.egress.votes.get(p1.key())
			if assert.Equal(t, tc.want, registered) && registered {
				assert.Equal(t, egressVote{Identity: tc.identity, Config: consensusA, HasAnnotations: true, PodUID: p1.UID,
					PodCreated: p1.Created}, vote)
			}
			assert.Equal(t, StateCreated, op.State, "seeding evaluates nothing")
			assert.True(t, op.Egress.StartupBaseline, "seeding keeps the startup baseline")
			assert.Empty(t, dt.serviceUpdaterTrigger)
			assert.Empty(t, takeEgressEvents(dt), "the unchanged baseline owes no event")
			if tc.want {
				addOutboundIdentityToPodForTest(dt, memberNode, memberIP(memberPod("p2")), otherIdentity, memberPod("p2"))
				assertEgressInvariants(t, dt)
			}
		})
	}
}

// addOutboundIdentityToPodForTest admits the address of pod to identity as startup does; an empty identity
// admits nothing.
func addOutboundIdentityToPodForTest(dt *DiffTracker, node, address, identity string, pod egressPodRef) {
	if identity == "" {
		return
	}
	ensureNodeExists(&dt.K8sResources, node)
	addOutboundIdentityToPod(&dt.K8sResources, node, address, identity, pod.key(), string(pod.UID))
}

// completeEgressCreation completes the creation of identity, dispatched with its desired configuration, as a
// managed NAT Gateway that satisfies applied.
func completeEgressCreation(dt *DiffTracker, identity string, applied EgressConfig) {
	dt.mu.Lock()
	op := dt.pendingServiceOps[identity]
	op.State = StateCreationInProgress
	op.InFlightKind = operationCreate
	inFlight := op.Config.snapshot()
	op.InFlightConfig = &inFlight
	dt.mu.Unlock()
	dt.NRPResources.NATGateways.Insert(identity) // the ServiceUpdater registers a created identity before its completion
	dt.onServiceOperationComplete(identity, true, nil, &egressOutcome{Mode: egressModeManaged, Applied: &applied, Swept: true})
}

// TestEgressMembership_Sequence drives the rows of 4.1 the way the pod handler does, one event after another on
// one tracker, and checks the voters, the operations, the events and the invariants I1 and I2 after every event.
func TestEgressMembership_Sequence(t *testing.T) {
	p1, p2, p3 := memberPod("p1"), memberPod("p2"), memberPod("p3")
	x, y := memberIdentity, otherIdentity
	applied := memberEvent(consts.ServiceEgressGatewayReasonConfigApplied, "p1")
	conflict := consts.ServiceEgressGatewayReasonConfigConflict
	dt := newTestDiffTracker()

	steps := []struct {
		name       string
		run        func()
		wantVoters map[string][]string
		wantStates map[string]ResourceState // an identity mapped to -1 has no operation
		wantEvents []string
	}{
		{name: "row 1: p1 creates x voting A", run: func() { joinMember(dt, x, p1, &consensusA) },
			wantVoters: map[string][]string{x: {"ns/p1"}}, wantStates: map[string]ResourceState{x: StateNotStarted}},
		{name: "x is created with A", run: func() { completeEgressCreation(dt, x, consensusA) },
			wantVoters: map[string][]string{x: {"ns/p1"}}, wantStates: map[string]ResourceState{x: StateCreated},
			wantEvents: []string{applied}},
		{name: "row 1: p2 joins x voting A", run: func() { joinMember(dt, x, p2, &consensusA) },
			wantVoters: map[string][]string{x: {"ns/p1", "ns/p2"}}, wantStates: map[string]ResourceState{x: StateCreated}},
		{name: "row 1: p3 joins x voting B", run: func() { joinMember(dt, x, p3, &consensusB) },
			wantVoters: map[string][]string{x: {"ns/p1", "ns/p2", "ns/p3"}}, wantStates: map[string]ResourceState{x: StateCreated},
			wantEvents: []string{memberEvent(conflict, "p3")}},
		{name: "row 5: p3 changes its annotations to A", run: func() { dt.SetEgressVote(x, p3, memberVote(&consensusA)) },
			wantVoters: map[string][]string{x: {"ns/p1", "ns/p2", "ns/p3"}}, wantStates: map[string]ResourceState{x: StateCreated}},
		{name: "row 5: p1 changes its annotations to B", run: func() { dt.SetEgressVote(x, p1, memberVote(&consensusB)) },
			wantVoters: map[string][]string{x: {"ns/p1", "ns/p2", "ns/p3"}}, wantStates: map[string]ResourceState{x: StateCreated},
			wantEvents: []string{memberEvent(conflict, "p1")}},
		{name: "row 4, label move: p1 moves to y, and x agrees on its applied A again",
			run: func() {
				dt.DeletePod(x, memberNode, []string{memberIP(p1)}, "", "", "")
				joinMember(dt, y, p1, &consensusB)
			},
			wantVoters: map[string][]string{x: {"ns/p2", "ns/p3"}, y: {"ns/p1"}},
			wantStates: map[string]ResourceState{x: StateCreated, y: StateNotStarted}},
		{name: "row 4, address change: p2 is re-registered in x with its vote",
			run: func() {
				dt.DeletePodForReplacement(x, memberNode, []string{memberIP(p2)}, "", "", "")
				joinMember(dt, x, p2, &consensusA)
			},
			wantVoters: map[string][]string{x: {"ns/p2", "ns/p3"}, y: {"ns/p1"}},
			wantStates: map[string]ResourceState{x: StateCreated, y: StateNotStarted}},
		{name: "row 5: p2 changes its annotations to B", run: func() { dt.SetEgressVote(x, p2, memberVote(&consensusB)) },
			wantVoters: map[string][]string{x: {"ns/p2", "ns/p3"}, y: {"ns/p1"}},
			wantStates: map[string]ResourceState{x: StateCreated, y: StateNotStarted},
			wantEvents: []string{memberEvent(conflict, "p2")}},
		{name: "row 2: p2 moves to an identity it is not accepted for",
			run: func() {
				dt.DeletePod(x, memberNode, []string{memberIP(p2)}, "", "", "")
				dt.RemoveEgressVote(x, p2.key(), p2.UID)
			},
			wantVoters: map[string][]string{x: {"ns/p3"}, y: {"ns/p1"}},
			wantStates: map[string]ResourceState{x: StateCreated, y: StateNotStarted}},
		{name: "row 3: p3 is deleted, the last pod of x",
			run:        func() { dt.DeletePod(x, memberNode, []string{memberIP(p3)}, "ns", "p3", string(p3.UID)) },
			wantVoters: map[string][]string{y: {"ns/p1"}},
			wantStates: map[string]ResourceState{x: StateDeletionPending, y: StateNotStarted}},
		{name: "row 4, no accepted address left: p1 leaves y, which was not created yet",
			run:        func() { dt.LivePodLeftEgress(y, memberNode, []string{memberIP(p1)}, p1) },
			wantVoters: map[string][]string{},
			wantStates: map[string]ResourceState{x: StateDeletionPending, y: -1}},
		{name: "row 1: p1 joins x before its deletion is dispatched and revives it",
			run:        func() { joinMember(dt, x, p1, &consensusA) },
			wantVoters: map[string][]string{x: {"ns/p1"}},
			wantStates: map[string]ResourceState{x: StateCreated, y: -1}},
	}

	for _, step := range steps {
		step.run()

		assert.Equal(t, step.wantVoters, votersOf(dt), step.name)
		for identity, state := range step.wantStates {
			op, tracked := dt.pendingServiceOps[identity]
			if state == -1 {
				assert.False(t, tracked, "%s: %s has no operation", step.name, identity)
				continue
			}
			if assert.True(t, tracked, "%s: %s is tracked", step.name, identity) {
				assert.Equal(t, state, op.State, "%s: %s", step.name, identity)
			}
		}
		assert.Equal(t, step.wantEvents, takeEgressEvents(dt), step.name)
		assertEgressInvariants(t, dt)
	}
}

// TestEgressMembership_EmitsEventsWithoutTheLock pins that the events of a vote change are emitted once dt.mu is
// released, as the recorder performs I/O.
func TestEgressMembership_EmitsEventsWithoutTheLock(t *testing.T) {
	dt := newTestDiffTracker()
	recorder := &eventLockRecorder{FakeRecorder: *record.NewFakeRecorder(10), dt: dt}
	dt.eventRecorder = recorder
	trackManagedEgressIdentity(dt, memberIdentity, consensusA)
	joinMember(dt, memberIdentity, memberPod("p1"), &consensusA)

	joinMember(dt, memberIdentity, memberPod("p2"), nil)
	dt.SetEgressVote(memberIdentity, memberPod("p1"), memberVote(&consensusB))

	events := drainEvents(&recorder.FakeRecorder)
	if assert.Len(t, events, 2) {
		assert.True(t, strings.HasPrefix(events[0], v1.EventTypeWarning+" "+consts.ServiceEgressGatewayReasonInvalidConfig+" "))
		assert.True(t, strings.HasPrefix(events[1], v1.EventTypeWarning+" "+consts.ServiceEgressGatewayReasonConfigConflict+" "))
	}
	assert.False(t, recorder.heldByEmit, "an event must not be emitted while dt.mu is held")
}

// TestEgressMembership_ConcurrentEntryPoints runs the membership entry points concurrently under -race. The vote
// counters stay exact (I2) throughout, and once every pod is removed no vote and no member is left (I1).
func TestEgressMembership_ConcurrentEntryPoints(t *testing.T) {
	dt := newTestDiffTracker()
	identities := []string{memberIdentity, otherIdentity}
	trackManagedEgressIdentity(dt, memberIdentity, consensusApplied)
	trackManagedEgressIdentity(dt, otherIdentity, consensusA)
	var pods []egressPodRef
	for i := 1; i <= 6; i++ {
		pods = append(pods, memberPod(fmt.Sprintf("p%d", i)))
	}
	votes := []*EgressConfig{&consensusA, &consensusB, nil}

	var wg sync.WaitGroup
	start := make(chan struct{})
	for worker := 0; worker < 6; worker++ {
		wg.Add(1)
		go func(seed uint64) {
			defer wg.Done()
			r := rand.New(rand.NewPCG(seed, 8)) //nolint:gosec // G404: a fixed, reproducible operation sequence per worker
			<-start
			for i := 0; i < 300; i++ {
				pod, identity, vote := pods[r.IntN(len(pods))], identities[r.IntN(len(identities))], memberVote(votes[r.IntN(len(votes))])
				address := []string{memberIP(pod)}
				switch r.IntN(8) {
				case 0, 1:
					dt.AddEgressPod(identity, pod, vote, memberAccepted(pod))
				case 2:
					dt.SetEgressVote(identity, pod, vote)
				case 3:
					dt.RemoveEgressVote([]string{"", identity}[r.IntN(2)], pod.key(), pod.UID)
				case 4:
					dt.DeletePod(identity, memberNode, address, pod.Namespace, pod.Name, string(pod.UID))
				case 5:
					dt.DeletePodForReplacement(identity, memberNode, address, "", "", "")
					dt.AddEgressPod(identity, pod, vote, memberAccepted(pod))
				case 6:
					dt.LivePodLeftEgress(identity, memberNode, address, pod)
				case 7:
					dt.DeletePodWithoutAddresses(identity, pod.Namespace, pod.Name, string(pod.UID))
					dt.CheckPendingServiceDeletions()
				}
				dt.mu.Lock()
				assertRecounted(t, dt.egress.votes)
				dt.mu.Unlock()
			}
		}(uint64(worker))
	}
	close(start)
	wg.Wait()

	for _, pod := range pods {
		for _, identity := range identities {
			dt.DeletePod(identity, memberNode, []string{memberIP(pod)}, pod.Namespace, pod.Name, string(pod.UID))
		}
	}
	assert.Empty(t, votersOf(dt), "a removed pod votes for no identity")
	assertEgressInvariants(t, dt)
}
