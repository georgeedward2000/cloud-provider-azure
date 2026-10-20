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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"

	"sigs.k8s.io/cloud-provider-azure/pkg/consts"
)

const eventsIdentity = "egress-events"

// eventsTracker returns a tracker whose identity has the members ns/a (votes A), ns/b (votes B), ns/c (invalid)
// and ns/d (votes the default without annotations); ns/x is a member of another identity.
func eventsTracker() *DiffTracker {
	dt := newTestDiffTracker()
	castEgressVote(dt, eventsIdentity, "ns/b", &consensusB, consensusEpoch)
	castEgressVote(dt, eventsIdentity, "ns/a", &consensusA, consensusEpoch)
	castEgressVote(dt, eventsIdentity, "ns/c", nil, consensusEpoch)
	dt.egress.votes.set("ns/d", egressVote{Identity: eventsIdentity, Config: consensusApplied, PodUID: "d-uid", PodCreated: consensusEpoch})
	castEgressVote(dt, "egress-other", "ns/x", &consensusA, consensusEpoch)
	return dt
}

// describeEvents returns events as "type reason ns/name uid".
func describeEvents(events []egressEvent) []string {
	var described []string
	for _, event := range events {
		described = append(described, strings.Join([]string{event.eventType, event.reason,
			event.pod.Namespace + "/" + event.pod.Name, string(event.pod.UID)}, " "))
	}
	return described
}

// TestEgressMemberEvents pins who is told what: an applied configuration every member of the identity, a
// rejection the members voting for the rejected configuration, a BYO NAT Gateway the members with egress
// annotations. Events are addressed to the voting pod (namespace, name and UID), in pod key order, and never to
// a member of another identity.
func TestEgressMemberEvents(t *testing.T) {
	normal, warning := v1.EventTypeNormal, v1.EventTypeWarning
	applied := consts.ServiceEgressGatewayReasonConfigApplied
	rejected := consts.ServiceEgressGatewayReasonConfigRejected
	invalid := consts.ServiceEgressGatewayReasonInvalidConfig

	cases := []struct {
		name        string
		mode        egressMode
		events      func(dt *DiffTracker) []egressEvent
		want        []string
		wantMessage []string // parts of every message
	}{
		{name: "applied", mode: egressModeManaged,
			events: func(dt *DiffTracker) []egressEvent { return dt.egressAppliedEventsLocked(eventsIdentity, consensusA) },
			want: []string{normal + " " + applied + " ns/a ns/a-uid", normal + " " + applied + " ns/b ns/b-uid",
				normal + " " + applied + " ns/c ns/c-uid", normal + " " + applied + " ns/d d-uid"},
			wantMessage: []string{`"egress-events"`, consensusA.Summary()}},
		{name: "rejected terminally", mode: egressModeManaged,
			events: func(dt *DiffTracker) []egressEvent {
				return dt.egressRejectedEventsLocked(eventsIdentity, egressRejection{Config: consensusA, Reason: "too many tags"})
			},
			want:        []string{warning + " " + rejected + " ns/a ns/a-uid"},
			wantMessage: []string{`"egress-events"`, consensusA.Summary(), "too many tags", "will not be retried"}},
		{name: "rejected for now", mode: egressModeManaged,
			events: func(dt *DiffTracker) []egressEvent {
				return dt.egressRejectedEventsLocked(eventsIdentity, egressRejection{Config: consensusB, Reason: "quota", Retryable: true})
			},
			want:        []string{warning + " " + rejected + " ns/b ns/b-uid"},
			wantMessage: []string{consensusB.Summary(), "quota", "will be retried while the members of the identity agree on it"}},
		{name: "rejected zero configuration: never told to an invalid vote", mode: egressModeManaged,
			events: func(dt *DiffTracker) []egressEvent {
				return dt.egressRejectedEventsLocked(eventsIdentity, egressRejection{Reason: "rejected"})
			}},
		{name: "rejected configuration nobody votes for", mode: egressModeManaged,
			events: func(dt *DiffTracker) []egressEvent {
				return dt.egressRejectedEventsLocked(eventsIdentity, egressRejection{Config: consensusRejected, Reason: "quota"})
			}},
		{name: "BYO NAT Gateway", mode: egressModeBYO,
			events: func(dt *DiffTracker) []egressEvent { return dt.egressBYOInvalidEventsLocked(eventsIdentity) },
			want: []string{warning + " " + invalid + " ns/a ns/a-uid", warning + " " + invalid + " ns/b ns/b-uid",
				warning + " " + invalid + " ns/c ns/c-uid"},
			wantMessage: []string{`"egress-events"`, "accepts no egress configuration"}},
		{name: "managed NAT Gateway: no annotation is refused", mode: egressModeManaged,
			events: func(dt *DiffTracker) []egressEvent { return dt.egressBYOInvalidEventsLocked(eventsIdentity) }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dt := eventsTracker()
			dt.egress.mode[eventsIdentity] = tc.mode
			dt.egress.mode["egress-other"] = tc.mode

			events := tc.events(dt)

			assert.Equal(t, tc.want, describeEvents(events))
			for _, event := range events {
				for _, part := range tc.wantMessage {
					assert.Contains(t, event.message, part)
				}
			}
		})
	}
}

// TestEgressVoteRefusedByBYOLocked pins the BYO-derived validity of a vote: a pod with egress annotations, valid
// or not, is refused by an identity whose NAT Gateway is BYO, derived from the mode at the time of asking.
func TestEgressVoteRefusedByBYOLocked(t *testing.T) {
	cases := []struct {
		name        string
		mode        egressMode
		annotations bool
		want        bool
	}{
		{"BYO, annotated", egressModeBYO, true, true},
		{"BYO, without annotations", egressModeBYO, false, false},
		{"managed, annotated", egressModeManaged, true, false},
		{"mode not resolved, annotated", egressModeUnknown, true, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dt := newTestDiffTracker()
			dt.egress.mode[eventsIdentity] = tc.mode
			vote := egressVote{Identity: eventsIdentity, Config: consensusA, HasAnnotations: tc.annotations}
			assert.Equal(t, tc.want, dt.egressVoteRefusedByBYOLocked(vote))
		})
	}
}

// TestEgressVoteRejectedEventLocked pins the vote-change half of decision H for rejections: a pod whose vote
// changes to a configuration rejected for its identity (terminally or for now) is told, once; an unchanged
// vote, an invalid one and a configuration nobody rejected are not. A same-name replacement pod is a new voter.
func TestEgressVoteRejectedEventLocked(t *testing.T) {
	rejection := egressRejection{Config: consensusRejected, Reason: "too many tags"}
	vote := func(cfg EgressConfig, uid types.UID) egressVote {
		return egressVote{Identity: eventsIdentity, Config: cfg, HasAnnotations: true, PodUID: uid}
	}
	invalidVote := egressVote{Identity: eventsIdentity, Invalid: "idle-timeout: not an integer", HasAnnotations: true, PodUID: "uid"}

	cases := []struct {
		name         string
		terminal     bool
		warned       bool
		zeroRejected bool // the rejected configuration is the zero one, which an invalid vote carries
		other        bool // the rejection is recorded for another identity
		previous     *egressVote
		vote         egressVote
		wantEvent    bool
	}{
		{name: "new member votes for a terminally rejected configuration", terminal: true,
			vote: vote(consensusRejected, "uid"), wantEvent: true},
		{name: "new member votes for a configuration warned about", warned: true,
			vote: vote(consensusRejected, "uid"), wantEvent: true},
		{name: "vote changes to a rejected configuration", terminal: true,
			previous: voteRef(vote(consensusA, "uid")), vote: vote(consensusRejected, "uid"), wantEvent: true},
		{name: "an invalid vote becomes a vote for a rejected configuration", terminal: true,
			previous: &invalidVote, vote: vote(consensusRejected, "uid"), wantEvent: true},
		{name: "vote moves from another identity to a rejected configuration", terminal: true,
			previous: &egressVote{Identity: "egress-other", Config: consensusRejected, PodUID: "uid"}, vote: vote(consensusRejected, "uid"),
			wantEvent: true},
		{name: "a same-name replacement pod votes for a rejected configuration", terminal: true,
			previous: voteRef(vote(consensusRejected, "old-uid")), vote: vote(consensusRejected, "uid"), wantEvent: true},
		{name: "unchanged vote for a rejected configuration", terminal: true,
			previous: voteRef(vote(consensusRejected, "uid")), vote: vote(consensusRejected, "uid")},
		{name: "vote for a configuration nobody rejected", terminal: true, vote: vote(consensusA, "uid")},
		{name: "rejection recorded for another identity", terminal: true, other: true, vote: vote(consensusRejected, "uid")},
		{name: "invalid vote", terminal: true, vote: invalidVote},
		{name: "invalid vote while the zero configuration is rejected", zeroRejected: true, vote: invalidVote},
		{name: "an invalid vote becomes a vote for the rejected zero configuration", zeroRejected: true,
			previous: &invalidVote, vote: vote(EgressConfig{}, "uid"), wantEvent: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dt := newTestDiffTracker()
			identity := eventsIdentity
			if tc.other {
				identity = "egress-other"
			}
			if tc.terminal {
				dt.egress.rejected[identity] = rejection
			}
			if tc.warned {
				dt.egress.warned[identity] = egressRejection{Config: consensusRejected, Reason: "quota", Retryable: true}
			}
			if tc.zeroRejected {
				dt.egress.rejected[identity] = egressRejection{Reason: "rejected"}
			}

			event, owed := dt.egressVoteRejectedEventLocked("ns/pod", tc.previous, tc.vote)

			if !assert.Equal(t, tc.wantEvent, owed) || !owed {
				return
			}
			assert.Equal(t, []string{v1.EventTypeWarning + " " + consts.ServiceEgressGatewayReasonConfigRejected + " ns/pod uid"},
				describeEvents([]egressEvent{event}))
			assert.Contains(t, event.message, tc.vote.Config.Summary())
		})
	}
}

// TestEgressRejectionOfLocked pins which rejection of a configuration is reported: the terminal one, else the
// retryable one already warned about.
func TestEgressRejectionOfLocked(t *testing.T) {
	terminal := egressRejection{Config: consensusRejected, Reason: "too many tags"}
	warned := egressRejection{Config: consensusRejected, Reason: "quota", Retryable: true}

	cases := []struct {
		name     string
		terminal *egressRejection
		warned   *egressRejection
		cfg      EgressConfig
		want     *egressRejection
	}{
		{name: "terminal before warned", terminal: &terminal, warned: &warned, cfg: consensusRejected, want: &terminal},
		{name: "warned", warned: &warned, cfg: consensusRejected, want: &warned},
		{name: "another configuration rejected", terminal: &terminal, warned: &warned, cfg: consensusA},
		{name: "nothing rejected", cfg: consensusRejected},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dt := newTestDiffTracker()
			if tc.terminal != nil {
				dt.egress.rejected[eventsIdentity] = *tc.terminal
			}
			if tc.warned != nil {
				dt.egress.warned[eventsIdentity] = *tc.warned
			}
			got, ok := dt.egressRejectionOfLocked(eventsIdentity, tc.cfg)
			if assert.Equal(t, tc.want != nil, ok) && ok {
				assert.Equal(t, *tc.want, got)
			}
		})
	}
}

// TestEmitEgressEvents pins that raised events are queued, in order, until a recorder is published, and are
// emitted directly afterwards.
func TestEmitEgressEvents(t *testing.T) {
	dt := eventsTracker()
	dt.egress.mode[eventsIdentity] = egressModeManaged
	events := dt.egressAppliedEventsLocked(eventsIdentity, consensusA)

	dt.emitEgressEvents(events[:2])
	assert.Equal(t, describeEvents(events[:2]), describeEvents(dt.egress.pendingEvents), "events raised before a recorder exists are queued")

	recorder := record.NewFakeRecorder(10)
	dt.SetEventRecorder(recorder)
	dt.emitEgressEvents(events[2:])

	emitted := drainEvents(recorder)
	if assert.Len(t, emitted, len(events), "the queued events and the later ones are emitted") {
		for _, event := range emitted {
			assert.True(t, strings.HasPrefix(event, v1.EventTypeNormal+" "+consts.ServiceEgressGatewayReasonConfigApplied+" "))
		}
	}
	assert.Empty(t, dt.egress.pendingEvents)
}

func voteRef(vote egressVote) *egressVote { return &vote }

// TestEgressVoteEventsLocked pins the vote-change half of decision H (2.4) for the pod whose registered vote
// changed: InvalidEgressConfig when its annotations are invalid, or refused by a BYO identity; for a new valid
// vote (a join, a same-name replacement, a move, a configuration change, an invalid vote becoming valid) of an
// identity that is not BYO, EgressConfigConflict when its members do not agree and the vote is not the applied
// configuration, and EgressConfigRejected when the vote asks for a rejected configuration. A change of the
// annotations alone, that keeps the configuration, owes nothing to a pod of a managed identity.
func TestEgressVoteEventsLocked(t *testing.T) {
	warning := v1.EventTypeWarning + " "
	invalid := warning + consts.ServiceEgressGatewayReasonInvalidConfig + " ns/pod uid"
	conflict := warning + consts.ServiceEgressGatewayReasonConfigConflict + " ns/pod uid"
	rejected := warning + consts.ServiceEgressGatewayReasonConfigRejected + " ns/pod uid"
	annotated := func(cfg EgressConfig) egressVote {
		return egressVote{Identity: eventsIdentity, Config: cfg, HasAnnotations: true, PodUID: "uid"}
	}
	plain := func(cfg EgressConfig) egressVote {
		return egressVote{Identity: eventsIdentity, Config: cfg, PodUID: "uid"}
	}
	invalidVote := egressVote{Identity: eventsIdentity, Invalid: "idle-timeout: not an integer", HasAnnotations: true, PodUID: "uid"}

	cases := []struct {
		name        string
		mode        egressMode
		untracked   bool          // the identity has no operation
		applied     *EgressConfig // nil: nothing applied
		others      []*EgressConfig
		terminal    *EgressConfig // terminally rejected
		previous    *egressVote
		vote        egressVote
		want        []string
		wantMessage []string // parts of the first message
	}{
		{name: "invalid annotations", mode: egressModeManaged, applied: &consensusA, vote: invalidVote,
			want: []string{invalid}, wantMessage: []string{"idle-timeout: not an integer", `"egress-events"`}},
		{name: "invalid annotations, changed while invalid", mode: egressModeManaged, applied: &consensusA,
			previous: voteRef(egressVote{Identity: eventsIdentity, Invalid: "tags: duplicate key", HasAnnotations: true, PodUID: "uid"}),
			vote:     invalidVote, want: []string{invalid}},
		{name: "invalid annotations of an identity being created", mode: egressModeUnknown, vote: invalidVote, want: []string{invalid}},
		{name: "valid annotations refused by a BYO identity", mode: egressModeBYO, vote: annotated(consensusA),
			want: []string{invalid}, wantMessage: []string{"provided by the user", `"egress-events"`}},
		{name: "invalid annotations refused by a BYO identity", mode: egressModeBYO, vote: invalidVote,
			want: []string{invalid}, wantMessage: []string{"provided by the user"}},
		{name: "no annotations on a BYO identity", mode: egressModeBYO, others: []*EgressConfig{&consensusB}, vote: plain(consensusA)},
		{name: "a new vote the members do not agree on, not applied", mode: egressModeManaged, applied: &consensusApplied,
			others: []*EgressConfig{&consensusB}, vote: annotated(consensusA),
			want: []string{conflict}, wantMessage: []string{consensusA.Summary(), `"egress-events"`}},
		{name: "a new vote for the applied configuration", mode: egressModeManaged, applied: &consensusA,
			others: []*EgressConfig{&consensusB}, vote: annotated(consensusA)},
		{name: "a new vote the members agree on", mode: egressModeManaged, applied: &consensusApplied,
			others: []*EgressConfig{&consensusA}, vote: annotated(consensusA)},
		{name: "an invalid member blocks the agreement", mode: egressModeManaged, applied: &consensusApplied,
			others: []*EgressConfig{&consensusA, nil}, vote: annotated(consensusA), want: []string{conflict}},
		{name: "an unchanged vote in a disagreement", mode: egressModeManaged, applied: &consensusApplied,
			others: []*EgressConfig{&consensusB}, previous: voteRef(annotated(consensusA)), vote: annotated(consensusA)},
		{name: "annotations alone changed in a disagreement", mode: egressModeManaged, applied: &consensusApplied,
			others: []*EgressConfig{&consensusB}, previous: voteRef(plain(consensusA)), vote: annotated(consensusA)},
		{name: "a changed configuration in a disagreement", mode: egressModeManaged, applied: &consensusApplied,
			others: []*EgressConfig{&consensusB}, previous: voteRef(annotated(consensusB)), vote: annotated(consensusA),
			want: []string{conflict}},
		{name: "a vote moved from another identity into a disagreement", mode: egressModeManaged, applied: &consensusApplied,
			others:   []*EgressConfig{&consensusB},
			previous: voteRef(egressVote{Identity: "egress-other", Config: consensusA, HasAnnotations: true, PodUID: "uid"}),
			vote:     annotated(consensusA), want: []string{conflict}},
		{name: "a same-name replacement pod in a disagreement", mode: egressModeManaged, applied: &consensusApplied,
			others:   []*EgressConfig{&consensusB},
			previous: voteRef(egressVote{Identity: eventsIdentity, Config: consensusA, HasAnnotations: true, PodUID: "old-uid"}),
			vote:     annotated(consensusA), want: []string{conflict}},
		{name: "an invalid vote becoming valid in a disagreement", mode: egressModeManaged, applied: &consensusApplied,
			others: []*EgressConfig{&consensusB}, previous: &invalidVote, vote: annotated(consensusA), want: []string{conflict}},
		{name: "a new vote for a rejected configuration in a disagreement", mode: egressModeManaged, applied: &consensusA,
			others: []*EgressConfig{&consensusA}, terminal: &consensusRejected, vote: annotated(consensusRejected),
			want: []string{conflict, rejected}},
		{name: "a new vote agreeing on a rejected configuration", mode: egressModeManaged, applied: &consensusA,
			others: []*EgressConfig{&consensusRejected}, terminal: &consensusRejected, vote: annotated(consensusRejected),
			want: []string{rejected}},
		{name: "an identity being created, its members disagreeing", mode: egressModeUnknown,
			others: []*EgressConfig{&consensusB}, vote: annotated(consensusA), want: []string{conflict}},
		{name: "an identity without an operation, its members disagreeing", mode: egressModeManaged, untracked: true,
			others: []*EgressConfig{&consensusB}, vote: annotated(consensusA), want: []string{conflict}},
		{name: "an identity whose applied configuration is unknown, its members disagreeing", mode: egressModeManaged,
			applied: &egressUnknown, others: []*EgressConfig{&consensusB}, vote: annotated(consensusA), want: []string{conflict}},
		{name: "no conflict is reported on a BYO identity", mode: egressModeBYO, applied: &consensusApplied,
			others: []*EgressConfig{&consensusB}, terminal: &consensusA, vote: plain(consensusA)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dt := newTestDiffTracker()
			dt.egress.mode[eventsIdentity] = tc.mode
			if !tc.untracked {
				op := &ServiceOperationState{ServiceUID: eventsIdentity, State: StateCreated,
					Config: NewOutboundServiceConfig(eventsIdentity, &OutboundConfig{IPFamilies: []string{"IPv4"}})}
				if tc.applied != nil {
					op.LastAppliedConfig = &ServiceConfig{UID: eventsIdentity, OutboundConfig: &OutboundConfig{Egress: *tc.applied}}
				}
				dt.pendingServiceOps[eventsIdentity] = op
			}
			for i, cfg := range tc.others {
				castEgressVote(dt, eventsIdentity, fmt.Sprintf("ns/other-%d", i), cfg, consensusEpoch)
			}
			if tc.terminal != nil {
				dt.egress.rejected[eventsIdentity] = egressRejection{Config: *tc.terminal, Reason: "too many tags"}
			}
			dt.egress.votes.set("ns/pod", tc.vote)

			events := dt.egressVoteEventsLocked("ns/pod", tc.previous, tc.vote)

			assert.Equal(t, tc.want, describeEvents(events))
			for _, part := range tc.wantMessage {
				if assert.NotEmpty(t, events) {
					assert.Contains(t, events[0].message, part)
				}
			}
		})
	}
}
