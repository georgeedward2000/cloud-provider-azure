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

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/cloud-provider-azure/pkg/consts"
)

// Egress pod events are raised only when a pod's vote or its identity's applied configuration changes.
// They are raised under dt.mu, from the vote registry, and emitted by emitEgressEvents once dt.mu is
// released.

// egressVotePod returns the pod that cast a vote, as the object of its events.
func egressVotePod(podKey string, uid types.UID) *v1.Pod {
	namespace, name, _ := strings.Cut(podKey, "/")
	return &v1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name, UID: uid}}
}

// egressMemberEventsLocked returns the event for every member of identity whose vote matches, in pod key
// order. Must be called with dt.mu held.
func (dt *DiffTracker) egressMemberEventsLocked(identity string, matches func(egressVote) bool, eventType, reason, message string) []egressEvent {
	var events []egressEvent
	for _, podKey := range dt.egress.votes.podKeys(identity) {
		vote, _ := dt.egress.votes.get(podKey)
		if !matches(vote) {
			continue
		}
		events = append(events, egressEvent{pod: egressVotePod(podKey, vote.PodUID), eventType: eventType, reason: reason, message: message})
	}
	return events
}

// egressAppliedEventsLocked returns the events telling every member of identity that its NAT Gateway now
// has the configuration applied. Must be called with dt.mu held.
func (dt *DiffTracker) egressAppliedEventsLocked(identity string, applied EgressConfig) []egressEvent {
	return dt.egressMemberEventsLocked(identity, func(egressVote) bool { return true }, v1.EventTypeNormal,
		consts.ServiceEgressGatewayReasonConfigApplied,
		fmt.Sprintf("Egress identity %q applied NAT Gateway configuration: %s.", identity, applied.Summary()))
}

// egressRejectedMessage describes a rejection to the pods voting for the rejected configuration.
func egressRejectedMessage(identity string, rejection egressRejection) string {
	next := "it will not be retried until the members of the identity agree on another configuration"
	if rejection.Retryable {
		next = "it will be retried while the members of the identity agree on it"
	}
	return fmt.Sprintf("Egress identity %q rejected NAT Gateway configuration %s: %s; %s.",
		identity, rejection.Config.Summary(), rejection.Reason, next)
}

// egressRejectedEventsLocked returns the events telling the members of identity that vote for the rejected
// configuration that it was rejected. Must be called with dt.mu held.
func (dt *DiffTracker) egressRejectedEventsLocked(identity string, rejection egressRejection) []egressEvent {
	return dt.egressMemberEventsLocked(identity, func(vote egressVote) bool {
		return vote.Invalid == "" && vote.Config == rejection.Config
	}, v1.EventTypeWarning, consts.ServiceEgressGatewayReasonConfigRejected, egressRejectedMessage(identity, rejection))
}

// egressRejectionOfLocked returns the rejection recorded for configuration cfg of identity: the terminal one,
// else the retryable one already warned about. Must be called with dt.mu held.
func (dt *DiffTracker) egressRejectionOfLocked(identity string, cfg EgressConfig) (egressRejection, bool) {
	if rejection, ok := dt.egress.rejected[identity]; ok && rejection.Config == cfg {
		return rejection, true
	}
	if rejection, ok := dt.egress.warned[identity]; ok && rejection.Config == cfg {
		return rejection, true
	}
	return egressRejection{}, false
}

// egressVoteRejectedEventLocked returns the event owed to the pod podKey whose vote changed from previous
// (nil when it had none) to vote, when vote asks for a configuration rejected for its identity. An
// unchanged vote owes nothing: the pod was told when the configuration was first rejected or when it
// voted for it. Must be called with dt.mu held.
func (dt *DiffTracker) egressVoteRejectedEventLocked(podKey string, previous *egressVote, vote egressVote) (egressEvent, bool) {
	if !isNewValidEgressVote(previous, vote) {
		return egressEvent{}, false
	}
	rejection, rejected := dt.egressRejectionOfLocked(vote.Identity, vote.Config)
	if !rejected {
		return egressEvent{}, false
	}
	return egressEvent{
		pod:       egressVotePod(podKey, vote.PodUID),
		eventType: v1.EventTypeWarning,
		reason:    consts.ServiceEgressGatewayReasonConfigRejected,
		message:   egressRejectedMessage(vote.Identity, rejection),
	}, true
}

// isNewValidEgressVote reports whether vote, which replaced previous (nil when the pod had no vote), is a valid
// vote the pod did not cast before: the pod joined the identity, or a same-name replacement pod did, or its vote
// became valid or asks for another configuration.
func isNewValidEgressVote(previous *egressVote, vote egressVote) bool {
	if vote.Invalid != "" {
		return false
	}
	return previous == nil || previous.Invalid != "" || previous.Identity != vote.Identity || previous.PodUID != vote.PodUID ||
		previous.Config != vote.Config
}

// egressVoteEventsLocked returns the events owed to the pod podKey whose registered vote changed from previous
// (nil when it had none) to vote (decision H):
//   - InvalidEgressConfig when its egress annotations are refused because the identity's NAT Gateway is BYO,
//     which accepts no configuration, or are invalid;
//   - for a new valid vote of a managed or not yet created identity, EgressConfigConflict when the members of
//     the identity do not agree and the vote is not the applied configuration, and EgressConfigRejected when the
//     vote asks for a configuration rejected for the identity.
//
// Must be called with dt.mu held.
func (dt *DiffTracker) egressVoteEventsLocked(podKey string, previous *egressVote, vote egressVote) []egressEvent {
	pod := egressVotePod(podKey, vote.PodUID)
	switch {
	case dt.egressVoteRefusedByBYOLocked(vote):
		return []egressEvent{{pod: pod, eventType: v1.EventTypeWarning, reason: consts.ServiceEgressGatewayReasonInvalidConfig,
			message: egressBYOInvalidMessage(vote.Identity)}}
	case vote.Invalid != "":
		return []egressEvent{{pod: pod, eventType: v1.EventTypeWarning, reason: consts.ServiceEgressGatewayReasonInvalidConfig,
			message: fmt.Sprintf("The egress annotations of this pod are invalid: %s. The pod keeps egress through egress identity %q, "+
				"whose NAT Gateway configuration cannot change until they are fixed.", vote.Invalid, vote.Identity)}}
	case dt.egress.mode[vote.Identity] == egressModeBYO || !isNewValidEgressVote(previous, vote):
		return nil
	}

	var events []egressEvent
	if _, agreed := dt.egress.votes.agreed(vote.Identity); !agreed && !dt.egressVoteAppliedLocked(vote) {
		events = append(events, egressEvent{pod: pod, eventType: v1.EventTypeWarning, reason: consts.ServiceEgressGatewayReasonConfigConflict,
			message: fmt.Sprintf("Egress identity %q keeps its NAT Gateway configuration: its pods do not agree on one, and this pod "+
				"votes for %s. The configuration changes once every pod of the identity votes for the same one.",
				vote.Identity, vote.Config.Summary())})
	}
	if event, owed := dt.egressVoteRejectedEventLocked(podKey, previous, vote); owed {
		events = append(events, event)
	}
	return events
}

// egressVoteAppliedLocked reports whether vote asks for the configuration applied to its identity's NAT Gateway.
// Must be called with dt.mu held.
func (dt *DiffTracker) egressVoteAppliedLocked(vote egressVote) bool {
	opState, exists := dt.pendingServiceOps[vote.Identity]
	return exists && appliedEgress(opState) == vote.Config
}

// egressVoteRefusedByBYOLocked reports whether vote is invalid because its identity's NAT Gateway is BYO,
// which accepts no egress configuration: the pod has egress annotations. This is derived from the
// identity's mode and never stored in the vote, so the vote counts again when the identity is re-created
// as a managed one. Must be called with dt.mu held.
func (dt *DiffTracker) egressVoteRefusedByBYOLocked(vote egressVote) bool {
	return vote.HasAnnotations && dt.egress.mode[vote.Identity] == egressModeBYO
}

// egressBYOInvalidMessage tells a pod with egress annotations that its identity's NAT Gateway is BYO.
func egressBYOInvalidMessage(identity string) string {
	return fmt.Sprintf("Egress identity %q uses a NAT Gateway provided by the user, which accepts no egress configuration; "+
		"the egress annotations of this pod are ignored.", identity)
}

// egressBYOInvalidEventsLocked returns the events telling the members of a BYO identity that have egress
// annotations that the annotations are invalid there. Must be called with dt.mu held.
func (dt *DiffTracker) egressBYOInvalidEventsLocked(identity string) []egressEvent {
	return dt.egressMemberEventsLocked(identity, dt.egressVoteRefusedByBYOLocked, v1.EventTypeWarning,
		consts.ServiceEgressGatewayReasonInvalidConfig, egressBYOInvalidMessage(identity))
}

// emitEgressEvents emits events raised under dt.mu, in order, or queues them until a recorder is
// published. Must be called without dt.mu.
func (dt *DiffTracker) emitEgressEvents(events []egressEvent) {
	for _, event := range events {
		dt.recordEgressEvent(event.pod, event.eventType, event.reason, event.message)
	}
}
