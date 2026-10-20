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
	"slices"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork/v12"
	"k8s.io/apimachinery/pkg/types"
)

// ================================================================================================
// Egress membership: every member pod of an egress identity casts one vote (4.1)
// ================================================================================================

// addEgressPod makes pod a member of the egress identity, in one step, with the addresses accepted for it (at
// least one, each with the node location of its IP family) and the vote castVote returns for the parse context of
// identity, its NAT Gateway configuration vote for the identity's IP families, whose identity, pod UID and creation
// time are the pod's. castVote runs under dt.mu once the pod's addresses are recorded, when the identity's
// operation, created, revived or found, decided the IP families the vote is counted for
// (egressParseContextOfLocked), so that no change of them, or of the cluster's, can come between parsing the vote
// and registering it.
//
// An identity that collides with an inbound service admits nothing: the vote the same pod (UID) cast before,
// for whichever identity, is withdrawn and that identity re-evaluated. Otherwise every address takes the path
// of a pod address, where a new operation is created with this pod (name and UID) as its seed; the identity's
// votes parsed for other IP families are parsed again (reparseEgressVotesLocked); the vote then replaces the
// pod's previous vote, also one for another identity (a label move), which is re-evaluated; and the identity is
// evaluated. A vote that changed, or votes parsed again, also clear the identity's startup baseline and owe their
// pods their events, emitted once dt.mu is released.
func (dt *DiffTracker) addEgressPod(identity string, pod egressPodRef, accepted []egressAddress, castVote func(egressParseContext) egressVote) {
	var events []egressEvent
	defer func() { dt.emitEgressEvents(events) }()
	dt.mu.Lock()
	defer dt.mu.Unlock()

	podKey := pod.key()
	if identity == "" || pod.Namespace == "" || pod.Name == "" || !slices.ContainsFunc(accepted, egressAddress.valid) {
		dt.logger.V(4).Info("Could not add egress pod with invalid parameters", "service", identity, "pod", podKey, "addresses", accepted)
		return
	}
	if dt.outboundIdentityConflictsWithInboundLocked(identity) {
		dt.logger.V(2).Info("Rejected egress pod whose identity collides with an inbound service", "service", identity, "pod", podKey)
		if previous, ok := dt.egress.votes.get(podKey); ok {
			dt.removeEgressVoteLocked(previous.Identity, podKey, pod.UID)
		}
		return
	}

	for _, address := range accepted {
		if address.valid() {
			dt.addPodAddressLocked(identity, podKey, string(pod.UID), address.Location, address.IP)
		}
	}
	pc := dt.egressParseContextOfLocked(identity)
	flips, reparsed := dt.reparseEgressVotesLocked(identity, pc)
	vote := pod.voteFor(identity, castVote(pc))
	previous, hadVote := dt.egress.votes.get(podKey)
	unchanged := hadVote && sameEgressVote(previous, vote)
	// Registered also when unchanged: a later re-parse reads the annotations it was cast from.
	if oldIdentity := dt.egress.votes.set(podKey, vote); oldIdentity != "" && oldIdentity != identity {
		dt.egressVotesChangedLocked(oldIdentity)
	}
	if unchanged && !reparsed {
		// The operation may have been created or revived above.
		dt.evaluateEgressConsensusLocked(identity)
		return
	}
	var before *egressVote
	if hadVote {
		before = &previous
	}
	events = dt.egressCastEventsLocked(flips, podKey, before, unchanged, vote)
	dt.egressVotesChangedLocked(identity)
}

// setEgressVote records the vote castVote returns, under dt.mu, for the parse context of identity as the vote of pod,
// a member of the egress identity whose egress annotations alone changed. It is ignored unless the pod (name and UID)
// has a vote registered for identity: only members vote. The identity's votes parsed for other IP families are parsed
// again first (reparseEgressVotesLocked). A vote that changed, or votes parsed again, clear the identity's startup
// baseline, re-evaluate the identity and owe their pods their events, emitted once dt.mu is released.
func (dt *DiffTracker) setEgressVote(identity string, pod egressPodRef, castVote func(egressParseContext) egressVote) {
	var events []egressEvent
	defer func() { dt.emitEgressEvents(events) }()
	dt.mu.Lock()
	defer dt.mu.Unlock()

	podKey := pod.key()
	if previous, ok := dt.egress.votes.get(podKey); !ok || previous.Identity != identity || previous.PodUID != pod.UID {
		dt.logger.V(4).Info("Ignored egress vote of a pod that is not a member of the identity", "service", identity, "pod", podKey, "podUID", pod.UID)
		return
	}
	pc := dt.egressParseContextOfLocked(identity)
	flips, reparsed := dt.reparseEgressVotesLocked(identity, pc)
	previous, _ := dt.egress.votes.get(podKey)
	vote := pod.voteFor(identity, castVote(pc))
	unchanged := sameEgressVote(previous, vote)
	// Registered also when unchanged: a later re-parse reads the annotations it was cast from.
	dt.egress.votes.set(podKey, vote)
	if unchanged && !reparsed {
		return
	}
	events = dt.egressCastEventsLocked(flips, podKey, &previous, unchanged, vote)
	dt.egressVotesChangedLocked(identity)
}

// RemoveEgressVote withdraws the vote of the pod podKey with uid from the egress identity, or from whichever
// identity it was cast for when identity is empty (a pod without an egress label is a member of none), and
// re-evaluates that identity. The pod's addresses are left alone. It reports whether a vote was withdrawn.
func (dt *DiffTracker) RemoveEgressVote(identity, podKey string, uid types.UID) bool {
	dt.mu.Lock()
	defer dt.mu.Unlock()

	if identity == "" {
		vote, _ := dt.egress.votes.get(podKey)
		identity = vote.Identity
	}
	return dt.removeEgressVoteLocked(identity, podKey, uid)
}

// LivePodLeftEgress handles a live pod of the egress identity that has no accepted address left, such as a pod
// whose addresses changed to ones without a node location of their IP family. The addresses it leaves are
// drained, only where this pod (name and UID) holds them, its vote is withdrawn, and the identity is deleted
// after its last pod as for a removed pod. Nothing is recorded to remove the finalizer of the live pod: it keeps
// it until it is deleted, which takes the removal path of a pod (DeletePod).
func (dt *DiffTracker) LivePodLeftEgress(identity, location string, addresses []string, pod egressPodRef) {
	dt.mu.Lock()
	defer dt.mu.Unlock()

	if pod.Namespace == "" || pod.Name == "" {
		dt.logger.V(4).Info("Could not drain live egress pod without a name", "service", identity, "pod", pod.key())
		return
	}
	result := dt.deletePodLocked(identity, location, addresses, pod.Namespace, pod.Name, string(pod.UID), podLeftLive)
	removed := dt.removeEgressVoteLocked(identity, pod.key(), pod.UID)
	dt.logger.V(2).Info("Drained live egress pod without accepted addresses", "service", identity, "pod", pod.key(),
		"podUID", pod.UID, "lastPod", result.IsLastPod, "voteWithdrawn", removed)
}

// seedEgressVoteLocked registers the vote of pod, a member of the egress identity admitted at startup, before
// the identities are planned: nothing is evaluated or triggered and no event is raised, as the unchanged
// baseline owes none (2.4). An identity that collides with an inbound service, tracked or known from the
// cluster or from NRP, has no members: the vote is not registered, it reports false, and the caller must not
// admit the pod's addresses either (I1). Must be called with dt.mu held.
func (dt *DiffTracker) seedEgressVoteLocked(identity string, pod egressPodRef, vote egressVote) bool {
	if identity == "" || dt.outboundIdentityConflictsWithInboundLocked(identity) ||
		dt.K8sResources.Services.Has(identity) || dt.NRPResources.LoadBalancers.Has(identity) {
		return false
	}
	dt.egress.votes.set(pod.key(), pod.voteFor(identity, vote))
	return true
}

// removeEgressVoteLocked withdraws the vote of the pod podKey with uid from identity, when it was cast there,
// and then follows the change of the identity's votes. It reports whether a vote was withdrawn. Must be called
// with dt.mu held.
func (dt *DiffTracker) removeEgressVoteLocked(identity, podKey string, uid types.UID) bool {
	if !dt.egress.votes.remove(podKey, uid, identity) {
		return false
	}
	dt.logger.V(4).Info("Withdrew egress vote", "service", identity, "pod", podKey, "podUID", uid)
	dt.egressVotesChangedLocked(identity)
	return true
}

// egressVotesChangedLocked follows a change of the votes of identity: a rejection that startup found is no
// longer the unchanged baseline, and the votes may now agree on another configuration, or no longer agree.
// Must be called with dt.mu held.
func (dt *DiffTracker) egressVotesChangedLocked(identity string) {
	if opState, exists := dt.pendingServiceOps[identity]; exists {
		opState.Egress.StartupBaseline = false
	}
	dt.evaluateEgressConsensusLocked(identity)
}

// reparseEgressVotesLocked parses again for pc the votes of identity that were parsed for other IP families, from the
// annotations they were parsed from, so that every vote counts for the families its NAT Gateway serves or is
// (re)created with; when every vote was parsed for them, it costs a counter lookup. It returns the votes that became
// valid or invalid (flips), whose pods are owed the events of a changed vote once the caller has registered the votes it
// changes too (egressFlipEventsLocked): a vote that stays valid or invalid owes nothing, as its pod did not change. It
// reports whether a vote was parsed again, a change of the votes the caller follows. Must be called with dt.mu held.
func (dt *DiffTracker) reparseEgressVotesLocked(identity string, pc egressParseContext) ([]egressVoteFlip, bool) {
	if dt.egress.votes.parsedFor(identity, pc.WantsIPv6) {
		return nil, false
	}
	var flips []egressVoteFlip
	n := 0
	for _, podKey := range dt.egress.votes.podKeys(identity) {
		previous, _ := dt.egress.votes.get(podKey)
		if previous.ParsedForIPv6 == pc.WantsIPv6 {
			continue
		}
		vote := previous.reparsed(pc)
		dt.egress.votes.set(podKey, vote)
		n++
		if (previous.Invalid == "") != (vote.Invalid == "") && !dt.egressVoteRefusedByBYOLocked(vote) {
			flips = append(flips, egressVoteFlip{podKey: podKey, previous: previous})
		}
	}
	dt.logger.V(2).Info("Parsed egress votes again for the IP families of the identity", "service", identity,
		"ipv6", pc.WantsIPv6, "votes", n, "validityChanged", len(flips))
	return flips, true
}

// egressVoteFlip is a vote that a re-parse for other IP families made valid or invalid: its pod and the vote before.
type egressVoteFlip struct {
	podKey   string
	previous egressVote
}

// egressFlipEventsLocked returns the events owed, under the rules of a changed vote (egressVoteEventsLocked) and the
// votes registered now, to the pods whose vote a re-parse made valid or invalid (flips), except the pod skip, whose own
// cast decides its events. Must be called with dt.mu held.
func (dt *DiffTracker) egressFlipEventsLocked(flips []egressVoteFlip, skip string) []egressEvent {
	var events []egressEvent
	for _, f := range flips {
		if f.podKey == skip {
			continue
		}
		if vote, ok := dt.egress.votes.get(f.podKey); ok {
			events = append(events, dt.egressVoteEventsLocked(f.podKey, &f.previous, vote)...)
		}
	}
	return events
}

// egressCastEventsLocked returns the events owed once the vote of the pod podKey, cast after the re-parse that returned
// flips, is registered, so that every event judges the votes as they now are: to the other pods whose vote the re-parse
// made valid or invalid, and to the pod itself, judged from its vote before the re-parse when that re-parse flipped it,
// else from before (nil when it had no vote) unless its vote is unchanged. Must be called with dt.mu held.
func (dt *DiffTracker) egressCastEventsLocked(flips []egressVoteFlip, podKey string, before *egressVote, unchanged bool,
	vote egressVote) []egressEvent {
	events := dt.egressFlipEventsLocked(flips, podKey)
	if i := slices.IndexFunc(flips, func(f egressVoteFlip) bool { return f.podKey == podKey }); i >= 0 {
		before = &flips[i].previous
	} else if unchanged {
		return events
	}
	return append(events, dt.egressVoteEventsLocked(podKey, before, vote)...)
}

// sameEgressVote reports whether a and b are the same pod voting the same way for the same identity. The pod's
// creation time is not compared: it never changes; nor is what the votes were parsed from.
func sameEgressVote(a, b egressVote) bool {
	return a.Identity == b.Identity && a.PodUID == b.PodUID && a.Config == b.Config && a.Invalid == b.Invalid &&
		a.HasAnnotations == b.HasAnnotations
}

// ================================================================================================
// Egress NAT Gateway configuration by pod consensus
// ================================================================================================

// noteEgressMutationStarted marks the operation of serviceUID, if any, as having started to write to
// Azure, so its completion knows that the run may have changed Azure. Takes dt.mu.
func (dt *DiffTracker) noteEgressMutationStarted(serviceUID string) {
	dt.mu.Lock()
	defer dt.mu.Unlock()
	if opState, exists := dt.pendingServiceOps[serviceUID]; exists {
		opState.Egress.MutationStarted = true
	}
}

// egressParseContextFor returns what parsing needs to know about an egress identity of this cluster
// whose NAT Gateway serves families.
func (dt *DiffTracker) egressParseContextFor(families []string) egressParseContext {
	return egressParseContext{
		WantsIPv6: slices.ContainsFunc(families, func(family string) bool {
			return strings.EqualFold(family, string(armnetwork.IPVersionIPv6))
		}),
		NetworkSubscription: dt.config.networkResourceSubscriptionID(),
		NodeResourceGroup:   dt.config.ResourceGroup,
	}
}

// creationConfigLocked returns the configuration the NAT Gateway of identity, serving families, is
// created with: the agreed configuration, else the seed's vote when it is valid, else the default. The
// seed is the pod that triggered the identity's operation (the same pod: name and UID) while that pod is
// a member of the identity, else the identity's oldest member. Must be called with dt.mu held.
func (dt *DiffTracker) creationConfigLocked(identity string, families []string) EgressConfig {
	if agreed, ok := dt.egress.votes.agreed(identity); ok {
		return agreed
	}
	if seed, ok := dt.egressSeedVoteLocked(identity); ok && seed.Invalid == "" {
		return seed.Config
	}
	return defaultEgressConfig(dt.egressParseContextFor(families))
}

// egressSeedVoteLocked returns the vote of the pod that seeds the creation configuration of identity.
// Must be called with dt.mu held.
func (dt *DiffTracker) egressSeedVoteLocked(identity string) (egressVote, bool) {
	if opState, exists := dt.pendingServiceOps[identity]; exists && opState.TriggeringPodUID != "" {
		podKey := opState.TriggeringPodNamespace + "/" + opState.TriggeringPodName
		if vote, ok := dt.egress.votes.get(podKey); ok && vote.Identity == identity && string(vote.PodUID) == opState.TriggeringPodUID {
			return vote, true
		}
	}
	_, vote, ok := dt.egress.votes.oldest(identity)
	return vote, ok
}

// evaluateEgressConsensusLocked aligns the operation of a managed egress identity with the votes of
// its members. It is called after a vote, mode or completion change of the identity.
//
// The agreed configuration is dispatchable when it differs from the applied one and was not rejected
// terminally; the identity is actionable when it is dispatchable or its NAT Gateway is owed an
// observation. A rejection is remembered until the members agree on another configuration, so that
// agreeing on the rejected one again tries it once more. A Created identity with something actionable
// starts an update run; an update that is not dispatched (queued, backing off or quiet) follows the votes
// or is cancelled, and a new agreement it takes is dispatched without the backoff of the configuration it
// replaces; a dispatched update runs to completion; a creation is left alone, as its configuration is decided
// at dispatch (processBatch), and so is a deletion, as it sweeps every reserved name. A BYO identity accepts no
// configuration: it only gets a residue cleanup when it is owed an observation (evaluateEgressResidueLocked); an
// identity whose mode is not resolved waits for its creation. Must be called with dt.mu held.
func (dt *DiffTracker) evaluateEgressConsensusLocked(identity string) {
	opState, exists := dt.pendingServiceOps[identity]
	if !exists || opState.Config.IsInbound {
		return
	}
	switch dt.egress.mode[identity] {
	case egressModeManaged:
	case egressModeBYO:
		dt.evaluateEgressResidueLocked(opState)
		return
	default:
		return
	}
	agreed, hasAgreed := dt.egress.votes.agreed(identity)
	if hasAgreed {
		dt.egress.forgetRejections(identity, func(rejected EgressConfig) bool { return rejected != agreed })
	}
	applied := appliedEgress(opState)
	rejection, rejected := dt.egress.rejected[identity]
	agreedRejected := rejected && rejection.Config == agreed
	dispatchable := hasAgreed && agreed != applied && !agreedRejected
	actionable := dispatchable || opState.Egress.NeedsObservation

	switch opState.State {
	case StateCreated:
		if !actionable {
			return
		}
		desired := egressUnknown // observe-and-clean
		if dispatchable {
			desired = agreed
		}
		setDesiredEgress(opState, desired)
		opState.State = StateUpdateInProgress
		opState.LastAttempt = time.Now().Format(time.RFC3339)
		dt.triggerServiceUpdater()

	case StateUpdateInProgress:
		if opState.InFlightConfig != nil {
			// Dispatch is the linearisation point: the run applies its snapshot. A new agreement is
			// applied after it; without one the in-flight value stays desired, so nothing is reverted.
			if hasAgreed {
				setDesiredEgress(opState, agreed)
			} else {
				setDesiredEgress(opState, outboundEgress(opState.InFlightConfig.OutboundConfig))
			}
			return
		}
		switch {
		case dispatchable:
			if outboundEgress(opState.Config.OutboundConfig) != agreed {
				// A new agreement is applied at once, not after the backoff or the park of the configuration it
				// replaces. The quiet period still holds it back (retryGate).
				resetRetryStateLocked(opState)
				dt.triggerServiceUpdater()
			}
			setDesiredEgress(opState, agreed)
		case opState.Egress.NeedsObservation:
			setDesiredEgress(opState, egressUnknown)
		default:
			// Nothing left to apply or observe: cancel the run.
			setDesiredEgress(opState, applied)
			opState.State = StateCreated
			resetRetryStateLocked(opState)
		}
	}
}

// evaluateEgressResidueLocked gives a BYO identity that is owed an observation (startup found managed
// resources of its managed past, 4.6) one residue cleanup run. That run is an update of a BYO identity, and
// its desired configuration is the marker egressUnknown, since no configuration is ever applied to a BYO NAT
// Gateway: it deletes the leftovers and never touches the user's NAT Gateway or the registration (4.4). A
// residue cleanup not dispatched yet is cancelled once nothing is owed; one in flight runs to completion.
// Must be called with dt.mu held.
func (dt *DiffTracker) evaluateEgressResidueLocked(opState *ServiceOperationState) {
	switch opState.State {
	case StateCreated:
		if !opState.Egress.NeedsObservation {
			return
		}
		setDesiredEgress(opState, egressUnknown)
		opState.State = StateUpdateInProgress
		opState.LastAttempt = time.Now().Format(time.RFC3339)
		dt.triggerServiceUpdater()

	case StateUpdateInProgress:
		switch {
		case opState.InFlightConfig != nil:
		case opState.Egress.NeedsObservation:
			setDesiredEgress(opState, egressUnknown)
		default:
			setDesiredEgress(opState, appliedEgress(opState))
			opState.State = StateCreated
			resetRetryStateLocked(opState)
		}
	}
}

// setDesiredEgress makes cfg the desired NAT Gateway configuration of opState. A published outbound
// config is never modified: a new value replaces it, so every snapshot taken from it keeps its value.
func setDesiredEgress(opState *ServiceOperationState, cfg EgressConfig) {
	desired := opState.Config.OutboundConfig.DeepCopy()
	if desired == nil {
		desired = &OutboundConfig{}
	}
	desired.Egress = cfg
	opState.Config.OutboundConfig = desired
}

// appliedEgress returns the NAT Gateway configuration last applied for opState; the zero value when
// nothing was recorded.
func appliedEgress(opState *ServiceOperationState) EgressConfig {
	if opState.LastAppliedConfig == nil {
		return EgressConfig{}
	}
	return outboundEgress(opState.LastAppliedConfig.OutboundConfig)
}

// adoptUnknownEgressLocked records that this process never applied the NAT Gateway of a revived managed
// identity: its applied configuration is egressUnknown (LastAppliedConfig is set for every managed
// identity) with none known before it, the desired one follows it, and the NAT Gateway is owed an
// observation. A missing outbound config takes the cluster's families, as a new identity's does. Must be
// called with dt.mu held.
func (dt *DiffTracker) adoptUnknownEgressLocked(opState *ServiceOperationState) {
	outbound := opState.Config.OutboundConfig.DeepCopy()
	if outbound == nil {
		outbound = &OutboundConfig{IPFamilies: dt.outboundIPFamiliesLocked()}
	}
	outbound.Egress = egressUnknown
	opState.Config.OutboundConfig = outbound
	applied := opState.Config.snapshot()
	opState.LastAppliedConfig = &applied
	opState.Egress.LastKnownApplied, opState.Egress.UnclearWrites = nil, nil
	opState.Egress.requireObservation()
}

// isEgressConfigurationRunLocked reports whether opState is an egress configuration run: an update of an
// identity whose NAT Gateway already serves egress, an apply or observe-and-clean of a managed one or the
// residue cleanup of a BYO one. Must be called with dt.mu held.
func (dt *DiffTracker) isEgressConfigurationRunLocked(serviceUID string, opState *ServiceOperationState) bool {
	return !opState.Config.IsInbound && opState.State == StateUpdateInProgress && dt.egress.mode[serviceUID] != egressModeUnknown
}

// setAppliedEgressLocked records cfg as the configuration applied to the NAT Gateway of opState's identity. A
// published config is never modified: the applied config is a new value, built from the config the run was
// dispatched with, else from the applied or the desired one; a missing outbound config takes the cluster's
// families. Recording the unknown configuration after a known one remembers that one (LastKnownApplied); recording a
// known configuration forgets what was remembered while it was unknown. Must be called with dt.mu held.
func (dt *DiffTracker) setAppliedEgressLocked(opState *ServiceOperationState, cfg EgressConfig) {
	if cfg != egressUnknown {
		opState.Egress.LastKnownApplied, opState.Egress.UnclearWrites = nil, nil
	} else if previous := appliedEgress(opState); opState.LastAppliedConfig != nil && previous != egressUnknown {
		opState.Egress.LastKnownApplied = &previous
	}
	base := opState.InFlightConfig
	if base == nil {
		base = opState.LastAppliedConfig
	}
	if base == nil {
		base = &opState.Config
	}
	applied := base.snapshot()
	if applied.OutboundConfig == nil {
		applied.OutboundConfig = &OutboundConfig{IPFamilies: dt.outboundIPFamiliesLocked()}
	}
	applied.OutboundConfig.Egress = cfg
	opState.LastAppliedConfig = &applied
}

// appliedEgressChanged reports whether recording applied as the configuration of an identity's NAT Gateway,
// after previous when hadApplied, is a change its members are told about: not to the unknown configuration, not
// the default when the identity is first created, and otherwise when it differs from the configuration last known
// applied, which is lastKnown while previous is unknown. When none is known (an identity this process adopted), it
// is told only when written (this process wrote it, with the run's NAT Gateway PUT or an earlier write whose outcome
// was unknown).
func appliedEgressChanged(hadApplied bool, previous, applied, defaults EgressConfig, written bool, lastKnown *EgressConfig) bool {
	if previous == egressUnknown && lastKnown != nil {
		previous = *lastKnown
	}
	switch {
	case applied == egressUnknown:
		return false
	case !hadApplied:
		return applied != defaults
	case previous == egressUnknown:
		return written
	default:
		return previous != applied
	}
}

// recordEgressOutcomeLocked records what an outbound run did to Azure. It runs first for every outbound
// completion, also one that a deletion now preempts and a recovered panic, which reports no outcome and is
// judged by Egress.MutationStarted alone (4.5):
//
//  0. A write whose outcome is unknown (CommitUnknown, or a panic after the first write) leaves the applied
//     configuration unknown, owes an observation and starts the quiet period. The configuration such a write of
//     an apply was applying is remembered (UnclearWrites): an observation reports it when it finds it applied, as
//     the apply would have, also while the members no longer agree on it.
//  1. The mode the run resolved is recorded. An identity found to be BYO tells its pods with egress
//     annotations that these are invalid there; this is derived from the mode, never stored in the votes.
//  2. The configuration the NAT Gateway satisfies is recorded as applied, also after an update error past
//     the commit point, and announced to the members when it changed (appliedEgressChanged), also when a
//     write whose outcome was unknown applied it: it is compared with the configuration last known applied, or,
//     when none is known, announced when this process wrote it, also with such a write. An applied configuration
//     is no longer rejected.
//  3. The observation is owed after a run that wrote, or that reports an applied configuration, without
//     completing its link repair and sweep (Swept): what it left undone is retried even when the NAT Gateway
//     needed no write. A complete sweep settles it unless an obligation was raised since the run was
//     dispatched. A busy run or a refusal before any write leaves it as it is.
//  4. A rejection is recorded, terminal or retryable (warned about once), and announced to the pods voting
//     for the rejected configuration unless that configuration was rejected already or this is the startup
//     baseline; a configuration warned about that a later try rejects terminally is announced once more, with
//     the final reason. After a creation fell back to the default, the creation configuration it was dispatched
//     with stays desired: after a retryable rejection the evaluation applies it again while the members agree on
//     it; after a terminal one, only once they have agreed on another configuration (4.2).
//
// A run that failed before the identity's NAT Gateway was ever recorded records no applied configuration: a
// retried creation applies again, and an identity that was never created still tells a creation from an
// update. It returns the events to emit once dt.mu is released. Must be called with dt.mu held.
func (dt *DiffTracker) recordEgressOutcomeLocked(serviceUID string, opState *ServiceOperationState, hadApplied, success bool,
	outcome *egressOutcome) []egressEvent {
	wrote := opState.Egress.MutationStarted
	opState.Egress.MutationStarted = false
	previous, lastKnown, unclear := appliedEgress(opState), opState.Egress.LastKnownApplied, opState.Egress.UnclearWrites
	recordApplied := success || hadApplied

	commitUnknown := (outcome == nil && wrote) || (outcome != nil && outcome.CommitUnknown)
	if commitUnknown {
		if recordApplied {
			dt.setAppliedEgressLocked(opState, egressUnknown)
		}
		if inFlight := opState.InFlightConfig; hadApplied && inFlight != nil {
			if written := outboundEgress(inFlight.OutboundConfig); written != egressUnknown {
				opState.Egress.rememberUnclearWrite(written)
			}
		}
		opState.Egress.requireObservation()
		opState.Egress.QuietUntil = time.Now().Add(getEgressSettleDelay())
		dt.logger.V(2).Info("Held back egress identity runs after a write whose outcome is unknown",
			"service", serviceUID, "quietUntil", opState.Egress.QuietUntil)
	}
	if outcome == nil {
		return nil
	}

	var events []egressEvent
	if outcome.Mode != egressModeUnknown {
		wasBYO := dt.egress.mode[serviceUID] == egressModeBYO
		dt.egress.mode[serviceUID] = outcome.Mode
		if outcome.Mode == egressModeBYO && !wasBYO {
			events = append(events, dt.egressBYOInvalidEventsLocked(serviceUID)...)
		}
	}

	if outcome.Applied != nil && recordApplied {
		applied := *outcome.Applied
		dt.setAppliedEgressLocked(opState, applied)
		defaults := defaultEgressConfig(dt.egressParseContextFor(outboundFamilies(opState.LastAppliedConfig.OutboundConfig)))
		// This process wrote it: the run's NAT Gateway PUT, or an earlier write whose outcome was unknown.
		written := outcome.NATWritten || slices.Contains(unclear, applied)
		if appliedEgressChanged(hadApplied, previous, applied, defaults, written, lastKnown) {
			events = append(events, dt.egressAppliedEventsLocked(serviceUID, applied)...)
		}
		dt.egress.forgetRejections(serviceUID, func(rejected EgressConfig) bool { return rejected == applied })
	}

	switch {
	case commitUnknown:
		// Owed since rule 0.
	case outcome.Swept && !opState.Egress.raisedSinceDispatch():
		opState.Egress.NeedsObservation = false
	case !outcome.Swept && (wrote || outcome.Applied != nil):
		opState.Egress.requireObservation()
	}

	if rejection := outcome.Rejected; rejection != nil {
		prior, known := dt.egressRejectionOfLocked(serviceUID, rejection.Config)
		turnedFinal := known && prior.Retryable && !rejection.Retryable
		if rejection.Retryable {
			dt.egress.warned[serviceUID] = *rejection
		} else {
			dt.egress.rejected[serviceUID] = *rejection
		}
		agreed, hasAgreed := dt.egress.votes.agreed(serviceUID)
		baseline := opState.Egress.StartupBaseline && hasAgreed && agreed == rejection.Config
		if (!known || turnedFinal) && !baseline {
			events = append(events, dt.egressRejectedEventsLocked(serviceUID, *rejection)...)
		}
	}
	if outcome.Applied != nil || outcome.Rejected != nil {
		opState.Egress.StartupBaseline = false
	}
	return events
}

// completeOutboundUpdateLocked ends an update run of an egress identity (an apply, an observe-and-clean or a
// BYO residue cleanup) once its outcome is recorded. The in-flight snapshot is cleared and the identity, whose
// NAT Gateway exists, is never parked in StateNotStarted: evaluateEgressConsensusLocked decides what follows
// (a retry, a cancellation or an observe-and-clean), with the retry backoff, budget and park re-arm, except for
// a configuration agreed while a failed run was in flight, which is dispatched at once. Must be called with
// dt.mu held.
func (dt *DiffTracker) completeOutboundUpdateLocked(serviceUID string, opState *ServiceOperationState, success bool, err error,
	startTime time.Time) {
	finished := opState.InFlightConfig
	opState.InFlightConfig = nil
	opState.LastAttempt = time.Now().Format(time.RFC3339)

	if success {
		dt.logger.V(2).Info("Updated service", "service", serviceUID)
		retries := opState.RetryCount
		recordServiceOperation("update", false, startTime, nil, "", opState.IsOrphan)
		recordServiceOperationRetries("update", false, retries)
		opState.RetryCount = 0
		opState.State = StateCreated
		dt.evaluateEgressConsensusLocked(serviceUID)
		backOffUnconvergedEgressRunLocked(opState, finished, retries)
	} else {
		dt.logger.V(4).Info("Could not update service", "err", err, "service", serviceUID)
		errCode := "ValidationError"
		if !isTerminalError(err) {
			_, errCode = extractAzureErrorInfo(err)
		}
		recordServiceOperation("update", false, startTime, err, errCode, opState.IsOrphan)
		opState.RetryCount++
		opState.NextRetryAt = time.Now().Add(computeRetryBackoff(opState.RetryCount))
		retries := opState.RetryCount
		dt.evaluateEgressConsensusLocked(serviceUID)
		desired := outboundEgress(opState.Config.OutboundConfig)
		switch {
		case opState.State == StateCreated:
			// The evaluation cancelled the update, so it ends here.
			recordServiceOperationRetries("update", false, retries)
		case finished != nil && desired != egressUnknown && desired != outboundEgress(finished.OutboundConfig):
			// Agreed while the failed run was in flight: new intent, applied at once rather than after the backoff
			// of the configuration that failed.
			resetRetryStateLocked(opState)
		}
	}

	if opState.State == StateUpdateInProgress {
		dt.logger.V(4).Info("Scheduled egress identity update", "service", serviceUID, "attempt", opState.RetryCount,
			"nextRetryAt", opState.NextRetryAt)
		dt.triggerServiceUpdater()
	}
	dt.checkInitializationCompleteLocked()
}

// backOffUnconvergedEgressRunLocked makes the update that the evaluation queued after a successful outbound run
// wait out a retry backoff when it repeats that run: another observation (one is still owed) or another apply
// of the configuration the finished run was dispatched with (it was not applied). Dispatched at once, a run
// that cannot make progress would repeat without bound; the backoff counts as a retry on top of retries, so
// the retry budget and the park re-arm bound it. An update for a configuration agreed while the finished run
// was in flight is new intent and is dispatched at once. Must be called with dt.mu held.
func backOffUnconvergedEgressRunLocked(opState *ServiceOperationState, finished *ServiceConfig, retries int) {
	if opState.State != StateUpdateInProgress {
		return
	}
	desired := outboundEgress(opState.Config.OutboundConfig)
	if desired != egressUnknown && (finished == nil || desired != outboundEgress(finished.OutboundConfig)) {
		return
	}
	opState.RetryCount = retries + 1
	opState.NextRetryAt = time.Now().Add(computeRetryBackoff(opState.RetryCount))
}

// forgetEgressIdentityLocked drops what is known about an egress identity whose deletion succeeded, so a
// re-creation starts afresh: its mode, rejections, warnings, startup baseline, what it remembered while its applied
// configuration was unknown, and owed observation, unless keepResidueCleanup hands an owed residue cleanup over to
// the re-created identity's first run. Its votes are kept, as its pods may still exist, and so is its quiet period.
// Must be called with dt.mu held.
func (dt *DiffTracker) forgetEgressIdentityLocked(serviceUID string, opState *ServiceOperationState, keepResidueCleanup bool) {
	delete(dt.egress.mode, serviceUID)
	delete(dt.egress.rejected, serviceUID)
	delete(dt.egress.warned, serviceUID)
	opState.Egress.StartupBaseline = false
	opState.Egress.LastKnownApplied, opState.Egress.UnclearWrites = nil, nil
	if keepResidueCleanup {
		return
	}
	opState.Egress.NeedsObservation = false
	opState.Egress.ObservationGen = 0
	opState.Egress.DispatchedObservationGen = 0
}

// updateRunKindLocked returns the kind of the update run the dispatcher starts for opState: the residue
// cleanup of a BYO egress identity, else an update (for a managed egress identity an apply or an
// observe-and-clean). Must be called with dt.mu held.
func (dt *DiffTracker) updateRunKindLocked(serviceUID string, opState *ServiceOperationState) operationKind {
	if !opState.Config.IsInbound && dt.egress.mode[serviceUID] == egressModeBYO {
		return operationEgressResidueCleanup
	}
	return operationUpdate
}

// completedRunKind returns the kind of the run whose completion is being processed and clears it: the kind
// recorded when the run was dispatched, else, for a completion that follows no recorded dispatch, the kind
// the operation's state implies, the run of a preempted operation being an update once the service was
// applied. Must be called with dt.mu held.
func completedRunKind(opState *ServiceOperationState, hadApplied bool) operationKind {
	kind := opState.InFlightKind
	opState.InFlightKind = operationNone
	if kind != operationNone {
		return kind
	}
	switch {
	case opState.State == StateDeletionInProgress && opState.InFlightConfig == nil:
		return operationDelete
	case opState.State == StateUpdateInProgress,
		(opState.State == StateDeletionPending || opState.State == StateDeletionInProgress) && hadApplied:
		return operationUpdate
	default:
		return operationCreate
	}
}

// revivalState returns the state a deletion-pending operation returns to when a pod revives it: Created
// when no run is in flight, the update state while an update run is, so that run completes as the update
// it is. An operation whose creation or deletion is in flight is not revived, nor, with no run in flight,
// one whose identity is not registered (its creation never succeeded) or has retries (a failed creation or
// deletion may have left its NAT Gateway missing): the deletion after the last pod resets the retries of an
// update (markOutboundServiceForDeletionLocked).
func revivalState(opState *ServiceOperationState, registered bool) (ResourceState, bool) {
	switch opState.InFlightKind {
	case operationNone:
		return StateCreated, registered && opState.RetryCount == 0
	case operationUpdate, operationEgressResidueCleanup:
		return StateUpdateInProgress, true
	default:
		return opState.State, false
	}
}
