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

// The managed egress runs of the ServiceUpdater (design 4.4): the managed branch of an identity's creation, its updates
// (an apply, or an observe-and-clean), the residue cleanup of a BYO identity, and the parts of a deletion that delete
// the managed NAT Gateway or clean up a BYO identity's residue. Every Azure call goes through the egress call layer
// (egress_apply.go, egress_cleanup.go) with a budget of its own, taken from the updater's context, and every run
// completes the identity's operation once, with the outcome of what it found and did (completeOutbound), which the
// engine records (4.5).

import (
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork/v12"
	"k8s.io/utils/ptr"
)

// egressRunResult is how an egress run, or a step of one, ended: what it found in Azure and what it did. Each step sets
// the fields that describe it: an apply (applyEgressConfig, applyEgressCreation) Applied, NATWritten, Rejected, NAT and
// Services, and ends in exactly one way: Applied, Rejected, CommitUnknown, Busy, or a plain error (Err alone); a link
// repair (repairEgressLinks) NAT; an observe-and-clean (observeEgress) Applied, NATGone and Swept; a deletion
// (deleteManagedEgress) and a BYO residue cleanup (cleanupBYOResidue) NATGone, NotApplicable and Swept; every step
// CommitUnknown, Busy and Err. The run reports it with its completion (toOutcome).
type egressRunResult struct {
	// Applied is the configuration the NAT Gateway satisfies. An apply sets it after a successful NAT Gateway PUT, and
	// when the settled NAT Gateway already satisfied it, so that nothing was written. An observation sets it to the
	// first configuration it looks for that the NAT Gateway satisfies (egressObservationTargets, then the fallback),
	// else egressUnknown, which is also reported after a write whose outcome is unknown; it leaves it nil when it could
	// not tell: the NAT Gateway is absent or busy, or a read it depends on failed.
	Applied *EgressConfig
	// NATWritten is set by an apply whose NAT Gateway PUT succeeded: the NAT Gateway satisfies Applied because the
	// apply wrote it, not because it already did.
	NATWritten bool
	// Rejected is the configuration refused by the preflight of an apply, before any write, or by Azure on a PUT. After
	// a creation fallback (applyEgressCreation), Rejected reports the creation configuration and the other fields how
	// the fallback ended.
	Rejected *egressRejection
	// NAT is the NAT Gateway the rest of the run (link repair, sweep) works from: as the step's NAT Gateway PUT returned
	// it, else as read or given; nil when it does not exist. An apply leaves it nil when its reads ended it (a failed or
	// unsettled read, a NAT Gateway the controller does not own or that another service uses) and after CommitUnknown.
	NAT *armnetwork.NatGateway
	// Services are the services registered with the Service Gateway, as an apply that ended Applied read them before
	// any write; its link repair works from them.
	Services []*armnetwork.ServiceGatewayService
	// NATGone is set once the identity's NAT Gateway does not exist: absent when read, or deleted by the run. An
	// observation that finds it absent does nothing else and reports Swept: a NAT Gateway deleted out of band is not
	// re-created or cleaned up after (out of scope), so nothing is owed.
	NATGone bool
	// NotApplicable is set when the identity's NAT Gateway exists but is not the controller's to delete: not owned, or
	// (BYO residue) used by a Service Gateway service. It was left alone with its resources and Swept is set: nothing
	// is left that the run may delete. Nothing was written after the decision.
	NotApplicable bool
	// Swept is set when every reserved name was read settled and every DELETE it needed ended in success, 404, or a
	// refusal (400) of a resource its read shows in use elsewhere (sweepEgressReserved). A deletion or a BYO residue
	// cleanup sets it only once its NAT Gateway is gone or not applicable.
	Swept bool
	// CommitUnknown is set when a write ended without a conclusive answer, so Azure may still apply it. The step stopped
	// there: an apply sends no NAT Gateway PUT after it and no fallback.
	CommitUnknown bool
	// Busy is set when the step stopped at an unsettled read, possibly after writes; a busy apply or observation wrote
	// nothing.
	Busy bool
	// Err is why the step did not do everything: a failed read, a refused write, or a write whose outcome is unknown
	// (with CommitUnknown). With Busy it is the unsettled read of an apply; on its own, after an apply, a plain
	// retryable error (a failed read, which is never commit unknown, or a write Azure refused with a conflict).
	Err error
}

// toOutcome returns what the run reports with its completion, for an identity whose NAT Gateway is provided as mode
// (egressModeUnknown when the run resolved nothing).
func (r egressRunResult) toOutcome(mode egressMode) *egressOutcome {
	return &egressOutcome{Mode: mode, Applied: r.Applied, NATWritten: r.NATWritten, Rejected: r.Rejected, CommitUnknown: r.CommitUnknown,
		Swept: r.Swept}
}

// Complete reports whether a deletion or a BYO residue cleanup left nothing to do: the NAT Gateway is gone, or not the
// controller's, and the reserved names are swept.
func (r egressRunResult) Complete() bool {
	return r.Err == nil && !r.Busy && !r.CommitUnknown && r.Swept && (r.NATGone || r.NotApplicable)
}

// createManagedOutboundService is the managed branch of creating egress identity id (design 4.4): it applies the
// creation configuration the operation was dispatched with, which falls back to the default of the identity's families
// in the same run when it is rejected; once the NAT Gateway satisfies one of them, it repairs both links, which
// registers the identity with its NAT Gateway, and sweeps the reserved names. The identity is recorded in NRP once it
// is registered with a NAT Gateway that satisfies its configuration.
func (s *ServiceUpdater) createManagedOutboundService(id string, config *OutboundConfig, correlationID, pod string) {
	dt := s.diffTracker
	ipv6 := outboundWantsIPv6(config)
	fallback := defaultEgressConfig(dt.egressParseContextFor(outboundFamilies(config)))
	creation := outboundEgress(config)

	outcome, err := s.finishEgressApply(id, ipv6, dt.applyEgressCreation(s.ctx, id, creation, fallback, ipv6), false)
	if err != nil {
		s.logger.V(4).Info("Could not create outbound service", "serviceUID", id, "correlationID", correlationID, "pod", pod, "err", err)
		s.completeOutbound(id, false, err, outcome)
		return
	}
	dt.UpdateNRPNATGateways(SyncServicesReturnType{Additions: newIgnoreCaseSetFromSlice([]string{id})})
	s.completeOutbound(id, true, nil, outcome)
	s.logger.V(2).Info("Created outbound service", "serviceUID", id, "correlationID", correlationID, "pod", pod,
		"configuration", outcome.Applied.Summary(), "swept", outcome.Swept)
}

// updateOutboundService is an update run of managed egress identity id (design 4.4). The configuration the operation
// was dispatched with is applied and, once the NAT Gateway satisfies it, both links are repaired and the reserved names
// swept. The unknown configuration (egressUnknown) makes the run an observe-and-clean instead, which changes no
// configuration.
func (s *ServiceUpdater) updateOutboundService(id string, config *OutboundConfig, correlationID string) {
	desired := outboundEgress(config)
	if desired == egressUnknown {
		s.observeOutboundService(id, config, correlationID)
		return
	}
	ipv6 := outboundWantsIPv6(config)
	outcome, err := s.finishEgressApply(id, ipv6, s.diffTracker.applyEgressConfig(s.ctx, id, desired, ipv6), true)
	if err != nil {
		s.logger.V(4).Info("Could not update outbound service", "serviceUID", id, "correlationID", correlationID,
			"configuration", desired.Summary(), "err", err)
		s.completeOutbound(id, false, err, outcome)
		return
	}
	s.completeOutbound(id, true, nil, outcome)
	s.logger.V(2).Info("Updated outbound service", "serviceUID", id, "correlationID", correlationID,
		"configuration", desired.Summary(), "swept", outcome.Swept)
}

// observeOutboundService is an observe-and-clean run of managed egress identity id (design 4.4): it repairs both
// links, finds the configuration the NAT Gateway satisfies (the one its members agree on, else that of a write Azure
// did not answer clearly, else the one last known applied, else the default of its families, else egressUnknown) and
// sweeps the reserved names against it. A run that found an Azure resource unsettled records nothing and is retried;
// one that found no NAT Gateway completes with nothing owed (observeEgress).
func (s *ServiceUpdater) observeOutboundService(id string, config *OutboundConfig, correlationID string) {
	dt := s.diffTracker
	fallback := defaultEgressConfig(dt.egressParseContextFor(outboundFamilies(config)))
	result := dt.observeEgress(s.ctx, id, outboundWantsIPv6(config), append(dt.egressObservationTargets(id), &fallback)...)

	outcome := result.toOutcome(egressModeManaged)
	err := result.Err
	if result.Busy {
		err = fmt.Errorf("observing egress identity %q: its NAT Gateway or a resource it references is not settled", id)
	}
	if err != nil {
		s.logger.V(4).Info("Could not observe egress identity", "serviceUID", id, "correlationID", correlationID, "err", err)
		s.completeOutbound(id, false, err, outcome)
		return
	}
	s.completeOutbound(id, true, nil, outcome)
	s.logger.V(2).Info("Observed egress identity", "serviceUID", id, "correlationID", correlationID,
		"configuration", ptr.Deref(outcome.Applied, egressUnknown).Summary(), "swept", outcome.Swept)
}

// finishEgressApply ends an apply run of managed egress identity id, whose NAT Gateway serves IPv6 when ipv6 is set,
// from how the apply ended (design 4.4, step 4), and returns the run's outcome and error. Once the NAT Gateway satisfies
// the configuration (Applied), both links are repaired from the services the apply read, and the reserved names are
// swept against the NAT Gateway; the run succeeds when the links are in place. The sweep never fails it: what the sweep
// leaves undone is reported (Swept, CommitUnknown) and owed to the next run, so leftovers never hold back a working NAT
// Gateway. An apply that did not end with Applied fails the run: commit unknown, rejected, or a plain error, which
// includes a busy apply (it reports nothing applied and nothing unknown, so nothing is recorded). A rejection fails the
// run with a terminal error when terminalRejections is set and the rejection is terminal; that never parks an update.
func (s *ServiceUpdater) finishEgressApply(id string, ipv6 bool, result egressRunResult, terminalRejections bool) (*egressOutcome, error) {
	dt := s.diffTracker
	switch {
	case result.CommitUnknown:
		return result.toOutcome(egressModeManaged),
			fmt.Errorf("applying the egress configuration of identity %q: Azure may still apply a write: %w", id, result.Err)
	case result.Applied == nil && result.Rejected != nil && result.Err == nil:
		rejection := result.Rejected
		err := fmt.Errorf("egress configuration %s of identity %q was rejected: %s", rejection.Config.Summary(), id, rejection.Reason)
		if terminalRejections && !rejection.Retryable {
			err = newTerminalError(err)
		}
		return result.toOutcome(egressModeManaged), err
	case result.Applied == nil:
		return result.toOutcome(egressModeManaged), fmt.Errorf("applying the egress configuration of identity %q: %w", id, result.Err)
	}

	link := dt.repairEgressLinks(s.ctx, id, result.NAT, result.Services)
	if link.Err != nil {
		result.CommitUnknown = link.CommitUnknown
		return result.toOutcome(egressModeManaged), link.Err
	}
	swept, unknown, err := dt.sweepEgressReserved(s.ctx, id, ipv6, link.NAT)
	result.Swept, result.CommitUnknown = swept, unknown
	if err != nil {
		s.logger.V(4).Info("Could not sweep every reserved egress resource; the next run sweeps again", "serviceUID", id, "err", err)
	}
	return result.toOutcome(egressModeManaged), nil
}

// cleanupOutboundResidue is the residue cleanup run of BYO egress identity id (design 4.4): it removes what the
// identity's managed past left in the cluster resource group, never touching its BYO NAT Gateway or its registration.
// The run succeeds once nothing is left (egressRunResult.Complete), which settles the observation it was owed.
func (s *ServiceUpdater) cleanupOutboundResidue(id, correlationID string) {
	result := s.diffTracker.cleanupBYOResidue(s.ctx, id)
	outcome := result.toOutcome(egressModeBYO)
	if err := egressCleanupError(fmt.Sprintf("cleaning up the residue of BYO egress identity %q", id), result); err != nil {
		s.logger.V(4).Info("Could not clean up the residue of a BYO egress identity", "serviceUID", id, "correlationID", correlationID, "err", err)
		s.completeOutbound(id, false, err, outcome)
		return
	}
	s.completeOutbound(id, true, nil, outcome)
	s.logger.V(2).Info("Cleaned up the residue of a BYO egress identity", "serviceUID", id, "correlationID", correlationID)
}

// deleteManagedOutboundEgress is Steps 3-4 of deleting managed egress identity id, after its unlink and unregister
// steps: it deletes the NAT Gateway on the etag of a read taken right before the DELETE, then sweeps every reserved name
// of both IP families (deleteManagedEgress). It returns the run's outcome, whose Swept settles any observation the
// identity was owed, and why the deletion is not complete; nil once it is.
func (s *ServiceUpdater) deleteManagedOutboundEgress(id, correlationID string) (*egressOutcome, error) {
	result := s.diffTracker.deleteManagedEgress(s.ctx, id)
	err := egressCleanupError(fmt.Sprintf("deleting the NAT Gateway of egress identity %q", id), result)
	if err != nil {
		httpStatus, errCode := extractAzureErrorInfo(err)
		s.logger.V(4).Info("Could not delete NAT Gateway for outbound service", "serviceUID", id, "correlationID", correlationID,
			"httpStatus", httpStatus, "errorCode", errCode, "err", err)
	}
	return result.toOutcome(egressModeUnknown), err
}

// byoDeletionResidue is the residue cleanup a deletion of BYO egress identity id runs, once the identity's BYO NAT
// Gateway is unlinked and the identity unregistered, when the identity is owed one (design 4.4). It returns the outcome
// of the deletion run, whose Swept settles the residue owed. A cleanup that is not complete does not fail the deletion:
// the residue stays owed, and the engine makes one more deletion pass for it.
func (s *ServiceUpdater) byoDeletionResidue(id, correlationID string) *egressOutcome {
	if !s.diffTracker.egressObservationOwed(id) {
		return &egressOutcome{}
	}
	result := s.diffTracker.cleanupBYOResidue(s.ctx, id)
	if err := egressCleanupError(fmt.Sprintf("cleaning up the residue of BYO egress identity %q", id), result); err != nil {
		s.logger.V(4).Info("Could not clean up the residue of a deleted BYO egress identity; it stays owed", "serviceUID", id,
			"correlationID", correlationID, "err", err)
	}
	return result.toOutcome(egressModeUnknown)
}

// egressCleanupError returns why a deletion or a residue cleanup, described by what, is not complete; nil once it is.
func egressCleanupError(what string, result egressRunResult) error {
	switch {
	case result.Complete():
		return nil
	case result.Err != nil:
		return fmt.Errorf("%s: %w", what, result.Err)
	case result.Busy:
		return fmt.Errorf("%s: its NAT Gateway is not settled", what)
	default:
		return fmt.Errorf("%s: a reserved Public IP or prefix is not settled yet", what)
	}
}

// egressObservationTargets returns the configurations an observation of egress identity looks for before the
// default, in that order: the one every member agrees on (nil without consensus); those of the applies whose writes
// Azure did not answer clearly, newest first, kept while the applied configuration is unknown; and the one last known
// applied (nil when none is known), so that a NAT Gateway that still has it is not taken for the default, which it
// also satisfies when its only difference is tags (natSatisfies matches tags by presence). The configurations are
// copies. Takes dt.mu.
func (dt *DiffTracker) egressObservationTargets(identity string) []*EgressConfig {
	dt.mu.Lock()
	defer dt.mu.Unlock()
	var agreed *EgressConfig
	if cfg, ok := dt.egress.votes.agreed(identity); ok {
		agreed = &cfg
	}
	targets := []*EgressConfig{agreed}
	opState, exists := dt.pendingServiceOps[identity]
	if !exists {
		return append(targets, nil)
	}
	for _, written := range opState.Egress.UnclearWrites {
		targets = append(targets, &written)
	}
	var known *EgressConfig
	switch applied := appliedEgress(opState); {
	case opState.LastAppliedConfig != nil && applied != egressUnknown:
		known = &applied
	case opState.Egress.LastKnownApplied != nil:
		last := *opState.Egress.LastKnownApplied
		known = &last
	}
	return append(targets, known)
}

// egressObservationOwed reports whether the operation of egress identity is owed an observation
// (Egress.NeedsObservation). Takes dt.mu.
func (dt *DiffTracker) egressObservationOwed(identity string) bool {
	dt.mu.Lock()
	defer dt.mu.Unlock()
	opState, exists := dt.pendingServiceOps[identity]
	return exists && opState.Egress.NeedsObservation
}
