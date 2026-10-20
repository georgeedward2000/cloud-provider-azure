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
	"crypto/rand"
	"errors"
	"fmt"
	"maps"
	"math/big"
	"net/netip"
	"slices"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	"sigs.k8s.io/cloud-provider-azure/pkg/consts"
)

// ResyncPeriod returns a function that generates a randomized resync duration
// to prevent controllers from syncing in lock-step and overloading the API server.
func ResyncPeriod(base time.Duration) func() time.Duration {
	return func() time.Duration {
		n, _ := rand.Int(rand.Reader, big.NewInt(1000))
		factor := float64(n.Int64())/1000.0 + 1.0
		return time.Duration(float64(base.Nanoseconds()) * factor)
	}
}

// setUpPodInformerForEgress creates an informer for Pods with egress labels.
// It uses label selectors to filter pods efficiently at the API server level,
// reducing memory and CPU overhead by only watching relevant pods.
// podFromDeleteObj decodes the object an informer DeleteFunc receives into a Pod.
//
// A delete event carries either the Pod itself or, when the watch missed the deletion, a
// DeletedFinalStateUnknown tombstone wrapping it. Dropping the tombstone case would silently
// ignore the deletion of an egress pod and strand its cleanup finalizer forever, so the decode is
// kept as a named function that can be exercised directly.
func podFromDeleteObj(obj interface{}) (*v1.Pod, bool) {
	switch v := obj.(type) {
	case *v1.Pod:
		return v, true
	case cache.DeletedFinalStateUnknown:
		pod, ok := v.Obj.(*v1.Pod)
		if !ok {
			klog.Errorf("Cannot convert to *v1.Pod: %T", v.Obj)
			return nil, false
		}
		klog.V(2).Infof("DeleteFunc: processing DeletedFinalStateUnknown for pod %s/%s",
			pod.Namespace, pod.Name)
		return pod, true
	default:
		klog.Errorf("Cannot convert to *v1.Pod: %T", v)
		return nil, false
	}
}

// egressPodListOptions shapes the list and watch requests of the pod informer (SetUpPodInformer). They select the pods
// with the egress label key, whatever its value, as startup's listing does (processK8sEgresses), and the informer's
// first list is made consistent. The reflector makes that list with resourceVersion "0", which the API server may serve
// from a stale watch cache, and watches from its resource version, replaying every change since. Startup admitted
// members from its own consistent listing, and the informer takes the first ADD of each as what became of it since, and
// its absence from the synced store as its removal (reconcileEgressPodAdd, removeUnlistedEgressPods): a store older
// than startup's listing would miss live members, and replay older states that undo the newer ones startup admitted.
// Listing with resourceVersion "" (a consistent read) keeps the store never older than startup's listing. Any other
// resource version is kept: only the first list uses "0", later lists and watches go on from a resource version the
// informer saw, and a watch-list request starts from a consistent snapshot already.
func egressPodListOptions(options *metav1.ListOptions) {
	options.LabelSelector = consts.PodLabelServiceEgressGateway
	if options.ResourceVersion == "0" {
		options.ResourceVersion = ""
	}
}

func (dt *DiffTracker) SetUpPodInformer(stopCh <-chan struct{}) error {
	klog.V(2).Infof("setUpPodInformerForEgress: Setting up pod informer with label selector: %s", consts.PodLabelServiceEgressGateway)

	// Create a separate informer factory with label selector to filter pods at the API server, and a consistent first
	// list (egressPodListOptions)
	//
	// Resync is a safety net, not a correctness dependency: the watch should deliver every pod
	// event, and a resync only re-delivers current state as synthetic updates. It is kept short
	// deliberately so that recovery path is exercised routinely rather than a handful of times a
	// day, which is when a latent bug in it would otherwise surface. ResyncPeriod randomizes
	// x1.0-2.0 to avoid lock-step across controllers, so this is an effective 1-2h.
	podInformerFactory := informers.NewSharedInformerFactoryWithOptions(
		dt.kubeClient,
		ResyncPeriod(time.Hour)(),
		informers.WithTweakListOptions(egressPodListOptions),
	)

	podInformer := podInformerFactory.Core().V1().Pods().Informer()
	_, err := podInformer.AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				pod := obj.(*v1.Pod)
				dt.reconcileEgressPodAdd(pod)
			},
			UpdateFunc: func(oldObj, newObj interface{}) {
				oldPod := oldObj.(*v1.Pod)
				newPod := newObj.(*v1.Pod)
				dt.reconcileEgressPodUpdate(oldPod, newPod)
			},
			DeleteFunc: func(obj interface{}) {
				// Deletion is normally handled via UpdateFunc (2-phase deletion sets DeletionTimestamp
				// before the finalizer is removed). This is a defensive backup for
				// DeletedFinalStateUnknown and pods created during CCM downtime.
				pod, ok := podFromDeleteObj(obj)
				if !ok {
					return
				}
				// Idempotent - safe even if already processed in UpdateFunc.
				dt.podInformerRemovePod(pod)
			},
		})
	if err != nil {
		return fmt.Errorf("setUpPodInformerForEgress: add event handlers: %w", err)
	}

	// Start the informer factory
	klog.V(2).Infof("setUpPodInformerForEgress: Starting pod informer factory")
	podInformerFactory.Start(stopCh)
	if !cache.WaitForCacheSync(stopCh, podInformer.HasSynced) {
		return fmt.Errorf("setUpPodInformerForEgress: cache sync stopped before completion")
	}
	// Once the store holds the listing, the pods startup admitted that it does not hold are removed; the others are
	// reconciled by their own ADD, which may still be on its way through the handler (reconcileEgressPodAdd). Listed
	// consistently (egressPodListOptions), the store is never older than startup's listing, so a pod it does not hold
	// was deleted, or left the informer's selection, since startup listed it. A pod the store listed and dropped again
	// just before this check may still get its queued ADD, which adds it back until its queued DELETE removes it again:
	// the end state is the same.
	store := podInformer.GetStore()
	dt.removeUnlistedEgressPods(func(podKey string) bool {
		_, listed, err := store.GetByKey(podKey)
		return listed || err != nil
	})
	klog.V(2).Infof("setUpPodInformerForEgress: Pod informer successfully initialized and synced")
	return nil
}

// reconcileEgressPodAdd applies an egress pod ADD. The informer starts after startup, which admitted the members of
// its own listing (seedEgressVotes), and its listing is never older than startup's (egressPodListOptions), so the
// first ADD of a pod with the key of an admitted pod also reports what became of that pod meanwhile (4.6). A same-name
// pod with another UID replaced it: the admitted pod, which no delete will report, is removed as its delete would
// remove it, before the new pod is added. A change that removes the pod from its identity, or re-registers it (another
// identity, other addresses), is applied as the update from the pod as admitted (reconcileEgressPodUpdate), so the pod
// leaves the identity it was admitted to with the addresses it was admitted with. An unchanged member, or one whose
// egress annotations alone changed, is added as any pod: it gets its cleanup finalizer, and its vote, unless changed,
// raises no event.
func (dt *DiffTracker) reconcileEgressPodAdd(pod *v1.Pod) {
	if admitted, ok := dt.takeAdmittedEgressPod(pod.Namespace + "/" + pod.Name); ok {
		if admitted.UID != pod.UID {
			dt.podInformerRemovePod(admitted)
		} else if needsRemove, _, _, _ := egressPodUpdateActions(admitted, pod); needsRemove {
			dt.reconcileEgressPodUpdate(admitted, pod)
			return
		}
	}
	dt.podInformerAddPod(pod)
}

// reconcileEgressPodUpdate applies an egress pod UPDATE; each change is one engine call (4.1). A live
// re-registration (needsRemove AND needsAdd) goes through podInformerReregister, which drains without a
// finalizer record, so a concurrent stripper can't touch the still-live pod; a genuine removal
// (needsRemove only) uses podInformerRemovePod, which holds the finalizer until every address drains; a
// change of the egress annotations alone replaces the pod's vote (needsVote).
func (dt *DiffTracker) reconcileEgressPodUpdate(oldPod, newPod *v1.Pod) {
	needsRemove, needsAdd, needsVote, reason := egressPodUpdateActions(oldPod, newPod)
	if !needsRemove && !needsAdd && !needsVote {
		klog.V(4).Infof("setUpPodInformerForEgress: Pod %s/%s update has no relevant changes, skipping",
			newPod.Namespace, newPod.Name)
		return
	}

	klog.V(2).Infof("setUpPodInformerForEgress: Pod %s/%s update: %s", newPod.Namespace, newPod.Name, reason)
	switch {
	case needsRemove && needsAdd:
		dt.podInformerReregister(oldPod, newPod)
	case needsRemove:
		dt.podInformerRemovePod(oldPod)
	case needsAdd:
		dt.podInformerAddPod(newPod)
	default:
		dt.podInformerSetVote(newPod)
	}
}

// egressPodUpdateActions decides whether an updated egress pod must be removed from its old gateway
// and/or (re-)added to its current one, or, when neither, whether its vote must be replaced because
// its egress annotations alone changed. Change-detection uses the full PodIPs set and the per-family
// node locations, so a dual-stack address or secondary-family location change is reconciled.
// DeletionTimestamp being set, or the Phase leaving Running/Pending, makes the pod invalid (removal).
func egressPodUpdateActions(oldPod, newPod *v1.Pod) (needsRemove, needsAdd, needsVote bool, reason string) {
	prevEgressGatewayName := egressIdentityOf(oldPod)
	currEgressGatewayName := egressIdentityOf(newPod)

	oldIPs := PodEgressAddresses(oldPod)
	newIPs := PodEgressAddresses(newPod)
	labelChanged := prevEgressGatewayName != currEgressGatewayName
	oldHadIPs := oldPod.Status.HostIP != "" && len(oldIPs) > 0
	newHasIPs := newPod.Status.HostIP != "" && len(newIPs) > 0
	// Re-register when the addresses OR the per-family node locations change: a secondary-family host
	// IP change (Status.HostIPs) while the primary HostIP is unchanged still moves that family's
	// address to a new location and must be re-synced, so compare the family->location map, not just
	// the primary HostIP.
	ipsChanged := !slices.Equal(oldIPs, newIPs) ||
		!maps.Equal(PodNodeLocationsByFamily(oldPod), PodNodeLocationsByFamily(newPod))

	oldWasValid := oldPod.DeletionTimestamp == nil &&
		(oldPod.Status.Phase == v1.PodRunning || oldPod.Status.Phase == v1.PodPending)
	newIsValid := newPod.DeletionTimestamp == nil &&
		(newPod.Status.Phase == v1.PodRunning || newPod.Status.Phase == v1.PodPending)

	switch {
	case labelChanged:
		// Remove from the old gateway (if it had IPs), add to the new one (if valid and has IPs).
		needsRemove = prevEgressGatewayName != "" && oldHadIPs
		needsAdd = currEgressGatewayName != "" && newHasIPs && newIsValid
		reason = fmt.Sprintf("label changed from %s to %s", prevEgressGatewayName, currEgressGatewayName)
	case currEgressGatewayName == "":
		// Not an egress pod and the label did not change; nothing to do.
	case oldWasValid && !newIsValid && oldHadIPs:
		needsRemove = true
		reason = fmt.Sprintf("pod became invalid (Phase: %s, DeletionTimestamp: %v)",
			newPod.Status.Phase, newPod.DeletionTimestamp != nil)
	case !oldHadIPs && newHasIPs && newIsValid:
		needsAdd = true
		reason = "completed IP initialization"
	case oldHadIPs && !newHasIPs:
		needsRemove = true
		reason = "lost IPs"
	case !oldWasValid && newIsValid && oldHadIPs && newHasIPs && !ipsChanged:
		// Pod regained validity (e.g. recovered from a transient node-NotReady Unknown phase) with the
		// same addresses; it was removed when it went invalid, so re-add the current set.
		needsAdd = true
		reason = "pod regained validity"
	case oldHadIPs && newHasIPs && ipsChanged && newIsValid:
		// Pod moved, or gained/lost/swapped an IP family - re-register the full address set.
		needsRemove = true
		needsAdd = true
		reason = fmt.Sprintf("IPs changed (HostIP: %s→%s, PodIPs: %v→%v)",
			oldPod.Status.HostIP, newPod.Status.HostIP, oldIPs, newIPs)
	case oldHadIPs && newHasIPs && !newIsValid:
		needsRemove = true
		reason = fmt.Sprintf("pod became invalid while having IPs (Phase: %s, DeletionTimestamp: %v)",
			newPod.Status.Phase, newPod.DeletionTimestamp != nil)
	}
	// Only a member votes, and a member is valid and has addresses; the engine ignores the vote of a pod
	// that is not one (setEgressVote). Any other change re-adds or removes the pod, which handles its vote.
	if !needsRemove && !needsAdd && currEgressGatewayName != "" && newIsValid && newHasIPs &&
		!maps.Equal(egressAnnotations(oldPod), egressAnnotations(newPod)) {
		needsVote = true
		reason = "egress annotations changed"
	}
	return needsRemove, needsAdd, needsVote, reason
}

// egressAnnotations returns the annotations of pod that configure its egress identity's NAT Gateway: those
// with the egress prefix.
func egressAnnotations(pod *v1.Pod) map[string]string {
	var annotations map[string]string
	for key, value := range pod.Annotations {
		if strings.HasPrefix(key, consts.PodAnnotationServiceEgressGatewayPrefix) {
			if annotations == nil {
				annotations = make(map[string]string)
			}
			annotations[key] = value
		}
	}
	return annotations
}

// egressIdentityOf returns the egress identity named by the egress label of pod: its value, lower-cased;
// empty when the pod has none.
func egressIdentityOf(pod *v1.Pod) string {
	return strings.ToLower(pod.Labels[consts.PodLabelServiceEgressGateway])
}

// egressPodRefOf returns what identifies pod in the egress membership calls of the engine.
func egressPodRefOf(pod *v1.Pod) egressPodRef {
	return egressPodRef{Namespace: pod.Namespace, Name: pod.Name, UID: pod.UID, Created: pod.CreationTimestamp.Time}
}

// egressMembershipProblem keeps a pod, or one of its addresses, out of its egress identity. The pod informer
// reports one with an event reason to the pod as a Warning event.
type egressMembershipProblem struct {
	eventReason, message string
}

// egressMembership decides the membership of pod in its egress identity (3.2). identity is the value of the
// pod's egress label, lower-cased. accepted are the pod's addresses whose IP family has a node location, each
// with that location, so a dual-stack pod on a node that exposes only one of its families keeps that family.
// The pod is a member iff accepted is not empty, which needs a valid identity, a pod that is not being deleted,
// in phase Running or Pending, with a well-formed HostIP. reason tells what kept the pod, or some of its
// addresses, out; it is empty when every address is accepted.
//
// It is the one definition of membership: the pod informer adds pods with it (podInformerAddPod), and startup
// admits pods (processK8sEgresses) and seeds their votes (seedEgressVotes) with it.
func egressMembership(pod *v1.Pod) (identity string, accepted []egressAddress, reason string) {
	identity, accepted, problems := checkEgressMembership(pod)
	return identity, accepted, egressMembershipReason(problems)
}

// egressMembershipReason describes problems in one line.
func egressMembershipReason(problems []egressMembershipProblem) string {
	messages := make([]string, 0, len(problems))
	for _, problem := range problems {
		messages = append(messages, problem.message)
	}
	return strings.Join(messages, "; ")
}

// checkEgressMembership is egressMembership with the problems found, in the order they were found.
func checkEgressMembership(pod *v1.Pod) (identity string, accepted []egressAddress, problems []egressMembershipProblem) {
	identity = egressIdentityOf(pod)
	notMember := func(eventReason, message string) []egressMembershipProblem {
		return append(problems, egressMembershipProblem{eventReason: eventReason, message: message})
	}
	switch {
	case identity == "":
		return identity, nil, notMember("", "the pod has no egress label value")
	case pod.DeletionTimestamp != nil:
		return identity, nil, notMember("", "the pod is being deleted")
	case pod.Status.Phase != v1.PodRunning && pod.Status.Phase != v1.PodPending:
		// Succeeded/Failed containers have terminated and Unknown is unobtainable, so none is a live
		// egress backend; registering one would program a stale NRP address no later event clears.
		return identity, nil, notMember("", fmt.Sprintf("the pod is in phase %s, not Running or Pending", pod.Status.Phase))
	case pod.Status.HostIP == "" || len(PodEgressAddresses(pod)) == 0:
		return identity, nil, notMember("", "the pod has no HostIP or PodIP yet")
	}

	// A malformed HostIP would produce a rejected ARM request and endless create retries.
	if _, err := netip.ParseAddr(pod.Status.HostIP); err != nil {
		return identity, nil, notMember("ServiceGatewayInvalidPodIP", fmt.Sprintf("Malformed HostIP %q on the pod status", pod.Status.HostIP))
	}

	// A dual-stack pod exposes one address per IP family in Status.PodIPs; each is a member address, so the
	// secondary family egresses through the NAT Gateway too. Individually malformed addresses are skipped.
	var addresses []string
	for _, podIP := range PodEgressAddresses(pod) {
		if _, err := netip.ParseAddr(podIP); err != nil {
			problems = append(problems, egressMembershipProblem{eventReason: "ServiceGatewayInvalidPodIP",
				message: fmt.Sprintf("Malformed PodIP %q on the pod status", podIP)})
			continue
		}
		addresses = append(addresses, podIP)
	}
	if len(addresses) == 0 {
		return identity, nil, notMember("", "the pod has no valid PodIP")
	}

	// The label becomes the NAT Gateway name/ARM resource ID; an invalid value would produce malformed ARM
	// requests (endless retries), and a reserved value would make this controller manage a resource it does
	// not own.
	if IsReservedEgressIdentity(identity) {
		return identity, nil, notMember("ServiceGatewayReservedEgressLabel", fmt.Sprintf(
			"Egress gateway label %q is reserved for the cluster's default outbound gateway and cannot be used; choose a different value", identity))
	}
	if !IsValidEgressIdentity(identity) {
		return identity, nil, notMember("ServiceGatewayInvalidEgressLabel", fmt.Sprintf(
			"Invalid egress gateway label %q: must be a valid Azure resource name (alphanumerics, '-', '_', '.'; start alphanumeric; 1-%d chars)",
			identity, maxEgressIdentityLength))
	}

	// Each address is registered under the node location of its IP family (see PodNodeLocationsByFamily):
	// NRP rejects a location that mixes families.
	locations := PodNodeLocationsByFamily(pod)
	for _, address := range addresses {
		location, ok := NodeLocationForAddress(locations, address)
		if !ok {
			problems = append(problems, egressMembershipProblem{eventReason: "ServiceGatewayNoNodeLocation",
				message: fmt.Sprintf("No same-family node IP for pod address %q; the node must expose that IP family in status.hostIPs", address)})
			continue
		}
		accepted = append(accepted, egressAddress{IP: address, Location: location})
	}
	return identity, accepted, problems
}

// parseEgressVote returns the vote cast by the egress annotations of pod, parsed with pc (egressVoteOf).
func parseEgressVote(pod *v1.Pod, pc egressParseContext) egressVote {
	return egressVoteOf(egressAnnotations(pod), pc)
}

// egressVoteParser returns how pod votes for an identity whose parse context is pc: with its egress annotations,
// parsed by the engine under dt.mu (addEgressPod, setEgressVote).
func egressVoteParser(pod *v1.Pod) func(pc egressParseContext) egressVote {
	return func(pc egressParseContext) egressVote { return parseEgressVote(pod, pc) }
}

// egressParseContextOfLocked returns what parsing needs to know about the egress identity: its NAT Gateway
// serves the IP families its operation was created with, else the cluster's, which a new operation takes. An
// identity being deleted takes the cluster's too: a pod that joins it without reviving it waits for its
// re-creation, which takes them like a new operation. Every vote of an identity counts for the same context, so
// pods without annotations agree: votes parsed for other families are parsed again (reparseEgressVotesLocked).
// Must be called with dt.mu held.
func (dt *DiffTracker) egressParseContextOfLocked(identity string) egressParseContext {
	if opState, exists := dt.pendingServiceOps[identity]; exists && opState.Config.OutboundConfig != nil &&
		opState.State != StateDeletionPending && opState.State != StateDeletionInProgress {
		return dt.egressParseContextFor(opState.Config.OutboundConfig.IPFamilies)
	}
	return dt.egressParseContextFor(dt.outboundIPFamiliesLocked())
}

// egressVoteIdentityOf returns the egress identity the pod podKey with uid is a member of according to the
// engine, which knows it from the pod's vote.
func (dt *DiffTracker) egressVoteIdentityOf(podKey string, uid types.UID) (string, bool) {
	dt.mu.Lock()
	defer dt.mu.Unlock()
	vote, ok := dt.egress.votes.get(podKey)
	if !ok || vote.PodUID != uid {
		return "", false
	}
	return vote.Identity, true
}

// podInformerAddPod handles an egress pod that may be a member of its egress identity: an informer ADD, or
// the add half of an UPDATE. egressMembership decides, and its problems are reported to the pod as Warning
// events. A member gets the cleanup finalizer and is added in one engine call (addEgressPod) with its
// accepted addresses and the vote its egress annotations cast, parsed in that call; the engine creates, revives
// or buffers the identity's NAT Gateway as needed, and emits the vote's events. A pod already marked for
// deletion is routed to the delete handler. It reports whether the pod was added to the engine as a member.
func (dt *DiffTracker) podInformerAddPod(pod *v1.Pod) bool {
	// Should always be set due to the label selector, but the selector matches an empty value too.
	if egressIdentityOf(pod) == "" {
		dt.logger.V(4).Info("Skipped egress pod without an egress label value", "namespace", pod.Namespace, "pod", pod.Name)
		return false
	}

	// A pod already marked for deletion (informer sync, or a delete delivered as an ADD) routes to
	// the delete handler.
	if pod.DeletionTimestamp != nil {
		dt.logger.V(2).Info("Routed egress pod being deleted to the delete handler", "namespace", pod.Namespace, "pod", pod.Name)
		dt.podInformerRemovePod(pod)
		return false
	}

	identity, accepted, problems := checkEgressMembership(pod)
	podKey := fmt.Sprintf("%s/%s", pod.Namespace, pod.Name)
	logLevel := 4
	for _, problem := range problems {
		if problem.eventReason != "" {
			logLevel = 2
			dt.recordEvent(pod, v1.EventTypeWarning, problem.eventReason, problem.message)
		}
	}
	if len(accepted) == 0 {
		dt.logger.V(logLevel).Info("Skipped egress pod that is not a member of its egress identity",
			"pod", podKey, "service", identity, "reason", egressMembershipReason(problems))
		return false
	}
	if len(problems) > 0 {
		dt.logger.V(2).Info("Skipped egress pod addresses that cannot be registered",
			"pod", podKey, "service", identity, "reason", egressMembershipReason(problems))
	}
	// Add the finalizer before registering. On sustained apiserver failure we still register the pod
	// (returning would kill its egress) but without the cleanup finalizer, losing NRP-drain
	// protection on a later delete; surface that via a metric + Event.
	if err := dt.AddPodFinalizer(context.Background(), pod); err != nil {
		if errors.Is(err, ErrPodGoneOrReplaced) {
			// The named pod is gone or already replaced by a same-name pod with a different UID.
			// Registering this stale event pod's address would add an unprotected NRP mapping (no
			// finalizer to drain it); the live replacement registers itself via its own Add event.
			dt.logger.V(4).Info("Skipped stale egress pod registration of a pod that is gone or replaced", "pod", podKey)
			return false
		}
		dt.logger.Error(err, "Registered egress pod without its cleanup finalizer after retries", "pod", podKey)
		RecordPodFinalizerAddFailed()
		dt.recordEvent(pod, v1.EventTypeWarning, "ServiceGatewayFinalizerAddFailed",
			fmt.Sprintf("Failed to add ServiceGateway cleanup finalizer after retries; egress pod registered without NRP-drain protection: %v", err))
	}

	dt.addEgressPod(identity, egressPodRefOf(pod), accepted, egressVoteParser(pod))
	dt.logger.V(2).Info("Added egress pod", "pod", podKey, "service", identity, "addresses", accepted)
	return true
}

// podInformerSetVote records the vote of an egress pod whose egress annotations alone changed (4.1). The
// engine ignores it unless the pod is a member of its identity, and emits the events of a changed vote.
func (dt *DiffTracker) podInformerSetVote(pod *v1.Pod) {
	dt.setEgressVote(egressIdentityOf(pod), egressPodRefOf(pod), egressVoteParser(pod))
}

// podInformerRemovePod handles egress pod deletion. Engine.DeletePod drain-gates the finalizer: for
// a tracked pod it is removed only after every address drains from NRP (CheckPendingPodDeletions, or
// RemoveLastPodFinalizers after NAT Gateway deletion for the last pod). For an untracked pod or one
// with no IPs there is nothing to drain, so the finalizer is removed directly here. The removed pod is
// a member of no egress identity: DeletePod and DeletePodWithoutAddresses withdraw its vote.
func (dt *DiffTracker) podInformerRemovePod(pod *v1.Pod) {
	podKey := fmt.Sprintf("%s/%s", pod.Namespace, pod.Name)
	egressName := egressIdentityOf(pod)
	if egressName == "" {
		// A pod without an egress label value may still be a member of the identity its vote names: a live
		// pod that loses the label key leaves the informer's selection as a delete, and a delete can carry
		// the pod as it is after an update the informer never delivered. Its addresses then drain from that
		// identity like those of any removed pod, before its finalizer goes.
		identity, member := dt.egressVoteIdentityOf(podKey, pod.UID)
		if !member {
			dt.podInformerReleaseUnlabelledPod(pod)
			return
		}
		dt.logger.V(2).Info("Removed egress pod without an egress label value from the identity it is a member of",
			"pod", podKey, "service", identity)
		egressName = identity
	}

	podIPs := PodEgressAddresses(pod)

	// No new IPs to drain. But if an earlier event already drain-gated this pod (the kubelet can
	// clear a terminating pod's PodIPs after we registered its address, so the delete event carries
	// none), the finalizer must stay until CheckPendingPodDeletions strips it once NRP confirms the
	// drain. Only strip inline when the engine has no pending drain - a genuinely untracked or
	// never-registered pod - to avoid stranding it.
	if pod.Status.HostIP == "" || len(podIPs) == 0 {
		result := dt.DeletePodWithoutAddresses(egressName, pod.Namespace, pod.Name, string(pod.UID))
		klog.V(2).Infof("podInformerRemovePod: Pod %s has no IPs; finalizer decision=%s (lastPod=%t, enqueued=%t)",
			podKey, result.FinalizerDecision, result.IsLastPod, result.Enqueued)
		if result.FinalizerDecision == PodFinalizerDecisionReleaseNoDrain {
			if err := dt.RemovePodFinalizerByPod(context.Background(), pod); err != nil {
				// The engine proved there is no drain, so queue a finalizer-only record: nothing
				// else re-drives this pod, because the informer only reacts to a state change and a
				// resync delivers an unchanged object.
				RecordPodFinalizerRemoveFailed()
				klog.Errorf("podInformerRemovePod: pod %s could not have its cleanup finalizer removed, queued for retry: %v", podKey, err)
				dt.enqueuePodFinalizerRetry(pod, egressName)
			}
		}
		return
	}

	klog.V(2).Infof("podInformerRemovePod: Pod %s removed from egress %s (HostIP=%s, PodIPs=%v)",
		podKey, egressName, pod.Status.HostIP, podIPs)

	// Atomically remove every address (a dual-stack pod has one per family); the single finalizer is
	// stripped only after all have drained from NRP, never inline.
	result := dt.DeletePod(egressName, pod.Status.HostIP, podIPs, pod.Namespace, pod.Name, string(pod.UID))

	klog.V(2).Infof("podInformerRemovePod: Pod %s finalizer decision=%s (lastPod=%t, enqueued=%t)",
		podKey, result.FinalizerDecision, result.IsLastPod, result.Enqueued)

	// Remove only after the engine explicitly proves that neither local nor NRP state needs a drain.
	// Do not infer release from !IsLastPod && !Enqueued: a local-state miss can still have an NRP
	// mapping, in which case DeletePod reconstructs pending drain tracking and holds the finalizer.
	if result.FinalizerDecision == PodFinalizerDecisionReleaseNoDrain {
		klog.V(2).Infof("podInformerRemovePod: Pod %s has no local or NRP drain; removing finalizer directly", podKey)
		if err := dt.RemovePodFinalizerByPod(context.Background(), pod); err != nil {
			RecordPodFinalizerRemoveFailed()
			klog.Errorf("podInformerRemovePod: pod %s could not have its cleanup finalizer removed, queued for retry: %v", podKey, err)
			dt.enqueuePodFinalizerRetry(pod, egressName)
		}
	}
}

// podInformerReleaseUnlabelledPod finishes a removed pod that has no egress label value and is a member of no
// egress identity, so it has no vote and no address to drain.
func (dt *DiffTracker) podInformerReleaseUnlabelledPod(pod *v1.Pod) {
	// An unlabelled pod carrying our cleanup finalizer is still ours to finish with. The informer
	// selects on the label key alone, so a pod whose value was emptied keeps matching and is
	// still delivered here, with nothing left to identify its egress service. Skipping it would
	// leave the finalizer attached with nothing able to remove it, blocking node drain and
	// namespace deletion.
	if hasFinalizer(pod.Finalizers, ServiceGatewayPodCleanupFinalizer) {
		klog.V(2).Infof("podInformerRemovePod: Pod %s/%s has no egress label but carries the cleanup finalizer; removing it directly",
			pod.Namespace, pod.Name)
		if err := dt.RemovePodFinalizerByPod(context.Background(), pod); err != nil {
			RecordPodFinalizerRemoveFailed()
			klog.Errorf("podInformerRemovePod: pod %s/%s could not have its cleanup finalizer removed, queued for retry: %v",
				pod.Namespace, pod.Name, err)
			dt.enqueuePodFinalizerRetry(pod, "")
		}
		return
	}
	klog.V(4).Infof("podInformerRemovePod: Pod %s/%s has no egress label, skipping", pod.Namespace, pod.Name)
}

// podInformerReregister re-registers a live egress pod whose egress label or addresses changed (4.1).
//
// A label move drains the pod from its old identity without touching its vote, then adds it to the new
// one, which moves the vote there; when the pod is no member of the new identity, the vote is withdrawn
// instead, as the pod is a member of none.
//
// An address change within the identity is decided by the pod's new accepted addresses. When some are
// left, the addresses the pod leaves are drained without letting the identity go
// (DeletePodForReplacement) and the pod is re-added, keeping its vote. When none is left, the live pod
// leaves the identity (LivePodLeftEgress): its addresses drain, its vote is withdrawn and the identity is
// deleted after its last pod, while the pod keeps its cleanup finalizer until it is really deleted, which
// takes the removal path (it still has addresses, so its delete event reaches DeletePod).
func (dt *DiffTracker) podInformerReregister(oldPod, newPod *v1.Pod) {
	oldIdentity := egressIdentityOf(oldPod)
	podKey := fmt.Sprintf("%s/%s", oldPod.Namespace, oldPod.Name)
	if oldIdentity != egressIdentityOf(newPod) {
		dt.podInformerDrainForReplace(oldPod, newPod)
		if !dt.podInformerAddPod(newPod) {
			dt.RemoveEgressVote(oldIdentity, podKey, oldPod.UID)
		}
		return
	}

	if _, accepted, reason := egressMembership(newPod); len(accepted) == 0 {
		dt.logger.V(2).Info("Drained live egress pod without an accepted address from its egress identity",
			"pod", podKey, "service", oldIdentity, "reason", reason)
		dt.LivePodLeftEgress(oldIdentity, oldPod.Status.HostIP, PodEgressAddresses(oldPod), egressPodRefOf(oldPod))
		return
	}
	dt.podInformerDrainForReplace(oldPod, newPod)
	dt.podInformerAddPod(newPod)
}

// podInformerDrainForReplace drains the NRP addresses a still-live egress pod is LEAVING during an
// immediate re-registration (an IP change or egress-label move where podInformerAddPod re-adds it on
// the same event), without touching its cleanup finalizer.
//
// Empty namespace/name makes Engine.DeletePod drain but enqueue no finalizer record, so a concurrent
// stripper can't touch the still-live pod. Only addresses that won't stay at their current family
// location are drained; ones that stay put are kept so a sole pod's ref-count never dips to zero.
func (dt *DiffTracker) podInformerDrainForReplace(oldPod, newPod *v1.Pod) {
	if oldPod.Labels == nil || oldPod.Labels[consts.PodLabelServiceEgressGateway] == "" {
		return
	}

	oldEgress := strings.ToLower(oldPod.Labels[consts.PodLabelServiceEgressGateway])
	podKey := fmt.Sprintf("%s/%s", oldPod.Namespace, oldPod.Name)
	oldAddrs := PodEgressAddresses(oldPod)
	if oldPod.Status.HostIP == "" || len(oldAddrs) == 0 {
		return
	}

	var newEgress string
	if newPod.Labels != nil {
		newEgress = strings.ToLower(newPod.Labels[consts.PodLabelServiceEgressGateway])
	}

	// Drain each old address that will not stay at its current family node location (removed or
	// moved); keep the ones that stay put so a sole pod's ref-count does not dip to zero. A different
	// egress service drains the full old set.
	var toDrain []string
	if oldEgress != newEgress {
		toDrain = oldAddrs
	} else {
		oldLocs := PodNodeLocationsByFamily(oldPod)
		newLocs := PodNodeLocationsByFamily(newPod)
		newAddrs := PodEgressAddresses(newPod)
		for _, addr := range oldAddrs {
			oldLoc, _ := NodeLocationForAddress(oldLocs, addr)
			newLoc, _ := NodeLocationForAddress(newLocs, addr)
			if !slices.Contains(newAddrs, addr) || oldLoc != newLoc {
				toDrain = append(toDrain, addr)
			}
		}
	}
	if len(toDrain) == 0 {
		return
	}

	klog.V(2).Infof("podInformerDrainForReplace: draining egress %s addresses %v for live re-registration of pod %s (HostIP=%s)",
		oldEgress, toDrain, podKey, oldPod.Status.HostIP)

	// Empty namespace/name: drain from NRP but enqueue no finalizer record, so no stripper can act on
	// the still-live pod. The finalizer is re-ensured by the following AddPod.
	if oldEgress == newEgress {
		dt.DeletePodForReplacement(oldEgress, oldPod.Status.HostIP, toDrain, "", "", "")
		return
	}
	dt.DeletePod(oldEgress, oldPod.Status.HostIP, toDrain, "", "", "")
}
