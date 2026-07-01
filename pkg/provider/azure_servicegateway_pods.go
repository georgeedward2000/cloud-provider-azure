package provider

import (
	"context"
	"crypto/rand"
	"fmt"
	"math/big"
	"net/netip"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	"sigs.k8s.io/cloud-provider-azure/pkg/consts"
	"sigs.k8s.io/cloud-provider-azure/pkg/provider/servicegateway/difftracker"
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
func (az *Cloud) setUpPodInformerForEgress() {
	klog.V(2).Infof("setUpPodInformerForEgress: Setting up pod informer with label selector: %s", consts.PodLabelServiceEgressGateway)

	// Create a separate informer factory with label selector to filter pods at the API server
	podInformerFactory := informers.NewSharedInformerFactoryWithOptions(
		az.KubeClient,
		ResyncPeriod(12*time.Hour)(),
		informers.WithTweakListOptions(func(options *metav1.ListOptions) {
			// Only watch pods with the egress gateway label
			options.LabelSelector = consts.PodLabelServiceEgressGateway
		}),
	)

	podInformer := podInformerFactory.Core().V1().Pods().Informer()
	_, err := podInformer.AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				pod := obj.(*v1.Pod)
				az.podInformerAddPod(pod)
			},
			UpdateFunc: func(oldObj, newObj interface{}) {
				oldPod := oldObj.(*v1.Pod)
				newPod := newObj.(*v1.Pod)

				// Extract egress gateway names from labels
				var prevEgressGatewayName, currEgressGatewayName string
				if oldPod.Labels != nil {
					prevEgressGatewayName = strings.ToLower(oldPod.Labels[consts.PodLabelServiceEgressGateway])
				}
				if newPod.Labels != nil {
					currEgressGatewayName = strings.ToLower(newPod.Labels[consts.PodLabelServiceEgressGateway])
				}

				// Detect relevant changes
				labelChanged := prevEgressGatewayName != currEgressGatewayName
				oldHadIPs := oldPod.Status.HostIP != "" && oldPod.Status.PodIP != ""
				newHasIPs := newPod.Status.HostIP != "" && newPod.Status.PodIP != ""
				ipsChanged := oldPod.Status.HostIP != newPod.Status.HostIP || oldPod.Status.PodIP != newPod.Status.PodIP

				// Check if pod became invalid (non-Running/Pending, or got deletion timestamp)
				// This is similar to needsCleanup() in k8s.io/cloud-provider/controllers/service/controller.go
				// Kubernetes uses 2-phase deletion: Phase 1 sets DeletionTimestamp (UPDATE event),
				// Phase 2 removes object after finalizers are removed (DELETE event).
				// We detect deletion here in UpdateFunc when DeletionTimestamp transitions from nil.
				oldWasValid := oldPod.DeletionTimestamp == nil &&
					(oldPod.Status.Phase == v1.PodRunning || oldPod.Status.Phase == v1.PodPending)
				newIsValid := newPod.DeletionTimestamp == nil &&
					(newPod.Status.Phase == v1.PodRunning || newPod.Status.Phase == v1.PodPending)

				// Determine required actions
				var needsRemove, needsAdd bool
				var reason string

				if labelChanged {
					// Label changed: remove from old gateway (if had IPs), add to new gateway (if valid and has IPs)
					needsRemove = prevEgressGatewayName != "" && oldHadIPs
					needsAdd = currEgressGatewayName != "" && newHasIPs && newIsValid
					reason = fmt.Sprintf("label changed from %s to %s", prevEgressGatewayName, currEgressGatewayName)
				} else if currEgressGatewayName != "" {
					// Same label: check various state transitions
					if oldWasValid && !newIsValid && oldHadIPs {
						// Pod became invalid (terminated, failed, or being deleted)
						needsRemove = true
						reason = fmt.Sprintf("pod became invalid (Phase: %s, DeletionTimestamp: %v)",
							newPod.Status.Phase, newPod.DeletionTimestamp != nil)
					} else if !oldHadIPs && newHasIPs && newIsValid {
						// Pod just got IPs and is valid
						needsAdd = true
						reason = "completed IP initialization"
					} else if oldHadIPs && !newHasIPs {
						// Pod lost IPs
						needsRemove = true
						reason = "lost IPs"
					} else if oldHadIPs && newHasIPs && ipsChanged && newIsValid {
						// IPs changed (pod moved or got new IP) - only re-add if still valid
						needsRemove = true
						needsAdd = true
						reason = fmt.Sprintf("IPs changed (HostIP: %s→%s, PodIP: %s→%s)",
							oldPod.Status.HostIP, newPod.Status.HostIP, oldPod.Status.PodIP, newPod.Status.PodIP)
					} else if oldHadIPs && newHasIPs && !newIsValid {
						// Pod still has IPs but became invalid - need to remove
						needsRemove = true
						reason = fmt.Sprintf("pod became invalid while having IPs (Phase: %s, DeletionTimestamp: %v)",
							newPod.Status.Phase, newPod.DeletionTimestamp != nil)
					}
				} // Execute actions
				if needsRemove || needsAdd {
					klog.V(2).Infof("setUpPodInformerForEgress: Pod %s/%s update: %s",
						newPod.Namespace, newPod.Name, reason)
					if needsRemove {
						az.podInformerRemovePod(oldPod)
					}
					if needsAdd {
						az.podInformerAddPod(newPod)
					}
				} else {
					klog.V(4).Infof("setUpPodInformerForEgress: Pod %s/%s update has no relevant changes, skipping",
						newPod.Namespace, newPod.Name)
				}
			},
			DeleteFunc: func(obj interface{}) {
				// NOTE: Following the Kubernetes service controller pattern, deletion is primarily
				// handled via UpdateFunc when DeletionTimestamp is set (Kubernetes 2-phase deletion).
				// See: vendor/k8s.io/cloud-provider/controllers/service/controller.go
				// "No need to handle deletion event because the deletion would be handled by
				// the update path when the deletion timestamp is added."
				//
				// When pod finalizers are in use, the delete event only fires AFTER we've already
				// processed the pod deletion in UpdateFunc (where DeletionTimestamp transition is
				// detected) and removed the finalizer. This DeleteFunc serves as a defensive backup
				// for edge cases like DeletedFinalStateUnknown from watch stream disconnection.
				//
				// This is also needed for pods created during CCM downtime that may not have
				// finalizers added in time before namespace deletion.
				var pod *v1.Pod
				switch v := obj.(type) {
				case *v1.Pod:
					pod = v
				case cache.DeletedFinalStateUnknown:
					// Watch stream was disconnected and the object was deleted from the server.
					// This is a rare edge case when finalizers couldn't prevent deletion.
					var ok bool
					pod, ok = v.Obj.(*v1.Pod)
					if !ok {
						klog.Errorf("Cannot convert to *v1.Pod: %T", v.Obj)
						return
					}
					klog.V(2).Infof("DeleteFunc: processing DeletedFinalStateUnknown for pod %s/%s",
						pod.Namespace, pod.Name)
				default:
					klog.Errorf("Cannot convert to *v1.Pod: %T", v)
					return
				}
				// Call is idempotent - safe even if already processed in UpdateFunc
				az.podInformerRemovePod(pod)
			},
		})
	if err != nil {
		klog.Errorf("setUpPodInformerForEgress: Failed to add event handlers to pod informer: %v", err)
		return
	}

	// Start the informer factory
	klog.V(2).Infof("setUpPodInformerForEgress: Starting pod informer factory")
	podInformerFactory.Start(wait.NeverStop)
	podInformerFactory.WaitForCacheSync(wait.NeverStop)
	klog.V(2).Infof("setUpPodInformerForEgress: Pod informer successfully initialized and synced")
}

// podInformerAddPod handles pod addition events for egress.
// It validates the pod has the required egress label and IPs, then calls Engine.AddPod().
// The Engine handles all states:
// - Service doesn't exist → Engine creates NAT Gateway and buffers pod
// - Service being created → Engine buffers pod
// - Service ready → Engine adds pod immediately
func (az *Cloud) podInformerAddPod(pod *v1.Pod) {
	// Validate pod has egress label (should always be true due to label selector, but check anyway)
	if pod.Labels == nil || pod.Labels[consts.PodLabelServiceEgressGateway] == "" {
		klog.V(4).Infof("podInformerAddPod: Pod %s/%s has no egress label, skipping", pod.Namespace, pod.Name)
		return
	}

	// Handle pods that are being deleted - route to delete handler
	// This happens when:
	// 1. Informer sync sees a pod that was marked for deletion before the informer started
	// 2. Pod deletion was initiated and we receive it as an ADD (client-go behavior)
	if pod.DeletionTimestamp != nil {
		klog.V(2).Infof("podInformerAddPod: Pod %s/%s is being deleted (DeletionTimestamp set), routing to delete handler",
			pod.Namespace, pod.Name)
		az.podInformerRemovePod(pod)
		return
	}

	// Only process pods in Running or Pending phase
	if pod.Status.Phase != v1.PodRunning && pod.Status.Phase != v1.PodPending {
		klog.V(4).Infof("podInformerAddPod: Pod %s/%s is in phase %s (not Running/Pending), skipping",
			pod.Namespace, pod.Name, pod.Status.Phase)
		return
	}

	// Validate pod has required IPs
	if pod.Status.HostIP == "" || pod.Status.PodIP == "" {
		klog.V(4).Infof("podInformerAddPod: Pod %s/%s has egress label but no HostIP or PodIP yet, skipping",
			pod.Namespace, pod.Name)
		return
	}

	// Validate the IPs are well-formed before they flow into the ServiceGateway address/location
	// payload. A malformed value (kubelet bug or a crafted pod status) would otherwise produce a
	// rejected ARM request and endless create retries.
	if _, err := netip.ParseAddr(pod.Status.HostIP); err != nil {
		klog.Warningf("podInformerAddPod: pod %s/%s has a malformed HostIP %q; skipping egress registration",
			pod.Namespace, pod.Name, pod.Status.HostIP)
		az.Event(pod, v1.EventTypeWarning, "ServiceGatewayInvalidPodIP",
			fmt.Sprintf("Malformed HostIP %q on the pod status", pod.Status.HostIP))
		return
	}
	if _, err := netip.ParseAddr(pod.Status.PodIP); err != nil {
		klog.Warningf("podInformerAddPod: pod %s/%s has a malformed PodIP %q; skipping egress registration",
			pod.Namespace, pod.Name, pod.Status.PodIP)
		az.Event(pod, v1.EventTypeWarning, "ServiceGatewayInvalidPodIP",
			fmt.Sprintf("Malformed PodIP %q on the pod status", pod.Status.PodIP))
		return
	}

	egressName := strings.ToLower(pod.Labels[consts.PodLabelServiceEgressGateway])
	if !difftracker.IsValidEgressIdentity(egressName) {
		// The egress label is set by the workload owner and becomes the NAT Gateway name and ARM
		// resource ID, so an invalid value would produce malformed ARM requests (endless create
		// retries). Reject it and surface the misconfiguration rather than registering the pod.
		klog.Warningf("podInformerAddPod: pod %s/%s has an invalid egress gateway label %q; skipping egress registration",
			pod.Namespace, pod.Name, egressName)
		az.Event(pod, v1.EventTypeWarning, "ServiceGatewayInvalidEgressLabel",
			fmt.Sprintf("Invalid egress gateway label %q: must be a valid Azure resource name (alphanumerics, '-', '_', '.'; start alphanumeric; 1-80 chars)", egressName))
		return
	}
	podKey := fmt.Sprintf("%s/%s", pod.Namespace, pod.Name)

	klog.V(2).Infof("podInformerAddPod: Pod %s added with egress %s (HostIP=%s, PodIP=%s)",
		podKey, egressName, pod.Status.HostIP, pod.Status.PodIP)

	// Add pod finalizer before registering with engine. AddPodFinalizer already retries with
	// backoff; this runs synchronously on the dedicated, label-filtered egress informer, so the
	// bounded (~1.5s) wait can only delay other egress pod events, never the main CCM informers.
	// A failure here means a sustained apiserver outage during the pod's IP burst. We deliberately
	// still register the pod (returning would silently kill its egress), but the pod is now synced
	// into NRP WITHOUT the cleanup finalizer, so it lacks NRP-drain protection on a later delete.
	// Make that rare, unprotected state observable via a metric + warning Event rather than only a
	// log line.
	if err := az.diffTracker.AddPodFinalizer(context.Background(), pod); err != nil {
		klog.Warningf("podInformerAddPod: registering egress pod %s WITHOUT cleanup finalizer after retries: %v", podKey, err)
		difftracker.RecordPodFinalizerAddFailed()
		az.Event(pod, v1.EventTypeWarning, "ServiceGatewayFinalizerAddFailed",
			fmt.Sprintf("Failed to add ServiceGateway cleanup finalizer after retries; egress pod registered without NRP-drain protection: %v", err))
	}

	// Call Engine.AddPod - it handles all states and service creation
	az.diffTracker.AddPod(egressName, podKey, pod.Status.HostIP, pod.Status.PodIP)
}

// podInformerRemovePod handles pod deletion events for egress.
// It calls Engine.DeletePod() which handles last-pod deletion logic:
//   - Tracked, not last pod → Engine enqueues it; the finalizer is removed by
//     CheckPendingPodDeletions once the address is drained from NRP.
//   - Tracked, last pod → the finalizer is removed after NAT Gateway/PIP deletion completes.
//   - Not tracked (stale/duplicate delete, or no live address after a restart) or no IPs → there is
//     nothing to drain, so the finalizer is removed directly here to avoid stranding the pod.
//
// In the tracked cases the finalizer is removed only after NRP has drained the pod's overlay
// address, so the pod (and its IP) is not reclaimed while NRP still maps the address.
func (az *Cloud) podInformerRemovePod(pod *v1.Pod) {
	// Validate pod has egress label
	if pod.Labels == nil || pod.Labels[consts.PodLabelServiceEgressGateway] == "" {
		klog.V(4).Infof("podInformerRemovePod: Pod %s/%s has no egress label, skipping", pod.Namespace, pod.Name)
		return
	}

	egressName := strings.ToLower(pod.Labels[consts.PodLabelServiceEgressGateway])
	podKey := fmt.Sprintf("%s/%s", pod.Namespace, pod.Name)

	// Need IPs to identify which NRP address/location to drain. Without them there is nothing to
	// drain, so remove the finalizer directly to avoid stranding the pod in Terminating; the
	// periodic reconcile drains any orphaned NRP address independently of the finalizer.
	if pod.Status.HostIP == "" || pod.Status.PodIP == "" {
		klog.V(2).Infof("podInformerRemovePod: Pod %s has egress label but no IPs; removing finalizer directly (nothing to drain)", podKey)
		if err := az.diffTracker.RemovePodFinalizerByPod(context.Background(), pod); err != nil {
			klog.Warningf("podInformerRemovePod: Failed to remove finalizer from no-IP pod %s: %v", podKey, err)
		}
		return
	}

	klog.V(2).Infof("podInformerRemovePod: Pod %s removed from egress %s (HostIP=%s, PodIP=%s)",
		podKey, egressName, pod.Status.HostIP, pod.Status.PodIP)

	// Call Engine.DeletePod. For a tracked pod the finalizer is intentionally NOT removed here:
	// the engine records it in pendingPodDeletions and the finalizer is stripped only after the
	// pod's overlay address has been drained from NRP (CheckPendingPodDeletions for non-last pods,
	// RemoveLastPodFinalizers after NAT Gateway deletion for the last pod). Removing it inline
	// would let the pod (and its IP) be reclaimed while NRP still maps the address.
	result := az.diffTracker.DeletePod(egressName, pod.Status.HostIP, pod.Status.PodIP, pod.Namespace, pod.Name, string(pod.UID))

	// If the engine did not enqueue the pod (a stale/duplicate delete, or a pod no longer in live
	// state after a restart) and it is not the last pod, there is nothing to drain from NRP, so the
	// finalizer must be removed directly here; otherwise it would be stranded forever.
	if !result.IsLastPod && !result.Enqueued {
		klog.V(2).Infof("podInformerRemovePod: Pod %s is not tracked by the engine; removing finalizer directly (nothing to drain)", podKey)
		if err := az.diffTracker.RemovePodFinalizerByPod(context.Background(), pod); err != nil {
			klog.Warningf("podInformerRemovePod: Failed to remove finalizer from untracked pod %s: %v", podKey, err)
		}
	}
}
