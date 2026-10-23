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
	"net/netip"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork/v12"
	"github.com/google/uuid"
	v1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/cloud-provider-azure/pkg/consts"
)

var errBYOEgressConfig = errors.New("BYO NAT Gateways do not accept egress configuration annotations")

func eligibleEgressConfigPod(pod *v1.Pod) bool {
	if pod.DeletionTimestamp != nil || (pod.Status.Phase != v1.PodRunning && pod.Status.Phase != v1.PodPending) {
		return false
	}
	if _, err := netip.ParseAddr(pod.Status.HostIP); err != nil {
		return false
	}
	locations := PodNodeLocationsByFamily(pod)
	for _, address := range PodEgressAddresses(pod) {
		if _, err := netip.ParseAddr(address); err != nil {
			continue
		}
		if _, ok := NodeLocationForAddress(locations, address); ok {
			return true
		}
	}
	return false
}

func (dt *DiffTracker) egressVoteForPodLocked(pod *v1.Pod) (egressPodVote, error) {
	identity := strings.ToLower(pod.Labels[consts.PodLabelServiceEgressGateway])
	if !IsValidEgressIdentity(identity) || IsReservedEgressIdentity(identity) || !eligibleEgressConfigPod(pod) ||
		dt.outboundIdentityConflictsWithInboundLocked(identity) ||
		(dt.NRPResources.ReservedOutboundServices != nil && dt.NRPResources.ReservedOutboundServices.Has(identity)) {
		return egressPodVote{}, nil
	}
	config, err := parseEgressConfig(pod.Annotations)
	return egressPodVote{UID: pod.UID, Identity: identity, Config: config, Invalid: err != nil}, err
}

// Informer storage can advance ahead of its callbacks. Check coherence before using the counters;
// do not rebuild them or select a configuration from this cache snapshot.
func (dt *DiffTracker) validateEgressVotesLocked(identity string) error {
	if dt.egressPodInformer == nil || !dt.egressPodInformer.HasSynced() {
		return errors.New("egress pod informer is not synchronized")
	}
	members := 0
	for _, object := range dt.egressPodInformer.GetStore().List() {
		pod, ok := object.(*v1.Pod)
		if !ok || pod == nil {
			return fmt.Errorf("egress pod store contains %T", object)
		}
		if !strings.EqualFold(pod.Labels[consts.PodLabelServiceEgressGateway], identity) {
			continue
		}
		current, _ := dt.egressVoteForPodLocked(pod)
		if current.Identity == "" {
			continue
		}
		members++
		key := pod.Namespace + "/" + pod.Name
		if recorded, ok := dt.egressVotes.byPod[key]; !ok || recorded != current {
			return fmt.Errorf("waiting for the current egress vote of pod %s", key)
		}
	}
	group := dt.egressVotes.byIdentity[identity]
	if group == nil || members != group.pods {
		return fmt.Errorf("waiting for current egress membership of %q", identity)
	}
	return nil
}

func (dt *DiffTracker) observeEgressConfigPod(event *v1.Pod, resync bool) {
	key := event.Namespace + "/" + event.Name
	var reportedPod *v1.Pod
	var noticeErr error
	dt.mu.Lock()
	defer func() {
		dt.mu.Unlock()
		if noticeErr != nil {
			dt.recordEvent(reportedPod, v1.EventTypeWarning, consts.ServiceEgressGatewayReasonInvalidConfig, noticeErr.Error())
		}
	}()
	if dt.egressPodInformer == nil {
		dt.logger.Error(errors.New("egress pod informer is not set"), "Could not record egress vote", "pod", key)
		return
	}
	object, exists, err := dt.egressPodInformer.GetStore().GetByKey(key)
	if err != nil {
		dt.logger.Error(err, "Could not read current pod for egress vote", "pod", key)
		return
	}
	old := dt.egressVotes.byPod[key]
	vote := egressPodVote{}
	if exists {
		pod, ok := object.(*v1.Pod)
		if !ok || pod == nil {
			dt.logger.Error(fmt.Errorf("unexpected cached object %T", object), "Could not record egress vote", "pod", key)
			return
		}
		vote, noticeErr = dt.egressVoteForPodLocked(pod)
		if vote.Identity != "" {
			reportedPod = pod
			if dt.byoNATGateways[vote.Identity] != "" {
				for key := range pod.Annotations {
					if isEgressConfigAnnotation(key) {
						noticeErr = errBYOEgressConfig
						break
					}
				}
			}
		}
	}
	identities := [2]string{old.Identity, vote.Identity}
	var before [2]egressConfig
	var beforeErr [2]error
	for i, identity := range identities {
		before[i], beforeErr[i] = dt.egressVotes.agreed(identity)
	}
	var changed bool
	if exists {
		_, changed = dt.egressVotes.set(key, vote)
	} else {
		_, changed = dt.egressVotes.remove(key, event.UID)
	}
	for i, identity := range identities {
		if identity == "" || (i == 1 && identity == identities[0]) {
			continue
		}
		after, afterErr := dt.egressVotes.agreed(identity)
		if resync || (changed && (before[i] != after || !errors.Is(afterErr, beforeErr[i]))) {
			dt.queueEgressConfigLocked(identity)
		}
	}
}

func (dt *DiffTracker) queueEgressConfig(identity string) {
	dt.mu.Lock()
	defer dt.mu.Unlock()
	dt.queueEgressConfigLocked(identity)
}

func (dt *DiffTracker) queueEgressConfigLocked(identity string) {
	if !IsValidEgressIdentity(identity) || IsReservedEgressIdentity(identity) ||
		dt.egressPodInformer == nil || !dt.egressPodInformer.HasSynced() || !dt.egressVotesReady ||
		atomic.LoadInt32(&dt.isInitializing) != 0 ||
		dt.outboundIdentityConflictsWithInboundLocked(identity) ||
		(dt.NRPResources.ReservedOutboundServices != nil && dt.NRPResources.ReservedOutboundServices.Has(identity)) {
		return
	}
	op := dt.pendingServiceOps[identity]
	if op == nil {
		if dt.NRPResources.NATGateways == nil || !dt.NRPResources.NATGateways.Has(identity) {
			return
		}
		now := time.Now()
		op = &ServiceOperationState{
			ServiceUID: identity,
			Config: NewOutboundServiceConfig(identity, &OutboundConfig{
				IPFamilies: dt.outboundIPFamiliesLocked(),
			}),
			State: StateCreated, CreatedAt: now, LastAttempt: now.Format(time.RFC3339),
			CorrelationID: uuid.NewString(),
		}
		dt.pendingServiceOps[identity] = op
	}
	if op.Config.IsInbound || op.IsOrphan {
		return
	}
	switch op.State {
	case StateNotStarted, StateCreationInProgress:
		op.EgressConfigDirty = true
	case StateCreated:
		op.EgressConfigDirty = true
		op.State = StateUpdateInProgress
		resetRetryStateLocked(op)
		dt.triggerServiceUpdater()
	case StateUpdateInProgress:
		op.EgressConfigDirty = true
		dt.triggerServiceUpdater()
	}
}

func (dt *DiffTracker) egressConfigMembers(identity string) ([]*v1.Pod, error) {
	dt.mu.Lock()
	informer := dt.egressPodInformer
	dt.mu.Unlock()
	if informer == nil || !informer.HasSynced() {
		return nil, errors.New("egress pod informer is not synchronized")
	}
	keys := dt.egressPodKeysOf(identity)
	pods := make([]*v1.Pod, 0, len(keys))
	for _, key := range keys {
		obj, exists, err := informer.GetStore().GetByKey(key)
		if err != nil {
			return nil, fmt.Errorf("reading cached egress member %q: %w", key, err)
		}
		if !exists {
			continue
		}
		pod, ok := obj.(*v1.Pod)
		if !ok || pod == nil {
			return nil, fmt.Errorf("egress pod store contains %T", obj)
		}
		if strings.ToLower(pod.Labels[consts.PodLabelServiceEgressGateway]) == identity && eligibleEgressConfigPod(pod) {
			pods = append(pods, pod)
		}
	}
	return pods, nil
}

func (dt *DiffTracker) reconcileEgressConfig(ctx context.Context, identity string) (err error) {
	changed := false
	defer func() {
		if err == nil && !changed {
			recordOutboundServiceUpdateSkipped()
		}
	}()
	dt.mu.Lock()
	ready := dt.egressVotesReady
	hasMembers := dt.egressVotes.byIdentity[identity] != nil
	config, voteErr := dt.egressVotes.agreed(identity)
	if ready && hasMembers && voteErr == nil && dt.byoNATGateways[identity] == "" {
		voteErr = dt.validateEgressVotesLocked(identity)
	}
	dt.mu.Unlock()
	if !ready {
		return errors.New("egress votes have not completed initial synchronization")
	}
	if !hasMembers {
		return nil
	}
	pods, err := dt.egressConfigMembers(identity)
	if err != nil {
		return err
	}
	if dt.byoNATGatewayID(identity) != "" {
		for _, pod := range pods {
			for key := range pod.Annotations {
				if isEgressConfigAnnotation(key) {
					dt.recordEvent(pod, v1.EventTypeWarning, consts.ServiceEgressGatewayReasonInvalidConfig,
						errBYOEgressConfig.Error())
					break
				}
			}
		}
		return nil
	}
	if voteErr != nil {
		reason := consts.ServiceEgressGatewayReasonInvalidConfig
		if errors.Is(voteErr, errEgressConfigConflict) {
			reason = consts.ServiceEgressGatewayReasonConfigConflict
		} else if !errors.Is(voteErr, errInvalidEgressVotes) {
			return voteErr
		}
		for _, pod := range pods {
			dt.recordEvent(pod, v1.EventTypeWarning, reason, voteErr.Error())
		}
		return nil
	}
	minutes := config.IdleTimeoutMinutes
	changed, err = dt.updateEgressConfig(ctx, identity, config)
	if err != nil {
		for _, pod := range pods {
			dt.recordEvent(pod, v1.EventTypeWarning, consts.ServiceEgressGatewayReasonConfigRetrying, err.Error())
		}
		return err
	}
	for _, pod := range pods {
		if _, configured := pod.Annotations[consts.PodAnnotationServiceEgressGatewayIdleTimeout]; changed || configured {
			dt.recordEvent(pod, v1.EventTypeNormal, consts.ServiceEgressGatewayReasonConfigApplied,
				fmt.Sprintf("Egress identity %q has idle timeout %d minutes", identity, minutes))
		}
	}
	return nil
}

func (dt *DiffTracker) updateEgressConfig(ctx context.Context, identity string, config egressConfig) (bool, error) {
	if !IsValidEgressIdentity(identity) || dt.isReservedOutboundService(identity) ||
		dt.byoNATGatewayID(identity) != "" {
		return false, fmt.Errorf("egress identity %q cannot configure a managed NAT Gateway", identity)
	}
	minutes := config.IdleTimeoutMinutes
	if minutes < consts.ServiceEgressGatewayIdleTimeoutMinMinutes || minutes > consts.ServiceEgressGatewayIdleTimeoutMaxMinutes {
		return false, fmt.Errorf("egress idle timeout %d is out of range", minutes)
	}
	id := fmt.Sprintf(consts.NatGatewayIDTemplate, dt.config.networkResourceSubscriptionID(), dt.config.ResourceGroup, identity)
	gateway, exists, err := dt.getNatGatewayByID(ctx, id)
	if err != nil {
		return false, err
	}
	if !exists || gateway == nil || gateway.Properties == nil {
		return false, fmt.Errorf("managed NAT Gateway %q is missing or unreadable", identity)
	}
	if !ownsNATGateway(gateway) {
		return false, fmt.Errorf("NAT Gateway %q is not owned by egress identity %q", id, identity)
	}
	if gateway.Properties.ProvisioningState == nil ||
		*gateway.Properties.ProvisioningState != armnetwork.ProvisioningStateSucceeded {
		return false, fmt.Errorf("NAT Gateway %q has not completed provisioning", id)
	}
	if !strings.EqualFold(natGatewayServiceGatewayID(gateway), dt.config.ServiceGatewayResourceID()) {
		return false, fmt.Errorf("NAT Gateway %q is not linked to this Service Gateway", id)
	}
	if user, readErr := dt.otherServiceUsing(ctx, identity, id); readErr != nil {
		return false, readErr
	} else if user != "" {
		return false, fmt.Errorf("NAT Gateway %q is also used by service %q", id, user)
	}
	if ptr.Deref(gateway.Properties.IdleTimeoutInMinutes, consts.ServiceEgressGatewayIdleTimeoutDefaultMinutes) == minutes {
		return false, nil
	}
	writeContext, err := withNATGatewayETag(ctx, gateway.Etag)
	if err != nil {
		return false, err
	}
	desired, properties := *gateway, *gateway.Properties
	properties.IdleTimeoutInMinutes = ptr.To(minutes)
	desired.Properties = &properties
	if err := dt.createOrUpdateNatGateway(writeContext, dt.config.ResourceGroup, desired); err != nil {
		return false, err
	}
	return true, nil
}
