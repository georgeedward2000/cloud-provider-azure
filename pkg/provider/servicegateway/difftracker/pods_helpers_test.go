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
	"go.uber.org/mock/gomock"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"

	"sigs.k8s.io/cloud-provider-azure/pkg/azclient/mock_azclient"
	"sigs.k8s.io/cloud-provider-azure/pkg/consts"
	"sigs.k8s.io/cloud-provider-azure/pkg/log"
	utilsets "sigs.k8s.io/cloud-provider-azure/pkg/util/sets"
)

// seedDiffTracker builds a real engine pre-seeded with the given K8s and NRP state, so a test can
// start from live egress pods rather than driving the full create lifecycle.
func seedDiffTracker(t *testing.T, clientFactory *mock_azclient.MockClientFactory, kubeClient kubernetes.Interface, k8s K8sState, nrp NRPState) *DiffTracker {
	t.Helper()
	dt, err := New(
		log.Noop(),
		k8s,
		nrp,
		Config{
			SubscriptionID:             "sub",
			ResourceGroup:              "rg",
			Location:                   "eastus",
			VNetName:                   "vnet",
			ServiceGatewayResourceName: "sgw",
		},
		clientFactory,
		kubeClient,
	)
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	return dt
}

// newSeededDiffTracker builds a real seeded engine with a fresh event recorder, so the egress
// informer paths can emit pod events. It returns the recorder for tests that assert on them.
func newSeededDiffTracker(t *testing.T, ctrl *gomock.Controller, kubeClient kubernetes.Interface, k8s K8sState, nrp NRPState) (*DiffTracker, *record.FakeRecorder) {
	t.Helper()
	dt := seedDiffTracker(t, mock_azclient.NewMockClientFactory(ctrl), kubeClient, k8s, nrp)
	rec := record.NewFakeRecorder(50)
	dt.SetEventRecorder(rec)
	return dt, rec
}

// seededDT builds a real seeded engine (recorder attached, handle discarded).
func seededDT(t *testing.T, ctrl *gomock.Controller, kubeClient kubernetes.Interface, k8s K8sState, nrp NRPState) *DiffTracker {
	t.Helper()
	dt, _ := newSeededDiffTracker(t, ctrl, kubeClient, k8s, nrp)
	return dt
}

// newProviderDiffTracker builds a real engine with empty K8s and NRP state.
func newProviderDiffTracker(t *testing.T, ctrl *gomock.Controller, kubeClient kubernetes.Interface) *DiffTracker {
	t.Helper()
	return seededDT(t, ctrl, kubeClient,
		K8sState{Services: utilsets.NewString(), Egresses: utilsets.NewString(), Nodes: map[string]Node{}},
		NRPState{LoadBalancers: utilsets.NewString(), NATGateways: utilsets.NewString(), Locations: map[string]NRPLocation{}})
}

// newTestPod creates a pod with the given attributes for egress informer tests.
func newTestPod(namespace, name, egressLabel, hostIP, podIP string, phase v1.PodPhase, deletionTimestamp *metav1.Time) *v1.Pod {
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
		},
		Status: v1.PodStatus{
			HostIP: hostIP,
			PodIP:  podIP,
			Phase:  phase,
		},
	}
	if egressLabel != "" {
		pod.Labels = map[string]string{
			consts.PodLabelServiceEgressGateway: egressLabel,
		}
	}
	if deletionTimestamp != nil {
		pod.DeletionTimestamp = deletionTimestamp
	}
	return pod
}

// withPodIPs sets a pod's Status.PodIPs (keeping Status.PodIP aligned with PodIPs[0]) so tests can
// exercise dual-stack egress pods.
func withPodIPs(pod *v1.Pod, ips ...string) *v1.Pod {
	pod.Status.PodIPs = nil
	for _, ip := range ips {
		pod.Status.PodIPs = append(pod.Status.PodIPs, v1.PodIP{IP: ip})
	}
	if len(ips) > 0 {
		pod.Status.PodIP = ips[0]
	}
	return pod
}

// AddEgressPod is addEgressPod with vote, for tests that build votes rather than pods: vote is registered as parsed
// for the identity's IP families (its ParsedForIPv6 is overwritten), so it must be one parsed for them. Production
// parses the pod's annotations for them under dt.mu instead (egressVoteParser).
func (dt *DiffTracker) AddEgressPod(identity string, pod egressPodRef, vote egressVote, accepted []egressAddress) {
	dt.addEgressPod(identity, pod, accepted, func(pc egressParseContext) egressVote {
		vote.ParsedForIPv6 = pc.WantsIPv6
		return vote
	})
}

// SetEgressVote is setEgressVote with vote, registered as parsed for the identity's IP families as by AddEgressPod.
func (dt *DiffTracker) SetEgressVote(identity string, pod egressPodRef, vote egressVote) {
	dt.setEgressVote(identity, pod, func(pc egressParseContext) egressVote {
		vote.ParsedForIPv6 = pc.WantsIPv6
		return vote
	})
}

// AddPod and AddPodWithUID add one address of an egress pod to its identity without a vote, for tests of the address
// and lifecycle state machine that do not exercise votes: like addEgressPod, they reject invalid parameters and an
// identity that collides with an inbound service, then call addPodAddressLocked. Production adds members only through
// addEgressPod, so every member votes.
func (dt *DiffTracker) AddPod(serviceUID, podKey, location, address string) {
	dt.addPod(serviceUID, podKey, "", location, address)
}

// AddPodWithUID is AddPod for a pod with UID podUID.
func (dt *DiffTracker) AddPodWithUID(serviceUID, podKey, podUID, location, address string) {
	dt.addPod(serviceUID, podKey, podUID, location, address)
}

func (dt *DiffTracker) addPod(serviceUID, podKey, podUID, location, address string) {
	dt.mu.Lock()
	defer dt.mu.Unlock()

	if serviceUID == "" || location == "" || address == "" {
		dt.logger.V(4).Info("Could not add pod with invalid parameters", "service", serviceUID, "location", location, "address", address)
		return
	}

	if dt.outboundIdentityConflictsWithInboundLocked(serviceUID) {
		dt.logger.V(2).Info("Rejected egress pod whose identity collides with an inbound service",
			"service", serviceUID, "pod", podKey)
		return
	}

	dt.addPodAddressLocked(serviceUID, podKey, podUID, location, address)
}
