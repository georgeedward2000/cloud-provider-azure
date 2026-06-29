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

// Caller-level (RULE 1) guards for egress pod-finalizer strand conditions. These complement the
// engine-isolation guards in pkg/provider/difftracker/finalizers_strand_guards_test.go by driving
// the REAL informer caller podInformerRemovePod, where the !result.Enqueued fallback removes the
// finalizer for pods the engine does not enqueue for drain-gated removal.

package provider

import (
	"context"
	"testing"

	"go.uber.org/mock/gomock"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"sigs.k8s.io/cloud-provider-azure/pkg/consts"
	"sigs.k8s.io/cloud-provider-azure/pkg/provider/difftracker"

	"github.com/stretchr/testify/assert"
)

// TestPodInformerRemovePod_BufferedPodFinalizerRemovedByCaller verifies that a pod deleted while it
// is still BUFFERED for an in-flight (never-created) egress service does NOT strand its cleanup
// finalizer. DeletePod returns early via cancelBufferedPodLocked with Enqueued=false (no
// pendingPodDeletions entry, nothing to drain from NRP), so podInformerRemovePod's
// `!result.IsLastPod && !result.Enqueued` branch removes the finalizer directly. This is the
// caller-level refutation of the engine-isolation strand documented (skipped) in
// difftracker/finalizers_strand_guards_test.go: the engine intentionally does not track a buffered
// pod, and the caller is responsible for the direct removal.
func TestPodInformerRemovePod_BufferedPodFinalizerRemovedByCaller(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "egress-buffered",
			Namespace:  "default",
			Labels:     map[string]string{consts.PodLabelServiceEgressGateway: "egress-svc"},
			Finalizers: []string{difftracker.ServiceGatewayPodCleanupFinalizer},
		},
		Status: v1.PodStatus{HostIP: "10.0.0.1", PodIP: "10.244.0.7"},
	}
	kubeClient := fake.NewSimpleClientset(pod)

	az := GetTestCloudWithContainerLoadBalancer(ctrl)
	az.KubeClient = kubeClient
	az.diffTracker = newProviderDiffTracker(t, az, kubeClient)

	// Buffer the pod for an in-flight egress service (the harness does not run async workers, so the
	// service stays in StateNotStarted with the pod buffered in pendingPods).
	az.diffTracker.AddPod("egress-svc", "default/egress-buffered", "10.0.0.1", "10.244.0.7")

	// Delete the pod while it is still buffered, through the real informer caller.
	az.podInformerRemovePod(pod)

	got, err := kubeClient.CoreV1().Pods("default").Get(context.Background(), "egress-buffered", metav1.GetOptions{})
	assert.NoError(t, err)
	assert.NotContains(t, got.Finalizers, difftracker.ServiceGatewayPodCleanupFinalizer,
		"a pod deleted while buffered must have its finalizer removed by the caller (!Enqueued fallback), not stranded in Terminating")
}
