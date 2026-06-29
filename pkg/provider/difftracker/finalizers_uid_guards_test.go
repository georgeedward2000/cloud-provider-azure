/*
Copyright 2026 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

// Tests for pod finalizer handling when pods are replaced.

package difftracker

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
)

func newFinalizerPod(name, uid string, hasFinalizer bool) *v1.Pod {
	p := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      name,
			UID:       types.UID(uid),
		},
	}
	if hasFinalizer {
		p.ObjectMeta.Finalizers = []string{ServiceGatewayPodCleanupFinalizer}
	}
	return p
}

// PendingPodDeletion should not remove a replacement pod's finalizer.
func TestGuardPendingPodDeletion_DoesNotStripReplacementPodFinalizer(t *testing.T) {
	// Seed kube with the REPLACEMENT pod (same name, different UID),
	// already carrying the finalizer (added by an AddPod against the new UID).
	replacement := newFinalizerPod("foo", "uid-NEW", true)
	kube := fake.NewSimpleClientset(replacement)

	dt := newTestDiffTracker()
	dt.kubeClient = kube
	euid := "egress-uid-confusion"
	dt.NRPResources.NATGateways.Insert(euid)
	dt.pendingServiceOps[euid] = &ServiceOperationState{
		ServiceUID: euid,
		Config:     NewOutboundServiceConfig(euid, nil),
		State:      StateCreated,
	}

	// Seed a PendingPodDeletion for the OLD pod (same ns/name, but the predecessor's UID).
	dt.pendingPodDeletions["default/foo"] = &PendingPodDeletion{
		Namespace:  "default",
		Name:       "foo",
		UID:        "uid-OLD",
		ServiceUID: euid,
		Address:    "10.244.0.7",
		Location:   "10.0.0.1",
		IsLastPod:  false,
	}

	// Address has been removed from NRP (location-sync complete) so
	// CheckPendingPodDeletions will try to strip the finalizer.
	dt.CheckPendingPodDeletions(context.Background())

	got, err := kube.CoreV1().Pods("default").Get(context.Background(), "foo", metav1.GetOptions{})
	assert.NoError(t, err)
	assert.Contains(t, got.ObjectMeta.Finalizers, ServiceGatewayPodCleanupFinalizer,
		"replacement pod (different UID) must NOT have its finalizer stripped by predecessor's PendingPodDeletion")
}

// Last-pod finalizer removal should not affect a replacement pod.
func TestGuardLastPodFinalizer_DoesNotStripReplacementPod(t *testing.T) {
	replacement := newFinalizerPod("foo", "uid-NEW", true)
	kube := fake.NewSimpleClientset(replacement)

	dt := newTestDiffTracker()
	dt.kubeClient = kube
	euid := "egress-last-pod-uid"

	dt.pendingPodDeletions["default/foo"] = &PendingPodDeletion{
		Namespace:  "default",
		Name:       "foo",
		UID:        "uid-OLD",
		ServiceUID: euid,
		Address:    "10.244.0.7",
		Location:   "10.0.0.1",
		IsLastPod:  true,
	}

	dt.RemoveLastPodFinalizers(context.Background(), euid)

	got, err := kube.CoreV1().Pods("default").Get(context.Background(), "foo", metav1.GetOptions{})
	assert.NoError(t, err)
	assert.Contains(t, got.ObjectMeta.Finalizers, ServiceGatewayPodCleanupFinalizer,
		"replacement pod (different UID) must NOT have its finalizer stripped by predecessor's last-pod entry")
}

// The normal path should still remove the finalizer.
func TestGuardFinalizers_PositiveStripRemovesFinalizer(t *testing.T) {
	pod := newFinalizerPod("foo", "uid-LIVE", true)
	kube := fake.NewSimpleClientset(pod)

	dt := newTestDiffTracker()
	dt.kubeClient = kube
	euid := "egress-positive-strip"

	dt.pendingPodDeletions["default/foo"] = &PendingPodDeletion{
		Namespace:  "default",
		Name:       "foo",
		UID:        "uid-LIVE",
		ServiceUID: euid,
		Address:    "10.244.0.7",
		Location:   "10.0.0.1",
		IsLastPod:  true,
	}

	dt.RemoveLastPodFinalizers(context.Background(), euid)

	got, err := kube.CoreV1().Pods("default").Get(context.Background(), "foo", metav1.GetOptions{})
	assert.NoError(t, err)
	assert.NotContains(t, got.ObjectMeta.Finalizers, ServiceGatewayPodCleanupFinalizer,
		"happy-path: pod that matches the entry must have its finalizer stripped")
	// And the entry must be cleared from the map.
	_, stillPending := dt.pendingPodDeletions["default/foo"]
	assert.False(t, stillPending, "processed entry must be removed from pendingPodDeletions")
}
