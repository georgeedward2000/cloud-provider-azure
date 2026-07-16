package difftracker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"

	"sigs.k8s.io/cloud-provider-azure/pkg/azclient"
)

func TestControllerStart(t *testing.T) {
	originalInitialize := initializeControllerDiffTracker
	originalStartPodInformer := startControllerPodInformer
	t.Cleanup(func() {
		initializeControllerDiffTracker = originalInitialize
		startControllerPodInformer = originalStartPodInformer
	})

	var runtimeCtx context.Context
	tracker := &DiffTracker{}
	initializeControllerDiffTracker = func(
		ctx context.Context,
		_ Config,
		_ azclient.ClientFactory,
		_ kubernetes.Interface,
	) (*DiffTracker, error) {
		runtimeCtx = ctx
		return tracker, nil
	}
	startControllerPodInformer = func(ctx context.Context, got *DiffTracker) error {
		assert.Same(t, tracker, got)
		assert.Same(t, runtimeCtx, ctx)
		return nil
	}

	kubeClient := fake.NewSimpleClientset()
	factory := informers.NewSharedInformerFactory(kubeClient, 0)
	controller := NewController(Config{})

	ctx, cancel := context.WithCancel(context.Background())
	if !assert.NoError(t, controller.Start(ctx, factory, nil, kubeClient, record.NewFakeRecorder(1))) {
		cancel()
		return
	}
	gotTracker, err := controller.diffTracker()
	assert.NoError(t, err)
	assert.Same(t, tracker, gotTracker)

	err = controller.Start(ctx, factory, nil, kubeClient, record.NewFakeRecorder(1))
	assert.EqualError(t, err, "ServiceGateway controller is already started")

	cancel()
	select {
	case <-runtimeCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("ServiceGateway runtime context was not cancelled")
	}
}

func TestControllerStartFailureRollsBack(t *testing.T) {
	originalInitialize := initializeControllerDiffTracker
	originalStartPodInformer := startControllerPodInformer
	t.Cleanup(func() {
		initializeControllerDiffTracker = originalInitialize
		startControllerPodInformer = originalStartPodInformer
	})

	tracker := &DiffTracker{}
	var runtimeCtx context.Context
	initializeControllerDiffTracker = func(
		context.Context,
		Config,
		azclient.ClientFactory,
		kubernetes.Interface,
	) (*DiffTracker, error) {
		return tracker, nil
	}
	startControllerPodInformer = func(ctx context.Context, _ *DiffTracker) error {
		runtimeCtx = ctx
		return errors.New("pod informer failed")
	}

	kubeClient := fake.NewSimpleClientset()
	controller := NewController(Config{})
	err := controller.Start(
		context.Background(),
		informers.NewSharedInformerFactory(kubeClient, 0),
		nil,
		kubeClient,
		record.NewFakeRecorder(1),
	)
	assert.EqualError(t, err, "start filtered Pod informer: pod informer failed")
	_, dependencyErr := controller.diffTracker()
	assert.EqualError(t, dependencyErr, "ServiceGateway LoadBalancer is not initialized")
	select {
	case <-runtimeCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("failed ServiceGateway runtime context was not cancelled")
	}
}
