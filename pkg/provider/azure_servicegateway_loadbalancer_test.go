package provider

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"
	cloudprovider "k8s.io/cloud-provider"

	"sigs.k8s.io/cloud-provider-azure/pkg/provider/servicegateway/difftracker"
)

func initializeTestServiceGatewayLoadBalancer(az *Cloud, tracker *difftracker.DiffTracker) cloudprovider.LoadBalancer {
	return difftracker.NewLoadBalancer(tracker, az.eventRecorder)
}

func TestCloudSelectsStableServiceGatewayLoadBalancer(t *testing.T) {
	az := &Cloud{}
	legacy, supported := az.LoadBalancer()
	assert.True(t, supported)
	assert.Same(t, az, legacy)

	az.ServiceGatewayEnabled = true
	az.serviceGatewayController = difftracker.NewController(difftracker.Config{})
	first, supported := az.LoadBalancer()
	assert.True(t, supported)
	assert.NotSame(t, az, first)
	second, supported := az.LoadBalancer()
	assert.True(t, supported)
	assert.Same(t, first, second)
}

func TestServiceGatewayLoadBalancerRequiresRuntimeInitialization(t *testing.T) {
	az := &Cloud{Config: *serviceGatewayConfig()}
	az.serviceGatewayController = difftracker.NewController(difftracker.Config{})
	loadBalancer, _ := az.LoadBalancer()
	service := getTestService("servicegateway-not-started", v1.ProtocolTCP, nil, false, 80)

	status, err := loadBalancer.EnsureLoadBalancer(context.Background(), testClusterName, &service, nil)
	assert.EqualError(t, err, "ServiceGateway LoadBalancer is not initialized")
	assert.Nil(t, status)
	assert.EqualError(t, loadBalancer.UpdateLoadBalancer(context.Background(), testClusterName, &service, nil), "ServiceGateway LoadBalancer is not initialized")
	assert.EqualError(t, loadBalancer.EnsureLoadBalancerDeleted(context.Background(), testClusterName, &service), "ServiceGateway LoadBalancer is not initialized")
	_, _, err = loadBalancer.GetLoadBalancer(context.Background(), testClusterName, &service)
	assert.EqualError(t, err, "ServiceGateway LoadBalancer is not initialized")
}

func TestServiceGatewayLoadBalancerLifecycle(t *testing.T) {
	ctrl := gomock.NewController(t)
	az := GetTestCloudWithContainerLoadBalancer(ctrl)
	az.KubeClient = fake.NewSimpleClientset()
	az.eventRecorder = record.NewFakeRecorder(10)
	tracker := newProviderDiffTracker(t, az, az.KubeClient)
	loadBalancer := initializeTestServiceGatewayLoadBalancer(az, tracker)
	service := getTestService("servicegateway-adapter", v1.ProtocolTCP, nil, false, 80)

	status, err := loadBalancer.EnsureLoadBalancer(context.Background(), testClusterName, &service, nil)
	assert.NoError(t, err)
	assert.NotNil(t, status)
	assert.True(t, tracker.IsServiceTracked(difftracker.ServiceUID(&service)))

	status, exists, err := loadBalancer.GetLoadBalancer(context.Background(), testClusterName, &service)
	assert.NoError(t, err)
	assert.True(t, exists)
	assert.Equal(t, service.Status.LoadBalancer.DeepCopy(), status)

	assert.NoError(t, loadBalancer.UpdateLoadBalancer(context.Background(), testClusterName, &service, nil))
	assert.NoError(t, loadBalancer.EnsureLoadBalancerDeleted(context.Background(), testClusterName, &service))
}
