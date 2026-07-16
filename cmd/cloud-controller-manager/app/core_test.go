/*
Copyright 2021 The Kubernetes Authors.

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

package app

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	cloudprovider "k8s.io/cloud-provider"
	genericcontrollermanager "k8s.io/controller-manager/app"

	cloudcontrollerconfig "sigs.k8s.io/cloud-provider-azure/cmd/cloud-controller-manager/app/config"
	nodeipamconfig "sigs.k8s.io/cloud-provider-azure/pkg/nodeipam/config"
)

type fakeServiceGatewayControllerProvider struct {
	cloudprovider.Interface

	enabled                   bool
	started                   bool
	loadBalancerCapturedEarly bool
}

func (f *fakeServiceGatewayControllerProvider) IsServiceGatewayEnabled() bool {
	return f.enabled
}

func (f *fakeServiceGatewayControllerProvider) StartServiceGatewayController(context.Context, informers.SharedInformerFactory) error {
	f.started = true
	return nil
}

func (f *fakeServiceGatewayControllerProvider) LoadBalancer() (cloudprovider.LoadBalancer, bool) {
	if !f.started {
		f.loadBalancerCapturedEarly = true
	}
	return nil, true
}

type staticControllerClientBuilder struct {
	cloudprovider.ControllerClientBuilder
	client kubernetes.Interface
}

func (b staticControllerClientBuilder) ClientOrDie(string) kubernetes.Interface {
	return b.client
}

func TestSetNodeCIDRMaskSizesDualStack(t *testing.T) {
	for _, testCase := range []struct {
		description                        string
		mask, ipv4Mask, ipv6Mask           int32
		expectedIPV4Mask, expectedIPV6Mask int
	}{
		{
			description:      "setNodeCIDRMaskSizesDualStack should ignore the node cidr mask size",
			mask:             17,
			ipv6Mask:         65,
			expectedIPV4Mask: 24,
			expectedIPV6Mask: 65,
		},
		{
			description:      "setNodeCIDRMaskSizesDualStack should set the ipv4 and ipv6 mask sizes as configured",
			mask:             17,
			ipv4Mask:         18,
			ipv6Mask:         65,
			expectedIPV4Mask: 18,
			expectedIPV6Mask: 65,
		},
		{
			description:      "setNodeCIDRMaskSizesDualStack should set the default ipv4 and ipv6 mask sizes",
			mask:             17,
			expectedIPV4Mask: 24,
			expectedIPV6Mask: 64,
		},
	} {
		t.Run(testCase.description, func(t *testing.T) {
			cfg := nodeipamconfig.NodeIPAMControllerConfiguration{
				NodeCIDRMaskSize:     testCase.mask,
				NodeCIDRMaskSizeIPv4: testCase.ipv4Mask,
				NodeCIDRMaskSizeIPv6: testCase.ipv6Mask,
			}

			ipv4Mask, ipv6Mask, err := setNodeCIDRMaskSizesDualStack(cfg)
			assert.NoError(t, err)
			assert.Equal(t, testCase.expectedIPV4Mask, ipv4Mask)
			assert.Equal(t, testCase.expectedIPV6Mask, ipv6Mask)
		})
	}
}

func TestValidateServiceGatewayControllerConfiguration(t *testing.T) {
	tests := []struct {
		name        string
		cloud       any
		controllers []string
		wantErr     string
	}{
		{
			name:        "non ServiceGateway provider",
			cloud:       struct{}{},
			controllers: []string{"*"},
		},
		{
			name:        "ServiceGateway disabled",
			cloud:       &fakeServiceGatewayControllerProvider{},
			controllers: []string{"-service-lb-controller"},
		},
		{
			name:        "service controller enabled",
			cloud:       &fakeServiceGatewayControllerProvider{enabled: true},
			controllers: []string{"*"},
		},
		{
			name:        "service controller disabled",
			cloud:       &fakeServiceGatewayControllerProvider{enabled: true},
			controllers: []string{"-service-lb-controller"},
			wantErr:     `ServiceGateway requires "service-lb-controller" to be enabled`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateServiceGatewayControllerConfiguration(test.cloud, test.controllers)
			if test.wantErr == "" {
				assert.NoError(t, err)
				return
			}
			assert.EqualError(t, err, test.wantErr)
		})
	}
}

func TestStartServiceControllerBootstrapsServiceGatewayBeforeLoadBalancerCapture(t *testing.T) {
	kubeClient := fake.NewSimpleClientset()
	config := (&cloudcontrollerconfig.Config{
		LoopbackClientConfig: &rest.Config{},
		ClientBuilder:        staticControllerClientBuilder{client: kubeClient},
		SharedInformers:      informers.NewSharedInformerFactory(kubeClient, 0),
	}).Complete()
	cloud := &fakeServiceGatewayControllerProvider{enabled: true}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, started, err := startServiceController(
		ctx,
		genericcontrollermanager.ControllerContext{},
		config,
		cloud,
	)

	assert.NoError(t, err)
	assert.True(t, started)
	assert.True(t, cloud.started)
	assert.False(t, cloud.loadBalancerCapturedEarly)
}
