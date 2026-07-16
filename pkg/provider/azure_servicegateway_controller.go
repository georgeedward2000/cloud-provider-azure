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

package provider

import (
	"context"
	"fmt"

	"k8s.io/client-go/informers"

	"sigs.k8s.io/cloud-provider-azure/pkg/consts"
	"sigs.k8s.io/cloud-provider-azure/pkg/provider/servicegateway/difftracker"
)

func (az *Cloud) serviceGatewayConfig() difftracker.Config {
	return difftracker.Config{
		SubscriptionID:                az.SubscriptionID,
		NetworkResourceSubscriptionID: az.getNetworkResourceSubscriptionID(),
		ResourceGroup:                 az.ResourceGroup,
		Location:                      az.Location,
		VNetName:                      az.VnetName,
		VNetResourceGroup:             az.VnetResourceGroup,
		ServiceGatewayResourceName:    consts.DefaultServiceGatewayResourceName,
	}
}

// IsServiceGatewayEnabled reports whether the provider is configured to use ServiceGateway.
func (az *Cloud) IsServiceGatewayEnabled() bool {
	return az.ServiceGatewayEnabled
}

// StartServiceGatewayController initializes the ServiceGateway runtime before the service
// controller captures the provider's LoadBalancer implementation.
func (az *Cloud) StartServiceGatewayController(ctx context.Context, informerFactory informers.SharedInformerFactory) error {
	if az.serviceGatewayController == nil {
		return fmt.Errorf("ServiceGateway controller is not configured")
	}
	return az.serviceGatewayController.Start(
		ctx,
		informerFactory,
		az.NetworkClientFactory,
		az.KubeClient,
		az.eventRecorder,
	)
}
