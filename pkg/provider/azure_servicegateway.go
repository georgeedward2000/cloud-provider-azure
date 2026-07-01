package provider

import (
	"context"
	"fmt"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork/v9"
	"k8s.io/klog/v2"
	"sigs.k8s.io/cloud-provider-azure/pkg/consts"
)

// TODO(enechitoaia): remove after added aks-rp support
func (az *Cloud) existsServiceGateway(ctx context.Context, serviceGatewayName string) (bool, error) {
	sgw, err := az.GetServiceGateway(ctx, serviceGatewayName)
	if err != nil {
		if strings.Contains(err.Error(), consts.ResourceNotFoundMessageCode) {
			return false, nil
		}
		klog.Infof("ExistsServiceGateway: error checking existence of Service Gateway %s in resource group %s: %v", serviceGatewayName, az.ResourceGroup, err)
		return false, err
	}

	if sgw == nil || sgw.Properties == nil || sgw.Properties.RouteTargetAddress == nil ||
		sgw.Properties.RouteTargetAddress.PrivateIPAllocationMethod == nil ||
		*sgw.Properties.RouteTargetAddress.PrivateIPAllocationMethod != armnetwork.IPAllocationMethodDynamic ||
		sgw.Properties.RouteTargetAddress.Subnet == nil {
		klog.Infof("ExistsServiceGateway: Service Gateway %s in resource group %s is not properly configured", serviceGatewayName, az.ResourceGroup)
		return false, nil
	}

	return true, nil
}

// defaultServiceGatewaySubnetName is used when the cluster config specifies no subnet (the AKS node subnet).
const defaultServiceGatewaySubnetName = "aks-subnet"

// serviceGatewaySubnetName returns the subnet used by the route target and subnet attachment,
// defaulting to the AKS node subnet when unset so both paths always resolve identically.
func (az *Cloud) serviceGatewaySubnetName() string {
	if az.SubnetName != "" {
		return az.SubnetName
	}
	return defaultServiceGatewaySubnetName
}

// TODO(enechitoaia): remove after added aks-rp support
func (az *Cloud) createServiceGateway(ctx context.Context, serviceGatewayName string) error {
	// Resolve the VNet/subnet IDs as the subnet attachment does: getVnetResourceID honours
	// VnetResourceGroup (BYO-VNet), and the subnet defaults to the AKS node subnet. Using
	// az.ResourceGroup or a bare az.SubnetName here would target the wrong VNet.
	vnetID := az.getVnetResourceID()
	subnetID := fmt.Sprintf("%s/subnets/%s", vnetID, az.serviceGatewaySubnetName())

	// Create the service gateway if it does not exist.
	serviceGateway := armnetwork.ServiceGateway{
		Location: to.Ptr(az.Location),
		SKU: &armnetwork.ServiceGatewaySKU{
			Name: to.Ptr(armnetwork.ServiceGatewaySKUNameStandard),
			Tier: to.Ptr(armnetwork.ServiceGatewaySKUTierRegional),
		},
		Properties: &armnetwork.ServiceGatewayPropertiesFormat{
			VirtualNetwork: &armnetwork.VirtualNetwork{ID: to.Ptr(vnetID)},
			RouteTargetAddress: &armnetwork.RouteTargetAddressPropertiesFormat{
				PrivateIPAllocationMethod: to.Ptr(armnetwork.IPAllocationMethodDynamic),
				Subnet: &armnetwork.Subnet{
					ID: to.Ptr(subnetID),
				},
			},
		},
	}
	// logObject(serviceGateway)
	err := az.CreateOrUpdateServiceGateway(ctx, serviceGatewayName, serviceGateway)
	if err != nil {
		klog.Infof("createServiceGateway: error creating Service Gateway %s in resource group %s: %v", serviceGatewayName, az.ResourceGroup, err)
		return fmt.Errorf("InitializeCloudFromConfig: failed to create Service Gateway %s: %w", serviceGatewayName, err)
	}
	klog.Infof("createServiceGateway: successfully created Service Gateway %s in resource group %s", serviceGatewayName, az.ResourceGroup)
	return nil
}
