package provider

import (
	"context"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork/v9"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	"sigs.k8s.io/cloud-provider-azure/pkg/azclient/subnetclient/mock_subnetclient"
)

func TestAttachServiceGatewayToSubnet_UsesConfiguredVNetRGAndSubnet(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	az := GetTestCloud(ctrl)
	az.ResourceGroup = "cluster-rg"
	az.VnetResourceGroup = "network-rg"
	az.VnetName = "byovnet"
	az.SubnetName = "custom-subnet"

	subnetClient := az.NetworkClientFactory.GetSubnetClient().(*mock_subnetclient.MockInterface)
	subnetClient.EXPECT().Get(gomock.Any(), az.VnetResourceGroup, az.VnetName, az.SubnetName, nil).Return(&armnetwork.Subnet{
		Properties: &armnetwork.SubnetPropertiesFormat{},
	}, nil).Times(1)
	subnetClient.EXPECT().CreateOrUpdate(gomock.Any(), az.VnetResourceGroup, az.VnetName, az.SubnetName, gomock.Any()).Return(&armnetwork.Subnet{}, nil).Times(1)

	err := az.attachServiceGatewayToSubnet(context.Background())
	assert.NoError(t, err)
}
