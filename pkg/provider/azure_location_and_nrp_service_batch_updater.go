package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	discovery_v1 "k8s.io/api/discovery/v1"
	"k8s.io/klog/v2"
	"sigs.k8s.io/cloud-provider-azure/pkg/provider/difftracker"
)

// LocationAndNRPServiceBatchUpdater is a batch processor for updating NRP locations and services.
type locationAndNRPServiceBatchUpdater struct {
	az                   *Cloud
	channelUpdateTrigger chan bool
}

func newLocationAndNRPServiceBatchUpdater(az *Cloud) *locationAndNRPServiceBatchUpdater {
	return &locationAndNRPServiceBatchUpdater{
		az:                   az,
		channelUpdateTrigger: make(chan bool, 1),
	}
}

func (updater *locationAndNRPServiceBatchUpdater) run(ctx context.Context) {
	klog.V(2).Info("locationAndNRPServiceBatchUpdater.run: started")
	for {
		select {
		case <-updater.channelUpdateTrigger:
			updater.process(ctx)
		case <-ctx.Done():
			klog.Infof("locationAndNRPServiceBatchUpdater.run: stopped due to context cancellation")
			return
		}
	}
}

func (updater *locationAndNRPServiceBatchUpdater) process(ctx context.Context) {
	klog.Infof("CLB-ENECHITOAIA-locationAndNRPServiceBatchUpdater.process BEGIN: processing batch update\n")
	klog.Infof("CLB-ENECHITOAIA-locationAndNRPServiceBatchUpdater.process BEGIN: difftracker:\n")
	logDiffTracker(updater.az.diffTracker)
	klog.Infof("CLB-ENECHITOAIA-locationAndNRPServiceBatchUpdater.process BEGIN: subscription ID: %s\n", updater.az.SubscriptionID)
	klog.Infof("CLB-ENECHITOAIA-locationAndNRPServiceBatchUpdater.process BEGIN: resource group: %s\n", updater.az.ResourceGroup)
	klog.Infof("CLB-ENECHITOAIA-locationAndNRPServiceBatchUpdater.process BEGIN: location and NRP service batch updater started\n")

	serviceLoadBalancerList := updater.az.diffTracker.GetSyncLoadBalancerServices()
	klog.Infof("CLB-ENECHITOAIA-locationAndNRPServiceBatchUpdater.process: serviceLoadBalancerList:\n")
	logObject(serviceLoadBalancerList)
	// Add services
	if serviceLoadBalancerList.Additions.Len() > 0 {
		createServicesRequestDTO := difftracker.MapLoadBalancerUpdatesToServicesDataDTO(
			difftracker.SyncServicesReturnType{
				Additions: serviceLoadBalancerList.Additions,
				Removals:  nil,
			},
			updater.az.SubscriptionID,
			updater.az.ResourceGroup)
		klog.Infof("CLB-ENECHITOAIA-locationAndNRPServiceBatchUpdater.process: createServicesRequestDTO:\n")
		logObject(createServicesRequestDTO)
		createServicesResponseDTO := NRPAPIClientUpdateNRPServices(ctx, createServicesRequestDTO, updater.az.SubscriptionID, updater.az.ResourceGroup)
		klog.Infof("CLB-ENECHITOAIA-locationAndNRPServiceBatchUpdater.process: createServicesResponseDTO:\n")
		logObject(createServicesResponseDTO)
		if createServicesResponseDTO == nil {
			updater.az.diffTracker.UpdateNRPLoadBalancers(
				difftracker.SyncServicesReturnType{
					Additions: serviceLoadBalancerList.Additions,
					Removals:  nil,
				},
			)
		} else {
			klog.Errorf("locationAndNRPServiceBatchUpdater.process: failed to create services: %+v\n", createServicesResponseDTO)
			return
		}
		klog.Infof("CLB-ENECHITOAIA-locationAndNRPServiceBatchUpdater.process: successfully created services\n")
	}

	// Update locations and addresses for the added and deleted services
	for _, serviceName := range serviceLoadBalancerList.Additions.UnsortedList() {
		updater.az.localServiceNameToNRPServiceMap.LoadOrStore(serviceName, struct{}{})

		updateK8sEndpointsInputType := difftracker.UpdateK8sEndpointsInputType{
			InboundIdentity: serviceName,
			OldAddresses:    nil,
			NewAddresses:    map[string]string{},
		}
		updater.az.endpointSlicesCache.Range(func(_, value interface{}) bool {
			endpointSlice := value.(*discovery_v1.EndpointSlice)
			serviceUID, loaded := getServiceUIDOfEndpointSlice(endpointSlice)
			if loaded && strings.EqualFold(serviceUID, serviceName) {
				updateK8sEndpointsInputType.NewAddresses = mergeMaps(updateK8sEndpointsInputType.NewAddresses, updater.az.getPodIPToNodeIPMapFromEndpointSlice(endpointSlice, false))
			}
			return true
		})
		klog.Infof("CLB-ENECHITOAIA-locationAndNRPServiceBatchUpdater.process: Additions updateK8sEndpointsInputType after range:\n")
		logObject(updateK8sEndpointsInputType)
		if len(updateK8sEndpointsInputType.NewAddresses) > 0 {
			updater.az.diffTracker.UpdateK8sEndpoints(updateK8sEndpointsInputType)
		}
	}

	for _, serviceName := range serviceLoadBalancerList.Removals.UnsortedList() {
		updateK8sEndpointsInputType := difftracker.UpdateK8sEndpointsInputType{
			InboundIdentity: serviceName,
			OldAddresses:    map[string]string{},
			NewAddresses:    nil,
		}
		updater.az.endpointSlicesCache.Range(func(_, value interface{}) bool {
			endpointSlice := value.(*discovery_v1.EndpointSlice)
			serviceUID, loaded := getServiceUIDOfEndpointSlice(endpointSlice)
			if loaded && strings.EqualFold(serviceUID, serviceName) {
				updateK8sEndpointsInputType.OldAddresses = mergeMaps(updateK8sEndpointsInputType.OldAddresses, updater.az.getPodIPToNodeIPMapFromEndpointSlice(endpointSlice, false))
			}
			return true
		})
		klog.Infof("CLB-ENECHITOAIA-locationAndNRPServiceBatchUpdater.process: Removals updateK8sEndpointsInputType after range:\n")
		logObject(updateK8sEndpointsInputType)
		if len(updateK8sEndpointsInputType.OldAddresses) > 0 {
			updater.az.diffTracker.UpdateK8sEndpoints(updateK8sEndpointsInputType)
		}
	}

	// Update all locations and addresses
	locationData := updater.az.diffTracker.GetSyncLocationsAddresses()
	klog.Infof("CLB-ENECHITOAIA-locationAndNRPServiceBatchUpdater.process: locationData:\n")
	logObject(locationData)
	if len(locationData.Locations) > 0 {
		locationDataRequestDTO := difftracker.MapLocationDataToDTO(locationData)
		klog.Infof("CLB-ENECHITOAIA-locationAndNRPServiceBatchUpdater.process: locationDataRequestDTO:\n")
		logObject(locationDataRequestDTO)
		locationDataResponseDTO := NRPAPIClientUpdateNRPLocations(ctx, locationDataRequestDTO, updater.az.SubscriptionID, updater.az.ResourceGroup)
		klog.Infof("CLB-ENECHITOAIA-locationAndNRPServiceBatchUpdater.process: locationDataResponseDTO:\n")
		logObject(locationDataResponseDTO)
		if locationDataResponseDTO == nil {
			updater.az.diffTracker.UpdateLocationsAddresses(locationData)
		} else {
			klog.Errorf("locationAndNRPServiceBatchUpdater.process: failed to update locations and addresses: %v\n", locationDataResponseDTO)
			return
		}
		klog.Infof("CLB-ENECHITOAIA-locationAndNRPServiceBatchUpdater.process: successfully updated locations and addresses\n")
	}

	// Remove services
	if serviceLoadBalancerList.Removals.Len() > 0 {
		removeServicesRequestDTO := difftracker.MapLoadBalancerUpdatesToServicesDataDTO(
			difftracker.SyncServicesReturnType{
				Additions: nil,
				Removals:  serviceLoadBalancerList.Removals,
			},
			updater.az.SubscriptionID,
			updater.az.ResourceGroup)
		klog.Infof("CLB-ENECHITOAIA-locationAndNRPServiceBatchUpdater.process: removeServicesRequestDTO: %+v\n", removeServicesRequestDTO)
		// TODO (enechitoaia): discuss about ServiceGatewayName (VnetName?)
		removeServicesResponseDTO := NRPAPIClientUpdateNRPServices(ctx, removeServicesRequestDTO, updater.az.SubscriptionID, updater.az.ResourceGroup)
		klog.Infof("CLB-ENECHITOAIA-locationAndNRPServiceBatchUpdater.process: removeServicesResponseDTO: %+v\n", removeServicesResponseDTO)
		if removeServicesResponseDTO == nil {
			updater.az.diffTracker.UpdateNRPLoadBalancers(
				difftracker.SyncServicesReturnType{
					Additions: nil,
					Removals:  serviceLoadBalancerList.Removals,
				})
		} else {
			klog.Errorf("locationAndNRPServiceBatchUpdater.process: failed to remove services: %v\n", removeServicesResponseDTO)
			return
		}

		klog.Infof("CLB-ENECHITOAIA-locationAndNRPServiceBatchUpdater.process: successfully removed services: %v\n", serviceLoadBalancerList.Removals)
	}

	klog.Infof("CLB-ENECHITOAIA-locationAndNRPServiceBatchUpdater.process END: processing batch update")
	klog.Infof("CLB-ENECHITOAIA-locationAndNRPServiceBatchUpdater.process END: difftracker:")
	logDiffTracker(updater.az.diffTracker)
}

func logDiffTracker(dt interface{}) {
	bytes, err := json.MarshalIndent(dt, "", " ")
	if err != nil {
		klog.Errorf("Failed to marshal diffTracker: %v", err)
		return
	}
	klog.Infof("CLB-ENECHITOAIA-locationAndNRPServiceBatchUpdater.process: diff tracker: %s", string(bytes))
}

func logObject(data interface{}) {
	bytes, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		klog.Errorf("Failed to marshal: %v", err)
		return
	}
	klog.Infof("CLB-ENECHITOAIA-locationAndNRPServiceBatchUpdater.process: object: %s", string(bytes))
}

func NRPAPIClientUpdateNRPLocations(
	ctx context.Context,
	locationDataRequestDTO difftracker.LocationsDataDTO,
	subscriptionId string,
	resourceGroupName string,
) error {
	klog.V(2).Infof("NRPAPIClientUpdateNRPLocations: request DTO: %+v", locationDataRequestDTO)

	// Marshal the DTO to JSON
	payload, err := json.Marshal(locationDataRequestDTO)
	if err != nil {
		return fmt.Errorf("failed to marshal request DTO: %w", err)
	}

	// Construct the in-cluster service URL
	url := fmt.Sprintf("http://nrp-bypass/subscriptions/%s/resourceGroups/%s/providers/Microsoft.Network/serviceGateways/ServiceGateway/UpdateLocations",
		subscriptionId, resourceGroupName)

	// Create the HTTP request
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewBuffer(payload))
	if err != nil {
		return fmt.Errorf("failed to create HTTP request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	// Send the request
	client := &http.Client{Timeout: 10 * time.Second}
	var resp *http.Response
	maxRetries := 3
	for i := 0; i < maxRetries; i++ {
		resp, err = client.Do(req)
		if err == nil {
			break
		}

		klog.V(2).Infof("NRPAPIClientUpdateNRPLocations: attempt %d/%d failed: %v", i+1, maxRetries, err)

		if i < maxRetries-1 {
			// Exponential backoff: 1s, 2s, 4s
			backoff := time.Duration(1<<uint(i)) * time.Second
			select {
			case <-time.After(backoff):
				// Continue to next retry
			case <-ctx.Done():
				return fmt.Errorf("context cancelled during retry: %w", ctx.Err())
			}
		}
	}
	if err != nil {
		return fmt.Errorf("failed to send HTTP request: %w", err)
	}
	defer resp.Body.Close()

	// Check for non-2xx status codes
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("received non-success status code: %d", resp.StatusCode)
	}

	klog.V(2).Infof("NRPAPIClientUpdateNRPLocations: successfully updated locations")
	return nil
}

func NRPAPIClientUpdateNRPServices(ctx context.Context, createServicesRequestDTO difftracker.ServicesDataDTO, subscriptionId string, resourceGroupName string) error {
	klog.V(2).Infof("NRPAPIClientUpdateNRPServices: request DTO: %+v", createServicesRequestDTO)

	// Marshal the DTO to JSON
	payload, err := json.Marshal(createServicesRequestDTO)
	if err != nil {
		return fmt.Errorf("failed to marshal request DTO: %w", err)
	}

	// Construct the in-cluster service URL
	// Replace with actual values or inject them via config/env
	url := fmt.Sprintf("http://nrp-bypass/subscriptions/%s/resourceGroups/%s/providers/Microsoft.Network/serviceGateways/ServiceGateway/UpdateServices",
		subscriptionId, resourceGroupName)

	// Create the HTTP request
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewBuffer(payload))
	if err != nil {
		return fmt.Errorf("failed to create HTTP request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	// Send the request
	client := &http.Client{Timeout: 10 * time.Second}

	var resp *http.Response
	maxRetries := 3
	for i := 0; i < maxRetries; i++ {
		resp, err = client.Do(req)
		if err == nil {
			break
		}

		klog.V(2).Infof("NRPAPIClientUpdateNRPServices: attempt %d/%d failed: %v", i+1, maxRetries, err)

		if i < maxRetries-1 {
			// Exponential backoff: 1s, 2s, 4s
			backoff := time.Duration(1<<uint(i)) * time.Second
			select {
			case <-time.After(backoff):
				// Continue to next retry
			case <-ctx.Done():
				return fmt.Errorf("context cancelled during retry: %w", ctx.Err())
			}
		}
	}
	if err != nil {
		return fmt.Errorf("failed to send HTTP request: %w", err)
	}
	defer resp.Body.Close()

	// Check for non-2xx status codes
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("received non-success status code: %d", resp.StatusCode)
	}

	klog.V(2).Infof("NRPAPIClientUpdateNRPServices: successfully updated services")
	return nil
}

func mergeMaps[K comparable, V any](maps ...map[K]V) map[K]V {
	result := make(map[K]V)
	for _, m := range maps {
		for k, v := range m {
			result[k] = v
		}
	}
	return result
}

func (updater *locationAndNRPServiceBatchUpdater) addOperation(operation batchOperation) batchOperation {
	// This is a no-op function. Add operation is handled via the DiffTracker APIs.
	return operation
}

func (updater *locationAndNRPServiceBatchUpdater) removeOperation(name string) {
	// This is a no-op function. Remove operation is handled via the DiffTracker APIs.
	return
}
