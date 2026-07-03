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
	"net/netip"
	"strings"

	discovery_v1 "k8s.io/api/discovery/v1"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"
)

// serviceUIDOfEndpointSlice returns the owning Service UID of an EndpointSlice, if any.
func serviceUIDOfEndpointSlice(es *discovery_v1.EndpointSlice) (uid string, loaded bool) {
	for _, owner := range es.ObjectMeta.OwnerReferences {
		if owner.Kind == "Service" {
			return string(owner.UID), true
		}
	}
	return "", false
}

// ReconcileNodeIPChange replays the cached EndpointSlices hosting a pod on nodeName into the diff
// tracker when the node's InternalIP set changes, or the node is added or removed. The EndpointSlice
// informer never fires for a node-only change (the slice content is unchanged), and it derives an
// endpoint's "old" location from the live (already-updated) node cache, so it can never emit the
// removal of a pod from its previous node IP. oldNodeIPs/newNodeIPs are taken from the informer's node
// objects rather than the mutable cache, so the old location is accurate: each affected pod is moved
// from its old same-family location to its new one. Empty newNodeIPs (node deleted) drains the pods;
// empty oldNodeIPs (node added) registers a pod dropped while its node was not yet cached. Egress pods
// are unaffected — they resolve their node location from pod.Status.HostIPs.
func (dt *DiffTracker) ReconcileNodeIPChange(nodeName string, oldNodeIPs, newNodeIPs []string) {
	if dt == nil || dt.endpointSlicesCache == nil || nodeName == "" {
		return
	}

	type endpointDelta struct {
		oldAddresses map[string]string
		newAddresses map[string]string
	}
	perService := make(map[string]*endpointDelta)

	dt.endpointSlicesCache.Range(func(_, value interface{}) bool {
		es, ok := value.(*discovery_v1.EndpointSlice)
		if !ok || es == nil || es.DeletionTimestamp != nil {
			return true
		}
		serviceUID, loaded := serviceUIDOfEndpointSlice(es)
		if !loaded {
			return true
		}

		// Resolve the same-family location on this node before and after the change, using the
		// deterministic picker shared with the EndpointSlice path so the keys match NRP state.
		ipv6 := es.AddressType == discovery_v1.AddressTypeIPv6
		oldLocation, oldOK := SelectSameFamilyNodeIP(oldNodeIPs, ipv6)
		newLocation, newOK := SelectSameFamilyNodeIP(newNodeIPs, ipv6)
		if !oldOK && !newOK {
			return true
		}

		for _, ep := range es.Endpoints {
			if !ptr.Deref(ep.Conditions.Ready, true) {
				continue
			}
			if !strings.EqualFold(ptr.Deref(ep.NodeName, ""), nodeName) {
				continue
			}
			delta := perService[serviceUID]
			if delta == nil {
				delta = &endpointDelta{oldAddresses: map[string]string{}, newAddresses: map[string]string{}}
				perService[serviceUID] = delta
			}
			for _, podIP := range ep.Addresses {
				addr, err := netip.ParseAddr(podIP)
				if err != nil {
					continue
				}
				if oldOK {
					delta.oldAddresses[addr.String()] = oldLocation
				}
				if newOK {
					delta.newAddresses[addr.String()] = newLocation
				}
			}
		}
		return true
	})

	for serviceUID, delta := range perService {
		if len(delta.oldAddresses) == 0 && len(delta.newAddresses) == 0 {
			continue
		}
		klog.V(2).Infof("ReconcileNodeIPChange: node %s changed, moving pod addresses for service %s (old=%d new=%d)",
			nodeName, serviceUID, len(delta.oldAddresses), len(delta.newAddresses))
		dt.UpdateEndpoints(serviceUID, delta.oldAddresses, delta.newAddresses)
	}
}
