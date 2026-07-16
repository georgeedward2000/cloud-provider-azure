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
	"context"
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"
	cloudprovider "k8s.io/cloud-provider"

	"sigs.k8s.io/cloud-provider-azure/pkg/log"
	"sigs.k8s.io/cloud-provider-azure/pkg/metrics"
	"sigs.k8s.io/cloud-provider-azure/pkg/trace"
)

var _ cloudprovider.LoadBalancer = (*Controller)(nil)

// NewLoadBalancer returns an initialized LoadBalancer backed by an existing DiffTracker.
func NewLoadBalancer(
	tracker *DiffTracker,
	eventRecorder record.EventRecorder,
) cloudprovider.LoadBalancer {
	tracker.SetEventRecorder(eventRecorder)
	return &Controller{tracker: tracker}
}

func (c *Controller) diffTracker() (*DiffTracker, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.tracker == nil {
		return nil, fmt.Errorf("ServiceGateway LoadBalancer is not initialized")
	}
	return c.tracker, nil
}

func (c *Controller) GetLoadBalancer(ctx context.Context, _ string, service *v1.Service) (status *v1.LoadBalancerStatus, exists bool, err error) {
	const operation = "GetLoadBalancer"
	ctx, span := trace.BeginReconcile(ctx, trace.DefaultTracer(), operation)
	defer func() { span.Observe(ctx, err) }()

	tracker, err := c.diffTracker()
	if err != nil {
		return nil, false, err
	}
	if !tracker.IsServiceTracked(ServiceUID(service)) {
		return nil, false, nil
	}

	log.FromContextOrBackground(ctx).WithName(operation).V(5).Info(
		"ServiceGateway service is tracked; reporting it as existing so deletion is engine-driven",
		"service", service.Name,
	)
	return service.Status.LoadBalancer.DeepCopy(), true, nil
}

func (c *Controller) GetLoadBalancerName(_ context.Context, _ string, service *v1.Service) string {
	return cloudprovider.DefaultLoadBalancerName(service)
}

func (c *Controller) EnsureLoadBalancer(ctx context.Context, clusterName string, service *v1.Service, _ []*v1.Node) (status *v1.LoadBalancerStatus, err error) {
	const operation = "EnsureLoadBalancer"
	ctx, span := trace.BeginReconcile(ctx, trace.DefaultTracer(), operation)
	defer func() { span.Observe(ctx, err) }()

	tracker, err := c.diffTracker()
	if err != nil {
		return nil, err
	}

	serviceName := fmt.Sprintf("%s/%s", service.Namespace, service.Name)
	logger := log.FromContextOrBackground(ctx).WithName(operation).WithValues("cluster", clusterName, "service", serviceName)
	metricContext := newLoadBalancerMetricContext(tracker, "ensure_loadbalancer", serviceName)
	defer func() { metricContext.ObserveOperationWithResult(err == nil) }()

	if err = tracker.ReconcileInboundService(service); err != nil {
		logger.Error(err, "Failed to reconcile ServiceGateway Service")
		return nil, err
	}

	return service.Status.LoadBalancer.DeepCopy(), nil
}

func (c *Controller) UpdateLoadBalancer(context.Context, string, *v1.Service, []*v1.Node) error {
	_, err := c.diffTracker()
	return err
}

func (c *Controller) EnsureLoadBalancerDeleted(ctx context.Context, _ string, service *v1.Service) (err error) {
	const operation = "EnsureLoadBalancerDeleted"
	ctx, span := trace.BeginReconcile(ctx, trace.DefaultTracer(), operation)
	defer func() { span.Observe(ctx, err) }()

	tracker, err := c.diffTracker()
	if err != nil {
		return err
	}

	serviceName := fmt.Sprintf("%s/%s", service.Namespace, service.Name)
	metricContext := newLoadBalancerMetricContext(tracker, "ensure_loadbalancer_deleted", serviceName)
	err = tracker.DeleteInboundService(service)
	metricContext.ObserveOperationWithResult(err == nil)
	return err
}

func newLoadBalancerMetricContext(tracker *DiffTracker, operation, serviceName string) *metrics.MetricContext {
	return metrics.NewMetricContext(
		"services",
		operation,
		tracker.config.ResourceGroup,
		tracker.config.networkResourceSubscriptionID(),
		serviceName,
	)
}
