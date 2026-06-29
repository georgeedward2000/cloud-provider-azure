/*
Copyright 2025 The Kubernetes Authors.

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

package network

import (
	"context"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	clientset "k8s.io/client-go/kubernetes"

	"sigs.k8s.io/cloud-provider-azure/tests/e2e/utils"
)

// egressOutboundGoneErr returns nil once no Outbound service with the given name remains in the
// Service Gateway (i.e. the NAT gateway/outbound registration has been torn down).
func egressOutboundGoneErr(egressName string) error {
	resp, err := queryServiceGatewayServices()
	if err != nil {
		return fmt.Errorf("query Service Gateway services: %w", err)
	}
	for _, s := range resp.Value {
		if s.Properties.ServiceType == "Outbound" && s.Name == egressName {
			return fmt.Errorf("outbound service %s still registered", egressName)
		}
	}
	return nil
}

// Egress (NAT gateway) lifecycle edge case: draining the last egress pod must tear the NAT
// gateway down, and re-adding pods must rebuild it.
var _ = Describe("SLB - Egress Lifecycle", Label(slbTestLabel), func() {
	basename := "slb-egress-lifecycle-test"

	var (
		cs clientset.Interface
		ns *v1.Namespace
	)

	BeforeEach(func() {
		var err error
		cs, err = utils.CreateKubeClientSet()
		Expect(err).NotTo(HaveOccurred())
		ns, err = utils.CreateTestingNamespace(basename, cs)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		if cs != nil && ns != nil {
			Expect(utils.DeleteNamespace(cs, ns.Name)).To(Succeed())
			By("Waiting for Azure cleanup (egress cleanup is slower)")
			eventuallyAzureCleanup(3 * time.Minute)
			By("Verifying Service Gateway cleanup")
			verifyServiceGatewayCleanup()
			By("Verifying Address Locations cleanup")
			verifyAddressLocationsCleanup()
		}
		cs = nil
		ns = nil
	})

	makeEgressPod := func(name, egressName string, targetPort int) *v1.Pod {
		return &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns.Name, Labels: map[string]string{egressLabel: egressName}},
			Spec: v1.PodSpec{
				Containers: []v1.Container{{
					Name:            "test-app",
					Image:           utils.AgnhostImage,
					ImagePullPolicy: v1.PullIfNotPresent,
					Args:            []string{"netexec", fmt.Sprintf("--http-port=%d", targetPort)},
				}},
			},
		}
	}

	It("should tear down the NAT gateway when the last egress pod is removed and rebuild it when pods return", func() {
		const (
			numPods    = 2
			egressName = "egress-drain-gateway"
			targetPort = 8080
			waitTime   = 2 * time.Minute
		)
		egressSelector := egressLabel + "=" + egressName

		By(fmt.Sprintf("Creating %d egress pods", numPods))
		for i := 0; i < numPods; i++ {
			_, err := cs.CoreV1().Pods(ns.Name).Create(context.TODO(), makeEgressPod(fmt.Sprintf("egress-pod-%d", i), egressName, targetPort), metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())
		}
		Expect(utils.WaitPodsToBeReady(cs, ns.Name)).To(Succeed())

		By("Waiting for the NAT gateway to be provisioned and pods registered")
		eventuallyEgressRegistered(egressName, numPods, waitTime)

		By("Deleting all egress pods")
		Expect(cs.CoreV1().Pods(ns.Name).DeleteCollection(context.TODO(), metav1.DeleteOptions{},
			metav1.ListOptions{LabelSelector: egressSelector})).To(Succeed())

		By("Verifying the egress pods drain (their finalizers clear after NAT gateway teardown)")
		Eventually(func() (int, error) {
			pods, err := cs.CoreV1().Pods(ns.Name).List(context.TODO(), metav1.ListOptions{LabelSelector: egressSelector})
			if err != nil {
				return -1, err
			}
			return len(pods.Items), nil
		}, waitTime, defaultPollInterval).Should(Equal(0), "egress pods should fully delete")

		By("Verifying the outbound service is removed from the Service Gateway")
		Eventually(func() error {
			return egressOutboundGoneErr(egressName)
		}, waitTime, defaultPollInterval).Should(Succeed(), "the NAT gateway/outbound service should be torn down")

		By(fmt.Sprintf("Recreating %d egress pods", numPods))
		for i := 0; i < numPods; i++ {
			_, err := cs.CoreV1().Pods(ns.Name).Create(context.TODO(), makeEgressPod(fmt.Sprintf("egress-pod-new-%d", i), egressName, targetPort), metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())
		}
		Expect(utils.WaitPodsToBeReady(cs, ns.Name)).To(Succeed())

		By("Verifying the NAT gateway is rebuilt and pods re-registered")
		eventuallyEgressRegistered(egressName, numPods, waitTime)

		utils.Logf("\n✓ Egress NAT gateway drained to zero and rebuilt")
	})
})

// Inbound idempotency under rapid create/delete of the SAME service name: each cycle must
// provision a fresh LB under a new UID and tear it down cleanly without stranding finalizers.
var _ = Describe("SLB - Service Idempotency", Label(slbTestLabel), func() {
	basename := "slb-idempotency-test"

	var (
		cs clientset.Interface
		ns *v1.Namespace
	)

	BeforeEach(func() {
		var err error
		cs, err = utils.CreateKubeClientSet()
		Expect(err).NotTo(HaveOccurred())
		ns, err = utils.CreateTestingNamespace(basename, cs)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		if cs != nil && ns != nil {
			Expect(utils.DeleteNamespace(cs, ns.Name)).To(Succeed())
			By("Waiting for Azure cleanup")
			eventuallyAzureCleanup(2 * time.Minute)
			By("Verifying Service Gateway cleanup")
			verifyServiceGatewayCleanup()
			By("Verifying Address Locations cleanup")
			verifyAddressLocationsCleanup()
		}
		cs = nil
		ns = nil
	})

	It("should cleanly provision and tear down across rapid same-name create/delete cycles", func() {
		const (
			cycles      = 3
			numPods     = 3
			servicePort = int32(80)
			targetPort  = 8080
			waitTime    = 90 * time.Second
		)
		serviceName := "recycled-service"
		labels := map[string]string{"app": serviceName}

		By(fmt.Sprintf("Creating %d backing pods once", numPods))
		for i := 0; i < numPods; i++ {
			pod := &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("%s-pod-%d", serviceName, i), Namespace: ns.Name, Labels: labels},
				Spec: v1.PodSpec{
					Containers: []v1.Container{{
						Name:            "test-app",
						Image:           utils.AgnhostImage,
						ImagePullPolicy: v1.PullIfNotPresent,
						Args:            []string{"netexec", fmt.Sprintf("--http-port=%d", targetPort)},
					}},
				},
			}
			_, err := cs.CoreV1().Pods(ns.Name).Create(context.TODO(), pod, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())
		}
		Expect(utils.WaitPodsToBeReady(cs, ns.Name)).To(Succeed())

		service := &v1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: serviceName, Namespace: ns.Name},
			Spec: v1.ServiceSpec{
				Type:     v1.ServiceTypeLoadBalancer,
				Selector: labels,
				Ports: []v1.ServicePort{{
					Port:       servicePort,
					TargetPort: intstr.FromInt(targetPort),
					Protocol:   v1.ProtocolTCP,
				}},
			},
		}

		var lastUID string
		for c := 1; c <= cycles; c++ {
			By(fmt.Sprintf("Cycle %d/%d: creating service %s", c, cycles, serviceName))
			created, err := cs.CoreV1().Services(ns.Name).Create(context.TODO(), service, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())
			uid := string(created.UID)
			Expect(uid).NotTo(Equal(lastUID), "each recreate must get a fresh UID")
			lastUID = uid

			By(fmt.Sprintf("Cycle %d/%d: waiting for provisioning + registration", c, cycles))
			eventuallyServiceReconciled(uid, numPods, waitTime)

			By(fmt.Sprintf("Cycle %d/%d: deleting service and waiting for full teardown", c, cycles))
			Expect(cs.CoreV1().Services(ns.Name).Delete(context.TODO(), serviceName, metav1.DeleteOptions{})).To(Succeed())
			Eventually(func() error {
				// Wait for the K8s object to fully disappear (finalizers cleared) AND the SGW
				// entry to deregister, so the next cycle's create cannot race a pending delete.
				if _, getErr := cs.CoreV1().Services(ns.Name).Get(context.TODO(), serviceName, metav1.GetOptions{}); getErr == nil {
					return fmt.Errorf("service %s still exists (deletion in progress)", serviceName)
				}
				return serviceDeletedErr(uid)
			}, waitTime, defaultPollInterval).Should(Succeed(),
				"service should fully delete and deregister before the next cycle")
		}

		utils.Logf("\n✓ %d same-name create/delete cycles completed cleanly", cycles)
	})
})
