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

package servicegateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net"
	"os"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	ginkgotypes "github.com/onsi/ginkgo/v2/types"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clientset "k8s.io/client-go/kubernetes"

	"sigs.k8s.io/cloud-provider-azure/pkg/consts"
	"sigs.k8s.io/cloud-provider-azure/tests/e2e/utils"
)

var _ = Describe("Container Load Balancer Outbound (NAT Gateway)", Label(slbTestLabel, slbEgressConfigTestLabel), func() {
	basename := "slb-egress-config"

	var (
		cs clientset.Interface
		ns *v1.Namespace
		// Set by a spec that scales the CCM down (cleanUpEgressConfigSpec).
		ccmToRestore *CCMClusterClient
	)

	BeforeEach(func() {
		cs, ns = newEgressConfigSpec(basename)
	})

	AfterEach(func() {
		cleanUpEgressConfigSpec(cs, ns, ccmToRestore)
		cs, ns, ccmToRestore = nil, nil, nil
	})

	// The pods of an egress identity configure its NAT Gateway with the
	// kubernetes.azure.com/service-egress-gateway-* annotations. Every pod votes, and the controller
	// changes the NAT Gateway, in place, only when all of them agree. Pod 0 of most identities is a
	// probe pod that records where its egress leaves; where the environment carries no egress traffic
	// a spec verifies the control plane and is reported skipped at its end, naming the egress it left out (leaveOut).
	Context("egress NAT Gateway configuration by pod consensus", func() {
		var (
			suffix string
			ipv6   bool
		)

		BeforeEach(func() {
			suffix = ns.Name[len(ns.Name)-5:]
			ipv6 = clusterHasIPv6Node(cs)
		})

		It("should create the NAT Gateway with the configuration its pods agree on and egress through its Public IPs", func() {
			egressName, defaultName := "egcfg-create-"+suffix, "egcfg-default-"+suffix

			By("Creating three pods that ask for idle timeout 10 and two managed Public IPs, and a pod of another identity without annotations")
			pods := createConfiguredEgressPods(cs, ns.Name, egressName, 3, map[string]string{
				consts.PodAnnotationServiceEgressGatewayIdleTimeout:   "10",
				consts.PodAnnotationServiceEgressGatewayPublicIPCount: "2",
			}, true)
			defaultPods := createConfiguredEgressPods(cs, ns.Name, defaultName, 1, nil, false)
			Expect(utils.WaitPodsToBeReady(cs, ns.Name)).To(Succeed())

			By("Waiting for a NAT Gateway with idle timeout 10 and the tagged Public IPs <id>-pip and <id>-pip-2")
			want := defaultEgressNATWant(egressName, ipv6)
			want.idleTimeout = 10
			want.publicIPs = egressManagedPublicIPIDs(egressName, false, 2)
			eventuallyEgressNAT(egressName, want)
			eventuallyManagedRegistered(cs, ns.Name, egressName, byoNATGatewayARMID(resourceGroupName, egressName))
			eventuallyEachPodApplied(cs, ns.Name, pods, "idle timeout 10m", "IPv4: 2 managed IPs")
			expectNoPodEvent(cs, ns.Name, pods, consts.ServiceEgressGatewayReasonInvalidConfig,
				consts.ServiceEgressGatewayReasonConfigConflict, consts.ServiceEgressGatewayReasonConfigRejected)
			expectEgressNATWritesAtMost(egressName, 1, "the NAT Gateway must be created with the agreed configuration, not created and then updated")

			By("Checking the identity without annotations got the default NAT Gateway and no Applied event")
			eventuallyEgressNAT(defaultName, defaultEgressNATWant(defaultName, ipv6))
			eventuallyManagedRegistered(cs, ns.Name, defaultName, byoNATGatewayARMID(resourceGroupName, defaultName))
			Consistently(func() bool {
				return podHasEvent(cs, ns.Name, defaultPods, consts.ServiceEgressGatewayReasonConfigApplied)
			}, 30*time.Second, 5*time.Second).Should(BeFalse(), "creating an identity with the default configuration is not announced")

			By("Checking egress leaves through the NAT Gateway's Public IPs")
			addresses := egressNATAddresses(egressName)
			first, observable := waitForProbeEgress(cs, ns.Name, pods[0], addresses)
			if observable {
				eventuallyProbeSuccesses(cs, ns.Name, pods[0], first, 10)
				expectEgressContinuity(cs, ns.Name, pods[0], first, addresses)
			}

			By("Deleting the pods: both NAT Gateways and all their Public IPs are deleted")
			deleteEgressPods(cs, ns.Name, egressName)
			deleteEgressPods(cs, ns.Name, defaultName)
			eventuallyEgressIdentityGone(egressName)
			eventuallyEgressIdentityGone(defaultName)
			leaveOutEgressUnlessObservable(observable)
		})

		It("should add and remove a managed Public IP in place while egress continues", func() {
			egressName := "egcfg-scale-" + suffix
			countKey := consts.PodAnnotationServiceEgressGatewayPublicIPCount

			By("Creating two pods without annotations: the NAT Gateway gets one managed Public IP")
			pods := createConfiguredEgressPods(cs, ns.Name, egressName, 2, nil, true)
			Expect(utils.WaitPodsToBeReady(cs, ns.Name)).To(Succeed())
			one := defaultEgressNATWant(egressName, ipv6)
			eventuallyEgressNAT(egressName, one)
			eventuallyManagedRegistered(cs, ns.Name, egressName, byoNATGatewayARMID(resourceGroupName, egressName))
			natBefore := readEgressNAT(egressName)
			firstAddress := publicIPAddress(one.publicIPs[0])
			first, observable := waitForProbeEgress(cs, ns.Name, pods[0], egressNATAddresses(egressName))

			By("Asking for two managed Public IPs on both pods: <id>-pip-2 is added to the same NAT Gateway")
			setEgressPodAnnotations(cs, ns.Name, map[string]string{countKey: "2"}, pods...)
			two := one
			two.publicIPs = egressManagedPublicIPIDs(egressName, false, 2)
			eventuallyEgressNAT(egressName, two)
			changed := 0
			if observable {
				changed = egressProbeSampleCount(cs, ns.Name, pods[0])
			}
			expectSameEgressNAT(egressName, natBefore)
			Expect(publicIPAddress(one.publicIPs[0])).To(Equal(firstAddress), "the first Public IP must keep its address")
			eventuallyEachPodApplied(cs, ns.Name, pods, "IPv4: 2 managed IPs")
			secondAddress := egressAddressesOf(two.publicIPs[1])
			both := egressNATAddresses(egressName)
			if observable {
				By("Checking egress continued through the NAT Gateway's addresses and now leaves through both Public IPs")
				eventuallyProbeSources(cs, ns.Name, pods[0], changed, egressAddressesOf(one.publicIPs[0]), secondAddress)
				expectEgressContinuity(cs, ns.Name, pods[0], first, both)
			}

			By("Removing the annotation from both pods: <id>-pip-2 is detached and deleted, the NAT Gateway kept")
			setEgressPodAnnotations(cs, ns.Name, map[string]string{countKey: ""}, pods...)
			eventuallyEgressNAT(egressName, one)
			expectSameEgressNAT(egressName, natBefore)
			eventuallyEachPodApplied(cs, ns.Name, pods, "IPv4: 1 managed IP")
			if observable {
				By("Checking egress continues and no longer leaves through the deleted Public IP")
				expectProbeSourcesGone(cs, ns.Name, pods[0], secondAddress)
				expectEgressContinuity(cs, ns.Name, pods[0], first, both)
			}

			By("Deleting the pods: the NAT Gateway and its Public IPs are deleted")
			deleteEgressPods(cs, ns.Name, egressName)
			eventuallyEgressIdentityGone(egressName)
			leaveOutEgressUnlessObservable(observable)
		})

		It("should apply annotations rolled out by a Deployment in place only once every pod has them", func() {
			const deployment = "egcfg-rollout"
			egressName := "egcfg-rollout-" + suffix
			idleKey := consts.PodAnnotationServiceEgressGatewayIdleTimeout

			By("Creating a Deployment of three pods and a probe pod, all asking for idle timeout 5")
			createEgressConfigDeployment(cs, ns.Name, deployment, egressName, 3, map[string]string{idleKey: "5"})
			probe := createConfiguredEgressPods(cs, ns.Name, egressName, 1, map[string]string{idleKey: "5"}, true)[0]
			Expect(utils.WaitPodsToBeReady(cs, ns.Name)).To(Succeed())
			before := defaultEgressNATWant(egressName, ipv6)
			before.idleTimeout = 5
			eventuallyEgressNAT(egressName, before)
			eventuallyManagedRegistered(cs, ns.Name, egressName, byoNATGatewayARMID(resourceGroupName, egressName))
			natBefore := readEgressNAT(egressName)
			addresses := egressNATAddresses(egressName)
			first, observable := waitForProbeEgress(cs, ns.Name, probe, addresses)

			By("Moving the probe pod to idle timeout 15 first: it now disagrees with the Deployment and is told so")
			setEgressPodAnnotations(cs, ns.Name, map[string]string{idleKey: "15"}, probe)
			eventuallyPodEvent(cs, ns.Name, []string{probe}, consts.ServiceEgressGatewayReasonConfigConflict, v1.EventTypeWarning)

			By("Rolling the Deployment out to idle timeout 15")
			setDeploymentPodAnnotations(cs, ns.Name, deployment, map[string]string{idleKey: "15"})

			By("Watching the rollout: idle timeout 15 may only be applied once no pod asks for 5 any more")
			after := before
			after.idleTimeout = 15
			Eventually(func() error {
				asking, err := livePodsAnnotated(cs, ns.Name, egressName, idleKey, "5")
				if err != nil {
					return err
				}
				nat, found, err := getAzureNetworkResource(byoNATGatewayARMID(resourceGroupName, egressName))
				if err != nil {
					return err
				}
				if !found {
					return fmt.Errorf("NAT Gateway %s does not exist", egressName)
				}
				stillAsking, err := livePodsAnnotated(cs, ns.Name, egressName, idleKey, "5")
				if err != nil {
					return err
				}
				// A pod that asked for 5 both before and after the read was a member throughout: the
				// controller cannot have had an agreement on 15 while the NAT Gateway was read.
				if egressIdleTimeout(nat) == 15 {
					for uid, name := range asking {
						if _, ok := stillAsking[uid]; ok {
							Fail(fmt.Sprintf("the NAT Gateway got idle timeout 15 while pod %s still asked for 5", name))
						}
					}
				}
				if len(stillAsking) > 0 {
					return fmt.Errorf("%d pod(s) still ask for idle timeout 5", len(stillAsking))
				}
				return egressNATShapeErr(egressName, after)
			}, 10*time.Minute, 3*time.Second).Should(Succeed(), "the rolled-out configuration must be applied once every pod has it")
			expectSameEgressNAT(egressName, natBefore)
			expectEgressNATWritesAtMost(egressName, 2, "the NAT Gateway must be created and then updated once, for the rollout")
			rolledOut, err := livePodsAnnotated(cs, ns.Name, egressName, idleKey, "15")
			Expect(err).NotTo(HaveOccurred())
			members := slices.Sorted(maps.Values(rolledOut))
			Expect(members).To(HaveLen(4), "the three rolled-out pods and the probe pod")
			eventuallyEachPodApplied(cs, ns.Name, members, fmt.Sprintf("Egress identity %q", egressName), "idle timeout 15m")
			By("Checking every rolled-out pod was told it disagreed while pods of the old revision were running")
			eventuallyEachPodEvent(cs, ns.Name, slices.DeleteFunc(slices.Clone(members), func(name string) bool { return name == probe }),
				consts.ServiceEgressGatewayReasonConfigConflict, v1.EventTypeWarning, "idle timeout 15m")
			if observable {
				By("Checking the probe pod's egress continued through the NAT Gateway during the rollout")
				expectEgressContinuity(cs, ns.Name, probe, first, addresses)
			}

			By("Deleting the Deployment and the probe pod: the NAT Gateway and its Public IPs are deleted")
			foreground := metav1.DeletePropagationForeground
			Expect(cs.AppsV1().Deployments(ns.Name).Delete(context.TODO(), deployment,
				metav1.DeleteOptions{PropagationPolicy: &foreground})).To(Succeed())
			deleteEgressPod(cs, ns.Name, probe)
			eventuallyEgressIdentityGone(egressName)
			leaveOutEgressUnlessObservable(observable)
		})

		It("should keep the NAT Gateway while its pods disagree and apply the configuration once they agree", func() {
			egressName := "egcfg-dissent-" + suffix
			idleKey := consts.PodAnnotationServiceEgressGatewayIdleTimeout

			By("Creating three pods without annotations: the NAT Gateway gets the default configuration")
			pods := createConfiguredEgressPods(cs, ns.Name, egressName, 3, nil, false)
			Expect(utils.WaitPodsToBeReady(cs, ns.Name)).To(Succeed())
			defaults := defaultEgressNATWant(egressName, ipv6)
			eventuallyEgressNAT(egressName, defaults)
			eventuallyManagedRegistered(cs, ns.Name, egressName, byoNATGatewayARMID(resourceGroupName, egressName))
			natBefore := readEgressNAT(egressName)
			etags := egressResourceEtags(egressName)

			By("Asking for idle timeout 20 on two of the three pods: they are told they disagree, the NAT Gateway is kept")
			setEgressPodAnnotations(cs, ns.Name, map[string]string{idleKey: "20"}, pods[0], pods[1])
			eventuallyEachPodEvent(cs, ns.Name, pods[:2], consts.ServiceEgressGatewayReasonConfigConflict, v1.EventTypeWarning, "idle timeout 20m")
			Consistently(func() error {
				return egressEtagsUnchangedErr(etags)
			}, time.Minute, defaultPollInterval).Should(Succeed(), "a configuration not every pod asks for must not be written")
			expectNoPodEvent(cs, ns.Name, pods[2:], consts.ServiceEgressGatewayReasonConfigConflict)
			expectNoPodEvent(cs, ns.Name, pods, consts.ServiceEgressGatewayReasonConfigApplied)

			By("Asking for idle timeout 20 on the third pod too: the configuration is applied in place")
			setEgressPodAnnotations(cs, ns.Name, map[string]string{idleKey: "20"}, pods[2])
			agreed := defaults
			agreed.idleTimeout = 20
			eventuallyEgressNAT(egressName, agreed)
			expectSameEgressNAT(egressName, natBefore)
			eventuallyEachPodApplied(cs, ns.Name, pods, "idle timeout 20m")

			By("Deleting the pods: the NAT Gateway and its Public IPs are deleted")
			deleteEgressPods(cs, ns.Name, egressName)
			eventuallyEgressIdentityGone(egressName)
		})

		It("should keep the NAT Gateway and the pods' egress while a pod's configuration is invalid", func() {
			egressName := "egcfg-invalid-" + suffix
			idleKey := consts.PodAnnotationServiceEgressGatewayIdleTimeout

			By("Creating two pods without annotations: the NAT Gateway gets the default configuration")
			pods := createConfiguredEgressPods(cs, ns.Name, egressName, 2, nil, true)
			Expect(utils.WaitPodsToBeReady(cs, ns.Name)).To(Succeed())
			defaults := defaultEgressNATWant(egressName, ipv6)
			eventuallyEgressNAT(egressName, defaults)
			eventuallyManagedRegistered(cs, ns.Name, egressName, byoNATGatewayARMID(resourceGroupName, egressName))
			natBefore := readEgressNAT(egressName)
			addresses := egressNATAddresses(egressName)
			first, observable := waitForProbeEgress(cs, ns.Name, pods[0], addresses)
			etags := egressResourceEtags(egressName)

			By("Giving the probe pod an out-of-range idle timeout and the other pod idle timeout 12")
			setEgressPodAnnotations(cs, ns.Name, map[string]string{idleKey: "1000"}, pods[0])
			setEgressPodAnnotations(cs, ns.Name, map[string]string{idleKey: "12"}, pods[1])

			By("The invalid pod is told, keeps its egress registration, and blocks the change")
			eventuallyPodEvent(cs, ns.Name, pods[:1], consts.ServiceEgressGatewayReasonInvalidConfig, v1.EventTypeWarning,
				`idle-timeout: "1000" is not an integer from 4 to 120`)
			eventuallyPodEvent(cs, ns.Name, pods[1:], consts.ServiceEgressGatewayReasonConfigConflict, v1.EventTypeWarning, "idle timeout 12m")
			Consistently(func() error {
				if err := egressEtagsUnchangedErr(etags); err != nil {
					return err
				}
				want, err := livePodIPsWithLabel(cs, ns.Name, egressLabel, egressName)
				if err != nil {
					return err
				}
				return egressRegisteredMatchErr(egressName, want)
			}, time.Minute, defaultPollInterval).Should(Succeed(), "an invalid vote blocks the change, and the pod keeps its egress")
			expectNoPodEvent(cs, ns.Name, pods[1:], consts.ServiceEgressGatewayReasonInvalidConfig)
			expectNoPodEvent(cs, ns.Name, pods[:1], consts.ServiceEgressGatewayReasonConfigConflict)
			if observable {
				expectEgressContinuity(cs, ns.Name, pods[0], first, addresses)
			}

			By("Fixing the invalid annotation: the agreed idle timeout 12 is applied in place")
			setEgressPodAnnotations(cs, ns.Name, map[string]string{idleKey: "12"}, pods[0])
			agreed := defaults
			agreed.idleTimeout = 12
			eventuallyEgressNAT(egressName, agreed)
			expectSameEgressNAT(egressName, natBefore)
			eventuallyEachPodApplied(cs, ns.Name, pods, "idle timeout 12m")
			if observable {
				expectEgressContinuity(cs, ns.Name, pods[0], first, addresses)
			}

			By("Deleting the pods: the NAT Gateway and its Public IPs are deleted")
			deleteEgressPods(cs, ns.Name, egressName)
			eventuallyEgressIdentityGone(egressName)
			leaveOutEgressUnlessObservable(observable)
		})

		It("should egress through a user Public IP prefix and leave the prefix in place when the identity is deleted", func() {
			egressName := "egcfg-uprefix-" + suffix

			By("Creating StandardV2 Public IP prefixes in a test resource group of the cluster's subscription")
			rg, granted := createEgressUserResourceGroup(suffix)
			// A /30 counts as 2 addresses on the pod and 4 once its real size is read: the configuration stays valid.
			prefix := createBYOPublicIPPrefix(rg, "egcfg-prefix", "IPv4", 30)
			annotations := map[string]string{consts.PodAnnotationServiceEgressGatewayPublicIPPrefixIDs: prefix}
			want := egressNATWant{idleTimeout: consts.ServiceEgressGatewayIdleTimeoutDefaultMinutes, prefixes: []string{prefix}}
			if ipv6 {
				prefixV6 := createBYOPublicIPPrefix(rg, "egcfg-prefix-v6", "IPv6", 127)
				annotations[consts.PodAnnotationServiceEgressGatewayPublicIPPrefixIDsIPv6] = prefixV6
				want.prefixesV6 = []string{prefixV6}
			}
			userPrefixes := append(slices.Clone(want.prefixes), want.prefixesV6...)
			configurations := userEgressResourceConfigurations(userPrefixes...)

			By("Creating pods that ask for the prefixes: the NAT Gateway uses them and no managed Public IP")
			pods := createConfiguredEgressPods(cs, ns.Name, egressName, 2, annotations, true)
			Expect(utils.WaitPodsToBeReady(cs, ns.Name)).To(Succeed())
			eventuallyEgressNATWithUserResources(cs, ns.Name, pods, egressName, want, rg, granted)
			eventuallyManagedRegistered(cs, ns.Name, egressName, byoNATGatewayARMID(resourceGroupName, egressName))
			eventuallyEachPodAppliedWithUserResources(cs, ns.Name, pods, "user prefixes (egcfg-prefix")
			expectUserEgressResourcesUnchanged(configurations)

			By("Checking egress leaves through the prefix's addresses")
			addresses := egressNATAddresses(egressName)
			first, observable := waitForProbeEgress(cs, ns.Name, pods[0], addresses)
			if observable {
				eventuallyProbeSuccesses(cs, ns.Name, pods[0], first, 10)
				expectEgressContinuity(cs, ns.Name, pods[0], first, addresses)
			}

			By("Deleting the pods: the NAT Gateway is deleted and the prefixes are kept, attached to nothing")
			deleteEgressPods(cs, ns.Name, egressName)
			eventuallyEgressIdentityGone(egressName)
			expectUserEgressResourcesReleased(userPrefixes...)
			expectUserEgressResourcesUnchanged(configurations)
			leaveOutEgressUnlessObservable(observable)
		})

		It("should egress through a user Public IP and keep it across a controller restart and the identity's deletion",
			Label(slbCrashTestLabel, slbCCMRestartTestLabel), func() {
				egressName := "egcfg-upip-" + suffix

				By("Creating a StandardV2 Public IP in a test resource group of the cluster's subscription")
				rg, granted := createEgressUserResourceGroup(suffix)
				userPublicIP := createBYOPublicIP(rg, "egcfg-pip", "IPv4")
				userAddress := publicIPAddress(userPublicIP)
				configurations := userEgressResourceConfigurations(userPublicIP)

				By("Creating pods that ask for it: the NAT Gateway uses it instead of a managed IPv4 Public IP")
				pods := createConfiguredEgressPods(cs, ns.Name, egressName, 2,
					map[string]string{consts.PodAnnotationServiceEgressGatewayPublicIPIDs: userPublicIP}, true)
				Expect(utils.WaitPodsToBeReady(cs, ns.Name)).To(Succeed())
				want := defaultEgressNATWant(egressName, ipv6)
				want.publicIPs = []string{userPublicIP}
				eventuallyEgressNATWithUserResources(cs, ns.Name, pods, egressName, want, rg, granted)
				eventuallyManagedRegistered(cs, ns.Name, egressName, byoNATGatewayARMID(resourceGroupName, egressName))
				eventuallyEachPodAppliedWithUserResources(cs, ns.Name, pods, "user IPs (egcfg-pip)")
				expectUserEgressResourcesUnchanged(configurations)
				addresses := egressAddressesOf(userPublicIP)
				first, observable := waitForProbeEgress(cs, ns.Name, pods[0], addresses)

				restarted := IsCCMClusterConfigured()
				if restarted {
					By("Restarting the cloud-controller-manager: the NAT Gateway keeps the user Public IP, which keeps its address")
					ccmClient, err := NewCCMClusterClient()
					Expect(err).NotTo(HaveOccurred())
					etags := egressResourceEtags(egressName)
					maps.Copy(etags, azureResourceEtags(userPublicIP))
					expectNoCCMCrash(egressSpecStart)
					Expect(ccmClient.CrashCCMAndWaitForRecovery(context.TODO(), GetCCMRecoveryTimeout())).To(Succeed())
					Consistently(func() error {
						if err := egressEtagsUnchangedErr(etags); err != nil {
							return err
						}
						current, err := egressAddressesOfErr(userPublicIP)
						if err != nil {
							return err
						}
						if !current.contains(userAddress) {
							return fmt.Errorf("the user Public IP's address changed from %s to %v", userAddress, current)
						}
						return nil
					}, egressRestartObservation, defaultPollInterval).Should(Succeed())
					if observable {
						expectEgressContinuity(cs, ns.Name, pods[0], first, addresses)
					}
				}

				By("Deleting the pods: the NAT Gateway is deleted and the user Public IP kept with its address, attached to nothing")
				deleteEgressPods(cs, ns.Name, egressName)
				eventuallyEgressIdentityGone(egressName)
				expectUserEgressResourcesReleased(userPublicIP)
				expectUserEgressResourcesUnchanged(configurations)
				if !restarted {
					leaveOut("the user Public IP across a restart of the cloud-controller-manager (CCM cluster access is not configured)")
				}
				leaveOutEgressUnlessObservable(observable)
			})

		It("should create a managed Public IP prefix, resize it in place while egress continues and delete it with the identity", func() {
			egressName := "egcfg-mprefix-" + suffix
			lengthKey := consts.PodAnnotationServiceEgressGatewayPublicIPPrefixLength

			By("Creating pods that ask for a managed /31 prefix: <id>-prefix-31 replaces the managed IPv4 Public IP")
			pods := createConfiguredEgressPods(cs, ns.Name, egressName, 2, map[string]string{lengthKey: "31"}, true)
			Expect(utils.WaitPodsToBeReady(cs, ns.Name)).To(Succeed())
			slash31 := defaultEgressNATWant(egressName, ipv6)
			slash31.publicIPs = nil
			slash31.prefixes = []string{egressManagedPrefixID(egressName, false, 31)}
			eventuallyEgressNAT(egressName, slash31)
			eventuallyManagedRegistered(cs, ns.Name, egressName, byoNATGatewayARMID(resourceGroupName, egressName))
			eventuallyEachPodApplied(cs, ns.Name, pods, "managed prefix /31")
			natBefore := readEgressNAT(egressName)
			oldPrefix := egressAddressesOf(slash31.prefixes[0])
			first, observable := waitForProbeEgress(cs, ns.Name, pods[0], egressNATAddresses(egressName))

			By("Asking for a /30 prefix: <id>-prefix-30 replaces <id>-prefix-31 on the same NAT Gateway")
			setEgressPodAnnotations(cs, ns.Name, map[string]string{lengthKey: "30"}, pods...)
			slash30 := slash31
			slash30.prefixes = []string{egressManagedPrefixID(egressName, false, 30)}
			Eventually(func() error {
				if nat := readEgressNAT(egressName); len(nat.Properties.PublicIPPrefixes) == 0 {
					Fail(fmt.Sprintf("NAT Gateway %s referenced no IPv4 prefix during the swap: its IPv4 egress had no address", egressName))
				}
				return egressNATShapeErr(egressName, slash30)
			}, egressApplyTimeout, 2*time.Second).Should(Succeed(), "the /30 prefix must replace the /31 one in place")
			expectSameEgressNAT(egressName, natBefore)
			eventuallyEachPodApplied(cs, ns.Name, pods, "managed prefix /30")
			newPrefix := egressAddressesOf(slash30.prefixes[0])
			if observable {
				By("Checking egress continued, moved to the /30 prefix and no longer leaves through the /31 one")
				eventuallyProbeSources(cs, ns.Name, pods[0], first, newPrefix)
				expectProbeSourcesGone(cs, ns.Name, pods[0], oldPrefix)
				expectEgressContinuity(cs, ns.Name, pods[0], first, oldPrefix.union(newPrefix))
			}

			By("Deleting the pods: the NAT Gateway and <id>-prefix-30 are deleted")
			deleteEgressPods(cs, ns.Name, egressName)
			eventuallyEgressIdentityGone(egressName)
			leaveOutEgressUnlessObservable(observable)
		})

		It("should add the configured tags to the NAT Gateway, its Public IPs and prefix and keep tags set by others", func() {
			egressName := "egcfg-tags-" + suffix
			tagsKey := consts.PodAnnotationServiceEgressGatewayTags

			By("Creating pods that ask for a managed Public IP, a managed /31 prefix and two tags")
			pods := createConfiguredEgressPods(cs, ns.Name, egressName, 2, map[string]string{
				consts.PodAnnotationServiceEgressGatewayPublicIPCount:        "1",
				consts.PodAnnotationServiceEgressGatewayPublicIPPrefixLength: "31",
				tagsKey: "e2e-team=payments,e2e-env=test",
			}, false)
			Expect(utils.WaitPodsToBeReady(cs, ns.Name)).To(Succeed())
			want := defaultEgressNATWant(egressName, ipv6)
			want.prefixes = []string{egressManagedPrefixID(egressName, false, 31)}
			want.tags = map[string]string{"e2e-team": "payments", "e2e-env": "test"}
			eventuallyEgressNAT(egressName, want)
			eventuallyManagedRegistered(cs, ns.Name, egressName, byoNATGatewayARMID(resourceGroupName, egressName))

			By("Adding a tag to the NAT Gateway, its Public IPs and its prefix out of band")
			natGatewayID := byoNATGatewayARMID(resourceGroupName, egressName)
			foreignTagged := append([]string{natGatewayID}, want.publicIPs...)
			foreignTagged = append(append(foreignTagged, want.publicIPsV6...), want.prefixes...)
			for _, id := range foreignTagged {
				setNetworkResourceTag(id, "e2e-foreign", "kept")
			}
			natBefore := readEgressNAT(egressName)

			By("Changing the configured tags: e2e-env is updated everywhere, e2e-team is not removed, the foreign tags are kept")
			setEgressPodAnnotations(cs, ns.Name, map[string]string{tagsKey: "e2e-env=prod"}, pods...)
			want.tags = map[string]string{"e2e-team": "payments", "e2e-env": "prod"}
			eventuallyEgressNAT(egressName, want)
			expectSameEgressNAT(egressName, natBefore)
			eventuallyEachPodApplied(cs, ns.Name, pods, "tags: e2e-env=prod")
			for _, id := range foreignTagged {
				Expect(tagValue(azureResourceTags(id), "e2e-foreign")).To(Equal("kept"),
					"a tag someone else added to %s must survive the controller's update", resourceNameFromID(id))
			}

			By("Deleting the pods: the NAT Gateway, its Public IPs and its prefix are deleted")
			deleteEgressPods(cs, ns.Name, egressName)
			eventuallyEgressIdentityGone(egressName)
		})

		It("should not update a satisfied NAT Gateway across a controller restart and apply an agreement reached while it was down",
			Label(slbInitTestLabel, slbCrashTestLabel, slbCCMRestartTestLabel), func() {
				ccmClient := ccmClientOrSkip("restarts and stops the cloud-controller-manager")
				egressName := "egcfg-restart-" + suffix
				idleKey := consts.PodAnnotationServiceEgressGatewayIdleTimeout

				By("Creating pods that ask for idle timeout 8 and two managed Public IPs")
				pods := createConfiguredEgressPods(cs, ns.Name, egressName, 2, map[string]string{
					idleKey: "8",
					consts.PodAnnotationServiceEgressGatewayPublicIPCount: "2",
				}, false)
				Expect(utils.WaitPodsToBeReady(cs, ns.Name)).To(Succeed())
				want := defaultEgressNATWant(egressName, ipv6)
				want.idleTimeout = 8
				want.publicIPs = egressManagedPublicIPIDs(egressName, false, 2)
				eventuallyEgressNAT(egressName, want)
				eventuallyManagedRegistered(cs, ns.Name, egressName, byoNATGatewayARMID(resourceGroupName, egressName))
				natBefore := readEgressNAT(egressName)
				etags := egressResourceEtags(egressName)
				Consistently(func() error { return egressEtagsUnchangedErr(etags) }, 30*time.Second, defaultPollInterval).Should(Succeed(),
					"precondition: the controller has finished writing the identity's resources")
				events := egressConfigEventCounts(cs, ns.Name)

				By("Restarting the cloud-controller-manager: nothing is written to the satisfied resources, nothing announced")
				expectNoCCMCrash(egressSpecStart)
				Expect(ccmClient.CrashCCMAndWaitForRecovery(context.TODO(), GetCCMRecoveryTimeout())).To(Succeed())
				Consistently(func() error { return egressEtagsUnchangedErr(etags) }, egressRestartObservation, defaultPollInterval).Should(Succeed(),
					"a restarted controller must not write a NAT Gateway or Public IP that already has the agreed configuration")
				Expect(egressConfigEventCounts(cs, ns.Name)).To(Equal(events), "a restart must not announce an unchanged configuration again")
				if writes, ok := ccmEgressWrites(ccmClient, egressName, "the restarted controller's writes to "+egressName); ok {
					Expect(writes).To(BeEmpty(), "a restarted controller must not write the satisfied resources, not even without changing them")
				}

				By("Stopping the cloud-controller-manager and moving every pod to idle timeout 16 while it is down")
				ccmToRestore = ccmClient
				expectNoCCMCrash(egressSpecStart)
				Expect(scaleCCMDeployment(context.TODO(), ccmClient, 0)).To(Succeed())
				Expect(waitForCCMFullyDown(context.TODO(), ccmClient, GetCCMRecoveryTimeout())).To(Succeed())
				setEgressPodAnnotations(cs, ns.Name, map[string]string{idleKey: "16"}, pods...)
				Consistently(func() error { return egressEtagsUnchangedErr(etags) }, 30*time.Second, defaultPollInterval).Should(Succeed(),
					"nothing may change the NAT Gateway while the controller is down")

				By("Starting the cloud-controller-manager: the agreement is applied in place and announced")
				Expect(scaleCCMDeployment(context.TODO(), ccmClient, 1)).To(Succeed())
				Expect(waitForCCMFullyUp(context.TODO(), ccmClient, GetCCMRecoveryTimeout())).To(Succeed())
				ccmToRestore = nil
				want.idleTimeout = 16
				eventuallyEgressNAT(egressName, want)
				expectSameEgressNAT(egressName, natBefore)
				eventuallyEachPodApplied(cs, ns.Name, pods, "idle timeout 16m")
				expectCCMEgressWrites(ccmClient, egressName, "Creating or updating NAT Gateway")

				By("Deleting the pods: the NAT Gateway and its Public IPs are deleted")
				deleteEgressPods(cs, ns.Name, egressName)
				eventuallyEgressIdentityGone(egressName)
			})

		It("should create an identity whose pods disagree at start-up with the configuration of its oldest pod",
			Label(slbInitTestLabel, slbCCMRestartTestLabel), func() {
				ccmClient := ccmClientOrSkip("stops the cloud-controller-manager")
				egressName := "egcfg-oldest-" + suffix
				idleKey := consts.PodAnnotationServiceEgressGatewayIdleTimeout

				By("Stopping the cloud-controller-manager")
				ccmToRestore = ccmClient
				expectNoCCMCrash(egressSpecStart)
				Expect(scaleCCMDeployment(context.TODO(), ccmClient, 0)).To(Succeed())
				Expect(waitForCCMFullyDown(context.TODO(), ccmClient, GetCCMRecoveryTimeout())).To(Succeed())

				By("Creating three pods that ask for idle timeouts 9, 11 and 13, oldest first and numbered 2, 1 and 0, so that their " +
					"names cannot stand in for their creation times, and a fourth, newest pod whose idle timeout 1000 is invalid")
				var pods []string
				for i, idle := range []string{"9", "11", "13", "1000"} {
					if i > 0 {
						time.Sleep(2 * time.Second) // creation timestamps have a resolution of one second
					}
					index := 2 - i
					if i == 3 {
						index = 3
					}
					pods = append(pods, createEgressConfigPod(cs, newEgressConfigPod(ns.Name, egressName, index, map[string]string{idleKey: idle}, false)))
				}
				Expect(utils.WaitPodsToBeReady(cs, ns.Name)).To(Succeed())

				By("Starting the cloud-controller-manager: the identity is created with the oldest pod's idle timeout 9")
				Expect(scaleCCMDeployment(context.TODO(), ccmClient, 1)).To(Succeed())
				Expect(waitForCCMFullyUp(context.TODO(), ccmClient, GetCCMRecoveryTimeout())).To(Succeed())
				ccmToRestore = nil
				want := defaultEgressNATWant(egressName, ipv6)
				want.idleTimeout = 9
				eventuallyEgressNAT(egressName, want)
				eventuallyManagedRegistered(cs, ns.Name, egressName, byoNATGatewayARMID(resourceGroupName, egressName))
				eventuallyEachPodApplied(cs, ns.Name, pods, "idle timeout 9m")
				expectEgressNATWritesAtMost(egressName, 1, "the NAT Gateway must be created with the oldest pod's configuration, not created and then updated")
				// Without an agreement the identity keeps the configuration it was created with.
				expectEgressUntouched(egressName, want, egressResourceEtags(egressName), time.Minute)
				// The votes found at start-up, the invalid one too, are taken without telling the pods.
				expectNoPodEvent(cs, ns.Name, pods, consts.ServiceEgressGatewayReasonConfigConflict, consts.ServiceEgressGatewayReasonInvalidConfig)

				By("Deleting the pods: the NAT Gateway and its Public IPs are deleted")
				deleteEgressPods(cs, ns.Name, egressName)
				eventuallyEgressIdentityGone(egressName)
			})

		It("should give a dual-stack identity the number of managed IPv6 Public IPs its pods ask for", func() {
			if !ipv6 {
				Skip("no node exposes an IPv6 InternalIP; egress identities serve IPv6 on dual-stack clusters only")
			}
			egressName := "egcfg-v6count-" + suffix
			countKey := consts.PodAnnotationServiceEgressGatewayPublicIPCountIPv6

			By("Creating pods that ask for two managed IPv6 Public IPs")
			pods := createConfiguredEgressPods(cs, ns.Name, egressName, 2, map[string]string{countKey: "2"}, false)
			Expect(utils.WaitPodsToBeReady(cs, ns.Name)).To(Succeed())
			two := defaultEgressNATWant(egressName, true)
			two.publicIPsV6 = egressManagedPublicIPIDs(egressName, true, 2)
			eventuallyEgressNAT(egressName, two)
			eventuallyManagedRegistered(cs, ns.Name, egressName, byoNATGatewayARMID(resourceGroupName, egressName))
			eventuallyEachPodApplied(cs, ns.Name, pods, "IPv6: 2 managed IPs")
			natBefore := readEgressNAT(egressName)
			firstV6 := publicIPAddress(two.publicIPsV6[0])

			By("Checking IPv6 egress leaves through the NAT Gateway where the environment carries it")
			observeIPv6Egress(ns.Name, pods[0], egressNATAddresses(egressName))

			By("Removing the annotation: <id>-pip-v6-2 is detached and deleted, <id>-pip-v6 keeps its address")
			setEgressPodAnnotations(cs, ns.Name, map[string]string{countKey: ""}, pods...)
			eventuallyEgressNAT(egressName, defaultEgressNATWant(egressName, true))
			expectSameEgressNAT(egressName, natBefore)
			Expect(publicIPAddress(two.publicIPsV6[0])).To(Equal(firstV6), "the first IPv6 Public IP must keep its address")
			eventuallyEachPodApplied(cs, ns.Name, pods, "IPv6: 1 managed IP")

			By("Deleting the pods: the NAT Gateway and its Public IPs are deleted")
			deleteEgressPods(cs, ns.Name, egressName)
			eventuallyEgressIdentityGone(egressName)
		})

		It("should count the vote of a partial dual-stack member", func() {
			if !ipv6 {
				Skip("no node exposes an IPv6 InternalIP; there are no partial dual-stack members on a single-stack cluster")
			}
			node := singleStackNode(cs)
			if node == "" {
				Skip("every schedulable node exposes both IP families, so no pod can be a partial dual-stack member " +
					"(a member with an address of one family only)")
			}
			dual := dualStackNode(cs)
			if dual == "" {
				Skip("no schedulable node exposes both IP families, so the identity can have no dual-stack member besides the partial one")
			}
			egressName := "egcfg-partial-" + suffix
			countKey := consts.PodAnnotationServiceEgressGatewayPublicIPCountIPv6
			twoV6 := map[string]string{countKey: "2"}

			By(fmt.Sprintf("Creating a pod on single-stack node %s and a pod on dual-stack node %s, both asking for two IPv6 Public IPs", node, dual))
			partial := newEgressConfigPod(ns.Name, egressName, 0, twoV6, false)
			partial.Spec.NodeSelector = map[string]string{v1.LabelHostname: node}
			full := newEgressConfigPod(ns.Name, egressName, 1, twoV6, false)
			full.Spec.NodeSelector = map[string]string{v1.LabelHostname: dual}
			pods := []string{createEgressConfigPod(cs, partial), createEgressConfigPod(cs, full)}
			Expect(utils.WaitPodsToBeReady(cs, ns.Name)).To(Succeed())
			want := map[string]struct{}{}
			accepted := make([][]string, len(pods))
			for i, name := range pods {
				pod, err := cs.CoreV1().Pods(ns.Name).Get(context.TODO(), name, metav1.GetOptions{})
				Expect(err).NotTo(HaveOccurred())
				accepted[i] = acceptedEgressAddresses(pod)
				for _, address := range accepted[i] {
					want[address] = struct{}{}
				}
			}
			Expect(accepted[0]).To(HaveLen(1), "a pod on single-stack node %s is a member with one address", node)
			if len(accepted[1]) < 2 {
				Skip(fmt.Sprintf("pod %s on dual-stack node %s has addresses %v: pods here do not get an address of each family", pods[1], dual, accepted[1]))
			}

			By("The partial member registers only the address of its node's family, and its IPv6 annotation is valid")
			two := defaultEgressNATWant(egressName, true)
			two.publicIPsV6 = egressManagedPublicIPIDs(egressName, true, 2)
			eventuallyEgressNAT(egressName, two)
			Eventually(func() error {
				return egressRegisteredMatchErr(egressName, want)
			}, 3*time.Minute, defaultPollInterval).Should(Succeed())
			expectNoPodEvent(cs, ns.Name, pods, consts.ServiceEgressGatewayReasonInvalidConfig)

			By("Moving only the other pod back to one IPv6 Public IP: the partial member's vote blocks the change")
			etags := egressResourceEtags(egressName)
			setEgressPodAnnotations(cs, ns.Name, map[string]string{countKey: ""}, pods[1])
			eventuallyPodEvent(cs, ns.Name, pods[1:], consts.ServiceEgressGatewayReasonConfigConflict, v1.EventTypeWarning, "IPv6: 1 managed IP")
			Consistently(func() error {
				return egressEtagsUnchangedErr(etags)
			}, time.Minute, defaultPollInterval).Should(Succeed(), "the partial member is a member: without its vote there is no agreement")

			By("Moving the partial member too: the change is applied and <id>-pip-v6-2 deleted")
			setEgressPodAnnotations(cs, ns.Name, map[string]string{countKey: ""}, pods[0])
			eventuallyEgressNAT(egressName, defaultEgressNATWant(egressName, true))
			eventuallyEachPodApplied(cs, ns.Name, pods, "IPv6: 1 managed IP")

			By("Deleting the pods: the NAT Gateway and its Public IPs are deleted")
			deleteEgressPods(cs, ns.Name, egressName)
			eventuallyEgressIdentityGone(egressName)
		})

		It("should reject egress configuration on a BYO NAT Gateway identity and leave the NAT Gateway untouched", func() {
			byoRG := byoNATGatewayResourceGroup()
			egressName := "byo-egcfg-" + suffix

			By("Creating a BYO NAT Gateway and egress pods that name it and set the default idle timeout explicitly")
			addresses := createBYOAddresses(cs, byoRG, egressName)
			natGatewayID := createBYONATGateway(byoRG, egressName, addresses)
			configuration, err := natGatewayConfiguration(natGatewayID)
			Expect(err).NotTo(HaveOccurred())
			pods := createConfiguredEgressPods(cs, ns.Name, egressName, 2,
				map[string]string{consts.PodAnnotationServiceEgressGatewayIdleTimeout: "4"}, false)
			Expect(utils.WaitPodsToBeReady(cs, ns.Name)).To(Succeed())
			eventuallyBYORegistered(cs, ns.Name, byoRG, egressName, natGatewayID, 3*time.Minute)

			By("Every pod is told a BYO NAT Gateway accepts no egress configuration")
			for _, pod := range pods {
				eventuallyPodEvent(cs, ns.Name, []string{pod}, consts.ServiceEgressGatewayReasonInvalidConfig, v1.EventTypeWarning,
					"accepts no egress configuration")
			}

			By("The BYO NAT Gateway keeps the configuration it was created with and no managed resource is created")
			Consistently(func() (string, error) {
				return natGatewayConfiguration(natGatewayID)
			}, 90*time.Second, defaultPollInterval).Should(Equal(configuration), "the controller must never configure a BYO NAT Gateway")
			expectNoManagedNATGateway(egressName)
			Expect(egressReservedResourcesErr(egressName, nil, nil)).To(Succeed(), "no managed Public IP or prefix may be created for a BYO identity")
			expectNoPodEvent(cs, ns.Name, pods, consts.ServiceEgressGatewayReasonConfigApplied)

			By("Deleting the pods: the BYO NAT Gateway is only unlinked")
			deleteEgressPods(cs, ns.Name, egressName)
			eventuallyBYOUnlinked(egressName, natGatewayID)
			expectBYOResourcesExist(natGatewayID, addresses)
		})
	})
})

// Helpers of the egress NAT Gateway configuration specs.

const (
	// slbEgressConfigTestLabel selects the egress NAT Gateway configuration specs.
	slbEgressConfigTestLabel = "SLB-EgressConfig"

	// slbCCMRestartTestLabel marks the specs that stop, crash or re-image the cloud-controller-manager, as it marks the
	// dual-stack egress specs that restart it. They also carry slbInitTestLabel when the controller starts afresh (stop
	// and start, a new image) and slbCrashTestLabel when it crashes.
	slbCCMRestartTestLabel = "SLB-CCMRestart"

	// slbEgressConfigFaultTestLabel marks the egress configuration specs that create faults out of band: Azure resources
	// the controller must refuse, retry on or leave alone (user resources that are missing, held, in use, of the wrong
	// SKU, region or IP version; resources at reserved names; locks; another service on the identity's NAT Gateway),
	// and NAT Gateways, links, registrations or ownership tags changed behind the controller's back.
	slbEgressConfigFaultTestLabel = "SLB-EgressConfig-Fault"

	// slbEgressConfigUpgradeTestLabel selects the egress configuration spec that runs the cloud-controller-manager on the
	// image named by egressConfigPreviousCCMImageEnvVar.
	slbEgressConfigUpgradeTestLabel = "SLB-EgressConfig-Upgrade"
)

// newEgressConfigSpec starts an egress configuration spec: it registers the end of the spec (finishEgressConfigSpec:
// the crash check and the report of the checks the spec leaves out) first, so that it runs after the AfterEach and
// every other cleanup, and returns a client of the cluster and a new namespace named after basename.
func newEgressConfigSpec(basename string) (clientset.Interface, *v1.Namespace) {
	egressChecksLeftOut, egressSpecStart = nil, time.Now()
	DeferCleanup(finishEgressConfigSpec)
	cs, err := utils.CreateKubeClientSet()
	Expect(err).NotTo(HaveOccurred())
	ns, err := utils.CreateTestingNamespace(basename, cs)
	Expect(err).NotTo(HaveOccurred())
	return cs, ns
}

// egressChecksLeftOut lists what the running spec could not check in this environment (leaveOut).
var egressChecksLeftOut []string

// leaveOut records that the running spec cannot check what here; the spec goes on with its other checks.
func leaveOut(what string) {
	utils.Logf("Not checked: %s", what)
	if !slices.Contains(egressChecksLeftOut, what) {
		egressChecksLeftOut = append(egressChecksLeftOut, what)
	}
}

// finishEgressConfigSpec fails the spec when the cloud-controller-manager crashed while it ran, its cleanup included
// (expectNoCCMCrash), and skips it, as it verified everything else, when it left checks out (leaveOut). It runs after
// the AfterEach and every other cleanup of the spec (newEgressConfigSpec), so a spec whose checks or cleanup failed
// stays failed.
func finishEgressConfigSpec() {
	leftOut := egressChecksLeftOut
	egressChecksLeftOut = nil
	expectNoCCMCrash(egressSpecStart)
	if len(leftOut) > 0 {
		Skip("everything else verified; not checked: " + strings.Join(leftOut, "; "))
	}
}

// failSkippedSpecWithFailedCleanup fails a spec that was skipped but whose AfterEach or cleanup failed afterwards, which
// Ginkgo keeps only as an additional failure of the skipped spec, passing the suite. Every egress configuration spec,
// by its label, reports with it.
func failSkippedSpecWithFailedCleanup(report SpecReport) {
	if report.State.Is(ginkgotypes.SpecStateSkipped) && len(report.AdditionalFailures) > 0 {
		Fail("skipped, but its cleanup failed: " + report.AdditionalFailures[0].Failure.Message)
	}
}

var _ = ReportAfterEach(func(report SpecReport) {
	if slices.Contains(report.Labels(), slbEgressConfigTestLabel) {
		failSkippedSpecWithFailedCleanup(report)
	}
})

// cleanUpEgressConfigSpec ends an egress configuration spec: it starts the CCM again when the spec scaled it down and
// left it in ccmToRestore (DeferCleanup would only run after this cleanup, which needs a running CCM), deletes the
// spec's namespace and checks that the ServiceGateway and Azure hold nothing of it any more.
func cleanUpEgressConfigSpec(cs clientset.Interface, ns *v1.Namespace, ccmToRestore *CCMClusterClient) {
	// Taken first, so that a failed check below cannot leave them to the next spec.
	identities := egressIdentitiesUnderTest
	egressIdentitiesUnderTest = map[string]bool{}
	if ccmToRestore != nil {
		// A spec that failed while the controller was down must not leave it down for the next ones.
		Eventually(func() error {
			return scaleCCMDeployment(context.TODO(), ccmToRestore, 1)
		}, 2*time.Minute, defaultPollInterval).Should(Succeed(), "the cloud-controller-manager must be scaled up again")
		Expect(waitForCCMFullyUp(context.TODO(), ccmToRestore, CCMRecoveryTimeout)).To(Succeed(), "the cloud-controller-manager must be up again")
	}
	if cs == nil || ns == nil {
		return
	}
	Expect(utils.DeleteNamespace(cs, ns.Name)).To(Succeed())

	By("Waiting for Azure cleanup to complete (egress gateway cleanup is slower)")
	eventuallyAzureCleanup(6 * time.Minute)

	By("Verifying Service Gateway cleanup")
	verifyServiceGatewayCleanup()

	By("Verifying Address Locations cleanup")
	verifyAddressLocationsCleanup()

	// The global checks count only tagged egress resources, so every identity the spec configured is also
	// checked by name, even when the spec failed before its own check.
	for _, identity := range slices.Sorted(maps.Keys(identities)) {
		eventuallyEgressIdentityGone(identity)
	}
}

// expectNoCCMCrash fails the spec when the cloud-controller-manager crashed after since (ccmRestartedErr), also when no
// log check of the spec spans the crash. A restart the platform caused is not a crash. The specs call it before each
// replacement of the controller's pod, which takes the evidence of a crash with it, and once more after all their
// cleanup (finishEgressConfigSpec). The restores of the controller in cleanup do not, so that a crash cannot keep the
// controller from being restored.
func expectNoCCMCrash(since time.Time) {
	if since.IsZero() || !IsCCMClusterConfigured() {
		return
	}
	ccmClient, err := NewCCMClusterClient()
	Expect(err).NotTo(HaveOccurred())
	Eventually(func() error {
		pods, err := ccmClient.GetCCMPods(context.TODO())
		if err != nil {
			return err
		}
		for _, pod := range pods {
			for _, status := range pod.Status.ContainerStatuses {
				if status.Name != ccmContainerName {
					continue
				}
				switch err := ccmRestartedErr(pod.Name, status, since.Add(-ccmLogClockSkew), ccmPreviousTail(ccmClient, pod)); {
				case errors.Is(err, errCCMCrashed):
					return StopTrying("the cloud-controller-manager crashed while the spec ran").Wrap(err)
				case err != nil && !errors.Is(err, errCCMLogRotated):
					return err
				}
			}
		}
		return nil
	}, ccmLogReadTimeout, defaultPollInterval).Should(Succeed(), "the cloud-controller-manager must not crash while a spec runs")
}

const (
	// egressProbeContainer is the container of a probe pod, which records where its egress leaves.
	egressProbeContainer = "probe"

	// egressProbeScript prints a line about every 3 seconds: the Unix time and the IPv4 source address
	// the internet sees for the pod, or FAIL. ifconfig.me is what the other egress specs of this package
	// observe SNAT with; -f makes an HTTP error page a failure rather than an address.
	egressProbeScript = `trap 'exit 0' TERM; while true; do ` +
		`src=$(curl -4 -fs -m 4 http://ifconfig.me/ip | head -c 64 | tr -cd '0-9.'); ` +
		`echo "$(date +%s) ${src:-FAIL}"; sleep 3; done`

	// egressMaxProbeGap is the longest a probe pod may go without a successful probe while its egress
	// is said to continue. It probes every 3 to 7 seconds, so this is several failures in a row: an
	// interruption, not a lost request.
	egressMaxProbeGap = 30 * time.Second

	// egressSteadyWindow is how long a probe pod's egress must leave through the expected addresses only
	// before it counts as steady: for several seconds after an identity is registered, its data path
	// alternates between the identity's NAT Gateway and the cluster's default outbound path.
	egressSteadyWindow = 30 * time.Second

	// egressDataplaneSettle is the time the data path is given to stop using an address after the
	// controller detached and deleted it.
	egressDataplaneSettle = 15 * time.Second

	// egressApplyTimeout bounds the creation or the in-place update of a NAT Gateway with its Public IPs
	// and prefixes, the controller's retries included.
	egressApplyTimeout = 6 * time.Minute

	// egressRetryApplyTimeout bounds how long the controller takes to finish on its own what a problem it retries kept
	// it from doing, once the problem is gone: its retries back off to 30 s and pause for 5 minutes after 12 of them.
	egressRetryApplyTimeout = 10 * time.Minute

	// egressRestartObservation is how long a spec watches a restarted controller: longer than the 2
	// minutes after which it lists Azure a second time to catch late writes of the previous process.
	egressRestartObservation = 3 * time.Minute
)

// newEgressConfigPod returns the i-th pod of egressName with the given egress configuration
// annotations. A probe pod records where its egress leaves (egressProbeScript) instead of serving HTTP.
func newEgressConfigPod(namespace, egressName string, i int, annotations map[string]string, probe bool) *v1.Pod {
	container := v1.Container{
		Name: "test-app", Image: utils.AgnhostImage, ImagePullPolicy: v1.PullIfNotPresent, Args: []string{"netexec", "--http-port=8080"},
	}
	if probe {
		container = v1.Container{
			Name: egressProbeContainer, Image: utils.AgnhostImage, ImagePullPolicy: v1.PullIfNotPresent,
			Command: []string{"/bin/sh", "-c", egressProbeScript},
		}
	}
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: egressPodName(egressName, i), Namespace: namespace,
			Labels: map[string]string{egressLabel: egressName}, Annotations: maps.Clone(annotations),
		},
		Spec: v1.PodSpec{Containers: []v1.Container{container}},
	}
}

// egressIdentitiesUnderTest holds the egress identities whose pods the running spec created with the egress
// configuration helpers; the AfterEach checks that each is gone, whatever the tags of its resources.
var egressIdentitiesUnderTest = map[string]bool{}

// createEgressConfigPod creates pod and returns its name.
func createEgressConfigPod(cs clientset.Interface, pod *v1.Pod) string {
	egressIdentitiesUnderTest[pod.Labels[egressLabel]] = true
	_, err := cs.CoreV1().Pods(pod.Namespace).Create(context.TODO(), pod, metav1.CreateOptions{})
	Expect(err).NotTo(HaveOccurred())
	return pod.Name
}

// createConfiguredEgressPods creates count pods of egressName, numbered from 0, with the given egress
// configuration annotations, and returns their names. With probe set, pod 0 is a probe pod.
func createConfiguredEgressPods(cs clientset.Interface, namespace, egressName string, count int, annotations map[string]string, probe bool) []string {
	names := make([]string, 0, count)
	for i := 0; i < count; i++ {
		names = append(names, createEgressConfigPod(cs, newEgressConfigPod(namespace, egressName, i, annotations, probe && i == 0)))
	}
	return names
}

// annotationPatch returns annotations for a JSON merge patch, in which an empty value removes the
// annotation.
func annotationPatch(annotations map[string]string) map[string]any {
	values := make(map[string]any, len(annotations))
	for key, value := range annotations {
		values[key] = value
		if value == "" {
			values[key] = nil
		}
	}
	return values
}

// setEgressPodAnnotations sets annotations on the named pods; an empty value removes the annotation.
func setEgressPodAnnotations(cs clientset.Interface, namespace string, annotations map[string]string, podNames ...string) {
	patch, err := json.Marshal(map[string]any{"metadata": map[string]any{"annotations": annotationPatch(annotations)}})
	Expect(err).NotTo(HaveOccurred())
	for _, name := range podNames {
		_, err := cs.CoreV1().Pods(namespace).Patch(context.TODO(), name, types.MergePatchType, patch, metav1.PatchOptions{})
		Expect(err).NotTo(HaveOccurred())
	}
}

// setEgressPodLabel moves pod podName to egress identity egressName, or removes its egress label when egressName is "".
func setEgressPodLabel(cs clientset.Interface, namespace, podName, egressName string) {
	var value any
	if egressName != "" {
		egressIdentitiesUnderTest[egressName] = true
		value = egressName
	}
	patch, err := json.Marshal(map[string]any{"metadata": map[string]any{"labels": map[string]any{egressLabel: value}}})
	Expect(err).NotTo(HaveOccurred())
	_, err = cs.CoreV1().Pods(namespace).Patch(context.TODO(), podName, types.MergePatchType, patch, metav1.PatchOptions{})
	Expect(err).NotTo(HaveOccurred())
}

// deleteEgressPod deletes pod podName.
func deleteEgressPod(cs clientset.Interface, namespace, podName string) {
	Expect(cs.CoreV1().Pods(namespace).Delete(context.TODO(), podName, metav1.DeleteOptions{})).To(Succeed())
}

// egressPodGoneErr returns nil once pod podName is deleted.
func egressPodGoneErr(cs clientset.Interface, namespace, podName string) error {
	pod, err := cs.CoreV1().Pods(namespace).Get(context.TODO(), podName, metav1.GetOptions{})
	switch {
	case apierrors.IsNotFound(err):
		return nil
	case err != nil:
		return err
	}
	return fmt.Errorf("pod %s still exists (being deleted: %v, finalizers %v)", podName, pod.DeletionTimestamp != nil, pod.Finalizers)
}

// egressPodReleasedErr returns nil once pod podName, which is not being deleted, no longer carries the controller's
// cleanup finalizer: it is no member of an egress identity any more.
func egressPodReleasedErr(cs clientset.Interface, namespace, podName string) error {
	pod, err := cs.CoreV1().Pods(namespace).Get(context.TODO(), podName, metav1.GetOptions{})
	switch {
	case err != nil:
		return err
	case pod.DeletionTimestamp != nil:
		return fmt.Errorf("pod %s is being deleted", podName)
	case hasFinalizer(pod.Finalizers, serviceGatewayPodFinalizer):
		return fmt.Errorf("pod %s still carries the cleanup finalizer %s", podName, serviceGatewayPodFinalizer)
	}
	return nil
}

// createEgressConfigDeployment creates a Deployment of replicas pods of egressName with the given
// egress configuration annotations on their template.
func createEgressConfigDeployment(cs clientset.Interface, namespace, name, egressName string, replicas int32, annotations map[string]string) {
	egressIdentitiesUnderTest[egressName] = true
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": name}},
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{"app": name, egressLabel: egressName}, Annotations: maps.Clone(annotations),
				},
				Spec: v1.PodSpec{Containers: []v1.Container{{
					Name: "test-app", Image: utils.AgnhostImage, ImagePullPolicy: v1.PullIfNotPresent, Args: []string{"netexec", "--http-port=8080"},
				}}},
			},
		},
	}
	_, err := cs.AppsV1().Deployments(namespace).Create(context.TODO(), deployment, metav1.CreateOptions{})
	Expect(err).NotTo(HaveOccurred())
}

// setDeploymentPodAnnotations sets annotations on the pod template of a Deployment, which rolls it
// out; an empty value removes the annotation.
func setDeploymentPodAnnotations(cs clientset.Interface, namespace, name string, annotations map[string]string) {
	patch, err := json.Marshal(map[string]any{"spec": map[string]any{"template": map[string]any{
		"metadata": map[string]any{"annotations": annotationPatch(annotations)},
	}}})
	Expect(err).NotTo(HaveOccurred())
	_, err = cs.AppsV1().Deployments(namespace).Patch(context.TODO(), name, types.MergePatchType, patch, metav1.PatchOptions{})
	Expect(err).NotTo(HaveOccurred())
}

// livePodsAnnotated returns, by UID, the pods of egressName that are members of the identity (not
// being deleted, Pending or Running, with a pod IP) and whose annotation key has value.
func livePodsAnnotated(cs clientset.Interface, namespace, egressName, key, value string) (map[types.UID]string, error) {
	pods, err := cs.CoreV1().Pods(namespace).List(context.TODO(), metav1.ListOptions{LabelSelector: egressLabel + "=" + egressName})
	if err != nil {
		return nil, err
	}
	live := map[types.UID]string{}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.DeletionTimestamp == nil && (pod.Status.Phase == v1.PodRunning || pod.Status.Phase == v1.PodPending) &&
			len(pod.Status.PodIPs) > 0 && pod.Annotations[key] == value {
			live[pod.UID] = pod.Name
		}
	}
	return live, nil
}

// acceptedEgressAddresses returns the pod IPs an egress pod registers: those of an IP family its node
// has an address of (status.hostIPs), which is how the controller admits a partial dual-stack member.
func acceptedEgressAddresses(pod *v1.Pod) []string {
	families := map[bool]bool{}
	for _, hostIP := range pod.Status.HostIPs {
		if ip := net.ParseIP(hostIP.IP); ip != nil {
			families[ip.To4() == nil] = true
		}
	}
	if ip := net.ParseIP(pod.Status.HostIP); len(families) == 0 && ip != nil {
		families[ip.To4() == nil] = true
	}
	var accepted []string
	for _, podIP := range pod.Status.PodIPs {
		if ip := net.ParseIP(podIP.IP); ip != nil && families[ip.To4() == nil] {
			accepted = append(accepted, podIP.IP)
		}
	}
	return accepted
}

// singleStackNode returns a Ready, schedulable Linux node whose InternalIPs are all of one IP family,
// or "" when there is none.
func singleStackNode(cs clientset.Interface) string {
	return schedulableNodeWithFamilies(cs, 1)
}

// dualStackNode returns a Ready, schedulable Linux node with InternalIPs of both IP families, or "" when
// there is none.
func dualStackNode(cs clientset.Interface) string {
	return schedulableNodeWithFamilies(cs, 2)
}

// schedulableNodeWithFamilies returns a Ready, schedulable Linux node whose InternalIPs are of count IP
// families, or "" when there is none.
func schedulableNodeWithFamilies(cs clientset.Interface, count int) string {
	nodes, err := cs.CoreV1().Nodes().List(context.TODO(), metav1.ListOptions{})
	Expect(err).NotTo(HaveOccurred())
	for i := range nodes.Items {
		node := &nodes.Items[i]
		if node.Spec.Unschedulable || node.Labels[v1.LabelOSStable] != "linux" ||
			slices.ContainsFunc(node.Spec.Taints, func(t v1.Taint) bool { return t.Effect != v1.TaintEffectPreferNoSchedule }) ||
			!slices.ContainsFunc(node.Status.Conditions, func(c v1.NodeCondition) bool {
				return c.Type == v1.NodeReady && c.Status == v1.ConditionTrue
			}) {
			continue
		}
		families := map[bool]bool{}
		for _, address := range node.Status.Addresses {
			if ip := net.ParseIP(address.Address); address.Type == v1.NodeInternalIP && ip != nil {
				families[ip.To4() == nil] = true
			}
		}
		if len(families) == count {
			return node.Name
		}
	}
	return ""
}

const (
	// egressRejectedForGood and egressRejectedRetried end the EgressConfigRejected message: whether the controller
	// tries the rejected configuration again while the pods keep agreeing on it.
	egressRejectedForGood = "it will not be retried until the members of the identity agree on another configuration"
	egressRejectedRetried = "it will be retried while the members of the identity agree on it"
)

// egressConfigEventReasons are the reasons of the events about the egress configuration of a pod.
var egressConfigEventReasons = []string{
	consts.ServiceEgressGatewayReasonInvalidConfig, consts.ServiceEgressGatewayReasonConfigConflict,
	consts.ServiceEgressGatewayReasonConfigApplied, consts.ServiceEgressGatewayReasonConfigRejected,
}

// egressPodEvents returns the events with reason in namespace, by the name of the object (a pod) they
// are about.
func egressPodEvents(cs clientset.Interface, namespace, reason string) (map[string][]v1.Event, error) {
	events, err := cs.CoreV1().Events(namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	byPod := map[string][]v1.Event{}
	for _, e := range events.Items {
		if e.Reason == reason {
			byPod[e.InvolvedObject.Name] = append(byPod[e.InvolvedObject.Name], e)
		}
	}
	return byPod, nil
}

// podEventErr returns nil once one of podNames has an event of reason and eventType whose message contains every one
// of wants.
func podEventErr(cs clientset.Interface, namespace string, podNames []string, reason, eventType string, wants ...string) error {
	byPod, err := egressPodEvents(cs, namespace, reason)
	if err != nil {
		return err
	}
	for _, name := range podNames {
		if slices.ContainsFunc(byPod[name], func(e v1.Event) bool { return egressEventMatches(e, eventType, wants) }) {
			return nil
		}
	}
	return fmt.Errorf("no %s %s event containing %q on pods %v", eventType, reason, wants, podNames)
}

// eachPodEventErr returns nil once every one of podNames has an event of reason and eventType whose message
// contains every one of wants.
func eachPodEventErr(cs clientset.Interface, namespace string, podNames []string, reason, eventType string, wants ...string) error {
	byPod, err := egressPodEvents(cs, namespace, reason)
	if err != nil {
		return err
	}
	for _, name := range podNames {
		if !slices.ContainsFunc(byPod[name], func(e v1.Event) bool { return egressEventMatches(e, eventType, wants) }) {
			return fmt.Errorf("pod %s has no %s %s event containing %q", name, eventType, reason, wants)
		}
	}
	return nil
}

// egressEventMatches reports whether event e is of eventType, any type when it is "", and its message contains every
// one of wants.
func egressEventMatches(e v1.Event, eventType string, wants []string) bool {
	return (eventType == "" || e.Type == eventType) &&
		!slices.ContainsFunc(wants, func(want string) bool { return !strings.Contains(e.Message, want) })
}

func TestEgressEventMatches(t *testing.T) {
	e := v1.Event{Type: v1.EventTypeWarning, Message: "Egress identity \"egi\" keeps its NAT Gateway configuration"}
	for _, tc := range []struct {
		name      string
		eventType string
		wants     []string
		want      bool
	}{
		{name: "of the type, without wants", eventType: v1.EventTypeWarning, want: true},
		{name: "of another type", eventType: v1.EventTypeNormal},
		{name: "of any type", want: true},
		{name: "every want in the message", wants: []string{`"egi"`, "keeps its"}, want: true},
		{name: "a want not in the message", eventType: v1.EventTypeWarning, wants: []string{`"egi"`, "applied"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := egressEventMatches(e, tc.eventType, tc.wants); got != tc.want {
				t.Errorf("egressEventMatches(%q, %q) = %v; want %v", tc.eventType, tc.wants, got, tc.want)
			}
		})
	}
}

// podHasEvent reports whether one of podNames has an event of reason.
func podHasEvent(cs clientset.Interface, namespace string, podNames []string, reason string) bool {
	byPod, err := egressPodEvents(cs, namespace, reason)
	Expect(err).NotTo(HaveOccurred())
	return slices.ContainsFunc(podNames, func(name string) bool { return len(byPod[name]) > 0 })
}

// eventuallyPodEvent waits until podEventErr holds: one of podNames was told.
func eventuallyPodEvent(cs clientset.Interface, namespace string, podNames []string, reason, eventType string, wants ...string) {
	Expect(podNames).NotTo(BeEmpty(), "the pods of which one must have the event")
	Eventually(func() error {
		return podEventErr(cs, namespace, podNames, reason, eventType, wants...)
	}, 3*time.Minute, defaultPollInterval).Should(Succeed())
}

// egressAppliedWait bounds a wait for the EgressConfigApplied events of a configuration a step writes to a NAT Gateway:
// the write, up to a minute until the controller gives up on Azure's answer to it (the 1-minute timeout of each Azure
// request, pkg/azclient), the 2-minute quiet period that follows, the 5-minute pause of the identity's retries when that
// run, which failed, was the last try of a burst (as when it continues the retries of the same configuration), and the
// run that then finds the configuration applied and announces it.
const egressAppliedWait = 10 * time.Minute

// eventuallyEachPodApplied waits until every one of podNames was told of a configuration of its egress identity, applied
// or created, whose EgressConfigApplied message contains every one of wants. Such a configuration is announced also when
// a NAT Gateway write whose outcome Azure did not report in time applied it; the wait gives that case its time,
// egressAppliedWait.
func eventuallyEachPodApplied(cs clientset.Interface, namespace string, podNames []string, wants ...string) {
	Expect(podNames).NotTo(BeEmpty(), "the pods that must have the event")
	Eventually(func() error {
		return eachPodEventErr(cs, namespace, podNames, consts.ServiceEgressGatewayReasonConfigApplied, v1.EventTypeNormal, wants...)
	}, egressAppliedWait, defaultPollInterval).Should(Succeed())
}

// eventuallyEachPodAppliedWithUserResources waits, once the NAT Gateway of an egress identity uses the user resources
// its pods ask for (eventuallyEgressNATWithUserResources), until every one of podNames was told of the configuration
// whose EgressConfigApplied message contains every one of wants. The identity's creation announces it, and then no pod
// is told of a rejection, unless the controller was refused access to the user resources (egressAccessDenial), as while
// the role createEgressUserResourceGroup assigned is not applied: the identity was then created with the default, and
// an update applied the configuration.
func eventuallyEachPodAppliedWithUserResources(cs clientset.Interface, namespace string, podNames []string, wants ...string) {
	denied := egressAccessDenial(cs, namespace, podNames) != ""
	eventuallyEachPodApplied(cs, namespace, podNames, wants...)
	if !denied {
		expectNoPodEvent(cs, namespace, podNames, consts.ServiceEgressGatewayReasonConfigRejected)
	}
}

// egressCountsReachedErr returns how many events of reason and eventType whose message contains every one of wants each
// pod of want got (egressEventCounts), and an error while one of them got fewer than want says.
func egressCountsReachedErr(cs clientset.Interface, namespace, reason, eventType string, want map[string]int32,
	wants ...string) (map[string]int32, error) {
	podNames := slices.Sorted(maps.Keys(want))
	counts, err := egressEventCounts(cs, namespace, podNames, reason, eventType, wants...)
	if err != nil {
		return nil, err
	}
	for _, name := range podNames {
		if counts[name] < want[name] {
			return counts, fmt.Errorf("pod %s got %d %s events containing %q, want %d", name, counts[name], reason, wants, want[name])
		}
	}
	return counts, nil
}

// eventuallyEgressEventCounts waits until each pod of want got events of reason and eventType whose message contains
// every one of wants as many times as want says (egressCountsReachedErr), and checks that none got them more often.
func eventuallyEgressEventCounts(cs clientset.Interface, namespace, reason, eventType string, want map[string]int32, wants ...string) {
	var counts map[string]int32
	Eventually(func() (err error) {
		counts, err = egressCountsReachedErr(cs, namespace, reason, eventType, want, wants...)
		return err
	}, 3*time.Minute, defaultPollInterval).Should(Succeed())
	Expect(counts).To(Equal(want), "how many %s events containing %q each pod got", reason, wants)
}

// eventuallyEgressAppliedCounts is eventuallyEgressEventCounts for EgressConfigApplied events, waiting as long as
// eventuallyEachPodApplied does.
func eventuallyEgressAppliedCounts(cs clientset.Interface, namespace string, want map[string]int32, wants ...string) {
	reason := consts.ServiceEgressGatewayReasonConfigApplied
	var counts map[string]int32
	Eventually(func() (err error) {
		counts, err = egressCountsReachedErr(cs, namespace, reason, v1.EventTypeNormal, want, wants...)
		return err
	}, egressAppliedWait, defaultPollInterval).Should(Succeed())
	Expect(counts).To(Equal(want), "how many %s events containing %q each pod got", reason, wants)
}

// eventuallyEachPodEvent waits until eachPodEventErr holds: every one of podNames was told.
func eventuallyEachPodEvent(cs clientset.Interface, namespace string, podNames []string, reason, eventType string, wants ...string) {
	Expect(podNames).NotTo(BeEmpty(), "the pods that must have the event")
	Eventually(func() error {
		return eachPodEventErr(cs, namespace, podNames, reason, eventType, wants...)
	}, 3*time.Minute, defaultPollInterval).Should(Succeed())
}

// expectNoPodEvent checks that none of podNames has an event with one of reasons.
func expectNoPodEvent(cs clientset.Interface, namespace string, podNames []string, reasons ...string) {
	for _, reason := range reasons {
		byPod, err := egressPodEvents(cs, namespace, reason)
		Expect(err).NotTo(HaveOccurred())
		for _, name := range podNames {
			Expect(byPod[name]).To(BeEmpty(), "pod %s must have no %s event", name, reason)
		}
	}
}

// egressEventOccurrences returns how many times event e was raised: a recorder that repeats an event counts it on the
// same event, in its count or, for an event series, its series count; another recorder creates a new event.
func egressEventOccurrences(e v1.Event) int32 {
	if e.Series != nil {
		return max(e.Series.Count, 1)
	}
	return max(e.Count, 1)
}

func TestEgressEventOccurrences(t *testing.T) {
	for _, tc := range []struct {
		name  string
		event v1.Event
		want  int32
	}{
		{name: "an event raised once", event: v1.Event{Count: 1}, want: 1},
		{name: "a repeated event", event: v1.Event{Count: 2}, want: 2},
		{name: "an event series", event: v1.Event{Count: 1, Series: &v1.EventSeries{Count: 4}}, want: 4},
		{name: "an event without a count", want: 1},
		{name: "an event series without a count", event: v1.Event{Series: &v1.EventSeries{}}, want: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := egressEventOccurrences(tc.event); got != tc.want {
				t.Errorf("egressEventOccurrences(%+v) = %d; want %d", tc.event, got, tc.want)
			}
		})
	}
}

// egressEventCounts returns how many times each of podNames got an event of reason and eventType ("" for any) whose
// message contains every one of wants, repeats included.
func egressEventCounts(cs clientset.Interface, namespace string, podNames []string, reason, eventType string, wants ...string) (map[string]int32, error) {
	byPod, err := egressPodEvents(cs, namespace, reason)
	if err != nil {
		return nil, err
	}
	counts := make(map[string]int32, len(podNames))
	for _, name := range podNames {
		counts[name] = 0
		for _, e := range byPod[name] {
			if egressEventMatches(e, eventType, wants) {
				counts[name] += egressEventOccurrences(e)
			}
		}
	}
	return counts, nil
}

// egressEventTally returns how many times each of podNames got an event of each of reasons, repeats included, by reason
// and pod.
func egressEventTally(cs clientset.Interface, namespace string, podNames []string, reasons ...string) map[string]map[string]int32 {
	tally := make(map[string]map[string]int32, len(reasons))
	for _, reason := range reasons {
		counts, err := egressEventCounts(cs, namespace, podNames, reason, "")
		Expect(err).NotTo(HaveOccurred())
		tally[reason] = counts
	}
	return tally
}

// egressConfigEventCounts returns how many times each egress configuration event in namespace was
// raised, by event name; a repeated event is counted on the same object.
func egressConfigEventCounts(cs clientset.Interface, namespace string) map[string]int32 {
	counts := map[string]int32{}
	for _, reason := range egressConfigEventReasons {
		byPod, err := egressPodEvents(cs, namespace, reason)
		Expect(err).NotTo(HaveOccurred())
		for _, events := range byPod {
			for _, e := range events {
				counts[e.Name] = egressEventOccurrences(e)
			}
		}
	}
	return counts
}

// egressNATWant is the shape the managed NAT Gateway of an egress identity must have for a
// configuration.
type egressNATWant struct {
	idleTimeout int
	// publicIPs, publicIPsV6, prefixes and prefixesV6 are the IDs of the Public IPs and Public IP
	// prefixes the NAT Gateway references, per IP family: managed ones, which are in the cluster
	// resource group, and user ones, which never are.
	publicIPs, publicIPsV6, prefixes, prefixesV6 []string
	// tags are configured tags that the NAT Gateway and every managed Public IP and prefix it
	// references must carry.
	tags map[string]string
}

// defaultEgressNATWant is the shape of the NAT Gateway of an identity without configuration: idle
// timeout 4 and one managed Public IP per IP family it serves (IPv6 on dual-stack clusters only).
func defaultEgressNATWant(egressName string, ipv6 bool) egressNATWant {
	want := egressNATWant{
		idleTimeout: consts.ServiceEgressGatewayIdleTimeoutDefaultMinutes,
		publicIPs:   egressManagedPublicIPIDs(egressName, false, 1),
	}
	if ipv6 {
		want.publicIPsV6 = egressManagedPublicIPIDs(egressName, true, 1)
	}
	return want
}

// egressManagedPublicIPIDs returns the IDs of the first count managed Public IPs of egressName in one
// IP family.
func egressManagedPublicIPIDs(egressName string, ipv6 bool, count int) []string {
	ids := make([]string, 0, count)
	for n := 1; n <= count; n++ {
		ids = append(ids, publicIPID(resourceGroupName, egressManagedPublicIPName(egressName, ipv6, n)))
	}
	return ids
}

// egressManagedPrefixID returns the ID of the managed Public IP prefix of egressName of the given length.
func egressManagedPrefixID(egressName string, ipv6 bool, length int) string {
	return publicIPPrefixID(resourceGroupName, egressManagedPrefixName(egressName, ipv6, length))
}

// egressIdleTimeout returns the idle timeout of a NAT Gateway: Azure's default, 4 minutes, when unset.
func egressIdleTimeout(nat azureNetworkResource) int {
	if idle := nat.Properties.IdleTimeoutInMinutes; idle != nil && *idle > 0 {
		return *idle
	}
	return consts.ServiceEgressGatewayIdleTimeoutDefaultMinutes
}

// missingEgressTags returns the tags of want, as "key=value", that tags lacks.
func missingEgressTags(tags, want map[string]string) []string {
	var missing []string
	for key, value := range want {
		if tagValue(tags, key) != value {
			missing = append(missing, key+"="+value)
		}
	}
	sort.Strings(missing)
	return missing
}

// subResourceIDs returns the IDs of references.
func subResourceIDs(refs []armSubResource) []string {
	ids := make([]string, 0, len(refs))
	for _, ref := range refs {
		ids = append(ids, ref.ID)
	}
	return ids
}

// egressNATShapeErr returns nil once the managed NAT Gateway of egressName has the shape want: it is
// provisioned, StandardV2, owned by the identity and linked to the Service Gateway, with want's idle
// timeout, references and tags; every managed Public IP and prefix it references is provisioned,
// StandardV2, of its IP family, owned by the identity and tagged; and the identity's reserved names
// hold nothing else but the Public IPs and prefixes others (names), which the controller leaves alone:
// resources it does not own, or of its own that it may not delete.
func egressNATShapeErr(egressName string, want egressNATWant, others ...string) error {
	nat, found, err := getAzureNetworkResource(byoNATGatewayARMID(resourceGroupName, egressName))
	switch {
	case err != nil:
		return err
	case !found:
		return fmt.Errorf("NAT Gateway %s does not exist", egressName)
	}
	props := nat.Properties
	switch {
	case props.ProvisioningState != "Succeeded":
		return fmt.Errorf("NAT Gateway %s is %s", egressName, props.ProvisioningState)
	case !strings.EqualFold(nat.SKU.Name, "StandardV2"):
		return fmt.Errorf("NAT Gateway %s has SKU %q, want StandardV2", egressName, nat.SKU.Name)
	case !strings.EqualFold(tagValue(nat.Tags, egressIdentityTagKey), egressName):
		return fmt.Errorf("NAT Gateway %s is not tagged %s=%s: %v", egressName, egressIdentityTagKey, egressName, nat.Tags)
	case props.ServiceGateway == nil || !strings.EqualFold(props.ServiceGateway.ID, serviceGatewayResourceID()):
		return fmt.Errorf("NAT Gateway %s is not linked to the Service Gateway", egressName)
	case egressIdleTimeout(nat) != want.idleTimeout:
		return fmt.Errorf("NAT Gateway %s has idle timeout %d, want %d", egressName, egressIdleTimeout(nat), want.idleTimeout)
	}
	for _, refs := range []struct {
		property  string
		got, want []string
	}{
		{"publicIpAddresses", subResourceIDs(props.PublicIPAddresses), want.publicIPs},
		{"publicIpAddressesV6", subResourceIDs(props.PublicIPAddressesV6), want.publicIPsV6},
		{"publicIpPrefixes", subResourceIDs(props.PublicIPPrefixes), want.prefixes},
		{"publicIpPrefixesV6", subResourceIDs(props.PublicIPPrefixesV6), want.prefixesV6},
	} {
		if !sameNamesFold(refs.got, refs.want) {
			return fmt.Errorf("NAT Gateway %s has %s %v, want %v", egressName, refs.property, refs.got, refs.want)
		}
	}
	if missing := missingEgressTags(nat.Tags, want.tags); len(missing) > 0 {
		return fmt.Errorf("NAT Gateway %s lacks the configured tags %v: %v", egressName, missing, nat.Tags)
	}
	return egressManagedResourcesErr(egressName, want, others...)
}

// egressManagedResourcesErr returns nil when every managed Public IP and prefix of want, those in the cluster resource
// group, has its shape (egressManagedResourceErr), and the reserved names of egressName hold them and the Public IPs
// and prefixes others (names) only.
func egressManagedResourcesErr(egressName string, want egressNATWant, others ...string) error {
	var managedPublicIPs, managedPrefixes []string
	for _, family := range []struct {
		ipv6                bool
		publicIPs, prefixes []string
	}{{false, want.publicIPs, want.prefixes}, {true, want.publicIPsV6, want.prefixesV6}} {
		for _, id := range family.publicIPs {
			if strings.EqualFold(resourceGroupFromID(id), resourceGroupName) {
				if err := egressManagedResourceErr(egressName, id, family.ipv6, 0, want.tags); err != nil {
					return err
				}
				managedPublicIPs = append(managedPublicIPs, resourceNameFromID(id))
			}
		}
		for _, id := range family.prefixes {
			if strings.EqualFold(resourceGroupFromID(id), resourceGroupName) {
				name := resourceNameFromID(id)
				length, err := strconv.Atoi(name[strings.LastIndex(name, "-")+1:])
				Expect(err).NotTo(HaveOccurred(), "the length of managed prefix %s", name)
				if err := egressManagedResourceErr(egressName, id, family.ipv6, length, want.tags); err != nil {
					return err
				}
				managedPrefixes = append(managedPrefixes, name)
			}
		}
	}
	reservedPublicIPs, _ := egressReservedNames(egressName)
	for _, name := range others {
		if containsFold(reservedPublicIPs, name) {
			managedPublicIPs = append(managedPublicIPs, name)
		} else {
			managedPrefixes = append(managedPrefixes, name)
		}
	}
	return egressReservedResourcesErr(egressName, managedPublicIPs, managedPrefixes)
}

// egressManagedResourceErr returns nil when the managed Public IP (prefixLength 0) or Public IP
// prefix id of egressName is provisioned, StandardV2, of its IP family, static (a Public IP) or of
// prefixLength (a prefix), owned by the identity and tagged with tags.
func egressManagedResourceErr(egressName, id string, ipv6 bool, prefixLength int, tags map[string]string) error {
	resource, found, err := getAzureNetworkResource(id)
	if err != nil {
		return err
	}
	name, props, version := resourceNameFromID(id), resource.Properties, "IPv4"
	if ipv6 {
		version = "IPv6"
	}
	switch {
	case !found:
		return fmt.Errorf("managed %s does not exist", name)
	case props.ProvisioningState != "Succeeded":
		return fmt.Errorf("managed %s is %s", name, props.ProvisioningState)
	case !strings.EqualFold(resource.SKU.Name, "StandardV2"):
		return fmt.Errorf("managed %s has SKU %q, want StandardV2", name, resource.SKU.Name)
	case !strings.EqualFold(props.AddressVersion, version):
		return fmt.Errorf("managed %s is %s, want %s", name, props.AddressVersion, version)
	case !strings.EqualFold(tagValue(resource.Tags, egressIdentityTagKey), egressName):
		return fmt.Errorf("managed %s is not tagged %s=%s: %v", name, egressIdentityTagKey, egressName, resource.Tags)
	case prefixLength == 0 && !strings.EqualFold(props.AllocationMethod, "Static"):
		return fmt.Errorf("managed %s has allocation method %q, want Static", name, props.AllocationMethod)
	case prefixLength > 0 && props.PrefixLength != prefixLength:
		return fmt.Errorf("managed %s has length %d, want %d", name, props.PrefixLength, prefixLength)
	}
	if missing := missingEgressTags(resource.Tags, tags); len(missing) > 0 {
		return fmt.Errorf("managed %s lacks the configured tags %v: %v", name, missing, resource.Tags)
	}
	return nil
}

// eventuallyEgressNAT waits until egressNATShapeErr holds.
func eventuallyEgressNAT(egressName string, want egressNATWant) {
	Eventually(func() error {
		return egressNATShapeErr(egressName, want)
	}, egressApplyTimeout, defaultPollInterval).Should(Succeed(), "the NAT Gateway of %s must get the configuration its pods agree on", egressName)
}

// readEgressNAT reads the managed NAT Gateway of egressName.
func readEgressNAT(egressName string) azureNetworkResource {
	nat, found, err := getAzureNetworkResource(byoNATGatewayARMID(resourceGroupName, egressName))
	Expect(err).NotTo(HaveOccurred())
	Expect(found).To(BeTrue(), "NAT Gateway %s must exist", egressName)
	return nat
}

// expectSameEgressNAT checks that the NAT Gateway of egressName is still the one read as before: it was
// updated in place, never deleted and created again.
func expectSameEgressNAT(egressName string, before azureNetworkResource) {
	Expect(before.Properties.ResourceGUID).NotTo(BeEmpty(), "ARM reports the resource GUID of a NAT Gateway")
	Expect(readEgressNAT(egressName).Properties.ResourceGUID).To(Equal(before.Properties.ResourceGUID),
		"the NAT Gateway of %s must be updated in place, not re-created", egressName)
}

// updateServiceGatewayService sends one request for Service Gateway service name, out of band: registering or updating
// it with properties, or deleting it (isDelete).
func updateServiceGatewayService(name string, isDelete bool, properties map[string]any) ([]byte, error) {
	body, err := json.Marshal(map[string]any{
		"action": "PartialUpdate",
		"serviceRequests": []any{map[string]any{
			"isDelete": isDelete,
			"service":  map[string]any{"name": name, "properties": properties},
		}},
	})
	if err != nil {
		return nil, err
	}
	return runAz("rest", "--method", "post", "--url", buildServiceGatewayURL("updateServices"), "--body", string(body))
}

// rewriteNATGatewayOutOfBand writes NAT Gateway natGatewayID out of band, as its owner might: it PUTs the NAT Gateway as
// read, without its read-only properties and with change applied to its properties, and waits until it is provisioned.
func rewriteNATGatewayOutOfBand(natGatewayID string, change func(properties map[string]any)) {
	out, err := runAz("rest", "--method", "get", "--url", armURL(natGatewayID))
	Expect(err).NotTo(HaveOccurred(), string(out))
	var nat map[string]any
	Expect(json.Unmarshal(out, &nat)).To(Succeed())
	properties, ok := nat["properties"].(map[string]any)
	Expect(ok).To(BeTrue(), "NAT Gateway %s must have properties", natGatewayID)
	delete(nat, "etag")
	delete(properties, "provisioningState")
	delete(properties, "resourceGuid")
	change(properties)
	body, err := json.Marshal(nat)
	Expect(err).NotTo(HaveOccurred())
	putBYOResource(natGatewayID, string(body))
}

// egressResourceEtags returns the etags of the NAT Gateway of egressName and of the managed Public IPs
// and prefixes it references, by ID.
func egressResourceEtags(egressName string) map[string]string {
	nat := readEgressNAT(egressName)
	etags := map[string]string{nat.ID: nat.Etag}
	props := nat.Properties
	for _, refs := range [][]armSubResource{props.PublicIPAddresses, props.PublicIPAddressesV6, props.PublicIPPrefixes, props.PublicIPPrefixesV6} {
		for _, ref := range refs {
			if strings.EqualFold(resourceGroupFromID(ref.ID), resourceGroupName) {
				resource, found, err := getAzureNetworkResource(ref.ID)
				Expect(err).NotTo(HaveOccurred())
				Expect(found).To(BeTrue(), "%s must exist", ref.ID)
				etags[ref.ID] = resource.Etag
			}
		}
	}
	for id, etag := range etags {
		Expect(etag).NotTo(BeEmpty(), "ARM reports the etag of %s", id)
	}
	return etags
}

// azureResourceEtags returns the etags of network resources ids, which must exist, by ID.
func azureResourceEtags(ids ...string) map[string]string {
	etags := make(map[string]string, len(ids))
	for _, id := range ids {
		resource, found, err := getAzureNetworkResource(id)
		Expect(err).NotTo(HaveOccurred())
		Expect(found).To(BeTrue(), "%s must exist", id)
		Expect(resource.Etag).NotTo(BeEmpty(), "ARM reports the etag of %s", id)
		etags[id] = resource.Etag
	}
	return etags
}

// egressEtagsUnchangedErr returns nil while every resource of etags still has its etag: nothing changed it. A write
// that changes nothing keeps the etag; ccmEgressWrites sees those.
func egressEtagsUnchangedErr(etags map[string]string) error {
	for id, etag := range etags {
		resource, found, err := getAzureNetworkResource(id)
		switch {
		case err != nil:
			return err
		case !found:
			return fmt.Errorf("%s was deleted", id)
		case resource.Etag != etag:
			return fmt.Errorf("%s was written: etag %s, was %s", resourceNameFromID(id), resource.Etag, etag)
		}
	}
	return nil
}

// ccmClientOrSkip returns a client of the cloud-controller-manager's cluster, skipping the spec, which what (stops,
// restarts or crashes it), when that cluster is not configured.
func ccmClientOrSkip(what string) *CCMClusterClient {
	if !IsCCMClusterConfigured() {
		Skip("CCM cluster access is not configured; this spec " + what)
	}
	ccmClient, err := NewCCMClusterClient()
	Expect(err).NotTo(HaveOccurred())
	return ccmClient
}

// egressLogVerbosity is the verbosity (-v) from which the cloud-controller-manager logs its Azure writes, how its egress
// applies end and the steps of its start-up that the specs check in its log.
const egressLogVerbosity = 2

// ccmVerbosity caches the verbosity ccmLogVerbosity read: the command line of the cloud-controller-manager does not change
// during a run.
var ccmVerbosity *int

// ccmLogVerbosity returns the verbosity (-v) the cloud-controller-manager's container runs with, read once from its
// deployment.
func ccmLogVerbosity(ccmClient *CCMClusterClient) (int, error) {
	if ccmVerbosity != nil {
		return *ccmVerbosity, nil
	}
	deployment, err := ccmClient.ClientSet.AppsV1().Deployments(ccmClient.Namespace).Get(context.TODO(), ccmDeploymentName, metav1.GetOptions{})
	if err != nil {
		return 0, fmt.Errorf("reading the cloud-controller-manager deployment: %w", err)
	}
	for _, container := range deployment.Spec.Template.Spec.Containers {
		if container.Name == ccmContainerName {
			v, err := logVerbosityOf(slices.Concat(container.Command, container.Args))
			if err != nil {
				return 0, err
			}
			ccmVerbosity = &v
			return v, nil
		}
	}
	return 0, fmt.Errorf("the cloud-controller-manager deployment has no container %s", ccmContainerName)
}

// logVerbosityOf returns the klog verbosity that the command line args sets with -v or --v (the last one, as for any
// flag), or 0, klog's default, when it sets none.
func logVerbosityOf(args []string) (int, error) {
	v := 0
	for i, arg := range args {
		name, value, hasValue := strings.Cut(strings.TrimLeft(arg, "-"), "=")
		if !strings.HasPrefix(arg, "-") || name != "v" {
			continue
		}
		if !hasValue {
			if i+1 == len(args) {
				return 0, fmt.Errorf("flag %s has no value", arg)
			}
			value = args[i+1]
		}
		n, err := strconv.Atoi(value)
		if err != nil {
			return 0, fmt.Errorf("flag %s has value %q, not a number", name, value)
		}
		v = n
	}
	return v, nil
}

func TestLogVerbosityOf(t *testing.T) {
	for _, tc := range []struct {
		name    string
		args    []string
		want    int
		wantErr bool
	}{
		{name: "not set", args: []string{"cloud-controller-manager", "--leader-elect=true"}},
		{name: "--v=5", args: []string{"--cloud-provider=azure", "--v=5", "--secure-port=10267"}, want: 5},
		{name: "-v=1", args: []string{"-v=1"}, want: 1},
		{name: "a separate value", args: []string{"--v", "3", "--leader-elect"}, want: 3},
		{name: "the last one wins", args: []string{"--v=1", "-v", "4"}, want: 4},
		{name: "other flags starting with v", args: []string{"--vmodule=egress*=6", "--version=false", "--v=2"}, want: 2},
		{name: "a value that is not a number", args: []string{"--v=$(LOG_LEVEL)"}, wantErr: true},
		{name: "no value", args: []string{"--v"}, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := logVerbosityOf(tc.args)
			if (err != nil) != tc.wantErr || got != tc.want {
				t.Errorf("logVerbosityOf(%q) = %d, %v; want %d, error %v", tc.args, got, err, tc.want, tc.wantErr)
			}
		})
	}
}

// leaveOutLogCheck leaves out what, a check of the cloud-controller-manager's log that cannot be done, for why
// (leaveOut).
func leaveOutLogCheck(what, why string) {
	leaveOut(fmt.Sprintf("%s in the cloud-controller-manager's log (%s)", what, why))
}

// ccmLogsEgress reports whether the cloud-controller-manager of ccmClient logs at egressLogVerbosity or more, so that a
// spec can check what in its log, where a missing line is then a failure. When it does not, or its verbosity cannot be
// read, the spec leaves the check out (leaveOutLogCheck).
func ccmLogsEgress(ccmClient *CCMClusterClient, what string) bool {
	v, err := ccmLogVerbosity(ccmClient)
	switch {
	case err != nil:
		leaveOutLogCheck(what, fmt.Sprintf("its verbosity cannot be read: %v", err))
		return false
	case v < egressLogVerbosity:
		leaveOutLogCheck(what, fmt.Sprintf("it logs at verbosity %d, below %d", v, egressLogVerbosity))
		return false
	}
	return true
}

// ccmEgressLog returns a client of the cloud-controller-manager's cluster with which to check what in its log
// (ccmLogsEgress), or nil, leaving the check out, when that cluster is not configured or the log cannot tell.
func ccmEgressLog(what string) *CCMClusterClient {
	if !IsCCMClusterConfigured() {
		leaveOutLogCheck(what, "CCM cluster access is not configured")
		return nil
	}
	ccmClient, err := NewCCMClusterClient()
	Expect(err).NotTo(HaveOccurred())
	if !ccmLogsEgress(ccmClient, what) {
		return nil
	}
	return ccmClient
}

const (
	// ccmLogClockSkew is how much earlier than a time taken on the test machine's clock a spec reads the controller's
	// log from, for the difference between that clock and the one of the controller's node.
	ccmLogClockSkew = 5 * time.Second

	// ccmLogStartAllowance is how much later than its start the first line of the log of a cloud-controller-manager
	// container may be before the log counts as rotated: the time a new container takes to log.
	ccmLogStartAllowance = 30 * time.Second

	// ccmLogHeadBytes is how much of the start of a log ccmLogLines reads, with the kubelet's timestamps, to find when it
	// begins: the timestamp of its first line.
	ccmLogHeadBytes = 256

	// ccmLogReadTimeout is how long ccmLogLinesFor retries reading the controller's log before the spec fails.
	ccmLogReadTimeout = 2 * time.Minute

	// ccmCrashTailLines is how many of the last lines of the log of a container that restarted ccmRestartedErr reads to
	// tell a restart the platform caused from a crash.
	ccmCrashTailLines = 50
)

// egressSpecStart is when the running egress configuration spec started (newEgressConfigSpec).
var egressSpecStart time.Time

// errCCMLogRotated says that the log of a running cloud-controller-manager container no longer tells what happened from
// the start of the window a spec reads: the kubelet rotated it, and only its latest file is served, or the platform
// restarted the container in the window (ccmRestartedErr).
var errCCMLogRotated = errors.New("the controller's log no longer tells what happened from the start of the window")

// errCCMCrashed says that the cloud-controller-manager container restarted while a spec ran for another reason than
// the platform's (ccmPlatformExits): it crashed (a panic or fatal error, OOMKilled, a liveness kill, a start-up that
// failed for another reason), which fails the spec (ccmRestartedErr).
var errCCMCrashed = errors.New("the cloud-controller-manager crashed")

// ccmPlatformExits are the restarts of the controller that the platform causes: what it logs right before it exits with
// code 1 (cmd/cloud-controller-manager/app/controllermanager.go), and why. Any other restart is a crash.
var ccmPlatformExits = []struct{ line, cause string }{
	{"leaderelection lost", "after it lost its leader lease"},
	{"wait for apiserver being healthy", "because the API server did not answer while it started"},
	// Wraps only the failures of the Kubernetes lists the ServiceGateway start-up makes (difftracker.buildK8sState).
	{"failed to build K8s state", "because the API server failed a list while it started"},
}

// ccmLogLines returns the lines of the logs of the running cloud-controller-manager containers that contain every one
// of wants, from since on (less ccmLogClockSkew), from the start of the spec when since is the zero time, or from the
// start of a container when that is later. With the lines it read, it returns errCCMLogRotated when the log of a
// container begins later than that, or when the platform restarted a container in the window; it returns
// errCCMCrashed, without lines, when a container crashed in the window (ccmRestartedErr).
func ccmLogLines(ccmClient *CCMClusterClient, since time.Time, wants ...string) ([]string, error) {
	pods, err := ccmClient.GetCCMPods(context.TODO())
	if err != nil {
		return nil, err
	}
	if since.IsZero() {
		since = egressSpecStart
	}
	var lines []string
	var rotated error
	for _, pod := range pods {
		if pod.Status.Phase != v1.PodRunning || pod.DeletionTimestamp != nil {
			continue
		}
		// The log must reach back to the start of the window, give or take the clocks' difference, or to the container's
		// first line when the window starts with the container.
		from, allowance := since.Add(-ccmLogClockSkew), ccmLogClockSkew
		for _, status := range pod.Status.ContainerStatuses {
			if status.Name != ccmContainerName {
				continue
			}
			switch err := ccmRestartedErr(pod.Name, status, from, ccmPreviousTail(ccmClient, pod)); {
			case errors.Is(err, errCCMLogRotated):
				rotated = err
			case err != nil:
				return nil, err
			}
			if running := status.State.Running; running != nil && running.StartedAt.After(from) {
				from, allowance = running.StartedAt.Time, ccmLogStartAllowance
			}
		}
		podLogs := ccmClient.ClientSet.CoreV1().Pods(pod.Namespace)
		headBytes := int64(ccmLogHeadBytes)
		head, err := podLogs.GetLogs(pod.Name, &v1.PodLogOptions{Container: ccmContainerName, Timestamps: true, LimitBytes: &headBytes}).
			DoRaw(context.TODO())
		if err != nil {
			return nil, fmt.Errorf("reading the log of cloud-controller-manager pod %s: %w", pod.Name, err)
		}
		if err := ccmLogRotatedErr(pod.Name, string(head), from, allowance); err != nil {
			rotated = err
		}
		sinceTime := metav1.NewTime(from)
		raw, err := podLogs.GetLogs(pod.Name, &v1.PodLogOptions{Container: ccmContainerName, SinceTime: &sinceTime}).DoRaw(context.TODO())
		if err != nil {
			return nil, fmt.Errorf("reading the log of cloud-controller-manager pod %s: %w", pod.Name, err)
		}
		lines = append(lines, logLinesContaining(string(raw), wants...)...)
	}
	return lines, rotated
}

// ccmRestartedErr reports whether the cloud-controller-manager container of pod podName, whose status is status,
// restarted after from; previousTail reads the last lines of the log of the container before the restart. The specs
// restart the controller by deleting its pod, whose replacement has no previous container, so this restart is either
// the platform's (ccmPlatformExits: it lost its leader lease, or the API server did not answer or failed a list while it
// started, all exit code 1) or a crash. After the platform's restart the new process redid its start-up (it tries the agreed
// configurations again, sweeps leftovers, runs its confirmation pass), so its log no longer tells what the spec's steps
// did: errCCMLogRotated, and the checks of the window are left out. Any other restart is a crash, which must fail the
// spec: errCCMCrashed, with the last lines of the crashed container. It returns the error of previousTail when that
// fails, and nil when the container did not restart after from.
func ccmRestartedErr(podName string, status v1.ContainerStatus, from time.Time, previousTail func() (string, error)) error {
	previous := status.LastTerminationState.Terminated
	if previous == nil || !previous.FinishedAt.After(from) {
		return nil
	}
	restart := fmt.Sprintf("cloud-controller-manager pod %s restarted at %s (%s, exit code %d)", podName,
		previous.FinishedAt.UTC().Format(time.RFC3339), previous.Reason, previous.ExitCode)
	tail, err := previousTail()
	if err != nil {
		return fmt.Errorf("reading the log of the container before the restart: %s: %w", restart, err)
	}
	if previous.ExitCode == 1 {
		for _, exit := range ccmPlatformExits {
			if strings.Contains(tail, exit.line) {
				return fmt.Errorf("%w: %s %s", errCCMLogRotated, restart, exit.cause)
			}
		}
	}
	return fmt.Errorf("%w: %s; the last lines of its log:\n%s", errCCMCrashed, restart, tail)
}

// ccmPreviousTail returns a reader of the last ccmCrashTailLines lines of the log of the container before the last
// restart of the cloud-controller-manager container of pod.
func ccmPreviousTail(ccmClient *CCMClusterClient, pod v1.Pod) func() (string, error) {
	return func() (string, error) {
		lines := int64(ccmCrashTailLines)
		raw, err := ccmClient.ClientSet.CoreV1().Pods(pod.Namespace).GetLogs(pod.Name, &v1.PodLogOptions{Container: ccmContainerName,
			Previous: true, TailLines: &lines}).DoRaw(context.TODO())
		return string(raw), err
	}
}

func TestCCMRestartedErr(t *testing.T) {
	from := time.Date(2026, 10, 19, 14, 1, 44, 0, time.UTC)
	restarted := func(d time.Duration, reason string, exitCode int32) v1.ContainerStatus {
		return v1.ContainerStatus{RestartCount: 1, LastTerminationState: v1.ContainerState{Terminated: &v1.ContainerStateTerminated{
			Reason: reason, ExitCode: exitCode, StartedAt: metav1.NewTime(from.Add(-time.Hour)), FinishedAt: metav1.NewTime(from.Add(d))}}}
	}
	leaseLost := `E1019 14:02:13.538825       1 controllermanager.go:174] "leaderelection lost" logger="NewCloudControllerManagerCommand"`
	apiServerDown := `E1019 14:02:28.123456       1 controllermanager.go:467] "Failed to wait for apiserver being healthy" err="timed out waiting for the condition"`
	contextFailed := `E1019 14:02:28.123456       1 controllermanager.go:401] "error building controller context" err="failed to wait for apiserver being healthy: timed out"`
	panicked := "panic: assignment to entry in nil map\n\ngoroutine 412 [running]:"
	for _, tc := range []struct {
		name      string
		status    v1.ContainerStatus
		tail      string
		tailErr   error
		want      error // nil, errCCMLogRotated or errCCMCrashed
		wantInMsg string
		readsTail bool
	}{
		{name: "never restarted", status: v1.ContainerStatus{State: v1.ContainerState{Running: &v1.ContainerStateRunning{
			StartedAt: metav1.NewTime(from.Add(-time.Hour))}}}},
		{name: "restarted before the window", status: restarted(-time.Minute, "Error", 2), tail: panicked},
		{name: "restarted as the window starts", status: restarted(0, "Error", 2), tail: panicked},
		{name: "lost its lease in the window", status: restarted(29*time.Second, "Error", 1), tail: leaseLost, want: errCCMLogRotated,
			wantInMsg: "at 2026-10-19T14:02:13Z (Error, exit code 1) after it lost its leader lease", readsTail: true},
		{name: "the API server did not answer while it started", status: restarted(29*time.Second, "Error", 1), tail: apiServerDown,
			want: errCCMLogRotated, wantInMsg: "because the API server did not answer while it started", readsTail: true},
		{name: "its controller context could not be built without the API server", status: restarted(29*time.Second, "Error", 1),
			tail: contextFailed, want: errCCMLogRotated, wantInMsg: "because the API server did not answer", readsTail: true},
		{name: "a list the API server failed while it started", status: restarted(29*time.Second, "Error", 1),
			tail: `E1019 14:02:41.123456       1 controllermanager.go:406] "error running controllers" err="failed to start ServiceGateway runtime: ` +
				`initialize difftracker: failed to build K8s state: failed to list nodes: Get \"https://hcp-kubernetes:443/api/v1/nodes\": ` +
				`dial tcp 10.0.0.1:443: connect: connection refused"`,
			want: errCCMLogRotated, wantInMsg: "because the API server failed a list while it started", readsTail: true},
		{name: "an Azure failure while it started is a crash", status: restarted(29*time.Second, "Error", 1),
			tail: `E1019 14:02:41.123456       1 controllermanager.go:406] "error running controllers" err="failed to start ServiceGateway runtime: ` +
				`initialize difftracker: failed to build NRP state: GET https://management.azure.com/...: 503 Service Unavailable"`,
			want: errCCMCrashed, readsTail: true},
		{name: "a start-up that failed for another reason, exit code 1, is a crash", status: restarted(29*time.Second, "Error", 1),
			tail: `E1019 14:02:28.123456       1 controllermanager.go:406] "error running controllers" err="egress start-up failed"`,
			want: errCCMCrashed, readsTail: true},
		{name: "a panic after it lost its lease is a crash", status: restarted(29*time.Second, "Error", 2), tail: leaseLost + "\n" + panicked,
			want: errCCMCrashed, readsTail: true},
		{name: "panicked in the window", status: restarted(29*time.Second, "Error", 2), tail: panicked, want: errCCMCrashed,
			wantInMsg: "(Error, exit code 2); the last lines of its log:\n" + panicked, readsTail: true},
		{name: "OOMKilled in the window", status: restarted(29*time.Second, "OOMKilled", 137), want: errCCMCrashed,
			wantInMsg: "(OOMKilled, exit code 137)", readsTail: true},
		{name: "its previous log cannot be read", status: restarted(29*time.Second, "Error", 1), tailErr: errors.New("server unavailable"),
			wantInMsg: "server unavailable", readsTail: true},
	} {
		read := false
		err := ccmRestartedErr("ccm", tc.status, from, func() (string, error) { read = true; return tc.tail, tc.tailErr })
		switch {
		case read != tc.readsTail:
			t.Errorf("%s: read the previous log %v, want %v", tc.name, read, tc.readsTail)
		case tc.want == nil && tc.wantInMsg == "" && err != nil:
			t.Errorf("%s: ccmRestartedErr() = %v, want nil", tc.name, err)
		case tc.want != nil && !errors.Is(err, tc.want):
			t.Errorf("%s: ccmRestartedErr() = %v, want %v", tc.name, err, tc.want)
		case tc.want == nil && (errors.Is(err, errCCMLogRotated) || errors.Is(err, errCCMCrashed)):
			t.Errorf("%s: ccmRestartedErr() = %v, want neither left out nor a crash", tc.name, err)
		case tc.wantInMsg != "" && (err == nil || !strings.Contains(err.Error(), tc.wantInMsg)):
			t.Errorf("%s: ccmRestartedErr() = %v, want it to contain %q", tc.name, err, tc.wantInMsg)
		}
	}
}

// ccmLogRotatedErr returns errCCMLogRotated when the log of cloud-controller-manager pod podName, whose head was read
// with the kubelet's timestamps, begins more than allowance after from, or has no line to tell when it begins: the
// kubelet rotated it, and the lines from then on are gone.
func ccmLogRotatedErr(podName, head string, from time.Time, allowance time.Duration) error {
	stamp, _, _ := strings.Cut(head, " ")
	begins, err := time.Parse(time.RFC3339Nano, stamp)
	switch {
	case err != nil:
		return fmt.Errorf("%w: the log of cloud-controller-manager pod %s does not start with a line the kubelet timestamped (%q)",
			errCCMLogRotated, podName, stamp)
	case begins.After(from.Add(allowance)):
		return fmt.Errorf("%w: the log of cloud-controller-manager pod %s begins at %s, after %s", errCCMLogRotated, podName,
			begins.UTC().Format(time.RFC3339Nano), from.UTC().Format(time.RFC3339Nano))
	}
	return nil
}

func TestCCMLogRotatedErr(t *testing.T) {
	begins := time.Date(2026, 10, 16, 14, 29, 43, 106106123, time.UTC)
	stamp := begins.Format(time.RFC3339Nano)
	klogLine := stamp + ` I1016 14:29:43.106106       1 flags.go:64] FLAG: --allocate-node-cidrs="false"` + "\n"
	for _, tc := range []struct {
		name    string
		head    string
		from    time.Time
		rotated bool
	}{
		{"begins before the window", klogLine, begins.Add(time.Hour), false},
		{"begins within the allowance", klogLine, begins.Add(-ccmLogClockSkew), false},
		{"begins after the allowance", klogLine, begins.Add(-ccmLogClockSkew - time.Microsecond), true},
		{"in any log format", stamp + ` {"ts":1792160983106.106,"v":0,"msg":"FLAG"}` + "\n", begins, false},
		{"in another time zone", begins.In(time.FixedZone("UTC+2", 2*60*60)).Format(time.RFC3339Nano) + " line\n", begins, false},
		{"its first line cut after the timestamp", stamp, begins, false},
		{"empty", "", begins, true},
		{"without timestamps", `I1016 14:29:43.106106       1 flags.go:64] FLAG: --allocate-node-cidrs="false"` + "\n", begins, true},
	} {
		if err := ccmLogRotatedErr("ccm", tc.head, tc.from, ccmLogClockSkew); errors.Is(err, errCCMLogRotated) != tc.rotated {
			t.Errorf("%s: ccmLogRotatedErr() = %v, want rotated %v", tc.name, err, tc.rotated)
		}
	}
}

// ccmLogLinesFor returns the lines of ccmLogLines, to check what in them, or false, leaving the check out, when the log
// of ccmClient cannot tell: there is no client (ccmEgressLog left the check out), the controller logs below
// egressLogVerbosity (ccmLogsEgress), or its log no longer tells what happened from the start of the window, the kubelet
// having rotated it or the platform having restarted the controller. A read that fails is retried for ccmLogReadTimeout;
// a crash of the controller in the window fails the spec at once.
func ccmLogLinesFor(ccmClient *CCMClusterClient, since time.Time, what string, wants ...string) ([]string, bool) {
	if ccmClient == nil || !ccmLogsEgress(ccmClient, what) {
		return nil, false
	}
	// The API server that serves the log fails a read now and then: only reads that keep failing fail the spec.
	var lines []string
	var err error
	Eventually(func() error {
		switch lines, err = ccmLogLines(ccmClient, since, wants...); {
		case errors.Is(err, errCCMCrashed):
			return StopTrying("the cloud-controller-manager crashed while the spec ran").Wrap(err)
		case errors.Is(err, errCCMLogRotated):
			return nil
		}
		return err
	}, ccmLogReadTimeout, defaultPollInterval).Should(Succeed(), "reading the cloud-controller-manager's log for %s", what)
	if err != nil {
		leaveOutLogCheck(what, err.Error())
		return nil, false
	}
	return lines, true
}

// expectLeftoversDeletedAtStartup checks, once the leftovers of egress identity id, which has neither NAT Gateway nor
// pods, are gone, that the restarted cloud-controller-manager's start-up cleanup deleted them: its confirmation pass,
// 2 minutes later, deletes leftovers it still finds through the deletion of the identity, which it logs. The start-up
// cleanup is best-effort; the check is left out when it failed, or when the log cannot tell (its failures are logged at
// verbosity 4).
func expectLeftoversDeletedAtStartup(ccmClient *CCMClusterClient, id string) {
	what := "that the start-up cleanup, not the confirmation pass, deleted what " + id + " left behind"
	scheduled, ok := ccmLogLinesFor(ccmClient, time.Time{}, what, `"Scheduled deletion of an egress identity that left resources behind"`,
		fmt.Sprintf("egressIdentity=%q", id))
	if !ok || len(scheduled) == 0 {
		return
	}
	failed, _ := ccmLogLinesFor(ccmClient, time.Time{}, what, `"Could not clean up orphaned Public IP`)
	if v, err := ccmLogVerbosity(ccmClient); len(failed) > 0 || err != nil || v < 4 {
		leaveOut(fmt.Sprintf("%s (the confirmation pass deleted them after a start-up cleanup that failed or does not log its failures: %q)",
			what, failed))
		return
	}
	Fail(fmt.Sprintf("the start-up cleanup must delete what %s left behind, not the confirmation pass: %s", id, scheduled[0]))
}

// ccmRecentLogLines is ccmLogLines for a spec that waits for a line: one written while it waits is in the latest file of
// a rotated log too.
func ccmRecentLogLines(ccmClient *CCMClusterClient, since time.Time, wants ...string) ([]string, error) {
	switch lines, err := ccmLogLines(ccmClient, since, wants...); {
	case errors.Is(err, errCCMCrashed):
		return nil, StopTrying("the cloud-controller-manager crashed while the spec waited").Wrap(err)
	case errors.Is(err, errCCMLogRotated):
		return lines, nil
	default:
		return lines, err
	}
}

// logLinesContaining returns the non-empty lines of log that contain every one of wants.
func logLinesContaining(log string, wants ...string) []string {
	var lines []string
	for _, line := range strings.Split(log, "\n") {
		if line != "" && !slices.ContainsFunc(wants, func(want string) bool { return !strings.Contains(line, want) }) {
			lines = append(lines, line)
		}
	}
	return lines
}

func TestLogLinesContaining(t *testing.T) {
	lines := []string{
		`I1016 04:31:00.722828       1 egress_apply.go:84] "Applied egress configuration" logger="difftracker" identity="egi" ` +
			`configuration="idle timeout 4m; IPv4: 1 managed IP" wrote=true`,
		`I1016 04:31:05.100000       1 egress_apply.go:86] "Rejected egress configuration" logger="difftracker" identity="egi" ` +
			`configuration="idle timeout 4m; IPv4: user IPs (pip)" reason="user Public IP \"/subscriptions/s/resourcegroups/rg/providers/` +
			`microsoft.network/publicipaddresses/pip\" has SKU \"Standard\", not StandardV2" retryable=false`,
		`I1016 04:31:06.000000       1 egress_apply.go:86] "Rejected egress configuration" logger="difftracker" identity="egi-2" ` +
			`configuration="idle timeout 4m" reason="IPv4: no public address" retryable=false`,
		`I1016 04:31:07.000000       1 egress_azure.go:291] "Creating or updating NAT Gateway" logger="difftracker" natGateway="egi" resourceGroup="rg"`,
		`I1016 04:31:08.000000       1 egress_runs.go:163] "Observed egress identity" serviceUID="egi" configuration="idle timeout 4m" swept=true`,
		`I1016 04:31:09.000000       1 egress_apply.go:86] "Rejected egress configuration" logger="difftracker" identity="EGI" ` +
			`configuration="idle timeout 4m" reason="x" retryable=true`,
		`I1016 04:31:10.000000       1 egress_azure.go:359] "Deleting public IP" logger="difftracker" pip="egi-pip-2" resourceGroup="rg"`,
	}
	log := strings.Join(lines, "\n") + "\n"
	for _, tc := range []struct {
		name  string
		wants []string
		want  []string
	}{
		{name: "every line but the empty last one without wants", want: lines},
		{name: "one want", wants: []string{`pip="egi-pip-2"`}, want: lines[6:]},
		{name: "every want on the same line", wants: []string{`"Rejected egress configuration"`, `identity="egi"`}, want: lines[1:2]},
		{name: "a quoted identity is neither a prefix of another, another case of it nor the value of another key",
			wants: []string{`identity="egi"`}, want: lines[:2]},
		{name: "wants on different lines only", wants: []string{`"Applied egress configuration"`, "retryable=false"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := logLinesContaining(log, tc.wants...); !slices.Equal(got, tc.want) {
				t.Errorf("logLinesContaining(%q) = %q; want %q", tc.wants, got, tc.want)
			}
		})
	}
}

// egressCreationTries returns, in order, what lines of the cloud-controller-manager's log tell of the creations of egress
// identity egressName: "R" for a rejected configuration (a "Rejected egress configuration" line), "F" for a creation
// falling back to the default after rejecting its configuration (a line that only a creation run logs, right after its
// rejection), both lines containing every one of wants, "C" for the identity created ("Created outbound service") and
// "D" for its NAT Gateway being deleted ("Deleting NAT Gateway"). "RF" is one try of a creation; a creation that fails
// after it is retried and tries again, all before its "Created outbound service" line, so specs match the tries against
// a pattern, such as `^(RF)+C$`, rather than count them: a try outside a creation is an "R" without its "F".
func egressCreationTries(lines []string, egressName string, wants ...string) string {
	var tries strings.Builder
	for _, line := range lines {
		ofIdentity := strings.Contains(line, fmt.Sprintf("identity=%q", egressName)) && len(logLinesContaining(line, wants...)) == 1
		switch {
		case strings.Contains(line, `"Rejected egress configuration"`) && ofIdentity:
			tries.WriteByte('R')
		case strings.Contains(line, `"Applying the default egress configuration after the creation configuration was rejected"`) && ofIdentity:
			tries.WriteByte('F')
		case strings.Contains(line, `"Created outbound service"`) && strings.Contains(line, fmt.Sprintf("serviceUID=%q", egressName)):
			tries.WriteByte('C')
		case strings.Contains(line, `"Deleting NAT Gateway"`) && strings.Contains(line, fmt.Sprintf("natGateway=%q", egressName)):
			tries.WriteByte('D')
		}
	}
	return tries.String()
}

func TestEgressCreationTries(t *testing.T) {
	rejected := `I1017 05:12:01.100000       1 egress_apply.go:86] "Rejected egress configuration" logger="difftracker" ` +
		`identity="eglc-again-x" configuration="idle timeout 4m; IPv4: user IPs (eglc-standard-pip)" ` +
		`reason="user Public IP \"/subscriptions/s/resourceGroups/rg/providers/Microsoft.Network/publicIPAddresses/eglc-standard-pip\" ` +
		`has SKU \"Standard\"" retryable=false`
	fallback := `I1017 05:12:01.100100       1 egress_apply.go:67] "Applying the default egress configuration after the creation ` +
		`configuration was rejected" logger="difftracker" identity="eglc-again-x" rejected="idle timeout 4m; IPv4: user IPs ` +
		`(eglc-standard-pip)" reason="user Public IP \"eglc-standard-pip\" has SKU \"Standard\""`
	created := `I1017 05:12:14.600000       1 egress_runs.go:116] "Created outbound service" logger="difftracker.ServiceUpdater" ` +
		`serviceUID="eglc-again-x" correlationID="c8df21d6-74ac-40fa-802a-61e3a008ca10" pod="/" ` +
		`configuration="idle timeout 4m; IPv4: 1 managed IP" swept=true`
	notCreated := `I1017 05:12:44.600000       1 egress_runs.go:110] "Could not create outbound service" ` +
		`logger="difftracker.ServiceUpdater" serviceUID="eglc-again-x" correlationID="c" pod="/" err="registering egress identity ` +
		`\"eglc-again-x\" with its NAT Gateway: updating Service Gateway services for \"servicegateway\": throttled due to too many requests"`
	deleting := `I1017 05:14:54.800000       1 egress_azure.go:320] "Deleting NAT Gateway" logger="difftracker" ` +
		`natGateway="eglc-again-x" resourceGroup="mc_rg" ifMatch="W/\"b513c6b4-ac73-4941-b312-086a7acceb59\""`
	written := `I1017 05:12:02.900000       1 egress_azure.go:291] "Creating or updating NAT Gateway" logger="difftracker" ` +
		`natGateway="eglc-again-x" resourceGroup="mc_rg"`
	applied := `I1017 05:12:13.200000       1 egress_apply.go:84] "Applied egress configuration" logger="difftracker" ` +
		`identity="eglc-again-x" configuration="idle timeout 4m; IPv4: 1 managed IP" wrote=true`
	other := func(line string) string { return strings.ReplaceAll(line, `"eglc-again-x"`, `"eglc-again-x2"`) }
	otherConfiguration := strings.ReplaceAll(rejected, "eglc-standard-pip", "eglc-other-pip")
	otherFallback := strings.ReplaceAll(fallback, "eglc-standard-pip", "eglc-other-pip")
	for _, tc := range []struct {
		name  string
		lines []string
		wants []string
		want  string
	}{
		{name: "tried at each creation", lines: []string{rejected, fallback, written, created, deleting, rejected, fallback, written, created},
			wants: []string{"eglc-standard-pip"}, want: "RFCDRFC"},
		{name: "a creation retried after its rejection tries again before it is created",
			lines: []string{rejected, fallback, applied, notCreated, rejected, fallback, applied, created}, want: "RFRFC"},
		{name: "a try outside a creation has no fallback", lines: []string{rejected, fallback, created, deleting, rejected, rejected, fallback, created},
			want: "RFCDRRFC"},
		{name: "a deletion retried", lines: []string{created, deleting, deleting}, want: "CDD"},
		{name: "another identity", lines: []string{other(rejected), other(fallback), other(created), other(deleting)}},
		{name: "a rejection of another configuration", lines: []string{otherConfiguration, otherFallback, created},
			wants: []string{"eglc-standard-pip"}, want: "C"},
		{name: "any rejection without wants", lines: []string{otherConfiguration, otherFallback, created}, want: "RFC"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := egressCreationTries(tc.lines, "eglc-again-x", tc.wants...); got != tc.want {
				t.Errorf("egressCreationTries() = %q; want %q", got, tc.want)
			}
		})
	}
}

// waitForCCMLogLine waits, polling every second, until a running cloud-controller-manager logs a line that contains
// every one of wants, a line it logs at egressLogVerbosity, and reports true. It reports false at once when the
// controller logs below that verbosity: the spec, which times a step by that line, then verifies what it can without it.
func waitForCCMLogLine(ccmClient *CCMClusterClient, timeout time.Duration, wants ...string) bool {
	if v, err := ccmLogVerbosity(ccmClient); err != nil || v < egressLogVerbosity {
		return false
	}
	Eventually(func() ([]string, error) {
		return ccmRecentLogLines(ccmClient, time.Time{}, wants...)
	}, timeout, time.Second).ShouldNot(BeEmpty(), "the cloud-controller-manager must log a line with %q", wants)
	return true
}

// egressConfirmationPassEnd is how long after the end of its initialization the start-up confirmation pass of a
// cloud-controller-manager is over: it lists Azure again once the 2-minute settle delay has passed, then runs the
// observations it owes the identities that listing shows changed.
const egressConfirmationPassEnd = 2*time.Minute + 45*time.Second

// waitForEgressConfirmationPassOver waits, for a spec about to change out of band what the controller must leave alone,
// until the start-up confirmation pass of the running cloud-controller-manager is over (egressConfirmationPassOverAt):
// the pass repairs what changed since the start-up listing (documented), so after a spec that restarted the controller
// just before, it would act on the change. It does not wait when the controller started long enough ago, nor without
// access to the controller's cluster, without which no spec restarts it.
func waitForEgressConfirmationPassOver() {
	if !IsCCMClusterConfigured() {
		return
	}
	ccmClient, err := NewCCMClusterClient()
	Expect(err).NotTo(HaveOccurred())
	logged := false
	Eventually(func() error {
		over, err := egressConfirmationPassOverAt(ccmClient)
		if err != nil {
			return err
		}
		if wait := time.Until(over); wait > 0 {
			if !logged {
				utils.Logf("Waiting %v for the start-up confirmation pass of the cloud-controller-manager to be over", wait.Round(time.Second))
				logged = true
			}
			return fmt.Errorf("the start-up confirmation pass of the cloud-controller-manager may run until %s", over.UTC().Format(time.RFC3339))
		}
		return nil
	}, egressStartupAllowance+egressConfirmationPassEnd+5*time.Minute, defaultPollInterval).Should(Succeed())
}

// egressConfirmationPassOverAt returns when the start-up confirmation pass of the running cloud-controller-manager
// containers is over: egressConfirmationPassEnd after a container last logged the end of its initialization
// (ccmInitializedAt), or, where its log does not show that (below egressLogVerbosity, unreadable, rotated or not logged
// yet), egressStartupAllowance and egressConfirmationPassEnd after the container started.
func egressConfirmationPassOverAt(ccmClient *CCMClusterClient) (time.Time, error) {
	pods, err := ccmClient.GetCCMPods(context.TODO())
	if err != nil {
		return time.Time{}, err
	}
	v, err := ccmLogVerbosity(ccmClient)
	logsInitialization := err == nil && v >= egressLogVerbosity
	var over time.Time
	for _, pod := range pods {
		if pod.Status.Phase != v1.PodRunning || pod.DeletionTimestamp != nil {
			continue
		}
		for _, status := range pod.Status.ContainerStatuses {
			if status.Name != ccmContainerName || status.State.Running == nil {
				continue
			}
			initialized := status.State.Running.StartedAt.Add(egressStartupAllowance)
			if logsInitialization {
				if at, ok := ccmInitializedAt(ccmClient, pod.Namespace, pod.Name); ok {
					initialized = at
				}
			}
			if end := initialized.Add(egressConfirmationPassEnd); end.After(over) {
				over = end
			}
		}
	}
	return over, nil
}

// ccmInitializedAt returns when the running container of cloud-controller-manager pod podName last logged the end of its
// initialization, which it logs at egressLogVerbosity, by the kubelet's timestamp of that line, or false when its log
// does not show it.
func ccmInitializedAt(ccmClient *CCMClusterClient, namespace, podName string) (time.Time, bool) {
	raw, err := ccmClient.ClientSet.CoreV1().Pods(namespace).GetLogs(podName, &v1.PodLogOptions{Container: ccmContainerName, Timestamps: true}).
		DoRaw(context.TODO())
	if err != nil {
		return time.Time{}, false
	}
	return lastLogLineTime(string(raw), `"Completed DiffTracker initialization"`)
}

// lastLogLineTime returns the kubelet's timestamp of the last line of log, read with the kubelet's timestamps, that
// contains want, or false when no such line has one.
func lastLogLineTime(log, want string) (time.Time, bool) {
	lines := logLinesContaining(log, want)
	for i := len(lines) - 1; i >= 0; i-- {
		stamp, _, _ := strings.Cut(lines[i], " ")
		if at, err := time.Parse(time.RFC3339Nano, stamp); err == nil {
			return at, true
		}
	}
	return time.Time{}, false
}

func TestLastLogLineTime(t *testing.T) {
	first := time.Date(2026, 10, 17, 5, 16, 18, 670858091, time.UTC)
	again := first.Add(40 * time.Minute)
	initialized := ` I1017 05:16:18.670732       1 initialization.go:287] "Completed DiffTracker initialization"`
	line := func(at time.Time, text string) string { return at.Format(time.RFC3339Nano) + text + "\n" }
	for _, tc := range []struct {
		name string
		log  string
		want time.Time
		ok   bool
	}{
		{"the line among others", line(first.Add(-time.Millisecond), ` I1017 05:16:18.670711       1 egress_startup.go:652] "Skipped attached Public IP prefix"`) +
			line(first, initialized) + line(first.Add(time.Millisecond), ` I1017 05:16:18.670795       1 azure_metrics.go:106] "Observed Request Latency"`),
			first, true},
		{"the last of two", line(first, initialized) + line(again, initialized), again, true},
		{"in another time zone", again.In(time.FixedZone("UTC+2", 2*60*60)).Format(time.RFC3339Nano) + initialized + "\n", again, true},
		{"no such line", line(first, ` I1017 05:16:18.670795       1 azure_metrics.go:106] "Observed Request Latency"`), time.Time{}, false},
		{"without the kubelet's timestamp", initialized[1:] + "\n", time.Time{}, false},
		{"empty", "", time.Time{}, false},
	} {
		if got, ok := lastLogLineTime(tc.log, `"Completed DiffTracker initialization"`); ok != tc.ok || !got.Equal(tc.want) {
			t.Errorf("%s: lastLogLineTime() = %v, %v; want %v, %v", tc.name, got, ok, tc.want, tc.ok)
		}
	}
}

// ccmEgressWrites returns the Azure writes (PUTs and DELETEs) that the running cloud-controller-manager containers logged
// since the spec or they started to the NAT Gateway, Public IP or prefix named name and, when name is that of an egress
// identity, to its reserved Public IP and prefix names; or false, leaving the check what out, when the log cannot tell
// (ccmLogLinesFor).
func ccmEgressWrites(ccmClient *CCMClusterClient, name, what string) ([]string, bool) {
	write := regexp.MustCompile(`"(Creating or updating|Deleting) (NAT Gateway|public IP|Public IP prefix)".* (natGateway|pip|prefix)="` +
		regexp.QuoteMeta(name) + `(-pip[^"]*|-prefix[^"]*)?"`)
	lines, ok := ccmLogLinesFor(ccmClient, time.Time{}, what)
	return slices.DeleteFunc(lines, func(line string) bool { return !write.MatchString(line) }), ok
}

// ccmEgressPuts returns the PUTs among ccmEgressWrites: only the log tells a PUT that changes nothing, which keeps the
// etag.
func ccmEgressPuts(ccmClient *CCMClusterClient, name, what string) ([]string, bool) {
	writes, ok := ccmEgressWrites(ccmClient, name, what)
	return slices.DeleteFunc(writes, func(line string) bool { return !strings.Contains(line, `"Creating or updating `) }), ok
}

// ccmEgressRegistrations returns the lines in which the running cloud-controller-manager logged registering egress
// identity id with its NAT Gateway, a Service Gateway write that no etag tells; or false, leaving the check out, when its
// log cannot tell (ccmLogLinesFor).
func ccmEgressRegistrations(ccmClient *CCMClusterClient, id string) ([]string, bool) {
	return ccmLogLinesFor(ccmClient, time.Time{}, "the registrations of "+id, `"Registered egress identity with its NAT Gateway"`,
		fmt.Sprintf("egressIdentity=%q", id))
}

// ccmEgressRegistrationClears returns the lines in which the running cloud-controller-manager logged clearing the NAT
// Gateway of egress identity id's Service Gateway registration; or false, leaving the check out, when its log cannot tell
// (ccmLogLinesFor).
func ccmEgressRegistrationClears(ccmClient *CCMClusterClient, id string) ([]string, bool) {
	return ccmLogLinesFor(ccmClient, time.Time{}, "the registration clears of "+id,
		`"Clearing the NAT Gateway of a Service Gateway service"`, fmt.Sprintf("service=%q", id))
}

// expectEgressNATWritesAtMost checks, in the log of the running cloud-controller-manager (ccmEgressLog), that it wrote
// the NAT Gateway of egressName, which it created or changed since the spec or it started, at least once and at most
// max times.
func expectEgressNATWritesAtMost(egressName string, maxWrites int, why string) {
	what := "the writes of NAT Gateway " + egressName
	writes, ok := ccmEgressWrites(ccmEgressLog(what), egressName, what)
	if !ok {
		return
	}
	writes = slices.DeleteFunc(writes, func(line string) bool { return !strings.Contains(line, `"Creating or updating NAT Gateway"`) })
	Expect(writes).NotTo(BeEmpty(), "the controller must log the writes of NAT Gateway %s", egressName)
	Expect(len(writes)).To(BeNumerically("<=", maxWrites), "%s; writes:\n%s", why, strings.Join(writes, "\n"))
}

// expectEgressAppliesWritingAtMost checks, in the log of the running cloud-controller-manager (ccmEgressLog), that at
// least one and at most maxApplies of its applies of a configuration of egressName wrote to Azure ("Applied egress
// configuration" with wrote=true). Unlike expectEgressNATWritesAtMost it does not count the writes Azure refused.
func expectEgressAppliesWritingAtMost(egressName string, maxApplies int, why string) {
	what := "the applies that wrote egress identity " + egressName
	applies, ok := ccmLogLinesFor(ccmEgressLog(what), time.Time{}, what, `"Applied egress configuration"`,
		fmt.Sprintf("identity=%q", egressName), "wrote=true")
	if !ok {
		return
	}
	Expect(applies).NotTo(BeEmpty(), "the controller must log the applies of %s", egressName)
	Expect(len(applies)).To(BeNumerically("<=", maxApplies), "%s; applies:\n%s", why, strings.Join(applies, "\n"))
}

// expectCCMEgressWrites checks that the Azure writes to egressName's resources that the running cloud-controller-manager
// logged since the spec or it started (ccmEgressWrites) are exactly want, in any order, each given by its log message
// such as "Creating or updating NAT Gateway", where its log tells them.
func expectCCMEgressWrites(ccmClient *CCMClusterClient, egressName string, want ...string) {
	writes, ok := ccmEgressWrites(ccmClient, egressName, "the Azure writes to "+egressName)
	if !ok {
		return
	}
	matchers := make([]any, 0, len(want))
	for _, message := range want {
		matchers = append(matchers, ContainSubstring(`"`+message+`"`))
	}
	Expect(writes).To(ConsistOf(matchers...), "the Azure writes of the controller to %s since it started", egressName)
}

// azureResourceTags returns the tags of network resource id, which must exist.
func azureResourceTags(id string) map[string]string {
	resource, found, err := getAzureNetworkResource(id)
	Expect(err).NotTo(HaveOccurred())
	Expect(found).To(BeTrue(), "%s must exist", id)
	return resource.Tags
}

// userEgressResourceConfiguration returns what configures a user Public IP or prefix (tags, SKU, IP version, allocation
// method, idle timeout, address or address range and length), as JSON: what the controller must never change.
func userEgressResourceConfiguration(id string) string {
	out, err := runAz("rest", "--method", "get", "--url", armURL(id), "--query",
		"{tags:tags,sku:sku.name,version:properties.publicIPAddressVersion,allocation:properties.publicIPAllocationMethod,"+
			"idle:properties.idleTimeoutInMinutes,address:properties.ipAddress,prefix:properties.ipPrefix,length:properties.prefixLength}", "-o", "json")
	Expect(err).NotTo(HaveOccurred(), "reading user resource %s: %s", id, string(out))
	return string(out)
}

// userEgressResourceConfigurations returns the configurations of user Public IPs and prefixes
// (userEgressResourceConfiguration), by ID.
func userEgressResourceConfigurations(ids ...string) map[string]string {
	configurations := make(map[string]string, len(ids))
	for _, id := range ids {
		configurations[id] = userEgressResourceConfiguration(id)
	}
	return configurations
}

// expectUserEgressResourcesUnchanged checks that user Public IPs and prefixes still have the configurations read before
// (userEgressResourceConfigurations): the controller neither tagged nor otherwise changed them.
func expectUserEgressResourcesUnchanged(before map[string]string) {
	for id, configuration := range before {
		Expect(userEgressResourceConfiguration(id)).To(Equal(configuration), "the controller must never change user resource %s", id)
	}
}

// setNetworkResourceTag sets tag key of a network resource (a NAT Gateway, Public IP or prefix) to value out of band, as
// its owner might, keeping its other tags; an empty value removes the tag. Tag names compare case-insensitively, as in
// Azure.
func setNetworkResourceTag(resourceID, key, value string) {
	tags := map[string]string{}
	for k, v := range azureResourceTags(resourceID) {
		if !strings.EqualFold(k, key) {
			tags[k] = v
		}
	}
	if value != "" {
		tags[key] = value
	}
	body, err := json.Marshal(map[string]any{"tags": tags})
	Expect(err).NotTo(HaveOccurred())
	out, err := runAz("rest", "--method", "patch", "--url", armURL(resourceID), "--body", string(body))
	Expect(err).NotTo(HaveOccurred(), string(out))
	Eventually(func() (string, error) {
		resource, _, err := getAzureNetworkResource(resourceID)
		return fmt.Sprintf("%q/%s", tagValue(resource.Tags, key), resource.Properties.ProvisioningState), err
	}, 3*time.Minute, defaultPollInterval).Should(Equal(fmt.Sprintf("%q/Succeeded", value)), "tag %s of %s must be %q", key, resourceID, value)
}

// natGatewayConfiguration returns what configures a NAT Gateway (tags, idle timeout, Public IPs and
// prefixes), as JSON.
func natGatewayConfiguration(natGatewayID string) (string, error) {
	out, err := runAz("rest", "--method", "get", "--url", armURL(natGatewayID), "--query",
		"{tags:tags,idle:properties.idleTimeoutInMinutes,ips:properties.publicIpAddresses,ipsV6:properties.publicIpAddressesV6,"+
			"prefixes:properties.publicIpPrefixes,prefixesV6:properties.publicIpPrefixesV6}", "-o", "json")
	if err != nil {
		return "", fmt.Errorf("reading NAT Gateway %s: %w (%s)", natGatewayID, err, string(out))
	}
	return string(out), nil
}

// egressAddressSet is where egress through a NAT Gateway may leave from: its Public IPs and every
// address of its Public IP prefixes.
type egressAddressSet struct {
	ips      []net.IP
	prefixes []*net.IPNet
}

func (s egressAddressSet) contains(address string) bool {
	ip := net.ParseIP(address)
	if ip == nil {
		return false
	}
	return slices.ContainsFunc(s.ips, ip.Equal) ||
		slices.ContainsFunc(s.prefixes, func(prefix *net.IPNet) bool { return prefix.Contains(ip) })
}

func (s egressAddressSet) union(other egressAddressSet) egressAddressSet {
	return egressAddressSet{ips: slices.Concat(s.ips, other.ips), prefixes: slices.Concat(s.prefixes, other.prefixes)}
}

func (s egressAddressSet) String() string {
	parts := make([]string, 0, len(s.ips)+len(s.prefixes))
	for _, ip := range s.ips {
		parts = append(parts, ip.String())
	}
	for _, prefix := range s.prefixes {
		parts = append(parts, prefix.String())
	}
	return "[" + strings.Join(parts, " ") + "]"
}

// egressAddressesOfErr returns the addresses of Public IPs and Public IP prefixes, by ID.
func egressAddressesOfErr(ids ...string) (egressAddressSet, error) {
	var set egressAddressSet
	for _, id := range ids {
		resource, found, err := getAzureNetworkResource(id)
		switch {
		case err != nil:
			return set, err
		case !found:
			return set, fmt.Errorf("%s does not exist", id)
		case resource.Properties.IPPrefix != "":
			_, prefix, err := net.ParseCIDR(resource.Properties.IPPrefix)
			if err != nil {
				return set, fmt.Errorf("prefix %s has address range %q: %w", id, resource.Properties.IPPrefix, err)
			}
			set.prefixes = append(set.prefixes, prefix)
		default:
			ip := net.ParseIP(resource.Properties.IPAddress)
			if ip == nil {
				return set, fmt.Errorf("%s has no address yet", id)
			}
			set.ips = append(set.ips, ip)
		}
	}
	return set, nil
}

// egressAddressesOf waits until the Public IPs and prefixes ids have addresses and returns them.
func egressAddressesOf(ids ...string) egressAddressSet {
	var set egressAddressSet
	Eventually(func() (err error) {
		set, err = egressAddressesOfErr(ids...)
		return err
	}, 2*time.Minute, defaultPollInterval).Should(Succeed())
	return set
}

// egressNATAddresses returns the addresses the NAT Gateway of egressName egresses from, both IP families.
func egressNATAddresses(egressName string) egressAddressSet {
	props := readEgressNAT(egressName).Properties
	var ids []string
	for _, refs := range [][]armSubResource{props.PublicIPAddresses, props.PublicIPAddressesV6, props.PublicIPPrefixes, props.PublicIPPrefixesV6} {
		ids = append(ids, subResourceIDs(refs)...)
	}
	Expect(ids).NotTo(BeEmpty(), "NAT Gateway %s must have a public address", egressName)
	return egressAddressesOf(ids...)
}

// publicIPAddress waits until Public IP id has an address and returns it.
func publicIPAddress(id string) string {
	return egressAddressesOf(id).ips[0].String()
}

// egressProbeSample is one probe of a probe pod: when, by the pod's clock, and the IPv4 source address
// the internet saw, "" when the probe failed.
type egressProbeSample struct {
	at     time.Time
	source string
}

// egressProbeSamples returns the probes of probe pod podName so far, in order. A sample keeps its index
// as the log grows.
func egressProbeSamples(cs clientset.Interface, namespace, podName string) ([]egressProbeSample, error) {
	raw, err := cs.CoreV1().Pods(namespace).GetLogs(podName, &v1.PodLogOptions{Container: egressProbeContainer}).DoRaw(context.TODO())
	if err != nil {
		return nil, fmt.Errorf("reading the log of probe pod %s: %w", podName, err)
	}
	return parseEgressProbeLog(string(raw)), nil
}

// parseEgressProbeLog returns the samples of a probe pod's log (egressProbeScript).
func parseEgressProbeLog(log string) []egressProbeSample {
	lines := strings.Split(log, "\n")
	var samples []egressProbeSample
	for _, line := range lines[:len(lines)-1] { // the last line is still being written
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		seconds, err := strconv.ParseInt(fields[0], 10, 64)
		if err != nil {
			continue
		}
		sample := egressProbeSample{at: time.Unix(seconds, 0)}
		if ip := net.ParseIP(fields[1]); ip != nil && ip.To4() != nil {
			sample.source = ip.String()
		}
		samples = append(samples, sample)
	}
	return samples
}

// probeSources returns the sources of the successful samples from index from on, with their counts.
func probeSources(samples []egressProbeSample, from int) map[string]int {
	sources := map[string]int{}
	for _, sample := range samples[min(from, len(samples)):] {
		if sample.source != "" {
			sources[sample.source]++
		}
	}
	return sources
}

// waitForProbeEgress waits until probe pod podName's egress leaves steadily through want and returns
// the index of the first sample from which it does (steadyEgressStart). observable is false when the
// environment carries no egress traffic: the probe never reached the internet, and neither does a pod
// without the egress label. It fails the spec when the probe reaches the internet but not steadily
// through want, or cannot reach it while other pods can.
func waitForProbeEgress(cs clientset.Interface, namespace, podName string, want egressAddressSet) (first int, observable bool) {
	var samples []egressProbeSample
	var unreadableSince time.Time
	deadline := time.Now().Add(5 * time.Minute)
	for time.Now().Before(deadline) {
		latest, err := egressProbeSamples(cs, namespace, podName)
		switch {
		case err == nil:
			samples, unreadableSince = latest, time.Time{}
		case len(samples) > 0 || !podRunning(cs, namespace, podName):
			utils.Logf("%v", err)
		case unreadableSince.IsZero():
			unreadableSince = time.Now()
			utils.Logf("%v", err)
		case time.Since(unreadableSince) > egressProbeLogUnreadableLimit:
			utils.Logf("the log of running probe pod %s stayed unreadable for %v (%v): this environment does not serve "+
				"pod logs, so egress is not observed", podName, egressProbeLogUnreadableLimit, err)
			return 0, false
		}
		if first, ok := steadyEgressStart(samples, want); ok {
			utils.Logf("probe pod %s egresses through the NAT Gateway (%s)", podName, samples[first].source)
			return first, true
		}
		time.Sleep(defaultPollInterval)
	}
	if sources := probeSources(samples, 0); len(sources) > 0 {
		Fail(fmt.Sprintf("probe pod %s egressed as %v but not steadily through the NAT Gateway's addresses %v", podName, sources, want))
	}
	if podsReachInternet(cs, namespace) {
		Fail(fmt.Sprintf("probe pod %s never reached the internet although a pod without the egress label does", podName))
	}
	utils.Logf("no pod reaches the internet: this environment carries no egress traffic to observe")
	return 0, false
}

// egressProbeLogUnreadableLimit is how long waitForProbeEgress reads the log of a running probe pod that it never
// could read before it decides that the environment does not serve pod logs (as overlay dual-stack clusters do not).
const egressProbeLogUnreadableLimit = 2 * time.Minute

// podRunning reports whether pod podName is Running.
func podRunning(cs clientset.Interface, namespace, podName string) bool {
	pod, err := cs.CoreV1().Pods(namespace).Get(context.TODO(), podName, metav1.GetOptions{})
	return err == nil && pod.Status.Phase == v1.PodRunning
}

// steadyEgressStart returns the index of the first sample of a run of at least three successful probes
// through want, with no successful probe elsewhere, that spans egressSteadyWindow. This leaves out the
// probes from while the data path was being programmed; ok is false when there is no such run yet.
func steadyEgressStart(samples []egressProbeSample, want egressAddressSet) (first int, ok bool) {
	run := 0
	for i, sample := range samples {
		switch {
		case sample.source == "":
		case !want.contains(sample.source):
			run = 0
		default:
			if run == 0 {
				first = i
			}
			if run++; run >= 3 && sample.at.Sub(samples[first].at) >= egressSteadyWindow {
				return first, true
			}
		}
	}
	return 0, false
}

// egressProbePattern returns probes 3 seconds apart from start: N through 192.0.2.10, D through another address, F failed.
func egressProbePattern(start time.Time, pattern string) []egressProbeSample {
	samples := make([]egressProbeSample, 0, len(pattern))
	for i, c := range pattern {
		sample := egressProbeSample{at: start.Add(time.Duration(3*i) * time.Second)}
		switch c {
		case 'N':
			sample.source = "192.0.2.10"
		case 'D':
			sample.source = "198.51.100.7"
		}
		samples = append(samples, sample)
	}
	return samples
}

func TestSteadyEgressStart(t *testing.T) {
	nat := egressAddressSet{ips: []net.IP{net.ParseIP("192.0.2.10")}}
	for _, tc := range []struct {
		name      string
		pattern   string
		wantFirst int
		wantOK    bool
	}{
		{name: "no probes"},
		{name: "steady for the whole window", pattern: "NNNNNNNNNNN", wantOK: true},
		{name: "steady for less than the window", pattern: "NNNNNNNNNN"},
		{name: "steady for less than the window after probes elsewhere", pattern: "DDDDDDDDNNN"},
		{name: "the data path alternating restarts the run", pattern: "DDNNNDNDNNNNNNNNNNN", wantFirst: 8, wantOK: true},
		{name: "two successful probes across the window", pattern: "NFFFFFFFFFN"},
		{name: "failed probes do not restart the run", pattern: "NFFFFFFFFFNN", wantOK: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			first, ok := steadyEgressStart(egressProbePattern(time.Unix(1_700_000_000, 0), tc.pattern), nat)
			if ok != tc.wantOK || first != tc.wantFirst {
				t.Errorf("steadyEgressStart(%q) = %d, %v; want %d, %v", tc.pattern, first, ok, tc.wantFirst, tc.wantOK)
			}
		})
	}
}

func TestProbeSuccessesSince(t *testing.T) {
	nat := egressAddressSet{ips: []net.IP{net.ParseIP("192.0.2.10")}}
	start := time.Unix(1_700_000_000, 0)
	for _, tc := range []struct {
		name    string
		pattern string
		first   int
		since   time.Duration // after start
		want    int
	}{
		{name: "an interruption that began before since", pattern: "NNNNFFFF", since: 12 * time.Second},
		{name: "probes elsewhere do not count", pattern: "NNNNDDDD", since: 12 * time.Second},
		{name: "probes from since on count", pattern: "NNNNFNNF", since: 12 * time.Second, want: 2},
		{name: "a probe at since counts", pattern: "NNNNN", since: 12 * time.Second, want: 1},
		{name: "probes before sample first do not count", pattern: "NNNNN", first: 4, want: 1},
		{name: "first beyond the samples", pattern: "NN", first: 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := probeSuccessesSince(egressProbePattern(start, tc.pattern), tc.first, start.Add(tc.since), nat)
			if got != tc.want {
				t.Errorf("probeSuccessesSince(%q, %d, +%v) = %d; want %d", tc.pattern, tc.first, tc.since, got, tc.want)
			}
		})
	}
}

func TestEgressContinuityErr(t *testing.T) {
	nat := egressAddressSet{ips: []net.IP{net.ParseIP("192.0.2.10")}}
	for _, tc := range []struct {
		name    string
		pattern string
		first   int
		prefix  string // the Public IP prefix the NAT Gateway egresses through, instead of its Public IP
		wantErr string // "" for none
	}{
		{name: "continuous", pattern: "NNNNNN"},
		{name: "continuous for longer than the longest gap", pattern: "NNNNNNNNNNNNN"},
		{name: "continuous through a Public IP prefix", pattern: "NNNN", prefix: "192.0.2.8/30"},
		{name: "a probe elsewhere than a Public IP prefix", pattern: "NNDNN", prefix: "192.0.2.8/30", wantErr: "outside the NAT Gateway"},
		{name: "a probe elsewhere", pattern: "NNDNN", wantErr: "outside the NAT Gateway"},
		{name: "a probe elsewhere before sample first", pattern: "DNNNN", first: 1},
		{name: "failed probes long before sample first", pattern: "FFFFFFFFFFFNN", first: 11},
		{name: "failed probes for exactly the longest gap", pattern: "NFFFFFFFFFN"},
		{name: "an interruption longer than the longest gap", pattern: "NFFFFFFFFFFFN", wantErr: "interrupted"},
		{name: "failed probes at the end for longer than the longest gap", pattern: "NNFFFFFFFFFFF", wantErr: "interrupted"},
		{name: "first just past the samples", pattern: "NN", first: 2, wantErr: "no probe since sample"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			allowed := nat
			if tc.prefix != "" {
				_, prefix, _ := net.ParseCIDR(tc.prefix)
				allowed = egressAddressSet{prefixes: []*net.IPNet{prefix}}
			}
			err := egressContinuityErr(egressProbePattern(time.Unix(1_700_000_000, 0), tc.pattern), tc.first, allowed)
			switch {
			case tc.wantErr == "" && err != nil:
				t.Errorf("egressContinuityErr(%q, %d) = %v; want nil", tc.pattern, tc.first, err)
			case tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)):
				t.Errorf("egressContinuityErr(%q, %d) = %v; want an error containing %q", tc.pattern, tc.first, err, tc.wantErr)
			}
		})
	}
}

// podsReachInternet reports whether a pod without the egress label, which egresses through the cluster's
// default outbound path, reaches the internet: whether the environment carries egress traffic at all.
func podsReachInternet(cs clientset.Interface, namespace string) bool {
	pod := utils.CreatePodGetIPManifest()
	pod.Name = "egress-internet-check"
	Expect(utils.CreatePod(cs, namespace, pod)).To(Succeed())
	defer func() {
		_ = cs.CoreV1().Pods(namespace).Delete(context.TODO(), pod.Name, metav1.DeleteOptions{})
	}()
	ip, err := utils.GetPodOutboundIP(cs, pod, namespace)
	return err == nil && ip != ""
}

// eventuallyProbeSuccesses waits until probe pod podName has probed successfully count times since
// sample from.
func eventuallyProbeSuccesses(cs clientset.Interface, namespace, podName string, from, count int) {
	Eventually(func() (int, error) {
		samples, err := egressProbeSamples(cs, namespace, podName)
		total := 0
		for _, n := range probeSources(samples, from) {
			total += n
		}
		return total, err
	}, 3*time.Minute, defaultPollInterval).Should(BeNumerically(">=", count), "probe pod %s must keep reaching the internet", podName)
}

// eventuallyProbeSources waits until probe pod podName's egress has left through each of sets since
// sample from.
func eventuallyProbeSources(cs clientset.Interface, namespace, podName string, from int, sets ...egressAddressSet) {
	Eventually(func() error {
		samples, err := egressProbeSamples(cs, namespace, podName)
		if err != nil {
			return err
		}
		sources := probeSources(samples, from)
		for _, set := range sets {
			if !slices.ContainsFunc(slices.Collect(maps.Keys(sources)), set.contains) {
				return fmt.Errorf("probe pod %s egressed as %v, not yet through %v", podName, sources, set)
			}
		}
		return nil
	}, 5*time.Minute, defaultPollInterval).Should(Succeed())
}

// egressContinuityErr checks samples from index first on: every successful probe left through allowed,
// and no two successful probes, nor the last one and the last sample, are more than egressMaxProbeGap
// apart.
func egressContinuityErr(samples []egressProbeSample, first int, allowed egressAddressSet) error {
	if first >= len(samples) {
		return fmt.Errorf("no probe since sample %d", first)
	}
	var outside []string
	last, worst, worstFrom := samples[first].at, time.Duration(0), samples[first].at
	for _, sample := range samples[first:] {
		if sample.source == "" {
			continue
		}
		if !allowed.contains(sample.source) {
			outside = append(outside, sample.source+" at "+sample.at.UTC().Format(time.TimeOnly))
		}
		if gap := sample.at.Sub(last); gap > worst {
			worst, worstFrom = gap, last
		}
		last = sample.at
	}
	if gap := samples[len(samples)-1].at.Sub(last); gap > worst {
		worst, worstFrom = gap, last
	}
	switch {
	case len(outside) > 0:
		return fmt.Errorf("egress left through addresses outside the NAT Gateway's %v: %v", allowed, outside[:min(len(outside), 10)])
	case worst > egressMaxProbeGap:
		return fmt.Errorf("egress was interrupted for %v from %s UTC (no successful probe for more than %v)",
			worst, worstFrom.UTC().Format(time.TimeOnly), egressMaxProbeGap)
	}
	return nil
}

// expectEgressContinuity checks probe pod podName's egress since sample first with egressContinuityErr, once the probe
// has egressed through allowed again after this call, so an interruption that began just before cannot hide among the
// last samples.
func expectEgressContinuity(cs clientset.Interface, namespace, podName string, first int, allowed egressAddressSet) {
	since := time.Now()
	var samples []egressProbeSample
	Eventually(func() error {
		var err error
		if samples, err = egressProbeSamples(cs, namespace, podName); err != nil {
			return err
		}
		if n := probeSuccessesSince(samples, first, since, allowed); n < 2 {
			return fmt.Errorf("probe pod %s egressed through %v %d times since %s UTC", podName, allowed, n, since.UTC().Format(time.TimeOnly))
		}
		return nil
	}, 2*egressMaxProbeGap, defaultPollInterval).Should(Succeed(), "egress of probe pod %s must continue through the NAT Gateway", podName)
	Expect(egressContinuityErr(samples, first, allowed)).To(Succeed(), "egress of probe pod %s must continue through the NAT Gateway", podName)
	utils.Logf("probe pod %s egressed continuously through %v: %v", podName, allowed, probeSources(samples, first))
}

// probeSuccessesSince returns the number of successful probes through allowed from sample first on that the probe pod
// took at or after since.
func probeSuccessesSince(samples []egressProbeSample, first int, since time.Time, allowed egressAddressSet) int {
	n := 0
	for _, sample := range samples[min(first, len(samples)):] {
		if !sample.at.Before(since) && allowed.contains(sample.source) {
			n++
		}
	}
	return n
}

// expectProbeSourcesGone checks that probe pod podName's egress keeps working without leaving through
// gone any more: after egressDataplaneSettle it needs 10 successful probes, none of them through gone.
func expectProbeSourcesGone(cs clientset.Interface, namespace, podName string, gone egressAddressSet) {
	time.Sleep(egressDataplaneSettle)
	from := egressProbeSampleCount(cs, namespace, podName)
	eventuallyProbeSuccesses(cs, namespace, podName, from, 10)
	samples, err := egressProbeSamples(cs, namespace, podName)
	Expect(err).NotTo(HaveOccurred())
	for source := range probeSources(samples, from) {
		Expect(gone.contains(source)).To(BeFalse(), "probe pod %s still egressed as %s after %v was deleted", podName, source, gone)
	}
}

// egressProbeSampleCount returns how many probes probe pod podName has made so far: the index its next
// probe will have.
func egressProbeSampleCount(cs clientset.Interface, namespace, podName string) int {
	samples, err := egressProbeSamples(cs, namespace, podName)
	Expect(err).NotTo(HaveOccurred())
	return len(samples)
}

// leaveOutEgressUnlessObservable leaves out the egress of a spec when it could not be observed (leaveOut).
func leaveOutEgressUnlessObservable(observable bool) {
	if !observable {
		leaveOut("egress through the NAT Gateway (this environment carries no egress traffic, as no pod reaches the internet, " +
			"or does not serve pod logs)")
	}
}

// observeIPv6Egress checks, where the environment carries IPv6 egress, that pod podName's IPv6 egress leaves through
// addresses. For a while after an identity is registered its data path alternates with the cluster's default outbound
// path, so it samples the egress every 3 seconds, for up to twice egressSteadyWindow, until it left through addresses
// steadily (steadyEgressStart), and fails only when it still leaves elsewhere then. IPv6 internet egress is not
// available on every cluster: where none is ever observed, the IPv6 configuration is verified on the control plane only
// and the IPv6 egress left out (leaveOut).
func observeIPv6Egress(namespace, podName string, addresses egressAddressSet) {
	var samples []egressProbeSample
	last := ""
	for deadline := time.Now().Add(2 * egressSteadyWindow); ; time.Sleep(3 * time.Second) {
		out, _ := utils.RunKubectl(namespace, "exec", podName, "--", "/bin/sh", "-c", "curl -6 -fs -m 10 https://ifconfig.co/ip || true")
		sample := egressProbeSample{at: time.Now(), source: ipv6EgressSource(out)}
		if sample.source != "" {
			last = sample.source
		}
		samples = append(samples, sample)
		if first, steady := steadyEgressStart(samples, addresses); steady {
			utils.Logf("IPv6 egress of pod %s leaves steadily through the NAT Gateway (%s)", podName, samples[first].source)
			return
		}
		if time.Now().After(deadline) {
			break
		}
	}
	switch {
	case last == "":
		leaveOut(fmt.Sprintf("IPv6 egress through the NAT Gateway (none observable from pod %s)", podName))
	case !addresses.contains(last):
		Fail(fmt.Sprintf("IPv6 egress of pod %s still leaves as %s after %s, not through the NAT Gateway's addresses %v (sources: %v)",
			podName, last, 2*egressSteadyWindow, addresses, probeSources(samples, 0)))
	default:
		utils.Logf("IPv6 egress of pod %s leaves through the NAT Gateway (%s) at last, though not steadily for %s (sources: %v)",
			podName, last, egressSteadyWindow, probeSources(samples, 0))
	}
}

// ipv6EgressSource returns the IPv6 address that out, the output of utils.RunKubectl running curl against an echo
// service, reports as the source of the pod's egress, or "" when kubectl's stdout holds none: curl failed, or answered
// with an IPv4 address or anything else. RunKubectl returns "stdout:<stdout>\nstderr:<stderr>"; only stdout counts.
func ipv6EgressSource(out string) string {
	stdout, _, _ := strings.Cut(strings.TrimPrefix(out, "stdout:"), "\nstderr:")
	ip := net.ParseIP(strings.TrimSpace(stdout))
	if ip == nil || ip.To4() != nil {
		return ""
	}
	return ip.String()
}

func TestIPv6EgressSource(t *testing.T) {
	// What utils.RunKubectl returns (utils.KubectlBuilder.Exec).
	runKubectlOutput := func(stdout, stderr string) string { return fmt.Sprintf("stdout:%s\nstderr:%s", stdout, stderr) }
	for _, tc := range []struct {
		name, out, want string
	}{
		{"IPv6", runKubectlOutput("2001:db8::1\n", ""), "2001:db8::1"},
		{"IPv6 of Azure", runKubectlOutput("2603:1030:20e:3::2b1\n", ""), "2603:1030:20e:3::2b1"},
		{"unique local IPv6", runKubectlOutput("fd00:1234::5\n", ""), "fd00:1234::5"},
		{"IPv6 with stderr noise", runKubectlOutput("2001:db8::1\n", `Defaulted container "test-app" out of: test-app`+"\n"), "2001:db8::1"},
		{"IPv4 only", runKubectlOutput("203.0.113.7\n", ""), ""},
		{"IPv4-mapped IPv6", runKubectlOutput("::ffff:203.0.113.7\n", ""), ""},
		{"empty", runKubectlOutput("", ""), ""},
		{"garbage", runKubectlOutput("<html><body>rate limited</body></html>\n", ""), ""},
		{"IPv6 in stderr only", runKubectlOutput("", "curl: (7) Failed to connect to 2001:db8::1 port 443: Network is unreachable\n"), ""},
		{"exec failed", runKubectlOutput("", `error: unable to upgrade connection: container not found ("test-app")`+"\n"), ""},
	} {
		if got := ipv6EgressSource(tc.out); got != tc.want {
			t.Errorf("%s: ipv6EgressSource(%q) = %q, want %q", tc.name, tc.out, got, tc.want)
		}
	}
}

// clusterIdentityObjectIDs returns the object IDs listed in byoAccessIdentitiesEnvVar.
func clusterIdentityObjectIDs() []string {
	var ids []string
	for _, id := range strings.Split(os.Getenv(byoAccessIdentitiesEnvVar), ",") {
		if id = strings.TrimSpace(id); id != "" {
			ids = append(ids, id)
		}
	}
	return ids
}

// publicIPID returns the ID of the Public IP named name in resource group rg, which need not exist.
func publicIPID(rg, name string) string {
	return fmt.Sprintf("/subscriptions/%s/resourceGroups/%s/providers/Microsoft.Network/publicIPAddresses/%s", subscriptionID, rg, name)
}

// publicIPPrefixID returns the ID of the Public IP prefix named name in resource group rg, which need not exist.
func publicIPPrefixID(rg, name string) string {
	return fmt.Sprintf("/subscriptions/%s/resourceGroups/%s/providers/Microsoft.Network/publicIPPrefixes/%s", subscriptionID, rg, name)
}

// createUserPublicIP creates a static IPv4 Public IP of sku with tags in resource group rg and location, as its user
// would, that is deleted after the spec, and returns its ID, or Azure's refusal to create it.
func createUserPublicIP(rg, name, sku, location string, tags map[string]string) (string, error) {
	id := publicIPID(rg, name)
	resource := map[string]any{
		"location":   location,
		"sku":        map[string]string{"name": sku},
		"properties": map[string]string{"publicIPAllocationMethod": "Static", "publicIPAddressVersion": "IPv4"},
	}
	if len(tags) > 0 {
		resource["tags"] = tags
	}
	body, err := json.Marshal(resource)
	Expect(err).NotTo(HaveOccurred())
	// Registered first, so a failed create is still cleaned up (deleting a missing resource succeeds).
	DeferCleanup(deleteBYOResource, id)
	if out, err := runAz("rest", "--method", "put", "--url", armURL(id), "--body", string(body)); err != nil {
		return "", fmt.Errorf("creating Public IP %s: %w: %s", id, err, string(out))
	}
	Eventually(func() (string, error) {
		out, err := runAz("rest", "--method", "get", "--url", armURL(id), "--query", "properties.provisioningState", "-o", "tsv")
		return strings.TrimSpace(string(out)), err
	}, 3*time.Minute, defaultPollInterval).Should(Equal("Succeeded"), "Public IP %s should be provisioned", id)
	return id, nil
}

// outOfBandResource is a StandardV2 Public IP, or a Public IP prefix when prefixLength is set, that
// createOutOfBandResources creates in the cluster resource group.
type outOfBandResource struct {
	name, version string
	prefixLength  int
	// owner is the egress identity whose ownership tag the resource carries, "" for none; tags are its other tags.
	owner string
	tags  map[string]string
}

// createOutOfBandResources creates Public IPs and prefixes in the cluster resource group out of band, as an older
// controller leaves them behind or someone else creates them at reserved names, all at once, and waits until they are
// provisioned. It returns their IDs. Each is deleted after the spec unless the controller or the spec deletes it first.
func createOutOfBandResources(resources ...outOfBandResource) []string {
	location := clusterLocation()
	ids := make([]string, 0, len(resources))
	for _, resource := range resources {
		id := publicIPID(resourceGroupName, resource.name)
		properties := map[string]any{"publicIPAllocationMethod": "Static", "publicIPAddressVersion": resource.version}
		if resource.prefixLength > 0 {
			id = publicIPPrefixID(resourceGroupName, resource.name)
			properties = map[string]any{"prefixLength": resource.prefixLength, "publicIPAddressVersion": resource.version}
		}
		tags := maps.Clone(resource.tags)
		if tags == nil {
			tags = map[string]string{}
		}
		if resource.owner != "" {
			tags[egressIdentityTagKey] = resource.owner
		}
		body, err := json.Marshal(map[string]any{"location": location, "sku": map[string]string{"name": "StandardV2"}, "tags": tags, "properties": properties})
		Expect(err).NotTo(HaveOccurred())
		// Registered first, so a failed create is still cleaned up (deleting a missing resource succeeds).
		DeferCleanup(deleteBYOResource, id)
		out, err := runAz("rest", "--method", "put", "--url", armURL(id), "--body", string(body))
		Expect(err).NotTo(HaveOccurred(), string(out))
		ids = append(ids, id)
	}
	Eventually(func() error {
		for _, id := range ids {
			resource, found, err := getAzureNetworkResource(id)
			switch {
			case err != nil:
				return err
			case !found:
				return fmt.Errorf("%s does not exist yet", resourceNameFromID(id))
			case resource.Properties.ProvisioningState != "Succeeded":
				return fmt.Errorf("%s is %s", resourceNameFromID(id), resource.Properties.ProvisioningState)
			}
		}
		return nil
	}, 3*time.Minute, defaultPollInterval).Should(Succeed(), "the resources created out of band must be provisioned")
	return ids
}

// createEgressUserResourceGroup creates a resource group for user Public IPs and prefixes (createEgressResourceGroup).
// The controller reads and attaches user resources with its own identity: the identities listed in
// byoAccessIdentitiesEnvVar get Network Contributor on the resource group, and granted reports that.
func createEgressUserResourceGroup(suffix string) (rg string, granted bool) {
	rg = createEgressResourceGroup("e2e-sgw-egress-" + suffix)
	ids := clusterIdentityObjectIDs()
	grantEgressNetworkContributor(rg, ids)
	return rg, len(ids) > 0
}

// createEgressResourceGroup creates resource group rg for user Public IPs and prefixes, in the cluster's subscription
// and location and never the node resource group, and deletes it after the spec, after the resources created in it.
// The spec skips when the test identity cannot create resource groups.
func createEgressResourceGroup(rg string) string {
	// Registered first, so a failed create is still cleaned up. A group that was never created is not deleted, which a
	// test identity that may not create one could not do, nor even tell that it does not exist.
	refused := false
	DeferCleanup(func() {
		if refused {
			return
		}
		if out, err := runAz("group", "exists", "--subscription", subscriptionID, "--name", rg); err == nil && strings.TrimSpace(string(out)) == "false" {
			return
		}
		out, err := runAz("group", "delete", "--subscription", subscriptionID, "--name", rg, "--yes")
		Expect(err).NotTo(HaveOccurred(), "deleting test resource group %s: %s", rg, string(out))
	})
	out, err := runAz("group", "create", "--subscription", subscriptionID, "--name", rg, "--location", clusterLocation(),
		"--tags", "purpose=cloud-provider-azure-e2e", "--output", "none")
	if err != nil && strings.Contains(string(out), "AuthorizationFailed") {
		refused = true
		Skip(fmt.Sprintf("the test identity cannot create resource group %s for user Public IPs and prefixes: %s", rg, string(out)))
	}
	Expect(err).NotTo(HaveOccurred(), string(out))
	return rg
}

// grantEgressNetworkContributor gives each of the identities ids Network Contributor on resource group rg, until the
// spec ends.
func grantEgressNetworkContributor(rg string, ids []string) {
	scope := fmt.Sprintf("/subscriptions/%s/resourceGroups/%s", subscriptionID, rg)
	for _, id := range ids {
		DeferCleanup(func() {
			_, _ = runAz("role", "assignment", "delete", "--assignee", id, "--role", "Network Contributor", "--scope", scope)
		})
		out, err := runAz("role", "assignment", "create", "--assignee-object-id", id, "--assignee-principal-type", "ServicePrincipal",
			"--role", "Network Contributor", "--scope", scope)
		Expect(err).NotTo(HaveOccurred(), string(out))
	}
}

// egressAccessDenial returns the message of an EgressConfigRejected event of one of podNames that says the controller
// was refused access to user resources ("access denied" or "HTTP 403"), or "" when there is none.
func egressAccessDenial(cs clientset.Interface, namespace string, podNames []string) string {
	byPod, err := egressPodEvents(cs, namespace, consts.ServiceEgressGatewayReasonConfigRejected)
	if err != nil {
		return ""
	}
	for _, name := range podNames {
		for _, e := range byPod[name] {
			if strings.Contains(e.Message, "access denied") || strings.Contains(e.Message, "HTTP 403") {
				return e.Message
			}
		}
	}
	return ""
}

// skipIfEgressUserResourcesDenied skips the spec when the controller rejected the configuration of
// podNames because its identity may not read or attach the user resources in rg, unless the spec
// granted it access (granted), in which case the controller retries until the role assignment applies.
func skipIfEgressUserResourcesDenied(cs clientset.Interface, namespace string, podNames []string, rg string, granted bool) {
	if granted {
		return
	}
	if denial := egressAccessDenial(cs, namespace, podNames); denial != "" {
		Skip(fmt.Sprintf("the controller may not use the user resources in resource group %s (%s); set %s to the object IDs "+
			"of its identities so that the spec grants them Network Contributor there", rg, denial, byoAccessIdentitiesEnvVar))
	}
}

// eventuallyEgressRejection waits until every pod of podNames was told once that the configuration it votes for was
// rejected, with every one of wants in the message. It skips the spec when the controller was refused access to the user
// resources in rg instead: its identities may not use them, or the role createEgressUserResourceGroup assigned is not
// applied yet. The pods were then told a retried rejection for the denial, so a retried rejection the spec waits for is
// not told again (its reason changes only), and a final one is told only once access applies, after the tries and the
// events the spec checks for the controller's first tries.
func eventuallyEgressRejection(cs clientset.Interface, namespace string, podNames []string, rg string, wants ...string) {
	Eventually(func() error {
		if denial := egressAccessDenial(cs, namespace, podNames); denial != "" {
			Skip(fmt.Sprintf("the controller was refused access to the user resources in resource group %s (%s), so the rejection "+
				"this spec waits for is not told as on the controller's first tries", rg, denial))
		}
		return eachPodEventErr(cs, namespace, podNames, consts.ServiceEgressGatewayReasonConfigRejected, v1.EventTypeWarning, wants...)
	}, 3*time.Minute, defaultPollInterval).Should(Succeed())
	eventuallyEgressEventCounts(cs, namespace, consts.ServiceEgressGatewayReasonConfigRejected, v1.EventTypeWarning,
		egressCountsOf(podNames, 1), wants...)
}

// eventuallyEgressNATWithUserResources is eventuallyEgressNAT for a configuration that references user
// resources in rg (see createEgressUserResourceGroup).
func eventuallyEgressNATWithUserResources(cs clientset.Interface, namespace string, podNames []string, egressName string,
	want egressNATWant, rg string, granted bool) {
	timeout := egressApplyTimeout
	if granted {
		// A new role assignment can take minutes to apply; the rejected configuration is retried with
		// backoff meanwhile, every 5 minutes at the latest.
		timeout = 15 * time.Minute
	}
	Eventually(func() error {
		skipIfEgressUserResourcesDenied(cs, namespace, podNames, rg, granted)
		return egressNATShapeErr(egressName, want)
	}, timeout, defaultPollInterval).Should(Succeed(), "the NAT Gateway of %s must use the user resources its pods ask for", egressName)
}

// expectUserEgressResourcesReleased checks that user Public IPs or prefixes still exist, attached to no
// NAT Gateway: the controller never deletes them, and deleting the NAT Gateway releases them.
func expectUserEgressResourcesReleased(ids ...string) {
	Eventually(func() error {
		for _, id := range ids {
			resource, found, err := getAzureNetworkResource(id)
			switch {
			case err != nil:
				return err
			case !found:
				Fail(fmt.Sprintf("user resource %s was deleted; the controller must never delete it", id))
			case resource.Properties.NatGateway != nil:
				return fmt.Errorf("user resource %s is still attached to %s", id, resource.Properties.NatGateway.ID)
			}
		}
		return nil
	}, 3*time.Minute, defaultPollInterval).Should(Succeed())
}
