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
	"fmt"
	"maps"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"

	"sigs.k8s.io/cloud-provider-azure/pkg/consts"
	"sigs.k8s.io/cloud-provider-azure/tests/e2e/utils"
)

var _ = Describe("Container Load Balancer Outbound (NAT Gateway)", Label(slbTestLabel, slbEgressConfigTestLabel), func() {
	basename := "slb-egress-restart"

	var (
		cs clientset.Interface
		ns *v1.Namespace
		// Set by a spec that scales the CCM down (cleanUpEgressConfigSpec).
		ccmToRestore *CCMClusterClient
		// Set by a spec that runs the CCM on another image, to the image under test: AfterEach switches back to it
		// first, so that the cleanup runs on the controller under test.
		ccmImageToRestore string
	)

	BeforeEach(func() {
		cs, ns = newEgressConfigSpec(basename)
	})

	AfterEach(func() {
		if image := ccmImageToRestore; image != "" {
			ccmImageToRestore = ""
			if err := switchCCMImageErr(image); err != nil {
				utils.Logf("could not switch the cloud-controller-manager back to %s: %v", image, err)
			}
		}
		cleanUpEgressConfigSpec(cs, ns, ccmToRestore)
		cs, ns, ccmToRestore = nil, nil, nil
	})

	// A new controller process adopts the egress identities it finds in Azure. These specs restart, crash, stop or
	// replace the cloud-controller-manager and check that the new process writes and announces nothing that did not
	// change, corrects out-of-band changes only when the pods agree, and keeps retrying a rejected configuration without
	// holding up anything else; and that an image without this feature leaves nothing that the feature cannot clean up.
	Context("egress NAT Gateway configuration: controller restarts and crashes", func() {
		var (
			suffix string
			ipv6   bool
		)

		BeforeEach(func() {
			suffix = ns.Name[len(ns.Name)-5:]
			ipv6 = clusterHasIPv6Node(cs)
		})

		It("should neither write nor announce anything at a restart or a crash, whatever the configuration and votes of its identities",
			Label(slbInitTestLabel, slbCrashTestLabel, slbCCMRestartTestLabel, slbEgressConfigFaultTestLabel), func() {
				ccmClient := ccmClientOrSkip("restarts and crashes the cloud-controller-manager")
				full, conflict, invalid := "egcfg-rst-full-"+suffix, "egcfg-rst-conflict-"+suffix, "egcfg-rst-invalid-"+suffix
				rejected, legacy := "egcfg-rst-rejected-"+suffix, "egcfg-rst-legacy-"+suffix
				identities := []string{full, conflict, invalid, rejected, legacy}
				idleKey := consts.PodAnnotationServiceEgressGatewayIdleTimeout

				By("Creating a StandardV2 Public IP, a StandardV2 /30 prefix and a Standard Public IP in a test resource group")
				rg, granted := createEgressUserResourceGroup(suffix)
				userPublicIP := createBYOPublicIP(rg, "egcfg-rst-pip", "IPv4")
				userPrefix := createBYOPublicIPPrefix(rg, "egcfg-rst-prefix", "IPv4", 30)
				standardPublicIP, err := createUserPublicIP(rg, "egcfg-rst-standard", "Standard", clusterLocation(), nil)
				Expect(err).NotTo(HaveOccurred())

				By("Creating an identity whose pods ask for idle timeout 7, two managed Public IPs, a managed /31 prefix, " +
					"the user Public IP and prefix and two tags")
				fullAnnotations := map[string]string{
					idleKey: "7",
					consts.PodAnnotationServiceEgressGatewayPublicIPCount:        "2",
					consts.PodAnnotationServiceEgressGatewayPublicIPPrefixLength: "31",
					consts.PodAnnotationServiceEgressGatewayPublicIPIDs:          userPublicIP,
					consts.PodAnnotationServiceEgressGatewayPublicIPPrefixIDs:    userPrefix,
					consts.PodAnnotationServiceEgressGatewayTags:                 "e2e-team=restart,e2e-env=test",
				}
				fullWant := egressNATWant{
					idleTimeout: 7,
					publicIPs:   append(egressManagedPublicIPIDs(full, false, 2), userPublicIP),
					prefixes:    []string{egressManagedPrefixID(full, false, 31), userPrefix},
					tags:        map[string]string{"e2e-team": "restart", "e2e-env": "test"},
				}
				if ipv6 {
					fullAnnotations[consts.PodAnnotationServiceEgressGatewayPublicIPCountIPv6] = "2"
					fullAnnotations[consts.PodAnnotationServiceEgressGatewayPublicIPPrefixLengthIPv6] = "127"
					fullWant.publicIPsV6 = egressManagedPublicIPIDs(full, true, 2)
					fullWant.prefixesV6 = []string{egressManagedPrefixID(full, true, 127)}
				}
				fullPods := createConfiguredEgressPods(cs, ns.Name, full, 2, fullAnnotations, false)

				By("Creating the first pod of two identities (one asks for idle timeout 5, the other for nothing) and an identity without annotations")
				conflictPods := createConfiguredEgressPods(cs, ns.Name, conflict, 1, map[string]string{idleKey: "5"}, false)
				invalidPods := createConfiguredEgressPods(cs, ns.Name, invalid, 1, nil, false)
				createConfiguredEgressPods(cs, ns.Name, legacy, 2, nil, false)
				Expect(utils.WaitPodsToBeReady(cs, ns.Name)).To(Succeed())
				eventuallyEgressNATWithUserResources(cs, ns.Name, fullPods, full, fullWant, rg, granted)
				eventuallyEachPodAppliedWithUserResources(cs, ns.Name, fullPods, "idle timeout 7m",
					"IPv4: 2 managed IPs, managed prefix /31, user IPs (egcfg-rst-pip), user prefixes (egcfg-rst-prefix)",
					"tags: e2e-env=test,e2e-team=restart")
				conflictWant := defaultEgressNATWant(conflict, ipv6)
				conflictWant.idleTimeout = 5
				eventuallyEgressNAT(conflict, conflictWant)
				eventuallyEgressNAT(invalid, defaultEgressNATWant(invalid, ipv6))
				eventuallyEgressNAT(legacy, defaultEgressNATWant(legacy, ipv6))

				By("Creating, now that the controller uses the test resource group, an identity whose pods agree on the Standard Public IP: " +
					"the configuration is rejected as final and the identity created with the default")
				rejectedPods := createConfiguredEgressPods(cs, ns.Name, rejected, 2,
					map[string]string{consts.PodAnnotationServiceEgressGatewayPublicIPIDs: standardPublicIP}, false)
				Expect(utils.WaitPodsToBeReady(cs, ns.Name)).To(Succeed())
				eventuallyEgressNAT(rejected, defaultEgressNATWant(rejected, ipv6))
				eventuallyEgressRejection(cs, ns.Name, rejectedPods, rg, `has SKU "Standard"`, egressRejectedForGood)

				By("Adding a pod that asks for idle timeout 6 to the second identity and one with an invalid idle timeout to the third")
				conflictPods = append(conflictPods, createEgressConfigPod(cs, newEgressConfigPod(ns.Name, conflict, 1, map[string]string{idleKey: "6"}, false)))
				invalidPods = append(invalidPods, createEgressConfigPod(cs, newEgressConfigPod(ns.Name, invalid, 1, map[string]string{idleKey: "1000"}, false)))
				Expect(utils.WaitPodsToBeReady(cs, ns.Name)).To(Succeed())
				eventuallyPodEvent(cs, ns.Name, conflictPods[1:], consts.ServiceEgressGatewayReasonConfigConflict, v1.EventTypeWarning, "idle timeout 6m")
				eventuallyPodEvent(cs, ns.Name, invalidPods[1:], consts.ServiceEgressGatewayReasonInvalidConfig, v1.EventTypeWarning,
					`idle-timeout: "1000" is not an integer from 4 to 120`)
				for _, id := range identities {
					eventuallyManagedRegistered(cs, ns.Name, id, byoNATGatewayARMID(resourceGroupName, id))
				}

				By("Removing the ownership tag from the last identity's NAT Gateway and Public IPs, as on an identity created before the tag")
				legacyResources := append([]string{byoNATGatewayARMID(resourceGroupName, legacy)}, egressManagedPublicIPIDs(legacy, false, 1)...)
				if ipv6 {
					legacyResources = append(legacyResources, egressManagedPublicIPIDs(legacy, true, 1)...)
				}
				for _, id := range legacyResources {
					setNetworkResourceTag(id, egressIdentityTagKey, "")
				}

				By("Recording the etags of the NAT Gateways and of the managed and user resources, and the events")
				etags := map[string]string{}
				for _, id := range identities {
					maps.Copy(etags, egressResourceEtags(id))
				}
				maps.Copy(etags, azureResourceEtags(userPublicIP, userPrefix, standardPublicIP))
				Consistently(func() error { return egressEtagsUnchangedErr(etags) }, 30*time.Second, defaultPollInterval).Should(Succeed(),
					"precondition: the controller has finished writing the identities' resources")
				events := egressConfigEventCounts(cs, ns.Name)

				expectNothingWrittenOrAnnounced := func() {
					observe := waitForCCMInitialization(ccmClient)
					Consistently(func() error { return egressEtagsUnchangedErr(etags) }, observe, defaultPollInterval).Should(Succeed(),
						"a restarted controller must not write the NAT Gateways and resources of identities it leaves as they are")
					Expect(egressConfigEventCounts(cs, ns.Name)).To(Equal(events), "a restart must announce nothing again")
					for _, id := range identities {
						if writes, ok := ccmEgressWrites(ccmClient, id, "the restarted controller's writes to "+id); ok {
							Expect(writes).To(BeEmpty(), "the restarted controller must not write the resources of %s, not even without changing them", id)
						}
					}
					// Today's behaviour (the startup baseline of decision H): the new process tries the rejected configuration
					// once more and rejects it again before any write, without telling the pods again. Its log starts with it.
					if rejections, ok := ccmLogLinesFor(ccmClient, time.Time{}, "the restarted controller's tries of the configuration of "+rejected,
						`"Rejected egress configuration"`, fmt.Sprintf("identity=%q", rejected), "retryable=false"); ok {
						Expect(rejections).To(HaveLen(1), "the restarted controller must try the configuration it rejected once more, and only once")
					}
				}

				By("Stopping and starting the cloud-controller-manager: nothing is written or announced, the rejected configuration is tried again")
				ccmToRestore = ccmClient
				expectNoCCMCrash(egressSpecStart)
				Expect(scaleCCMDeployment(context.TODO(), ccmClient, 0)).To(Succeed())
				Expect(waitForCCMFullyDown(context.TODO(), ccmClient, GetCCMRecoveryTimeout())).To(Succeed())
				Expect(scaleCCMDeployment(context.TODO(), ccmClient, 1)).To(Succeed())
				Expect(waitForCCMFullyUp(context.TODO(), ccmClient, GetCCMRecoveryTimeout())).To(Succeed())
				ccmToRestore = nil
				expectNothingWrittenOrAnnounced()

				By("Crashing the cloud-controller-manager: again nothing is written or announced")
				expectNoCCMCrash(egressSpecStart)
				Expect(ccmClient.CrashCCMAndWaitForRecovery(context.TODO(), GetCCMRecoveryTimeout())).To(Succeed())
				expectNothingWrittenOrAnnounced()

				By("Deleting the pods of the identity without ownership tags: its NAT Gateway and Public IPs are deleted")
				deleteEgressPods(cs, ns.Name, legacy)
				eventuallyEgressIdentityGone(legacy)

				By("Deleting the other pods: the NAT Gateways and managed resources are deleted, the user resources kept")
				for _, id := range identities[:4] {
					deleteEgressPods(cs, ns.Name, id)
				}
				for _, id := range identities[:4] {
					eventuallyEgressIdentityGone(id)
				}
				expectUserEgressResourcesReleased(userPublicIP, userPrefix, standardPublicIP)
			})

		It("should correct a NAT Gateway changed out of band when it starts only while the pods agree on a configuration",
			Label(slbInitTestLabel, slbCCMRestartTestLabel, slbEgressConfigFaultTestLabel), func() {
				ccmClient := ccmClientOrSkip("stops the cloud-controller-manager")
				egressName := "egcfg-rst-drift-" + suffix
				natGatewayID := byoNATGatewayARMID(resourceGroupName, egressName)
				idleKey := consts.PodAnnotationServiceEgressGatewayIdleTimeout

				By("Creating two pods that ask for idle timeout 6 and two managed Public IPs")
				pods := createConfiguredEgressPods(cs, ns.Name, egressName, 2, map[string]string{
					idleKey: "6",
					consts.PodAnnotationServiceEgressGatewayPublicIPCount: "2",
				}, false)
				Expect(utils.WaitPodsToBeReady(cs, ns.Name)).To(Succeed())
				want := defaultEgressNATWant(egressName, ipv6)
				want.idleTimeout = 6
				want.publicIPs = egressManagedPublicIPIDs(egressName, false, 2)
				eventuallyEgressNAT(egressName, want)
				eventuallyManagedRegistered(cs, ns.Name, egressName, natGatewayID)
				eventuallyEachPodApplied(cs, ns.Name, pods, "idle timeout 6m", "IPv4: 2 managed IPs")
				natBefore := readEgressNAT(egressName)
				secondAddress := publicIPAddress(want.publicIPs[1])
				applied, err := egressEventCounts(cs, ns.Name, pods, consts.ServiceEgressGatewayReasonConfigApplied, "")
				Expect(err).NotTo(HaveOccurred())

				By("Stopping the cloud-controller-manager, then setting idle timeout 9 and detaching <id>-pip-2 out of band")
				ccmToRestore = ccmClient
				expectNoCCMCrash(egressSpecStart)
				Expect(scaleCCMDeployment(context.TODO(), ccmClient, 0)).To(Succeed())
				Expect(waitForCCMFullyDown(context.TODO(), ccmClient, GetCCMRecoveryTimeout())).To(Succeed())
				rewriteNATGatewayOutOfBand(natGatewayID, func(properties map[string]any) {
					properties["idleTimeoutInMinutes"] = 9
					properties["publicIpAddresses"] = []map[string]string{{"id": want.publicIPs[0]}}
				})
				drifted := readEgressNAT(egressName)
				Expect(egressIdleTimeout(drifted)).To(Equal(9))
				Expect(subResourceIDs(drifted.Properties.PublicIPAddresses)).To(HaveLen(1), "<id>-pip-2 must be detached")
				Eventually(func() (bool, error) {
					pip, found, err := getAzureNetworkResource(want.publicIPs[1])
					return found && pip.Properties.NatGateway == nil, err
				}, time.Minute, defaultPollInterval).Should(BeTrue(), "<id>-pip-2 must exist, attached to nothing")

				By("Starting the cloud-controller-manager: the agreed configuration is applied again in place, with <id>-pip-2 and its address")
				Expect(scaleCCMDeployment(context.TODO(), ccmClient, 1)).To(Succeed())
				Expect(waitForCCMFullyUp(context.TODO(), ccmClient, GetCCMRecoveryTimeout())).To(Succeed())
				ccmToRestore = nil
				eventuallyEgressNAT(egressName, want)
				expectSameEgressNAT(egressName, natBefore)
				Expect(publicIPAddress(want.publicIPs[1])).To(Equal(secondAddress), "<id>-pip-2 must be attached again, not created again")
				expectEgressNATWritesAtMost(egressName, 1, "the restarted controller must correct the NAT Gateway with one write")

				By("Every pod is told once: the correction is a write of the new process, not an earlier write found applied")
				// Each pod was told of this configuration only, so applied counts the events that announce it.
				announcedOnce := egressCountsPlusOne(applied)
				eventuallyEgressAppliedCounts(cs, ns.Name, announcedOnce, "idle timeout 6m", "IPv4: 2 managed IPs")

				By("Moving one pod to idle timeout 7: the pods disagree and the NAT Gateway is kept")
				etags := egressResourceEtags(egressName)
				setEgressPodAnnotations(cs, ns.Name, map[string]string{idleKey: "7"}, pods[1])
				eventuallyPodEvent(cs, ns.Name, pods[1:], consts.ServiceEgressGatewayReasonConfigConflict, v1.EventTypeWarning, "idle timeout 7m")
				Consistently(func() error { return egressEtagsUnchangedErr(etags) }, 30*time.Second, defaultPollInterval).Should(Succeed(),
					"a configuration not every pod asks for must not be written")

				By("Stopping the cloud-controller-manager and setting idle timeout 9 out of band again")
				ccmToRestore = ccmClient
				expectNoCCMCrash(egressSpecStart)
				Expect(scaleCCMDeployment(context.TODO(), ccmClient, 0)).To(Succeed())
				Expect(waitForCCMFullyDown(context.TODO(), ccmClient, GetCCMRecoveryTimeout())).To(Succeed())
				rewriteNATGatewayOutOfBand(natGatewayID, func(properties map[string]any) { properties["idleTimeoutInMinutes"] = 9 })
				Expect(egressIdleTimeout(readEgressNAT(egressName))).To(Equal(9))
				etags = egressResourceEtags(egressName)
				events := egressConfigEventCounts(cs, ns.Name)

				By("Starting the cloud-controller-manager: while the pods disagree it corrects, writes and announces nothing")
				Expect(scaleCCMDeployment(context.TODO(), ccmClient, 1)).To(Succeed())
				Expect(waitForCCMFullyUp(context.TODO(), ccmClient, GetCCMRecoveryTimeout())).To(Succeed())
				ccmToRestore = nil
				observe := waitForCCMInitialization(ccmClient)
				Consistently(func() error { return egressEtagsUnchangedErr(etags) }, observe, defaultPollInterval).Should(Succeed(),
					"a controller must leave a NAT Gateway changed out of band while its pods agree on no configuration")
				if writes, ok := ccmEgressWrites(ccmClient, egressName, "the restarted controller's writes to "+egressName); ok {
					Expect(writes).To(BeEmpty(), "the restarted controller must not write the identity's resources")
				}
				Expect(egressConfigEventCounts(cs, ns.Name)).To(Equal(events), "a restart must announce nothing while the pods disagree")
				Expect(egressEventCounts(cs, ns.Name, pods, consts.ServiceEgressGatewayReasonConfigApplied, "")).To(Equal(announcedOnce),
					"the correction at the first start must have been announced once to every pod")

				By("Moving the other pod to idle timeout 7 as well: the agreement is applied in place and announced")
				setEgressPodAnnotations(cs, ns.Name, map[string]string{idleKey: "7"}, pods[0])
				want.idleTimeout = 7
				eventuallyEgressNAT(egressName, want)
				expectSameEgressNAT(egressName, natBefore)
				eventuallyEachPodApplied(cs, ns.Name, pods, "idle timeout 7m")

				By("Deleting the pods: the NAT Gateway and its Public IPs are deleted")
				deleteEgressPods(cs, ns.Name, egressName)
				eventuallyEgressIdentityGone(egressName)
			})

		It("should leave identities untouched when upgraded to this feature and clean up what a rollback leaves behind",
			Label(slbInitTestLabel, slbCCMRestartTestLabel, slbEgressConfigUpgradeTestLabel), func() {
				previousImage := os.Getenv(egressConfigPreviousCCMImageEnvVar)
				if previousImage == "" {
					Skip(egressConfigPreviousCCMImageEnvVar + " is not set; this spec runs the cloud-controller-manager on that image, " +
						"one without egress NAT Gateway configuration")
				}
				ccmClient := ccmClientOrSkip("changes the image of the cloud-controller-manager")
				image, err := getCCMCurrentImage(context.TODO(), ccmClient)
				Expect(err).NotTo(HaveOccurred())
				Expect(image).NotTo(Equal(previousImage), "%s must name an image other than the one under test", egressConfigPreviousCCMImageEnvVar)
				plain, configured, rolledBack := "egcfg-rst-oldplain-"+suffix, "egcfg-rst-oldconf-"+suffix, "egcfg-rst-rollback-"+suffix
				countKey := consts.PodAnnotationServiceEgressGatewayPublicIPCount

				By("Switching the cloud-controller-manager to " + previousImage)
				ccmImageToRestore = image
				DeferCleanup(func() {
					Expect(switchCCMImageErr(image)).To(Succeed(), "the cloud-controller-manager must run %s again", image)
				})
				expectNoCCMCrash(egressSpecStart)
				Expect(switchCCMImageErr(previousImage)).To(Succeed())

				By("Creating an identity without annotations and one whose pods ask for idle timeout 10, two Public IPs and a tag: " +
					"the controller without the feature gives both the default and says nothing")
				plainPods := createConfiguredEgressPods(cs, ns.Name, plain, 2, nil, false)
				configuredPods := createConfiguredEgressPods(cs, ns.Name, configured, 2, map[string]string{
					consts.PodAnnotationServiceEgressGatewayIdleTimeout: "10",
					countKey: "2",
					consts.PodAnnotationServiceEgressGatewayTags: "e2e-upgrade=yes",
				}, false)
				Expect(utils.WaitPodsToBeReady(cs, ns.Name)).To(Succeed())
				for _, id := range []string{plain, configured} {
					eventuallyEgressNAT(id, defaultEgressNATWant(id, ipv6))
					eventuallyManagedRegistered(cs, ns.Name, id, byoNATGatewayARMID(resourceGroupName, id))
				}
				plainEtags := egressResourceEtags(plain)
				configuredEtags := egressResourceEtags(configured)
				Consistently(func() error {
					if err := egressEtagsUnchangedErr(plainEtags); err != nil {
						return err
					}
					return egressEtagsUnchangedErr(configuredEtags)
				}, 30*time.Second, defaultPollInterval).Should(Succeed(), "a controller without the feature leaves both NAT Gateways as created")
				Expect(egressConfigEventCounts(cs, ns.Name)).To(BeEmpty(), "a controller without the feature raises no egress configuration event")
				configuredNAT := readEgressNAT(configured)

				By("Upgrading to " + image + ": the identity without annotations is left as it is, the other one gets its configuration in place")
				expectNoCCMCrash(egressSpecStart)
				Expect(switchCCMImageErr(image)).To(Succeed())
				observe := waitForCCMInitialization(ccmClient)
				want := defaultEgressNATWant(configured, ipv6)
				want.idleTimeout = 10
				want.publicIPs = egressManagedPublicIPIDs(configured, false, 2)
				want.tags = map[string]string{"e2e-upgrade": "yes"}
				eventuallyEgressNAT(configured, want)
				expectSameEgressNAT(configured, configuredNAT)
				expectEgressNATWritesAtMost(configured, 1, "the upgraded controller must apply the agreement with one write")
				eventuallyEachPodApplied(cs, ns.Name, configuredPods, "idle timeout 10m", "IPv4: 2 managed IPs",
					"tags: e2e-upgrade=yes")
				Consistently(func() error { return egressEtagsUnchangedErr(plainEtags) }, observe, defaultPollInterval).Should(Succeed(),
					"an upgraded controller must not write an identity whose pods ask for nothing")
				if writes, ok := ccmEgressWrites(ccmClient, plain, "the upgraded controller's writes to "+plain); ok {
					Expect(writes).To(BeEmpty(), "an upgraded controller must not write an identity whose pods ask for nothing")
				}
				expectNoPodEvent(cs, ns.Name, plainPods, consts.ServiceEgressGatewayReasonConfigApplied, consts.ServiceEgressGatewayReasonConfigConflict,
					consts.ServiceEgressGatewayReasonInvalidConfig, consts.ServiceEgressGatewayReasonConfigRejected)

				By("Creating an identity with two managed Public IPs and a managed /31 prefix")
				createConfiguredEgressPods(cs, ns.Name, rolledBack, 2, map[string]string{
					countKey: "2",
					consts.PodAnnotationServiceEgressGatewayPublicIPPrefixLength: "31",
				}, false)
				Expect(utils.WaitPodsToBeReady(cs, ns.Name)).To(Succeed())
				rollbackWant := defaultEgressNATWant(rolledBack, ipv6)
				rollbackWant.publicIPs = egressManagedPublicIPIDs(rolledBack, false, 2)
				rollbackWant.prefixes = []string{egressManagedPrefixID(rolledBack, false, 31)}
				eventuallyEgressNAT(rolledBack, rollbackWant)
				eventuallyManagedRegistered(cs, ns.Name, rolledBack, byoNATGatewayARMID(resourceGroupName, rolledBack))

				By("Rolling back to " + previousImage + " and deleting that identity's pods: only its NAT Gateway and <id>-pip are deleted, " +
					"and the other identities are left as they are")
				kept := egressResourceEtags(configured)
				maps.Copy(kept, egressResourceEtags(plain))
				applied, err := egressEventCounts(cs, ns.Name, configuredPods, consts.ServiceEgressGatewayReasonConfigApplied, "")
				Expect(err).NotTo(HaveOccurred())
				expectNoCCMCrash(egressSpecStart)
				Expect(switchCCMImageErr(previousImage)).To(Succeed())
				deleteEgressPods(cs, ns.Name, rolledBack)
				leftoverPublicIP, leftoverPrefix := rollbackWant.publicIPs[1], rollbackWant.prefixes[0]
				leftoversErr := func() error {
					return egressReservedResourcesErr(rolledBack, []string{resourceNameFromID(leftoverPublicIP)}, []string{resourceNameFromID(leftoverPrefix)})
				}
				Eventually(func() error {
					if err := natGatewayCleanupErr([]string{rolledBack}); err != nil {
						return err
					}
					return leftoversErr()
				}, 8*time.Minute, defaultPollInterval).Should(Succeed(), "a controller without the feature deletes the NAT Gateway, <id>-pip and <id>-pip-v6 only")
				Eventually(func() error { return egressPodsGoneErr(cs, ns.Name, rolledBack) }, 3*time.Minute, defaultPollInterval).Should(Succeed(),
					"the pods must be released")
				for _, id := range []string{leftoverPublicIP, leftoverPrefix} {
					Expect(tagValue(azureResourceTags(id), egressIdentityTagKey)).To(Equal(rolledBack), "%s must keep its ownership tag", resourceNameFromID(id))
				}
				Consistently(func() error {
					if err := leftoversErr(); err != nil {
						return err
					}
					return egressEtagsUnchangedErr(kept)
				}, time.Minute, defaultPollInterval).Should(Succeed(),
					"a controller without the feature leaves <id>-pip-2 and the prefix, and writes no other identity's resources")

				By("Upgrading to " + image + " again: its start-up, not its confirmation pass, deletes what the rollback left behind; " +
					"the configuration kept across the rollback is neither written nor announced again")
				expectNoCCMCrash(egressSpecStart)
				Expect(switchCCMImageErr(image)).To(Succeed())
				observe = waitForCCMInitialization(ccmClient)
				eventuallyEgressIdentityGone(rolledBack)
				expectLeftoversDeletedAtStartup(ccmClient, rolledBack)
				Consistently(func() error {
					if err := egressNATShapeErr(configured, want); err != nil {
						return err
					}
					return egressEtagsUnchangedErr(kept)
				}, observe, defaultPollInterval).Should(Succeed(),
					"a configuration kept across the rollback stays applied, and nothing writes the identities kept across it")
				for _, id := range []string{configured, plain} {
					if writes, ok := ccmEgressWrites(ccmClient, id, "the re-upgraded controller's writes to "+id); ok {
						Expect(writes).To(BeEmpty(), "a controller upgraded again must not write %s, not even without changing it", id)
					}
				}
				Expect(egressEventCounts(cs, ns.Name, configuredPods, consts.ServiceEgressGatewayReasonConfigApplied, "")).To(Equal(applied),
					"a configuration kept across the rollback is not announced again")
				expectNoPodEvent(cs, ns.Name, plainPods, consts.ServiceEgressGatewayReasonConfigApplied, consts.ServiceEgressGatewayReasonConfigConflict,
					consts.ServiceEgressGatewayReasonInvalidConfig, consts.ServiceEgressGatewayReasonConfigRejected)

				By("Deleting the other pods: their NAT Gateways and Public IPs are deleted")
				deleteEgressPods(cs, ns.Name, plain)
				deleteEgressPods(cs, ns.Name, configured)
				eventuallyEgressIdentityGone(plain)
				eventuallyEgressIdentityGone(configured)
			})

		It("should not delay a LoadBalancer Service created while it was down by an identity whose agreed configuration keeps being rejected, "+
			"and apply another agreement at once while its retries pause", Label(slbInitTestLabel, slbCCMRestartTestLabel, slbEgressConfigFaultTestLabel), func() {
			ccmClient := ccmClientOrSkip("stops the cloud-controller-manager")
			egressName, parked, fixed := "egcfg-rst-retrying-"+suffix, "egcfg-rst-parked-"+suffix, "egcfg-rst-fixed-"+suffix
			serviceName := "egcfg-rst-downtime"
			ipIDsKey := consts.PodAnnotationServiceEgressGatewayPublicIPIDs

			// One pod each: the first identity's next vote replaces its agreement at once, without the pods disagreeing in
			// between, the second identity's pod is its last, and the third identity's Public IP is created once its retries pause.
			By("Creating a pod that asks for a user Public IP that does not exist, the pod of a second identity that asks for it too, and " +
				"the pod of a third identity that asks for another one: each identity gets the default, the configuration is retried")
			rg, granted := createEgressUserResourceGroup(suffix)
			missing := map[string]string{ipIDsKey: publicIPID(rg, "egcfg-rst-missing")}
			pods := createConfiguredEgressPods(cs, ns.Name, egressName, 1, missing, false)
			parkedPods := createConfiguredEgressPods(cs, ns.Name, parked, 1, missing, false)
			fixedPods := createConfiguredEgressPods(cs, ns.Name, fixed, 1, map[string]string{ipIDsKey: publicIPID(rg, "egcfg-rst-fixed")}, false)
			Expect(utils.WaitPodsToBeReady(cs, ns.Name)).To(Succeed())
			for _, identity := range []string{egressName, parked, fixed} {
				eventuallyEgressNAT(identity, defaultEgressNATWant(identity, ipv6))
				eventuallyManagedRegistered(cs, ns.Name, identity, byoNATGatewayARMID(resourceGroupName, identity))
			}
			eventuallyEachPodEvent(cs, ns.Name, slices.Concat(pods, parkedPods, fixedPods), consts.ServiceEgressGatewayReasonConfigRejected,
				v1.EventTypeWarning, egressRejectedRetried)
			fixedNAT := readEgressNAT(fixed)
			etags := egressResourceEtags(egressName)
			Consistently(func() error { return egressEtagsUnchangedErr(etags) }, 30*time.Second, defaultPollInterval).Should(Succeed(),
				"precondition: the controller retries the rejected configuration without writing")
			events := egressConfigEventCounts(cs, ns.Name)

			By("Stopping the cloud-controller-manager and creating a LoadBalancer Service while it is down")
			ccmToRestore = ccmClient
			expectNoCCMCrash(egressSpecStart)
			Expect(scaleCCMDeployment(context.TODO(), ccmClient, 0)).To(Succeed())
			Expect(waitForCCMFullyDown(context.TODO(), ccmClient, GetCCMRecoveryTimeout())).To(Succeed())
			serviceUID := createLoadBalancerServiceWithBackend(cs, ns.Name, serviceName)

			By("Starting the cloud-controller-manager: the Service gets its ingress IP in the usual time while the configuration is retried")
			Expect(scaleCCMDeployment(context.TODO(), ccmClient, 1)).To(Succeed())
			Expect(waitForCCMFullyUp(context.TODO(), ccmClient, GetCCMRecoveryTimeout())).To(Succeed())
			ccmToRestore = nil
			started := time.Now()
			// Holding initialization open for the retries would delay it until they are exhausted, about 4.5 minutes.
			Eventually(func() (string, error) {
				return loadBalancerIngressIP(cs, ns.Name, serviceName)
			}, 3*time.Minute, 5*time.Second).ShouldNot(BeEmpty(), "the Service must get its ingress IP while the egress configuration is retried")
			utils.Logf("Service %s got its ingress IP %v after the cloud-controller-manager started", serviceName, time.Since(started).Round(time.Second))
			eventuallyServiceReconciled(serviceUID, 1, 2*time.Minute)
			if ccmLogsEgress(ccmClient, "the restarted controller's tries of the configuration of "+egressName) {
				Eventually(func() ([]string, error) {
					return ccmRecentLogLines(ccmClient, time.Time{}, `"Rejected egress configuration"`, fmt.Sprintf("identity=%q", egressName), "retryable=true")
				}, time.Minute, 5*time.Second).Should(WithTransform(func(lines []string) int { return len(lines) }, BeNumerically(">=", 2)),
					"precondition: the restarted controller keeps trying the rejected configuration")
			}
			Expect(egressEtagsUnchangedErr(etags)).To(Succeed(), "a rejected configuration must not be written")
			Expect(egressConfigEventCounts(cs, ns.Name)).To(Equal(events), "the restarted controller must not report the known rejection again")

			By("Asking the first identity's pod for idle timeout 5 instead, once the controller paused its retries after 12 tries: the new " +
				"agreement is applied at once, not after the 5-minute pause, and announced")
			eventuallyEgressRetriesPaused(ccmClient, egressName)
			paused := time.Now()
			setEgressConfig(cs, ns.Name, map[string]string{consts.PodAnnotationServiceEgressGatewayIdleTimeout: "5"}, pods...)
			want := defaultEgressNATWant(egressName, ipv6)
			want.idleTimeout = 5
			Eventually(func() error {
				return egressNATShapeErr(egressName, want)
			}, 2*time.Minute, defaultPollInterval).Should(Succeed(), "a new agreement must not wait for the retry pause of the configuration it replaces")
			utils.Logf("idle timeout 5 was applied %v after the controller paused its retries", time.Since(paused).Round(time.Second))
			eventuallyEachPodApplied(cs, ns.Name, pods, "idle timeout 5m")

			By("The controller paused the retries of the third identity too")
			eventuallyEgressRetriesPaused(ccmClient, fixed)
			// The burst's last rejection is logged with the pause; the tries after the pause start 5 minutes later.
			afterPause := time.Now().Add(30 * time.Second)
			// Where the log tells the tries after the pause, the Public IP is created once they were rejected again; otherwise now.
			var fixedPublicIP string
			var fixedAt time.Time
			rearmLogged := ccmLogsEgress(ccmClient, "the tries of the configuration of "+fixed+" after the pause")
			if !rearmLogged {
				fixedPublicIP, fixedAt = createBYOPublicIP(rg, "egcfg-rst-fixed", "IPv4"), time.Now()
			}

			By("Deleting the second identity's pod once the controller paused its retries too: the identity is deleted at once with " +
				"its NAT Gateway and its pod released, not after the 5-minute pause")
			eventuallyEgressRetriesPaused(ccmClient, parked)
			deleted := time.Now()
			deleteEgressPods(cs, ns.Name, parked)
			Eventually(func() error {
				if err := egressPodsGoneErr(cs, ns.Name, parked); err != nil {
					return err
				}
				return egressIdentityGoneErr(parked)
			}, 3*time.Minute, defaultPollInterval).Should(Succeed(), "the deletion of an identity must not wait for the retry pause of its update")
			utils.Logf("%s was deleted %v after its pod", parked, time.Since(deleted).Round(time.Second))

			By("Deleting the Service and the pods: the load balancer and the identity are deleted")
			Expect(cs.CoreV1().Services(ns.Name).Delete(context.TODO(), serviceName, metav1.DeleteOptions{})).To(Succeed())
			eventuallyServiceDeleted(serviceUID, 5*time.Minute)
			deleteEgressPods(cs, ns.Name, egressName)
			eventuallyEgressIdentityGone(egressName)

			By("Once the pause ends, the third identity's configuration is tried again and rejected without being reported again; " +
				"its Public IP, created then, is applied in place and announced; deleting its pod releases the Public IP")
			if rearmLogged {
				// Any retryable rejection of the configuration that asks for the Public IP: it does not exist, or the controller may not
				// read it, which the apply below then skips on.
				eventuallyEgressRejectionLogged(ccmClient, fixed, afterPause, []string{"user IPs (egcfg-rst-fixed)", "retryable=true"})
				Expect(egressEventCounts(cs, ns.Name, fixedPods, consts.ServiceEgressGatewayReasonConfigRejected, v1.EventTypeWarning)).To(
					Equal(egressCountsOf(fixedPods, 1)), "the tries after the pause must not report the rejection again")
				fixedPublicIP, fixedAt = createBYOPublicIP(rg, "egcfg-rst-fixed", "IPv4"), time.Now()
			}
			fixedWant := defaultEgressNATWant(fixed, ipv6)
			fixedWant.publicIPs = []string{fixedPublicIP}
			Eventually(func() error {
				skipIfEgressUserResourcesDenied(cs, ns.Name, fixedPods, rg, granted)
				return egressNATShapeErr(fixed, fixedWant)
			}, egressRetryApplyTimeout, defaultPollInterval).Should(Succeed(), "a configuration fixed after its retries paused must be "+
				"applied by the tries after the pause")
			utils.Logf("the Public IP of %s was applied %v after it was created", fixed, time.Since(fixedAt).Round(time.Second))
			expectSameEgressNAT(fixed, fixedNAT)
			// The tries may write the NAT Gateway before the creation of the Public IP returns.
			eventuallyEachPodApplied(cs, ns.Name, fixedPods, "user IPs (egcfg-rst-fixed)")
			Expect(egressEventCounts(cs, ns.Name, fixedPods, consts.ServiceEgressGatewayReasonConfigRejected, v1.EventTypeWarning)).To(
				Equal(egressCountsOf(fixedPods, 1)), "the rejection is reported once")
			deleteEgressPods(cs, ns.Name, fixed)
			eventuallyEgressIdentityGone(fixed)
			expectUserEgressResourcesReleased(fixedPublicIP)
		})
	})
})

// Helpers of the egress NAT Gateway configuration specs that restart the controller.

// eventuallyEgressRetriesPaused waits until the cloud-controller-manager logged that it paused the retries of the
// configuration of egress identity identity after their burst of 12 tries.
func eventuallyEgressRetriesPaused(ccmClient *CCMClusterClient, identity string) {
	Eventually(func() ([]string, error) {
		return ccmRecentLogLines(ccmClient, time.Time{}, `"Parked service operation after exhausting its retry burst`, fmt.Sprintf("serviceUID=%q", identity))
	}, 8*time.Minute, 5*time.Second).ShouldNot(BeEmpty(), "the controller must pause its retries of the rejected configuration of %s "+
		"after 12 tries", identity)
}

// egressConfigPreviousCCMImageEnvVar names a cloud-controller-manager image without egress NAT Gateway configuration,
// such as the release before it, for the upgrade and rollback spec, which is skipped while it is not set.
const egressConfigPreviousCCMImageEnvVar = "EGRESS_CONFIG_PREVIOUS_CCM_IMAGE"

// ccmImageSwitchTimeout bounds a change of the cloud-controller-manager's image: the image pull, the rollout and the
// readiness of the new pod.
const ccmImageSwitchTimeout = 5 * time.Minute

// egressStartupAllowance is how long a spec gives a ready cloud-controller-manager that does not log the end of its
// initialization (below egressLogVerbosity) to end it.
const egressStartupAllowance = 2 * time.Minute

// waitForCCMInitialization waits until the ready cloud-controller-manager ended its initialization and returns how long
// a spec must then watch it to see its confirmation pass over the egress identities it adopted, which runs 2 minutes
// later, too: egressRestartObservation from the end of its initialization, which it logs at egressLogVerbosity; below
// that verbosity, egressStartupAllowance longer, from now.
func waitForCCMInitialization(ccmClient *CCMClusterClient) time.Duration {
	if v, err := ccmLogVerbosity(ccmClient); err != nil || v < egressLogVerbosity {
		return egressStartupAllowance + egressRestartObservation
	}
	waitForCCMLogLine(ccmClient, 5*time.Minute, `"Completed DiffTracker initialization"`)
	return egressRestartObservation
}

// switchCCMImageErr makes the cloud-controller-manager run image, unless it does already, and waits until it does
// (ccmPodsRolledOutErr). The rest of the deployment, such as the container's environment, is kept.
func switchCCMImageErr(image string) error {
	ctx := context.TODO()
	ccmClient, err := NewCCMClusterClient()
	if err != nil {
		return err
	}
	current, err := getCCMCurrentImage(ctx, ccmClient)
	if err != nil {
		return err
	}
	if current != image {
		utils.Logf("Switching the cloud-controller-manager from %s to %s", current, image)
		if err := updateCCMImage(ctx, ccmClient, image); err != nil {
			return err
		}
	}
	var rolledOut error
	if err := wait.PollUntilContextTimeout(ctx, 5*time.Second, ccmImageSwitchTimeout, true, func(ctx context.Context) (bool, error) {
		pods, err := ccmClient.GetCCMPods(ctx)
		if err != nil {
			rolledOut = err
			return false, nil
		}
		rolledOut = ccmPodsRolledOutErr(pods, image)
		return rolledOut == nil, nil
	}); err != nil {
		if rolledOut == nil {
			rolledOut = err
		}
		return fmt.Errorf("the cloud-controller-manager does not run %s after %v: %w", image, ccmImageSwitchTimeout, rolledOut)
	}
	return nil
}

// ccmPodsRolledOutErr returns nil when the cloud-controller-manager pods run image: every pod runs it, none is
// terminating, and one is ready.
func ccmPodsRolledOutErr(pods []v1.Pod, image string) error {
	ready := false
	for _, pod := range pods {
		if pod.DeletionTimestamp != nil {
			return fmt.Errorf("cloud-controller-manager pod %s is terminating", pod.Name)
		}
		for _, container := range pod.Spec.Containers {
			if container.Name == ccmContainerName && container.Image != image {
				return fmt.Errorf("cloud-controller-manager pod %s runs %s", pod.Name, container.Image)
			}
		}
		if pod.Status.Phase == v1.PodRunning && len(pod.Status.ContainerStatuses) > 0 &&
			!slices.ContainsFunc(pod.Status.ContainerStatuses, func(s v1.ContainerStatus) bool { return !s.Ready }) {
			ready = true
		}
	}
	if !ready {
		return fmt.Errorf("no cloud-controller-manager pod running %s is ready", image)
	}
	return nil
}

// createLoadBalancerServiceWithBackend creates a LoadBalancer Service named name with one ready backend pod and
// returns the Service's UID, after which the controller names its Azure resources.
func createLoadBalancerServiceWithBackend(cs clientset.Interface, namespace, name string) string {
	labels := map[string]string{"app": name}
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name + "-backend", Namespace: namespace, Labels: labels},
		Spec: v1.PodSpec{Containers: []v1.Container{{
			Name: "test-app", Image: utils.AgnhostImage, ImagePullPolicy: v1.PullIfNotPresent, Args: []string{"netexec", "--http-port=8080"},
		}}},
	}
	_, err := cs.CoreV1().Pods(namespace).Create(context.TODO(), pod, metav1.CreateOptions{})
	Expect(err).NotTo(HaveOccurred())
	service := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: v1.ServiceSpec{
			Type:     v1.ServiceTypeLoadBalancer,
			Selector: labels,
			Ports:    []v1.ServicePort{{Name: "http", Protocol: v1.ProtocolTCP, Port: 8080, TargetPort: intstr.FromInt32(8080)}},
		},
	}
	created, err := cs.CoreV1().Services(namespace).Create(context.TODO(), service, metav1.CreateOptions{})
	Expect(err).NotTo(HaveOccurred())
	Expect(utils.WaitPodsToBeReady(cs, namespace)).To(Succeed())
	return string(created.UID)
}

// loadBalancerIngressIP returns the ingress IP of LoadBalancer Service name, "" while it has none.
func loadBalancerIngressIP(cs clientset.Interface, namespace, name string) (string, error) {
	service, err := cs.CoreV1().Services(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	if err != nil || len(service.Status.LoadBalancer.Ingress) == 0 {
		return "", err
	}
	return service.Status.LoadBalancer.Ingress[0].IP, nil
}

func TestCCMPodsRolledOutErr(t *testing.T) {
	const image, other = "registry/ccm:new", "registry/ccm:old"
	ccmPod := func(name, image string, phase v1.PodPhase, ready, terminating bool) v1.Pod {
		pod := v1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Spec:       v1.PodSpec{Containers: []v1.Container{{Name: ccmContainerName, Image: image}, {Name: "sidecar", Image: "registry/sidecar"}}},
			Status: v1.PodStatus{Phase: phase, ContainerStatuses: []v1.ContainerStatus{
				{Name: ccmContainerName, Ready: ready}, {Name: "sidecar", Ready: true},
			}},
		}
		if terminating {
			pod.DeletionTimestamp = &metav1.Time{Time: time.Unix(1_700_000_000, 0)}
		}
		return pod
	}
	for _, tc := range []struct {
		name    string
		pods    []v1.Pod
		wantErr string // "" for none
	}{
		{name: "a ready pod on the image", pods: []v1.Pod{ccmPod("a", image, v1.PodRunning, true, false)}},
		{name: "a ready pod with the cloud-controller-manager container only", pods: []v1.Pod{{
			ObjectMeta: metav1.ObjectMeta{Name: "a"},
			Spec:       v1.PodSpec{Containers: []v1.Container{{Name: ccmContainerName, Image: image}}},
			Status:     v1.PodStatus{Phase: v1.PodRunning, ContainerStatuses: []v1.ContainerStatus{{Name: ccmContainerName, Ready: true}}},
		}}},
		{name: "no pod", wantErr: "is ready"},
		{name: "the pod on the image is not ready yet", pods: []v1.Pod{ccmPod("a", image, v1.PodRunning, false, false)}, wantErr: "is ready"},
		{name: "the pod on the image is pending", pods: []v1.Pod{ccmPod("a", image, v1.PodPending, false, false)}, wantErr: "is ready"},
		{name: "a pod on the other image is still running", pods: []v1.Pod{
			ccmPod("a", image, v1.PodRunning, true, false), ccmPod("b", other, v1.PodRunning, true, false),
		}, wantErr: "runs " + other},
		{name: "the pod on the other image is terminating", pods: []v1.Pod{
			ccmPod("a", image, v1.PodRunning, true, false), ccmPod("b", other, v1.PodRunning, true, true),
		}, wantErr: "is terminating"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := ccmPodsRolledOutErr(tc.pods, image)
			switch {
			case tc.wantErr == "" && err != nil:
				t.Errorf("ccmPodsRolledOutErr() = %v; want nil", err)
			case tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)):
				t.Errorf("ccmPodsRolledOutErr() = %v; want an error containing %q", err, tc.wantErr)
			}
		})
	}
}
