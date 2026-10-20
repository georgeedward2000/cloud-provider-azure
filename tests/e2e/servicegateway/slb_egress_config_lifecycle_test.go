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
	"slices"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clientset "k8s.io/client-go/kubernetes"

	"sigs.k8s.io/cloud-provider-azure/pkg/consts"
	"sigs.k8s.io/cloud-provider-azure/tests/e2e/utils"
)

var _ = Describe("Container Load Balancer Outbound (NAT Gateway)", Label(slbTestLabel, slbEgressConfigTestLabel), func() {
	basename := "slb-egress-lifecycle"

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

	// The NAT Gateway configuration of an egress identity over time: across stops, restarts and crashes of the
	// cloud-controller-manager, with leftovers of an older controller in the cluster resource group, with rejections
	// the user fixes without touching the pods, with votes that change while a change runs, and when an identity's last
	// pods are replaced at once. Specs that stop, restart or crash the controller need its cluster (CCM_KUBECONFIG) and
	// skip without it.
	Context("egress NAT Gateway configuration: lifecycle, restarts and retries", func() {
		var (
			suffix string
			ipv6   bool
		)

		BeforeEach(func() {
			suffix = ns.Name[len(ns.Name)-5:]
			ipv6 = clusterHasIPv6Node(cs)
		})

		It("should apply at start-up what the pods changed while the controller was down, without phantom votes",
			Label(slbInitTestLabel, slbCCMRestartTestLabel), func() {
				ccmClient := ccmClientOrSkip("stops the cloud-controller-manager")
				down, drop, moved, rejected := "eglc-down-"+suffix, "eglc-drop-"+suffix, "eglc-moved-"+suffix, "eglc-rej-"+suffix
				idleKey := consts.PodAnnotationServiceEgressGatewayIdleTimeout

				By("Creating three pods without annotations for each of two identities")
				downPods := createConfiguredEgressPods(cs, ns.Name, down, 3, nil, true)
				stay, deleted, mover := downPods[0], downPods[1], downPods[2]
				dropPods := createConfiguredEgressPods(cs, ns.Name, drop, 3, nil, false)
				Expect(utils.WaitPodsToBeReady(cs, ns.Name)).To(Succeed())
				wantDown, wantDrop := defaultEgressNATWant(down, ipv6), defaultEgressNATWant(drop, ipv6)
				eventuallyEgressNAT(down, wantDown)
				eventuallyEgressNAT(drop, wantDrop)
				eventuallyManagedRegistered(cs, ns.Name, down, byoNATGatewayARMID(resourceGroupName, down))
				eventuallyManagedRegistered(cs, ns.Name, drop, byoNATGatewayARMID(resourceGroupName, drop))

				By("Asking for idle timeout 9 on two pods of the second identity: they disagree with its third pod, nothing is applied")
				setEgressPodAnnotations(cs, ns.Name, map[string]string{idleKey: "9"}, dropPods[0], dropPods[1])
				for _, pod := range dropPods[:2] {
					eventuallyPodEvent(cs, ns.Name, []string{pod}, consts.ServiceEgressGatewayReasonConfigConflict, v1.EventTypeWarning)
				}
				natDown, natDrop := readEgressNAT(down), readEgressNAT(drop)
				natEtags := azureResourceEtags(natDown.ID, natDrop.ID)
				publicIPEtags := azureResourceEtags(slices.Concat(wantDown.publicIPs, wantDown.publicIPsV6, wantDrop.publicIPs, wantDrop.publicIPsV6)...)
				addresses := egressNATAddresses(down)
				first, observable := waitForProbeEgress(cs, ns.Name, stay, addresses)

				By("Stopping the cloud-controller-manager")
				ccmToRestore = ccmClient
				expectNoCCMCrash(egressSpecStart)
				Expect(scaleCCMDeployment(context.TODO(), ccmClient, 0)).To(Succeed())
				Expect(waitForCCMFullyDown(context.TODO(), ccmClient, GetCCMRecoveryTimeout())).To(Succeed())

				By("While it is down: moving one pod of the first identity to idle timeout 11, deleting one and moving the third to a new " +
					"identity with idle timeout 7 and two managed Public IPs; deleting the dissenting pod of the second identity, whose vote set " +
					"its configuration")
				setEgressPodAnnotations(cs, ns.Name, map[string]string{idleKey: "11"}, stay)
				deleteEgressPod(cs, ns.Name, deleted)
				setEgressPodAnnotations(cs, ns.Name, map[string]string{idleKey: "7", consts.PodAnnotationServiceEgressGatewayPublicIPCount: "2"}, mover)
				setEgressPodLabel(cs, ns.Name, mover, moved)
				deleteEgressPod(cs, ns.Name, dropPods[2])

				By("While it is down too: creating a new identity's older pod, which asks for a user Public IP that does not exist, " +
					"and a newer pod that asks for idle timeout 5")
				rejectedPods := []string{createEgressConfigPod(cs, newEgressConfigPod(ns.Name, rejected, 0,
					map[string]string{consts.PodAnnotationServiceEgressGatewayPublicIPIDs: publicIPID("e2e-sgw-egress-none-"+suffix,
						"eglc-rej-pip")}, false))}
				time.Sleep(2 * time.Second) // creation timestamps have a resolution of one second
				rejectedPods = append(rejectedPods, createEgressConfigPod(cs, newEgressConfigPod(ns.Name, rejected, 1,
					map[string]string{idleKey: "5"}, false)))
				Expect(utils.WaitPodsToBeReady(cs, ns.Name)).To(Succeed())

				By("The deleted pods stay Terminating, held by the cleanup finalizer, and nothing is written to Azure")
				Consistently(func() error {
					for _, pod := range []string{deleted, dropPods[2]} {
						if err := egressPodHeldErr(cs, ns.Name, pod); err != nil {
							return err
						}
					}
					return errors.Join(egressEtagsUnchangedErr(natEtags), egressEtagsUnchangedErr(publicIPEtags))
				}, 30*time.Second, 5*time.Second).Should(Succeed(), "nothing may release a pod or change Azure while the controller is down")
				allPods := slices.Concat(downPods, dropPods, rejectedPods)
				told := egressEventTally(cs, ns.Name, allPods, egressConfigEventReasons...)

				By("Starting the cloud-controller-manager")
				Expect(scaleCCMDeployment(context.TODO(), ccmClient, 1)).To(Succeed())
				Expect(waitForCCMFullyUp(context.TODO(), ccmClient, GetCCMRecoveryTimeout())).To(Succeed())
				ccmToRestore = nil

				By("Only the remaining member of the first identity votes: its idle timeout 11 is applied in place and announced to it")
				wantDown.idleTimeout = 11
				eventuallyEgressNAT(down, wantDown)
				expectSameEgressNAT(down, natDown)
				eventuallyManagedRegistered(cs, ns.Name, down, byoNATGatewayARMID(resourceGroupName, down))
				eventuallyEachPodApplied(cs, ns.Name, []string{stay}, fmt.Sprintf("Egress identity %q", down),
					"idle timeout 11m")

				By("Without its dissenter the second identity's members agree: their idle timeout 9 is applied in place and announced to them")
				wantDrop.idleTimeout = 9
				eventuallyEgressNAT(drop, wantDrop)
				expectSameEgressNAT(drop, natDrop)
				eventuallyManagedRegistered(cs, ns.Name, drop, byoNATGatewayARMID(resourceGroupName, drop))
				eventuallyEachPodApplied(cs, ns.Name, dropPods[:2], fmt.Sprintf("Egress identity %q", drop),
					"idle timeout 9m")

				By("The moved pod creates the new identity directly with its vote, which is announced to it")
				wantMoved := defaultEgressNATWant(moved, ipv6)
				wantMoved.idleTimeout = 7
				wantMoved.publicIPs = egressManagedPublicIPIDs(moved, false, 2)
				eventuallyEgressNAT(moved, wantMoved)
				eventuallyManagedRegistered(cs, ns.Name, moved, byoNATGatewayARMID(resourceGroupName, moved))
				eventuallyEachPodApplied(cs, ns.Name, []string{mover}, fmt.Sprintf("Egress identity %q", moved), "idle timeout 7m",
					"IPv4: 2 managed IPs")
				creation := []string{"Creating or updating public IP", "Creating or updating public IP", "Creating or updating NAT Gateway"}
				if ipv6 {
					creation = append(creation, "Creating or updating public IP")
				}
				expectCCMEgressWrites(ccmClient, moved, creation...)

				By("The other new identity is created with the default, as the vote of its oldest pod, which it is created with " +
					"otherwise, is rejected: only that pod is told, once, that it will be retried")
				eventuallyEgressNAT(rejected, defaultEgressNATWant(rejected, ipv6))
				eventuallyManagedRegistered(cs, ns.Name, rejected, byoNATGatewayARMID(resourceGroupName, rejected))
				eventuallyEgressEventCounts(cs, ns.Name, consts.ServiceEgressGatewayReasonConfigRejected, v1.EventTypeWarning,
					map[string]int32{rejectedPods[0]: 1}, egressRejectedRetried)
				told[consts.ServiceEgressGatewayReasonConfigRejected][rejectedPods[0]]++

				By("The deleted pods are released; each member was told once, the deleted pods nothing, seeding raised nothing, " +
					"and only the NAT Gateways were written: no Public IP, and no registration again")
				for _, pod := range []string{deleted, dropPods[2]} {
					Eventually(func() error {
						return egressPodGoneErr(cs, ns.Name, pod)
					}, 4*time.Minute, defaultPollInterval).Should(Succeed(), "the controller must release a pod deleted while it was down")
				}
				for _, pod := range []string{stay, mover, dropPods[0], dropPods[1]} {
					told[consts.ServiceEgressGatewayReasonConfigApplied][pod]++
				}
				Expect(egressEventTally(cs, ns.Name, allPods, egressConfigEventReasons...)).To(Equal(told),
					"each member must be told once of the configuration applied at start-up, and no pod anything else")
				Expect(egressEtagsUnchangedErr(publicIPEtags)).To(Succeed(), "an idle timeout change writes no Public IP")
				// The rejected configuration it was created without is tried again only while the pods agree on it, which they do not.
				if lines, ok := ccmLogLinesFor(ccmClient, time.Time{}, "the tries after the creation of "+rejected, fmt.Sprintf("%q", rejected)); ok {
					created := slices.IndexFunc(lines, func(line string) bool { return strings.Contains(line, `"Created outbound service"`) })
					Expect(created).NotTo(Equal(-1), "the controller must log the creation of %s", rejected)
					for _, line := range lines[created+1:] {
						Expect(line).NotTo(ContainSubstring(`"Rejected egress configuration"`),
							"a configuration rejected when %s was created is tried again although its pods do not agree on it", rejected)
					}
				}
				expectCCMEgressWrites(ccmClient, down, "Creating or updating NAT Gateway")
				expectCCMEgressWrites(ccmClient, drop, "Creating or updating NAT Gateway")
				for _, id := range []string{down, drop} {
					if registrations, ok := ccmEgressRegistrations(ccmClient, id); ok {
						Expect(registrations).To(BeEmpty(), "applying the configuration of %s must not register it again", id)
					}
				}
				if observable {
					By("Checking the remaining member's egress continued through the NAT Gateway")
					expectEgressContinuity(cs, ns.Name, stay, first, addresses)
				}

				By("Deleting the pods: the four NAT Gateways and their Public IPs are deleted")
				deleteEgressPods(cs, ns.Name, down)
				deleteEgressPods(cs, ns.Name, drop)
				deleteEgressPods(cs, ns.Name, moved)
				deleteEgressPods(cs, ns.Name, rejected)
				eventuallyEgressIdentityGone(down)
				eventuallyEgressIdentityGone(drop)
				eventuallyEgressIdentityGone(moved)
				eventuallyEgressIdentityGone(rejected)
				leaveOutEgressUnlessObservable(observable)
			})

		It("should delete at start-up an identity whose pods were deleted while the controller was down, and reconcile pods changed while it started",
			Label(slbInitTestLabel, slbCCMRestartTestLabel), func() {
				ccmClient := ccmClientOrSkip("stops the cloud-controller-manager")
				gone, boot := "eglc-gone-"+suffix, "eglc-boot-"+suffix

				By("Creating two pods that ask for two managed Public IPs, a managed /31 prefix and a tag, and three pods of another identity without annotations")
				createConfiguredEgressPods(cs, ns.Name, gone, 2, map[string]string{
					consts.PodAnnotationServiceEgressGatewayPublicIPCount:        "2",
					consts.PodAnnotationServiceEgressGatewayPublicIPPrefixLength: "31",
					consts.PodAnnotationServiceEgressGatewayTags:                 "e2e-lifecycle=gone",
				}, false)
				bootPods := createConfiguredEgressPods(cs, ns.Name, boot, 3, nil, false)
				stay, deleted, unlabelled := bootPods[0], bootPods[1], bootPods[2]
				Expect(utils.WaitPodsToBeReady(cs, ns.Name)).To(Succeed())
				wantGone := defaultEgressNATWant(gone, ipv6)
				wantGone.publicIPs = egressManagedPublicIPIDs(gone, false, 2)
				wantGone.prefixes = []string{egressManagedPrefixID(gone, false, 31)}
				wantGone.tags = map[string]string{"e2e-lifecycle": "gone"}
				eventuallyEgressNAT(gone, wantGone)
				eventuallyManagedRegistered(cs, ns.Name, gone, byoNATGatewayARMID(resourceGroupName, gone))
				wantBoot := defaultEgressNATWant(boot, ipv6)
				eventuallyEgressNAT(boot, wantBoot)
				eventuallyManagedRegistered(cs, ns.Name, boot, byoNATGatewayARMID(resourceGroupName, boot))
				natBoot := readEgressNAT(boot)
				goneEtags := egressResourceEtags(gone)

				By("Stopping the cloud-controller-manager and deleting the pods of the first identity while it is down")
				ccmToRestore = ccmClient
				expectNoCCMCrash(egressSpecStart)
				Expect(scaleCCMDeployment(context.TODO(), ccmClient, 0)).To(Succeed())
				Expect(waitForCCMFullyDown(context.TODO(), ccmClient, GetCCMRecoveryTimeout())).To(Succeed())
				deleteEgressPods(cs, ns.Name, gone)
				Consistently(func() error {
					for i := range 2 {
						if err := egressPodHeldErr(cs, ns.Name, egressPodName(gone, i)); err != nil {
							return err
						}
					}
					return egressEtagsUnchangedErr(goneEtags)
				}, 30*time.Second, 5*time.Second).Should(Succeed(), "nothing may release a pod or change Azure while the controller is down")

				// Deleting the first identity at start-up keeps initialization, and so the start of the pod informer,
				// waiting long enough to change the second identity's pods in between.
				By("Starting the cloud-controller-manager and, once it has listed the pods but before it watches them, moving one pod of " +
					"the second identity to idle timeout 13, deleting one and removing the egress label of the third")
				Expect(scaleCCMDeployment(context.TODO(), ccmClient, 1)).To(Succeed())
				timed := waitForCCMLogLine(ccmClient, 3*time.Minute, `"Seeded egress votes"`)
				if !timed {
					// Its log cannot time the changes. The start-up deletion of the first identity is planned from the start-up
					// listing and holds the pod informer back until it ends: once it has started, the changes come after the listing.
					Eventually(func() error {
						return egressDeletionStartedErr(gone)
					}, 3*time.Minute, time.Second).Should(Succeed(), "the controller must start deleting %s at start-up", gone)
				}
				setEgressPodAnnotations(cs, ns.Name, map[string]string{consts.PodAnnotationServiceEgressGatewayIdleTimeout: "13"}, stay)
				deleteEgressPod(cs, ns.Name, deleted)
				setEgressPodLabel(cs, ns.Name, unlabelled, "")
				defer releaseEgressFinalizer(cs, ns.Name, unlabelled)
				if !timed {
					leaveOut("the premise that the pods changed after the controller listed them and before its pod informer started: " +
						"the controller logs the lines that tell it at verbosity 2 and up")
				} else if lines, err := ccmLogLines(ccmClient, time.Time{}, "Setting up pod informer"); err != nil {
					// Read once, not retried: the informer starts moments later, so a later read may see it legitimately.
					Expect(err).NotTo(MatchError(errCCMCrashed), "the cloud-controller-manager must not crash")
					leaveOut(fmt.Sprintf("the premise that the pods changed after the controller listed them and before its pod "+
						"informer started: its log cannot tell (%v)", err))
				} else {
					Expect(lines).To(BeEmpty(), "premise: the pods must change after the controller listed them and before its pod informer "+
						"started, which waits for the start-up deletion of %s", gone)
				}
				Expect(waitForCCMFullyUp(context.TODO(), ccmClient, GetCCMRecoveryTimeout())).To(Succeed())
				ccmToRestore = nil

				By("The first identity is deleted with its NAT Gateway, Public IPs and prefix, and its pods are released")
				Eventually(func() error {
					return egressPodsGoneErr(cs, ns.Name, gone)
				}, 4*time.Minute, defaultPollInterval).Should(Succeed(), "the controller must release the pods deleted while it was down")
				eventuallyEgressIdentityGone(gone)

				By("The second identity applies the idle timeout 13 of its remaining member in place, and the pods that left it are released")
				wantBoot.idleTimeout = 13
				eventuallyEgressNAT(boot, wantBoot)
				expectSameEgressNAT(boot, natBoot)
				eventuallyManagedRegistered(cs, ns.Name, boot, byoNATGatewayARMID(resourceGroupName, boot))
				eventuallyEachPodApplied(cs, ns.Name, []string{stay}, fmt.Sprintf("Egress identity %q", boot), "idle timeout 13m")
				Expect(egressEventCounts(cs, ns.Name, []string{stay}, consts.ServiceEgressGatewayReasonConfigApplied, "")).To(
					HaveKeyWithValue(stay, int32(1)), "pod %s must be told once of the applied configuration", stay)
				Eventually(func() error {
					return errors.Join(egressPodGoneErr(cs, ns.Name, deleted), egressPodReleasedErr(cs, ns.Name, unlabelled))
				}, 4*time.Minute, defaultPollInterval).Should(Succeed(), "the pods that left the identity must lose the cleanup finalizer")
				expectNoPodEvent(cs, ns.Name, []string{deleted, unlabelled}, consts.ServiceEgressGatewayReasonConfigApplied)

				By("Deleting the pods: the NAT Gateway and its Public IPs are deleted")
				deleteEgressPods(cs, ns.Name, boot)
				deleteEgressPod(cs, ns.Name, unlabelled)
				eventuallyEgressIdentityGone(boot)
			})

		It("should sweep leftovers it owns at start-up and in the confirmation pass, and never touch reserved names it does not own",
			Label(slbInitTestLabel, slbCCMRestartTestLabel, slbEgressConfigFaultTestLabel), func() {
				ccmClient := ccmClientOrSkip("stops the cloud-controller-manager")
				live, later, orphan := "eglc-live-"+suffix, "eglc-later-"+suffix, "eglc-orphan-"+suffix
				// The orphaned identity has no pod; the AfterEach checks it is gone all the same.
				egressIdentitiesUnderTest[orphan] = true

				By("Creating two identities of two pods without annotations, and a test resource group")
				livePods := createConfiguredEgressPods(cs, ns.Name, live, 2, nil, false)
				laterPods := createConfiguredEgressPods(cs, ns.Name, later, 2, nil, false)
				Expect(utils.WaitPodsToBeReady(cs, ns.Name)).To(Succeed())
				for _, name := range []string{live, later} {
					eventuallyEgressNAT(name, defaultEgressNATWant(name, ipv6))
					eventuallyManagedRegistered(cs, ns.Name, name, byoNATGatewayARMID(resourceGroupName, name))
				}
				rg, _ := createEgressUserResourceGroup(suffix)
				etags := egressResourceEtags(live)
				maps.Copy(etags, egressResourceEtags(later))

				By("Stopping the cloud-controller-manager")
				ccmToRestore = ccmClient
				expectNoCCMCrash(egressSpecStart)
				Expect(scaleCCMDeployment(context.TODO(), ccmClient, 0)).To(Succeed())
				Expect(waitForCCMFullyDown(context.TODO(), ccmClient, GetCCMRecoveryTimeout())).To(Succeed())

				// A controller without egress configuration deletes only an identity's NAT Gateway, <id>-pip and <id>-pip-v6.
				By("While it is down: creating tagged leftovers of the first identity and of an identity without NAT Gateway or pods, " +
					"as an older controller leaves them behind")
				owned := createOutOfBandResources(
					outOfBandResource{egressManagedPublicIPName(live, false, 4), "IPv4", 0, live, nil},
					outOfBandResource{egressManagedPrefixName(live, false, 30), "IPv4", 30, live, nil},
					outOfBandResource{egressManagedPublicIPName(orphan, false, 2), "IPv4", 0, orphan, nil},
					outOfBandResource{egressManagedPublicIPName(orphan, true, 2), "IPv6", 0, orphan, nil},
					outOfBandResource{egressManagedPrefixName(orphan, false, 31), "IPv4", 31, orphan, nil},
				)
				By("Creating resources at their reserved names that are not the controller's: untagged, or tagged for another identity")
				// What the controller never deletes, removed in this order before the AfterEach checks the identities, also when the
				// spec fails first.
				var notOurs []string
				defer func() { deleteNetworkResourcesQuietly(notOurs...) }()
				foreign := createOutOfBandResources(
					outOfBandResource{egressManagedPublicIPName(live, false, 5), "IPv4", 0, "", nil},
					outOfBandResource{egressManagedPublicIPName(orphan, false, 3), "IPv4", 0, "", nil},
					outOfBandResource{egressManagedPrefixName(orphan, false, 29), "IPv4", 29, live, nil},
				)
				notOurs = slices.Clone(foreign)
				By("Creating a tagged Public IP of the first identity that a NAT Gateway of the test resource group uses: Azure refuses to delete it")
				inUse := createOutOfBandResources(outOfBandResource{egressManagedPublicIPName(live, false, 6), "IPv4", 0, live, nil})[0]
				notOurs = append([]string{inUse}, notOurs...)
				holder := createBYONATGateway(rg, "eglc-holder-nat", byoAddresses{ipv4: inUse})
				notOurs = append([]string{holder}, notOurs...)
				out, err := runAz("rest", "--method", "delete", "--url", armURL(inUse))
				Expect(err).To(HaveOccurred(), "Azure must refuse to delete a Public IP a NAT Gateway uses")
				Expect(string(out)).To(ContainSubstring("PublicIPAddressCannotBeDeleted"))
				kept := azureResourceEtags(append(slices.Clone(foreign), inUse)...)

				By("Starting the cloud-controller-manager and, once it has adopted the second identity, creating a tagged Public IP of it, " +
					"which only the confirmation pass can find")
				Expect(scaleCCMDeployment(context.TODO(), ccmClient, 1)).To(Succeed())
				var late string
				if waitForCCMLogLine(ccmClient, 3*time.Minute, `"Adopted managed egress identity found in Azure"`, fmt.Sprintf("egressIdentity=%q", later)) {
					late = createOutOfBandResources(outOfBandResource{egressManagedPublicIPName(later, false, 3), "IPv4", 0, later, nil})[0]
				} else {
					leaveOut("the confirmation pass deleting a leftover created after the start-up listing: the spec times its creation by " +
						"a line the controller logs at verbosity 2 and up")
				}
				Expect(waitForCCMFullyUp(context.TODO(), ccmClient, GetCCMRecoveryTimeout())).To(Succeed())
				ccmToRestore = nil

				By("The tagged leftovers are deleted at start-up, and the second identity's by the confirmation pass after the settle delay")
				eventuallyResourcesDeleted(owned...)
				expectLeftoversDeletedAtStartup(ccmClient, orphan)
				if late != "" {
					Eventually(func() (bool, error) {
						_, found, err := getAzureNetworkResource(late)
						return found, err
					}, 5*time.Minute, defaultPollInterval).Should(BeFalse(), "the confirmation pass must delete the leftover created after the start-up listing")
					if lines, ok := ccmLogLinesFor(ccmClient, time.Time{}, "that the confirmation pass found the leftover of "+later,
						"changed since the startup listing", fmt.Sprintf("egressIdentity=%q", later)); ok {
						Expect(lines).NotTo(BeEmpty(), "the confirmation pass must be what found it")
					}
				}

				By("Nothing else is written: what is not the controller's keeps its etag, the Public IP in use stays attached, " +
					"and the NAT Gateways, Public IPs and registrations of the identities are unchanged")
				Consistently(func() error {
					return errors.Join(egressEtagsUnchangedErr(kept), egressEtagsUnchangedErr(etags))
				}, egressRestartObservation, defaultPollInterval).Should(Succeed(), "the controller may delete only its own leftovers")
				attached, found, err := getAzureNetworkResource(inUse)
				Expect(err).NotTo(HaveOccurred())
				Expect(found).To(BeTrue())
				Expect(attached.Properties.NatGateway).NotTo(BeNil())
				Expect(strings.EqualFold(attached.Properties.NatGateway.ID, holder)).To(BeTrue(), "%s must stay attached to %s", inUse, holder)
				expectNoPodEvent(cs, ns.Name, slices.Concat(livePods, laterPods), consts.ServiceEgressGatewayReasonInvalidConfig,
					consts.ServiceEgressGatewayReasonConfigConflict, consts.ServiceEgressGatewayReasonConfigApplied, consts.ServiceEgressGatewayReasonConfigRejected)
				for _, id := range foreign {
					name := resourceNameFromID(id)
					if writes, ok := ccmEgressWrites(ccmClient, name, "the writes to "+name); ok {
						Expect(writes).To(BeEmpty(), "a resource that is not the controller's must never be written")
					}
				}
				// Only the deletions of the leftovers are writes here: an etag does not change for a write that changes nothing,
				// and the registration is not a NAT Gateway, Public IP or prefix write.
				for _, id := range []string{live, later} {
					if puts, ok := ccmEgressPuts(ccmClient, id, "the restarted controller's writes to "+id); ok {
						Expect(puts).To(BeEmpty(), "sweeping the leftovers of %s must not write its NAT Gateway or Public IPs, not even "+
							"without changing them", id)
					}
					if registrations, ok := ccmEgressRegistrations(ccmClient, id); ok {
						Expect(registrations).To(BeEmpty(), "sweeping the leftovers of %s must not register it again", id)
					}
				}
				// Each run that finds the Public IP in use elsewhere tries to delete it once and keeps it on Azure's 400: the
				// start-up observation and the confirmation pass, and no retry after that.
				what := "the attempts to delete " + resourceNameFromID(inUse)
				if attempts, ok := ccmEgressWrites(ccmClient, resourceNameFromID(inUse), what); ok {
					Expect(attempts).To(HaveLen(2), "the start-up observation and the confirmation pass must each try to delete the unused "+
						"leftover once, and a leftover in use elsewhere must not be retried")
					if kept, ok := ccmLogLinesFor(ccmClient, time.Time{}, what, `"Kept reserved egress resource in use elsewhere"`,
						fmt.Sprintf("resource=%q", resourceNameFromID(inUse))); ok {
						Expect(kept).To(HaveLen(2), "Azure must refuse each attempt with HTTP 400, which the controller counts as final")
					}
				}

				By("Removing what is not the controller's, then deleting the pods: the identities' NAT Gateways and Public IPs are deleted")
				for _, id := range notOurs {
					deleteBYOResource(id)
				}
				notOurs = nil
				Expect(egressIdentityGoneErr(orphan)).To(Succeed())
				deleteEgressPods(cs, ns.Name, live)
				deleteEgressPods(cs, ns.Name, later)
				eventuallyEgressIdentityGone(live)
				eventuallyEgressIdentityGone(later)
			})

		It("should converge the NAT Gateway in place without leaking when the controller crashes during an apply",
			Label(slbCrashTestLabel, slbCCMRestartTestLabel), func() {
				ccmClient := ccmClientOrSkip("crashes the cloud-controller-manager")
				egressName := "eglc-crash-" + suffix

				By("Creating three pods without annotations: the NAT Gateway gets the default configuration")
				pods := createConfiguredEgressPods(cs, ns.Name, egressName, 3, nil, true)
				Expect(utils.WaitPodsToBeReady(cs, ns.Name)).To(Succeed())
				eventuallyEgressNAT(egressName, defaultEgressNATWant(egressName, ipv6))
				eventuallyManagedRegistered(cs, ns.Name, egressName, byoNATGatewayARMID(resourceGroupName, egressName))
				natBefore := readEgressNAT(egressName)
				first, observable := waitForProbeEgress(cs, ns.Name, pods[0], egressNATAddresses(egressName))

				By("Asking every pod for three managed Public IPs and a managed /30 prefix")
				agreed := defaultEgressNATWant(egressName, ipv6)
				agreed.publicIPs = egressManagedPublicIPIDs(egressName, false, 3)
				agreed.prefixes = []string{egressManagedPrefixID(egressName, false, 30)}
				setEgressPodAnnotations(cs, ns.Name, map[string]string{
					consts.PodAnnotationServiceEgressGatewayPublicIPCount:        "3",
					consts.PodAnnotationServiceEgressGatewayPublicIPPrefixLength: "30",
				}, pods...)

				By("Crashing the cloud-controller-manager as soon as Azure shows the first new resource")
				// The controller creates the new Public IPs and the prefix at once; two of them are enough to watch.
				created := waitForFirstEgressResource(2*time.Minute, agreed.publicIPs[1], agreed.prefixes[0])
				utils.Logf("Azure shows %s: crashing the cloud-controller-manager", resourceNameFromID(created))
				expectNoCCMCrash(egressSpecStart)
				crashedUIDs, err := ccmClient.CrashCCMAndWaitForDown(context.TODO(), CCMRecoveryTimeout)
				Expect(err).NotTo(HaveOccurred())
				logEgressStaging(egressName, slices.Concat(agreed.publicIPs[1:], agreed.prefixes)...)
				Expect(egressNATShapeErr(egressName, agreed)).To(HaveOccurred(),
					"premise: the controller must crash before its apply completed; it was already applied")

				By("Waiting for the cloud-controller-manager to recover: the NAT Gateway converges in place to the agreed configuration, " +
					"with exactly the managed resources it asks for")
				Expect(ccmClient.WaitForCCMReady(context.TODO(), CCMRecoveryTimeout, crashedUIDs...)).To(Succeed())
				Eventually(func() error {
					return egressNATShapeErr(egressName, agreed)
				}, 10*time.Minute, defaultPollInterval).Should(Succeed(), "the restarted controller must finish the interrupted apply")
				expectSameEgressNAT(egressName, natBefore)
				etags := egressResourceEtags(egressName)
				Consistently(func() error {
					return egressEtagsUnchangedErr(etags)
				}, egressRestartObservation, defaultPollInterval).Should(Succeed(),
					"once converged nothing is written any more, also when the controller lists Azure again after the settle delay")
				Expect(egressNATShapeErr(egressName, agreed)).To(Succeed(), "no leftover of the interrupted apply may appear later")

				By("Each pod is told of the new configuration once if the restarted controller wrote the NAT Gateway, else not at all, " +
					"and of nothing else")
				// An apply whose NAT Gateway write the crashed controller sent is found applied and not announced; one that
				// the restarted controller writes is announced, also when Azure did not report the write's outcome in time.
				writes, logged := ccmEgressWrites(ccmClient, egressName, "whether the restarted controller wrote NAT Gateway "+egressName)
				told := int32(0)
				if logged && slices.ContainsFunc(writes, func(line string) bool { return strings.Contains(line, `"Creating or updating NAT Gateway"`) }) {
					eventuallyEachPodApplied(cs, ns.Name, pods, "IPv4: 3 managed IPs, managed prefix /30")
					told = 1
				}
				applied, err := egressEventCounts(cs, ns.Name, pods, consts.ServiceEgressGatewayReasonConfigApplied, "")
				Expect(err).NotTo(HaveOccurred())
				if logged {
					Expect(applied).To(Equal(egressCountsOf(pods, told)), "each pod must be told once exactly when the restarted controller wrote "+
						"the NAT Gateway and announced it")
				} else {
					Expect(applied).To(HaveEach(BeNumerically("<=", 1)), "each pod must be told at most once")
				}
				Expect(egressEventCounts(cs, ns.Name, pods, consts.ServiceEgressGatewayReasonConfigApplied, "",
					"IPv4: 3 managed IPs, managed prefix /30")).To(Equal(applied), "each pod must be told of the new configuration only")
				// The pods are annotated one by one, so the first ones disagree with the others for a moment and are told
				// (EgressConfigConflict); nothing is invalid or rejected.
				expectNoPodEvent(cs, ns.Name, pods, consts.ServiceEgressGatewayReasonInvalidConfig, consts.ServiceEgressGatewayReasonConfigRejected)
				if observable {
					By("Checking egress continued through the NAT Gateway's addresses across the crash")
					expectEgressContinuity(cs, ns.Name, pods[0], first, egressNATAddresses(egressName))
				}

				By("Deleting the pods: the NAT Gateway, its Public IPs and its prefix are deleted")
				deleteEgressPods(cs, ns.Name, egressName)
				eventuallyEgressIdentityGone(egressName)
				leaveOutEgressUnlessObservable(observable)
			})

		It("should create a new identity with the default configuration when its agreed one is rejected and apply it once the user fixes it, "+
			"keeping egress", Label(slbEgressConfigFaultTestLabel), func() {
			late, held := "eglc-late-"+suffix, "eglc-held-"+suffix
			idsKey := consts.PodAnnotationServiceEgressGatewayPublicIPIDs
			userTags := map[string]string{"e2e-owner": "user"}

			By("Creating a test resource group with a user Public IP that another NAT Gateway holds, and naming one not created yet")
			rg, _ := createEgressUserResourceGroup(suffix)
			heldPublicIP := createBYOPublicIP(rg, "eglc-held-pip", "IPv4")
			configurations := userEgressResourceConfigurations(heldPublicIP)
			otherNAT := createBYONATGateway(rg, "eglc-other-nat", byoAddresses{ipv4: heldPublicIP})
			latePublicIP := publicIPID(rg, "eglc-late-pip")

			By("Creating two pods of a new identity that ask for the missing Public IP, and two pods of another identity without annotations")
			latePods := createConfiguredEgressPods(cs, ns.Name, late, 2, map[string]string{idsKey: latePublicIP}, true)
			heldPods := createConfiguredEgressPods(cs, ns.Name, held, 2, nil, false)
			Expect(utils.WaitPodsToBeReady(cs, ns.Name)).To(Succeed())

			By("The new identity is created, linked and registered with the default configuration, unannounced, " +
				"and each pod is told the configuration was rejected and will be retried")
			lateDefault := defaultEgressNATWant(late, ipv6)
			eventuallyEgressNAT(late, lateDefault)
			eventuallyManagedRegistered(cs, ns.Name, late, byoNATGatewayARMID(resourceGroupName, late))
			eventuallyEgressRejection(cs, ns.Name, latePods, rg, "does not exist", egressRejectedRetried)
			expectNoPodEvent(cs, ns.Name, latePods, consts.ServiceEgressGatewayReasonConfigApplied)
			natLate := readEgressNAT(late)
			managedAddress := egressAddressesOf(lateDefault.publicIPs[0])
			first, observable := waitForProbeEgress(cs, ns.Name, latePods[0], egressNATAddresses(late))

			By("A third pod that joins with the same vote is told when it joins")
			joined := createEgressConfigPod(cs, newEgressConfigPod(ns.Name, late, 2, map[string]string{idsKey: latePublicIP}, false))
			latePods = append(latePods, joined)
			Expect(utils.WaitPodsToBeReady(cs, ns.Name)).To(Succeed())
			eventuallyEgressRejection(cs, ns.Name, []string{joined}, rg, "does not exist", egressRejectedRetried)

			By("Asking every pod of the other identity for the Public IP the other NAT Gateway holds: rejected and retried, nothing written")
			heldDefault := defaultEgressNATWant(held, ipv6)
			eventuallyEgressNAT(held, heldDefault)
			eventuallyManagedRegistered(cs, ns.Name, held, byoNATGatewayARMID(resourceGroupName, held))
			natHeld := readEgressNAT(held)
			heldEtags := egressResourceEtags(held)
			setEgressPodAnnotations(cs, ns.Name, map[string]string{idsKey: heldPublicIP}, heldPods...)
			eventuallyEgressRejection(cs, ns.Name, heldPods, rg, "is attached to NAT Gateway", egressRejectedRetried)
			Consistently(func() error {
				return egressEtagsUnchangedErr(heldEtags)
			}, time.Minute, defaultPollInterval).Should(Succeed(), "a rejected configuration must never be written, however often it is retried")

			By("Creating the missing Public IP, with a tag of its own, and deleting the other NAT Gateway: both configurations are " +
				"applied in place without any pod change")
			changed := 0
			if observable {
				changed = egressProbeSampleCount(cs, ns.Name, latePods[0])
			}
			_, err := createUserPublicIP(rg, "eglc-late-pip", "StandardV2", clusterLocation(), userTags)
			Expect(err).NotTo(HaveOccurred())
			maps.Copy(configurations, userEgressResourceConfigurations(latePublicIP))
			deleteBYOResource(otherNAT)
			wantLate, wantHeld := lateDefault, heldDefault
			wantLate.publicIPs, wantHeld.publicIPs = []string{latePublicIP}, []string{heldPublicIP}
			for name, want := range map[string]egressNATWant{late: wantLate, held: wantHeld} {
				Eventually(func() error {
					return egressNATShapeErr(name, want)
				}, egressRetryApplyTimeout, defaultPollInterval).Should(Succeed(), "the retried configuration of %s must be applied once it is fixed", name)
			}
			expectSameEgressNAT(late, natLate)
			expectSameEgressNAT(held, natHeld)
			eventuallyEachPodApplied(cs, ns.Name, latePods, "user IPs (eglc-late-pip)")
			eventuallyEachPodApplied(cs, ns.Name, heldPods, "user IPs (eglc-held-pip)")

			By("Each pod was told of the rejection once, although the controller retried it")
			Expect(egressEventCounts(cs, ns.Name, slices.Concat(latePods, heldPods), consts.ServiceEgressGatewayReasonConfigRejected, "")).
				To(HaveEach(Equal(int32(1))), "every pod must be told once that its configuration was rejected")

			By("A pod that joins with the same vote after the configuration was applied is told nothing: it is no longer rejected")
			after := createEgressConfigPod(cs, newEgressConfigPod(ns.Name, late, 3, map[string]string{idsKey: latePublicIP}, false))
			Expect(utils.WaitPodsToBeReady(cs, ns.Name)).To(Succeed())
			eventuallyEgressMember(cs, ns.Name, after)
			Consistently(func() bool {
				return podHasEvent(cs, ns.Name, []string{after}, consts.ServiceEgressGatewayReasonConfigRejected) ||
					podHasEvent(cs, ns.Name, []string{after}, consts.ServiceEgressGatewayReasonConfigConflict)
			}, 30*time.Second, defaultPollInterval).Should(BeFalse(),
				"a pod voting for the applied configuration must be told neither that it is rejected nor that it disagrees")
			if observable {
				By("Checking egress moved to the user Public IP without an outage and no longer leaves through the deleted managed one")
				userAddress := egressAddressesOf(latePublicIP)
				eventuallyProbeSources(cs, ns.Name, latePods[0], changed, userAddress)
				expectProbeSourcesGone(cs, ns.Name, latePods[0], managedAddress)
				expectEgressContinuity(cs, ns.Name, latePods[0], first, managedAddress.union(userAddress))
			}

			By("Deleting the pods: the NAT Gateways are deleted and the user Public IPs kept, attached to nothing, with their own tags only")
			deleteEgressPods(cs, ns.Name, late)
			deleteEgressPods(cs, ns.Name, held)
			eventuallyEgressIdentityGone(late)
			eventuallyEgressIdentityGone(held)
			expectUserEgressResourcesReleased(latePublicIP, heldPublicIP)
			expectUserEgressResourcesUnchanged(configurations)
			leaveOutEgressUnlessObservable(observable)
		})

		It("should cancel a pending change when its pods stop agreeing before it is applied", Label(slbEgressConfigFaultTestLabel), func() {
			egressName := "eglc-cancel-" + suffix
			idsKey := consts.PodAnnotationServiceEgressGatewayPublicIPIDs

			By("Creating three pods without annotations: the NAT Gateway gets the default configuration")
			rg, _ := createEgressUserResourceGroup(suffix)
			missing := publicIPID(rg, "eglc-cancel-pip")
			pods := createConfiguredEgressPods(cs, ns.Name, egressName, 3, nil, false)
			Expect(utils.WaitPodsToBeReady(cs, ns.Name)).To(Succeed())
			defaults := defaultEgressNATWant(egressName, ipv6)
			eventuallyEgressNAT(egressName, defaults)
			eventuallyManagedRegistered(cs, ns.Name, egressName, byoNATGatewayARMID(resourceGroupName, egressName))
			natBefore := readEgressNAT(egressName)
			etags := egressResourceEtags(egressName)

			By("Asking every pod for a user Public IP that does not exist yet: the change is rejected and retried, nothing is written")
			setEgressPodAnnotations(cs, ns.Name, map[string]string{idsKey: missing}, pods...)
			eventuallyEgressRejection(cs, ns.Name, pods, rg, "does not exist", egressRejectedRetried)
			events := egressConfigEventCounts(cs, ns.Name)

			By("Moving the third pod back to no annotations: it is not told of a conflict, as it votes for the applied configuration")
			setEgressPodAnnotations(cs, ns.Name, map[string]string{idsKey: ""}, pods[2])
			Consistently(func() bool {
				return podHasEvent(cs, ns.Name, pods[2:], consts.ServiceEgressGatewayReasonConfigConflict)
			}, 30*time.Second, 5*time.Second).Should(BeFalse(), "a vote for the applied configuration is no conflict")

			By("Creating the Public IP: for 6 minutes, longer than the retry pause, nothing is written or announced, as the change was cancelled")
			userPublicIP := createBYOPublicIP(rg, "eglc-cancel-pip", "IPv4")
			Consistently(func() error {
				if podHasEvent(cs, ns.Name, pods, consts.ServiceEgressGatewayReasonConfigApplied) {
					return errors.New("a configuration was announced")
				}
				return egressEtagsUnchangedErr(etags)
			}, 6*time.Minute, defaultPollInterval).Should(Succeed(), "a change cancelled when its pods stopped agreeing must not be applied late")
			Expect(egressConfigEventCounts(cs, ns.Name)).To(Equal(events), "no pod may be told anything while the change is cancelled")

			By("Moving the third pod back to the Public IP: the pods agree again, and the configuration is applied in place at once and announced to each")
			setEgressPodAnnotations(cs, ns.Name, map[string]string{idsKey: missing}, pods[2])
			want := defaults
			want.publicIPs = []string{userPublicIP}
			Eventually(func() error {
				return egressNATShapeErr(egressName, want)
			}, 3*time.Minute, defaultPollInterval).Should(Succeed(), "a new agreement is applied at once, not after the retry pause")
			expectSameEgressNAT(egressName, natBefore)
			eventuallyEachPodApplied(cs, ns.Name, pods, "user IPs (eglc-cancel-pip)")
			// The pod that starts voting again for the configuration still rejected is told again; the others are not. The
			// reason told again is the latest try's, so only the configuration and that it is retried are pinned.
			eventuallyEgressEventCounts(cs, ns.Name, consts.ServiceEgressGatewayReasonConfigRejected, v1.EventTypeWarning,
				map[string]int32{pods[0]: 1, pods[1]: 1, pods[2]: 2})
			retold, err := egressEventCounts(cs, ns.Name, pods[2:], consts.ServiceEgressGatewayReasonConfigRejected, v1.EventTypeWarning,
				"user IPs (eglc-cancel-pip)", egressRejectedRetried)
			Expect(err).NotTo(HaveOccurred())
			Expect(retold).To(Equal(map[string]int32{pods[2]: 2}), "both times the pod was told its configuration was rejected and will be retried")

			By("Deleting the pods: the NAT Gateway is deleted and the user Public IP kept, attached to nothing")
			deleteEgressPods(cs, ns.Name, egressName)
			eventuallyEgressIdentityGone(egressName)
			expectUserEgressResourcesReleased(userPublicIP)
		})

		It("should complete a change it started although a pod's vote changes meanwhile, and end at the last of rapid agreements", func() {
			egressName := "eglc-inflight-" + suffix
			countKey := consts.PodAnnotationServiceEgressGatewayPublicIPCount
			idleKey := consts.PodAnnotationServiceEgressGatewayIdleTimeout

			By("Creating two pods without annotations: the NAT Gateway gets one managed Public IP")
			pods := createConfiguredEgressPods(cs, ns.Name, egressName, 2, nil, false)
			Expect(utils.WaitPodsToBeReady(cs, ns.Name)).To(Succeed())
			want := defaultEgressNATWant(egressName, ipv6)
			eventuallyEgressNAT(egressName, want)
			eventuallyManagedRegistered(cs, ns.Name, egressName, byoNATGatewayARMID(resourceGroupName, egressName))
			natBefore := readEgressNAT(egressName)

			By("Asking for six managed Public IPs on both pods and moving one pod to three as soon as <id>-pip-2 exists")
			six := egressManagedPublicIPIDs(egressName, false, 6)
			setEgressPodAnnotations(cs, ns.Name, map[string]string{countKey: "6"}, pods...)
			waitForFirstEgressResource(egressApplyTimeout, six[1])
			setEgressPodAnnotations(cs, ns.Name, map[string]string{countKey: "3"}, pods[1])

			By("The moved pod is told the pods disagree, and the started change completes in place")
			eventuallyPodEvent(cs, ns.Name, pods[1:], consts.ServiceEgressGatewayReasonConfigConflict, v1.EventTypeWarning, "IPv4: 3 managed IPs")
			want.publicIPs = six
			eventuallyEgressNAT(egressName, want)
			expectSameEgressNAT(egressName, natBefore)
			eventuallyEachPodApplied(cs, ns.Name, pods, "IPv4: 6 managed IPs")
			// The run announces its outcome when it ends, about 10 seconds after <id>-pip-2 appeared (the NAT Gateway PUT), or
			// later, when Azure did not answer that PUT in time: the moved pod's Conflict event, raised when the controller
			// took the vote, must come first.
			Expect(firstEgressEventTime(cs, ns.Name, pods[1], consts.ServiceEgressGatewayReasonConfigConflict, v1.EventTypeWarning, "IPv4: 3 managed IPs")).
				To(BeTemporally("<", firstEgressEventTime(cs, ns.Name, pods[1], consts.ServiceEgressGatewayReasonConfigApplied, v1.EventTypeNormal,
					"IPv4: 6 managed IPs")), "precondition: the controller took the vote change while the change to six Public IPs was being applied")

			By("The six Public IPs are kept while the pods disagree: the completed change is not reverted")
			expectEgressUntouched(egressName, want, egressResourceEtags(egressName), time.Minute)

			By("Moving the other pod to three as well: <id>-pip-4 to <id>-pip-6 are detached and deleted")
			setEgressPodAnnotations(cs, ns.Name, map[string]string{countKey: "3"}, pods[0])
			want.publicIPs = six[:3]
			eventuallyEgressNAT(egressName, want)
			expectSameEgressNAT(egressName, natBefore)
			eventuallyEachPodApplied(cs, ns.Name, pods, "IPv4: 3 managed IPs")

			By("Agreeing on idle timeouts 5, 6, 7 and 8 a few seconds apart: the NAT Gateway ends at 8")
			for i, idle := range []string{"5", "6", "7", "8"} {
				if i > 0 {
					time.Sleep(3 * time.Second) // agreements reached faster than the controller applies them
				}
				setEgressPodAnnotations(cs, ns.Name, map[string]string{idleKey: idle}, pods...)
			}
			want.idleTimeout = 8
			eventuallyEgressNAT(egressName, want)
			expectSameEgressNAT(egressName, natBefore)
			eventuallyEachPodApplied(cs, ns.Name, pods, "idle timeout 8m")
			// No earlier agreement may be applied after the last one.
			expectEgressUntouched(egressName, want, egressResourceEtags(egressName), time.Minute)
			expectEgressNATWritesAtMost(egressName, 7, "the NAT Gateway is written once for its creation, the six and the three Public IPs, "+
				"and at most once for each of the four idle timeouts")

			By("Deleting the pods: the NAT Gateway and its Public IPs are deleted")
			deleteEgressPods(cs, ns.Name, egressName)
			eventuallyEgressIdentityGone(egressName)
		})

		It("should retry a configuration whose user Public IP the controller may not access until it is granted access",
			Label(slbEgressConfigFaultTestLabel), func() {
				egressName := "eglc-denied-" + suffix

				By("Creating a user Public IP in a test resource group on which the controller's identities were granted nothing")
				rg := createEgressResourceGroup("e2e-sgw-egress-noaccess-" + suffix)
				userPublicIP := createBYOPublicIP(rg, "eglc-denied-pip", "IPv4")

				By("Creating pods that ask for it: the controller either uses it, having access beyond the resource group, or is denied access")
				pods := createConfiguredEgressPods(cs, ns.Name, egressName, 2,
					map[string]string{consts.PodAnnotationServiceEgressGatewayPublicIPIDs: userPublicIP}, false)
				Expect(utils.WaitPodsToBeReady(cs, ns.Name)).To(Succeed())
				want := defaultEgressNATWant(egressName, ipv6)
				want.publicIPs = []string{userPublicIP}
				denied := false
				Eventually(func() bool {
					denied = egressAccessDenial(cs, ns.Name, pods) != ""
					return denied || egressNATShapeErr(egressName, want) == nil
				}, egressApplyTimeout, defaultPollInterval).Should(BeTrue(), "the controller must either use the Public IP or report that it may not")
				if !denied {
					Skip("the controller used a Public IP of a resource group its identities were granted nothing on: they have access beyond it " +
						"(for example on the subscription), so access cannot be denied to them")
				}

				By("The identity is created with the default configuration, and every pod is told access was denied and the configuration will be retried")
				eventuallyEgressNAT(egressName, defaultEgressNATWant(egressName, ipv6))
				eventuallyManagedRegistered(cs, ns.Name, egressName, byoNATGatewayARMID(resourceGroupName, egressName))
				eventuallyEachPodEvent(cs, ns.Name, pods, consts.ServiceEgressGatewayReasonConfigRejected, v1.EventTypeWarning,
					"eglc-denied-pip", egressRejectedRetried)
				ids := clusterIdentityObjectIDs()
				if len(ids) == 0 {
					Skip("access denied was reported and retried; set " + byoAccessIdentitiesEnvVar + " to the object IDs of the controller's " +
						"identities so that the spec can grant them access and check the configuration is then applied")
				}

				By("Granting the controller's identities Network Contributor on the resource group: the configuration is applied without any pod change")
				grantEgressNetworkContributor(rg, ids)
				Eventually(func() error {
					return egressNATShapeErr(egressName, want)
				}, 20*time.Minute, defaultPollInterval).Should(Succeed(), "the configuration must be applied once the role assignment applies")
				eventuallyEachPodApplied(cs, ns.Name, pods, "user IPs (eglc-denied-pip)")

				By("Deleting the pods: the NAT Gateway is deleted and the user Public IP kept, attached to nothing")
				deleteEgressPods(cs, ns.Name, egressName)
				eventuallyEgressIdentityGone(egressName)
				expectUserEgressResourcesReleased(userPublicIP)
			})

		It("should end with one NAT Gateway of the new pod's configuration when an identity's last pod is replaced at once", func() {
			same, swap := "eglc-same-"+suffix, "eglc-swap-"+suffix
			two := map[string]string{consts.PodAnnotationServiceEgressGatewayPublicIPCount: "2"}

			By("Creating, for each of two identities, one pod that asks for two managed Public IPs")
			createConfiguredEgressPods(cs, ns.Name, same, 1, two, false)
			createConfiguredEgressPods(cs, ns.Name, swap, 1, two, false)
			Expect(utils.WaitPodsToBeReady(cs, ns.Name)).To(Succeed())
			wantSame, wantSwap := defaultEgressNATWant(same, ipv6), defaultEgressNATWant(swap, ipv6)
			wantSame.publicIPs = egressManagedPublicIPIDs(same, false, 2)
			wantSwap.publicIPs = egressManagedPublicIPIDs(swap, false, 2)
			eventuallyEgressNAT(same, wantSame)
			eventuallyEgressNAT(swap, wantSwap)
			eventuallyManagedRegistered(cs, ns.Name, same, byoNATGatewayARMID(resourceGroupName, same))
			eventuallyManagedRegistered(cs, ns.Name, swap, byoNATGatewayARMID(resourceGroupName, swap))
			replaced := map[string]azureNetworkResource{same: readEgressNAT(same), swap: readEgressNAT(swap)}

			By("Deleting each identity's only pod and creating a new one straight away: with the same configuration for the first " +
				"identity, with idle timeout 6 and a managed /31 prefix for the second")
			newPods := map[string]string{}
			for name, annotations := range map[string]map[string]string{same: two, swap: {
				consts.PodAnnotationServiceEgressGatewayIdleTimeout:          "6",
				consts.PodAnnotationServiceEgressGatewayPublicIPPrefixLength: "31",
			}} {
				deleteEgressPod(cs, ns.Name, egressPodName(name, 0))
				newPods[name] = createEgressConfigPod(cs, newEgressConfigPod(ns.Name, name, 1, annotations, true))
			}
			Expect(utils.WaitPodsToBeReady(cs, ns.Name)).To(Succeed())

			By("Each identity ends with one NAT Gateway of its new pod's configuration, kept or created again, with exactly the managed " +
				"resources it asks for and only the new pod registered")
			wantSwap.idleTimeout, wantSwap.publicIPs = 6, nil
			wantSwap.prefixes = []string{egressManagedPrefixID(swap, false, 31)}
			ends := map[string]azureNetworkResource{}
			for name, want := range map[string]egressNATWant{same: wantSame, swap: wantSwap} {
				eventuallyEgressNAT(name, want)
				eventuallyManagedRegistered(cs, ns.Name, name, byoNATGatewayARMID(resourceGroupName, name))
				Eventually(func() error {
					return egressPodGoneErr(cs, ns.Name, egressPodName(name, 0))
				}, 4*time.Minute, defaultPollInterval).Should(Succeed(), "the replaced pod must be released")
				ends[name] = readEgressNAT(name)
				kept := ends[name].Properties.ResourceGUID == replaced[name].Properties.ResourceGUID
				utils.Logf("the NAT Gateway of %s was kept: %v", name, kept)
			}
			etags := egressResourceEtags(same)
			maps.Copy(etags, egressResourceEtags(swap))
			// Kept (an update) or created again (a creation that is not the default), the new configuration is announced, also
			// after a write of unknown outcome.
			eventuallyEachPodApplied(cs, ns.Name, []string{newPods[swap]}, "idle timeout 6m", "managed prefix /31")
			// Created again with two managed Public IPs, which is not the default, the first identity's creation is announced;
			// kept, nothing changed, and its new pod is told nothing (checked once the window below is over).
			sameKept := ends[same].Properties.ResourceGUID == replaced[same].Properties.ResourceGUID
			if !sameKept {
				eventuallyEachPodApplied(cs, ns.Name, []string{newPods[same]}, "IPv4: 2 managed IPs")
			}
			expectNoPodEvent(cs, ns.Name, []string{newPods[same], newPods[swap]}, consts.ServiceEgressGatewayReasonInvalidConfig,
				consts.ServiceEgressGatewayReasonConfigConflict, consts.ServiceEgressGatewayReasonConfigRejected)

			By("Checking the new pods egress through their NAT Gateway")
			observable := true
			var blackHoled []string
			for _, name := range []string{same, swap} {
				if !observable {
					break
				}
				recreated := ends[name].Properties.ResourceGUID != replaced[name].Properties.ResourceGUID
				if recreated && reusedNameBlackHoled(cs, ns.Name, newPods[name], egressNATAddresses(name)) {
					blackHoled = append(blackHoled, name)
					continue
				}
				var first int
				first, observable = waitForProbeEgress(cs, ns.Name, newPods[name], egressNATAddresses(name))
				if observable {
					eventuallyProbeSuccesses(cs, ns.Name, newPods[name], first, 5)
				}
			}

			By("The NAT Gateways, their resources and registrations stay as they are: nothing of the replaced pods' deletion is left " +
				"to run, also after a retry backoff or a quiet period")
			Consistently(func() error {
				errs := []error{egressEtagsUnchangedErr(etags)}
				for name, want := range map[string]egressNATWant{same: wantSame, swap: wantSwap} {
					errs = append(errs, egressNATShapeErr(name, want), outboundServiceUsesNATGatewayErr(name, ends[name].ID))
				}
				return errors.Join(errs...)
			}, egressRestartObservation, defaultPollInterval).Should(Succeed(), "each identity must keep the NAT Gateway it ended with")
			if sameKept {
				expectNoPodEvent(cs, ns.Name, []string{newPods[same]}, consts.ServiceEgressGatewayReasonConfigApplied)
			}

			By("Deleting the pods: both NAT Gateways and their managed resources are deleted")
			deleteEgressPods(cs, ns.Name, same)
			deleteEgressPods(cs, ns.Name, swap)
			eventuallyEgressIdentityGone(same)
			eventuallyEgressIdentityGone(swap)
			if len(blackHoled) > 0 {
				leaveOut(fmt.Sprintf("the egress of the re-created identities %v, which got none at all: the ServiceGateway black-holes "+
					"an outbound service registered under a name it had before (a known data-plane issue; changing the service's NAT "+
					"Gateway reference heals it)", blackHoled))
			}
			leaveOutEgressUnlessObservable(observable)
		})

		It("should re-create an identity deleted under its pods with the vote of its oldest member, not of its creator, and forget "+
			"its old rejections", Label(slbEgressConfigFaultTestLabel), func() {
			egressName := "eglc-again-" + suffix
			idsKey := consts.PodAnnotationServiceEgressGatewayPublicIPIDs
			idleKey := consts.PodAnnotationServiceEgressGatewayIdleTimeout
			userTags := map[string]string{"e2e-owner": "user"}

			By("Creating, in a test resource group, a Standard Public IP, which no NAT Gateway accepts, and a StandardV2 one with a tag of its own")
			rg, granted := createEgressUserResourceGroup(suffix)
			standard, err := createUserPublicIP(rg, "eglc-standard-pip", "Standard", clusterLocation(), nil)
			Expect(err).NotTo(HaveOccurred())
			userPublicIP, err := createUserPublicIP(rg, "eglc-again-pip", "StandardV2", clusterLocation(), userTags)
			Expect(err).NotTo(HaveOccurred())
			configurations := userEgressResourceConfigurations(userPublicIP)

			By("Creating a pod without the egress label that asks for the Standard Public IP, then the identity's creator, which asks " +
				"for it too: it is rejected as final and the identity gets the default configuration")
			// The older pod gets the higher index, so that its name cannot stand in for its creation time.
			olderPod := newEgressConfigPod(ns.Name, egressName, 1, map[string]string{idsKey: standard}, false)
			olderPod.Labels = nil // created directly: it joins no identity yet
			_, err = cs.CoreV1().Pods(ns.Name).Create(context.TODO(), olderPod, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())
			older := olderPod.Name
			time.Sleep(2 * time.Second) // creation timestamps have a resolution of one second
			creator := createConfiguredEgressPods(cs, ns.Name, egressName, 1, map[string]string{idsKey: standard}, false)[0]
			Expect(utils.WaitPodsToBeReady(cs, ns.Name)).To(Succeed())
			defaults := defaultEgressNATWant(egressName, ipv6)
			eventuallyEgressNAT(egressName, defaults)
			eventuallyManagedRegistered(cs, ns.Name, egressName, byoNATGatewayARMID(resourceGroupName, egressName))
			eventuallyEgressRejection(cs, ns.Name, []string{creator}, rg, `has SKU "Standard"`, egressRejectedForGood)
			deleted := readEgressNAT(egressName)
			deletedPublicIP, found, err := getAzureNetworkResource(defaults.publicIPs[0])
			Expect(err).NotTo(HaveOccurred())
			Expect(found).To(BeTrue(), "%s must exist", defaults.publicIPs[0])

			By("Removing the egress label from the creator, which leaves the identity without members, and giving it an invalid " +
				"idle timeout")
			setEgressPodLabel(cs, ns.Name, creator, "")
			setEgressPodAnnotations(cs, ns.Name, map[string]string{idsKey: "", idleKey: "3"}, creator)

			By("Labelling the creator, which the deletion holds until it ends, and then the older pod with the identity once its " +
				"deletion has started: with the creator's invalid vote the members agree on nothing, so the identity being deleted " +
				"still remembers its rejection")
			Eventually(func() error {
				return egressDeletionStartedErr(egressName)
			}, 4*time.Minute, 2*time.Second).Should(Succeed(), "the identity must be deleted after its last pod")
			setEgressPodLabel(cs, ns.Name, creator, egressName)
			setEgressPodLabel(cs, ns.Name, older, egressName)
			eventuallyEgressMember(cs, ns.Name, older)
			// The premise of the checks of the seed and of the forgotten rejection: both pods joined while the deletion ran, which
			// deletes the NAT Gateway, then <id>-pip, and only then forgets the rejections of the identity.
			joinedInTime := false
			for id, guid := range map[string]string{deleted.ID: deleted.Properties.ResourceGUID, deletedPublicIP.ID: deletedPublicIP.Properties.ResourceGUID} {
				resource, found, err := getAzureNetworkResource(id)
				Expect(err).NotTo(HaveOccurred())
				joinedInTime = joinedInTime || (found && resource.Properties.ResourceGUID == guid)
			}
			utils.Logf("both pods joined %s before its old NAT Gateway and <id>-pip were deleted: %v", egressName, joinedInTime)
			eventuallyInvalidEgressProblem(cs, ns.Name, creator, egressName, idleKey+`: "3" is not an integer from 4 to 120`)
			eventuallyPodEvent(cs, ns.Name, []string{older}, consts.ServiceEgressGatewayReasonConfigConflict, v1.EventTypeWarning,
				"votes for idle timeout 4m; IPv4: user IPs (eglc-standard-pip)")

			if joinedInTime {
				By("The identity is created again on a new NAT Gateway with the vote of its oldest member, not with the invalid vote " +
					"of its creator, the first pod to join and the first by name: the Standard Public IP, whose rejection the deletion " +
					"forgot, is tried again and rejected, with a new event, and the default applied")
				Eventually(func() error {
					return recreatedEgressNATErr(egressName, deleted, defaults)
				}, egressApplyTimeout, defaultPollInterval).Should(Succeed(), "the identity must be created again with the default, "+
					"its oldest member's vote being rejected")
				eventuallyManagedRegistered(cs, ns.Name, egressName, byoNATGatewayARMID(resourceGroupName, egressName))
				// Told when it joined, while the identity being deleted remembered the rejection, and once more when the identity,
				// which forgot it with its deletion, was created again with its vote. Created with the creator's invalid vote, it
				// would have got the default without trying the Standard Public IP.
				eventuallyEgressEventCounts(cs, ns.Name, consts.ServiceEgressGatewayReasonConfigRejected, v1.EventTypeWarning,
					map[string]int32{older: 2, creator: 1}, `has SKU "Standard"`, egressRejectedForGood)
				expectNoPodEvent(cs, ns.Name, []string{older, creator}, consts.ServiceEgressGatewayReasonConfigApplied)
				expectNoPodEvent(cs, ns.Name, []string{older}, consts.ServiceEgressGatewayReasonInvalidConfig)
				what := "the tries of the Standard Public IP of " + egressName
				if lines, ok := ccmLogLinesFor(ccmEgressLog(what), time.Time{}, what, fmt.Sprintf("%q", egressName)); ok {
					// The second "Created outbound service" line follows the events of its creation by moments.
					Expect(egressCreationTries(lines, egressName, "eglc-standard-pip")).To(MatchRegexp(`^(RF)+CD+(RF)+C?$`), "the configuration "+
						"rejected as final must be tried at each creation (a creation retried after it tries again), and never in between: %q", lines)
				}
			} else {
				By("The identity is created again on a new NAT Gateway with the default, as the older pod joined late")
				Eventually(func() error {
					return recreatedEgressNATErr(egressName, deleted, defaults)
				}, egressApplyTimeout, defaultPollInterval).Should(Succeed(), "the identity must be created again with the default, "+
					"the vote of its oldest member being rejected and that of its creator invalid")
				leaveOut("the vote of the oldest member, which is neither the identity's creator nor the first pod to join, and the " +
					"forgotten rejection (the old NAT Gateway and <id>-pip were gone before both pods joined)")
			}
			// Released by the deletion or not, the creator, which joined again, is a member of the identity created again.
			eventuallyEgressMember(cs, ns.Name, creator)
			recreated := readEgressNAT(egressName)

			By("Asking both pods for three managed Public IPs, a managed /30 prefix, the StandardV2 Public IP and a tag: " +
				"applied in place and announced")
			setEgressPodAnnotations(cs, ns.Name, map[string]string{
				consts.PodAnnotationServiceEgressGatewayPublicIPCount:        "3",
				consts.PodAnnotationServiceEgressGatewayPublicIPPrefixLength: "30",
				idsKey: userPublicIP,
				consts.PodAnnotationServiceEgressGatewayTags: "e2e-life=again",
				idleKey: "",
			}, creator, older)
			want := defaults
			want.publicIPs = append(egressManagedPublicIPIDs(egressName, false, 3), userPublicIP)
			want.prefixes = []string{egressManagedPrefixID(egressName, false, 30)}
			want.tags = map[string]string{"e2e-life": "again"}
			eventuallyEgressNATWithUserResources(cs, ns.Name, []string{creator, older}, egressName, want, rg, granted)
			expectSameEgressNAT(egressName, recreated)
			eventuallyEachPodApplied(cs, ns.Name, []string{creator, older},
				"IPv4: 3 managed IPs, managed prefix /30, user IPs (eglc-again-pip)", "tags: e2e-life=again")

			By("Deleting the pods: the NAT Gateway and every managed Public IP and prefix are deleted, the user Public IP kept as it was")
			deleteEgressPods(cs, ns.Name, egressName)
			eventuallyEgressIdentityGone(egressName)
			expectUserEgressResourcesReleased(userPublicIP)
			expectUserEgressResourcesUnchanged(configurations)
		})

		It("should restore both links of identities whose deletion was interrupted while their pods stayed",
			Label(slbInitTestLabel, slbCCMRestartTestLabel, slbEgressConfigFaultTestLabel), func() {
				ccmClient := ccmClientOrSkip("stops the cloud-controller-manager")
				agreed, split, gone := "eglc-agreed-"+suffix, "eglc-split-"+suffix, "eglc-cut-"+suffix
				idleKey := consts.PodAnnotationServiceEgressGatewayIdleTimeout

				By("Creating two pods that agree on idle timeout 6 and two managed Public IPs, a pod of a second identity asking for " +
					"idle timeout 5, and a pod without annotations of a third")
				agreedPods := createConfiguredEgressPods(cs, ns.Name, agreed, 2, map[string]string{
					idleKey: "6", consts.PodAnnotationServiceEgressGatewayPublicIPCount: "2",
				}, true)
				splitPods := createConfiguredEgressPods(cs, ns.Name, split, 1, map[string]string{idleKey: "5"}, false)
				createConfiguredEgressPods(cs, ns.Name, gone, 1, nil, false)
				Expect(utils.WaitPodsToBeReady(cs, ns.Name)).To(Succeed())
				wantAgreed, wantSplit := defaultEgressNATWant(agreed, ipv6), defaultEgressNATWant(split, ipv6)
				wantAgreed.idleTimeout, wantAgreed.publicIPs = 6, egressManagedPublicIPIDs(agreed, false, 2)
				wantSplit.idleTimeout = 5
				for name, want := range map[string]egressNATWant{agreed: wantAgreed, split: wantSplit, gone: defaultEgressNATWant(gone, ipv6)} {
					eventuallyEgressNAT(name, want)
					eventuallyManagedRegistered(cs, ns.Name, name, byoNATGatewayARMID(resourceGroupName, name))
				}

				By("Adding a pod asking for idle timeout 7 to the second identity: its pods disagree, and it keeps idle timeout 5")
				splitPods = append(splitPods, createEgressConfigPod(cs, newEgressConfigPod(ns.Name, split, 1, map[string]string{idleKey: "7"}, false)))
				Expect(utils.WaitPodsToBeReady(cs, ns.Name)).To(Succeed())
				eventuallyPodEvent(cs, ns.Name, splitPods[1:], consts.ServiceEgressGatewayReasonConfigConflict, v1.EventTypeWarning)
				eventuallyManagedRegistered(cs, ns.Name, split, byoNATGatewayARMID(resourceGroupName, split))
				natAgreed, natSplit := readEgressNAT(agreed), readEgressNAT(split)
				managed := slices.Concat(wantAgreed.publicIPs, wantAgreed.publicIPsV6, wantSplit.publicIPs, wantSplit.publicIPsV6)
				publicIPEtags := azureResourceEtags(managed...)
				addresses := egressNATAddresses(agreed)
				_, observable := waitForProbeEgress(cs, ns.Name, agreedPods[0], addresses)
				// The first identity's pods were told when it was created with their configuration.
				told := egressEventTally(cs, ns.Name, slices.Concat(agreedPods, splitPods), egressConfigEventReasons...)
				applied := egressAppliedMessage(agreed, "idle timeout 6m; IPv4: 2 managed IPs"+defaultIPv6Summary(ipv6))
				appliedBefore, err := egressEventCounts(cs, ns.Name, agreedPods, consts.ServiceEgressGatewayReasonConfigApplied,
					v1.EventTypeNormal, applied)
				Expect(err).NotTo(HaveOccurred())

				By("Stopping the cloud-controller-manager and, out of band, interrupting the deletion of each identity after its unlink: " +
					"its registration no longer names its NAT Gateway, which is no longer linked to the Service Gateway")
				ccmToRestore = ccmClient
				expectNoCCMCrash(egressSpecStart)
				Expect(scaleCCMDeployment(context.TODO(), ccmClient, 0)).To(Succeed())
				Expect(waitForCCMFullyDown(context.TODO(), ccmClient, GetCCMRecoveryTimeout())).To(Succeed())
				for _, name := range []string{agreed, split, gone} {
					clearEgressRegistrationNAT(name)
					unlinkEgressNATGateway(name)
				}
				By("Deleting the third identity's pod while the controller is down")
				deleteEgressPods(cs, ns.Name, gone)
				restarted := 0
				if observable {
					restarted = egressProbeSampleCount(cs, ns.Name, agreedPods[0])
				}

				By("Starting the cloud-controller-manager")
				Expect(scaleCCMDeployment(context.TODO(), ccmClient, 1)).To(Succeed())
				Expect(waitForCCMFullyUp(context.TODO(), ccmClient, GetCCMRecoveryTimeout())).To(Succeed())
				ccmToRestore = nil

				By("The first identity gets both links back on the same NAT Gateway and Public IPs: its configuration is applied again " +
					"from an unknown state and announced once to each pod")
				eventuallyEgressNAT(agreed, wantAgreed)
				expectSameEgressNAT(agreed, natAgreed)
				eventuallyManagedRegistered(cs, ns.Name, agreed, byoNATGatewayARMID(resourceGroupName, agreed))
				eventuallyEgressAppliedCounts(cs, ns.Name, egressCountsPlusOne(appliedBefore), applied)
				for _, pod := range agreedPods {
					told[consts.ServiceEgressGatewayReasonConfigApplied][pod]++
				}
				Eventually(func() map[string]map[string]int32 {
					return egressEventTally(cs, ns.Name, slices.Concat(agreedPods, splitPods), egressConfigEventReasons...)
				}, 3*time.Minute, defaultPollInterval).Should(Equal(told),
					"each pod of the first identity must be told once more of its configuration, and no pod of anything else")

				By("The second identity, whose pods disagree, gets both links back by a write of the link alone: its configuration is " +
					"kept and nothing is announced")
				eventuallyEgressNAT(split, wantSplit)
				expectSameEgressNAT(split, natSplit)
				eventuallyManagedRegistered(cs, ns.Name, split, byoNATGatewayARMID(resourceGroupName, split))
				if lines, ok := ccmLogLinesFor(ccmClient, time.Time{}, "the link write of "+split, `"Linked NAT Gateway to Service Gateway"`,
					fmt.Sprintf("egressIdentity=%q", split)); ok {
					Expect(lines).NotTo(BeEmpty(), "the NAT Gateway of %s must be linked by a write of the link alone", split)
				}
				if lines, ok := ccmEgressRegistrations(ccmClient, split); ok {
					Expect(lines).NotTo(BeEmpty(), "the registration of %s must be restored by a write", split)
				}

				By("Each identity's NAT Gateway was written once and its Public IPs not at all")
				expectCCMEgressWrites(ccmClient, agreed, "Creating or updating NAT Gateway")
				expectCCMEgressWrites(ccmClient, split, "Creating or updating NAT Gateway")
				Expect(egressEtagsUnchangedErr(publicIPEtags)).To(Succeed(), "restoring the links writes no Public IP")
				Expect(egressEventTally(cs, ns.Name, slices.Concat(agreedPods, splitPods), egressConfigEventReasons...)).To(Equal(told),
					"the pods must be told nothing else")

				By("The third identity's interrupted deletion is finished and its pod released, without unlinking again what was " +
					"unlinked")
				Eventually(func() error {
					return egressPodsGoneErr(cs, ns.Name, gone)
				}, 4*time.Minute, defaultPollInterval).Should(Succeed(), "the controller must release the pod deleted while it was down")
				eventuallyEgressIdentityGone(gone)
				if puts, ok := ccmEgressPuts(ccmClient, gone, "the restarted controller's writes to "+gone); ok {
					Expect(puts).To(BeEmpty(), "finishing the interrupted deletion of %s must not unlink its NAT Gateway again", gone)
				}
				if clears, ok := ccmEgressRegistrationClears(ccmClient, gone); ok {
					Expect(clears).To(BeEmpty(), "finishing the interrupted deletion of %s must not clear its registration again", gone)
				}
				if observable {
					By("Checking egress returns through the first identity's NAT Gateway")
					eventuallyProbeSources(cs, ns.Name, agreedPods[0], restarted, addresses)
				}

				By("Deleting the pods: the NAT Gateways and their Public IPs are deleted")
				deleteEgressPods(cs, ns.Name, agreed)
				deleteEgressPods(cs, ns.Name, split)
				eventuallyEgressIdentityGone(agreed)
				eventuallyEgressIdentityGone(split)
				if clears, ok := ccmEgressRegistrationClears(ccmClient, agreed); ok {
					Expect(clears).NotTo(BeEmpty(), "deleting %s must clear the NAT Gateway of its restored registration", agreed)
				}
				leaveOutEgressUnlessObservable(observable)
			})
	})
})

// Helpers of the egress NAT Gateway configuration lifecycle specs.

// egressPodHeldErr returns nil while pod podName is being deleted and kept by the controller's cleanup finalizer.
func egressPodHeldErr(cs clientset.Interface, namespace, podName string) error {
	pod, err := cs.CoreV1().Pods(namespace).Get(context.TODO(), podName, metav1.GetOptions{})
	switch {
	case err != nil:
		return err
	case pod.DeletionTimestamp == nil:
		return fmt.Errorf("pod %s is not being deleted", podName)
	case !hasFinalizer(pod.Finalizers, serviceGatewayPodFinalizer):
		return fmt.Errorf("pod %s lost the cleanup finalizer %s", podName, serviceGatewayPodFinalizer)
	}
	return nil
}

// releaseEgressFinalizer removes the controller's cleanup finalizer from pod podName when it still carries it, so
// that a pod the controller failed to release cannot keep the spec's namespace from being deleted. It runs deferred,
// also while a failed spec unwinds, so it only logs what goes wrong.
func releaseEgressFinalizer(cs clientset.Interface, namespace, podName string) {
	pod, err := cs.CoreV1().Pods(namespace).Get(context.TODO(), podName, metav1.GetOptions{})
	if err != nil || !hasFinalizer(pod.Finalizers, serviceGatewayPodFinalizer) {
		return
	}
	finalizers := slices.DeleteFunc(slices.Clone(pod.Finalizers), func(f string) bool { return f == serviceGatewayPodFinalizer })
	patch, err := json.Marshal(map[string]any{"metadata": map[string]any{"finalizers": finalizers}})
	if err == nil {
		_, err = cs.CoreV1().Pods(namespace).Patch(context.TODO(), podName, types.MergePatchType, patch, metav1.PatchOptions{})
	}
	utils.Logf("released pod %s from the cleanup finalizer the controller left on it (err: %v)", podName, err)
}

// waitForFirstEgressResource polls Azure until one of the network resources ids exists, as soon as possible, and
// returns its ID.
func waitForFirstEgressResource(timeout time.Duration, ids ...string) string {
	var first string
	Eventually(func() bool {
		for _, id := range ids {
			if _, found, err := getAzureNetworkResource(id); err == nil && found {
				first = id
				return true
			}
		}
		return false
	}, timeout, time.Second).Should(BeTrue(), "the controller must start creating %v", ids)
	return first
}

// logEgressStaging logs which of the managed resources ids of egressName exist, in which provisioning state, and
// what its NAT Gateway references: how far an apply got before the controller crashed.
func logEgressStaging(egressName string, ids ...string) {
	for _, id := range ids {
		resource, found, err := getAzureNetworkResource(id)
		utils.Logf("after the crash %s exists: %v (provisioning state %q, err: %v)", resourceNameFromID(id), found, resource.Properties.ProvisioningState, err)
	}
	nat := readEgressNAT(egressName)
	props := nat.Properties
	utils.Logf("after the crash NAT Gateway %s is %s, references Public IPs %v and prefixes %v", egressName, props.ProvisioningState,
		subResourceIDs(props.PublicIPAddresses), subResourceIDs(props.PublicIPPrefixes))
}

// recreatedEgressNATErr returns nil once egress identity egressName has a NAT Gateway other than deleted, the one the
// controller deleted, with the shape want (egressNATShapeErr).
func recreatedEgressNATErr(egressName string, deleted azureNetworkResource, want egressNATWant) error {
	nat, found, err := getAzureNetworkResource(deleted.ID)
	switch {
	case err != nil:
		return err
	case !found:
		return fmt.Errorf("NAT Gateway %s is not created again yet", egressName)
	case nat.Properties.ResourceGUID == deleted.Properties.ResourceGUID:
		return fmt.Errorf("NAT Gateway %s is still the one being deleted", egressName)
	}
	return egressNATShapeErr(egressName, want)
}

// deleteNetworkResourcesQuietly deletes network resources, in order, and only logs what goes wrong: it runs deferred, also
// while a failed spec unwinds.
func deleteNetworkResourcesQuietly(ids ...string) {
	for _, id := range ids {
		if out, err := runAz("resource", "delete", "--ids", id); err != nil {
			utils.Logf("deleting %s: %v: %s", id, err, string(out))
		}
	}
}

// egressDeletionStartedErr returns nil once the deletion of egress identity egressName has started: its first step
// cleared the NAT Gateway of the identity's Service Gateway registration, or the registration is gone.
func egressDeletionStartedErr(egressName string) error {
	sgResponse, err := queryServiceGatewayServices()
	if err != nil {
		return err
	}
	for _, svc := range sgResponse.Value {
		if strings.EqualFold(svc.Name, egressName) && svc.Properties.PublicNatGatewayID != "" {
			return fmt.Errorf("egress identity %s is still registered with NAT Gateway %s", egressName, svc.Properties.PublicNatGatewayID)
		}
	}
	return nil
}

// clearEgressRegistrationNAT clears, out of band, the NAT Gateway of egress identity egressName's Service Gateway
// registration, as the unlink that starts the deletion of an identity does: a PartialUpdate of that service alone,
// which leaves every other service of the Service Gateway as it is.
func clearEgressRegistrationNAT(egressName string) {
	sgResponse, err := queryServiceGatewayServices()
	Expect(err).NotTo(HaveOccurred())
	index := slices.IndexFunc(sgResponse.Value, func(svc ServiceGatewayService) bool { return strings.EqualFold(svc.Name, egressName) })
	Expect(index).NotTo(Equal(-1), "egress identity %s must be registered", egressName)
	service := sgResponse.Value[index]
	out, err := updateServiceGatewayService(service.Name, false, map[string]any{"serviceType": service.Properties.ServiceType, "publicNatGatewayId": nil})
	Expect(err).NotTo(HaveOccurred(), string(out))
	Eventually(func() error {
		return outboundServiceUsesNATGatewayErr(egressName, "")
	}, 2*time.Minute, defaultPollInterval).Should(Succeed(), "the registration of %s must no longer name its NAT Gateway", egressName)
}

// reusedNameBlackHoled reports whether probe pod podName, a member of an identity the controller re-created under its
// old name, got no egress at all: its probes never left steadily through want within 5 minutes, its last 20 probes
// failed, and a pod without the egress label reaches the internet. The ServiceGateway black-holes an outbound service
// registered under a name it had before; any other outcome is left to waitForProbeEgress to judge.
func reusedNameBlackHoled(cs clientset.Interface, namespace, podName string, want egressAddressSet) bool {
	var samples []egressProbeSample
	for deadline := time.Now().Add(5 * time.Minute); time.Now().Before(deadline); time.Sleep(defaultPollInterval) {
		if latest, err := egressProbeSamples(cs, namespace, podName); err == nil {
			samples = latest
		}
		if _, ok := steadyEgressStart(samples, want); ok {
			return false
		}
	}
	const tail = 20
	if len(samples) < tail || slices.ContainsFunc(samples[len(samples)-tail:], func(s egressProbeSample) bool { return s.source != "" }) {
		return false
	}
	return podsReachInternet(cs, namespace)
}

// unlinkEgressNATGateway writes the NAT Gateway of egress identity egressName, out of band, without its Service Gateway
// link, as the unlink that starts the deletion of an identity does once the registration no longer names it
// (clearEgressRegistrationNAT): a PUT of the NAT Gateway as read, with the link set to null.
func unlinkEgressNATGateway(egressName string) {
	id := byoNATGatewayARMID(resourceGroupName, egressName)
	rewriteNATGatewayOutOfBand(id, func(properties map[string]any) { properties["serviceGateway"] = nil })
	Expect(byoNATGatewayLink(id)).To(BeEmpty(), "NAT Gateway %s must no longer be linked to the Service Gateway", egressName)
}

// firstEgressEventTime returns when pod podName was first told an event of reason and eventType whose message contains
// every one of wants, to the second; the pod must have one.
func firstEgressEventTime(cs clientset.Interface, namespace, podName, reason, eventType string, wants ...string) time.Time {
	byPod, err := egressPodEvents(cs, namespace, reason)
	Expect(err).NotTo(HaveOccurred())
	first := earliestEgressEventTime(byPod[podName], eventType, wants)
	Expect(first.IsZero()).To(BeFalse(), "pod %s must have a %s %s event containing %q", podName, eventType, reason, wants)
	return first
}

// earliestEgressEventTime returns when the earliest of events of eventType whose message contains every one of wants
// was first raised, or the zero time when there is none.
func earliestEgressEventTime(events []v1.Event, eventType string, wants []string) time.Time {
	var earliest time.Time
	for _, e := range events {
		at := e.FirstTimestamp.Time
		if at.IsZero() {
			at = e.EventTime.Time
		}
		if egressEventMatches(e, eventType, wants) && !at.IsZero() && (earliest.IsZero() || at.Before(earliest)) {
			earliest = at
		}
	}
	return earliest
}

func TestEarliestEgressEventTime(t *testing.T) {
	at := func(seconds int64) metav1.Time { return metav1.Time{Time: time.Unix(1_700_000_000+seconds, 0)} }
	event := func(eventType, message string, first metav1.Time) v1.Event {
		return v1.Event{Type: eventType, Message: message, FirstTimestamp: first}
	}
	events := []v1.Event{
		event(v1.EventTypeNormal, "applied: IPv4: 6 managed IPs", at(20)),
		event(v1.EventTypeNormal, "applied: IPv4: 6 managed IPs", at(12)),
		event(v1.EventTypeNormal, "applied: IPv4: 3 managed IPs", at(5)),
		event(v1.EventTypeWarning, "conflict: IPv4: 6 managed IPs", at(1)),
		{Type: v1.EventTypeNormal, Message: "applied: IPv4: 6 managed IPs, as an event series", EventTime: metav1.MicroTime{Time: time.Unix(1_700_000_010, 0)}},
	}
	for _, tc := range []struct {
		name      string
		eventType string
		wants     []string
		want      time.Time
	}{
		{name: "the earliest matching event", eventType: v1.EventTypeNormal, wants: []string{"IPv4: 6 managed IPs"}, want: at(10).Time},
		{name: "every substring must match", eventType: v1.EventTypeNormal, wants: []string{"applied", "3 managed IPs"}, want: at(5).Time},
		{name: "the earliest of several matching events", eventType: v1.EventTypeNormal, wants: []string{"managed IPs"}, want: at(5).Time},
		{name: "of the event type only", eventType: v1.EventTypeWarning, wants: []string{"IPv4: 6 managed IPs"}, want: at(1).Time},
		{name: "no matching event", eventType: v1.EventTypeWarning, wants: []string{"IPv4: 3 managed IPs"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := earliestEgressEventTime(events, tc.eventType, tc.wants); !got.Equal(tc.want) {
				t.Errorf("earliestEgressEventTime(%s, %q) = %v; want %v", tc.eventType, tc.wants, got, tc.want)
			}
		})
	}
}
