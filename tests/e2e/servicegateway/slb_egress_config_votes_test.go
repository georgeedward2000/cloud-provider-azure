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
	"io/fs"
	"maps"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/diff"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	clientset "k8s.io/client-go/kubernetes"

	"sigs.k8s.io/cloud-provider-azure/pkg/consts"
	"sigs.k8s.io/cloud-provider-azure/tests/e2e/utils"
)

var _ = Describe("Container Load Balancer Outbound (NAT Gateway)", Label(slbTestLabel, slbEgressConfigTestLabel), func() {
	basename := "slb-egress-votes"

	var (
		cs clientset.Interface
		ns *v1.Namespace
		// otherNS is a second namespace a spec created for member pods of its identities.
		otherNS *v1.Namespace
	)

	BeforeEach(func() {
		cs, ns = newEgressConfigSpec(basename)
	})

	AfterEach(func() {
		// Deleted first: a member pod there would keep its identity alive through the checks of the cleanup.
		var err error
		if other := otherNS; other != nil {
			otherNS = nil
			err = utils.DeleteNamespace(cs, other.Name)
		}
		cleanUpEgressConfigSpec(cs, ns, nil)
		cs, ns = nil, nil
		Expect(err).NotTo(HaveOccurred(), "deleting the spec's second namespace")
	})

	// Which pods vote, which votes are invalid and which spellings are one vote. A member of an egress identity is a
	// pod labelled with it, in any namespace, that is Running or Pending with an address and is not terminating.
	Context("egress NAT Gateway configuration: votes and members", func() {
		var (
			suffix string
			ipv6   bool
		)

		BeforeEach(func() {
			suffix = ns.Name[len(ns.Name)-5:]
			ipv6 = clusterHasIPv6Node(cs)
		})

		It("should tell each pod which egress annotation is invalid, once per change, keep its egress and write nothing", func() {
			egressName := "egvote-invalid-" + suffix
			idleKey := consts.PodAnnotationServiceEgressGatewayIdleTimeout
			countKey := consts.PodAnnotationServiceEgressGatewayPublicIPCount
			tagsKey := consts.PodAnnotationServiceEgressGatewayTags

			By("Creating a pod without annotations: the NAT Gateway gets the default configuration")
			valid := createConfiguredEgressPods(cs, ns.Name, egressName, 1, nil, false)[0]
			Expect(utils.WaitPodsToBeReady(cs, ns.Name)).To(Succeed())
			defaults := defaultEgressNATWant(egressName, ipv6)
			eventuallyEgressNAT(egressName, defaults)
			eventuallyManagedRegistered(cs, ns.Name, egressName, byoNATGatewayARMID(resourceGroupName, egressName))
			etags := egressResourceEtags(egressName)
			Consistently(func() error { return egressEtagsUnchangedErr(etags) }, 30*time.Second, defaultPollInterval).Should(Succeed(),
				"precondition: the controller has finished writing the identity's resources")
			ccmClient, writes := ccmEgressWritesSoFar(egressName)

			cases := invalidEgressAnnotations(suffix, ipv6)
			By(fmt.Sprintf("Creating %d pods that each break another rule of the annotations, one that breaks three, one without a "+
				"public address, and two pods with invalid annotations that are no members", len(cases)))
			pods := make([]string, 0, len(cases)+1)
			for i, c := range cases {
				pods = append(pods, createEgressConfigPod(cs, newEgressConfigPod(ns.Name, egressName, i+1, c.annotations, false)))
			}
			several := createEgressConfigPod(cs, newEgressConfigPod(ns.Name, egressName, len(cases)+1,
				map[string]string{tagsKey: "=a", idleKey: "3", countKey: "0"}, false))
			pods = append(pods, several)
			noAddress := createEgressConfigPod(cs, newEgressConfigPod(ns.Name, egressName, len(cases)+2, map[string]string{countKey: "0"}, false))
			// Created directly, as they join no identity: one has no egress label, the other an empty one.
			var outsiders []string
			for i, labels := range []map[string]string{nil, {egressLabel: ""}} {
				pod := newEgressConfigPod(ns.Name, egressName, 0, map[string]string{idleKey: "1000"}, false)
				pod.Name, pod.Labels = fmt.Sprintf("egvote-outsider-%d", i), labels
				_, err := cs.CoreV1().Pods(ns.Name).Create(context.TODO(), pod, metav1.CreateOptions{})
				Expect(err).NotTo(HaveOccurred())
				outsiders = append(outsiders, pod.Name)
			}
			Expect(utils.WaitPodsToBeReady(cs, ns.Name)).To(Succeed())

			By("Each member is told which annotation breaks which rule, and that it keeps its egress")
			for i, c := range cases {
				eventuallyInvalidEgressProblem(cs, ns.Name, pods[i], egressName, c.problem)
			}
			eventuallyInvalidEgressProblem(cs, ns.Name, noAddress, egressName, "IPv4: no public address")
			// Every problem is listed: those of an IP family first, then by annotation.
			eventuallyPodEvent(cs, ns.Name, []string{several}, consts.ServiceEgressGatewayReasonInvalidConfig, v1.EventTypeWarning,
				"The egress annotations of this pod are invalid: "+
					"IPv4: no public address (the Public IP count is 0 and no prefix or user Public IP is set); "+
					idleKey+`: "3" is not an integer from 4 to 120; `+tagsKey+`: "=a" has an empty key. The pod keeps egress`)

			By("Every member stays registered, and nothing is written to the NAT Gateway or its Public IPs")
			eventuallyEgressMembersRegistered(cs, egressName, ns.Name)
			Consistently(func() error {
				if err := egressEtagsUnchangedErr(etags); err != nil {
					return err
				}
				want, err := egressMemberAddresses(cs, egressName, ns.Name)
				if err != nil {
					return err
				}
				return egressRegisteredMatchErr(egressName, want)
			}, time.Minute, defaultPollInterval).Should(Succeed(), "invalid votes change nothing, and their pods keep egress")
			expectNoNewEgressWrites(ccmClient, egressName, writes)
			Expect(egressEventCounts(cs, ns.Name, append(slices.Clone(pods), noAddress), consts.ServiceEgressGatewayReasonInvalidConfig, "")).
				To(HaveEach(Equal(int32(1))), "every member joined once: it is told once")
			expectNoPodEvent(cs, ns.Name, append([]string{valid}, outsiders...), consts.ServiceEgressGatewayReasonInvalidConfig,
				consts.ServiceEgressGatewayReasonConfigConflict, consts.ServiceEgressGatewayReasonConfigApplied,
				consts.ServiceEgressGatewayReasonConfigRejected)

			By("Changing the pod without a public address from Public IP count 0 to 00, then to 17: only the new problem is told")
			setEgressPodAnnotations(cs, ns.Name, map[string]string{countKey: "00"}, noAddress)
			setEgressPodAnnotations(cs, ns.Name, map[string]string{countKey: "17"}, noAddress)
			eventuallyInvalidEgressProblem(cs, ns.Name, noAddress, egressName, countKey+`: "17" is not an integer from 0 to 16`)
			// The controller handles the updates of a pod in order: count 00 was handled before 17 was told.
			Expect(egressEventCounts(cs, ns.Name, []string{noAddress}, consts.ServiceEgressGatewayReasonInvalidConfig, "", "IPv4: no public address")).
				To(HaveKeyWithValue(noAddress, int32(1)), "an edit that leaves the same problem must not tell the pod again")

			By("Deleting the other invalid pods and asking for idle timeout 6 on the valid pod: it is told the pods disagree")
			for _, pod := range pods {
				deleteEgressPod(cs, ns.Name, pod)
			}
			setEgressPodAnnotations(cs, ns.Name, map[string]string{idleKey: "6"}, valid)
			eventuallyPodEvent(cs, ns.Name, []string{valid}, consts.ServiceEgressGatewayReasonConfigConflict, v1.EventTypeWarning,
				"votes for idle timeout 6m")

			By("Fixing the invalid pod with idle timeout 6: it is told nothing new, and the agreement is applied")
			setEgressPodAnnotations(cs, ns.Name, map[string]string{countKey: "", idleKey: "6"}, noAddress)
			agreed := defaults
			agreed.idleTimeout = 6
			eventuallyEgressNAT(egressName, agreed)
			eventuallyEachPodApplied(cs, ns.Name, []string{valid, noAddress}, "idle timeout 6m")
			Expect(egressEventCounts(cs, ns.Name, []string{noAddress}, consts.ServiceEgressGatewayReasonInvalidConfig, "")).To(
				HaveKeyWithValue(noAddress, int32(2)), "fixing the annotations must not tell the pod they are invalid again")
			expectNoPodEvent(cs, ns.Name, []string{noAddress}, consts.ServiceEgressGatewayReasonConfigConflict)

			By("Deleting the pods: the NAT Gateway and its Public IPs are deleted")
			deleteEgressPods(cs, ns.Name, egressName)
			eventuallyEgressIdentityGone(egressName)
		})

		It("should count equivalent spellings of a configuration as one vote and write nothing when pods switch between them", func() {
			egressName, defaultName := "egvote-equal-"+suffix, "egvote-dflt-"+suffix
			idleKey := consts.PodAnnotationServiceEgressGatewayIdleTimeout
			countKey := consts.PodAnnotationServiceEgressGatewayPublicIPCount
			countV6Key := consts.PodAnnotationServiceEgressGatewayPublicIPCountIPv6
			ipsKey := consts.PodAnnotationServiceEgressGatewayPublicIPIDs
			tagsKey := consts.PodAnnotationServiceEgressGatewayTags

			By("Creating two StandardV2 Public IPs in a test resource group of the cluster's subscription")
			rg, granted := createEgressUserResourceGroup(suffix)
			pipA := createBYOPublicIP(rg, "egvote-pip-a", "IPv4")
			pipB := createBYOPublicIP(rg, "egvote-pip-b", "IPv4")

			// Spellings of idle timeout 10, the two user Public IPs and the tags e2e-a=1 and E2E-b=Two: leading zeros, spaces
			// around values and list entries, list order, resource IDs in another case, and counts set to their default (no
			// managed IPv4 Public IP next to user ones, one managed IPv6 Public IP).
			spellings := []map[string]string{
				{idleKey: "10", ipsKey: pipA + "," + pipB, tagsKey: "E2E-b=Two,e2e-a=1"},
				{idleKey: "010", ipsKey: " " + pipB + " , " + pipA + " ", tagsKey: " e2e-a = 1 , E2E-b = Two "},
				{idleKey: " 10 ", ipsKey: strings.ToUpper(pipA) + "," + pipB, tagsKey: "E2E-b=Two, e2e-a=1", countKey: "0"},
				{idleKey: "10 ", ipsKey: pipB + "," + strings.ToLower(pipA), tagsKey: "e2e-a=1,E2E-b=Two", countKey: "00"},
			}
			// The default spelled explicitly, two ways, and removed.
			explicitDefaults := []map[string]string{{idleKey: "4", countKey: "1"}, {idleKey: "04", countKey: " 1 "}, {idleKey: "", countKey: ""}}
			if ipv6 {
				spellings[2][countV6Key], spellings[3][countV6Key] = "1", "01"
				explicitDefaults[0][countV6Key], explicitDefaults[1][countV6Key], explicitDefaults[2][countV6Key] = "1", " 1", ""
			}

			By("Creating a pod of identity D without annotations and one that sets the default explicitly, and a pod of identity Q")
			defaultPods := []string{
				createEgressConfigPod(cs, newEgressConfigPod(ns.Name, defaultName, 0, nil, false)),
				createEgressConfigPod(cs, newEgressConfigPod(ns.Name, defaultName, 1, explicitDefaults[0], false)),
			}
			equalPods := []string{createEgressConfigPod(cs, newEgressConfigPod(ns.Name, egressName, 0, spellings[0], false))}
			Expect(utils.WaitPodsToBeReady(cs, ns.Name)).To(Succeed())

			By("D's pods agree on the default: its NAT Gateway gets it, and nothing is announced")
			eventuallyEgressNAT(defaultName, defaultEgressNATWant(defaultName, ipv6))
			eventuallyManagedRegistered(cs, ns.Name, defaultName, byoNATGatewayARMID(resourceGroupName, defaultName))

			By("Q's NAT Gateway gets idle timeout 10, the user Public IPs and the tags, listed by key, which its pod is told")
			want := defaultEgressNATWant(egressName, ipv6)
			want.idleTimeout = 10
			want.publicIPs = []string{pipA, pipB}
			want.tags = map[string]string{"e2e-a": "1", "E2E-b": "Two"}
			eventuallyEgressNATWithUserResources(cs, ns.Name, equalPods, egressName, want, rg, granted)
			eventuallyManagedRegistered(cs, ns.Name, egressName, byoNATGatewayARMID(resourceGroupName, egressName))
			eventuallyEachPodAppliedWithUserResources(cs, ns.Name, equalPods, "idle timeout 10m",
				"user IPs (egvote-pip-a, egvote-pip-b)", "tags: e2e-a=1,E2E-b=Two.")
			etags := egressResourceEtags(egressName)
			maps.Copy(etags, egressResourceEtags(defaultName))
			Consistently(func() error { return egressEtagsUnchangedErr(etags) }, 30*time.Second, defaultPollInterval).Should(Succeed(),
				"precondition: the controller has finished writing the identities' resources")
			ccmClient, writes := ccmEgressWritesSoFar(egressName)
			_, defaultWrites := ccmEgressWritesSoFar(defaultName)
			events := egressConfigEventCounts(cs, ns.Name)

			By("Two pods join Q with other spellings, then every pod switches to another one: one vote, nothing written or announced")
			for i, spelling := range spellings[1:3] {
				equalPods = append(equalPods, createEgressConfigPod(cs, newEgressConfigPod(ns.Name, egressName, i+1, spelling, false)))
			}
			Expect(utils.WaitPodsToBeReady(cs, ns.Name)).To(Succeed())
			eventuallyEgressMembersRegistered(cs, egressName, ns.Name)
			for i, spelling := range []map[string]string{spellings[1], spellings[0], spellings[3]} {
				setEgressPodAnnotations(cs, ns.Name, spelling, equalPods[i])
			}
			setEgressPodAnnotations(cs, ns.Name, explicitDefaults[1], defaultPods[0])
			setEgressPodAnnotations(cs, ns.Name, explicitDefaults[2], defaultPods[1])
			Consistently(func() error { return egressEtagsUnchangedErr(etags) }, time.Minute, defaultPollInterval).Should(Succeed(),
				"votes spelled differently are one vote: there is nothing to apply")
			Expect(egressConfigEventCounts(cs, ns.Name)).To(Equal(events), "votes spelled differently are one vote: there is nothing to announce")
			expectNoNewEgressWrites(ccmClient, egressName, writes)
			expectNoNewEgressWrites(ccmClient, defaultName, defaultWrites)
			Expect(egressEventCounts(cs, ns.Name, equalPods[:1], consts.ServiceEgressGatewayReasonConfigApplied, "")).To(
				HaveKeyWithValue(equalPods[0], int32(1)), "Q's configuration was applied and announced once")
			expectNoPodEvent(cs, ns.Name, slices.Concat(equalPods[1:], defaultPods), consts.ServiceEgressGatewayReasonInvalidConfig,
				consts.ServiceEgressGatewayReasonConfigConflict, consts.ServiceEgressGatewayReasonConfigApplied)

			By("A pod joins Q with the same tags, their keys in upper case, and another with the value of E2E-b in lower case: tags must " +
				"be spelled identically, so both disagree")
			upperTags, lowerValueTags := maps.Clone(spellings[0]), maps.Clone(spellings[0])
			upperTags[tagsKey], lowerValueTags[tagsKey] = "E2E-B=Two,E2E-A=1", "e2e-a=1,E2E-b=two"
			upper := createEgressConfigPod(cs, newEgressConfigPod(ns.Name, egressName, len(equalPods), upperTags, false))
			lowerValue := createEgressConfigPod(cs, newEgressConfigPod(ns.Name, egressName, len(equalPods)+1, lowerValueTags, false))
			eventuallyPodEvent(cs, ns.Name, []string{upper}, consts.ServiceEgressGatewayReasonConfigConflict, v1.EventTypeWarning,
				"tags: E2E-A=1,E2E-B=Two.")
			eventuallyPodEvent(cs, ns.Name, []string{lowerValue}, consts.ServiceEgressGatewayReasonConfigConflict, v1.EventTypeWarning,
				"tags: e2e-a=1,E2E-b=two.")
			Consistently(func() error { return egressEtagsUnchangedErr(etags) }, 30*time.Second, defaultPollInterval).Should(Succeed(),
				"a disagreement changes nothing")

			if egressAccessDenial(cs, ns.Name, equalPods[:1]) != "" {
				// The controller was refused the user Public IPs until the role the spec assigned applied: Q was created
				// with the default, then updated. Azure may have refused writes until then, which the controller logs as
				// writes too, so the applies that wrote are counted.
				expectEgressAppliesWritingAtMost(egressName, 2, "Q is written when it is created and once more when its configuration is applied")
			} else {
				expectEgressNATWritesAtMost(egressName, 1, "Q's NAT Gateway is written once, when it is created")
			}
			expectEgressNATWritesAtMost(defaultName, 1, "D's NAT Gateway is written once, when it is created")

			By("Deleting the pods: both NAT Gateways are deleted, the user Public IPs are kept")
			deleteEgressPods(cs, ns.Name, egressName)
			deleteEgressPods(cs, ns.Name, defaultName)
			eventuallyEgressIdentityGone(egressName)
			eventuallyEgressIdentityGone(defaultName)
			expectUserEgressResourcesReleased(pipA, pipB)
		})

		It("should count the vote of every member: in another namespace, Pending with an address or labelled in upper case", func() {
			egressName := "egvote-member-" + suffix
			idle := func(minutes string) map[string]string {
				return map[string]string{consts.PodAnnotationServiceEgressGatewayIdleTimeout: minutes}
			}

			By("Creating two pods without annotations: the NAT Gateway gets the default configuration")
			pods := createConfiguredEgressPods(cs, ns.Name, egressName, 2, nil, false)
			Expect(utils.WaitPodsToBeReady(cs, ns.Name)).To(Succeed())
			want := defaultEgressNATWant(egressName, ipv6)
			eventuallyEgressNAT(egressName, want)
			eventuallyManagedRegistered(cs, ns.Name, egressName, byoNATGatewayARMID(resourceGroupName, egressName))
			natBefore := readEgressNAT(egressName)

			By("Creating a pod that asks for idle timeout 30 and cannot be scheduled: Pending without an address, it is no member")
			pending := newEgressConfigPod(ns.Name, egressName, 2, idle("30"), false)
			pending.Spec.NodeSelector = map[string]string{v1.LabelHostname: "egvote-no-such-node"}
			createEgressConfigPod(cs, pending)

			By("Creating a pod of the identity in another namespace that asks for idle timeout 12: it is told the pods disagree")
			var err error
			otherNS, err = utils.CreateTestingNamespace(basename, cs)
			Expect(err).NotTo(HaveOccurred())
			other := createEgressConfigPod(cs, newEgressConfigPod(otherNS.Name, egressName, 3, idle("12"), false))
			eventuallyPodEvent(cs, otherNS.Name, []string{other}, consts.ServiceEgressGatewayReasonConfigConflict, v1.EventTypeWarning,
				fmt.Sprintf("Egress identity %q", egressName), "votes for idle timeout 12m")

			By("Asking for idle timeout 6 on the first two pods: the pod in the other namespace blocks the change")
			etags := egressResourceEtags(egressName)
			setEgressPodAnnotations(cs, ns.Name, idle("6"), pods...)
			Consistently(func() error { return egressEtagsUnchangedErr(etags) }, time.Minute, defaultPollInterval).Should(Succeed(),
				"a pod of the identity in another namespace is a member: its vote counts")

			By("Asking for idle timeout 6 in the other namespace too: the change is applied, although the Pending pod asks for 30")
			setEgressPodAnnotations(cs, otherNS.Name, idle("6"), other)
			want.idleTimeout = 6
			eventuallyEgressNAT(egressName, want)
			expectSameEgressNAT(egressName, natBefore)
			eventuallyEachPodApplied(cs, ns.Name, pods, "idle timeout 6m")
			eventuallyEachPodApplied(cs, otherNS.Name, []string{other}, "idle timeout 6m")

			By("Creating a pod labelled with the identity in upper case that asks for idle timeout 14: it joins the identity")
			// Created directly: its label names no other identity to clean up.
			upper := newEgressConfigPod(ns.Name, egressName, 4, idle("14"), false)
			upper.Labels[egressLabel] = strings.ToUpper(egressName)
			_, err = cs.CoreV1().Pods(ns.Name).Create(context.TODO(), upper, metav1.CreateOptions{})
			Expect(err).NotTo(HaveOccurred())
			eventuallyPodEvent(cs, ns.Name, []string{upper.Name}, consts.ServiceEgressGatewayReasonConfigConflict, v1.EventTypeWarning,
				fmt.Sprintf("Egress identity %q", egressName), "votes for idle timeout 14m")

			By("Creating a pod that asks for idle timeout 16 and whose init container runs for 3 minutes: Pending with an address, it is a member")
			initializing := newEgressConfigPod(ns.Name, egressName, 5, idle("16"), false)
			initializing.Spec.InitContainers = []v1.Container{{
				Name: "init", Image: utils.AgnhostImage, ImagePullPolicy: v1.PullIfNotPresent, Command: []string{"/bin/sh", "-c", "sleep 180"},
			}}
			createEgressConfigPod(cs, initializing)
			eventuallyPodEvent(cs, ns.Name, []string{initializing.Name}, consts.ServiceEgressGatewayReasonConfigConflict, v1.EventTypeWarning,
				"votes for idle timeout 16m")

			By("Asking for idle timeout 8 on every other member: the Pending pod blocks the change")
			etags = egressResourceEtags(egressName)
			setEgressPodAnnotations(cs, ns.Name, idle("8"), pods[0], pods[1], upper.Name)
			setEgressPodAnnotations(cs, otherNS.Name, idle("8"), other)
			Consistently(func() error { return egressEtagsUnchangedErr(etags) }, 45*time.Second, defaultPollInterval).Should(Succeed(),
				"a Pending pod with an address is a member: its vote counts")
			pod, err := cs.CoreV1().Pods(ns.Name).Get(context.TODO(), initializing.Name, metav1.GetOptions{})
			Expect(err).NotTo(HaveOccurred())
			Expect(pod.Status.Phase).To(Equal(v1.PodPending), "pod %s must still be initializing", initializing.Name)

			By("Asking for idle timeout 8 on the Pending pod too: the change is applied, and every member is told")
			setEgressPodAnnotations(cs, ns.Name, idle("8"), initializing.Name)
			want.idleTimeout = 8
			eventuallyEgressNAT(egressName, want)
			expectSameEgressNAT(egressName, natBefore)
			eventuallyEachPodApplied(cs, ns.Name, []string{pods[0], pods[1], upper.Name, initializing.Name}, "idle timeout 8m")
			eventuallyEachPodApplied(cs, otherNS.Name, []string{other}, "idle timeout 8m")
			eventuallyEgressMembersRegistered(cs, egressName, ns.Name, otherNS.Name)

			By("The pod Pending without an address never got an address nor an event")
			pod, err = cs.CoreV1().Pods(ns.Name).Get(context.TODO(), pending.Name, metav1.GetOptions{})
			Expect(err).NotTo(HaveOccurred())
			Expect(pod.Status.Phase).To(Equal(v1.PodPending))
			Expect(pod.Status.PodIPs).To(BeEmpty())
			expectNoPodEvent(cs, ns.Name, []string{pending.Name}, consts.ServiceEgressGatewayReasonInvalidConfig,
				consts.ServiceEgressGatewayReasonConfigConflict, consts.ServiceEgressGatewayReasonConfigApplied,
				consts.ServiceEgressGatewayReasonConfigRejected)

			By("Deleting the pods: the NAT Gateway and its Public IPs are deleted")
			deleteEgressPod(cs, ns.Name, upper.Name)
			deleteEgressPods(cs, ns.Name, egressName)
			deleteEgressPods(cs, otherNS.Name, egressName)
			eventuallyEgressIdentityGone(egressName)
		})

		It("should stop counting the vote of a pod once it completed, and as soon as it is deleted", func() {
			egressName := "egvote-leave-" + suffix
			idle := func(minutes string) map[string]string {
				return map[string]string{consts.PodAnnotationServiceEgressGatewayIdleTimeout: minutes}
			}

			By("Creating two pods without annotations: the NAT Gateway gets the default configuration")
			pods := createConfiguredEgressPods(cs, ns.Name, egressName, 2, nil, false)
			Expect(utils.WaitPodsToBeReady(cs, ns.Name)).To(Succeed())
			want := defaultEgressNATWant(egressName, ipv6)
			eventuallyEgressNAT(egressName, want)
			eventuallyManagedRegistered(cs, ns.Name, egressName, byoNATGatewayARMID(resourceGroupName, egressName))
			natBefore := readEgressNAT(egressName)

			By("Creating a pod that asks for idle timeout 20 and exits after 90 seconds")
			completing := newEgressConfigPod(ns.Name, egressName, 2, idle("20"), false)
			completing.Spec.RestartPolicy = v1.RestartPolicyNever
			completing.Spec.Containers[0].Command, completing.Spec.Containers[0].Args = []string{"/bin/sh", "-c", "sleep 90"}, nil
			createEgressConfigPod(cs, completing)
			eventuallyEgressMember(cs, ns.Name, completing.Name)

			By("Asking for idle timeout 8 on the other pods: the pod blocks the change while it runs, and stops voting once it completed")
			setEgressPodAnnotations(cs, ns.Name, idle("8"), pods...)
			want.idleTimeout = 8
			Eventually(func() error {
				nat, found, err := getAzureNetworkResource(natBefore.ID)
				if err != nil {
					return err
				}
				pod, err := cs.CoreV1().Pods(ns.Name).Get(context.TODO(), completing.Name, metav1.GetOptions{})
				switch {
				case err != nil:
					return err
				case !found:
					return fmt.Errorf("NAT Gateway %s does not exist", egressName)
				// The phase is read after the NAT Gateway, and the controller learns of the new phase only after it is set.
				case nat.Etag != natBefore.Etag && pod.Status.Phase == v1.PodRunning:
					Fail(fmt.Sprintf("NAT Gateway %s was written while pod %s, which asks for idle timeout 20, was still running",
						egressName, completing.Name))
				case pod.Status.Phase != v1.PodSucceeded:
					return fmt.Errorf("pod %s is %s", completing.Name, pod.Status.Phase)
				}
				return egressNATShapeErr(egressName, want)
			}, egressApplyTimeout, defaultPollInterval).Should(Succeed(), "a completed pod no longer votes")
			expectSameEgressNAT(egressName, natBefore)
			eventuallyEachPodApplied(cs, ns.Name, pods, "idle timeout 8m")
			eventuallyPodEvent(cs, ns.Name, []string{completing.Name}, consts.ServiceEgressGatewayReasonConfigConflict, v1.EventTypeWarning,
				"votes for idle timeout 20m")
			expectNoPodEvent(cs, ns.Name, []string{completing.Name}, consts.ServiceEgressGatewayReasonConfigApplied)
			eventuallyEgressMembersRegistered(cs, egressName, ns.Name)

			By("Creating a pod that asks for idle timeout 25 and takes 10 minutes to terminate")
			terminating := newEgressConfigPod(ns.Name, egressName, 3, idle("25"), false)
			grace := int64(600)
			terminating.Spec.TerminationGracePeriodSeconds = &grace
			terminating.Spec.Containers[0].Lifecycle = &v1.Lifecycle{PreStop: &v1.LifecycleHandler{
				Exec: &v1.ExecAction{Command: []string{"/bin/sh", "-c", "sleep 600"}},
			}}
			createEgressConfigPod(cs, terminating)
			removeTerminating := func() {
				immediately := int64(0)
				err := cs.CoreV1().Pods(ns.Name).Delete(context.TODO(), terminating.Name, metav1.DeleteOptions{GracePeriodSeconds: &immediately})
				if err != nil && !apierrors.IsNotFound(err) {
					utils.Logf("deleting pod %s at once: %v", terminating.Name, err)
				}
			}
			// Also when the spec fails before it: a pod that takes 10 minutes to terminate would hold the namespace's deletion.
			defer removeTerminating()
			eventuallyEgressMember(cs, ns.Name, terminating.Name)

			By("Asking for idle timeout 15 on the other pods: the pod blocks the change")
			etags := egressResourceEtags(egressName)
			setEgressPodAnnotations(cs, ns.Name, idle("15"), pods...)
			Consistently(func() error { return egressEtagsUnchangedErr(etags) }, 45*time.Second, defaultPollInterval).Should(Succeed(),
				"the running pod's vote counts")

			By("Deleting the pod: it stops voting at once, and idle timeout 15 is applied while it still terminates")
			Expect(cs.CoreV1().Pods(ns.Name).Delete(context.TODO(), terminating.Name, metav1.DeleteOptions{})).To(Succeed())
			want.idleTimeout = 15
			eventuallyEgressNAT(egressName, want)
			pod, err := cs.CoreV1().Pods(ns.Name).Get(context.TODO(), terminating.Name, metav1.GetOptions{})
			Expect(err).NotTo(HaveOccurred(), "pod %s must still be terminating when the change is applied", terminating.Name)
			Expect(pod.DeletionTimestamp).NotTo(BeNil())
			expectSameEgressNAT(egressName, natBefore)
			eventuallyEachPodApplied(cs, ns.Name, pods, "idle timeout 15m")
			expectNoPodEvent(cs, ns.Name, []string{terminating.Name}, consts.ServiceEgressGatewayReasonConfigApplied)
			eventuallyEgressMembersRegistered(cs, egressName, ns.Name)

			By("Deleting the pods: the NAT Gateway and its Public IPs are deleted")
			removeTerminating()
			deleteEgressPods(cs, ns.Name, egressName)
			eventuallyEgressIdentityGone(egressName)
		})

		It("should create an identity with its first pod's vote, or the default when that vote is invalid, and keep it while later pods disagree", func() {
			firstName, invalidName := "egvote-first-"+suffix, "egvote-invfirst-"+suffix
			idleKey := consts.PodAnnotationServiceEgressGatewayIdleTimeout
			idle := func(minutes string) map[string]string { return map[string]string{idleKey: minutes} }

			By("Creating the first pod of identity F, which asks for idle timeout 7, and of identity I, whose idle timeout is invalid")
			creators := []string{
				createEgressConfigPod(cs, newEgressConfigPod(ns.Name, firstName, 0, idle("7"), false)),
				createEgressConfigPod(cs, newEgressConfigPod(ns.Name, invalidName, 0, idle("1000"), false)),
			}
			for _, pod := range creators {
				eventuallyEgressMember(cs, ns.Name, pod)
			}

			By("Once the controller made them members, creating a pod of each identity that asks for idle timeout 9")
			late := []string{
				createEgressConfigPod(cs, newEgressConfigPod(ns.Name, firstName, 1, idle("9"), false)),
				createEgressConfigPod(cs, newEgressConfigPod(ns.Name, invalidName, 1, idle("9"), false)),
			}
			Expect(utils.WaitPodsToBeReady(cs, ns.Name)).To(Succeed())

			By("F is created with idle timeout 7, its first pod's vote, and I with the default, as its first pod's vote is invalid")
			want := defaultEgressNATWant(firstName, ipv6)
			want.idleTimeout = 7
			eventuallyEgressNAT(firstName, want)
			eventuallyEgressNAT(invalidName, defaultEgressNATWant(invalidName, ipv6))
			eventuallyManagedRegistered(cs, ns.Name, firstName, byoNATGatewayARMID(resourceGroupName, firstName))
			eventuallyManagedRegistered(cs, ns.Name, invalidName, byoNATGatewayARMID(resourceGroupName, invalidName))
			eventuallyEachPodApplied(cs, ns.Name, creators[:1], "idle timeout 7m")
			eventuallyEachPodEvent(cs, ns.Name, late, consts.ServiceEgressGatewayReasonConfigConflict, v1.EventTypeWarning,
				"votes for idle timeout 9m")
			eventuallyInvalidEgressProblem(cs, ns.Name, creators[1], invalidName, idleKey+`: "1000" is not an integer from 4 to 120`)
			etags := egressResourceEtags(firstName)
			maps.Copy(etags, egressResourceEtags(invalidName))
			Consistently(func() error {
				if err := egressEtagsUnchangedErr(etags); err != nil {
					return err
				}
				if err := egressNATShapeErr(firstName, want); err != nil {
					return err
				}
				return egressNATShapeErr(invalidName, defaultEgressNATWant(invalidName, ipv6))
			}, 30*time.Second, defaultPollInterval).Should(Succeed(), "without an agreement each identity keeps the configuration it was created with")
			expectNoPodEvent(cs, ns.Name, []string{creators[1], late[1]}, consts.ServiceEgressGatewayReasonConfigApplied)
			natBefore := readEgressNAT(firstName)

			By("Asking for idle timeout 9 on F's first pod too: the late pod's value is applied in place, and both are told")
			setEgressPodAnnotations(cs, ns.Name, idle("9"), creators[0])
			want.idleTimeout = 9
			eventuallyEgressNAT(firstName, want)
			expectSameEgressNAT(firstName, natBefore)
			eventuallyEachPodApplied(cs, ns.Name, []string{creators[0], late[0]}, "idle timeout 9m")

			By("Deleting the pods: both NAT Gateways and their Public IPs are deleted")
			deleteEgressPods(cs, ns.Name, firstName)
			deleteEgressPods(cs, ns.Name, invalidName)
			eventuallyEgressIdentityGone(firstName)
			eventuallyEgressIdentityGone(invalidName)
		})

		It("should move a relabelled pod's vote to its new identity and release a pod whose egress label is removed", func() {
			xName, yName, bName := "egvote-movex-"+suffix, "egvote-movey-"+suffix, "egvote-moveb-"+suffix
			idleKey := consts.PodAnnotationServiceEgressGatewayIdleTimeout
			countKey := consts.PodAnnotationServiceEgressGatewayPublicIPCount

			By("Creating three pods of identity X that ask for two managed Public IPs and two pods of identity Y that ask for idle timeout 9")
			xPods := createConfiguredEgressPods(cs, ns.Name, xName, 3, map[string]string{countKey: "2"}, false)
			createConfiguredEgressPods(cs, ns.Name, yName, 2, map[string]string{idleKey: "9"}, false)
			Expect(utils.WaitPodsToBeReady(cs, ns.Name)).To(Succeed())
			xWant := defaultEgressNATWant(xName, ipv6)
			xWant.publicIPs = egressManagedPublicIPIDs(xName, false, 2)
			yWant := defaultEgressNATWant(yName, ipv6)
			yWant.idleTimeout = 9
			eventuallyEgressNAT(xName, xWant)
			eventuallyEgressNAT(yName, yWant)
			eventuallyManagedRegistered(cs, ns.Name, xName, byoNATGatewayARMID(resourceGroupName, xName))
			eventuallyManagedRegistered(cs, ns.Name, yName, byoNATGatewayARMID(resourceGroupName, yName))
			xNAT := readEgressNAT(xName)

			By("Asking for three managed Public IPs on two of X's pods: the third pod blocks the change")
			xEtags := egressResourceEtags(xName)
			setEgressPodAnnotations(cs, ns.Name, map[string]string{countKey: "3"}, xPods[0], xPods[1])
			Consistently(func() error { return egressEtagsUnchangedErr(xEtags) }, 45*time.Second, defaultPollInterval).Should(Succeed(),
				"the third pod's vote blocks the change")

			By("Moving the third pod to a new identity B: B is created with its vote, X's remaining pods agree, and each pod is told of its identity only")
			setEgressPodLabel(cs, ns.Name, xPods[2], bName)
			bWant := defaultEgressNATWant(bName, ipv6)
			bWant.publicIPs = egressManagedPublicIPIDs(bName, false, 2)
			eventuallyEgressNAT(bName, bWant)
			eventuallyEachPodApplied(cs, ns.Name, xPods[2:], fmt.Sprintf("Egress identity %q", bName), "IPv4: 2 managed IPs")
			xWant.publicIPs = egressManagedPublicIPIDs(xName, false, 3)
			eventuallyEgressNAT(xName, xWant)
			expectSameEgressNAT(xName, xNAT)
			xApplied := []string{fmt.Sprintf("Egress identity %q", xName), "IPv4: 3 managed IPs"}
			eventuallyEachPodApplied(cs, ns.Name, xPods[:2], xApplied...)
			Expect(egressEventCounts(cs, ns.Name, xPods[2:], consts.ServiceEgressGatewayReasonConfigApplied, "", xApplied[1])).To(
				HaveKeyWithValue(xPods[2], BeZero()), "the moved pod is no member of X any more")
			eventuallyEgressMembersRegistered(cs, xName, ns.Name)
			eventuallyEgressMembersRegistered(cs, bName, ns.Name)

			By("Moving it on to Y, whose pods disagree with it: it is told so, Y keeps its configuration, and B, left without pods, is deleted")
			yEtags := egressResourceEtags(yName)
			setEgressPodLabel(cs, ns.Name, xPods[2], yName)
			eventuallyPodEvent(cs, ns.Name, xPods[2:], consts.ServiceEgressGatewayReasonConfigConflict, v1.EventTypeWarning,
				fmt.Sprintf("Egress identity %q", yName), "IPv4: 2 managed IPs")
			eventuallyEgressMembersRegistered(cs, yName, ns.Name)

			By("A pod joins X asking for idle timeout 15, then its egress label is removed: X does not change, and the pod is released at once")
			xEtags = egressResourceEtags(xName)
			xEvents := egressEventTally(cs, ns.Name, xPods[:2], egressConfigEventReasons...)
			z := createEgressConfigPod(cs, newEgressConfigPod(ns.Name, xName, 3, map[string]string{idleKey: "15"}, false))
			eventuallyPodEvent(cs, ns.Name, []string{z}, consts.ServiceEgressGatewayReasonConfigConflict, v1.EventTypeWarning,
				fmt.Sprintf("Egress identity %q", xName), "votes for idle timeout 15m")
			setEgressPodLabel(cs, ns.Name, z, "")
			eventuallyPodReleased(cs, ns.Name, z)
			deleteEgressPod(cs, ns.Name, z)
			Eventually(func() error {
				return egressPodGoneErr(cs, ns.Name, z)
			}, 2*time.Minute, 5*time.Second).Should(Succeed(), "pod %s, released, must be deleted at once", z)
			Consistently(func() error { return egressEtagsUnchangedErr(xEtags) }, 30*time.Second, defaultPollInterval).Should(Succeed(),
				"a pod that joins and leaves X without agreeing changes nothing")
			Expect(egressEventTally(cs, ns.Name, xPods[:2], egressConfigEventReasons...)).To(Equal(xEvents), "X's pods are told nothing")
			eventuallyEgressIdentityGone(bName)
			Expect(egressEtagsUnchangedErr(yEtags)).To(Succeed(), "Y keeps its configuration while the moved pod disagrees")

			By("A pod joins X asking for idle timeout 20 and blocks X's pods' agreement on idle timeout 12; relabelled default-natgw, " +
				"which no pod may join, it is told so and leaves X, which applies idle timeout 12 in place, while default-natgw is not " +
				"touched; deleted, it is released at once")
			w := createEgressConfigPod(cs, newEgressConfigPod(ns.Name, xName, 4, map[string]string{countKey: "3", idleKey: "20"}, false))
			eventuallyPodEvent(cs, ns.Name, []string{w}, consts.ServiceEgressGatewayReasonConfigConflict, v1.EventTypeWarning,
				fmt.Sprintf("Egress identity %q", xName), "votes for idle timeout 20m")
			eventuallyEgressMembersRegistered(cs, xName, ns.Name)
			xEtags = egressResourceEtags(xName)
			setEgressPodAnnotations(cs, ns.Name, map[string]string{idleKey: "12"}, xPods[0], xPods[1])
			Consistently(func() error { return egressEtagsUnchangedErr(xEtags) }, 45*time.Second, defaultPollInterval).Should(Succeed(),
				"the new pod's vote blocks the change")
			defaultNATGatewayID := defaultOutboundNATGatewayID()
			Expect(defaultNATGatewayID).NotTo(BeEmpty(), "the cluster must have a default outbound service")
			defaultEtags := azureResourceEtags(defaultNATGatewayID)
			// Patched directly: setEgressPodLabel would have the AfterEach expect default-natgw to be gone.
			_, err := cs.CoreV1().Pods(ns.Name).Patch(context.TODO(), w, types.MergePatchType,
				[]byte(fmt.Sprintf(`{"metadata":{"labels":{%q:"default-natgw"}}}`, egressLabel)), metav1.PatchOptions{})
			Expect(err).NotTo(HaveOccurred())
			eventuallyPodEvent(cs, ns.Name, []string{w}, "ServiceGatewayReservedEgressLabel", v1.EventTypeWarning,
				`Egress gateway label "default-natgw" is reserved`)
			eventuallyEgressMembersRegistered(cs, xName, ns.Name)
			xWant.idleTimeout = 12
			eventuallyEgressNAT(xName, xWant)
			expectSameEgressNAT(xName, xNAT)
			eventuallyEachPodApplied(cs, ns.Name, xPods[:2], fmt.Sprintf("Egress identity %q", xName), "idle timeout 12m")
			Expect(egressEtagsUnchangedErr(defaultEtags)).To(Succeed(), "the default outbound service's NAT Gateway must not be written")
			Expect(defaultOutboundNATGatewayID()).To(Equal(defaultNATGatewayID), "the default outbound service must keep its NAT Gateway")
			deleteEgressPod(cs, ns.Name, w)
			Eventually(func() error {
				return egressPodGoneErr(cs, ns.Name, w)
			}, 2*time.Minute, 5*time.Second).Should(Succeed(), "pod %s must be released when it is deleted", w)

			By("Deleting X's first pod and removing the label from its last one: X is deleted with its NAT Gateway and all its Public IPs")
			deleteEgressPod(cs, ns.Name, xPods[0])
			eventuallyEgressMembersRegistered(cs, xName, ns.Name)
			setEgressPodLabel(cs, ns.Name, xPods[1], "")
			eventuallyEgressIdentityGone(xName)
			eventuallyPodReleased(cs, ns.Name, xPods[1])
			Expect(egressEtagsUnchangedErr(yEtags)).To(Succeed())

			By("Deleting Y's pods: Y is deleted")
			deleteEgressPods(cs, ns.Name, yName)
			eventuallyEgressIdentityGone(yName)
		})

		It("should move an identity from its managed Public IP to a user Public IP and prefix in place and back without modifying them", func() {
			egressName := "egvote-touser-" + suffix
			ipsKey := consts.PodAnnotationServiceEgressGatewayPublicIPIDs
			prefixesKey := consts.PodAnnotationServiceEgressGatewayPublicIPPrefixIDs

			By("Creating a StandardV2 Public IP and /30 Public IP prefix in a test resource group of the cluster's subscription")
			rg, granted := createEgressUserResourceGroup(suffix)
			// Named as the reserved names of an identity would be, so that ccmEgressWrites finds any write to them.
			userName := "egvote-user-" + suffix
			userIP := createBYOPublicIP(rg, userName+"-pip", "IPv4")
			userPrefix := createBYOPublicIPPrefix(rg, userName+"-prefix", "IPv4", 30)
			configurations := userEgressResourceConfigurations(userIP, userPrefix)
			userAddresses := egressAddressesOf(userIP, userPrefix)
			ccmClient, userWrites := ccmEgressWritesSoFar(userName)

			By("Creating two pods that ask for a tag: the NAT Gateway gets the tagged <id>-pip")
			pods := createConfiguredEgressPods(cs, ns.Name, egressName, 2,
				map[string]string{consts.PodAnnotationServiceEgressGatewayTags: "e2e-team=payments"}, true)
			Expect(utils.WaitPodsToBeReady(cs, ns.Name)).To(Succeed())
			managed := defaultEgressNATWant(egressName, ipv6)
			managed.tags = map[string]string{"e2e-team": "payments"}
			eventuallyEgressNAT(egressName, managed)
			eventuallyManagedRegistered(cs, ns.Name, egressName, byoNATGatewayARMID(resourceGroupName, egressName))
			appliedManaged := []string{"IPv4: 1 managed IP", "tags: e2e-team=payments."}
			eventuallyEachPodApplied(cs, ns.Name, pods, appliedManaged...)
			natBefore := readEgressNAT(egressName)
			managedAddress := egressAddressesOf(managed.publicIPs[0])
			first, observable := waitForProbeEgress(cs, ns.Name, pods[0], egressNATAddresses(egressName))

			By("Asking for the user Public IP and prefix, keeping the tag: they replace <id>-pip on the same NAT Gateway")
			var changed int
			if observable {
				changed = egressProbeSampleCount(cs, ns.Name, pods[0])
			}
			setEgressPodAnnotations(cs, ns.Name, map[string]string{ipsKey: userIP, prefixesKey: userPrefix}, pods...)
			user := managed
			user.publicIPs, user.prefixes = []string{userIP}, []string{userPrefix}
			eventuallyEgressNATWithUserResources(cs, ns.Name, pods, egressName, user, rg, granted)
			expectSameEgressNAT(egressName, natBefore)
			eventuallyEachPodApplied(cs, ns.Name, pods,
				fmt.Sprintf("IPv4: user IPs (%s-pip), user prefixes (%s-prefix)", userName, userName), "tags: e2e-team=payments.")
			expectUserEgressResourcesUnchanged(configurations)
			if observable {
				By("Checking egress continued, moved to the user addresses and no longer leaves through <id>-pip's address")
				eventuallyProbeSources(cs, ns.Name, pods[0], changed, userAddresses)
				expectProbeSourcesGone(cs, ns.Name, pods[0], managedAddress)
				expectEgressContinuity(cs, ns.Name, pods[0], first, managedAddress.union(userAddresses))
				changed = egressProbeSampleCount(cs, ns.Name, pods[0])
			}

			By("Removing the address annotations: a new tagged <id>-pip replaces the user resources, which are detached and unchanged")
			setEgressPodAnnotations(cs, ns.Name, map[string]string{ipsKey: "", prefixesKey: ""}, pods...)
			eventuallyEgressNAT(egressName, managed)
			expectSameEgressNAT(egressName, natBefore)
			// The configuration is the one the identity was created with: its Applied event is repeated.
			eventuallyEgressAppliedCounts(cs, ns.Name, egressCountsOf(pods, 2), appliedManaged[0])
			expectUserEgressResourcesReleased(userIP, userPrefix)
			expectUserEgressResourcesUnchanged(configurations)
			expectNoNewEgressWrites(ccmClient, userName, userWrites)
			newAddress := egressAddressesOf(managed.publicIPs[0])
			if observable {
				By("Checking egress continued, moved to the new <id>-pip and no longer leaves through the user addresses")
				eventuallyProbeSources(cs, ns.Name, pods[0], changed, newAddress)
				expectProbeSourcesGone(cs, ns.Name, pods[0], userAddresses)
				expectEgressContinuity(cs, ns.Name, pods[0], first, managedAddress.union(userAddresses).union(newAddress))
			}

			By("Deleting the pods: the NAT Gateway and <id>-pip are deleted, the user resources are kept")
			deleteEgressPods(cs, ns.Name, egressName)
			eventuallyEgressIdentityGone(egressName)
			expectUserEgressResourcesReleased(userIP, userPrefix)
			leaveOutEgressUnlessObservable(observable)
		})

		It("should let the documented admission policy restrict identities and annotations to their namespace without blocking the controller", func() {
			if !admissionPoliciesServed(cs) {
				Skip("the cluster does not serve ValidatingAdmissionPolicy (admissionregistration.k8s.io/v1, Kubernetes 1.30 or later)")
			}
			egressName := ns.Name + "-adm"
			idleKey := consts.PodAnnotationServiceEgressGatewayIdleTimeout
			// Every API server applies a policy, and sees a namespace label, on its own: a request can still reach one that
			// does not yet, so each admission decision is retried until it holds, as dry runs that change nothing.
			deniedErr := func(what string, err error, message string) error {
				switch {
				case err == nil:
					return fmt.Errorf("%s was admitted, want a denial containing %q", what, message)
				case !strings.Contains(err.Error(), message):
					return fmt.Errorf("%s: %w, want a denial containing %q", what, err, message)
				}
				return nil
			}
			denied := func(pod *v1.Pod, message string) error {
				_, err := cs.CoreV1().Pods(pod.Namespace).Create(context.TODO(), pod, metav1.CreateOptions{DryRun: []string{metav1.DryRunAll}})
				return deniedErr("pod "+pod.Name, err, message)
			}
			patchDenied := func(podName string, patch []byte, message string) error {
				_, err := cs.CoreV1().Pods(ns.Name).Patch(context.TODO(), podName, types.MergePatchType, patch,
					metav1.PatchOptions{DryRun: []string{metav1.DryRunAll}})
				return deniedErr("the patch of pod "+podName, err, message)
			}

			By("Binding the user documentation's example policy to this namespace only")
			policy := createEgressAdmissionPolicy(cs, "e2e-egress-votes-"+suffix, ns.Name)
			foreign := newEgressConfigPod(ns.Name, "egvote-foreign-"+suffix, 0, nil, false)
			Eventually(func() error {
				return denied(foreign, "the egress identity must be the namespace name or start with '<namespace>-'")
			}, 2*time.Minute, 5*time.Second).Should(Succeed(), "a pod of an identity not named after its namespace is denied")

			By("A pod with egress annotations is denied while the namespace lacks the label egress.example.com/configure=true")
			annotated := newEgressConfigPod(ns.Name, egressName, 1, map[string]string{idleKey: "4"}, false)
			Eventually(func() error {
				return denied(annotated, "egress NAT Gateway annotations need the namespace label egress.example.com/configure=true")
			}, 2*time.Minute, 5*time.Second).Should(Succeed(), "a pod with egress annotations is denied while the namespace lacks the label")

			By("A pod of the namespace's identity without annotations is admitted, and the controller makes it a member")
			pods := createConfiguredEgressPods(cs, ns.Name, egressName, 1, nil, false)
			Expect(utils.WaitPodsToBeReady(cs, ns.Name)).To(Succeed())
			eventuallyEgressMember(cs, ns.Name, pods[0])
			eventuallyEgressNAT(egressName, defaultEgressNATWant(egressName, ipv6))
			eventuallyManagedRegistered(cs, ns.Name, egressName, byoNATGatewayARMID(resourceGroupName, egressName))

			By("Annotating it is denied; once the namespace has the label, a pod with annotations is admitted")
			patch, err := json.Marshal(map[string]any{"metadata": map[string]any{"annotations": map[string]string{idleKey: "4"}}})
			Expect(err).NotTo(HaveOccurred())
			Eventually(func() error {
				return patchDenied(pods[0], patch, "egress NAT Gateway annotations need the namespace label")
			}, 2*time.Minute, 5*time.Second).Should(Succeed(), "annotating a pod is denied while the namespace lacks the label")
			patch, err = json.Marshal(map[string]any{"metadata": map[string]any{"labels": map[string]string{"egress.example.com/configure": "true"}}})
			Expect(err).NotTo(HaveOccurred())
			_, err = cs.CoreV1().Namespaces().Patch(context.TODO(), ns.Name, types.MergePatchType, patch, metav1.PatchOptions{})
			Expect(err).NotTo(HaveOccurred())
			Eventually(func() error {
				_, err := cs.CoreV1().Pods(ns.Name).Create(context.TODO(), annotated, metav1.CreateOptions{})
				if apierrors.IsAlreadyExists(err) {
					return nil
				}
				return err
			}, 2*time.Minute, 5*time.Second).Should(Succeed(), "the policy admits annotations in a labelled namespace")
			pods = append(pods, annotated.Name)
			Expect(utils.WaitPodsToBeReady(cs, ns.Name)).To(Succeed())
			eventuallyEgressMember(cs, ns.Name, pods[1])

			By("Removing the label from the namespace: annotations are denied again, also on the pod that has them")
			patch, err = json.Marshal(map[string]any{"metadata": map[string]any{"labels": map[string]any{"egress.example.com/configure": nil}}})
			Expect(err).NotTo(HaveOccurred())
			_, err = cs.CoreV1().Namespaces().Patch(context.TODO(), ns.Name, types.MergePatchType, patch, metav1.PatchOptions{})
			Expect(err).NotTo(HaveOccurred())
			Eventually(func() error {
				return denied(newEgressConfigPod(ns.Name, egressName, 2, map[string]string{idleKey: "4"}, false),
					"egress NAT Gateway annotations need the namespace label egress.example.com/configure=true")
			}, 2*time.Minute, 5*time.Second).Should(Succeed(), "the policy denies annotations in a namespace without the label")
			patch, err = json.Marshal(map[string]any{"metadata": map[string]any{"annotations": map[string]string{idleKey: "5"}}})
			Expect(err).NotTo(HaveOccurred())
			Eventually(func() error {
				return patchDenied(pods[1], patch, "egress NAT Gateway annotations need the namespace label")
			}, 2*time.Minute, 5*time.Second).Should(Succeed(), "changing the annotations of a pod that has them is denied too")

			By("Deleting the pods: the controller releases them, also the one with annotations, as the policy does not check " +
				"an update that changes no label or annotation, and the identity is deleted")
			deleteEgressPods(cs, ns.Name, egressName)
			eventuallyEgressIdentityGone(egressName)
			Eventually(func() ([]v1.Pod, error) {
				list, err := cs.CoreV1().Pods(ns.Name).List(context.TODO(), metav1.ListOptions{LabelSelector: egressLabel + "=" + egressName})
				return list.Items, err
			}, 2*time.Minute, 5*time.Second).Should(BeEmpty(), "the controller must be able to remove its finalizers")
			deleteEgressAdmissionPolicy(cs, policy)
		})
	})
})

// Helpers of the egress NAT Gateway configuration specs on votes and members.

// invalidEgressAnnotation is egress annotations that break one rule, and the problem that the pod is told about.
type invalidEgressAnnotation struct {
	annotations map[string]string
	problem     string
}

// invalidEgressAnnotations returns, for each rule the controller checks on a pod's egress annotations but the one of a
// public address per IP family, annotations that break that rule only, for an identity that serves IPv6 when ipv6 is
// set. User resources are only parsed, never read, so they need not exist.
func invalidEgressAnnotations(suffix string, ipv6 bool) []invalidEgressAnnotation {
	const prefix = consts.PodAnnotationServiceEgressGatewayPrefix
	idleKey := consts.PodAnnotationServiceEgressGatewayIdleTimeout
	countKey := consts.PodAnnotationServiceEgressGatewayPublicIPCount
	lengthKey := consts.PodAnnotationServiceEgressGatewayPublicIPPrefixLength
	ipsKey := consts.PodAnnotationServiceEgressGatewayPublicIPIDs
	tagsKey := consts.PodAnnotationServiceEgressGatewayTags
	resourceID := func(subscription, rg, kind, name string) string {
		return fmt.Sprintf("/subscriptions/%s/resourceGroups/%s/providers/Microsoft.Network/%s/%s", subscription, rg, kind, name)
	}
	userIP := resourceID(subscriptionID, "egvote-user-"+suffix, "publicIPAddresses", "egvote-pip")
	userPrefix := resourceID(subscriptionID, "egvote-user-"+suffix, "publicIPPrefixes", "egvote-prefix")
	otherSubscription := resourceID("00000000-0000-0000-0000-000000000000", "egvote-user", "publicIPAddresses", "egvote-pip")
	nodeResourceGroup := resourceID(subscriptionID, resourceGroupName, "publicIPAddresses", "egvote-pip")
	longKey := strings.Repeat("k", consts.ServiceEgressGatewayMaxTagKeyLength+1)
	tags := make([]string, 0, consts.ServiceEgressGatewayMaxTags)
	for i := 0; i < consts.ServiceEgressGatewayMaxTags; i++ {
		tags = append(tags, fmt.Sprintf("e2e-tag-%d=v", i))
	}
	one := func(key, value, problem string) invalidEgressAnnotation {
		return invalidEgressAnnotation{annotations: map[string]string{key: value}, problem: key + ": " + problem}
	}

	cases := []invalidEgressAnnotation{
		one(prefix+"idle-timout", "10", "unknown annotation"),
		one(idleKey, "", "empty value"),
		one(idleKey, "ten", `"ten" is not an integer from 4 to 120`),
		one(idleKey, "3", `"3" is not an integer from 4 to 120`),
		one(idleKey, "121", `"121" is not an integer from 4 to 120`),
		one(countKey, "17", `"17" is not an integer from 0 to 16`),
		one(lengthKey, "27", `"27" is not an integer from 28 to 31`),
		{annotations: map[string]string{countKey: "16", lengthKey: "31"}, problem: "IPv4: 18 public addresses, more than 16"},
		{annotations: map[string]string{countKey: "15", consts.PodAnnotationServiceEgressGatewayPublicIPPrefixIDs: userPrefix},
			problem: "IPv4: 17 public addresses, more than 16"},
		one(ipsKey, userPrefix, fmt.Sprintf("%q is not a Public IP address ID", userPrefix)),
		one(consts.PodAnnotationServiceEgressGatewayPublicIPPrefixIDs, "egvote-not-an-id", `"egvote-not-an-id" is not a Public IP prefix ID`),
		one(ipsKey, otherSubscription, fmt.Sprintf("%q is not in subscription", otherSubscription)),
		one(ipsKey, nodeResourceGroup, fmt.Sprintf("%q is in the node resource group", nodeResourceGroup)),
		one(ipsKey, userIP+","+strings.ToUpper(userIP), fmt.Sprintf("%q is listed more than once", strings.ToUpper(userIP))),
		one(ipsKey, userIP+", ", "empty entry in the list"),
		one(tagsKey, "K8s-Azure-Egress-Identity=e2e", `key "K8s-Azure-Egress-Identity" is reserved for the cluster`),
		one(tagsKey, "service=e2e", `key "service" is reserved for the cluster`),
		one(tagsKey, strings.Join(tags, ","), "50 tags, more than 49"),
		one(tagsKey, "e2e-team=NULL", `value of "e2e-team" is "NULL", which Azure reserves`),
		one(tagsKey, "e2e-team=a=b", `"e2e-team=a=b" is not key=value`),
		one(tagsKey, "e2e-team=a,b", `"b" is not key=value`),
		one(tagsKey, "e2e/team=a", `key "e2e/team" contains one of <>%&\?/`),
		one(tagsKey, "e2e-team=a,E2E-Team=b", `key "E2E-Team" is set more than once (keys are case-insensitive)`),
		one(tagsKey, longKey+"=a", fmt.Sprintf("key %q is longer than 512 characters", longKey)),
		one(tagsKey, "e2e-team="+strings.Repeat("v", consts.ServiceEgressGatewayMaxTagValueLength+1), `value of "e2e-team" is longer than 256 characters`),
		one(tagsKey, "e2e-team=a,,e2e-env=b", "empty entry in the list"),
		one(tagsKey, "=a", `"=a" has an empty key`),
	}
	if !ipv6 {
		return append(cases, one(consts.PodAnnotationServiceEgressGatewayPublicIPCountIPv6, "1", "this egress identity does not use IPv6"))
	}
	ipsV6Key := consts.PodAnnotationServiceEgressGatewayPublicIPIDsIPv6
	return append(cases,
		one(consts.PodAnnotationServiceEgressGatewayPublicIPPrefixLengthIPv6, "123", `"123" is not an integer from 124 to 127`),
		invalidEgressAnnotation{
			annotations: map[string]string{ipsKey: userIP, ipsV6Key: userIP},
			problem:     fmt.Sprintf("%s: %q is also listed in %s", ipsV6Key, userIP, ipsKey),
		})
}

// eventuallyInvalidEgressProblem waits until pod podName is told that its egress annotations have one problem, which
// starts with problem, and that it keeps its egress through egressName.
func eventuallyInvalidEgressProblem(cs clientset.Interface, namespace, podName, egressName, problem string) {
	eventuallyPodEvent(cs, namespace, []string{podName}, consts.ServiceEgressGatewayReasonInvalidConfig, v1.EventTypeWarning,
		"The egress annotations of this pod are invalid: "+problem,
		fmt.Sprintf("The pod keeps egress through egress identity %q", egressName))
	byPod, err := egressPodEvents(cs, namespace, consts.ServiceEgressGatewayReasonInvalidConfig)
	Expect(err).NotTo(HaveOccurred())
	for _, e := range byPod[podName] {
		// The problems of a pod are joined by "; ".
		Expect(e.Message).NotTo(ContainSubstring("; "), "pod %s must break one rule only", podName)
	}
}

// ccmEgressWritesSoFar returns a client of the CCM cluster and the Azure writes to egressName's resources that the
// running cloud-controller-manager logged so far (ccmEgressWrites); nil and none, the check being left out, when its log
// cannot tell them.
func ccmEgressWritesSoFar(egressName string) (*CCMClusterClient, []string) {
	what := "the Azure writes to " + egressName
	ccmClient := ccmEgressLog(what)
	writes, ok := ccmEgressWrites(ccmClient, egressName, what)
	if !ok {
		return nil, nil
	}
	return ccmClient, writes
}

// expectNoNewEgressWrites checks that the running cloud-controller-manager logged no Azure write to egressName's
// resources besides before (ccmEgressWritesSoFar). It checks nothing without a CCM cluster client, the check being
// left out then, nor when the log cannot tell them any more.
func expectNoNewEgressWrites(ccmClient *CCMClusterClient, egressName string, before []string) {
	if ccmClient == nil {
		return
	}
	writes, ok := ccmEgressWrites(ccmClient, egressName, "the Azure writes to "+egressName)
	if !ok {
		return
	}
	var added []string
	for _, line := range writes {
		if !slices.Contains(before, line) {
			added = append(added, line)
		}
	}
	Expect(added).To(BeEmpty(), "nothing may be written to the resources of %s", egressName)
}

// egressMemberAddresses returns the addresses that the members of egressName in namespaces register: the accepted pod
// IPs (acceptedEgressAddresses) of the pods labelled with it, in any case, that are Running or Pending and not terminating.
func egressMemberAddresses(cs clientset.Interface, egressName string, namespaces ...string) (map[string]struct{}, error) {
	addresses := map[string]struct{}{}
	for _, namespace := range namespaces {
		pods, err := cs.CoreV1().Pods(namespace).List(context.TODO(), metav1.ListOptions{LabelSelector: egressLabel})
		if err != nil {
			return nil, err
		}
		for i := range pods.Items {
			pod := &pods.Items[i]
			if !strings.EqualFold(pod.Labels[egressLabel], egressName) || pod.DeletionTimestamp != nil ||
				(pod.Status.Phase != v1.PodRunning && pod.Status.Phase != v1.PodPending) {
				continue
			}
			for _, address := range acceptedEgressAddresses(pod) {
				addresses[address] = struct{}{}
			}
		}
	}
	return addresses, nil
}

// eventuallyEgressMembersRegistered waits until the Service Gateway registers exactly the addresses of the members of
// egressName in namespaces.
func eventuallyEgressMembersRegistered(cs clientset.Interface, egressName string, namespaces ...string) {
	Eventually(func() error {
		want, err := egressMemberAddresses(cs, egressName, namespaces...)
		if err != nil {
			return err
		}
		return egressRegisteredMatchErr(egressName, want)
	}, 3*time.Minute, defaultPollInterval).Should(Succeed(), "the members of %s, and only they, must be registered", egressName)
}

// eventuallyEgressMember waits until the controller made pod podName a member of its egress identity: it adds its
// cleanup finalizer to a member and then counts its vote, before it handles any other pod event.
func eventuallyEgressMember(cs clientset.Interface, namespace, podName string) {
	Eventually(func() ([]string, error) {
		pod, err := cs.CoreV1().Pods(namespace).Get(context.TODO(), podName, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
		return pod.Finalizers, nil
	}, 3*time.Minute, time.Second).Should(ContainElement(serviceGatewayPodFinalizer), "pod %s must become a member", podName)
}

// eventuallyPodReleased waits until the controller removed its cleanup finalizer from pod podName, which is no member
// of an egress identity any more (egressPodReleasedErr).
func eventuallyPodReleased(cs clientset.Interface, namespace, podName string) {
	Eventually(func() error {
		return egressPodReleasedErr(cs, namespace, podName)
	}, 2*time.Minute, 5*time.Second).Should(Succeed(), "pod %s must be released", podName)
}

// admissionPoliciesServed reports whether the cluster serves ValidatingAdmissionPolicy in admissionregistration.k8s.io/v1.
// A NotFound means the cluster does not serve that group version at all (served from Kubernetes 1.16 on, unless turned
// off), so the spec skips. Any other discovery error is retried for a minute and then fails the spec: it never means the
// policies are not served.
func admissionPoliciesServed(cs clientset.Interface) bool {
	var resources *metav1.APIResourceList
	Eventually(func() error {
		var err error
		resources, err = cs.Discovery().ServerResourcesForGroupVersion(admissionregistrationv1.SchemeGroupVersion.String())
		if apierrors.IsNotFound(err) {
			resources, err = &metav1.APIResourceList{}, nil
		}
		return err
	}, time.Minute, 5*time.Second).Should(Succeed(), "discovering %s", admissionregistrationv1.SchemeGroupVersion)
	return slices.ContainsFunc(resources.APIResources, func(r metav1.APIResource) bool {
		return r.Name == "validatingadmissionpolicies"
	})
}

// createEgressAdmissionPolicy creates the example admission policy of the user documentation (egressAdmissionPolicy)
// as name, bound to namespace only. Both are deleted after the spec when the spec does not delete them itself
// (deleteEgressAdmissionPolicy).
func createEgressAdmissionPolicy(cs clientset.Interface, name, namespace string) string {
	policy, binding := egressAdmissionPolicy(name, namespace)
	policies := cs.AdmissionregistrationV1().ValidatingAdmissionPolicies()
	bindings := cs.AdmissionregistrationV1().ValidatingAdmissionPolicyBindings()
	// Registered first, so a failed create is still cleaned up.
	DeferCleanup(func() {
		for _, err := range []error{
			bindings.Delete(context.TODO(), name, metav1.DeleteOptions{}),
			policies.Delete(context.TODO(), name, metav1.DeleteOptions{}),
		} {
			if !apierrors.IsNotFound(err) {
				Expect(err).NotTo(HaveOccurred())
			}
		}
	})
	_, err := policies.Create(context.TODO(), policy, metav1.CreateOptions{})
	Expect(err).NotTo(HaveOccurred())
	_, err = bindings.Create(context.TODO(), binding, metav1.CreateOptions{})
	Expect(err).NotTo(HaveOccurred())
	return name
}

// egressAdmissionPolicy returns the example ValidatingAdmissionPolicy of the user documentation
// (egressAdmissionPolicyDoc) as name, and a binding of the same name that applies it to namespace only
// (TestEgressAdmissionPolicyMatchesDoc).
func egressAdmissionPolicy(name, namespace string) (*admissionregistrationv1.ValidatingAdmissionPolicy,
	*admissionregistrationv1.ValidatingAdmissionPolicyBinding) {
	fail := admissionregistrationv1.Fail
	policy := &admissionregistrationv1.ValidatingAdmissionPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: admissionregistrationv1.ValidatingAdmissionPolicySpec{
			FailurePolicy: &fail,
			MatchConstraints: &admissionregistrationv1.MatchResources{
				ResourceRules: []admissionregistrationv1.NamedRuleWithOperations{{
					RuleWithOperations: admissionregistrationv1.RuleWithOperations{
						Operations: []admissionregistrationv1.OperationType{admissionregistrationv1.Create, admissionregistrationv1.Update},
						Rule:       admissionregistrationv1.Rule{APIGroups: []string{""}, APIVersions: []string{"v1"}, Resources: []string{"pods"}},
					},
				}},
			},
			MatchConditions: []admissionregistrationv1.MatchCondition{{
				Name: "labels-or-annotations-changed",
				Expression: "request.operation == 'CREATE' || " +
					"object.metadata.?labels.orValue({}) != oldObject.metadata.?labels.orValue({}) || " +
					"object.metadata.?annotations.orValue({}) != oldObject.metadata.?annotations.orValue({})",
			}},
			Variables: []admissionregistrationv1.Variable{
				{Name: "identity", Expression: "object.metadata.?labels[?'kubernetes.azure.com/service-egress-gateway'].orValue('').lowerAscii()"},
				{Name: "ns", Expression: "namespaceObject.metadata.name"},
			},
			Validations: []admissionregistrationv1.Validation{
				{
					Expression: "variables.identity in ['', variables.ns] || variables.identity.startsWith(variables.ns + '-')",
					Message:    "the egress identity must be the namespace name or start with '<namespace>-'",
				},
				{
					Expression: "!object.metadata.?annotations.orValue({}).exists(k, k.startsWith('kubernetes.azure.com/service-egress-gateway-')) || " +
						"namespaceObject.metadata.?labels[?'egress.example.com/configure'].orValue('') == 'true'",
					Message: "egress NAT Gateway annotations need the namespace label egress.example.com/configure=true",
				},
			},
		},
	}
	binding := &admissionregistrationv1.ValidatingAdmissionPolicyBinding{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: admissionregistrationv1.ValidatingAdmissionPolicyBindingSpec{
			PolicyName:        name,
			ValidationActions: []admissionregistrationv1.ValidationAction{admissionregistrationv1.Deny},
			MatchResources: &admissionregistrationv1.MatchResources{
				NamespaceSelector: &metav1.LabelSelector{MatchLabels: map[string]string{v1.LabelMetadataName: namespace}},
			},
		},
	}
	return policy, binding
}

// egressAdmissionPolicyDoc is the user documentation with the example admission policy, relative to this package.
const egressAdmissionPolicyDoc = "../../../docs/services/servicegateway-egress.md"

func TestEgressAdmissionPolicyMatchesDoc(t *testing.T) {
	doc, err := os.ReadFile(egressAdmissionPolicyDoc)
	if errors.Is(err, fs.ErrNotExist) {
		t.Skipf("%s is not there: the spec's admission policy is compared with it in the repository only", egressAdmissionPolicyDoc)
	}
	if err != nil {
		t.Fatal(err)
	}
	var block string
	for _, part := range strings.Split(string(doc), "```yaml\n")[1:] {
		if yaml, _, ok := strings.Cut(part, "```"); ok && strings.Contains(yaml, "kind: ValidatingAdmissionPolicy\n") {
			block = yaml
		}
	}
	if block == "" {
		t.Fatalf("%s has no YAML block with a ValidatingAdmissionPolicy", egressAdmissionPolicyDoc)
	}
	var (
		policy  admissionregistrationv1.ValidatingAdmissionPolicy
		binding admissionregistrationv1.ValidatingAdmissionPolicyBinding
	)
	decoder := utilyaml.NewYAMLOrJSONDecoder(strings.NewReader(block), 4096)
	if err := decoder.Decode(&policy); err != nil {
		t.Fatalf("decoding the documentation's policy: %v", err)
	}
	if err := decoder.Decode(&binding); err != nil {
		t.Fatalf("decoding the documentation's binding: %v", err)
	}
	policy.TypeMeta, binding.TypeMeta = metav1.TypeMeta{}, metav1.TypeMeta{}

	// Only the names and the namespace the spec binds the policy to may differ: the documentation's binding applies it to
	// every namespace.
	wantPolicy, wantBinding := egressAdmissionPolicy(policy.Name, "e2e-namespace")
	if wantBinding.Name != wantPolicy.Name || !equality.Semantic.DeepEqual(wantBinding.Spec.MatchResources, &admissionregistrationv1.MatchResources{
		NamespaceSelector: &metav1.LabelSelector{MatchLabels: map[string]string{v1.LabelMetadataName: "e2e-namespace"}},
	}) {
		t.Errorf("the spec's binding must be named after its policy and apply it to namespace e2e-namespace only: %+v", wantBinding)
	}
	wantBinding.Name, wantBinding.Spec.MatchResources = binding.Name, nil
	if !equality.Semantic.DeepEqual(wantPolicy, &policy) {
		t.Errorf("the spec's policy differs from the one in %s (-spec +documentation):\n%s", egressAdmissionPolicyDoc, diff.Diff(wantPolicy, &policy))
	}
	if !equality.Semantic.DeepEqual(wantBinding, &binding) {
		t.Errorf("the spec's binding differs from the one in %s (-spec +documentation):\n%s", egressAdmissionPolicyDoc, diff.Diff(wantBinding, &binding))
	}
}

// deleteEgressAdmissionPolicy deletes the binding and the policy createEgressAdmissionPolicy created as name.
func deleteEgressAdmissionPolicy(cs clientset.Interface, name string) {
	Expect(cs.AdmissionregistrationV1().ValidatingAdmissionPolicyBindings().Delete(context.TODO(), name, metav1.DeleteOptions{})).To(Succeed())
	Expect(cs.AdmissionregistrationV1().ValidatingAdmissionPolicies().Delete(context.TODO(), name, metav1.DeleteOptions{})).To(Succeed())
}
