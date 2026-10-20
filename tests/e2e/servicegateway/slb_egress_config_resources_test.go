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
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/types"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"

	"sigs.k8s.io/cloud-provider-azure/pkg/consts"
	"sigs.k8s.io/cloud-provider-azure/tests/e2e/utils"
)

var _ = Describe("Container Load Balancer Outbound (NAT Gateway)", Label(slbTestLabel, slbEgressConfigTestLabel), func() {
	basename := "slb-egress-resources"

	var (
		cs clientset.Interface
		ns *v1.Namespace
	)

	BeforeEach(func() {
		cs, ns = newEgressConfigSpec(basename)
	})

	AfterEach(func() {
		cleanUpEgressConfigSpec(cs, ns, nil)
		cs, ns = nil, nil
	})

	// Before it writes anything, the controller checks the Azure resources behind the configuration the pods
	// of an egress identity agree on: the user Public IPs and prefixes it references, the managed ones it asks
	// for and the tags it adds. A problem only another configuration avoids rejects it for good; one the user
	// or Azure can clear rejects it until it is gone. Either way the NAT Gateway is left as it is. Most specs
	// check the control plane only, so they also run where pod egress and logs cannot be observed.
	Context("egress NAT Gateway configuration: Azure resources", func() {
		var (
			suffix string
			ipv6   bool
			// undo reverts what a spec changed out of band that would keep the AfterEach from cleaning up (a
			// lock) or that its checks would count as left over (a resource at a reserved name), should the spec
			// fail before reverting it itself. Unlike DeferCleanup, it runs before that AfterEach.
			undo []func()
		)

		BeforeEach(func() {
			suffix = ns.Name[len(ns.Name)-5:]
			ipv6 = clusterHasIPv6Node(cs)
		})

		AfterEach(func() {
			revertOutOfBand(&undo)
		})

		It("should configure IPv6 managed prefixes and user IPv6 resources in place without writing the IPv4 side, "+
			"and reject an IPv4 Public IP under an IPv6 key", Label(slbEgressConfigFaultTestLabel), func() {
			if !ipv6 {
				Skip("no node exposes an IPv6 InternalIP; egress identities serve IPv6 on dual-stack clusters only")
			}
			egressName := "egres-dual-" + suffix
			ipIDsV6Key := consts.PodAnnotationServiceEgressGatewayPublicIPIDsIPv6
			prefixIDsV6Key := consts.PodAnnotationServiceEgressGatewayPublicIPPrefixIDsIPv6
			lengthV6Key := consts.PodAnnotationServiceEgressGatewayPublicIPPrefixLengthIPv6
			ipv4 := map[string]string{
				consts.PodAnnotationServiceEgressGatewayPublicIPCount:        "2",
				consts.PodAnnotationServiceEgressGatewayPublicIPPrefixLength: "31",
				consts.PodAnnotationServiceEgressGatewayTags:                 "e2e-family=dual",
			}
			summary := func(ipv6Side string) string {
				return "idle timeout 4m; IPv4: 2 managed IPs, managed prefix /31; IPv6: " + ipv6Side + "; tags: e2e-family=dual"
			}

			By("Creating a StandardV2 IPv6 Public IP, an IPv6 /127 prefix and an IPv4 Public IP in a test resource group")
			rg, granted := createEgressUserResourceGroup(suffix)
			userPublicIP := createBYOPublicIP(rg, "egres-pip-v6", "IPv6")
			userPrefix := createBYOPublicIPPrefix(rg, "egres-prefix-v6", "IPv6", 127)
			ipv4PublicIP := createBYOPublicIP(rg, "egres-pip-v4", "IPv4")
			configurations := userEgressResourceConfigurations(userPublicIP, userPrefix, ipv4PublicIP)

			By("Creating pods that ask for two managed IPv4 Public IPs, a managed IPv4 /31 prefix, two managed IPv6 Public IPs and a tag")
			pods := createConfiguredEgressPods(cs, ns.Name, egressName, 2,
				egressAnnotations(ipv4, map[string]string{consts.PodAnnotationServiceEgressGatewayPublicIPCountIPv6: "2"}), false)
			Expect(utils.WaitPodsToBeReady(cs, ns.Name)).To(Succeed())
			want := defaultEgressNATWant(egressName, true)
			want.publicIPs = egressManagedPublicIPIDs(egressName, false, 2)
			want.prefixes = []string{egressManagedPrefixID(egressName, false, 31)}
			want.publicIPsV6 = egressManagedPublicIPIDs(egressName, true, 2)
			want.tags = map[string]string{"e2e-family": "dual"}
			eventuallyEgressNAT(egressName, want)
			eventuallyManagedRegistered(cs, ns.Name, egressName, byoNATGatewayARMID(resourceGroupName, egressName))
			logs := egressApplyLogReader(egressName)
			natBefore := readEgressNAT(egressName)
			ipv4Etags := azureResourceEtags(slices.Concat(want.publicIPs, want.prefixes)...)
			// A PUT that changes nothing keeps the etag: only the controller's log tells it.
			ipv4Logs, ipv4Writes := map[string]*CCMClusterClient{}, map[string][]string{}
			for _, id := range slices.Concat(want.publicIPs, want.prefixes) {
				name := resourceNameFromID(id)
				ipv4Logs[name], ipv4Writes[name] = ccmEgressWritesSoFar(name)
			}
			expectIPv4Untouched := func() {
				Expect(egressEtagsUnchangedErr(ipv4Etags)).To(Succeed(), "a change of the IPv6 side must not write the IPv4 Public IPs and prefix")
				for name, before := range ipv4Writes {
					expectNoNewEgressWrites(ipv4Logs[name], name, before)
				}
			}

			By("Asking for the user IPv6 Public IP: it replaces <id>-pip-v6 and <id>-pip-v6-2, which are deleted, and the IPv4 resources are not written")
			setEgressConfig(cs, ns.Name, egressAnnotations(ipv4, map[string]string{ipIDsV6Key: userPublicIP}), pods...)
			want.publicIPsV6 = []string{userPublicIP}
			eventuallyEgressNATWithUserResources(cs, ns.Name, pods, egressName, want, rg, granted)
			expectSameEgressNAT(egressName, natBefore)
			expectIPv4Untouched()
			eventuallyEgressAppliedCounts(cs, ns.Name, egressCountsOf(pods, 1),
				egressAppliedMessage(egressName, summary("user IPs (egres-pip-v6)")))

			By("Asking for a managed IPv6 /127 prefix and the user IPv6 prefix instead: <id>-prefix-v6-127 is created and tagged, " +
				"the user Public IP detached and kept, the IPv4 resources not written")
			want.publicIPsV6, want.prefixesV6 = nil, []string{egressManagedPrefixID(egressName, true, 127), userPrefix}
			expectEgressApplied(cs, ns.Name, egressName, pods, egressAnnotations(ipv4, map[string]string{lengthV6Key: "127", prefixIDsV6Key: userPrefix}),
				want, summary("managed prefix /127, user prefixes (egres-prefix-v6)"))
			expectIPv4Untouched()
			expectUserEgressResourcesReleased(userPublicIP)

			By("Resizing the IPv6 prefix to /126: <id>-prefix-v6-126 is created and swapped in, <id>-prefix-v6-127 deleted, the IPv4 resources not written")
			resized := egressAnnotations(ipv4, map[string]string{lengthV6Key: "126", prefixIDsV6Key: userPrefix})
			want.prefixesV6 = []string{egressManagedPrefixID(egressName, true, 126), userPrefix}
			expectEgressApplied(cs, ns.Name, egressName, pods, resized, want, summary("managed prefix /126, user prefixes (egres-prefix-v6)"))
			expectIPv4Untouched()

			By("Asking for the IPv4 Public IP under the IPv6 key as well: rejected for good, nothing written, not tried again")
			expectEgressRejectedForGood(cs, ns.Name, egressName, pods, logs, egressAnnotations(resized, map[string]string{ipIDsV6Key: ipv4PublicIP}), want,
				fmt.Sprintf("user Public IP %q is IPv4, but it is listed for IPv6", strings.ToLower(ipv4PublicIP)))

			By("Asking for no managed IPv6 Public IP and no other IPv6 address: every pod is told its vote is invalid, the NAT Gateway is kept")
			etags := egressResourceEtags(egressName)
			setEgressConfig(cs, ns.Name, egressAnnotations(ipv4, map[string]string{consts.PodAnnotationServiceEgressGatewayPublicIPCountIPv6: "0"}), pods...)
			eventuallyEachPodEvent(cs, ns.Name, pods, consts.ServiceEgressGatewayReasonInvalidConfig, v1.EventTypeWarning,
				"The egress annotations of this pod are invalid: IPv6: no public address (the Public IP count is 0 and no prefix or user Public IP is set).")
			expectEgressUntouched(egressName, want, etags, egressBackoffObservation)

			By("Deleting the pods: the NAT Gateway and the managed resources of both IP families are deleted, the user resources kept unchanged")
			deleteEgressPods(cs, ns.Name, egressName)
			eventuallyEgressIdentityGone(egressName)
			expectUserEgressResourcesReleased(userPublicIP, userPrefix, ipv4PublicIP)
			expectUserEgressResourcesUnchanged(configurations)
		})

		It("should fall back to the default when the first agreed configuration is rejected for good, never retry it on its own, "+
			"and try it again only after another agreement", Label(slbEgressConfigFaultTestLabel), func() {
			egressName := "egres-final-" + suffix
			ipIDsKey := consts.PodAnnotationServiceEgressGatewayPublicIPIDs

			By("Creating a Standard and a StandardV2 Public IP in a test resource group")
			rg, _ := createEgressUserResourceGroup(suffix)
			standard, err := createUserPublicIP(rg, "egres-std-pip", "Standard", clusterLocation(), nil)
			Expect(err).NotTo(HaveOccurred())
			good := createBYOPublicIP(rg, "egres-good-pip", "IPv4")
			configurations := userEgressResourceConfigurations(standard, good)

			By("Creating pods that ask for the Standard Public IP and one that does not exist: the identity gets the default NAT Gateway, " +
				"every pod is told the configuration is rejected for good, for the SKU alone, and nothing is announced as applied")
			rejected := map[string]string{ipIDsKey: standard + "," + publicIPID(rg, "egres-missing-pip")}
			pods := createConfiguredEgressPods(cs, ns.Name, egressName, 2, rejected, false)
			Expect(utils.WaitPodsToBeReady(cs, ns.Name)).To(Succeed())
			want := defaultEgressNATWant(egressName, ipv6)
			eventuallyEgressNAT(egressName, want)
			eventuallyManagedRegistered(cs, ns.Name, egressName, byoNATGatewayARMID(resourceGroupName, egressName))
			reason := fmt.Sprintf("user Public IP %q has SKU %q, not StandardV2", strings.ToLower(standard), "Standard")
			eventuallyEgressRejection(cs, ns.Name, pods, rg, egressRejectionWants(egressName, false, reason)...)
			Expect(egressEventCounts(cs, ns.Name, pods, consts.ServiceEgressGatewayReasonConfigRejected, v1.EventTypeWarning, "does not exist")).
				To(HaveEach(BeZero()), "a problem that rejects a configuration for good is reported alone, without those that would be retried")
			expectNoPodEvent(cs, ns.Name, pods, consts.ServiceEgressGatewayReasonConfigApplied)
			logs := egressApplyLogReader(egressName)

			By("Watching for 2 minutes: nothing is written, the pods are told nothing more and the configuration is not tried again")
			etags := egressResourceEtags(egressName)
			events := egressConfigEventCounts(cs, ns.Name)
			Consistently(func() error {
				if err := egressEtagsUnchangedErr(etags); err != nil {
					return err
				}
				return egressNATShapeErr(egressName, want)
			}, 2*time.Minute, defaultPollInterval).Should(Succeed(), "a configuration rejected for good must not be tried again on its own")
			Expect(egressConfigEventCounts(cs, ns.Name)).To(Equal(events), "a configuration rejected for good is announced once")
			what := "the tries of the configurations of " + egressName
			if lines, ok := ccmLogLinesFor(logs, time.Time{}, what, fmt.Sprintf("%q", egressName)); ok {
				Expect(egressCreationTries(lines, egressName)).To(MatchRegexp(`^(RF)+C$`), "the configuration rejected for good must be "+
					"tried at the creation only (a creation retried after it tries again), never after it: %q", lines)
			}

			By("Agreeing on the StandardV2 Public IP: it is applied in place and every pod is told")
			want.publicIPs = []string{good}
			expectEgressApplied(cs, ns.Name, egressName, pods, map[string]string{ipIDsKey: good}, want,
				"idle timeout 4m; IPv4: user IPs (egres-good-pip)"+defaultIPv6Summary(ipv6))

			By("Agreeing on the rejected configuration again: it is tried once more and rejected for good again, every pod told once more, the NAT Gateway kept")
			expectEgressRejectedForGood(cs, ns.Name, egressName, pods, logs, rejected, want, reason)

			By("Deleting the pods: the NAT Gateway and its managed Public IPs are deleted, the user Public IPs are kept unchanged")
			deleteEgressPods(cs, ns.Name, egressName)
			eventuallyEgressIdentityGone(egressName)
			expectUserEgressResourcesReleased(standard, good)
			expectUserEgressResourcesUnchanged(configurations)
		})

		It("should reject for good a user Public IP in another region or under the other IP version's key and a user prefix "+
			"too large for the NAT Gateway, and try each again only once the pods agree on it again", Label(slbEgressConfigFaultTestLabel), func() {
			egressName := "egres-sizeloc-" + suffix
			ipIDsKey, prefixIDsKey := consts.PodAnnotationServiceEgressGatewayPublicIPIDs, consts.PodAnnotationServiceEgressGatewayPublicIPPrefixIDs
			location := clusterLocation()

			By("Creating a StandardV2 /28 prefix, StandardV2 IPv4 and IPv6 Public IPs and a StandardV2 Public IP in another region in a test resource group")
			rg, granted := createEgressUserResourceGroup(suffix)
			prefix := createBYOPublicIPPrefix(rg, "egres-prefix-28", "IPv4", 28)
			good := createBYOPublicIP(rg, "egres-good-pip", "IPv4")
			v6 := createBYOPublicIP(rg, "egres-v6-pip", "IPv6")
			far, farLocation := createUserPublicIPElsewhere(rg, "egres-far-pip", location)

			By("Creating pods that ask for the /28 prefix: the NAT Gateway uses it, so the controller can read the test resource group")
			pods := createConfiguredEgressPods(cs, ns.Name, egressName, 2, map[string]string{prefixIDsKey: prefix}, false)
			Expect(utils.WaitPodsToBeReady(cs, ns.Name)).To(Succeed())
			current := defaultEgressNATWant(egressName, ipv6)
			current.publicIPs, current.prefixes = nil, []string{prefix}
			eventuallyEgressNATWithUserResources(cs, ns.Name, pods, egressName, current, rg, granted)
			eventuallyManagedRegistered(cs, ns.Name, egressName, byoNATGatewayARMID(resourceGroupName, egressName))
			logs := egressApplyLogReader(egressName)

			if far != "" {
				elsewhere := map[string]string{ipIDsKey: far}
				elsewhereReason := fmt.Sprintf("user Public IP %q is in location %q, not %q", strings.ToLower(far), farLocation, location)
				By(fmt.Sprintf("Asking for the Public IP in %s: rejected for good, nothing written, not tried again", farLocation))
				expectEgressRejectedForGood(cs, ns.Name, egressName, pods, logs, elsewhere, current, elsewhereReason)

				By("Agreeing on the Public IP in the cluster's region instead: it is applied in place")
				current.publicIPs, current.prefixes = []string{good}, nil
				expectEgressApplied(cs, ns.Name, egressName, pods, map[string]string{ipIDsKey: good}, current,
					"idle timeout 4m; IPv4: user IPs (egres-good-pip)"+defaultIPv6Summary(ipv6))

				By(fmt.Sprintf("Agreeing on the Public IP in %s again: tried once more and rejected for good again, the NAT Gateway kept", farLocation))
				expectEgressRejectedForGood(cs, ns.Name, egressName, pods, logs, elsewhere, current, elsewhereReason)
			}

			By("Asking for the IPv6 Public IP under the IPv4 key: rejected for good, nothing written, not tried again")
			underIPv4 := map[string]string{ipIDsKey: v6}
			underIPv4Reason := fmt.Sprintf("user Public IP %q is IPv6, but it is listed for IPv4", strings.ToLower(v6))
			expectEgressRejectedForGood(cs, ns.Name, egressName, pods, logs, underIPv4, current, underIPv4Reason)

			corrected, correctedSummary := map[string]string{}, "idle timeout 4m; IPv4: 1 managed IP"
			current = defaultEgressNATWant(egressName, ipv6)
			if ipv6 {
				By("Agreeing on it under the IPv6 key instead: it is applied in place")
				corrected[consts.PodAnnotationServiceEgressGatewayPublicIPIDsIPv6] = v6
				current.publicIPsV6 = []string{v6}
				correctedSummary += "; IPv6: user IPs (egres-v6-pip)"
			} else {
				By("Agreeing on the default configuration instead, as the identity serves IPv4 only: it is applied in place")
			}
			expectEgressApplied(cs, ns.Name, egressName, pods, corrected, current, correctedSummary)

			By("Agreeing on the IPv6 Public IP under the IPv4 key again: tried once more and rejected for good again, the NAT Gateway kept")
			expectEgressRejectedForGood(cs, ns.Name, egressName, pods, logs, underIPv4, current, underIPv4Reason)

			// The pods count each user prefix as 2 addresses; the controller reads its real size before it writes.
			By("Asking for a managed Public IP next to the /28 prefix, 17 addresses: rejected for good, nothing written or created, not tried again")
			oversized := map[string]string{consts.PodAnnotationServiceEgressGatewayPublicIPCount: "1", prefixIDsKey: prefix}
			expectEgressRejectedForGood(cs, ns.Name, egressName, pods, logs, oversized, current, egressAddressesReason(17))

			By("Agreeing on the prefix alone instead, 16 addresses: it is applied in place")
			current = defaultEgressNATWant(egressName, ipv6)
			current.publicIPs, current.prefixes = nil, []string{prefix}
			expectEgressApplied(cs, ns.Name, egressName, pods, map[string]string{prefixIDsKey: prefix}, current,
				"idle timeout 4m; IPv4: user prefixes (egres-prefix-28)"+defaultIPv6Summary(ipv6))

			By("Agreeing on the managed Public IP next to the prefix again: tried once more and rejected for good again, the NAT Gateway kept")
			expectEgressRejectedForGood(cs, ns.Name, egressName, pods, logs, oversized, current, egressAddressesReason(17))

			By("Deleting the pods: the NAT Gateway and its managed Public IPs are deleted, the user resources detached and kept")
			deleteEgressPods(cs, ns.Name, egressName)
			eventuallyEgressIdentityGone(egressName)
			expectUserEgressResourcesReleased(prefix, good, v6)
			if far == "" {
				leaveOut(fmt.Sprintf("a user Public IP in another region (no region other than %s of %v accepted a StandardV2 Public IP)",
					location, egressOtherRegions))
			}
		})

		It("should reject for good tags that would take the NAT Gateway over 50, and tell the pods once more when a retry "+
			"rejects for good a configuration it retried", Label(slbEgressConfigFaultTestLabel), func() {
			egressName := "egres-tagsize-" + suffix
			tagsKey := consts.PodAnnotationServiceEgressGatewayTags

			By("Creating pods without annotations and adding two tags to their NAT Gateway out of band")
			pods := createConfiguredEgressPods(cs, ns.Name, egressName, 2, nil, false)
			Expect(utils.WaitPodsToBeReady(cs, ns.Name)).To(Succeed())
			want := defaultEgressNATWant(egressName, ipv6)
			eventuallyEgressNAT(egressName, want)
			eventuallyManagedRegistered(cs, ns.Name, egressName, byoNATGatewayARMID(resourceGroupName, egressName))
			logs := egressApplyLogReader(egressName)
			Expect(readEgressNAT(egressName).Tags).To(HaveLen(1), "the NAT Gateway of a new identity carries its ownership tag alone")
			foreign := map[string]string{"e2e-foreign-a": "kept", "e2e-foreign-b": "kept"}
			for key, value := range foreign {
				setNetworkResourceTag(byoNATGatewayARMID(resourceGroupName, egressName), key, value)
			}

			By("Asking for 49 tags, 52 with the NAT Gateway's two and its ownership tag: rejected for good, nothing written, not tried again")
			tooMany := map[string]string{tagsKey: egressTagList(49)}
			tooManyReason := fmt.Sprintf("egress NAT Gateway %q: 52 tags, more than the 50 Azure accepts on a resource", egressName)
			expectEgressRejectedForGood(cs, ns.Name, egressName, pods, logs, tooMany, want, tooManyReason)

			By("Agreeing on 47 of them instead: they are applied in place, the NAT Gateway carries 50 tags and keeps those of others")
			want.tags = egressTagMap(47)
			expectEgressApplied(cs, ns.Name, egressName, pods, map[string]string{tagsKey: egressTagList(47)}, want,
				"idle timeout 4m; IPv4: 1 managed IP"+defaultIPv6Summary(ipv6)+"; tags: "+egressTagList(47))
			natTags := readEgressNAT(egressName).Tags
			Expect(missingEgressTags(natTags, foreign)).To(BeEmpty(), "the tags others added to the NAT Gateway must be kept")
			Expect(natTags).To(HaveLen(consts.ServiceEgressGatewayMaxTags))

			By("Agreeing on the 49 tags again: tried once more and rejected for good again, the NAT Gateway kept")
			expectEgressRejectedForGood(cs, ns.Name, egressName, pods, logs, tooMany, want, tooManyReason)

			By("Creating the pod of a second identity that asks for a managed Public IP and a managed /31 prefix, and adding a tag of " +
				"its own to each of them out of band")
			other := "egres-tagpip-" + suffix
			otherConfig := map[string]string{
				consts.PodAnnotationServiceEgressGatewayPublicIPCount:        "1",
				consts.PodAnnotationServiceEgressGatewayPublicIPPrefixLength: "31",
			}
			otherPods := createConfiguredEgressPods(cs, ns.Name, other, 1, otherConfig, false)
			Expect(utils.WaitPodsToBeReady(cs, ns.Name)).To(Succeed())
			otherWant := defaultEgressNATWant(other, ipv6)
			otherWant.prefixes = []string{egressManagedPrefixID(other, false, 31)}
			eventuallyEgressNAT(other, otherWant)
			eventuallyManagedRegistered(cs, ns.Name, other, byoNATGatewayARMID(resourceGroupName, other))
			for _, id := range []string{otherWant.publicIPs[0], otherWant.prefixes[0]} {
				setNetworkResourceTag(id, "e2e-foreign", "kept")
			}

			By("Asking it for 49 tags, 50 with the NAT Gateway's ownership tag but 51 on the Public IP and on the prefix: rejected for " +
				"good before anything is written, as the limit holds for every resource the controller writes, and not tried again")
			otherConfig[tagsKey] = egressTagList(49)
			expectEgressRejectedForGood(cs, ns.Name, other, otherPods, egressApplyLogReader(other), otherConfig, otherWant,
				fmt.Sprintf("egress Public IP %q: 51 tags, more than the 50 Azure accepts on a resource", egressManagedPublicIPName(other, false, 1)),
				fmt.Sprintf("egress Public IP prefix %q: 51 tags, more than the 50 Azure accepts on a resource", egressManagedPrefixName(other, false, 31)))
			deleteEgressPods(cs, ns.Name, other)
			eventuallyEgressIdentityGone(other)

			By("Asking for a managed Public IP and a user prefix that does not exist yet: every pod is told it is rejected, to be retried")
			rg, _ := createEgressUserResourceGroup(suffix)
			latePrefix := publicIPPrefixID(rg, "egres-late-prefix")
			retried := egressRejectionWants(egressName, true, fmt.Sprintf("user Public IP prefix %q does not exist", strings.ToLower(latePrefix)))
			setEgressConfig(cs, ns.Name, map[string]string{
				consts.PodAnnotationServiceEgressGatewayPublicIPCount:     "1",
				consts.PodAnnotationServiceEgressGatewayPublicIPPrefixIDs: latePrefix,
			}, pods...)
			eventuallyEgressRejection(cs, ns.Name, pods, rg, retried...)
			etags := egressResourceEtags(egressName)
			tally := egressEventTally(cs, ns.Name, pods, egressConfigEventReasons...)

			By("Creating the prefix as a /28, 17 addresses with the managed Public IP: the next try rejects the configuration for good, " +
				"which every pod is told once more, with that reason, and it is not tried again")
			created := time.Now()
			Expect(createBYOPublicIPPrefix(rg, "egres-late-prefix", "IPv4", 28)).To(Equal(latePrefix))
			final := egressAddressesReason(17)
			watch := egressBackoffObservation
			if logs == nil {
				// Nothing else shows when the controller tries again: watch for longer than its longest backoff.
				watch = 2 * time.Minute
			}
			eventuallyEgressRejectionLogged(logs, egressName, created, []string{final, "retryable=false"})
			eventuallyEgressEventCounts(cs, ns.Name, consts.ServiceEgressGatewayReasonConfigRejected, v1.EventTypeWarning,
				egressCountsOf(pods, 1), egressRejectionWants(egressName, false, final)...)
			expectEgressUntouched(egressName, want, etags, watch)
			tally[consts.ServiceEgressGatewayReasonConfigRejected] = egressCountsPlusOne(tally[consts.ServiceEgressGatewayReasonConfigRejected])
			Expect(egressEventTally(cs, ns.Name, pods, egressConfigEventReasons...)).To(Equal(tally),
				"a retried configuration later rejected for good is told once more to every pod, and nothing else")
			if rejections, ok := ccmLogLinesFor(logs, created, "the tries of the configuration of "+egressName+" after the prefix was created",
				`"Rejected egress configuration"`, fmt.Sprintf("identity=%q", egressName)); ok {
				Expect(slices.IndexFunc(rejections, func(line string) bool { return strings.Contains(line, final) })).
					To(Equal(len(rejections)-1), "rejected for good, the configuration must not be tried again: %q", rejections)
			}

			By("Deleting the pods: the NAT Gateway and its Public IPs are deleted, the user prefix is kept")
			deleteEgressPods(cs, ns.Name, egressName)
			eventuallyEgressIdentityGone(egressName)
			expectUserEgressResourcesReleased(latePrefix)
		})

		It("should never write or delete resources at its reserved names that lack the ownership tag, and apply the configuration "+
			"on its own once those it needs are gone", Label(slbEgressConfigFaultTestLabel), func() {
			egressName := "egres-taken-" + suffix

			By("Creating pods without annotations: the NAT Gateway gets the default configuration")
			pods := createConfiguredEgressPods(cs, ns.Name, egressName, 2, nil, false)
			Expect(utils.WaitPodsToBeReady(cs, ns.Name)).To(Succeed())
			current := defaultEgressNATWant(egressName, ipv6)
			eventuallyEgressNAT(egressName, current)
			eventuallyManagedRegistered(cs, ns.Name, egressName, byoNATGatewayARMID(resourceGroupName, egressName))
			logs := egressApplyLogReader(egressName)
			natBefore := readEgressNAT(egressName)
			etags := egressResourceEtags(egressName)
			base := []string{egressManagedPublicIPName(egressName, false, 1)}
			if ipv6 {
				base = append(base, egressManagedPublicIPName(egressName, true, 1))
			}

			// <id>-pip and <id>-pip-v6 are the controller's by their names; every other reserved name only with the
			// identity's ownership tag, whatever other tags the resource has.
			By("Creating, out of band, Public IPs <id>-pip-2 and <id>-pip-3 tagged as someone else's and a prefix <id>-prefix-30 tagged for another identity")
			publicIP2, publicIP3 := egressManagedPublicIPName(egressName, false, 2), egressManagedPublicIPName(egressName, false, 3)
			prefix30 := egressManagedPrefixName(egressName, false, 30)
			someoneElse := map[string]string{"e2e-owner": "someone-else"}
			taken := createOutOfBandResources(
				outOfBandResource{publicIP2, "IPv4", 0, "", someoneElse},
				outOfBandResource{prefix30, "IPv4", 30, "egres-other-" + suffix, nil},
				outOfBandResource{publicIP3, "IPv4", 0, "", someoneElse},
			)
			for _, id := range taken {
				undo = append(undo, func() { deleteBYOResource(id) })
			}
			takenEtags := azureResourceEtags(taken...)
			publicIP3Etag := map[string]string{taken[2]: takenEtags[taken[2]]}

			By("Asking for two managed Public IPs and a managed /30 prefix: every pod is told the two names they need are taken, to be retried")
			since := time.Now()
			setEgressConfig(cs, ns.Name, map[string]string{
				consts.PodAnnotationServiceEgressGatewayPublicIPCount:        "2",
				consts.PodAnnotationServiceEgressGatewayPublicIPPrefixLength: "30",
			}, pods...)
			rejection := egressRejectionWants(egressName, true,
				egressNotOwnedReason("Public IP", publicIP2, egressName), egressNotOwnedReason("Public IP prefix", prefix30, egressName))
			eventuallyEgressEventCounts(cs, ns.Name, consts.ServiceEgressGatewayReasonConfigRejected, v1.EventTypeWarning, egressCountsOf(pods, 1), rejection...)
			Expect(egressEventCounts(cs, ns.Name, pods, consts.ServiceEgressGatewayReasonConfigRejected, v1.EventTypeWarning, publicIP3)).
				To(HaveEach(BeZero()), "a reserved name the configuration does not need is no problem")
			events := egressConfigEventCounts(cs, ns.Name)

			By("Watching the controller retry it: nothing is written or created, the resources at the reserved names are neither changed " +
				"nor deleted, and the pods are not told again")
			Consistently(func() error {
				if err := egressEtagsUnchangedErr(etags); err != nil {
					return err
				}
				if err := egressEtagsUnchangedErr(takenEtags); err != nil {
					return err
				}
				return egressReservedResourcesErr(egressName, append(slices.Clone(base), publicIP2, publicIP3), []string{prefix30})
			}, egressBackoffObservation, defaultPollInterval).Should(Succeed(), "a rejected configuration must not touch Azure")
			Expect(egressConfigEventCounts(cs, ns.Name)).To(Equal(events), "a rejected configuration is announced once, however often it is tried")
			expectEgressRejectionsLogged(logs, egressName, since, egressRetriedWithBackoff())

			By("Deleting <id>-pip-2: still rejected, now for <id>-prefix-30 alone, nothing created, and the pods are not told again")
			deleteBYOResource(taken[0])
			eventuallyEgressRejectionLogged(logs, egressName, time.Now(), []string{prefix30, "exists without the ownership tag"}, publicIP2)
			Consistently(func() error {
				if err := egressEtagsUnchangedErr(etags); err != nil {
					return err
				}
				if err := egressEtagsUnchangedErr(map[string]string{taken[1]: takenEtags[taken[1]], taken[2]: takenEtags[taken[2]]}); err != nil {
					return err
				}
				return egressReservedResourcesErr(egressName, append(slices.Clone(base), publicIP3), []string{prefix30})
			}, 30*time.Second, defaultPollInterval).Should(Succeed(), "no managed resource may be created while the configuration is rejected")
			Expect(egressConfigEventCounts(cs, ns.Name)).To(Equal(events), "a rejection whose reasons change is not announced again")

			By("Deleting <id>-prefix-30: the controller applies the configuration on its own, with a Public IP and a prefix of its own " +
				"at those names, and leaves <id>-pip-3 alone")
			deleteBYOResource(taken[1])
			want := current
			want.publicIPs = egressManagedPublicIPIDs(egressName, false, 2)
			want.prefixes = []string{egressManagedPrefixID(egressName, false, 30)}
			Eventually(func() error {
				return egressNATShapeErr(egressName, want, publicIP3)
			}, egressRetryApplyTimeout, defaultPollInterval).Should(Succeed(), "the controller must apply the configuration once nothing rejects it")
			expectSameEgressNAT(egressName, natBefore)
			eventuallyEgressAppliedCounts(cs, ns.Name, egressCountsOf(pods, 1),
				egressAppliedMessage(egressName, "idle timeout 4m; IPv4: 2 managed IPs, managed prefix /30"+defaultIPv6Summary(ipv6)))
			Expect(egressEventCounts(cs, ns.Name, pods, consts.ServiceEgressGatewayReasonConfigRejected, v1.EventTypeWarning, rejection...)).
				To(HaveEach(Equal(int32(1))), "a rejected configuration is announced once")
			Expect(egressEtagsUnchangedErr(publicIP3Etag)).To(Succeed(), "<id>-pip-3 must survive the sweep after the apply unchanged")

			By("Deleting the pods: the NAT Gateway and all it owns are deleted without holding the last pod, <id>-pip-3 is left alone")
			deleteEgressPods(cs, ns.Name, egressName)
			Eventually(func() error {
				if err := egressPodsGoneErr(cs, ns.Name, egressName); err != nil {
					return err
				}
				return egressIdentityGoneErr(egressName, publicIP3)
			}, 8*time.Minute, defaultPollInterval).Should(Succeed(), "the identity must be deleted, except the resource that is not its own")
			Expect(egressEtagsUnchangedErr(publicIP3Etag)).To(Succeed(), "<id>-pip-3 must survive the identity's deletion unchanged")
			deleteBYOResource(taken[2])
		})

		It("should retry a configuration whose user prefix has Public IPs allocated from it and apply it on its own once they are deleted, "+
			"and leave in place a prefix of its own that Public IPs are allocated from", Label(slbEgressConfigFaultTestLabel), func() {
			egressName := "egres-inuse-" + suffix
			prefixIDsKey := consts.PodAnnotationServiceEgressGatewayPublicIPPrefixIDs

			By("Creating a StandardV2 Public IP and a StandardV2 /31 prefix with a Public IP allocated from it in a test resource group")
			rg, granted := createEgressUserResourceGroup(suffix)
			free := createBYOPublicIP(rg, "egres-free-pip", "IPv4")
			prefix := createBYOPublicIPPrefix(rg, "egres-used-prefix", "IPv4", 31)
			allocated := []string{createUserPublicIPFromPrefix(rg, "egres-allocated-pip-1", prefix)}

			By("Creating pods that ask for the Public IP: the NAT Gateway uses it, so the controller can read the test resource group")
			pods := createConfiguredEgressPods(cs, ns.Name, egressName, 2,
				map[string]string{consts.PodAnnotationServiceEgressGatewayPublicIPIDs: free}, false)
			Expect(utils.WaitPodsToBeReady(cs, ns.Name)).To(Succeed())
			current := defaultEgressNATWant(egressName, ipv6)
			current.publicIPs = []string{free}
			eventuallyEgressNATWithUserResources(cs, ns.Name, pods, egressName, current, rg, granted)
			eventuallyManagedRegistered(cs, ns.Name, egressName, byoNATGatewayARMID(resourceGroupName, egressName))
			logs := egressApplyLogReader(egressName)
			natBefore := readEgressNAT(egressName)

			By("Asking for the prefix instead: every pod is told it is in use by the Public IP allocated from it, to be retried, and nothing is written")
			inUse := func(publicIPs int) string {
				return fmt.Sprintf("is in use by %d Public IPs allocated from it", publicIPs)
			}
			rejection := egressRejectionWants(egressName, true, fmt.Sprintf("user Public IP prefix %q %s", strings.ToLower(prefix), inUse(1)))
			events := expectEgressRejectedRetried(cs, ns.Name, egressName, pods, logs, map[string]string{prefixIDsKey: prefix}, current, rejection...)

			By("Allocating a second Public IP from the prefix: the controller now rejects the configuration for both, and the pods are not told again")
			etags := egressResourceEtags(egressName)
			allocatedAt := time.Now()
			allocated = append(allocated, createUserPublicIPFromPrefix(rg, "egres-allocated-pip-2", prefix))
			eventuallyEgressRejectionLogged(logs, egressName, allocatedAt, []string{inUse(2)})
			expectEgressUntouched(egressName, current, etags, 30*time.Second)
			Expect(egressConfigEventCounts(cs, ns.Name)).To(Equal(events), "a rejection whose reason changes is not announced again")

			By("Deleting both Public IPs allocated from the prefix: the controller applies the configuration on its own")
			for _, id := range allocated {
				deleteBYOResource(id)
			}
			current.publicIPs, current.prefixes = nil, []string{prefix}
			Eventually(func() error {
				return egressNATShapeErr(egressName, current)
			}, egressRetryApplyTimeout, defaultPollInterval).Should(Succeed(), "the controller must apply the configuration once nothing rejects it")
			expectSameEgressNAT(egressName, natBefore)
			eventuallyEgressAppliedCounts(cs, ns.Name, egressCountsOf(pods, 1),
				egressAppliedMessage(egressName, "idle timeout 4m; IPv4: user prefixes (egres-used-prefix)"+defaultIPv6Summary(ipv6)))
			Expect(egressEventCounts(cs, ns.Name, pods, consts.ServiceEgressGatewayReasonConfigRejected, v1.EventTypeWarning, rejection...)).
				To(HaveEach(Equal(int32(1))), "a rejected configuration is announced once")
			Expect(egressEventCounts(cs, ns.Name, pods, consts.ServiceEgressGatewayReasonConfigRejected, v1.EventTypeWarning, inUse(2))).
				To(HaveEach(BeZero()), "a rejection whose reason changes is not announced again")

			// Azure allocates no Public IP from a prefix that a NAT Gateway uses, so the prefix is one the controller no longer uses.
			By("Creating, out of band, a /28 prefix at the reserved name <id>-prefix-28 with the identity's ownership tag and a Public IP " +
				"allocated from it, and agreeing on idle timeout 5: applied in place, and the prefix, in use, is left in place, its " +
				"deletion tried once")
			// A pending confirmation pass would try that deletion once more.
			waitForEgressConfirmationPassOver()
			leftover := createOutOfBandResources(outOfBandResource{egressManagedPrefixName(egressName, false, 28), "IPv4", 28, egressName, nil})[0]
			undo = append(undo, func() { deleteBYOResource(leftover) })
			allocatedFromLeftover := createUserPublicIPFromPrefix(rg, "egres-allocated-pip-3", leftover)
			undo = append(undo, func() { deleteBYOResource(allocatedFromLeftover) })
			leftoverEtag := azureResourceEtags(leftover)
			current.idleTimeout = 5
			setEgressConfig(cs, ns.Name, map[string]string{prefixIDsKey: prefix, consts.PodAnnotationServiceEgressGatewayIdleTimeout: "5"}, pods...)
			Eventually(func() error {
				return egressNATShapeErr(egressName, current, resourceNameFromID(leftover))
			}, egressApplyTimeout, defaultPollInterval).Should(Succeed(), "the agreed change must be applied although a leftover cannot be deleted")
			expectSameEgressNAT(egressName, natBefore)
			eventuallyEgressAppliedCounts(cs, ns.Name, egressCountsOf(pods, 1),
				egressAppliedMessage(egressName, "idle timeout 5m; IPv4: user prefixes (egres-used-prefix)"+defaultIPv6Summary(ipv6)))
			Consistently(func() error {
				if err := egressEtagsUnchangedErr(leftoverEtag); err != nil {
					return err
				}
				return egressNATShapeErr(egressName, current, resourceNameFromID(leftover))
			}, egressBackoffObservation, defaultPollInterval).Should(Succeed(), "a prefix of the controller that Public IPs are allocated from is left in place")
			what := "the attempts to delete " + resourceNameFromID(leftover)
			deletionLog := ccmEgressLog(what)
			// leftoverTries returns, where the log can tell (ok), how many times the controller tried to delete <id>-prefix-28:
			// it never writes the prefix otherwise, and Azure refuses each try, the prefix being in use.
			leftoverTries := func() (int, bool) {
				writes, ok := ccmEgressWrites(deletionLog, resourceNameFromID(leftover), what)
				if !ok {
					return 0, false
				}
				Expect(writes).To(HaveEach(ContainSubstring(`"Deleting Public IP prefix"`)), "the controller must never write its prefix, only try to delete it")
				if kept, ok := ccmLogLinesFor(deletionLog, time.Time{}, what, `"Kept reserved egress resource in use elsewhere"`,
					fmt.Sprintf("resource=%q", resourceNameFromID(leftover))); ok {
					Expect(kept).To(HaveLen(len(writes)), "Azure must refuse every deletion of a prefix in use")
				}
				return len(writes), true
			}
			if tries, ok := leftoverTries(); ok {
				Expect(tries).To(Equal(1), "the controller must try to delete its unused prefix once, and not again on Azure's refusal")
			}

			By("Deleting the pods while a Public IP is still allocated from <id>-prefix-28: the identity is deleted with its NAT " +
				"Gateway and the pods are released, the prefix, in use, is left in place, its deletion tried once more")
			deleteEgressPods(cs, ns.Name, egressName)
			Eventually(func() error {
				if err := egressPodsGoneErr(cs, ns.Name, egressName); err != nil {
					return err
				}
				return egressIdentityGoneErr(egressName, resourceNameFromID(leftover))
			}, 8*time.Minute, defaultPollInterval).Should(Succeed(), "the identity must be deleted although a prefix of it is in use elsewhere")
			Expect(egressEtagsUnchangedErr(leftoverEtag)).To(Succeed(), "a prefix of the controller that Public IPs are allocated from is left in place")
			// A deletion run that fails after its sweep for another reason, such as a throttled unregister, sweeps again; one that
			// retried on the refusal itself would never release the pods, checked above.
			if tries, ok := leftoverTries(); ok {
				Expect(tries).To(BeNumerically(">=", 2), "the controller must try to delete its prefix once more when the identity is deleted")
			}

			By("Deleting the Public IP allocated from <id>-prefix-28 and the prefix: nothing of the identity is left, and the user " +
				"resources are detached and kept")
			deleteBYOResource(allocatedFromLeftover)
			deleteBYOResource(leftover)
			eventuallyEgressIdentityGone(egressName)
			expectUserEgressResourcesReleased(free, prefix)
		})

		It("should keep its obligations when locks refuse its writes and finish them once the locks are lifted",
			Label(slbEgressConfigFaultTestLabel), func() {
				egressName := "egres-locked-" + suffix
				countKey := consts.PodAnnotationServiceEgressGatewayPublicIPCount

				By("Creating pods that ask for two managed Public IPs")
				pods := createConfiguredEgressPods(cs, ns.Name, egressName, 2, map[string]string{countKey: "2"}, false)
				Expect(utils.WaitPodsToBeReady(cs, ns.Name)).To(Succeed())
				two := defaultEgressNATWant(egressName, ipv6)
				two.publicIPs = egressManagedPublicIPIDs(egressName, false, 2)
				eventuallyEgressNAT(egressName, two)
				eventuallyManagedRegistered(cs, ns.Name, egressName, byoNATGatewayARMID(resourceGroupName, egressName))
				natGatewayID, publicIP2 := byoNATGatewayARMID(resourceGroupName, egressName), two.publicIPs[1]
				one := defaultEgressNATWant(egressName, ipv6)
				natBefore := readEgressNAT(egressName)
				// Only this spec verifies these live.
				unverified := "a scale-down applied although a Public IP it no longer needs cannot be deleted yet, which the controller " +
					"deletes on its own once it may; a NAT Gateway update refused after the Public IPs it needs were created, which the " +
					"controller deletes once the pods go back; an identity's deletion that holds its last pod until its NAT Gateway may " +
					"be deleted"

				By("Locking <id>-pip-2 against deletion and going back to one Public IP: applied in place and announced, " +
					"<id>-pip-2 detached and kept while it is locked")
				unlockPublicIP2 := lockAzureResource(publicIP2, "CanNotDelete", unverified)
				undo = append(undo, unlockPublicIP2)
				setEgressConfig(cs, ns.Name, nil, pods...)
				Eventually(func() error {
					return egressNATShapeErr(egressName, one, resourceNameFromID(publicIP2))
				}, egressApplyTimeout, defaultPollInterval).Should(Succeed(), "the scale-down must be applied although <id>-pip-2 cannot be deleted")
				expectSameEgressNAT(egressName, natBefore)
				eventuallyEgressAppliedCounts(cs, ns.Name, egressCountsOf(pods, 1),
					egressAppliedMessage(egressName, "idle timeout 4m; IPv4: 1 managed IP"+defaultIPv6Summary(ipv6)))
				Consistently(func() error {
					return egressNATShapeErr(egressName, one, resourceNameFromID(publicIP2))
				}, time.Minute, defaultPollInterval).Should(Succeed(), "a Public IP locked against deletion is kept")

				By("Unlocking <id>-pip-2: the controller deletes it on its own")
				unlockPublicIP2()
				Eventually(func() error {
					return egressNATShapeErr(egressName, one)
				}, egressRetryApplyTimeout, defaultPollInterval).Should(Succeed(), "the controller must delete the Public IP it no longer needs once it may")

				By("Locking the NAT Gateway read-only and asking for two Public IPs again: <id>-pip-2 is created, the NAT Gateway update " +
					"is refused and retried, and the pods are told nothing")
				unlockNAT := lockAzureResource(natGatewayID, "ReadOnly", unverified)
				undo = append(undo, unlockNAT)
				natEtag := azureResourceEtags(natGatewayID)
				announcedCounts := func() map[string]map[string]int32 {
					return egressEventTally(cs, ns.Name, pods, consts.ServiceEgressGatewayReasonConfigApplied, consts.ServiceEgressGatewayReasonConfigRejected)
				}
				announced := announcedCounts()
				setEgressConfig(cs, ns.Name, map[string]string{countKey: "2"}, pods...)
				Eventually(func() error {
					return egressManagedResourceErr(egressName, publicIP2, false, 0, nil)
				}, egressApplyTimeout, defaultPollInterval).Should(Succeed(), "the controller creates <id>-pip-2 before it updates the NAT Gateway")
				pip2Etag := azureResourceEtags(publicIP2)
				Consistently(func() error {
					if err := egressEtagsUnchangedErr(natEtag); err != nil {
						return err
					}
					if err := egressManagedResourceErr(egressName, publicIP2, false, 0, nil); err != nil {
						return err
					}
					return egressEtagsUnchangedErr(pip2Etag)
				}, time.Minute, defaultPollInterval).Should(Succeed(), "a read-only NAT Gateway cannot be written, and <id>-pip-2 is kept "+
					"while the refused update is retried")
				Expect(announcedCounts()).To(Equal(announced), "a refused NAT Gateway update is retried without telling the pods")

				By("Going back to one Public IP while the NAT Gateway is still locked: the controller deletes the <id>-pip-2 it created and announces nothing")
				setEgressConfig(cs, ns.Name, nil, pods...)
				Eventually(func() error {
					return egressNATShapeErr(egressName, one)
				}, egressRetryApplyTimeout, defaultPollInterval).Should(Succeed(), "the controller must delete the Public IP it created for a change it did not make")
				Expect(egressEtagsUnchangedErr(natEtag)).To(Succeed(), "a read-only NAT Gateway cannot be written")
				Expect(announcedCounts()).To(Equal(announced), "going back to the applied configuration is not announced")
				unlockNAT()

				By("Locking the NAT Gateway against deletion and deleting the pods: the last pod is held Terminating while the NAT Gateway cannot be deleted")
				unlockNAT = lockAzureResource(natGatewayID, "CanNotDelete", unverified)
				undo = append(undo, unlockNAT)
				deleteEgressPods(cs, ns.Name, egressName)
				Consistently(func() error {
					if egressPodsGoneErr(cs, ns.Name, egressName) == nil {
						return fmt.Errorf("no pod of %s is left while its NAT Gateway cannot be deleted", egressName)
					}
					_, found, err := getAzureNetworkResource(natGatewayID)
					switch {
					case err != nil:
						return err
					case !found:
						return fmt.Errorf("NAT Gateway %s is gone despite its lock", egressName)
					}
					return nil
				}, 2*time.Minute, defaultPollInterval).Should(Succeed(), "the last pod keeps its finalizer until its NAT Gateway is deleted")

				By("Unlocking the NAT Gateway: it and its Public IPs are deleted and the last pod goes")
				unlockNAT()
				Eventually(func() error {
					if err := egressPodsGoneErr(cs, ns.Name, egressName); err != nil {
						return err
					}
					return egressIdentityGoneErr(egressName)
				}, egressRetryApplyTimeout, defaultPollInterval).Should(Succeed(), "the identity must be deleted once its NAT Gateway may be")
			})

		It("should leave out-of-band changes of its NAT Gateway, its link and its registration alone until the pods agree on a change, "+
			"then correct them in place", Label(slbEgressConfigFaultTestLabel), func() {
			egressName := "egres-drift-" + suffix
			idleKey := consts.PodAnnotationServiceEgressGatewayIdleTimeout

			By("Creating pods that ask for idle timeout 10")
			pods := createConfiguredEgressPods(cs, ns.Name, egressName, 2, map[string]string{idleKey: "10"}, false)
			Expect(utils.WaitPodsToBeReady(cs, ns.Name)).To(Succeed())
			want := defaultEgressNATWant(egressName, ipv6)
			want.idleTimeout = 10
			eventuallyEgressNAT(egressName, want)
			eventuallyManagedRegistered(cs, ns.Name, egressName, byoNATGatewayARMID(resourceGroupName, egressName))
			natGatewayID := byoNATGatewayARMID(resourceGroupName, egressName)

			By("Changing the NAT Gateway's idle timeout to 20 and tagging it out of band, then clearing it from the identity's " +
				"registration and unlinking it from the Service Gateway: the controller corrects nothing for 2 minutes")
			waitForEgressConfirmationPassOver()
			rewriteNATGatewayOutOfBand(natGatewayID, func(properties map[string]any) { properties["idleTimeoutInMinutes"] = 20 })
			Expect(egressIdleTimeout(readEgressNAT(egressName))).To(Equal(20), "NAT Gateway %s must have the new idle timeout", natGatewayID)
			setNetworkResourceTag(natGatewayID, "e2e-foreign", "kept")
			clearEgressRegistrationNAT(egressName)
			unlinkEgressNATGateway(egressName)
			etags := egressResourceEtags(egressName)
			events := egressConfigEventCounts(cs, ns.Name)
			Consistently(func() error {
				if err := egressEtagsUnchangedErr(etags); err != nil {
					return err
				}
				if link := byoNATGatewayLink(natGatewayID); link != "" {
					return fmt.Errorf("NAT Gateway %s was linked to %s again", egressName, link)
				}
				// A correction that landed before the etags were read would be in them.
				nat := readEgressNAT(egressName)
				if idle := egressIdleTimeout(nat); idle != 20 {
					return fmt.Errorf("NAT Gateway %s was corrected to idle timeout %d", egressName, idle)
				}
				if tagValue(nat.Tags, "e2e-foreign") != "kept" {
					return fmt.Errorf("NAT Gateway %s lost the tag e2e-foreign", egressName)
				}
				return outboundServiceUsesNATGatewayErr(egressName, "")
			}, 2*time.Minute, defaultPollInterval).Should(Succeed(), "the controller does not watch the NAT Gateway, its link or the registration")
			Expect(egressConfigEventCounts(cs, ns.Name)).To(Equal(events), "nothing is announced for a change the controller does not watch")

			By("Agreeing on two managed Public IPs: the update also restores idle timeout 10, in place, keeps the tag, and links " +
				"and registers the NAT Gateway again")
			want.publicIPs = egressManagedPublicIPIDs(egressName, false, 2)
			expectEgressApplied(cs, ns.Name, egressName, pods, map[string]string{idleKey: "10", consts.PodAnnotationServiceEgressGatewayPublicIPCount: "2"},
				want, "idle timeout 10m; IPv4: 2 managed IPs"+defaultIPv6Summary(ipv6))
			eventuallyManagedRegistered(cs, ns.Name, egressName, natGatewayID)
			Expect(tagValue(readEgressNAT(egressName).Tags, "e2e-foreign")).To(Equal("kept"), "a tag someone else added must survive the correction")

			By("Deleting the pods: the NAT Gateway and its Public IPs are deleted")
			deleteEgressPods(cs, ns.Name, egressName)
			eventuallyEgressIdentityGone(egressName)
		})

		It("should neither change, register nor delete a NAT Gateway that another Service Gateway service uses, telling an existing "+
			"identity's pods nothing and the pod that creates the identity why, and create the identity on it once that service is "+
			"gone, with that pod's vote, or with the oldest member's when that pod is gone", Label(slbEgressConfigFaultTestLabel), func() {
			egressName, other := "egres-shared-"+suffix, "e2e-egres-other-"+suffix
			idleKey := consts.PodAnnotationServiceEgressGatewayIdleTimeout
			// createUnlabelled creates the i-th pod of the identity, asking for idle timeout idle, without the egress label: it
			// joins the identity once labelled. Pods are numbered apart, as events are matched by pod name.
			createUnlabelled := func(i int, idle string) string {
				pod := newEgressConfigPod(ns.Name, egressName, i, map[string]string{idleKey: idle}, false)
				pod.Labels = nil // created directly: it joins no identity yet
				_, err := cs.CoreV1().Pods(ns.Name).Create(context.TODO(), pod, metav1.CreateOptions{})
				Expect(err).NotTo(HaveOccurred())
				return pod.Name
			}

			By("Creating a pod without annotations: the NAT Gateway gets the default configuration")
			pod := createConfiguredEgressPods(cs, ns.Name, egressName, 1, nil, false)[0]
			Expect(utils.WaitPodsToBeReady(cs, ns.Name)).To(Succeed())
			want := defaultEgressNATWant(egressName, ipv6)
			eventuallyEgressNAT(egressName, want)
			natGatewayID := byoNATGatewayARMID(resourceGroupName, egressName)
			eventuallyManagedRegistered(cs, ns.Name, egressName, natGatewayID)
			natBefore := readEgressNAT(egressName)

			By("Clearing the NAT Gateway from the identity's registration and registering another outbound service with it, out of band")
			waitForEgressConfirmationPassOver()
			// Removed before the AfterEach checks the Service Gateway, also when the spec fails, and else after it, so that it
			// cannot fail the specs that follow.
			DeferCleanup(func() { Expect(removeOutboundServiceOutOfBand(other)).To(Succeed()) })
			defer func() {
				if err := removeOutboundServiceOutOfBand(other); err != nil {
					utils.Logf("%v", err)
				}
			}()
			clearEgressRegistrationNAT(egressName)
			registerOutboundServiceOutOfBand(other, natGatewayID)
			etags := egressResourceEtags(egressName)
			logs, writes := ccmEgressWritesSoFar(egressName)
			events := egressConfigEventCounts(cs, ns.Name)

			By("Asking the pod for idle timeout 9: while the other service uses the NAT Gateway, the controller neither changes it " +
				"nor registers the identity with it, and tells the pod nothing")
			setEgressPodAnnotations(cs, ns.Name, map[string]string{idleKey: "9"}, pod)
			Consistently(func() error {
				if err := egressEtagsUnchangedErr(etags); err != nil {
					return err
				}
				if err := outboundServiceUsesNATGatewayErr(egressName, ""); err != nil {
					return err
				}
				return outboundServiceUsesNATGatewayErr(other, natGatewayID)
			}, 2*time.Minute, defaultPollInterval).Should(Succeed(), "the controller must leave a NAT Gateway that another service uses alone")
			Expect(egressConfigEventCounts(cs, ns.Name)).To(Equal(events), "a NAT Gateway that another service uses is reported by no event")
			expectNoNewEgressWrites(logs, egressName, writes)

			By("Removing the other service: the controller, which kept retrying, applies idle timeout 9 in place on its own, " +
				"registers the identity with its NAT Gateway again and tells the pod")
			Expect(removeOutboundServiceOutOfBand(other)).To(Succeed(), "the other service must be removed")
			want.idleTimeout = 9
			Eventually(func() error {
				return egressNATShapeErr(egressName, want)
			}, egressRetryApplyTimeout, defaultPollInterval).Should(Succeed(), "the agreed change must be applied once the NAT Gateway is the identity's alone")
			expectSameEgressNAT(egressName, natBefore)
			eventuallyManagedRegistered(cs, ns.Name, egressName, natGatewayID)
			eventuallyEgressAppliedCounts(cs, ns.Name, egressCountsOf([]string{pod}, 1),
				egressAppliedMessage(egressName, "idle timeout 9m; IPv4: 1 managed IP"+defaultIPv6Summary(ipv6)))
			expectNoPodEvent(cs, ns.Name, []string{pod}, consts.ServiceEgressGatewayReasonInvalidConfig,
				consts.ServiceEgressGatewayReasonConfigConflict, consts.ServiceEgressGatewayReasonConfigRejected,
				"ServiceGatewayEgressNATGatewayRejected")

			By("Registering the other service with the NAT Gateway again and deleting the pod: the controller only unregisters the " +
				"identity and releases the pod, and keeps the NAT Gateway and its Public IP, which the other service uses")
			clearEgressRegistrationNAT(egressName)
			registerOutboundServiceOutOfBand(other, natGatewayID)
			etags = egressResourceEtags(egressName)
			logs, writes = ccmEgressWritesSoFar(egressName)
			// From here until a new pod's creation of the identity has been tried (its Warning below), nothing of the controller
			// may be left to delete the NAT Gateway and its Public IPs: a deletion run that decided to keep them can end after
			// the other service is gone, and deleting a pod whose creation was not tried yet runs no deletion. Should the spec
			// fail in between, its pods are deleted, after the other service, and once they are released, whatever is left is
			// removed, before the AfterEach checks.
			orphaned := true
			undo = append(undo, func() {
				if !orphaned {
					return
				}
				deleteEgressPods(cs, ns.Name, egressName)
				Eventually(func() error {
					return egressPodsGoneErr(cs, ns.Name, egressName)
				}, 8*time.Minute, defaultPollInterval).Should(Succeed(), "the pods of %s must be released", egressName)
				_, found, err := getAzureNetworkResource(natGatewayID)
				Expect(err).NotTo(HaveOccurred(), "reading NAT Gateway %s", egressName)
				if found {
					unlinkEgressNATGateway(egressName)
					deleteBYOResource(natGatewayID)
				}
				for _, id := range slices.Concat(want.publicIPs, want.publicIPsV6) {
					deleteBYOResource(id)
				}
			})
			deletion := time.Now()
			deleteEgressPods(cs, ns.Name, egressName)
			Eventually(func() error {
				if err := egressPodsGoneErr(cs, ns.Name, egressName); err != nil {
					return err
				}
				return outboundServiceGoneErr(egressName)
			}, 8*time.Minute, defaultPollInterval).Should(Succeed(), "the identity must be unregistered and its pod released")
			Expect(egressEtagsUnchangedErr(etags)).To(Succeed(), "the NAT Gateway and the Public IP that another service uses must be kept")
			Expect(outboundServiceUsesNATGatewayErr(other, natGatewayID)).To(Succeed(), "the other service must keep its NAT Gateway")
			kept := "that the deletion of " + egressName + " kept the NAT Gateway another service uses"
			if lines, ok := ccmLogLinesFor(ccmEgressLog(kept), deletion, kept, `"Kept NAT Gateway and Public IPs not owned by this identity"`,
				fmt.Sprintf("serviceUID=%q", egressName)); ok {
				Expect(lines).NotTo(BeEmpty(), "the deletion must keep the NAT Gateway because another service uses it")
			}

			By("Creating a pod without the egress label that asks for idle timeout 5, then a pod that asks for idle timeout 9 while the " +
				"other service still uses the NAT Gateway: the controller does not create the identity, writes nothing and tells the " +
				"second pod why, with the Warning ServiceGatewayEgressNATGatewayRejected")
			// The older pod joins while the creation is held, which must keep the vote of the pod that made the controller create
			// the identity. Its index is the higher one, so that its name cannot stand in for its creation time; index 1, as events
			// are matched by pod name and the deleted pod had index 0.
			older := createUnlabelled(2, "5")
			time.Sleep(2 * time.Second) // creation timestamps have a resolution of one second
			newPod := createEgressConfigPod(cs, newEgressConfigPod(ns.Name, egressName, 1, map[string]string{idleKey: "9"}, false))
			Expect(utils.WaitPodsToBeReady(cs, ns.Name)).To(Succeed())
			eventuallyPodEvent(cs, ns.Name, []string{newPod}, "ServiceGatewayEgressNATGatewayRejected", v1.EventTypeWarning,
				"is not managed by this controller")
			// Its creation is tried again until it succeeds; once the other service is gone, deleting the pods deletes it all.
			orphaned = false

			By("Labelling the older pod with the identity while its creation is held: it is told the members disagree, and the " +
				"retries of the creation tell why to the pod that made the controller create the identity, and to no other pod")
			setEgressPodLabel(cs, ns.Name, older, egressName)
			eventuallyPodEvent(cs, ns.Name, []string{older}, consts.ServiceEgressGatewayReasonConfigConflict, v1.EventTypeWarning, "idle timeout 5m")
			warned, err := egressEventCounts(cs, ns.Name, []string{newPod}, "ServiceGatewayEgressNATGatewayRejected", v1.EventTypeWarning)
			Expect(err).NotTo(HaveOccurred())
			Consistently(func() error {
				if err := egressEtagsUnchangedErr(etags); err != nil {
					return err
				}
				return outboundServiceGoneErr(egressName)
			}, 2*time.Minute, defaultPollInterval).Should(Succeed(), "the controller must not create the identity on a NAT Gateway another service uses")
			expectNoNewEgressWrites(logs, egressName, writes)
			warnedAgain, err := egressEventCounts(cs, ns.Name, []string{newPod}, "ServiceGatewayEgressNATGatewayRejected", v1.EventTypeWarning)
			Expect(err).NotTo(HaveOccurred())
			Expect(warnedAgain[newPod]).To(BeNumerically(">", warned[newPod]), "the retries of the creation must tell the pod that made the "+
				"controller create the identity again")
			expectNoPodEvent(cs, ns.Name, []string{newPod}, consts.ServiceEgressGatewayReasonConfigApplied,
				consts.ServiceEgressGatewayReasonInvalidConfig, consts.ServiceEgressGatewayReasonConfigConflict,
				consts.ServiceEgressGatewayReasonConfigRejected)
			expectNoPodEvent(cs, ns.Name, []string{older}, "ServiceGatewayEgressNATGatewayRejected", consts.ServiceEgressGatewayReasonConfigApplied,
				consts.ServiceEgressGatewayReasonInvalidConfig, consts.ServiceEgressGatewayReasonConfigRejected)

			By("Removing the other service: the controller, which kept retrying, creates the identity on the NAT Gateway it kept with " +
				"the vote of the pod that made it create it, idle timeout 9, which the NAT Gateway already has, not with that of its " +
				"oldest member, writes nothing to it and tells both members")
			Expect(removeOutboundServiceOutOfBand(other)).To(Succeed(), "the other service must be removed")
			Eventually(func() error {
				return outboundServiceUsesNATGatewayErr(egressName, natGatewayID)
			}, egressRetryApplyTimeout, defaultPollInterval).Should(Succeed(), "the identity must be created on its NAT Gateway once that is its alone")
			eventuallyManagedRegistered(cs, ns.Name, egressName, natGatewayID)
			expectSameEgressNAT(egressName, natBefore)
			eventuallyEgressNAT(egressName, want)
			eventuallyEachPodApplied(cs, ns.Name, []string{newPod, older}, "idle timeout 9m; IPv4: 1 managed IP"+defaultIPv6Summary(ipv6))
			expectNoNewEgressWrites(logs, egressName, writes)
			expectNoPodEvent(cs, ns.Name, []string{newPod}, consts.ServiceEgressGatewayReasonInvalidConfig,
				consts.ServiceEgressGatewayReasonConfigConflict, consts.ServiceEgressGatewayReasonConfigRejected)
			expectNoPodEvent(cs, ns.Name, []string{older}, "ServiceGatewayEgressNATGatewayRejected",
				consts.ServiceEgressGatewayReasonInvalidConfig, consts.ServiceEgressGatewayReasonConfigRejected)
			Expect(egressEventCounts(cs, ns.Name, []string{older}, consts.ServiceEgressGatewayReasonConfigConflict, v1.EventTypeWarning)).To(
				HaveKeyWithValue(older, int32(1)), "the older pod is told once that the members disagree, when it joins")
			Expect(egressEventCounts(cs, ns.Name, []string{newPod, older}, consts.ServiceEgressGatewayReasonConfigApplied, "")).To(
				Equal(egressCountsOf([]string{newPod, older}, 1)), "each member is told of the creation once")

			By("Registering the other service with the NAT Gateway again and deleting the identity's pods: the controller only " +
				"unregisters the identity and releases the pods, and keeps the NAT Gateway, as before")
			clearEgressRegistrationNAT(egressName)
			registerOutboundServiceOutOfBand(other, natGatewayID)
			etags = egressResourceEtags(egressName)
			logs, writes = ccmEgressWritesSoFar(egressName)
			orphaned = true
			deleteEgressPods(cs, ns.Name, egressName)
			Eventually(func() error {
				if err := egressPodsGoneErr(cs, ns.Name, egressName); err != nil {
					return err
				}
				return outboundServiceGoneErr(egressName)
			}, 8*time.Minute, defaultPollInterval).Should(Succeed(), "the identity must be unregistered and its pods released")
			Expect(egressEtagsUnchangedErr(etags)).To(Succeed(), "the NAT Gateway and the Public IP that another service uses must be kept")

			By("Creating, without the egress label, an older pod that asks for idle timeout 9 and a newer one that asks for 8, then " +
				"a pod that asks for 7, whose creation of the identity is held; labelling the two others and deleting the creator")
			oldest := createUnlabelled(3, "9")
			time.Sleep(2 * time.Second) // creation timestamps have a resolution of one second
			newer := createUnlabelled(4, "8")
			time.Sleep(2 * time.Second)
			creator := createEgressConfigPod(cs, newEgressConfigPod(ns.Name, egressName, 5, map[string]string{idleKey: "7"}, false))
			Expect(utils.WaitPodsToBeReady(cs, ns.Name)).To(Succeed())
			eventuallyPodEvent(cs, ns.Name, []string{creator}, "ServiceGatewayEgressNATGatewayRejected", v1.EventTypeWarning,
				"is not managed by this controller")
			orphaned = false
			setEgressPodLabel(cs, ns.Name, oldest, egressName)
			setEgressPodLabel(cs, ns.Name, newer, egressName)
			for _, pod := range []string{oldest, newer} {
				eventuallyPodEvent(cs, ns.Name, []string{pod}, consts.ServiceEgressGatewayReasonConfigConflict, v1.EventTypeWarning)
			}
			deleteEgressPod(cs, ns.Name, creator)
			Eventually(func() error {
				return egressPodGoneErr(cs, ns.Name, creator)
			}, 4*time.Minute, defaultPollInterval).Should(Succeed(), "the creator, which is not the last member, must be released")

			By("Removing the other service: the creation, decided again at its next try, takes the vote of the oldest member, idle " +
				"timeout 9, not that of the creator, which is gone, nor that of the newer member; the NAT Gateway, which already has it, " +
				"is not written, and both members are told")
			Expect(removeOutboundServiceOutOfBand(other)).To(Succeed(), "the other service must be removed")
			Eventually(func() error {
				return outboundServiceUsesNATGatewayErr(egressName, natGatewayID)
			}, egressRetryApplyTimeout, defaultPollInterval).Should(Succeed(), "the identity must be created on its NAT Gateway once that is its alone")
			eventuallyManagedRegistered(cs, ns.Name, egressName, natGatewayID)
			expectSameEgressNAT(egressName, natBefore)
			eventuallyEgressNAT(egressName, want)
			eventuallyEachPodApplied(cs, ns.Name, []string{oldest, newer}, "idle timeout 9m; IPv4: 1 managed IP"+defaultIPv6Summary(ipv6))
			expectNoNewEgressWrites(logs, egressName, writes)
			expectNoPodEvent(cs, ns.Name, []string{oldest, newer}, "ServiceGatewayEgressNATGatewayRejected",
				consts.ServiceEgressGatewayReasonInvalidConfig, consts.ServiceEgressGatewayReasonConfigRejected)
			for _, reason := range []string{consts.ServiceEgressGatewayReasonConfigApplied, consts.ServiceEgressGatewayReasonConfigConflict} {
				Expect(egressEventCounts(cs, ns.Name, []string{oldest, newer}, reason, "")).To(Equal(egressCountsOf([]string{oldest, newer}, 1)),
					"each member is told once that the members disagree, when it joins, not again when the creator leaves, and once of the creation")
			}

			By("Deleting the pods: the NAT Gateway and its Public IPs are deleted")
			deleteEgressPods(cs, ns.Name, egressName)
			eventuallyEgressIdentityGone(egressName)
		})

		It("should create sixteen managed Public IPs per IP family and replace them in place with a prefix of sixteen addresses", func() {
			egressName := "egres-sixteen-" + suffix
			sixteen := map[string]string{consts.PodAnnotationServiceEgressGatewayPublicIPCount: "16"}
			prefixes := map[string]string{consts.PodAnnotationServiceEgressGatewayPublicIPPrefixLength: "28"}
			publicIPsV6, summary, prefixSummary := 0, "idle timeout 4m; IPv4: 16 managed IPs", "idle timeout 4m; IPv4: managed prefix /28"
			if ipv6 {
				sixteen[consts.PodAnnotationServiceEgressGatewayPublicIPCountIPv6] = "16"
				prefixes[consts.PodAnnotationServiceEgressGatewayPublicIPPrefixLengthIPv6] = "124"
				publicIPsV6, summary, prefixSummary = 16, summary+"; IPv6: 16 managed IPs", prefixSummary+"; IPv6: managed prefix /124"
			}
			skipUnlessPublicIPQuota(16, publicIPsV6)

			By(fmt.Sprintf("Creating pods, the first a probe, that ask for sixteen managed Public IPs per IP family: %d Public IPs", 16+publicIPsV6))
			pods := createConfiguredEgressPods(cs, ns.Name, egressName, 2, sixteen, true)
			Expect(utils.WaitPodsToBeReady(cs, ns.Name)).To(Succeed())
			want := defaultEgressNATWant(egressName, ipv6)
			want.publicIPs = egressManagedPublicIPIDs(egressName, false, 16)
			if ipv6 {
				want.publicIPsV6 = egressManagedPublicIPIDs(egressName, true, 16)
			}
			Eventually(func() error {
				skipIfEgressQuotaReached(cs, ns.Name, pods)
				return egressNATShapeErr(egressName, want)
			}, egressRetryApplyTimeout, defaultPollInterval).Should(Succeed(), "the NAT Gateway must get the sixteen Public IPs per IP family its pods agree on")
			eventuallyManagedRegistered(cs, ns.Name, egressName, byoNATGatewayARMID(resourceGroupName, egressName))
			eventuallyEachPodApplied(cs, ns.Name, pods, egressAppliedMessage(egressName, summary))
			Expect(egressEventCounts(cs, ns.Name, pods, consts.ServiceEgressGatewayReasonConfigApplied, v1.EventTypeNormal,
				egressAppliedMessage(egressName, summary))).To(Equal(egressCountsOf(pods, 1)), "each pod must be told of the creation once")
			natBefore := readEgressNAT(egressName)
			publicIPAddresses := egressNATAddresses(egressName)
			first, observable := waitForProbeEgress(cs, ns.Name, pods[0], publicIPAddresses)

			By("Asking for a /28 prefix per IP family instead: it replaces the Public IPs in place and every one of them is deleted")
			setEgressConfig(cs, ns.Name, prefixes, pods...)
			want.publicIPs, want.publicIPsV6 = nil, nil
			want.prefixes = []string{egressManagedPrefixID(egressName, false, 28)}
			if ipv6 {
				want.prefixesV6 = []string{egressManagedPrefixID(egressName, true, 124)}
			}
			Eventually(func() error {
				skipIfEgressQuotaReached(cs, ns.Name, pods)
				return egressNATShapeErr(egressName, want)
			}, egressRetryApplyTimeout, defaultPollInterval).Should(Succeed(), "the prefixes must replace the sixteen Public IPs per IP family")
			expectSameEgressNAT(egressName, natBefore)
			eventuallyEgressAppliedCounts(cs, ns.Name, egressCountsOf(pods, 1), egressAppliedMessage(egressName, prefixSummary))
			if observable {
				By("Checking egress continued and moved from the Public IPs to the prefix")
				eventuallyProbeSources(cs, ns.Name, pods[0], first, egressAddressesOf(want.prefixes...))
				expectProbeSourcesGone(cs, ns.Name, pods[0], publicIPAddresses)
				expectEgressContinuity(cs, ns.Name, pods[0], first, publicIPAddresses.union(egressNATAddresses(egressName)))
			}

			By("Deleting the pods: the NAT Gateway and its prefixes are deleted")
			deleteEgressPods(cs, ns.Name, egressName)
			eventuallyEgressIdentityGone(egressName)
			leaveOutEgressUnlessObservable(observable)
		})
	})
})

// Helpers of the specs on the Azure resources behind an egress NAT Gateway configuration.

// egressBackoffObservation is how long a spec watches a rejected configuration: longer than the first five retries of one
// rejected until its problem is gone (1, 2, 4, 8 and 16 s apart) and than the longest backoff, 30 s.
const egressBackoffObservation = 45 * time.Second

// egressOtherRegions are the regions, other than the cluster's, in which a spec tries to create a user Public IP.
var egressOtherRegions = []string{"eastus", "westus2"}

// egressAnnotations merges egress configuration annotations; a later part overrides an earlier one.
func egressAnnotations(parts ...map[string]string) map[string]string {
	merged := map[string]string{}
	for _, part := range parts {
		maps.Copy(merged, part)
	}
	return merged
}

// setEgressConfig makes annotations the whole egress configuration of the named pods: their other egress
// configuration annotations are removed.
func setEgressConfig(cs clientset.Interface, namespace string, annotations map[string]string, podNames ...string) {
	for _, name := range podNames {
		pod, err := cs.CoreV1().Pods(namespace).Get(context.TODO(), name, metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		patch := egressAnnotations(annotations)
		for key := range pod.Annotations {
			if _, kept := annotations[key]; !kept && strings.HasPrefix(key, consts.PodAnnotationServiceEgressGatewayPrefix) {
				patch[key] = ""
			}
		}
		setEgressPodAnnotations(cs, namespace, patch, name)
	}
}

// egressCountsOf returns count for every pod of podNames.
func egressCountsOf(podNames []string, count int32) map[string]int32 {
	counts := make(map[string]int32, len(podNames))
	for _, name := range podNames {
		counts[name] = count
	}
	return counts
}

// egressCountsPlusOne returns counts with one more for every pod.
func egressCountsPlusOne(counts map[string]int32) map[string]int32 {
	plus := make(map[string]int32, len(counts))
	for name, count := range counts {
		plus[name] = count + 1
	}
	return plus
}

// egressRejectionWants returns the parts of the EgressConfigRejected message that tell the pods of egressName that the
// configuration they agree on was rejected for reasons, and whether it is retried.
func egressRejectionWants(egressName string, retried bool, reasons ...string) []string {
	next := egressRejectedForGood
	if retried {
		next = egressRejectedRetried
	}
	return slices.Concat([]string{fmt.Sprintf("Egress identity %q rejected NAT Gateway configuration ", egressName)}, reasons,
		[]string{"; " + next + "."})
}

// egressAddressesReason is the reason an IPv4 configuration with addresses public addresses is rejected for, once the
// controller has read the real size of its user prefixes.
func egressAddressesReason(addresses int) string {
	return fmt.Sprintf("IPv4: %d public addresses, more than the %d a NAT Gateway accepts "+
		"(managed Public IPs + managed prefix size + user Public IPs + user prefix sizes)", addresses, consts.ServiceEgressGatewayMaxAddressesPerFamily)
}

// egressNotOwnedReason is the reason a configuration of egressName that needs the reserved name name is rejected for
// while a resource of kind ("Public IP" or "Public IP prefix") without the identity's ownership tag holds the name.
func egressNotOwnedReason(kind, name, egressName string) string {
	return fmt.Sprintf("%s %q exists without the ownership tag of egress identity %q and is never overwritten", kind, name, egressName)
}

// egressAppliedMessage returns the EgressConfigApplied message telling the pods of egressName that its NAT Gateway now
// has the configuration summarized as summary.
func egressAppliedMessage(egressName, summary string) string {
	return fmt.Sprintf("Egress identity %q applied NAT Gateway configuration: %s.", egressName, summary)
}

// defaultIPv6Summary is how an event summarizes the default IPv6 side of a configuration: one managed Public IP where
// identities serve IPv6, nothing elsewhere.
func defaultIPv6Summary(ipv6 bool) string {
	if ipv6 {
		return "; IPv6: 1 managed IP"
	}
	return ""
}

// egressTagMap returns count tags "e2e-tNN" with value "1".
func egressTagMap(count int) map[string]string {
	tags := make(map[string]string, count)
	for i := 1; i <= count; i++ {
		tags[fmt.Sprintf("e2e-t%02d", i)] = "1"
	}
	return tags
}

// egressTagList returns the tags of egressTagMap as a tags annotation, in the canonical order of the events.
func egressTagList(count int) string {
	tags := egressTagMap(count)
	entries := make([]string, 0, count)
	for _, key := range slices.Sorted(maps.Keys(tags)) {
		entries = append(entries, key+"="+tags[key])
	}
	return strings.Join(entries, ",")
}

// expectEgressRejectedForGood gives the pods of egressName the egress configuration annotations, which the controller must
// reject for good, and checks that it does: every pod is told so once more, for reasons; and for longer than the
// controller's retry backoff the NAT Gateway keeps the shape want, nothing writes it or its managed Public IPs and
// prefixes, the pods are told nothing more and, where logs can read the controller's log, the controller does not try
// the configuration again.
func expectEgressRejectedForGood(cs clientset.Interface, namespace, egressName string, pods []string, logs *CCMClusterClient,
	annotations map[string]string, want egressNATWant, reasons ...string) {
	wants := egressRejectionWants(egressName, false, reasons...)
	before, err := egressEventCounts(cs, namespace, pods, consts.ServiceEgressGatewayReasonConfigRejected, v1.EventTypeWarning, wants...)
	Expect(err).NotTo(HaveOccurred())
	etags := egressResourceEtags(egressName)
	since := time.Now()
	setEgressConfig(cs, namespace, annotations, pods...)
	eventuallyEgressEventCounts(cs, namespace, consts.ServiceEgressGatewayReasonConfigRejected, v1.EventTypeWarning, egressCountsPlusOne(before), wants...)
	events := egressConfigEventCounts(cs, namespace)
	expectEgressUntouched(egressName, want, etags, egressBackoffObservation)
	Expect(egressConfigEventCounts(cs, namespace)).To(Equal(events), "a configuration rejected for good is announced once")
	expectEgressRejectionsLogged(logs, egressName, since, Equal(1))
}

// expectEgressRejectedRetried gives the pods of egressName the egress configuration annotations, which the controller must
// reject until a problem outside the configuration is gone, and checks that it does: every pod is told so once more,
// with wants in the message; and for longer than the controller's retry backoff the NAT Gateway keeps the shape want,
// nothing writes it or its managed Public IPs and prefixes, the pods are told nothing more and, where logs can read the
// controller's log, the controller tries the configuration again, backing off. It returns the counts of the egress
// configuration events after the rejection (egressConfigEventCounts).
func expectEgressRejectedRetried(cs clientset.Interface, namespace, egressName string, pods []string, logs *CCMClusterClient,
	annotations map[string]string, want egressNATWant, wants ...string) map[string]int32 {
	before, err := egressEventCounts(cs, namespace, pods, consts.ServiceEgressGatewayReasonConfigRejected, v1.EventTypeWarning, wants...)
	Expect(err).NotTo(HaveOccurred())
	etags := egressResourceEtags(egressName)
	since := time.Now()
	setEgressConfig(cs, namespace, annotations, pods...)
	eventuallyEgressEventCounts(cs, namespace, consts.ServiceEgressGatewayReasonConfigRejected, v1.EventTypeWarning, egressCountsPlusOne(before), wants...)
	events := egressConfigEventCounts(cs, namespace)
	expectEgressUntouched(egressName, want, etags, egressBackoffObservation)
	Expect(egressConfigEventCounts(cs, namespace)).To(Equal(events), "a rejected configuration is announced once, however often it is tried")
	expectEgressRejectionsLogged(logs, egressName, since, egressRetriedWithBackoff())
	return events
}

// expectEgressApplied gives the pods of egressName the egress configuration annotations and checks that it is applied in
// place: the NAT Gateway gets the shape want, and every pod is told once more that the configuration summarized as
// summary was applied.
func expectEgressApplied(cs clientset.Interface, namespace, egressName string, pods []string, annotations map[string]string,
	want egressNATWant, summary string) {
	message := egressAppliedMessage(egressName, summary)
	before, err := egressEventCounts(cs, namespace, pods, consts.ServiceEgressGatewayReasonConfigApplied, v1.EventTypeNormal, message)
	Expect(err).NotTo(HaveOccurred())
	natBefore := readEgressNAT(egressName)
	setEgressConfig(cs, namespace, annotations, pods...)
	eventuallyEgressNAT(egressName, want)
	expectSameEgressNAT(egressName, natBefore)
	eventuallyEgressAppliedCounts(cs, namespace, egressCountsPlusOne(before), message)
}

// expectEgressUntouched checks, for duration, that the NAT Gateway of egressName keeps the shape want and that nothing
// writes it or the resources of etags.
func expectEgressUntouched(egressName string, want egressNATWant, etags map[string]string, duration time.Duration) {
	Consistently(func() error {
		if err := egressEtagsUnchangedErr(etags); err != nil {
			return err
		}
		return egressNATShapeErr(egressName, want)
	}, duration, defaultPollInterval).Should(Succeed(), "nothing may change the NAT Gateway of %s or its managed resources", egressName)
}

// egressRetriedWithBackoff matches how many times the controller tried a configuration rejected until its problem is
// gone in about egressBackoffObservation: again and again, but backing off (1 s, doubling up to 30 s), never in a busy
// loop.
func egressRetriedWithBackoff() types.GomegaMatcher {
	return And(BeNumerically(">=", 2), BeNumerically("<=", 12))
}

// registerOutboundServiceOutOfBand registers outbound service name with the Service Gateway, out of band, as another
// user of NAT Gateway natGatewayID would, and waits until it uses it. The spec removes it itself
// (removeOutboundServiceOutOfBand), before the AfterEach checks the Service Gateway.
func registerOutboundServiceOutOfBand(name, natGatewayID string) {
	out, err := updateServiceGatewayService(name, false, map[string]any{"serviceType": "Outbound", "publicNatGatewayId": natGatewayID})
	Expect(err).NotTo(HaveOccurred(), "registering Service Gateway service %s: %s", name, string(out))
	Eventually(func() error {
		return outboundServiceUsesNATGatewayErr(name, natGatewayID)
	}, 2*time.Minute, defaultPollInterval).Should(Succeed(), "Service Gateway service %s must use NAT Gateway %s", name, natGatewayID)
}

// removeOutboundServiceOutOfBand removes Service Gateway service name, which registerOutboundServiceOutOfBand registered,
// asking again until it is gone, for up to 3 minutes: a refused request, such as a 409 while the Service Gateway is busy,
// is sent again. It returns why the service is still there, so that it can also run deferred, while a failed spec
// unwinds.
func removeOutboundServiceOutOfBand(name string) error {
	for deadline := time.Now().Add(3 * time.Minute); ; time.Sleep(defaultPollInterval) {
		err := outboundServiceGoneErr(name)
		if err == nil {
			return nil
		}
		if out, deleteErr := updateServiceGatewayService(name, true, map[string]any{"serviceType": "Outbound"}); deleteErr != nil &&
			!strings.Contains(string(out), "NotFound") {
			err = fmt.Errorf("removing Service Gateway service %s: %w: %s", name, deleteErr, string(out))
		}
		if time.Now().After(deadline) {
			return err
		}
	}
}

// egressPodsGoneErr returns nil once no pod of egressName is left in namespace, terminating ones included: none is
// held by its finalizer.
func egressPodsGoneErr(cs clientset.Interface, namespace, egressName string) error {
	pods, err := livePodCount(cs, namespace, egressName)
	switch {
	case err != nil:
		return err
	case pods > 0:
		return fmt.Errorf("%d pod(s) of %s are still there", pods, egressName)
	}
	return nil
}

// egressApplyLogReader returns a client of the cloud-controller-manager's cluster whose log tells how the controller's
// tries of egressName's configurations end (ccmEgressLog), or nil when it cannot tell them: the specs then check those
// tries by etags and events, and leave out what only the log tells.
func egressApplyLogReader(egressName string) *CCMClusterClient {
	return ccmEgressLog("how the controller's tries of the configurations of " + egressName + " ended")
}

func TestEgressTagList(t *testing.T) {
	if got, want := egressTagList(3), "e2e-t01=1,e2e-t02=1,e2e-t03=1"; got != want {
		t.Errorf("egressTagList(3) = %q; want %q", got, want)
	}
	if got := strings.Split(egressTagList(49), ","); len(got) != 49 || got[48] != "e2e-t49=1" || !slices.IsSorted(got) {
		t.Errorf("egressTagList(49) = %q; want 49 sorted tags up to e2e-t49=1", got)
	}
}

// expectEgressRejectionsLogged checks, where logs can read the controller's log (ccmLogLinesFor), how many times the
// controller logged from since on that it rejected egressName's configuration.
func expectEgressRejectionsLogged(logs *CCMClusterClient, egressName string, since time.Time, matcher types.GomegaMatcher) {
	rejections, ok := ccmLogLinesFor(logs, since, "the tries of the configurations of "+egressName, `"Rejected egress configuration"`,
		fmt.Sprintf("identity=%q", egressName))
	if !ok {
		return
	}
	Expect(len(rejections)).To(matcher, "how many times the controller tried and rejected the configuration of %s since %s UTC: %q",
		egressName, since.UTC().Format(time.TimeOnly), rejections)
}

// eventuallyEgressRejectionLogged waits, where logs can read the controller's log, until the last rejection of
// egressName's configuration the controller logged from since on contains every one of wants and none of unwanted.
// The log quotes the reason, so wants and unwanted must not contain quotes.
func eventuallyEgressRejectionLogged(logs *CCMClusterClient, egressName string, since time.Time, wants []string, unwanted ...string) {
	if logs == nil {
		return
	}
	Eventually(func() error {
		rejections, err := ccmRecentLogLines(logs, since, `"Rejected egress configuration"`, fmt.Sprintf("identity=%q", egressName))
		if err != nil {
			return err
		}
		if len(rejections) == 0 {
			return errors.New("no rejection logged yet")
		}
		last := rejections[len(rejections)-1]
		for _, want := range wants {
			if !strings.Contains(last, want) {
				return fmt.Errorf("the last rejection logged lacks %q: %s", want, last)
			}
		}
		for _, notWanted := range unwanted {
			if strings.Contains(last, notWanted) {
				return fmt.Errorf("the last rejection logged has %q: %s", notWanted, last)
			}
		}
		return nil
	}, egressRetryApplyTimeout, defaultPollInterval).Should(Succeed(), "the controller must try the configuration of %s again", egressName)
}

// createUserPublicIPElsewhere creates a StandardV2 IPv4 Public IP in rg in the first region of egressOtherRegions, other
// than location, that accepts one, which is deleted after the spec, and returns its ID and region; "" when none did.
func createUserPublicIPElsewhere(rg, name, location string) (id, region string) {
	for _, candidate := range egressOtherRegions {
		if strings.EqualFold(candidate, location) {
			continue
		}
		id, err := createUserPublicIP(rg, name, "StandardV2", candidate, nil)
		if err == nil {
			return id, candidate
		}
		utils.Logf("could not create a StandardV2 Public IP in %s: %v", candidate, err)
	}
	return "", ""
}

// createUserPublicIPFromPrefix creates a StandardV2 IPv4 Public IP in rg allocated from Public IP prefix prefixID, which
// is deleted after the spec, before the prefix, and returns its ID.
func createUserPublicIPFromPrefix(rg, name, prefixID string) string {
	id := publicIPID(rg, name)
	// Registered first, so a failed create is still cleaned up (deleting a missing resource succeeds).
	DeferCleanup(deleteBYOResource, id)
	putBYOResource(id, fmt.Sprintf(`{"location":%q,"sku":{"name":"StandardV2"},"properties":{"publicIPAllocationMethod":"Static",`+
		`"publicIPAddressVersion":"IPv4","publicIPPrefix":{"id":%q}}}`, clusterLocation(), prefixID))
	return id
}

// revertOutOfBand runs the reverts in undo, the last added first, after emptying the list: a revert that fails fails
// the spec that added it, and is not run again after the next spec.
func revertOutOfBand(undo *[]func()) {
	reverts := *undo
	*undo = nil
	for _, revert := range slices.Backward(reverts) {
		revert()
	}
}

// lockAzureResource puts a management lock of level ("CanNotDelete" or "ReadOnly") on resource id and returns the
// function that removes it, which the spec calls itself and which is also called after the spec. It skips the spec
// when the test identity may not create management locks, naming what is then not verified (unverified).
func lockAzureResource(id, level, unverified string) (unlock func()) {
	url := fmt.Sprintf("https://management.azure.com%s/providers/Microsoft.Authorization/locks/e2e-egress-%s?api-version=2016-09-01",
		id, strings.ToLower(level))
	out, err := runAz("rest", "--method", "put", "--url", url, "--body", fmt.Sprintf(`{"properties":{"level":%q,"notes":"cloud-provider-azure e2e"}}`, level))
	if err != nil && strings.Contains(string(out), "AuthorizationFailed") {
		Skip(fmt.Sprintf("the test identity may not lock resources (Microsoft.Authorization/locks/write), so these are not verified: %s (%s)",
			unverified, strings.TrimSpace(string(out))))
	}
	Expect(err).NotTo(HaveOccurred(), "locking %s: %s", id, string(out))
	unlocked := false
	unlock = func() {
		if unlocked {
			return
		}
		// A lock that is gone already, or with its resource, counts as removed (404, such as LockNotFound); one that could
		// not be removed is removed again by the next call.
		out, err := runAz("rest", "--method", "delete", "--url", url)
		if gone := strings.Contains(string(out), "NotFound") || strings.Contains(string(out), "Not Found"); err != nil && !gone {
			Expect(err).NotTo(HaveOccurred(), "unlocking %s: %s", id, string(out))
		}
		unlocked = true
	}
	DeferCleanup(unlock)
	return unlock
}

// skipUnlessPublicIPQuota skips the spec unless the subscription may create ipv4 more IPv4 and ipv6 more IPv6 Public IPs
// in the cluster's region: of any SKU (PublicIPAddresses), and of the Standard SKUs per IP version where the region
// reports those usages. Others that use the subscription may take what is left first (skipIfEgressQuotaReached).
func skipUnlessPublicIPQuota(ipv4, ipv6 int) {
	location := clusterLocation()
	out, err := runAz("network", "list-usages", "--subscription", subscriptionID, "--location", location,
		"--query", "[].[name.value, currentValue, limit]", "-o", "tsv")
	Expect(err).NotTo(HaveOccurred(), string(out))
	needed := map[string]int{"PublicIPAddresses": ipv4 + ipv6, "IPv4StandardSkuPublicIpAddresses": ipv4, "IPv6StandardSkuPublicIpAddresses": ipv6}
	found := false
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 3 || needed[fields[0]] == 0 {
			continue
		}
		used, err := strconv.Atoi(fields[1])
		Expect(err).NotTo(HaveOccurred(), "the usage %q", line)
		limit, err := strconv.Atoi(fields[2])
		Expect(err).NotTo(HaveOccurred(), "the usage %q", line)
		if limit-used < needed[fields[0]] {
			Skip(fmt.Sprintf("the subscription uses %d of its %d %s in %s; this spec needs %d more", used, limit, fields[0], location, needed[fields[0]]))
		}
		found = found || fields[0] == "PublicIPAddresses"
	}
	Expect(found).To(BeTrue(), "the Public IP usages of the subscription in %s: %q", location, string(out))
}

// skipIfEgressQuotaReached skips the spec when one of podNames was told that the configuration it votes for was rejected
// for a quota of the subscription (QuotaExceeded, ...CountLimitReached), which others that use it can exhaust.
func skipIfEgressQuotaReached(cs clientset.Interface, namespace string, podNames []string) {
	byPod, err := egressPodEvents(cs, namespace, consts.ServiceEgressGatewayReasonConfigRejected)
	if err != nil {
		return
	}
	for _, name := range podNames {
		for _, e := range byPod[name] {
			if strings.Contains(e.Message, "QuotaExceeded") || strings.Contains(e.Message, "CountLimitReached") {
				Skip(fmt.Sprintf("the subscription ran out of quota while the controller created the Public IPs: %s", e.Message))
			}
		}
	}
}
