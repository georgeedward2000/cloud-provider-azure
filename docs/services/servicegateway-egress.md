# ServiceGateway egress: NAT Gateway configuration by pod consensus

Pods labelled `kubernetes.azure.com/service-egress-gateway: <identity>` send their outbound traffic through a NAT
Gateway dedicated to that egress identity. With the annotations on this page, the pods of an identity configure that
NAT Gateway: its idle timeout, how many Public IPs the cloud controller manager (CCM) creates for it, a CCM-managed
Public IP prefix, your own Public IPs and prefixes, and tags. Every pod of the identity votes, and the CCM changes the
NAT Gateway, in place, only when all of them ask for the same configuration. A disagreement or a typo blocks the
change; it never stops egress.

Use it when an identity needs more outbound addresses (more SNAT ports), stable addresses that others allow-list (your
own Public IPs or prefixes), a longer idle timeout for long-lived idle connections, or tags for cost tracking.

## Egress identities in brief

- A pod joins the identity named by its label value, lower-cased. The value must be a valid Azure resource name, and
  `default-natgw` is reserved. Pods of any namespace can join the same identity.
- For the first pod, the CCM creates a StandardV2 NAT Gateway named `<identity>` in the cluster's node resource group,
  links it to the cluster's ServiceGateway and sends the pods' egress through it. After the last pod is gone, it deletes
  the NAT Gateway and the Public IPs and prefixes it created. Without annotations the NAT Gateway has an idle timeout of
  4 minutes and one Public IP per IP family: `<identity>-pip` and, on dual-stack clusters, `<identity>-pip-v6`.
- If the cluster's virtual network is in a resource group other than the node resource group and a NAT Gateway named
  `<identity>` exists there, the identity uses it instead (bring your own NAT Gateway, BYO). Its configuration is yours:
  the annotations below are not accepted there. The CCM links it to the ServiceGateway (the pod gets the Normal event
  `ServiceGatewayEgressNATGatewayLinked`) and, after the last pod is gone, unlinks it; it never deletes it or its Public
  IPs. The NAT Gateway is chosen when the identity is created: one created there later, or read access granted later,
  is used only once all the identity's pods are gone and it is created again.
  - One that cannot serve the identity (not StandardV2, no Public IP or prefix for a family the identity needs, linked to
    another Service Gateway or used by another service), or that the CCM may read but has no write access to (HTTP 403)
    to link it, gets the identity no NAT Gateway: the CCM reports the Warning `ServiceGatewayEgressNATGatewayRejected` on
    the pod and tries again until it is fixed or write access is granted. A lock that refuses the link has the same
    result without an event: the CCM tries again until the lock is removed.
  - If the CCM may not read that resource group, the identity gets a NAT Gateway the CCM manages, configured by the
    annotations, and the pod gets the Normal event `ServiceGatewayEgressNATGatewayUnreadable`.
  - If the CCM may not unlink it (no write access, or a lock), it leaves the link for you to remove; the pods are not
    held.
- A NAT Gateway named `<identity>` in the node resource group that the CCM did not create for the identity blocks it: the
  CCM reports the Warning `ServiceGatewayEgressNATGatewayRejected` on the pod and does not create the identity's NAT
  Gateway, retrying until that NAT Gateway is gone; if it already existed when the CCM started, removing it takes effect
  only after the CCM restarts.
- These `ServiceGatewayEgressNATGateway...` events go to the pod whose arrival made the running CCM create the
  identity, also for the retries of that creation, and nowhere else: an identity the CCM found or created at startup
  raises none of them, and an identity created again because pods joined while it was being deleted reports them, if at
  all, to the pod that made the running CCM create it before, which may be gone. Find them with
  `kubectl get events -A --field-selector reason=<reason>`.

## Quick start

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: payments
  namespace: payments
spec:
  replicas: 3
  selector:
    matchLabels:
      app: payments
  template:
    metadata:
      labels:
        app: payments
        kubernetes.azure.com/service-egress-gateway: payments
      annotations:
        kubernetes.azure.com/service-egress-gateway-idle-timeout: "10"
        kubernetes.azure.com/service-egress-gateway-public-ip-count: "2"
        kubernetes.azure.com/service-egress-gateway-tags: "team=payments,env=prod"
    spec:
      containers:
      - name: app
        image: <your-image>
```

Once the pods run, the NAT Gateway `payments` in the node resource group has an idle timeout of 10 minutes, the
CCM-managed IPv4 Public IPs `payments-pip` and `payments-pip-2` (StandardV2, static), plus `payments-pip-v6` on a
dual-stack cluster, and the tags `team=payments`, `env=prod` and `k8s-azure-egress-identity=payments`, also on its
Public IPs. The pods get the Normal event `EgressConfigApplied`: `Egress identity "payments" applied NAT Gateway
configuration: idle timeout 10m; IPv4: 2 managed IPs; tags: env=prod,team=payments.` (on a dual-stack cluster,
`IPv6: 1 managed IP; ` comes before the tags).

```sh
kubectl get events -n payments --field-selector reason=EgressConfigApplied
az network nat gateway show --resource-group <node-resource-group> --name payments
```

To change the configuration, change the annotations in the pod template and roll out (or `kubectl annotate` the pods):
the NAT Gateway is updated in place once every pod of the identity has the new values.

## Annotation reference

All keys start with `kubernetes.azure.com/service-egress-gateway-`. Values are strings (quote numbers in YAML); spaces
around values and list entries are ignored.

| Key suffix | Meaning | Values | Default |
|---|---|---|---|
| `idle-timeout` | TCP idle timeout of the NAT Gateway, in minutes | 4 to 120 | 4 |
| `public-ip-count` | CCM-managed IPv4 Public IPs | 0 to 16 | 1; 0 when another IPv4 address source is set |
| `public-ip-count-ipv6` | CCM-managed IPv6 Public IPs | 0 to 16 | 1 on identities that serve IPv6; 0 when another IPv6 address source is set |
| `public-ip-prefix-length` | one CCM-managed IPv4 Public IP prefix of this length | 28 to 31 | none |
| `public-ip-prefix-length-ipv6` | one CCM-managed IPv6 Public IP prefix of this length | 124 to 127 | none |
| `public-ip-ids` | your IPv4 Public IPs | comma-separated resource IDs | none |
| `public-ip-ids-ipv6` | your IPv6 Public IPs | comma-separated resource IDs | none |
| `public-ip-prefix-ids` | your IPv4 Public IP prefixes | comma-separated resource IDs | none |
| `public-ip-prefix-ids-ipv6` | your IPv6 Public IP prefixes | comma-separated resource IDs | none |
| `tags` | tags added to the NAT Gateway and the CCM-managed Public IPs and prefixes | `key=value` pairs, comma-separated, at most 49 | none |

These rules are checked on the pod; a pod that breaks one casts an invalid vote:

- **Known keys only.** Any other key with the prefix is invalid (so a typo is reported), and so is an empty value.
- **IPv6 keys** need an identity whose NAT Gateway serves IPv6, that is one created on a dual-stack cluster. A vote is
  checked again for the IP families its identity is re-created with, for example after the cluster gained IPv6.
- **Mixing address sources.** Per IP family you can combine CCM-managed Public IPs, one CCM-managed prefix, your Public
  IPs and your prefixes. When a family has any of the last three, its managed Public IP count defaults to 0; set the
  count explicitly to keep managed Public IPs as well.
- **1 to 16 addresses per family.** For each family the identity serves, managed Public IPs + managed prefix size + your
  Public IPs + your prefix sizes must be from 1 to 16. A /31, /30, /29 or /28 prefix holds 2, 4, 8 or 16 addresses
  (IPv6: /127, /126, /125 or /124). On the pod each of your prefixes counts as 2, and so does one the CCM cannot read
  yet (missing or access denied, which is retried); its real size is checked before the configuration is applied.
- **Your resource IDs** must be `Microsoft.Network/publicIPAddresses` or `Microsoft.Network/publicIPPrefixes` IDs in the
  cluster's subscription (the network resource subscription when `networkResourceSubscriptionID` is configured) and
  outside the node resource group. Each ID may appear only once across all keys.
- **Tags:** at most 49 (Azure accepts 50 per resource and the CCM adds its ownership tag). Keys are case-insensitive
  and unique, at most 512 characters, without any of `<>%&\?/`. Values are at most 256 characters, without `=` or `,`,
  and not `null` in any case. Reserved keys, in any case: `k8s-azure-egress-identity`, `k8s-azure-cluster-name`, `service`,
  `kubernetes-cluster-name`, `k8s-azure-service`, `k8s-azure-dns-label-service`, `kubernetes-dns-label-service`.
- **Tags are only added or updated.** A tag removed from the annotation stays on the resources, tags set by others are
  kept, and your own Public IPs and prefixes are never tagged.

## How the configuration is decided

- **Every pod votes.** The members of an identity are its labelled pods, in any namespace, that are Running or Pending,
  have an IP address and are not terminating. A pod without annotations votes for the default (idle timeout 4, one
  CCM-managed Public IP per family). Votes are compared after normalization: `"010"` equals `"10"`, lists may be in
  any order, resource IDs are case-insensitive, and a default value set explicitly equals no annotation. Tag keys and
  values must have the same case on every pod (`team=a` and `Team=a` disagree); their order and spaces do not matter.
- **Agreement is applied at once, in place.** When every member votes for the same configuration and it differs from the
  applied one, the CCM updates the existing NAT Gateway immediately; egress continues during the update.
- **Disagreement keeps the applied configuration.** If agreement is lost before a pending change starts, the change is
  cancelled; a change already started completes.
- **Invalid votes block.** A pod with an invalid annotation keeps its egress, but the identity's configuration cannot
  change until the annotation is fixed or the pod is gone.
- **A new identity takes the first pod's configuration.** Its NAT Gateway is created with the configuration all members
  agree on; without agreement, with the vote of the pod that created the identity or, when that pod is gone, the CCM
  restarted or the identity is created again after its deletion, of the oldest member (by creation time, then by
  `<namespace>/<name>` in byte order). An invalid vote there gives the default.
- **A rejected configuration leaves the NAT Gateway as it is**; a new identity is created with the default instead (if
  Azure refuses the default too, for example for a quota, the identity gets no NAT Gateway until a retry of its creation
  succeeds).
  Problems you can fix without changing the configuration (a resource that is missing, inaccessible, not provisioned or
  in use, a quota, a name collision) are retried automatically while the pods keep agreeing on it. Other rejections are
  final until the pods agree on another configuration: agree on a corrected one. Agreeing on the rejected one again
  after that tries it once more, as does a CCM restart. A configuration is reported rejected once while it is
  retried: if the reason changes, no new event is raised, unless a retry rejects it for good, which is reported once
  more with that final reason. If it is still not applied after you fixed the reported problem, check its resources
  against the other rejection reasons below (SKU, region, IP version, size) and agree on a corrected configuration.
- **Scaling down is allowed.** Fewer Public IPs, a smaller prefix or fewer of your resources are applied like any other
  change: the CCM detaches what is no longer used and deletes the managed resources among it. Connections that use a
  removed address are dropped.
- **BYO NAT Gateways accept no configuration.** There, any of these annotations, even one set to its default value, is
  reported invalid and ignored; egress continues.

## What the CCM creates and owns

All in the node resource group, with the StandardV2 SKU and the ownership tag `k8s-azure-egress-identity=<identity>`:

| Resource | Name |
|---|---|
| NAT Gateway | `<identity>` |
| IPv4 Public IPs | `<identity>-pip`, `<identity>-pip-2` ... `<identity>-pip-16` |
| IPv6 Public IPs | `<identity>-pip-v6`, `<identity>-pip-v6-2` ... `<identity>-pip-v6-16` |
| IPv4 Public IP prefix | `<identity>-prefix-<length>`, length 28 to 31 |
| IPv6 Public IP prefix | `<identity>-prefix-v6-<length>`, length 124 to 127 |

- These names are reserved for the identity. The CCM creates, updates and deletes the resources as the configuration
  requires, and deletes them all with the identity. A new prefix length creates the new prefix, swaps it in place and
  deletes the old one.
- `<identity>-pip` and `<identity>-pip-v6` belong to the CCM by name, as before this feature; every other reserved name
  only with the ownership tag. A resource at such a name without the tag is never modified or deleted, and a
  configuration that needs the name is rejected (and retried while the pods agree on it) until that resource is gone.
- Do not change these resources yourself. Changes are not watched: the CCM corrects them at its next start or the next
  time it applies a configuration, provided the pods agree on one. Tags you add are kept.
- A managed Public IP or prefix you attached to another resource (a NAT Gateway, network interface or load balancer, or
  Public IPs allocated from the prefix) is left in place when no longer needed: detach and delete it yourself. While
  Azure still reports it attached to the identity's own NAT Gateway, the CCM retries its deletion.

Your own Public IPs and prefixes are only referenced: the CCM never modifies, tags or deletes them. When the identity is
deleted or stops listing them, they are detached and left as they are. Before changing the NAT Gateway, the CCM checks
that each of them has the StandardV2 SKU, is in the cluster's region, has the IP version of the key it is listed in, is
provisioned, and is not used elsewhere (another NAT Gateway, a network interface, a load balancer; for a prefix, also
Public IPs allocated from it). The identity the cloud provider uses must be able to read them and attach them, for
example with Network Contributor on their resource group.

## Events and troubleshooting

The CCM reports on the pods, once per change. At startup it reports none of the votes it finds, even those cast or changed while it was down (no `InvalidEgressConfig` or `EgressConfigConflict`), nor a rejection of the configuration the pods of an existing identity agree on; restart the pods (for example
`kubectl rollout restart`) to be told. It does report what it does at startup: a configuration it writes (`EgressConfigApplied`, as in the table below) and, for an identity it creates, the configuration it creates it with, also when its NAT Gateway already had it (`EgressConfigApplied`, unless it is the default), the rejection of the configuration it was asked for (`EgressConfigRejected`, to the pods that vote for it) and, to pods with annotations, that it uses a BYO NAT Gateway (`InvalidEgressConfig`).

| Reason | Type | When |
|---|---|---|
| `InvalidEgressConfig` | Warning | the pod joins with invalid annotations or an edit makes them invalid for another reason, or it has annotations and its identity uses a BYO NAT Gateway |
| `EgressConfigConflict` | Warning | the pod joins or changes its vote while the members disagree, and its vote is not the applied configuration |
| `EgressConfigApplied` | Normal | the identity's NAT Gateway got a new configuration (to every member; not for a creation with the default), also one that a write Azure did not answer in time applied, once the CCM finds it applied |
| `EgressConfigRejected` | Warning | the configuration the pod votes for is rejected, or the pod starts voting for a rejected configuration; the message says whether it will be retried, and a retried configuration that a retry rejects for good is reported once more |

```sh
kubectl describe pod <pod> -n <namespace>
kubectl get events -A --field-selector reason=EgressConfigRejected
```

| The message contains | What to do |
|---|---|
| `unknown annotation`, `is not an integer from` | fix the key or the value |
| `this egress identity does not use IPv6` | remove the IPv6 keys: the identity serves IPv4 only |
| `no public address`, `public addresses, more than` | give each family 1 to 16 addresses |
| `is not in subscription`, `is in the node resource group` | use a resource in the cluster's subscription, outside the node resource group |
| `is reserved for the cluster`, `tags, more than` | use other tag keys or fewer tags (count the tags the resource already has) |
| `has SKU`, `is in location`, `but it is listed for` | use a StandardV2 resource in the cluster's region, under the key of its IP version |
| `does not exist`, `has provisioning state`, `access denied`, `is attached to`, `is in use by` | create the resource or wait until it is provisioned (re-create it if it failed), grant the CCM access or detach it; retried automatically while the pods agree on the configuration |
| `exists without the ownership tag` | delete your resource that uses a reserved name; retried automatically while the pods agree on the configuration |
| `Azure refused` | follow Azure's error: quotas (`QuotaExceeded`, `...CountLimitReached`) and HTTP 403 or 404 are retried automatically while the pods agree on the configuration |
| reason `ServiceGatewayInvalidEgressLabel` or `ServiceGatewayReservedEgressLabel` | fix the label value |

## Operational notes

- **Rollouts.** A rollout that changes the annotations is applied once every member has the new values. While old and
  new pods run side by side, the configuration is kept and the new pods report `EgressConfigConflict`, as expected.
  Terminating pods no longer vote, so the change applies once the last old pod starts terminating. Pods of other
  workloads that share the identity must change too.
- **CCM restarts.** Nothing is written when the NAT Gateway already matches what the pods agree on; an agreement reached
  while the CCM was down is applied at startup. When Azure does not answer a write clearly (a timeout, a server error, a
  failed status poll, or throttling of a NAT Gateway, Public IP or prefix write), the CCM leaves that identity's NAT Gateway alone for 2 minutes, then reads
  Azure again and finishes or cleans up; a configuration that write applied is then reported (`EgressConfigApplied`),
  also when the pods no longer agree on it. A throttled ServiceGateway write was not applied and is retried after the usual
  backoff.
- **Upgrade.** Existing identities whose pods have no annotations are not touched: those pods vote for what they
  already have, and an identity keeps the IP families its NAT Gateway was created with. Annotations set before the
  upgrade are applied at startup, like any agreement reached while the CCM was down.
- **Shared NAT Gateway.** The CCM never changes, links, registers or deletes an identity's NAT Gateway while another
  ServiceGateway service, such as the default outbound service, uses it. For an identity that exists, a change is retried
  after a backoff and reports no event; when its last pod is gone, the CCM only unregisters the identity and leaves that
  NAT Gateway and its Public IPs and prefixes in place. An identity being created gets no NAT Gateway, its pod gets the
  Warning `ServiceGatewayEgressNATGatewayRejected`, and the CCM tries again.
- **Rollback.** A CCM without this feature ignores the annotations and leaves the NAT Gateways as they are. When it
  deletes an identity, it deletes only the NAT Gateway, `<identity>-pip` and `<identity>-pip-v6`; further managed Public
  IPs and prefixes remain, tagged `k8s-azure-egress-identity`, until a CCM with this feature removes them, or you do.
- **IPv6** needs a dual-stack cluster and applies only to identities created while the cluster has IPv6 nodes. It is
  verified at the Azure resource level (the NAT Gateway, Public IPs and prefixes are configured as requested); IPv6
  egress traffic through them has not been verified end to end.

### Security: voting is cluster-wide

Any pod in any namespace that carries the label of an identity is a member and votes. One pod can therefore block every
configuration change of an identity, and the first pod of a new identity chooses its configuration, up to 16 IPv4 and
16 IPv6 addresses (managed Public IPs, a managed prefix or both), which costs money and quota. Restrict who may use the
label and the annotations, for example with an admission policy and RBAC on who may create pods.

The policy below is **an example only; review and adapt it before use** (Kubernetes 1.30 or later). Pods may use only
identities named after their namespace (`<namespace>` or `<namespace>-...`), and the annotations only in namespaces
labelled `egress.example.com/configure: "true"`. Updates that change no label or annotation, such as the CCM's
finalizer updates, are not checked, so existing pods can always terminate.

```yaml
apiVersion: admissionregistration.k8s.io/v1
kind: ValidatingAdmissionPolicy
metadata: {name: service-egress-gateway}
spec:
  failurePolicy: Fail
  matchConstraints:
    resourceRules:
    - {apiGroups: [""], apiVersions: ["v1"], operations: ["CREATE", "UPDATE"], resources: ["pods"]}
  matchConditions:
  - name: labels-or-annotations-changed
    expression: >-
      request.operation == 'CREATE' ||
      object.metadata.?labels.orValue({}) != oldObject.metadata.?labels.orValue({}) ||
      object.metadata.?annotations.orValue({}) != oldObject.metadata.?annotations.orValue({})
  variables:
  - name: identity
    expression: "object.metadata.?labels[?'kubernetes.azure.com/service-egress-gateway'].orValue('').lowerAscii()"
  - name: ns
    expression: "namespaceObject.metadata.name"
  validations:
  - expression: "variables.identity in ['', variables.ns] || variables.identity.startsWith(variables.ns + '-')"
    message: "the egress identity must be the namespace name or start with '<namespace>-'"
  - expression: >-
      !object.metadata.?annotations.orValue({}).exists(k, k.startsWith('kubernetes.azure.com/service-egress-gateway-')) ||
      namespaceObject.metadata.?labels[?'egress.example.com/configure'].orValue('') == 'true'
    message: "egress NAT Gateway annotations need the namespace label egress.example.com/configure=true"
---
apiVersion: admissionregistration.k8s.io/v1
kind: ValidatingAdmissionPolicyBinding
metadata: {name: service-egress-gateway}
spec:
  policyName: service-egress-gateway
  validationActions: ["Deny"]
```
