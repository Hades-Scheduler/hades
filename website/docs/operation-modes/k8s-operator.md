---
sidebar_position: 3
---

# Hades Operator *(Recommended)*

The **Hades Operator** is the production-grade execution mode for Kubernetes. It implements the [Kubernetes Operator pattern](https://kubernetes.io/docs/concepts/extend-kubernetes/operator/) using **Custom Resource Definitions (CRDs)**, giving Hades deep, native integration with the Kubernetes control plane.

## Why the Operator?

| Feature | Docker Executor | K8s Executor *(deprecated)* | Hades Operator |
|---|---|---|---|
| Kubernetes native | ❌ | Partial | ✅ |
| CRD-based job tracking | ❌ | ❌ | ✅ |
| Reconciliation loop | ❌ | ❌ | ✅ |
| Fine-grained RBAC | ❌ | Limited | ✅ |
| Helm chart support | ✅ | ❌ | ✅ |

## Architecture

The Operator introduces a `BuildJob` Custom Resource Definition (CRD). When the Scheduler receives a job (in `operator` mode), it creates a `BuildJob` resource instead of a Pod. The Operator's controller loop watches for `BuildJob` resources and:

1. Creates a Kubernetes `Job` with one init-container per step plus a finalizer container.
2. Monitors Pod status and updates the `BuildJob` status accordingly. A pod wedged in a
   terminal waiting state (e.g. `ImagePullBackOff` on a bad step image) is failed with the
   reason recorded in the status and surfaced in the dashboard, and its `Job` is deleted so
   it stops retrying.
3. Publishes status transitions and logs back to NATS.
4. Cleans up completed resources (`DeleteOnComplete`, default `true`).

```
NATS ──▶ Scheduler ──▶ BuildJob (CRD) ──▶ Operator Controller ──▶ Job / Pods
                                                │
                                                ▼
                                        Status & Logs ──▶ NATS
```

## Configuration

Operator mode is the **default** when deploying with the Helm chart. The scheduler is configured with:

```yaml
# helm/hades/values.yaml
hadesScheduler:
  executor: k8s
```

The scheduler uses a dynamic Kubernetes client to create `BuildJob` custom resources; the Operator does the rest. Authentication uses the pod's in-cluster `ServiceAccount` - no `kubeconfig` file is needed.

## Job pod hardening

Job pods do not mount a service account token, use the `RuntimeDefault` seccomp profile and set `allowPrivilegeEscalation: false`. They still run as the image's user. The API and the log manager have their own service accounts, and the operator role has no access to secrets or configmaps.

Set `networkPolicy.enabled: true` (default `false`) to add two NetworkPolicies: job pods cannot reach the release namespace, and only Hades components reach NATS on 4222. Before enabling, check the NATS pod labels. On CNIs that match pod IPs against `ipBlock`, set `networkPolicy.jobPods.blockedCIDRs` to the pod CIDR.

## RBAC

The Helm chart automatically creates the `ServiceAccount`, `Role`, and `RoleBinding` that grant the scheduler and operator the minimum permissions required to manage `BuildJob` resources and Jobs within the release namespace.

For cluster-wide access (e.g. to schedule jobs across multiple namespaces), set:

```yaml
hadesOperator:
  clusterWide: true
```

This switches from a `Role`/`RoleBinding` to a `ClusterRole`/`ClusterRoleBinding`.

## CRD Maintenance

The `BuildJob` CRD is defined in Go at `HadesScheduler/HadesOperator/api/v1/buildjob_types.go`. Whenever this file changes, regenerate the generated artifacts:

```bash
make -C HadesScheduler/HadesOperator manifests generate
```

This updates `zz_generated.deepcopy.go` and `helm/hades/crds/build.hades.tum.de_buildjobs.yaml`. A CI workflow (`verify-crd`) fails the build if the committed generated files are out of sync with the Go source.

:::warning CRDs are not upgraded by Helm
Helm does not upgrade CRDs after the first install. When a release changes the `BuildJob` CRD, apply it manually from the matching chart version:

```bash
helm show crds oci://ghcr.io/hades-scheduler/charts/hades --version <version> | kubectl apply -f -
```
:::

## Submitting a Test Job

Apply the sample `BuildJob` manifest directly:

```bash
kubectl apply -n hades -f ./HadesScheduler/HadesOperator/config/samples/build_v1_buildjob.yaml
```

Monitor its progress:

```bash
kubectl -n hades get buildjobs
kubectl -n hades describe buildjob <name>
```
