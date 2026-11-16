---
sidebar_position: 1
---

# Helm Chart Reference

This page covers the Hades Helm chart: what it deploys, how to configure it, and day-2 operations. For the full, always-current list of values, see the generated **[Values Reference](./helm-values)**.

## Chart Overview

The Hades Helm chart bundles:

| Component | Description |
|---|---|
| **hades-api** | Processes and validates job requests; publishes build events to NATS |
| **hades-scheduler** | Consumes NATS events and, in operator mode, creates `BuildJob` custom resources |
| **hades-operator** | Watches `BuildJob` CRs and reconciles them into Kubernetes `Job`s |
| **hades-log-manager** | Aggregates per-job build logs from NATS |
| **hades-nats** | Embedded [NATS JetStream](https://nats.io) message broker (sub-chart) |

The scheduler does not create Pods directly; it creates `BuildJob` CRs that the operator reconciles into Kubernetes Jobs.

## Prerequisites

- Kubernetes **v1.25+**
- Helm **v3.12+**
- `kubectl` configured to point to your target cluster

## Installation

See the [Helm installation guide](../installation/helm-chart) for the recommended GHCR (OCI) install and a local-checkout install.

## Values Reference

All user-configurable options live in `values.yaml`. The complete table - one row per value, with type, default, and description - is generated from the chart's `values.yaml` comments by [helm-docs](https://github.com/norwoodj/helm-docs) and published on the **[Values Reference](./helm-values)** page. Regenerate it with `make docs-helm`.

Common overrides:

```yaml
ingress:
  host: hades.example.com
  tls:
    enabled: true
    secretName: hades-tls

hadesOperator:
  clusterWide: false     # true for cross-namespace scheduling
  maxParallelism: "100"
```

## Cluster-Wide Access

By default the Operator is scoped to the release namespace. To schedule jobs across multiple namespaces:

```bash
helm upgrade hades oci://ghcr.io/hades-scheduler/charts/hades --version <version> -n hades \
  --set hadesApi.authKeySecret=hades-auth \
  --set hadesOperator.clusterWide=true
```

This switches the `Role`/`RoleBinding` to a `ClusterRole`/`ClusterRoleBinding`.

## Upgrade & Rollback

```bash
helm upgrade hades oci://ghcr.io/hades-scheduler/charts/hades --version <version> -n hades \
  --set hadesApi.authKeySecret=hades-auth

helm history hades -n hades
helm rollback hades <revision> -n hades
```

## Uninstall

```bash
helm uninstall hades -n hades
kubectl delete namespace hades
```

## Development Utilities

```bash
helm lint ./helm/hades
helm template hades ./helm/hades -n hades --set hadesApi.authKeySecret=hades-auth
make docs-helm          # regenerate the values table
```

## CRD Maintenance

The `BuildJob` CRD is generated from Go. After editing `HadesScheduler/HadesOperator/api/v1/buildjob_types.go`:

```bash
make -C HadesScheduler/HadesOperator manifests generate
```

This updates the deep-copy helper code and `helm/hades/crds/build.hades.tum.de_buildjobs.yaml`. A CI check (`verify-crd`) enforces that the committed generated files stay in sync.
