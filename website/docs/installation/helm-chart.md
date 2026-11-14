---
sidebar_position: 2
---

# Kubernetes via Helm (Production)

The recommended way to deploy Hades in production is with **Helm**. The chart bundles the API, Scheduler, Operator, and NATS broker, and defaults to the **Hades Operator** executor - the modern, Kubernetes-native execution mode.

## Prerequisites

- Kubernetes **v1.25+**
- [Helm](https://helm.sh/docs/intro/install/) **v3.12+**
- `kubectl` configured to point to your target cluster

## Install

### Option A - Published chart from GHCR (recommended)

The chart is published to GHCR as an OCI artifact. In every command below, replace
`<version>` with the chart version you want to install (for example `1.0.0`; see the
[available versions](https://github.com/Hades-Scheduler/hades/pkgs/container/charts%2Fhades)):

Create the API auth Secret first. `/build` then needs Basic Auth with user `hades` and that key. For throwaway test installs only, skip the Secret and pass `--set hadesApi.allowUnauthenticated=true` instead of `--set hadesApi.authKeySecret=hades-auth` (a set `authKeySecret` always wins).

```bash
kubectl create namespace hades
kubectl -n hades create secret generic hades-auth --from-literal=AUTH_KEY=<choose-a-key>
```

```bash
helm upgrade --install hades oci://ghcr.io/hades-scheduler/charts/hades \
  --version <version> -n hades --create-namespace \
  --set hadesApi.authKeySecret=hades-auth
```

### Option B - From a local checkout

Create the `hades-auth` Secret first, as shown in Option A, or use the unauthenticated test setting described there.

```bash
# 1. Add the NATS sub-chart dependency
helm repo add nats https://nats-io.github.io/k8s/helm/charts
helm dependency build ./helm/hades/

# 2. Install (default: operator mode)
helm upgrade --install hades ./helm/hades -n hades --create-namespace --set hadesApi.authKeySecret=hades-auth
```

Override values inline as needed - at minimum set your ingress host:

```bash
helm upgrade --install hades oci://ghcr.io/hades-scheduler/charts/hades --version <version> \
  -n hades --create-namespace \
  --set hadesApi.authKeySecret=hades-auth \
  --set ingress.host=hades.example.com \
  --set ingress.tls.secretName=my-tls-secret
```

:::tip Release name vs namespace
The first `hades` is the Helm **release name** (can be anything). The `-n hades` flag sets the **namespace**, created automatically with `--create-namespace`.
:::

### Verify Connectivity

Tail the Scheduler logs to confirm it connected to NATS and started in operator mode:

```bash
kubectl -n hades logs deploy/hades-scheduler -f
```

Expected healthy output:

```text
INFO Connected to NATS server url=nats://hades-nats.hades.svc:4222
INFO Started HadesScheduler in Kubernetes mode
INFO Using operator mode (dynamic client)
```

:::warning CRDs are not upgraded by Helm
Helm applies CRDs only on first install. When a release changes the `BuildJob` CRD, apply it manually from the matching chart version:

```bash
helm show crds oci://ghcr.io/hades-scheduler/charts/hades --version <version> | kubectl apply -f -
```
:::

## Upgrade & Rollback

The command below needs the `hades-auth` Secret to exist. If your release already uses a different Secret name, pass that name instead.

```bash
helm upgrade hades oci://ghcr.io/hades-scheduler/charts/hades --version <version> -n hades \
  --set hadesApi.authKeySecret=hades-auth

# Roll back to a previous revision
helm history hades -n hades
helm rollback hades <revision> -n hades
```

## Uninstall

```bash
helm uninstall hades -n hades
# Optionally remove the namespace and any leftover resources
kubectl delete namespace hades
```

## Next Steps

- Configuration: see the [Helm Chart Reference](../deployment/helm) and the generated [values table](../deployment/helm-values).
- Learn about the [Hades Operator execution mode](../operation-modes/k8s-operator).
- Expose the API with TLS using [Traefik](../deployment/traefik) (VM deployments).
