# Go 1.27 and dependency security updates (2026-11-06)

Moves the workspace to Go 1.27.3 and clears every reachable `govulncheck`
finding and every `npm audit` / `yarn audit` finding that has a patched release.
Dependabot alerts are disabled on the repository, so local scanners were the
source of truth.

## Go toolchain: 1.26 → 1.27.3

Go 1.27.4 was requested but is not released yet (go.dev and the `golang` Docker
image both stop at 1.27.3), so the bump targets 1.27.3, the latest patch.

- `go.work` and all five `go.mod` files: `go 1.26.0` → `go 1.27.3`. CI reads
  the version from `go.work` (`go-version-file`), so no workflow change.
- All four Dockerfiles: `golang:1.26-alpine` → `golang:1.27-alpine`.
- `govulncheck` must be rebuilt with Go 1.27 (`go install
  golang.org/x/vuln/cmd/govulncheck@latest`), otherwise it refuses to load
  packages that declare `go 1.27`.

On 1.27.3 `govulncheck` reports no standard-library vulnerabilities.

## Go modules

Before the update, `govulncheck` reported 8 reachable vulnerabilities across
three modules:

| ID | Module | Fixed in |
|---|---|---|
| GO-2026-6603, -6610, -6611, -6612, -6617 | `golang.org/x/net` v0.57.0 | v0.60.0 |
| GO-2026-6505 | `go.opentelemetry.io/otel/sdk` + `otlptrace` v1.44.0 | v1.45.0 |
| GO-2026-6061, GO-2026-6348 | `google.golang.org/grpc` v1.81.1 | v1.83.1 |

All direct dependencies were upgraded with `go get -u -t ./...` per module
(`-t` matters: without it, test-only deps like testcontainers and testify stay
put), followed by `go mod tidy` and `go work sync`. Notable bumps:

| Package | Before | After |
|---|---|---|
| `golang.org/x/net` | 0.57.0 | 0.62.0 |
| `golang.org/x/crypto` | 0.54.0 | 0.58.0 |
| `google.golang.org/grpc` | 1.81.1 | 1.85.0 |
| `go.opentelemetry.io/otel/*` | 1.44.0 | 1.47.0 |
| `k8s.io/{api,apimachinery,client-go}` | 0.36.3 | 0.37.2 |
| `k8s.io/apiextensions-apiserver` | 0.36.3 | 0.37.2 |
| `sigs.k8s.io/controller-runtime` | 0.24.1 | 0.25.2 |
| `github.com/nats-io/nats.go` | 1.52.0 | 1.55.0 |
| `github.com/moby/moby/client` | 0.5.1 | 0.6.2 |
| `github.com/moby/moby/api` | 1.55.0 | 1.56.1 |
| `github.com/testcontainers/testcontainers-go` | 0.44.0 | 0.45.0 |
| `github.com/prometheus/client_golang` | 1.24.1 | 1.25.0 |
| `github.com/stretchr/testify` | 1.11.1 | 1.12.1 |

`k8s.io/kube-openapi` is held at `v0.0.0-20260721132016-d427ff9ee9ad`, the
version k8s 0.37.2 itself requires. `go get -u` pulls a newer pseudo-version
that has moved to `structured-merge-diff/v7`, while apimachinery 0.37.2 still
uses v6, and the build fails in `managedfields/internal`. Let it float again
once a k8s release moves to smd v7.

The CRD and deepcopy output regenerate with no diff under controller-runtime
0.25, and the envtest suite runs against Kubernetes 1.37.

After the update `govulncheck` reports 0 vulnerabilities in every module.

## npm: `HadesAPI/web`

`npm audit` reported 14 issues (2 critical, 4 high, 8 moderate), now 0.

- `npm audit fix` + `npm update` within existing ranges (pulls `vite` 6.4.5,
  `source-map-js` 1.2.2, among others).
- `vitest` 3.2.7 → 5.1.0 (major). This removes the critical `tinypool`
  prototype-pollution RCE (GHSA-5gmw-xhrv-c9v3, GHSA-85c8-ppgw-ccpr) and the
  `@vitest/mocker` advisories. No test or config changes were needed.
- `overrides` pins `minimatch` to `^11.0.1` under
  `@typescript-eslint/typescript-estree` (GHSA-g3hv-75wg-m249). The latest
  typescript-eslint still declares `^10.2.2` and no patched 10.x exists. This is
  lint-time only. Drop the override once typescript-eslint moves to minimatch 11.

## yarn: `website`

`yarn audit` reported 326 issues (17 critical, 146 high), now 1 low.

- Refreshed `yarn.lock` within the existing ranges (`yarn upgrade`). Almost
  every finding was a stale transitive version under Docusaurus 3.9.2.
- Added a `tinypool: ^2.1.2` resolution. `@docusaurus/core` 3.9.2 declares
  tinypool ^1, which has no patched release for the same RCE advisories as
  above. The SSG step builds cleanly with tinypool 2.2.0.

Docusaurus stays on 3.9.2, following the earlier decision in
[dependabot-security-updates.md](dependabot-security-updates.md): 3.10 makes
`future.v4` imply the rspack-based bundler (`@docusaurus/faster`) and drops MDX1
comment compatibility, so the helm-docs output in `helm-values.md` stops
compiling. That is a bundler migration, not a security fix.

## Not fixed

| Package | Severity | Why |
|---|---|---|
| `deepmerge` 4.2.0-4.3.1 (website) | low | No patched release exists. Reached only via `@svgr/plugin-svgo` while bundling SVGs committed to this repo at docs build time. |

## Verification

- `make build`, `make lint`, `gofmt` - pass.
- `go test ./...` in every module (Docker-backed testcontainers suites
  included) and `make -C HadesScheduler/HadesOperator manifests generate fmt vet test` - pass.
- `make vuln` - 0 vulnerabilities in every module.
- Dashboard: `npm run lint`, `npm test` (33 tests), `npm run build` - pass.
- Website: `yarn typecheck`, `yarn build` - pass.
