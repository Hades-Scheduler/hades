# Contributing to Hades

Thanks for contributing! This guide covers the Hades-specific workflow. For the architecture and internal layout, read [AGENTS.md](./AGENTS.md); for docs orientation, see [docs/README.md](./docs/README.md).

## Development setup

Hades is a Go workspace (`go.work`, Go 1.27) with five modules. The top-level [`Makefile`](./Makefile) wraps every common task (`make help` lists them).

```bash
# Run API + scheduler + log manager locally (auto-starts NATS in Docker)
make run

# Build and test the whole workspace
make build
make test
```

Docker must be running: some tests (e.g. `HadesAPI/router_test.go`) spin up NATS via testcontainers.

## Before opening a pull request

1. **Format and lint:** `make fmt` and `make lint`.
2. **Test:** `make test` (or `make ci` to mirror CI: lint + test). Operator changes: `make test-operator`.
3. **Regenerate derived docs when relevant:**
   - Changed an HTTP handler annotation or a request/response DTO → `make docs-api`.
   - Changed Helm chart values → `make docs-helm`.
4. **Regenerate the CRD when you change `BuildJobSpec`:** if you edit
   `HadesScheduler/HadesOperator/api/v1/buildjob_types.go`, run
   `make -C HadesScheduler/HadesOperator manifests generate` and commit the
   updated `helm/hades/crds/build.hades.tum.de_buildjobs.yaml` and
   `zz_generated.deepcopy.go`. The `verify-crd` GitHub workflow fails otherwise.
   The `BuildJobSpec` is intentionally duplicated from `shared/payload`; keep the
   two in sync manually.
5. **Document the change:** update the relevant README/docs alongside the code
   (see the documentation-discipline note below).
6. Use the [pull request template](./.github/pull_request_template.md).

## Conventions

- **Logging:** `log/slog` everywhere (`DEBUG=true` for debug level); the operator uses controller-runtime's `zap` logger.
- **Config:** each binary has its own `Config` struct loaded via `utils.LoadConfig` (`caarlos0/env` + `joho/godotenv`). Document new variables in [docs/configuration.md](./docs/configuration.md).
- **Errors:** wrap and return (`fmt.Errorf("...: %w", err)`); avoid `log.Fatal` in libraries.
- **Dependencies:** don't introduce package-level mutable globals for dependencies; pass them in (see `setupRouter`).
- Follow the standard Go style; document exported types and functions.

## Dependency security

Dependabot watches the Go modules, `HadesAPI/web/package-lock.json`, and
`website/yarn.lock`. To reproduce its findings locally:

- Go: `make vuln` (govulncheck, per module).
- Dashboard: `cd HadesAPI/web && npm audit`.
- Website: `cd website && yarn audit`.

Two constraints are worth knowing before bumping a version:

- **Docker API client:** use `github.com/moby/moby/client` and
  `github.com/moby/moby/api`. The old `github.com/docker/docker` module path is
  frozen at v28.5.2 - Docker Engine 29+ ships under the `moby/moby` module - so
  advisories against it can only be resolved by staying on the new path.
  `testcontainers-go` uses the same modules.
- **`website` resolutions:** the `resolutions` block in `website/package.json`
  forces patched transitive dependencies that Docusaurus 3.9 still requests at
  vulnerable ranges. `webpack` and `webpackbar` must move together: Docusaurus
  3.9 bundles webpackbar 6, which passes options that webpack >= 5.98 rejects,
  so the webpackbar 7 resolution is what makes the webpack bump possible.

## Documenting changes

Keep documentation in step with code: when you add or change behavior, update the
corresponding README, `docs/` page, or generated reference in the **same** change.
An inaccurate doc is worse than a missing one.

## Reporting bugs and proposing features

Open a GitHub issue describing the problem or proposal. For bugs, include steps to reproduce, expected vs. actual behavior, and relevant logs.

## License

By contributing, you agree that your contributions are licensed under the project's MIT License (the `HadesOperator` submodule is Apache-2.0).
