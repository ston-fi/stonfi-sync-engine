# STON.fi Sync Engine Workspace Agent Guide

This repository is a public Rust library workspace published on crates.io and
released through Git tags. Use the `rust-library-review` skill for non-trivial
reviews, implementations, refactors, and workspace changes.

## Workspace boundary

All packages live under `crates/`:

- `crates/sync_core` contains the transport-independent synchronization engine
  in the `stonfi_sync_core` package.
- `crates/distributed_sync` contains the public `stonfi_distributed_sync`
  package owning in-memory distributed task coordination, gRPC/protobuf,
  server and worker lifecycles, and task serialization.

Do not implement distributed synchronization as a `stonfi_sync_core` feature.
The packages have different dependencies, runtime responsibilities, public API
boundaries, and release concerns. Each package must have its own manifest,
README, agent guide, examples or integration tests where useful, and
public-library review.

Package-specific API, ownership, lifecycle, metrics, and compatibility rules
are documented in the nearest package `AGENTS.md`. Apply both this workspace
guide and the relevant package guide.

## Workspace changes

- Keep the root manifest virtual; do not add a root Rust package.
- Keep package names, paths, README links, CI commands, lockfile, and workspace
  members synchronized.
- Preserve independent package versioning.
- Keep package changelogs concise and consumer-facing; do not use them as
  development logs.
- Review package contents from the workspace root with
  `cargo package --list -p <package>`.
- Declare dependency versions and shared default-feature policies in the root
  `[workspace.dependencies]` table. Crate manifests inherit them with
  `workspace = true` and add only crate-specific features locally.

## Validation

Fast gate:

```text
cargo test --workspace --all-features --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
```

Full gate:

```text
cargo test --workspace --doc --locked
cargo test --workspace --examples --locked
cargo +nightly fmt --check
RUSTDOCFLAGS="-D warnings -D missing_docs" cargo doc --workspace --no-deps --all-features --locked
cargo +1.95.0 check --workspace --all-features --locked
cargo package --list --locked -p stonfi_sync_core
cargo package --list --locked -p stonfi_distributed_sync
cargo publish --dry-run --locked -p stonfi_sync_core
bash .github/check-external-consumer.sh
```

Packages are published to crates.io. Do not change the distribution or
versioning policy unless the task explicitly requires it.
After successful `main` CI, release-plz owns version and changelog pull
requests, dependency-ordered crates.io publishing, package-specific tags, and
GitHub Releases. Configure the repository secret `CRATES_IO_REGISTRY_TOKEN`
with `publish-new` and `publish-update` scopes, and allow GitHub Actions to
create pull requests in the repository workflow settings. Package versions
remain independent.
