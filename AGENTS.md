# STON.fi Sync Engine Workspace Agent Guide

This repository is a public Rust library workspace distributed through Git
tags. Use the `rust-library-review` skill for non-trivial reviews,
implementations, refactors, workspace changes, and release preparation.

## Workspace boundary

All packages live under `crates/`:

- `crates/sync_core` contains the transport-independent synchronization engine
  and is published as the `stonfi_sync_core` package.
- `crates/distributed_sync` is reserved for a future separate package owning
  distributed task transport, gRPC/protobuf, server and worker lifecycles,
  serialization, and Tongrid integrations.

Do not implement distributed synchronization as a `stonfi_sync_core` feature.
The packages have different dependencies, runtime responsibilities, public API
boundaries, and release concerns. A new package must have its own manifest,
README, agent guide, changelog, examples or integration tests where useful, and
public-library review.

Package-specific API, ownership, lifecycle, metrics, and compatibility rules
are documented in the nearest package `AGENTS.md`. Apply both this workspace
guide and the relevant package guide.

## Workspace changes

- Keep the root manifest virtual; do not add a root Rust package.
- Keep package names, paths, README links, CI commands, lockfile, and workspace
  members synchronized.
- Preserve independent package versioning. A workspace tag may release one or
  more packages, but each affected changelog must state what the tag contains.
- Review package contents from the workspace root with
  `cargo package --list -p <package>`.
- Do not introduce shared workspace dependencies until at least two packages
  use the same dependency policy and centralizing it reduces real drift.

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
cargo +1.93.0 check --workspace --all-features --locked
cargo package --list --locked -p stonfi_sync_core
```

GitHub CI owns these gates. Packages are currently Git-distributed with
`publish = false`; do not enable crates.io publishing or change versions and
tags unless a release task explicitly requires it.
