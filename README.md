# STON.fi Sync Engine

This repository is the public Rust workspace for STON.fi synchronization
libraries. Each independently consumable synchronization layer lives in its
own package under `crates/`.

## Packages

| Package | Path | Status | Responsibility |
| --- | --- | --- | --- |
| [`stonfi_sync_core`](crates/sync_core/README.md) | `crates/sync_core` | Available at `v0.0.1` | Dependency-aware synchronization of ordered heights |
| [`stonfi_distributed_sync`](crates/distributed_sync/README.md) | `crates/distributed_sync` | Unreleased | Distributed task coordination, gRPC servers, and workers |

`stonfi_distributed_sync` is a separate package depending on
`stonfi_sync_core`. It is not exposed as a core feature because its gRPC,
protobuf, server, worker, serialization, and lifecycle concerns have an
independent dependency and release boundary.

## Using the core package

The initial release is Git-distributed and is not published to crates.io:

```toml
[dependencies]
stonfi_sync_core = { git = "https://github.com/ston-fi/stonfi-sync-engine", tag = "v0.0.1" }
```

See the [`stonfi_sync_core` README](crates/sync_core/README.md) for its API,
runtime requirements, metrics initialization, lifecycle, and complete example.
The [`stonfi_distributed_sync` README](crates/distributed_sync/README.md)
documents its currently unreleased API, at-least-once delivery contract,
trusted-network boundary, metrics, and runnable example.

## Releases

Merges to `main` run release-plz only after the workspace quality and MSRV jobs
succeed. Release-plz creates the version tag and GitHub Release in Git-only
mode; it does not run `cargo publish` or require a crates.io token.

## Workspace validation

```text
cargo test --workspace --all-features --locked
cargo test --workspace --doc --locked
cargo test --workspace --examples --locked
cargo +nightly fmt --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
RUSTDOCFLAGS="-D warnings -D missing_docs" cargo doc --workspace --no-deps --all-features --locked
cargo +1.93.0 check --workspace --all-features --locked
cargo package --list --locked -p stonfi_sync_core
cargo package --list --locked -p stonfi_distributed_sync
```
