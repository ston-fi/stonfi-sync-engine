# STON.fi Sync Engine

This repository is the public Rust workspace for STON.fi synchronization
libraries. Each independently consumable synchronization layer lives in its
own package under `crates/`.

## Packages

| Package | Path | Responsibility |
| --- | --- | --- |
| [`stonfi_sync_core`](crates/sync_core/README.md) | `crates/sync_core` | Dependency-aware synchronization of ordered heights |
| [`stonfi_distributed_sync`](crates/distributed_sync/README.md) | `crates/distributed_sync` | Distributed task coordination, gRPC servers, and workers |

`stonfi_distributed_sync` is a separate package depending on
`stonfi_sync_core`. It is not exposed as a core feature because its gRPC,
protobuf, server, worker, serialization, and lifecycle concerns have an
independent dependency and release boundary.

## Using the core package

Pin the package to a Git revision:

```toml
[dependencies]
stonfi_sync_core = { git = "https://github.com/ston-fi/stonfi-sync-engine", rev = "<revision>" }
```

See the [`stonfi_sync_core` README](crates/sync_core/README.md) for its API,
runtime requirements, metrics initialization, lifecycle, and complete example.
The [`stonfi_distributed_sync` README](crates/distributed_sync/README.md)
documents its API, at-least-once delivery contract,
trusted-network boundary, metrics, and runnable example.

## Workspace validation

```text
cargo test --workspace --all-features --locked
cargo test --workspace --doc --locked
cargo test --workspace --examples --locked
cargo +nightly fmt --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
RUSTDOCFLAGS="-D warnings -D missing_docs" cargo doc --workspace --no-deps --all-features --locked
cargo +1.95.0 check --workspace --all-features --locked
cargo package --list --locked -p stonfi_sync_core
cargo package --list --locked -p stonfi_distributed_sync
```
