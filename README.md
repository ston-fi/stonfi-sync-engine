# STON.fi Sync Engine

This repository is the public Rust workspace for STON.fi synchronization
libraries published on crates.io. Each independently consumable synchronization
layer lives in its own package under `crates/`.

## Packages

| Package | Path | Responsibility |
| --- | --- | --- |
| [`stonfi_sync_core`](crates/sync_core/README.md) | `crates/sync_core` | Dependency-aware synchronization of ordered heights |
| [`stonfi_distributed_sync`](crates/distributed_sync/README.md) | `crates/distributed_sync` | Distributed task coordination, gRPC servers, and workers |

`stonfi_distributed_sync` is a separate package depending on
`stonfi_sync_core`. It is not exposed as a core feature because its gRPC,
protobuf, server, worker, serialization, and lifecycle concerns have an
independent dependency and release boundary.

## Installation

Version `0.1.0` is the first crates.io release of both packages:

```toml
[dependencies]
stonfi_distributed_sync = "0.1"
stonfi_sync_core = "0.1"
```

Package versions remain independent. Release-plz creates a package-specific tag
and GitHub Release for each published version.

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
cargo publish --dry-run --locked -p stonfi_sync_core
bash .github/check-external-consumer.sh
```

## Releases

After `main` passes CI, release-plz publishes any package version not yet on
crates.io, creates its tag and GitHub Release, and creates or updates the next
release pull request with version and changelog changes. It resolves workspace
dependencies so `stonfi_sync_core` is published before
`stonfi_distributed_sync`.

Configure the repository secret `CRATES_IO_REGISTRY_TOKEN` with a crates.io
token that has `publish-new` and `publish-update` scopes before merging this
release. The first successful release-plz run publishes version `0.1.0` of both
currently new crates. In **Settings → Actions → General → Workflow
permissions**, enable **Allow GitHub Actions to create and approve pull
requests** so release-plz can maintain its release pull request.
