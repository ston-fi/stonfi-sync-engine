# `stonfi_sync_core`

`stonfi_sync_core` is a small asynchronous engine for synchronizing ordered
heights through a dependency graph. Initiators discover upstream progress,
synchronizers process inclusive ranges, and status managers persist committed
heights.

It is the transport-independent package in the `stonfi-sync-engine` workspace
and lives at `crates/sync_core`.

## When to use it

Use this crate when a service must:

- poll one or more monotonically advancing sources;
- process bounded ranges with retry and timeout behavior;
- make one synchronizer depend on completed progress from others;
- persist progress independently from the processing implementation; and
- observe lifecycle events through callbacks and Prometheus collectors.

It is not a distributed lock or multi-writer coordination system. Only one
active engine may write a given sync ID. Distributed task transport is also
outside this crate; it belongs in a separate workspace package so the core
engine remains independent of gRPC, protobuf code generation, and worker
infrastructure.

## Installation

The initial release is distributed from GitHub and is not published to
crates.io:

```toml
[dependencies]
stonfi_sync_core = { git = "https://github.com/ston-fi/stonfi-sync-engine", tag = "v0.0.1" }
stonfi_metrics = { version = "0.0.1", git = "https://github.com/ston-fi/stonfi-metrics", rev = "v0.0.1" }
```

The crate requires a Tokio runtime. Implement [`SyncInitiator`] for each source,
[`SyncHandler`] for each processor, and [`SyncStatusManager`] for durable
progress. Wrap implementations with [`Initiator`] and [`Synchronizer`], connect
their triggers through [`SyncEngine::builder`], and call [`SyncEngine::run`].

See [`examples/simple.rs`](examples/simple.rs) for a complete runnable example.

## Lifecycle and failure behavior

- Dropping [`SyncEngine`] is the cooperative shutdown signal.
- [`RunHandle::wait`] waits for spawned tasks after the engine is dropped.
- An in-progress consumer future is not preempted; it must return before the
  task can observe shutdown.
- Handler, callback, and status-manager failures are retried with the owning
  implementation's backoff.
- Callback delivery is at least once. Callbacks must be idempotent because a
  later callback failure can replay earlier callbacks.
- Returning `Ok(None)` from [`SyncHandler::sync_range`] defers that range until
  an upstream trigger advances again.
- `allow_wrap()` permits a handler to publish a lower height. Downstream users
  must be prepared to observe such decreases while waiting for all parents.

## Public API

The main types are [`SyncEngine`], [`Builder`], [`Initiator`], [`Synchronizer`],
[`RunHandle`], and [`MemStatusManager`]. Consumer-owned extension points are
[`SyncInitiator`], [`SyncHandler`], [`SyncStatusManager`], [`SyncTrigger`], and
[`SyncCallback`].

Engine metrics use the default Prometheus registry. `SyncEngine::builder`
initializes and registers them, returning a `SyncCoreError` if registration
fails. Applications that use `stonfi_metrics` should initialize it during
startup before building the engine:

```rust
# fn initialize() -> anyhow::Result<()> {
stonfi_metrics::init_metrics!()?;
# Ok(())
# }
```

Pass a listen address to `init_metrics!` to start its `/metrics` server. Both
startup paths are idempotent, so applications do not collect or register
metrics from individual engine values.

## Toolchain and features

The crate uses Rust 2024 and supports Rust 1.93 and newer. It has no optional
Cargo features.

## Validation

```text
cargo test -p stonfi_sync_core --all-features --locked
cargo test -p stonfi_sync_core --doc --locked
cargo test -p stonfi_sync_core --examples --locked
cargo +nightly fmt --check
cargo clippy -p stonfi_sync_core --all-targets --all-features --locked -- -D warnings
RUSTDOCFLAGS="-D warnings -D missing_docs" cargo doc -p stonfi_sync_core --no-deps --all-features --locked
cargo +1.93.0 check -p stonfi_sync_core --all-features --locked
cargo package --list --locked -p stonfi_sync_core
```

[`Builder`]: crate::Builder
[`Initiator`]: crate::Initiator
[`MemStatusManager`]: crate::MemStatusManager
[`RunHandle`]: crate::RunHandle
[`RunHandle::wait`]: crate::RunHandle::wait
[`SyncCallback`]: crate::SyncCallback
[`SyncEngine`]: crate::SyncEngine
[`SyncEngine::builder`]: crate::SyncEngine::builder
[`SyncEngine::run`]: crate::SyncEngine::run
[`SyncHandler`]: crate::SyncHandler
[`SyncHandler::sync_range`]: crate::SyncHandler::sync_range
[`SyncInitiator`]: crate::SyncInitiator
[`SyncStatusManager`]: crate::SyncStatusManager
[`SyncTrigger`]: crate::SyncTrigger
[`Synchronizer`]: crate::Synchronizer
