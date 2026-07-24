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

## Quick start

This helper accepts already constructed consumer implementations, runs the
dependency graph until the application supplies its shutdown future, and then
confirms that every engine task has exited:

```rust
use std::future::Future;
use std::sync::Arc;
use stonfi_sync_core::{
    errors::SyncCoreResult,
    sync_engine::{Initiator, SyncEngine, SyncStatusManager, Synchronizer},
};

async fn run_until<F>(
    status_manager: Arc<dyn SyncStatusManager>,
    source: Initiator,
    processor: Synchronizer,
    shutdown: F,
) -> SyncCoreResult<()>
where
    F: Future<Output = ()>,
{
    let engine = SyncEngine::builder(status_manager)?
        .add_sync(processor, &[&source])?
        .add_initiator(source)?
        .build();

    let run_handle = engine.run();
    shutdown.await;
    run_handle.shutdown().await
}
```

## Lifecycle and failure behavior

- [`SyncEngine::run`] consumes the engine definition, so it cannot be started
  twice.
- [`RunHandle::shutdown`] signals cooperative shutdown and waits for every
  spawned task. Dropping the handle signals best-effort shutdown without
  waiting.
- [`RunHandle::wait`] waits for natural task completion without requesting
  shutdown. Polling initiators normally require `shutdown()` instead.
- Engine-owned trigger waits and retry sleeps observe shutdown promptly. An
  in-progress consumer future is not preempted and must return before its task
  can stop.
- Both `shutdown()` and `wait()` report task panics and cancellations after
  awaiting every task.
- Handler, callback, and status-manager failures are retried with the owning
  implementation's backoff.
- Callback failures are retried while the engine is active. Callbacks must be
  idempotent because a later callback failure can replay earlier callbacks.
  Delivery is not persisted or guaranteed across shutdown, process failure, or
  restart.
- Returning `Ok(None)` from [`SyncHandler::sync_range`] defers that range until
  an upstream trigger advances again.
- `allow_wrap()` permits a handler to publish a lower height for its own
  synchronizer. An upstream decrease does not rewind a dependant or cancel
  forward progress already selected by the dependant's active wait. Subsequent
  waits observe current trigger values, and each dependant decides independently
  whether its handler may wrap.

## Public API

Public items use module-qualified paths. Error types are under [`errors`], the
in-memory status manager is under [`mem_status_manager`], and engine types and
extension traits are under [`sync_engine`]. The main types are [`SyncEngine`],
[`Builder`], [`Initiator`], [`Synchronizer`], [`RunHandle`], and
[`MemStatusManager`]. Consumer-owned extension points are [`SyncInitiator`],
[`SyncHandler`], [`SyncStatusManager`], [`SyncTrigger`], and [`SyncCallback`].

Initiators and handlers are owned by one engine task and need only implement
`Send + 'static`. `SyncHandler::sync_range` receives `&mut self`, so stateful
implementations can update their own fields without internal locking. Status
managers and callbacks are shared between tasks and therefore remain
`Send + Sync + 'static`.

### Migrating pre-release consumers

Root-level re-exports and the `SyncID` alias have been removed. Import public
items from their modules, keep owned IDs as `String` where needed, and return or
pass them as `&str` at the engine boundary. Handler implementations must also
change `sync_range(&self, ...)` to `sync_range(&mut self, ...)`.

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

[`Builder`]: crate::sync_engine::Builder
[`Initiator`]: crate::sync_engine::Initiator
[`MemStatusManager`]: crate::mem_status_manager::MemStatusManager
[`RunHandle`]: crate::sync_engine::RunHandle
[`RunHandle::shutdown`]: crate::sync_engine::RunHandle::shutdown
[`RunHandle::wait`]: crate::sync_engine::RunHandle::wait
[`SyncCallback`]: crate::sync_engine::SyncCallback
[`SyncEngine`]: crate::sync_engine::SyncEngine
[`SyncEngine::builder`]: crate::sync_engine::SyncEngine::builder
[`SyncEngine::run`]: crate::sync_engine::SyncEngine::run
[`SyncHandler`]: crate::sync_engine::SyncHandler
[`SyncHandler::sync_range`]: crate::sync_engine::SyncHandler::sync_range
[`SyncInitiator`]: crate::sync_engine::SyncInitiator
[`SyncStatusManager`]: crate::sync_engine::SyncStatusManager
[`SyncTrigger`]: crate::sync_engine::SyncTrigger
[`Synchronizer`]: crate::sync_engine::Synchronizer
[`errors`]: crate::errors
[`mem_status_manager`]: crate::mem_status_manager
[`sync_engine`]: crate::sync_engine
