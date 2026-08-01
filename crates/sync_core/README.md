# `stonfi_sync_core`

`stonfi_sync_core` is a small asynchronous engine for synchronizing ordered
heights through a dependency graph. Initiators discover upstream progress,
synchronizers process inclusive ranges, and status stores persist committed
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

`SyncHeight` is a `u64`. Height `0` remains the initial no-progress sentinel,
so consumers that need to process a chain's native height `0` must map their
external coordinates into the engine's height domain.

## Installation

The crate has not published a remote Git release yet and is not published to
crates.io. During development, pin it to a published workspace commit:

```toml
[dependencies]
stonfi_sync_core = { git = "https://github.com/ston-fi/stonfi-sync-engine", rev = "<published-commit>" }
stonfi_metrics = { version = "0.0.1", git = "https://github.com/ston-fi/stonfi-metrics", rev = "v0.0.1" }
```

The crate requires a Tokio runtime. Implement [`SyncInitiator`] for each source,
[`SyncHandler`] for each processor, and [`SyncStatusStore`] for durable
progress. Wrap implementations with [`Initiator`] and [`Synchronizer`], connect
their triggers through [`SyncEngine::builder`], and call [`SyncEngine::run`].

See [`examples/simple.rs`](examples/simple.rs) for a complete runnable example.

## Quick start

After application startup has initialized metrics, this helper accepts already
constructed consumer implementations, runs the dependency graph until the
application supplies its shutdown future, and then confirms that every engine
task has exited:

```rust
use std::future::Future;
use std::sync::Arc;
use stonfi_sync_core::{
    errors::SyncCoreResult,
    sync_engine::{Initiator, SyncEngine, SyncStatusStore, Synchronizer},
};

async fn run_until<F>(
    status_store: Arc<dyn SyncStatusStore>,
    source: Initiator,
    processor: Synchronizer,
    shutdown: F,
) -> SyncCoreResult<()>
where
    F: Future<Output = ()>,
{
    let engine = SyncEngine::builder(status_store)
        .add_synchronizer(processor, &[&source])?
        .add_initiator(source)?
        .build();

    let run_handle = engine.run();
    shutdown.await;
    run_handle.shutdown().await
}
```

## Lifecycle and failure behavior

- [`SyncEngine::run`] consumes the engine and returns its only runtime owner.
  [`RunHandle::shutdown`] requests cooperative shutdown, waits up to the
  configured timeout, then aborts remaining tasks. Dropping the handle requests
  best-effort shutdown without waiting.
- Shutdown defaults to 30 seconds. Configure it with
  [`Builder::with_shutdown_timeout`]. Consumer futures must yield for Tokio task
  abortion to take effect.
- [`RunHandle::wait`] observes natural completion without requesting shutdown.
  Both lifecycle methods report task panics and cancellations and stop the
  remaining tasks after a failure.
- Handler, callback, and status-store failures are retried after the owning
  implementation's `retry_delay()`.
- Handler range delivery is at-least-once. A timed-out or failed
  `sync_range()` call is retried for the same range, so handler effects must be
  idempotent, cancellation-safe, or transactional.
- Callback failures are retried while the engine is active. Callbacks must be
  idempotent because a later callback failure can replay earlier callbacks.
  Delivery is not persisted or guaranteed across shutdown, process failure, or
  restart.
- Returning `Ok(None)` from [`SyncHandler::sync_range`] defers that range until
  an upstream trigger advances again.
- `allow_rewind()` permits a handler to publish a lower height. Upstream
  decreases do not rewind dependants or cancel an active wait; later waits use
  current trigger values.

## Public API

Public items use module-qualified paths. Error types are under [`errors`], the
in-memory status implementation is under [`mem_status_manager`], and engine types and
extension traits are under [`sync_engine`]. The main types are [`SyncEngine`],
[`Builder`], [`Initiator`], [`Synchronizer`], [`RunHandle`], and
[`MemStatusManager`]. Consumer-owned extension points are [`SyncInitiator`],
[`SyncHandler`], [`SyncStatusStore`], [`SyncTrigger`], and [`SyncCallback`].
Pass dependencies by reference to `Builder::add_synchronizer`; the builder
clones their progress receivers before their owners are registered.

Initiators and handlers are owned by one engine task and need only implement
`Send + 'static`. `SyncHandler::sync_range` receives `&mut self`, so stateful
implementations can update their own fields without internal locking. Status
stores and callbacks are shared between tasks and therefore remain
`Send + Sync + 'static`.

## Metrics

Engine metrics use the default Prometheus registry and are registered with
`stonfi_metrics` automatically. Applications must initialize the registered
collectors during startup before running the engine:

```rust
# fn initialize() -> anyhow::Result<()> {
stonfi_metrics::init_metrics!()?;
# Ok(())
# }
```

Pass a listen address to `init_metrics!` to start its `/metrics` server.
Initialization is idempotent. Engine values never initialize or register
collectors themselves; metric access before startup initialization panics by
design.

Height gauges use unsigned `u64` storage. Prometheus exposes numeric samples as
`f64`, so scraped height values above `2^53` may lose unit precision; engine
processing and `SyncStatusStore` persistence still retain the full `u64` value.

## Toolchain and features

The crate uses Rust 2024 and supports Rust 1.93 and newer. It has no optional
Cargo features. Diagnostics contain no ANSI escapes and are emitted through
`tracing`; applications install and configure their own subscriber.

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
[`Builder::with_shutdown_timeout`]: crate::sync_engine::Builder::with_shutdown_timeout
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
[`SyncStatusStore`]: crate::sync_engine::SyncStatusStore
[`SyncTrigger`]: crate::sync_engine::SyncTrigger
[`Synchronizer`]: crate::sync_engine::Synchronizer
[`errors`]: crate::errors
[`mem_status_manager`]: crate::mem_status_manager
[`sync_engine`]: crate::sync_engine
