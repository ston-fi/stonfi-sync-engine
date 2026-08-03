# `stonfi_sync_core`

`stonfi_sync_core` is a small asynchronous engine for synchronizing ordered
heights through a dependency graph. Height providers discover upstream progress,
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
active engine may write a given handler ID. Distributed task transport is also
outside this crate; it belongs in a separate workspace package so the core
engine remains independent of gRPC, protobuf code generation, and worker
infrastructure.

`SyncHeight` is a `u64`. Height `0` remains the initial no-progress sentinel,
so consumers that need to process a chain's native height `0` must map their
external coordinates into the engine's height domain.

## Installation

The crate has not published a remote Git release yet and is not published to
crates.io. During development, pin it to a commit SHA that is reachable from
the remote:

```toml
[dependencies]
stonfi_sync_core = { git = "https://github.com/ston-fi/stonfi-sync-engine", rev = "<published-commit>" }
stonfi_metrics = { version = "0.0.1", git = "https://github.com/ston-fi/stonfi-metrics", rev = "v0.0.1" }
```

The crate requires a Tokio runtime. Implement [`HeightLoader`] for each source,
[`SyncHandler`] for each range handler, and [`SyncStatusStore`] for durable
progress. Wrap implementations with [`HeightProvider`] and [`Synchronizer`],
connect their progress providers through [`SyncEngine::builder`], and call
[`SyncEngine::run`].

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
    sync_engine::{HeightProvider, SyncEngine, SyncStatusStore, Synchronizer},
};

async fn run_until<F>(
    status_store: Arc<dyn SyncStatusStore>,
    source: HeightProvider,
    synchronizer: Synchronizer,
    shutdown: F,
) -> SyncCoreResult<()>
where
    F: Future<Output = ()>,
{
    let engine = SyncEngine::builder(status_store)
        .add_synchronizer(synchronizer, &[&source])?
        .add_height_provider(source)?
        .build();

    let run_handle = engine.run();
    shutdown.await;
    run_handle.shutdown().await
}
```

## Initial status

`SyncStatusStore` owns the engine-wide initial synced height. Construct the
store with the fallback selected by application configuration; for example,
`MemStatusStore::new(0)` uses the no-progress sentinel. When a synchronizer
starts, [`SyncStatusStore::load_synced_or_initial`] resolves state in this
order:

1. the synchronizer handler's own persisted ID;
2. the persisted [`INITIAL_HEIGHT`] value; or
3. the store's configured fallback, saved under `INITIAL_HEIGHT` before it is
   returned.

`INITIAL_HEIGHT` is the reserved initial-height key in the handler-ID namespace.
Status stores receive it through their `handler_id` arguments, but it cannot be
registered as a height loader or sync handler and never participates in the
dependency graph.
Only one active engine may initialize or write it because status stores do not
provide compare-and-set coordination.

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
- Initial source discovery emits `on_height_loaded` with previous height `0`.
  A nonzero initial height is then published and emits `on_height_published`
  with the same previous height.
- Returning `Ok(None)` from [`SyncHandler::sync_range`] defers that range until
  an upstream progress provider advances again.
- `allow_rewind()` permits a handler to publish a lower height. Upstream
  decreases do not rewind dependants or cancel an active wait; later waits use
  current progress-provider values.

## Public API

Public items use module-qualified paths. Error types are under [`errors`], the
in-memory status implementation is under [`mem_status_store`], and engine types
and extension traits are under [`sync_engine`]. The main types are [`SyncEngine`],
[`Builder`], [`HeightProvider`], [`Synchronizer`], [`RunHandle`], and
[`MemStatusStore`]. Consumer-owned extension points are [`HeightLoader`],
[`SyncHandler`], [`SyncStatusStore`], [`ProgressProvider`], and [`SyncCallback`].
Each progress subscription returns a [`ProgressReceiver`].
Pass dependencies by reference to `Builder::add_synchronizer`; the builder
subscribes to each [`ProgressProvider`] before its owner is registered.
Every referenced engine-owned provider must subsequently be registered with
the same builder. If it is dropped instead, its progress channel closes and the
dependant synchronizer stops. Custom providers must likewise keep their sender
alive for as long as the dependant should run. Keep the dependency graph
acyclic; the builder does not discover or reject cycles, which can wait for one
another indefinitely.

Height providers and handlers are owned by one engine task and need only implement
`Send + 'static`. `SyncHandler::sync_range` receives `&mut self`, so stateful
implementations can update their own fields without internal locking. Status
stores and callbacks are shared between tasks and therefore remain
`Send + Sync + 'static`.

Store implementations provide [`SyncStatusStore::initial_synced_height`] from
constructor or application configuration. The trait's default
`load_synced_or_initial` implementation owns the durable fallback algorithm;
handlers only process ranges.

## Metrics

Engine metrics use the default Prometheus registry. The crate declares its
collectors with `stonfi_metrics`; applications must initialize all declared
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

All engine metric series identify their height provider or synchronizer with
the `handler_id` label.

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
[`HeightProvider`]: crate::sync_engine::HeightProvider
[`MemStatusStore`]: crate::mem_status_store::MemStatusStore
[`RunHandle`]: crate::sync_engine::RunHandle
[`RunHandle::shutdown`]: crate::sync_engine::RunHandle::shutdown
[`RunHandle::wait`]: crate::sync_engine::RunHandle::wait
[`SyncCallback`]: crate::sync_engine::SyncCallback
[`SyncEngine`]: crate::sync_engine::SyncEngine
[`SyncEngine::builder`]: crate::sync_engine::SyncEngine::builder
[`SyncEngine::run`]: crate::sync_engine::SyncEngine::run
[`SyncHandler`]: crate::sync_engine::SyncHandler
[`SyncHandler::sync_range`]: crate::sync_engine::SyncHandler::sync_range
[`INITIAL_HEIGHT`]: crate::sync_engine::INITIAL_HEIGHT
[`HeightLoader`]: crate::sync_engine::HeightLoader
[`SyncStatusStore`]: crate::sync_engine::SyncStatusStore
[`SyncStatusStore::initial_synced_height`]: crate::sync_engine::SyncStatusStore::initial_synced_height
[`SyncStatusStore::load_synced_or_initial`]: crate::sync_engine::SyncStatusStore::load_synced_or_initial
[`ProgressProvider`]: crate::sync_engine::ProgressProvider
[`ProgressReceiver`]: crate::sync_engine::ProgressReceiver
[`Synchronizer`]: crate::sync_engine::Synchronizer
[`errors`]: crate::errors
[`mem_status_store`]: crate::mem_status_store
[`sync_engine`]: crate::sync_engine
