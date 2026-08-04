# `stonfi_sync_core`

`stonfi_sync_core` is a small asynchronous engine for synchronizing ordered
heights through a dependency graph. Height providers discover upstream progress,
synchronizers process inclusive ranges, and progress stores persist committed
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

Depend on the crate from crates.io:

```toml
[dependencies]
stonfi_metrics = "0.1"
stonfi_sync_core = "0.1"
```

To use the built-in ScyllaDB progress store, enable its non-default feature and
depend directly on the client only when constructing a customized client:

```toml
[dependencies]
stonfi_metrics = "0.1"
stonfi_scylla_client = "0.2"
stonfi_sync_core = { version = "0.1", features = ["scylla"] }
```

The crate requires a Tokio runtime. Implement [`HeightLoader`] for each source,
[`SyncHandler`] for each range handler, and [`SyncProgressStore`] for durable
progress. Register implementations directly when they are leaves in the
dependency graph. Wrap them with [`HeightProvider`] or [`Synchronizer`] first
when their progress must be passed to a dependent handler, connect the graph
through [`SyncEngine::builder`], and call [`SyncEngine::run`].

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
    sync_engine::{HeightProvider, SyncEngine, SyncProgressStore, Synchronizer},
};

async fn run_until<F>(
    progress_store: Arc<dyn SyncProgressStore>,
    source: HeightProvider,
    synchronizer: Synchronizer,
    shutdown: F,
) -> SyncCoreResult<()>
where
    F: Future<Output = ()>,
{
    let engine = SyncEngine::builder(progress_store)
        .add_synchronizer(synchronizer, &[&source])?
        .add_height_provider(source)?
        .build();

    let run_handle = engine.run();
    shutdown.await;
    run_handle.shutdown().await
}
```

## Initial progress

`SyncProgressStore` owns the engine-wide initial synced height. Construct the
store with the fallback selected by application configuration; for example,
`MemProgressStore::new(0)` uses the no-progress sentinel. When a synchronizer
starts, [`SyncProgressStore::load_synced_or_initial`] resolves state in this
order:

1. the synchronizer handler's own persisted ID;
2. the persisted [`INITIAL_HEIGHT`] value; or
3. the store's configured fallback, saved under `INITIAL_HEIGHT` before it is
   returned.

`INITIAL_HEIGHT` is the reserved initial-height key in the handler-ID namespace.
Progress stores receive it through their `handler_id` arguments, but it cannot be
registered as a height loader or sync handler and never participates in the
dependency graph.
Consumers can read its persisted value directly with
[`SyncProgressStore::load_initial_height`].
Only one active engine may initialize or write it because progress stores do not
provide compare-and-set coordination.

## ScyllaDB progress storage

The `scylla` feature exposes [`ScyllaProgressStore`]. Initialize
`stonfi_metrics` before building it because migrations and client construction
perform instrumented database operations. Configure either an existing client:

```rust,ignore
# async fn create(
#     client: stonfi_scylla_client::client::ScyllaClient,
# ) -> stonfi_sync_core::errors::SyncCoreResult<()> {
use stonfi_sync_core::scylla_progress_store::ScyllaProgressStore;

let store = ScyllaProgressStore::builder(0)
    .with_scylla_client(client)
    .with_table_name("service_sync_progress")
    .build()
    .await?;
# let _ = store;
# Ok(())
# }
```

or let the builder create one with `stonfi_scylla_client`'s defaults:

```rust,ignore
# async fn create() -> stonfi_sync_core::errors::SyncCoreResult<()> {
use stonfi_sync_core::scylla_progress_store::ScyllaProgressStore;

let store = ScyllaProgressStore::builder(0)
    .with_endpoints("127.0.0.1:9042")
    .with_keyspace("my_service")
    .build()
    .await?;
# let _ = store;
# Ok(())
# }
```

These modes are exclusive. Endpoints and keyspace must be configured together.
The keyspace must already exist. Each build replays an idempotent migration that
creates only the configured table, `sync_progress` by default, with
`handler_id text PRIMARY KEY` and `height bigint`; it does not backfill legacy
Tongrid state. Custom table names must be unquoted CQL identifiers.

Handler IDs, including `INITIAL_HEIGHT`, are stored unchanged. CQL `bigint` is
signed, so this implementation accepts heights only through `i64::MAX` even
though the engine's `SyncHeight` is `u64`. Larger writes are rejected and
negative database values are reported as corrupt storage. The store adds no
locking, compare-and-set behavior, or retry loop: one engine must own each
handler ID, the client performs bounded request retries, and the engine retries
progress operations according to the handler policy.

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
- Handler, callback, and progress-store failures are retried after the owning
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
- [`SyncHandler::sync_timeout`] and the configured lifecycle shutdown timeout
  are passed directly to Tokio. A zero duration therefore uses Tokio's normal
  ready-first timeout behavior.
- `allow_rewind()` permits a handler to publish a lower height. Upstream
  decreases do not rewind dependants or cancel an active wait; later waits use
  current progress-provider values.

## Public API

Public items use module-qualified paths. Error types are under [`errors`], the
in-memory progress implementation is under [`mem_progress_store`], and engine types
and extension traits are under [`sync_engine`]. The main types are [`SyncEngine`],
[`Builder`], [`HeightProvider`], [`Synchronizer`], [`RunHandle`], and
[`MemProgressStore`]. Consumer-owned extension points are [`HeightLoader`],
[`SyncHandler`], [`SyncProgressStore`], [`ProgressProvider`], and [`SyncCallback`].
Each progress subscription returns a [`ProgressReceiver`].
`Builder::add_height_provider` accepts any [`HeightLoader`], and
`Builder::add_synchronizer` accepts any [`SyncHandler`], through their standard
conversions into the corresponding engine-owned wrapper. Construct a wrapper
explicitly only when its [`ProgressProvider`] must be referenced by another
registration.
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
implementations can update their own fields without internal locking. Progress
stores and callbacks are shared between tasks and therefore remain
`Send + Sync + 'static`.

Store implementations provide [`SyncProgressStore::initial_synced_height`] from
constructor or application configuration. The trait's default
`load_synced_or_initial` implementation owns the durable fallback algorithm;
handlers only process ranges.

## Metrics

Engine metrics use the default Prometheus registry. The crate declares its
collectors with `stonfi_metrics`; applications must initialize all declared
collectors during startup before running the engine:

```rust
# fn initialize() -> Result<(), Box<dyn std::error::Error>> {
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
`f64`, so scraped height values above `2^53` may lose unit precision. Engine
processing and the progress-store trait retain the full `u64` domain; the
optional ScyllaDB implementation is limited to `i64::MAX` by its CQL schema.

## Toolchain and features

The crate uses Rust 2024 and supports Rust 1.95 and newer. Its non-default
`scylla` feature adds [`ScyllaProgressStore`] and `stonfi_scylla_client`.
Diagnostics contain no ANSI escapes and are emitted through `tracing`;
applications install and configure their own subscriber.

## Validation

```text
cargo test -p stonfi_sync_core --all-features --locked
cargo test -p stonfi_sync_core --doc --locked
cargo test -p stonfi_sync_core --examples --all-features --locked
cargo test -p stonfi_sync_core --features scylla --test scylla_progress_store --locked -- --ignored --test-threads=1
cargo +nightly fmt --check
cargo clippy -p stonfi_sync_core --all-targets --all-features --locked -- -D warnings
RUSTDOCFLAGS="-D warnings -D missing_docs" cargo doc -p stonfi_sync_core --no-deps --all-features --locked
cargo +1.95.0 check -p stonfi_sync_core --all-features --locked
cargo package --list --locked -p stonfi_sync_core
cargo publish --dry-run --locked -p stonfi_sync_core
```

[`Builder`]: crate::sync_engine::Builder
[`Builder::with_shutdown_timeout`]: crate::sync_engine::Builder::with_shutdown_timeout
[`HeightProvider`]: crate::sync_engine::HeightProvider
[`MemProgressStore`]: crate::mem_progress_store::MemProgressStore
[`RunHandle`]: crate::sync_engine::RunHandle
[`RunHandle::shutdown`]: crate::sync_engine::RunHandle::shutdown
[`RunHandle::wait`]: crate::sync_engine::RunHandle::wait
[`ScyllaProgressStore`]: crate::scylla_progress_store::ScyllaProgressStore
[`SyncCallback`]: crate::sync_engine::SyncCallback
[`SyncEngine`]: crate::sync_engine::SyncEngine
[`SyncEngine::builder`]: crate::sync_engine::SyncEngine::builder
[`SyncEngine::run`]: crate::sync_engine::SyncEngine::run
[`SyncHandler`]: crate::sync_engine::SyncHandler
[`SyncHandler::sync_range`]: crate::sync_engine::SyncHandler::sync_range
[`SyncHandler::sync_timeout`]: crate::sync_engine::SyncHandler::sync_timeout
[`INITIAL_HEIGHT`]: crate::sync_engine::INITIAL_HEIGHT
[`HeightLoader`]: crate::sync_engine::HeightLoader
[`SyncProgressStore`]: crate::sync_engine::SyncProgressStore
[`SyncProgressStore::initial_synced_height`]: crate::sync_engine::SyncProgressStore::initial_synced_height
[`SyncProgressStore::load_initial_height`]: crate::sync_engine::SyncProgressStore::load_initial_height
[`SyncProgressStore::load_synced_or_initial`]: crate::sync_engine::SyncProgressStore::load_synced_or_initial
[`ProgressProvider`]: crate::sync_engine::ProgressProvider
[`ProgressReceiver`]: crate::sync_engine::ProgressReceiver
[`Synchronizer`]: crate::sync_engine::Synchronizer
[`errors`]: crate::errors
[`mem_progress_store`]: crate::mem_progress_store
[`sync_engine`]: crate::sync_engine
