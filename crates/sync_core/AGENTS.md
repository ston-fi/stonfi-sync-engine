# `stonfi_sync_core` Agent Guide

This package is a public Rust library published on crates.io and released
through Git tags as part of the `stonfi-sync-engine` workspace.
Use the `rust-library-review` skill for non-trivial reviews, implementations,
and refactors.

## Responsibility and workspace boundary

The `crates/sync_core` package coordinates dependency-aware synchronization of
ordered heights:

- `HeightLoader` loads the latest available height from an upstream source.
- `SyncHandler` processes bounded inclusive ranges.
- `SyncProgressStore` persists committed progress and owns the configured initial
  height fallback.
- `ProgressProvider` supplies progress subscriptions that connect height
  providers and synchronizers into a dependency graph.
- `SyncCallback` observes lifecycle events.

The crate does not provide distributed locking, multi-writer conflict
resolution, a Tokio runtime, or distributed task transport. It provides an
optional ScyllaDB-backed progress store behind the non-default `scylla` feature;
other durable stores remain consumer implementations. Distributed transport
concerns belong to the sibling `crates/distributed_sync` package; do not add
gRPC, protobuf, server, or worker dependencies to the core package.

## Public API and ownership

Keep public paths module-qualified: errors and result types live in `errors`,
the in-memory implementation lives in `mem_progress_store`, and all engine
types and extension traits live in `sync_engine`. The optional ScyllaDB store
lives in `scylla_progress_store`, with its builder in the public `builder`
submodule. Gate only the `scylla_progress_store` declaration in `lib.rs`; do not
repeat the feature gate in its source or tests. Do not add root re-exports.
`SyncEngine`, `Builder`, `HeightProvider`, `Synchronizer`, `RunHandle`, and
`MemProgressStore` are the primary consumer types. `HeightLoader`, `SyncHandler`,
`SyncProgressStore`, `ProgressProvider`, and `SyncCallback` are intentional
downstream extension points and must remain externally implementable.

Public ID boundaries use `&str`; store IDs privately as `String` only where
ownership is required. Height loaders and handlers belong to one task and
require `Send + 'static`, not `Sync`. `SyncHandler::sync_range` takes
`&mut self` so stateful handlers do not need internal synchronization. Progress
stores and callbacks are shared across tasks and require `Send + Sync + 'static`.

Prefer `SyncEngine::builder`, add synchronizers with references to their
upstream progress providers, add the corresponding height providers, then
build and run. Registration accepts raw `SyncHandler` and `HeightLoader`
implementations through their standard wrapper conversions. Construct
`Synchronizer` or `HeightProvider` explicitly when its progress must be
referenced by a dependant. Pass raw implementations or explicitly typed
wrappers to registration; an inline `.into()` can be ambiguous at these generic
method boundaries. The builder subscribes to progress before retaining
single ownership of height providers and synchronizers. It cannot verify that a
referenced provider is later registered: dropping an unregistered provider
closes its channel and stops the dependant. `SyncEngine::run` consumes the
engine definition and returns the runtime owner. Use `RunHandle::shutdown` for
bounded awaited shutdown; dropping the handle only signals best-effort
shutdown. Use `RunHandle::wait` only when every task can finish naturally.

Consumers use the crates.io dependency documented in `README.md`. The crate
requires a running Tokio runtime before `SyncEngine::run` is called and returns
typed `SyncCoreError` values for configuration and consumer failures.
Library diagnostics use `tracing` without embedded ANSI escapes; applications
own subscriber configuration. Do not add terminal styling to library messages.
`SyncEngine::builder` is infallible because it only stores the progress store;
keep `Builder::add_synchronizer` and `Builder::add_height_provider` fallible
because validation happens when each entity is registered.

Engine metrics are private global collectors registered through
`stonfi_metrics::register_metrics!`. Applications call
`stonfi_metrics::init_metrics!` during startup, before running an engine, to
initialize the global registry and optionally serve `/metrics`. Metric helpers
access the registered cells directly and therefore panic if startup skipped
initialization. Do not initialize individual metric cells from engine
constructors, add redundant availability checks, expose per-engine
collector APIs, or expose Prometheus types publicly.
All engine metric series use `handler_id` for the height-provider or
synchronizer identifier label.
Height gauges store `u64`, but Prometheus exposition converts numeric samples
to `f64` and may lose unit precision above `2^53`; this does not narrow the
engine height domain. The optional ScyllaDB store is narrower because its CQL
`bigint` column supports heights only through `i64::MAX`.

`ScyllaProgressStore::builder` requires the initial-height fallback. Configure
either a prebuilt `ScyllaClient` or both endpoints and an existing keyspace;
the latter uses the client's defaults. Building applies the idempotent table
migration and calls `use_keyspace`, so initialize `stonfi_metrics` first. The
table defaults to `sync_progress`; custom names must remain unquoted CQL
identifiers. The migration creates no keyspace and performs no legacy backfill.
Client requests have bounded internal retries, while the synchronizer owns the
outer retry loop. Do not add another retry or lock inside the store.

## Invariants and pitfalls

- Every height loader and sync handler ID must be unique within one
  engine.
- Dependency graphs must be acyclic. The builder subscribes to providers but
  does not perform graph discovery or cycle detection.
- `INITIAL_HEIGHT` (`"INITIAL_HEIGHT"`) is the reserved initial-height key in
  the handler-ID namespace. Progress stores receive it through their `handler_id`
  arguments, but it must never identify a height provider, synchronizer, or
  dependency-graph entity.
- Only one active engine may write a given handler ID. The progress-store API is
  not compare-and-set storage; this also applies to `INITIAL_HEIGHT`.
- `SyncProgressStore::load_synced_or_initial` prefers per-handler state, then the
  persisted `INITIAL_HEIGHT` state, and only then stores and returns the
  configured fallback.
- `SyncProgressStore::load_initial_height` reads the persisted `INITIAL_HEIGHT`
  state without applying or storing the configured fallback.
- `SyncHeight` is `u64`; height `0` remains the initial no-progress sentinel.
- `ScyllaProgressStore` rejects heights above `i64::MAX` and negative values read
  from its CQL `bigint` column.
- Batch sizes are positive, fit in `SyncHeight`, and satisfy `min <= max`.
- `sync_range(from, to)` processes an inclusive range and may report only a
  height in that range unless `allow_rewind()` is enabled.
- Returning `Ok(None)` defers progress; it does not commit or publish a height.
- Callback failures are retried only while the engine is active. Callbacks must
  be idempotent because earlier callbacks may replay; delivery is not durable
  across shutdown or restart.
- Initial source discovery invokes `on_height_loaded`; a nonzero initial value
  is published and then invokes `on_height_published`, both from previous
  height `0`.
- An upstream progress decrease does not rewind dependants or cancel progress
  selected by an active wait. Each handler controls its own rewind behavior
  through `allow_rewind()`; subsequent waits use current provider values.
- Retry loops are cooperative. Consumer futures must return or enforce their
  own timeout if bounded shutdown latency is required.
- Pass `SyncHandler::sync_timeout()` and lifecycle shutdown durations directly
  to Tokio without zero-specific normalization or validation.
- Do not add parallel builders, aliases, convenience re-exports, or alternate
  lifecycle APIs without a demonstrated downstream need.
- Production paths must not use `unwrap()`, `expect()`, or panic-driven control
  flow.
- Keep metric names, labels, buckets, and meanings stable because dashboards
  and alerts consume them as a behavioral contract.

## Changing the crate

For public API, behavior, feature, workspace, dependency, or package-surface
changes, review and update the README, rustdoc, example, tests, this guide, CI,
and package include rules in the same change, or record why an artifact is
unaffected. Keep the changelog concise and consumer-facing. Add deterministic
tests for owned behavior and failure modes, not for third-party behavior or
metric registration.

Use `Result`-returning Rust tests with `?` whenever a called operation is
fallible. Keep changes narrow and avoid refactoring the synchronizer state
machine unless correctness requires it. Do not introduce a
second public path for an existing capability.

## Validation

Fast gate:

```text
cargo test -p stonfi_sync_core --all-features --locked
cargo clippy -p stonfi_sync_core --all-targets --all-features --locked -- -D warnings
```

Full gate:

```text
cargo test -p stonfi_sync_core --doc --locked
cargo test -p stonfi_sync_core --examples --all-features --locked
cargo test -p stonfi_sync_core --features scylla --test scylla_progress_store --locked -- --ignored --test-threads=1
cargo +nightly fmt --check
RUSTDOCFLAGS="-D warnings -D missing_docs" cargo doc -p stonfi_sync_core --no-deps --all-features --locked
cargo +1.95.0 check -p stonfi_sync_core --all-features --locked
cargo package --list --locked -p stonfi_sync_core
cargo publish --dry-run --locked -p stonfi_sync_core
```

Release-plz publishes this package before a changed `stonfi_distributed_sync`
version that depends on it. Do not change the distribution or versioning policy
unless the task explicitly requires it.
