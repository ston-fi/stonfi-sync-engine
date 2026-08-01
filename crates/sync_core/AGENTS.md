# `stonfi_sync_core` Agent Guide

This package is a public Rust library distributed through Git tags as part of
the `stonfi-sync-engine` workspace.
Use the `rust-library-review` skill for non-trivial reviews, implementations,
refactors, and release preparation.

## Responsibility and workspace boundary

The `crates/sync_core` package coordinates dependency-aware synchronization of
ordered heights:

- `HeightLoader` loads the latest available height from an upstream source.
- `SyncHandler` processes bounded inclusive ranges.
- `SyncStatusStore` persists committed progress and owns the configured initial
  height fallback.
- `ProgressProvider` supplies progress subscriptions that connect height
  providers and synchronizers into a dependency graph.
- `SyncCallback` observes lifecycle events.

The crate does not provide distributed locking, multi-writer conflict
resolution, storage, a Tokio runtime, or distributed task transport. Those
transport concerns belong to the sibling `crates/distributed_sync` package; do
not add gRPC, protobuf, server, or worker dependencies to the core package.

## Public API and ownership

Keep public paths module-qualified: errors and result types live in `errors`,
the in-memory implementation lives in `mem_status_store`, and all engine
types and extension traits live in `sync_engine`. Do not add root re-exports.
`SyncEngine`, `Builder`, `HeightProvider`, `Synchronizer`, `RunHandle`, and
`MemStatusStore` are the primary consumer types. `HeightLoader`, `SyncHandler`,
`SyncStatusStore`, `ProgressProvider`, and `SyncCallback` are intentional
downstream extension points and must remain externally implementable.

Public ID boundaries use `&str`; store IDs privately as `String` only where
ownership is required. Height loaders and handlers belong to one task and
require `Send + 'static`, not `Sync`. `SyncHandler::sync_range` takes
`&mut self` so stateful handlers do not need internal synchronization. Status
stores and callbacks are shared across tasks and require `Send + Sync + 'static`.

Prefer `SyncEngine::builder`, add synchronizers with references to their
upstream progress providers, add the corresponding height providers, then
build and run. The builder subscribes to progress before retaining single
ownership of height providers and synchronizers. `SyncEngine::run` consumes
the engine definition and returns the runtime owner. Use `RunHandle::shutdown`
for bounded awaited shutdown; dropping the handle only signals best-effort
shutdown. Use `RunHandle::wait` only when every task can finish naturally.

Consumers use the Git dependency documented in `README.md`. The crate requires
a running Tokio runtime before `SyncEngine::run` is called and returns typed
`SyncCoreError` values for configuration and consumer failures.
Library diagnostics use `tracing` without embedded ANSI escapes; applications
own subscriber configuration. Do not add terminal styling to library messages.
`SyncEngine::builder` is infallible because it only stores the status store;
keep `Builder::add_synchronizer` and `Builder::add_height_provider` fallible because
validation happens when each entity is registered.

Engine metrics are private global collectors registered through
`stonfi_metrics::register_metrics!`. Applications call
`stonfi_metrics::init_metrics!` during startup, before running an engine, to
initialize the global registry and optionally serve `/metrics`. Metric helpers
access the registered cells directly and therefore panic if startup skipped
initialization. Do not initialize individual metric cells from engine
constructors, add redundant availability checks, reintroduce per-engine
collector APIs, or expose Prometheus types publicly.
All engine metric series use `component_id` for the height-provider or
synchronizer identifier label.
Height gauges store `u64`, but Prometheus exposition converts numeric samples
to `f64` and may lose unit precision above `2^53`; this does not narrow the
engine or status-store height domain.

## Invariants and pitfalls

- Every height provider and synchronizer component ID must be unique within one
  engine.
- `INITIAL_SYNC_ID` (`"INITIAL"`) is reserved for the status store's durable
  engine-wide baseline and must never identify a height provider, synchronizer,
  or dependency-graph entity.
- Only one active engine may write a given sync ID. The status-store API is
  not compare-and-set storage; this also applies to `INITIAL_SYNC_ID`.
- `SyncStatusStore::load_synced_or_initial` prefers per-sync state, then the
  persisted `INITIAL` state, and only then stores and returns the configured
  fallback.
- `SyncHeight` is `u64`; height `0` remains the initial no-progress sentinel.
- Batch sizes are positive, fit in `SyncHeight`, and satisfy `min <= max`.
- `sync_range(from, to)` processes an inclusive range and may report only a
  height in that range unless `allow_rewind()` is enabled.
- Returning `Ok(None)` defers progress; it does not commit or publish a height.
- Callback failures are retried only while the engine is active. Callbacks must
  be idempotent because earlier callbacks may replay; delivery is not durable
  across shutdown or restart.
- An upstream progress decrease does not rewind dependants or cancel progress
  selected by an active wait. Each handler controls its own rewind behavior
  through `allow_rewind()`; subsequent waits use current provider values.
- Retry loops are cooperative. Consumer futures must return or enforce their
  own timeout if bounded shutdown latency is required.
- Do not add parallel builders, aliases, convenience re-exports, or alternate
  lifecycle APIs without a demonstrated downstream need.
- Production paths must not use `unwrap()`, `expect()`, or panic-driven control
  flow.
- Keep metric names, labels, buckets, and meanings stable because dashboards
  and alerts consume them as a behavioral contract.

## Changing the crate

For public API, behavior, feature, workspace, dependency, or package-surface
changes, review and update the README, rustdoc, example, tests, this guide,
changelog, CI, and package include rules in the same change, or record why an
artifact is unaffected. Add deterministic tests for owned behavior and failure
modes, not for third-party behavior or metric registration.

Use `Result`-returning Rust tests with `?` whenever a called operation is
fallible. Keep changes narrow and avoid refactoring the synchronizer state
machine unless correctness cannot be restored locally. Do not introduce a
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
cargo test -p stonfi_sync_core --examples --locked
cargo +nightly fmt --check
RUSTDOCFLAGS="-D warnings -D missing_docs" cargo doc -p stonfi_sync_core --no-deps --all-features --locked
cargo +1.93.0 check -p stonfi_sync_core --all-features --locked
cargo package --list --locked -p stonfi_sync_core
```

GitHub CI owns these validation gates. The package is intended for Git
distribution, has no remote release yet, and has `publish = false`. Release-plz
runs only after the quality, MSRV, and external-consumer jobs pass on `main`,
and creates the Git tag and GitHub Release without publishing to crates.io. Do
not add a registry token or manually change versions and tags unless a release
task explicitly requires it.
