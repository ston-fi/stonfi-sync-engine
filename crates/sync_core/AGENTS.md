# `stonfi_sync_core` Agent Guide

This package is a public Rust library distributed through Git tags as part of
the `stonfi-sync-engine` workspace.
Use the `rust-library-review` skill for non-trivial reviews, implementations,
refactors, and release preparation.

## Responsibility and workspace boundary

The `crates/sync_core` package coordinates dependency-aware synchronization of
ordered heights:

- `SyncInitiator` discovers upstream progress.
- `SyncHandler` processes bounded inclusive ranges.
- `SyncStatusManager` persists committed progress.
- `SyncTrigger` connects initiators and synchronizers into a dependency graph.
- `SyncCallback` observes lifecycle events.

The crate does not provide distributed locking, multi-writer conflict
resolution, storage, a Tokio runtime, or distributed task transport. Add the
planned distributed implementation as a separate workspace package under
`crates/distributed_sync`; do not add gRPC, protobuf, task-server, or worker
dependencies to the core package or hide that surface behind a core feature.

## Public API and ownership

Keep public paths module-qualified: errors and result types live in `errors`,
the in-memory implementation lives in `mem_status_manager`, and all engine
types and extension traits live in `sync_engine`. Do not add root re-exports.
`SyncEngine`, `Builder`, `Initiator`, `Synchronizer`, `RunHandle`, and
`MemStatusManager` are the primary consumer types. The five `Sync*` traits above
are intentional downstream extension points and must remain externally
implementable.

Public ID boundaries use `&str`; store IDs privately as `String` only where
ownership is required. Initiators and handlers belong to one task and require
`Send + 'static`, not `Sync`. `SyncHandler::sync_range` takes `&mut self` so
stateful handlers do not need internal synchronization. Status managers and
callbacks are shared across tasks and require `Send + Sync + 'static`.

Prefer `SyncEngine::builder`, add synchronizers with explicit triggers, add the
corresponding initiators, then build and run. `SyncEngine::run` consumes the
engine definition and returns the runtime owner. Use `RunHandle::shutdown` for
awaited cooperative shutdown; dropping the handle only signals best-effort
shutdown. Use `RunHandle::wait` only when every task can finish naturally.

Consumers use the Git dependency documented in `README.md`. The crate requires
a running Tokio runtime before `SyncEngine::run` is called and returns typed
`SyncCoreError` values for configuration and consumer failures. Keep
`SyncEngine::builder`, `Builder::add_sync`, and `Builder::add_initiator`
fallible: validation intentionally happens at the boundary where each invalid
state can first be detected.

Engine metrics are private global collectors registered through
`stonfi_metrics`. `SyncEngine::builder` initializes them and preserves its
fallible return type. Applications call `stonfi_metrics::init_metrics!` during
startup to initialize the global registry and optionally serve `/metrics`; do
not reintroduce per-engine collector APIs or expose Prometheus types publicly.

## Invariants and pitfalls

- Every initiator and synchronizer ID must be unique within one engine.
- Only one active engine may write a given sync ID. The status-manager API is
  not compare-and-set storage.
- Range limits are positive, fit in `SyncHeight`, and satisfy `min <= max`.
- `sync_range(from, to)` processes an inclusive range and may report only a
  height in that range unless `allow_wrap()` is enabled.
- Returning `Ok(None)` defers progress; it does not commit or publish a height.
- Callback failures are retried only while the engine is active. Callbacks must
  be idempotent because earlier callbacks may replay; delivery is not durable
  across shutdown or restart.
- An upstream trigger decrease does not rewind dependants or cancel progress
  selected by an active wait. Each handler controls its own wrap behavior
  through `allow_wrap()`; subsequent waits use current trigger values.
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

GitHub CI owns these validation gates. The package is Git-distributed and has
`publish = false`. Release-plz runs only after the quality and MSRV jobs pass
on `main`, and creates the Git tag and GitHub Release without publishing to
crates.io. Do not add a registry token or manually change versions and tags
unless a release task explicitly requires it.
