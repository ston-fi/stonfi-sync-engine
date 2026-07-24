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

Keep the established root and module paths. `SyncEngine`, `Builder`,
`Initiator`, `Synchronizer`, `RunHandle`, and `MemStatusManager` are the primary
consumer types. The five `Sync*` traits above are intentional downstream
extension points and must remain externally implementable.

Prefer `SyncEngine::builder`, add synchronizers with explicit triggers, add the
corresponding initiators, then build and run. Dropping the engine signals
cooperative shutdown; await `RunHandle::wait` when task completion matters.

Consumers use the Git dependency documented in `README.md`. The crate requires
a running Tokio runtime before `SyncEngine::run` is called and returns typed
`SyncCoreError` values for configuration and consumer failures.

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
- Callback delivery is at least once, so callbacks must be idempotent.
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
`publish = false`; do not enable crates.io publishing or change versions and
tags unless a release task explicitly requires it.
