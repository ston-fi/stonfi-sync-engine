# Changelog

All notable changes to this project are documented in this file.

## Unreleased

- Delegate metric-cell initialization exclusively to
  `stonfi_metrics::init_metrics!` and access registered collectors directly
  from metric helpers.
- Make `SyncEngine::builder` infallible and keep validation on the registration
  methods that can actually reject input.
- Observe engine task completion concurrently, reporting a later task failure
  even when an earlier task does not terminate.
- Document at-least-once handler range delivery and the idempotency and
  cancellation requirements it places on handler effects.

## 0.0.1

- Import the dependency-aware synchronization engine into its public repository.
- Give running engines explicit cooperative shutdown ownership through
  `RunHandle` and one engine-owned cancellation signal, including awaited
  shutdown and task failure reporting.
- Clarify process-local callback retries and independent synchronizer wrap
  behavior.
- Establish module-qualified public paths, replace `SyncID` with borrowed
  `&str` boundaries, and let task-owned initiators and mutable handlers require
  only `Send`.
- Migrate engine metrics to `stonfi_metrics` v0.0.1 while preserving their names and semantics.
- Place the package under `crates/sync_core` in a virtual workspace prepared
  for the future separate `crates/distributed_sync` package.
- Add strict public-library validation and Git-only GitHub release automation.
