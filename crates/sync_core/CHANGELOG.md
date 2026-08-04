# Changelog

All notable consumer-facing changes to `stonfi_sync_core` are documented here.

## [0.2.1](https://github.com/ston-fi/stonfi-sync-engine/compare/stonfi_sync_core-v0.2.0...stonfi_sync_core-v0.2.1) - 2026-08-04

### Other

- support untracked Cargo lockfile
- up version
- up version
- add initial height loader

## [0.1.0](https://github.com/ston-fi/stonfi-sync-engine/releases/tag/stonfi_sync_core-v0.1.0) - 2026-08-04

- Let engine builder registrations accept height loaders and sync handlers
  directly through their `HeightProvider` and `Synchronizer` conversions. Pass
  the implementation directly instead of calling `.into()` inline.
- Pass synchronization and shutdown durations directly to Tokio, including
  zero, and make `Builder::with_shutdown_timeout` infallible.
- Publish the package on crates.io and use the crates.io release of
  `stonfi_metrics`.

## [0.0.1](https://github.com/ston-fi/stonfi-sync-engine/releases/tag/v0.0.1) - 2026-08-03

- Introduce the module-qualified synchronization API with `u64` heights,
  stateful handlers, progress providers, callbacks, and status stores.
- Move initial-height ownership and persistence into `SyncStatusStore`.
- Add owned engine lifecycle management with bounded shutdown and task failure
  reporting.
- Integrate startup-initialized metrics through `stonfi_metrics` and document
  runtime, retry, rewind, dependency-graph, and single-writer contracts.
