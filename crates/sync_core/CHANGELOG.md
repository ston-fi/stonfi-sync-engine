# Changelog

All notable consumer-facing changes to `stonfi_sync_core` are documented here.

## Unreleased

## 0.1.0 - 2026-08-03

- Introduce the module-qualified synchronization API with `u64` heights,
  stateful handlers, progress providers, callbacks, and status stores.
- Move initial-height ownership and persistence into `SyncStatusStore`.
- Add owned engine lifecycle management with bounded shutdown and task failure
  reporting.
- Integrate startup-initialized metrics through `stonfi_metrics` and document
  runtime, retry, rewind, dependency-graph, and single-writer contracts.

This release intentionally replaces the incompatible `0.0.1` API.
