# Changelog

All notable changes to this project are documented in this file.

## 0.0.1

- Import the dependency-aware synchronization engine into its public repository.
- Preserve the established engine API, lifecycle, callback, and retry behavior.
- Migrate engine metrics to `stonfi_metrics` v0.0.1 while preserving their names and semantics.
- Place the package under `crates/sync_core` in a virtual workspace prepared
  for the future separate `crates/distributed_sync` package.
