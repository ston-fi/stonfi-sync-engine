# Changelog

All notable changes to this project are documented in this file.

## Unreleased

- Replace the initiator/trigger vocabulary with `HeightLoader`,
  `HeightProvider`, and `ProgressProvider`. Rename registration to
  `Builder::add_height_provider`, progress subscriptions to
  `ProgressProvider::subscribe`, callback hooks to `on_height_load_error`,
  `on_height_loaded`, and `on_height_published`, and channel aliases to
  `ProgressReceiver` and private `ProgressSender`.
- Rename the source-height metric to `sync_engine_last_loaded_height`, its
  retry phase label to `HeightLoad`, and the common metric label to
  `component_id`.
- Move the initial synced-height fallback from `SyncHandler` into
  `SyncStatusStore`. Stores now provide a configured fallback and
  `load_synced_or_initial`, persist it under the reserved `INITIAL_SYNC_ID`,
  and prefer existing per-sync or initial state.
- Rename `MemStatusManager` to `MemStatusStore` and its module to
  `mem_status_store`.
- Emit diagnostics through `tracing` without embedded ANSI escape sequences.
- Use `u64` for `SyncHeight` while preserving height `0` as the initial
  no-progress sentinel. Height gauges use unsigned storage; Prometheus
  exposition may lose unit precision above `2^53` while engine and stored
  heights retain full `u64` precision.
- Use explicit synchronization names: `latest_height`, `min_batch_size`,
  `max_batch_size`, `retry_delay`, `allow_rewind`, `processed_to`,
  `Builder::add_synchronizer`, and `SyncStatusStore`.
- Delegate metric-cell initialization exclusively to
  `stonfi_metrics::init_metrics!` and access registered collectors directly
  from metric helpers.
- Make `SyncEngine::builder` infallible and keep validation on the registration
  methods that can actually reject input.
- Observe engine task completion concurrently, reporting a later task failure
  even when an earlier task does not terminate.
- Document at-least-once handler range delivery and the idempotency and
  cancellation requirements it places on handler effects.
- Bound cooperative engine shutdown and abort stuck consumer tasks after the
  configured timeout.
- Reject empty and whitespace-padded component IDs and support the maximum remaining
  `SyncHeight` range without intermediate overflow.
