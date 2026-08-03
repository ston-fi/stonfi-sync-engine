# Changelog

All notable consumer-facing changes to `stonfi_distributed_sync` are documented
here.

## Unreleased

## [0.0.1](https://github.com/ston-fi/stonfi-sync-engine/releases/tag/v0.0.1) - 2026-08-03

- Add the initial public distributed synchronization package with an in-memory
  coordinator, gRPC task server, workers, and one typed handler extension point.
- Provide at-least-once task delivery, ordered batch results, priority/FIFO
  queues, exclusive service tasks, bounded coordinator concurrency, and shared
  absolute deadlines across retries and processing.
- Add owned server and worker lifecycle handles with bounded shutdown.
- Document the trusted-network boundary, versioned `v1` wire protocol,
  idempotency requirements, metrics, and complete integration workflow.
