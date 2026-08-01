# Changelog

All notable changes to `stonfi_distributed_sync` are documented here.

## Unreleased

- Add the initial public distributed synchronization coordinator, gRPC task
  server, worker, payload contract, lifecycle handles, and metrics.
- Introduce the versioned `stonfi.distributed_sync.v1` protocol as a deliberate
  clean break from Tongrid's private `distributed_sync` implementation.
- Reimplement the behavior reviewed at Tongrid revision
  `7bdf790d2e081112ecb733fa5450694114c12b62` without Tongrid or Commons
  dependencies.
- Name the shared process-local coordination state
  `coordinator::Coordinator`; no earlier public path has been released.
- Delegate metric-cell initialization exclusively to application startup
  through `stonfi_metrics::init_metrics!`.
- Keep assignment routing worker-agnostic except for service-task support;
  workers report missing handlers as retryable task failures.
- Keep long-poll requests outside worker processing capacity so idle polls
  cannot starve exclusive service tasks.
- Back off failed task attempts within the synchronization deadline.
- Use one batch deadline across task creation, coordinator queueing, worker
  capacity waits, and task processing.
- Replace server and worker configuration structs with private-module builders,
  default worker parallelism to the system's available parallelism, and simplify queue and run
  handle internals without changing dispatch behavior.
- Document every direct dependency required by the canonical consumer example.
- Clarify service-queue preference and priority/FIFO ordering, and relax
  `TaskPayload` values from `Send + Sync` to `Send`.
