# Changelog

All notable changes to the ThreadWeave Python SDK are documented in this file.

## [0.1.8] - 2026-08-15

### Added

- Synchronous worker runtime protocol client for acquiring, starting, completing,
  and failing executions through the generated gRPC API.
- Unit tests around the generated runtime service stub and execution lifecycle
  reports.

### Changed

- Updated the worker runtime and CLI to use the dedicated runtime protocol client,
  keeping worker operations separate from the user-facing protocol client.

## [0.1.1] - 2026-08-13

### Added

- Synchronous and asynchronous APIs for applications, tasks, and jobs.
- Synchronous and asynchronous gRPC protocol clients.
- Core process lifecycle support and resource reservation documentation.
- Public API, protocol, and gRPC proof-of-concept tests.

### Changed

- Updated the generated ThreadWeave protocol dependencies to support accepted and
  rejected job states.

[0.1.8]: https://github.com/threadweave-dev/threadweave-python/compare/v0.1.7...v0.1.8
[0.1.1]: https://github.com/threadweave-dev/threadweave-python/compare/v0.1.0...v0.1.1

## [0.1.12] - 2026-08-16
### Async runtime sessions

- Migrated `RuntimeProtocolClient` to `grpc.aio`.
- Replaced repeated `AcquireExecution` calls with one persistent runtime session.
- Added asynchronous lifecycle, progress, metrics, failure, and heartbeat reporting.
- Synchronous user tasks now execute through `asyncio.to_thread`, keeping the event loop responsive.
- Added separate deserialization, user-function execution, and serialization timings.
- Added handling for Worker-issued cancellation commands.
- Multiple assignments can reuse the same runtime session.

### Breaking changes

The runtime client and runtime loop are now asynchronous. Integrations must await
connection, event iteration, lifecycle reporting, and shutdown.

### Cancellation limitation

Cancelling an execution stops the awaiting coroutine but cannot safely terminate
arbitrary synchronous Python code already running in a worker thread. Strong
cancellation will require process-based task isolation.