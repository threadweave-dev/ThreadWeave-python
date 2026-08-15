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
