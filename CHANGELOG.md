# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- Release automation: the changelog fold is now an in-repo transform with
  Keep a Changelog's own link conventions; the third-party action enforced
  a private link format and failed on our first-release links. No library
  changes.

## [0.1.1] - 2026-11-21

### Fixed

- Release automation: the changelog fold for a published release now lands
  through a pull request, satisfying the repository rule that requires PRs
  for `main`; the previous direct push was rejected. No library changes.

## [0.1.0] - 2026-11-21

### Added

- `ThreadAffinityExecutor` library: `ThreadID` (cross-platform OS thread
  identity), the `ThreadBoundExecutor` protocol (`SerialExecutor` +
  `TaskExecutor` pinned to one OS thread), `DedicatedThreadExecutor`
  (library-owned worker thread), `AttachedThreadExecutor` +
  `FixedThreadDispatcher` (attach to any fixed-thread mechanism;
  `MainQueueDispatcher` for the main queue), `QueueDispatcher` (pump for
  threads without an event loop), shared on-thread lifecycle hooks
  (`ThreadLifecycleDelegate`, `ClosureLifecycle`), and the prototype
  `MigratingThreadExecutor` with `ThreadGenerationDelegate`.
- `BlockingQueue` library: `UnboundedQueue`, `BoundedQueue` and the lock-free
  `LinkedQueue`, each split into statically shaped producer / consumer /
  supervisor ends (`.spsc` / `.mpsc` / `.spmc` / `.mpmc`), plus the shared
  capability protocols and the generic `QueueEnds` bundle.
- Ownership-transfer elements: queue elements need not be `Sendable` and may
  be `~Copyable`; the handoff is compiler-checked (`sending`).

[Unreleased]: https://github.com/ghostflyby/swift-thread-affinity-executor/compare/0.1.1...HEAD
[0.1.1]: https://github.com/ghostflyby/swift-thread-affinity-executor/compare/0.1.0...0.1.1
[0.1.0]: https://github.com/ghostflyby/swift-thread-affinity-executor/releases/tag/0.1.0
