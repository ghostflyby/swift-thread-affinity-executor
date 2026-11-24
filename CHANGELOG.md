# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `DedicatedThreadExecutor.shutdown() async`: suspends the calling task
  until the worker drained, ran `finalize` and exited, instead of blocking a
  thread. The synchronous `shutdown()` remains for synchronous contexts.
- `Affinity`: the common protocol of the executor families. Both
  `ThreadBoundExecutor` and `MigratingThreadExecutor` refine it.
  `currentThreadID` names the thread jobs accepted right now run on, and the
  thread-based `isIsolatingCurrentContext()` moved to it from
  `ThreadBoundExecutor`.

### Changed

- **Breaking:** `MigratingThreadExecutor` is rebuilt over thread-bound
  executors instead of pumps. It is constructed with `init(bound:)` on an
  existing `ThreadBoundExecutor` and moves with `migrate(to:)`. Bound
  executors are borrowed: a migration never ends one. The migration cut is
  acceptance — jobs accepted before it finish on the old bound executor,
  later ones run on the new one.
- **Breaking:** `ThreadBoundExecutor` now requires jobs to run in acceptance
  order. `SerialExecutor` alone permits reordering, and the migrating
  executor's barrier depends on it.

### Removed

- **Breaking:** `ThreadGenerationDelegate` and `ClosureGenerationDelegate`,
  along with the pump-based `MigratingThreadExecutor` API: `attach(to:)`,
  `handOver(to:)`, `terminate()`, `current` and `Generation`.
  `MigrationError` keeps only `busy`.

## [0.1.2] - 2026-11-21

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

[Unreleased]: https://github.com/ghostflyby/swift-thread-affinity-executor/compare/0.1.2...HEAD
[0.1.2]: https://github.com/ghostflyby/swift-thread-affinity-executor/compare/0.1.1...0.1.2
[0.1.1]: https://github.com/ghostflyby/swift-thread-affinity-executor/compare/0.1.0...0.1.1
[0.1.0]: https://github.com/ghostflyby/swift-thread-affinity-executor/releases/tag/0.1.0
