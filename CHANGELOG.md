# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

### Notes

- Requires Swift 6.4. Deployment floors: macOS 15 / iOS 18 / tvOS 18 /
  watchOS 11 / visionOS 2.
- `MigratingThreadExecutor` is experimental and may still change.
- Executors impose a quiescence contract before `shutdown()` / `detach()`
  (see README).

[Unreleased]: https://github.com/ghostflyby/swift-thread-affinity-executor/compare/0.1.0...HEAD
