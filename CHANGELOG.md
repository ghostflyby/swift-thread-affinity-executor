# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `RunLoopThreadExecutor`: a `ThreadBoundExecutor` whose dedicated thread
  waits in a `CFRunLoop` instead of a blocking queue take. Jobs are drained
  through a run-loop source; `enqueue` signals the source and wakes the loop,
  so an idle executor reacts immediately. Timers and other run-loop sources
  registered on the bound thread run between job batches. A per-turn budget
  keeps a sustained backlog from starving them, and a nested run loop in a
  job never re-enters job execution. Shutdown re-signals the source so an
  enclosing invocation still observes the close when a callback pumped a
  nested run loop. Requires CoreFoundation and Dispatch.
- `MainThreadExecutor`: the main-thread executor protocol.
  `mainThreadExecutor` is the main-thread-bound view; its first off-main
  access blocks until the main queue runs while the main thread ID is
  captured. `mainRunLoopExecutor` returns the `RunLoopServicingExecutor`
  view after a precondition that the main run loop is running; it traps
  otherwise. Requires CoreFoundation and Dispatch.
- `RunLoopServicingExecutor`: a capability refinement of
  `ThreadBoundExecutor` for executors whose bound thread services a run
  loop. `RunLoopThreadExecutor` conforms; `mainRunLoopExecutor` returns a
  conforming view; `DedicatedThreadExecutor` and `MigratingThreadExecutor`
  do not.

## [0.2.0] - 2026-11-26

### Added

- `DedicatedThreadExecutor.shutdown() async`: suspends the calling task
  until the worker drained and exited, instead of blocking a thread. The
  synchronous `shutdown()` remains for synchronous contexts.
- `ThreadAffinityExecutor`: the common protocol of the executor families.
  Both `ThreadBoundExecutor` and `MigratingThreadExecutor` refine it.
  `currentThreadID` names the thread this executor's jobs run on — it
  switches at the migration barrier, not when routing stops — and the
  thread-based `isIsolatingCurrentContext()` moved to it from
  `ThreadBoundExecutor`.
- `MigratingThreadExecutor` migration notifications: the executor owns a
  `NotificationCenter` (`notificationCenter`, a fresh one by default; pass
  `notificationCenter:` to share one). The public identity is the
  `MigrationNotification` enum: `.leavingBoundThread` is posted on the old
  bound thread after the last job accepted before the cut and before the
  switch is published; `.enteringBoundThread` is posted on the new bound
  thread before the jobs released by the migration. Both carry the same
  `MigrationEvent`, unboxed by the typed
  `NotificationCenter.addObserver(for:object:queue:using:)` overload; the
  underlying `Notification.Name` is internal. Observers registered with a
  nil queue run synchronously on the bound thread; a non-nil queue delivers
  asynchronously. Register with `addObserver(for:queue:using:)` on the
  executor (or the center overload); the returned token removes its
  registration with `remove()` (weak center, idempotent).
  Observers registered on the executor's center with the executor as object
  are removed when it deinitializes; the executor retains their observer
  tokens until then, and removing a registration releases its handler.

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
- **Breaking:** `DedicatedThreadExecutor.init(name:)` no longer takes a
  lifecycle and no longer throws. On-thread setup and teardown are the
  caller's to compose.
- **Breaking:** `MigratingThreadExecutor` always posts the entering
  notification on the new bound, so a new bound executor that already
  stopped leaves the migration pending instead of silently discarding the
  staged jobs.

### Removed

- **Breaking:** the attach family and its lifecycle hooks:
  `AttachedThreadExecutor`, `AttachError`, `FixedThreadDispatcher`,
  `ThreadPumpDelegate`, `MainQueueDispatcher`, `QueueDispatcher`,
  `ThreadLifecycleDelegate` and `ClosureLifecycle`. Executors no longer
  accept caller-supplied on-thread setup or teardown.
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

[Unreleased]: https://github.com/ghostflyby/swift-thread-affinity-executor/compare/0.2.0...HEAD
[0.2.0]: https://github.com/ghostflyby/swift-thread-affinity-executor/compare/0.1.2...0.2.0
[0.1.2]: https://github.com/ghostflyby/swift-thread-affinity-executor/compare/0.1.1...0.1.2
[0.1.1]: https://github.com/ghostflyby/swift-thread-affinity-executor/compare/0.1.0...0.1.1
[0.1.0]: https://github.com/ghostflyby/swift-thread-affinity-executor/releases/tag/0.1.0
