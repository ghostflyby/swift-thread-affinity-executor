//
//  Affinity.swift
//  ThreadAffinityExecutor
//
//  The common type of the two executor families: at any moment, every job
//  runs on one known OS thread.
//

/// An executor whose jobs all run on one known OS thread at any moment.
///
/// Both executor families refine it. A `ThreadBoundExecutor` keeps that
/// thread for its whole lifetime; a `MigratingThreadExecutor` changes it
/// only through an explicit migration. What every conformance must uphold:
///
/// 1. **One current thread** — `currentThreadID` names the thread that runs
///    the jobs accepted right now, and no job of this executor runs
///    elsewhere.
/// 2. **Ordered exchange** — when the current thread changes, every job
///    accepted before the change completes before any later job starts.
public protocol Affinity: SerialExecutor, TaskExecutor {
  /// The OS thread running the jobs accepted right now.
  var currentThreadID: ThreadID { get }
}

extension Affinity {
  /// Dynamic isolation check consumed by the Swift 6.4+ runtime after
  /// regular executor-identity comparison fails.
  ///
  /// A synchronous caller running on the current thread is isolated with
  /// respect to this executor's jobs. Because the check inspects the
  /// calling thread, it also holds across executors that share the thread —
  /// two `ThreadBoundExecutor` objects on one OS thread, or an actor bound
  /// to a `MigratingThreadExecutor` reached from its current bound
  /// executor.
  ///
  /// The package requires Swift 6.4, so the requirement is available on
  /// every supported platform.
  public func isIsolatingCurrentContext() -> Bool? {
    currentThreadID == ThreadID.current
  }
}
