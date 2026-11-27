//
//  ThreadBoundExecutor.swift
//  ThreadAffinityExecutor
//

/// A `SerialExecutor` and `TaskExecutor` whose every accepted job is executed
/// by one fixed OS thread for the executor's whole effective lifetime.
///
/// The protocol deliberately leaves construction, thread ownership, queue
/// mechanics, loop style and shutdown policy to concrete executors. What
/// every conformance must uphold:
///
/// 1. **Unique thread** — all jobs run on the same OS thread from the moment
///    the executor becomes available until it leaves its effective lifetime.
/// 2. **Stable identity** — `threadID` never changes after publication. A
///    bound executor never migrates: work moves to another thread only by
///    being routed through a different executor, as
///    `MigratingThreadExecutor` does.
/// 3. **Real submission** — `enqueue` is safe from any thread and the job is
///    eventually handed to the bound thread.
/// 4. **Serialization** — jobs obey `SerialExecutor` mutual exclusion and
///    run in acceptance order (`SerialExecutor` alone permits reordering);
///    no two jobs run concurrently.
/// 5. **Synchronous isolation correctness** — any synchronous call observing
///    `isIsolatingCurrentContext() == true` is properly ordered with the
///    executor's jobs; custom conformances must audit nested-loop
///    re-entrancy.
/// 6. **Complete execution** — every job enqueued while the executor is within
///    its effective lifetime runs exactly once; jobs are never duplicated or
///    silently discarded. The documented shutdown drop window — jobs arriving
///    after the termination request — is the sole exception.
/// 7. **Lifetime validity** — while tasks or actors may still submit jobs,
///    the bound thread and the scheduling channel remain usable.
public protocol ThreadBoundExecutor: ThreadAffinityExecutor {
  /// The OS thread this executor is bound to. Stable after publication.
  var threadID: ThreadID { get }
}

/// A `ThreadBoundExecutor` whose bound thread also services a run loop.
///
/// The affinity contract is unchanged: jobs run serially, in acceptance
/// order, on one OS thread. The capability adds that the thread's run loop
/// is driven when the thread is idle, so timers, run-loop sources and
/// `perform` blocks registered on the bound thread are serviced between job
/// batches. Run-loop callbacks are not executor jobs; they run on the bound
/// thread but outside `runJob`, and must not wait for a job of this
/// executor. The thread-based isolation check reports `true` in them; a
/// callback dispatched while a job pumps a nested run loop is re-entrant
/// with that job, so it must not assume the job's actor state is quiescent.
///
/// The refinement deliberately does not expose the run loop itself: it is
/// thread-affine and not `Sendable`. Code that needs it reads
/// `RunLoop.current` from synchronous code on the bound thread — the
/// accessor is marked `noasync`, so inside a job put it in a synchronous
/// helper.
///
/// A `MigratingThreadExecutor` does not conform: a migration may switch to a
/// bound executor without a run loop, so the capability is not stable across
/// migrations. Require this protocol only from executors whose binding is
/// fixed.
public protocol RunLoopServicingExecutor: ThreadBoundExecutor {}

extension ThreadBoundExecutor {
  /// The binding never changes: the current thread is the bound thread.
  public var currentThreadID: ThreadID {
    threadID
  }

  /// Runs a job on the calling thread. The executor's scheduling already
  /// guarantees the calling thread is the bound thread.
  func runJob(_ job: consuming UnownedJob) {
    job.runSynchronously(
      isolatedTo: asUnownedSerialExecutor(),
      taskExecutor: asUnownedTaskExecutor()
    )
  }
}
