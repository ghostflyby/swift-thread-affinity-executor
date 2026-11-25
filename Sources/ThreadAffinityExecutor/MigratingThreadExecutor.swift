//
//  MigratingThreadExecutor.swift
//  ThreadAffinityExecutor
//
//  One executor identity over a sequence of thread-bound executors, each
//  serving for a while and exchanged through an explicit migration.
//

import Synchronization

/// Migration failures.
public enum MigrationError: Error, Sendable, Equatable {
  /// Another migration is in flight; nothing changed. Retry after it
  /// completed — which never happens if the old bound executor stopped
  /// servicing and left the migration pending (see
  /// `MigratingThreadExecutor`).
  case busy
}

/// An executor whose jobs run on a sequence of `ThreadBoundExecutor`s: one
/// bound executor at a time, exchanged through explicit migrations while
/// the identity — and therefore every actor bound to it — stays the same.
///
/// The executor owns no thread. It routes every job to its current bound
/// executor and never switches on its own; holders trigger each exchange:
///
///     let executor = MigratingThreadExecutor(bound: first)
///     try await executor.migrate(to: second)
///
/// **The migration cut.** A job accepted before the cut runs on the old
/// bound thread, one accepted after it on the new one; a migration moves
/// only what is accepted afterwards:
///
/// 1. Routing stops. Jobs arriving from now on are staged.
/// 2. A barrier is queued on the old bound executor. It runs once every
///    job routed there has completed.
/// 3. The staged jobs are released to the new bound executor in order, and
///    routing turns direct again.
///
/// Every job accepted before the cut therefore completes before any later
/// job starts. `currentThreadID` switches when the barrier has run, not
/// when routing stops: until then jobs are still running on the old
/// thread, so that is the current one although new jobs are already bound
/// for the new. The release runs on the old bound executor's thread, and
/// whatever is queued behind it there waits for it.
///
/// **Bound executors are borrowed.** A bound executor may serve other
/// clients before, during and after; a migration neither ends it nor
/// touches its lifecycle. Ending it stays with its holder.
///
/// **Contract on the bound executors.** They must run jobs in acceptance
/// order (the barrier is a job), keep running the jobs they accepted, and
/// have an `enqueue` that returns promptly. Direct submissions call it with
/// the routing lock held, so those calls must not call back into this
/// executor. Their jobs must not re-enter the bound executor's own loop: a
/// nested run loop on the bound thread would run the barrier while an
/// earlier job is still live, and the ordering above would not hold.
///
/// **When a bound executor stops.** `Executor` cannot refuse a job, and
/// nothing reports this. An old bound executor that stops servicing leaves
/// the migration pending for good: the barrier never runs, `migrate(to:)`
/// never returns, later jobs stay staged and later migrations throw
/// `MigrationError.busy`. A new bound executor that already stopped accepts
/// the release silently and discards the staged jobs, and `migrate(to:)`
/// still returns. A holder cannot tell the pending state from a long
/// backlog.
///
/// **What follows a migration.** Jobs are enqueued on the bound executor
/// through its own `enqueue`, so they run as that executor; the
/// thread-based `isIsolatingCurrentContext()` is what keeps synchronous
/// isolation checks working for actors bound to the migrating executor.
/// Work that names the migrating executor follows: a task suspended across
/// the cut resumes on the new thread, and so do structured child tasks,
/// nested `withTaskExecutorPreference` scopes and unstructured tasks whose
/// body uses an actor bound to the executor. Any other unstructured `Task`
/// stays on the default executor, as Swift defines for unstructured tasks;
/// it needs an explicit `withTaskExecutorPreference`. Code after such a
/// scope is no longer the executor's job: it continues on the executor the
/// task had before the scope.
public final class MigratingThreadExecutor: ThreadAffinityExecutor {

  /// Routing table under one lock. A job is accepted inside the lock, so
  /// the lock order is always routing first, then a bound executor's queue.
  private struct Routing {
    /// The bound executor direct submissions go to.
    var target: any ThreadBoundExecutor
    /// Non-nil while a migration is in flight: the jobs accepted since
    /// routing stopped, in acceptance order.
    var staged: [UnownedJob]?
  }

  private let routing: Mutex<Routing>
  /// The thread reported through `ThreadAffinityExecutor`, kept out of the
  /// routing lock: isolation checks read it on a hot path, and its switch
  /// point is the release, not the routing flip. Switches when the barrier
  /// ran, not when routing stopped.
  private let publishedThread = Atomic<UInt64>(0)

  /// Routes jobs to `bound`, which must already be published — its
  /// `threadID` is read here.
  public init(bound: any ThreadBoundExecutor) {
    routing = Mutex(Routing(target: bound, staged: nil))
    publishedThread.store(bound.threadID.rawValue, ordering: .releasing)
  }

  /// The thread this executor's jobs run on right now. It switches when the
  /// barrier has run, not when routing stops.
  public var currentThreadID: ThreadID {
    ThreadID(rawValue: publishedThread.load(ordering: .acquiring))
  }

  public func enqueue(_ job: consuming ExecutorJob) {
    let pending = UnownedJob(job)
    routing.withLock { routing in
      if routing.staged != nil {
        routing.staged?.append(pending)
      } else {
        routing.target.enqueue(ExecutorJob(pending))
      }
    }
  }

  /// Migrates to `newBound`. Returns once every job routed to the old bound
  /// executor completed and the staged jobs were released, so jobs enqueued
  /// after the return run on `newBound`'s thread.
  ///
  /// Safe to await from a task bound to this executor: the ceremony runs on
  /// a detached task, and the caller resumes on the new thread.
  ///
  /// The old bound executor's backlog bounds the duration from below. A
  /// bound executor that stops servicing its queue keeps this call pending.
  ///
  /// - Throws: `MigrationError.busy` while another migration is in flight.
  public func migrate(to newBound: any ThreadBoundExecutor) async throws {
    let oldBound = try routing.withLock { routing -> any ThreadBoundExecutor in
      guard routing.staged == nil else {
        throw MigrationError.busy
      }
      routing.staged = []
      return routing.target
    }
    // The barrier has to be a real enqueue on the old bound executor. A
    // detached task inherits neither actor isolation nor a task executor, so
    // its hop to `oldBound` is exactly that enqueue.
    await Task.detached {
      await withTaskExecutorPreference(oldBound) {
        self.releaseStagedJobs(to: newBound)
      }
    }.value
  }

  /// Runs on the old bound executor's thread once its barrier job is
  /// reached: every job routed there before routing stopped has completed,
  /// and none of this executor's jobs is running anywhere.
  private func releaseStagedJobs(to newBound: any ThreadBoundExecutor) {
    // The current thread switches here: nothing accepted earlier is still
    // running, and the first job released below lands on the new thread.
    publishedThread.store(newBound.threadID.rawValue, ordering: .releasing)

    // One critical section: take the whole staging set, hand it over, and
    // turn routing direct. A job arriving meanwhile waits for the lock and
    // then goes direct, behind the released batch, so the release is bounded
    // by the staged jobs alone — a producer that keeps arriving cannot keep
    // the migration from settling.
    routing.withLock { routing in
      for job in routing.staged ?? [] {
        newBound.enqueue(ExecutorJob(job))
      }
      routing.staged = nil
      routing.target = newBound
    }
  }
}
