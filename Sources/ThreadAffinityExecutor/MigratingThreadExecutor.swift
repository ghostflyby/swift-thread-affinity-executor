//
//  MigratingThreadExecutor.swift
//  ThreadAffinityExecutor
//
//  One executor identity over a sequence of thread-bound executors, each
//  serving for a while and exchanged through an explicit migration.
//

import Foundation
import Synchronization

/// Migration failures.
public enum MigrationError: Error, Sendable, Equatable {
  /// Another migration is in flight; nothing changed. Retry after it
  /// completed — which never happens if a bound executor stopped and left
  /// the migration pending (see `MigratingThreadExecutor`).
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
/// for the new. The release starts on the old bound thread and suspends
/// while the entering notification is posted on the new one; the flush
/// resumes as a later job, so jobs queued behind the barrier may run in
/// between.
///
/// **Migration notifications.** `.leavingBoundThread` is posted on the old
/// bound thread after the last pre-cut job and before the switch;
/// `.enteringBoundThread` on the new bound thread before the released jobs.
/// Both carry the same `MigrationEvent`. A nil queue runs the observer
/// synchronously on the posting thread; a non-nil queue delivers
/// asynchronously. `addObserver(for:queue:using:)` registers observers;
/// `token.remove()` removes a registration. Observers registered on the
/// executor's center with the executor as object are removed on deinit; the
/// executor retains their observer tokens until then, while removal releases
/// the handler block. A handler must not capture the executor strongly.
/// NotificationCenter promises neither observer order nor a registration
/// snapshot.
///
/// **Bound executors are borrowed.** A bound executor may serve other
/// clients before, during and after; a migration neither ends it nor
/// changes how it runs. Ending it stays with its holder.
///
/// **Contract on the bound executors.** They must run jobs in acceptance
/// order (the barrier is a job), keep running the jobs they accepted, and
/// have an `enqueue` that returns promptly. Direct submissions call it with
/// the routing lock held, so those calls must not call back into this
/// executor. Their jobs must not re-enter the bound executor's job
/// execution: a nested run loop on the bound thread that dispatches queued
/// jobs would run the barrier while an earlier job is still live, and the
/// ordering above would not hold. A conformance may pump a nested run loop
/// as long as it refuses to execute jobs from it, as
/// `RunLoopThreadExecutor` does.
///
/// **When a bound executor stops.** `Executor` cannot refuse a job, and
/// nothing reports this. An old bound executor that stops servicing before
/// the barrier leaves the migration pending for good: the barrier never
/// runs, `migrate(to:)` never returns, later jobs stay staged and later
/// migrations throw `MigrationError.busy`. A new bound executor that already
/// stopped cannot run the entering-notification job, so the migration stays
/// pending as well.
///
/// The barrier always suspends between publishing and flushing, because the
/// entering notification is posted on the new bound. If the old bound stops
/// in that window, the flush never resumes: the switch is published, the
/// notifications were posted, the staged jobs stay staged and `migrate(to:)`
/// never returns.
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

  /// A tracked registration. The observer token is retained so deinit can
  /// remove it; `removeObserver` releases the handler block.
  private struct ObservationEntry: Hashable {
    let observer: any NSObjectProtocol

    static func == (lhs: Self, rhs: Self) -> Bool {
      ObjectIdentifier(lhs.observer as AnyObject)
        == ObjectIdentifier(rhs.observer as AnyObject)
    }

    func hash(into hasher: inout Hasher) {
      hasher.combine(ObjectIdentifier(observer as AnyObject))
    }
  }

  private let routing: Mutex<Routing>
  /// The thread reported through `ThreadAffinityExecutor`, kept out of the
  /// routing lock: isolation checks read it on a hot path, and its switch
  /// point is the release, not the routing flip. Switches when the barrier
  /// ran, not when routing stopped.
  private let publishedThread = Atomic<UInt64>(0)
  /// The center migration notifications are posted to and observed on.
  public let notificationCenter: NotificationCenter
  /// Migrations that reached the switch, reported through
  /// `MigrationEvent.generation`.
  private let generation = Atomic<UInt64>(0)
  /// Registrations made on this executor's center with it as object; cleaned
  /// up on deinit.
  private let observations = Mutex<Set<ObservationEntry>>([])

  /// Routes jobs to `bound`, which must already be published — its
  /// `threadID` is read here.
  ///
  /// - Parameter notificationCenter: Center for the migration notifications;
  ///   a fresh center per executor by default.
  public init(
    bound: any ThreadBoundExecutor,
    notificationCenter: NotificationCenter = NotificationCenter()
  ) {
    self.notificationCenter = notificationCenter
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
  /// The old bound executor's backlog bounds the duration from below, and
  /// the new bound's queue gates completion: the entering notification must
  /// be posted there before the staged jobs are flushed. A bound executor
  /// that stops servicing its queue keeps this call pending.
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
        await self.releaseStagedJobs(to: newBound)
      }
    }.value
  }

  /// Observes `notification` on `notificationCenter`, with this executor as
  /// the object. A nil queue runs on the posting bound thread; must not
  /// block.
  @discardableResult
  public func addObserver(
    for notification: MigrationNotification,
    queue: OperationQueue? = nil,
    using handler: @escaping @Sendable (MigrationEvent) -> Void
  ) -> MigrationHookToken {
    notificationCenter.addObserver(
      for: notification,
      object: self,
      queue: queue,
      using: handler
    )
  }

  /// Tracks a registration made on this executor's center with the executor
  /// as its object.
  func track(_ token: MigrationHookToken) {
    observations.withLock {
      _ = $0.insert(ObservationEntry(observer: token.observer))
    }
  }

  deinit {
    let entries = observations.withLock { $0 }
    for entry in entries {
      notificationCenter.removeObserver(entry.observer)
    }
  }

  /// Runs on the old bound executor's thread once its barrier job is
  /// reached: every job routed there before routing stopped has completed,
  /// and none of this executor's jobs is running anywhere.
  private func releaseStagedJobs(to newBound: any ThreadBoundExecutor) async {
    let nextGeneration = generation.wrappingAdd(1, ordering: .acquiringAndReleasing).newValue
    let event = MigrationEvent(
      generation: nextGeneration,
      from: ThreadID.current,
      to: newBound.threadID
    )

    // Leaving: old thread, before the switch.
    postMigrationNotification(.leavingBoundThread, event: event)

    // The thread switches here.
    publishedThread.store(newBound.threadID.rawValue, ordering: .releasing)

    // Entering: new thread, before the flush. Always hop; no observer count.
    await withTaskExecutorPreference(newBound) {
      self.postMigrationNotification(.enteringBoundThread, event: event)
    }

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

  private func postMigrationNotification(
    _ notification: MigrationNotification,
    event: MigrationEvent
  ) {
    notificationCenter.post(
      name: notification.notificationName,
      object: self,
      userInfo: [MigrationEvent.userInfoKey: event]
    )
  }
}
