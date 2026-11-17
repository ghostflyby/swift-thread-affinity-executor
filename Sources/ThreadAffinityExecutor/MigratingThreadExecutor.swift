//
//  MigratingThreadExecutor.swift
//  ThreadAffinityExecutor
//
//  One executor identity spanning a sequence of bound-thread generations.
//

import DequeModule
import Foundation

/// Migration control failures.
public enum MigrationError: Error, Sendable, Equatable {
  /// Another migration is already in progress.
  case busy
  /// The executor is terminated.
  case terminated
  /// The pump rejected a required post. A rejected handshake leaves the
  /// current generation serving; a rejected serving post terminates the
  /// executor after the successor was already published.
  case pumpUnavailable
}

/// An executor whose bound thread changes through explicit migration
/// handoffs while its identity — and therefore every actor bound to it —
/// stays the same.
///
/// Every generation is brought by a `ThreadGenerationDelegate` — that
/// generation's pump, whose `post` delivers the handshake and whose
/// `initialize`/`finalize` run on the generation's thread (any dispatcher
/// wraps into one via `ClosureGenerationDelegate(to:)`). Jobs serialize
/// across the whole chain: the successor handshakes and initializes first
/// (transactionally — failure aborts the migration with the current
/// generation still serving), then the old generation drains and
/// finalizes, then the successor starts serving. The successor's serving
/// loop is posted after the old drain, so the successor pump is not
/// blocked while the old generation finishes. Jobs enqueued during the
/// handoff are routed to the successor and run after everything already
/// accepted.
///
/// Each generation's pump must service posts and belong to this executor
/// exclusively until the next handoff (`QueueDispatcher` qualifies). This
/// includes the serving post made after the old generation finalized; a
/// pump that stops accepting posts between the handshake and that post
/// terminates the executor (see `MigrationError.pumpUnavailable`).
///
/// Deliberately not a `ThreadBoundExecutor`: here thread affinity is
/// generation-scoped, not executor-scoped.
public final class MigratingThreadExecutor: SerialExecutor, TaskExecutor, @unchecked Sendable {

  /// One bound-thread tenure: the epoch and its thread.
  public struct Generation: Sendable, Equatable {
    public let epoch: Int
    public let threadID: ThreadID
  }

  private let orchestrator = Orchestrator()

  private init() {}

  /// Attaches the first generation. The handshake and `initialize` run on
  /// the pump's thread, so it must be serviced. The ceremony runs
  /// detached, so no continuation of the attaching task routes through
  /// this executor.
  ///
  /// - Throws: `MigrationError.pumpUnavailable` when the pump rejects the
  ///   handshake or serving post, or the pump's `initialize` error.
  public static func attach(
    to pump: any ThreadGenerationDelegate
  ) async throws -> MigratingThreadExecutor {
    let executor = MigratingThreadExecutor()
    try await Task.detached {
      try await executor.handoff(to: pump)
    }.value
    return executor
  }

  /// The serving generation; switches only at handoff barriers.
  public var current: Generation {
    orchestrator.current
  }

  /// Jobs accepted by the serving generation and not yet taken. Internal;
  /// diagnostics and tests use it to observe a settled backlog.
  var pendingJobCount: Int {
    orchestrator.pendingJobCount
  }

  public func enqueue(_ job: consuming ExecutorJob) {
    let pending = UnownedJob(job)
    orchestrator.withTarget { target in
      guard let target else {
        return  // terminated: the documented drop window
      }
      let accepted = target.enqueue(pending)
      assert(accepted, "enqueue observed a sealed routing target")
    }
  }

  /// Migrates to a successor generation on the pump's thread; returns
  /// once the successor is serving. Transactional: successor `initialize`
  /// failure aborts with the current generation still serving. The
  /// ceremony runs detached, so awaiting this from a task bound to the
  /// executor is safe — the caller resumes on the new generation.
  ///
  /// - Throws: `MigrationError.busy` while another migration is in
  ///   flight, `MigrationError.terminated` after `terminate()`, the
  ///   delegate's `initialize` error on a failed successor handshake, and
  ///   `MigrationError.pumpUnavailable` when the pump rejects the
  ///   handshake or serving post. A rejected serving post terminates the
  ///   executor: the successor was already published, so the caller must
  ///   discard it after `terminate()`. A pump that never services its
  ///   handshake leaves this call pending.
  /// - Precondition: `pump` must not share its fixed thread or queue with
  ///   the currently serving pump. Migrating to the same pump — or to a
  ///   different wrapper around the same thread — deadlocks: the
  ///   handshake post queues behind the old serving loop, which exits only
  ///   after retarget seals its queue, and retarget waits for the
  ///   handshake. The handshake does not block the successor's thread
  ///   while the old generation drains; the serving loop is posted later.
  public func migrate(to pump: any ThreadGenerationDelegate) async throws {
    try await Task.detached { [self] in
      try await handoff(to: pump)
    }.value
  }

  /// Fire-and-forget handoff from inside a job on the bound thread: the
  /// current job completes, the generation drains and finalizes, and the
  /// successor takes over. Completion is observable via `current`. A
  /// successor `initialize` failure aborts silently — the initiating job
  /// gets no signal — and a handoff racing another migration is ignored;
  /// a pump that never services its handshake leaves the migration
  /// pending.
  ///
  /// - Precondition: `pump` must not share its fixed thread or queue with
  ///   the currently serving pump. The same-thread handoff deadlocks: the
  ///   handshake post queues behind the old serving loop, which exits only
  ///   after retarget seals its queue, and retarget waits for the
  ///   handshake. The handshake does not block the successor's thread
  ///   while the old generation drains; the serving loop is posted later.
  ///   A rejected serving post terminates the executor silently, like a
  ///   swallowed `MigrationError.pumpUnavailable`.
  public func handOver(to pump: any ThreadGenerationDelegate) {
    precondition(
      ThreadID.current == orchestrator.current.threadID,
      "handOver(to:) must be called from the executor's bound thread"
    )
    Task.detached { [self] in
      try? await handoff(to: pump)
    }
  }

  /// Terminates the executor: the current generation drains, runs
  /// `finalize` on its thread, and later enqueues are dropped. Waits out
  /// an in-flight migration first; idempotent.
  ///
  /// - Precondition: not awaited from a task bound to this executor —
  ///   the caller's resumption would fall into the post-termination drop
  ///   window and never run. A migration whose pump never services its
  ///   handshake wedges the migration flag and leaves this call pending
  ///   as well.
  public func terminate() async {
    await Task.detached { [self] in
      guard let generation = orchestrator.beginTermination() else {
        return
      }
      await awaitLatch(generation.latch)
    }.value
  }

  // MARK: - Handoff ceremony

  /// The transactional handoff: successor handshake and `initialize`
  /// first (abortable), then routing flips and the old generation seals
  /// atomically (no enqueued job falls between), then publish. The
  /// successor's serving loop is posted only after the old generation
  /// finalized, so the successor pump is never parked on the old drain.
  private func handoff(to pump: any ThreadGenerationDelegate) async throws {
    let (epoch, oldGeneration) = try orchestrator.beginMigration()
    let generation = GenerationQueue()
    let handshake = AttachedThreadState()

    let handshakePosted = pump.post { [pump, handshake, epoch] in
      let info = Generation(epoch: epoch, threadID: ThreadID.current)
      do {
        try pump.initialize(info)
        handshake.fulfillSuccess(info.threadID)
      } catch {
        handshake.fulfillFailure(error)
      }
    }
    guard handshakePosted else {
      orchestrator.abortMigration()
      throw MigrationError.pumpUnavailable
    }

    let successorThreadID: ThreadID
    do {
      successorThreadID = try await handshake.awaitReady()
    } catch {
      orchestrator.abortMigration()
      throw error
    }

    orchestrator.retarget(generation, sealing: oldGeneration)
    if let oldGeneration {
      await awaitLatch(oldGeneration.latch)
    }
    orchestrator.publish(Generation(epoch: epoch, threadID: successorThreadID))
    let info = Generation(epoch: epoch, threadID: successorThreadID)
    let servingPosted = pump.post { [self, generation, pump, info] in
      while let job = generation.take() {
        // The pump may wrap this whole closure in an autorelease pool
        // (QueueDispatcher does); this per-job pool is what releases each
        // job's autoreleased objects before the next job starts.
        withPerWorkAutoreleasePool {
          run(job)
        }
      }
      pump.finalize(info)
      generation.latch.signal()
    }
    guard servingPosted else {
      // The successor is already published but can never serve. Release
      // terminate()'s latch and mark the executor terminated so later
      // enqueues drop and later terminate()/migrate() calls return
      // instead of waiting for a serving loop that will never be posted.
      generation.close()
      generation.latch.signal()
      orchestrator.terminateBroken()
      throw MigrationError.pumpUnavailable
    }
    orchestrator.finishMigration()
  }

  private func run(_ job: UnownedJob) {
    job.runSynchronously(
      isolatedTo: asUnownedSerialExecutor(),
      taskExecutor: asUnownedTaskExecutor()
    )
  }
}

/// Job channel for one generation: enqueue/close/take under one
/// condition; `latch` signals drain-and-finalize completion.
private final class GenerationQueue: @unchecked Sendable {
  private let condition = NSCondition()
  private var jobs: Deque<UnownedJob> = []
  private var closed = false
  let latch = ExitLatch()

  func enqueue(_ job: UnownedJob) -> Bool {
    condition.lock()
    if closed {
      condition.unlock()
      return false
    }
    jobs.append(job)
    condition.unlock()
    condition.broadcast()
    return true
  }

  func close() {
    condition.lock()
    closed = true
    condition.unlock()
    condition.broadcast()
  }

  /// Blocking take; nil seals this generation (closed and drained).
  func take() -> UnownedJob? {
    condition.lock()
    while jobs.isEmpty && !closed {
      condition.wait()
    }
    if let job = jobs.popFirst() {
      condition.unlock()
      return job
    }
    condition.unlock()
    return nil
  }

  /// Jobs accepted by this generation and not yet taken.
  var count: Int {
    condition.lock()
    defer { condition.unlock() }
    return jobs.count
  }
}

/// Routing and generation bookkeeping, guarded by one condition: the
/// enqueue target, the published generation, and the one-migration-at-a-
/// time flag. Routing flips and old-generation sealing happen in a single
/// critical section, so an enqueue observes one coherent target — a job
/// never falls between "routed to the old generation" and "old generation
/// sealed".
private final class Orchestrator: @unchecked Sendable {
  private let condition = NSCondition()
  private var target: GenerationQueue?
  private var currentInfo: MigratingThreadExecutor.Generation?
  private var migrating = false
  private var terminated = false

  var current: MigratingThreadExecutor.Generation {
    condition.lock()
    defer { condition.unlock() }
    guard let currentInfo else {
      preconditionFailure("current generation read before publication")
    }
    return currentInfo
  }

  /// Jobs accepted by the current target and not yet taken. Holds the
  /// routing lock while sampling the generation queue; the lock order is
  /// the same as enqueue/retarget (routing, then generation).
  var pendingJobCount: Int {
    condition.lock()
    defer { condition.unlock() }
    return target?.count ?? 0
  }

  func withTarget(_ body: (GenerationQueue?) -> Void) {
    condition.lock()
    defer { condition.unlock() }
    body(target)
  }

  /// Returns the successor's epoch and the old generation (nil for the
  /// first). Throws when another migration is in flight or the executor
  /// is terminated — never traps, so fire-and-forget callers stay safe.
  func beginMigration() throws -> (epoch: Int, old: GenerationQueue?) {
    condition.lock()
    defer { condition.unlock() }
    if migrating {
      throw MigrationError.busy
    }
    if terminated {
      throw MigrationError.terminated
    }
    migrating = true
    return ((currentInfo?.epoch ?? -1) + 1, target)
  }

  func retarget(_ successor: GenerationQueue, sealing old: GenerationQueue?) {
    condition.lock()
    defer { condition.unlock() }
    old?.close()
    target = successor
  }

  func publish(_ info: MigratingThreadExecutor.Generation) {
    condition.lock()
    defer { condition.unlock() }
    currentInfo = info
  }

  /// Marks the published successor's serving post as accepted: a new
  /// migration or a termination may now proceed. Kept separate from
  /// `publish` so that neither can observe the successor before its
  /// serving loop was posted.
  func finishMigration() {
    condition.lock()
    defer { condition.unlock() }
    migrating = false
    condition.broadcast()  // wakes a terminate() waiting out this migration
  }

  func abortMigration() {
    condition.lock()
    defer { condition.unlock() }
    migrating = false
    condition.broadcast()
  }

  /// Marks the executor terminated after a committed handoff lost its
  /// serving post. The successor was already published, so there is no
  /// previous generation to fall back to; clearing routing makes later
  /// enqueues drop and lets a subsequent `terminate()` finish immediately.
  /// The caller signals the generation's latch separately so a
  /// `terminate()` already waiting on it can return.
  func terminateBroken() {
    condition.lock()
    defer { condition.unlock() }
    terminated = true
    target = nil
    migrating = false
    condition.broadcast()
  }

  /// Atomically seals the current generation and clears routing; later
  /// enqueues observe no target (the drop window). Nil when already
  /// terminated (idempotent). Waits out an in-flight migration first.
  func beginTermination() -> GenerationQueue? {
    condition.lock()
    defer { condition.unlock() }
    while migrating {
      condition.wait()
    }
    guard !terminated else {
      return nil
    }
    terminated = true
    let sealed = target
    target?.close()
    target = nil
    return sealed
  }
}

/// Awaits a latch on a detached task: the caller's cooperative thread is
/// not parked, but the detached task blocks one global executor thread for
/// the duration of the (possibly long) drain.
private func awaitLatch(_ latch: ExitLatch) async {
  await Task.detached {
    latch.wait()
  }.value
}
