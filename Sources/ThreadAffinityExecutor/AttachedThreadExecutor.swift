//
//  AttachedThreadExecutor.swift
//  ThreadAffinityExecutor
//
//  Attaching to threads the library does not own: host event loops (main
//  queue, RunLoop, third-party single-threaded loops) through a minimal
//  `post` adapter, and caller-driven pumps through self-attach.
//

import Foundation
import Synchronization

/// Minimal adapter surface for attaching to an existing fixed-thread
/// mechanism.
///
/// Implementations must run posted work **serially, in post order, never
/// concurrently, always on the same fixed OS thread** for the executor's
/// whole lifetime; otherwise the type cannot satisfy `ThreadBoundExecutor`.
public protocol FixedThreadDispatcher: Sendable {
  /// Runs `work` on the dispatcher's fixed OS thread.
  func post(_ work: @escaping @Sendable () -> Void)
}

/// Adapter posting onto `DispatchQueue.main`, whose target is the process
/// main thread. Requires a main queue that is actually being serviced.
public struct MainQueueDispatcher: FixedThreadDispatcher {
  public init() {}

  public func post(_ work: @escaping @Sendable () -> Void) {
    DispatchQueue.main.async {
      work()
    }
  }
}

/// Executor whose jobs run on a thread the library does not own, reached
/// through a `FixedThreadDispatcher`.
///
/// Two attach modalities share one rule — the binding handshake, including
/// the caller's `initialize` closure, runs on the target thread before any
/// job, so `threadID` is always valid and thread-local setup is in place
/// before the first job; an `initialize` error fails the attach and the
/// executor is never published:
///
/// - `attach(to:initialize:)` posts the handshake through the dispatcher
///   and awaits it: for hosts whose pump is already running (main queue
///   via `MainQueueDispatcher`, RunLoop, EventLoop). Never blocks the
///   target thread.
/// - `attachOnCurrentThread(to:initialize:)` runs the handshake
///   synchronously: for caller-driven pumps that have not started yet. The
///   calling thread is the dispatcher's target; pair with
///   `QueueDispatcher`, whose `run()` the caller enters afterwards.
///
/// `finalize` runs at attach termination, delivered through the same
/// dispatcher: `detach()` posts it — FIFO puts it after all accepted work,
/// on the bound thread — and joins its completion. Abandoning an executor
/// without detaching posts it best-effort from deinit, un-joined; a pump
/// that no longer accepts posts drops it silently. `QueueDispatcher`'s own
/// `finalize` is the pump's exit hook for bare pump usage, independent of
/// the attachment's.
public final class AttachedThreadExecutor: ThreadBoundExecutor {
  private let dispatcher: any FixedThreadDispatcher
  private let state = AttachedThreadState()
  private let finalize: @Sendable () -> Void
  private let detachGate = DetachGate()
  private let established = Atomic<Bool>(false)

  private init(
    dispatcher: any FixedThreadDispatcher,
    finalize: @escaping @Sendable () -> Void
  ) {
    self.dispatcher = dispatcher
    self.finalize = finalize
  }

  /// Attaches to the dispatcher's fixed thread. Never blocks the target
  /// thread: the handshake closure is posted and awaited from the
  /// attaching task. Requires the dispatcher's pump to be serviced.
  ///
  /// `initialize` runs on the target thread as the first posted work —
  /// before any job, because the executor only accepts jobs once this
  /// call returned. An error it throws fails the attach; cleanup after a
  /// failed `initialize` is the closure's own responsibility (e.g.
  /// `defer`). `detach()` runs `finalize` at attach termination.
  public static func attach(
    to dispatcher: any FixedThreadDispatcher,
    initialize: @escaping @Sendable () throws -> Void = {},
    finalize: @escaping @Sendable () -> Void = {}
  ) async throws -> AttachedThreadExecutor {
    let executor = AttachedThreadExecutor(dispatcher: dispatcher, finalize: finalize)
    dispatcher.post { [state = executor.state] in
      do {
        try initialize()
        state.fulfillSuccess(ThreadID.current)
      } catch {
        state.fulfillFailure(error)
      }
    }
    _ = try await executor.state.awaitReady()
    executor.established.store(true, ordering: .releasing)
    return executor
  }

  /// Self-attach: the calling thread is the dispatcher's target thread.
  ///
  /// Unlike `attach(to:)` the handshake runs synchronously — nothing is
  /// posted, so the dispatcher's pump need not be running yet. Contract:
  /// the caller enters the dispatcher's pump on this very thread (e.g.
  /// `QueueDispatcher.run()`); posted work must always run here.
  ///
  /// `initialize` runs immediately on this thread; an error it throws
  /// fails the attach. `detach()` runs `finalize` at attach termination.
  public static func attachOnCurrentThread(
    to dispatcher: any FixedThreadDispatcher,
    initialize: @escaping @Sendable () throws -> Void = {},
    finalize: @escaping @Sendable () -> Void = {}
  ) throws -> AttachedThreadExecutor {
    let executor = AttachedThreadExecutor(dispatcher: dispatcher, finalize: finalize)
    try initialize()
    executor.state.fulfillSuccess(ThreadID.current)
    executor.established.store(true, ordering: .releasing)
    return executor
  }

  public var threadID: ThreadID {
    state.threadID
  }

  public func enqueue(_ job: consuming ExecutorJob) {
    guard !detachGate.isClaimed else {
      return  // attachment terminated: the job falls into the drop window
    }
    let pending = UnownedJob(job)
    dispatcher.post { [self] in
      runJob(pending)
    }
  }

  /// Terminates the attachment: posts `finalize` through the same
  /// dispatcher — FIFO puts it after all accepted work, on the bound
  /// thread — and blocks until it completed. Idempotent: a second call
  /// returns once the first detach's finalize finished.
  ///
  /// - Precondition: called off the bound thread (waiting from inside the
  ///   pump deadlocks), after producers stopped submitting, and while the
  ///   pump still accepts posts — a dropped post cannot be observed
  ///   (`post` returns `Void`), so the join would block forever; detach
  ///   before stopping the pump.
  /// - Jobs enqueued after `detach()` began are dropped; one racing past
  ///   the drop guard may instead be posted after the finalize post and
  ///   run after `finalize` — the producers-stopped precondition covers
  ///   this window.
  public func detach() {
    precondition(
      ThreadID.current != threadID,
      "detach() must not be called from the executor's bound thread"
    )
    if detachGate.claim() {
      dispatcher.post { [finalize, detachGate] in
        finalize()
        detachGate.signalDone()
      }
    }
    detachGate.waitDone()
  }

  deinit {
    // Abandonment backstop, un-joined: queued closures held self, so the
    // backlog has drained by now and FIFO still orders this after all
    // accepted work. A pump that no longer accepts posts drops it.
    guard established.load(ordering: .acquiring), detachGate.claim() else {
      return
    }
    let dispatcher = self.dispatcher
    let finalize = self.finalize
    dispatcher.post {
      finalize()
    }
  }
}
