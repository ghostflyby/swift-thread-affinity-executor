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
/// main thread.
///
/// Requires the main queue to be serviced **by the main thread through a
/// run loop** — `RunLoop.main.run()` (GUI apps do this implicitly). A
/// main thread parked in `dispatchMain()` is not enough on some
/// toolchains: the Swift runtime then diverts task-context main-queue
/// posts onto the cooperative pool, so executor jobs would execute off
/// the main thread. The `ThreadPumpDelegate` conformance's `initialize`
/// traps at handshake time when the attachment did not land on the main
/// thread. With the requirement satisfied, isolation checks are
/// bidirectionally compatible with `MainActor` context.
public struct MainQueueDispatcher: FixedThreadDispatcher {
  public init() {}

  public func post(_ work: @escaping @Sendable () -> Void) {
    DispatchQueue.main.async {
      work()
    }
  }
}

extension MainQueueDispatcher: ThreadPumpDelegate {
  /// Handshake-time guard: posts through this dispatcher reach the main
  /// thread only when the main thread services the main queue via a run
  /// loop — start `RunLoop.main.run()`.
  public func initialize() throws {
    precondition(
      Thread.isMainThread,
      "MainQueueDispatcher attached off the process main thread — service the main queue with RunLoop.main.run(); a dispatchMain()-parked main thread lets the runtime divert task-context posts to the cooperative pool"
    )
  }
}

/// Executor whose jobs run on a thread the library does not own, reached
/// through a `ThreadPumpDelegate` — the pump's delivery plus its thread's
/// lifecycle in one object.
///
/// Two attach modalities share one rule — the binding handshake, running
/// the pump's `initialize`, executes on the target thread before any job,
/// so `threadID` is always valid and thread-local setup is in place
/// before the first job; an `initialize` error fails the attach and the
/// executor is never published:
///
/// - `attach(to:)` posts the handshake through the pump and awaits it:
///   for hosts whose pump is already running (main queue via
///   `MainQueueDispatcher`, RunLoop, EventLoop). Never blocks the target
///   thread.
/// - `attachOnCurrentThread(to:)` runs the handshake synchronously: for
///   caller-driven pumps that have not started yet. The calling thread is
///   the pump's target; pair with `QueueDispatcher`, whose `run()` the
///   caller enters afterwards.
///
/// The pump's `finalize` runs at attach termination, delivered through the
/// same pump: `detach()` posts it — FIFO puts it after all accepted work,
/// on the bound thread — and joins its completion. Abandoning an executor
/// without detaching posts it best-effort from deinit, un-joined; a pump
/// that no longer accepts posts drops it silently.
public final class AttachedThreadExecutor: ThreadBoundExecutor {
  private let pump: any ThreadPumpDelegate
  private let state = AttachedThreadState()
  private let detachGate = DetachGate()
  private let established = Atomic<Bool>(false)

  private init(pump: any ThreadPumpDelegate) {
    self.pump = pump
  }

  /// Attaches to the pump's fixed thread. Never blocks the target thread:
  /// the handshake closure is posted and awaited from the attaching task.
  /// Requires the pump to be serviced.
  ///
  /// The pump's `initialize` runs as the first posted work — the executor
  /// accepts jobs only once this call returned. The pump's `finalize`
  /// runs at attach termination (`detach()`).
  public static func attach(
    to pump: any ThreadPumpDelegate
  ) async throws -> AttachedThreadExecutor {
    let executor = AttachedThreadExecutor(pump: pump)
    pump.post { [state = executor.state, pump] in
      do {
        try pump.initialize()
        state.fulfillSuccess(ThreadID.current)
      } catch {
        state.fulfillFailure(error)
      }
    }
    _ = try await executor.state.awaitReady()
    executor.established.store(true, ordering: .releasing)
    return executor
  }

  /// Self-attach: the calling thread is the pump's target thread.
  ///
  /// Unlike `attach(to:)` the handshake runs synchronously — nothing is
  /// posted, so the pump need not be running yet. Contract: the caller
  /// enters the pump on this very thread (e.g. `QueueDispatcher.run()`);
  /// posted work must always run here.
  public static func attachOnCurrentThread(
    to pump: any ThreadPumpDelegate
  ) throws -> AttachedThreadExecutor {
    let executor = AttachedThreadExecutor(pump: pump)
    try pump.initialize()
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
    pump.post { [self] in
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
      pump.post { [pump, detachGate] in
        pump.finalize()
        detachGate.signalDone()
      }
    }
    detachGate.waitDone()
  }

  deinit {
    // Abandonment backstop, un-joined: queued closures held self, so the
    // backlog has drained by now and FIFO still orders this after all
    // accepted work. A pump that no longer accepts posts drops it; posted
    // work holds the pump, so an abandoned never-drained pump leaks
    // (drain it — run to return, or let detach complete).
    guard established.load(ordering: .acquiring), detachGate.claim() else {
      return
    }
    let pump = self.pump
    pump.post {
      pump.finalize()
    }
  }
}
