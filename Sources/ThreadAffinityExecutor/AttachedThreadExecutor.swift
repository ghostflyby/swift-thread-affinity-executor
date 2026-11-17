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
/// The executor does not re-check the delivering thread per job, so a
/// violating dispatcher fails silently rather than trapping.
public protocol FixedThreadDispatcher: Sendable {
  /// Runs `work` on the dispatcher's fixed OS thread.
  ///
  /// Returns `false` when the dispatcher no longer accepts work (for
  /// example, a stopped pump); the work is then not scheduled. A `true`
  /// return only means the dispatcher accepted the work — the pump's
  /// thread must still service accepted posts.
  @discardableResult
  func post(_ work: @escaping @Sendable () -> Void) -> Bool
}

/// Attachment failures.
public enum AttachError: Error, Sendable, Equatable {
  /// The pump rejected the handshake post; no attachment was established.
  case pumpUnavailable
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

  @discardableResult
  public func post(_ work: @escaping @Sendable () -> Void) -> Bool {
    DispatchQueue.main.async {
      work()
    }
    return true
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
/// on the bound thread — and joins its completion. `detach()` returns
/// `false` when the pump rejected that post, so `finalize` did not run.
/// Abandoning an executor without detaching posts it best-effort from
/// deinit, un-joined; a pump that no longer accepts posts drops it silently.
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
  ///
  /// - Throws: `AttachError.pumpUnavailable` when the pump rejects the
  ///   handshake post, or the pump's `initialize` error.
  public static func attach(
    to pump: any ThreadPumpDelegate
  ) async throws -> AttachedThreadExecutor {
    let executor = AttachedThreadExecutor(pump: pump)
    let posted = pump.post { [state = executor.state, pump] in
      do {
        try pump.initialize()
        state.fulfillSuccess(ThreadID.current)
      } catch {
        state.fulfillFailure(error)
      }
    }
    guard posted else {
      throw AttachError.pumpUnavailable
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
    _ = pump.post { [self] in
      runJob(pending)
    }
  }

  /// Terminates the attachment: posts `finalize` through the same
  /// dispatcher — FIFO puts it after all accepted work, on the bound
  /// thread — and blocks until the claim settles. Idempotent: a second
  /// call joins the first claim's outcome.
  ///
  /// - Precondition: called off the bound thread (waiting from inside the
  ///   pump deadlocks), after producers stopped submitting, and while the
  ///   pump still services accepted posts. A pump that rejects the post
  ///   is reported by a `false` return; a pump that accepts it but never
  ///   runs it still blocks the join — detach before stopping the pump.
  /// - Returns: `true` when the posted `finalize` ran; `false` when the
  ///   pump rejected the post, so `finalize` did not run.
  /// - Jobs enqueued after `detach()` began are dropped; one racing past
  ///   the drop guard may instead be posted after the finalize post and
  ///   run after `finalize` — the producers-stopped precondition covers
  ///   this window.
  @available(*, noasync)
  @discardableResult
  public func detach() -> Bool {
    precondition(
      ThreadID.current != threadID,
      "detach() must not be called from the executor's bound thread"
    )
    if detachGate.claim() {
      let posted = pump.post { [pump, detachGate] in
        pump.finalize()
        detachGate.signalDone(true)
      }
      if !posted {
        detachGate.signalDone(false)
      }
    }
    return detachGate.waitDone()
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
    _ = pump.post { [pump, detachGate] in
      pump.finalize()
      detachGate.signalDone(true)
    }
  }
}
