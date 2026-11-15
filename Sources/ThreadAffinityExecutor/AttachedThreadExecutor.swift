//
//  AttachedThreadExecutor.swift
//  ThreadAffinityExecutor
//
//  Attaching to threads the library does not own: host event loops (main
//  queue, RunLoop, third-party single-threaded loops) through a minimal
//  `post` adapter, and caller-driven pumps through self-attach.
//

import Foundation

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
/// `finalize` is a per-modality capability, not an attach parameter: the
/// executor neither owns nor stops the host thread, so classic attachment
/// has no exit point to hook — a caller-driven pump takes its `finalize`
/// in `QueueDispatcher.init` and runs it at `run()` exit.
public final class AttachedThreadExecutor: ThreadBoundExecutor {
  private let dispatcher: any FixedThreadDispatcher
  private let state = AttachedThreadState()

  private init(dispatcher: any FixedThreadDispatcher) {
    self.dispatcher = dispatcher
  }

  /// Attaches to the dispatcher's fixed thread. Never blocks the target
  /// thread: the handshake closure is posted and awaited from the
  /// attaching task. Requires the dispatcher's pump to be serviced.
  ///
  /// `initialize` runs on the target thread as the first posted work —
  /// before any job, because the executor only accepts jobs once this
  /// call returned. An error it throws fails the attach; cleanup after a
  /// failed `initialize` is the closure's own responsibility (e.g.
  /// `defer`).
  public static func attach(
    to dispatcher: any FixedThreadDispatcher,
    initialize: @escaping @Sendable () throws -> Void = {}
  ) async throws -> AttachedThreadExecutor {
    let executor = AttachedThreadExecutor(dispatcher: dispatcher)
    dispatcher.post { [state = executor.state] in
      do {
        try initialize()
        state.fulfillSuccess(ThreadID.current)
      } catch {
        state.fulfillFailure(error)
      }
    }
    _ = try await executor.state.awaitReady()
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
  /// fails the attach.
  public static func attachOnCurrentThread(
    to dispatcher: any FixedThreadDispatcher,
    initialize: @escaping @Sendable () throws -> Void = {}
  ) throws -> AttachedThreadExecutor {
    let executor = AttachedThreadExecutor(dispatcher: dispatcher)
    try initialize()
    executor.state.fulfillSuccess(ThreadID.current)
    return executor
  }

  public var threadID: ThreadID {
    state.threadID
  }

  public func enqueue(_ job: consuming ExecutorJob) {
    let pending = UnownedJob(job)
    dispatcher.post { [self] in
      runJob(pending)
    }
  }
}
