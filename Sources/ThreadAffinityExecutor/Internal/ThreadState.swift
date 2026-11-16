//
//  ThreadState.swift
//  ThreadAffinityExecutor
//
//  Handshake state for attached executors: the target thread publishes
//  its `ThreadID` and the outcome of the on-thread `initialize` hook
//  exactly once via the dispatcher's `post` entry; `attach` awaits it
//  without ever blocking the host thread.
//

import Synchronization

final class AttachedThreadState: Sendable {
  private struct State {
    var threadID: ThreadID?
    var failure: Error?
    var continuation: CheckedContinuation<ThreadID, any Error>?
  }

  private let state = Mutex<State>(State())

  /// Called on the target thread after on-thread initialization succeeded.
  ///
  /// Always stores the ID: the `threadID` accessor must stay valid for the
  /// executor's whole lifetime, not just for the attaching task.
  func fulfillSuccess(_ id: ThreadID) {
    let pending = state.withLock { state -> CheckedContinuation<ThreadID, any Error>? in
      state.threadID = id
      let pending = state.continuation
      state.continuation = nil
      return pending
    }
    pending?.resume(returning: id)
  }

  /// Called on the target thread when on-thread initialization threw; the
  /// failing executor is never published.
  func fulfillFailure(_ error: Error) {
    let pending = state.withLock { state -> CheckedContinuation<ThreadID, any Error>? in
      state.failure = error
      let pending = state.continuation
      state.continuation = nil
      return pending
    }
    pending?.resume(throwing: error)
  }

  /// Called by the attaching task; never blocks the target thread.
  ///
  /// Single-slot by design: exactly one attach path awaits the published
  /// outcome. A second concurrent waiter traps instead of being silently
  /// dropped on the floor.
  func awaitReady() async throws -> ThreadID {
    try await withCheckedThrowingContinuation {
      (continuation: CheckedContinuation<ThreadID, any Error>) in
      let ready = state.withLock { state -> Result<ThreadID, any Error>? in
        if let threadID = state.threadID {
          return .success(threadID)
        }
        if let failure = state.failure {
          return .failure(failure)
        }
        precondition(
          state.continuation == nil,
          "a second waiter registered on the single-slot AttachedThreadState"
        )
        state.continuation = continuation
        return nil
      }
      switch ready {
      case .success(let id):
        continuation.resume(returning: id)
      case .failure(let error):
        continuation.resume(throwing: error)
      case nil:
        break
      }
    }
  }

  /// The published thread ID. Traps before a successful handshake —
  /// executors only become reachable once `attach` awaited it.
  var threadID: ThreadID {
    guard let published = state.withLock({ $0.threadID }) else {
      preconditionFailure("ThreadID read before the attached thread published it")
    }
    return published
  }
}
