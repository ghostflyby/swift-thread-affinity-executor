//
//  ThreadState.swift
//  ThreadAffinityExecutor
//
//  Handshake state for attached executors: the target thread
//  publishes its `ThreadID` exactly once via the dispatcher's `post`
//  entry; `attach` awaits it without ever blocking the host thread.
//

import Synchronization

final class AttachedThreadState: Sendable {
  private struct State {
    var threadID: ThreadID?
    var continuation: CheckedContinuation<ThreadID, Never>?
  }

  private let state = Mutex<State>(State())

  /// Called on the target thread through a posted closure.
  ///
  /// Always stores the ID: the `threadID` accessor must stay valid for the
  /// executor's whole lifetime, not just for the attaching task.
  func fulfill(_ id: ThreadID) {
    let pending = state.withLock { state -> CheckedContinuation<ThreadID, Never>? in
      state.threadID = id
      let pending = state.continuation
      state.continuation = nil
      return pending
    }
    pending?.resume(returning: id)
  }

  /// Called by the attaching task; never blocks the target thread.
  ///
  /// Single-slot by design: exactly one attach path awaits the published
  /// ID. A second concurrent waiter traps instead of being silently
  /// dropped on the floor.
  func waitForThreadID() async -> ThreadID {
    await withCheckedContinuation { continuation in
      let ready = state.withLock { state -> ThreadID? in
        if let threadID = state.threadID {
          return threadID
        }
        precondition(
          state.continuation == nil,
          "a second waiter registered on the single-slot AttachedThreadState"
        )
        state.continuation = continuation
        return nil
      }
      if let ready {
        continuation.resume(returning: ready)
      }
    }
  }

  /// The published thread ID. Traps before publication completed —
  /// executors only become reachable once `attach` awaited it.
  var threadID: ThreadID {
    guard let published = state.withLock({ $0.threadID }) else {
      preconditionFailure("ThreadID read before the attached thread published it")
    }
    return published
  }
}
