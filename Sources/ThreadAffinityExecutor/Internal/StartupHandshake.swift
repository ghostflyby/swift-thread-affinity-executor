//
//  StartupHandshake.swift
//  ThreadAffinityExecutor
//
//  One-shot handshake carrying the bound thread's `ThreadID` and the
//  outcome of the on-thread initialization from the freshly started worker
//  thread to the thread constructing the executor. Blocking variant used
//  by `DedicatedThreadExecutor`'s synchronous initializer; attached
//  executors publish through `AttachedThreadState`.
//

import Foundation

final class StartupHandshake: @unchecked Sendable {
  private enum Outcome {
    case ready(ThreadID)
    case failed(Error)
  }

  private let condition = NSCondition()
  private var outcome: Outcome?

  /// Called on the bound thread after on-thread initialization succeeded.
  func publishSuccess(_ id: ThreadID) {
    condition.lock()
    outcome = .ready(id)
    condition.unlock()
    condition.broadcast()
  }

  /// Called on the bound thread when on-thread initialization threw; the
  /// failing executor is never published.
  func publishFailure(_ error: Error) {
    condition.lock()
    outcome = .failed(error)
    condition.unlock()
    condition.broadcast()
  }

  /// Blocks until an outcome was published: returns on success, rethrows
  /// the initialization error otherwise.
  func awaitReady() throws {
    condition.lock()
    while outcome == nil {
      condition.wait()
    }
    let published = outcome!
    condition.unlock()
    switch published {
    case .ready:
      return
    case .failed(let error):
      throw error
    }
  }

  /// The published thread ID. Traps before a successful handshake —
  /// instances only become reachable through the constructor that awaits
  /// the handshake.
  var threadID: ThreadID {
    condition.lock()
    defer { condition.unlock() }
    guard case .ready(let publishedThreadID) = outcome else {
      preconditionFailure("ThreadID read before the bound thread published it")
    }
    return publishedThreadID
  }
}
