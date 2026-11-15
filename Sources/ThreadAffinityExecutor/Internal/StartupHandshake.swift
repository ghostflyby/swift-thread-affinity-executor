//
//  StartupHandshake.swift
//  ThreadAffinityExecutor
//
//  One-shot handshake carrying the bound thread's `ThreadID` from the
//  freshly started worker thread to the thread constructing the executor.
//  Blocking variant used by `DedicatedThreadExecutor`'s synchronous
//  initializer; attached executors publish through `AttachedThreadState`.
//

import Foundation

final class StartupHandshake: @unchecked Sendable {
  private let condition = NSCondition()
  private var publishedThreadID: ThreadID?

  /// Called on the freshly started bound thread.
  func publish(_ id: ThreadID) {
    condition.lock()
    publishedThreadID = id
    condition.unlock()
    condition.broadcast()
  }

  /// Blocks until `publish`, then returns the published ID.
  func waitUntilPublished() -> ThreadID {
    condition.lock()
    while publishedThreadID == nil {
      condition.wait()
    }
    let id = publishedThreadID!
    condition.unlock()
    return id
  }

  /// The published thread ID. Traps before publication completed —
  /// instances only become reachable through the constructor that awaits
  /// the handshake.
  var threadID: ThreadID {
    condition.lock()
    defer { condition.unlock() }
    guard let publishedThreadID else {
      preconditionFailure("ThreadID read before the bound thread published it")
    }
    return publishedThreadID
  }
}
