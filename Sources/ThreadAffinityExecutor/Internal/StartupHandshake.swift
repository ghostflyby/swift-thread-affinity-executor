//
//  StartupHandshake.swift
//  ThreadAffinityExecutor
//
//  One-shot handshake carrying the bound thread's `ThreadID` from the
//  freshly started worker thread to the thread constructing the executor.
//  Blocking handshake used by `DedicatedThreadExecutor`'s initializer.
//

import Foundation

final class StartupHandshake: @unchecked Sendable {
  private let condition = NSCondition()
  private var publishedThreadID: ThreadID?

  /// Called on the bound thread once it starts servicing.
  func publish(_ id: ThreadID) {
    condition.lock()
    publishedThreadID = id
    condition.unlock()
    condition.broadcast()
  }

  /// Blocks until the bound thread published its ID.
  func awaitReady() {
    condition.lock()
    while publishedThreadID == nil {
      condition.wait()
    }
    condition.unlock()
  }

  /// The published thread ID. Traps before the handshake — instances only
  /// become reachable through the constructor that awaits it.
  var threadID: ThreadID {
    condition.lock()
    defer { condition.unlock() }
    guard let publishedThreadID else {
      preconditionFailure("ThreadID read before the bound thread published it")
    }
    return publishedThreadID
  }
}
