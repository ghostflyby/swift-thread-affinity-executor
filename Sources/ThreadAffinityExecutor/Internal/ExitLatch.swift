//
//  ExitLatch.swift
//  ThreadAffinityExecutor
//
//  One-shot latch carrying "the bound thread finished its exit epilogue"
//  from the worker to `shutdown()`, which joins on it so callers are
//  sequenced after `finalize`.
//

import Foundation

final class ExitLatch: @unchecked Sendable {
  private let condition = NSCondition()
  private var done = false

  /// Called on the bound thread after its exit epilogue completed.
  func signal() {
    condition.lock()
    done = true
    condition.unlock()
    condition.broadcast()
  }

  /// Blocks until `signal`.
  func wait() {
    condition.lock()
    while !done {
      condition.wait()
    }
    condition.unlock()
  }
}
