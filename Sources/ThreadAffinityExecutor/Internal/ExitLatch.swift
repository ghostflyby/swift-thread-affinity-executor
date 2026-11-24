//
//  ExitLatch.swift
//  ThreadAffinityExecutor
//
//  One-shot latch carrying "the bound thread finished its exit epilogue"
//  from the worker to `shutdown()`, which joins on it so callers are
//  sequenced after `finalize`. Waiters may block or suspend.
//

import Foundation

final class ExitLatch: @unchecked Sendable {
  private let condition = NSCondition()
  private var done = false
  private var waiters: [CheckedContinuation<Void, Never>] = []

  /// Called on the bound thread after its exit epilogue completed.
  func signal() {
    condition.lock()
    done = true
    let waiters = self.waiters
    self.waiters = []
    condition.unlock()
    condition.broadcast()
    for waiter in waiters {
      waiter.resume()
    }
  }

  /// Blocks until `signal`.
  func wait() {
    condition.lock()
    while !done {
      condition.wait()
    }
    condition.unlock()
  }

  /// Suspends until `signal` without blocking the calling thread. The wait
  /// does not respond to cancellation: the caller resumes only once the
  /// latch fired.
  func waitAsync() async {
    await withCheckedContinuation { (continuation: CheckedContinuation<Void, Never>) in
      condition.lock()
      if done {
        condition.unlock()
        continuation.resume()
        return
      }
      waiters.append(continuation)
      condition.unlock()
    }
  }
}
