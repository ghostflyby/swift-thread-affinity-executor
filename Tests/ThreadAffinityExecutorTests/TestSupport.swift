//
//  TestSupport.swift
//  ThreadAffinityExecutorTests
//

import Dispatch
import Foundation
import Synchronization
import Testing

@testable import ThreadAffinityExecutor

/// Awaits `semaphore` without parking a cooperative test thread.
func waitForSignal(_ semaphore: DispatchSemaphore) async {
  await withCheckedContinuation { continuation in
    DispatchQueue.global().async {
      semaphore.wait()
      continuation.resume()
    }
  }
}

/// Awaits `semaphore` like `waitForSignal`, but gives up after `timeout` and
/// records a test issue rather than hanging the run.
///
/// The wait still runs on the global queue: parking a cooperative thread would
/// starve the very tasks whose progress produces the signal.
@discardableResult
func waitForSignal(
  _ semaphore: DispatchSemaphore,
  timeout: TimeInterval,
  _ message: Comment = "timed out waiting for a signal"
) async -> Bool {
  // The wait and the timeout run in one block: a late-scheduled waiter then
  // delays the result instead of losing a claim to the timeout and reporting
  // a signal that arrived in time as a timeout.
  let didSignal = await withCheckedContinuation {
    (continuation: CheckedContinuation<Bool, Never>) in
    DispatchQueue.global().async {
      let result = semaphore.wait(timeout: .now() + timeout)
      continuation.resume(returning: result == .success)
    }
  }
  if !didSignal {
    Issue.record(message)
  }
  return didSignal
}

/// A mutex-guarded box for synchronizing test fixtures.
final class TestBox<T>: Sendable where T: Sendable {
  private let value: Mutex<T>

  init(_ value: T) {
    self.value = Mutex(value)
  }

  func get() -> T {
    value.withLock { $0 }
  }

  func set(_ newValue: T) {
    value.withLock { $0 = newValue }
  }

  func with<R>(_ body: (inout T) -> R) -> R {
    value.withLock { body(&$0) }
  }
}

/// Atomic counter that also records which threads performed the work.
final class ThreadSafeCounter: Sendable {
  private let count = Atomic<Int>(0)

  var value: Int {
    count.load(ordering: .acquiring)
  }

  func increment(recording threads: TestBox<Set<ThreadID>>) {
    _ = count.wrappingAdd(1, ordering: .relaxed)
    threads.with { _ = $0.insert(ThreadID.current) }
  }
}
