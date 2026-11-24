//
//  TestSupport.swift
//  ThreadAffinityExecutorTests
//

import Dispatch
import Foundation
import Synchronization

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

/// Timed variant for tests that must distinguish "never woke" from a slow
/// wake-up without parking a cooperative thread.
func waitForSignal(_ semaphore: DispatchSemaphore, timeout: TimeInterval) async -> Bool {
  await withCheckedContinuation { continuation in
    DispatchQueue.global().async {
      let result = semaphore.wait(timeout: .now() + timeout)
      continuation.resume(returning: result == .success)
    }
  }
}

/// Runs the blocking attachment detach off the cooperative pool.
@discardableResult
func detach(_ executor: AttachedThreadExecutor) async -> Bool {
  await withCheckedContinuation { continuation in
    DispatchQueue.global().async {
      continuation.resume(returning: executor.detach())
    }
  }
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
