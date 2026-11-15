//
//  TestSupport.swift
//  ThreadAffinityExecutorTests
//

import Synchronization

@testable import ThreadAffinityExecutor

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
    threads.with { $0.insert(ThreadID.current) }
  }
}
