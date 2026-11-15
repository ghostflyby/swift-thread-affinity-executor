//
//  TestSupport.swift
//  BlockingQueueTests
//

import Synchronization

/// Mutex-guarded box for synchronizing test observations.
final class Box<T: Sendable>: Sendable {
  private let value: Mutex<T>

  init(_ value: T) {
    self.value = Mutex(value)
  }

  func set(_ newValue: T) {
    value.withLock { $0 = newValue }
  }

  func get() -> T {
    value.withLock { $0 }
  }
}

/// Mutex-protected sink for concurrently consumed elements.
final class Collector: Sendable {
  private let values = Mutex<[Int]>([])

  func append(_ value: Int) {
    values.withLock { $0.append(value) }
  }

  func snapshot() -> [Int] {
    values.withLock { $0 }
  }
}

/// Consumes a move-only value so its deinit (and its disconnect effect)
/// runs deterministically.
func release<T: ~Copyable>(_ value: consuming T) {}
