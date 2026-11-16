//
//  TestSupport.swift
//  BlockingQueueTests
//

import Dispatch
import Synchronization

/// Waits for `semaphore` without occupying a cooperative thread. Under the
/// default parallel test scheduler, blocking a cooperative thread can keep
/// the work that signals it from ever being scheduled.
func awaitSignal(_ semaphore: DispatchSemaphore) async {
  await withCheckedContinuation { continuation in
    DispatchQueue.global().async {
      semaphore.wait()
      continuation.resume()
    }
  }
}

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

/// Deliberately non-`Sendable` reference payload: proves queue elements
/// need only ownership transfer, not concurrent shareability.
final class OpaquePayload {
  let value: Int

  init(value: Int) {
    self.value = value
  }
}

/// Deliberately ~Copyable (and therefore non-`Sendable`) payload.
struct SingleShot: ~Copyable {}
