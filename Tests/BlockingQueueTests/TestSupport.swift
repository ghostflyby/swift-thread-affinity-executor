//
//  TestSupport.swift
//  BlockingQueueTests
//

import Dispatch
import Foundation
import Testing
@testable import BlockingQueue

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

/// Lock-guarded box for synchronizing test observations.
///
/// `Synchronization.Mutex` is not visible to TSan on Linux, so TSan reports
/// the protected accesses as races. `NSLock` is pthread-backed on every
/// supported platform and is understood by the sanitizer.
final class Box<T: Sendable>: @unchecked Sendable {
  private let lock = NSLock()
  private var value: T

  init(_ value: T) {
    self.value = value
  }

  func set(_ newValue: T) {
    lock.lock()
    defer { lock.unlock() }
    value = newValue
  }

  func get() -> T {
    lock.lock()
    defer { lock.unlock() }
    return value
  }
}

/// Lock-protected sink for concurrently consumed elements.
final class Collector: @unchecked Sendable {
  private let lock = NSLock()
  private var values: [Int] = []

  func append(_ value: Int) {
    lock.lock()
    defer { lock.unlock() }
    values.append(value)
  }

  func snapshot() -> [Int] {
    lock.lock()
    defer { lock.unlock() }
    return values
  }
}

/// Consumes a move-only value so its deinit (and its disconnect effect)
/// runs deterministically.
func release<T: ~Copyable>(_ value: consuming T) {}

/// Blocks until the queue's parked-waiter counts reach the requested values.
///
/// Wake-policy tests need both waiter classes registered before the state
/// change that decides between `signal` and `broadcast`; a fixed sleep only
/// guesses. Polls the internal snapshot and fails loudly if the waiters never
/// register.
func waitForWaiters<Element: ~Copyable>(
  _ supervisor: QueueSupervisor<Element>,
  producers: Int = 0,
  consumers: Int = 0,
  sealed: Int = 0,
  timeout: TimeInterval = 2
) {
  let deadline = Date().addingTimeInterval(timeout)
  while Date() < deadline {
    let snapshot = supervisor.cell.waiterSnapshot
    if snapshot.producers >= producers,
      snapshot.consumers >= consumers,
      snapshot.sealed >= sealed
    {
      return
    }
    Thread.sleep(forTimeInterval: 0.001)
  }
  Issue.record(
    "timed out waiting for waiters: expected producers>=\(producers), consumers>=\(consumers), sealed>=\(sealed); got \(supervisor.cell.waiterSnapshot)"
  )
}

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
