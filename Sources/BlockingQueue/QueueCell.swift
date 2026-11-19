//
//  QueueCell.swift
//  BlockingQueue
//
//  Internal shared state behind every queue end and supervisor: the
//  element deque plus lifecycle flags and endpoint refcounts, guarded by a
//  single NSCondition. Producers, consumers and `waitUntilSealed` all wait
//  on that one condition, so the wake policy counts each class: a single
//  signal is safe only when the class being woken is the only one waiting.
//

import DequeModule
import Foundation

final class QueueCell<Element: ~Copyable>: @unchecked Sendable {
  let condition = NSCondition()
  var elements = UniqueDeque<Element>()
  let capacity: Int?
  var closed = false
  var sealed = false
  var producerCount = 0
  var consumerCount = 0
  var waitingProducers = 0
  var waitingConsumers = 0
  var waitingSealed = 0

  /// How many waiters a state change must wake. `one` is valid only when
  /// the target class is the sole waiter class on the shared condition.
  private enum WakeDecision {
    case none
    case one
    case all
  }

  /// Callers hold the lock.
  private func consumerWakeLocked() -> WakeDecision {
    guard waitingConsumers > 0 else { return .none }
    return waitingProducers == 0 && waitingSealed == 0 ? .one : .all
  }

  /// Callers hold the lock.
  private func producerWakeLocked() -> WakeDecision {
    guard waitingProducers > 0 else { return .none }
    return waitingConsumers == 0 && waitingSealed == 0 ? .one : .all
  }

  /// Callers hold the lock and apply the decision before unlocking. A
  /// signal sent after unlocking could be consumed by a waiter of another
  /// class that registered in the gap; a broadcast is safe either way.
  private func apply(_ decision: WakeDecision) {
    switch decision {
    case .none:
      break
    case .one:
      condition.signal()
    case .all:
      condition.broadcast()
    }
  }

  init(capacity: Int?) {
    self.capacity = capacity
    condition.name = capacity.map { "BlockingQueue.bounded(\($0))" } ?? "BlockingQueue.unbounded"
  }

  /// Try-enqueue: never waits. Fails on sealed, closed, missing consumers
  /// or (bounded only) a full queue.
  func enqueue(_ element: consuming Element) -> Bool {
    condition.lock()
    if sealed || closed || consumerCount == 0 {
      condition.unlock()
      return false
    }
    if let capacity, elements.count >= capacity {
      condition.unlock()
      return false
    }
    elements.append(element)
    apply(consumerWakeLocked())
    condition.unlock()
    return true
  }

  /// Enqueue that waits while the queue is full. Fails without waiting on
  /// sealed, closed or missing consumers.
  func enqueueBlocking(_ element: consuming Element) -> Bool {
    condition.lock()
    waitingProducers += 1
    while true {
      if sealed || closed || consumerCount == 0 {
        waitingProducers -= 1
        condition.unlock()
        return false
      }
      if let capacity, elements.count >= capacity {
        condition.wait()
        continue
      }
      elements.append(element)
      waitingProducers -= 1
      apply(consumerWakeLocked())
      condition.unlock()
      return true
    }
  }

  /// Blocking take; `nil` seals the queue (closed and drained) in the same
  /// critical section.
  func take() -> sending Element? {
    condition.lock()
    waitingConsumers += 1
    while elements.isEmpty && !closed && !sealed {
      condition.wait()
    }
    waitingConsumers -= 1
    if let element = elements.popFirst() {
      // A freed bounded slot wakes one blocked producer; other waiter
      // classes on the shared condition turn that into a broadcast.
      if capacity != nil {
        apply(producerWakeLocked())
      }
      condition.unlock()
      return element
    }
    sealed = true
    condition.broadcast()
    condition.unlock()
    return nil
  }

  func close() {
    condition.lock()
    closed = true
    condition.unlock()
    condition.broadcast()
  }

  /// Registers both sides of one split in the same critical section. The
  /// matching ends decrement their side in `deinit`; every split must call
  /// this exactly once, regardless of which ends are shared.
  func registerEnds() {
    condition.lock()
    producerCount += 1
    consumerCount += 1
    condition.unlock()
  }

  /// Releasing the last producer end closes production.
  func removeProducer() {
    condition.lock()
    producerCount -= 1
    if producerCount == 0 {
      closed = true
    }
    condition.unlock()
    condition.broadcast()
  }

  /// Releasing the last consumer end seals the queue.
  func removeConsumer() {
    condition.lock()
    consumerCount -= 1
    if consumerCount == 0 {
      sealed = true
    }
    condition.unlock()
    condition.broadcast()
  }

  func waitUntilSealed() {
    condition.lock()
    waitingSealed += 1
    while !sealed {
      condition.wait()
    }
    waitingSealed -= 1
    condition.unlock()
  }

  var isClosed: Bool {
    condition.lock()
    defer { condition.unlock() }
    return closed
  }

  var isSealed: Bool {
    condition.lock()
    defer { condition.unlock() }
    return sealed
  }

  /// Test/diagnostic snapshot of the waiters parked on the shared condition.
  var waiterSnapshot: (producers: Int, consumers: Int, sealed: Int) {
    condition.lock()
    defer { condition.unlock() }
    return (waitingProducers, waitingConsumers, waitingSealed)
  }

  var count: Int {
    condition.lock()
    defer { condition.unlock() }
    return elements.count
  }

  /// Removes and returns everything currently queued.
  func drainAll() -> sending UniqueDeque<Element> {
    condition.lock()
    var drained = UniqueDeque<Element>()
    swap(&drained, &elements)
    condition.unlock()
    condition.broadcast()
    return drained
  }
}
