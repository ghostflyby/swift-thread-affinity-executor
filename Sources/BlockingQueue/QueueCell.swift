//
//  QueueCell.swift
//  BlockingQueue
//
//  Internal shared state behind every queue end and supervisor: the
//  element deque plus lifecycle flags and endpoint refcounts, guarded by a
//  single NSCondition.
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
    condition.unlock()
    condition.broadcast()
    return true
  }

  /// Enqueue that waits while the queue is full. Fails without waiting on
  /// sealed, closed or missing consumers.
  func enqueueBlocking(_ element: consuming Element) -> Bool {
    condition.lock()
    while true {
      if sealed || closed || consumerCount == 0 {
        condition.unlock()
        return false
      }
      if let capacity, elements.count >= capacity {
        condition.wait()
        continue
      }
      elements.append(element)
      condition.unlock()
      condition.broadcast()
      return true
    }
  }

  /// Blocking take; `nil` seals the queue (closed and drained) in the same
  /// critical section.
  func take() -> sending Element? {
    condition.lock()
    while elements.isEmpty && !closed && !sealed {
      condition.wait()
    }
    if let element = elements.popFirst() {
      condition.unlock()
      condition.broadcast()
      return element
    }
    sealed = true
    condition.unlock()
    condition.broadcast()
    return nil
  }

  func close() {
    condition.lock()
    closed = true
    condition.unlock()
    condition.broadcast()
  }

  func addProducer() {
    condition.lock()
    producerCount += 1
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

  func addConsumer() {
    condition.lock()
    consumerCount += 1
    condition.unlock()
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
    while !sealed {
      condition.wait()
    }
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
