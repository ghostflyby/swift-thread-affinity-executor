//
//  BlockingQueue.swift
//  BlockingQueue
//
//  A generic, NSCondition-based blocking FIFO queue backed by the
//  ring-buffered `UniqueDeque` from swift-collections: producers append to
//  the back, takers block for and remove from the front, both ends O(1).
//

import DequeModule
import Foundation

/// A thread-safe blocking FIFO queue.
///
/// Producers `enqueue` to the back without blocking; a taker blocks inside
/// `take` until an element arrives and removes it from the front.
///
/// `close` requests closure: remaining elements stay available to takers,
/// and the first `take` that observes a closed, empty queue returns `nil`
/// and seals the queue — the terminal state in which further `enqueue`
/// returns `false` and discards its element.
public final class BlockingQueue<Element: ~Copyable>: @unchecked Sendable where Element: Sendable {
  private let condition = NSCondition()
  private var elements = UniqueDeque<Element>()
  private var closed = false
  private var sealed = false

  public init() {
    condition.name = "BlockingQueue"
  }

  /// Appends `element` to the back of the queue and wakes one waiter.
  ///
  /// Returns `false` (discarding `element`) in the sealed state — after
  /// `close()` and a full drain there is no taker left to run it.
  @discardableResult
  public func enqueue(_ element: consuming Element) -> Bool {
    condition.lock()
    if sealed {
      condition.unlock()
      return false
    }
    elements.append(element)
    condition.unlock()
    condition.signal()
    return true
  }

  /// Blocks until an element is available, then removes and returns it
  /// from the front.
  ///
  /// Returns `nil` once the queue is closed and fully drained; that same
  /// critical section seals the queue.
  public func take() -> Element? {
    condition.lock()
    while elements.isEmpty && !closed {
      condition.wait()
    }
    if let element = elements.popFirst() {
      condition.unlock()
      return element
    }
    sealed = true
    condition.broadcast()
    condition.unlock()
    return nil
  }

  /// Requests closure. Remaining elements stay available to takers;
  /// idempotent.
  public func close() {
    condition.lock()
    closed = true
    condition.unlock()
    condition.broadcast()
  }

  /// Whether closure has been requested.
  public var isClosed: Bool {
    condition.lock()
    defer { condition.unlock() }
    return closed
  }

  /// Terminal state: closed and fully drained. `enqueue` returns `false`
  /// and `take` returns `nil` immediately.
  public var isSealed: Bool {
    condition.lock()
    defer { condition.unlock() }
    return sealed
  }

  /// Blocks until the queue is sealed.
  public func waitUntilSealed() {
    condition.lock()
    while !sealed {
      condition.wait()
    }
    condition.unlock()
  }
}
