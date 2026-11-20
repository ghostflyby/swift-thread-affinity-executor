//
//  QueueSupervisor.swift
//  BlockingQueue
//

import DequeModule

/// Non-minting control handle over a split queue: closes production, awaits
/// the terminal state, inspects and drains. Cannot create ends.
public final class QueueSupervisor<Element: ~Copyable>: Sendable, QueueLifecycleProtocol,
  QueueInspectionProtocol, QueueDrainProtocol
{
  public typealias Element = Element

  let cell: QueueCell<Element>

  init(cell: QueueCell<Element>) {
    self.cell = cell
  }

  /// Closes production without holding any producer end: further enqueues
  /// fail. Remaining elements stay available to consumers.
  public func close() {
    cell.close()
  }

  /// Whether production is closed.
  public var isClosed: Bool {
    cell.isClosed
  }

  /// Terminal state: closed and fully drained, or no consumer end remains.
  public var isSealed: Bool {
    cell.isSealed
  }

  /// Number of elements currently queued.
  public var count: Int {
    cell.count
  }

  /// Blocks until the queue is sealed.
  @available(*, noasync)
  public func waitUntilSealed() {
    cell.waitUntilSealed()
  }

  /// Removes and returns everything currently queued, in order.
  public func drainAll() -> sending UniqueDeque<Element> {
    cell.drainAll()
  }
}
