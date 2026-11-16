//
//  QueueConsumer.swift
//  BlockingQueue
//

/// Single, move-only consumer end. `take()` blocks until an element is
/// available; `nil` means the queue is sealed.
///
/// Releasing the end seals the queue from the consumer side.
public struct QueueConsumer<Element: ~Copyable>: ~Copyable, Sendable {
  let cell: QueueCell<Element>

  /// Blocks until an element is available, then removes and returns it from
  /// the front.
  ///
  /// Returns `nil` once the queue is closed and fully drained (or no
  /// consumer end remains); that observation seals the queue.
  public func take() -> sending Element? {
    cell.take()
  }

  /// Upgrades this end into a cloneable, `Sendable` consumer.
  public consuming func share() -> SharedQueueConsumer<Element> {
    let shared = SharedQueueConsumer(cell: cell)
    // Pin `self` past the registration above so its deinit disconnects this
    // end only after the shared end registered (count 1 → 2 → 1).
    withExtendedLifetime(self) {}
    return shared
  }

  deinit {
    cell.removeConsumer()
  }
}

/// Cloneable, `Sendable` consumer end: any number of takers sharing one
/// object, competing per element.
public final class SharedQueueConsumer<Element: ~Copyable>: Sendable {
  private let cell: QueueCell<Element>

  init(cell: QueueCell<Element>) {
    self.cell = cell
    cell.addConsumer()
  }

  deinit {
    cell.removeConsumer()
  }

  /// Blocks until an element is available, then removes and returns it from
  /// the front.
  ///
  /// Returns `nil` once the queue is closed and fully drained (or no
  /// consumer end remains); that observation seals the queue.
  public func take() -> sending Element? {
    cell.take()
  }
}
