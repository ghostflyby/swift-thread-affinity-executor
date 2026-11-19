//
//  QueueConsumer.swift
//  BlockingQueue
//

/// Single, move-only consumer end. `take()` blocks until an element is
/// available; `nil` means the queue is sealed.
///
/// Minted by `split()` / `split(.spsc)` / `split(.mpsc)`. Releasing the end
/// seals the queue from the consumer side. There is no `share()` upgrade:
/// the endpoint shape is fixed at split time.
public struct QueueConsumer<Element: ~Copyable>: ~Copyable, Sendable {
  let cell: QueueCell<Element>

  /// Blocks until an element is available, then removes and returns it from
  /// the front.
  ///
  /// Returns `nil` once the queue is closed and fully drained (or no
  /// consumer end remains); that observation seals the queue.
  @available(*, noasync)
  public func take() -> sending Element? {
    cell.take()
  }

  deinit {
    cell.removeConsumer()
  }
}

/// Cloneable, `Sendable` consumer end: any number of takers sharing one
/// object, competing per element. Minted by `split(.spmc)` /
/// `split(.mpmc)`.
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
  @available(*, noasync)
  public func take() -> sending Element? {
    cell.take()
  }
}
