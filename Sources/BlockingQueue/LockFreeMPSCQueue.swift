//
//  LockFreeMPSCQueue.swift
//  BlockingQueue
//
//  Lock-free unbounded multi-producer / single-consumer queue: one exclusive
//  consumer end, one cloneable producer end and a control handle.
//

/// Linear construction token for a lock-free unbounded MPSC queue.
///
/// `split(.mpsc)` consumes the token and hands out one cloneable producer
/// end, one exclusive consumer end and a supervisor. Unlike `Queue`, this
/// implementation is not backed by a lock: producers publish nodes through an
/// atomic tail exchange, and the single consumer advances a stub head.
///
/// Elements need not be `Sendable`: ownership transfers through the queue —
/// `consuming sending` in on enqueue, `sending` out on take. The handoff is
/// compiler-enforced (the check lives in the SIL sendability pass, so it
/// surfaces on build rather than in a pure typecheck).
///
/// The queue is unbounded and allocates one node per enqueued element;
/// consumed nodes are freed by the consumer. `close()`/`enqueue` race safely:
/// an element accepted by `enqueue` is delivered, and the queue never seals
/// while a producer is between its close check and its link.
///
/// `split(_:)` returns a `sending` bundle: the shared producer and supervisor
/// can cross domains, while the exclusive consumer is non-`Sendable` and stays
/// in the domain it was handed to.
public struct LockFreeMPSCQueue<Element: ~Copyable>: ~Copyable, Sendable {
  let cell: LockFreeMPSCQueueCell<Element>

  public init() {
    cell = LockFreeMPSCQueueCell()
  }

  /// Splits the queue into a cloneable producer end, one exclusive consumer
  /// end and a supervisor. Consumes the token. The result is `sending`: the
  /// consumer end is non-`Sendable`, so the bundle must be handed to one
  /// domain rather than shared.
  public consuming func split(_ option: MPSCOption) -> sending LockFreeMPSCQueueEnds<Element> {
    cell.registerEnds()
    return LockFreeMPSCQueueEnds<Element>(
      producer: LockFreeMPSCQueueProducer(cell: cell),
      consumer: LockFreeMPSCQueueConsumer(cell: cell),
      supervisor: LockFreeMPSCQueueSupervisor(cell: cell)
    )
  }
}

/// The capabilities handed out by `LockFreeMPSCQueue.split(.mpsc)`:
/// move-only and non-`Sendable` because of the exclusive consumer end.
@frozen
public struct LockFreeMPSCQueueEnds<Element: ~Copyable>: ~Copyable {
  public let producer: LockFreeMPSCQueueProducer<Element>
  public let consumer: LockFreeMPSCQueueConsumer<Element>
  public let supervisor: LockFreeMPSCQueueSupervisor<Element>

  init(
    producer: LockFreeMPSCQueueProducer<Element>,
    consumer: consuming LockFreeMPSCQueueConsumer<Element>,
    supervisor: LockFreeMPSCQueueSupervisor<Element>
  ) {
    self.producer = producer
    self.consumer = consumer
    self.supervisor = supervisor
  }
}

/// Cloneable, `Sendable` producer end: any number of producers sharing one
/// object. Minted by `split(.mpsc)`.
///
/// Production closes through `close()` or once the last producer reference
/// is released.
public final class LockFreeMPSCQueueProducer<Element: ~Copyable>: Sendable {
  private let cell: LockFreeMPSCQueueCell<Element>

  init(cell: LockFreeMPSCQueueCell<Element>) {
    // The owning split registered both endpoint sides.
    self.cell = cell
  }

  deinit {
    cell.removeProducer()
  }

  /// Appends `element` to the back without waiting. The queue is unbounded,
  /// so capacity never rejects an element.
  ///
  /// The element is handed over (`consuming sending`): a non-`Sendable`
  /// element must not be used after the call.
  ///
  /// Returns `false` (discarding `element`) once production is closed or no
  /// consumer end remains.
  @discardableResult
  public func enqueue(_ element: consuming sending Element) -> Bool {
    cell.enqueue(element)
  }

  /// Closes production. Remaining elements stay available to consumers.
  public func close() {
    cell.close()
  }
}

/// Single, move-only, non-`Sendable` consumer end. It is confined to one
/// isolation domain; there is exactly one taker, and only one call may be in
/// flight at a time. Non-`Sendable` keeps it from being shared across domains;
/// the cell additionally traps on concurrent entry.
///
/// `tryTake()` never waits; `take()` parks on a condition variable until an
/// element is available or the queue is closed and drained. The data path
/// itself does not take that lock; producers touch it only when a consumer
/// is parked.
public struct LockFreeMPSCQueueConsumer<Element: ~Copyable>: ~Copyable {
  let cell: LockFreeMPSCQueueCell<Element>

  init(cell: LockFreeMPSCQueueCell<Element>) {
    // The owning split registered both endpoint sides.
    self.cell = cell
  }

  deinit {
    cell.removeConsumer()
  }

  /// Removes and returns the front element, or `nil` when nothing is
  /// currently linked. Never waits. An empty observation seals the queue only
  /// when it is closed, drained and free of in-flight producers; while a
  /// producer is in flight, `nil` is not terminal, so a non-blocking drain
  /// must continue until `supervisor.isSealed` (or use `take()`).
  ///
  /// The element is handed over (`sending`).
  @discardableResult
  public func tryTake() -> sending Element? {
    cell.tryTake()
  }

  /// Blocks until an element is available, then removes and returns it from
  /// the front.
  ///
  /// Returns `nil` once the queue is closed and fully drained; that
  /// observation seals the queue.
  @available(*, noasync)
  @discardableResult
  public func take() -> sending Element? {
    cell.take()
  }
}

/// Non-minting control handle over a split lock-free MPSC queue: closes
/// production, awaits the terminal state and inspects. Cannot create ends.
///
/// There is deliberately no `drainAll`: the consumer end is exclusive, so a
/// drain from the control handle would introduce a second taker and break
/// the single-consumer algorithm. Drain through the consumer instead.
public final class LockFreeMPSCQueueSupervisor<Element: ~Copyable>: Sendable {
  let cell: LockFreeMPSCQueueCell<Element>

  init(cell: LockFreeMPSCQueueCell<Element>) {
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

  /// Terminal state: closed, fully drained and free of in-flight producers,
  /// or no consumer end remains (after any in-flight producer finishes).
  public var isSealed: Bool {
    cell.isSealed
  }

  /// Number of elements currently queued. Approximate under concurrent
  /// producers and takers; in-flight enqueues may transiently over-count, and
  /// the value never goes negative.
  public var count: Int {
    cell.elementCount
  }

  /// Blocks until the queue is sealed.
  @available(*, noasync)
  public func waitUntilSealed() {
    cell.waitUntilSealed()
  }
}
