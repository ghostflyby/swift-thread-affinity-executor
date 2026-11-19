//
//  BoundedQueue.swift
//  BlockingQueue
//
//  Bounded (fixed-capacity) queue family: a linear construction token whose
//  producer ends offer a blocking enqueue in addition to the try variant.
//

/// Linear construction token for a bounded queue with fixed capacity.
///
/// `split(_:)` consumes the token; afterwards no further ends can be minted.
/// The endpoint shape is fixed at split time — `split(.spsc)` yields
/// exclusive ends, `split(.mpsc)` / `.spmc` / `.mpmc` share the requested
/// side(s). Exclusive ends have no `share()` upgrade, so the chosen shape
/// is a static guarantee.
///
/// Elements need not be `Sendable`: ownership transfers through the queue —
/// `consuming sending` in on enqueue, `sending` out on take. The handoff
/// is compiler-enforced (the check lives in the SIL sendability pass, so
/// it surfaces on build rather than in a pure typecheck).
public struct BoundedQueue<Element: ~Copyable>: ~Copyable, Sendable {
  let cell: QueueCell<Element>

  /// - Precondition: `capacity >= 1`.
  public init(capacity: Int) {
    precondition(capacity >= 1, "capacity must be at least 1")
    cell = QueueCell(capacity: capacity)
  }

  /// Splits the queue into one exclusive producer end, one exclusive
  /// consumer end and a supervisor — SPSC. Consumes the token.
  public consuming func split(_ option: SPSCOption) -> BoundedSPSCQueueEnds<Element> {
    cell.registerEnds()
    return BoundedSPSCQueueEnds(
      producer: BoundedQueueProducer(cell: cell),
      consumer: QueueConsumer(cell: cell),
      supervisor: QueueSupervisor(cell: cell)
    )
  }

  /// Splits the queue into a cloneable producer end, one exclusive consumer
  /// end and a supervisor — MPSC. Consumes the token.
  public consuming func split(_ option: MPSCOption) -> BoundedMPSCQueueEnds<Element> {
    cell.registerEnds()
    return BoundedMPSCQueueEnds(
      producer: SharedBoundedQueueProducer(cell: cell),
      consumer: QueueConsumer(cell: cell),
      supervisor: QueueSupervisor(cell: cell)
    )
  }

  /// Splits the queue into one exclusive producer end, a cloneable consumer
  /// end and a supervisor — SPMC. Consumes the token.
  public consuming func split(_ option: SPMCOption) -> BoundedSPMCQueueEnds<Element> {
    cell.registerEnds()
    return BoundedSPMCQueueEnds(
      producer: BoundedQueueProducer(cell: cell),
      consumer: SharedQueueConsumer(cell: cell),
      supervisor: QueueSupervisor(cell: cell)
    )
  }

  /// Splits the queue into a cloneable producer end, a cloneable consumer
  /// end and a supervisor — MPMC. Consumes the token.
  public consuming func split(_ option: MPMCOption) -> BoundedMPMCQueueEnds<Element> {
    cell.registerEnds()
    return BoundedMPMCQueueEnds(
      producer: SharedBoundedQueueProducer(cell: cell),
      consumer: SharedQueueConsumer(cell: cell),
      supervisor: QueueSupervisor(cell: cell)
    )
  }
}

/// The three capabilities handed out by `BoundedQueue.split(.spsc)`:
/// move-only.
@frozen
public struct BoundedSPSCQueueEnds<Element: ~Copyable>: ~Copyable, Sendable {
  public let producer: BoundedQueueProducer<Element>
  public let consumer: QueueConsumer<Element>
  public let supervisor: QueueSupervisor<Element>

  init(
    producer: consuming BoundedQueueProducer<Element>,
    consumer: consuming QueueConsumer<Element>,
    supervisor: QueueSupervisor<Element>
  ) {
    self.producer = producer
    self.consumer = consumer
    self.supervisor = supervisor
  }
}

/// The capabilities handed out by `BoundedQueue.split(.mpsc)`: move-only.
@frozen
public struct BoundedMPSCQueueEnds<Element: ~Copyable>: ~Copyable, Sendable {
  public let producer: SharedBoundedQueueProducer<Element>
  public let consumer: QueueConsumer<Element>
  public let supervisor: QueueSupervisor<Element>

  init(
    producer: SharedBoundedQueueProducer<Element>,
    consumer: consuming QueueConsumer<Element>,
    supervisor: QueueSupervisor<Element>
  ) {
    self.producer = producer
    self.consumer = consumer
    self.supervisor = supervisor
  }
}

/// The capabilities handed out by `BoundedQueue.split(.spmc)`: move-only.
@frozen
public struct BoundedSPMCQueueEnds<Element: ~Copyable>: ~Copyable, Sendable {
  public let producer: BoundedQueueProducer<Element>
  public let consumer: SharedQueueConsumer<Element>
  public let supervisor: QueueSupervisor<Element>

  init(
    producer: consuming BoundedQueueProducer<Element>,
    consumer: SharedQueueConsumer<Element>,
    supervisor: QueueSupervisor<Element>
  ) {
    self.producer = producer
    self.consumer = consumer
    self.supervisor = supervisor
  }
}

/// The capabilities handed out by `BoundedQueue.split(.mpmc)`: move-only.
@frozen
public struct BoundedMPMCQueueEnds<Element: ~Copyable>: ~Copyable, Sendable {
  public let producer: SharedBoundedQueueProducer<Element>
  public let consumer: SharedQueueConsumer<Element>
  public let supervisor: QueueSupervisor<Element>
}

/// Single, move-only producer end of a bounded queue.
///
/// Minted by `split(.spsc)` / `split(.spmc)`. Releasing the end
/// (or `finish()`) closes production. There is no `share()` upgrade: the
/// endpoint shape is fixed at split time.
public struct BoundedQueueProducer<Element: ~Copyable>: ~Copyable, Sendable {
  let cell: QueueCell<Element>

  /// Tries to append `element` without waiting.
  ///
  /// The element is handed over (`consuming sending`): a non-`Sendable`
  /// element must not be used after the call.
  ///
  /// Returns `false` (discarding `element`) when the queue is full,
  /// production is closed, or no consumer end remains.
  @discardableResult
  public func enqueue(_ element: consuming sending Element) -> Bool {
    cell.enqueue(element)
  }

  /// Appends `element`, waiting while the queue is full.
  ///
  /// The element is handed over (`consuming sending`): a non-`Sendable`
  /// element must not be used after the call.
  ///
  /// Returns `false` (discarding `element`) once production is closed or no
  /// consumer end remains.
  @available(*, noasync)
  @discardableResult
  public func enqueueBlocking(_ element: consuming sending Element) -> Bool {
    cell.enqueueBlocking(element)
  }

  /// Closes production and releases this end. Remaining elements stay
  /// available to consumers.
  public consuming func finish() {
    cell.close()
  }

  deinit {
    cell.removeProducer()
  }
}

/// Cloneable, `Sendable` producer end of a bounded queue: any number of
/// producers sharing one object. Minted by `split(.mpsc)` /
/// `split(.mpmc)`.
///
/// Production closes through `close()` or once the last producer reference
/// is released.
public final class SharedBoundedQueueProducer<Element: ~Copyable>: Sendable {
  private let cell: QueueCell<Element>

  fileprivate init(cell: QueueCell<Element>) {
    // The owning split registered both endpoint sides.
    self.cell = cell
  }

  deinit {
    cell.removeProducer()
  }

  /// Tries to append `element` without waiting.
  ///
  /// The element is handed over (`consuming sending`): a non-`Sendable`
  /// element must not be used after the call.
  ///
  /// Returns `false` (discarding `element`) when the queue is full,
  /// production is closed, or no consumer end remains.
  @discardableResult
  public func enqueue(_ element: consuming sending Element) -> Bool {
    cell.enqueue(element)
  }

  /// Appends `element`, waiting while the queue is full.
  ///
  /// The element is handed over (`consuming sending`): a non-`Sendable`
  /// element must not be used after the call.
  ///
  /// Returns `false` (discarding `element`) once production is closed or no
  /// consumer end remains.
  @available(*, noasync)
  @discardableResult
  public func enqueueBlocking(_ element: consuming sending Element) -> Bool {
    cell.enqueueBlocking(element)
  }

  /// Closes production. Remaining elements stay available to consumers.
  public func close() {
    cell.close()
  }
}
