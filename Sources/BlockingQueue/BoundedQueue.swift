//
//  BoundedQueue.swift
//  BlockingQueue
//
//  Bounded (fixed-capacity) queue family: a linear construction token whose
//  producer ends offer a blocking enqueue in addition to the try variant.
//

/// Linear construction token for a bounded queue with fixed capacity.
///
/// `split()` consumes the token; afterwards no further ends can be minted.
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

  /// Splits the queue into one producer end, one consumer end and a
  /// supervisor. Consumes the token.
  public consuming func split() -> BoundedQueueEnds<Element> {
    cell.addProducer()
    cell.addConsumer()
    return BoundedQueueEnds(
      producer: BoundedQueueProducer(cell: cell),
      consumer: QueueConsumer(cell: cell),
      supervisor: QueueSupervisor(cell: cell)
    )
  }
}

/// The three capabilities handed out by `BoundedQueue.split()`: move-only.
@frozen
public struct BoundedQueueEnds<Element: ~Copyable>: ~Copyable, Sendable {
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

/// Single, move-only producer end of a bounded queue.
///
/// Releasing the end (or `finish()`) closes production.
public struct BoundedQueueProducer<Element: ~Copyable>: ~Copyable, Sendable {
  let cell: QueueCell<Element>

  init(cell: QueueCell<Element>) {
    self.cell = cell
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
  @discardableResult
  public func enqueueBlocking(_ element: consuming sending Element) -> Bool {
    cell.enqueueBlocking(element)
  }

  /// Upgrades this end into a cloneable, `Sendable` producer.
  public consuming func share() -> SharedBoundedQueueProducer<Element> {
    let shared = SharedBoundedQueueProducer(cell: cell)
    // Pin `self` past the registration above so its deinit disconnects this
    // end only after the shared end registered (count 1 → 2 → 1).
    withExtendedLifetime(self) {}
    return shared
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
/// producers sharing one object.
public final class SharedBoundedQueueProducer<Element: ~Copyable>: Sendable {
  private let cell: QueueCell<Element>

  init(cell: QueueCell<Element>) {
    self.cell = cell
    cell.addProducer()
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
  @discardableResult
  public func enqueueBlocking(_ element: consuming sending Element) -> Bool {
    cell.enqueueBlocking(element)
  }

  /// Closes production. Remaining elements stay available to consumers.
  public func close() {
    cell.close()
  }
}
