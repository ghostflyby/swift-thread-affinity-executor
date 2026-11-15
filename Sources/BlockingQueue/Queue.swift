//
//  Queue.swift
//  BlockingQueue
//
//  Unbounded (auto-growing) queue family: a linear construction token that
//  splits into producer and consumer ends plus a supervisor.
//

/// Linear construction token for an unbounded queue.
///
/// `split()` consumes the token; afterwards no further ends can be minted.
///
/// Elements need not be `Sendable`: ownership transfers through the queue —
/// `consuming sending` in on enqueue, `sending` out on take. The handoff
/// is compiler-enforced (the check lives in the SIL sendability pass, so
/// it surfaces on build rather than in a pure typecheck).
public struct Queue<Element: ~Copyable>: ~Copyable, Sendable {
  let cell: QueueCell<Element>

  public init() {
    cell = QueueCell(capacity: nil)
  }

  /// Splits the queue into one producer end, one consumer end and a
  /// supervisor. Consumes the token.
  public consuming func split() -> QueueEnds<Element> {
    cell.addProducer()
    cell.addConsumer()
    return QueueEnds(
      producer: QueueProducer(cell: cell),
      consumer: QueueConsumer(cell: cell),
      supervisor: QueueSupervisor(cell: cell)
    )
  }
}

/// The three capabilities handed out by `Queue.split()`: move-only.
@frozen
public struct QueueEnds<Element: ~Copyable>: ~Copyable, Sendable {
  public let producer: QueueProducer<Element>
  public let consumer: QueueConsumer<Element>
  public let supervisor: QueueSupervisor<Element>

  init(
    producer: consuming QueueProducer<Element>,
    consumer: consuming QueueConsumer<Element>,
    supervisor: QueueSupervisor<Element>
  ) {
    self.producer = producer
    self.consumer = consumer
    self.supervisor = supervisor
  }
}

/// Single, move-only producer end of an unbounded queue.
///
/// Releasing the end (or `finish()`) closes production.
public struct QueueProducer<Element: ~Copyable>: ~Copyable, Sendable {
  let cell: QueueCell<Element>

  init(cell: QueueCell<Element>) {
    self.cell = cell
  }

  /// Appends `element` to the back; the queue grows as needed.
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

  /// Upgrades this end into a cloneable, `Sendable` producer.
  public consuming func share() -> SharedQueueProducer<Element> {
    let shared = SharedQueueProducer(cell: cell)
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

/// Cloneable, `Sendable` producer end: any number of producers sharing one
/// object.
///
/// Production closes through `close()` or once every producer end (shared
/// or unique) is released.
public final class SharedQueueProducer<Element: ~Copyable>: Sendable {
  private let cell: QueueCell<Element>

  init(cell: QueueCell<Element>) {
    self.cell = cell
    cell.addProducer()
  }

  deinit {
    cell.removeProducer()
  }

  /// Appends `element` to the back; the queue grows as needed.
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
