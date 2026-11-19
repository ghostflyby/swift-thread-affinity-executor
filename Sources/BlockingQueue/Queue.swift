//
//  Queue.swift
//  BlockingQueue
//
//  Unbounded (auto-growing) queue family: a linear construction token that
//  splits into producer and consumer ends plus a supervisor.
//

/// Linear construction token for an unbounded queue.
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
public struct Queue<Element: ~Copyable>: ~Copyable, Sendable {
  let cell: QueueCell<Element>

  public init() {
    cell = QueueCell(capacity: nil)
  }

  /// Splits the queue into one exclusive producer end, one exclusive
  /// consumer end and a supervisor — SPSC. Consumes the token.
  public consuming func split(_ option: SPSCOption) -> SPSCQueueEnds<Element> {
    cell.registerEnds()
    return SPSCQueueEnds(
      producer: QueueProducer(cell: cell),
      consumer: QueueConsumer(cell: cell),
      supervisor: QueueSupervisor(cell: cell)
    )
  }

  /// Splits the queue into a cloneable producer end, one exclusive consumer
  /// end and a supervisor — MPSC. Consumes the token.
  public consuming func split(_ option: MPSCOption) -> MPSCQueueEnds<Element> {
    cell.registerEnds()
    return MPSCQueueEnds(
      producer: SharedQueueProducer(cell: cell),
      consumer: QueueConsumer(cell: cell),
      supervisor: QueueSupervisor(cell: cell)
    )
  }

  /// Splits the queue into one exclusive producer end, a cloneable consumer
  /// end and a supervisor — SPMC. Consumes the token.
  public consuming func split(_ option: SPMCOption) -> SPMCQueueEnds<Element> {
    cell.registerEnds()
    return SPMCQueueEnds(
      producer: QueueProducer(cell: cell),
      consumer: SharedQueueConsumer(cell: cell),
      supervisor: QueueSupervisor(cell: cell)
    )
  }

  /// Splits the queue into a cloneable producer end, a cloneable consumer
  /// end and a supervisor — MPMC. Consumes the token.
  public consuming func split(_ option: MPMCOption) -> MPMCQueueEnds<Element> {
    cell.registerEnds()
    return MPMCQueueEnds(
      producer: SharedQueueProducer(cell: cell),
      consumer: SharedQueueConsumer(cell: cell),
      supervisor: QueueSupervisor(cell: cell)
    )
  }
}

/// The three capabilities handed out by `Queue.split(.spsc)`: move-only.
@frozen
public struct SPSCQueueEnds<Element: ~Copyable>: ~Copyable, Sendable {
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

/// The capabilities handed out by `Queue.split(.mpsc)`: move-only.
@frozen
public struct MPSCQueueEnds<Element: ~Copyable>: ~Copyable, Sendable {
  public let producer: SharedQueueProducer<Element>
  public let consumer: QueueConsumer<Element>
  public let supervisor: QueueSupervisor<Element>

  init(
    producer: SharedQueueProducer<Element>,
    consumer: consuming QueueConsumer<Element>,
    supervisor: QueueSupervisor<Element>
  ) {
    self.producer = producer
    self.consumer = consumer
    self.supervisor = supervisor
  }
}

/// The capabilities handed out by `Queue.split(.spmc)`: move-only.
@frozen
public struct SPMCQueueEnds<Element: ~Copyable>: ~Copyable, Sendable {
  public let producer: QueueProducer<Element>
  public let consumer: SharedQueueConsumer<Element>
  public let supervisor: QueueSupervisor<Element>

  init(
    producer: consuming QueueProducer<Element>,
    consumer: SharedQueueConsumer<Element>,
    supervisor: QueueSupervisor<Element>
  ) {
    self.producer = producer
    self.consumer = consumer
    self.supervisor = supervisor
  }
}

/// The capabilities handed out by `Queue.split(.mpmc)`: move-only.
@frozen
public struct MPMCQueueEnds<Element: ~Copyable>: ~Copyable, Sendable {
  public let producer: SharedQueueProducer<Element>
  public let consumer: SharedQueueConsumer<Element>
  public let supervisor: QueueSupervisor<Element>
}

/// Single, move-only producer end of an unbounded queue.
///
/// Minted by `split(.spsc)` / `split(.spmc)`. Releasing the end
/// (or `finish()`) closes production. There is no `share()` upgrade: the
/// endpoint shape is fixed at split time.
public struct QueueProducer<Element: ~Copyable>: ~Copyable, Sendable {
  let cell: QueueCell<Element>

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
/// object. Minted by `split(.mpsc)` / `split(.mpmc)`.
///
/// Production closes through `close()` or once the last producer reference
/// is released.
public final class SharedQueueProducer<Element: ~Copyable>: Sendable {
  private let cell: QueueCell<Element>

  fileprivate init(cell: QueueCell<Element>) {
    // The owning split registered both endpoint sides.
    self.cell = cell
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
