//
//  UnboundedQueue.swift
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
public struct UnboundedQueue<Element: ~Copyable>: ~Copyable, Sendable, MPMCQueueProtocol {
  public typealias Element = Element

  public typealias SPSCProducer = QueueProducer<Element>
  public typealias SPSCConsumer = QueueConsumer<Element>
  public typealias SPSCSupervisor = QueueSupervisor<Element>

  public typealias MPSCProducer = SharedQueueProducer<Element>
  public typealias MPSCConsumer = QueueConsumer<Element>
  public typealias MPSCSupervisor = QueueSupervisor<Element>

  public typealias SPMCProducer = QueueProducer<Element>
  public typealias SPMCConsumer = SharedQueueConsumer<Element>
  public typealias SPMCSupervisor = QueueSupervisor<Element>

  public typealias MPMCProducer = SharedQueueProducer<Element>
  public typealias MPMCConsumer = SharedQueueConsumer<Element>
  public typealias MPMCSupervisor = QueueSupervisor<Element>

  let cell: QueueCell<Element>

  public init() {
    cell = QueueCell(capacity: nil)
  }

  /// Splits the queue into one exclusive producer end, one exclusive
  /// consumer end and a supervisor — SPSC. Consumes the token.
  public consuming func split(_ option: SPSCOption) -> sending SPSCQueueEnds<Element> {
    cell.registerEnds()
    return SPSCQueueEnds<Element>(
      producer: QueueProducer(cell: cell),
      consumer: QueueConsumer(cell: cell),
      supervisor: QueueSupervisor(cell: cell)
    )
  }

  /// Splits the queue into a cloneable producer end, one exclusive consumer
  /// end and a supervisor — MPSC. Consumes the token.
  public consuming func split(_ option: MPSCOption) -> sending MPSCQueueEnds<Element> {
    cell.registerEnds()
    return MPSCQueueEnds<Element>(
      producer: SharedQueueProducer(cell: cell),
      consumer: QueueConsumer(cell: cell),
      supervisor: QueueSupervisor(cell: cell)
    )
  }

  /// Splits the queue into one exclusive producer end, a cloneable consumer
  /// end and a supervisor — SPMC. Consumes the token.
  public consuming func split(_ option: SPMCOption) -> sending SPMCQueueEnds<Element> {
    cell.registerEnds()
    return SPMCQueueEnds<Element>(
      producer: QueueProducer(cell: cell),
      consumer: SharedQueueConsumer(cell: cell),
      supervisor: QueueSupervisor(cell: cell)
    )
  }

  /// Splits the queue into a cloneable producer end, a cloneable consumer
  /// end and a supervisor — MPMC. Consumes the token.
  public consuming func split(_ option: MPMCOption) -> sending MPMCQueueEnds<Element> {
    cell.registerEnds()
    return MPMCQueueEnds<Element>(
      producer: SharedQueueProducer(cell: cell),
      consumer: SharedQueueConsumer(cell: cell),
      supervisor: QueueSupervisor(cell: cell)
    )
  }
}

/// The capabilities handed out by `UnboundedQueue.split(.spsc)`: move-only.
public typealias SPSCQueueEnds<Element: ~Copyable> = QueueEnds<
  QueueProducer<Element>, QueueConsumer<Element>, QueueSupervisor<Element>
>

/// The capabilities handed out by `UnboundedQueue.split(.mpsc)`: move-only.
public typealias MPSCQueueEnds<Element: ~Copyable> = QueueEnds<
  SharedQueueProducer<Element>, QueueConsumer<Element>, QueueSupervisor<Element>
>

/// The capabilities handed out by `UnboundedQueue.split(.spmc)`: move-only.
public typealias SPMCQueueEnds<Element: ~Copyable> = QueueEnds<
  QueueProducer<Element>, SharedQueueConsumer<Element>, QueueSupervisor<Element>
>

/// The capabilities handed out by `UnboundedQueue.split(.mpmc)`: move-only.
public typealias MPMCQueueEnds<Element: ~Copyable> = QueueEnds<
  SharedQueueProducer<Element>, SharedQueueConsumer<Element>, QueueSupervisor<Element>
>

/// Single, move-only producer end of an unbounded queue.
///
/// Minted by `split(.spsc)` / `split(.spmc)`. Releasing the end
/// (or `finish()`) closes production. There is no `share()` upgrade: the
/// endpoint shape is fixed at split time.
public struct QueueProducer<Element: ~Copyable>: ~Copyable, Sendable,
  QueueProducerProtocol, SingleProducerProtocol
{
  public typealias Element = Element

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
public final class SharedQueueProducer<Element: ~Copyable>: Sendable,
  QueueProducerProtocol, MultiProducerProtocol
{
  public typealias Element = Element

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
