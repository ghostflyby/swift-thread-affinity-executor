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

  public typealias SPSCProducer = UnboundedQueueProducer<Element>
  public typealias SPSCConsumer = QueueConsumer<Element>
  public typealias SPSCSupervisor = QueueSupervisor<Element>

  public typealias MPSCProducer = SharedUnboundedQueueProducer<Element>
  public typealias MPSCConsumer = QueueConsumer<Element>
  public typealias MPSCSupervisor = QueueSupervisor<Element>

  public typealias SPMCProducer = UnboundedQueueProducer<Element>
  public typealias SPMCConsumer = SharedQueueConsumer<Element>
  public typealias SPMCSupervisor = QueueSupervisor<Element>

  public typealias MPMCProducer = SharedUnboundedQueueProducer<Element>
  public typealias MPMCConsumer = SharedQueueConsumer<Element>
  public typealias MPMCSupervisor = QueueSupervisor<Element>

  let cell: QueueCell<Element>

  public init() {
    cell = QueueCell(capacity: nil)
  }

  /// Splits the queue into one exclusive producer end, one exclusive
  /// consumer end and a supervisor — SPSC. Consumes the token.
  public consuming func split(_ option: SPSCOption) -> sending UnboundedSPSCQueueEnds<Element> {
    cell.registerEnds()
    return UnboundedSPSCQueueEnds<Element>(
      producer: UnboundedQueueProducer(cell: cell),
      consumer: QueueConsumer(cell: cell),
      supervisor: QueueSupervisor(cell: cell)
    )
  }

  /// Splits the queue into a cloneable producer end, one exclusive consumer
  /// end and a supervisor — MPSC. Consumes the token.
  public consuming func split(_ option: MPSCOption) -> sending UnboundedMPSCQueueEnds<Element> {
    cell.registerEnds()
    return UnboundedMPSCQueueEnds<Element>(
      producer: SharedUnboundedQueueProducer(cell: cell),
      consumer: QueueConsumer(cell: cell),
      supervisor: QueueSupervisor(cell: cell)
    )
  }

  /// Splits the queue into one exclusive producer end, a cloneable consumer
  /// end and a supervisor — SPMC. Consumes the token.
  public consuming func split(_ option: SPMCOption) -> sending UnboundedSPMCQueueEnds<Element> {
    cell.registerEnds()
    return UnboundedSPMCQueueEnds<Element>(
      producer: UnboundedQueueProducer(cell: cell),
      consumer: SharedQueueConsumer(cell: cell),
      supervisor: QueueSupervisor(cell: cell)
    )
  }

  /// Splits the queue into a cloneable producer end, a cloneable consumer
  /// end and a supervisor — MPMC. Consumes the token.
  public consuming func split(_ option: MPMCOption) -> sending UnboundedMPMCQueueEnds<Element> {
    cell.registerEnds()
    return UnboundedMPMCQueueEnds<Element>(
      producer: SharedUnboundedQueueProducer(cell: cell),
      consumer: SharedQueueConsumer(cell: cell),
      supervisor: QueueSupervisor(cell: cell)
    )
  }
}

/// The capabilities handed out by `UnboundedQueue.split(.spsc)`: move-only.
public typealias UnboundedSPSCQueueEnds<Element: ~Copyable> = QueueEnds<
  UnboundedQueueProducer<Element>, QueueConsumer<Element>, QueueSupervisor<Element>
>

/// The capabilities handed out by `UnboundedQueue.split(.mpsc)`: move-only.
public typealias UnboundedMPSCQueueEnds<Element: ~Copyable> = QueueEnds<
  SharedUnboundedQueueProducer<Element>, QueueConsumer<Element>, QueueSupervisor<Element>
>

/// The capabilities handed out by `UnboundedQueue.split(.spmc)`: move-only.
public typealias UnboundedSPMCQueueEnds<Element: ~Copyable> = QueueEnds<
  UnboundedQueueProducer<Element>, SharedQueueConsumer<Element>, QueueSupervisor<Element>
>

/// The capabilities handed out by `UnboundedQueue.split(.mpmc)`: move-only.
public typealias UnboundedMPMCQueueEnds<Element: ~Copyable> = QueueEnds<
  SharedUnboundedQueueProducer<Element>, SharedQueueConsumer<Element>, QueueSupervisor<Element>
>

/// Single, move-only producer end of an unbounded queue.
///
/// Minted by `split(.spsc)` / `split(.spmc)`. Releasing the end
/// (or `finish()`) closes production. There is no `share()` upgrade: the
/// endpoint shape is fixed at split time.
public struct UnboundedQueueProducer<Element: ~Copyable>: ~Copyable, Sendable,
  QueueProducerProtocol, BlockingQueueProducerProtocol, SingleProducerProtocol
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

  /// Appends `element`, waiting only while a bounded queue is full; this
  /// queue is unbounded, so the call never waits.
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

/// Cloneable, `Sendable` producer end: any number of producers sharing one
/// object. Minted by `split(.mpsc)` / `split(.mpmc)`.
///
/// Production closes through `close()` or once the last producer reference
/// is released.
public final class SharedUnboundedQueueProducer<Element: ~Copyable>: Sendable,
  QueueProducerProtocol, BlockingQueueProducerProtocol, MultiProducerProtocol
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

  /// Appends `element`, waiting only while a bounded queue is full; this
  /// queue is unbounded, so the call never waits.
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
