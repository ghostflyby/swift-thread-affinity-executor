//
//  LinkedQueue.swift
//  BlockingQueue
//
//  Unbounded linked multi-producer / single-consumer queue: one exclusive
//  consumer end, one cloneable producer end and a control handle.
//

/// Linear construction token for an unbounded linked queue.
///
/// `split(.mpsc)` consumes the token and hands out one cloneable producer
/// end, one exclusive consumer end and a supervisor; `split(.spsc)` hands out
/// a move-only wrapper over the same producer reference instead. Unlike
/// `UnboundedQueue`, this implementation is not backed by a lock: producers
/// publish nodes through an atomic tail exchange, and the single consumer
/// advances a stub head.
///
/// Elements need not be `Sendable`: ownership transfers through the queue —
/// `consuming sending` in on enqueue, `sending` out on take. The handoff is
/// compiler-enforced (the check lives in the SIL sendability pass, so it
/// surfaces on build rather than in a pure typecheck).
///
/// The queue is unbounded and allocates one node per enqueued element;
/// consumed nodes are freed by the consumer. `close()`/`enqueue` race safely:
/// while a consumer end remains, an element accepted by `enqueue` is
/// delivered; releasing the consumer end waits for in-flight producers before
/// the queue seals, and an enqueue accepted just before that release whose
/// link lands after it is stranded without a taker.
///
/// Both splits return a `sending` bundle: the `.mpsc` producer and the
/// supervisor can cross domains, while the exclusive consumer is
/// non-`Sendable` and stays in the domain it was handed to.
public struct LinkedQueue<Element: ~Copyable>: ~Copyable, Sendable,
  MPSCQueueProtocol
{
  public typealias Element = Element

  public typealias SPSCProducer = LinkedQueueExclusiveProducer<Element>
  public typealias SPSCConsumer = LinkedQueueConsumer<Element>
  public typealias SPSCSupervisor = LinkedQueueSupervisor<Element>

  public typealias MPSCProducer = LinkedQueueProducer<Element>
  public typealias MPSCConsumer = LinkedQueueConsumer<Element>
  public typealias MPSCSupervisor = LinkedQueueSupervisor<Element>

  let cell: LockFreeMPSCQueueCell<Element>

  public init() {
    cell = LockFreeMPSCQueueCell()
  }

  /// Splits the queue into the exclusive single-producer view. The producer
  /// end is a move-only wrapper over the shared producer reference; MPSC
  /// refines SPSC. Consumes the token.
  public consuming func split(_ option: SPSCOption) -> sending LinkedSPSCQueueEnds<Element> {
    cell.registerEnds()
    return LinkedSPSCQueueEnds<Element>(
      producer: LinkedQueueExclusiveProducer(
        LinkedQueueProducer(cell: cell)
      ),
      consumer: LinkedQueueConsumer(cell: cell),
      supervisor: LinkedQueueSupervisor(cell: cell)
    )
  }

  /// Splits the queue into a cloneable producer end, one exclusive consumer
  /// end and a supervisor. Consumes the token. The result is `sending`: the
  /// consumer end is non-`Sendable`, so the bundle must be handed to one
  /// domain rather than shared.
  public consuming func split(_ option: MPSCOption) -> sending LinkedMPSCQueueEnds<Element> {
    cell.registerEnds()
    return LinkedMPSCQueueEnds<Element>(
      producer: LinkedQueueProducer(cell: cell),
      consumer: LinkedQueueConsumer(cell: cell),
      supervisor: LinkedQueueSupervisor(cell: cell)
    )
  }
}

/// The capabilities handed out by `LinkedQueue.split(.mpsc)`:
/// move-only and non-`Sendable` because of the exclusive consumer end.
public typealias LinkedMPSCQueueEnds<Element: ~Copyable> = QueueEnds<
  LinkedQueueProducer<Element>,
  LinkedQueueConsumer<Element>,
  LinkedQueueSupervisor<Element>
>

/// The capabilities handed out by `LinkedQueue.split(.spsc)`: a
/// move-only exclusive producer view over the shared reference, the exclusive
/// consumer and the supervisor.
public typealias LinkedSPSCQueueEnds<Element: ~Copyable> = QueueEnds<
  LinkedQueueExclusiveProducer<Element>,
  LinkedQueueConsumer<Element>,
  LinkedQueueSupervisor<Element>
>

/// Move-only producer view minted by `split(.spsc)`.
///
/// The lock-free queue's native producer end is a cloneable class reference.
/// This wrapper holds that reference without exposing it, so an SPSC split
/// yields a genuinely noncopyable producer end while the queue still supports
/// multiple producers in its MPSC shape.
public struct LinkedQueueExclusiveProducer<Element: ~Copyable>: ~Copyable,
  Sendable, QueueProducerProtocol, SingleProducerProtocol
{
  public typealias Element = Element

  private let producer: LinkedQueueProducer<Element>

  init(_ producer: consuming LinkedQueueProducer<Element>) {
    self.producer = producer
  }

  /// Appends `element` to the back without waiting.
  @discardableResult
  public func enqueue(_ element: consuming sending Element) -> Bool {
    producer.enqueue(element)
  }

  /// Closes production. Remaining elements stay available to consumers.
  public func close() {
    producer.close()
  }
}

/// Cloneable, `Sendable` producer end: any number of producers sharing one
/// object. Minted by `split(.mpsc)`.
///
/// Production closes through `close()` or once the last producer reference
/// is released.
public final class LinkedQueueProducer<Element: ~Copyable>: Sendable,
  QueueProducerProtocol, MultiProducerProtocol
{
  public typealias Element = Element

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
public struct LinkedQueueConsumer<Element: ~Copyable>: ~Copyable,
  QueueConsumerProtocol, TryQueueConsumerProtocol, SingleConsumerProtocol
{
  public typealias Element = Element

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
public final class LinkedQueueSupervisor<Element: ~Copyable>: Sendable,
  QueueLifecycleProtocol, QueueInspectionProtocol
{
  public typealias Element = Element

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
