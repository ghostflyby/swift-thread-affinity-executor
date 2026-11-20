//
//  QueueProtocols.swift
//  BlockingQueue
//
//  Capability protocols shared by the lock-based and lock-free queues, plus
//  the generic ends bundle that composes them.
//

import DequeModule

/// Non-blocking enqueue capability.
public protocol QueueProducerProtocol<Element>: ~Copyable, Sendable {
  associatedtype Element: ~Copyable

  @discardableResult
  func enqueue(_ element: consuming sending Element) -> Bool
}

/// Enqueue that waits while a bounded queue is full.
public protocol BlockingQueueProducerProtocol<Element>: ~Copyable, QueueProducerProtocol
where Element: ~Copyable {
  @available(*, noasync)
  @discardableResult
  func enqueueBlocking(_ element: consuming sending Element) -> Bool
}

/// Blocking take capability.
public protocol QueueConsumerProtocol<Element>: ~Copyable {
  associatedtype Element: ~Copyable

  @available(*, noasync)
  @discardableResult
  func take() -> sending Element?
}

/// Non-blocking dequeue capability, added where the implementation supports it.
public protocol TryQueueConsumerProtocol<Element>: ~Copyable, QueueConsumerProtocol
where Element: ~Copyable {
  @discardableResult
  func tryTake() -> sending Element?
}

/// Close/seal capability; mode-independent and element-independent.
public protocol QueueLifecycleProtocol: Sendable {
  func close()
  var isClosed: Bool { get }
  var isSealed: Bool { get }

  @available(*, noasync)
  func waitUntilSealed()
}

/// Element-count inspection.
public protocol QueueInspectionProtocol<Element>: Sendable {
  associatedtype Element: ~Copyable
  var count: Int { get }
}

/// Supervisor-side drain, available only when the implementation can hand the
/// queued elements to a second taker.
public protocol QueueDrainProtocol<Element>: Sendable {
  associatedtype Element: ~Copyable
  func drainAll() -> sending UniqueDeque<Element>
}

/// The producer/consumer/supervisor triple handed out by every split.
///
/// `Element` is shared by all three. The supervisor provides lifecycle and
/// inspection; draining remains an optional capability (`QueueDrainProtocol`)
/// because the lock-free consumer end is exclusive.
@frozen
public struct QueueEnds<
  P: QueueProducerProtocol & ~Copyable,
  C: QueueConsumerProtocol & ~Copyable,
  S: QueueLifecycleProtocol & QueueInspectionProtocol
>: ~Copyable
where P.Element == C.Element, P.Element == S.Element, P.Element: ~Copyable {
  public let producer: P
  public let consumer: C
  public let supervisor: S

  init(producer: consuming P, consumer: consuming C, supervisor: S) {
    self.producer = producer
    self.consumer = consumer
    self.supervisor = supervisor
  }
}

extension QueueEnds: Sendable where P: Sendable, C: Sendable, S: Sendable {}

/// A producer end usable as the only producer.
public protocol SingleProducerProtocol<Element>: QueueProducerProtocol, ~Copyable
where Element: ~Copyable {}

/// A producer end that multiple producers may share; usable as a single one.
public protocol MultiProducerProtocol<Element>: SingleProducerProtocol
where Element: ~Copyable {}

/// A consumer end usable as the only consumer.
public protocol SingleConsumerProtocol<Element>: QueueConsumerProtocol, ~Copyable
where Element: ~Copyable {}

/// A consumer end that multiple consumers may share; usable as a single one.
public protocol MultiConsumerProtocol<Element>: SingleConsumerProtocol, Sendable
where Element: ~Copyable {}

/// Split capability for single-producer / single-consumer ends.
public protocol SPSCQueueProtocol<Element>: ~Copyable, Sendable {
  associatedtype Element: ~Copyable
  associatedtype SPSCProducer: SingleProducerProtocol & ~Copyable
  where SPSCProducer.Element == Element
  associatedtype SPSCConsumer: SingleConsumerProtocol & ~Copyable
  where SPSCConsumer.Element == Element
  associatedtype SPSCSupervisor: QueueLifecycleProtocol & QueueInspectionProtocol
  where SPSCSupervisor.Element == Element

  consuming func split(_ option: SPSCOption)
    -> sending QueueEnds<
      SPSCProducer, SPSCConsumer, SPSCSupervisor
    >
}

/// Split capability for multiple-producer / single-consumer ends.
///
/// Refines `SPSCQueueProtocol`: a multi-producer token can also be split into
/// the exclusive single-producer view.
public protocol MPSCQueueProtocol<Element>: SPSCQueueProtocol, ~Copyable, Sendable
where Element: ~Copyable {
  associatedtype MPSCProducer: MultiProducerProtocol
  where MPSCProducer.Element == Element
  associatedtype MPSCConsumer: SingleConsumerProtocol & ~Copyable
  where MPSCConsumer.Element == Element
  associatedtype MPSCSupervisor: QueueLifecycleProtocol & QueueInspectionProtocol
  where MPSCSupervisor.Element == Element

  consuming func split(_ option: MPSCOption)
    -> sending QueueEnds<
      MPSCProducer, MPSCConsumer, MPSCSupervisor
    >
}

/// Split capability for single-producer / multiple-consumer ends.
///
/// Refines `SPSCQueueProtocol`: a multi-consumer token can also be split into
/// the exclusive single-consumer view.
public protocol SPMCQueueProtocol<Element>: SPSCQueueProtocol, ~Copyable, Sendable
where Element: ~Copyable {
  associatedtype SPMCProducer: SingleProducerProtocol & ~Copyable
  where SPMCProducer.Element == Element
  associatedtype SPMCConsumer: MultiConsumerProtocol
  where SPMCConsumer.Element == Element
  associatedtype SPMCSupervisor: QueueLifecycleProtocol & QueueInspectionProtocol
  where SPMCSupervisor.Element == Element

  consuming func split(_ option: SPMCOption)
    -> sending QueueEnds<
      SPMCProducer, SPMCConsumer, SPMCSupervisor
    >
}

/// Split capability for multiple-producer / multiple-consumer ends.
///
/// Refines both `MPSCQueueProtocol` and `SPMCQueueProtocol`, and transitively
/// `SPSCQueueProtocol`.
public protocol MPMCQueueProtocol<Element>: MPSCQueueProtocol, SPMCQueueProtocol,
  ~Copyable, Sendable
where Element: ~Copyable {
  associatedtype MPMCProducer: MultiProducerProtocol
  where MPMCProducer.Element == Element
  associatedtype MPMCConsumer: MultiConsumerProtocol
  where MPMCConsumer.Element == Element
  associatedtype MPMCSupervisor: QueueLifecycleProtocol & QueueInspectionProtocol
  where MPMCSupervisor.Element == Element

  consuming func split(_ option: MPMCOption)
    -> sending QueueEnds<
      MPMCProducer, MPMCConsumer, MPMCSupervisor
    >
}
