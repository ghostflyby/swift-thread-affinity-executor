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
