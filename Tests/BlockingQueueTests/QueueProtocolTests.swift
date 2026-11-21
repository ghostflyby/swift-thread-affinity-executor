//
//  QueueProtocolTests.swift
//  BlockingQueueTests
//
//  The lock-based and lock-free implementations expose the same capability
//  protocols, so generic code can drive both.
//

import BlockingQueue
import Testing

private func roundTrip<
  P: QueueProducerProtocol & ~Copyable,
  C: QueueConsumerProtocol & ~Copyable
>(
  _ producer: consuming P,
  _ consumer: borrowing C
) where P.Element == Int, C.Element == Int {
  let accepted = producer.enqueue(1)
  #expect(accepted)
  let value = consumer.take()
  #expect(value == 1)
}

@Test func lockBasedEndpointsShareCapabilityProtocols() {
  let ends = UnboundedQueue<Int>().split(.spsc)
  roundTrip(ends.producer, ends.consumer)
  release(ends.consumer)
}

@Test func lockFreeEndpointsShareCapabilityProtocols() {
  let ends = LinkedQueue<Int>().split(.mpsc)
  roundTrip(ends.producer, ends.consumer)
  release(ends.consumer)
}

private func roundTripSPSC<Q: SPSCQueueProtocol & ~Copyable>(
  _ token: consuming Q
) where Q.Element == Int {
  let ends = token.split(.spsc)
  let accepted = ends.producer.enqueue(1)
  #expect(accepted)
  let value = ends.consumer.take()
  #expect(value == 1)
}

private func roundTripMPSC<Q: MPSCQueueProtocol & ~Copyable>(
  _ token: consuming Q
) where Q.Element == Int {
  let ends = token.split(.mpsc)
  let accepted = ends.producer.enqueue(1)
  #expect(accepted)
  let value = ends.consumer.take()
  #expect(value == 1)
}

private func roundTripSPMC<Q: SPMCQueueProtocol & ~Copyable>(
  _ token: consuming Q
) where Q.Element == Int {
  let ends = token.split(.spmc)
  let accepted = ends.producer.enqueue(1)
  #expect(accepted)
  let value = ends.consumer.take()
  #expect(value == 1)
}

private func roundTripMPMC<Q: MPMCQueueProtocol & ~Copyable>(
  _ token: consuming Q
) where Q.Element == Int {
  let ends = token.split(.mpmc)
  let accepted = ends.producer.enqueue(1)
  #expect(accepted)
  let value = ends.consumer.take()
  #expect(value == 1)
}

@Test func lockBasedTokensConformToEveryCategoryProtocol() {
  roundTripSPSC(UnboundedQueue<Int>())
  roundTripMPSC(UnboundedQueue<Int>())
  roundTripSPMC(UnboundedQueue<Int>())
  roundTripMPMC(UnboundedQueue<Int>())

  roundTripSPSC(BoundedQueue<Int>(capacity: 4))
  roundTripMPSC(BoundedQueue<Int>(capacity: 4))
  roundTripSPMC(BoundedQueue<Int>(capacity: 4))
  roundTripMPMC(BoundedQueue<Int>(capacity: 4))
}

@Test func lockFreeTokenConformsToMPSCProtocol() {
  roundTripMPSC(LinkedQueue<Int>())
}

@Test func lockFreeTokenCanBeUsedAsSPSC() {
  roundTripSPSC(LinkedQueue<Int>())
}

private func useMPSCAsSPSC<Q: MPSCQueueProtocol & ~Copyable>(
  _ token: consuming Q
) where Q.Element == Int {
  roundTripSPSC(token)
}

@Test func mpscCategoryRefinesSPSC() {
  useMPSCAsSPSC(UnboundedQueue<Int>())
  useMPSCAsSPSC(LinkedQueue<Int>())
}

private func inspectLifecycle<S: QueueLifecycleProtocol & QueueInspectionProtocol>(
  _ supervisor: S
) where S.Element == Int {
  #expect(supervisor.count == 1)
  #expect(supervisor.isClosed == false)
  supervisor.close()
  #expect(supervisor.isClosed)
}

private func drainSupervisor<S: QueueDrainProtocol>(
  _ supervisor: S
) where S.Element == Int {
  var drained = supervisor.drainAll()
  #expect(drained.count == 1)
  #expect(drained.popFirst() == 1)
}

private func exerciseLockBasedSupervisor<Q: MPMCQueueProtocol & ~Copyable>(
  _ token: consuming Q
) where Q.Element == Int, Q.MPMCSupervisor: QueueDrainProtocol {
  let ends = token.split(.mpmc)
  let accepted = ends.producer.enqueue(1)
  #expect(accepted)
  inspectLifecycle(ends.supervisor)
  drainSupervisor(ends.supervisor)
  release(ends.consumer)
}

private func exerciseLockFreeSupervisor<Q: MPSCQueueProtocol & ~Copyable>(
  _ token: consuming Q
) where Q.Element == Int {
  let ends = token.split(.mpsc)
  let accepted = ends.producer.enqueue(1)
  #expect(accepted)
  inspectLifecycle(ends.supervisor)
  release(ends.consumer)
}

@Test func supervisorCapabilitiesAreUsableThroughProtocols() {
  exerciseLockBasedSupervisor(UnboundedQueue<Int>())
  exerciseLockBasedSupervisor(BoundedQueue<Int>(capacity: 2))
  exerciseLockFreeSupervisor(LinkedQueue<Int>())
}

private func blockingEnqueue<P: BlockingQueueProducerProtocol & ~Copyable>(
  _ producer: consuming P
) where P.Element == Int {
  let accepted = producer.enqueueBlocking(1)
  #expect(accepted)
}

private func tryTake<C: TryQueueConsumerProtocol & ~Copyable>(
  _ consumer: borrowing C,
  expecting expected: Int = 1
) where C.Element == Int {
  let value = consumer.tryTake()
  #expect(value == expected)
}

@Test func optionalEndpointCapabilitiesAreUsableThroughProtocols() {
  let bounded = BoundedQueue<Int>(capacity: 1).split(.mpsc)
  blockingEnqueue(bounded.producer)
  release(bounded.consumer)

  let unbounded = UnboundedQueue<Int>().split(.mpsc)
  blockingEnqueue(unbounded.producer)
  release(unbounded.consumer)

  let lockFree = LinkedQueue<Int>().split(.mpsc)
  let accepted = lockFree.producer.enqueue(1)
  #expect(accepted)
  tryTake(lockFree.consumer)
  release(lockFree.consumer)
}

@Test func blockingConsumersShareTryTake() {
  let exclusive = BoundedQueue<Int>(capacity: 2).split(.spsc)
  let accepted = exclusive.producer.enqueue(1)
  #expect(accepted)
  tryTake(exclusive.consumer)
  release(exclusive.consumer)

  let shared = UnboundedQueue<Int>().split(.mpmc)
  #expect(shared.producer.enqueue(1))
  tryTake(shared.consumer)
  release(shared.consumer)
}

@Test func blockingTryTakeReportsEmptyWithoutSealing() {
  let ends = UnboundedQueue<Int>().split(.spsc)
  let empty = ends.consumer.tryTake()
  #expect(empty == nil)
  #expect(ends.supervisor.isSealed == false)

  let accepted = ends.producer.enqueue(1)
  #expect(accepted)
  tryTake(ends.consumer)
  release(ends.consumer)
}

@Test func blockingTryTakeSealsClosedDrainedQueue() {
  let ends = BoundedQueue<Int>(capacity: 1).split(.mpsc)
  ends.producer.close()

  let empty = ends.consumer.tryTake()
  #expect(empty == nil)
  #expect(ends.supervisor.isSealed)
  release(ends.consumer)
}

@Test func categoryProtocolsAcceptNonCopyableElements() {
  func roundTripMPSC<Q: MPSCQueueProtocol & ~Copyable>(
    _ token: consuming Q
  ) where Q.Element == SingleShot {
    let ends = token.split(.mpsc)
    let accepted = ends.producer.enqueue(SingleShot())
    #expect(accepted)
    let value = ends.consumer.take()
    switch consume value {
    case .some: break
    case .none: Issue.record("expected an element")
    }
  }

  func roundTripSPMC<Q: SPMCQueueProtocol & ~Copyable>(
    _ token: consuming Q
  ) where Q.Element == SingleShot {
    let ends = token.split(.spmc)
    let accepted = ends.producer.enqueue(SingleShot())
    #expect(accepted)
    let value = ends.consumer.take()
    switch consume value {
    case .some: break
    case .none: Issue.record("expected an element")
    }
  }

  func roundTripMPMC<Q: MPMCQueueProtocol & ~Copyable>(
    _ token: consuming Q
  ) where Q.Element == SingleShot {
    let ends = token.split(.mpmc)
    let accepted = ends.producer.enqueue(SingleShot())
    #expect(accepted)
    let value = ends.consumer.take()
    switch consume value {
    case .some: break
    case .none: Issue.record("expected an element")
    }
  }

  roundTripMPSC(UnboundedQueue<SingleShot>())
  roundTripMPSC(BoundedQueue<SingleShot>(capacity: 2))
  roundTripMPSC(LinkedQueue<SingleShot>())
  roundTripSPMC(UnboundedQueue<SingleShot>())
  roundTripSPMC(BoundedQueue<SingleShot>(capacity: 2))
  roundTripMPMC(UnboundedQueue<SingleShot>())
  roundTripMPMC(BoundedQueue<SingleShot>(capacity: 2))
}
