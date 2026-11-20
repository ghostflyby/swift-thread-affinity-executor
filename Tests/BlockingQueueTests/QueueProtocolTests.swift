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
  let ends = Queue<Int>().split(.spsc)
  roundTrip(ends.producer, ends.consumer)
  release(ends.consumer)
}

@Test func lockFreeEndpointsShareCapabilityProtocols() {
  let ends = LockFreeMPSCQueue<Int>().split(.mpsc)
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
  roundTripSPSC(Queue<Int>())
  roundTripMPSC(Queue<Int>())
  roundTripSPMC(Queue<Int>())
  roundTripMPMC(Queue<Int>())

  roundTripSPSC(BoundedQueue<Int>(capacity: 4))
  roundTripMPSC(BoundedQueue<Int>(capacity: 4))
  roundTripSPMC(BoundedQueue<Int>(capacity: 4))
  roundTripMPMC(BoundedQueue<Int>(capacity: 4))
}

@Test func lockFreeTokenConformsToMPSCProtocol() {
  roundTripMPSC(LockFreeMPSCQueue<Int>())
}

@Test func lockFreeTokenCanBeUsedAsSPSC() {
  roundTripSPSC(LockFreeMPSCQueue<Int>())
}

private func useMPSCAsSPSC<Q: MPSCQueueProtocol & ~Copyable>(
  _ token: consuming Q
) where Q.Element == Int {
  roundTripSPSC(token)
}

@Test func mpscCategoryRefinesSPSC() {
  useMPSCAsSPSC(Queue<Int>())
  useMPSCAsSPSC(LockFreeMPSCQueue<Int>())
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

  roundTripMPSC(Queue<SingleShot>())
  roundTripMPSC(BoundedQueue<SingleShot>(capacity: 2))
  roundTripMPSC(LockFreeMPSCQueue<SingleShot>())
}
