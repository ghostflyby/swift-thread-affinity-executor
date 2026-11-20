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
