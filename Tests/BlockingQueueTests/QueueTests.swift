//
//  QueueTests.swift
//  BlockingQueueTests
//

import BlockingQueue
import Foundation
import Testing

@Test func fifoOrderWithUniqueEnds() {
  let ends = UnboundedQueue<Int>().split(.spsc)

  for i in 0..<100 {
    let accepted = ends.producer.enqueue(i)
    #expect(accepted)
  }
  for i in 0..<100 {
    let element = ends.consumer.take()
    #expect(element == i)
  }

  release(ends.producer)
  let sealed = ends.consumer.take()
  #expect(sealed == nil)
  #expect(ends.supervisor.isSealed)
  release(ends.consumer)
}

/// `split(.spsc)` mints the exclusive producer/consumer ends.
@Test func spscOptionMintsExclusiveEnds() {
  let ends = UnboundedQueue<Int>().split(.spsc)

  let accepted = ends.producer.enqueue(1)
  #expect(accepted)
  let first = ends.consumer.take()
  #expect(first == 1)
  ends.producer.finish()

  let drained = ends.consumer.take()
  #expect(drained == nil)
  #expect(ends.supervisor.isSealed)
  release(ends.consumer)
}

/// `.mpsc` mints a cloneable producer end while the consumer side stays
/// exclusive; sharing the class reference feeds the one consumer.
@Test func sharedProducerFeedsExclusiveConsumer() {
  let ends = UnboundedQueue<Int>().split(.mpsc)
  let producer = ends.producer
  let clone = producer

  #expect(clone.enqueue(1))
  #expect(producer.enqueue(2))
  producer.close()

  let first = ends.consumer.take()
  #expect(first == 1)
  let second = ends.consumer.take()
  #expect(second == 2)
  let drained = ends.consumer.take()
  #expect(drained == nil)
  #expect(ends.supervisor.isSealed)
  release(ends.consumer)
}

/// Releasing the last shared-producer reference closes production even
/// without an explicit `close()`.
@Test func sharedProducerReleaseClosesProduction() {
  let ends = UnboundedQueue<Int>().split(.mpmc)
  let consumer = ends.consumer
  let supervisor = ends.supervisor
  #expect(supervisor.isClosed == false)

  // dropping the ends' shared-producer reference closes production
  release(ends)
  #expect(supervisor.isClosed)

  let drained = consumer.take()
  #expect(drained == nil)
  #expect(supervisor.isSealed)
}

@Test func blockedTakeWakesOnEnqueue() async {
  let ends = UnboundedQueue<String>().split(.spmc)
  let consumer = ends.consumer

  // The blocking take gets its own thread: a blocked cooperative thread can
  // starve the pool that this test needs to make progress.
  let result = Box<String?>(nil)
  let taken = DispatchSemaphore(value: 0)
  let taker = Thread {
    result.set(consumer.take())
    taken.signal()
  }
  taker.start()
  // the queue is empty and open, so the taker registers as a consumer waiter
  waitForWaiters(ends.supervisor, consumers: 1)

  let accepted = ends.producer.enqueue("hello")
  #expect(accepted)
  await awaitSignal(taken)
  #expect(result.get() == "hello")

  release(ends.producer)
}

@Test func supervisorCloseDrainsThenSeals() {
  let ends = UnboundedQueue<Int>().split(.spsc)
  ends.producer.enqueue(1)
  ends.producer.enqueue(2)

  ends.supervisor.close()
  #expect(ends.supervisor.isClosed)
  #expect(ends.supervisor.isSealed == false)

  // elements enqueued before close stay available
  let first = ends.consumer.take()
  #expect(first == 1)
  let second = ends.consumer.take()
  #expect(second == 2)
  #expect(ends.supervisor.isSealed == false)

  // the take that observes closed-and-empty returns nil and seals
  let drained = ends.consumer.take()
  #expect(drained == nil)
  #expect(ends.supervisor.isSealed)

  release(ends.producer)
  release(ends.consumer)
}

@Test func closeOnEmptySealsOnFirstTake() {
  let ends = UnboundedQueue<Int>().split(.spsc)
  ends.supervisor.close()
  let drained = ends.consumer.take()
  #expect(drained == nil)
  #expect(ends.supervisor.isSealed)

  release(ends.producer)
  release(ends.consumer)
}

@Test func enqueueFailsAfterSeal() {
  let ends = UnboundedQueue<Int>().split(.spsc)
  ends.producer.enqueue(1)
  ends.supervisor.close()
  let first = ends.consumer.take()
  #expect(first == 1)
  let drained = ends.consumer.take()
  #expect(drained == nil)
  #expect(ends.supervisor.isSealed)

  // sealed: the element is discarded and reported as failure
  let accepted = ends.producer.enqueue(42)
  #expect(accepted == false)

  release(ends.producer)
  release(ends.consumer)
}

@Test func producerFinishClosesProduction() {
  let ends = UnboundedQueue<Int>().split(.spsc)
  ends.producer.enqueue(7)

  ends.producer.finish()
  #expect(ends.supervisor.isClosed)

  let element = ends.consumer.take()
  #expect(element == 7)
  let drained = ends.consumer.take()
  #expect(drained == nil)
  release(ends.consumer)
}

@Test func producerReleaseClosesProduction() {
  let ends = UnboundedQueue<Int>().split(.spsc)
  release(ends.producer)

  // last producer end gone: closed and empty, the take seals
  let drained = ends.consumer.take()
  #expect(drained == nil)
  #expect(ends.supervisor.isSealed)
  release(ends.consumer)
}

@Test func consumerReleaseFailsEnqueue() {
  let ends = UnboundedQueue<Int>().split(.spsc)
  release(ends.consumer)

  #expect(ends.supervisor.isSealed)
  let accepted = ends.producer.enqueue(1)
  #expect(accepted == false)
  release(ends.producer)
}

@Test func waitUntilSealedReturnsAfterConsumerDrains() async {
  let ends = UnboundedQueue<Int>().split(.spmc)
  ends.producer.enqueue(7)
  ends.supervisor.close()

  // waitUntilSealed blocks; run it off the cooperative pool.
  let supervisor = ends.supervisor
  let consumer = ends.consumer
  let sealed = DispatchSemaphore(value: 0)
  let waiter = Thread {
    supervisor.waitUntilSealed()
    sealed.signal()
  }
  waiter.start()

  let element = Box<Int?>(nil)
  let drained = Box<Bool?>(nil)
  let taken = DispatchSemaphore(value: 0)
  let taker = Thread {
    element.set(consumer.take())
    drained.set(consumer.take() == nil)
    taken.signal()
  }
  taker.start()
  await awaitSignal(taken)

  #expect(element.get() == 7)
  #expect(drained.get() == true)

  await awaitSignal(sealed)
  #expect(supervisor.isSealed)

  release(ends.producer)
}

@Test func concurrentSharedEndsDeliverExactlyOnce() async {
  let ends = UnboundedQueue<Int>().split(.mpmc)
  let producer = ends.producer
  let consumer = ends.consumer
  let supervisor = ends.supervisor

  let producerCount = 4
  let perProducer = 500
  let total = producerCount * perProducer
  let collector = Collector()

  // Blocking takers get dedicated threads; the cooperative pool stays
  // available for the producer tasks whose completion closes the queue.
  let takersDone = DispatchSemaphore(value: 0)
  for _ in 0..<2 {
    let taker = Thread {
      while let value = consumer.take() {
        collector.append(value)
      }
      takersDone.signal()
    }
    taker.start()
  }

  await withTaskGroup(of: Void.self) { producers in
    for p in 0..<producerCount {
      producers.addTask {
        for i in 0..<perProducer {
          producer.enqueue(p * perProducer + i)
        }
      }
    }
  }
  producer.close()

  // Both takers return nil once production is closed and the queue drained.
  await awaitSignal(takersDone)
  await awaitSignal(takersDone)
  #expect(supervisor.isSealed)

  let received = collector.snapshot()
  #expect(received.count == total)
  var counts: Set<Int> = []
  for value in received {
    counts.insert(value)
  }
  #expect(counts.count == total)
}

@Test func sharedConsumerReleaseSeals() {
  let ends = UnboundedQueue<Int>().split(.mpmc)
  let producer = ends.producer
  let supervisor = ends.supervisor
  #expect(supervisor.isSealed == false)

  // dropping the ends' shared-consumer reference disconnects the consumer
  // side: sealed
  release(ends)
  #expect(supervisor.isSealed)

  // sealed: the element is discarded and reported as failure
  let accepted = producer.enqueue(1)
  #expect(accepted == false)
  producer.close()
}

/// Elements need not be `Sendable`: ownership crosses domains through
/// the queue (created in the sender's domain, consumed in the taker's).
@Test func nonSendableElementTraversesQueue() async {
  let ends = UnboundedQueue<OpaquePayload>().split(.mpmc)
  let producer = ends.producer
  let consumer = ends.consumer

  await Task.detached {
    producer.enqueue(OpaquePayload(value: 42))
    producer.close()
  }.value

  let value = Box<Int?>(nil)
  let drained = Box<Bool?>(nil)
  let taken = DispatchSemaphore(value: 0)
  let taker = Thread {
    let payload = consumer.take()
    value.set(payload?.value)
    drained.set(consumer.take() == nil)
    taken.signal()
  }
  taker.start()
  await awaitSignal(taken)

  #expect(value.get() == 42)
  #expect(drained.get() == true)
}

/// The `~Copyable` axis of the element constraint: a move-only payload
/// traverses the queue as well.
@Test func nonCopyableElementTraversesQueue() {
  let ends = UnboundedQueue<SingleShot>().split(.spsc)
  let accepted = ends.producer.enqueue(SingleShot())
  #expect(accepted)
  ends.producer.finish()

  guard let token = ends.consumer.take() else {
    Issue.record("expected the enqueued element")
    return
  }
  release(token)
  release(ends.consumer)
}

/// The `sending` handoff must not produce false positives: reusing a
/// `Sendable` value after enqueue stays legal.
@Test func sendableElementRemainsUsableAfterEnqueue() {
  let ends = UnboundedQueue<Int>().split(.spsc)
  let value = 7
  let accepted = ends.producer.enqueue(value)
  #expect(accepted)
  #expect(value == 7)

  release(ends.producer)
  let taken = ends.consumer.take()
  #expect(taken == 7)
  release(ends.consumer)
}
