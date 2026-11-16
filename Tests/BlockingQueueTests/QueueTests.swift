//
//  QueueTests.swift
//  BlockingQueueTests
//

import BlockingQueue
import DequeModule
import Foundation
import Testing

@Test func fifoOrderWithUniqueEnds() {
  let ends = Queue<Int>().split()

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

@Test func blockedTakeWakesOnEnqueue() async {
  let ends = Queue<String>().split()
  let consumer = ends.consumer.share()

  // The blocking take gets its own thread: a blocked cooperative thread can
  // starve the pool that this test needs to make progress.
  let result = Box<String?>(nil)
  let taken = DispatchSemaphore(value: 0)
  let taker = Thread {
    result.set(consumer.take())
    taken.signal()
  }
  taker.start()
  // the queue is empty and open, so the taker can only still be blocked
  try? await Task.sleep(for: .milliseconds(50))

  let accepted = ends.producer.enqueue("hello")
  #expect(accepted)
  await awaitSignal(taken)
  #expect(result.get() == "hello")

  release(ends.producer)
}

@Test func supervisorCloseDrainsThenSeals() {
  let ends = Queue<Int>().split()
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
  let ends = Queue<Int>().split()
  ends.supervisor.close()
  let drained = ends.consumer.take()
  #expect(drained == nil)
  #expect(ends.supervisor.isSealed)

  release(ends.producer)
  release(ends.consumer)
}

@Test func enqueueFailsAfterSeal() {
  let ends = Queue<Int>().split()
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
  let ends = Queue<Int>().split()
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
  let ends = Queue<Int>().split()
  release(ends.producer)

  // last producer end gone: closed and empty, the take seals
  let drained = ends.consumer.take()
  #expect(drained == nil)
  #expect(ends.supervisor.isSealed)
  release(ends.consumer)
}

@Test func consumerReleaseFailsEnqueue() {
  let ends = Queue<Int>().split()
  release(ends.consumer)

  #expect(ends.supervisor.isSealed)
  let accepted = ends.producer.enqueue(1)
  #expect(accepted == false)
  release(ends.producer)
}

@Test func supervisorDrainAllTakesEverythingInOrder() {
  let ends = Queue<Int>().split()
  ends.producer.enqueue(1)
  ends.producer.enqueue(2)
  ends.producer.enqueue(3)

  var drained = ends.supervisor.drainAll()
  #expect(drained.count == 3)
  #expect(ends.supervisor.count == 0)

  var received: [Int] = []
  while let element = drained.popFirst() {
    received.append(element)
  }
  #expect(received == [1, 2, 3])

  release(ends.producer)
  release(ends.consumer)
}

@Test func waitUntilSealedReturnsAfterConsumerDrains() async {
  let ends = Queue<Int>().split()
  ends.producer.enqueue(7)
  ends.supervisor.close()

  // waitUntilSealed blocks; run it off the cooperative pool.
  let supervisor = ends.supervisor
  let sealed = DispatchSemaphore(value: 0)
  let waiter = Thread {
    supervisor.waitUntilSealed()
    sealed.signal()
  }
  waiter.start()

  let element = ends.consumer.take()
  #expect(element == 7)
  let drained = ends.consumer.take()
  #expect(drained == nil)

  await awaitSignal(sealed)
  #expect(ends.supervisor.isSealed)

  release(ends.producer)
  release(ends.consumer)
}

@Test func concurrentSharedEndsDeliverExactlyOnce() async {
  let ends = Queue<Int>().split()
  let producer = ends.producer.share()
  let consumer = ends.consumer.share()
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
  supervisor.waitUntilSealed()

  let received = collector.snapshot()
  #expect(received.count == total)
  var counts: Set<Int> = []
  for value in received {
    counts.insert(value)
  }
  #expect(counts.count == total)
}

@Test func sharedConsumerReleaseSeals() {
  let ends = Queue<Int>().split()
  var consumer: SharedQueueConsumer<Int>? = ends.consumer.share()
  #expect(consumer != nil)
  #expect(ends.supervisor.isSealed == false)

  // dropping the last reference disconnects the shared end: sealed
  consumer = nil
  #expect(ends.supervisor.isSealed)

  let accepted = ends.producer.enqueue(1)
  #expect(accepted == false)
  release(ends.producer)
}

/// Elements need not be `Sendable`: ownership crosses domains through
/// the queue (created in the sender's domain, consumed in the taker's).
@Test func nonSendableElementTraversesQueue() async {
  let ends = Queue<OpaquePayload>().split()
  let producer = ends.producer.share()
  let consumer = ends.consumer.share()

  await Task.detached {
    producer.enqueue(OpaquePayload(value: 42))
    producer.close()
  }.value

  guard let payload = consumer.take() else {
    Issue.record("expected the enqueued element")
    return
  }
  #expect(payload.value == 42)
  #expect(consumer.take() == nil)
}

/// The `~Copyable` axis of the element constraint: a move-only payload
/// traverses the queue as well.
@Test func nonCopyableElementTraversesQueue() {
  let ends = Queue<SingleShot>().split()
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
  let ends = Queue<Int>().split()
  let value = 7
  let accepted = ends.producer.enqueue(value)
  #expect(accepted)
  #expect(value == 7)

  release(ends.producer)
  let taken = ends.consumer.take()
  #expect(taken == 7)
  release(ends.consumer)
}
