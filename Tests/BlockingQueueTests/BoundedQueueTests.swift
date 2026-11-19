//
//  BoundedQueueTests.swift
//  BlockingQueueTests
//

import BlockingQueue
import DequeModule
import Foundation
import Testing

@Test func tryEnqueueFailsWhenFullAndRecoversAfterTake() {
  let ends = BoundedQueue<Int>(capacity: 2).split(.spsc)

  let first = ends.producer.enqueue(1)
  #expect(first)
  let second = ends.producer.enqueue(2)
  #expect(second)
  // full: try-enqueue discards and reports failure
  let rejected = ends.producer.enqueue(3)
  #expect(rejected == false)

  // taking frees a slot
  let taken = ends.consumer.take()
  #expect(taken == 1)
  let refilled = ends.producer.enqueue(3)
  #expect(refilled)

  release(ends.producer)
  let drained1 = ends.consumer.take()
  #expect(drained1 == 2)
  let drained2 = ends.consumer.take()
  #expect(drained2 == 3)
  let drained3 = ends.consumer.take()
  #expect(drained3 == nil)
  release(ends.consumer)
}

@Test func enqueueBlockingWaitsForSpace() {
  let ends = BoundedQueue<Int>(capacity: 1).split(.spsc)
  let producer = ends.producer
  let consumer = ends.consumer

  let initial = producer.enqueue(1)
  #expect(initial)

  let result = Box<Bool?>(nil)
  let done = DispatchSemaphore(value: 0)
  let thread = Thread {
    result.set(producer.enqueueBlocking(2))
    done.signal()
  }
  thread.start()
  waitForWaiters(ends.supervisor, producers: 1)

  // freeing the slot unblocks the waiting producer
  let taken = consumer.take()
  #expect(taken == 1)
  done.wait()
  #expect(result.get() == true)
  let delivered = consumer.take()
  #expect(delivered == 2)

  // the producer end lives as long as the thread object, so production is
  // not closed yet
}

@Test func enqueueBlockingFailsWithoutConsumers() {
  let ends = BoundedQueue<Int>(capacity: 1).split(.spsc)
  release(ends.consumer)

  let blocked = ends.producer.enqueueBlocking(1)
  #expect(blocked == false)
  let rejected = ends.producer.enqueue(1)
  #expect(rejected == false)
  release(ends.producer)
}

@Test func boundedFifoAcrossRounds() {
  let ends = BoundedQueue<Int>(capacity: 4).split(.spsc)

  for round in 0..<3 {
    let base = round * 4
    for i in 0..<4 {
      let accepted = ends.producer.enqueue(base + i)
      #expect(accepted)
    }
    for i in 0..<4 {
      let element = ends.consumer.take()
      #expect(element == base + i)
    }
  }

  release(ends.producer)
  let drained = ends.consumer.take()
  #expect(drained == nil)
  release(ends.consumer)
}

@Test func sharedBoundedProducerFillUpToCapacity() async {
  let ends = BoundedQueue<Int>(capacity: 3).split(.mpsc)
  let producer = ends.producer

  let accepted = await withTaskGroup(of: Int.self) { group in
    for _ in 0..<10 {
      group.addTask {
        producer.enqueue(1) ? 1 : 0
      }
    }
    var total = 0
    for await contribution in group {
      total += contribution
    }
    return total
  }

  // exactly the capacity fits; the rest are rejected
  #expect(accepted == 3)
  #expect(ends.supervisor.count == 3)

  producer.close()
  release(ends.consumer)
}

/// `.spmc` mints an exclusive producer end while the consumer side is a
/// cloneable class reference; competing takers each element exactly once.
@Test func boundedSharedConsumersCompeteForElements() async {
  let ends = BoundedQueue<Int>(capacity: 4).split(.spmc)
  let producer = ends.producer
  let collector = Collector()

  let takersDone = DispatchSemaphore(value: 0)
  for _ in 0..<2 {
    let consumer = ends.consumer
    let taker = Thread {
      while let value = consumer.take() {
        collector.append(value)
      }
      takersDone.signal()
    }
    taker.start()
  }
  waitForWaiters(ends.supervisor, consumers: 2)

  for value in 0..<4 {
    let accepted = producer.enqueue(value)
    #expect(accepted)
  }
  producer.finish()

  await awaitSignal(takersDone)
  await awaitSignal(takersDone)

  #expect(ends.supervisor.isSealed)
  let received = collector.snapshot()
  #expect(received.count == 4)
  #expect(Set(received) == Set(0..<4))
}

/// Releasing the last shared-producer reference closes production even
/// without an explicit `close()`.
@Test func boundedSharedProducerReleaseClosesProduction() {
  let ends = BoundedQueue<Int>(capacity: 1).split(.mpmc)
  let consumer = ends.consumer
  let supervisor = ends.supervisor
  #expect(supervisor.isClosed == false)

  // dropping the ends' shared-producer reference closes production
  release(ends)
  #expect(supervisor.isClosed)

  let drained = consumer.take()
  #expect(drained == nil)
  #expect(supervisor.isSealed)
  release(consumer)
}

/// `.spmc`'s exclusive producer wakes when a shared taker frees a bounded
/// slot; no other waiter class is present, so this exercises the single
/// signal path.
@Test func boundedSPMCTakeWakesBlockedProducer() {
  let ends = BoundedQueue<Int>(capacity: 1).split(.spmc)
  let producer = ends.producer
  let consumer = ends.consumer

  let initial = producer.enqueue(1)
  #expect(initial)

  let result = Box<Bool?>(nil)
  let done = DispatchSemaphore(value: 0)
  let thread = Thread {
    result.set(producer.enqueueBlocking(2))
    done.signal()
  }
  thread.start()
  waitForWaiters(ends.supervisor, producers: 1)

  let first = consumer.take()
  #expect(first == 1)
  done.wait()
  #expect(result.get() == true)
  let second = consumer.take()
  #expect(second == 2)

  ends.supervisor.close()
  let drained = consumer.take()
  #expect(drained == nil)
  #expect(ends.supervisor.isSealed)
}

@Test func blockedEnqueueWakesOnClose() {
  let ends = BoundedQueue<Int>(capacity: 1).split(.spsc)
  let producer = ends.producer
  let consumer = ends.consumer

  let initial = producer.enqueue(1)
  #expect(initial)

  let result = Box<Bool?>(nil)
  let done = DispatchSemaphore(value: 0)
  let thread = Thread {
    result.set(producer.enqueueBlocking(2))
    done.signal()
  }
  thread.start()
  waitForWaiters(ends.supervisor, producers: 1)

  // closing production wakes the blocked producer with a failure
  ends.supervisor.close()
  done.wait()
  #expect(result.get() == false)

  // the queued element still drains; the take observing closed-and-empty seals
  let taken = consumer.take()
  #expect(taken == 1)
  let drained = consumer.take()
  #expect(drained == nil)
  #expect(ends.supervisor.isSealed)
}

@Test func blockedEnqueueWakesOnConsumerRelease() {
  let ends = BoundedQueue<Int>(capacity: 1).split(.spsc)
  let producer = ends.producer
  let consumer = ends.consumer

  let initial = producer.enqueue(1)
  #expect(initial)

  let result = Box<Bool?>(nil)
  let done = DispatchSemaphore(value: 0)
  let thread = Thread {
    result.set(producer.enqueueBlocking(2))
    done.signal()
  }
  thread.start()
  waitForWaiters(ends.supervisor, producers: 1)

  // releasing the last consumer end wakes the blocked producer with failure
  release(consumer)
  done.wait()
  #expect(result.get() == false)
  #expect(ends.supervisor.isSealed)
}

@Test func boundedSupervisorCloseDrainsThenSeals() {
  let ends = BoundedQueue<Int>(capacity: 2).split(.spsc)
  let first = ends.producer.enqueue(1)
  #expect(first)
  let second = ends.producer.enqueue(2)
  #expect(second)
  // full before close: further try-enqueues are rejected
  let rejected = ends.producer.enqueue(3)
  #expect(rejected == false)

  ends.supervisor.close()
  #expect(ends.supervisor.isClosed)
  #expect(ends.supervisor.isSealed == false)

  // close refuses new production even while elements remain queued
  let late = ends.producer.enqueue(9)
  #expect(late == false)

  // elements enqueued before close stay available and drain in order
  let a = ends.consumer.take()
  #expect(a == 1)
  let b = ends.consumer.take()
  #expect(b == 2)

  let drained = ends.consumer.take()
  #expect(drained == nil)
  #expect(ends.supervisor.isSealed)

  release(ends.producer)
  release(ends.consumer)
}

@Test func drainAllUnblocksWaitingProducer() {
  let ends = BoundedQueue<Int>(capacity: 1).split(.spsc)
  let producer = ends.producer
  let consumer = ends.consumer
  let supervisor = ends.supervisor

  let initial = producer.enqueue(1)
  #expect(initial)

  let result = Box<Bool?>(nil)
  let done = DispatchSemaphore(value: 0)
  let thread = Thread {
    result.set(producer.enqueueBlocking(2))
    done.signal()
  }
  thread.start()
  waitForWaiters(supervisor, producers: 1)

  // draining frees the slot and wakes the blocked producer
  var drained = supervisor.drainAll()
  #expect(drained.popFirst() == 1)

  done.wait()
  #expect(result.get() == true)
  let delivered = consumer.take()
  #expect(delivered == 2)

  release(consumer)
}

/// Non-`Sendable` copyable elements traverse the bounded family too — both
/// the try and the blocking enqueue resolve to the `sending` handoff.
@Test func boundedNonSendableElementTraversesQueue() {
  let ends = BoundedQueue<OpaquePayload>(capacity: 1).split(.spsc)

  let first = ends.producer.enqueue(OpaquePayload(value: 5))
  #expect(first)
  let taken = ends.consumer.take()
  #expect(taken?.value == 5)

  let second = ends.producer.enqueueBlocking(OpaquePayload(value: 6))
  #expect(second)
  ends.producer.finish()

  let last = ends.consumer.take()
  #expect(last?.value == 6)
  let drained = ends.consumer.take()
  #expect(drained == nil)
  release(ends.consumer)
}

/// A sealed-waiter on the shared condition must not consume the wake-up
/// meant for a blocked consumer: the queue only uses a single signal when
/// the consumer class is the sole waiter class.
@Test func enqueueWakesConsumerDespiteSealedWaiter() {
  let ends = BoundedQueue<Int>(capacity: 2).split(.mpmc)
  let producer = ends.producer
  let consumer = ends.consumer
  let supervisor = ends.supervisor

  let took = DispatchSemaphore(value: 0)
  let taker = Thread {
    _ = consumer.take()
    took.signal()
  }
  let sealed = DispatchSemaphore(value: 0)
  let sealedWaiter = Thread {
    supervisor.waitUntilSealed()
    sealed.signal()
  }
  taker.start()
  sealedWaiter.start()
  waitForWaiters(supervisor, consumers: 1, sealed: 1)

  #expect(producer.enqueue(1))
  #expect(took.wait(timeout: .now() + 2) == .success)

  supervisor.close()
  _ = consumer.take()
  #expect(sealed.wait(timeout: .now() + 2) == .success)
  producer.close()
}

/// Symmetric case: a blocked producer must be woken even when a sealed
/// waiter shares the condition, so a freed bounded slot is not lost.
@Test func takeWakesProducerDespiteSealedWaiter() {
  let ends = BoundedQueue<Int>(capacity: 1).split(.mpmc)
  let producer = ends.producer
  let consumer = ends.consumer
  let supervisor = ends.supervisor

  #expect(producer.enqueue(1))

  let produced = DispatchSemaphore(value: 0)
  let producerThread = Thread {
    _ = producer.enqueueBlocking(2)
    produced.signal()
  }
  let sealed = DispatchSemaphore(value: 0)
  let sealedWaiter = Thread {
    supervisor.waitUntilSealed()
    sealed.signal()
  }
  producerThread.start()
  sealedWaiter.start()
  waitForWaiters(supervisor, producers: 1, sealed: 1)

  #expect(consumer.take() == 1)
  #expect(produced.wait(timeout: .now() + 2) == .success)

  supervisor.close()
  #expect(consumer.take() == 2)
  #expect(consumer.take() == nil)
  #expect(sealed.wait(timeout: .now() + 2) == .success)
  producer.close()
}
