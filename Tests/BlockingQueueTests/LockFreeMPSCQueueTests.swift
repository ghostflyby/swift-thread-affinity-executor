//
//  LockFreeMPSCQueueTests.swift
//  BlockingQueueTests
//

import Dispatch
import Foundation
import Synchronization
import Testing

@testable import BlockingQueue

/// Blocks until the consumer has parked on the element condition, so wake
/// tests do not depend on a fixed sleep before the producer acts.
private func waitForParkedConsumer<Element: ~Copyable>(
  _ supervisor: LockFreeMPSCQueueSupervisor<Element>,
  timeout: TimeInterval = 2
) {
  let deadline = Date().addingTimeInterval(timeout)
  while Date() < deadline {
    if supervisor.cell.waitingConsumerCount > 0 {
      return
    }
    Thread.sleep(forTimeInterval: 0.001)
  }
  Issue.record("timed out waiting for a parked lock-free MPSC consumer")
}

@Test func lockFreeFifoWithExclusiveConsumer() {
  let ends = LockFreeMPSCQueue<Int>().split(.mpsc)
  let producer = ends.producer
  let consumer = ends.consumer
  let supervisor = ends.supervisor

  for i in 0..<100 {
    #expect(producer.enqueue(i))
  }
  #expect(supervisor.count == 100)

  for i in 0..<100 {
    #expect(consumer.tryTake() == i)
  }
  #expect(supervisor.count == 0)
  #expect(consumer.tryTake() == nil)

  producer.close()
  #expect(supervisor.isClosed)
  #expect(supervisor.isSealed == false)
  #expect(consumer.take() == nil)
  #expect(supervisor.isSealed)
  release(consumer)
}

@Test func lockFreeProducerReleaseClosesProduction() {
  // White-box: the ends bundle owns a producer reference too, so the public
  // shape cannot drop only the producer while keeping the other ends alive.
  let cell = LockFreeMPSCQueueCell<Int>()
  cell.registerEnds()
  let consumer = LockFreeMPSCQueueConsumer(cell: cell)
  let supervisor = LockFreeMPSCQueueSupervisor(cell: cell)

  do {
    let producer = LockFreeMPSCQueueProducer(cell: cell)
    #expect(producer.enqueue(1))
  }

  // dropping the last producer reference closes production
  #expect(supervisor.isClosed)

  #expect(consumer.take() == 1)
  #expect(consumer.take() == nil)
  #expect(supervisor.isSealed)
  release(consumer)
}

@Test func lockFreeConsumerReleaseFailsEnqueueAndSeals() {
  let ends = LockFreeMPSCQueue<Int>().split(.mpsc)
  let producer = ends.producer
  let supervisor = ends.supervisor
  release(ends.consumer)

  #expect(supervisor.isSealed)
  #expect(producer.enqueue(1) == false)
  producer.close()
}

@Test func lockFreeSupervisorCloseDrainsThenSeals() {
  let ends = LockFreeMPSCQueue<Int>().split(.mpsc)
  let producer = ends.producer
  let consumer = ends.consumer
  let supervisor = ends.supervisor

  #expect(producer.enqueue(1))
  #expect(producer.enqueue(2))
  supervisor.close()
  #expect(supervisor.isClosed)
  #expect(supervisor.isSealed == false)

  // elements enqueued before close stay available
  #expect(consumer.take() == 1)
  #expect(consumer.take() == 2)
  #expect(supervisor.isSealed == false)

  // the take that observes closed-and-empty returns nil and seals
  #expect(consumer.take() == nil)
  #expect(supervisor.isSealed)
  release(consumer)
}

/// Draining a closed queue with the non-blocking dequeue must reach the same
/// terminal state as a blocking take, or `waitUntilSealed()` would hang.
@Test func lockFreeTryTakeSealsClosedDrainedQueue() {
  let ends = LockFreeMPSCQueue<Int>().split(.mpsc)
  let producer = ends.producer
  let consumer = ends.consumer
  let supervisor = ends.supervisor

  #expect(producer.enqueue(1))
  producer.close()
  #expect(consumer.tryTake() == 1)
  #expect(supervisor.isSealed == false)

  #expect(consumer.tryTake() == nil)
  #expect(supervisor.isSealed)

  let sealed = DispatchSemaphore(value: 0)
  let waiter = Thread {
    supervisor.waitUntilSealed()
    sealed.signal()
  }
  waiter.start()
  #expect(sealed.wait(timeout: .now() + 2) == .success)
  release(consumer)
}

/// The count increment must happen-before the link that makes an element
/// visible; otherwise a concurrent take can decrement first and expose a
/// negative element count.
@Test func lockFreeCountNeverGoesNegativeUnderConcurrency() {
  let ends = LockFreeMPSCQueue<Int>().split(.mpsc)
  let producer = ends.producer
  let consumer = ends.consumer
  let supervisor = ends.supervisor

  let producerCount = 4
  let perProducer = 5_000
  let total = producerCount * perProducer
  let done = Atomic<Bool>(false)
  let group = DispatchGroup()
  for p in 0..<producerCount {
    DispatchQueue.global().async(group: group) {
      for i in 0..<perProducer {
        #expect(producer.enqueue(p * perProducer + i))
      }
    }
  }
  group.notify(queue: .global()) {
    producer.close()
    done.store(true, ordering: .releasing)
  }

  var received = 0
  var sawNegative = false
  while true {
    if consumer.tryTake() != nil {
      received += 1
      if supervisor.count < 0 {
        sawNegative = true
      }
      continue
    }
    if done.load(ordering: .acquiring) {
      while consumer.tryTake() != nil {
        received += 1
        if supervisor.count < 0 {
          sawNegative = true
        }
      }
      break
    }
    Thread.sleep(forTimeInterval: 0.0001)
  }

  #expect(received == total)
  #expect(sawNegative == false)
  #expect(supervisor.isSealed)
  release(consumer)
}

@Test func lockFreeBlockedTakeWakesOnEnqueue() {
  let ends = LockFreeMPSCQueue<String>().split(.mpsc)
  let producer = ends.producer
  let consumer = ends.consumer
  let supervisor = ends.supervisor

  let accepted = Box<Bool?>(nil)
  let done = DispatchSemaphore(value: 0)
  let feeder = Thread {
    waitForParkedConsumer(supervisor)
    accepted.set(producer.enqueue("hello"))
    producer.close()
    done.signal()
  }
  feeder.start()

  #expect(consumer.take() == "hello")
  #expect(consumer.take() == nil)
  #expect(supervisor.isSealed)
  done.wait()
  #expect(accepted.get() == true)
  release(consumer)
}

@Test func lockFreeCloseWakesBlockedTake() {
  let ends = LockFreeMPSCQueue<Int>().split(.mpsc)
  let consumer = ends.consumer
  let supervisor = ends.supervisor

  let done = DispatchSemaphore(value: 0)
  let closer = Thread {
    waitForParkedConsumer(supervisor)
    supervisor.close()
    done.signal()
  }
  closer.start()

  #expect(consumer.take() == nil)
  #expect(supervisor.isSealed)
  done.wait()
  release(consumer)
}

@Test func lockFreeConcurrentProducersDeliverExactlyOnce() {
  let ends = LockFreeMPSCQueue<Int>().split(.mpsc)
  let producer = ends.producer
  let consumer = ends.consumer
  let supervisor = ends.supervisor

  let producerCount = 4
  let perProducer = 1_000
  let group = DispatchGroup()
  for p in 0..<producerCount {
    DispatchQueue.global().async(group: group) {
      for i in 0..<perProducer {
        #expect(producer.enqueue(p * perProducer + i))
      }
    }
  }
  group.wait()
  producer.close()

  var last = Array(repeating: -1, count: producerCount)
  var received = 0
  while let value = consumer.take() {
    let p = value / perProducer
    let i = value % perProducer
    #expect(p >= 0 && p < producerCount)
    #expect(i == last[p] + 1)
    last[p] = i
    received += 1
  }
  #expect(received == producerCount * perProducer)
  #expect(supervisor.isSealed)
  release(consumer)
}

/// An `enqueue` that returned `true` must be delivered even when `close()`
/// races with the producers: the queue may not seal around an accepted
/// element.
@Test func lockFreeConcurrentCloseDoesNotLoseAcceptedElements() {
  let ends = LockFreeMPSCQueue<Int>().split(.mpsc)
  let producer = ends.producer
  let consumer = ends.consumer
  let supervisor = ends.supervisor
  let accepted = Atomic<Int>(0)

  let producerCount = 4
  let perProducer = 2_000
  let group = DispatchGroup()
  for p in 0..<producerCount {
    DispatchQueue.global().async(group: group) {
      for i in 0..<perProducer {
        if producer.enqueue(i) {
          accepted.wrappingAdd(1, ordering: .relaxed)
        }
        if p == 0 && i == perProducer / 2 {
          producer.close()
        }
      }
    }
  }
  group.wait()
  producer.close()

  var received = 0
  while consumer.take() != nil {
    received += 1
  }
  #expect(received == accepted.load(ordering: .relaxed))
  #expect(supervisor.isSealed)
  release(consumer)
}

@Test func lockFreeWaitUntilSealedReturnsAfterConsumerDrains() {
  let ends = LockFreeMPSCQueue<Int>().split(.mpsc)
  let producer = ends.producer
  let consumer = ends.consumer
  let supervisor = ends.supervisor

  #expect(producer.enqueue(7))
  producer.close()

  let sealed = DispatchSemaphore(value: 0)
  let waiter = Thread {
    supervisor.waitUntilSealed()
    sealed.signal()
  }
  waiter.start()

  #expect(consumer.take() == 7)
  #expect(consumer.take() == nil)
  sealed.wait()
  #expect(supervisor.isSealed)
  release(consumer)
}

/// Elements need not be `Sendable`: ownership crosses domains through the
/// queue (created in the producer's domain, consumed in the taker's).
@Test func lockFreeNonSendableElementTraversesQueue() async {
  let ends = LockFreeMPSCQueue<OpaquePayload>().split(.mpsc)
  let producer = ends.producer
  let consumer = ends.consumer

  await Task.detached {
    let accepted = producer.enqueue(OpaquePayload(value: 42))
    #expect(accepted)
    producer.close()
  }.value

  let payload = consumer.tryTake()
  #expect(payload?.value == 42)
  release(consumer)
}

/// The `~Copyable` axis of the element constraint: a move-only payload
/// traverses the queue as well.
@Test func lockFreeNonCopyableElementTraversesQueue() {
  let ends = LockFreeMPSCQueue<SingleShot>().split(.mpsc)
  let producer = ends.producer
  let consumer = ends.consumer

  let accepted = producer.enqueue(SingleShot())
  #expect(accepted)
  producer.close()
  guard let token = consumer.tryTake() else {
    Issue.record("expected the enqueued element")
    return
  }
  release(token)
  release(consumer)
}

private final class DeinitProbe: Sendable {
  let count = Atomic<Int>(0)
}

private struct TrackedValue: ~Copyable {
  let probe: DeinitProbe

  deinit {
    probe.count.wrappingAdd(1, ordering: .relaxed)
  }
}

/// Releasing a queue without draining must destroy the elements the nodes
/// still own.
@Test func lockFreeReleasingUndrainedQueueDestroysElements() {
  let probe = DeinitProbe()
  let ends = LockFreeMPSCQueue<TrackedValue>().split(.mpsc)
  let first = ends.producer.enqueue(TrackedValue(probe: probe))
  #expect(first)
  let second = ends.producer.enqueue(TrackedValue(probe: probe))
  #expect(second)

  release(ends)
  #expect(probe.count.load(ordering: .relaxed) == 2)
}
