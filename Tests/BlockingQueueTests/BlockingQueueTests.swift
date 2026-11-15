//
//  BlockingQueueTests.swift
//  BlockingQueueTests
//

import BlockingQueue
import Foundation
import Synchronization
import Testing

/// Mutex-protected sink for concurrently consumed elements.
private final class Collector: Sendable {
  private let values = Mutex<[Int]>([])

  func append(_ value: Int) {
    values.withLock { $0.append(value) }
  }

  func snapshot() -> [Int] {
    values.withLock { $0 }
  }
}

@Test func fifoOrderWithinSingleProducer() {
  let queue = BlockingQueue<Int>()
  for i in 0..<100 {
    queue.push(i)
  }
  for i in 0..<100 {
    #expect(queue.waitAndTake() == i)
  }
  #expect(queue.isClosed == false)
}

@Test func blockedTakeWakesOnPush() async {
  let queue = BlockingQueue<String>()
  let taker = Task.detached {
    queue.waitAndTake()
  }
  // the queue is empty and open, so the taker can only still be blocked
  try? await Task.sleep(for: .milliseconds(50))

  queue.push("hello")
  let value = await taker.value
  #expect(value == "hello")
}

@Test func closeDrainsRemainingElementsThenSeals() {
  let queue = BlockingQueue<Int>()
  queue.push(1)
  queue.push(2)

  queue.close()
  #expect(queue.isClosed)
  #expect(queue.isSealed == false)

  // elements pushed before close stay available
  #expect(queue.waitAndTake() == 1)
  #expect(queue.waitAndTake() == 2)
  #expect(queue.isSealed == false)

  // the take that observes closed-and-empty returns nil and seals
  #expect(queue.waitAndTake() == nil)
  #expect(queue.isSealed)
  #expect(queue.waitAndTake() == nil)
}

@Test func closeOnEmptyQueueSealsOnFirstTake() {
  let queue = BlockingQueue<Int>()
  queue.close()
  #expect(queue.waitAndTake() == nil)
  #expect(queue.isSealed)
}

@Test func waitUntilSealedReturnsAfterConsumerDrains() async {
  let queue = BlockingQueue<Int>()
  queue.push(7)
  queue.close()

  let waiter = Task.detached {
    queue.waitUntilSealed()
  }
  #expect(queue.waitAndTake() == 7)
  #expect(queue.waitAndTake() == nil)

  await waiter.value
  #expect(queue.isSealed)
}

@Test func concurrentUseDeliversExactlyOnce() async {
  let queue = BlockingQueue<Int>()
  let collector = Collector()
  let producerCount = 4
  let perProducer = 500
  let total = producerCount * perProducer

  await withTaskGroup(of: Void.self) { group in
    // consumers: block on take until the queue seals
    for _ in 0..<2 {
      group.addTask {
        while let value = queue.waitAndTake() {
          collector.append(value)
        }
      }
    }
    // producers run to completion, then the queue closes
    group.addTask {
      await withTaskGroup(of: Void.self) { producers in
        for p in 0..<producerCount {
          producers.addTask {
            for i in 0..<perProducer {
              queue.push(p * perProducer + i)
            }
          }
        }
      }
      queue.close()
    }
  }

  let received = collector.snapshot()
  #expect(received.count == total)

  var counts: [Int: Int] = [:]
  for value in received {
    counts[value, default: 0] += 1
  }
  // every produced element delivered exactly once
  #expect(counts.count == total)
}
