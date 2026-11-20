//
//  NonSendableConfinementTests.swift
//  BlockingQueueTests
//
//  Construction: many tasks can drive one non-Sendable consumer, but only by
//  sending the calls into the actor that owns it. The consumer never crosses
//  the isolation domain and the actor serializes the calls.
//

import Testing

@testable import BlockingQueue

private actor NonSendableConsumerRunner {
  private let consumer: LockFreeMPSCQueueConsumer<Int>

  init(_ consumer: consuming sending LockFreeMPSCQueueConsumer<Int>) {
    self.consumer = consumer
  }

  func tryTake() -> Int? {
    consumer.tryTake()
  }
}

@Test func nonSendableConsumerServesMultipleTasksThroughActor() async {
  let ends = LockFreeMPSCQueue<Int>().split(.mpsc)
  let producer = ends.producer
  let consumer = ends.consumer
  let supervisor = ends.supervisor

  for value in 0..<100 {
    #expect(producer.enqueue(value))
  }
  producer.close()

  let runner = NonSendableConsumerRunner(consumer)

  let total = await withTaskGroup(of: Int.self) { group in
    for _ in 0..<4 {
      group.addTask {
        var count = 0
        while await runner.tryTake() != nil {
          count += 1
        }
        return count
      }
    }
    var sum = 0
    for await count in group {
      sum += count
    }
    return sum
  }

  #expect(total == 100)
  #expect(supervisor.isSealed)
}
