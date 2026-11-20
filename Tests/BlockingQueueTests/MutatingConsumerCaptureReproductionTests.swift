//
//  MutatingConsumerCaptureReproductionTests.swift
//  BlockingQueueTests
//
//  Minimal reproduction of the concurrent-capture check. The check lives in
//  the SIL sendability/region-isolation pass, so `-typecheck` accepts this
//  file while a full compile rejects it. Default builds skip the sample;
//  reproduce the rejection with:
//
//    swift build --build-tests -Xswiftc -DMUTATING_CAPTURE_REPRODUCTION
//
//  This is a compile-time reproduction, not a passing test: the functions
//  must not be executed, and a full compile rejects them.
//

#if MUTATING_CAPTURE_REPRODUCTION

  private struct Consumer: ~Copyable, Sendable {
    var value = 0

    mutating func take() {
      value += 1
    }
  }

  private func spawnThenUse() {
    var consumer = Consumer()
    Task.detached { consumer.take() }
    consumer.take()
  }

  private func twoTasks() async {
    var consumer = Consumer()
    await withTaskGroup(of: Void.self) { group in
      group.addTask { consumer.take() }
      group.addTask { consumer.take() }
    }
  }

#endif
