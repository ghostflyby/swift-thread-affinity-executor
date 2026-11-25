//
//  DedicatedThreadExecutorTests.swift
//  ThreadAffinityExecutorTests
//

import Foundation
import Testing

@testable import ThreadAffinityExecutor

// MARK: - Startup / shutdown

@Test func initPublishesRunningExecutor() {
  let executor = DedicatedThreadExecutor(name: "tae-basic")
  #expect(!executor.hasFinished)
  #expect(executor.threadID != ThreadID.current)
  executor.shutdown()
  #expect(executor.hasFinished)
}

@Test func shutdownIsIdempotent() {
  let executor = DedicatedThreadExecutor(name: "tae-idempotent")
  executor.shutdown()
  executor.shutdown()
  #expect(executor.hasFinished)
}

@Test func concurrentInitsAreSafe() async {
  await withTaskGroup(of: Void.self) { group in
    for index in 0..<8 {
      group.addTask {
        let executor = DedicatedThreadExecutor(name: "tae-concurrent-\(index)")
        #expect(executor.threadID != ThreadID.current)
        await executor.shutdown()
      }
    }
  }
}

// MARK: - Thread binding

/// Many consecutive jobs always read the same TID,
/// which is the executor's published one.
@Test func jobsRunOnBoundThread() async throws {
  let executor = DedicatedThreadExecutor(name: "tae-binding")

  let observed: [ThreadID] = try await withTaskExecutorPreference(executor) {
    try await withThrowingTaskGroup(of: ThreadID.self) { group in
      for _ in 0..<200 {
        group.addTask { ThreadID.current }
      }
      var collected: [ThreadID] = []
      for try await tid in group {
        collected.append(tid)
      }
      return collected
    }
  }

  #expect(observed.count == 200)
  #expect(observed.allSatisfy { $0 == executor.threadID })
  await executor.shutdown()
}

/// Concurrent submitters, exactly-once
/// execution, no data race in shared state, all on a single OS thread.
@Test func jobExecutionIsExactlyOnceUnderConcurrency() async {
  let executor = DedicatedThreadExecutor(name: "tae-serial")
  let counter = ThreadSafeCounter()
  let threadsSeen = TestBox<Set<ThreadID>>([])

  await withTaskExecutorPreference(executor) {
    await withTaskGroup(of: Void.self) { group in
      for _ in 0..<8 {
        group.addTask {
          for _ in 0..<250 {
            counter.increment(recording: threadsSeen)
            await Task.yield()
          }
        }
      }
    }
  }

  #expect(counter.value == 8 * 250)
  #expect(threadsSeen.get() == [executor.threadID])
  await executor.shutdown()
}

// MARK: - Isolation checks

/// Off the bound thread the check is negative.
@Test func isolationCheckIsNegativeOffBoundThread() {
  let executor = DedicatedThreadExecutor(name: "tae-negative")
  #expect(executor.isIsolatingCurrentContext() == false)
  executor.shutdown()
}

/// On the bound thread the check is
/// positive and thread-based (not executor-identity based).
@Test func isolationCheckIsPositiveOnBoundThread() async {
  let executor = DedicatedThreadExecutor(name: "tae-positive")

  let positive = await withTaskExecutorPreference(executor) {
    executor.isIsolatingCurrentContext()
  }

  #expect(positive == true)
  await executor.shutdown()
}

/// Jobs accepted before shutdown all execute; the
/// thread finishes and `shutdown` returns.
@Test func shutdownDrainsAcceptedJobs() async {
  let executor = DedicatedThreadExecutor(name: "tae-drain")
  let counter = ThreadSafeCounter()

  await withTaskExecutorPreference(executor) {
    await withTaskGroup(of: Void.self) { group in
      for _ in 0..<4 {
        group.addTask {
          for _ in 0..<50 {
            counter.increment(recording: TestBox<Set<ThreadID>>([]))
            await Task.yield()
          }
        }
      }
    }
  }

  await executor.shutdown()
  #expect(counter.value == 200)
  #expect(executor.hasFinished)
}

/// The async shutdown suspends until the worker drained: a bound job holds
/// the thread, and the shutdown task has not returned while it runs.
/// A second call after the exit resolves immediately.
@Test func asyncShutdownSuspendsUntilDrain() async {
  let executor = DedicatedThreadExecutor(name: "tae-async-shutdown")
  let jobStarted = DispatchSemaphore(value: 0)
  let jobRelease = DispatchSemaphore(value: 0)
  let job = Task {
    await withTaskExecutorPreference(executor) {
      jobStarted.signal()
      blockingWait(jobRelease)
    }
  }

  await waitForSignal(jobStarted)
  let returned = TestBox(false)
  let shutdown = Task {
    await executor.shutdown()
    returned.set(true)
  }
  // The job is running and held: the drain has not completed.
  #expect(returned.get() == false)

  jobRelease.signal()
  await job.value
  await shutdown.value
  #expect(returned.get())
  #expect(executor.hasFinished)

  // After the exit the wait resolves immediately.
  await executor.shutdown()
}

/// The async shutdown does not respond to cancellation: a cancelled caller
/// still resumes, normally, once the worker drained and exited.
@Test func asyncShutdownDoesNotRespondToCancellation() async {
  let executor = DedicatedThreadExecutor(name: "tae-async-cancel")
  let jobStarted = DispatchSemaphore(value: 0)
  let jobRelease = DispatchSemaphore(value: 0)
  let job = Task {
    await withTaskExecutorPreference(executor) {
      jobStarted.signal()
      blockingWait(jobRelease)
    }
  }

  await waitForSignal(jobStarted)
  let returned = TestBox(false)
  let shutdown = Task {
    await executor.shutdown()
    returned.set(true)
  }
  shutdown.cancel()
  #expect(shutdown.isCancelled)
  #expect(returned.get() == false)

  jobRelease.signal()
  await job.value
  await shutdown.value
  #expect(returned.get())
}

/// Blocking wait usable from a job body on a dedicated thread.
private func blockingWait(_ semaphore: DispatchSemaphore) {
  semaphore.wait()
}

#if canImport(Darwin)
  private final class AutoreleaseProbe: NSObject {
    private let onDeinit: @Sendable () -> Void

    init(onDeinit: @escaping @Sendable () -> Void) {
      self.onDeinit = onDeinit
    }

    deinit {
      onDeinit()
    }
  }

  /// Each job drains its own autoreleased objects before the next job
  /// starts.
  @Test func jobDrainsAutoreleasedObjectsPerWorkItem() async {
    let order = TestBox<[String]>([])
    let executor = DedicatedThreadExecutor(name: "tae-autorelease")

    await withTaskExecutorPreference(executor) {
      let probe = AutoreleaseProbe {
        order.with { $0.append("deinit") }
      }
      // `passRetained` + `autorelease` puts the probe in the thread's
      // current pool; the pool's drain owns the release that deinitializes
      // it after this job returns.
      _ = Unmanaged.passRetained(probe).autorelease()
      order.with { $0.append("first") }
    }
    await withTaskExecutorPreference(executor) {
      order.with { $0.append("second") }
    }
    await executor.shutdown()

    #expect(order.get() == ["first", "deinit", "second"])
  }
#endif
