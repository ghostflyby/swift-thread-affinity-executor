//
//  DedicatedThreadExecutorTests.swift
//  ThreadAffinityExecutorTests
//

import Foundation
import Testing

@testable import ThreadAffinityExecutor

// MARK: - Startup / shutdown

@Test func initPublishesRunningExecutor() throws {
  let executor = try DedicatedThreadExecutor(name: "tae-basic")
  #expect(!executor.hasFinished)
  #expect(executor.threadID != ThreadID.current)
  executor.shutdown()
  #expect(executor.hasFinished)
}

@Test func shutdownIsIdempotent() throws {
  let executor = try DedicatedThreadExecutor(name: "tae-idempotent")
  executor.shutdown()
  executor.shutdown()
  #expect(executor.hasFinished)
}

@Test func concurrentInitsAreSafe() async throws {
  try await withThrowingTaskGroup(of: Void.self) { group in
    for index in 0..<8 {
      group.addTask {
        let executor = try DedicatedThreadExecutor(name: "tae-concurrent-\(index)")
        #expect(executor.threadID != ThreadID.current)
        await shutdown(executor)
      }
    }
    try await group.waitForAll()
  }
}

// MARK: - Initialization model

/// The lifecycle's `initialize` runs exactly once on the bound thread,
/// before any job.
@Test func initializeRunsOnceBeforeFirstJobOnBoundThread() async throws {
  let initializerThreads = TestBox<[ThreadID]>([])
  let executor = try DedicatedThreadExecutor(
    name: "tae-init",
    lifecycle: ClosureLifecycle {
      initializerThreads.with { $0.append(ThreadID.current) }
    }
  )
  #expect(initializerThreads.get().count == 1)

  let jobThread = await withTaskExecutorPreference(executor) {
    ThreadID.current
  }
  #expect(jobThread == initializerThreads.get().first)
  #expect(jobThread == executor.threadID)
  await shutdown(executor)
}

private struct InitializationFailed: Error {}

/// An `initialize` error fails construction: rethrown to the caller, no
/// executor published, `finalize` never runs.
@Test func initializationFailurePropagatesAndSkipsFinalize() throws {
  let finalized = TestBox(false)
  #expect(throws: InitializationFailed.self) {
    _ = try DedicatedThreadExecutor(
      name: "tae-init-fail",
      lifecycle: ClosureLifecycle(
        initialize: { throw InitializationFailed() },
        finalize: { finalized.set(true) }
      )
    )
  }
  #expect(finalized.get() == false)
}

/// The lifecycle's `finalize` runs on the bound thread after the queue
/// drained.
@Test func finalizeRunsAfterDrainOnBoundThread() async throws {
  let jobThread = TestBox<ThreadID?>(nil)
  let finalThread = TestBox<ThreadID?>(nil)
  let executor = try DedicatedThreadExecutor(
    name: "tae-finalize",
    lifecycle: ClosureLifecycle(
      finalize: { finalThread.set(ThreadID.current) }
    )
  )

  await withTaskExecutorPreference(executor) {
    jobThread.set(ThreadID.current)
  }

  // shutdown() joins the exit epilogue: finalize has completed here.
  await shutdown(executor)
  #expect(finalThread.get() == jobThread.get())
  #expect(finalThread.get() == executor.threadID)
}

// MARK: - Thread binding

/// Many consecutive jobs always read the same TID,
/// which is the executor's published one.
@Test func jobsRunOnBoundThread() async throws {
  let executor = try DedicatedThreadExecutor(name: "tae-binding")

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
  await shutdown(executor)
}

/// Concurrent submitters, exactly-once
/// execution, no data race in shared state, all on a single OS thread.
@Test func jobExecutionIsExactlyOnceUnderConcurrency() async throws {
  let executor = try DedicatedThreadExecutor(name: "tae-serial")
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
  await shutdown(executor)
}

// MARK: - Isolation checks

/// Off the bound thread the check is negative.
@Test func isolationCheckIsNegativeOffBoundThread() throws {
  let executor = try DedicatedThreadExecutor(name: "tae-negative")
  #expect(executor.isIsolatingCurrentContext() == false)
  executor.shutdown()
}

/// On the bound thread the check is
/// positive and thread-based (not executor-identity based).
@Test func isolationCheckIsPositiveOnBoundThread() async throws {
  let executor = try DedicatedThreadExecutor(name: "tae-positive")

  let positive = await withTaskExecutorPreference(executor) {
    executor.isIsolatingCurrentContext()
  }

  #expect(positive == true)
  await shutdown(executor)
}

/// Jobs accepted before shutdown all execute; the
/// thread finishes and `shutdown` returns.
@Test func shutdownDrainsAcceptedJobs() async throws {
  let executor = try DedicatedThreadExecutor(name: "tae-drain")
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

  await shutdown(executor)
  #expect(counter.value == 200)
  #expect(executor.hasFinished)
}
