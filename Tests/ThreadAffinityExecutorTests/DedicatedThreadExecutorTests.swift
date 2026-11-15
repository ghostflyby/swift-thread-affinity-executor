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
                executor.shutdown()
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
    executor.shutdown()
}

/// Concurrent submitters, exactly-once
/// execution, no data race in shared state, all on a single OS thread.
@Test func jobExecutionIsExactlyOnceUnderConcurrency() async throws {
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
    executor.shutdown()
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
@Test func isolationCheckIsPositiveOnBoundThread() async throws {
    let executor = DedicatedThreadExecutor(name: "tae-positive")

    let positive = await withTaskExecutorPreference(executor) {
        executor.isIsolatingCurrentContext()
    }

    #expect(positive == true)
    executor.shutdown()
}

/// Jobs accepted before shutdown all execute; the
/// thread finishes and `shutdown` returns.
@Test func shutdownDrainsAcceptedJobs() async throws {
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

    executor.shutdown()
    #expect(counter.value == 200)
    #expect(executor.hasFinished)
}
