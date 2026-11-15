//
//  ActorIsolationTests.swift
//  ThreadAffinityExecutorTests
//
//  Actor binding to thread-bound executors and dynamic isolation access.
//

import Foundation
import Testing
@testable import ThreadAffinityExecutor

/// Fixture: an actor whose `unownedExecutor` is a dedicated
/// thread-bound executor.
actor Counter {
    nonisolated let executor: DedicatedThreadExecutor
    private var value = 0

    nonisolated var unownedExecutor: UnownedSerialExecutor {
        executor.asUnownedSerialExecutor()
    }

    init(executor: DedicatedThreadExecutor) {
        self.executor = executor
    }

    func next() -> Int {
        value += 1
        return value
    }

    func boundThreadHolds() -> Bool {
        ThreadID.current == executor.threadID
    }

    /// Suspends (resume happens on the global pool) and verifies that the
    /// actor isolation returns to the bound thread afterwards.
    func hopAwayAndComeBack() async -> Bool {
        let before = ThreadID.current == executor.threadID
        await withCheckedContinuation { continuation in
            Task.detached {
                continuation.resume()
            }
        }
        let after = ThreadID.current == executor.threadID
        return before && after
    }
}

/// A custom `unownedExecutor` drives actor methods
/// on the dedicated thread.
@Test func actorRunsOnBoundThread() async throws {
    let executor = DedicatedThreadExecutor(name: "tae-actor")
    let counter = Counter(executor: executor)

    #expect(await counter.boundThreadHolds())
    #expect(await counter.next() == 1)
    #expect(await counter.next() == 2)

    executor.shutdown()
}

/// After suspension and resumption the actor
/// isolation still lands on the bound thread.
@Test func actorIsolationSurvivesSuspension() async throws {
    let executor = DedicatedThreadExecutor(name: "tae-actor-hop")
    let counter = Counter(executor: executor)

    #expect(await counter.hopAwayAndComeBack())

    executor.shutdown()
}

/// Second actor sharing the same dedicated executor (one executor may
/// serve multiple actors). Its *synchronous* method provides the
/// external-callback scenario: sync code already running on
/// the bound thread, entering `Counter` via `assumeIsolated`.
actor Sibling {
    nonisolated let executor: DedicatedThreadExecutor
    private let counter: Counter

    nonisolated var unownedExecutor: UnownedSerialExecutor {
        executor.asUnownedSerialExecutor()
    }

    init(executor: DedicatedThreadExecutor, counter: Counter) {
        self.executor = executor
        self.counter = counter
    }

    func syncAccessCounter() -> Int {
        counter.assumeIsolated { site in
            site.next()
        }
    }
}

/// Synchronous `assumeIsolated` access
/// from the bound thread is accepted via dynamic isolation checking — even
/// across different actor objects sharing the thread.
@Test func assumeIsolatedSucceedsOnBoundThread() async throws {
    let executor = DedicatedThreadExecutor(name: "tae-assume")
    let counter = Counter(executor: executor)
    let sibling = Sibling(executor: executor, counter: counter)

    let result = await sibling.syncAccessCounter()

    #expect(result == 1)
    executor.shutdown()
}

/// Old-runtime fallback path: `checkIsolated()` — the
/// `isIsolatingCurrentContext` predecessor — passes on the bound thread.
/// (Its off-thread failure mode is a `precondition` trap, which cannot be
/// asserted in-process.)
@Test func checkIsolatedFallbackPassesOnBoundThread() async throws {
    let executor = DedicatedThreadExecutor(name: "tae-checkfallback")

    let passed = await withTaskExecutorPreference(executor) {
        executor.checkIsolated()
        return true
    }

    #expect(passed)
    executor.shutdown()
}
