//
//  AttachedExecutorTests.swift
//  ThreadAffinityExecutorTests
//
//  Attached executors. The main-queue paths are
//  environment-conditional: a CLI test process may not service the main
//  queue, so those tests probe first and vacuously pass when no main-queue
//  servicing exists. The generic attach path is tested deterministically
//  against a hand-rolled pump.
//

import Foundation
import Testing

@testable import ThreadAffinityExecutor

/// Probes whether this process services the main queue at all. Synchronous
/// helper because `DispatchSemaphore.wait` is unavailable from async
/// contexts; the behavior under test is environment-conditional.
private func probeMainQueuePumped(timeout: TimeInterval) -> Bool {
  let pumped = DispatchSemaphore(value: 0)
  DispatchQueue.main.async { pumped.signal() }
  return pumped.wait(timeout: .now() + timeout) == .success
}

/// Deterministic `FixedThreadDispatcher` for tests: one Foundation thread
/// running an NSCondition-based FIFO work queue. Queue state lives in its
/// own object so the thread closure can capture it without capturing the
/// not-yet-initialized dispatcher.
private final class TestPumpState: @unchecked Sendable {
  let condition = NSCondition()
  var queue: [@Sendable () -> Void] = []
  var stopRequested = false
  var finished = false
}

private final class TestPumpDispatcher: FixedThreadDispatcher, @unchecked Sendable {
  private let state = TestPumpState()
  private let thread: Thread

  init() {
    let state = self.state
    thread = Thread { [state] in
      state.condition.lock()
      while true {
        if let work = state.queue.first {
          state.queue.removeFirst()
          state.condition.unlock()
          work()
          state.condition.lock()
          continue
        }
        if state.stopRequested {
          state.finished = true
          state.condition.broadcast()
          break
        }
        state.condition.wait()
      }
      state.condition.unlock()
    }
    thread.start()
  }

  func post(_ work: @escaping @Sendable () -> Void) {
    state.condition.lock()
    state.queue.append(work)
    state.condition.unlock()
    state.condition.signal()
  }

  func stopAndJoin() {
    state.condition.lock()
    state.stopRequested = true
    state.condition.broadcast()
    while !state.finished {
      state.condition.wait()
    }
    state.condition.unlock()
  }
}

/// Actor fixture bound to an attached executor.
actor AttachedCounter {
  nonisolated let executor: AttachedThreadExecutor
  private var value = 0

  nonisolated var unownedExecutor: UnownedSerialExecutor {
    executor.asUnownedSerialExecutor()
  }

  init(executor: AttachedThreadExecutor) {
    self.executor = executor
  }

  func next() -> Int {
    value += 1
    return value
  }
}

// MARK: - Generic attach path (deterministic)

@Test func attachPublishesHandshakenExecutor() async throws {
  let dispatcher = TestPumpDispatcher()
  let executor = try await AttachedThreadExecutor.attach(to: dispatcher)

  // handshake completed: threadID valid and different from the caller's.
  #expect(executor.threadID != ThreadID.current)
  #expect(executor.isIsolatingCurrentContext() == false)

  dispatcher.stopAndJoin()
}

@Test func attachedJobsRunOnDispatcherThread() async throws {
  let dispatcher = TestPumpDispatcher()
  let executor = try await AttachedThreadExecutor.attach(to: dispatcher)

  let tids = await withTaskExecutorPreference(executor) {
    await withTaskGroup(of: ThreadID.self) { group in
      for _ in 0..<50 {
        group.addTask { ThreadID.current }
      }
      var collected: [ThreadID] = []
      for await tid in group {
        collected.append(tid)
      }
      return collected
    }
  }

  #expect(tids.count == 50)
  #expect(tids.allSatisfy { $0 == executor.threadID })

  dispatcher.stopAndJoin()
}

@Test func twoAttachmentsShareTheFixedThread() async throws {
  let dispatcher = TestPumpDispatcher()
  let first = try await AttachedThreadExecutor.attach(to: dispatcher)
  let second = try await AttachedThreadExecutor.attach(to: dispatcher)
  #expect(first.threadID == second.threadID)

  dispatcher.stopAndJoin()
}

@Test func actorBindsToAttachedExecutor() async throws {
  let dispatcher = TestPumpDispatcher()
  let executor = try await AttachedThreadExecutor.attach(to: dispatcher)
  let counter = AttachedCounter(executor: executor)

  #expect(await counter.next() == 1)
  #expect(await counter.next() == 2)

  dispatcher.stopAndJoin()
}

// MARK: - Main queue path (environment-conditional)

@Test func mainQueueDispatcherIsAValidFixedThreadDispatcher() async throws {
  guard probeMainQueuePumped(timeout: 1.0) else {
    return
  }

  let executor = try await AttachedThreadExecutor.attach(to: MainQueueDispatcher())
  let positive = await withTaskExecutorPreference(executor) {
    executor.isIsolatingCurrentContext()
  }
  #expect(positive == true)
}

private struct AttachInitializationFailed: Error {}

/// Posted `initialize` runs exactly once on the target thread, before any
/// job submitted to the attached executor.
@Test func attachInitializeRunsOnTargetThreadBeforeJobs() async throws {
  let dispatcher = TestPumpDispatcher()
  let initializerThreads = TestBox<[ThreadID]>([])
  let executor = try await AttachedThreadExecutor.attach(
    to: dispatcher,
    initialize: { initializerThreads.with { $0.append(ThreadID.current) } }
  )
  #expect(initializerThreads.get().count == 1)

  let jobThread = await withTaskExecutorPreference(executor) { ThreadID.current }
  #expect(jobThread == initializerThreads.get().first)
  #expect(jobThread == executor.threadID)

  dispatcher.stopAndJoin()
}

/// A posted `initialize` error fails the attach; no executor is published.
@Test func attachInitializeFailureFailsAttach() async throws {
  let dispatcher = TestPumpDispatcher()
  await #expect(throws: AttachInitializationFailed.self) {
    _ = try await AttachedThreadExecutor.attach(
      to: dispatcher,
      initialize: { throw AttachInitializationFailed() }
    )
  }
  dispatcher.stopAndJoin()
}

/// A failed attach leaves the dispatcher reusable: a retry on the same
/// dispatcher succeeds with a fresh handshake state.
@Test func attachRetryAfterInitializeFailureSucceedsOnSameDispatcher() async throws {
  let dispatcher = TestPumpDispatcher()
  await #expect(throws: AttachInitializationFailed.self) {
    _ = try await AttachedThreadExecutor.attach(
      to: dispatcher,
      initialize: { throw AttachInitializationFailed() }
    )
  }

  let executor = try await AttachedThreadExecutor.attach(to: dispatcher)
  #expect(executor.threadID != ThreadID.current)
  dispatcher.stopAndJoin()
}
