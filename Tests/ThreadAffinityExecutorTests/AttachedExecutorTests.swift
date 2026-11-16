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

/// Probes whether this process services the main queue on the main
/// thread. The probed block records `isMainThread`: a runner parked in
/// `dispatchMain()` diverts task-context main-queue posts to the
/// cooperative pool (the signal still fires — from a pool thread), which
/// `MainQueueDispatcher`'s handshake guard would trap on; treat that as
/// the vacuous branch. Synchronous helper because `DispatchSemaphore.wait`
/// is unavailable from async contexts.
private func probeMainQueuePumped(timeout: TimeInterval) -> Bool {
  let pumped = DispatchSemaphore(value: 0)
  let onMainThread = TestBox<Bool?>(nil)
  DispatchQueue.main.async {
    onMainThread.set(Thread.isMainThread)
    pumped.signal()
  }
  guard pumped.wait(timeout: .now() + timeout) == .success else {
    return false
  }
  return onMainThread.get() == true
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

private final class TestPumpDispatcher: ThreadPumpDelegate, @unchecked Sendable {
  private let state = TestPumpState()
  private let thread: Thread
  private let lifecycle: any ThreadLifecycleDelegate

  init(lifecycle: any ThreadLifecycleDelegate = ClosureLifecycle()) {
    self.lifecycle = lifecycle
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

  func initialize() throws {
    try lifecycle.initialize()
  }

  func finalize() {
    lifecycle.finalize()
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

  // MainQueueDispatcher.initialize preconditions the main thread — attach
  // returning proves the handshake landed there (the test process's main
  // thread services the main queue through a run loop).
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
  let initializerThreads = TestBox<[ThreadID]>([])
  let dispatcher = TestPumpDispatcher(
    lifecycle: ClosureLifecycle {
      initializerThreads.with { $0.append(ThreadID.current) }
    }
  )
  let executor = try await AttachedThreadExecutor.attach(to: dispatcher)
  #expect(initializerThreads.get().count == 1)

  let jobThread = await withTaskExecutorPreference(executor) { ThreadID.current }
  #expect(jobThread == initializerThreads.get().first)
  #expect(jobThread == executor.threadID)

  dispatcher.stopAndJoin()
}

/// A posted `initialize` error fails the attach; no executor is published.
@Test func attachInitializeFailureFailsAttach() async throws {
  let dispatcher = TestPumpDispatcher(
    lifecycle: ClosureLifecycle(initialize: { throw AttachInitializationFailed() })
  )
  await #expect(throws: AttachInitializationFailed.self) {
    _ = try await AttachedThreadExecutor.attach(to: dispatcher)
  }
  dispatcher.stopAndJoin()
}

/// A failed attach leaves the dispatcher reusable: a retry on the same
/// dispatcher succeeds with a fresh handshake state.
@Test func attachRetryAfterInitializeFailureSucceedsOnSameDispatcher() async throws {
  let failingOnce = TestBox(true)
  let dispatcher = TestPumpDispatcher(
    lifecycle: ClosureLifecycle(
      initialize: {
        if failingOnce.get() {
          failingOnce.set(false)
          throw AttachInitializationFailed()
        }
      }
    )
  )
  await #expect(throws: AttachInitializationFailed.self) {
    _ = try await AttachedThreadExecutor.attach(to: dispatcher)
  }

  let executor = try await AttachedThreadExecutor.attach(to: dispatcher)
  #expect(executor.threadID != ThreadID.current)
  dispatcher.stopAndJoin()
}

/// Synchronous helper — `DispatchSemaphore.wait` is unavailable from async
/// contexts; the block is bounded (the pump services the signal).
private func waitForSignal(_ semaphore: DispatchSemaphore) {
  semaphore.wait()
}

/// `detach()` posts `finalize` after all accepted work, on the bound
/// thread, and joins: it returns only after finalize completed. The job is
/// mid-flight when detach posts — FIFO must still run it first. Idempotent.
@Test func detachRunsFinalizeAfterAllWorkAndJoins() async throws {
  let order = TestBox<[String]>([])
  let finalThreads = TestBox<[ThreadID]>([])
  let dispatcher = TestPumpDispatcher(
    lifecycle: ClosureLifecycle(
      finalize: {
        order.with { $0.append("finalize") }
        finalThreads.with { $0.append(ThreadID.current) }
      }
    )
  )
  let executor = try await AttachedThreadExecutor.attach(to: dispatcher)

  let delivered = DispatchSemaphore(value: 0)
  let job = Task {
    await withTaskExecutorPreference(executor) {
      order.with { $0.append("job") }
      delivered.signal()
    }
  }
  waitForSignal(delivered)  // the job is executing: its post already precedes the finalize post

  executor.detach()
  executor.detach()  // idempotent: finalize ran once, both calls returned

  await job.value

  #expect(order.get() == ["job", "finalize"])
  #expect(finalThreads.get().count == 1)
  #expect(finalThreads.get().first == executor.threadID)

  dispatcher.stopAndJoin()
}

/// Jobs submitted after `detach()` fall into the drop window.
@Test func jobsEnqueuedAfterDetachAreDropped() async throws {
  let dispatcher = TestPumpDispatcher()
  let ran = TestBox(0)
  let executor = try await AttachedThreadExecutor.attach(to: dispatcher)

  executor.detach()
  Task.detached {
    await withTaskExecutorPreference(executor) {
      ran.with { $0 += 1 }
    }
  }
  try await Task.sleep(for: .milliseconds(100))

  #expect(ran.get() == 0)
  dispatcher.stopAndJoin()
}

/// Abandoning an executor without detaching posts finalize best-effort:
/// the binding dies at scope exit while a job is still executing — the
/// in-flight task's captures keep the executor alive until the job
/// completes, so deinit follows the backlog and FIFO still orders
/// finalize after all work.
@Test func abandonedExecutorPostsFinalizeOnDeinit() async throws {
  let order = TestBox<[String]>([])
  let finalThreads = TestBox<[ThreadID]>([])
  let dispatcher = TestPumpDispatcher(
    lifecycle: ClosureLifecycle(
      finalize: {
        order.with { $0.append("finalize") }
        finalThreads.with { $0.append(ThreadID.current) }
      }
    )
  )
  let running = DispatchSemaphore(value: 0)
  var boundThreadID: ThreadID?
  var pending: Task<Void, Never>?

  do {
    let executor = try await AttachedThreadExecutor.attach(to: dispatcher)
    boundThreadID = executor.threadID
    pending = Task {
      await withTaskExecutorPreference(executor) {
        order.with { $0.append("job") }
        running.signal()
      }
    }
    waitForSignal(running)
  }

  await pending!.value

  var attempts = 100
  while finalThreads.get().isEmpty && attempts > 0 {
    try await Task.sleep(for: .milliseconds(10))
    attempts -= 1
  }
  #expect(order.get() == ["job", "finalize"])
  #expect(finalThreads.get().count == 1)
  #expect(finalThreads.get().first == boundThreadID)

  dispatcher.stopAndJoin()
}
