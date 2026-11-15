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

@Test func attachPublishesHandshakenExecutor() async {
  let dispatcher = TestPumpDispatcher()
  let executor = await AttachedThreadExecutor.attach(to: dispatcher)

  // handshake completed: threadID valid and different from the caller's.
  #expect(executor.threadID != ThreadID.current)
  #expect(executor.isIsolatingCurrentContext() == false)

  dispatcher.stopAndJoin()
}

@Test func attachedJobsRunOnDispatcherThread() async {
  let dispatcher = TestPumpDispatcher()
  let executor = await AttachedThreadExecutor.attach(to: dispatcher)

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

@Test func twoAttachmentsShareTheFixedThread() async {
  let dispatcher = TestPumpDispatcher()
  let first = await AttachedThreadExecutor.attach(to: dispatcher)
  let second = await AttachedThreadExecutor.attach(to: dispatcher)
  #expect(first.threadID == second.threadID)

  dispatcher.stopAndJoin()
}

@Test func actorBindsToAttachedExecutor() async {
  let dispatcher = TestPumpDispatcher()
  let executor = await AttachedThreadExecutor.attach(to: dispatcher)
  let counter = AttachedCounter(executor: executor)

  #expect(await counter.next() == 1)
  #expect(await counter.next() == 2)

  dispatcher.stopAndJoin()
}

// MARK: - Main queue path (environment-conditional)

@Test func mainThreadExecutorWhenMainQueueIsServiced() async throws {
  guard probeMainQueuePumped(timeout: 1.0) else {
    return
  }

  let executor = await MainThreadExecutor.shared
  let tid = await withTaskExecutorPreference(executor) {
    ThreadID.current
  }
  #expect(tid == executor.threadID)
  #expect(tid != ThreadID.current)

  // Singleton identity.
  let again = await MainThreadExecutor.shared
  #expect(again === executor)
}

@Test func mainQueueDispatcherIsAValidFixedThreadDispatcher() async throws {
  guard probeMainQueuePumped(timeout: 1.0) else {
    return
  }

  let executor = await AttachedThreadExecutor.attach(to: MainQueueDispatcher())
  let positive = await withTaskExecutorPreference(executor) {
    executor.isIsolatingCurrentContext()
  }
  #expect(positive == true)
}
