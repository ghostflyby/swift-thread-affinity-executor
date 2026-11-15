//
//  SelfAttachTests.swift
//  ThreadAffinityExecutorTests
//
//  Self-attach modality: caller-driven pumps. `attachOnCurrentThread`
//  binds the executor to the calling thread synchronously; `QueueDispatcher`
//  provides the pump the caller enters via `run()`.
//

import Foundation
import Testing

@testable import ThreadAffinityExecutor

/// `attachOnCurrentThread(to:)` captures the calling thread's ID
/// synchronously at attach time.
@Test func selfAttachCapturesCurrentThreadID() throws {
  let dispatcher = QueueDispatcher()
  let executor = try AttachedThreadExecutor.attachOnCurrentThread(to: dispatcher)
  #expect(executor.threadID == ThreadID.current)
  dispatcher.stop()
  dispatcher.run()
}

/// Jobs submitted before the pump runs are drained on the attaching
/// thread; a job may request the stop that ends the pump deterministically.
@Test func selfAttachedPumpDrainsSubmittedJobs() throws {
  let dispatcher = QueueDispatcher()
  let executor = try AttachedThreadExecutor.attachOnCurrentThread(to: dispatcher)
  let recorder = TestBox<[ThreadID]>([])
  let counter = TestBox<Int>(0)

  for _ in 0..<5 {
    Task {
      await withTaskExecutorPreference(executor) {
        recorder.with { $0.append(ThreadID.current) }
        let done = counter.with { value -> Bool in
          value += 1
          return value == 5
        }
        if done {
          dispatcher.stop()
        }
      }
    }
  }

  dispatcher.run()

  // run() returned only after draining; the executing thread was the
  // attaching thread, so all observations are sequentially visible here.
  #expect(counter.get() == 5)
  #expect(recorder.get().count == 5)
  #expect(recorder.get().allSatisfy { $0 == executor.threadID })
}

/// `stop()` from another thread ends `run()` (the caller drives entry and
/// exit of the pump).
@Test func stopFromAnotherThreadEndsPumpRun() throws {
  let dispatcher = QueueDispatcher()
  _ = try AttachedThreadExecutor.attachOnCurrentThread(to: dispatcher)
  let stopped = DispatchSemaphore(value: 0)
  let stopper = Thread {
    Thread.sleep(forTimeInterval: 0.02)
    dispatcher.stop()
    stopped.signal()
  }
  stopper.start()

  dispatcher.run()
  stopped.wait()
}

/// Pumps are plain objects: several can be attached on one thread and run
/// one after another; a completed pump's `run()` returns immediately.
@Test func sequentialPumpsOnOneThreadRunIndependently() throws {
  let first = QueueDispatcher()
  let firstExecutor = try AttachedThreadExecutor.attachOnCurrentThread(to: first)
  first.stop()
  first.run()

  let second = QueueDispatcher()
  let secondExecutor = try AttachedThreadExecutor.attachOnCurrentThread(to: second)
  second.stop()
  second.run()

  #expect(firstExecutor.threadID == secondExecutor.threadID)
}

/// Plain `post`ed work queues up before `run()` and executes in post
/// order on the attaching thread.
@Test func postedWorkRunsInOrderOncePumpEntered() throws {
  let dispatcher = QueueDispatcher()
  _ = try AttachedThreadExecutor.attachOnCurrentThread(to: dispatcher)
  let observed = TestBox<[Int]>([])

  for value in 0..<10 {
    dispatcher.post {
      observed.with { $0.append(value) }
    }
  }
  dispatcher.stop()
  dispatcher.run()

  #expect(observed.get() == Array(0..<10))
}

/// The self-attached executor answers `isIsolatingCurrentContext()` from
/// its bound thread (positive) and from any other thread (negative).
@Test func selfAttachedIsolationChecksFollowThread() throws {
  let dispatcher = QueueDispatcher()
  let executor = try AttachedThreadExecutor.attachOnCurrentThread(to: dispatcher)
  #expect(executor.isIsolatingCurrentContext() == true)

  let probed = DispatchSemaphore(value: 0)
  let negative = TestBox<Bool?>(nil)
  let probe = Thread {
    negative.set(executor.isIsolatingCurrentContext())
    probed.signal()
  }
  probe.start()
  probed.wait()
  #expect(negative.get() == false)

  dispatcher.stop()
  dispatcher.run()
}

private struct SelfAttachInitializationFailed: Error {}

/// Synchronous `initialize` runs on the attaching thread before any job.
@Test func selfAttachInitializeRunsBeforeJobs() throws {
  let dispatcher = QueueDispatcher()
  let initializerThreads = TestBox<[ThreadID]>([])
  let executor = try AttachedThreadExecutor.attachOnCurrentThread(
    to: dispatcher,
    initialize: { initializerThreads.with { $0.append(ThreadID.current) } }
  )
  #expect(initializerThreads.get() == [ThreadID.current])
  #expect(executor.threadID == ThreadID.current)

  dispatcher.stop()
  dispatcher.run()
}

/// An `initialize` error fails the self-attach synchronously.
@Test func selfAttachInitializeFailureThrows() {
  #expect(throws: SelfAttachInitializationFailed.self) {
    _ = try AttachedThreadExecutor.attachOnCurrentThread(
      to: QueueDispatcher(),
      initialize: { throw SelfAttachInitializationFailed() }
    )
  }
}

/// Pump `finalize` runs once at `run()` exit, on the owner thread — even
/// when `run()` is entered again afterwards.
@Test func pumpFinalizeRunsOnceAtRunExitOnOwnerThread() throws {
  let finalThreads = TestBox<[ThreadID]>([])
  let dispatcher = QueueDispatcher(finalize: { finalThreads.with { $0.append(ThreadID.current) } })
  _ = try AttachedThreadExecutor.attachOnCurrentThread(to: dispatcher)

  dispatcher.stop()
  dispatcher.run()
  dispatcher.run()  // a completed pump returns immediately; finalize stays once

  #expect(finalThreads.get().count == 1)
  #expect(finalThreads.get().first == ThreadID.current)
}

/// Work posted after `stop()` is dropped; the pump drains what was posted
/// before the stop and exits.
@Test func postedWorkAfterStopIsDropped() throws {
  let dispatcher = QueueDispatcher()
  _ = try AttachedThreadExecutor.attachOnCurrentThread(to: dispatcher)
  let observed = TestBox<[Int]>([])

  dispatcher.post { observed.with { $0.append(1) } }
  dispatcher.stop()
  dispatcher.post { observed.with { $0.append(2) } }
  dispatcher.run()

  #expect(observed.get() == [1])
}
