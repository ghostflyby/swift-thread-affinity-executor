//
//  CurrentThreadLoopTests.swift
//  ThreadAffinityExecutorTests
//

import Foundation
import Testing

@testable import ThreadAffinityExecutor

/// Jobs submitted before `run()` are drained on the installing thread; a job
/// may request the stop that ends the loop deterministically.
@Test func loopDrainsJobsOnInstallingThread() {
  let executor = CurrentThreadLoopExecutor.installOnCurrentThread()
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
          executor.stop()
        }
      }
    }
  }

  executor.run()

  // run() returned only after draining; the executing thread was the
  // installing thread, so all observations are sequentially visible here.
  #expect(counter.get() == 5)
  #expect(recorder.get().count == 5)
  #expect(recorder.get().allSatisfy { $0 == executor.threadID })
}

/// `stop()` from another thread ends `run()` (the caller drives
/// entry and exit of the loop).
@Test func stopFromAnotherThreadEndsRun() throws {
  let executor = CurrentThreadLoopExecutor.installOnCurrentThread()
  let stopped = DispatchSemaphore(value: 0)
  let stopper = Thread {
    Thread.sleep(forTimeInterval: 0.02)
    executor.stop()
    stopped.signal()
  }
  stopper.start()

  executor.run()
  stopped.wait()

  #expect(executor.lifecycleIsDeactivated)
}

/// Loops are plain objects: several can be installed on one thread and run
/// one after another; a completed loop's `run()` returns immediately.
@Test func sequentialLoopsOnOneThreadRunIndependently() {
  let first = CurrentThreadLoopExecutor.installOnCurrentThread()
  first.stop()
  first.run()

  let second = CurrentThreadLoopExecutor.installOnCurrentThread()
  second.stop()
  second.run()

  #expect(first.lifecycleIsDeactivated)
  #expect(second.lifecycleIsDeactivated)
}

/// `installOnCurrentThread()` captures the installing thread's ID
/// synchronously at install time.
@Test func installCapturesCurrentThreadID() {
  let expected = ThreadID.current
  let executor = CurrentThreadLoopExecutor.installOnCurrentThread()
  #expect(executor.threadID == expected)
  executor.stop()
  executor.run()
}

/// The second concrete executor type answers
/// `isIsolatingCurrentContext()` from its own bound thread (positive) and
/// from any other thread (negative).
@Test func loopExecutorIsolationChecksFollowThread() {
  let executor = CurrentThreadLoopExecutor.installOnCurrentThread()
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

  executor.stop()
  executor.run()
}
