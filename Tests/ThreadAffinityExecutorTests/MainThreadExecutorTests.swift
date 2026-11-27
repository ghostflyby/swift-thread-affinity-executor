//
//  MainThreadExecutorTests.swift
//  ThreadAffinityExecutorTests
//

import Dispatch
import Foundation
import Testing

@testable import ThreadAffinityExecutor

// The test runner's main actor is not the main thread on Linux/Windows, so
// the main queue cannot be serviced from these tests.
#if canImport(Darwin)
  @Test @MainActor func mainThreadExecutorIsMainBound() {
    let executor = mainThreadExecutor
    #expect(executor.threadID == ThreadID.current)
    #expect(executor.isIsolatingCurrentContext() == true)

    if RunLoop.main.currentMode != nil {
      let runLoop = mainRunLoopExecutor
      #expect(runLoop.threadID == executor.threadID)
    }
  }

  @Test @MainActor func mainThreadExecutorIsStableAcrossThreads() async {
    let executor = mainThreadExecutor
    let offMain = TestBox<(thread: ThreadID, object: ObjectIdentifier)?>(nil)
    let done = DispatchSemaphore(value: 0)

    DispatchQueue.global().async {
      let value = mainThreadExecutor
      offMain.set((value.threadID, ObjectIdentifier(value as AnyObject)))
      done.signal()
    }

    await waitForSignal(done, timeout: 30, "off-main access did not finish")
    let observed = offMain.get()
    #expect(observed?.thread == executor.threadID)
    #expect(observed?.object == ObjectIdentifier(executor as AnyObject))
  }

  @Test @MainActor func jobsRunOnMainThread() async {
    let executor = mainThreadExecutor
    let done = DispatchSemaphore(value: 0)
    let observed = TestBox<ThreadID?>(nil)

    _ = Task.detached {
      await withTaskExecutorPreference(executor) {
        observed.set(ThreadID.current)
        done.signal()
      }
    }

    let signaled = await waitForSignal(done, timeout: 30, "job did not run")
    if signaled {
      #expect(observed.get() == executor.threadID)
    }
  }
#endif
