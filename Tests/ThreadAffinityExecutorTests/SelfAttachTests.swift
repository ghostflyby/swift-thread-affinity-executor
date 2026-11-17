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
  let initializerThreads = TestBox<[ThreadID]>([])
  let dispatcher = QueueDispatcher(
    lifecycle: ClosureLifecycle {
      initializerThreads.with { $0.append(ThreadID.current) }
    }
  )
  let executor = try AttachedThreadExecutor.attachOnCurrentThread(to: dispatcher)
  #expect(initializerThreads.get() == [ThreadID.current])
  #expect(executor.threadID == ThreadID.current)

  dispatcher.stop()
  dispatcher.run()
}

/// An `initialize` error fails the self-attach synchronously.
@Test func selfAttachInitializeFailureThrows() {
  #expect(throws: SelfAttachInitializationFailed.self) {
    _ = try AttachedThreadExecutor.attachOnCurrentThread(
      to: QueueDispatcher(
        lifecycle: ClosureLifecycle(initialize: { throw SelfAttachInitializationFailed() })
      )
    )
  }
}

/// The pump's `finalize` fires exactly once regardless of which anchor
/// reaches it first: the discarded executor's deinit backstop posts it
/// before `run()`, the first `run()` drains it in the work loop, and the
/// run-exit guard no-ops; re-entering `run()` stays a no-op.
@Test func pumpFinalizeRunsOnceAtRunExitOnOwnerThread() throws {
  let finalThreads = TestBox<[ThreadID]>([])
  let dispatcher = QueueDispatcher(
    lifecycle: ClosureLifecycle(finalize: { finalThreads.with { $0.append(ThreadID.current) } })
  )
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

/// Self-attached executor: `detach()` from another thread posts finalize
/// through the pump; FIFO runs it after all accepted work on the owner
/// thread, and `run()` drains it before returning.
@Test func selfAttachedDetachRunsFinalizeThroughPump() throws {
  let finalThreads = TestBox<[ThreadID]>([])
  let pumpBox = TestBox<QueueDispatcher?>(nil)
  let dispatcher = QueueDispatcher(
    lifecycle: ClosureLifecycle(
      finalize: {
        finalThreads.with { $0.append(ThreadID.current) }
        pumpBox.get()?.stop()  // the test pump's exit
      }
    )
  )
  pumpBox.set(dispatcher)
  let executor = try AttachedThreadExecutor.attachOnCurrentThread(to: dispatcher)

  let detached = DispatchSemaphore(value: 0)
  let stopper = Thread {
    executor.detach()
    detached.signal()
  }
  stopper.start()

  dispatcher.run()
  detached.wait()

  #expect(finalThreads.get() == [ThreadID.current])
}

/// `post` reports rejection once the pump stopped instead of silently
/// queueing work that never runs.
@Test func postAfterStopReturnsFalse() {
  let dispatcher = QueueDispatcher()
  dispatcher.stop()
  #expect(dispatcher.post {} == false)
  dispatcher.run()
}

/// A dispatcher is not bound at construction: the first `run()`/lifecycle
/// entry claims the owner thread, so a Sendable pump can be created on one
/// thread and entered on another.
@Test func dispatcherBindsOwnerThreadOnFirstRun() async {
  let dispatcher = QueueDispatcher()
  let observed = TestBox<ThreadID?>(nil)
  let pumpThread = TestBox<ThreadID?>(nil)
  let ran = DispatchSemaphore(value: 0)
  let finished = DispatchSemaphore(value: 0)

  dispatcher.post {
    observed.set(ThreadID.current)
    ran.signal()
  }
  dispatcher.stop()

  let thread = Thread {
    pumpThread.set(ThreadID.current)
    dispatcher.run()
    finished.signal()
  }
  thread.start()

  await waitForSignal(ran)
  await waitForSignal(finished)
  #expect(observed.get() == pumpThread.get())
  #expect(observed.get() != ThreadID.current)
}

/// `initialize()` is a separate binding entry: self-attach on a thread
/// other than the constructing thread binds the pump there.
@Test func dispatcherBindsOwnerThreadOnSelfAttach() async {
  let dispatcher = QueueDispatcher()
  let pumpThread = TestBox<ThreadID?>(nil)
  let executorBox = TestBox<AttachedThreadExecutor?>(nil)
  let ready = DispatchSemaphore(value: 0)
  let finished = DispatchSemaphore(value: 0)

  let thread = Thread {
    pumpThread.set(ThreadID.current)
    executorBox.set(try? AttachedThreadExecutor.attachOnCurrentThread(to: dispatcher))
    ready.signal()
    dispatcher.stop()
    dispatcher.run()
    finished.signal()
  }
  thread.start()

  await waitForSignal(ready)
  await waitForSignal(finished)

  #expect(executorBox.get()?.threadID == pumpThread.get())
  #expect(pumpThread.get() != ThreadID.current)
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

  /// Each pump work item drains its own autoreleased objects before the
  /// next work item starts.
  @Test func pumpDrainsAutoreleasedObjectsPerWorkItem() {
    let order = TestBox<[String]>([])
    let dispatcher = QueueDispatcher()

    dispatcher.post {
      let probe = AutoreleaseProbe {
        order.with { $0.append("deinit") }
      }
      // `passRetained` + `autorelease` puts the probe in the thread's
      // current pool; the pool's drain owns the release that deinitializes
      // it after this work item returns.
      _ = Unmanaged.passRetained(probe).autorelease()
      order.with { $0.append("first") }
    }
    dispatcher.post {
      order.with { $0.append("second") }
    }
    dispatcher.stop()
    dispatcher.run()

    #expect(order.get() == ["first", "deinit", "second"])
  }
#endif
