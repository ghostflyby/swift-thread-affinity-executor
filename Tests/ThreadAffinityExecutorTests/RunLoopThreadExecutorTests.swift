//
//  RunLoopThreadExecutorTests.swift
//  ThreadAffinityExecutorTests
//

#if canImport(CoreFoundation) && canImport(Dispatch)
  import Foundation
  import Testing

  @testable import ThreadAffinityExecutor

  // MARK: - Startup / shutdown

  @Test func runLoopInitPublishesRunningExecutor() {
    let executor = RunLoopThreadExecutor(name: "tae-runloop-basic")
    #expect(!executor.hasFinished)
    #expect(executor.threadID != ThreadID.current)
    executor.shutdown()
    #expect(executor.hasFinished)
  }

  @Test func runLoopConformsToServicingCapability() {
    let executor = RunLoopThreadExecutor(name: "tae-runloop-capability")
    let servicing: any RunLoopServicingExecutor = executor
    #expect(servicing.threadID == executor.threadID)
    executor.shutdown()
  }

  @Test func runLoopShutdownIsIdempotent() {
    let executor = RunLoopThreadExecutor(name: "tae-runloop-idempotent")
    executor.shutdown()
    executor.shutdown()
    #expect(executor.hasFinished)
  }

  /// Closing an idle executor wakes the run loop: no poll interval delays
  /// the exit.
  @Test func runLoopShutdownFromIdleReturns() async {
    let executor = RunLoopThreadExecutor(name: "tae-runloop-idle-shutdown")
    try? await Task.sleep(for: .milliseconds(20))
    await executor.shutdown()
    #expect(executor.hasFinished)
  }

  // MARK: - Thread binding

  /// Many consecutive jobs always read the same TID,
  /// which is the executor's published one.
  @Test func runLoopJobsRunOnBoundThread() async throws {
    let executor = RunLoopThreadExecutor(name: "tae-runloop-binding")

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
    await executor.shutdown()
  }

  /// Concurrent submitters, exactly-once execution, no data race in shared
  /// state, all on a single OS thread. Exercises the per-turn budget and the
  /// source re-signal under a sustained backlog.
  @Test func runLoopJobExecutionIsExactlyOnceUnderConcurrency() async {
    let executor = RunLoopThreadExecutor(name: "tae-runloop-serial")
    let counter = ThreadSafeCounter()
    let threadsSeen = TestBox<Set<ThreadID>>([])

    _ = await withTaskExecutorPreference(executor) {
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
    await executor.shutdown()
  }

  // MARK: - Run loop servicing

  /// A timer scheduled from a job fires on the bound thread while the
  /// executor is otherwise idle.
  @Test func runLoopTimerFiresOnBoundThreadWhileIdle() async {
    let executor = RunLoopThreadExecutor(name: "tae-runloop-timer")
    let fired = DispatchSemaphore(value: 0)
    let timerThread = TestBox<ThreadID?>(nil)

    _ = await withTaskExecutorPreference(executor) {
      let timer = Timer(timeInterval: 0.01, repeats: false) { _ in
        timerThread.set(ThreadID.current)
        fired.signal()
      }
      addTimer(timer)
    }

    await waitForSignal(fired, timeout: 2, "timer did not fire")
    #expect(timerThread.get() == executor.threadID)
    await executor.shutdown()
  }

  /// A run-loop callback can submit a job; `enqueue` wakes the loop and the
  /// job runs on the bound thread.
  @Test func runLoopTimerCallbackCanSubmitJobs() async {
    let executor = RunLoopThreadExecutor(name: "tae-runloop-timer-job")
    let timerFired = DispatchSemaphore(value: 0)
    let jobRan = DispatchSemaphore(value: 0)

    _ = await withTaskExecutorPreference(executor) {
      let timer = Timer(timeInterval: 0.01, repeats: false) { _ in
        timerFired.signal()
        Task {
          await withTaskExecutorPreference(executor) {
            jobRan.signal()
          }
        }
      }
      addTimer(timer)
    }

    await waitForSignal(timerFired, timeout: 2, "timer did not fire")
    await waitForSignal(jobRan, timeout: 2, "timer-submitted job did not run")
    await executor.shutdown()
  }

  /// A job submitted while the worker sleeps in the run loop runs without a
  /// poll interval.
  @Test func runLoopWakesForJobsSubmittedWhileIdle() async {
    let executor = RunLoopThreadExecutor(name: "tae-runloop-wake")
    let firstDone = DispatchSemaphore(value: 0)
    let secondDone = DispatchSemaphore(value: 0)

    _ = await withTaskExecutorPreference(executor) {
      firstDone.signal()
    }
    await waitForSignal(firstDone, timeout: 2, "first job did not run")

    // Let the worker re-enter the run loop and sleep.
    try? await Task.sleep(for: .milliseconds(20))

    _ = await withTaskExecutorPreference(executor) {
      secondDone.signal()
    }
    await waitForSignal(secondDone, timeout: 2, "queued job did not wake")
    await executor.shutdown()
  }

  // MARK: - Nested run loop

  /// A job that pumps a nested run loop does not re-enter job execution:
  /// the source callback refuses while the outer drain is live.
  @Test func runLoopNestedLoopDoesNotReenterJobs() async {
    let executor = RunLoopThreadExecutor(name: "tae-runloop-nested")
    let nestedStarted = DispatchSemaphore(value: 0)
    let nestedCompleted = TestBox(false)
    let secondRan = TestBox(false)

    let first = Task {
      await withTaskExecutorPreference(executor) {
        nestedStarted.signal()
        pumpRunLoop(for: 0.3)
        nestedCompleted.set(true)
      }
    }

    await waitForSignal(nestedStarted, timeout: 2, "nested loop did not start")

    let second = Task {
      await withTaskExecutorPreference(executor) {
        secondRan.set(true)
      }
    }

    // The second job is queued while the first job pumps a nested loop; the
    // nested source callback must not run it.
    try? await Task.sleep(for: .milliseconds(50))
    #expect(secondRan.get() == false)

    await first.value
    await second.value
    #expect(nestedCompleted.get())
    #expect(secondRan.get())
    await executor.shutdown()
  }

  // MARK: - Shutdown drain

  /// Jobs accepted before shutdown all execute; the thread finishes and
  /// `shutdown` returns.
  @Test func runLoopShutdownDrainsAcceptedJobs() async {
    let executor = RunLoopThreadExecutor(name: "tae-runloop-drain")
    let counter = ThreadSafeCounter()
    let threadsSeen = TestBox<Set<ThreadID>>([])

    _ = await withTaskExecutorPreference(executor) {
      await withTaskGroup(of: Void.self) { group in
        for _ in 0..<4 {
          group.addTask {
            for _ in 0..<50 {
              counter.increment(recording: threadsSeen)
              await Task.yield()
            }
          }
        }
      }
    }

    await executor.shutdown()
    #expect(counter.value == 200)
    #expect(threadsSeen.get() == [executor.threadID])
    #expect(executor.hasFinished)
  }

  /// The async shutdown suspends until the worker drained: a bound job holds
  /// the thread, and the shutdown task has not returned while it runs.
  @Test func runLoopAsyncShutdownSuspendsUntilDrain() async {
    let executor = RunLoopThreadExecutor(name: "tae-runloop-async-shutdown")
    let jobStarted = DispatchSemaphore(value: 0)
    let jobRelease = DispatchSemaphore(value: 0)
    let job = Task {
      await withTaskExecutorPreference(executor) {
        jobStarted.signal()
        blockingWait(jobRelease)
      }
    }

    await waitForSignal(jobStarted, timeout: 2, "job did not start")
    let returned = TestBox(false)
    let shutdown = Task {
      await executor.shutdown()
      returned.set(true)
    }
    #expect(returned.get() == false)

    jobRelease.signal()
    await job.value
    await shutdown.value
    #expect(returned.get())
    #expect(executor.hasFinished)

    // After the exit the wait resolves immediately.
    await executor.shutdown()
  }

  /// Adds a timer to the bound thread's run loop. Synchronous on purpose:
  /// `RunLoop.current` is unavailable from async contexts.
  private func addTimer(_ timer: Timer) {
    RunLoop.current.add(timer, forMode: .default)
  }

  /// Runs the bound thread's run loop for `interval`, nested inside the
  /// calling job. Synchronous on purpose: the `noasync` run-loop entry
  /// point must be called from outside the job's async context.
  private func pumpRunLoop(for interval: TimeInterval) {
    RunLoop.current.run(until: Date(timeIntervalSinceNow: interval))
  }

  /// Blocking wait usable from a job body on a dedicated thread.
  private func blockingWait(_ semaphore: DispatchSemaphore) {
    semaphore.wait()
  }
#endif
