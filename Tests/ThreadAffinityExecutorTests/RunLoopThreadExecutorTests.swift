//
//  RunLoopThreadExecutorTests.swift
//  ThreadAffinityExecutorTests
//

#if canImport(CoreFoundation) && canImport(Dispatch)
  import Foundation
  import Testing

  @testable import ThreadAffinityExecutor

  /// The Linux runners can take ten-plus seconds to start a blocked job under
  /// the full parallel suite; keep failure detection generous.
  private let runLoopTestWaitTimeout: TimeInterval = 30
  private let runLoopTestPumpDeadline: TimeInterval = 60

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

  // MARK: - Isolation checks

  /// Off the bound thread the check is negative.
  @Test func runLoopIsolationCheckIsNegativeOffBoundThread() {
    let executor = RunLoopThreadExecutor(name: "tae-runloop-negative")
    #expect(executor.isIsolatingCurrentContext() == false)
    executor.shutdown()
  }

  /// On the bound thread the check is positive and thread-based.
  @Test func runLoopIsolationCheckIsPositiveOnBoundThread() async {
    let executor = RunLoopThreadExecutor(name: "tae-runloop-positive")
    let positive = await withTaskExecutorPreference(executor) {
      executor.isIsolatingCurrentContext()
    }
    #expect(positive == true)
    await executor.shutdown()
  }

  /// Concurrent submitters, exactly-once execution, no data race in shared
  /// state, all on a single OS thread.
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

    await waitForSignal(fired, timeout: runLoopTestWaitTimeout, "timer did not fire")
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

    await waitForSignal(timerFired, timeout: runLoopTestWaitTimeout, "timer did not fire")
    await waitForSignal(jobRan, timeout: runLoopTestWaitTimeout, "timer-submitted job did not run")
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
    await waitForSignal(firstDone, timeout: runLoopTestWaitTimeout, "first job did not run")

    // Let the worker re-enter the run loop and sleep.
    try? await Task.sleep(for: .milliseconds(20))

    _ = await withTaskExecutorPreference(executor) {
      secondDone.signal()
    }
    await waitForSignal(secondDone, timeout: runLoopTestWaitTimeout, "queued job did not wake")
    await executor.shutdown()
  }

  /// The per-turn budget lets a due run-loop timer fire before a large
  /// backlog drains.
  @Test func runLoopBudgetLetsDueTimerFireBeforeBacklogDrains() async {
    let executor = RunLoopThreadExecutor(name: "tae-runloop-budget")
    let counter = ThreadSafeCounter()
    let threadsSeen = TestBox<Set<ThreadID>>([])
    let blockerStarted = DispatchSemaphore(value: 0)
    let gate = DispatchSemaphore(value: 0)
    let timerFired = DispatchSemaphore(value: 0)
    let firedAt = TestBox<Int?>(nil)

    let blocker = Task {
      await withTaskExecutorPreference(executor) {
        let timer = Timer(fire: Date(), interval: 0, repeats: false) { _ in
          firedAt.set(counter.value)
          timerFired.signal()
        }
        addTimer(timer)
        blockerStarted.signal()
        blockingWait(gate)
      }
    }

    await waitForSignal(blockerStarted, timeout: runLoopTestWaitTimeout, "blocker did not start")

    var jobs: [Task<Void, Never>] = []
    for _ in 0..<200 {
      jobs.append(
        Task {
          await withTaskExecutorPreference(executor) {
            counter.increment(recording: threadsSeen)
          }
        }
      )
    }

    let queued = await waitForPendingJobs(executor, atLeast: 200, timeout: runLoopTestWaitTimeout)
    gate.signal()
    await blocker.value
    for job in jobs {
      await job.value
    }

    #expect(queued, "the 200-job backlog did not queue")
    if queued {
      await waitForSignal(timerFired, timeout: runLoopTestWaitTimeout, "timer did not fire")
      let at = firedAt.get()
      #expect(at != nil)
      if let at {
        #expect(at > 0)
        #expect(at < 200)
      }
    }
    #expect(counter.value == 200)
    #expect(threadsSeen.get() == [executor.threadID])
    await executor.shutdown()
  }

  // MARK: - Wake coalescing

  /// A burst submitted while the worker is busy does not request one wake
  /// per job: the running drain holds the pending flag, so the whole burst
  /// coalesces behind the wake that started it.
  @Test func runLoopEnqueueWakesCoalesceWhileDraining() async {
    let executor = RunLoopThreadExecutor(name: "tae-runloop-wake-coalescing")
    let gateStarted = DispatchSemaphore(value: 0)
    let gate = DispatchSemaphore(value: 0)
    let blocker = Task {
      await withTaskExecutorPreference(executor) {
        gateStarted.signal()
        blockingWait(gate)
      }
    }

    await waitForSignal(
      gateStarted, timeout: runLoopTestWaitTimeout, "blocker did not start")
    // The blocker's enqueue is the single wake that started this drain.
    #expect(executor.wakeRequestCount == 1)

    let submitted = 200
    var jobs: [Task<Void, Never>] = []
    for _ in 0..<submitted {
      jobs.append(
        Task {
          await withTaskExecutorPreference(executor) {}
        })
    }

    let queued = await waitForPendingJobs(
      executor, atLeast: submitted, timeout: runLoopTestWaitTimeout)
    #expect(queued)
    if queued {
      #expect(executor.wakeRequestCount == 1)
    }

    gate.signal()
    await blocker.value
    for job in jobs {
      await job.value
    }
    await executor.shutdown()
  }

  // MARK: - Nested run loop

  /// A job that pumps a nested run loop does not re-enter job execution:
  /// the source callback refuses while the outer drain is live. The queued
  /// job runs once the pumping job returns.
  @Test func runLoopNestedLoopDoesNotReenterJobs() async {
    let executor = RunLoopThreadExecutor(name: "tae-runloop-nested")
    let nestedStarted = DispatchSemaphore(value: 0)
    let nestedCompleted = TestBox(false)
    let releaseNested = TestBox(false)
    let secondRan = TestBox(false)

    let first = Task {
      await withTaskExecutorPreference(executor) {
        nestedStarted.signal()
        // The outer deadline is a safety net only; the test releases the
        // pump well before it. Keeping it longer than the enqueue-wait
        // timeout avoids coupling the two deadlines.
        let deadline = Date(timeIntervalSinceNow: runLoopTestPumpDeadline)
        while !releaseNested.get() && Date() < deadline {
          pumpRunLoop(for: 0.01)
        }
        nestedCompleted.set(true)
      }
    }

    await waitForSignal(nestedStarted, timeout: runLoopTestWaitTimeout, "nested loop did not start")

    let second = Task {
      await withTaskExecutorPreference(executor) {
        secondRan.set(true)
      }
    }

    // Wait until the second job is actually queued while the first job is
    // still pumping. Without the guard, the nested source callback would
    // take it; with it, the job stays pending until the first job returns.
    let queued = await waitForPendingJobs(executor, atLeast: 1, timeout: runLoopTestWaitTimeout)
    #expect(queued)
    if queued {
      try? await Task.sleep(for: .milliseconds(20))
      #expect(secondRan.get() == false)
      #expect(nestedCompleted.get() == false)
    }
    releaseNested.set(true)

    await first.value
    await second.value
    #expect(nestedCompleted.get())
    #expect(secondRan.get())
    await executor.shutdown()
  }

  /// Shutdown exits even when a run-loop callback is looping through nested
  /// run loop invocations: every closed drain re-signals the source so an
  /// enclosing invocation observes the close.
  @Test func runLoopShutdownExitsWhenCallbackPumpsNestedLoop() async {
    let executor = RunLoopThreadExecutor(
      name: "tae-runloop-callback-nested")
    let callbackStarted = DispatchSemaphore(value: 0)
    let callbackFinished = DispatchSemaphore(value: 0)

    _ = await withTaskExecutorPreference(executor) {
      let timer = Timer(timeInterval: 0.01, repeats: false) { _ in
        callbackStarted.signal()
        // Pump until shutdown closes the queue, so the close necessarily
        // lands inside a nested invocation; then hold one final invocation.
        let deadline = Date(timeIntervalSinceNow: runLoopTestPumpDeadline)
        while !executor.isClosed && Date() < deadline {
          pumpRunLoop(for: 0.01)
        }
        pumpRunLoop(for: 0.2)
        callbackFinished.signal()
      }
      addTimer(timer)
    }

    await waitForSignal(callbackStarted, timeout: runLoopTestWaitTimeout, "callback did not start")

    let shutdownReturned = DispatchSemaphore(value: 0)
    let shutdown = Task {
      await executor.shutdown()
      shutdownReturned.signal()
    }

    let returned = await waitForSignal(
      shutdownReturned, timeout: runLoopTestWaitTimeout, "shutdown did not return")
    if !returned {
      // Unstick the worker so the test can finish; the assertion below
      // records the failure.
      await executor.shutdown()
    }
    await shutdown.value
    await waitForSignal(
      callbackFinished, timeout: runLoopTestWaitTimeout, "callback did not finish")
    #expect(returned)
    #expect(executor.hasFinished)
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

    await waitForSignal(jobStarted, timeout: runLoopTestWaitTimeout, "job did not start")
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

  /// Waits until at least `count` jobs are queued but not yet taken.
  private func waitForPendingJobs(
    _ executor: RunLoopThreadExecutor,
    atLeast count: Int,
    timeout: TimeInterval
  ) async -> Bool {
    let deadline = Date(timeIntervalSinceNow: timeout)
    while Date() < deadline {
      if executor.pendingJobCount >= count { return true }
      try? await Task.sleep(for: .milliseconds(2))
    }
    return executor.pendingJobCount >= count
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
