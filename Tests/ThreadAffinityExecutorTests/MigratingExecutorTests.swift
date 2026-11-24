//
//  MigratingExecutorTests.swift
//  ThreadAffinityExecutorTests
//
//  The migrating executor over thread-bound executors: the migration cut,
//  staging and release order, isolation across the two identities, what
//  follows a migration, and bound executors of either family — borrowed.
//

import Foundation
import Synchronization
import Testing

@testable import ThreadAffinityExecutor

/// `ThreadBoundExecutor` the test drives by hand: every hand-over queues in
/// the test's own queue and signals, and only a `permit` runs the next job
/// on the bound thread. It is the test's barrier, and its queue is where
/// acceptance is observed — not inside the library.
private final class ManualBound: ThreadBoundExecutor, @unchecked Sendable {
  private final class State: @unchecked Sendable {
    let condition = NSCondition()
    var jobs: [UnownedJob] = []
    var permits = 0
    var stopped = false
  }

  private let state = State()
  private let published = TestBox<ThreadID?>(nil)
  private let started = DispatchSemaphore(value: 0)
  /// One signal per job handed over.
  let received = DispatchSemaphore(value: 0)
  private let finished = DispatchSemaphore(value: 0)

  init(name: String) {
    let thread = Thread { [self] in controlLoop() }
    thread.name = name
    thread.start()
    started.wait()
  }

  var threadID: ThreadID {
    guard let threadID = published.get() else {
      preconditionFailure("ManualBound read before its thread started")
    }
    return threadID
  }

  func enqueue(_ job: consuming ExecutorJob) {
    state.condition.lock()
    state.jobs.append(UnownedJob(job))
    state.condition.broadcast()
    state.condition.unlock()
    received.signal()
  }

  /// Lets the next `count` queued jobs run on the bound thread.
  func permit(_ count: Int = 1) {
    state.condition.lock()
    state.permits += count
    state.condition.broadcast()
    state.condition.unlock()
  }

  func stopAndJoin() async {
    stop()
    await waitForSignal(finished)
  }

  private func stop() {
    state.condition.lock()
    state.stopped = true
    state.condition.broadcast()
    state.condition.unlock()
  }

  private func controlLoop() {
    published.set(ThreadID.current)
    started.signal()
    state.condition.lock()
    while true {
      if state.jobs.isEmpty && state.stopped {
        state.condition.unlock()
        finished.signal()
        return
      }
      if state.permits > 0 && !state.jobs.isEmpty {
        state.permits -= 1
        let job = state.jobs.removeFirst()
        state.condition.unlock()
        runJob(job)
        state.condition.lock()
        continue
      }
      state.condition.wait()
    }
  }
}

/// Actor fixture bound to the migrating executor — jobs must keep flowing
/// across migrations without rebinding.
private actor MigrationCounter {
  nonisolated let executor: MigratingThreadExecutor
  private var value = 0

  nonisolated var unownedExecutor: UnownedSerialExecutor {
    executor.asUnownedSerialExecutor()
  }

  init(executor: MigratingThreadExecutor) {
    self.executor = executor
  }

  func next() -> Int {
    value += 1
    return value
  }

  func threadID() -> ThreadID {
    ThreadID.current
  }

  /// From actor-isolated code, a task whose body uses the actor: it is an
  /// actor job, so it takes the actor's executor.
  func spawnFollower(
    gate: DispatchSemaphore,
    running: DispatchSemaphore,
    done: DispatchSemaphore,
    threads: TestBox<[ThreadID]>
  ) {
    Task {
      _ = next()
      threads.with { $0.append(ThreadID.current) }
      running.signal()
      await waitForSignal(gate)
      threads.with { $0.append(ThreadID.current) }
      done.signal()
    }
  }

  /// From actor-isolated code, a task whose body needs no isolation: it
  /// reports the thread it started on.
  func spawnBystander() -> Task<ThreadID, Never> {
    Task {
      ThreadID.current
    }
  }
}

/// Sync helper: `assumeIsolated` is unavailable from an async context.
private func nextAssumingIsolated(_ counter: MigrationCounter) -> Int {
  counter.assumeIsolated { site in
    site.next()
  }
}

/// Blocking wait for a job that must park its bound thread.
private func blockingWait(_ semaphore: DispatchSemaphore) {
  semaphore.wait()
}

/// The body of an `async let` child: records its thread, signals that it is
/// running, waits for the gate, and records the thread again.
private func recordAcross(
  gate: DispatchSemaphore,
  running: DispatchSemaphore,
  threads: TestBox<[ThreadID]>
) async {
  threads.with { $0.append(ThreadID.current) }
  running.signal()
  await waitForSignal(gate)
  threads.with { $0.append(ThreadID.current) }
}

/// Runs `count` jobs on `executor`; the threads they ran on.
private func sampleThreads(
  _ executor: MigratingThreadExecutor,
  count: Int
) async -> [ThreadID] {
  await withTaskGroup(of: ThreadID.self) { group in
    for _ in 0..<count {
      group.addTask {
        await withTaskExecutorPreference(executor) {
          ThreadID.current
        }
      }
    }
    var threads: [ThreadID] = []
    for await thread in group {
      threads.append(thread)
    }
    return threads
  }
}

/// An attached executor and the pump its thread runs.
private struct AttachedHost: Sendable {
  let executor: AttachedThreadExecutor
  let dispatcher: QueueDispatcher
}

/// Hands an executor created on the pump thread back to its spawner; the
/// executor is written before the signal and read after the wait.
private final class AttachBootstrap: @unchecked Sendable {
  let ready = DispatchSemaphore(value: 0)
  var executor: AttachedThreadExecutor?

  func publish(_ executor: AttachedThreadExecutor) {
    self.executor = executor
    ready.signal()
  }
}

/// A thread the test owns, running a `QueueDispatcher` pump with an
/// attached executor on it: a bound executor that borrows its thread,
/// unlike the dedicated ones the other tests use.
private func spawnAttached(_ name: String) -> AttachedHost {
  let dispatcher = QueueDispatcher()
  let bootstrap = AttachBootstrap()
  let thread = Thread {
    let executor = try! AttachedThreadExecutor.attachOnCurrentThread(to: dispatcher)
    bootstrap.publish(executor)
    dispatcher.run()
  }
  thread.name = name
  thread.start()
  bootstrap.ready.wait()
  return AttachedHost(executor: bootstrap.executor!, dispatcher: dispatcher)
}

/// Ends an attached host: the attachment detaches — its finalize runs
/// through the pump — and the pump's `run()` then returns.
private func stop(_ host: AttachedHost) async {
  _ = await detach(host.executor)
  host.dispatcher.stop()
}

@Test func jobsRunOnTheInitialBoundThread() async throws {
  let first = try DedicatedThreadExecutor(name: "tae-migrate-first")
  let executor = MigratingThreadExecutor(bound: first)
  #expect(executor.currentThreadID == first.threadID)

  let threads = await sampleThreads(executor, count: 8)
  #expect(threads.count == 8)
  #expect(threads.allSatisfy { $0 == first.threadID })

  await first.shutdown()
}

@Test func migrationMovesSubsequentJobsToTheNewBoundThread() async throws {
  let first = try DedicatedThreadExecutor(name: "tae-migrate-first")
  let second = try DedicatedThreadExecutor(name: "tae-migrate-second")
  let executor = MigratingThreadExecutor(bound: first)

  try await executor.migrate(to: second)

  #expect(executor.currentThreadID == second.threadID)
  let threads = await sampleThreads(executor, count: 8)
  #expect(threads.allSatisfy { $0 == second.threadID })

  await first.shutdown()
  await second.shutdown()
}

/// The cut, observed at the bound itself: a hand-driven old bound holds
/// every job until the test permits it, so each report's acceptance is a
/// signal the test waits for. Two reports sent through the migrating
/// executor before the migration are served by the old thread; the arrival
/// of the migration's own barrier at the bound marks the cut, and the two
/// reports sent after it are served by the new thread.
@Test func migrationIDsAcrossTheCut() async throws {
  let oldBound = ManualBound(name: "tae-migrate-first")
  let newBound = try DedicatedThreadExecutor(name: "tae-migrate-second")
  let executor = MigratingThreadExecutor(bound: oldBound)
  let oldThread = oldBound.threadID

  func report() -> Task<ThreadID, Never> {
    Task {
      await withTaskExecutorPreference(executor) {
        ThreadID.current
      }
    }
  }

  // Two reports accepted before the cut; nothing runs until permitted.
  let first = report()
  await waitForSignal(oldBound.received)
  let second = report()
  await waitForSignal(oldBound.received)

  // The migration starts. Its barrier arrives at the old bound once routing
  // has stopped — that hand-over is the cut.
  let migration = Task {
    try await executor.migrate(to: newBound)
  }
  await waitForSignal(oldBound.received)

  // Sent after the cut: it cannot reach the old bound any more.
  let third = report()

  // The old side drains: the two reports, then the migration's own barrier,
  // whose release hands the staged report to the new bound.
  oldBound.permit(3)
  try await migration.value

  // Sent after the release.
  let fourth = report()

  #expect(await first.value == oldThread)
  #expect(await second.value == oldThread)
  #expect(await third.value == newBound.threadID)
  #expect(await fourth.value == newBound.threadID)
  #expect(executor.currentThreadID == newBound.threadID)

  await oldBound.stopAndJoin()
  await newBound.shutdown()
}

/// While a migration is in flight a second one is refused. The hand-driven
/// old bound receives the migration's own barrier and holds it until
/// permitted: that hand-over is the in-flight state, observed without
/// blocking any thread.
@Test func concurrentMigrationReportsBusy() async throws {
  let oldBound = ManualBound(name: "tae-migrate-first")
  let second = try DedicatedThreadExecutor(name: "tae-migrate-second")
  let third = try DedicatedThreadExecutor(name: "tae-migrate-third")
  let executor = MigratingThreadExecutor(bound: oldBound)

  let migration = Task {
    try await executor.migrate(to: second)
  }
  // Routing has stopped by the time the barrier reaches the old bound; it
  // stays in flight until the barrier runs.
  await waitForSignal(oldBound.received)

  await #expect(throws: MigrationError.busy) {
    try await executor.migrate(to: third)
  }

  oldBound.permit(1)
  try await migration.value
  #expect(executor.currentThreadID == second.threadID)

  // Once the first completed, a later migration proceeds.
  try await executor.migrate(to: third)
  #expect(executor.currentThreadID == third.threadID)

  await oldBound.stopAndJoin()
  await second.shutdown()
  await third.shutdown()
}

/// Awaiting `migrate` from a task bound to the executor is safe: the
/// caller resumes on the new bound thread, behind the released jobs.
@Test func migrateAwaitedFromOnExecutorContextResumesOnTheNewThread() async throws {
  let first = try DedicatedThreadExecutor(name: "tae-migrate-first")
  let second = try DedicatedThreadExecutor(name: "tae-migrate-second")
  let executor = MigratingThreadExecutor(bound: first)
  let threads = TestBox<[ThreadID]>([])

  try await withTaskExecutorPreference(executor) {
    threads.with { $0.append(ThreadID.current) }
    try await executor.migrate(to: second)
    threads.with { $0.append(ThreadID.current) }
  }

  #expect(threads.get() == [first.threadID, second.threadID])

  await first.shutdown()
  await second.shutdown()
}

@Test func actorBoundToTheExecutorSurvivesMigration() async throws {
  let first = try DedicatedThreadExecutor(name: "tae-migrate-first")
  let second = try DedicatedThreadExecutor(name: "tae-migrate-second")
  let executor = MigratingThreadExecutor(bound: first)
  let counter = MigrationCounter(executor: executor)

  #expect(await counter.next() == 1)
  let before = await counter.threadID()
  #expect(before == first.threadID)

  try await executor.migrate(to: second)

  #expect(await counter.next() == 2)
  let after = await counter.threadID()
  #expect(after == second.threadID)

  await first.shutdown()
  await second.shutdown()
}

/// Synchronous `assumeIsolated` entry from the current thread. The job runs
/// as the bound executor while the counter names the migrating executor, so
/// the identities never match and only the thread-based check accepts it.
@Test func assumeIsolatedFromTheCurrentThreadIsAccepted() async throws {
  let first = try DedicatedThreadExecutor(name: "tae-migrate-first")
  let second = try DedicatedThreadExecutor(name: "tae-migrate-second")
  let executor = MigratingThreadExecutor(bound: first)
  let counter = MigrationCounter(executor: executor)

  let viaMigrating = await withTaskExecutorPreference(executor) {
    nextAssumingIsolated(counter)
  }
  #expect(viaMigrating == 1)

  // Straight onto the bound executor: a different identity, same thread.
  let viaBound = await withTaskExecutorPreference(first) {
    nextAssumingIsolated(counter)
  }
  #expect(viaBound == 2)

  try await executor.migrate(to: second)

  let viaNewBound = await withTaskExecutorPreference(second) {
    nextAssumingIsolated(counter)
  }
  #expect(viaNewBound == 3)

  await first.shutdown()
  await second.shutdown()
}

/// The isolation check follows the current thread: positive from a job on
/// it, negative anywhere else, and it moves with a migration.
@Test func isolationChecksFollowTheCurrentThread() async throws {
  let first = try DedicatedThreadExecutor(name: "tae-migrate-first")
  let second = try DedicatedThreadExecutor(name: "tae-migrate-second")
  let executor = MigratingThreadExecutor(bound: first)

  #expect(executor.isIsolatingCurrentContext() == false)
  let onFirst = await withTaskExecutorPreference(executor) {
    executor.isIsolatingCurrentContext()
  }
  #expect(onFirst == true)

  try await executor.migrate(to: second)

  #expect(executor.isIsolatingCurrentContext() == false)
  let onSecond = await withTaskExecutorPreference(executor) {
    executor.isIsolatingCurrentContext()
  }
  #expect(onSecond == true)

  await first.shutdown()
  await second.shutdown()
}

/// A task suspended when the cut passes resumes on the new bound thread.
@Test func tasksSuspendedAcrossTheCutResumeOnTheNewThread() async throws {
  let first = try DedicatedThreadExecutor(name: "tae-migrate-first")
  let second = try DedicatedThreadExecutor(name: "tae-migrate-second")
  let executor = MigratingThreadExecutor(bound: first)
  let gate = DispatchSemaphore(value: 0)
  let threads = TestBox<[ThreadID]>([])

  let running = DispatchSemaphore(value: 0)
  let suspended = Task {
    await withTaskExecutorPreference(executor) {
      threads.with { $0.append(ThreadID.current) }
      running.signal()
      await waitForSignal(gate)
      threads.with { $0.append(ThreadID.current) }
    }
  }
  await waitForSignal(running)

  try await executor.migrate(to: second)
  gate.signal()
  await suspended.value

  #expect(threads.get() == [first.threadID, second.threadID])

  await first.shutdown()
  await second.shutdown()
}

/// Structured child tasks inherit the executor they were created under and
/// follow a migration: started before the cut, resumed after it. So does an
/// unstructured task spawned from actor-isolated code whose body uses the
/// actor — it is an actor job and takes the actor's executor.
@Test func structuredChildrenAndActorSpawnedTasksFollowMigration() async throws {
  let first = try DedicatedThreadExecutor(name: "tae-migrate-first")
  let second = try DedicatedThreadExecutor(name: "tae-migrate-second")
  let executor = MigratingThreadExecutor(bound: first)
  let counter = MigrationCounter(executor: executor)
  let gate = DispatchSemaphore(value: 0)
  let running = DispatchSemaphore(value: 0)
  let done = DispatchSemaphore(value: 0)
  let threads = TestBox<[ThreadID]>([])
  let created = 8

  let parent = Task {
    await withTaskExecutorPreference(executor) {
      await withTaskGroup(of: Void.self) { group in
        for _ in 0..<created {
          group.addTask {
            threads.with { $0.append(ThreadID.current) }
            running.signal()
            await waitForSignal(gate)
            threads.with { $0.append(ThreadID.current) }
            done.signal()
          }
        }
      }
    }
  }
  for _ in 0..<created {
    await counter.spawnFollower(gate: gate, running: running, done: done, threads: threads)
  }
  for _ in 0..<(2 * created) {
    await waitForSignal(running)
  }

  try await executor.migrate(to: second)
  for _ in 0..<(2 * created) {
    gate.signal()
  }
  await parent.value
  for _ in 0..<(2 * created) {
    await waitForSignal(done)
  }

  let before = Array(repeating: first.threadID, count: 2 * created)
  let after = Array(repeating: second.threadID, count: 2 * created)
  #expect(threads.get() == before + after)

  await first.shutdown()
  await second.shutdown()
}

/// `async let` children, like task-group children, inherit the executor they
/// were created under and follow a migration.
@Test func asyncLetChildrenFollowMigration() async throws {
  let first = try DedicatedThreadExecutor(name: "tae-migrate-first")
  let second = try DedicatedThreadExecutor(name: "tae-migrate-second")
  let executor = MigratingThreadExecutor(bound: first)
  let gate = DispatchSemaphore(value: 0)
  let running = DispatchSemaphore(value: 0)
  let threads = TestBox<[ThreadID]>([])

  try await withTaskExecutorPreference(executor) {
    async let left = recordAcross(gate: gate, running: running, threads: threads)
    async let right = recordAcross(gate: gate, running: running, threads: threads)
    await waitForSignal(running)
    await waitForSignal(running)

    try await executor.migrate(to: second)
    gate.signal()
    gate.signal()
    _ = await (left, right)
  }

  let observed = threads.get()
  #expect(observed.count == 4)
  #expect(observed.prefix(2).allSatisfy { $0 == first.threadID })
  #expect(observed.suffix(2).allSatisfy { $0 == second.threadID })

  await first.shutdown()
  await second.shutdown()
}

/// Nested scopes do not strand work on the old thread: a task suspended
/// inside an inner `withTaskExecutorPreference` scope resumes on the new
/// bound thread, and so does everything up to the end of the outer scope.
/// After the outer scope the task is back on the executor it had before.
@Test func nestedScopesFollowMigration() async throws {
  let first = try DedicatedThreadExecutor(name: "tae-migrate-first")
  let second = try DedicatedThreadExecutor(name: "tae-migrate-second")
  let executor = MigratingThreadExecutor(bound: first)
  let threads = TestBox<[ThreadID]>([])
  let gate = DispatchSemaphore(value: 0)
  let inInner = DispatchSemaphore(value: 0)

  let task = Task {
    await withTaskExecutorPreference(executor) {
      threads.with { $0.append(ThreadID.current) }
      await withTaskExecutorPreference(executor) {
        threads.with { $0.append(ThreadID.current) }
        inInner.signal()
        await waitForSignal(gate)
        threads.with { $0.append(ThreadID.current) }
      }
      threads.with { $0.append(ThreadID.current) }
      await Task.yield()
      threads.with { $0.append(ThreadID.current) }
    }
    threads.with { $0.append(ThreadID.current) }
  }
  await waitForSignal(inInner)
  try await executor.migrate(to: second)
  gate.signal()
  await task.value

  // Outer start, inner start; inner resumed, inner left, yielded; scope left.
  let observed = threads.get()
  #expect(observed.count == 6)
  #expect(observed.prefix(2).allSatisfy { $0 == first.threadID })
  #expect(observed.dropFirst(2).prefix(3).allSatisfy { $0 == second.threadID })
  let afterScope = observed.last
  #expect(afterScope != first.threadID && afterScope != second.threadID)

  await first.shutdown()
  await second.shutdown()
}

/// An unstructured `Task` whose body needs neither the executor nor an
/// actor inherits neither: Swift leaves it on the default executor, before
/// and after a migration — whether created in a plain job under the
/// executor's preference or from an actor method whose state it does not
/// touch. Structured children, tasks that are actor jobs, or an explicit
/// `withTaskExecutorPreference` are what follow the executor.
@Test func unstructuredTasksDoNotInheritTheExecutor() async throws {
  let first = try DedicatedThreadExecutor(name: "tae-migrate-first")
  let second = try DedicatedThreadExecutor(name: "tae-migrate-second")
  let executor = MigratingThreadExecutor(bound: first)
  let counter = MigrationCounter(executor: executor)
  let created = 8

  // Each task reports the thread it started on and shares nothing.
  var bystanders: [Task<ThreadID, Never>] = []
  await withTaskExecutorPreference(executor) {
    for _ in 0..<created {
      bystanders.append(Task { ThreadID.current })
    }
  }
  for _ in 0..<created {
    bystanders.append(await counter.spawnBystander())
  }
  try await executor.migrate(to: second)

  var observed: [ThreadID] = []
  for bystander in bystanders {
    observed.append(await bystander.value)
  }
  #expect(observed.count == 2 * created)
  #expect(observed.allSatisfy { $0 != first.threadID && $0 != second.threadID })

  await first.shutdown()
  await second.shutdown()
}

/// Producers racing the migration: a job lands on either side of the cut,
/// but execution is still all-old-thread, then all-new-thread. A job before
/// the cut is awaited and a sample after it is taken, so neither half of the
/// check can be empty.
@Test func jobsSerializeAcrossTheCutUnderRacingProducers() async throws {
  let first = try DedicatedThreadExecutor(name: "tae-migrate-first")
  let second = try DedicatedThreadExecutor(name: "tae-migrate-second")
  let executor = MigratingThreadExecutor(bound: first)
  let threads = TestBox<[ThreadID]>([])
  let racing = 200

  let preCut = await withTaskExecutorPreference(executor) {
    ThreadID.current
  }
  #expect(preCut == first.threadID)

  // The producers begin on the old side of the cut and keep submitting
  // across it.
  let producers = Task {
    await withTaskGroup(of: Void.self) { group in
      for _ in 0..<racing {
        group.addTask {
          await withTaskExecutorPreference(executor) {
            threads.with { $0.append(ThreadID.current) }
          }
        }
      }
    }
  }
  try await executor.migrate(to: second)
  await producers.value
  let sampled = await sampleThreads(executor, count: 8)

  let sequence = [preCut] + threads.get() + sampled
  #expect(sequence.count == 1 + racing + 8)
  let cut = sequence.firstIndex(where: { $0 != first.threadID })
  #expect(cut != nil)
  if let cut {
    #expect(cut >= 1)
    #expect(sequence[..<cut].allSatisfy { $0 == first.threadID })
    #expect(sequence[cut...].allSatisfy { $0 == second.threadID })
  }

  await first.shutdown()
  await second.shutdown()
}

/// The barrier belongs to the old bound executor, not to the migrating
/// executor: it waits behind another client's job on the same executor.
@Test func migrationWaitsForTheBoundExecutorsOwnBacklog() async throws {
  let first = try DedicatedThreadExecutor(name: "tae-migrate-first")
  let second = try DedicatedThreadExecutor(name: "tae-migrate-second")
  let executor = MigratingThreadExecutor(bound: first)
  let order = TestBox<[String]>([])

  // Another client of the first executor holds its thread.
  let hold = DispatchSemaphore(value: 0)
  let held = DispatchSemaphore(value: 0)
  let otherClient = Task {
    await withTaskExecutorPreference(first) {
      held.signal()
      blockingWait(hold)
      order.with { $0.append("other client done") }
    }
  }
  await waitForSignal(held)
  defer { hold.signal() }

  let migration = Task {
    try await executor.migrate(to: second)
    order.with { $0.append("migration done") }
  }

  // The migration's barrier is queued behind the other client's job, so the
  // migration cannot complete while that job holds the thread.
  try await Task.sleep(for: .milliseconds(100))
  #expect(order.get().isEmpty)

  hold.signal()
  await otherClient.value
  try await migration.value
  #expect(order.get() == ["other client done", "migration done"])
  #expect(executor.currentThreadID == second.threadID)

  await first.shutdown()
  await second.shutdown()
}

/// Bound executors are borrowed: one migrating executor leaving does not
/// disturb another on the same bound executor, and two can converge on one.
@Test func migrationLeavesTheOldBoundExecutorServing() async throws {
  let first = try DedicatedThreadExecutor(name: "tae-migrate-first")
  let second = try DedicatedThreadExecutor(name: "tae-migrate-second")
  let departing = MigratingThreadExecutor(bound: first)
  let staying = MigratingThreadExecutor(bound: first)

  try await departing.migrate(to: second)

  let stayingThreads = await sampleThreads(staying, count: 8)
  #expect(stayingThreads.allSatisfy { $0 == first.threadID })

  try await staying.migrate(to: second)
  async let departingThreads = sampleThreads(departing, count: 8)
  async let convergedThreads = sampleThreads(staying, count: 8)
  let (left, right) = await (departingThreads, convergedThreads)
  #expect((left + right).allSatisfy { $0 == second.threadID })

  await first.shutdown()
  await second.shutdown()
}

/// A bound executor need not own its thread: attached executors on borrowed
/// pump threads serve on either side of a migration, and the one left behind
/// stays attached to its own clients.
@Test func migrationMovesBetweenAttachedExecutors() async throws {
  let first = spawnAttached("tae-migrate-first")
  let second = spawnAttached("tae-migrate-second")
  let executor = MigratingThreadExecutor(bound: first.executor)
  #expect(executor.currentThreadID == first.executor.threadID)

  let before = await sampleThreads(executor, count: 8)
  #expect(before.allSatisfy { $0 == first.executor.threadID })

  try await executor.migrate(to: second.executor)

  #expect(executor.currentThreadID == second.executor.threadID)
  let after = await sampleThreads(executor, count: 8)
  #expect(after.allSatisfy { $0 == second.executor.threadID })

  // The attachment left behind still serves its own clients.
  let direct = await withTaskExecutorPreference(first.executor) {
    ThreadID.current
  }
  #expect(direct == first.executor.threadID)

  await stop(first)
  await stop(second)
}

/// Migrating to the executor already in place is a plain barrier: the
/// accepted backlog completes, then serving continues on the same thread.
@Test func migratingToTheCurrentBoundExecutorQuiesces() async throws {
  let first = try DedicatedThreadExecutor(name: "tae-migrate-first")
  let executor = MigratingThreadExecutor(bound: first)

  try await executor.migrate(to: first)

  #expect(executor.currentThreadID == first.threadID)
  let threads = await sampleThreads(executor, count: 8)
  #expect(threads.allSatisfy { $0 == first.threadID })

  await first.shutdown()
}

@Test func bothExecutorFamiliesAreAffinity() async throws {
  let first = try DedicatedThreadExecutor(name: "tae-migrate-first")
  let executor = MigratingThreadExecutor(bound: first)
  let executors: [any ThreadAffinityExecutor] = [first, executor]

  #expect(executors.map { $0.currentThreadID } == [first.threadID, first.threadID])

  await first.shutdown()
}
