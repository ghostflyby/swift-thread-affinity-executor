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

/// `ThreadBoundExecutor` counting the jobs it was handed, so tests can
/// observe when a whole batch was accepted by a bound executor. Also stands
/// in for any conformer that is not the library's own executor.
private final class CountingBound: ThreadBoundExecutor {
  private let inner: DedicatedThreadExecutor
  let accepted = Atomic<Int>(0)

  init(_ inner: DedicatedThreadExecutor) {
    self.inner = inner
  }

  var threadID: ThreadID {
    inner.threadID
  }

  func enqueue(_ job: consuming ExecutorJob) {
    _ = accepted.wrappingAdd(1, ordering: .relaxed)
    inner.enqueue(job)
  }
}

/// `ThreadBoundExecutor` that parks the first job it is handed until the
/// test opens the gate. A release handing over its first batch is then held
/// mid-flight, so later jobs can be staged behind that batch.
private final class GatingBound: ThreadBoundExecutor {
  private let inner: DedicatedThreadExecutor
  private let entered = Atomic<Bool>(false)
  let handoverStarted = DispatchSemaphore(value: 0)
  let gate = DispatchSemaphore(value: 0)

  init(_ inner: DedicatedThreadExecutor) {
    self.inner = inner
  }

  var threadID: ThreadID {
    inner.threadID
  }

  func enqueue(_ job: consuming ExecutorJob) {
    if entered.compareExchange(expected: false, desired: true, ordering: .acquiringAndReleasing)
      .exchanged
    {
      handoverStarted.signal()
      gate.wait()
    }
    inner.enqueue(job)
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
    started: TestBox<Int>,
    threads: TestBox<[ThreadID]>
  ) {
    Task {
      _ = next()
      threads.with { $0.append(ThreadID.current) }
      started.with { $0 += 1 }
      await waitForSignal(gate)
      threads.with { $0.append(ThreadID.current) }
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

/// Blocks the calling thread until `condition` holds; false when it still
/// does not after the attempt budget. For a job that must hold its bound
/// thread until something happened elsewhere.
private func blockUntil(_ condition: () -> Bool, attempts: Int = 1_000) -> Bool {
  var remaining = attempts
  while !condition() && remaining > 0 {
    Thread.sleep(forTimeInterval: 0.001)
    remaining -= 1
  }
  return condition()
}

/// Polls `condition` every 10 ms; false when it still does not hold after
/// the attempt budget.
private func waitUntil(
  _ condition: @Sendable () -> Bool,
  attempts: Int = 1_000
) async throws -> Bool {
  var remaining = attempts
  while !condition() && remaining > 0 {
    try await Task.sleep(for: .milliseconds(10))
    remaining -= 1
  }
  return condition()
}

/// One recorded job: its submission index and the thread it ran on.
private typealias Record = (index: Int, thread: ThreadID)

/// Submits one job to `executor` that records the thread it ran on.
private func submit(
  _ executor: MigratingThreadExecutor,
  index: Int,
  to records: TestBox<[Record]>
) -> Task<Void, Never> {
  Task {
    await withTaskExecutorPreference(executor) {
      records.with { $0.append((index, ThreadID.current)) }
    }
  }
}

/// The body of an `async let` child: records its thread, waits for the
/// gate, and records it again.
private func recordAcross(
  gate: DispatchSemaphore,
  started: TestBox<Int>,
  threads: TestBox<[ThreadID]>
) async {
  threads.with { $0.append(ThreadID.current) }
  started.with { $0 += 1 }
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
  #expect(executor.isMigrating == false)

  let threads = await sampleThreads(executor, count: 8)
  #expect(threads.count == 8)
  #expect(threads.allSatisfy { $0 == first.threadID })

  await shutdown(first)
}

@Test func migrationMovesSubsequentJobsToTheNewBoundThread() async throws {
  let first = try DedicatedThreadExecutor(name: "tae-migrate-first")
  let second = try DedicatedThreadExecutor(name: "tae-migrate-second")
  let executor = MigratingThreadExecutor(bound: first)

  try await executor.migrate(to: second)

  #expect(executor.currentThreadID == second.threadID)
  #expect(executor.isMigrating == false)
  let threads = await sampleThreads(executor, count: 8)
  #expect(threads.allSatisfy { $0 == second.threadID })

  await shutdown(first)
  await shutdown(second)
}

/// The migration cut is acceptance: everything the old bound executor
/// accepted finishes there, in order, before anything accepted after
/// routing stopped runs on the new one — also in order. While the old
/// executor is still draining, the staged jobs wait, the reported thread is
/// still the old one, and so is the thread the isolation check accepts.
@Test func migrationMovesOnlyJobsAcceptedAfterwards() async throws {
  let first = try DedicatedThreadExecutor(name: "tae-migrate-first")
  let second = try DedicatedThreadExecutor(name: "tae-migrate-second")
  let firstBound = CountingBound(first)
  let executor = MigratingThreadExecutor(bound: firstBound)
  let records = TestBox<[Record]>([])
  let queued = 16
  let staged = 16

  // One job holds the first executor, so everything submitted below queues.
  // Once released it submits again — a job running before the cut handing in
  // work after it — and holds the thread until that work is staged.
  let hold = DispatchSemaphore(value: 0)
  let held = DispatchSemaphore(value: 0)
  let resubmitted = TestBox<Task<Void, Never>?>(nil)
  let restaged = TestBox(false)
  let holder = Task {
    await withTaskExecutorPreference(executor) {
      held.signal()
      blockingWait(hold)
      resubmitted.set(submit(executor, index: queued + staged, to: records))
      restaged.set(blockUntil { executor.stagedJobCount == staged + 1 })
    }
  }
  await waitForSignal(held)
  defer { hold.signal() }

  // A direct client of the first executor, queued behind the holder. It runs
  // before the barrier, so the old thread must still count as current.
  let isolatedOnOld = TestBox<Bool?>(nil)
  let probe = Task {
    await withTaskExecutorPreference(firstBound) {
      isolatedOnOld.set(executor.isIsolatingCurrentContext())
    }
  }
  let probeQueued = try await waitUntil {
    firstBound.accepted.load(ordering: .acquiring) == 2
  }
  #expect(probeQueued)

  // Submitted one at a time, each after the previous one was accepted, so
  // acceptance order is index order.
  var queuedTasks: [Task<Void, Never>] = []
  for index in 0..<queued {
    queuedTasks.append(submit(executor, index: index, to: records))
    let settled = try await waitUntil {
      firstBound.accepted.load(ordering: .acquiring) == index + 3
    }
    #expect(settled)
  }

  let finished = TestBox(false)
  let migration = Task {
    try await executor.migrate(to: second)
    finished.set(true)
  }
  let routingStopped = try await waitUntil { executor.isMigrating }
  #expect(routingStopped)

  var stagedTasks: [Task<Void, Never>] = []
  for index in 0..<staged {
    stagedTasks.append(submit(executor, index: queued + index, to: records))
    let settled = try await waitUntil { executor.stagedJobCount == index + 1 }
    #expect(settled)
  }

  // The old executor still runs its backlog: nothing moved yet.
  #expect(finished.get() == false)
  #expect(executor.currentThreadID == first.threadID)
  #expect(records.get().isEmpty)
  // The new executor already serves its own clients, but nothing of the
  // migrating executor runs there, so that thread does not count as current.
  let isolatedOnNew = await withTaskExecutorPreference(second) {
    executor.isIsolatingCurrentContext()
  }
  #expect(isolatedOnNew == false)

  hold.signal()
  await holder.value
  await probe.value
  if let task = resubmitted.get() {
    await task.value
  }
  try await migration.value
  for task in queuedTasks {
    await task.value
  }
  for task in stagedTasks {
    await task.value
  }

  #expect(isolatedOnOld.get() == true)
  #expect(restaged.get())
  let observed = records.get()
  #expect(observed.count == queued + staged + 1)
  #expect(observed.map(\.index) == Array(0..<(queued + staged + 1)))
  #expect(observed.prefix(queued).allSatisfy { $0.thread == first.threadID })
  #expect(observed.dropFirst(queued).allSatisfy { $0.thread == second.threadID })
  #expect(executor.currentThreadID == second.threadID)

  await shutdown(first)
  await shutdown(second)
}

@Test func concurrentMigrationReportsBusy() async throws {
  let first = try DedicatedThreadExecutor(name: "tae-migrate-first")
  let second = try DedicatedThreadExecutor(name: "tae-migrate-second")
  let third = try DedicatedThreadExecutor(name: "tae-migrate-third")
  let executor = MigratingThreadExecutor(bound: first)

  // The first migration cannot finish while the first executor is held.
  let hold = DispatchSemaphore(value: 0)
  let held = DispatchSemaphore(value: 0)
  let holder = Task {
    await withTaskExecutorPreference(executor) {
      held.signal()
      blockingWait(hold)
    }
  }
  await waitForSignal(held)
  defer { hold.signal() }

  let migration = Task {
    try await executor.migrate(to: second)
  }
  let routingStopped = try await waitUntil { executor.isMigrating }
  #expect(routingStopped)

  await #expect(throws: MigrationError.busy) {
    try await executor.migrate(to: third)
  }
  // The rejected call changed nothing.
  #expect(executor.currentThreadID == first.threadID)

  hold.signal()
  await holder.value
  try await migration.value
  #expect(executor.currentThreadID == second.threadID)

  // Once the first completed, a later migration proceeds.
  try await executor.migrate(to: third)
  #expect(executor.currentThreadID == third.threadID)

  await shutdown(first)
  await shutdown(second)
  await shutdown(third)
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

  await shutdown(first)
  await shutdown(second)
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

  await shutdown(first)
  await shutdown(second)
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

  await shutdown(first)
  await shutdown(second)
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

  await shutdown(first)
  await shutdown(second)
}

/// A task suspended when the cut passes resumes on the new bound thread.
@Test func tasksSuspendedAcrossTheCutResumeOnTheNewThread() async throws {
  let first = try DedicatedThreadExecutor(name: "tae-migrate-first")
  let second = try DedicatedThreadExecutor(name: "tae-migrate-second")
  let executor = MigratingThreadExecutor(bound: first)
  let gate = DispatchSemaphore(value: 0)
  let threads = TestBox<[ThreadID]>([])

  let suspended = Task {
    await withTaskExecutorPreference(executor) {
      threads.with { $0.append(ThreadID.current) }
      await waitForSignal(gate)
      threads.with { $0.append(ThreadID.current) }
    }
  }
  let started = try await waitUntil { threads.get().count == 1 }
  #expect(started)

  try await executor.migrate(to: second)
  gate.signal()
  await suspended.value

  #expect(threads.get() == [first.threadID, second.threadID])

  await shutdown(first)
  await shutdown(second)
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
  let started = TestBox<Int>(0)
  let threads = TestBox<[ThreadID]>([])
  let created = 8

  let parent = Task {
    await withTaskExecutorPreference(executor) {
      await withTaskGroup(of: Void.self) { group in
        for _ in 0..<created {
          group.addTask {
            threads.with { $0.append(ThreadID.current) }
            started.with { $0 += 1 }
            await waitForSignal(gate)
            threads.with { $0.append(ThreadID.current) }
          }
        }
      }
    }
  }
  for _ in 0..<created {
    await counter.spawnFollower(gate: gate, started: started, threads: threads)
  }
  let allStarted = try await waitUntil { started.get() == 2 * created }
  #expect(allStarted)

  try await executor.migrate(to: second)
  for _ in 0..<(2 * created) {
    gate.signal()
  }
  await parent.value
  let allResumed = try await waitUntil { threads.get().count == 4 * created }
  #expect(allResumed)

  let before = Array(repeating: first.threadID, count: 2 * created)
  let after = Array(repeating: second.threadID, count: 2 * created)
  #expect(threads.get() == before + after)

  await shutdown(first)
  await shutdown(second)
}

/// `async let` children, like task-group children, inherit the executor they
/// were created under and follow a migration.
@Test func asyncLetChildrenFollowMigration() async throws {
  let first = try DedicatedThreadExecutor(name: "tae-migrate-first")
  let second = try DedicatedThreadExecutor(name: "tae-migrate-second")
  let executor = MigratingThreadExecutor(bound: first)
  let gate = DispatchSemaphore(value: 0)
  let started = TestBox<Int>(0)
  let threads = TestBox<[ThreadID]>([])

  try await withTaskExecutorPreference(executor) {
    async let left = recordAcross(gate: gate, started: started, threads: threads)
    async let right = recordAcross(gate: gate, started: started, threads: threads)
    let bothStarted = try await waitUntil { started.get() == 2 }
    #expect(bothStarted)

    try await executor.migrate(to: second)
    gate.signal()
    gate.signal()
    _ = await (left, right)
  }

  let observed = threads.get()
  #expect(observed.count == 4)
  #expect(observed.prefix(2).allSatisfy { $0 == first.threadID })
  #expect(observed.suffix(2).allSatisfy { $0 == second.threadID })

  await shutdown(first)
  await shutdown(second)
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

  await shutdown(first)
  await shutdown(second)
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

  await shutdown(first)
  await shutdown(second)
}

/// Producers racing the migration: a job lands on either side of the cut,
/// but execution is still all-old-thread, then all-new-thread. A batch on
/// each side is pinned first, so neither half of the check can be empty.
@Test func jobsSerializeAcrossTheCutUnderRacingProducers() async throws {
  let first = try DedicatedThreadExecutor(name: "tae-migrate-first")
  let second = try DedicatedThreadExecutor(name: "tae-migrate-second")
  let firstBound = CountingBound(first)
  let executor = MigratingThreadExecutor(bound: firstBound)
  let threads = TestBox<[ThreadID]>([])
  let queued = 50
  let racing = 200

  // One job holds the first executor, so the pre-cut batch cannot run yet.
  let hold = DispatchSemaphore(value: 0)
  let held = DispatchSemaphore(value: 0)
  let holder = Task {
    await withTaskExecutorPreference(executor) {
      held.signal()
      blockingWait(hold)
    }
  }
  await waitForSignal(held)
  defer { hold.signal() }

  // The pre-cut batch: every job of it accepted by the first executor.
  let preCut = Task {
    await withTaskGroup(of: Void.self) { group in
      for _ in 0..<queued {
        group.addTask {
          await withTaskExecutorPreference(executor) {
            threads.with { $0.append(ThreadID.current) }
          }
        }
      }
    }
  }
  let allQueued = try await waitUntil {
    firstBound.accepted.load(ordering: .acquiring) == queued + 1
  }
  #expect(allQueued)

  // The racing producers begin on the old side of the cut and keep
  // submitting across it.
  let racers = Task {
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
  let someRacedAhead = try await waitUntil {
    firstBound.accepted.load(ordering: .acquiring) > queued + 1
  }
  #expect(someRacedAhead)
  let migration = Task {
    try await executor.migrate(to: second)
  }
  let routingStopped = try await waitUntil { executor.isMigrating }
  #expect(routingStopped)

  hold.signal()
  await holder.value
  try await migration.value
  await preCut.value
  await racers.value
  let sampled = await sampleThreads(executor, count: 8)

  let sequence = threads.get() + sampled
  #expect(sequence.count == queued + racing + 8)
  let cut = sequence.firstIndex(where: { $0 != first.threadID })
  #expect(cut != nil)
  if let cut {
    #expect(cut >= queued)
    #expect(sequence[..<cut].allSatisfy { $0 == first.threadID })
    #expect(sequence[cut...].allSatisfy { $0 == second.threadID })
  }

  await shutdown(first)
  await shutdown(second)
}

/// The release is not one shot: a job staged while an earlier batch is being
/// handed over is released after that batch, in order, and only then does
/// routing turn direct.
@Test func jobsStagedDuringTheReleaseFollowTheBatch() async throws {
  let first = try DedicatedThreadExecutor(name: "tae-migrate-first")
  let second = try DedicatedThreadExecutor(name: "tae-migrate-second")
  let secondBound = GatingBound(second)
  let executor = MigratingThreadExecutor(bound: first)
  let records = TestBox<[Record]>([])
  let batch = 8

  // Hold the first executor while a batch is staged, then let the barrier
  // pass.
  let hold = DispatchSemaphore(value: 0)
  let held = DispatchSemaphore(value: 0)
  let holder = Task {
    await withTaskExecutorPreference(executor) {
      held.signal()
      blockingWait(hold)
    }
  }
  await waitForSignal(held)
  defer { hold.signal() }

  let finished = TestBox(false)
  let migration = Task {
    try await executor.migrate(to: secondBound)
    finished.set(true)
  }
  let routingStopped = try await waitUntil { executor.isMigrating }
  #expect(routingStopped)

  var stagedTasks: [Task<Void, Never>] = []
  for index in 0..<batch {
    stagedTasks.append(submit(executor, index: index, to: records))
    let settled = try await waitUntil { executor.stagedJobCount == index + 1 }
    #expect(settled)
  }

  // The holder is not awaited yet: what follows its scope is queued on the
  // first executor behind the release, and the release is about to be parked.
  hold.signal()
  let handoverStarted = await waitForSignal(secondBound.handoverStarted, timeout: 10)
  #expect(handoverStarted)

  // A job arriving now is staged again, behind the batch already taken.
  let late = submit(executor, index: batch, to: records)
  let lateStaged = try await waitUntil { executor.stagedJobCount == 1 }
  #expect(lateStaged)
  #expect(executor.isMigrating)
  #expect(finished.get() == false)
  #expect(records.get().isEmpty)

  secondBound.gate.signal()
  await holder.value
  try await migration.value
  await late.value
  for task in stagedTasks {
    await task.value
  }

  let observed = records.get()
  #expect(observed.map(\.index) == Array(0...batch))
  #expect(observed.allSatisfy { $0.thread == second.threadID })
  #expect(executor.isMigrating == false)
  #expect(executor.currentThreadID == second.threadID)

  await shutdown(first)
  await shutdown(second)
}

/// The barrier belongs to the old bound executor, not to the migrating
/// executor: it waits behind another client's job on the same executor.
@Test func migrationWaitsForTheBoundExecutorsOwnBacklog() async throws {
  let first = try DedicatedThreadExecutor(name: "tae-migrate-first")
  let second = try DedicatedThreadExecutor(name: "tae-migrate-second")
  let executor = MigratingThreadExecutor(bound: first)

  // Another client of the first executor holds its thread.
  let hold = DispatchSemaphore(value: 0)
  let held = DispatchSemaphore(value: 0)
  let otherClient = Task {
    await withTaskExecutorPreference(first) {
      held.signal()
      blockingWait(hold)
    }
  }
  await waitForSignal(held)
  defer { hold.signal() }

  let finished = TestBox(false)
  let migration = Task {
    try await executor.migrate(to: second)
    finished.set(true)
  }
  let routingStopped = try await waitUntil { executor.isMigrating }
  #expect(routingStopped)

  // Nothing of the migrating executor is pending there; the other client's
  // job alone keeps the migration from completing.
  try await Task.sleep(for: .milliseconds(100))
  #expect(finished.get() == false)

  hold.signal()
  await otherClient.value
  try await migration.value
  #expect(executor.currentThreadID == second.threadID)

  await shutdown(first)
  await shutdown(second)
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

  await shutdown(first)
  await shutdown(second)
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

  await shutdown(first)
}

@Test func bothExecutorFamiliesAreAffinity() async throws {
  let first = try DedicatedThreadExecutor(name: "tae-migrate-first")
  let executor = MigratingThreadExecutor(bound: first)
  let executors: [any Affinity] = [first, executor]

  #expect(executors.map { $0.currentThreadID } == [first.threadID, first.threadID])

  await shutdown(first)
}
