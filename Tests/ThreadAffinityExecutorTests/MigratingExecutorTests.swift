//
//  MigratingExecutorTests.swift
//  ThreadAffinityExecutorTests
//
//  The migrating executor over thread-bound executors: the migration cut,
//  staging and release order, isolation across the two identities, and
//  borrowed bound executors.
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

  /// From actor-isolated code, a task whose body needs no isolation.
  func spawnBystander(_ threads: TestBox<[ThreadID]>) {
    Task {
      threads.with { $0.append(ThreadID.current) }
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
/// executor is still draining, the staged jobs wait and the reported
/// thread is still the old one.
@Test func migrationMovesOnlyJobsAcceptedAfterwards() async throws {
  let first = try DedicatedThreadExecutor(name: "tae-migrate-first")
  let second = try DedicatedThreadExecutor(name: "tae-migrate-second")
  let firstBound = CountingBound(first)
  let executor = MigratingThreadExecutor(bound: firstBound)
  let records = TestBox<[(index: Int, thread: ThreadID)]>([])

  // One job holds the first executor, so everything submitted below queues.
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

  // Submitted one at a time, each after the previous one was accepted, so
  // acceptance order is index order.
  let queued = 16
  var queuedTasks: [Task<Void, Never>] = []
  for index in 0..<queued {
    queuedTasks.append(
      Task {
        await withTaskExecutorPreference(executor) {
          records.with { $0.append((index, ThreadID.current)) }
        }
      })
    let settled = try await waitUntil {
      firstBound.accepted.load(ordering: .acquiring) == index + 2
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

  let staged = 16
  var stagedTasks: [Task<Void, Never>] = []
  for index in 0..<staged {
    stagedTasks.append(
      Task {
        await withTaskExecutorPreference(executor) {
          records.with { $0.append((queued + index, ThreadID.current)) }
        }
      })
    let settled = try await waitUntil { executor.stagedJobCount == index + 1 }
    #expect(settled)
  }

  // The old executor still runs its backlog: nothing moved yet.
  #expect(finished.get() == false)
  #expect(executor.currentThreadID == first.threadID)
  #expect(records.get().isEmpty)

  hold.signal()
  await holder.value
  try await migration.value
  for task in queuedTasks {
    await task.value
  }
  for task in stagedTasks {
    await task.value
  }

  let observed = records.get()
  #expect(observed.count == queued + staged)
  #expect(observed.map(\.index) == Array(0..<(queued + staged)))
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
  let threads = TestBox<[ThreadID]>([])
  let created = 8

  await withTaskExecutorPreference(executor) {
    for _ in 0..<created {
      Task {
        threads.with { $0.append(ThreadID.current) }
      }
    }
  }
  for _ in 0..<created {
    await counter.spawnBystander(threads)
  }
  try await executor.migrate(to: second)

  let recorded = try await waitUntil { threads.get().count == 2 * created }
  #expect(recorded)
  #expect(threads.get().allSatisfy { $0 != first.threadID && $0 != second.threadID })

  await shutdown(first)
  await shutdown(second)
}

/// Producers racing the migration: a job lands on either side of the cut,
/// but execution is still all-old-thread, then all-new-thread.
@Test func jobsSerializeAcrossTheCutUnderRacingProducers() async throws {
  let first = try DedicatedThreadExecutor(name: "tae-migrate-first")
  let second = try DedicatedThreadExecutor(name: "tae-migrate-second")
  let executor = MigratingThreadExecutor(bound: first)
  let threads = TestBox<[ThreadID]>([])
  let jobCount = 200

  let racing = Task {
    await withTaskGroup(of: Void.self) { group in
      for _ in 0..<jobCount {
        group.addTask {
          await withTaskExecutorPreference(executor) {
            threads.with { $0.append(ThreadID.current) }
          }
        }
      }
    }
  }
  try await executor.migrate(to: second)
  await racing.value

  let sequence = threads.get()
  #expect(sequence.count == jobCount)
  if let cut = sequence.firstIndex(where: { $0 != first.threadID }) {
    #expect(sequence[..<cut].allSatisfy { $0 == first.threadID })
    #expect(sequence[cut...].allSatisfy { $0 == second.threadID })
  }

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
