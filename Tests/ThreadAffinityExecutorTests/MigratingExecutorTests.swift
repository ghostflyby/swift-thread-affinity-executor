//
//  MigratingExecutorTests.swift
//  ThreadAffinityExecutorTests
//
//  Migration handoffs: generation advance, cross-generation serialization,
//  actor transparency, transactional hook ordering, abort-on-failure,
//  in-job handOver, termination.
//

import Foundation
import Testing

@testable import ThreadAffinityExecutor

private struct MigrationSetupFailed: Error {}

/// Spawns a pump thread servicing a `QueueDispatcher` — an exclusive
/// pump, as the successor contract requires. The dispatcher is created on
/// the pump thread (its owner precondition requires it) and handed back
/// through a semaphore-ordered bootstrap.
private final class PumpBootstrap: @unchecked Sendable {
  let ready = DispatchSemaphore(value: 0)
  var dispatcher: QueueDispatcher?  // written before signal, read after wait

  func publish(_ dispatcher: QueueDispatcher) {
    self.dispatcher = dispatcher
    ready.signal()
  }
}

private func spawnPump(_ name: String) -> QueueDispatcher {
  let bootstrap = PumpBootstrap()
  let thread = Thread {
    let dispatcher = QueueDispatcher()
    bootstrap.publish(dispatcher)
    dispatcher.run()
  }
  thread.name = name
  thread.start()
  bootstrap.ready.wait()
  return bootstrap.dispatcher!
}

/// Per-generation pump delegate recording hook events with their threads;
/// the boxes are shared across generations so event order stays observable.
private final class RecordingPump: ThreadGenerationDelegate, @unchecked Sendable {
  private let pump: QueueDispatcher
  private let events: TestBox<[String]>
  private let failEpoch: TestBox<Int?>

  init(
    pump: QueueDispatcher,
    events: TestBox<[String]> = TestBox<[String]>([]),
    failEpoch: TestBox<Int?> = TestBox(nil)
  ) {
    self.pump = pump
    self.events = events
    self.failEpoch = failEpoch
  }

  func post(_ work: @escaping @Sendable () -> Void) {
    pump.post(work)
  }

  func initialize(_ generation: MigratingThreadExecutor.Generation) throws {
    if failEpoch.get() == generation.epoch {
      throw MigrationSetupFailed()
    }
    events.with { $0.append("initialize(\(generation.epoch))@\(ThreadID.current)") }
  }

  func finalize(_ generation: MigratingThreadExecutor.Generation) {
    events.with { $0.append("finalize(\(generation.epoch))@\(ThreadID.current)") }
  }
}

/// Actor fixture bound to a migrating executor — jobs must keep flowing
/// across handoffs without rebinding.
actor MigrationCounter {
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
}

@Test func migrationAdvancesGenerationAndThread() async throws {
  let first = spawnPump("tae-migrate-first")
  let second = spawnPump("tae-migrate-second")
  let executor = try await MigratingThreadExecutor.attach(
    to: ClosureGenerationDelegate(to: first)
  )

  let before = await withTaskExecutorPreference(executor) { ThreadID.current }
  #expect(before == executor.current.threadID)
  #expect(executor.current.epoch == 0)

  try await executor.migrate(to: ClosureGenerationDelegate(to: second))

  let after = await withTaskExecutorPreference(executor) { ThreadID.current }
  #expect(after == executor.current.threadID)
  #expect(after != before)
  #expect(executor.current.epoch == 1)

  await executor.terminate()
  first.stop()
  second.stop()
}

/// Jobs serialize across the handoff: every job that ran on the old
/// generation's thread completes before any job runs on the successor's —
/// the thread sequence never interleaves generations.
@Test func jobsSerializeAcrossGenerations() async throws {
  let first = spawnPump("tae-migrate-first")
  let second = spawnPump("tae-migrate-second")
  let executor = try await MigratingThreadExecutor.attach(
    to: ClosureGenerationDelegate(to: first)
  )
  let observedThreads = TestBox<[ThreadID]>([])

  // The first five jobs are fully accepted (executed) before migrating.
  for _ in 0..<5 {
    await withTaskExecutorPreference(executor) {
      observedThreads.with { $0.append(ThreadID.current) }
    }
  }
  let firstThread = executor.current.threadID
  #expect(observedThreads.get().allSatisfy { $0 == firstThread })

  // Jobs racing the handoff land on either generation — execution must
  // still be all-old-thread, then all-new-thread. The group collects all
  // child tasks so the final count is exact.
  let racing = Task {
    await withTaskGroup(of: Void.self) { group in
      for _ in 0..<10 {
        group.addTask {
          await withTaskExecutorPreference(executor) {
            observedThreads.with { $0.append(ThreadID.current) }
          }
        }
      }
    }
  }
  try await executor.migrate(to: ClosureGenerationDelegate(to: second))
  await racing.value

  await withTaskExecutorPreference(executor) {
    observedThreads.with { $0.append(ThreadID.current) }
  }

  let sequence = observedThreads.get()
  #expect(sequence.count == 16)
  #expect(sequence.first == firstThread)
  #expect(sequence.last == executor.current.threadID)
  if let handoff = sequence.firstIndex(where: { $0 != firstThread }) {
    let after = sequence.dropFirst(handoff)
    #expect(after.allSatisfy { $0 == executor.current.threadID })
  }

  await executor.terminate()
  first.stop()
  second.stop()
}

/// An actor bound to the executor survives migration transparently: no
/// rebinding, state continuity, jobs on the new thread.
@Test func actorSurvivesMigrationTransparently() async throws {
  let first = spawnPump("tae-migrate-first")
  let second = spawnPump("tae-migrate-second")
  let executor = try await MigratingThreadExecutor.attach(
    to: ClosureGenerationDelegate(to: first)
  )
  let counter = MigrationCounter(executor: executor)

  #expect(await counter.next() == 1)
  let before = await counter.threadID()

  try await executor.migrate(to: ClosureGenerationDelegate(to: second))

  #expect(await counter.next() == 2)
  let after = await counter.threadID()
  #expect(after != before)
  #expect(after == executor.current.threadID)

  await executor.terminate()
  first.stop()
  second.stop()
}

/// Per-generation hooks run on their threads in the transactional order:
/// initialize(1) precedes finalize(0) — the successor is prepared before
/// the predecessor seals, so failure leaves the old generation serving.
@Test func generationHooksRunPerGenerationInTransactionalOrder() async throws {
  let first = spawnPump("tae-migrate-first")
  let second = spawnPump("tae-migrate-second")
  let events = TestBox<[String]>([])
  let executor = try await MigratingThreadExecutor.attach(
    to: RecordingPump(pump: first, events: events)
  )

  await withTaskExecutorPreference(executor) {}
  try await executor.migrate(to: RecordingPump(pump: second, events: events))
  await withTaskExecutorPreference(executor) {}
  await executor.terminate()

  let recorded = events.get()
  #expect(recorded.count == 4)
  #expect(recorded[0].hasPrefix("initialize(0)@"))
  #expect(recorded[1].hasPrefix("initialize(1)@"))
  #expect(recorded[2].hasPrefix("finalize(0)@"))
  #expect(recorded[3].hasPrefix("finalize(1)@"))
  #expect(recorded[0].dropFirst("initialize(0)@".count) == recorded[2].dropFirst("finalize(0)@".count))
  #expect(recorded[1].dropFirst("initialize(1)@".count) == recorded[3].dropFirst("finalize(1)@".count))

  first.stop()
  second.stop()
}

/// A successor initialize failure aborts the migration transactionally:
/// the error rethrows, the current generation keeps serving, and a later
/// migration to the same pump succeeds.
@Test func successorInitializeFailureAbortsMigration() async throws {
  let first = spawnPump("tae-migrate-first")
  let second = spawnPump("tae-migrate-second")
  let failEpoch = TestBox<Int?>(nil)
  let executor = try await MigratingThreadExecutor.attach(
    to: RecordingPump(pump: first, failEpoch: failEpoch)
  )

  failEpoch.set(1)
  await #expect(throws: MigrationSetupFailed.self) {
    try await executor.migrate(to: RecordingPump(pump: second, failEpoch: failEpoch))
  }
  #expect(executor.current.epoch == 0)
  let stillServing = await withTaskExecutorPreference(executor) { ThreadID.current }
  #expect(stillServing == executor.current.threadID)

  failEpoch.set(nil)
  try await executor.migrate(to: RecordingPump(pump: second, failEpoch: failEpoch))
  #expect(executor.current.epoch == 1)

  await executor.terminate()
  first.stop()
  second.stop()
}

/// `handOver` from inside a job: the calling job completes first, the
/// generation seals, the successor takes over; later jobs run on it.
@Test func handOverFromJobSealsAfterJobCompletes() async throws {
  let first = spawnPump("tae-migrate-first")
  let second = spawnPump("tae-migrate-second")
  let executor = try await MigratingThreadExecutor.attach(
    to: ClosureGenerationDelegate(to: first)
  )
  let order = TestBox<[String]>([])

  await withTaskExecutorPreference(executor) {
    order.with { $0.append("job") }
    executor.handOver(to: ClosureGenerationDelegate(to: second))
  }

  var attempts = 200
  while executor.current.epoch != 1 && attempts > 0 {
    try await Task.sleep(for: .milliseconds(10))
    attempts -= 1
  }
  #expect(executor.current.epoch == 1)

  let after = await withTaskExecutorPreference(executor) { ThreadID.current }
  #expect(after == executor.current.threadID)
  #expect(order.get() == ["job"])

  await executor.terminate()
  first.stop()
  second.stop()
}

/// `terminate()` drains, runs the generation's finalize on its thread,
/// drops later enqueues, and is idempotent.
@Test func terminateDrainsFinalizesAndDrops() async throws {
  let first = spawnPump("tae-migrate-first")
  let events = TestBox<[String]>([])
  let executor = try await MigratingThreadExecutor.attach(
    to: RecordingPump(pump: first, events: events)
  )

  await withTaskExecutorPreference(executor) {}
  await executor.terminate()
  await executor.terminate()

  let recorded = events.get()
  #expect(recorded.count == 2)
  #expect(recorded[0].hasPrefix("initialize(0)@"))
  #expect(recorded[1].hasPrefix("finalize(0)@"))

  let ran = TestBox(0)
  Task.detached {
    await withTaskExecutorPreference(executor) {
      ran.with { $0 += 1 }
    }
  }
  try await Task.sleep(for: .milliseconds(100))
  #expect(ran.get() == 0)

  first.stop()
}

/// Awaiting `migrate()` from a task bound to this executor is safe: the
/// ceremony runs detached and the caller resumes on the new generation —
/// an in-caller ceremony would deadlock (its resumptions route through
/// the gated successor).
@Test func migrateAwaitedFromOnExecutorContextCompletes() async throws {
  let first = spawnPump("tae-migrate-first")
  let second = spawnPump("tae-migrate-second")
  let executor = try await MigratingThreadExecutor.attach(
    to: ClosureGenerationDelegate(to: first)
  )

  try await withTaskExecutorPreference(executor) {
    try await executor.migrate(to: ClosureGenerationDelegate(to: second))
  }

  #expect(executor.current.epoch == 1)
  let after = await withTaskExecutorPreference(executor) { ThreadID.current }
  #expect(after == executor.current.threadID)

  await executor.terminate()
  first.stop()
  second.stop()
}
