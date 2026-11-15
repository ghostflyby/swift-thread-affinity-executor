//
//  CurrentThreadLoopExecutor.swift
//  ThreadAffinityExecutor
//
//  A thread-bound executor driven by an explicit `run()` call occupying the
//  installing thread. Suited for program-provided thread
//  entries — command-line main threads, host-provided fixed threads and
//  embedding environments without an existing event loop.
//

import BlockingQueue

/// Executor whose jobs are executed by whichever thread calls `run()`.
///
/// `installOnCurrentThread()` captures the calling thread's `ThreadID`
/// synchronously; `run()` occupies the calling thread until `stop()` was
/// requested and the queue drained. If the calling thread already belongs
/// to a running UI / third-party event loop, attach through an existing
/// dispatch entry instead of competing pumps.
public final class CurrentThreadLoopExecutor: ThreadBoundExecutor {
  /// Captured synchronously on the installing thread.
  public let threadID: ThreadID

  private let producer: SharedQueueProducer<UnownedJob>
  private let consumer: QueueConsumer<UnownedJob>
  private let supervisor: QueueSupervisor<UnownedJob>

  private init(threadID: ThreadID) {
    let ends = Queue<UnownedJob>().split()
    self.producer = ends.producer.share()
    self.consumer = ends.consumer
    self.supervisor = ends.supervisor
    self.threadID = threadID
  }

  /// Installs the loop — and an executor bound to it — on the calling
  /// thread, capturing its `ThreadID` synchronously.
  ///
  /// Keep a strong reference to the executor until `run()` returned:
  /// enqueued jobs do not retain it.
  public static func installOnCurrentThread() -> CurrentThreadLoopExecutor {
    CurrentThreadLoopExecutor(threadID: ThreadID.current)
  }

  public func enqueue(_ job: consuming ExecutorJob) {
    producer.enqueue(UnownedJob(job))
  }

  /// Occupies the calling thread, executing enqueued jobs until `stop()`
  /// was requested and the queue drained.
  ///
  /// - Must be called on the thread the loop was installed on: running
  ///   the jobs elsewhere would silently break the thread-binding
  ///   guarantee, hence the precondition.
  /// - A loop whose `run()` already completed has nothing left to do and
  ///   returns immediately.
  /// - From the `stop()` request onward, new jobs are dropped (producer
  ///   `enqueue` returns `false`); jobs already queued still run before
  ///   `run()` returns.
  public func run() {
    precondition(
      ThreadID.current == threadID,
      "run() must be called from the thread the loop was installed on: expected \(threadID), got \(ThreadID.current)"
    )

    while let job = consumer.take() {
      runJob(job)
    }
  }

  /// Requests that `run()` stop once the queue drained. Safe from any
  /// thread, including from inside a job running on the loop. Jobs arriving
  /// after this request are dropped.
  public func stop() {
    supervisor.close()
  }

  /// Whether the queue sealed (the loop is draining out or already
  /// returned). Internal; tests and diagnostics.
  var lifecycleIsDeactivated: Bool {
    supervisor.isSealed
  }
}
