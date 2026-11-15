//
//  DedicatedThreadExecutor.swift
//  ThreadAffinityExecutor
//
//  A thread-bound executor bound to a dedicated OS thread created with
//  Foundation.Thread. The executor *is* the worker thread: it subclasses
//  `Thread` and runs its service loop in the overridden `main()`.
//

import BlockingQueue
import Foundation

/// An executor bound to a dedicated OS thread created via `Foundation.Thread`.
///
/// The executor subclasses `Thread` directly. Jobs travel through the
/// generic blocking queue from the `BlockingQueue` library; a one-shot
/// handshake carries the worker's `ThreadID` and the outcome of the
/// on-thread `initialize` closure from the bound thread to the constructor.
///
/// Lifecycle:
///
/// - `initialize` runs exactly once on the bound thread, before the first
///   job — for thread-local state, thread-affine native runtimes and other
///   create-thread-equals-use-thread requirements. An error it throws is
///   rethrown by `init`: the failed executor is never published and the
///   worker thread exits. Cleanup after a failed `initialize` is the
///   closure's own responsibility (e.g. `defer`).
/// - `finalize` runs on the bound thread after the queue drained, before
///   the thread exits — only on the success path; it is skipped when
///   `initialize` failed. `shutdown()` returns only after `finalize`
///   completed.
/// - `init` returns only after `initialize` succeeded — the instance
///   accepts jobs the moment construction completes.
/// - The running `Thread` is retained by Foundation until `main()` returns,
///   so the executor cannot deallocate underneath an accepted-but-unrun job.
/// - `shutdown()` requires the quiescence contract: no actor, task or
///   external producer may still submit jobs. Remaining queued jobs are
///   executed before the thread exits; a job enqueued from the
///   `shutdown()` request onward is dropped and reported by the
///   producer's `enqueue` returning `false`.
public final class DedicatedThreadExecutor: Thread, ThreadBoundExecutor, @unchecked Sendable {
  /// The worker thread's ID, published during the startup handshake.
  public var threadID: ThreadID {
    handshake.threadID
  }

  private let producer: SharedQueueProducer<UnownedJob>
  private let consumer: QueueConsumer<UnownedJob>
  private let supervisor: QueueSupervisor<UnownedJob>
  private let handshake = StartupHandshake()
  private let exitLatch = ExitLatch()
  private let initialize: @Sendable () throws -> Void
  private let finalize: @Sendable () -> Void

  /// Creates the worker thread, runs `initialize` on it and waits for the
  /// startup handshake.
  ///
  /// - Parameters:
  ///   - name: Best-effort OS thread name; diagnostic only.
  ///   - initialize: Runs once on the bound thread before the first job;
  ///     an error it throws fails construction and the worker exits.
  ///   - finalize: Runs once on the bound thread after the queue drained,
  ///     before the thread exits; never runs when `initialize` failed.
  ///
  /// `Foundation.Thread.start()` has no failure return, so a thread that
  /// cannot be spawned (resource exhaustion) blocks this initializer —
  /// accepted in exchange for not re-implementing thread management.
  public init(
    name: String? = nil,
    initialize: @escaping @Sendable () throws -> Void = {},
    finalize: @escaping @Sendable () -> Void = {}
  ) throws {
    let ends = Queue<UnownedJob>().split()
    self.producer = ends.producer.share()
    self.consumer = ends.consumer
    self.supervisor = ends.supervisor
    self.initialize = initialize
    self.finalize = finalize
    super.init()
    if let name {
      self.name = name
    }
    self.start()
    // Ready ⇒ the worker initialized and published its ThreadID.
    try handshake.awaitReady()
  }

  /// The worker's service loop: initializes, then blocks on the queue and
  /// executes one job per wake-up until the queue is closed and fully
  /// drained; `finalize` is the last code to run on the thread.
  public override func main() {
    let threadID = ThreadID.current
    do {
      try initialize()
      handshake.publishSuccess(threadID)
    } catch {
      handshake.publishFailure(error)
      return
    }

    while let job = consumer.take() {
      runJob(job)
    }
    finalize()
    exitLatch.signal()
  }

  public func enqueue(_ job: consuming ExecutorJob) {
    producer.enqueue(UnownedJob(job))
  }

  /// Requests shutdown and blocks until the queue drained, the worker ran
  /// `finalize` and its exit epilogue completed (the thread is exiting).
  /// Jobs arriving after this request are dropped.
  ///
  /// - Precondition: called off the bound thread with the quiescence
  ///   contract satisfied (canceling tasks does not by itself guarantee
  ///   that). Idempotent.
  public func shutdown() {
    precondition(
      ThreadID.current != threadID,
      "shutdown() must not be called from the executor's bound thread"
    )
    supervisor.close()
    supervisor.waitUntilSealed()
    exitLatch.wait()
  }

  /// Whether the queue sealed, i.e. the worker observed drain-and-close
  /// and is exiting. Internal; tests and diagnostics.
  var hasFinished: Bool {
    supervisor.isSealed
  }
}
