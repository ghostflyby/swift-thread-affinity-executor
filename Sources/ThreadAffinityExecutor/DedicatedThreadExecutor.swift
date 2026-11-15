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
/// handshake carries the worker's `ThreadID` from the bound thread to the
/// constructor.
///
/// Lifecycle:
///
/// - `init` starts the worker thread and only returns once the worker has
///   published its `ThreadID` — the instance accepts jobs the moment
///   construction completes.
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

  /// Creates the worker thread and waits for its startup handshake.
  ///
  /// - Parameter name: Best-effort OS thread name; diagnostic only.
  ///
  /// `Foundation.Thread.start()` has no failure return, so a thread that
  /// cannot be spawned (resource exhaustion) blocks this initializer —
  /// accepted in exchange for not re-implementing thread management.
  public init(name: String? = nil) {
    let ends = Queue<UnownedJob>().split()
    self.producer = ends.producer.share()
    self.consumer = ends.consumer
    self.supervisor = ends.supervisor
    super.init()
    if let name {
      self.name = name
    }
    self.start()
    // Ready ⇒ the worker published its ThreadID and accepts jobs.
    _ = handshake.waitUntilPublished()
  }

  /// The worker's service loop: blocks on the queue and executes one job
  /// per wake-up until the queue is closed and fully drained.
  public override func main() {
    handshake.publish(ThreadID.current)

    while let job = consumer.take() {
      runJob(job)
    }
  }

  public func enqueue(_ job: consuming ExecutorJob) {
    producer.enqueue(UnownedJob(job))
  }

  /// Requests shutdown and blocks until the queue drained and sealed
  /// (the worker's service loop is exiting). Jobs arriving after this
  /// request are dropped.
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
  }

  /// Whether the queue sealed, i.e. the worker observed drain-and-close
  /// and is exiting. Internal; tests and diagnostics.
  var hasFinished: Bool {
    supervisor.isSealed
  }
}
