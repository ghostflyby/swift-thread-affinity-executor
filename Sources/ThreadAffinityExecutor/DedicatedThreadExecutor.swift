//
//  DedicatedThreadExecutor.swift
//  ThreadAffinityExecutor
//
//  A thread-bound executor bound to a dedicated OS thread created with
//  Foundation.Thread. The executor owns the worker thread and runs its
//  service loop in a closure; the public type is not a Thread subclass.
//

import BlockingQueue
import Foundation

/// An executor bound to a dedicated OS thread created via `Foundation.Thread`.
///
/// The executor owns a private `Thread` whose entry closure runs the service
/// loop; the public type is not a `Thread` subclass. Jobs travel through the
/// generic blocking queue from the `BlockingQueue` library; a one-shot
/// handshake carries the worker's `ThreadID` to the constructor.
///
/// - `init` returns only after the worker published its ID — the instance
///   accepts jobs the moment construction completes.
/// - The worker `Thread` retains its entry closure — and therefore this
///   executor — until the loop exits, so the executor cannot deallocate
///   underneath an accepted-but-unrun job.
/// - `shutdown()` requires the quiescence contract: no actor, task or
///   external producer may still submit jobs. Remaining queued jobs are
///   executed before the thread exits; a job enqueued from the
///   `shutdown()` request onward is dropped.
public final class DedicatedThreadExecutor: ThreadBoundExecutor, Sendable {
  /// The worker thread's ID, published during the startup handshake.
  public var threadID: ThreadID {
    handshake.threadID
  }

  private let producer: SharedUnboundedQueueProducer<UnownedJob>
  private let consumer: QueueConsumer<UnownedJob>
  private let supervisor: QueueSupervisor<UnownedJob>
  private let handshake = StartupHandshake()
  private let exitLatch = ExitLatch()

  /// Creates the worker thread and waits for the startup handshake.
  ///
  /// - Parameter name: Best-effort OS thread name; diagnostic only.
  ///
  /// The initializer waits only for thread startup, not for queued work.
  /// `Foundation.Thread.start()` has no failure return, so a thread that
  /// cannot be spawned (resource exhaustion) blocks this initializer —
  /// accepted in exchange for not re-implementing thread management.
  public init(name: String? = nil) {
    let ends = UnboundedQueue<UnownedJob>().split(.mpsc)
    self.producer = ends.producer
    self.consumer = ends.consumer
    self.supervisor = ends.supervisor

    let worker = Thread { [self] in
      serviceLoop()
    }
    if let name {
      worker.name = name
    }
    worker.start()
    // Ready ⇒ the worker published its ThreadID.
    handshake.awaitReady()
  }

  /// The worker's service loop: publishes the thread ID, then blocks on the
  /// queue and executes one job per wake-up until the queue is closed and
  /// fully drained.
  private func serviceLoop() {
    handshake.publish(ThreadID.current)

    while let job = consumer.take() {
      withPerWorkAutoreleasePool {
        runJob(job)
      }
    }
    exitLatch.signal()
  }

  public func enqueue(_ job: consuming ExecutorJob) {
    producer.enqueue(UnownedJob(job))
  }

  /// Requests shutdown and blocks until the queue drained and the worker's
  /// exit epilogue completed (the thread is exiting). Jobs arriving after
  /// this request are dropped.
  ///
  /// - Precondition: called off the bound thread with the quiescence
  ///   contract satisfied (canceling tasks does not by itself guarantee
  ///   that). Idempotent.
  @available(*, noasync)
  public func shutdown() {
    precondition(
      ThreadID.current != threadID,
      "shutdown() must not be called from the executor's bound thread"
    )
    supervisor.close()
    supervisor.waitUntilSealed()
    exitLatch.wait()
  }

  /// Requests shutdown and suspends until the queue drained and the worker's
  /// exit epilogue completed. Jobs arriving after this request are dropped.
  ///
  /// The asynchronous counterpart of `shutdown()`: the calling task suspends
  /// instead of blocking a thread. The wait does not respond to cancellation
  /// — it returns normally, once the worker finished, even when the calling
  /// task was cancelled.
  ///
  /// - Precondition: awaited from a task that does not run on this executor,
  ///   with the quiescence contract satisfied (canceling tasks does not by
  ///   itself guarantee that). A task bound to the executor cannot await it:
  ///   its resumption would be routed to the closed queue and never run. The
  ///   check traps only when the awaiting thread is the bound thread at the
  ///   call. Idempotent.
  public func shutdown() async {
    precondition(
      ThreadID.current != threadID,
      "shutdown() must not be awaited from a task bound to the executor"
    )
    supervisor.close()
    await exitLatch.waitAsync()
  }

  /// Whether the queue sealed, i.e. the worker observed drain-and-close
  /// and is exiting. Internal; tests and diagnostics.
  var hasFinished: Bool {
    supervisor.isSealed
  }
}
