//
//  QueueDispatcher.swift
//  ThreadAffinityExecutor
//
//  Library-provided fixed-thread pump: `post` enqueues onto a blocking
//  queue; the thread that calls `run()` drains it. Unlike host dispatchers,
//  the pump is entered manually — pair with
//  `AttachedThreadExecutor.attachOnCurrentThread(to:)`.
//

import BlockingQueue
import Synchronization

/// `FixedThreadDispatcher` backed by the `BlockingQueue` library: posted
/// work queues up; `run()` occupies the calling thread until `stop()` was
/// requested and the queue drained.
///
/// This is the dispatcher for threads without an existing event loop —
/// program main threads, host-provided entry threads, embedding
/// environments. The thread relationship matches any other attachment:
/// the library neither creates nor stops the thread; when `run()` returns,
/// control is back with the caller's code on that same thread.
///
/// As a `ThreadPumpDelegate` it carries one `lifecycle`; this pump anchors
/// `finalize` at the first of the attachment's `detach()` or a bare
/// pump's `run()` exit — whichever the usage reaches.
public final class QueueDispatcher: FixedThreadDispatcher {
  private let producer: SharedQueueProducer<@Sendable () -> Void>
  private let consumer: QueueConsumer<@Sendable () -> Void>
  private let supervisor: QueueSupervisor<@Sendable () -> Void>
  private let ownerThreadID: ThreadID
  private let lifecycle: any ThreadLifecycleDelegate
  private let finalized = Atomic<Bool>(false)

  public init(lifecycle: any ThreadLifecycleDelegate = ClosureLifecycle()) {
    let ends = Queue<@Sendable () -> Void>().split()
    producer = ends.producer.share()
    consumer = ends.consumer
    supervisor = ends.supervisor
    ownerThreadID = ThreadID.current
    self.lifecycle = lifecycle
  }

  /// Returns false once `stop()` was requested or no consumer remains;
  /// the work is then not scheduled.
  @discardableResult
  public func post(_ work: @escaping @Sendable () -> Void) -> Bool {
    producer.enqueue(work)
  }

  /// Occupies the calling thread, running posted work until `stop()` was
  /// requested and the queue drained.
  ///
  /// - Must be called from the thread that created the dispatcher (the
  ///   self-attach thread): running the work elsewhere would silently
  ///   break the thread-binding guarantee, hence the precondition.
  /// - A pump whose `run()` already completed has nothing left to do and
  ///   returns immediately.
  /// - Work posted before `run()` is entered runs in post order once the
  ///   pump starts.
  @available(*, noasync)
  public func run() {
    precondition(
      ThreadID.current == ownerThreadID,
      "run() must be called from the thread that created the dispatcher: expected \(ownerThreadID), got \(ThreadID.current)"
    )

    while let work = consumer.take() {
      withPerWorkAutoreleasePool {
        work()
      }
    }
    runLifecycleFinalize()
  }

  /// Requests that `run()` stop once the queue drained. Safe from any
  /// thread, including from inside posted work. Work arriving after this
  /// request is dropped (`post` returns false).
  public func stop() {
    supervisor.close()
  }

  /// The lifecycle's finalize, exactly once, on the owner thread — shared
  /// by `run()` exit and the delegate conformance.
  private func runLifecycleFinalize() {
    precondition(
      ThreadID.current == ownerThreadID,
      "QueueDispatcher lifecycle hooks run on the owner thread: expected \(ownerThreadID), got \(ThreadID.current)"
    )
    if finalized.compareExchange(expected: false, desired: true, ordering: .acquiringAndReleasing)
      .exchanged
    {
      lifecycle.finalize()
    }
  }
}

extension QueueDispatcher: ThreadPumpDelegate {
  /// Runs at the attach handshake on the owner thread — posted via `run()`
  /// for classic attachment, synchronously for self-attach — before any
  /// posted work.
  public func initialize() throws {
    precondition(
      ThreadID.current == ownerThreadID,
      "QueueDispatcher lifecycle hooks run on the owner thread: expected \(ownerThreadID), got \(ThreadID.current)"
    )
    try lifecycle.initialize()
  }

  public func finalize() {
    runLifecycleFinalize()
  }
}
