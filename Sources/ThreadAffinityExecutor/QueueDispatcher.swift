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
///
/// The first `initialize()` or `run()` call binds the pump to the calling
/// thread; later `run()` and lifecycle calls must come from that thread.
/// `finalize()` requires an existing binding. Construction itself is not
/// bound, and `post()`/`stop()` are safe from any thread.
public final class QueueDispatcher: FixedThreadDispatcher {
  private let producer: SharedQueueProducer<@Sendable () -> Void>
  private let consumer: QueueConsumer<@Sendable () -> Void>
  private let supervisor: QueueSupervisor<@Sendable () -> Void>
  private let ownerThread = Mutex<ThreadID?>(nil)
  private let lifecycle: any ThreadLifecycleDelegate
  private let finalized = Atomic<Bool>(false)

  public init(lifecycle: any ThreadLifecycleDelegate = ClosureLifecycle()) {
    let ends = Queue<@Sendable () -> Void>().split()
    producer = ends.producer.share()
    consumer = ends.consumer
    supervisor = ends.supervisor
    self.lifecycle = lifecycle
  }

  /// Returns false once `stop()` was requested or no consumer remains;
  /// the work is then not scheduled.
  @discardableResult
  public func post(_ work: @escaping @Sendable () -> Void) -> Bool {
    producer.enqueue(work)
  }

  /// Binds the pump to the first thread that enters `run()` or
  /// `initialize()`, and traps when another thread already bound it.
  private func bindOwnerThread(_ operation: String) {
    let current = ThreadID.current
    let owner = ownerThread.withLock { owner -> ThreadID in
      if let existing = owner {
        return existing
      }
      owner = current
      return current
    }
    precondition(
      current == owner,
      "\(operation) must be called from the thread that first bound the pump: expected \(owner), got \(current)"
    )
  }

  /// Traps unless the pump is already bound to the calling thread.
  private func requireOwnerThread(_ operation: String) {
    let current = ThreadID.current
    guard let owner = ownerThread.withLock({ $0 }) else {
      preconditionFailure("\(operation) called before the pump was bound to a thread")
    }
    precondition(
      current == owner,
      "\(operation) must be called from the bound thread: expected \(owner), got \(current)"
    )
  }

  /// Occupies the calling thread, running posted work until `stop()` was
  /// requested and the queue drained.
  ///
  /// - The first call binds the pump to the calling thread; later `run()`
  ///   and lifecycle calls must come from that thread. Running elsewhere
  ///   traps instead of breaking the binding silently.
  /// - A pump whose `run()` already completed has nothing left to do and
  ///   returns immediately.
  /// - Work posted before `run()` is entered runs in post order once the
  ///   pump starts.
  @available(*, noasync)
  public func run() {
    bindOwnerThread("run()")

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

  /// The lifecycle's finalize, exactly once, on the bound thread — shared
  /// by `run()` exit and the delegate conformance. The caller must already
  /// have bound the pump.
  private func runLifecycleFinalize() {
    requireOwnerThread("QueueDispatcher finalize()")
    if finalized.compareExchange(expected: false, desired: true, ordering: .acquiringAndReleasing)
      .exchanged
    {
      lifecycle.finalize()
    }
  }
}

extension QueueDispatcher: ThreadPumpDelegate {
  /// Runs at the attach handshake on the target thread — posted via `run()`
  /// for classic attachment, synchronously for self-attach — before any
  /// posted work. Binds the pump to this thread on first use.
  public func initialize() throws {
    bindOwnerThread("QueueDispatcher initialize()")
    try lifecycle.initialize()
  }

  public func finalize() {
    runLifecycleFinalize()
  }
}
