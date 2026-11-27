//
//  RunLoopThreadExecutor.swift
//  ThreadAffinityExecutor
//
//  A thread-bound executor whose dedicated thread waits in a CFRunLoop:
//  queue jobs arrive through a run-loop source instead of a blocking take.
//

#if canImport(CoreFoundation) && canImport(Dispatch)
  import BlockingQueue
  import CoreFoundation
  import Foundation
  import Synchronization

  /// An executor bound to a dedicated OS thread that services a `CFRunLoop`.
  ///
  /// The thread waits in `CFRunLoopRun`; jobs travel through the blocking
  /// queue as a backlog and are executed by a version-0 run-loop source.
  /// `enqueue` signals that source and wakes the loop, so an idle executor
  /// picks work up immediately rather than on a poll interval. While the
  /// queue is idle the thread is inside the run loop: timers, sources and
  /// `perform` blocks registered on the bound thread run there, between job
  /// batches.
  ///
  /// The run loop is serviced in the default mode. A job may itself run a
  /// nested run loop; the source then refuses to re-enter job execution, so
  /// queued work resumes only after the job returns. Jobs run one at a time,
  /// in acceptance order, on the bound thread.
  ///
  /// - `init` returns only after the worker published its `ThreadID` and
  ///   installed the run-loop source — the instance accepts jobs the moment
  ///   construction completes.
  /// - `shutdown()` requires the quiescence contract: no actor, task or
  ///   external producer may still submit jobs. Remaining queued jobs are
  ///   executed before the thread exits; a job enqueued from the shutdown
  ///   request onward is dropped. The request wakes the run loop, so an idle
  ///   executor exits without waiting for a poll interval.
  /// - A job must not block waiting for work that only the bound run loop
  ///   can deliver (a timer callback, a run-loop source): the loop is not
  ///   running while the job runs. It may pump a nested run loop, but must
  ///   not then wait for another job of this executor — nested job execution
  ///   is refused.
  /// - Run-loop callbacks are not jobs. `isIsolatingCurrentContext()` is
  ///   thread-based, so it reports `true` in them; a callback dispatched
  ///   while a job pumps a nested run loop is re-entrant with that job.
  ///   Callbacks must not assume executor-bound actor state is quiescent or
  ///   use `assumeIsolated` on it. A nested run loop executes queued jobs
  ///   only when no drain is on the stack: a callback dispatched from the
  ///   idle loop can run jobs from its own nested loop, while a job's nested
  ///   loop — or a callback dispatched underneath it — cannot.
  /// - A callback that pumps a nested run loop delays exit until its pump
  ///   returns; the worker cannot leave while the callback is on the stack.
  ///   After the queue closes, every closed drain re-signals the source so
  ///   the enclosing invocation stops as soon as it regains control.
  /// - The thread is not the main thread: `DispatchQueue.main`,
  ///   `RunLoop.main` and main-thread-only APIs are unaffected.
  ///
  /// Requires CoreFoundation and Dispatch, so the type is absent on
  /// platforms without a serviced run loop (WASI). It conforms to
  /// `RunLoopServicingExecutor`, which generic code can require without
  /// naming `CFRunLoop`.
  public final class RunLoopThreadExecutor: RunLoopServicingExecutor,
    @unchecked Sendable
  {
    /// The worker thread's ID, published during the startup handshake.
    public var threadID: ThreadID {
      handshake.threadID
    }

    private let producer: SharedUnboundedQueueProducer<UnownedJob>
    private let consumer: QueueConsumer<UnownedJob>
    private let supervisor: QueueSupervisor<UnownedJob>
    private let handshake = StartupHandshake()
    private let exitLatch = ExitLatch()
    /// The waker published by the bound thread once its run loop and source
    /// exist; nil before startup and after exit.
    private let wakerSlot = Mutex<RunLoopWaker?>(nil)
    /// Retained for ownership; a running `Thread` also retains itself.
    private var worker: Thread? = nil
    /// Bound-thread only: a nested run loop must not re-enter job execution.
    private var isDraining = false

    /// Jobs executed per source callout before yielding to the run loop, so
    /// a sustained backlog cannot starve timers and other sources.
    private static let jobsPerTurn = 64

    /// Creates the worker thread and waits for the startup handshake.
    ///
    /// - Parameter name: Best-effort OS thread name; diagnostic only.
    ///
    /// The initializer waits only for thread startup, not for queued work.
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
      self.worker = worker
      worker.start()
      // Ready ⇒ the worker published its ThreadID and installed its source.
      handshake.awaitReady()
    }

    /// The worker's service loop: creates the waker and its run-loop
    /// source, publishes the thread ID, then waits in the run loop until
    /// shutdown sealed the queue.
    private func serviceLoop() {
      let waker = RunLoopWaker(
        info: Unmanaged.passUnretained(self).toOpaque()
      )
      wakerSlot.withLock { $0 = waker }
      handshake.publish(ThreadID.current)

      while true {
        CFRunLoopRun()
        if supervisor.isClosed {
          // A foreign callback may have stopped the loop before the source
          // ran; finish the backlog on this thread before exiting.
          drain()
          if supervisor.isSealed {
            break
          }
        }
      }
      let exiting = wakerSlot.withLock { state in
        let waker = state
        state = nil
        return waker
      }
      exiting?.invalidate()
      exitLatch.signal()
    }

    /// Executes queued jobs until the queue is empty, the per-turn budget is
    /// spent, or the queue is closed and drained. Bound-thread only.
    ///
    /// The source callback and the service loop both call this. A nested run
    /// loop inside a job re-enters the source callback; the guard refuses
    /// that call, and the outer drain continues once the job returns.
    fileprivate func drain() {
      guard !isDraining else { return }
      isDraining = true
      defer { isDraining = false }

      var budget = Self.jobsPerTurn
      while true {
        if budget == 0, !supervisor.isClosed {
          // Let timers and other sources run; the source stays signalled
          // so the next callout continues the backlog.
          wakerSlot.withLock { $0?.signal() }
          return
        }
        guard let job = consumer.tryTake() else { break }
        budget -= 1
        withPerWorkAutoreleasePool {
          runJob(job)
        }
      }

      if supervisor.isClosed {
        // The empty observation above sealed the queue if it saw the close;
        // otherwise close() landed after the observation and the enclosing
        // invocation's next drain seals it. Signal again so an enclosing
        // invocation still observes the source: a run-loop callback may have
        // pumped a nested loop, and the stop below only stops that nested
        // invocation. The service loop then stops its own invocation and
        // exits.
        wakerSlot.withLock { $0?.signal() }
        CFRunLoopStop(CFRunLoopGetCurrent())
      }
    }

    public func enqueue(_ job: consuming ExecutorJob) {
      if producer.enqueue(UnownedJob(job)) {
        wakerSlot.withLock { $0?.wake() }
      }
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
      wakerSlot.withLock { $0?.wake() }
      supervisor.waitUntilSealed()
      exitLatch.wait()
    }

    /// Requests shutdown and suspends until the queue drained and the
    /// worker's exit epilogue completed. Jobs arriving after this request
    /// are dropped.
    ///
    /// The asynchronous counterpart of `shutdown()`: the calling task
    /// suspends instead of blocking a thread. The wait does not respond to
    /// cancellation — it returns normally, once the worker finished, even
    /// when the calling task was cancelled.
    ///
    /// - Precondition: awaited from a task that does not run on this
    ///   executor, with the quiescence contract satisfied (canceling tasks
    ///   does not by itself guarantee that). A task bound to the executor
    ///   cannot await it: its resumption would be routed to the closed queue
    ///   and never run. The check traps only when the awaiting thread is the
    ///   bound thread at the call. Idempotent.
    public func shutdown() async {
      precondition(
        ThreadID.current != threadID,
        "shutdown() must not be awaited from a task bound to the executor"
      )
      supervisor.close()
      wakerSlot.withLock { $0?.wake() }
      await exitLatch.waitAsync()
    }

    /// Whether the queue sealed, i.e. the worker observed drain-and-close
    /// and is exiting. Internal; tests and diagnostics.
    var hasFinished: Bool {
      supervisor.isSealed
    }

    /// Number of queued jobs not yet taken. Internal; tests and diagnostics.
    var pendingJobCount: Int {
      supervisor.count
    }
  }

  /// The cross-thread half of the run-loop plumbing: the bound thread's run
  /// loop and the source installed into it. The object is immutable; its
  /// only cross-thread operations are CoreFoundation's documented
  /// thread-safe source signal and wake-up. The unchecked conformance is the
  /// single assertion for the two non-`Sendable` handles; the executor's
  /// slot orders publication and exit.
  private final class RunLoopWaker: @unchecked Sendable {
    let runLoop: CFRunLoop
    let source: CFRunLoopSource

    /// Creates the source, adds it to the bound run loop and captures both
    /// handles. Must run on the bound thread, before the executor publishes
    /// its readiness.
    init(info: UnsafeMutableRawPointer) {
      let runLoop = CFRunLoopGetCurrent()!
      var context = CFRunLoopSourceContext(
        version: 0,
        info: info,
        retain: nil,
        release: nil,
        copyDescription: nil,
        equal: nil,
        hash: nil,
        schedule: nil,
        cancel: nil,
        perform: runLoopSourcePerform
      )
      guard
        let source = CFRunLoopSourceCreate(
          kCFAllocatorSystemDefault,
          0,
          &context
        )
      else {
        preconditionFailure("CFRunLoopSourceCreate failed")
      }
      CFRunLoopAddSource(runLoop, source, defaultRunLoopMode())
      self.runLoop = runLoop
      self.source = source
    }

    /// Marks the source ready and wakes the loop. Safe from any thread;
    /// CoreFoundation documents both calls as thread-safe.
    func wake() {
      CFRunLoopSourceSignal(source)
      CFRunLoopWakeUp(runLoop)
    }

    /// Marks the source ready without waking the loop. Called on the bound
    /// thread, inside the source callback.
    func signal() {
      CFRunLoopSourceSignal(source)
    }

    /// Invalidates the source after the run loop stopped. Called after the
    /// executor cleared its slot, so no other thread can reach this waker.
    func invalidate() {
      CFRunLoopSourceInvalidate(source)
    }
  }

  private func runLoopSourcePerform(_ info: UnsafeMutableRawPointer?) {
    guard let info else { return }
    Unmanaged<RunLoopThreadExecutor>.fromOpaque(info)
      .takeUnretainedValue()
      .drain()
  }

  private func defaultRunLoopMode() -> CFRunLoopMode {
    #if canImport(Darwin)
      CFRunLoopMode.defaultMode
    #else
      kCFRunLoopDefaultMode
    #endif
  }
#endif
