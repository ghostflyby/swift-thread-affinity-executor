//
//  RunLoopExecutorTests.swift
//  ThreadAffinityExecutorTests
//
//  Usage example against a genuine Foundation RunLoop host: a plain
//  Foundation thread servicing its RunLoop, work delivered cross-thread
//  through CFRunLoopPerformBlock — the thread-safe scheduling entry.
//
//  Darwin-only: swift-corelibs-foundation does not expose CFRunLoop via
//  Foundation and RunLoop has no getCFRunLoop().
//

#if canImport(Darwin)
  import Foundation
  import Synchronization
  import Testing

  @testable import ThreadAffinityExecutor

  /// `FixedThreadDispatcher` over a Foundation thread running its RunLoop.
  /// `post` uses CFRunLoopPerformBlock + CFRunLoopWakeUp (the documented
  /// cross-thread scheduling path); posted blocks run serially, in post
  /// order, on the host thread while it services the default mode.
  private final class RunLoopDispatcher: ThreadPumpDelegate, @unchecked Sendable {
    private let lifecycle: any ThreadLifecycleDelegate
    // Semaphore handoff carries the happens-before (TestPumpState style):
    // runLoop is written before ready.signal() and read after ready.wait().
    // CFRunLoop's own scheduling APIs are thread-safe, which the region
    // checker cannot see — hence @unchecked rather than Mutex.withLock.
    private final class Bootstrap: @unchecked Sendable {
      let ready = DispatchSemaphore(value: 0)
      let finished = DispatchSemaphore(value: 0)
      private let stopped = Atomic<Bool>(false)
      var runLoop: CFRunLoop?

      func publish(_ runLoop: CFRunLoop) {
        self.runLoop = runLoop
        ready.signal()
      }

      var isStopped: Bool {
        stopped.load(ordering: .acquiring)
      }

      /// `run()` never returns and `CFRunLoopStop` only ends the current
      /// pass — the checked loop in the thread body exits on this flag.
      func stop() {
        stopped.store(true, ordering: .releasing)
        if let runLoop {
          CFRunLoopStop(runLoop)
        }
      }
    }

    private let bootstrap = Bootstrap()
    private let runLoop: CFRunLoop
    private let thread: Thread

    init(lifecycle: any ThreadLifecycleDelegate = ClosureLifecycle()) {
      self.lifecycle = lifecycle
      let bootstrap = self.bootstrap
      thread = Thread {
        let runLoop = RunLoop.current
        bootstrap.publish(runLoop.getCFRunLoop())
        // The port gives the loop a source to wait on (without one,
        // runMode returns immediately and the loop below would spin); the
        // checked loop is the canonical stoppable-host pattern.
        runLoop.add(Port(), forMode: .default)
        while !bootstrap.isStopped {
          runLoop.run(mode: .default, before: .distantFuture)
        }
        bootstrap.finished.signal()
      }
      thread.name = "tae-runloop-host"
      thread.start()
      bootstrap.ready.wait()
      runLoop = bootstrap.runLoop!
    }

    @discardableResult
    func post(_ work: @escaping @Sendable () -> Void) -> Bool {
      CFRunLoopPerformBlock(runLoop, CFRunLoopMode.defaultMode.rawValue, work)
      CFRunLoopWakeUp(runLoop)
      return true
    }

    func initialize() throws {
      try lifecycle.initialize()
    }

    func finalize() {
      lifecycle.finalize()
    }

    func stopAndJoin() {
      bootstrap.stop()
      bootstrap.finished.wait()
    }
  }

  /// Full attachment lifecycle on a RunLoop host: posted handshake with
  /// `initialize`, jobs on the host thread, `detach` running `finalize`
  /// after all accepted work — all through the RunLoop's own delivery.
  @Test func runLoopHostedAttachmentLifecycle() async throws {
    let initializerThreads = TestBox<[ThreadID]>([])
    let jobThreads = TestBox<[ThreadID]>([])
    let finalThreads = TestBox<[ThreadID]>([])
    let dispatcher = RunLoopDispatcher(
      lifecycle: ClosureLifecycle(
        initialize: { initializerThreads.with { $0.append(ThreadID.current) } },
        finalize: { finalThreads.with { $0.append(ThreadID.current) } }
      )
    )

    let executor = try await AttachedThreadExecutor.attach(to: dispatcher)
    #expect(initializerThreads.get().count == 1)

    await withTaskExecutorPreference(executor) {
      jobThreads.with { $0.append(ThreadID.current) }
    }

    await detach(executor)

    #expect(jobThreads.get().first == initializerThreads.get().first)
    #expect(jobThreads.get().first == executor.threadID)
    #expect(finalThreads.get().count == 1)
    #expect(finalThreads.get().first == executor.threadID)

    dispatcher.stopAndJoin()
  }
#endif
