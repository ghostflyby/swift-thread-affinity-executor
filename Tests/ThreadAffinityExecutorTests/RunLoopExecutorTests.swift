//
//  RunLoopExecutorTests.swift
//  ThreadAffinityExecutorTests
//
//  Usage example against a genuine Foundation RunLoop host: a plain
//  Foundation thread servicing its RunLoop, work delivered cross-thread
//  through CFRunLoopPerformBlock — the thread-safe scheduling entry.
//

import Foundation
import Synchronization
import Testing

@testable import ThreadAffinityExecutor

/// `FixedThreadDispatcher` over a Foundation thread running its RunLoop.
/// `post` uses CFRunLoopPerformBlock + CFRunLoopWakeUp (the documented
/// cross-thread scheduling path); posted blocks run serially, in post
/// order, on the host thread while it services the default mode.
private final class RunLoopDispatcher: FixedThreadDispatcher, @unchecked Sendable {
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

  init() {
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

  func post(_ work: @escaping @Sendable () -> Void) {
    CFRunLoopPerformBlock(runLoop, CFRunLoopMode.defaultMode.rawValue, work)
    CFRunLoopWakeUp(runLoop)
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
  let dispatcher = RunLoopDispatcher()
  let initializerThreads = TestBox<[ThreadID]>([])
  let jobThreads = TestBox<[ThreadID]>([])
  let finalThreads = TestBox<[ThreadID]>([])

  let executor = try await AttachedThreadExecutor.attach(
    to: dispatcher,
    initialize: { initializerThreads.with { $0.append(ThreadID.current) } },
    finalize: { finalThreads.with { $0.append(ThreadID.current) } }
  )
  #expect(initializerThreads.get().count == 1)

  await withTaskExecutorPreference(executor) {
    jobThreads.with { $0.append(ThreadID.current) }
  }

  executor.detach()

  #expect(jobThreads.get().first == initializerThreads.get().first)
  #expect(jobThreads.get().first == executor.threadID)
  #expect(finalThreads.get().count == 1)
  #expect(finalThreads.get().first == executor.threadID)

  dispatcher.stopAndJoin()
}
