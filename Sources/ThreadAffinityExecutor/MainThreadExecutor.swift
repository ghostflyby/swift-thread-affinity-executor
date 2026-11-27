//
//  MainThreadExecutor.swift
//  ThreadAffinityExecutor
//

#if canImport(CoreFoundation) && canImport(Dispatch)
  import CoreFoundation
  import Dispatch
  import Foundation
  import Synchronization

  /// The main-thread executor.
  ///
  /// `mainThreadExecutor` is the main-thread-bound view: jobs run on
  /// `DispatchQueue.main` when the host drains it. The first access from a
  /// non-main thread captures the main thread ID by hopping to the main
  /// queue and blocks until that queue runs; warm it from the main thread
  /// before starting background work.
  ///
  /// `mainRunLoopExecutor` additionally returns the run-loop-servicing view.
  /// It requires the main run loop to be running and traps otherwise; the
  /// main run loop is owned by the host and is never started or stopped here.
  ///
  /// A job must not pump the main run loop: a nested main loop drains the
  /// main queue and can run another job reentrantly.
  ///
  /// Requires CoreFoundation and Dispatch.
  public protocol MainThreadExecutor: ThreadBoundExecutor {}

  /// The main-thread-bound view of the main executor.
  ///
  /// The first access from a non-main thread blocks until the main queue
  /// runs while the main thread ID is captured.
  public var mainThreadExecutor: any MainThreadExecutor {
    MainThreadExecutorImpl.instance
  }

  /// The run-loop-servicing view of the main executor.
  ///
  /// - Precondition: the main run loop is running.
  public var mainRunLoopExecutor:
    any MainThreadExecutor
      & RunLoopServicingExecutor
  {
    precondition(
      CFRunLoopCopyCurrentMode(CFRunLoopGetMain()) != nil,
      "The main run loop is not running"
    )
    return MainThreadExecutorImpl.instance
  }

  private final class MainThreadExecutorImpl: MainThreadExecutor,
    RunLoopServicingExecutor, Sendable
  {
    private static let sharedState = Mutex<MainThreadExecutorImpl?>(nil)

    static var instance: MainThreadExecutorImpl {
      if let existing = sharedState.withLock({ $0 }) {
        return existing
      }
      let threadID =
        Thread.isMainThread
        ? ThreadID.current
        : DispatchQueue.main.sync { ThreadID.current }
      return publish(threadID)
    }

    private static func publish(
      _ threadID: ThreadID
    ) -> MainThreadExecutorImpl {
      sharedState.withLock { state in
        if let existing = state {
          return existing
        }
        let executor = MainThreadExecutorImpl(threadID: threadID)
        state = executor
        return executor
      }
    }

    let threadID: ThreadID

    private init(threadID: ThreadID) {
      self.threadID = threadID
    }

    func enqueue(_ job: consuming ExecutorJob) {
      let job = UnownedJob(job)
      DispatchQueue.main.async { [self] in
        runJob(job)
      }
    }
  }
#endif
