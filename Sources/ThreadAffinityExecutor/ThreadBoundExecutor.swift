//
//  ThreadBoundExecutor.swift
//  ThreadAffinityExecutor
//

/// A `SerialExecutor` and `TaskExecutor` whose every accepted job is executed
/// by one fixed OS thread for the executor's whole effective lifetime.
///
/// The protocol deliberately leaves construction, thread ownership, queue
/// mechanics, loop style and shutdown policy to concrete executors. What
/// every conformance must uphold:
///
/// 1. **Unique thread** — all jobs run on the same OS thread from the moment
///    the executor becomes available until it leaves its effective lifetime.
/// 2. **Stable identity** — `threadID` never changes after publication;
///    migrating to another thread means retiring this executor instance.
/// 3. **Real submission** — `enqueue` is safe from any thread and the job is
///    eventually handed to the bound thread.
/// 4. **Serialization** — jobs obey `SerialExecutor` mutual exclusion and
///    ordering; no two jobs run concurrently.
/// 5. **Synchronous isolation correctness** — any synchronous call observing
///    `isIsolatingCurrentContext() == true` is properly ordered with the
///    executor's jobs; adapters must audit nested-loop re-entrancy.
/// 6. **Complete execution** — every accepted job runs exactly once; jobs are
///    never dropped, duplicated or silently discarded.
/// 7. **Lifetime validity** — while tasks or actors may still submit jobs,
///    the bound thread and the scheduling channel remain usable.
public protocol ThreadBoundExecutor: SerialExecutor, TaskExecutor {
    /// The OS thread this executor is bound to. Stable after publication.
    var threadID: ThreadID { get }
}

extension ThreadBoundExecutor {
    /// Dynamic isolation check consumed by the Swift 6.2+ runtime after
    /// regular executor-identity comparison fails.
    ///
    /// A synchronous caller running on the bound thread is isolated with
    /// respect to this executor's jobs. Because the check inspects the
    /// calling thread, it also holds when a *different* `ThreadBoundExecutor`
    /// object is bound to the same OS thread.
    ///
    /// Apple SDKs expose the requirement un-gated, while the open-source
    /// toolchain gates it behind `SwiftStdlib 6.2`; the platform split keeps
    /// both builds warning- and error-free.
    #if canImport(Darwin)
    public func isIsolatingCurrentContext() -> Bool? {
        threadID == ThreadID.current
    }
    #else
    @available(SwiftStdlib 6.2, *)
    public func isIsolatingCurrentContext() -> Bool? {
        threadID == ThreadID.current
    }
    #endif

    /// Fallback isolation check for runtimes without
    /// `isIsolatingCurrentContext()`.
    public func checkIsolated() {
        precondition(
            threadID == ThreadID.current,
            "Incorrect actor isolation: expected \(threadID), got \(ThreadID.current)"
        )
    }

    /// Runs a job on the calling thread, which must be the bound thread.
    func runJob(_ job: consuming UnownedJob) {
        precondition(
            threadID == ThreadID.current,
            "Job executed off the executor's bound thread: expected \(threadID), got \(ThreadID.current)"
        )
        job.runSynchronously(
            isolatedTo: asUnownedSerialExecutor(),
            taskExecutor: asUnownedTaskExecutor()
        )
    }
}
