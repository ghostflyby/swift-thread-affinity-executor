//
//  AttachedThreadExecutor.swift
//  ThreadAffinityExecutor
//
//  Attaching to threads the library does not own: the main thread via
//  Dispatch, and arbitrary fixed-thread loops through a minimal `post`
//  adapter.
//

import Foundation

/// Minimal adapter surface for attaching to an existing fixed-thread
/// mechanism.
///
/// Implementations must run posted work **serially, in post order, never
/// concurrently, always on the same fixed OS thread** for the executor's
/// whole lifetime; otherwise the type cannot satisfy `ThreadBoundExecutor`.
public protocol FixedThreadDispatcher: Sendable {
    /// Runs `work` on the dispatcher's fixed OS thread.
    func post(_ work: @escaping @Sendable () -> Void)
}

/// Adapter posting onto `DispatchQueue.main`, whose target is the process
/// main thread. Requires a main queue that is actually being serviced.
public struct MainQueueDispatcher: FixedThreadDispatcher {
    public init() {}

    public func post(_ work: @escaping @Sendable () -> Void) {
        DispatchQueue.main.async {
            work()
        }
    }
}

/// Executor whose jobs run on a thread the library does not own: the main
/// thread, a RunLoop host or a third-party single-threaded EventLoop,
/// reached through a `FixedThreadDispatcher`.
///
/// `attach` posts an initialization closure that reports `ThreadID.current`
/// from the target thread; the executor is published only after that
/// handshake, so `threadID` is always valid. The executor neither owns nor
/// stops the host thread and provides no shutdown.
public final class AttachedThreadExecutor: ThreadBoundExecutor {
    private let dispatcher: any FixedThreadDispatcher
    private let state = AttachedThreadState()

    private init(dispatcher: any FixedThreadDispatcher) {
        self.dispatcher = dispatcher
    }

    /// Attaches to the dispatcher's fixed thread. Never blocks the target
    /// thread: the handshake closure is posted and awaited from the
    /// attaching task.
    public static func attach(
        to dispatcher: any FixedThreadDispatcher
    ) async -> AttachedThreadExecutor {
        let executor = AttachedThreadExecutor(dispatcher: dispatcher)
        dispatcher.post { [state = executor.state] in
            state.fulfill(ThreadID.current)
        }
        _ = await executor.state.waitForThreadID()
        return executor
    }

    public var threadID: ThreadID {
        state.threadID
    }

    public func enqueue(_ job: consuming ExecutorJob) {
        let pending = UnownedJob(job)
        dispatcher.post { [self] in
            runJob(pending)
        }
    }
}

/// Executor whose jobs run on the process main thread through
/// `DispatchQueue.main`.
///
/// - `shared` performs a non-blocking handshake: a block posted to the main
///   queue reports `ThreadID.current` from the main thread before the
///   instance is published.
/// - Requires a main queue that is actually being serviced (GUI run loop,
///   `dispatchMain()`); awaiting `shared` from a thread that is itself
///   keeping the main queue unserviced (e.g. a blocked main thread) will
///   not complete.
/// - `@MainActor` keeps its own official isolation semantics; sharing the
///   thread does not make this executor the same Executor identity as the
///   system `MainActor`'s. The main thread belongs to the application:
///   this executor provides no shutdown.
public final class MainThreadExecutor: ThreadBoundExecutor {
    private let state = AttachedThreadState()

    private init() {}

    /// The process-wide instance, bound to the main thread.
    public static var shared: MainThreadExecutor {
        get async {
            await withCheckedContinuation { continuation in
                let mustBootstrap = MainThreadSharedState.shared.awaitShared(continuation)
                if mustBootstrap {
                    Task {
                        let executor = MainThreadExecutor()
                        await executor.performHandshake()
                        MainThreadSharedState.shared.publish(executor)
                    }
                }
            }
        }
    }

    /// Posts the TID probe to the main queue and awaits its result from the
    /// calling task — the main thread is never blocked by this library.
    private func performHandshake() async {
        DispatchQueue.main.async { [state] in
            state.fulfill(ThreadID.current)
        }
        _ = await state.waitForThreadID()
    }

    public var threadID: ThreadID {
        state.threadID
    }

    public func enqueue(_ job: consuming ExecutorJob) {
        let pending = UnownedJob(job)
        DispatchQueue.main.async { [self] in
            runJob(pending)
        }
    }
}

/// Single-flight bootstrap for `MainThreadExecutor.shared`: the first
/// caller performs the main-queue handshake while later callers park until
/// the instance is published.
private final class MainThreadSharedState: @unchecked Sendable {
    static let shared = MainThreadSharedState()

    private let lock = NSLock()
    private var instance: MainThreadExecutor?
    private var waiters: [CheckedContinuation<MainThreadExecutor, Never>] = []

    /// Hands back the instance when ready; otherwise parks `continuation`
    /// and reports whether the caller must run the bootstrap.
    func awaitShared(_ continuation: CheckedContinuation<MainThreadExecutor, Never>) -> Bool {
        lock.lock()
        if let instance {
            lock.unlock()
            continuation.resume(returning: instance)
            return false
        }
        waiters.append(continuation)
        let mustBootstrap = waiters.count == 1
        lock.unlock()
        return mustBootstrap
    }

    /// Publishes the bootstrapped instance and wakes all parked callers.
    func publish(_ executor: MainThreadExecutor) {
        lock.lock()
        instance = executor
        let parked = waiters
        waiters = []
        lock.unlock()
        for waiter in parked {
            waiter.resume(returning: executor)
        }
    }
}
