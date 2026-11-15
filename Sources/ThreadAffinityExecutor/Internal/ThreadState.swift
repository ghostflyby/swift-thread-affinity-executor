//
//  ThreadState.swift
//  ThreadAffinityExecutor
//
//  Handshake state for attached executors: the target thread
//  publishes its `ThreadID` exactly once via the dispatcher's `post`
//  entry; `attach` awaits it without ever blocking the host thread.
//

import Foundation

final class AttachedThreadState: @unchecked Sendable {
    private let lock = NSLock()
    private var threadIDStorage: ThreadID?
    private var continuation: CheckedContinuation<ThreadID, Never>?

    /// Called on the target thread through a posted closure.
    ///
    /// Always stores the ID: the `threadID` accessor must stay valid for the
    /// executor's whole lifetime, not just for the attaching task.
    func fulfill(_ id: ThreadID) {
        lock.lock()
        threadIDStorage = id
        let pending = continuation
        continuation = nil
        lock.unlock()
        pending?.resume(returning: id)
    }

    /// Called by the attaching task; never blocks the target thread.
    ///
    /// Single-slot by design: exactly one attach path awaits the published
    /// ID. A second concurrent waiter traps instead of being silently
    /// dropped on the floor.
    func waitForThreadID() async -> ThreadID {
        await withCheckedContinuation { continuation in
            lock.lock()
            if let threadIDStorage {
                lock.unlock()
                continuation.resume(returning: threadIDStorage)
            } else {
                precondition(
                    self.continuation == nil,
                    "a second waiter registered on the single-slot AttachedThreadState"
                )
                self.continuation = continuation
                lock.unlock()
            }
        }
    }

    /// The published thread ID. Traps before publication completed —
    /// executors only become reachable once `attach` awaited it.
    var threadID: ThreadID {
        lock.lock()
        defer { lock.unlock() }
        guard let threadIDStorage else {
            preconditionFailure("ThreadID read before the attached thread published it")
        }
        return threadIDStorage
    }
}
