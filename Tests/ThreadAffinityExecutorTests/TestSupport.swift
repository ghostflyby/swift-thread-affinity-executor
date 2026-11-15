//
//  TestSupport.swift
//  ThreadAffinityExecutorTests
//

import Foundation
@testable import ThreadAffinityExecutor

/// A tiny mutex-guarded box for synchronizing test fixtures.
final class TestBox<T>: @unchecked Sendable {
    private let lock = NSLock()
    private var value: T

    init(_ value: T) {
        self.value = value
    }

    func get() -> T {
        lock.lock()
        defer { lock.unlock() }
        return value
    }

    func set(_ newValue: T) {
        lock.lock()
        defer { lock.unlock() }
        value = newValue
    }

    func with<R>(_ body: (inout T) -> R) -> R {
        lock.lock()
        defer { lock.unlock() }
        return body(&value)
    }
}

/// Thread-safe counter that also records which threads performed the work.
final class ThreadSafeCounter: @unchecked Sendable {
    private let lock = NSLock()
    private var count = 0

    var value: Int {
        lock.lock()
        defer { lock.unlock() }
        return count
    }

    func increment(recording threads: TestBox<Set<ThreadID>>) {
        lock.lock()
        count += 1
        lock.unlock()
        threads.with { $0.insert(ThreadID.current) }
    }
}
