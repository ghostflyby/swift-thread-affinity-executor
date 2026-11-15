//
//  ThreadIDTests.swift
//  ThreadAffinityExecutorTests
//

import Foundation
import Testing

@testable import ThreadAffinityExecutor

@Test func threadIDIsStableOnCallingThread() {
  let first = ThreadID.current
  let second = ThreadID.current
  #expect(first == second)
  #expect(first.rawValue != 0)
}

@Test func threadIDDiffersAcrossThreads() throws {
  let callerID = ThreadID.current
  let box = TestBox<ThreadID?>(nil)
  let semaphore = DispatchSemaphore(value: 0)

  let thread = Thread {
    box.set(ThreadID.current)
    semaphore.signal()
  }
  thread.start()
  semaphore.wait()

  let other = try #require(box.get())
  #expect(other != callerID)
}

@Test func threadIDDescriptionContainsIdentifier() {
  let id = ThreadID.current
  #expect(id.description.contains(String(id.rawValue)))
}

@Test func threadIDEqualityFollowsValueSemantics() {
  let a = ThreadID.current
  let b = ThreadID.current
  let set: Set<ThreadID> = [a, b]
  #expect(set.count == 1)
}
