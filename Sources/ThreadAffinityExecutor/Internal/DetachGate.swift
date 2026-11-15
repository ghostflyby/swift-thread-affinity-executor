//
//  DetachGate.swift
//  ThreadAffinityExecutor
//
//  Attach-termination gate: one caller claims the detach; claimers and
//  late arrivals join on the posted finalize's completion.
//

import Foundation

final class DetachGate: @unchecked Sendable {
  private let condition = NSCondition()
  private var claimed = false
  private var done = false

  /// Returns false when another caller already claimed the detach.
  func claim() -> Bool {
    condition.lock()
    defer { condition.unlock() }
    if claimed {
      return false
    }
    claimed = true
    return true
  }

  var isClaimed: Bool {
    condition.lock()
    defer { condition.unlock() }
    return claimed
  }

  /// Called from the posted closure after finalize completed.
  func signalDone() {
    condition.lock()
    done = true
    condition.unlock()
    condition.broadcast()
  }

  /// Blocks until `signalDone`.
  func waitDone() {
    condition.lock()
    while !done {
      condition.wait()
    }
    condition.unlock()
  }
}
