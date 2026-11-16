//
//  DetachGate.swift
//  ThreadAffinityExecutor
//
//  Attach-termination gate: one caller claims the detach; claimers and
//  late arrivals join on the posted finalize's completion.
//
//  `claimed` and `outcome` are deliberately two domains: the atomic flag
//  gates new enqueues on the hot path, while the condition only carries
//  the finalize outcome to the detach callers. No data is published
//  through `claimed`; its acquire/release ordering is defensive.
//

import Foundation
import Synchronization

final class DetachGate: @unchecked Sendable {
  private let condition = NSCondition()
  private let claimed = Atomic<Bool>(false)
  /// `nil` while the claimed finalize is in flight; `true` when it ran;
  /// `false` when the pump rejected the finalize post.
  private var outcome: Bool?

  /// Returns false when another caller already claimed the detach.
  func claim() -> Bool {
    claimed.compareExchange(
      expected: false,
      desired: true,
      ordering: .acquiringAndReleasing
    ).exchanged
  }

  var isClaimed: Bool {
    claimed.load(ordering: .acquiring)
  }

  /// Called from the posted closure after finalize completed, or by the
  /// claimant when the pump rejected the post.
  func signalDone(_ succeeded: Bool) {
    condition.lock()
    if outcome == nil {
      outcome = succeeded
    }
    condition.unlock()
    condition.broadcast()
  }

  /// Blocks until the claimed detach settles and reports whether the
  /// posted finalize actually ran.
  func waitDone() -> Bool {
    condition.lock()
    while outcome == nil {
      condition.wait()
    }
    let result = outcome!
    condition.unlock()
    return result
  }
}
