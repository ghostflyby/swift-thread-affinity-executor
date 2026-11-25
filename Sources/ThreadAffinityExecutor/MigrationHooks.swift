//
//  MigrationHooks.swift
//  ThreadAffinityExecutor
//
//  Migration notification types.
//

import Foundation

/// One migration that reached the switch.
public struct MigrationEvent: Sendable, Equatable {
  /// One-based, counted at the switch.
  public let generation: UInt64
  /// The thread left.
  public let from: ThreadID
  /// The thread bound.
  public let to: ThreadID

  /// Internal: `userInfo` key for the event.
  static let userInfoKey = "ThreadAffinityExecutor.MigrationEvent"
}

/// A migration notification.
public enum MigrationNotification: Sendable, Hashable {
  /// Old bound thread: after the last pre-cut job, before the switch.
  case leavingBoundThread
  /// New bound thread: before the released jobs.
  case enteringBoundThread

  /// Internal: the underlying name.
  var notificationName: Notification.Name {
    switch self {
    case .leavingBoundThread:
      Notification.Name("ThreadAffinityExecutor.migration.leavingBoundThread")
    case .enteringBoundThread:
      Notification.Name("ThreadAffinityExecutor.migration.enteringBoundThread")
    }
  }
}

extension NotificationCenter {
  /// Observes a migration notification; a nil queue runs synchronously on
  /// the posting thread. With the executor as `object` on its own center,
  /// the observer is removed with it.
  @discardableResult
  public func addObserver(
    for notification: MigrationNotification,
    object: MigratingThreadExecutor? = nil,
    queue: OperationQueue? = nil,
    using handler: @escaping @Sendable (MigrationEvent) -> Void
  ) -> MigrationHookToken {
    let observer = addObserver(
      forName: notification.notificationName,
      object: object,
      queue: queue
    ) { raw in
      guard let event = raw.userInfo?[MigrationEvent.userInfoKey] as? MigrationEvent
      else {
        assertionFailure("migration notification without a MigrationEvent")
        return
      }
      handler(event)
    }
    let token = MigrationHookToken(center: self, observer: observer)
    // Only the executor's own center is cleaned up with the executor.
    if let object, self === object.notificationCenter {
      object.track(token)
    }
    return token
  }
}

/// One registered migration observer; removal is its only purpose.
///
/// `@unchecked Sendable`: the weak load is atomic, and the center's removal
/// is thread-safe.
public struct MigrationHookToken: @unchecked Sendable {
  weak var center: NotificationCenter?
  let observer: any NSObjectProtocol

  /// Removes the registration; idempotent, and a no-op after the center died.
  public func remove() {
    center?.removeObserver(observer)
  }
}

extension MigrationHookToken: Hashable {
  public static func == (lhs: Self, rhs: Self) -> Bool {
    ObjectIdentifier(lhs.observer as AnyObject) == ObjectIdentifier(rhs.observer as AnyObject)
  }

  public func hash(into hasher: inout Hasher) {
    hasher.combine(ObjectIdentifier(observer as AnyObject))
  }
}
