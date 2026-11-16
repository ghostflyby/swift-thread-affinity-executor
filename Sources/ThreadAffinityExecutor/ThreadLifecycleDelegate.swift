//
//  ThreadLifecycleDelegate.swift
//  ThreadAffinityExecutor
//
//  Shared configuration entry for thread lifecycle hooks: one delegate
//  object drives every executor family instead of loose closure pairs.
//

/// Thread lifecycle hooks, running on the bound thread at moments the
/// library controls. One delegate instance replaces the former
/// `initialize:`/`finalize:` closure pairs across the executor families.
public protocol ThreadLifecycleDelegate: Sendable {
  /// On the bound thread, before the first job. Throwing prevents
  /// publication: the executor (or attachment) is never handed out.
  func initialize() throws

  /// On the bound thread, at the controllable exit point: `shutdown()`
  /// for a dedicated executor, `detach()` for an attachment, a pump's
  /// `run()` exit for bare pump usage. Runs after all accepted work; a
  /// modality without such a point never runs it, and an attachment whose
  /// pump rejects the finalize post does not run it either (`detach()`
  /// returns `false`).
  func finalize()
}

extension ThreadLifecycleDelegate {
  public func initialize() throws {}

  public func finalize() {}
}

/// The attach-family delegate: a pump's delivery plus its thread's
/// lifecycle — `post` is the additional entry the attach side needs.
///
/// A pump serves one attachment: the lifecycle is the pump's, not the
/// attachment's — a second attachment on the same pump re-runs
/// `initialize` and shares the single once-fired `finalize`.
public protocol ThreadPumpDelegate: ThreadLifecycleDelegate, FixedThreadDispatcher {}

/// Closure sugar over `ThreadLifecycleDelegate`.
public struct ClosureLifecycle: ThreadLifecycleDelegate {
  private let initializeHook: @Sendable () throws -> Void
  private let finalizeHook: @Sendable () -> Void

  /// The first (trailing) closure is `initialize` — a teardown-only
  /// lifecycle must label it explicitly: `ClosureLifecycle(finalize:)`.
  public init(
    initialize: @escaping @Sendable () throws -> Void = {},
    finalize: @escaping @Sendable () -> Void = {}
  ) {
    self.initializeHook = initialize
    self.finalizeHook = finalize
  }

  public func initialize() throws {
    try initializeHook()
  }

  public func finalize() {
    finalizeHook()
  }
}
