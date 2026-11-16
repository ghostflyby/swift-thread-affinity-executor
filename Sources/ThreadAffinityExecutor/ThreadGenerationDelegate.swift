//
//  ThreadGenerationDelegate.swift
//  ThreadAffinityExecutor
//
//  One generation's thread environment and delivery: a
//  `FixedThreadDispatcher` that also owns its thread's lifecycle hooks.
//

/// One generation's pump and thread environment in one object: the
/// conformer delivers the generation's handshake and service installation
/// through `post`, and its hooks run on that same thread — `initialize`
/// before the generation's first job, `finalize` after its last.
///
/// Cross-generation state travels through whatever the migrator shares
/// between the two pump objects; the protocol itself is per generation.
public protocol ThreadGenerationDelegate: FixedThreadDispatcher {
  /// On the generation's thread, before its first job. `epoch == 0` marks
  /// the first generation. Throwing fails the attach/migration: that
  /// generation is never published (a failed successor leaves the current
  /// generation serving).
  func initialize(_ generation: MigratingThreadExecutor.Generation) throws

  /// On the generation's thread, after its last job — before the next
  /// handoff's successor opens, or a final termination. Transactional
  /// handoffs run this after the successor's `initialize`.
  func finalize(_ generation: MigratingThreadExecutor.Generation)
}

extension ThreadGenerationDelegate {
  public func initialize(_ generation: MigratingThreadExecutor.Generation) throws {}

  public func finalize(_ generation: MigratingThreadExecutor.Generation) {}
}

/// Closure sugar: generation hooks over an existing dispatcher.
public struct ClosureGenerationDelegate: ThreadGenerationDelegate {
  private let pump: any FixedThreadDispatcher
  private let initializeHook: @Sendable (MigratingThreadExecutor.Generation) throws -> Void
  private let finalizeHook: @Sendable (MigratingThreadExecutor.Generation) -> Void

  public init(
    to pump: any FixedThreadDispatcher,
    initialize: @escaping @Sendable (MigratingThreadExecutor.Generation) throws -> Void = { _ in },
    finalize: @escaping @Sendable (MigratingThreadExecutor.Generation) -> Void = { _ in }
  ) {
    self.pump = pump
    self.initializeHook = initialize
    self.finalizeHook = finalize
  }

  public func post(_ work: @escaping @Sendable () -> Void) {
    pump.post(work)
  }

  public func initialize(_ generation: MigratingThreadExecutor.Generation) throws {
    try initializeHook(generation)
  }

  public func finalize(_ generation: MigratingThreadExecutor.Generation) {
    finalizeHook(generation)
  }
}
