//
//  SplitOptions.swift
//  BlockingQueue
//
//  Compile-time shape selectors for `split`. The option value is never read;
//  each selector type carries a uniquely named constant so that
//  `queue.split(.mpsc)` resolves through overload resolution into the ends
//  struct with the matching endpoint types. Shapes are fixed at split time:
//  exclusive ends have no `share()` upgrade, so SPSC/MPSC/SPMC/MPMC are
//  static guarantees rather than conventions.
//

/// Selects single-producer, single-consumer ends: the exclusive SPSC ends.
public struct SPSCOption: Sendable {
  private init() {}

  /// The `split(.spsc)` selector.
  public static let spsc = SPSCOption()
}

/// Selects multiple-producer, single-consumer ends: a cloneable producer
/// side and an exclusive consumer.
public struct MPSCOption: Sendable {
  private init() {}

  /// The `split(.mpsc)` selector.
  public static let mpsc = MPSCOption()
}

/// Selects single-producer, multiple-consumer ends: an exclusive producer
/// and a cloneable consumer side whose takers compete per element.
public struct SPMCOption: Sendable {
  private init() {}

  /// The `split(.spmc)` selector.
  public static let spmc = SPMCOption()
}

/// Selects multiple-producer, multiple-consumer ends: both sides cloneable.
public struct MPMCOption: Sendable {
  private init() {}

  /// The `split(.mpmc)` selector.
  public static let mpmc = MPMCOption()
}
