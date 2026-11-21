//
//  SplitOptions.swift
//  BlockingQueue
//
//  Compile-time shape selectors for `split`. The option value is never read;
//  each selector type carries a uniquely named case so that
//  `queue.split(.mpsc)` resolves through overload resolution into the ends
//  struct with the matching endpoint types. Shapes are fixed at split time:
//  exclusive ends have no `share()` upgrade, so SPSC/MPSC/SPMC/MPMC are
//  static guarantees rather than conventions.
//

/// Selects single-producer, single-consumer ends: the exclusive SPSC ends.
public enum SPSCOption: Sendable {
  /// The `split(.spsc)` selector.
  case spsc
}

/// Selects multiple-producer, single-consumer ends: a cloneable producer
/// side and an exclusive consumer.
public enum MPSCOption: Sendable {
  /// The `split(.mpsc)` selector.
  case mpsc
}

/// Selects single-producer, multiple-consumer ends: an exclusive producer
/// and a cloneable consumer side whose takers compete per element.
public enum SPMCOption: Sendable {
  /// The `split(.spmc)` selector.
  case spmc
}

/// Selects multiple-producer, multiple-consumer ends: both sides cloneable.
public enum MPMCOption: Sendable {
  /// The `split(.mpmc)` selector.
  case mpmc
}
