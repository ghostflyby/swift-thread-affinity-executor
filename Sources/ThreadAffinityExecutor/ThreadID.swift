//
//  ThreadID.swift
//  ThreadAffinityExecutor
//
//  Cross-platform identifier of an OS thread of the current process.
//

#if canImport(Darwin)
  import Darwin
#elseif canImport(Glibc)
  import Glibc
#elseif canImport(Musl)
  import Musl
#elseif canImport(WinSDK)
  import WinSDK
#endif

/// A value that identifies an OS thread **of this process, while that thread
/// is alive**.
///
/// Semantics:
///
/// - `current` is read synchronously on the calling thread through the
///   platform's TID API; `Foundation.Thread` exposes no unified numeric OS
///   thread ID, which is why this value type exists.
/// - Identity comparison is only meaningful while the identified thread is
///   alive: the kernel may recycle numeric thread IDs after a thread exits,
///   so no stable "historical identity" is provided for dead threads.
/// - `rawValue` is internal on purpose; it is a platform numeric ID and must
///   not be used as a long-lived unique key.
public struct ThreadID: Sendable, Hashable, CustomStringConvertible {
  /// Platform numeric thread identifier (Darwin `pthread_threadid_np`
  /// value, Linux kernel TID, Windows thread ID).
  let rawValue: UInt64

  init(rawValue: UInt64) {
    self.rawValue = rawValue
  }

  /// The thread ID of the calling thread.
  public static var current: ThreadID {
    #if canImport(Darwin)
      var identifier: UInt64 = 0
      let status = pthread_threadid_np(nil, &identifier)
      precondition(status == 0, "pthread_threadid_np failed with status \(status)")
      return ThreadID(rawValue: identifier)
    #elseif canImport(Glibc) || canImport(Musl)
      // Kernel TID of the calling thread (glibc >= 2.30, musl >= 1.2.1).
      return ThreadID(rawValue: UInt64(Int64(gettid())))
    #elseif canImport(WinSDK)
      return ThreadID(rawValue: UInt64(GetCurrentThreadId()))
    #endif
  }

  public var description: String {
    "ThreadID(\(rawValue))"
  }
}
