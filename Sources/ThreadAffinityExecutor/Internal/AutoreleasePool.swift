//
//  AutoreleasePool.swift
//  ThreadAffinityExecutor
//
//  Per-work-item autorelease pool for long-lived executor loops: on
//  platforms with the Objective-C runtime, one work item's autoreleased
//  objects are released before the next item starts. Elsewhere the body
//  runs directly.
//

#if canImport(ObjectiveC)
  import ObjectiveC
#endif

/// Runs `body` in a fresh autorelease pool so a long-lived drain loop does
/// not accumulate the autoreleased objects created by one work item until
/// the thread exits.
///
/// On platforms without the Objective-C runtime this is a plain call.
@inline(__always)
func withPerWorkAutoreleasePool<Result>(
  _ body: () throws -> Result
) rethrows -> Result {
  #if canImport(ObjectiveC)
    return try autoreleasepool(invoking: body)
  #else
    return try body()
  #endif
}
