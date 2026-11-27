# ThreadAffinityExecutor

Thread-affinity executors for Swift Concurrency: at any moment, every job accepted by one of these executors runs on one known OS thread. A `ThreadBoundExecutor` keeps that thread for its whole lifetime; a `MigratingThreadExecutor` changes it only through an explicit migration. The bound thread is created by the library.

## Scope

| Type | Notes | Status |
| --- | --- | --- |
| `BlockingQueue` (separate library product) | Split-endpoint FIFO queues: `UnboundedQueue` (grows) and `BoundedQueue` (fixed capacity); shape chosen by `split(.spsc)` / `.mpsc` / `.spmc` / `.mpmc`; elements cross by ownership (`sending`) | ✅ |
| `LinkedQueue` (in `BlockingQueue`) | Unbounded MPSC queue; `split(.mpsc)` (cloneable producer) or `split(.spsc)` (move-only producer) | ✅ |
| `ThreadID` | Cross-platform OS thread ID value type | ✅ |
| `ThreadAffinityExecutor` | Common protocol of the executor families: jobs run on one known OS thread at any moment (`currentThreadID`) | ✅ |
| `ThreadBoundExecutor` | `ThreadAffinityExecutor` bound to one OS thread for its whole lifetime (`SerialExecutor + TaskExecutor`) | ✅ |
| `RunLoopServicingExecutor` | Refines `ThreadBoundExecutor`: the bound thread also services a run loop | ✅ |
| `DedicatedThreadExecutor` | Dedicated OS thread | ✅ |
| `RunLoopThreadExecutor` | Dedicated OS thread whose idle wait is a `CFRunLoop`; jobs arrive through a run-loop source (CoreFoundation + Dispatch platforms) | ✅ |
| `MigratingThreadExecutor` | One executor over a sequence of `ThreadBoundExecutor`s, exchanged through explicit migrations; bound executors are borrowed | 🔶 prototype |
| Shutdown scopes / task tracking | Full quiescence machinery | ⏳ quiescence contract only |

## Contract

> **Quiescence contract**: before `shutdown()`, no actor, task or external producer may submit further jobs. Queued work drains first; jobs arriving after the termination request are dropped.

## Quick start

### Dedicated thread

```swift
import ThreadAffinityExecutor

let executor = DedicatedThreadExecutor(name: "worker")

actor Counter {
    nonisolated let executor: DedicatedThreadExecutor
    private var value = 0

    init(executor: DedicatedThreadExecutor) { self.executor = executor }

    nonisolated var unownedExecutor: UnownedSerialExecutor {
        executor.asUnownedSerialExecutor()
    }

    func next() -> Int {
        value += 1
        return value
    }
}

let counter = Counter(executor: executor)
print(await counter.next()) // runs on the dedicated thread

// after quiescence:
await executor.shutdown() // suspends until the queue drained and sealed; idempotent; never await on the bound thread
// the synchronous shutdown() blocks instead, for synchronous contexts
```

### Dedicated thread with a run loop

`RunLoopThreadExecutor` is `DedicatedThreadExecutor` with the wait replaced
by the thread's `CFRunLoop`: the worker sits in `CFRunLoopRun`, and jobs reach
it through a version-0 run-loop source. `enqueue` first puts the job in the
queue, then signals the source and wakes the loop, so an idle executor reacts
immediately instead of on a poll interval. Timers, run-loop sources and
`perform` blocks registered on the bound thread run between job batches; the
loop is serviced in the default mode.

```swift
import Foundation
import ThreadAffinityExecutor

// `RunLoop.current` is synchronous-only; schedule from a sync helper.
func scheduleHeartbeat() {
    let timer = Timer(timeInterval: 0.1, repeats: true) { _ in
        // Runs on the bound thread, between jobs.
    }
    RunLoop.current.add(timer, forMode: .default)
}

let executor = RunLoopThreadExecutor(name: "run-loop-worker")

await withTaskExecutorPreference(executor) {
    // On the bound thread: the same run loop the executor services.
    scheduleHeartbeat()
}

// after quiescence:
await executor.shutdown()
```

A job must not block waiting for work that only the bound run loop can
deliver; it may pump a nested run loop, but that nested loop never re-enters
job execution. Run-loop callbacks are not jobs: the isolation check is
thread-based and reports `true` in them, and a callback dispatched while a job
pumps a nested loop is re-entrant with that job; callbacks must not assume
executor-bound actor state is quiescent. The thread is not the main thread,
so main-run-loop APIs are unaffected.

`RunLoopServicingExecutor` is the capability refinement: generic code that
requires a run loop takes `any RunLoopServicingExecutor`, while plain thread
affinity stays `any ThreadBoundExecutor`. `DedicatedThreadExecutor` and
`MigratingThreadExecutor` conform only to `ThreadBoundExecutor` — a migration
may switch to a bound executor without a run loop. The type requires
CoreFoundation and Dispatch; it is absent where those cannot be imported.

### Migrating between bound executors

```swift
let first = DedicatedThreadExecutor(name: "first")
let second = DedicatedThreadExecutor(name: "second")

// routes to `first`; actors bound to `executor` keep their binding across migrations
let executor = MigratingThreadExecutor(bound: first)

// jobs accepted before the cut finish on `first`; later ones run on `second`
try await executor.migrate(to: second)

// bound executors are borrowed: each ends through its own exit, once idle
await first.shutdown()
await second.shutdown()
```

The two thread-context moments are posted as typed notifications on the
executor's own `NotificationCenter` (a fresh one per executor by default).
With no queue the observer runs synchronously on the bound thread and must
not block or await; a queue delivers asynchronously and the migration does
not wait:

```swift
let leaving = executor.addObserver(for: .leavingBoundThread) { event in
    // old thread, after the last job accepted before the cut, before the switch
}
executor.addObserver(for: .enteringBoundThread, queue: myQueue) { event in
    // delivered on myQueue; omit the queue for the new bound thread
}
leaving.remove()
```

The center also has a type-refined observer for direct use; the
`MigrationNotification` enum is the public identity and the underlying
`Notification.Name` stays internal:

```swift
executor.notificationCenter.addObserver(
    for: .leavingBoundThread, object: executor, queue: nil
) { event in
    // event is a MigrationEvent
}
```

Observers registered on the executor's center with the executor as object
are removed when it deinitializes; the executor retains their observer
tokens until then, and removing a registration releases its handler. Capture
the executor weakly in the handler.

### Endpoint shapes

`UnboundedQueue` and `BoundedQueue` fix the endpoint shape when the
construction token is split, so the chosen shape is a static guarantee:

| Shape | Split call | Producer | Consumer |
| --- | --- | --- | --- |
| SPSC | `split(.spsc)` | `UnboundedQueueProducer` / `BoundedQueueProducer` | `QueueConsumer` |
| MPSC | `split(.mpsc)` | `SharedUnboundedQueueProducer` / `SharedBoundedQueueProducer` | `QueueConsumer` |
| SPMC | `split(.spmc)` | `UnboundedQueueProducer` / `BoundedQueueProducer` | `SharedQueueConsumer` |
| MPMC | `split(.mpmc)` | `SharedUnboundedQueueProducer` / `SharedBoundedQueueProducer` | `SharedQueueConsumer` |

`LinkedQueue` has its own endpoints: `split(.mpsc)` returns the cloneable
`LinkedQueueProducer` and the exclusive `LinkedQueueConsumer`;
`split(.spsc)` returns the move-only `LinkedQueueExclusiveProducer` with the
same consumer and supervisor.

```swift
import BlockingQueue

let ends = UnboundedQueue<Int>().split(.spsc)
ends.producer.enqueue(1)
ends.producer.finish()

let first = ends.consumer.take() // 1
let drained = ends.consumer.take() // nil

// Bounded MPSC: the producer end is cloneable and Sendable.
let bounded = BoundedQueue<Int>(capacity: 8).split(.mpsc)
let producer = bounded.producer
producer.enqueue(2)
producer.close()
```

### Linked queue

`LinkedQueue` is an unbounded multi-producer / single-consumer queue.
Elements need not be `Sendable`: ownership transfers through the queue.

```swift
import BlockingQueue

let ends = LinkedQueue<Int>().split(.mpsc)
let producer = ends.producer
let consumer = ends.consumer

producer.enqueue(1)  // never waits; the queue is unbounded

let first = consumer.tryTake()  // 1, or nil when currently empty
producer.close()

while let value = consumer.take() {  // blocks; nil after close + drain
  print(value)
}
```

`tryTake()` never waits; `take()` blocks until an element arrives or the
queue is closed and drained. `nil` from `tryTake()` is not terminal while
producers are still running; drain until `supervisor.isSealed`. The consumer
end is exclusive and non-`Sendable`; drain through the consumer.

### Capability protocols

The queues share one protocol set, so generic code can accept any of them:

| Protocol | Requirement |
| --- | --- |
| `QueueProducerProtocol` / `BlockingQueueProducerProtocol` | `enqueue` / `enqueueBlocking` |
| `QueueConsumerProtocol` / `TryQueueConsumerProtocol` | `take` / `tryTake` |
| `QueueLifecycleProtocol` | `close`, `isClosed`, `isSealed`, `waitUntilSealed` |
| `QueueInspectionProtocol` | `count` |
| `SingleProducerProtocol` / `MultiProducerProtocol` | exclusive / shareable producer end |
| `SingleConsumerProtocol` / `MultiConsumerProtocol` | exclusive / shareable consumer end |
| `SPSCQueueProtocol` | `split(.spsc)` |
| `MPSCQueueProtocol` | refines SPSC; `split(.mpsc)` |
| `SPMCQueueProtocol` | refines SPSC; `split(.spmc)` |
| `MPMCQueueProtocol` | refines MPSC and SPMC; `split(.mpmc)` |

`UnboundedQueue` and `BoundedQueue` declare `MPMCQueueProtocol`;
`LinkedQueue` declares `MPSCQueueProtocol`, so it is also usable wherever
`SPSCQueueProtocol` is required. A generic function can require a category
and call that `split` directly:

```swift
func drive<Q: MPSCQueueProtocol & ~Copyable>(_ queue: consuming Q)
where Q.Element == Int {
  let ends = queue.split(.mpsc)
  ends.producer.enqueue(1)
  _ = ends.consumer.take()
}
```

`QueueEnds<P, C, S>` bundles a producer, consumer and supervisor. Its
initializer is public, so another queue family can conform to the category
protocols; the `UnboundedSPSCQueueEnds`, `BoundedMPSCQueueEnds`,
`LinkedMPSCQueueEnds`, … names are aliases of that bundle.

## Build / test

```bash
swift build
swift test
```

Requires a Swift 6.4+ toolchain. Deployment floors: macOS 15 / iOS 18 / tvOS 18 / watchOS 11 / visionOS 2. Library products: `ThreadAffinityExecutor`, `BlockingQueue`. Linux and Windows are built and tested in CI.

## License

Apache 2.0 — see [LICENSE](LICENSE).
