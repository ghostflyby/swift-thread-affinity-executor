# ThreadAffinityExecutor

Thread-affinity executors for Swift Concurrency: at any moment, every job accepted by one of these executors runs on one known OS thread. A `ThreadBoundExecutor` keeps that thread for its whole lifetime; a `MigratingThreadExecutor` changes it only through an explicit migration. The bound thread can be created by the library, provided by the caller, or borrowed from an existing fixed-thread mechanism.

## Scope

| Type | Notes | Status |
| --- | --- | --- |
| `BlockingQueue` (separate library product) | Split-endpoint FIFO queues: `UnboundedQueue` (grows) and `BoundedQueue` (fixed capacity); shape chosen by `split(.spsc)` / `.mpsc` / `.spmc` / `.mpmc`; elements cross by ownership (`sending`) | ✅ |
| `LinkedQueue` (in `BlockingQueue`) | Unbounded MPSC queue; `split(.mpsc)` (cloneable producer) or `split(.spsc)` (move-only producer) | ✅ |
| `ThreadID` | Cross-platform OS thread ID value type | ✅ |
| `Affinity` | Common protocol of the executor families: jobs run on one known OS thread at any moment (`currentThreadID`) | ✅ |
| `ThreadBoundExecutor` | `Affinity` bound to one OS thread for its whole lifetime (`SerialExecutor + TaskExecutor`) | ✅ |
| `DedicatedThreadExecutor` | Dedicated OS thread with optional on-thread `initialize`/`finalize` | ✅ |
| `AttachedThreadExecutor` + `FixedThreadDispatcher` | Attach to any fixed thread with a `post` entry (main queue via `MainQueueDispatcher`, run loops, event loops); `detach()` terminates | ✅ |
| `QueueDispatcher` | Pump for threads without an event loop | ✅ |
| `MigratingThreadExecutor` | One executor over a sequence of `ThreadBoundExecutor`s, exchanged through explicit migrations; bound executors are borrowed | 🔶 prototype |
| Shutdown scopes / task tracking | Full quiescence machinery | ⏳ quiescence contract only |

## Contract

> **Quiescence contract**: before `shutdown()` or `detach()`, no actor, task or external producer may submit further jobs. Queued work drains first; jobs arriving after the termination request are dropped (`enqueue` returns `false`). The library never stops borrowed threads — detach an attachment before stopping its pump.

## Quick start

### Dedicated thread

```swift
import ThreadAffinityExecutor

// optional initialize/finalize run on the worker thread; an initialize error fails construction
let executor = try DedicatedThreadExecutor(name: "worker")

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
executor.shutdown() // blocks until the queue drained and sealed; idempotent; never call on the bound thread
// in an async context, `await executor.shutdown()` suspends instead of blocking
```

### Caller-driven pump (self-attach)

```swift
let pump = QueueDispatcher()
let executor = try AttachedThreadExecutor.attachOnCurrentThread(to: pump)

// after submitting work, occupy the calling thread until pump.stop() and a drained queue:
pump.run()
```

### Main thread / attached threads

```swift
// main thread (requires a serviced main queue, e.g. a GUI run loop or dispatchMain()):
let mainExecutor = try await AttachedThreadExecutor.attach(to: MainQueueDispatcher())

// any fixed-thread loop that provides a post entry:
let executor = try await AttachedThreadExecutor.attach(to: myEventLoopDispatcher)
```

### Migrating between bound executors

```swift
let first = try DedicatedThreadExecutor(name: "first")
let second = try DedicatedThreadExecutor(name: "second")

// routes to `first`; actors bound to `executor` keep their binding across migrations
let executor = MigratingThreadExecutor(bound: first)

// jobs accepted before the cut finish on `first`; later ones run on `second`
try await executor.migrate(to: second)

// bound executors are borrowed: each ends through its own exit, once idle
first.shutdown()
```

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
