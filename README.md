# ThreadAffinityExecutor

Thread-affinity executors for Swift Concurrency: every job accepted by one of these executors runs on the same OS thread for the executor's whole lifetime. The bound thread can be created by the library, provided by the caller, or borrowed from an existing fixed-thread mechanism.

## Scope

| Type | Notes | Status |
| --- | --- | --- |
| `BlockingQueue` (separate library product) | Split-endpoint blocking FIFO queues (`Queue` unbounded / `BoundedQueue` fixed capacity); endpoint shape fixed at split time — `split(.spsc)` / `.mpsc` / `.spmc` / `.mpmc`; elements cross by ownership transfer (`sending`) | ✅ |
| `LockFreeMPSCQueue` (in `BlockingQueue`) | Lock-free unbounded MPSC (`split(.mpsc)`, or the move-only `.spsc` view); one node per element, atomic enqueue/`tryTake`, condition-parked blocking `take`; exclusive consumer, no `drainAll` | ✅ |
| `ThreadID` | Cross-platform OS thread ID value type | ✅ |
| `ThreadBoundExecutor` | `SerialExecutor + TaskExecutor` protocol bound to one OS thread | ✅ |
| `DedicatedThreadExecutor` | Dedicated `Foundation.Thread` with optional on-thread `initialize`/`finalize` | ✅ |
| `AttachedThreadExecutor` + `FixedThreadDispatcher` | Attaches to any fixed thread exposing a `post` entry (main queue via `MainQueueDispatcher` — requires the main thread to service the main queue through a run loop; RunLoop hosts, event loops); `detach()` terminates | ✅ |
| `QueueDispatcher` | Library-provided pump for threads without an event loop; pair with `attachOnCurrentThread(to:)` | ✅ |
| `MigratingThreadExecutor` + `ThreadGenerationDelegate` | One executor identity across bound-thread generations (`migrate`/`handOver`); actors never rebind | 🔶 prototype |
| Shutdown scopes / task tracking | Full quiescence machinery | ⏳ explicit quiescence contract for now |

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

### Queue endpoint shapes

`Queue` and `BoundedQueue` fix the endpoint shape when the construction
token is split. Exclusive ends have no `share()` upgrade, so the shape is a
static guarantee:

| Shape | Split call | Producer | Consumer |
| --- | --- | --- | --- |
| SPSC | `split(.spsc)` | `QueueProducer` / `BoundedQueueProducer` | `QueueConsumer` |
| MPSC | `split(.mpsc)` | `SharedQueueProducer` / `SharedBoundedQueueProducer` | `QueueConsumer` |
| SPMC | `split(.spmc)` | `QueueProducer` / `BoundedQueueProducer` | `SharedQueueConsumer` |
| MPMC | `split(.mpmc)` | `SharedQueueProducer` / `SharedBoundedQueueProducer` | `SharedQueueConsumer` |

`LockFreeMPSCQueue` has its own endpoints: `split(.mpsc)` returns the
cloneable `LockFreeMPSCQueueProducer` and the exclusive
`LockFreeMPSCQueueConsumer`; `split(.spsc)` returns
`LockFreeMPSCQueueExclusiveProducer`, a move-only view that holds the shared
producer reference without exposing it, together with the same consumer and
supervisor.

```swift
import BlockingQueue

let ends = Queue<Int>().split(.spsc)
ends.producer.enqueue(1)
ends.producer.finish()

let first = ends.consumer.take() // 1
let drained = ends.consumer.take() // nil

// Bounded MPSC: the producer end is a cloneable, Sendable class reference.
let bounded = BoundedQueue<Int>(capacity: 8).split(.mpsc)
let producer = bounded.producer
producer.enqueue(2)
producer.close()
```

Migrating from the previous split API:

- `queue.split()` → `queue.split(.spsc)`.
- The previous split-result types `QueueEnds` / `BoundedQueueEnds` are now
  the generic `QueueEnds` bundle; for the `.spsc` shape use
  `SPSCQueueEnds` / `BoundedSPSCQueueEnds`.
- `split()` followed by `producer.share()` → construct with `split(.mpsc)`;
  use `split(.mpmc)` when the consumer side is shared as well.
- `split()` followed by `consumer.share()` → `split(.spmc)` /
  `split(.mpmc)`.
- `finish()` and `close()` are unchanged.

### Lock-free MPSC

`LockFreeMPSCQueue` is an unbounded multi-producer / single-consumer queue
backed by an intrusive singly-linked list rather than a lock. Producers
publish nodes through an atomic tail exchange; the one consumer end advances
a stub head, moves the element out and frees the node. Elements cross the
same `consuming sending` / `sending` ownership boundary as in the lock-based
queue, so they need not be `Sendable`.

```swift
import BlockingQueue

let ends = LockFreeMPSCQueue<Int>().split(.mpsc)
let producer = ends.producer
let consumer = ends.consumer

producer.enqueue(1)  // never waits; the queue is unbounded

let first = consumer.tryTake()  // 1, or nil when currently empty
producer.close()

while let value = consumer.take() {  // blocks; nil after close + drain
  print(value)
}
```

`tryTake()` is the non-blocking dequeue; its `nil` means "currently empty".
An empty observation of a closed queue seals it only when no producer is in
flight; while an in-flight producer exists, `nil` is not terminal and a
non-blocking drain must continue until `supervisor.isSealed`. `take()` is the
blocking shutdown path: it parks on a condition variable until an element
arrives or the terminal state is observed, and a producer takes that condition
only when a consumer is parked — the data path stays lock-free. `close()`
racing with `enqueue` is linearized: an element accepted by `enqueue` is
delivered while a consumer remains, and the queue does not seal around an
in-flight producer; releasing the consumer end waits for in-flight producers
before `isSealed` becomes true (an enqueue accepted before the release whose
link lands after it is stranded, since no taker remains). `split(.mpsc)`
returns a `sending` bundle: the producer and supervisor are `Sendable` and can
be shared, while the consumer is non-`Sendable` and must stay in the domain it
was handed to. `split(.spsc)` returns the same consumer and supervisor with a
move-only `LockFreeMPSCQueueExclusiveProducer`, so the single-producer shape
does not expose the shared class reference. The cell additionally traps on
concurrent consumer entry. The supervisor exposes no `drainAll`; drain through
the consumer.

### Capability protocols

The lock-based and lock-free queues expose their capabilities through one
protocol set, so generic code can accept either implementation:

| Protocol | Requirement |
| --- | --- |
| `QueueProducerProtocol<Element>` | non-blocking `enqueue` |
| `BlockingQueueProducerProtocol<Element>` | `enqueueBlocking` (bounded producers only) |
| `QueueConsumerProtocol<Element>` | blocking `take` |
| `TryQueueConsumerProtocol<Element>` | non-blocking `tryTake` (lock-free consumer only) |
| `QueueLifecycleProtocol` | `close` / `isClosed` / `isSealed` / `waitUntilSealed` |
| `QueueInspectionProtocol<Element>` | `count` |
| `QueueDrainProtocol<Element>` | `drainAll` (lock-based supervisor only) |
| `SingleProducerProtocol<Element>` | producer end usable as the only producer |
| `MultiProducerProtocol<Element>` | shareable producer end; refines `SingleProducerProtocol` |
| `SingleConsumerProtocol<Element>` | consumer end usable as the only consumer |
| `MultiConsumerProtocol<Element>` | shareable consumer end; refines `SingleConsumerProtocol` |
| `SPSCQueueProtocol<Element>` | `split(.spsc)`; base category |
| `MPSCQueueProtocol<Element>` | refines SPSC; `split(.mpsc)` |
| `SPMCQueueProtocol<Element>` | refines SPSC; `split(.spmc)` |
| `MPMCQueueProtocol<Element>` | refines MPSC and SPMC; `split(.mpmc)` |

The category protocols form a lattice:

```
            MPMC
           /    \
        MPSC    SPMC
           \    /
            SPSC
```

`Queue` and `BoundedQueue` declare only `MPMCQueueProtocol`;
`LockFreeMPSCQueue` declares `MPSCQueueProtocol`, so it is also usable
wherever `SPSCQueueProtocol` is required. Its `split(.spsc)` returns the
move-only `LockFreeMPSCQueueExclusiveProducer` wrapper over the shared
reference, so SPSC generic code cannot clone it and the shared class is not
exposed. A generic function can require a category and call that `split`
directly:

```swift
func drive<Q: MPSCQueueProtocol & ~Copyable>(_ queue: consuming Q)
where Q.Element == Int {
  let ends = queue.split(.mpsc)
  ends.producer.enqueue(1)
  ends.consumer.take()
}
```

The endpoint refinement (`Multi…: Single…`) makes shared endpoint types
eligible for the inherited single-sided constraints; the lock-free token
additionally wraps its shared producer for the concrete `.spsc` shape. The
category protocols use prefixed associated types (`SPSCProducer`,
`MPSCProducer`, …) because one token implements several categories; their
conformances are declared inside the token type declarations so move-only
element types are supported. `Queue` is a multi-shape construction token, so
a category constraint describes what it can be split into, not an exclusive
identity.

`QueueEnds<P, C, S>` composes a producer, consumer and supervisor with one
shared element type. Its initializer is public, so another queue family can
build the bundle and conform to the category protocols. The existing ends
names — `SPSCQueueEnds`, `BoundedMPSCQueueEnds`, `LockFreeMPSCQueueEnds`,
`LockFreeSPSCQueueEnds`, … — are aliases of that generic bundle. The lock-free
consumer is non-`Sendable`, so both lock-free ends bundles are non-`Sendable`;
the `.mpsc` producer and supervisor remain `Sendable`, and the `.spsc`
producer is the move-only exclusive wrapper.

## Build / test

```bash
swift build
swift test
```

Requires a Swift 6.4+ toolchain. Deployment floors: macOS 15 / iOS 18 / tvOS 18 / watchOS 11 / visionOS 2. Library products: `ThreadAffinityExecutor`, `BlockingQueue`. Linux and Windows are built and tested in CI: the dedicated executor owns a Foundation `Thread` instead of subclassing it, and Linux resolves `gettid` directly from libc.

## Known tradeoffs

- `Foundation.Thread.start()` has no failure return; if the thread cannot be spawned (resource exhaustion) the synchronous initializer blocks.
- An executor attached to the main thread via `MainQueueDispatcher` shares the thread with `MainActor` but is not the same executor identity and does not replace it.
- `MigratingThreadExecutor.migrate(to:)` and `handOver(to:)` require a successor pump whose fixed thread or queue is independent of the currently serving pump — a different wrapper around the same thread or queue still deadlocks. The handshake post queues behind the old serving loop, which exits only after retarget seals its queue, and retarget waits for the handshake. The handshake itself no longer blocks the successor's thread while the old generation drains; the serving loop is posted after the old finalize.
- `FixedThreadDispatcher.post` returns `false` once the pump stops accepting work. Attachment and migration handshakes surface that as `AttachError.pumpUnavailable` / `MigrationError.pumpUnavailable`; `detach()` returns `false` when the finalize post is rejected instead of blocking forever. A pump that accepts a post but never runs it still leaves the join pending.
- `LockFreeMPSCQueue` allocates a node plus its element storage per enqueued element and keeps them until the consumer takes it (or the queue is released). A blocking `take()` parks on an `NSCondition`; producers take that lock only when a consumer is parked, so enqueue is lock-free in the common case but a parked consumer adds a mutex-based wake path. Its supervisor has no `drainAll`: the consumer end is exclusive and non-`Sendable`, and a second taker would break the single-consumer head advance. The consumer cell traps on concurrent `take()`/`tryTake()` entry, so a shared holder cannot silently double-free the head.

## License

Apache 2.0 — see [LICENSE](LICENSE).
