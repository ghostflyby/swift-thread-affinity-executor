# ThreadAffinityExecutor

Thread-affinity executors for Swift Concurrency: every job accepted by one of these executors runs on the same OS thread for the executor's whole lifetime. The bound thread can be created by the library, provided by the caller, or borrowed from an existing fixed-thread mechanism.

## Scope

| Type | Notes | Status |
| --- | --- | --- |
| `BlockingQueue` (separate library product) | Split-endpoint blocking FIFO queues (`Queue` unbounded / `BoundedQueue` fixed capacity); endpoint shape fixed at split time — `split(.spsc)` / `.mpsc` / `.spmc` / `.mpmc`; elements cross by ownership transfer (`sending`) | ✅ |
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

`BlockingQueue` fixes the endpoint shape when the construction token is
split. Exclusive ends have no `share()` upgrade, so the shape is a static
guarantee:

| Shape | Split call | Producer | Consumer |
| --- | --- | --- | --- |
| SPSC | `split(.spsc)` | `QueueProducer` / `BoundedQueueProducer` | `QueueConsumer` |
| MPSC | `split(.mpsc)` | `SharedQueueProducer` / `SharedBoundedQueueProducer` | `QueueConsumer` |
| SPMC | `split(.spmc)` | `QueueProducer` / `BoundedQueueProducer` | `SharedQueueConsumer` |
| MPMC | `split(.mpmc)` | `SharedQueueProducer` / `SharedBoundedQueueProducer` | `SharedQueueConsumer` |

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
- `QueueEnds` / `BoundedQueueEnds` → `SPSCQueueEnds` /
  `BoundedSPSCQueueEnds`.
- `split()` followed by `producer.share()` → construct with `split(.mpsc)`;
  use `split(.mpmc)` when the consumer side is shared as well.
- `split()` followed by `consumer.share()` → `split(.spmc)` /
  `split(.mpmc)`.
- `finish()` and `close()` are unchanged.

## Build / test

```bash
swift build
swift test
```

Requires a Swift 6.2+ toolchain. Deployment floors: macOS 15 / iOS 18 / tvOS 18 / watchOS 11 / visionOS 2. Library products: `ThreadAffinityExecutor`, `BlockingQueue`. Linux and Windows are built and tested in CI: the dedicated executor owns a Foundation `Thread` instead of subclassing it, and Linux resolves `gettid` directly from libc.

## Known tradeoffs

- `Foundation.Thread.start()` has no failure return; if the thread cannot be spawned (resource exhaustion) the synchronous initializer blocks.
- An executor attached to the main thread via `MainQueueDispatcher` shares the thread with `MainActor` but is not the same executor identity and does not replace it.
- `MigratingThreadExecutor.migrate(to:)` and `handOver(to:)` require a successor pump whose fixed thread or queue is independent of the currently serving pump — a different wrapper around the same thread or queue still deadlocks. The handshake post queues behind the old serving loop, which exits only after retarget seals its queue, and retarget waits for the handshake. The handshake itself no longer blocks the successor's thread while the old generation drains; the serving loop is posted after the old finalize.
- `FixedThreadDispatcher.post` returns `false` once the pump stops accepting work. Attachment and migration handshakes surface that as `AttachError.pumpUnavailable` / `MigrationError.pumpUnavailable`; `detach()` returns `false` when the finalize post is rejected instead of blocking forever. A pump that accepts a post but never runs it still leaves the join pending.

## License

Apache 2.0 — see [LICENSE](LICENSE).
