# ThreadAffinityExecutor

Thread-affinity executors for Swift Concurrency: every job accepted by one of these executors runs on the same OS thread for the executor's whole lifetime. The bound thread can be created by the library, provided by the caller, or borrowed from an existing fixed-thread mechanism.

## Scope

| Type | Notes | Status |
| --- | --- | --- |
| `BlockingQueue` (separate library product) | Split-endpoint blocking FIFO queues: a linear queue token (`Queue` unbounded / `BoundedQueue` fixed capacity) is consumed by `split()` into producer and consumer ends plus a non-minting supervisor; ends come in move-only unique (`~Copyable`) and cloneable shared (`Sendable`) tiers; enqueue is a `Bool`-returning try, bounded producers add `enqueueBlocking`; ends auto-disconnect on release (last producer closes, last consumer seals); elements need not be `Sendable` — ownership transfers through the queue, enforced by the compiler's `sending` handoff on enqueue (build-time SIL check) | ✅ |
| `ThreadID` | Cross-platform OS thread ID value type (Darwin `pthread_threadid_np` / Linux `gettid` / Windows `GetCurrentThreadId`); `Foundation.Thread` exposes no unified numeric TID | ✅ |
| `ThreadBoundExecutor` | `SerialExecutor + TaskExecutor` protocol; `isIsolatingCurrentContext()` (Swift 6.2) with a `checkIsolated()` fallback | ✅ |
| `DedicatedThreadExecutor` | Subclasses `Foundation.Thread` directly; synchronous `init(name:)` completes the startup handshake; jobs travel through the `BlockingQueue` library; the worker's `ThreadID` is published through a one-shot handshake | ✅ |
| `CurrentThreadLoopExecutor` | `installOnCurrentThread()` captures the TID; `run()` synchronously occupies the calling thread until `stop()` and a drained queue | ✅ |
| `MainThreadExecutor` / `MainQueueDispatcher` | Main-thread attachment via `DispatchQueue.main`; requires a main queue that is actually serviced | ✅ |
| `AttachedThreadExecutor` + `FixedThreadDispatcher` | Generic attachment to any fixed thread exposing a `post` entry (third-party event loops, RunLoop hosts) | ✅ interface ready, real backends pending |
| Shutdown scopes / task tracking | Full quiescence machinery | ⏳ explicit quiescence contract for now |

## Contract

- Every accepted job runs on the same live OS thread for the executor's lifetime; `threadID` never changes after publication.
- `enqueue` is safe from any thread; jobs never overlap and run exactly once.
- While jobs may still arrive, the executor and its scheduling backend stay alive.

> **Quiescence contract**: before `shutdown()`, no actor, task or external producer may submit further jobs. Queued jobs drain before the thread exits; once production is closed — from the `stop()` / `shutdown()` request onward — enqueueing drops the job and the producer end's `enqueue` returns `false`. Borrowed threads (main thread / attached backends) are not stopped by the library and expose no shutdown.

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
executor.shutdown() // blocks until the queue drained and sealed; idempotent; never call on the bound thread
```

### Task executor preference

```swift
await withTaskExecutorPreference(executor) {
    await nonisolatedOperation()
}
```

### Synchronous actor access from the bound thread

```swift
// a native callback already running on the bound thread:
counter.assumeIsolated { site in
    site.next() // synchronous; must not cross an await
}
```

### Caller-driven loop

```swift
let executor = CurrentThreadLoopExecutor.installOnCurrentThread()

// keep a strong reference until run() returned — queued jobs do not retain the executor.
// after submitting work, occupy the calling thread until stop() and a drained queue:
executor.run()
```

### Main thread / attached threads

```swift
// main thread (requires a serviced main queue, e.g. a GUI run loop or dispatchMain()):
let mainExecutor = await MainThreadExecutor.shared

// any fixed-thread loop that provides a post entry:
let executor = await AttachedThreadExecutor.attach(to: myEventLoopDispatcher)
```

## Build / test

```bash
swift build
swift test
```

Requires a Swift 6.2+ toolchain. Deployment floors: macOS 15 / iOS 18 / tvOS 18 / watchOS 11 / visionOS 2. Library products: `ThreadAffinityExecutor`, `BlockingQueue`. The Linux and Windows paths are written but so far only compiled and tested on macOS.

## Known tradeoffs

- `ThreadID.rawValue` is not public; kernels recycle TIDs, so no historical identity is provided for dead threads.
- `Foundation.Thread.start()` has no failure return; if the thread cannot be spawned (resource exhaustion) the synchronous initializer blocks.
- The main-thread executor shares the thread with `MainActor` but is not the same executor identity and does not replace it.
- On older libc without an importable `gettid`, a thin shim for this one call is all that is missing.
