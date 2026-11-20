//
//  LockFreeMPSCQueueCell.swift
//  BlockingQueue
//
//  Internal state for the lock-free unbounded MPSC queue: an intrusive
//  singly-linked list. Producers allocate one node per element and publish
//  it through the tail; the single consumer advances a stub head, takes the
//  next node's element and frees the old stub. A producer never touches a
//  node after linking it, and only the consumer frees nodes, so no
//  reclamation coordination is needed beyond the link handoff.
//

import Foundation
import Synchronization

/// One list node: a link plus optional element storage. Nodes are allocated
/// with `UnsafeMutablePointer`; the element lives in its own allocation so the
/// consumer's move stays disjoint from a producer's concurrent link write.
/// The head is always empty: either the initial stub or a node whose element
/// was moved out (its storage pointer is left dangling and never used again).
private struct LockFreeMPSCQueueNode<Element: ~Copyable>: ~Copyable {
  /// Next node, `nil` until a producer links it.
  let next: Atomic<UnsafeMutablePointer<LockFreeMPSCQueueNode<Element>>?>

  private var storage: UnsafeMutablePointer<Element>?

  init(element: consuming Element) {
    next = Atomic<UnsafeMutablePointer<LockFreeMPSCQueueNode<Element>>?>(nil)
    let storage = UnsafeMutablePointer<Element>.allocate(capacity: 1)
    storage.initialize(to: consume element)
    self.storage = storage
  }

  /// The empty stub the head starts on and every consumed node becomes.
  init() {
    next = Atomic<UnsafeMutablePointer<LockFreeMPSCQueueNode<Element>>?>(nil)
    storage = nil
  }

  /// Moves the element out without mutating `self`. Only the single consumer
  /// calls this, and only for nodes produced with an element. The node becomes
  /// the head stub; `storage` keeps a dangling pointer that must not be freed
  /// again.
  func takeElement() -> Element {
    guard let storage else {
      preconditionFailure("lock-free MPSC node has no element")
    }
    let value = storage.move()
    storage.deallocate()
    return value
  }

  /// Frees the element of a node that is still queued. The head stub is never
  /// passed here: its storage is `nil` or already freed by `takeElement()`.
  func deallocateStorage() {
    guard let storage else { return }
    storage.deinitialize(count: 1)
    storage.deallocate()
  }
}

/// Shared state behind a `LinkedQueue` split.
///
/// Lock-free path: producers exchange `tail` and link the previous tail with a
/// releasing store; the consumer acquires `head.next`. `head` is advanced only
/// by the consumer.
///
/// Termination: `closed` and `activeProducers` are sequentially consistent.
/// A producer registers as active before checking `closed`, and unregisters
/// after publishing. The consumer may seal only when it observes `closed`,
/// no active producers and an unlinked `head.next`, which linearizes close
/// against concurrent enqueues without losing an accepted element.
///
/// Parking: `waitingConsumers` is updated under `elementCondition`; the
/// producer publishes, executes a sequentially consistent fence, then loads
/// the waiter count. The consumer increments the count, executes the same
/// fence, then re-checks the queue. The fence pair prevents a lost wake-up
/// while leaving the enqueue fast path free of the condition lock.
final class LockFreeMPSCQueueCell<Element: ~Copyable>: @unchecked Sendable {
  private typealias Node = LockFreeMPSCQueueNode<Element>

  private let head: Atomic<UnsafeMutablePointer<Node>>
  private let tail: Atomic<UnsafeMutablePointer<Node>>
  private let closed = Atomic<Bool>(false)
  private let sealed = Atomic<Bool>(false)
  private let count = Atomic<Int>(0)
  private let activeProducers = Atomic<Int>(0)
  private let producerCount = Atomic<Int>(0)
  private let consumerCount = Atomic<Int>(0)
  private let waitingConsumers = Atomic<Int>(0)
  private let takerActive = Atomic<Bool>(false)
  private let elementCondition = NSCondition()
  private let sealCondition = NSCondition()

  /// Test-only suspension points. Set them before starting any producer
  /// thread; the test target does not mutate them concurrently.
  var beforeLinkHook: (@Sendable () -> Void)?
  var afterProducerDecrementHook: (@Sendable () -> Void)?

  init() {
    let stub = UnsafeMutablePointer<Node>.allocate(capacity: 1)
    stub.initialize(to: Node())
    head = Atomic(stub)
    tail = Atomic(stub)
  }

  deinit {
    // The cell outlives every end and therefore every in-flight operation.
    // The head is the current stub: its element storage is nil or already
    // freed by a take, so only its successors still own element storage.
    let headNode = head.load(ordering: .relaxed)
    var current = headNode.pointee.next.load(ordering: .relaxed)
    headNode.deinitialize(count: 1)
    headNode.deallocate()
    while let node = current {
      let next = node.pointee.next.load(ordering: .relaxed)
      node.pointee.deallocateStorage()
      node.deinitialize(count: 1)
      node.deallocate()
      current = next
    }
  }

  private func nextValue() -> UnsafeMutablePointer<Node>? {
    head.load(ordering: .acquiring).pointee.next.load(ordering: .acquiring)
  }

  /// Appends `element`. Never waits and never fails for capacity: the queue is
  /// unbounded. Returns `false` only once production closed or no consumer
  /// end remains, discarding the element.
  func enqueue(_ element: consuming Element) -> Bool {
    activeProducers.wrappingAdd(1, ordering: .sequentiallyConsistent)

    guard !closed.load(ordering: .sequentiallyConsistent) else {
      finishProducer()
      atomicMemoryFence(ordering: .sequentiallyConsistent)
      wakeConsumerIfNeeded()
      return false
    }

    beforeLinkHook?()

    // Increment before publishing: the consumer's acquire load of the link
    // then orders this increment before its decrement, so `count` cannot go
    // negative. In-flight increments may temporarily over-count.
    count.wrappingAdd(1, ordering: .relaxed)
    let node = UnsafeMutablePointer<Node>.allocate(capacity: 1)
    node.initialize(to: Node(element: element))
    let previous = tail.exchange(node, ordering: .acquiringAndReleasing)
    previous.pointee.next.store(node, ordering: .releasing)
    finishProducer()
    atomicMemoryFence(ordering: .sequentiallyConsistent)
    wakeConsumerIfNeeded()
    return true
  }

  /// Drops the in-flight producer count. If the consumer end is already gone
  /// and no producer remains, no consumer observation can seal the queue, so
  /// the producer seals it here. Re-read `activeProducers` after the closed
  /// check: the decrement's own new value is stale, and a producer that
  /// registered after it but read `closed == false` before the seal must be
  /// seen. A producer that registers after the fresh load sees `closed` and
  /// fails, so it cannot strand an accepted element behind the seal.
  private func finishProducer() {
    activeProducers.wrappingSubtract(1, ordering: .sequentiallyConsistent)
    afterProducerDecrementHook?()
    if closed.load(ordering: .sequentiallyConsistent),
      consumerCount.load(ordering: .sequentiallyConsistent) == 0,
      activeProducers.load(ordering: .sequentiallyConsistent) == 0
    {
      markSealed()
    }
  }

  /// Signals a parked consumer, taking the condition lock only when one is
  /// registered. Called after the fence that pairs with the consumer's
  /// registration fence.
  private func wakeConsumerIfNeeded() {
    guard waitingConsumers.load(ordering: .relaxed) > 0 else { return }
    elementCondition.lock()
    elementCondition.signal()
    elementCondition.unlock()
  }

  /// Removes and returns the element at the front, or `nil` when none is
  /// currently linked. Never waits. An empty observation seals the queue only
  /// when it is closed, no producer is in flight, and the queue is drained.
  /// While a producer is in flight, `nil` is not terminal: a non-blocking
  /// drain must keep calling until `supervisor.isSealed` (or use `take()`).
  func tryTake() -> Element? {
    enterTaker()
    defer { leaveTaker() }
    guard let element = takeIfAvailable() else {
      if isTerminal() {
        markSealed()
      }
      return nil
    }
    return element
  }

  /// Serializes the single legal taker. Concurrent entry would double-free
  /// the head node, so it traps instead of corrupting memory.
  private func enterTaker() {
    let (acquired, _) = takerActive.compareExchange(
      expected: false, desired: true, ordering: .acquiringAndReleasing)
    precondition(acquired, "lock-free MPSC consumer used concurrently")
  }

  private func leaveTaker() {
    takerActive.store(false, ordering: .releasing)
  }

  /// The unguarded dequeue shared by `tryTake()` and `take()`.
  private func takeIfAvailable() -> Element? {
    let headNode = head.load(ordering: .acquiring)
    guard let next = headNode.pointee.next.load(ordering: .acquiring) else {
      return nil
    }

    head.store(next, ordering: .releasing)
    headNode.pointee.next.store(nil, ordering: .relaxed)
    let element = next.pointee.takeElement()
    // The old head is always the empty stub: its element storage is nil or
    // already freed by a take. Deallocate the node without touching that
    // pointer.
    headNode.deallocate()
    count.wrappingSubtract(1, ordering: .relaxed)
    return element
  }

  /// Blocks until an element is available or the terminal state is reached.
  /// `nil` seals the queue in the same critical section that observed it.
  func take() -> Element? {
    enterTaker()
    defer { leaveTaker() }
    while true {
      if let element = takeIfAvailable() {
        return element
      }

      elementCondition.lock()
      waitingConsumers.wrappingAdd(1, ordering: .relaxed)
      atomicMemoryFence(ordering: .sequentiallyConsistent)

      if let element = takeIfAvailable() {
        waitingConsumers.wrappingSubtract(1, ordering: .relaxed)
        elementCondition.unlock()
        return element
      }

      if isTerminal() {
        waitingConsumers.wrappingSubtract(1, ordering: .relaxed)
        markSealed()
        elementCondition.unlock()
        return nil
      }

      elementCondition.wait()
      waitingConsumers.wrappingSubtract(1, ordering: .relaxed)
      elementCondition.unlock()
    }
  }

  /// Closed, drained and free of in-flight producers. The sequential
  /// consistency of `closed`/`activeProducers` and the release/acquire chain
  /// to `head.next` make the three observations sound.
  private func isTerminal() -> Bool {
    guard closed.load(ordering: .sequentiallyConsistent) else { return false }
    guard activeProducers.load(ordering: .sequentiallyConsistent) == 0 else { return false }
    return nextValue() == nil
  }

  private func markSealed() {
    if sealed.load(ordering: .sequentiallyConsistent) {
      return
    }
    sealCondition.lock()
    sealed.store(true, ordering: .sequentiallyConsistent)
    sealCondition.broadcast()
    sealCondition.unlock()
  }

  func close() {
    closed.store(true, ordering: .sequentiallyConsistent)
    elementCondition.lock()
    elementCondition.broadcast()
    elementCondition.unlock()
  }

  /// Registers both endpoint sides before `split` returns. The two relaxed
  /// increments need no shared critical section: the ends are not published
  /// until the split call completes.
  func registerEnds() {
    producerCount.wrappingAdd(1, ordering: .relaxed)
    consumerCount.wrappingAdd(1, ordering: .relaxed)
  }

  /// Releasing the last producer end closes production.
  func removeProducer() {
    guard producerCount.wrappingSubtract(1, ordering: .acquiringAndReleasing).newValue == 0 else {
      return
    }
    closed.store(true, ordering: .sequentiallyConsistent)
    elementCondition.lock()
    elementCondition.broadcast()
    elementCondition.unlock()
  }

  /// Releasing the last consumer end closes production and seals the queue,
  /// but only once in-flight producers have finished. A producer that passed
  /// its `closed` check before the release may still publish; it is stranded
  /// (there is no taker), but `isSealed` does not race it.
  func removeConsumer() {
    guard
      consumerCount.wrappingSubtract(1, ordering: .sequentiallyConsistent).newValue == 0
    else {
      return
    }
    closed.store(true, ordering: .sequentiallyConsistent)
    elementCondition.lock()
    elementCondition.broadcast()
    elementCondition.unlock()
    if activeProducers.load(ordering: .sequentiallyConsistent) == 0 {
      markSealed()
    }
  }

  var isClosed: Bool { closed.load(ordering: .acquiring) }
  var isSealed: Bool { sealed.load(ordering: .acquiring) }
  var elementCount: Int { count.load(ordering: .relaxed) }
  var waitingConsumerCount: Int { waitingConsumers.load(ordering: .relaxed) }

  func waitUntilSealed() {
    sealCondition.lock()
    while !sealed.load(ordering: .acquiring) {
      sealCondition.wait()
    }
    sealCondition.unlock()
  }
}
