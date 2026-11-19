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

/// One list node: a link plus an optional element. Nodes are allocated with
/// `UnsafeMutablePointer` and destroyed explicitly by the consumer on take, or
/// by the cell's `deinit` when the queue is released without draining. The
/// element is optional because every consumed node becomes the empty stub the
/// next take reads from.
private struct LockFreeMPSCQueueNode<Element: ~Copyable>: ~Copyable {
  /// Next node, `nil` until a producer links it.
  let next: Atomic<UnsafeMutablePointer<LockFreeMPSCQueueNode<Element>>?>

  var element: Element?

  init(element: consuming Element) {
    next = Atomic<UnsafeMutablePointer<LockFreeMPSCQueueNode<Element>>?>(nil)
    self.element = consume element
  }

  /// The empty stub the head starts on and every consumed node becomes.
  init() {
    next = Atomic<UnsafeMutablePointer<LockFreeMPSCQueueNode<Element>>?>(nil)
    element = nil
  }

  /// Moves the element out. Only the single consumer calls this, and only for
  /// nodes produced with an element — never for the initial stub.
  mutating func takeElement() -> Element {
    guard let value = element.take() else {
      preconditionFailure("lock-free MPSC node has no element")
    }
    return value
  }
}

/// Shared state behind a `LockFreeMPSCQueue` split.
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
  private let elementCondition = NSCondition()
  private let sealCondition = NSCondition()

  init() {
    let stub = UnsafeMutablePointer<Node>.allocate(capacity: 1)
    stub.initialize(to: Node())
    head = Atomic(stub)
    tail = Atomic(stub)
  }

  deinit {
    // The cell outlives every end and therefore every in-flight operation:
    // walk the remaining chain and destroy the nodes the queue still owns.
    var current: UnsafeMutablePointer<Node>? = head.load(ordering: .relaxed)
    while let node = current {
      let next = node.pointee.next.load(ordering: .relaxed)
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
      activeProducers.wrappingSubtract(1, ordering: .sequentiallyConsistent)
      atomicMemoryFence(ordering: .sequentiallyConsistent)
      wakeConsumerIfNeeded()
      return false
    }

    let node = UnsafeMutablePointer<Node>.allocate(capacity: 1)
    node.initialize(to: Node(element: element))
    let previous = tail.exchange(node, ordering: .acquiringAndReleasing)
    previous.pointee.next.store(node, ordering: .releasing)
    count.wrappingAdd(1, ordering: .relaxed)
    activeProducers.wrappingSubtract(1, ordering: .sequentiallyConsistent)
    atomicMemoryFence(ordering: .sequentiallyConsistent)
    wakeConsumerIfNeeded()
    return true
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
  /// currently linked. Never waits.
  func tryTake() -> Element? {
    let headNode = head.load(ordering: .acquiring)
    guard let next = headNode.pointee.next.load(ordering: .acquiring) else {
      return nil
    }

    head.store(next, ordering: .releasing)
    headNode.pointee.next.store(nil, ordering: .relaxed)
    let element = next.pointee.takeElement()
    headNode.deinitialize(count: 1)
    headNode.deallocate()
    count.wrappingSubtract(1, ordering: .relaxed)
    return element
  }

  /// Blocks until an element is available or the terminal state is reached.
  /// `nil` seals the queue in the same critical section that observed it.
  func take() -> Element? {
    while true {
      if let element = tryTake() {
        return element
      }

      elementCondition.lock()
      waitingConsumers.wrappingAdd(1, ordering: .relaxed)
      atomicMemoryFence(ordering: .sequentiallyConsistent)

      if let element = tryTake() {
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

  /// Registers both sides of one split in the same critical section. Every
  /// split calls this exactly once; releasing the matching end decrements it.
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

  /// Releasing the last consumer end seals the queue.
  func removeConsumer() {
    guard consumerCount.wrappingSubtract(1, ordering: .acquiringAndReleasing).newValue == 0 else {
      return
    }
    closed.store(true, ordering: .sequentiallyConsistent)
    elementCondition.lock()
    elementCondition.broadcast()
    elementCondition.unlock()
    markSealed()
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
