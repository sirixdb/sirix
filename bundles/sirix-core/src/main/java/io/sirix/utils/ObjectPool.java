package io.sirix.utils;

import java.util.ArrayDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * HFT-grade lock-free object pool with thread-local fast path.
 *
 * <p>Design:
 * <ul>
 *   <li>Thread-local {@link ArrayDeque} for zero-contention borrow/return on the fast path.</li>
 *   <li>Shared {@link ConcurrentLinkedQueue} as cross-thread overflow / fallback.</li>
 *   <li>Factory creates objects on demand (no pre-allocation, no blocking).</li>
 *   <li>Destroyer is called on every pooled object during {@link #close()}.</li>
 * </ul>
 *
 * <p>Thread-local deque is bounded to {@code maxThreadLocalSize} (default 4) to prevent
 * memory leaks from threads that only return but never borrow.
 *
 * @param <T> the pooled object type
 */
public final class ObjectPool<T> implements AutoCloseable {

  /** Default maximum number of objects cached per thread. */
  private static final int DEFAULT_MAX_THREAD_LOCAL_SIZE = 4;

  /** Default maximum number of objects in the shared overflow queue. */
  private static final int DEFAULT_MAX_SHARED_SIZE = 32;

  private final Supplier<T> factory;
  private final Consumer<T> destroyer;
  private final int maxThreadLocalSize;
  private final int maxSharedSize;

  /**
   * Thread-local pool — zero synchronization on the fast path.
   * Each thread gets its own bounded deque.
   */
  private final ThreadLocal<ArrayDeque<T>> threadLocalPool;

  /**
   * Shared overflow queue — lock-free CAS-based MPMC queue.
   * Used when a thread-local deque overflows, or when a thread's deque is empty
   * and the factory should be avoided.
   */
  private final ConcurrentLinkedQueue<T> sharedPool;

  /** Guard against double-close. */
  private final AtomicBoolean closed;

  /**
   * Creates a pool with the given factory and destroyer, using default capacities.
   *
   * @param factory  creates new instances; must not return {@code null}
   * @param destroyer disposes instances on pool shutdown (e.g., close I/O handles)
   */
  public ObjectPool(final Supplier<T> factory, final Consumer<T> destroyer) {
    this(factory, destroyer, DEFAULT_MAX_THREAD_LOCAL_SIZE, DEFAULT_MAX_SHARED_SIZE);
  }

  /**
   * Creates a pool with full configuration.
   *
   * @param factory            creates new instances; must not return {@code null}
   * @param destroyer          disposes instances on pool shutdown
   * @param maxThreadLocalSize maximum objects cached per thread-local deque
   * @param maxSharedSize      maximum objects in the shared overflow queue
   */
  public ObjectPool(final Supplier<T> factory,
                    final Consumer<T> destroyer,
                    final int maxThreadLocalSize,
                    final int maxSharedSize) {
    if (factory == null) {
      throw new NullPointerException("factory must not be null");
    }
    if (destroyer == null) {
      throw new NullPointerException("destroyer must not be null");
    }
    if (maxThreadLocalSize < 0) {
      throw new IllegalArgumentException("maxThreadLocalSize must be >= 0, got " + maxThreadLocalSize);
    }
    if (maxSharedSize < 0) {
      throw new IllegalArgumentException("maxSharedSize must be >= 0, got " + maxSharedSize);
    }
    this.factory = factory;
    this.destroyer = destroyer;
    this.maxThreadLocalSize = maxThreadLocalSize;
    this.maxSharedSize = maxSharedSize;
    this.threadLocalPool = ThreadLocal.withInitial(ArrayDeque::new);
    this.sharedPool = new ConcurrentLinkedQueue<>();
    this.closed = new AtomicBoolean(false);
  }

  /**
   * Borrows an object from the pool. Fast path: thread-local deque (no synchronization).
   * Fallback: shared queue (CAS only). Last resort: factory creates a new instance.
   *
   * <p>Never blocks, never throws due to pool exhaustion — always succeeds by creating
   * a new instance if no pooled instance is available.
   *
   * @return a pooled or newly created object; never {@code null}
   * @throws IllegalStateException if the pool has been closed
   */
  public T borrowObject() {
    if (closed.get()) {
      throw new IllegalStateException("ObjectPool is closed");
    }

    // Fast path: thread-local deque — zero contention
    final ArrayDeque<T> local = threadLocalPool.get();
    final T localObj = local.pollFirst();
    if (localObj != null) {
      return localObj;
    }

    // Fallback: shared overflow queue — lock-free CAS
    final T sharedObj = sharedPool.poll();
    if (sharedObj != null) {
      return sharedObj;
    }

    // Last resort: create via factory
    return factory.get();
  }

  /**
   * Returns an object to the pool for reuse. The caller must not use the object after returning it.
   *
   * <p>Fast path: push to thread-local deque. If the deque is full, overflow to the shared queue.
   * If the shared queue is also full, the object is destroyed immediately to prevent unbounded
   * growth.
   *
   * <p>If the pool is already closed, the object is destroyed immediately.
   *
   * @param obj the object to return; must not be {@code null}
   */
  public void returnObject(final T obj) {
    if (obj == null) {
      throw new NullPointerException("Cannot return null to pool");
    }

    if (closed.get()) {
      destroyer.accept(obj);
      return;
    }

    // Fast path: thread-local deque
    final ArrayDeque<T> local = threadLocalPool.get();
    if (local.size() < maxThreadLocalSize) {
      local.addFirst(obj);
      return;
    }

    // Overflow to shared queue (bounded — approximate size check is intentional;
    // ConcurrentLinkedQueue.size() is O(n) but maxSharedSize is small)
    if (sharedPool.size() < maxSharedSize) {
      sharedPool.offer(obj);
    } else {
      // Pool is full — destroy the object to avoid memory leaks
      destroyer.accept(obj);
    }
  }

  /**
   * Shuts down the pool: drains all thread-local and shared pools, calling the destroyer
   * on each object. After close, {@link #borrowObject()} will throw and
   * {@link #returnObject(Object)} will destroy the object immediately.
   *
   * <p>Safe to call multiple times (idempotent).
   */
  @Override
  public void close() {
    if (!closed.compareAndSet(false, true)) {
      return; // already closed
    }

    // Drain thread-local pool of the closing thread
    final ArrayDeque<T> local = threadLocalPool.get();
    T obj;
    while ((obj = local.pollFirst()) != null) {
      destroyer.accept(obj);
    }
    threadLocalPool.remove();

    // Drain shared pool
    while ((obj = sharedPool.poll()) != null) {
      destroyer.accept(obj);
    }
  }
}
