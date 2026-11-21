/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot;

import io.sirix.api.StorageEngineWriter;
import io.sirix.cache.ShardedPageCache;
import io.sirix.index.hot.page.HOTLeafPage;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Set;

/**
 * A storage engine that evicts HOT leaves at the worst moments a writer can meet.
 *
 * <p>
 * The page cache may evict any leaf nobody guards, at any time: another thread sharing the global
 * cache does it concurrently, and this thread does it itself whenever it loads a page or allocates
 * a frame under pressure. A single-threaded test cannot interleave another thread at a chosen
 * instruction, so this adversary sits where the writer hands control to the storage engine. Every
 * call the writer makes on the engine is one <em>point</em>; at a chosen point (or at every point)
 * the adversary evicts every unguarded leaf from the real page cache through the cache's own
 * pressure eviction, so a guarded leaf survives exactly as it does in production.
 * </p>
 *
 * <p>
 * For an unguarded leaf handoff ({@code loadHOTPage}) the eviction runs <em>after</em> the engine
 * returned the leaf: the caller then holds a leaf that was evicted between its resolution and its
 * read, which is the race a concurrent evictor wins. Everything else is evicted <em>before</em> the
 * call, which is what loading a later sibling or allocating a page does to an earlier one.
 * </p>
 *
 * <p>
 * Inside the writer's two guard-retry loops the eviction runs before the handoff as well. They
 * resolve a leaf and then take a guard on it, re-resolving until they win; an adversary that wins
 * every round models no scheduler and would only time them out.
 * </p>
 */
final class HOTLeafEvictionAdversary implements InvocationHandler {

  /** Strike at every point instead of at one chosen point. */
  static final int EVERY_POINT = 0;

  private static final String UNGUARDED_HANDOFF = "loadHOTPage";

  /** The writer's guard-retry loops; see the class comment. */
  private static final Set<String> GUARD_RETRY_LOOPS =
      Set.of("acquireLeafForRead", "cowHOTLeafForModificationUnpoisoned");

  private static final StackWalker STACK = StackWalker.getInstance();

  private final StorageEngineWriter engine;
  private final StorageEngineWriter proxy;
  private boolean armed;
  private int strikePoint;
  private int points;
  private int strikes;
  private int evictedLeaves;

  /**
   * Whether to keep retiring the leaf handed out inside the writer's guard-retry loops. Normally the
   * eviction moves before the handoff there, so a retry gets a live leaf and succeeds, which is what
   * production looks like. Set this to make a required acquisition unwinnable, which is how FMT-304's
   * interim bound can be reached in a test.
   */
  private boolean starveGuardAcquisition;

  /**
   * The engine call from which to start starving guard acquisition. A put reads the slot it is about
   * to overwrite before it enters its mutation, so starving from the first call always fails that
   * read and never reaches the mutation; starting later lets the read through and makes the
   * acquisition inside the mutation unwinnable instead.
   */
  private int starveFromPoint;

  HOTLeafEvictionAdversary(final StorageEngineWriter engine) {
    this.engine = engine;
    this.proxy = (StorageEngineWriter) Proxy.newProxyInstance(StorageEngineWriter.class.getClassLoader(),
        new Class<?>[] {StorageEngineWriter.class}, this);
  }

  /** The engine to hand a writer: every call on it is a point the adversary may strike at. */
  StorageEngineWriter engine() {
    return proxy;
  }

  /**
   * Start counting points and striking.
   *
   * @param strikePoint the one-based point to strike at, or {@link #EVERY_POINT}
   */
  /**
   * Keep retiring the handed-out leaf even while the writer retries, so no attempt can ever guard it.
   *
   * @param starve whether to starve guard acquisition
   */
  void starveGuardAcquisition(final boolean starve) {
    starveGuardAcquisition = starve;
    starveFromPoint = 0;
  }

  /**
   * Starve guard acquisition only from the given engine call onwards, so earlier required reads
   * succeed and the unwinnable acquisition falls inside the mutation.
   *
   * @param point the first engine call to starve from, counted like a strike point
   */
  void starveGuardAcquisitionFromPoint(final int point) {
    starveGuardAcquisition = true;
    starveFromPoint = point;
  }

  void arm(final int strikePoint) {
    if (strikePoint < 0) {
      throw new IllegalArgumentException("strikePoint must not be negative: " + strikePoint);
    }
    this.strikePoint = strikePoint;
    points = 0;
    strikes = 0;
    evictedLeaves = 0;
    armed = true;
  }

  void disarm() {
    armed = false;
  }

  /** Engine calls seen since the adversary was armed. */
  int points() {
    return points;
  }

  /** Points at which the adversary evicted. */
  int strikes() {
    return strikes;
  }

  /** Leaves the adversary evicted since it was armed. */
  int evictedLeaves() {
    return evictedLeaves;
  }

  @Override
  public Object invoke(final Object self, final Method method, final Object[] args) throws Throwable {
    if (!armed) {
      return call(method, args);
    }
    final int point = ++points;
    final boolean strike = strikePoint == EVERY_POINT || strikePoint == point;
    final boolean afterHandoff = UNGUARDED_HANDOFF.equals(method.getName())
        && ((starveGuardAcquisition && point >= starveFromPoint) || !insideGuardRetryLoop());
    if (strike && !afterHandoff) {
      strike();
    }
    final Object result = call(method, args);
    if (strike && afterHandoff) {
      strike();
    }
    return result;
  }

  private Object call(final Method method, final Object[] args) throws Throwable {
    try {
      return method.invoke(engine, args);
    } catch (final InvocationTargetException failure) {
      throw failure.getCause();
    }
  }

  private static boolean insideGuardRetryLoop() {
    return STACK.walk(frames -> frames.anyMatch(frame -> GUARD_RETRY_LOOPS.contains(frame.getMethodName())));
  }

  /** Evict every leaf nobody guards, the way allocator pressure does. */
  private void strike() {
    strikes++;
    evictedLeaves += evictEveryUnguardedLeaf(engine);
  }

  /**
   * Evict every unguarded HOT leaf of {@code engine}'s page cache through the cache's own pressure
   * eviction.
   *
   * @return how many leaves left the cache
   */
  static int evictEveryUnguardedLeaf(final StorageEngineWriter engine) {
    @SuppressWarnings("unchecked")
    final ShardedPageCache<HOTLeafPage> cache =
        (ShardedPageCache<HOTLeafPage>) engine.getBufferManager().getHOTLeafPageCache();
    final int before = cache.asMap().size();
    // One pass sheds a quarter of what the cache holds and gives a recently used leaf a second
    // chance, so a full sweep takes several passes. Guarded leaves stay however many are made.
    for (int pass = 0; pass < 1_024 && holdsAnUnguardedLeaf(cache); pass++) {
      cache.evictUnderPressure();
    }
    return before - cache.asMap().size();
  }

  private static boolean holdsAnUnguardedLeaf(final ShardedPageCache<HOTLeafPage> cache) {
    for (final HOTLeafPage leaf : cache.asMap().values()) {
      if (leaf.getGuardCount() == 0) {
        return true;
      }
    }
    return false;
  }
}
