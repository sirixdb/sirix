/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import io.sirix.index.IndexType;
import io.sirix.page.PageReference;
import org.jspecify.annotations.Nullable;

import java.lang.foreign.MemorySegment;

/**
 * A cacheable page for the MEM step-2 tests. It implements the {@link CacheablePage} guard, orphan
 * and close contract the sharded caches rely on, weighs what it is told to weigh and, when built
 * over an allocator frame, gives that frame back to the allocator the moment it is torn down, as a
 * real page does. A guarded page defers its teardown to the last guard release, so a cache that
 * evicts a guarded entry is observable as a closed page that still has a guard.
 *
 * <p>
 * It is never "hot", so the second-chance reprieve of the clock order does not hide an eviction.
 */
final class MemCachePageStub implements CacheablePage {

  private final long pageKey;
  private final long weightBytes;
  private final @Nullable FrameSlotAllocator allocator;
  private final @Nullable MemorySegment frame;
  private int guards;
  private boolean closeRequested;
  private boolean closed;
  private PageReference lastCacheKey;

  private MemCachePageStub(final long pageKey, final long weightBytes, final @Nullable FrameSlotAllocator allocator,
      final @Nullable MemorySegment frame) {
    this.pageKey = pageKey;
    this.weightBytes = weightBytes;
    this.allocator = allocator;
    this.frame = frame;
  }

  /** A page that weighs {@code weightBytes} and owns no frame. */
  static MemCachePageStub weighing(final long pageKey, final long weightBytes) {
    if (weightBytes <= 0) {
      throw new IllegalArgumentException("a page must weigh something: " + weightBytes);
    }
    return new MemCachePageStub(pageKey, weightBytes, null, null);
  }

  /**
   * A page over a freshly allocated frame of {@code frameBytes} from {@code allocator}; it weighs
   * that frame's size and releases the frame when it is torn down.
   */
  static MemCachePageStub overFrame(final long pageKey, final FrameSlotAllocator allocator, final long frameBytes) {
    final MemorySegment frame = allocator.allocate(frameBytes);
    return new MemCachePageStub(pageKey, frameBytes, allocator, frame);
  }

  /** A distinct cache key for page number {@code index}. */
  static PageReference keyOf(final long index) {
    return new PageReference().setKey(index).setDatabaseId(1).setResourceId(1);
  }

  synchronized boolean isTornDown() {
    return closed;
  }

  @Override
  public long getActualMemorySize() {
    return weightBytes;
  }

  @Override
  public void markAccessed() {
    // Never hot: see the class documentation.
  }

  @Override
  public boolean isHot() {
    return false;
  }

  @Override
  public void clearHot() {
    // Never hot.
  }

  @Override
  public synchronized boolean acquireGuard() {
    if (closed) {
      return false;
    }
    guards++;
    return true;
  }

  @Override
  public synchronized void releaseGuard() {
    if (guards <= 0) {
      throw new IllegalStateException("guard released more often than acquired");
    }
    guards--;
    if (guards == 0 && closeRequested && !closed) {
      teardown();
    }
  }

  @Override
  public synchronized int getGuardCount() {
    return guards;
  }

  @Override
  public synchronized boolean isClosed() {
    return closed;
  }

  @Override
  public synchronized void markOrphaned() {
    closeRequested = true;
  }

  @Override
  public synchronized void close() {
    if (closed) {
      return;
    }
    if (guards > 0) {
      closeRequested = true;
      return;
    }
    teardown();
  }

  private void teardown() {
    closed = true;
    if (allocator != null && frame != null) {
      allocator.release(frame);
    }
  }

  @Override
  public void incrementVersion() {
    // No version-dependent readers in these tests.
  }

  @Override
  public long getPageKey() {
    return pageKey;
  }

  @Override
  public PageReference lastCacheKey() {
    return lastCacheKey;
  }

  @Override
  public void setLastCacheKey(final PageReference cacheKey) {
    this.lastCacheKey = cacheKey;
  }

  @Override
  public int getRevision() {
    return 1;
  }

  @Override
  public IndexType getIndexType() {
    return IndexType.DOCUMENT;
  }
}
