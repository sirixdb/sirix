/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import io.sirix.access.ResourceConfiguration;
import io.sirix.cache.Allocators;
import io.sirix.cache.FrameSlotAllocator;
import io.sirix.cache.MemorySegmentAllocator;
import io.sirix.index.IndexType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Constructor-stage native-frame ownership regressions for {@link KeyValueLeafPage}. */
final class KeyValueLeafPageConstructionFailureTest {

  private static FrameSlotAllocator allocator;

  @BeforeAll
  static void initializeAllocator() {
    allocator = assertInstanceOf(FrameSlotAllocator.class, Allocators.getInstance());
    allocator.init(8L * 1024 * 1024 * 1024);
  }

  @Test
  @DisplayName("A post-acquisition constructor failure releases its frame and retains the exact primary")
  void postAcquisitionFailureReleasesFrameAndSuppressesCleanupFailure() {
    final int frameClass = FrameSlotAllocator.indexForSize(PageLayout.INITIAL_PAGE_SIZE);
    final int liveBefore = allocator.liveSlotCount(frameClass);
    final long allocationsBefore = allocator.allocateCount(frameClass);
    final long releasesBefore = allocator.releaseCount(frameClass);
    final IllegalStateException primaryFailure = new IllegalStateException("injected after frame acquisition");
    final AssertionError releaseFailure = new AssertionError("injected after the frame was returned");
    final ReleaseFailingAllocator injectedAllocator = new ReleaseFailingAllocator(allocator, releaseFailure);
    final ResourceConfiguration config = new ResourceConfiguration.Builder("failed-fresh-frame").build();

    final IllegalStateException thrown = assertThrows(IllegalStateException.class,
        () -> new KeyValueLeafPage(101L, IndexType.DOCUMENT, config, 23, null, null, false,
            injectedAllocator, () -> {
              throw primaryFailure;
            }));

    assertSame(primaryFailure, thrown, "cleanup must rethrow the exact constructor-stage failure");
    assertTrue(injectedAllocator.releaseAttempted);
    assertNotNull(injectedAllocator.allocated);
    assertEquals(1L, allocator.allocateCount(frameClass) - allocationsBefore);
    assertEquals(1L, allocator.releaseCount(frameClass) - releasesBefore,
        "the acquired native frame must be returned even though construction never assigned a page");
    assertEquals(liveBefore, allocator.liveSlotCount(frameClass));
    assertEquals(FrameSlotAllocator.NO_SLOT_COORDINATES,
        allocator.slotCoordinates(injectedAllocator.allocated),
        "the failed constructor must leave no live allocator ownership entry");
    final Throwable[] suppressed = thrown.getSuppressed();
    assertEquals(1, suppressed.length);
    assertSame(releaseFailure, suppressed[0], "cleanup diagnostics belong on the original failure");
  }

  /** Delegates real frame ownership, then injects a failure after release has completed. */
  private static final class ReleaseFailingAllocator implements MemorySegmentAllocator {
    private final MemorySegmentAllocator delegate;
    private final Error releaseFailure;
    private MemorySegment allocated;
    private boolean releaseAttempted;

    private ReleaseFailingAllocator(final MemorySegmentAllocator delegate, final Error releaseFailure) {
      this.delegate = delegate;
      this.releaseFailure = releaseFailure;
    }

    @Override
    public void init(final long maxBufferSize) {
      delegate.init(maxBufferSize);
    }

    @Override
    public boolean isInitialized() {
      return delegate.isInitialized();
    }

    @Override
    public void free() {
      throw new UnsupportedOperationException("the test wrapper does not own the process allocator");
    }

    @Override
    public MemorySegment allocate(final long size) {
      allocated = delegate.allocate(size);
      return allocated;
    }

    @Override
    public void release(final MemorySegment segment) {
      releaseAttempted = true;
      delegate.release(segment);
      throw releaseFailure;
    }

    @Override
    public long getMaxBufferSize() {
      return delegate.getMaxBufferSize();
    }

    @Override
    public long getPhysicalMemoryBytes() {
      return delegate.getPhysicalMemoryBytes();
    }

    @Override
    public void resetSegment(final MemorySegment segment) {
      delegate.resetSegment(segment);
    }
  }
}
