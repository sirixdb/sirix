/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import io.sirix.JsonTestHelper;
import io.sirix.access.ResourceConfiguration;
import io.sirix.cache.Allocators;
import io.sirix.cache.FrameSlotAllocator;
import io.sirix.cache.MemorySegmentAllocator;
import io.sirix.index.IndexType;
import io.sirix.io.bytepipe.ByteHandler;
import io.sirix.node.Bytes;
import io.sirix.node.MemorySegmentBytesIn;
import io.sirix.node.MemorySegmentBytesOut;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Native-frame acquisition/publication ownership regressions for {@link HOTLeafPage}. */
final class HOTLeafPageFrameOwnershipTest {

  private static final int UNDERSIZED = 4 * 1024;
  private static FrameSlotAllocator allocator;

  @BeforeAll
  static void initializeAllocator() {
    allocator = assertInstanceOf(FrameSlotAllocator.class, Allocators.getInstance());
    allocator.init(8L * 1024 * 1024 * 1024);
  }

  @Test
  @DisplayName("a post-acquisition constructor failure returns its frame and preserves its primary")
  void postAcquisitionConstructorFailureReturnsFrame() {
    final int frameClass = FrameSlotAllocator.indexForSize(HOTLeafPage.DEFAULT_SIZE);
    final int liveBefore = allocator.liveSlotCount(frameClass);
    final long allocationsBefore = allocator.allocateCount(frameClass);
    final long releasesBefore = allocator.releaseCount(frameClass);
    final IllegalStateException primaryFailure = new IllegalStateException("injected after HOT frame acquisition");
    final AssertionError cleanupFailure = new AssertionError("injected after HOT frame release");
    final ReleaseFailingAllocator injectedAllocator = new ReleaseFailingAllocator(allocator, cleanupFailure);

    final IllegalStateException thrown = assertThrows(IllegalStateException.class,
        () -> new HOTLeafPage(101L, 23, IndexType.PROJECTION, injectedAllocator, () -> {
          throw primaryFailure;
        }));

    assertSame(primaryFailure, thrown, "cleanup must retain the exact constructor-stage failure");
    assertTrue(injectedAllocator.releaseAttempted);
    assertNotNull(injectedAllocator.allocated);
    assertEquals(1L, allocator.allocateCount(frameClass) - allocationsBefore);
    assertEquals(1L, allocator.releaseCount(frameClass) - releasesBefore);
    assertEquals(liveBefore, allocator.liveSlotCount(frameClass));
    assertEquals(FrameSlotAllocator.NO_SLOT_COORDINATES, allocator.slotCoordinates(injectedAllocator.allocated));
    assertEquals(1, thrown.getSuppressed().length);
    assertSame(cleanupFailure, thrown.getSuppressed()[0]);
  }

  @Test
  @DisplayName("promotion publishes the new frame owner before a failing old-frame release escapes")
  void promotionRetainsNewFrameWhenOldReleaseFails() {
    final int oldFrameClass = FrameSlotAllocator.indexForSize(UNDERSIZED);
    final int newFrameClass = FrameSlotAllocator.indexForSize(HOTLeafPage.DEFAULT_SIZE);
    final int oldLiveBefore = allocator.liveSlotCount(oldFrameClass);
    final int newLiveBefore = allocator.liveSlotCount(newFrameClass);
    final MemorySegment oldMemory = allocator.allocate(UNDERSIZED);
    final AssertionError oldReleaseFailure = new AssertionError("injected after old HOT frame release");
    final HOTLeafPage leaf = new HOTLeafPage(102L, 23, IndexType.PROJECTION, oldMemory, () -> {
      allocator.release(oldMemory);
      throw oldReleaseFailure;
    }, new int[HOTLeafPage.MAX_ENTRIES], 0, 0);
    final long oldBinding = leaf.readStampBinding();

    try {
      assertSame(oldReleaseFailure, assertThrows(AssertionError.class, () -> leaf.put(new byte[] {1}, new byte[] {2})));

      assertEquals(oldLiveBefore, allocator.liveSlotCount(oldFrameClass), "the replaced frame must be returned");
      assertEquals(newLiveBefore + 1, allocator.liveSlotCount(newFrameClass),
          "the live page must retain its promoted frame");
      assertNotEquals(oldBinding, leaf.readStampBinding());
      assertEquals(0L, leaf.readStampBinding() & 1L, "failed old cleanup must not poison the new binding");
      assertTrue(leaf.validateStamp(leaf.readStampBinding(), leaf.readStamp()));
      assertTrue(leaf.put(new byte[] {1}, new byte[] {2}), "the promoted page remains writable");
    } finally {
      leaf.close();
    }

    assertEquals(newLiveBefore, allocator.liveSlotCount(newFrameClass), "close must release the promoted frame");
  }

  @Test
  @DisplayName("copying deserialization returns its frame when the side-reference trailer is truncated")
  void copyingDeserializationReturnsFrameAfterTruncatedSideReferenceTrailer() {
    final byte[] truncatedWire = truncatedSideReferenceWire();
    final ResourceConfiguration config = resourceConfiguration("copying-truncated-side-ref");
    final int frameClass = FrameSlotAllocator.indexForSize(HOTLeafPage.DEFAULT_SIZE);
    final int liveBefore = allocator.liveSlotCount(frameClass);
    final long allocationsBefore = allocator.allocateCount(frameClass);
    final long releasesBefore = allocator.releaseCount(frameClass);

    assertThrows(IndexOutOfBoundsException.class,
        () -> new PagePersister().deserializePage(config, Bytes.wrapForRead(truncatedWire), SerializationType.DATA));

    assertEquals(1L, allocator.allocateCount(frameClass) - allocationsBefore,
        "the copying path must acquire exactly one HOT frame");
    assertEquals(1L, allocator.releaseCount(frameClass) - releasesBefore,
        "the unpublished page must return that frame exactly once");
    assertEquals(liveBefore, allocator.liveSlotCount(frameClass),
        "a corrupt trailer must not leave a live HOT frame behind");
  }

  @Test
  @DisplayName("zero-copy trailer failure releases once and keeps cleanup failure suppressed")
  void zeroCopyDeserializationReleasesTransferredFrameAfterTruncatedSideReferenceTrailer() {
    final byte[] truncatedWire = truncatedSideReferenceWire();
    final MemorySegment wireSegment = MemorySegment.ofArray(truncatedWire);
    final AtomicInteger releaseCalls = new AtomicInteger();
    final AssertionError cleanupFailure = new AssertionError("injected zero-copy frame cleanup failure");
    final ByteHandler.DecompressionResult decompressionResult =
        new ByteHandler.DecompressionResult(wireSegment, wireSegment, () -> {
          releaseCalls.incrementAndGet();
          throw cleanupFailure;
        }, new AtomicBoolean());

    final IndexOutOfBoundsException primaryFailure = assertThrows(IndexOutOfBoundsException.class,
        () -> new PagePersister().deserializePage(resourceConfiguration("zero-copy-truncated-side-ref"),
            new MemorySegmentBytesIn(wireSegment), SerializationType.DATA, decompressionResult));

    assertTrue(decompressionResult.ownershipTransferred().get(),
        "the HOT leaf must have taken ownership before its trailer failed");
    assertEquals(1, releaseCalls.get(), "failed deserialization must release the transferred frame exactly once");
    assertEquals(1, primaryFailure.getSuppressed().length, "cleanup failure must not replace the corruption exception");
    assertSame(cleanupFailure, primaryFailure.getSuppressed()[0]);

    // Mirrors AbstractReader's finally block. Once ownership was transferred, the result close is
    // intentionally inert even when the page-side releaser itself reported a failure.
    decompressionResult.close();
    assertEquals(1, releaseCalls.get(), "the outer scoped-result close must not release a second time");
  }

  private static byte[] truncatedSideReferenceWire() {
    final HOTLeafPage leaf = new HOTLeafPage(103L, 23, IndexType.PROJECTION);
    leaf.setPageReference(HOTLeafPage.overflowPageRefKey(7L, 3), new PageReference().setKey(29_003L));
    try (MemorySegmentBytesOut sink = new MemorySegmentBytesOut(1_024)) {
      PageKind.HOT_LEAF_PAGE.serializePage(resourceConfiguration("truncated-side-ref-wire"), sink, leaf,
          SerializationType.DATA);
      final byte[] completeWire = sink.toByteArray();
      return Arrays.copyOf(completeWire, completeWire.length - 1);
    } finally {
      leaf.close();
    }
  }

  private static ResourceConfiguration resourceConfiguration(final String suffix) {
    return new ResourceConfiguration.Builder(JsonTestHelper.RESOURCE + "-hot-leaf-ownership-" + suffix).build();
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
