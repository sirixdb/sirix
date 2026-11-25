/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.sirix.access.ResourceConfiguration;
import io.sirix.cache.CompactImageStore.Block;
import io.sirix.index.IndexType;
import io.sirix.index.hot.page.HOTLeafPage;
import io.sirix.io.bytepipe.ByteHandlerPipeline;
import io.sirix.node.MemorySegmentBytesIn;
import io.sirix.node.MemorySegmentBytesOut;
import io.sirix.page.PagePersister;
import io.sirix.page.SerializationType;
import io.sirix.settings.VersioningType;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

@Isolated
final class CompactImageAllocatorLifecycleTest {
  private static final ResourceConfiguration CONFIG =
      ResourceConfiguration.newBuilder("compact-image-lifecycle")
                           .versioningApproach(VersioningType.FULL)
                           .byteHandlerPipeline(new ByteHandlerPipeline())
                           .build();

  private enum Failure {
    RESERVE, COMMIT, POPULATE, HEAP_RESERVE, HEAP_COMMIT, HEAP_POPULATE
  }

  private enum FirstAllocation {
    FRAME, RECLAIMABLE_SLAB, PERMANENT_SLAB
  }

  @ParameterizedTest
  @EnumSource(FirstAllocation.class)
  void firstAllocationInitializesAndProvidesWritableStorage(final FirstAllocation kind) {
    // The standalone page/fragment contract permits an allocation before Databases startup.
    // Observe initialization and usable bytes, independently of the lazy-init implementation.
    final FrameSlotAllocator allocator = new FrameSlotAllocator();
    assertFalse(allocator.isInitialized());
    try {
      final MemorySegment segment = switch (kind) {
        case FRAME -> allocator.allocate(4096);
        case RECLAIMABLE_SLAB -> allocator.allocateSlab(4096);
        case PERMANENT_SLAB -> allocator.allocatePermanentSlab(4096);
      };
      assertNotNull(segment);
      try {
        assertTrue(allocator.isInitialized());
        segment.set(ValueLayout.JAVA_LONG, 0L, 42L);
        assertEquals(42L, segment.get(ValueLayout.JAVA_LONG, 0L));
      } finally {
        if (kind == FirstAllocation.FRAME) {
          allocator.release(segment);
        } else {
          assertTrue(allocator.releaseSlab(segment));
        }
      }
    } finally {
      allocator.shutdown();
    }
  }

  @Test
  void virtualAddressExhaustionIsARefusal() {
    final VirtualMemory memory = FrameSlotAllocator.virtualMemory();
    assertNull(memory.tryReserve(Long.MAX_VALUE));
    assertThrows(OutOfMemoryError.class, () -> memory.reserve(Long.MAX_VALUE));
  }

  @ParameterizedTest
  @EnumSource(Failure.class)
  void nativeRefusalRefundsOwnershipWhileHeapFailurePropagates(final Failure failure) {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(4L << 20);
    final RefusingMemory memory = new RefusingMemory(failure);
    allocator.setSlabMemoryForTesting(memory);
    try {
      if (failure == Failure.HEAP_RESERVE || failure == Failure.HEAP_COMMIT || failure == Failure.HEAP_POPULATE) {
        assertSame(memory.heapFailure,
            assertThrows(OutOfMemoryError.class, () -> allocator.allocatePermanentSlab(8192)));
      } else {
        assertNull(allocator.allocatePermanentSlab(8192));
      }
      assertEquals(0L, allocator.getPhysicalMemoryBytes());
      assertEquals(0L, allocator.nonExecutionCommittedBytes());
      assertEquals(0L, allocator.getSlabBytes());
      assertEquals(failure == Failure.RESERVE || failure == Failure.HEAP_RESERVE
          ? 0
          : 1, memory.releases);
      allocator.setSlabMemoryForTesting(FrameSlotAllocator.virtualMemory());
      final MemorySegment recovered = allocator.allocatePermanentSlab(8192);
      assertNotNull(recovered);
      recovered.set(ValueLayout.JAVA_BYTE, 0L, (byte) 42);
      assertEquals((byte) 42, recovered.get(ValueLayout.JAVA_BYTE, 0L));
      assertTrue(allocator.releaseSlab(recovered));
    } finally {
      allocator.setSlabMemoryForTesting(FrameSlotAllocator.virtualMemory());
      allocator.shutdown();
    }
  }

  @Test
  void permanentSlabPublicationFailureStillPropagates() {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(4L << 20);
    final OutOfMemoryError failure = new OutOfMemoryError("heap publication failure");
    allocator.setSlabPublicationHookForTesting(() -> {
      throw failure;
    });
    try {
      assertSame(failure, assertThrows(OutOfMemoryError.class, () -> allocator.allocatePermanentSlab(8192)));
      assertEquals(0L, allocator.getPhysicalMemoryBytes());
      assertEquals(0L, allocator.nonExecutionCommittedBytes());
      assertEquals(0L, allocator.getSlabBytes());
    } finally {
      allocator.setSlabPublicationHookForTesting(null);
      allocator.shutdown();
    }
  }

  @Test
  void poolDoesNotHideAllocatorPublicationFailure() {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(4L << 20);
    final CompactImageStore store = new CompactImageStore(allocator);
    final IllegalStateException failure = new IllegalStateException("slab publication failure");
    allocator.setSlabPublicationHookForTesting(() -> {
      throw failure;
    });
    try {
      assertSame(failure, assertThrows(IllegalStateException.class, () -> store.acquire(8192)));
      assertEquals(0, store.leasedBlockCount());
      assertEquals(0, store.blockCount());
      assertEquals(0L, allocator.getPhysicalMemoryBytes());
    } finally {
      allocator.setSlabPublicationHookForTesting(null);
      assertTrue(store.releaseIdleSlabs());
      allocator.shutdown();
    }
  }

  @Test
  void stoppedAllocatorRefusesPoolGrowth() {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(4L << 20);
    final CompactImageStore store = new CompactImageStore(allocator);
    allocator.shutdown();
    assertNull(store.acquire(8192));
    assertEquals(0, store.blockCount());
    assertEquals(0L, allocator.getPhysicalMemoryBytes());
  }

  @ParameterizedTest
  @EnumSource(value = Failure.class, names = {"RESERVE", "COMMIT", "POPULATE"})
  void nativeRefusalStillDecodesOnGcOwnedStorage(final Failure failure) throws IOException {
    final byte[] wire = wire();
    final CompactImageStore store = CompactImageStore.getInstance();
    final FrameSlotAllocator allocator = FrameSlotAllocator.getInstance();
    final List<Block> held = new ArrayList<>();
    allocator.setSlabMemoryForTesting(new RefusingMemory(failure));
    try {
      for (Block block; (block = store.acquire(60000)) != null;) {
        held.add(block);
      }
      assertFallbackRead(wire);
    } finally {
      allocator.setSlabMemoryForTesting(FrameSlotAllocator.virtualMemory());
      for (final Block block : held) {
        block.run();
      }
    }
    assertPooledRead(wire);
  }

  @Test
  void commitmentCeilingStillDecodesOnGcOwnedStorage() throws IOException {
    final byte[] wire = wire();
    final FrameSlotAllocator allocator = FrameSlotAllocator.getInstance();
    final CompactImageStore store = CompactImageStore.getInstance();
    final long budget = allocator.getMaxBufferSize();
    final long remaining =
        budget - budget / 8 - allocator.oversizedHeadroomBytes() - allocator.nonExecutionCommittedBytes();
    assertTrue(allocator.reserveSlabCommittedBytes(remaining));
    final List<Block> held = new ArrayList<>();
    try {
      for (Block block; (block = store.acquire(60000)) != null;) {
        held.add(block);
      }
      assertFallbackRead(wire);
    } finally {
      allocator.refundCommittedBytes(remaining);
      for (final Block block : held) {
        block.run();
      }
    }
    assertPooledRead(wire);
  }

  @Test
  void liveSlabShutdownRejectionRestoresPooledDecoding() throws IOException {
    final byte[] wire = wire();
    assertPooledRead(wire);
    final FrameSlotAllocator allocator = FrameSlotAllocator.getInstance();
    final MemorySegment other = allocator.allocateSlab(4096);
    assertNotNull(other);
    try {
      final IllegalStateException failure = assertThrows(IllegalStateException.class, allocator::shutdown);
      assertTrue(failure.getMessage().contains("live slab bytes"), failure.getMessage());
      assertPooledRead(wire);
    } finally {
      assertTrue(allocator.releaseSlab(other));
    }
  }

  @Test
  void foreignOversizedShutdownRejectionRestoresPooledDecoding() throws Exception {
    final byte[] wire = wire();
    assertPooledRead(wire);
    final FrameSlotAllocator allocator = FrameSlotAllocator.getInstance();
    final ExecutorService worker = Executors.newSingleThreadExecutor();
    final CountDownLatch allocated = new CountDownLatch(1);
    final CountDownLatch proceed = new CountDownLatch(1);
    final Future<?> owner = worker.submit(() -> {
      final MemorySegment oversized = allocator.allocate(512 * 1024);
      try {
        allocated.countDown();
        assertTrue(proceed.await(10, TimeUnit.SECONDS));
      } finally {
        allocator.release(oversized);
      }
      return null;
    });
    try {
      assertTrue(allocated.await(10, TimeUnit.SECONDS));
      final IllegalStateException failure = assertThrows(IllegalStateException.class, allocator::shutdown);
      assertTrue(failure.getMessage().contains("owned by"), failure.getMessage());
      assertPooledRead(wire);
    } finally {
      proceed.countDown();
      owner.get(10, TimeUnit.SECONDS);
      worker.shutdownNow();
      assertTrue(worker.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  private static void assertFallbackRead(final byte[] wire) throws IOException {
    final CompactImageStore store = CompactImageStore.getInstance();
    final int before = store.leasedBlockCount();
    final HOTLeafPage page = decode(wire);
    try {
      assertEquals(before, store.leasedBlockCount());
      assertEquals(60000L, page.slots().byteSize());
      assertValue(page);
    } finally {
      page.close();
    }
  }

  private static void assertPooledRead(final byte[] wire) throws IOException {
    final CompactImageStore store = CompactImageStore.getInstance();
    final int before = store.leasedBlockCount();
    final HOTLeafPage page = decode(wire);
    try {
      assertEquals(before + 1, store.leasedBlockCount());
      assertValue(page);
    } finally {
      page.close();
    }
    assertEquals(before, store.leasedBlockCount());
  }

  private static void assertValue(final HOTLeafPage page) {
    final byte[] expected = new byte[59996];
    Arrays.fill(expected, (byte) 42);
    assertArrayEquals(expected, page.copyStoredValue(page.findEntry(new byte[] {7})));
  }

  private static HOTLeafPage decode(final byte[] wire) throws IOException {
    return assertInstanceOf(HOTLeafPage.class, new PagePersister().deserializeHOTLeafFragment(CONFIG,
        new MemorySegmentBytesIn(MemorySegment.ofArray(wire)), SerializationType.DATA));
  }

  private static byte[] wire() throws IOException {
    final HOTLeafPage source = new HOTLeafPage(1, 7, IndexType.PROJECTION);
    try {
      final byte[] value = new byte[59996];
      Arrays.fill(value, (byte) 42);
      assertTrue(source.put(new byte[] {7}, value));
      assertEquals(60000, source.getUsedSlotsSize());
      source.setCompleteDump(true);
      try (MemorySegmentBytesOut out = new MemorySegmentBytesOut(128 * 1024)) {
        new PagePersister().serializePage(CONFIG, out, source, SerializationType.DATA);
        return out.toByteArray();
      }
    } finally {
      source.close();
    }
  }

  private static final class RefusingMemory implements VirtualMemory {
    private final VirtualMemory delegate = FrameSlotAllocator.virtualMemory();
    private final Failure failure;
    private final OutOfMemoryError heapFailure = new OutOfMemoryError("Java heap failure");
    private int releases;

    private RefusingMemory(final Failure failure) {
      this.failure = failure;
    }

    @Override
    public MemorySegment reserve(final long bytes) {
      return delegate.reserve(bytes);
    }

    @Override
    public @Nullable MemorySegment tryReserve(final long bytes) {
      if (failure == Failure.HEAP_RESERVE) {
        throw heapFailure;
      }
      return failure == Failure.RESERVE
          ? null
          : delegate.tryReserve(bytes);
    }

    @Override
    public boolean commitFresh(final MemorySegment segment) {
      return delegate.commitFresh(segment);
    }

    @Override
    public boolean tryCommitFresh(final MemorySegment segment) {
      if (failure == Failure.HEAP_COMMIT) {
        throw heapFailure;
      }
      return failure != Failure.COMMIT && delegate.tryCommitFresh(segment);
    }

    @Override
    public void discardToZeros(final MemorySegment segment) {
      delegate.discardToZeros(segment);
    }

    @Override
    public boolean release(final MemorySegment segment) {
      releases++;
      return delegate.release(segment);
    }

    @Override
    public int tryPopulate(final MemorySegment segment) {
      if (failure == Failure.HEAP_POPULATE) {
        throw heapFailure;
      }
      return failure == Failure.POPULATE
          ? POPULATE_REFUSED
          : delegate.tryPopulate(segment);
    }
  }
}
