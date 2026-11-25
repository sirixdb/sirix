/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.budget;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.sun.management.ThreadMXBean;
import io.sirix.access.ResourceConfiguration;
import io.sirix.cache.CompactImageStore;
import io.sirix.index.IndexType;
import io.sirix.index.hot.page.HOTLeafPage;
import io.sirix.io.bytepipe.ByteHandlerPipeline;
import io.sirix.node.MemorySegmentBytesIn;
import io.sirix.node.MemorySegmentBytesOut;
import io.sirix.page.PagePersister;
import io.sirix.page.SerializationType;
import io.sirix.page.interfaces.Page;
import io.sirix.settings.VersioningType;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.management.ManagementFactory;
import java.util.Arrays;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

/**
 * The storage a compact HOT-leaf image is decoded into must cost no allocation per decode.
 *
 * <p>
 * It used to cost one {@code Arena.ofAuto()} per decoded fragment on the per-fragment read path: a
 * 24-byte arena object whenever escape analysis fails to remove it — which is decided per
 * compilation, so 11 of 12 benchmark forks paid 994.5 bytes per operation and one paid 1018.4 —
 * plus a session and a {@code Cleaner} registration whose bookkeeping follows GC timing. The pooled
 * block the image now owns is leased and returned without allocating, which is what this test
 * bounds. The automatic arena is kept as the positive control, so the zero bound cannot be vacuous.
 * </p>
 */
@Isolated
@DisplayName("Compact-image storage is leased and returned without allocating")
final class CompactImageStorageAllocationBudgetTest {

  private static final ResourceConfiguration CONFIG =
      ResourceConfiguration.newBuilder("compact-image-allocation")
                           .versioningApproach(VersioningType.FULL)
                           .byteHandlerPipeline(new ByteHandlerPipeline())
                           .build();

  /** The typical compact image is around a kilobyte; the class above it is 1 KiB. */
  private static final int IMAGE_BYTES = 1000;

  private static final int WARMUP_ROUNDS = 20_000;

  private static final int MEASURED_ROUNDS = 100_000;

  /** Keeps a measured arena's segment from being scalar-replaced in the control leg. */
  private static volatile MemorySegment sink;

  @Test
  @DisplayName("a warmed lease and release of a compact-image block allocates nothing")
  void leasingAndReturningABlockAllocatesNothing() {
    final ThreadMXBean bean = allocationBean();
    final CompactImageStore store = CompactImageStore.getInstance();
    assumeTrue(leaseCycle(store, 1) == 1, "the pool serves this machine's allocator budget");

    leaseCycle(store, WARMUP_ROUNDS);
    // Three legs, and the smallest counts: a compilation or deoptimisation landing inside one leg
    // charges this thread a few hundred bytes of the JIT's own bookkeeping, which is not per lease
    // and does not survive into the next leg. An allocation on the lease path would be in all three
    // (a single boxed int per round would be 1.6 MB).
    final long[] legs = new long[3];
    for (int leg = 0; leg < legs.length; leg++) {
      final long before = bean.getCurrentThreadAllocatedBytes();
      final int leased = leaseCycle(store, MEASURED_ROUNDS);
      legs[leg] = bean.getCurrentThreadAllocatedBytes() - before;
      assertEquals(MEASURED_ROUNDS, leased, "every round leased a block");
    }
    final long allocated = Math.min(legs[0], Math.min(legs[1], legs[2]));

    assertEquals(0L, allocated, "a warmed lease/release cycle must allocate no heap; " + MEASURED_ROUNDS
        + " rounds allocated " + Arrays.toString(legs) + " bytes");
  }

  @Test
  @DisplayName("the automatic arena this replaced does allocate, so the zero bound means something")
  void theAutomaticArenaControlAllocates() {
    final ThreadMXBean bean = allocationBean();
    arenaCycle(WARMUP_ROUNDS);
    final long before = bean.getCurrentThreadAllocatedBytes();
    arenaCycle(MEASURED_ROUNDS);
    final long allocated = bean.getCurrentThreadAllocatedBytes() - before;
    assertTrue(allocated > 0L, "one automatic arena per image allocates; measured " + allocated + " bytes");
  }

  @Test
  @DisplayName("a compact fragment decode takes its storage from the pool, which is what the bound measures")
  void aDecodeTakesItsStorageFromThePool() throws IOException {
    final CompactImageStore store = CompactImageStore.getInstance();
    final byte[] wire = compactWire();
    final int leasedBefore = store.leasedBlockCount();
    final Page page = new PagePersister().deserializeHOTLeafFragment(CONFIG,
        new MemorySegmentBytesIn(MemorySegment.ofArray(wire)), SerializationType.DATA);
    final HOTLeafPage compact = assertInstanceOf(HOTLeafPage.class, page);
    try {
      assertTrue(compact.isCompactImage());
      assertEquals(leasedBefore + 1, store.leasedBlockCount(), "the decode leased exactly one block");
    } finally {
      compact.close();
    }
    assertEquals(leasedBefore, store.leasedBlockCount(), "and gave it back when the page closed");
  }

  private static int leaseCycle(final CompactImageStore store, final int rounds) {
    int leased = 0;
    for (int i = 0; i < rounds; i++) {
      final CompactImageStore.Block block = store.acquire(IMAGE_BYTES);
      if (block == null) {
        break;
      }
      leased++;
      block.run();
    }
    return leased;
  }

  private static void arenaCycle(final int rounds) {
    for (int i = 0; i < rounds; i++) {
      sink = Arena.ofAuto().allocate(IMAGE_BYTES, 8L);
    }
  }

  private static ThreadMXBean allocationBean() {
    final var platformBean = ManagementFactory.getThreadMXBean();
    assumeTrue(platformBean instanceof ThreadMXBean);
    final ThreadMXBean bean = (ThreadMXBean) platformBean;
    assumeTrue(bean.isThreadAllocatedMemorySupported());
    if (!bean.isThreadAllocatedMemoryEnabled()) {
      bean.setThreadAllocatedMemoryEnabled(true);
    }
    return bean;
  }

  /** A leaf whose packed image is about {@link #IMAGE_BYTES} bytes. */
  private static byte[] compactWire() throws IOException {
    final HOTLeafPage source = new HOTLeafPage(1, 3, IndexType.PROJECTION);
    try {
      for (int i = 0; i < 64; i++) {
        final byte[] key = {9, (byte) (i >>> 7), (byte) (i << 1)};
        assertTrue(source.put(key, new byte[] {(byte) i, (byte) (i >>> 8)}));
      }
      source.setCompleteDump(true);
      try (MemorySegmentBytesOut out = new MemorySegmentBytesOut(128 * 1024)) {
        new PagePersister().serializePage(CONFIG, out, source, SerializationType.DATA);
        final byte[] wire = out.toByteArray();
        assertNotNull(wire);
        return wire;
      }
    } finally {
      source.close();
    }
  }
}
