/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import com.sun.management.ThreadMXBean;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.lang.management.ManagementFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * MEM-301: on a warm pool a lease, a release and a decode allocate no Java object and reserve no
 * budget (CONV-301). The bytes are counted per thread, never timed. The seam's own calls are
 * measured as a control, and an allocating operation as a positive control, so a zero cannot come
 * from a harness that sees nothing.
 *
 * <p>
 * Planned (step 3): the pool does not exist yet and is bound through {@link MemCompactSeam}. The
 * CONV-302 comparison on HOT read suites is a timed gate and is not part of this class.
 */
@MemPlanned(step = 3, value = {"MEM-301"})
@Isolated
@ResourceLock("FrameSlotAllocator.pressureListener")
final class CompactImageAllocationBudgetTest {

  private static final int OPERATIONS = 4_096;
  private static final int PASSES = 9;
  private static final int SHARE_UNITS = 4;
  private static final int KEYS = 128;

  private static volatile Object sink;
  private static volatile long longSink;

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

  @Test
  @DisplayName("control: the seam's accessors allocate nothing, and the harness sees a real allocation")
  void controlsProveTheHarnessSeesAllocation() {
    final ThreadMXBean bean = allocationBean();
    final long unit = MemCompactSeam.slabUnitBytes();
    final FrameSlotAllocator allocator = new FrameSlotAllocator(Math.max(32L * 1024 * 1024, 8L * unit));
    final MemCompactSeam.Pool pool = MemCompactSeam.create(allocator, SHARE_UNITS * unit);
    try {
      final long thread = Thread.currentThread().threadId();
      long accessorBytes = 0;
      long allocatingBytes = 0;
      for (int pass = 0; pass < PASSES; pass++) {
        accessorBytes = 0;
        allocatingBytes = 0;
        for (int i = 0; i < OPERATIONS; i++) {
          final long before = bean.getThreadAllocatedBytes(thread);
          longSink = pool.slabBytes() + pool.accountBytes();
          accessorBytes += bean.getThreadAllocatedBytes(thread) - before;
          final long beforeAllocating = bean.getThreadAllocatedBytes(thread);
          sink = new byte[256];
          allocatingBytes += bean.getThreadAllocatedBytes(thread) - beforeAllocating;
        }
      }
      assertTrue(allocatingBytes >= (long) OPERATIONS * 256,
          "positive control: a 256-byte array per operation is seen");
      assertEquals(0L, accessorBytes, "control: reading the account through the seam allocates nothing");
    } finally {
      pool.shutdown();
      allocator.shutdown();
    }
  }

  @Test
  @DisplayName("MEM-301: lease and release on a warm pool allocate nothing and reserve nothing")
  void warmLeaseAndReleaseAllocateAndReserveNothing() {
    final ThreadMXBean bean = allocationBean();
    final long unit = MemCompactSeam.slabUnitBytes();
    final FrameSlotAllocator allocator = new FrameSlotAllocator(Math.max(32L * 1024 * 1024, 8L * unit));
    final MemCompactSeam.Pool pool = MemCompactSeam.create(allocator, SHARE_UNITS * unit);
    try {
      final long leaseBytes = unit / 16;
      final Object warm = pool.lease(leaseBytes);
      assertNotNull(warm, "the fixture must map a slab");
      pool.close(warm);
      final long thread = Thread.currentThread().threadId();
      long bytes = 0;
      long committedBefore = 0;
      for (int pass = 0; pass < PASSES; pass++) {
        bytes = 0;
        committedBefore = allocator.getPhysicalMemoryBytes();
        for (int i = 0; i < OPERATIONS; i++) {
          bytes += leaseAndReleaseAllocatedBytes(pool, bean, thread, leaseBytes);
        }
      }
      assertEquals(0L, bytes, "MEM-301 lease and release on a warm pool must not allocate");
      assertEquals(committedBefore, allocator.getPhysicalMemoryBytes(),
          "MEM-301 lease and release on a warm pool must not reserve budget");
      assertNotNull(sink, "the measured leases must be real");
    } finally {
      pool.shutdown();
      allocator.shutdown();
    }
  }

  @Test
  @DisplayName("MEM-301: a decode (cache hit, and miss with eviction) on a warm pool allocates nothing")
  void warmDecodeAllocatesNothing() {
    final ThreadMXBean bean = allocationBean();
    final long unit = MemCompactSeam.slabUnitBytes();
    final FrameSlotAllocator allocator = new FrameSlotAllocator(Math.max(32L * 1024 * 1024, 8L * unit));
    final MemCompactSeam.Pool pool = MemCompactSeam.create(allocator, SHARE_UNITS * unit);
    try {
      final long imageBytes = unit / 16;
      final long thread = Thread.currentThread().threadId();
      // Hits: the same key over and over.
      long hitBytes = 0;
      for (int pass = 0; pass < PASSES; pass++) {
        hitBytes = 0;
        for (int i = 0; i < OPERATIONS; i++) {
          hitBytes += decodeAllocatedBytes(pool, bean, thread, 0L, imageBytes);
        }
      }
      assertEquals(0L, hitBytes, "MEM-301 a decode served from the warm cache must not allocate");
      // Misses: more keys than the account can hold, so every pass evicts and decodes into reused leases.
      long missBytes = 0;
      final long evictedBefore = pool.evictedImageBytes();
      for (int pass = 0; pass < PASSES; pass++) {
        missBytes = 0;
        for (int i = 0; i < OPERATIONS; i++) {
          missBytes += decodeAllocatedBytes(pool, bean, thread, i % KEYS, imageBytes);
        }
      }
      assertTrue(pool.evictedImageBytes() > evictedBefore,
          "the miss loop must exercise eviction, or the zero below measures only hits");
      assertEquals(0L, missBytes, "MEM-301 a decode that evicts and reuses a pool lease must not allocate");
    } finally {
      pool.shutdown();
      allocator.shutdown();
    }
  }

  private static long leaseAndReleaseAllocatedBytes(final MemCompactSeam.Pool pool, final ThreadMXBean bean,
      final long thread, final long leaseBytes) {
    final long before = bean.getThreadAllocatedBytes(thread);
    final Object lease = pool.lease(leaseBytes);
    sink = lease;
    pool.close(lease);
    return bean.getThreadAllocatedBytes(thread) - before;
  }

  private static long decodeAllocatedBytes(final MemCompactSeam.Pool pool, final ThreadMXBean bean, final long thread,
      final long key, final long imageBytes) {
    final long before = bean.getThreadAllocatedBytes(thread);
    final Object decoded = pool.decode(key, imageBytes);
    sink = decoded;
    pool.closeDecoded(decoded);
    return bean.getThreadAllocatedBytes(thread) - before;
  }
}
