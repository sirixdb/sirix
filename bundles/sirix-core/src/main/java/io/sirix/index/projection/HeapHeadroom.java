package io.sirix.index.projection;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.util.List;

/**
 * The heap headroom the executor's per-query budgets may plan against: the maximum heap minus what
 * was LIVE after the last collection, not the maximum heap alone.
 *
 * <p>
 * A leg of queries retains state across queries — resident column fills, charged fingerprint chains,
 * payload windows, catalog descriptors — so a budget derived from {@code maxMemory} alone plans the
 * same group-table size for a query that runs first as for one that runs after 5.9 GB of an 8 GB heap
 * is already live; the latter dies in a worker with {@code OutOfMemoryError} instead of splitting the
 * key space into more passes (q32 at 100M/8 GB, second try inside a leg). The post-collection usage
 * of the heap pools is the JVM's own account of what is live; garbage is not counted.
 * </p>
 */
public final class HeapHeadroom {
  private static volatile long headroomForTesting = -1L;

  private HeapHeadroom() {}

  /** Bytes in the heap pools that survived their last collection, {@code 0} before any collection. */
  public static long liveAfterLastGc() {
    long live = 0L;
    final List<MemoryPoolMXBean> pools = ManagementFactory.getMemoryPoolMXBeans();
    for (final MemoryPoolMXBean pool : pools) {
      if (pool.getType() != MemoryType.HEAP || !pool.isCollectionUsageThresholdSupported()) {
        continue;
      }
      final MemoryUsage afterGc = pool.getCollectionUsage();
      if (afterGc != null) {
        live += Math.max(0L, afterGc.getUsed());
      }
    }
    return live;
  }

  /** {@code maxMemory - liveAfterLastGc()}, never negative; the test override when set. */
  public static long headroomBytes() {
    final long testing = headroomForTesting;
    if (testing >= 0L) {
      return testing;
    }
    return Math.max(0L, Runtime.getRuntime().maxMemory() - liveAfterLastGc());
  }

  /**
   * Test seam: pin the headroom so a budget derivation can be exercised deterministically.
   *
   * @param value the headroom in bytes, or a negative value to restore the derived default
   * @return the previous override, for restoring in a finally block
   */
  public static long setHeadroomForTesting(final long value) {
    final long previous = headroomForTesting;
    headroomForTesting = value;
    return previous;
  }
}
