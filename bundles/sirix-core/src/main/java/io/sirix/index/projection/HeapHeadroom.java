package io.sirix.index.projection;

import com.sun.management.GarbageCollectorMXBean;
import com.sun.management.GcInfo;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.util.List;
import java.util.Map;

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
  /** Kill switch: {@code -Dsirix.heapHeadroom.boundByUsage=false} takes the post-collection figures as they are. */
  private static final boolean BOUND_BY_USAGE =
      !"false".equalsIgnoreCase(System.getProperty("sirix.heapHeadroom.boundByUsage", "true"));
  /** Kill switch: {@code -Dsirix.heapHeadroom.lastGcInfo=false} reads the pools' own post-collection figures only. */
  private static final boolean LAST_GC_INFO =
      !"false".equalsIgnoreCase(System.getProperty("sirix.heapHeadroom.lastGcInfo", "true"));
  private static volatile long headroomForTesting = -1L;

  private HeapHeadroom() {}

  /**
   * Bytes in the heap pools that survived the last collection, {@code 0} before any collection —
   * each pool bounded by its CURRENT usage, see {@link #liveBound(long, long)}.
   *
   * <p>
   * Two records of the heap exist and the figure is the SMALLER: the after-collection usage the most
   * recent pause of ANY collector recorded ({@link #liveAfterLastCollection()}) and the pools' own
   * post-collection figures ({@link #liveAfterLastPoolCollection()}). Each is the live set plus a
   * different kind of not-yet-collected garbage. A young pause's record carries everything the running
   * query had PROMOTED — its group tables and decoded windows, dead a pass later but in the old
   * generation until a mixed pause — so read alone it held the per-pass group budget at a third of
   * its size for the whole query after a large aggregate (q31 at 100M: two hash-range passes where
   * one held). The old generation's own figure dates from the last mixed pause and carries the
   * humongous arrays that died at the next young one. Neither record is ever below the live set of
   * its instant, so the smaller is the tighter account of what a budget must leave alone; the current
   * usage bound catches the release of what either record still counts.
   * </p>
   */
  public static long liveAfterLastGc() {
    final long pools = liveAfterLastPoolCollection();
    if (!LAST_GC_INFO) {
      return Math.max(0L, pools);
    }
    final long latest = liveAfterLastCollection();
    if (latest < 0L) {
      return Math.max(0L, pools);
    }
    return pools < 0L
        ? latest
        : Math.min(latest, pools);
  }

  /**
   * The heap pools' usage after the most recent collection of any collector, summed and bounded per
   * pool by the current usage — or {@code -1} before the first collection and where the platform has
   * no collection records.
   *
   * <p>
   * A collector's record of a collection carries the usage of EVERY pool it manages as the pause
   * left it, so under G1 a young pause reports the old generation with the humongous arrays it eagerly
   * reclaimed already gone. The pools' own post-collection figures do not: see
   * {@link #liveAfterLastPoolCollection()}. Between collections the figure ages, and what was
   * allocated since is not in it — the budgets derived from it are planned against a heap that the
   * next pause will have cleared of garbage, which is the heap they run in.
   * </p>
   */
  static long liveAfterLastCollection() {
    final List<GarbageCollectorMXBean> collectors;
    try {
      collectors = ManagementFactory.getPlatformMXBeans(GarbageCollectorMXBean.class);
    } catch (final IllegalArgumentException notAPlatformExtension) {
      return -1L;
    }
    GcInfo latest = null;
    for (final GarbageCollectorMXBean collector : collectors) {
      final GcInfo info = collector.getLastGcInfo();
      if (info != null && (latest == null || info.getEndTime() > latest.getEndTime())) {
        latest = info;
      }
    }
    if (latest == null) {
      return -1L;
    }
    final Map<String, MemoryUsage> afterGc = latest.getMemoryUsageAfterGc();
    long live = 0L;
    for (final MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
      if (pool.getType() != MemoryType.HEAP) {
        continue;
      }
      final MemoryUsage after = afterGc.get(pool.getName());
      if (after == null) {
        continue;
      }
      live += liveBound(after.getUsed(), currentUsedBound(pool));
    }
    return live;
  }

  /**
   * The sum over the heap pools of each pool's own post-collection usage, bounded by its current
   * usage; {@code -1} where no heap pool keeps such a figure ({@code 0} before any collection).
   */
  static long liveAfterLastPoolCollection() {
    long live = 0L;
    boolean any = false;
    final List<MemoryPoolMXBean> pools = ManagementFactory.getMemoryPoolMXBeans();
    for (final MemoryPoolMXBean pool : pools) {
      if (pool.getType() != MemoryType.HEAP || !pool.isCollectionUsageThresholdSupported()) {
        continue;
      }
      final MemoryUsage afterGc = pool.getCollectionUsage();
      if (afterGc == null) {
        continue;
      }
      any = true;
      live += liveBound(afterGc.getUsed(), currentUsedBound(pool));
    }
    return any
        ? live
        : -1L;
  }

  /** The pool's current usage as the live figure's bound, or no bound with the switch off. */
  private static long currentUsedBound(final MemoryPoolMXBean pool) {
    if (!BOUND_BY_USAGE) {
      return Long.MAX_VALUE;
    }
    final MemoryUsage now = pool.getUsage();
    return now != null
        ? now.getUsed()
        : Long.MAX_VALUE;
  }

  /**
   * The tighter of a pool's post-collection usage and its current usage, never negative.
   *
   * <p>
   * A pool records its own post-collection usage only when a collection COVERED the pool. Under G1
   * the old generation's figure dates from the last mixed or full pause, and that pause may have run
   * while a query held gigabytes of group tables — humongous arrays that die at the next YOUNG pause
   * (eager reclaim) without the figure moving. The stale figure then reads as live for every query
   * until the next mixed pause: at 100M the per-pass group budget fell from 12.6M to 3.4M groups over
   * the six queries after a large aggregate, and each ran four to eight hash-range passes where two
   * held. A pool's current usage is never below its live set, so where it is below the recorded
   * figure the recorded figure is stale and the current usage is the tighter bound. The bound is
   * one-sided: a heap full of not-yet-collected garbage reads as live (the same leg saw a budget of
   * 1.5M groups from 11.2 GB of garbage), which is why the collection records are read first.
   * </p>
   *
   * @param afterGc the pool's usage after its last collection, in bytes
   * @param now the pool's current usage, in bytes
   * @return {@code max(0, min(afterGc, now))}
   */
  static long liveBound(final long afterGc, final long now) {
    return Math.max(0L, Math.min(afterGc, now));
  }

  /**
   * One line naming every figure a budget derives from, in MB: the two collector records, their
   * minimum, the current usage, the maximum heap and the planned share. Diagnostics only — a budget
   * that shrinks between two queries of one leg is explained by which record moved, and a bare
   * budget cannot say that.
   */
  public static String describe() {
    final Runtime runtime = Runtime.getRuntime();
    final long max = runtime.maxMemory();
    final long used = runtime.totalMemory() - runtime.freeMemory();
    final long latest = liveAfterLastCollection();
    final long pools = liveAfterLastPoolCollection();
    final long live = liveAfterLastGc();
    return "liveMB=" + (live >> 20) + " latestGcMB=" + (latest >> 20) + " poolsGcMB=" + (pools >> 20) + " usedMB="
        + (used >> 20) + " maxMB=" + (max >> 20) + " shareMB=" + (plannedShareBytes() >> 20) + " pooledMB="
        + (LongChunkPool.retainedBytes() >> 20);
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
   * The ONE heap share every projection-side budget plans against: the smaller of an eighth of the
   * maximum heap and a quarter of the current headroom.
   *
   * <p>
   * Three consumers size themselves from it and must not disagree about it — the per-pass group table
   * ({@link GroupTableSpill#groupBudgetFor}), the grouped {@code COUNT(DISTINCT)} ceiling
   * ({@link GroupDistinctAccumulator#defaultMaxValuesFor}) and, since R1, the column store's RETAINED
   * fill total ({@code ProjectionColumnStore}'s residency budget). They fight for one heap: while a
   * store retained every fill of every earlier query for its whole lifetime and the group tables
   * planned against the headroom that was left, the group side paid for the residency side's memory
   * in extra hash-range passes. One figure, three consumers, so raising the share raises all three
   * together and releasing residency raises what the group side may plan.
   * </p>
   *
   * @return the planned per-consumer share of the heap, in bytes
   */
  public static long plannedShareBytes() {
    return plannedShareBytes(Runtime.getRuntime().maxMemory(), headroomBytes());
  }

  /**
   * The share for an explicit {@code maxMemory} and {@code headroom} (pure, for tests and for the
   * derived budgets' own pure twins).
   *
   * @param maxMemory the maximum heap in bytes
   * @param headroom the headroom in bytes
   * @return {@code min(maxMemory / 8, headroom / 4)}, never negative
   */
  public static long plannedShareBytes(final long maxMemory, final long headroom) {
    return Math.max(0L, Math.min(maxMemory / 8L, headroom / 4L));
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
