package io.sirix.query.scan;

import io.sirix.index.projection.ProjectionIndexRowGroupPage;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.concurrent.atomic.LongAdder;

/**
 * Small per-thread heap scratch that kernels stream slab data through: one bulk copy fills a window
 * (checked once), then the kernel runs on plain arrays whose range checks the compiler hoists per
 * loop. Every window has a fixed maximum sized for the L2 cache, is allocated at most once per
 * thread and reused across chunks, leaves and queries; nothing here is retained per snapshot, and
 * the cached data itself stays in the slabs. A steady-state query grows no window, which
 * {@link #growthCount()} lets a work-budget test pin.
 */
public final class HeapWindows {
  /**
   * Longs of one leaf's lane block: {@value ProjectionIndexRowGroupPage#MAX_ROWS} rows of up to this
   * many fields.
   */
  public static final int MAX_LEAF_FIELDS = 16;
  public static final int LEAF_LONGS = ProjectionIndexRowGroupPage.MAX_ROWS * MAX_LEAF_FIELDS;
  /** Packed records per merge window: 80 KiB of build and 320 KiB of probe records. */
  public static final int BUILD_RECORDS = 2048;
  public static final int PROBE_RECORDS = 8192;
  /** Consecutive buckets per merge window. */
  public static final int WINDOW_SLOTS = 4096;
  private static final int RECORD_LONGS = 5;

  private static final ThreadLocal<HeapWindows> LOCAL = ThreadLocal.withInitial(HeapWindows::new);
  private static final LongAdder GROWTHS = new LongAdder();
  private static final long[] NONE = new long[0];
  private static final int[] NO_STARTS = new int[0];

  private long[] leaf = NONE;
  private long[] keys = NONE;
  private long[] presence = NONE;
  private long[] build = NONE;
  private long[] probe = NONE;
  private int[] buildStarts = NO_STARTS;
  private int[] probeStarts = NO_STARTS;

  private HeapWindows() {}

  /** The calling thread's windows. */
  public static HeapWindows current() {
    return LOCAL.get();
  }

  /** How often any window was (re)allocated on any thread; constant once a thread is warm. */
  public static long growthCount() {
    return GROWTHS.sum();
  }

  /** Bytes this thread's windows occupy; bounded by the fixed maxima. */
  public long capacityBytes() {
    return 8L * (leaf.length + keys.length + presence.length + build.length + probe.length)
        + 4L * (buildStarts.length + probeStarts.length);
  }

  private static long[] atLeast(final long[] window, final int longs, final int maximum) {
    if (longs > maximum) {
      throw new IllegalArgumentException("window request " + longs + " exceeds the fixed maximum " + maximum);
    }
    if (window.length >= longs) {
      return window;
    }
    GROWTHS.increment();
    return new long[maximum];
  }

  private static int[] atLeast(final int[] window, final int ints, final int maximum) {
    if (ints > maximum) {
      throw new IllegalArgumentException("window request " + ints + " exceeds the fixed maximum " + maximum);
    }
    if (window.length >= ints) {
      return window;
    }
    GROWTHS.increment();
    return new int[maximum];
  }

  /** A window for one leaf's row-major lane block of {@code longs} longs. */
  public long[] leaf(final int longs) {
    return leaf = atLeast(leaf, longs, LEAF_LONGS);
  }

  /** A window for one leaf's record keys. */
  public long[] keys(final int longs) {
    return keys = atLeast(keys, longs, ProjectionIndexRowGroupPage.MAX_ROWS);
  }

  /** A window for one leaf's presence words. */
  public long[] presence(final int words) {
    return presence = atLeast(presence, words, ProjectionIndexRowGroupPage.MAX_ROWS + 63 >>> 6);
  }

  /** A window for packed build records of the merge. */
  public long[] build(final int longs) {
    return build = atLeast(build, longs, BUILD_RECORDS * RECORD_LONGS);
  }

  /** A window for packed probe records of the merge. */
  public long[] probe(final int longs) {
    return probe = atLeast(probe, longs, PROBE_RECORDS * RECORD_LONGS);
  }

  /** Bucket starts of the build side within a merge window ({@code slots + 1} entries). */
  public int[] buildStarts(final int slots) {
    return buildStarts = atLeast(buildStarts, slots + 1, WINDOW_SLOTS + 1);
  }

  public int[] probeStarts(final int slots) {
    return probeStarts = atLeast(probeStarts, slots + 1, WINDOW_SLOTS + 1);
  }

  /** Copy {@code longs} longs at byte offset {@code offset} of {@code slab} into {@code window}. */
  public static void copy(final MemorySegment slab, final long offset, final long[] window, final int longs) {
    MemorySegment.copy(slab, ValueLayout.JAVA_LONG, offset, window, 0, longs);
  }
}
