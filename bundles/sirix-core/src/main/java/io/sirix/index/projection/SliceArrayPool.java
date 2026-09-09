package io.sirix.index.projection;

import io.sirix.index.projection.ProjectionColumnStore.ColumnSlice;

import java.util.concurrent.atomic.LongAdder;

/**
 * Recycles the presence and value arrays of a windowed access's long-lane slices.
 *
 * <p>
 * A windowed group-by pass decodes every leaf of every needed column into a fresh
 * {@code long[rowCount]} plus its presence words, keeps the slice for two windows in the access's
 * LRU and drops it — at 100M a four-column composite pass minted 3.2 GB of such arrays PER PASS
 * (q32: 8 passes × 97,654 leaves × 4 columns × 8 KB), all of it dying young and driving 80+
 * collections per try. The lifetime is known exactly: an array is dead the moment its slice leaves
 * the LRU, so the access hands the evicted slice here and the next window's decode takes the arrays
 * back instead of allocating. Not thread-safe — one pool per access, like the access itself.
 * </p>
 *
 * <p>
 * <b>Exact lengths only.</b> Consumers read {@code numericValues().length} as the row count, so a
 * recycled array is handed out only for a request of its exact length; a mismatch (the store's
 * last, short leaf) allocates. Both lanes are single stacks whose top is popped on a length match
 * and dropped otherwise — every leaf but one has {@link ProjectionIndexRowGroupPage#MAX_ROWS} rows,
 * so a stack of uniform lengths is the common case and the odd length costs one allocation.
 * </p>
 *
 * <p>
 * <b>Only the decoders zero what they need.</b> {@code decodePresenceInto} fills every word in
 * every mode and the FOR/ALP decoders overwrite every cell, so a recycled array carries no stale
 * state into a slice. A decoder that writes only "the set bits" must not be handed one.
 * </p>
 */
final class SliceArrayPool {
  /** Stack depth per lane: two windows per column of a handful of columns need far less. */
  static final int MAX_FREE = 1024;
  private static final LongAdder REUSED = new LongAdder();

  private long[][] presenceFree = new long[16][];
  private int presenceCount;
  private long[][] valuesFree = new long[16][];
  private int valuesCount;

  /** Test observability: arrays handed out from a pool instead of allocated, process-wide. */
  static long reusedCount() {
    return REUSED.sum();
  }

  /** A presence array of exactly {@code words} longs; recycled when the stack's top matches. */
  long[] presence(final int words) {
    if (presenceCount > 0) {
      final long[] top = presenceFree[--presenceCount];
      presenceFree[presenceCount] = null;
      if (top.length == words) {
        REUSED.increment();
        return top;
      }
    }
    return new long[words];
  }

  /** A values array of exactly {@code rowCount} longs; recycled when the stack's top matches. */
  long[] values(final int rowCount) {
    if (valuesCount > 0) {
      final long[] top = valuesFree[--valuesCount];
      valuesFree[valuesCount] = null;
      if (top.length == rowCount) {
        REUSED.increment();
        return top;
      }
    }
    return new long[rowCount];
  }

  /**
   * Take back the long-lane arrays of a slice that left its access's cache. Only the arrays this pool
   * hands out are taken: presence words and numeric values; dictionary and boolean lanes are
   * untouched. The caller guarantees no consumer still reads the slice — the windowed contract (fill,
   * kernel, release, then the next window) is what makes eviction that guarantee.
   */
  void recycle(final ColumnSlice slice) {
    final long[] presence = slice.presenceWords();
    if (presence != null && presence.length > 0 && presenceCount < MAX_FREE) {
      if (presenceCount == presenceFree.length) {
        presenceFree = grow(presenceFree);
      }
      presenceFree[presenceCount++] = presence;
    }
    final long[] values = slice.numericValues();
    if (values != null && values.length > 0 && valuesCount < MAX_FREE) {
      if (valuesCount == valuesFree.length) {
        valuesFree = grow(valuesFree);
      }
      valuesFree[valuesCount++] = values;
    }
  }

  /** Arrays currently held, both lanes (tests). */
  int held() {
    return presenceCount + valuesCount;
  }

  private static long[][] grow(final long[][] stack) {
    final long[][] bigger = new long[Math.min(MAX_FREE, stack.length << 1)][];
    System.arraycopy(stack, 0, bigger, 0, stack.length);
    return bigger;
  }
}
