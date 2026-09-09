package io.sirix.index.projection;

import java.util.Arrays;

/**
 * Worker-local direct addressing for a bounded numeric key range. Leaf zone bounds choose the range
 * without reading rows. The ordinary table remains the owner of every accumulator, including pass
 * filtering and first-seen order; this cache only avoids repeated probes within a leaf.
 */
final class NumericGroupLookup {
  private static final int MAX_RANGE = 4096;

  private int[] handles;
  private long minimum;
  private int range;
  private int rehashes;

  void beginLeaf(final long min, final long max, final int rows, final NumericGroupAggTable table) {
    final long distance = max - min;
    // Subtraction may overflow across the signed-long boundary. Sparse/wide ranges keep hashing;
    // allocating and clearing a cache needs at least two rows per possible key to be worthwhile.
    range = min <= max && distance >= 0 && distance < MAX_RANGE && distance < rows / 2
        ? (int) distance + 1
        : 0;
    minimum = min;
    rehashes = table.rehashes();
    if (range > 0) {
      if (handles == null || handles.length < range) {
        handles = new int[range];
      }
      Arrays.fill(handles, 0, range, -1);
    }
  }

  /** Nonzero keys only, matching {@link NumericGroupAggTable#acquire(long, long)}. */
  int acquire(final NumericGroupAggTable table, final long key, final long ordinal) {
    final long offset = key - minimum;
    if (range == 0 || offset < 0 || offset >= range) {
      return table.acquire(key, ordinal);
    }
    // Interleaved-table growth moves handles; dense-table growth can change their array mapping.
    // Cache only handles, resolve storage after acquisition, and invalidate on either kind of growth.
    if (rehashes != table.rehashes()) {
      Arrays.fill(handles, 0, range, -1);
      rehashes = table.rehashes();
    }
    final int index = (int) offset;
    final int cached = handles[index];
    if (cached >= 0) {
      return cached;
    }
    final int handle = table.acquire(key, ordinal);
    if (rehashes != table.rehashes()) {
      Arrays.fill(handles, 0, range, -1);
      rehashes = table.rehashes();
    }
    handles[index] = handle;
    return handle;
  }
}
