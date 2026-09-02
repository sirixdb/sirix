package io.sirix.index.projection;

import io.sirix.index.projection.ProjectionColumnStore.ColumnSlice;
import it.unimi.dsi.fastutil.ints.IntArrays;
import org.jspecify.annotations.Nullable;

import java.util.Arrays;

/**
 * The {@code k} best rows of a bounded sorted scan under its total order — per-key direction, then
 * document rank — kept as a max-heap whose root is the WORST kept row, so a candidate enters exactly
 * when it beats the root. Self-contained: string keys are held as their materialized bytes, so no
 * comparison reaches back into a leaf's dictionary (the previous heap packed {@code (leaf, dictId)}
 * refs and resolved both operands' entries through the leaf access on EVERY comparison — which tied
 * the heap to a resident or windowed column and made a merge of two heaps impossible). Bytes are
 * copied once, on ACCEPT; the compare path reads them in place from the candidate's slice.
 *
 * <p>
 * Two heaps with the same shape MERGE exactly ({@link #mergeFrom}): the top-k of a union is the top-k
 * of the union of the parts' top-k's, so a scan can select per worker and combine. Not thread-safe.
 * </p>
 */
final class TopKHeap {
  /** Sort-key kind: a raw long, compared directly (numeric and temporal kinds). */
  static final byte KEY_NUMERIC = 0;

  /** Sort-key kind: a per-leaf dictionary string whose bytes compare unsigned. */
  static final byte KEY_STRING_BYTES = 1;

  /** Sort-key kind: as {@link #KEY_STRING_BYTES}, but a supplementary character forces decoding. */
  static final byte KEY_STRING_COLLATED = 2;

  /** Sort-key kind: a resource-wide dictionary id resolved through a revision-bound read view. */
  static final byte KEY_STRING_GLOBAL = 3;

  private final int k;
  private final int keyCount;
  private final byte[] keyKind;
  private final boolean[] descending;
  private final GlobalValueDictionary.ReadView[] globalViews;
  /** Row-major {@code k * keyCount}: the long of a numeric key, the id of a global one; unused for string kinds. */
  private final long[] tuple;
  /** Row-major {@code k * keyCount}: the bytes of a string key; {@code null} rows when no key is a string. */
  private final byte @Nullable [] @Nullable [] strKey;
  private final long[] recordKey;
  private final long[] rank;
  private int size;

  TopKHeap(final int k, final byte[] keyKind, final boolean[] descending,
      final GlobalValueDictionary.ReadView[] globalViews) {
    if (k <= 0) {
      throw new IllegalArgumentException("k must be positive: " + k);
    }
    if (keyKind == null || descending == null || keyKind.length != descending.length || keyKind.length == 0) {
      throw new IllegalArgumentException("keyKind and descending must be non-empty and aligned");
    }
    this.k = k;
    this.keyCount = keyKind.length;
    this.keyKind = keyKind;
    this.descending = descending;
    this.globalViews = globalViews;
    this.tuple = new long[k * keyCount];
    boolean anyString = false;
    for (final byte kind : keyKind) {
      anyString |= kind == KEY_STRING_BYTES || kind == KEY_STRING_COLLATED;
    }
    this.strKey = anyString
        ? new byte[k * keyCount][]
        : null;
    this.recordKey = new long[k];
    this.rank = new long[k];
  }

  int size() {
    return size;
  }

  boolean full() {
    return size == k;
  }

  /** Forget every kept row; the arrays stay for reuse. */
  void clear() {
    if (strKey != null) {
      Arrays.fill(strKey, 0, size * keyCount, null);
    }
    size = 0;
  }

  /**
   * Offer row {@code rowIdx} of the leaf whose sort-column slices are {@code leafSort} (aligned to the
   * keys). Enters when the heap is not yet full or the row beats the worst kept one.
   *
   * @return whether the row was kept
   */
  boolean offer(final ColumnSlice[] leafSort, final int rowIdx, final long key, final long rowRank) {
    if (size < k) {
      final int slot = size++;
      store(slot, leafSort, rowIdx, key, rowRank);
      if (size == k) {
        for (int i = (k >>> 1) - 1; i >= 0; i--) {
          siftDown(i);
        }
      }
      return true;
    }
    if (compareCandidate(leafSort, rowIdx, rowRank, 0) >= 0) {
      return false;
    }
    store(0, leafSort, rowIdx, key, rowRank);
    siftDown(0);
    return true;
  }

  /** Fold every row {@code other} kept into this heap; {@code other} is left unchanged. */
  void mergeFrom(final TopKHeap other) {
    if (other.keyCount != keyCount || other.k != k) {
      throw new IllegalArgumentException("Heaps of different shape cannot merge");
    }
    for (int i = 0; i < other.size; i++) {
      if (size < k) {
        final int slot = size++;
        copySlot(other, i, slot);
        if (size == k) {
          for (int h = (k >>> 1) - 1; h >= 0; h--) {
            siftDown(h);
          }
        }
      } else if (compareSlots(other, i, this, 0) < 0) {
        copySlot(other, i, 0);
        siftDown(0);
      }
    }
  }

  /**
   * Whether a leaf whose BEST possible first key is {@code best} (a numeric key) is strictly worse
   * than the worst kept row on that key alone — then none of its rows can enter. Only meaningful on a
   * {@link #full()} heap.
   */
  boolean firstKeyStrictlyWorse(final long best) {
    final int cmp = Long.compare(best, tuple[0]);
    return descending[0]
        ? cmp < 0
        : cmp > 0;
  }

  /** {@link #firstKeyStrictlyWorse(long)} for a string first key given as bytes. */
  boolean firstKeyStrictlyWorse(final byte[] best, final int off, final int len) {
    final byte[] worst = strKey[0];
    final int cmp = keyKind[0] == KEY_STRING_COLLATED
        ? ProjectionIndexByteScan.compareStrSlices(best, off, len, worst, 0, worst.length)
        : Arrays.compareUnsigned(best, off, off + len, worst, 0, worst.length);
    return descending[0]
        ? cmp < 0
        : cmp > 0;
  }

  /** The kept rows' record keys in emission order: the total order the heap selects under. */
  long[] sortedRecordKeys() {
    final int kept = size;
    final int[] order = new int[kept];
    for (int i = 0; i < kept; i++) {
      order[i] = i;
    }
    IntArrays.mergeSort(order, (a, b) -> compareSlots(this, a, this, b));
    final long[] out = new long[kept];
    for (int i = 0; i < kept; i++) {
      out[i] = recordKey[order[i]];
    }
    return out;
  }

  private void store(final int slot, final ColumnSlice[] leafSort, final int rowIdx, final long key,
      final long rowRank) {
    final int base = slot * keyCount;
    for (int kk = 0; kk < keyCount; kk++) {
      final ColumnSlice slice = leafSort[kk];
      switch (keyKind[kk]) {
        case KEY_STRING_BYTES, KEY_STRING_COLLATED -> {
          final int id = slice.stringDictIds()[rowIdx];
          final int off = slice.dictOffset(id);
          strKey[base + kk] = Arrays.copyOfRange(slice.dictBytes(), off, off + slice.dictLength(id));
          tuple[base + kk] = id;
        }
        default -> tuple[base + kk] = slice.numericValues()[rowIdx];
      }
    }
    recordKey[slot] = key;
    rank[slot] = rowRank;
  }

  private void copySlot(final TopKHeap from, final int fromSlot, final int toSlot) {
    System.arraycopy(from.tuple, fromSlot * keyCount, tuple, toSlot * keyCount, keyCount);
    if (strKey != null) {
      System.arraycopy(from.strKey, fromSlot * keyCount, strKey, toSlot * keyCount, keyCount);
    }
    recordKey[toSlot] = from.recordKey[fromSlot];
    rank[toSlot] = from.rank[fromSlot];
  }

  /** The candidate row against kept slot {@code slot} under the total order. */
  private int compareCandidate(final ColumnSlice[] leafSort, final int rowIdx, final long rowRank, final int slot) {
    final int base = slot * keyCount;
    for (int kk = 0; kk < keyCount; kk++) {
      final ColumnSlice slice = leafSort[kk];
      final int cmp;
      switch (keyKind[kk]) {
        case KEY_NUMERIC -> cmp = Long.compare(slice.numericValues()[rowIdx], tuple[base + kk]);
        case KEY_STRING_GLOBAL -> cmp = globalViews[kk].compareIds(Math.toIntExact(slice.numericValues()[rowIdx]),
            Math.toIntExact(tuple[base + kk]));
        default -> {
          final int id = slice.stringDictIds()[rowIdx];
          final byte[] bytes = slice.dictBytes();
          final int off = slice.dictOffset(id);
          final int len = slice.dictLength(id);
          final byte[] kept = strKey[base + kk];
          cmp = keyKind[kk] == KEY_STRING_COLLATED
              ? ProjectionIndexByteScan.compareStrSlices(bytes, off, len, kept, 0, kept.length)
              : Arrays.compareUnsigned(bytes, off, off + len, kept, 0, kept.length);
        }
      }
      if (cmp != 0) {
        return descending[kk]
            ? -cmp
            : cmp;
      }
    }
    return Long.compare(rowRank, rank[slot]);
  }

  /** Slot {@code a} of heap {@code ha} against slot {@code b} of heap {@code hb} (same shape). */
  private static int compareSlots(final TopKHeap ha, final int a, final TopKHeap hb, final int b) {
    final int keyCount = ha.keyCount;
    final int ba = a * keyCount;
    final int bb = b * keyCount;
    for (int kk = 0; kk < keyCount; kk++) {
      final int cmp;
      switch (ha.keyKind[kk]) {
        case KEY_NUMERIC -> cmp = Long.compare(ha.tuple[ba + kk], hb.tuple[bb + kk]);
        case KEY_STRING_GLOBAL ->
          cmp = ha.globalViews[kk].compareIds(Math.toIntExact(ha.tuple[ba + kk]), Math.toIntExact(hb.tuple[bb + kk]));
        default -> {
          final byte[] x = ha.strKey[ba + kk];
          final byte[] y = hb.strKey[bb + kk];
          cmp = ha.keyKind[kk] == KEY_STRING_COLLATED
              ? ProjectionIndexByteScan.compareStrSlices(x, 0, x.length, y, 0, y.length)
              : Arrays.compareUnsigned(x, y);
        }
      }
      if (cmp != 0) {
        return ha.descending[kk]
            ? -cmp
            : cmp;
      }
    }
    return Long.compare(ha.rank[a], hb.rank[b]);
  }

  /** Max-heap sift-down (root = WORST kept row). */
  private void siftDown(final int start) {
    int i = start;
    final int half = size >>> 1;
    while (i < half) {
      int child = (i << 1) + 1;
      final int right = child + 1;
      if (right < size && compareSlots(this, right, this, child) > 0) {
        child = right;
      }
      if (compareSlots(this, child, this, i) <= 0) {
        return;
      }
      swap(i, child);
      i = child;
    }
  }

  private void swap(final int a, final int b) {
    final int ba = a * keyCount;
    final int bb = b * keyCount;
    for (int kk = 0; kk < keyCount; kk++) {
      final long t = tuple[ba + kk];
      tuple[ba + kk] = tuple[bb + kk];
      tuple[bb + kk] = t;
      if (strKey != null) {
        final byte[] s = strKey[ba + kk];
        strKey[ba + kk] = strKey[bb + kk];
        strKey[bb + kk] = s;
      }
    }
    long t = recordKey[a];
    recordKey[a] = recordKey[b];
    recordKey[b] = t;
    t = rank[a];
    rank[a] = rank[b];
    rank[b] = t;
  }
}
