package io.sirix.query.scan;

import io.sirix.index.projection.ProjectionColumnStore;
import io.sirix.index.projection.ProjectionColumnStore.ColumnSlice;
import io.sirix.node.SegmentAccess;
import org.jspecify.annotations.Nullable;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.ref.WeakReference;
import java.util.Arrays;

/**
 * An immutable numeric column selection resident in one allocator slab: the routed key roster that
 * identifies it, then per kept leaf its row mask, record keys and, per field, presence words and
 * one contiguous numeric lane, exactly the arrays of the heap view laid out one after another. A
 * kernel therefore reads a lane of a resident snapshot the way it reads a heap lane: a base offset
 * hoisted once per leaf plus eight bytes per row, through the typed slab handles. Only per-leaf
 * offsets, slice extrema and the field names stay on the heap; nothing here allocates per row.
 */
final class NativeNumericSnapshot extends DerivedCacheBudget.Entry {
  private final WeakReference<ProjectionColumnStore> store;
  private final String[] fields;
  private final int keyCount;
  private final Layout layout;
  private final Object rowIdentity;
  private final byte[] presenceMemo;

  /**
   * Byte offsets of every array in the slab, computed once from the heap view and reused to fill it.
   */
  static final class Layout {
    final int leaves;
    final int fieldCount;
    final long rows;
    final byte[] kinds;
    /** Per leaf: mask words, or 0 when the leaf is skipped. */
    final int[] maskWords;
    final long[] maskBase;
    final long[] keyBase;
    /** Per leaf: record keys, 0 when skipped. */
    final int[] leafRows;
    /** Per (field, leaf): -1 for a skipped or pruned leaf. */
    final long[] presenceBase;
    /** Per (field, leaf): start of the field's numeric lane, -1 when the leaf carries none. */
    final long[] laneBase;
    final int[] sliceRows;
    final long[] sliceMin;
    final long[] sliceMax;
    final long keysBase;
    final long bytes;

    private Layout(final int leaves, final int fieldCount, final long rows, final byte[] kinds, final int[] maskWords,
        final long[] maskBase, final long[] keyBase, final int[] leafRows, final long[] presenceBase,
        final long[] laneBase, final int[] sliceRows, final long[] sliceMin, final long[] sliceMax, final long keysBase,
        final long bytes) {
      this.leaves = leaves;
      this.fieldCount = fieldCount;
      this.rows = rows;
      this.kinds = kinds;
      this.maskWords = maskWords;
      this.maskBase = maskBase;
      this.keyBase = keyBase;
      this.leafRows = leafRows;
      this.presenceBase = presenceBase;
      this.laneBase = laneBase;
      this.sliceRows = sliceRows;
      this.sliceMin = sliceMin;
      this.sliceMax = sliceMax;
      this.keysBase = keysBase;
      this.bytes = bytes;
    }

    long bytes() {
      return bytes;
    }
  }

  private NativeNumericSnapshot(final ProjectionColumnStore store, final String[] fields, final int keyCount,
      final Layout layout, final Object rowIdentity, final byte[] presenceMemo) {
    this.store = new WeakReference<>(store);
    this.fields = fields;
    this.keyCount = keyCount;
    this.layout = layout;
    this.rowIdentity = rowIdentity;
    this.presenceMemo = presenceMemo;
  }

  /**
   * The slab layout for {@code columns}, or {@code null} when the selection is not a dense,
   * numeric-only candidate. Dense means at least a quarter of the rows over at least three quarters
   * of the leaves: sparse sources keep their locality route and its work budgets.
   */
  static @Nullable Layout layout(final MaskedColumns columns, final int keyCount) {
    if (keyCount < 0) {
      throw new IllegalArgumentException("Negative key count");
    }
    final byte[] kinds = columns.kinds();
    final int fieldCount = kinds.length;
    if (fieldCount == 0) {
      return null;
    }
    for (int field = 0; field < fieldCount; field++) {
      if (!columns.isLong(field) && !columns.isTimestamp(field)) {
        return null;
      }
    }
    final int leaves = columns.leafCount();
    final int[] maskWords = new int[leaves];
    final long[] maskBase = new long[leaves];
    final long[] keyBase = new long[leaves];
    final int[] leafRows = new int[leaves];
    final long[] presenceBase = new long[fieldCount * leaves];
    final long[] laneBase = new long[fieldCount * leaves];
    final int[] sliceRows = new int[fieldCount * leaves];
    final long[] sliceMin = new long[fieldCount * leaves];
    final long[] sliceMax = new long[fieldCount * leaves];
    Arrays.fill(maskBase, -1L);
    Arrays.fill(keyBase, -1L);
    Arrays.fill(presenceBase, -1L);
    Arrays.fill(laneBase, -1L);
    long cursor = 0;
    final long keysBase = cursor;
    cursor = DerivedCacheBudget.alignUp(cursor + 8L * keyCount);
    int kept = 0;
    long totalRows = 0;
    for (int leaf = 0; leaf < leaves; leaf++) {
      totalRows += columns.rowCount(leaf);
      final long[] mask = columns.rowMask(leaf);
      if (mask == null) {
        continue;
      }
      kept++;
      maskWords[leaf] = mask.length;
      maskBase[leaf] = cursor;
      cursor = DerivedCacheBudget.alignUp(cursor + 8L * mask.length);
      final long[] keys = columns.recordKeysOf(leaf);
      leafRows[leaf] = keys.length;
      keyBase[leaf] = cursor;
      cursor = DerivedCacheBudget.alignUp(cursor + 8L * keys.length);
      for (int field = 0; field < fieldCount; field++) {
        final int at = field * leaves + leaf;
        final ColumnSlice slice = columns.slice(field, leaf);
        sliceRows[at] = slice.rowCount();
        sliceMin[at] = slice.min();
        sliceMax[at] = slice.max();
        final long[] presence = slice.presenceWords();
        if (presence != null) {
          presenceBase[at] = cursor;
          cursor = DerivedCacheBudget.alignUp(cursor + 8L * presence.length);
        }
        final long[] values = slice.numericValues();
        if (values != null) {
          if (values.length != keys.length) {
            return null; // a lane shorter than the leaf cannot be addressed by row
          }
          laneBase[at] = cursor;
          cursor = DerivedCacheBudget.alignUp(
              DerivedCacheBudget.saturatedAdd(cursor, DerivedCacheBudget.saturatedMultiply(values.length, 8L)));
        }
      }
      if (cursor == Long.MAX_VALUE) {
        return null;
      }
    }
    final long rows = columns.rows();
    if (rows < totalRows / 4 + (totalRows % 4 == 0
        ? 0
        : 1) || kept * 4L < leaves * 3L) {
      return null;
    }
    return new Layout(leaves, fieldCount, rows, kinds.clone(), maskWords, maskBase, keyBase, leafRows, presenceBase,
        laneBase, sliceRows, sliceMin, sliceMax, keysBase, Math.max(64L, cursor));
  }

  /**
   * Fill {@code slab} from the heap view and the masks copied at admission: one bulk copy per array
   * (keys, masks, presence words and every lane). The returned entry is not yet published.
   */
  static NativeNumericSnapshot build(final MemorySegment slab, final Layout layout, final long[][] masks,
      final MaskedColumns columns, final ProjectionColumnStore store, final String[] fields, final long[] keys) {
    if (slab.byteSize() < layout.bytes || keys.length * 8L > layout.bytes) {
      throw new IllegalArgumentException("slab smaller than the snapshot layout");
    }
    MemorySegment.copy(keys, 0, slab, ValueLayout.JAVA_LONG, layout.keysBase, keys.length);
    final int leaves = layout.leaves;
    for (int leaf = 0; leaf < leaves; leaf++) {
      if (layout.maskBase[leaf] < 0) {
        continue;
      }
      final long[] mask = masks[leaf];
      if (mask == null || mask.length != layout.maskWords[leaf]) {
        throw new IllegalStateException("admission masks do not match the snapshot layout");
      }
      MemorySegment.copy(mask, 0, slab, ValueLayout.JAVA_LONG, layout.maskBase[leaf], mask.length);
      final long[] leafKeys = columns.recordKeysOf(leaf);
      MemorySegment.copy(leafKeys, 0, slab, ValueLayout.JAVA_LONG, layout.keyBase[leaf], leafKeys.length);
      for (int field = 0; field < layout.fieldCount; field++) {
        final int at = field * leaves + leaf;
        final ColumnSlice slice = columns.slice(field, leaf);
        final long[] presence = slice.presenceWords();
        if (layout.presenceBase[at] >= 0) {
          MemorySegment.copy(presence, 0, slab, ValueLayout.JAVA_LONG, layout.presenceBase[at], presence.length);
        }
        if (layout.laneBase[at] >= 0) {
          final long[] values = slice.numericValues();
          MemorySegment.copy(values, 0, slab, ValueLayout.JAVA_LONG, layout.laneBase[at], values.length);
        }
      }
    }
    return new NativeNumericSnapshot(store, fields, keys.length, layout, columns.rowIdentity(), columns.presenceMemo());
  }

  /** Same store identity, ordered field roster and routed keys. */
  boolean matches(final ProjectionColumnStore store, final String[] fields, final long[] keys) {
    return this.store.get() == store && Arrays.equals(this.fields, fields) && keysEqual(keys);
  }

  /** The routed roster against the resident one: the bulk mismatch intrinsic, no slice allocated. */
  private boolean keysEqual(final long[] keys) {
    if (keys.length != keyCount) {
      return false;
    }
    final long bytes = 8L * keyCount;
    return MemorySegment.mismatch(MemorySegment.ofArray(keys), 0L, bytes, slab(), layout.keysBase,
        layout.keysBase + bytes) < 0L;
  }

  /**
   * Whether a leaf's mask words equal the resident ones: a plain loop, nothing allocated per leaf.
   */
  private boolean wordsEqual(final long[] words, final long base) {
    final MemorySegment slab = slab();
    for (int i = 0; i < words.length; i++) {
      if (words[i] != SegmentAccess.getLong(slab, base + 8L * i)) {
        return false;
      }
    }
    return true;
  }

  /** A view over this snapshot; the caller already holds {@code lease} for it. */
  MaskedColumns view(final ProjectionColumnStore store, final DerivedCacheBudget.Lease lease) {
    return new MaskedColumns(store, copyMasks(), this, lease);
  }

  /** Fresh heap masks for one view; views mutate masks, the slab never changes. */
  long[][] copyMasks() {
    final MemorySegment slab = slab();
    final long[][] masks = new long[layout.leaves][];
    for (int leaf = 0; leaf < masks.length; leaf++) {
      final long base = layout.maskBase[leaf];
      if (base >= 0) {
        final long[] words = new long[layout.maskWords[leaf]];
        MemorySegment.copy(slab, ValueLayout.JAVA_LONG, base, words, 0, words.length);
        masks[leaf] = words;
      }
    }
    return masks;
  }

  /** Whether a view's masks still equal the immutable masks; derived reuse needs this. */
  boolean masksEqual(final long[][] masks) {
    if (masks.length != layout.leaves) {
      return false;
    }
    for (int leaf = 0; leaf < masks.length; leaf++) {
      final long[] words = masks[leaf];
      final long base = layout.maskBase[leaf];
      if (words == null != (base < 0)) {
        return false;
      }
      if (words == null) {
        continue;
      }
      if (words.length != layout.maskWords[leaf] || !wordsEqual(words, base)) {
        return false;
      }
    }
    return true;
  }

  int leafCount() {
    return layout.leaves;
  }

  int fieldCount() {
    return layout.fieldCount;
  }

  long rows() {
    return layout.rows;
  }

  byte[] kinds() {
    return layout.kinds;
  }

  Object rowIdentity() {
    return rowIdentity;
  }

  byte[] presenceMemo() {
    return presenceMemo;
  }

  /** Byte offset of leaf {@code leaf}'s record keys, or -1 when the leaf is skipped. */
  long keyBase(final int leaf) {
    return layout.keyBase[leaf];
  }

  int leafRows(final int leaf) {
    return layout.leafRows[leaf];
  }

  long[] keyBases() {
    return layout.keyBase;
  }

  int[] leafRowCounts() {
    return layout.leafRows;
  }

  /**
   * Byte offset of {@code field}'s numeric lane on {@code leaf}, or -1 when the leaf carries none.
   */
  long laneBase(final int field, final int leaf) {
    return layout.laneBase[field * layout.leaves + leaf];
  }

  /** Per leaf, the byte offsets of {@code field}'s numeric lane (-1 where absent). */
  long[] laneBases(final int field) {
    return Arrays.copyOfRange(layout.laneBase, field * layout.leaves, (field + 1) * layout.leaves);
  }

  long presenceBase(final int field, final int leaf) {
    return layout.presenceBase[field * layout.leaves + leaf];
  }

  long[] presenceBases(final int field) {
    return Arrays.copyOfRange(layout.presenceBase, field * layout.leaves, (field + 1) * layout.leaves);
  }

  int sliceRows(final int field, final int leaf) {
    return layout.sliceRows[field * layout.leaves + leaf];
  }

  long sliceMin(final int field, final int leaf) {
    return layout.sliceMin[field * layout.leaves + leaf];
  }

  long sliceMax(final int field, final int leaf) {
    return layout.sliceMax[field * layout.leaves + leaf];
  }

  long recordKey(final int leaf, final int row) {
    return SegmentAccess.getLong(slab(), layout.keyBase[leaf] + 8L * row);
  }

  boolean present(final int field, final int leaf, final int row) {
    final int at = field * layout.leaves + leaf;
    return layout.sliceRows[at] > 0
        && (SegmentAccess.getLong(slab(), layout.presenceBase[at] + 8L * (row >>> 6)) & 1L << (row & 63)) != 0L;
  }

  long longValue(final int field, final int leaf, final int row) {
    final long lane = layout.laneBase[field * layout.leaves + leaf];
    if (lane < 0) {
      throw new IllegalStateException("an admitted long field needs a numeric lane");
    }
    return SegmentAccess.getLong(slab(), lane + 8L * row);
  }

  @Override
  protected boolean isStale() {
    return store.get() == null;
  }

  @Override
  protected boolean dependsOn(final Object identity) {
    return false;
  }

  @Override
  protected void evicted() {
    // Packed sides are keyed by this snapshot's row identity; they leave with it.
    partition().budget().packed().evictDependents(rowIdentity);
  }
}
