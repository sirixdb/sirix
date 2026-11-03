package io.sirix.query.scan;

import io.sirix.index.projection.ProjectionColumnStore;
import io.sirix.index.projection.ProjectionColumnStore.ColumnSlice;
import io.sirix.node.SegmentAccess;
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;
import org.jspecify.annotations.Nullable;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.ref.WeakReference;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * An immutable numeric column selection resident in one allocator slab: the routed key roster that
 * identifies it, then per kept leaf its row mask, record keys, per-field presence words and one
 * row-major lane block holding every field of a row contiguously ({@code 8 * fieldCount} bytes per
 * row, 64-byte aligned, padded by {@value #ROW_SLACK_BYTES} bytes). A dense kernel therefore reads
 * a whole row with one checked access, a 256-bit vector when the roster has at most four fields,
 * instead of one checked access per field. Only per-leaf offsets, slice extrema and the field names
 * stay on the heap; nothing here allocates per row.
 */
final class NativeNumericSnapshot extends DerivedCacheBudget.Entry {
  private static final VectorSpecies<Long> SPECIES = LongVector.SPECIES_PREFERRED;
  private static final ByteOrder ORDER = ByteOrder.nativeOrder();
  /** Bytes past the last row of a lane block, so a 256-bit row load never leaves the block. */
  static final long ROW_SLACK_BYTES = 32L;

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
    /** Per leaf: start of the row-major lane block, -1 when skipped. */
    final long[] rowBase;
    /** Per (field, leaf): -1 for a skipped or pruned leaf. */
    final long[] presenceBase;
    final int[] sliceRows;
    /** Per (field, leaf): whether the lane block carries this field's values. */
    final boolean[] laneAvailable;
    final long[] sliceMin;
    final long[] sliceMax;
    final long keysBase;
    final long bytes;

    private Layout(final int leaves, final int fieldCount, final long rows, final byte[] kinds, final int[] maskWords,
        final long[] maskBase, final long[] keyBase, final int[] leafRows, final long[] rowBase,
        final long[] presenceBase, final int[] sliceRows, final boolean[] laneAvailable, final long[] sliceMin,
        final long[] sliceMax, final long keysBase, final long bytes) {
      this.leaves = leaves;
      this.fieldCount = fieldCount;
      this.rows = rows;
      this.kinds = kinds;
      this.maskWords = maskWords;
      this.maskBase = maskBase;
      this.keyBase = keyBase;
      this.leafRows = leafRows;
      this.rowBase = rowBase;
      this.presenceBase = presenceBase;
      this.sliceRows = sliceRows;
      this.laneAvailable = laneAvailable;
      this.sliceMin = sliceMin;
      this.sliceMax = sliceMax;
      this.keysBase = keysBase;
      this.bytes = bytes;
    }

    long bytes() {
      return bytes;
    }

    long stride() {
      return 8L * fieldCount;
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
    final long[] rowBase = new long[leaves];
    final long[] presenceBase = new long[fieldCount * leaves];
    final int[] sliceRows = new int[fieldCount * leaves];
    final boolean[] laneAvailable = new boolean[fieldCount * leaves];
    final long[] sliceMin = new long[fieldCount * leaves];
    final long[] sliceMax = new long[fieldCount * leaves];
    Arrays.fill(maskBase, -1L);
    Arrays.fill(keyBase, -1L);
    Arrays.fill(rowBase, -1L);
    Arrays.fill(presenceBase, -1L);
    final long stride = 8L * fieldCount;
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
            return null; // a lane shorter than the leaf cannot share the row-major block
          }
          laneAvailable[at] = true;
        }
      }
      rowBase[leaf] = cursor;
      cursor = DerivedCacheBudget.alignUp(DerivedCacheBudget.saturatedAdd(
          DerivedCacheBudget.saturatedAdd(cursor, DerivedCacheBudget.saturatedMultiply(keys.length, stride)),
          ROW_SLACK_BYTES));
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
    return new Layout(leaves, fieldCount, rows, kinds.clone(), maskWords, maskBase, keyBase, leafRows, rowBase,
        presenceBase, sliceRows, laneAvailable, sliceMin, sliceMax, keysBase, Math.max(64L, cursor));
  }

  /**
   * Fill {@code slab} from the heap view and the masks copied at admission: bulk copies for keys,
   * masks and presence words, one scatter pass per lane into the row-major block. The returned entry
   * is not yet published.
   */
  static NativeNumericSnapshot build(final MemorySegment slab, final Layout layout, final long[][] masks,
      final MaskedColumns columns, final ProjectionColumnStore store, final String[] fields, final long[] keys) {
    if (slab.byteSize() < layout.bytes || keys.length * 8L > layout.bytes) {
      throw new IllegalArgumentException("slab smaller than the snapshot layout");
    }
    MemorySegment.copy(keys, 0, slab, ValueLayout.JAVA_LONG, layout.keysBase, keys.length);
    final int leaves = layout.leaves;
    final long stride = layout.stride();
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
      final long rowBase = layout.rowBase[leaf];
      // The slack after the last row is read by vector row loads; it must hold defined bytes.
      slab.asSlice(rowBase, leafKeys.length * stride + ROW_SLACK_BYTES).fill((byte) 0);
      for (int field = 0; field < layout.fieldCount; field++) {
        final int at = field * leaves + leaf;
        final ColumnSlice slice = columns.slice(field, leaf);
        final long[] presence = slice.presenceWords();
        if (layout.presenceBase[at] >= 0) {
          MemorySegment.copy(presence, 0, slab, ValueLayout.JAVA_LONG, layout.presenceBase[at], presence.length);
        }
        if (layout.laneAvailable[at]) {
          final long[] values = slice.numericValues();
          final long laneBase = rowBase + 8L * field;
          for (int row = 0; row < values.length; row++) {
            SegmentAccess.putLong(slab, laneBase + row * stride, values[row]);
          }
        }
      }
    }
    return new NativeNumericSnapshot(store, fields, keys.length, layout, columns.rowIdentity(), columns.presenceMemo());
  }

  /** Same store identity, ordered field roster and routed keys. */
  boolean matches(final ProjectionColumnStore store, final String[] fields, final long[] keys) {
    return this.store.get() == store && Arrays.equals(this.fields, fields) && keysEqual(keys);
  }

  private boolean keysEqual(final long[] keys) {
    if (keys.length != keyCount) {
      return false;
    }
    final MemorySegment slab = slab();
    final long base = layout.keysBase;
    int i = 0;
    final int bound = SPECIES.loopBound(keyCount);
    for (; i < bound; i += SPECIES.length()) {
      final LongVector resident = LongVector.fromMemorySegment(SPECIES, slab, base + 8L * i, ORDER);
      if (LongVector.fromArray(SPECIES, keys, i).compare(VectorOperators.NE, resident).anyTrue()) {
        return false;
      }
    }
    for (; i < keyCount; i++) {
      if (keys[i] != SegmentAccess.getLong(slab, base + 8L * i)) {
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
    final MemorySegment slab = slab();
    for (int leaf = 0; leaf < masks.length; leaf++) {
      final long[] words = masks[leaf];
      final long base = layout.maskBase[leaf];
      if (words == null != (base < 0)) {
        return false;
      }
      if (words == null) {
        continue;
      }
      final int count = layout.maskWords[leaf];
      if (words.length != count) {
        return false;
      }
      int i = 0;
      final int bound = SPECIES.loopBound(count);
      for (; i < bound; i += SPECIES.length()) {
        final LongVector resident = LongVector.fromMemorySegment(SPECIES, slab, base + 8L * i, ORDER);
        if (LongVector.fromArray(SPECIES, words, i).compare(VectorOperators.NE, resident).anyTrue()) {
          return false;
        }
      }
      for (; i < count; i++) {
        if (words[i] != SegmentAccess.getLong(slab, base + 8L * i)) {
          return false;
        }
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

  /** Byte offset of leaf {@code leaf}'s row-major lane block, or -1 when the leaf is skipped. */
  long rowBase(final int leaf) {
    return layout.rowBase[leaf];
  }

  /** Bytes between consecutive rows of a lane block: eight per field. */
  long laneStride() {
    return layout.stride();
  }

  /** Whether {@code field}'s values are present in {@code leaf}'s lane block. */
  boolean laneAvailable(final int field, final int leaf) {
    return layout.laneAvailable[field * layout.leaves + leaf];
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
    if (!layout.laneAvailable[field * layout.leaves + leaf]) {
      throw new IllegalStateException("an admitted long field needs a numeric lane");
    }
    return SegmentAccess.getLong(slab(), layout.rowBase[leaf] + row * layout.stride() + 8L * field);
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
