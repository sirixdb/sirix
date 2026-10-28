package io.sirix.query.scan;

import static java.util.Objects.requireNonNull;

import io.sirix.index.projection.ProjectionColumnStore;
import io.sirix.index.projection.ProjectionColumnStore.ColumnSlice;
import io.sirix.index.projection.ProjectionIndexRowGroupPage;
import io.sirix.index.projection.ProjectionIndexScan.ColumnPredicate;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongArrays;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A projection's columns under a row mask: the leaves and rows an index-routed row source admits,
 * with the requested fields resident as slices. The build and probe sides of a column-side join,
 * and the two sides of a membership filter, read through this — no record object, no leaf the mask
 * dropped.
 *
 * <p>
 * Long columns are read from the slice's long lane with its presence; string columns
 * ({@code STRING_DICT}, per-leaf dictionaries) are interned into ONE query-local id space so a
 * value has the same id on every leaf and on either side of a join — built per leaf dictionary
 * entry, never per row. Single-threaded by contract.
 * </p>
 */
public final class MaskedColumns {

  private final ProjectionColumnStore store;
  private final long[][] rowMasks;
  private final long[][] recordKeys;
  private final ColumnSlice[][] slices;
  private final byte[] kinds;
  private final long rows;
  /** Interned string values, id-indexed; {@code 0} is reserved for "no value". */
  private final List<String> strings = new ArrayList<>();
  private final Object2IntOpenHashMap<String> stringIds = new Object2IntOpenHashMap<>();
  /** Per field, per leaf: the leaf dictionary id → interned id table, built on first touch. */
  private final int[][][] dictToInterned;
  /**
   * Original masks of an immutable revision; derived state is reusable only while they still match.
   */
  private long @Nullable [][] immutableMasks;
  private final Object rowIdentity;
  /**
   * Per field: 0 unknown, 1 every selected row present, 2 not; shared by the views of one snapshot.
   */
  private final byte[] presenceMemo;

  MaskedColumns(final ProjectionColumnStore store, final long[][] rowMasks, final long[][] recordKeys,
      final ColumnSlice[][] slices, final byte[] kinds, final long rows) {
    this(store, rowMasks, recordKeys, slices, kinds, rows, new Object(), null, new byte[kinds.length]);
  }

  private MaskedColumns(final ProjectionColumnStore store, final long[][] rowMasks, final long[][] recordKeys,
      final ColumnSlice[][] slices, final byte[] kinds, final long rows, final Object rowIdentity,
      final long @Nullable [][] immutableMasks, final byte[] presenceMemo) {
    this.store = store;
    this.rowMasks = rowMasks;
    this.recordKeys = recordKeys;
    this.slices = slices;
    this.kinds = kinds;
    this.rows = rows;
    this.dictToInterned = new int[kinds.length][rowMasks.length][];
    this.rowIdentity = rowIdentity;
    this.immutableMasks = immutableMasks;
    this.presenceMemo = presenceMemo;
    strings.add(null);
    stringIds.defaultReturnValue(0);
  }

  /** Numeric-only snapshots never retain a query's mutable string interner or its store key. */
  @SuppressWarnings("ArrayRecordComponent") // Cache lookup compares contents; views copy mutable masks.
  record NumericSnapshot(String[] fields, long[] keys, long[][] masks, long[][] recordKeys, ColumnSlice[][] slices,
      byte[] kinds, long rows, int weight, Object rowIdentity, byte[] presenceMemo) {
    MaskedColumns view(final ProjectionColumnStore store) {
      return new MaskedColumns(store, copyMasks(masks), recordKeys, slices, kinds, rows, rowIdentity, masks,
          presenceMemo);
    }
  }

  private static long[][] copyMasks(final long[][] masks) {
    final long[][] copy = masks.clone();
    for (int leaf = 0; leaf < copy.length; leaf++) {
      if (copy[leaf] != null) {
        copy[leaf] = copy[leaf].clone();
      }
    }
    return copy;
  }

  long numericSnapshotWeight(final int keyCount) {
    int kept = 0;
    long totalRows = 0;
    long bytes = 512L + 8L * keyCount + 64L * kinds.length + 8L * rowMasks.length * (3L + kinds.length);
    for (int field = 0; field < kinds.length; field++) {
      if (!isLong(field) && !isTimestamp(field)) {
        return -1;
      }
    }
    for (int leaf = 0; leaf < rowMasks.length; leaf++) {
      totalRows += store.rowCount(leaf);
      if (rowMasks[leaf] == null) {
        continue;
      }
      kept++;
      bytes += 32L + 8L * (rowMasks[leaf].length + recordKeys[leaf].length);
      for (int field = 0; field < kinds.length; field++) {
        final ColumnSlice slice = slices[field][leaf];
        final long[] values = slice.numericValues();
        final long[] presence = slice.presenceWords();
        bytes += 96L + 8L * (values == null
            ? 0
            : values.length) + 8L * (presence == null
                ? 0
                : presence.length);
      }
    }
    // Sparse sources retain their locality path and its work budgets. Dense reuse retains only
    // columns already requested, never fills a pruned leaf, and prices all retained arrays.
    return rows * 4L >= totalRows && kept * 4L >= rowMasks.length * 3L
        ? bytes
        : -1;
  }

  NumericSnapshot numericSnapshot(final String[] fields, final long[] keys, final int weight) {
    return new NumericSnapshot(fields.clone(), keys.clone(), copyMasks(rowMasks), recordKeys, slices, kinds, rows,
        weight, rowIdentity, isImmutableRevision()
            ? presenceMemo
            : new byte[kinds.length]);
  }

  /** How many rows the mask admits over every leaf. */
  public long rows() {
    return rows;
  }

  /** The column store these masks were read from; its identity scopes derived caches. */
  public ProjectionColumnStore store() {
    return store;
  }

  /**
   * An identity shared only by one loaded snapshot and its cached views, including its field roster.
   * Derived caches must also check {@link #isImmutableRevision()} because a view's masks may change.
   */
  public Object rowIdentity() {
    return rowIdentity;
  }

  /** Mark the columns as read from an immutable revision (no write transaction). */
  void markImmutableRevision() {
    immutableMasks = copyMasks(rowMasks);
  }

  public boolean isImmutableRevision() {
    return immutableMasks != null && Arrays.deepEquals(immutableMasks, rowMasks);
  }

  public int leafCount() {
    return rowMasks.length;
  }

  /**
   * The admitted rows of {@code leaf} as a bitset, or {@code null} when the leaf is skipped whole.
   */
  public long @Nullable [] rowMask(final int leaf) {
    return rowMasks[leaf];
  }

  public int rowCount(final int leaf) {
    return store.rowCount(leaf);
  }

  /** The record key of {@code row} on {@code leaf}. */
  public long recordKey(final int leaf, final int row) {
    return recordKeys[leaf][row];
  }

  public boolean isLong(final int field) {
    return kinds[field] == ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG;
  }

  /** Canonical UTC timestamps; the persisted lane stores epoch seconds. */
  public boolean isTimestamp(final int field) {
    return kinds[field] == ProjectionIndexRowGroupPage.COLUMN_KIND_TIMESTAMP;
  }

  public boolean isString(final int field) {
    return kinds[field] == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT;
  }

  /** Inclusive domain of a numeric field across admitted leaves; empty leaves retain sentinels. */
  public long minimum(final int field) {
    long minimum = Long.MAX_VALUE;
    for (int leaf = 0; leaf < rowMasks.length; leaf++) {
      if (rowMasks[leaf] != null) {
        minimum = Math.min(minimum, slices[field][leaf].min());
      }
    }
    return minimum;
  }

  public long maximum(final int field) {
    long maximum = Long.MIN_VALUE;
    for (int leaf = 0; leaf < rowMasks.length; leaf++) {
      if (rowMasks[leaf] != null) {
        maximum = Math.max(maximum, slices[field][leaf].max());
      }
    }
    return maximum;
  }

  /** Whether every selected row has this field, proved by the already loaded presence words. */
  public boolean allSelectedPresent(final int field) {
    if (!isImmutableRevision()) {
      return scanSelectedPresent(field);
    }
    final byte memo = presenceMemo[field];
    if (memo != 0) {
      return memo == 1;
    }
    final boolean present = scanSelectedPresent(field);
    presenceMemo[field] = present
        ? (byte) 1
        : (byte) 2;
    return present;
  }

  private boolean scanSelectedPresent(final int field) {
    for (int leaf = 0; leaf < rowMasks.length; leaf++) {
      final long[] mask = rowMasks[leaf];
      if (mask == null) {
        continue;
      }
      final ColumnSlice slice = slices[field][leaf];
      if (slice.rowCount() == 0) {
        return false;
      }
      final long[] presence = slice.presenceWords();
      for (int word = 0; word < mask.length; word++) {
        if ((mask[word] & ~presence[word]) != 0L) {
          return false;
        }
      }
    }
    return true;
  }

  /** Immutable numeric values for a leaf; the caller must have admitted a numeric field. */
  public long[] numericLane(final int field, final int leaf) {
    return requireNonNull(slices[field][leaf].numericValues(), "an admitted numeric field needs a numeric lane");
  }

  /** Whether {@code field} is present on {@code row} of {@code leaf}. */
  public boolean present(final int field, final int leaf, final int row) {
    final ColumnSlice slice = slices[field][leaf];
    return slice.rowCount() > 0 && (slice.presenceWords()[row >>> 6] & 1L << (row & 63)) != 0L;
  }

  /** The long value of {@code field} on a PRESENT row. */
  public long longValue(final int field, final int leaf, final int row) {
    return requireNonNull(slices[field][leaf].numericValues(), "an admitted long field needs a numeric lane")[row];
  }

  /**
   * The interned id of {@code field}'s string on a PRESENT row — equal ids are equal strings across
   * leaves and across every {@link MaskedColumns} sharing this interner.
   */
  public int stringId(final int field, final int leaf, final int row) {
    final ColumnSlice slice = slices[field][leaf];
    final int[] ids = requireNonNull(slice.stringDictIds(), "an admitted string field needs dictionary ids");
    int[] table = dictToInterned[field][leaf];
    if (table == null) {
      final int dictSize = slice.dictSize();
      table = new int[dictSize];
      for (int id = 0; id < dictSize; id++) {
        table[id] = intern(slice.dictString(id));
      }
      dictToInterned[field][leaf] = table;
    }
    return table[ids[row]];
  }

  /** The string behind an interned id. */
  public String string(final int id) {
    return strings.get(id);
  }

  /** Intern a string into this set's id space (shared with a join partner through {@link #adopt}). */
  public int intern(final String value) {
    final int known = stringIds.getInt(value);
    if (known != 0) {
      return known;
    }
    final int id = strings.size();
    strings.add(value);
    stringIds.put(value, id);
    return id;
  }

  /**
   * Make {@code other} intern into THIS set's id space, so the two sides of a join compare and group
   * string values by one id. Must be called before {@code other} interns anything.
   */
  public void adopt(final MaskedColumns other) {
    if (other.strings.size() != 1) {
      throw new IllegalStateException("the adopted side already interned values");
    }
    other.sharedInterner = this;
  }

  private @Nullable MaskedColumns sharedInterner;

  private int internShared(final String value) {
    return sharedInterner != null
        ? sharedInterner.intern(value)
        : intern(value);
  }

  /** Every present long value of {@code field} over the admitted rows. */
  public LongOpenHashSet presentLongValues(final int field) {
    final LongOpenHashSet values = new LongOpenHashSet();
    for (int leaf = 0; leaf < rowMasks.length; leaf++) {
      final long[] mask = rowMasks[leaf];
      if (mask == null) {
        continue;
      }
      final ColumnSlice slice = slices[field][leaf];
      if (slice.rowCount() <= 0) {
        continue;
      }
      final long[] presence = slice.presenceWords();
      final long[] lane = requireNonNull(slice.numericValues(), "an admitted long field needs a numeric lane");
      for (int w = 0; w < mask.length; w++) {
        long word = mask[w] & presence[w];
        final int rowBase = w << 6;
        while (word != 0L) {
          final int bit = Long.numberOfTrailingZeros(word);
          word &= word - 1L;
          values.add(lane[rowBase + bit]);
        }
      }
    }
    return values;
  }

  /**
   * The record keys (ascending) of the admitted rows whose long {@code field} is in {@code values}
   * ({@code anti == false}), or whose field is missing or not in {@code values} ({@code anti ==
   * true}) — the interpreter's semi- and anti-join over an equality: a missing field matches nothing,
   * so it survives an anti-join and fails a semi-join.
   */
  public long[] recordKeysByMembership(final int field, final LongOpenHashSet values, final boolean anti) {
    final LongArrayList keys = new LongArrayList();
    for (int leaf = 0; leaf < rowMasks.length; leaf++) {
      final long[] mask = rowMasks[leaf];
      if (mask == null) {
        continue;
      }
      final ColumnSlice slice = slices[field][leaf];
      final boolean pruned = slice.rowCount() <= 0;
      final long[] presence = pruned
          ? null
          : slice.presenceWords();
      final long[] lane = pruned
          ? LongArrays.EMPTY_ARRAY
          : requireNonNull(slice.numericValues(), "an admitted membership field needs a numeric lane");
      final long[] leafKeys = recordKeys[leaf];
      for (int w = 0; w < mask.length; w++) {
        long word = mask[w];
        final int rowBase = w << 6;
        while (word != 0L) {
          final int bit = Long.numberOfTrailingZeros(word);
          word &= word - 1L;
          final int row = rowBase + bit;
          final boolean present = presence != null && (presence[w] & 1L << bit) != 0L;
          final boolean member = present && values.contains(lane[row]);
          if (anti
              ? !member
              : member) {
            keys.add(leafKeys[row]);
          }
        }
      }
    }
    final long[] sorted = keys.toLongArray();
    Arrays.sort(sorted);
    return sorted;
  }

  ColumnPredicate recordKeyPredicateByMembership(final int field, final long[] sortedKeys, final LongOpenHashSet values,
      final boolean anti) {
    if (field < 0 || field >= kinds.length || !isLong(field)) {
      throw new IllegalArgumentException("membership requires a numeric field");
    }
    return ColumnPredicate.recordKeysByMembership(sortedKeys, store, rowMasks, recordKeys, slices[field], values, anti);
  }

  /** The interned id of a string on a PRESENT row, through the shared interner when adopted. */
  public int sharedStringId(final int field, final int leaf, final int row) {
    final ColumnSlice slice = slices[field][leaf];
    final int[] ids = requireNonNull(slice.stringDictIds(), "an admitted string field needs dictionary ids");
    int[] table = dictToInterned[field][leaf];
    if (table == null) {
      final int dictSize = slice.dictSize();
      table = new int[dictSize];
      for (int id = 0; id < dictSize; id++) {
        table[id] = internShared(slice.dictString(id));
      }
      dictToInterned[field][leaf] = table;
    }
    return table[ids[row]];
  }

}
