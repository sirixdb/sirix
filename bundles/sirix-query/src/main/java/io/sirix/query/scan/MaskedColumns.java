package io.sirix.query.scan;

import io.sirix.index.projection.ProjectionColumnStore;
import io.sirix.index.projection.ProjectionColumnStore.ColumnSlice;
import io.sirix.index.projection.ProjectionIndexRowGroupPage;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.jspecify.annotations.Nullable;

import java.nio.charset.StandardCharsets;
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

  MaskedColumns(final ProjectionColumnStore store, final long[][] rowMasks, final long[][] recordKeys,
      final ColumnSlice[][] slices, final byte[] kinds, final long rows) {
    this.store = store;
    this.rowMasks = rowMasks;
    this.recordKeys = recordKeys;
    this.slices = slices;
    this.kinds = kinds;
    this.rows = rows;
    this.dictToInterned = new int[kinds.length][rowMasks.length][];
    strings.add(null);
    stringIds.defaultReturnValue(0);
  }

  /** How many rows the mask admits over every leaf. */
  public long rows() {
    return rows;
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

  public boolean isString(final int field) {
    return kinds[field] == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT;
  }

  /** Whether {@code field} is present on {@code row} of {@code leaf}. */
  public boolean present(final int field, final int leaf, final int row) {
    final ColumnSlice slice = slices[field][leaf];
    return slice.rowCount() > 0 && (slice.presenceWords()[row >>> 6] & 1L << (row & 63)) != 0L;
  }

  /** The long value of {@code field} on a PRESENT row. */
  public long longValue(final int field, final int leaf, final int row) {
    return slices[field][leaf].numericValues()[row];
  }

  /**
   * The interned id of {@code field}'s string on a PRESENT row — equal ids are equal strings across
   * leaves and across every {@link MaskedColumns} sharing this interner.
   */
  public int stringId(final int field, final int leaf, final int row) {
    final ColumnSlice slice = slices[field][leaf];
    int[] table = dictToInterned[field][leaf];
    if (table == null) {
      final int dictSize = slice.dictSize();
      table = new int[dictSize];
      for (int id = 0; id < dictSize; id++) {
        table[id] = intern(slice.dictString(id));
      }
      dictToInterned[field][leaf] = table;
    }
    return table[slice.stringDictIds()[row]];
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
      final long[] lane = slice.numericValues();
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
          ? null
          : slice.numericValues();
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

  /** The interned id of a string on a PRESENT row, through the shared interner when adopted. */
  public int sharedStringId(final int field, final int leaf, final int row) {
    final ColumnSlice slice = slices[field][leaf];
    int[] table = dictToInterned[field][leaf];
    if (table == null) {
      final int dictSize = slice.dictSize();
      table = new int[dictSize];
      for (int id = 0; id < dictSize; id++) {
        table[id] = internShared(slice.dictString(id));
      }
      dictToInterned[field][leaf] = table;
    }
    return table[slice.stringDictIds()[row]];
  }

  /** The string behind a shared id (resolved through the interner that owns the id space). */
  public String sharedString(final int id) {
    return sharedInterner != null
        ? sharedInterner.string(id)
        : string(id);
  }

  /** UTF-8 bytes of an interned string, for callers hashing values. */
  public byte[] utf8(final int id) {
    return sharedString(id).getBytes(StandardCharsets.UTF_8);
  }
}
