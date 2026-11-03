package io.sirix.query.scan;

import static java.util.Objects.requireNonNull;

import io.sirix.index.projection.ProjectionColumnStore;
import io.sirix.index.projection.ProjectionColumnStore.ColumnSlice;
import io.sirix.index.projection.ProjectionIndexRowGroupPage;
import io.sirix.index.projection.ProjectionIndexScan.ColumnPredicate;
import io.sirix.node.SegmentAccess;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongArrays;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.jspecify.annotations.Nullable;

import java.lang.foreign.MemorySegment;
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
 *
 * <p>
 * A view is backed either by heap slices decoded for this query or by a resident
 * {@link NativeNumericSnapshot} read through its slab. Kernels decide the representation once per
 * leaf ({@link #isNative()}), never per row. A native view holds a lease on its snapshot and must
 * be {@linkplain #close() closed}; the masks are always this view's own heap copy.
 * </p>
 */
public final class MaskedColumns implements AutoCloseable {

  private final ProjectionColumnStore store;
  private final long[][] rowMasks;
  private final long @Nullable [][] recordKeys;
  private final ColumnSlice @Nullable [][] slices;
  private final @Nullable NativeNumericSnapshot snapshot;
  private DerivedCacheBudget.@Nullable Lease lease;
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
    this.store = store;
    this.rowMasks = rowMasks;
    this.recordKeys = recordKeys;
    this.slices = slices;
    this.snapshot = null;
    this.kinds = kinds;
    this.rows = rows;
    this.dictToInterned = new int[kinds.length][rowMasks.length][];
    this.rowIdentity = new Object();
    this.presenceMemo = new byte[kinds.length];
    strings.add(null);
    stringIds.defaultReturnValue(0);
  }

  /** A view over a resident snapshot; {@code masks} are this view's own copy. */
  MaskedColumns(final ProjectionColumnStore store, final long[][] masks, final NativeNumericSnapshot snapshot,
      final DerivedCacheBudget.Lease lease) {
    this.store = store;
    this.rowMasks = masks;
    this.recordKeys = null;
    this.slices = null;
    this.snapshot = snapshot;
    this.lease = lease;
    this.kinds = snapshot.kinds();
    this.rows = snapshot.rows();
    this.dictToInterned = new int[kinds.length][masks.length][];
    this.rowIdentity = snapshot.rowIdentity();
    this.presenceMemo = snapshot.presenceMemo();
    strings.add(null);
    stringIds.defaultReturnValue(0);
  }

  /** Whether this view reads a resident slab instead of heap slices. */
  public boolean isNative() {
    return snapshot != null;
  }

  /** The resident snapshot behind a native view; kernels hoist its offsets per leaf. */
  NativeNumericSnapshot nativeSnapshot() {
    return requireNonNull(snapshot, "not a native view");
  }

  /** Release this view's lease on its snapshot; idempotent, a no-op for heap views. */
  @Override
  public void close() {
    final DerivedCacheBudget.Lease held = lease;
    if (held != null) {
      lease = null;
      held.run();
    }
  }

  /** A lease for a background task that outlives this view; released by that task. */
  public DerivedCacheBudget.Lease acquireTaskLease() {
    final NativeNumericSnapshot resident = requireNonNull(snapshot, "not a native view");
    if (!resident.tryAcquire()) {
      throw new IllegalStateException("a leased snapshot cannot have been freed");
    }
    return new DerivedCacheBudget.Lease(resident);
  }

  /** The resident slab of a native view; valid while this view or a task lease is held. */
  public MemorySegment slab() {
    return requireNonNull(snapshot, "not a native view").slab();
  }

  /**
   * Byte offset of {@code leaf}'s row-major lane block, -1 when the leaf is skipped. Field {@code f}
   * of row {@code r} lies at {@code rowBase(leaf) + r * laneStride() + 8 * f}; a row of at most four
   * fields is one {@link NativeRows#load} away.
   */
  public long rowBase(final int leaf) {
    return requireNonNull(snapshot, "not a native view").rowBase(leaf);
  }

  /** Bytes between consecutive rows of a lane block: eight per rostered field. */
  public long laneStride() {
    return requireNonNull(snapshot, "not a native view").laneStride();
  }

  /** Whether {@code field}'s values exist in {@code leaf}'s lane block (pruned slices have none). */
  public boolean laneAvailable(final int field, final int leaf) {
    return requireNonNull(snapshot, "not a native view").laneAvailable(field, leaf);
  }

  /** Byte offset of {@code field}'s presence words on {@code leaf}, -1 when skipped or pruned. */
  public long presenceBase(final int field, final int leaf) {
    return requireNonNull(snapshot, "not a native view").presenceBase(field, leaf);
  }

  /** Byte offset of {@code leaf}'s record keys, -1 when skipped. */
  public long keyBase(final int leaf) {
    return requireNonNull(snapshot, "not a native view").keyBase(leaf);
  }

  static long[][] copyMasks(final long[][] masks) {
    final long[][] copy = masks.clone();
    for (int leaf = 0; leaf < copy.length; leaf++) {
      if (copy[leaf] != null) {
        copy[leaf] = copy[leaf].clone();
      }
    }
    return copy;
  }

  long[][] copyRowMasks() {
    return copyMasks(rowMasks);
  }

  byte[] kinds() {
    return kinds;
  }

  byte[] presenceMemo() {
    return presenceMemo;
  }

  /** The record keys of {@code leaf} in a heap view. */
  long[] recordKeysOf(final int leaf) {
    return requireNonNull(recordKeys, "native views keep record keys in their slab")[leaf];
  }

  /** The decoded slice of {@code field} on {@code leaf} in a heap view. */
  ColumnSlice slice(final int field, final int leaf) {
    return requireNonNull(slices, "native views keep lanes in their slab")[field][leaf];
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
    if (snapshot == null) {
      immutableMasks = copyMasks(rowMasks);
    }
  }

  public boolean isImmutableRevision() {
    if (snapshot != null) {
      return snapshot.masksEqual(rowMasks);
    }
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
    final NativeNumericSnapshot resident = snapshot;
    return resident != null
        ? resident.recordKey(leaf, row)
        : requireNonNull(recordKeys)[leaf][row];
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
    final NativeNumericSnapshot resident = snapshot;
    for (int leaf = 0; leaf < rowMasks.length; leaf++) {
      if (rowMasks[leaf] != null) {
        minimum = Math.min(minimum, resident != null
            ? resident.sliceMin(field, leaf)
            : requireNonNull(slices)[field][leaf].min());
      }
    }
    return minimum;
  }

  public long maximum(final int field) {
    long maximum = Long.MIN_VALUE;
    final NativeNumericSnapshot resident = snapshot;
    for (int leaf = 0; leaf < rowMasks.length; leaf++) {
      if (rowMasks[leaf] != null) {
        maximum = Math.max(maximum, resident != null
            ? resident.sliceMax(field, leaf)
            : requireNonNull(slices)[field][leaf].max());
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
    final NativeNumericSnapshot resident = snapshot;
    if (resident != null) {
      return scanSelectedPresentNative(resident, field);
    }
    final ColumnSlice[] fieldSlices = requireNonNull(slices)[field];
    for (int leaf = 0; leaf < rowMasks.length; leaf++) {
      final long[] mask = rowMasks[leaf];
      if (mask == null) {
        continue;
      }
      final ColumnSlice slice = fieldSlices[leaf];
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

  private boolean scanSelectedPresentNative(final NativeNumericSnapshot resident, final int field) {
    final MemorySegment slab = resident.slab();
    for (int leaf = 0; leaf < rowMasks.length; leaf++) {
      final long[] mask = rowMasks[leaf];
      if (mask == null) {
        continue;
      }
      if (resident.sliceRows(field, leaf) == 0) {
        return false;
      }
      final long presence = resident.presenceBase(field, leaf);
      for (int word = 0; word < mask.length; word++) {
        if ((mask[word] & ~SegmentAccess.getLong(slab, presence + 8L * word)) != 0L) {
          return false;
        }
      }
    }
    return true;
  }

  /** Immutable numeric values for a leaf of a heap view; native views hoist lane offsets instead. */
  public long[] numericLane(final int field, final int leaf) {
    if (snapshot != null) {
      throw new IllegalStateException("native views read lanes through their slab");
    }
    return requireNonNull(requireNonNull(slices)[field][leaf].numericValues(),
        "an admitted numeric field needs a numeric lane");
  }

  /** Whether {@code field} is present on {@code row} of {@code leaf}. */
  public boolean present(final int field, final int leaf, final int row) {
    final NativeNumericSnapshot resident = snapshot;
    if (resident != null) {
      return resident.present(field, leaf, row);
    }
    final ColumnSlice slice = requireNonNull(slices)[field][leaf];
    return slice.rowCount() > 0 && (slice.presenceWords()[row >>> 6] & 1L << (row & 63)) != 0L;
  }

  /** The long value of {@code field} on a PRESENT row. */
  public long longValue(final int field, final int leaf, final int row) {
    final NativeNumericSnapshot resident = snapshot;
    if (resident != null) {
      return resident.longValue(field, leaf, row);
    }
    return requireNonNull(requireNonNull(slices)[field][leaf].numericValues(),
        "an admitted long field needs a numeric lane")[row];
  }

  /**
   * The interned id of {@code field}'s string on a PRESENT row — equal ids are equal strings across
   * leaves and across every {@link MaskedColumns} sharing this interner.
   */
  public int stringId(final int field, final int leaf, final int row) {
    final ColumnSlice slice = stringSlice(field, leaf);
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

  private ColumnSlice stringSlice(final int field, final int leaf) {
    if (snapshot != null) {
      throw new IllegalStateException("numeric snapshots carry no string columns");
    }
    return requireNonNull(slices)[field][leaf];
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
    final NativeNumericSnapshot resident = snapshot;
    if (resident != null) {
      presentLongValuesNative(resident, field, values);
      return values;
    }
    final ColumnSlice[] fieldSlices = requireNonNull(slices)[field];
    for (int leaf = 0; leaf < rowMasks.length; leaf++) {
      final long[] mask = rowMasks[leaf];
      if (mask == null) {
        continue;
      }
      final ColumnSlice slice = fieldSlices[leaf];
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

  private void presentLongValuesNative(final NativeNumericSnapshot resident, final int field,
      final LongOpenHashSet values) {
    final MemorySegment slab = resident.slab();
    for (int leaf = 0; leaf < rowMasks.length; leaf++) {
      final long[] mask = rowMasks[leaf];
      if (mask == null || resident.sliceRows(field, leaf) <= 0) {
        continue;
      }
      final long presence = resident.presenceBase(field, leaf);
      if (!resident.laneAvailable(field, leaf)) {
        throw new IllegalStateException("an admitted long field needs a numeric lane");
      }
      final long stride = resident.laneStride();
      final long lane = resident.rowBase(leaf) + 8L * field;
      for (int w = 0; w < mask.length; w++) {
        long word = mask[w] & SegmentAccess.getLong(slab, presence + 8L * w);
        final long rowBase = lane + ((long) w << 6) * stride;
        while (word != 0L) {
          final int bit = Long.numberOfTrailingZeros(word);
          word &= word - 1L;
          values.add(SegmentAccess.getLong(slab, rowBase + bit * stride));
        }
      }
    }
  }

  /**
   * The record keys (ascending) of the admitted rows whose long {@code field} is in {@code values}
   * ({@code anti == false}), or whose field is missing or not in {@code values} ({@code anti ==
   * true}) — the interpreter's semi- and anti-join over an equality: a missing field matches nothing,
   * so it survives an anti-join and fails a semi-join.
   */
  public long[] recordKeysByMembership(final int field, final LongOpenHashSet values, final boolean anti) {
    final LongArrayList keys = new LongArrayList();
    final NativeNumericSnapshot resident = snapshot;
    if (resident != null) {
      recordKeysByMembershipNative(resident, field, values, anti, keys);
    } else {
      recordKeysByMembershipHeap(field, values, anti, keys);
    }
    final long[] sorted = keys.toLongArray();
    Arrays.sort(sorted);
    return sorted;
  }

  private void recordKeysByMembershipHeap(final int field, final LongOpenHashSet values, final boolean anti,
      final LongArrayList keys) {
    final ColumnSlice[] fieldSlices = requireNonNull(slices)[field];
    final long[][] leafKeyLanes = requireNonNull(recordKeys);
    for (int leaf = 0; leaf < rowMasks.length; leaf++) {
      final long[] mask = rowMasks[leaf];
      if (mask == null) {
        continue;
      }
      final ColumnSlice slice = fieldSlices[leaf];
      final boolean pruned = slice.rowCount() <= 0;
      final long[] presence = pruned
          ? null
          : slice.presenceWords();
      final long[] lane = pruned
          ? LongArrays.EMPTY_ARRAY
          : requireNonNull(slice.numericValues(), "an admitted membership field needs a numeric lane");
      final long[] leafKeys = leafKeyLanes[leaf];
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
  }

  private void recordKeysByMembershipNative(final NativeNumericSnapshot resident, final int field,
      final LongOpenHashSet values, final boolean anti, final LongArrayList keys) {
    final MemorySegment slab = resident.slab();
    for (int leaf = 0; leaf < rowMasks.length; leaf++) {
      final long[] mask = rowMasks[leaf];
      if (mask == null) {
        continue;
      }
      final boolean pruned = resident.sliceRows(field, leaf) <= 0;
      final long presence = resident.presenceBase(field, leaf);
      if (!pruned && !resident.laneAvailable(field, leaf)) {
        throw new IllegalStateException("an admitted membership field needs a numeric lane");
      }
      final long stride = resident.laneStride();
      final long lane = resident.rowBase(leaf) + 8L * field;
      final long leafKeys = resident.keyBase(leaf);
      for (int w = 0; w < mask.length; w++) {
        long word = mask[w];
        final long presenceWord = pruned
            ? 0L
            : SegmentAccess.getLong(slab, presence + 8L * w);
        final long rowBase = (long) w << 6;
        while (word != 0L) {
          final int bit = Long.numberOfTrailingZeros(word);
          word &= word - 1L;
          final long row = rowBase + bit;
          final boolean member =
              (presenceWord & 1L << bit) != 0L && values.contains(SegmentAccess.getLong(slab, lane + row * stride));
          if (anti
              ? !member
              : member) {
            keys.add(SegmentAccess.getLong(slab, leafKeys + 8L * row));
          }
        }
      }
    }
  }

  ColumnPredicate recordKeyPredicateByMembership(final int field, final long[] sortedKeys, final LongOpenHashSet values,
      final boolean anti) {
    if (field < 0 || field >= kinds.length || !isLong(field)) {
      throw new IllegalArgumentException("membership requires a numeric field");
    }
    final NativeNumericSnapshot resident = snapshot;
    if (resident != null) {
      final long[] laneBase = new long[rowMasks.length];
      for (int leaf = 0; leaf < laneBase.length; leaf++) {
        laneBase[leaf] = resident.rowBase(leaf) < 0 || !resident.laneAvailable(field, leaf)
            ? -1L
            : resident.rowBase(leaf) + 8L * field;
      }
      return ColumnPredicate.recordKeysByMembership(sortedKeys, store, rowMasks, resident.slab(), resident.keyBases(),
          resident.leafRowCounts(), laneBase, resident.laneStride(), resident.presenceBases(field), values, anti);
    }
    return ColumnPredicate.recordKeysByMembership(sortedKeys, store, rowMasks, requireNonNull(recordKeys),
        requireNonNull(slices)[field], values, anti);
  }

  /** The interned id of a string on a PRESENT row, through the shared interner when adopted. */
  public int sharedStringId(final int field, final int leaf, final int row) {
    final ColumnSlice slice = stringSlice(field, leaf);
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
