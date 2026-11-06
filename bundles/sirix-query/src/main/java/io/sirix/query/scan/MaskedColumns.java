package io.sirix.query.scan;

import static java.util.Objects.requireNonNull;

import io.sirix.cache.Allocators;
import io.sirix.index.projection.NumericLane;
import io.sirix.index.projection.ProjectionColumnStore;
import io.sirix.index.projection.ProjectionColumnStore.ColumnSlice;
import io.sirix.index.projection.ProjectionIndexRowGroupPage;
import io.sirix.index.projection.ProjectionIndexScan.ColumnPredicate;
import io.sirix.node.SegmentAccess;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.jspecify.annotations.Nullable;

import java.lang.foreign.MemorySegment;
import java.lang.ref.Cleaner;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

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
 * <b>Representation and lifetime</b> (docs/DERIVED_QUERY_CACHES.md). Every view reads a long lane
 * through its slice's segment ({@link #laneSegment}, {@link #laneBase}), hoisted once per leaf: the
 * store's resident column slab, the slab of the cache entry a leased view reads, this view's own
 * query-local slab, or — only when the allocator refused the memory — a heap array. What a view
 * releases on {@link #close()} is exactly what it holds: the lease on its entry, or its own slab; a
 * view over resident columns holds nothing itself, the query scope pins them. A {@link Cleaner}
 * releases what a view that was never closed held. The masks are always the view's own heap copy.
 * </p>
 */
public final class MaskedColumns implements AutoCloseable {

  /** Backstop for a view that was never closed: the release action, never the view, is registered. */
  private static final Cleaner CLEANER = Cleaner.create();

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
  /** Final once the view is published or served from a cache entry; see {@link #adoptIdentity}. */
  private Object rowIdentity;
  /**
   * Per field: 0 unknown, 1 every selected row present, 2 not; shared by the views of one entry.
   */
  private final byte[] presenceMemo;
  /** The cache entry a leased view reads, for task leases that outlive the view. */
  private DerivedCacheBudget.@Nullable Entry entry;
  /** What {@link #close()} releases — the entry lease or the owned slab; runs once. */
  private @Nullable Runnable release;
  private @Nullable MaskedColumns sharedInterner;

  MaskedColumns(final ProjectionColumnStore store, final long[][] rowMasks, final long[][] recordKeys,
      final ColumnSlice[][] slices, final byte[] kinds, final long rows) {
    this(store, rowMasks, recordKeys, slices, kinds, rows, new Object(), null, new byte[kinds.length]);
  }

  /**
   * A view over a cached {@link LaneSnapshot}: shared slices (lanes in the entry's slab or the
   * store's resident columns) and record keys, this view's own mask copy, the entry's identity and
   * original masks. The entry attaches the lease through {@link #adoptLease}.
   */
  MaskedColumns(final ProjectionColumnStore store, final long[][] masks, final long[][] recordKeys,
      final ColumnSlice[][] slices, final byte[] kinds, final long rows, final Object rowIdentity,
      final long @Nullable [][] immutableMasks, final byte[] presenceMemo) {
    this.store = requireNonNull(store);
    this.rowMasks = requireNonNull(masks);
    this.recordKeys = requireNonNull(recordKeys);
    this.slices = requireNonNull(slices);
    this.kinds = requireNonNull(kinds);
    if (slices.length != kinds.length || presenceMemo.length != kinds.length) {
      throw new IllegalArgumentException("one slice array and one presence memo per field");
    }
    if (recordKeys.length != masks.length) {
      throw new IllegalArgumentException("one record-key lane per leaf");
    }
    this.rows = rows;
    this.dictToInterned = new int[kinds.length][masks.length][];
    this.rowIdentity = requireNonNull(rowIdentity);
    this.immutableMasks = immutableMasks;
    this.presenceMemo = presenceMemo;
    strings.add(null);
    stringIds.defaultReturnValue(0);
  }

  /**
   * Attach the lease this view holds on {@code held}, the cache entry it reads; released by
   * {@link #close()}, or by the cleaner backstop for a view that is never closed.
   */
  void adoptLease(final DerivedCacheBudget.Entry held, final DerivedCacheBudget.Lease lease) {
    adoptRelease(lease);
    entry = requireNonNull(held);
  }

  /**
   * Make this (query-local) view the owner of {@code slab}, the memory its lanes were decoded into:
   * closing the view returns it to the allocator.
   */
  void ownSlab(final MemorySegment slab) {
    adoptRelease(new SlabRelease(slab));
  }

  private void adoptRelease(final Runnable once) {
    if (release != null) {
      throw new IllegalStateException("view already holds a lease or a slab");
    }
    release = once;
    CLEANER.register(this, once);
  }

  /**
   * Take the published entry's row identity, so derived state built by this (publisher's) view, the
   * packed join sides above all, is found by every later view over the same entry.
   */
  void adoptIdentity(final Object identity) {
    rowIdentity = requireNonNull(identity);
  }

  /** Whether this view reads a cache entry (a lease is held; see {@link #acquireTaskLease()}). */
  public boolean isCached() {
    return entry != null;
  }

  /**
   * A lease on this view's cache entry for a background task that outlives the view, released by that
   * task; {@code null} for a view that reads no entry — such a view's lanes are not guaranteed to
   * exist once it is closed, so a task must not read them.
   */
  public DerivedCacheBudget.@Nullable Lease acquireTaskLease() {
    final DerivedCacheBudget.Entry held = entry;
    if (held == null) {
      return null;
    }
    if (!held.tryAcquire()) {
      throw new IllegalStateException("a leased entry cannot have been freed");
    }
    return new DerivedCacheBudget.Lease(held);
  }

  /** Release what this view holds (its entry lease or its slab); idempotent. */
  @Override
  public void close() {
    final Runnable held = release;
    if (held != null) {
      release = null;
      entry = null;
      held.run();
    }
  }

  /** A once-only return of a query-local slab to the allocator. */
  private static final class SlabRelease implements Runnable {
    private final MemorySegment slab;
    private final AtomicBoolean released = new AtomicBoolean();

    SlabRelease(final MemorySegment slab) {
      this.slab = requireNonNull(slab);
    }

    @Override
    public void run() {
      if (released.compareAndSet(false, true)) {
        Allocators.getInstance().releaseSlab(slab);
      }
    }
  }

  ColumnSlice[][] slices() {
    return slices;
  }

  long[][] recordKeys() {
    return recordKeys;
  }

  /**
   * The original masks of an immutable revision, or {@code null} before
   * {@link #markImmutableRevision}.
   */
  long @Nullable [][] immutableMasks() {
    return immutableMasks;
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

  byte[] kinds() {
    return kinds;
  }

  byte[] presenceMemo() {
    return presenceMemo;
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
   * An identity shared only by one cached selection and its views, including its field roster.
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
    final ColumnSlice[] fieldSlices = slices[field];
    for (int leaf = 0; leaf < rowMasks.length; leaf++) {
      if (rowMasks[leaf] != null) {
        minimum = Math.min(minimum, fieldSlices[leaf].min());
      }
    }
    return minimum;
  }

  public long maximum(final int field) {
    long maximum = Long.MIN_VALUE;
    final ColumnSlice[] fieldSlices = slices[field];
    for (int leaf = 0; leaf < rowMasks.length; leaf++) {
      if (rowMasks[leaf] != null) {
        maximum = Math.max(maximum, fieldSlices[leaf].max());
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
    final ColumnSlice[] fieldSlices = slices[field];
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

  /**
   * The segment holding {@code field}'s lane on {@code leaf}: the store's column slab, the cache
   * entry's slab, this view's own slab, or (allocator refusal) the heap array viewed as a segment.
   * Kernels hoist it per leaf with {@link #laneBase(int, int)} and read eight bytes per row at linear
   * offsets.
   */
  public MemorySegment laneSegment(final int field, final int leaf) {
    return slices[field][leaf].laneSegment();
  }

  /**
   * Byte offset of {@code field}'s lane on {@code leaf} within {@link #laneSegment(int, int)}. Row
   * {@code r} of the lane lies {@code 8 * r} bytes past it.
   */
  public long laneBase(final int field, final int leaf) {
    return slices[field][leaf].laneBase();
  }

  /** Whether {@code field} is present on {@code row} of {@code leaf}. */
  public boolean present(final int field, final int leaf, final int row) {
    final ColumnSlice slice = slices[field][leaf];
    return slice.rowCount() > 0 && (slice.presenceWords()[row >>> 6] & 1L << (row & 63)) != 0L;
  }

  /** The long value of {@code field} on a PRESENT row. */
  public long longValue(final int field, final int leaf, final int row) {
    final ColumnSlice slice = slices[field][leaf];
    final NumericLane lane = slice.lane();
    return lane != null
        ? lane.get(row)
        : requireNonNull(slice.numericValues(), "an admitted long field needs a numeric lane")[row];
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

  private int internShared(final String value) {
    return sharedInterner != null
        ? sharedInterner.intern(value)
        : intern(value);
  }

  /** Every present long value of {@code field} over the admitted rows. */
  public LongOpenHashSet presentLongValues(final int field) {
    final LongOpenHashSet values = new LongOpenHashSet();
    final ColumnSlice[] fieldSlices = slices[field];
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
      final MemorySegment lane = slice.laneSegment();
      final long laneBase = slice.laneBase();
      for (int w = 0; w < mask.length; w++) {
        long word = mask[w] & presence[w];
        final int rowBase = w << 6;
        while (word != 0L) {
          final int bit = Long.numberOfTrailingZeros(word);
          word &= word - 1L;
          values.add(SegmentAccess.getLong(lane, laneBase + ((long) (rowBase + bit) << 3)));
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
    final ColumnSlice[] fieldSlices = slices[field];
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
      final MemorySegment lane = pruned
          ? MemorySegment.NULL
          : slice.laneSegment();
      final long laneBase = pruned
          ? 0L
          : slice.laneBase();
      final long[] leafKeys = recordKeys[leaf];
      for (int w = 0; w < mask.length; w++) {
        long word = mask[w];
        final int rowBase = w << 6;
        while (word != 0L) {
          final int bit = Long.numberOfTrailingZeros(word);
          word &= word - 1L;
          final int row = rowBase + bit;
          final boolean present = presence != null && (presence[w] & 1L << bit) != 0L;
          final boolean member = present && values.contains(SegmentAccess.getLong(lane, laneBase + ((long) row << 3)));
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
