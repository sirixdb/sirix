/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.index.projection.ProjectionColumnStore.ColumnSlice;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.jspecify.annotations.Nullable;

import java.util.Arrays;

import static java.util.Objects.requireNonNull;

/**
 * Turns a segment-scoped column's cells into CANONICAL VALUE IDS, so a kernel that groups integers
 * groups by value rather than by {@code (segment, value)}.
 *
 * <h2>Why a kernel cannot just group on the cell</h2>
 *
 * Grouping by the packed cell is a pre-aggregation: one value occurring in two segments carries a
 * different cell in each and comes back as two groups. Where nothing is dropped that is harmless —
 * merge the keys afterwards and the counts add up. Where a TOP-K prunes first it is not: the two
 * halves of a value compete separately and can both lose to a value that happens to sit in one
 * segment, so the answer is a plausible, wrong top-K. Canonicalising before the kernel puts the merge
 * on the right side of the pruning.
 *
 * <h2>What it costs</h2>
 *
 * One dictionary resolve and one hash per DISTINCT CELL, cached for the whole query — never per row.
 * A leaf holds at most {@link ProjectionIndexRowGroupPage#MAX_ROWS} rows and therefore at most that
 * many distinct cells, and repeats across leaves cost a single {@code long} lookup. The per-row work
 * is one array read and one map lookup into a table that stays in cache.
 *
 * <h2>Threading: a lock-free steady state</h2>
 *
 * The aggregation pass is parallel over leaves, so a shared map guarded by one monitor would put a
 * global lock in front of every distinct cell — roughly a million acquisitions across a 1M scan, all
 * contended by fifteen workers. Instead the memo is a per-segment {@code int[]} indexed by the cell's
 * ID, which is dense because ids are arrival-order mints. A hit is two array reads and no lock at
 * all; only the FIRST touch of a given {@code (segment, id)} takes the monitor, to resolve the value
 * and issue its canonical id.
 *
 * <p>
 * A stale read of a table entry is benign: {@code int} writes never tear, so a reader sees either 0
 * (and takes the slow path, which is idempotent under the lock and returns the same id) or a
 * complete, correct id. The volatile publication of the table array gives the happens-before edge
 * that makes a filled entry visible without a fence on the read path.
 * </p>
 *
 * @author Johannes Lichtenberger <a href="mailto:lichtenberger.johannes@gmail.com">mail</a>
 */
public final class SegmentGroupCanonicaliser {

  /** Answer for a cell whose value this revision cannot resolve. */
  private static final int UNRESOLVABLE = -1;

  /**
   * How a packed cell becomes a value. Narrowed to this one operation so the merge semantics can be
   * tested without standing up a dictionary, and because the slow path is the only caller — the
   * indirection is never on the row path.
   */
  @FunctionalInterface
  public interface CellResolver {
    /** The value {@code cell} names, or {@code null} when this revision cannot resolve it. */
    @Nullable String valueOfCell(long cell);
  }

  private final CellResolver resolver;

  /**
   * {@code memo[segment][id]} is that cell's canonical id, {@code 0} when it has never been resolved.
   * Written only under this instance's monitor; read without one.
   */
  @SuppressWarnings("VolatileArrayField") // the volatile is on the reference, which is what publishes
  private volatile int[] @Nullable [] memo;

  /** Value to canonical id — what actually merges two segments' cells into one group. */
  private final Object2IntOpenHashMap<String> canonicalByValue = new Object2IntOpenHashMap<>();

  /** Canonical id {@code i} names {@code values.get(i - 1)}; ids are 1-based like dictionary ids. */
  private final ObjectArrayList<String> values = new ObjectArrayList<>();

  /**
   * After {@link #sealOrderPreserving}: {@code rankByArrival[arrivalId - 1]} is the id that lane
   * consumers see, ordered by VALUE. {@code null} while ids are arrival-ordered.
   */
  private int @Nullable [] rankByArrival;

  /** After sealing: {@code sortedValues[rank - 1]} — the inverse of {@link #rankByArrival}. */
  private String @Nullable [] sortedValues;

  /**
   * @param view resolver for the packed cells, typically a segment union view
   * @param segments how many segments the resource sealed, so the memo is sized once rather than
   *        grown under contention; a cell above it still resolves, through a grow on the slow path
   */
  public SegmentGroupCanonicaliser(final GlobalValueDictionary.ReadView view, final int segments) {
    this(requireNonNull(view, "view must not be null")::valueOfCell, segments);
  }

  /**
   * @param resolver how a packed cell becomes a value
   * @param segments how many segments the resource sealed
   */
  public SegmentGroupCanonicaliser(final CellResolver resolver, final int segments) {
    this.resolver = requireNonNull(resolver, "resolver must not be null");
    if (segments < 0) {
      throw new IllegalArgumentException("segments must not be negative: " + segments);
    }
    this.memo = new int[Math.max(segments, 1)][];
    this.canonicalByValue.defaultReturnValue(0);
  }

  /**
   * The same slices with their cells replaced by canonical ids, or the slices unchanged when there is
   * nothing to canonicalise.
   *
   * @return {@code null} when any cell cannot be resolved — the caller must then decline, because a
   *         group whose key has no value would silently become a group of its own
   */
  public ColumnSlice @Nullable [] canonicalise(final ColumnSlice @Nullable [] slices) {
    if (slices == null) {
      return null;
    }
    final ColumnSlice[] out = new ColumnSlice[slices.length];
    for (int i = 0; i < slices.length; i++) {
      final ColumnSlice slice = slices[i];
      if (slice == null) {
        continue;
      }
      final long[] cells = slice.numericValues();
      if (cells == null) {
        // A segment-scoped column IS a long lane, so a slice without one cannot be canonicalised.
        // Passing it through would be far worse than declining: its raw cells would then group
        // BESIDE canonical ids from the leaves that were canonicalised, and a segment-0 cell is a
        // small integer — exactly the space canonical ids occupy. Unrelated values would silently
        // land in the same group.
        return null;
      }
      final int rows = slice.rowCount();
      final long[] presence = slice.presenceWords();
      final long[] canonical = new long[cells.length];
      long min = Long.MAX_VALUE;
      long max = Long.MIN_VALUE;
      int present = 0;
      for (int row = 0; row < rows; row++) {
        // ABSENT ROWS ARE NOT CELLS. A row whose field is missing carries whatever the lane was
        // filled with — resolving that would either invent a group or, far worse, declare the whole
        // pass unresolvable and decline a query that is perfectly servable. The kernel reads presence
        // itself, so an absent row's canonical entry is never looked at.
        if ((presence[row >>> 6] & 1L << (row & 63)) == 0L) {
          continue;
        }
        final int id = laneIdOf(cells[row]);
        if (id == UNRESOLVABLE) {
          return null;
        }
        canonical[row] = id;
        present++;
        if (id < min) {
          min = id;
        }
        if (id > max) {
          max = id;
        }
      }
      if (present == 0) {
        min = 0;
        max = 0;
      }
      // The zone map must describe what the lane now HOLDS. Carrying the cells' min/max over would
      // let a range prune drop a leaf whose canonical ids are nowhere near them.
      out[i] = new ColumnSlice(rows, slice.flags(), min, max, slice.presenceWords(), canonical, slice.boolWords(),
          slice.stringDictIds(), slice.dictBytes(), slice.dictOffsets(), slice.setCounts(), slice.dictHashes());
    }
    return out;
  }

  /**
   * Resolve every present cell of {@code slices} into the value space, WITHOUT building canonical
   * lanes — {@link #size()} is then the number of distinct values those slices hold.
   *
   * <p>
   * The count-distinct shape: a caller that wants a cardinality has no use for the per-row ids, and
   * allocating a {@code long[]} per leaf to throw away would be the dominant cost of the answer. The
   * work that matters — one dictionary read per DISTINCT cell — is identical either way, because both
   * go through the same memo.
   * </p>
   *
   * @return {@code false} when a present cell has no value in this revision; the caller must decline
   */
  public boolean observe(final ColumnSlice @Nullable [] slices) {
    if (slices == null) {
      return false;
    }
    for (final ColumnSlice slice : slices) {
      if (slice == null) {
        continue;
      }
      final long[] cells = slice.numericValues();
      if (cells == null) {
        return false; // a segment-scoped column IS a long lane; see canonicalise
      }
      final long[] presence = slice.presenceWords();
      final int rows = slice.rowCount();
      for (int row = 0; row < rows; row++) {
        if ((presence[row >>> 6] & 1L << (row & 63)) == 0L) {
          continue; // an absent row holds no value and is not a distinct one
        }
        if (canonicalOf(cells[row]) == UNRESOLVABLE) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Freeze the value space and renumber: from here the canonical ids a lane carries are in COLLATION
   * order, so an integer {@code min}, {@code max} or sort over that lane answers the question about
   * the values themselves.
   *
   * <p>
   * The primitive the arrival-order ids cannot supply. A cell's id is a MINT — the dictionary hands
   * them out in the order values were first seen, and the seal stores by rank behind a rank table —
   * so comparing two raw cells says nothing about how their values collate. Sorting once, over the
   * values the column actually REFERENCES rather than the whole dictionary, buys an order for every
   * later comparison at no per-row cost.
   * </p>
   *
   * <p>
   * The order is {@link String#compareTo}, which is UTF-16 code-unit order — the same order
   * {@code ValueDictionaryEntryNode.compareUtf16Range} imposes on the dictionary's own storage, so a
   * lane ranked here and a dictionary sealed there agree.
   * </p>
   *
   * <p>
   * Call it only after {@link #observe} has seen every cell the query will group on: once sealed, a
   * value that was never observed has no rank, and rather than invent one this class reports the cell
   * as unresolvable so the caller declines.
   * </p>
   */
  public synchronized void sealOrderPreserving() {
    if (rankByArrival != null) {
      return; // idempotent: a second seal would renumber ids the caller is already carrying
    }
    final int count = values.size();
    final int[] arrivalByRank = new int[count];
    for (int i = 0; i < count; i++) {
      arrivalByRank[i] = i + 1;
    }
    IntArrays.quickSort(arrivalByRank, (left, right) -> values.get(left - 1).compareTo(values.get(right - 1)));
    final int[] ranks = new int[count];
    final String[] sorted = new String[count];
    for (int rank = 0; rank < count; rank++) {
      final int arrival = arrivalByRank[rank];
      ranks[arrival - 1] = rank + 1;
      sorted[rank] = values.get(arrival - 1);
    }
    sortedValues = sorted;
    rankByArrival = ranks; // last: a reader that sees this sees both tables
  }

  /**
   * The canonical id a LITERAL takes in this value space, minting one if the column never held it.
   *
   * <p>
   * What a conditional key needs: {@code CASE WHEN c THEN col ELSE lit END} must put an else-row and
   * a then-row whose value IS {@code lit} in the SAME group, which only holds if both sides speak one
   * identity space. Hashing the literal's bytes instead would leave them in two domains and split a
   * group that the query defines as one.
   * </p>
   *
   * @throws IllegalStateException after {@link #sealOrderPreserving}, where a new value has no rank
   */
  public synchronized int canonicalOfValue(final String value) {
    requireNonNull(value, "value must not be null");
    int canonical = canonicalByValue.getInt(value);
    if (canonical != 0) {
      final int[] ranks = rankByArrival;
      return ranks == null
          ? canonical
          : ranks[canonical - 1];
    }
    if (rankByArrival != null) {
      throw new IllegalStateException("the value space is sealed; '" + value + "' has no rank");
    }
    values.add(value);
    canonical = values.size();
    canonicalByValue.put(value, canonical);
    return canonical;
  }

  /** Whether {@link #sealOrderPreserving} has run, so lane ids are in collation order. */
  public boolean isOrderPreserving() {
    return rankByArrival != null;
  }

  /**
   * The id a lane carries for {@code cell}: the arrival-order canonical id, or its RANK once the
   * value space has been sealed in collation order.
   */
  private int laneIdOf(final long cell) {
    final int arrival = canonicalOf(cell);
    if (arrival == UNRESOLVABLE) {
      return UNRESOLVABLE;
    }
    final int[] ranks = rankByArrival;
    if (ranks == null) {
      return arrival;
    }
    if (arrival > ranks.length) {
      // A value first seen AFTER the seal. Its rank would have to be invented, and any choice would
      // misorder it against everything already ranked, so the pass declines instead.
      return UNRESOLVABLE;
    }
    return ranks[arrival - 1];
  }

  /** The value a lane id names, or {@code null} when the id was never issued. */
  public synchronized @Nullable String valueOf(final int laneId) {
    final String[] sorted = sortedValues;
    if (sorted != null) {
      return laneId >= 1 && laneId <= sorted.length
          ? sorted[laneId - 1]
          : null;
    }
    return laneId >= 1 && laneId <= values.size()
        ? values.get(laneId - 1)
        : null;
  }

  /**
   * A per-id string-length table over this canonicaliser's id space — the segment-scoped twin of the
   * global dictionary's length table, and indexed exactly the same way, so the kernels need no arm of
   * their own: they index {@code table[id]} with the id the canonicalised lane already carries.
   *
   * @param lengthMode {@link ProjectionIndexByteScan#STRING_LENGTH_UTF8_BYTES} or
   *        {@link ProjectionIndexByteScan#STRING_LENGTH_CODE_POINTS}
   * @return {@code table[id]} for {@code id} in {@code 1..size()}; slot 0 is unused, as ids are
   *         1-based
   */
  public synchronized int[] lengthTable(final byte lengthMode) {
    if (lengthMode != ProjectionIndexByteScan.STRING_LENGTH_UTF8_BYTES
        && lengthMode != ProjectionIndexByteScan.STRING_LENGTH_CODE_POINTS) {
      throw new IllegalArgumentException("not a string-length mode: " + lengthMode);
    }
    final int count = size();
    final int[] table = new int[count + 1];
    for (int id = 1; id <= count; id++) {
      final String value = valueOf(id);
      if (value == null) {
        throw new IllegalStateException("canonical id " + id + " names no value");
      }
      table[id] = lengthMode == ProjectionIndexByteScan.STRING_LENGTH_CODE_POINTS
          ? value.codePointCount(0, value.length())
          : utf8Length(value);
    }
    return table;
  }

  /** UTF-8 byte length without encoding the string — the same count the dictionary stores. */
  private static int utf8Length(final String value) {
    int bytes = 0;
    final int length = value.length();
    for (int i = 0; i < length; i++) {
      final char c = value.charAt(i);
      if (c < 0x80) {
        bytes++;
      } else if (c < 0x800) {
        bytes += 2;
      } else if (Character.isHighSurrogate(c) && i + 1 < length && Character.isLowSurrogate(value.charAt(i + 1))) {
        bytes += 4; // one supplementary code point, encoded from the surrogate PAIR
        i++;
      } else {
        bytes += 3;
      }
    }
    return bytes;
  }

  /** Distinct values seen so far (test and diagnostic observability). */
  public synchronized int size() {
    return values.size();
  }

  /**
   * The canonical id of {@code cell}. The fast path is lock-free; the monitor is taken only the first
   * time a given {@code (segment, id)} is asked for.
   */
  private int canonicalOf(final long cell) {
    final int segment = ProjectionIndexRowGroupPage.segmentOfCell(cell);
    final int id = ProjectionIndexRowGroupPage.idOfCell(cell);
    final int[][] tables = memo;
    if (segment >= 0 && segment < tables.length && id >= 0) {
      final int[] table = tables[segment];
      if (table != null && id < table.length) {
        final int memoised = table[id];
        if (memoised != 0) {
          return memoised;
        }
      }
    }
    return resolveAndMemoise(cell, segment, id);
  }

  private synchronized int resolveAndMemoise(final long cell, final int segment, final int id) {
    if (segment < 0 || id < 0) {
      return UNRESOLVABLE;
    }
    int[][] tables = memo;
    if (segment >= tables.length) {
      tables = Arrays.copyOf(tables, segment + 1);
    }
    int[] table = tables[segment];
    if (table == null) {
      table = new int[Math.max(id + 1, 1024)];
      tables[segment] = table;
    } else if (id >= table.length) {
      // Ids are dense mints, so growth is rare; double so a scan that walks a dictionary upward does
      // not copy per entry.
      table = Arrays.copyOf(table, Math.max(id + 1, table.length << 1));
      tables[segment] = table;
    } else if (table[id] != 0) {
      return table[id]; // another worker resolved it between the fast-path read and this lock
    }
    final int canonical = issueCanonical(cell);
    table[id] = canonical;
    memo = tables; // volatile write: publishes both the entry above and any grown array
    return canonical;
  }

  private int issueCanonical(final long cell) {
    final String value;
    try {
      value = resolver.valueOfCell(cell);
    } catch (final RuntimeException unresolvable) {
      return UNRESOLVABLE;
    }
    if (value == null) {
      return UNRESOLVABLE;
    }
    int canonical = canonicalByValue.getInt(value);
    if (canonical == 0) {
      values.add(value);
      canonical = values.size();
      canonicalByValue.put(value, canonical);
    }
    return canonical;
  }
}
