/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.index.projection.ProjectionColumnStore.ColumnSlice;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
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

    /**
     * A content hash of that value, {@code 0} when the cell resolves to nothing.
     *
     * <p>
     * The default builds the String, which is what a TRANSFORMING resolver has to do anyway. A plain
     * one overrides it to hash the dictionary's stored bytes and allocate nothing — the difference
     * between 150 bytes per distinct value and none, which at eighteen million values is the
     * difference between running and an OutOfMemoryError.
     * </p>
     */
    default long hashOfCell(final long cell) {
      final String value = valueOfCell(cell);
      if (value == null) {
        return 0L;
      }
      final byte[] utf8 = value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
      final long hash = ProjectionIndexByteScan.fnv1a64(utf8, 0, utf8.length);
      return hash == 0L
          ? 1L
          : hash; // 0 is reserved for "unresolvable"
    }

    /** Whether two cells name the SAME value — the check that keeps a hash collision harmless. */
    default boolean sameValue(final long left, final long right) {
      final String a = valueOfCell(left);
      return a != null && a.equals(valueOfCell(right));
    }

    /**
     * Order two cells by the values they name, under the dictionary's collation.
     *
     * <p>
     * The default compares Strings, which is UTF-16 code-unit order — the same order
     * {@code compareUtf16Range} imposes on the dictionary's storage, so the two agree. A plain
     * resolver overrides it with {@code compareCells} and touches no String at all.
     * </p>
     */
    default int compareValues(final long left, final long right) {
      final String a = valueOfCell(left);
      final String b = valueOfCell(right);
      if (a == null || b == null) {
        return a == null
            ? (b == null
                ? 0
                : -1)
            : 1;
      }
      return a.compareTo(b);
    }
  }

  private final CellResolver resolver;

  /**
   * {@code memo[segment][id]} is that cell's canonical id, {@code 0} when it has never been resolved.
   * Written only under this instance's monitor; read without one.
   */
  @SuppressWarnings("VolatileArrayField") // the volatile is on the reference, which is what publishes
  private volatile int[] @Nullable [] memo;

  /**
   * Content hash of a value to the canonical ids carrying it — what merges two segments' cells into
   * one group.
   *
   * <p>
   * Keyed by a HASH of the stored bytes rather than by a materialised {@link String}: one String per
   * distinct value costs about 150 bytes and a GC-visible object, which is nothing at a million
   * distinct values and 2.7 GB at eighteen million — measured, as an OutOfMemoryError at 100M. The
   * chain behind each hash keeps the identity EXACT: a hit is confirmed by comparing the two cells'
   * bytes, so a collision costs one extra compare and never merges two values.
   * </p>
   */
  private final Long2ObjectOpenHashMap<int[]> idsByHash = new Long2ObjectOpenHashMap<>();

  /**
   * A cell that carries canonical id {@code i}, at {@code i - 1}. Eight bytes standing in for the
   * value itself: the bytes stay in the dictionary, and only a WINNER is ever turned into a String.
   */
  private final LongArrayList representativeCell = new LongArrayList();

  /** Ids issued for LITERALS, which no cell carries; a conditional key's else branch needs one. */
  private final Object2IntOpenHashMap<String> canonicalByLiteral = new Object2IntOpenHashMap<>();

  private final Int2ObjectOpenHashMap<String> literalById = new Int2ObjectOpenHashMap<>();

  /**
   * After {@link #sealOrderPreserving}: {@code rankByArrival[arrivalId - 1]} is the id that lane
   * consumers see, ordered by VALUE. {@code null} while ids are arrival-ordered.
   */
  private int @Nullable [] rankByArrival;

  /** After sealing: {@code sortedArrival[rank - 1]} is the arrival id ranked there. */
  private int @Nullable [] sortedArrival;

  /**
   * @param view resolver for the packed cells, typically a segment union view
   * @param segments how many segments the resource sealed, so the memo is sized once rather than
   *        grown under contention; a cell above it still resolves, through a grow on the slow path
   */
  public SegmentGroupCanonicaliser(final GlobalValueDictionary.ReadView view, final int segments) {
    this(byteResolver(requireNonNull(view, "view must not be null")), segments);
  }

  /** The untransformed resolver: hashes and compares the dictionary's own bytes, allocating nothing. */
  private static CellResolver byteResolver(final GlobalValueDictionary.ReadView view) {
    return new CellResolver() {
      @Override
      public @Nullable String valueOfCell(final long cell) {
        return view.valueOfCell(cell); // winners only
      }

      @Override
      public long hashOfCell(final long cell) {
        return view.cellHash(cell);
      }

      @Override
      public boolean sameValue(final long left, final long right) {
        return view.compareCells(left, right) == 0;
      }

      @Override
      public int compareValues(final long left, final long right) {
        return view.compareCells(left, right);
      }
    };
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
      // HOISTED ONCE PER LEAF, not read per row: `memo` is volatile and `laneIdOf` is a call, and a
      // leaf never straddles a segment, so both answer the same thing for every row of it. What the
      // row loop keeps is an array read; a cell whose entry is not settled yet — at most one per
      // distinct cell over the whole query — is the only one that calls back.
      final int[][] tables = memo;
      final int[] ranks = rankByArrival;
      int[] settled = null;
      for (int row = 0; row < rows; row++) {
        // ABSENT ROWS ARE NOT CELLS. A row whose field is missing carries whatever the lane was
        // filled with — resolving that would either invent a group or, far worse, declare the whole
        // pass unresolvable and decline a query that is perfectly servable. The kernel reads presence
        // itself, so an absent row's canonical entry is never looked at.
        if ((presence[row >>> 6] & 1L << (row & 63)) == 0L) {
          continue;
        }
        final long cell = cells[row];
        if (settled == null) {
          final int segment = ProjectionIndexRowGroupPage.segmentOfCell(cell);
          settled = segment >= 0 && segment < tables.length
              ? tables[segment]
              : null;
          if (settled == null) {
            settled = NO_SETTLED; // the segment has no table yet; every row takes the slow path
          }
        }
        final int cellId = ProjectionIndexRowGroupPage.idOfCell(cell);
        final int arrival = cellId >= 0 && cellId < settled.length
            ? settled[cellId]
            : 0;
        final int id = arrival != 0
            ? rankOf(arrival, ranks)
            : laneIdOf(cell);
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
  public synchronized boolean sealOrderPreserving() {
    if (rankByArrival != null) {
      return true; // idempotent: a second seal would renumber ids the caller is already carrying
    }
    final int count = representativeCell.size();
    if (count > MAX_ORDERED_VALUES) {
      // ORDERING IS THE ONE THING THAT DOES NOT SCALE HERE. Grouping, counting and predicate
      // evaluation are each one pass over the distinct values; a total order is n log n COMPARISONS,
      // and every comparison reads two dictionary entries. At eighteen million distinct URLs that is
      // hundreds of millions of reads for one query. Refuse, and let the caller decline to a pipeline
      // that is slower but finishes — the honest answer until MIN/MAX folds per segment and merges,
      // which is what a column store with block-local dictionaries actually does.
      return false;
    }
    // Materialise once, sort on that. Comparing through the dictionary instead would pay two reads
    // per comparison, which measured 2-3x slower on the queries that seal.
    final String[] byArrival = new String[count];
    for (int arrival = 1; arrival <= count; arrival++) {
      byArrival[arrival - 1] = resolver.valueOfCell(representativeCell.getLong(arrival - 1));
      if (byArrival[arrival - 1] == null) {
        return false;
      }
    }
    final int[] arrivalByRank = new int[count];
    for (int i = 0; i < count; i++) {
      arrivalByRank[i] = i + 1;
    }
    IntArrays.quickSort(arrivalByRank, (left, right) -> byArrival[left - 1].compareTo(byArrival[right - 1]));
    final int[] ranks = new int[count];
    final int[] sorted = new int[count];
    for (int rank = 0; rank < count; rank++) {
      final int arrival = arrivalByRank[rank];
      ranks[arrival - 1] = rank + 1;
      sorted[rank] = arrival;
    }
    sortedArrival = sorted;
    rankByArrival = ranks; // last: a reader that sees this sees both tables
    return true;
  }

  /**
   * Distinct values above which {@link #sealOrderPreserving} refuses. Sized so a 1M-shaped column
   * seals and a 100M-shaped one declines rather than spending minutes or the heap on a total order.
   */
  private static final int MAX_ORDERED_VALUES = 2_000_000;

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
    int canonical = canonicalByLiteral.getInt(value);
    if (canonical != 0) {
      final int[] ranks = rankByArrival;
      return ranks == null
          ? canonical
          : ranks[canonical - 1];
    }
    if (rankByArrival != null) {
      throw new IllegalStateException("the value space is sealed; '" + value + "' has no rank");
    }
    // A literal has no cell of its own; it takes an id past every cell-backed one and is remembered
    // here rather than in the dictionary, because nothing in the column may hold it.
    literalById.put(representativeCell.size() + literalById.size() + 1, value);
    canonical = representativeCell.size() + literalById.size();
    canonicalByLiteral.put(value, canonical);
    return canonical;
  }

  /** Whether {@link #sealOrderPreserving} has run, so lane ids are in collation order. */
  public boolean isOrderPreserving() {
    return rankByArrival != null;
  }

  /** Empty stand-in so the row loop never re-tests for a missing table. */
  private static final int[] NO_SETTLED = new int[0];

  /** An arrival id as the lane carries it: itself, or its rank once the value space is sealed. */
  private static int rankOf(final int arrival, final int @Nullable [] ranks) {
    if (ranks == null) {
      return arrival;
    }
    return arrival > ranks.length
        ? UNRESOLVABLE
        : ranks[arrival - 1];
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
    final int[] ranked = sortedArrival;
    if (ranked != null) {
      return laneId >= 1 && laneId <= ranked.length
          ? resolver.valueOfCell(representativeCell.getLong(ranked[laneId - 1] - 1))
          : null;
    }
    return laneId >= 1 && laneId <= representativeCell.size()
        ? resolver.valueOfCell(representativeCell.getLong(laneId - 1))
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
    return representativeCell.size();
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

  /**
   * Resolve one cell to its canonical id and remember it.
   *
   * <p>
   * The dictionary read stays UNDER this monitor. {@link GlobalValueDictionary.ReadView} carries plain
   * mutable caches — a per-id slice cache, a retained bucket and block — so two threads hashing
   * through one view tear each other's state; the read either fails to parse or, far worse, returns a
   * hash for a torn slice and puts two different values in one group. Hoisting the hash out of the
   * lock to unblock the scan was measured and REVERTED for exactly that reason; the way to parallelise
   * it is a view per worker, not a smaller critical section.
   * </p>
   */
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
    final long hash;
    try {
      hash = resolver.hashOfCell(cell);
    } catch (final RuntimeException unresolvable) {
      return UNRESOLVABLE;
    }
    if (hash == 0L) {
      return UNRESOLVABLE; // the cell names no entry; 0 is not a hash here
    }
    final int[] existing = idsByHash.get(hash);
    if (existing != null) {
      for (final int candidate : existing) {
        // EXACT, not hash-equal: two values sharing a hash must not share a group, so the bytes
        // decide. A collision costs one comparison and is otherwise invisible.
        if (resolver.sameValue(cell, representativeCell.getLong(candidate - 1))) {
          return candidate;
        }
      }
    }
    representativeCell.add(cell);
    final int canonical = representativeCell.size();
    idsByHash.put(hash, existing == null
        ? new int[] {canonical}
        : append(existing, canonical));
    return canonical;
  }

  private static int[] append(final int[] chain, final int id) {
    final int[] grown = Arrays.copyOf(chain, chain.length + 1);
    grown[chain.length] = id;
    return grown;
  }

}
