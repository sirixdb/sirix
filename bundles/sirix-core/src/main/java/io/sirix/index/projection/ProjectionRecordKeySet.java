/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.IntToLongFunction;

import io.sirix.index.projection.ProjectionColumnStore.ColumnSlice;
import io.sirix.node.SegmentAccess;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.jspecify.annotations.Nullable;

/**
 * The index-routed row source: membership of a leaf's record keys in a SORTED key set, the
 * primitive behind {@link ProjectionIndexScan.Op#KEY_IN}.
 *
 */
public final class ProjectionRecordKeySet {

  private static final LongAdder KEY_SET_ADVANCES = new LongAdder();
  private static final boolean DIAG = Boolean.getBoolean("sirix.projection.segmentDiag");
  private static final LongAdder LOOKUP_DESCRIPTORS = new LongAdder();
  private static final LongAdder LOOKUP_KEY_SEGMENTS = new LongAdder();
  private static final LongAdder DENSE_ROWS = new LongAdder();
  private static final LongAdder MEMBERSHIP_STAGED_LEAVES = new LongAdder();
  private static final LongAdder MEMBERSHIP_GATHERED_WORDS = new LongAdder();
  private static final LongAdder MEMBERSHIP_IN_PLACE_WORDS = new LongAdder();

  public record Masks(Long2ObjectOpenHashMap<long[]> byFirstKey, LongOpenHashSet physicalSlots) {
  }

  private ProjectionRecordKeySet() {}

  public static long keySetAdvances() {
    return KEY_SET_ADVANCES.sum();
  }

  public static long lookupDescriptorsRead() {
    return LOOKUP_DESCRIPTORS.sum();
  }

  public static long lookupKeySegmentsRead() {
    return LOOKUP_KEY_SEGMENTS.sum();
  }

  public static long denseRowsVisited() {
    return DENSE_ROWS.sum();
  }

  /**
   * Leaves whose value lane {@link #filterMembership} staged with one counted copy. Read by the tests
   * that pin which read form a given candidate density reaches; a counter, not a budget.
   */
  public static long membershipStagedLeaves() {
    return MEMBERSHIP_STAGED_LEAVES.sum();
  }

  /** Words {@link #filterMembership} gathered into its 512-byte window. */
  public static long membershipGatheredWords() {
    return MEMBERSHIP_GATHERED_WORDS.sum();
  }

  /** Words {@link #filterMembership} read in place, one checked lane read per candidate row. */
  public static long membershipInPlaceWords() {
    return MEMBERSHIP_IN_PLACE_WORDS.sum();
  }

  static Masks map(final long[] sortedKeys, final ProjectionPersistedRecordLookup lookup) {
    final Long2ObjectOpenHashMap<long[]> masks = new Long2ObjectOpenHashMap<>();
    final LongOpenHashSet slots = new LongOpenHashSet();
    for (final long key : sortedKeys) {
      final long location = lookup.find(key);
      if (location == ProjectionPersistedRecordLookup.ABSENT) {
        continue;
      }
      final int slot = ProjectionPersistedRecordLookup.slot(location);
      final long[] keys = lookup.keys(slot).view().recordKeys();
      long[] mask = masks.get(keys[0]);
      if (mask == null) {
        mask = new long[(keys.length + 63) >>> 6];
        masks.put(keys[0], mask);
        slots.add(slot);
      }
      final int row = ProjectionPersistedRecordLookup.row(location);
      mask[row >>> 6] |= 1L << (row & 63);
    }
    if (DIAG) {
      LOOKUP_DESCRIPTORS.add(lookup.descriptorsRead());
      LOOKUP_KEY_SEGMENTS.add(lookup.keySegmentsRead());
    }
    return new Masks(masks, slots);
  }

  static Masks map(final long[] sortedKeys, final long[][] leafKeys) {
    return map(sortedKeys, leafKeys, leaf -> leaf + 1L);
  }

  static Masks map(final long[] sortedKeys, final long[][] leafKeys, final IntToLongFunction physicalSlot) {
    final Long2ObjectOpenHashMap<long[]> masks = new Long2ObjectOpenHashMap<>(leafKeys.length);
    final LongOpenHashSet slots = new LongOpenHashSet();
    int cursor = 0;
    long previous = Long.MIN_VALUE;
    long visited = 0;
    for (int leaf = 0; leaf < leafKeys.length; leaf++) {
      final long[] keys = leafKeys[leaf];
      visited += keys.length;
      if (keys.length == 0) {
        continue;
      }
      final long[] mask = new long[(keys.length + 63) >>> 6];
      long matchedBits = 0;
      // Leaf rows and the key set are both ascending in the common case, so one merge cursor serves
      // every row. Membership is folded into the mask arithmetically: with a third of the rows
      // selected, a data-dependent branch per row mispredicts constantly and dominated this walk.
      // Each 64-row word is accumulated in a register and stored once; updating the mask word in
      // memory per row would chain every row of the word through one read-modify-write.
      final int limit = sortedKeys.length;
      final int rows = keys.length;
      int row = 0;
      for (int word = 0; word < mask.length; word++) {
        final int end = Math.min(row + 64, rows);
        long bits = 0;
        for (int bit = 0; row < end; row++, bit++) {
          final long key = keys[row];
          final long member;
          if (key >= previous) {
            while (cursor < limit && sortedKeys[cursor] < key) {
              cursor++;
            }
            member = cursor < limit && sortedKeys[cursor] == key
                ? 1L
                : 0L;
            previous = key;
          } else {
            member = Arrays.binarySearch(sortedKeys, 0, cursor, key) >= 0
                ? 1L
                : 0L;
          }
          bits |= member << bit;
        }
        mask[word] = bits;
        matchedBits |= bits;
      }
      final boolean matched = matchedBits != 0;
      if (matched && masks.put(keys[0], mask) != null) {
        throw new IllegalArgumentException("projection leaves repeat a record key");
      }
      if (matched) {
        slots.add(physicalSlot.applyAsLong(leaf));
      }
    }
    KEY_SET_ADVANCES.add(cursor);
    if (DIAG) {
      DENSE_ROWS.add(visited);
    }
    return new Masks(masks, slots);
  }

  static Masks filterMembership(final long[][] sourceMasks, final long[][] keys, final ColumnSlice[] slices,
      final IntToLongFunction physicalSlot, final LongOpenHashSet values, final boolean anti) {
    Objects.requireNonNull(values);
    int keptLeaves = 0;
    for (final long[] mask : sourceMasks) {
      if (mask != null) {
        keptLeaves++;
      }
    }
    final Long2ObjectOpenHashMap<long[]> byFirstKey = new Long2ObjectOpenHashMap<>(keptLeaves);
    final LongOpenHashSet slots = new LongOpenHashSet(keptLeaves);
    // One read scratch for the whole pass, reused by every leaf: nothing is allocated per leaf or
    // per word.
    final MembershipScratch scratch = new MembershipScratch();
    for (int leaf = 0; leaf < keys.length; leaf++) {
      if (keys[leaf].length == 0) {
        continue;
      }
      final long[] selected = sourceMasks[leaf];
      if (selected == null) {
        continue;
      }
      final ColumnSlice slice = slices[leaf];
      if (!slice.hasNumericLane()) {
        throw new IllegalArgumentException("membership requires an integral column, leaf " + leaf + " has none");
      }
      // Hoisted ONCE per leaf: the lane's slab (or, for a producer still on the heap, its array as a
      // segment) and the byte offset of row 0. The row walk below reads linear offsets off these.
      final MemorySegment lane = slice.laneSegment();
      final long laneBase = slice.laneBase();
      final long[] presence = slice.presenceWords();
      final long[] mask = new long[selected.length];
      if (filterMembershipWords(selected, lane, laneBase, presence, values, anti, mask, slice.rowCount(), scratch)) {
        byFirstKey.put(keys[leaf][0], mask);
        slots.add(physicalSlot.applyAsLong(leaf));
      }
    }
    return new Masks(byFirstKey, slots);
  }

  /**
   * The membership verdict of every selected row of one leaf, over the leaf's value lane.
   *
   * <p>
   * The lane is a slab the column store owns (or, for a producer still on the heap, its array seen as
   * a segment), so HOW the rows are read decides the cost: a {@link Long#numberOfTrailingZeros} walk
   * indexes by bit, so the loop is not counted over linear offsets and no compiler lifts the
   * segment's bounds and alignment checks out of it — every candidate row pays its own. One counted
   * copy pays one check per vector instead, so the form is chosen per leaf and per word, never per
   * row:
   * </p>
   *
   * <ol>
   * <li>a leaf that has a candidate in more than one row in eight stages its whole lane with ONE
   * counted copy and the walks index the staged array — the same bytes the per-word form would move,
   * in a sixteenth of the calls (a 1024-row leaf is 16 words);</li>
   * <li>otherwise a word above {@link ProjectionVectorKernels#FOLD_WALK_MAX_BITS} candidates is
   * gathered into a reused 512-byte window, the crossover the fold kernels already dispatch on;</li>
   * <li>otherwise the few candidates of the word are read in place.</li>
   * </ol>
   */
  private static boolean filterMembershipWords(final long[] selected, final MemorySegment lane, final long laneBase,
      final long[] presence, final LongOpenHashSet values, final boolean anti, final long[] mask, final int rowCount,
      final MembershipScratch scratch) {
    final int words = selected.length;
    final long[] candidateWords = scratch.candidates(words);
    // ONE SIZING PASS over the words, before any value is read: it costs one word operation per 64
    // rows, it is the AND the walk would do anyway, and it is what lets the read form be decided per
    // leaf instead of per row.
    int candidateBits = 0;
    for (int word = 0; word < words; word++) {
      final long candidates = selected[word] & presence[word];
      candidateWords[word] = candidates;
      candidateBits += Long.bitCount(candidates);
    }
    final long @Nullable [] staged = candidateBits > 0 && candidateBits << STAGE_MIN_DENSITY_SHIFT >= rowCount
        ? scratch.stage(lane, laneBase, rowCount) // null for a leaf wider than a row group may be
        : null;
    final long @Nullable [] window = staged == null
        ? scratch.window()
        : null;
    int gatheredWords = 0;
    int inPlaceWords = 0;
    long any = 0;
    for (int word = 0; word < words; word++) {
      // A selected row with no cell is a non-member, so only the ANTI form keeps it. Deciding that
      // whole class with one word operation leaves the row walk only the rows that have a value to
      // test, instead of a presence bit test per selected row.
      long kept = anti
          ? selected[word] & ~presence[word]
          : 0L;
      final long candidates = candidateWords[word];
      if (candidates != 0L) {
        final int rowBase = word << 6;
        if (staged != null) {
          kept |= walkStaged(candidates, staged, rowBase, values, anti);
        } else if (Long.bitCount(candidates) > ProjectionVectorKernels.FOLD_WALK_MAX_BITS) {
          final long[] into = Objects.requireNonNull(window, "a leaf that does not stage has a window");
          // Clamped to the leaf's own rows: the lane holds rowCount values and a partial last word
          // must not read past them.
          MemorySegment.copy(lane, ValueLayout.JAVA_LONG_UNALIGNED, laneBase + ((long) rowBase << 3), into, 0,
              Math.min(ProjectionColumnGroupScan.WINDOW_ROWS, rowCount - rowBase));
          kept |= walkStaged(candidates, into, 0, values, anti);
          gatheredWords++;
        } else {
          kept |= walkLane(candidates, lane, laneBase + ((long) rowBase << 3), values, anti);
          inPlaceWords++;
        }
      }
      mask[word] = kept;
      any |= kept;
    }
    // Counted once per leaf, never per word or per row: three adds on a path that moved thousands of
    // values.
    if (staged != null) {
      MEMBERSHIP_STAGED_LEAVES.increment();
    }
    if (gatheredWords != 0) {
      MEMBERSHIP_GATHERED_WORDS.add(gatheredWords);
    }
    if (inPlaceWords != 0) {
      MEMBERSHIP_IN_PLACE_WORDS.add(inPlaceWords);
    }
    return any != 0;
  }

  /**
   * The kept bits of one word, reading the word's values from {@code staged} at {@code offset + bit}.
   * Its own method so that it stays small enough to inline into all three dispatch arms (a kernel
   * grown to carry every walk stops inlining its per-row callee).
   */
  private static long walkStaged(final long candidates, final long[] staged, final int offset,
      final LongOpenHashSet values, final boolean anti) {
    long kept = 0L;
    long bits = candidates;
    while (bits != 0L) {
      final int bit = Long.numberOfTrailingZeros(bits);
      bits &= bits - 1L;
      if (anti != values.contains(staged[offset + bit])) {
        kept |= 1L << bit;
      }
    }
    return kept;
  }

  /** {@link #walkStaged} reading the lane in place — the cheaper side below the gather crossover. */
  private static long walkLane(final long candidates, final MemorySegment lane, final long wordBase,
      final LongOpenHashSet values, final boolean anti) {
    long kept = 0L;
    long bits = candidates;
    while (bits != 0L) {
      final int bit = Long.numberOfTrailingZeros(bits);
      bits &= bits - 1L;
      if (anti != values.contains(SegmentAccess.getLong(lane, wordBase + ((long) bit << 3)))) {
        kept |= 1L << bit;
      }
    }
    return kept;
  }

  /**
   * Above this candidate density a leaf stages its whole lane:
   * {@code candidateBits << 3 >= rowCount}, i.e. one row in eight. At that density the per-word form
   * already copies every word ({@code 64 / 8 == 8} candidates per full word is the gather crossover),
   * so staging moves the same bytes in one call instead of one per word; below it the per-word form
   * copies strictly less.
   */
  private static final int STAGE_MIN_DENSITY_SHIFT = 3;

  /**
   * The read scratch of one {@link #filterMembership} pass: the candidate words of the leaf being
   * walked, the per-word gather window, and the per-leaf staging buffer allocated once the first
   * dense leaf asks for it. A projection leaf holds at most
   * {@link ProjectionIndexRowGroupPage#MAX_ROWS} rows, which bounds both buffers.
   */
  private static final class MembershipScratch {
    private static final int MAX_WORDS = (ProjectionIndexRowGroupPage.MAX_ROWS + 63) >>> 6;

    private long[] candidateWords = new long[MAX_WORDS];

    private long @Nullable [] window;

    private long @Nullable [] leaf;

    /**
     * The candidate-word buffer for a mask of {@code words} words. A projection leaf's mask is at most
     * {@link #MAX_WORDS} words, so the buffer is sized for that up front and only a caller outside that
     * contract ever grows it — which is a resize, not a refusal, because the sizing pass is an
     * optimisation and must not be the thing that fails a scan.
     */
    long[] candidates(final int words) {
      long[] buffer = candidateWords;
      if (buffer.length < words) {
        buffer = new long[words];
        candidateWords = buffer;
      }
      return buffer;
    }

    long[] window() {
      long[] buffer = window;
      if (buffer == null) {
        buffer = new long[ProjectionColumnGroupScan.WINDOW_ROWS];
        window = buffer;
      }
      return buffer;
    }

    /**
     * The whole lane of a leaf of {@code rows} rows, staged with one counted copy, or {@code null} when
     * the leaf is wider than a row group may be — then the caller reads per word, which has no such
     * bound.
     */
    long @Nullable [] stage(final MemorySegment lane, final long laneBase, final int rows) {
      if (rows > ProjectionIndexRowGroupPage.MAX_ROWS) {
        return null;
      }
      long[] buffer = leaf;
      if (buffer == null) {
        buffer = new long[ProjectionIndexRowGroupPage.MAX_ROWS];
        leaf = buffer;
      }
      MemorySegment.copy(lane, ValueLayout.JAVA_LONG_UNALIGNED, laneBase, buffer, 0, rows);
      return buffer;
    }
  }

  /** Include the selected row bits, not only the original source keys, in predicate memo identity. */
  static long contentHash(final Masks masks) {
    long hash = 0;
    for (final var entry : masks.byFirstKey().long2ObjectEntrySet()) {
      long leafHash = entry.getLongKey();
      for (final long word : entry.getValue()) {
        leafHash = (leafHash ^ word) * 0xBF58476D1CE4E5B9L;
        leafHash ^= leafHash >>> 31;
      }
      hash += leafHash;
    }
    return hash;
  }

  /**
   * Validate a sorted key set: strictly ascending, non-negative keys.
   *
   * @throws IllegalArgumentException when the set is unsorted or holds a duplicate or negative key
   */
  static void requireSorted(final long[] sortedKeys) {
    if (sortedKeys == null) {
      throw new IllegalArgumentException("sortedKeys must not be null");
    }
    if (sortedKeys.length > 0 && sortedKeys[0] < 0L) {
      throw new IllegalArgumentException("record keys are non-negative: " + sortedKeys[0]);
    }
    for (int i = 1; i < sortedKeys.length; i++) {
      if (sortedKeys[i] <= sortedKeys[i - 1]) {
        throw new IllegalArgumentException("sortedKeys must be strictly ascending at index " + i + ": "
            + sortedKeys[i - 1] + " then " + sortedKeys[i]);
      }
    }
  }

  /**
   * Content hash of the set — what a plan memo keyed by predicate SHAPE must fold in, so two queries
   * over different key sets never share a shape fingerprint.
   */
  static long contentHash(final long[] sortedKeys) {
    long h = 0x9E3779B97F4A7C15L ^ sortedKeys.length;
    for (final long key : sortedKeys) {
      h = (h ^ key) * 0xBF58476D1CE4E5B9L;
      h ^= h >>> 31;
    }
    return h;
  }

  /** Whether any key of the set lies in {@code [min, max]}. */
  public static boolean anyIn(final long[] sortedKeys, final long min, final long max) {
    if (min > max || sortedKeys.length == 0) {
      return false;
    }
    final int at = lowerBound(sortedKeys, min);
    return at < sortedKeys.length && sortedKeys[at] <= max;
  }

  /**
   * AND the membership of each row's record key into {@code mask}: bit {@code row} stays set iff it
   * was set and the key of {@code row} is in the set. Only the first {@code ceil(rowCount/64)} words
   * are touched; tail bits beyond {@code rowCount} are left as the caller had them (every caller
   * fills the tail clear).
   *
   * <p>
   * The keys are read IN PLACE off {@code keyLane}: the slab lane of a key column, or a heap key
   * array viewed as a segment while the KEYS chain is still on the heap. The caller hoists the
   * segment and the base once per leaf; this loop only adds row offsets to them.
   *
   * @param keyLane segment holding the leaf's record keys in physical row order
   * @param keyBase byte offset of row 0 within {@code keyLane} ({@code rowCount} keys readable)
   * @param rowCount rows of the leaf
   * @param sortedKeys the set, strictly ascending
   * @param mask the conjunctive mask, updated in place
   * @return how many rows of the leaf are members (independent of the incoming mask)
   */
  public static int andMembership(final MemorySegment keyLane, final long keyBase, final int rowCount,
      final long[] sortedKeys, final long[] mask) {
    final int stride = (rowCount + 63) >>> 6;
    final int setSize = sortedKeys.length;
    if (setSize == 0) {
      Arrays.fill(mask, 0, stride, 0L);
      return 0;
    }
    int cursor = rowCount == 0
        ? 0
        : lowerBound(sortedKeys, SegmentAccess.getLong(keyLane, keyBase));
    long previous = Long.MIN_VALUE;
    int members = 0;
    for (int w = 0; w < stride; w++) {
      long out = 0L;
      final int rowBase = w << 6;
      final int rowEnd = Math.min(rowBase + 64, rowCount);
      for (int row = rowBase; row < rowEnd; row++) {
        final long key = SegmentAccess.getLong(keyLane, keyBase + ((long) row << 3));
        final boolean member;
        if (key >= previous) {
          // The ascending run: advance the set cursor monotonically.
          while (cursor < setSize && sortedKeys[cursor] < key) {
            cursor++;
          }
          member = cursor < setSize && sortedKeys[cursor] == key;
          previous = key;
        } else {
          // An order exception: the key fell behind the run, so the cursor says nothing about it.
          // The cursor itself is NOT moved — the run resumes from where it was.
          member = Arrays.binarySearch(sortedKeys, 0, cursor, key) >= 0;
        }
        if (member) {
          out |= 1L << (row & 63);
          members++;
        }
      }
      mask[w] &= out;
    }
    return members;
  }

  /** First index whose key is {@code >= key}, or {@code length}. */
  static int lowerBound(final long[] sorted, final long key) {
    int lo = 0;
    int hi = sorted.length;
    while (lo < hi) {
      final int mid = (lo + hi) >>> 1;
      if (sorted[mid] < key) {
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }
    return lo;
  }
}
