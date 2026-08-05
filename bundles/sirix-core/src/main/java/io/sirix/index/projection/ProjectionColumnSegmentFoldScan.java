/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.index.projection.ProjectionColumnStore.ColumnSegmentFetcher;
import io.sirix.index.projection.ProjectionIndexScan.ColumnPredicate;
import io.sirix.index.projection.ProjectionIndexScan.PredicateTree;
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

import java.util.Arrays;

/**
 * Fold-during-decode scan kernels (P5b stage 4): conjunctive counts and numeric-long
 * aggregates evaluated STRAIGHT from the verified BODY segment bytes cached by
 * {@link ProjectionColumnStore#columnBytes(int)} — no {@code long[rowCount]} slice arrays
 * are ever materialized. Values stream through an L1-resident 1024-value scratch block
 * (8&nbsp;KiB), the vector-at-a-time model of the analytical engines this path is measured
 * against: unpack a block, mask it, fold it, move on.
 *
 * <p><b>Why 1024-value blocks are safe for ANY width.</b> Packed runs are little-endian
 * LSB-first bit streams; a block boundary at value {@code 1024·n} sits at bit offset
 * {@code 1024·n·width}, which is a whole number of bytes for every width — so each block
 * decodes independently with the same positional bulk unpacker the slice path uses
 * ({@link ProjectionIndexRowGroupCodec#unpackInto(byte[], int, int, int, long, long[], int)}),
 * and block-local masks align with 64-bit presence words ({@code 1024 = 16 × 64}).
 *
 * <p><b>Parity contract.</b> Semantics mirror {@link ProjectionColumnScan} (and therefore
 * {@code ProjectionIndexByteScan.evaluateRowGroupMask}) bit for bit: numeric zone-skip on
 * segment-truth min/max with the {@code min > max} all-missing prune, missing ⇒ false via
 * the presence AND, boolean bitmap equality, and the aggregate column's own presence AND
 * before folding. {@code ProjectionColumnScanParityTest} pins the equivalence against both
 * the byte and the slice kernels over randomized stores.
 *
 * <p><b>Eligibility.</b> Only plain-FOR numeric streams (width 0–56 or 64) and boolean
 * streams are foldable; an ALP-escaped double stream ({@code width == 65}) or any reserved
 * escape routes the query to the slice kernels via {@link #eligible} — never an error.
 *
 * <p>Scratch is thread-local and fixed-size; per-leaf evaluation allocates nothing beyond
 * the per-call stream holders.
 *
 * <p><b>Compare/fold arms are Vector API, walks stay scalar — a measured verdict.</b>
 * {@code ProjectionFoldKernelMicrobench} (512-bit species) put the scalar compare-to-bitmask
 * loop at ~4.1&nbsp;ns/row on dense words against ~0.21 for the lane-compare kernel, and the
 * masked vector fold ahead of the ntz walk above ~8 surviving bits per word; the walk keeps
 * winning on nearly-empty words. Dispatch encodes exactly those crossovers
 * ({@link ProjectionVectorKernels#COMPARE_WALK_MAX_BITS},
 * {@link ProjectionVectorKernels#FOLD_WALK_MAX_BITS}); the presence/mask word combining
 * stays bit-parallel on plain longs, where the same profile showed nothing to reclaim.
 */
public final class ProjectionColumnSegmentFoldScan {

  /** Values per fold block; {@code 1024 · width} bits is byte-aligned for every width. */
  private static final int BLOCK_VALUES = 1024;
  private static final int BLOCK_WORDS = BLOCK_VALUES >>> 6;

  private static final class Scratch {
    final long[] mask = new long[BLOCK_WORDS];
    final long[] vals = new long[BLOCK_VALUES];
    final long[] aggVals = new long[BLOCK_VALUES];
    /** Mask stack for {@link PredicateTree} programs — depth bounded by the tree contract. */
    final long[][] stack = new long[PredicateTree.MAX_LEAVES][BLOCK_WORDS];
  }

  private static final ThreadLocal<Scratch> SCRATCH = ThreadLocal.withInitial(Scratch::new);

  /**
   * Per-leaf parsed view over one column's BODY segment bytes — a mutable flyweight reused
   * across leaves (thread-confined to one kernel invocation). Wire form after the 6-byte
   * PIXS header: {@code flags; [rowCount > 0] min, max; presence marker [+ words];} then
   * {@code NUMERIC: base, width, packed values | BOOLEAN: words verbatim}.
   */
  private static final class Stream {
    byte[] seg;
    long min;
    long max;
    int presenceMode;
    int presenceBase;
    long base;
    int width;
    int valuesBase;
    int boolBase;
    boolean numeric;
    boolean plainWidth;

    void open(final byte[] segment, final int rowCount, final boolean numericKind) {
      this.seg = segment;
      this.numeric = numericKind;
      this.plainWidth = true;
      if (rowCount <= 0) {
        return;
      }
      this.min = ProjectionIndexRowGroupCodec.getLongLE(segment, 7);
      this.max = ProjectionIndexRowGroupCodec.getLongLE(segment, 15);
      int pos = 23;
      this.presenceMode = segment[pos] & 0xFF;
      pos++;
      if (presenceMode == 2) {
        this.presenceBase = pos;
        pos += ((rowCount + 63) >>> 6) << 3;
      }
      if (numericKind) {
        this.base = ProjectionIndexRowGroupCodec.getLongLE(segment, pos);
        this.width = segment[pos + 8] & 0xFF;
        this.valuesBase = pos + 9;
        this.plainWidth = width <= 56 || width == 64;
      } else {
        this.boolBase = pos;
      }
    }

    /** Presence word {@code w} (leaf-global index) with tail semantics identical to decode. */
    long presenceWord(final int w, final int presWords, final int rowCount) {
      return switch (presenceMode) {
        case 0 -> ProjectionIndexRowGroupCodec.expectedFullWord(w, presWords, rowCount);
        case 1 -> 0L;
        case 2 -> ProjectionIndexRowGroupCodec.getLongLE(seg, presenceBase + (w << 3));
        default -> throw new IllegalStateException("Bad presence marker " + presenceMode);
      };
    }

    /** Boolean word {@code w} (leaf-global index), verbatim from the segment. */
    long boolWord(final int w) {
      return ProjectionIndexRowGroupCodec.getLongLE(seg, boolBase + (w << 3));
    }

    /** Unpack {@code count} values of the block starting at value {@code valueStart}. */
    void unpackBlock(final int valueStart, final int count, final long[] out) {
      final int byteOff = valuesBase
          + (width == 64 ? valueStart << 3 : (valueStart >>> 3) * width);
      ProjectionIndexRowGroupCodec.unpackInto(seg, byteOff, count, width, base, out, 0);
    }
  }

  private ProjectionColumnSegmentFoldScan() {
  }

  /**
   * Whether the fused kernels can serve this query shape: every predicate column sliceable
   * and non-string, and every involved NUMERIC stream plain-FOR in every leaf (no ALP or
   * reserved width escapes). Fetches (and caches) the involved columns' bytes — so a
   * {@code true} answer means the kernels' substrate is already resident.
   *
   * @throws IllegalStateException on corrupt/missing segments (same contract as
   *         {@link ProjectionColumnStore#columnBytes(int)}) — callers decline through the
   *         established fail-soft flow
   */
  public static boolean eligible(final ProjectionColumnStore store,
      final ColumnPredicate[] predicates, final int aggColOrNegative, final ColumnSegmentFetcher fetcher) {
    for (final ColumnPredicate p : predicates) {
      if (p.stringLitBytes != null || !store.columnSliceable(p.column)) {
        return false;
      }
    }
    // The aggregate must be a NUMERIC column, checked by KIND: columnSliceable now also admits
    // string and boolean columns, and a fold over their bytes would misparse them as numbers.
    if (aggColOrNegative >= 0
        && !ProjectionIndexRowGroupPage.isNumericKind(store.columnKind(aggColOrNegative))) {
      return false;
    }
    final Stream probe = new Stream();
    for (int i = 0; i <= predicates.length; i++) {
      final int col = i < predicates.length ? predicates[i].column : aggColOrNegative;
      if (col < 0) {
        continue;
      }
      final boolean numericKind =
          ProjectionIndexRowGroupPage.isNumericKind(store.columnKind(col));
      if (!numericKind) {
        continue;
      }
      final byte[][] segments = store.columnBytes(col, fetcher);
      for (int leaf = 0; leaf < segments.length; leaf++) {
        final int rowCount = store.rowCount(leaf);
        if (rowCount <= 0) {
          continue;
        }
        probe.open(segments[leaf], rowCount, true);
        if (!probe.plainWidth) {
          return false;
        }
      }
    }
    return true;
  }

  /** Conjunctive count folded straight from segment bytes. */
  public static long conjunctiveCount(final ProjectionColumnStore store,
      final ColumnPredicate[] predicates, final ColumnSegmentFetcher fetcher) {
    return conjunctiveCount(store, predicates, 0, store.rowGroupCount(), fetcher);
  }

  /** Ranged variant for the executor's chunked parallel dispatch — scratch is thread-local. */
  public static long conjunctiveCount(final ProjectionColumnStore store,
      final ColumnPredicate[] predicates, final int fromRowGroup, final int toRowGroup,
      final ColumnSegmentFetcher fetcher) {
    final byte[][][] predBytes = resolvePredicateBytes(store, predicates, fetcher);
    final boolean[] predNumeric = predicateNumeric(store, predicates);
    final Stream[] streams = newStreams(predicates.length);
    final Scratch s = SCRATCH.get();
    long total = 0;
    for (int leaf = fromRowGroup; leaf < toRowGroup; leaf++) {
      final int rowCount = store.rowCount(leaf);
      if (rowCount <= 0 || !openRowGroup(streams, predBytes, predNumeric, predicates, leaf, rowCount)) {
        continue;
      }
      final int presWords = (rowCount + 63) >>> 6;
      for (int blockStart = 0; blockStart < rowCount; blockStart += BLOCK_VALUES) {
        final int rows = Math.min(BLOCK_VALUES, rowCount - blockStart);
        final int words = (rows + 63) >>> 6;
        final int wordBase = blockStart >>> 6;
        fillAllTrue(s.mask, rows, words);
        if (evaluateBlock(streams, predicates, predNumeric, s, blockStart, rows, words, wordBase,
            presWords, rowCount)) {
          for (int w = 0; w < words; w++) {
            total += Long.bitCount(s.mask[w]);
          }
        }
      }
    }
    return total;
  }

  /**
   * Conjunctive numeric-long aggregate folded straight from segment bytes —
   * {@code acc = [count, sum, min, max]}, initialised by the caller to
   * {@code {0, 0, Long.MAX_VALUE, Long.MIN_VALUE}}.
   */
  public static void conjunctiveAggregateNumeric(final ProjectionColumnStore store,
      final ColumnPredicate[] predicates, final int numericColumn, final long[] acc,
      final ColumnSegmentFetcher fetcher) {
    conjunctiveAggregateNumeric(store, predicates, numericColumn, acc, 0, store.rowGroupCount(), fetcher);
  }

  /** Ranged variant for chunked parallel dispatch. */
  public static void conjunctiveAggregateNumeric(final ProjectionColumnStore store,
      final ColumnPredicate[] predicates, final int numericColumn, final long[] acc,
      final int fromRowGroup, final int toRowGroup, final ColumnSegmentFetcher fetcher) {
    if (store.columnKind(numericColumn) != ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG) {
      throw new IllegalStateException("aggregate column " + numericColumn + " is not NUMERIC_LONG");
    }
    final byte[][][] predBytes = resolvePredicateBytes(store, predicates, fetcher);
    final boolean[] predNumeric = predicateNumeric(store, predicates);
    final byte[][] aggBytes = store.columnBytes(numericColumn, fetcher);
    final Stream[] streams = newStreams(predicates.length);
    final Stream aggStream = new Stream();
    final Scratch s = SCRATCH.get();
    for (int leaf = fromRowGroup; leaf < toRowGroup; leaf++) {
      final int rowCount = store.rowCount(leaf);
      if (rowCount <= 0 || !openRowGroup(streams, predBytes, predNumeric, predicates, leaf, rowCount)) {
        continue;
      }
      openAggregateColumn(aggStream, aggBytes[leaf], rowCount, numericColumn);
      final int presWords = (rowCount + 63) >>> 6;
      for (int blockStart = 0; blockStart < rowCount; blockStart += BLOCK_VALUES) {
        final int rows = Math.min(BLOCK_VALUES, rowCount - blockStart);
        final int words = (rows + 63) >>> 6;
        final int wordBase = blockStart >>> 6;
        fillAllTrue(s.mask, rows, words);
        if (!evaluateBlock(streams, predicates, predNumeric, s, blockStart, rows, words, wordBase,
            presWords, rowCount)) {
          continue;
        }
        foldMaskedBlock(s.mask, aggStream, s, blockStart, rows, words, wordBase, presWords, rowCount,
            acc);
      }
    }
  }

  /** Open the aggregate column's stream for one row group, refusing a width escape the eligibility
   *  gate should already have declined. */
  private static void openAggregateColumn(final Stream aggStream, final byte[] segment,
      final int rowCount, final int numericColumn) {
    aggStream.open(segment, rowCount, true);
    if (!aggStream.plainWidth) {
      throw new IllegalStateException("Aggregate column " + numericColumn
          + " has a non-plain width escape — kernel dispatched without eligibility check");
    }
  }

  /**
   * Fold one block's surviving rows into {@code acc} — {@code {count, sum, min, max}}. This is the
   * shared tail of both numeric aggregate kernels, which differ only in how they PRODUCE
   * {@code maskWords}: a flat conjunction fills {@code Scratch.mask} in place, a predicate tree
   * evaluates to its root mask. Keeping one fold means the two cannot drift into disagreeing on a
   * count.
   *
   * <p>The scalar accumulators live in locals and the vector accumulators in registers across
   * the whole block, reducing to {@code acc} exactly once at the end; long addition wraps
   * associatively and min/max are order-insensitive, so lane order cannot change the result —
   * every arm is bit-exact with every other.
   */
  private static void foldMaskedBlock(final long[] maskWords, final Stream aggStream, final Scratch s,
      final int blockStart, final int rows, final int words, final int wordBase, final int presWords,
      final int rowCount, final long[] acc) {
    final VectorSpecies<Long> species = ProjectionVectorKernels.SPECIES;
    final int lanes = ProjectionVectorKernels.LANES;
    long count = acc[0];
    long sum = acc[1];
    long min = acc[2];
    long max = acc[3];
    LongVector vsum = LongVector.zero(species);
    LongVector vmin = LongVector.broadcast(species, Long.MAX_VALUE);
    LongVector vmax = LongVector.broadcast(species, Long.MIN_VALUE);
    boolean unpacked = false;
    for (int w = 0; w < words; w++) {
      long word = maskWords[w] & aggStream.presenceWord(wordBase + w, presWords, rowCount);
      if (word == 0L) {
        continue;
      }
      if (!unpacked) {
        // Unpack the aggregate block only once a bit actually survives the mask AND —
        // fully filtered/absent blocks never touch the packed values at all.
        aggStream.unpackBlock(blockStart, rows, s.aggVals);
        unpacked = true;
      }
      final int rowBase = w << 6;
      // The scratch is a fixed {@value #BLOCK_VALUES}-slot array, so full-width lane loads
      // stay in bounds on tail words; stale lanes beyond the unpacked rows are masked off
      // (a tail word can never be dense — fillAllTrue and the presence tail zero its top bits).
      if (word == -1L) {
        for (int k = 0; k < 64; k += lanes) {
          final LongVector v = LongVector.fromArray(species, s.aggVals, rowBase + k);
          vsum = vsum.add(v);
          vmin = vmin.min(v);
          vmax = vmax.max(v);
        }
        count += 64;
        continue;
      }
      if (Long.bitCount(word) > ProjectionVectorKernels.FOLD_WALK_MAX_BITS) {
        for (int k = 0; k < 64; k += lanes) {
          final VectorMask<Long> m = VectorMask.fromLong(species, word >>> k);
          final LongVector v = LongVector.fromArray(species, s.aggVals, rowBase + k);
          vsum = vsum.add(v, m);
          vmin = vmin.lanewise(VectorOperators.MIN, v, m);
          vmax = vmax.lanewise(VectorOperators.MAX, v, m);
        }
        count += Long.bitCount(word);
        continue;
      }
      while (word != 0L) {
        final int bit = Long.numberOfTrailingZeros(word);
        word &= word - 1L;
        final long v = s.aggVals[rowBase + bit];
        count++;
        sum += v;
        if (v < min) min = v;
        if (v > max) max = v;
      }
    }
    // Untouched vector accumulators reduce to the fold identities (0, MAX_VALUE, MIN_VALUE),
    // so the merge below is unconditional.
    sum += vsum.reduceLanes(VectorOperators.ADD);
    final long laneMin = vmin.reduceLanes(VectorOperators.MIN);
    if (laneMin < min) min = laneMin;
    final long laneMax = vmax.reduceLanes(VectorOperators.MAX);
    if (laneMax > max) max = laneMax;
    acc[0] = count;
    acc[1] = sum;
    acc[2] = min;
    acc[3] = max;
  }

  // ==================== predicate-tree kernels (P5b stage 6) ====================

  /** {@link #eligible} for a predicate tree — same gates, over the tree's leaves. */
  public static boolean eligibleTree(final ProjectionColumnStore store, final PredicateTree tree,
      final int aggColOrNegative, final ColumnSegmentFetcher fetcher) {
    return eligible(store, tree.leaves, aggColOrNegative, fetcher);
  }

  /**
   * Count of rows matching an arbitrary AND/OR {@link PredicateTree}, folded straight from
   * segment bytes. Leaf masks encode missing ⇒ {@code false}; combinators are word-wise
   * intersection/union — see the tree type's semantics contract.
   */
  public static long treeCount(final ProjectionColumnStore store, final PredicateTree tree,
      final ColumnSegmentFetcher fetcher) {
    return treeCount(store, tree, 0, store.rowGroupCount(), fetcher);
  }

  /** Ranged variant for chunked parallel dispatch. */
  public static long treeCount(final ProjectionColumnStore store, final PredicateTree tree,
      final int fromRowGroup, final int toRowGroup, final ColumnSegmentFetcher fetcher) {
    final ColumnPredicate[] leaves = tree.leaves;
    final byte[][][] leafBytes = resolvePredicateBytes(store, leaves, fetcher);
    final boolean[] leafNumeric = predicateNumeric(store, leaves);
    final Stream[] streams = newStreams(leaves.length);
    final boolean[] leafLive = new boolean[leaves.length];
    final Scratch s = SCRATCH.get();
    long total = 0;
    for (int leaf = fromRowGroup; leaf < toRowGroup; leaf++) {
      final int rowCount = store.rowCount(leaf);
      if (rowCount <= 0
          || !openTreeRowGroup(streams, leafLive, leafBytes, leafNumeric, leaves, tree, leaf, rowCount)) {
        continue;
      }
      final int presWords = (rowCount + 63) >>> 6;
      for (int blockStart = 0; blockStart < rowCount; blockStart += BLOCK_VALUES) {
        final int rows = Math.min(BLOCK_VALUES, rowCount - blockStart);
        final int words = (rows + 63) >>> 6;
        final int wordBase = blockStart >>> 6;
        final long[] root = evaluateTreeBlock(tree, streams, leafLive, leafNumeric, leaves, s,
            blockStart, rows, words, wordBase, presWords, rowCount);
        for (int w = 0; w < words; w++) {
          total += Long.bitCount(root[w]);
        }
      }
    }
    return total;
  }

  /**
   * Numeric-long aggregate over an arbitrary AND/OR {@link PredicateTree} —
   * {@code acc = [count, sum, min, max]}, aggregate-column presence ANDed before folding.
   */
  public static void treeAggregateNumeric(final ProjectionColumnStore store,
      final PredicateTree tree, final int numericColumn, final long[] acc,
      final ColumnSegmentFetcher fetcher) {
    treeAggregateNumeric(store, tree, numericColumn, acc, 0, store.rowGroupCount(), fetcher);
  }

  /** Ranged variant for chunked parallel dispatch. */
  public static void treeAggregateNumeric(final ProjectionColumnStore store,
      final PredicateTree tree, final int numericColumn, final long[] acc,
      final int fromRowGroup, final int toRowGroup, final ColumnSegmentFetcher fetcher) {
    if (store.columnKind(numericColumn) != ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG) {
      throw new IllegalStateException("aggregate column " + numericColumn + " is not NUMERIC_LONG");
    }
    final ColumnPredicate[] leaves = tree.leaves;
    final byte[][][] leafBytes = resolvePredicateBytes(store, leaves, fetcher);
    final boolean[] leafNumeric = predicateNumeric(store, leaves);
    final byte[][] aggBytes = store.columnBytes(numericColumn, fetcher);
    final Stream[] streams = newStreams(leaves.length);
    final boolean[] leafLive = new boolean[leaves.length];
    final Stream aggStream = new Stream();
    final Scratch s = SCRATCH.get();
    for (int leaf = fromRowGroup; leaf < toRowGroup; leaf++) {
      final int rowCount = store.rowCount(leaf);
      if (rowCount <= 0
          || !openTreeRowGroup(streams, leafLive, leafBytes, leafNumeric, leaves, tree, leaf, rowCount)) {
        continue;
      }
      openAggregateColumn(aggStream, aggBytes[leaf], rowCount, numericColumn);
      final int presWords = (rowCount + 63) >>> 6;
      for (int blockStart = 0; blockStart < rowCount; blockStart += BLOCK_VALUES) {
        final int rows = Math.min(BLOCK_VALUES, rowCount - blockStart);
        final int words = (rows + 63) >>> 6;
        final int wordBase = blockStart >>> 6;
        final long[] root = evaluateTreeBlock(tree, streams, leafLive, leafNumeric, leaves, s,
            blockStart, rows, words, wordBase, presWords, rowCount);
        foldMaskedBlock(root, aggStream, s, blockStart, rows, words, wordBase, presWords, rowCount,
            acc);
      }
    }
  }

  /**
   * Open every tree-leaf stream for {@code leaf} and run the TREE-aware zone phase:
   * per-leaf EMPTY states (zone skip, {@code min > max}, all-missing presence) propagate
   * through the program — {@code AND(EMPTY, x) = EMPTY}, {@code OR(EMPTY, x) = x} — and
   * only a provably-EMPTY root prunes the whole leaf. Non-pruned EMPTY leaves contribute
   * all-zero masks in the block phase without touching their packed values.
   */
  private static boolean openTreeRowGroup(final Stream[] streams, final boolean[] leafLive,
      final byte[][][] leafBytes, final boolean[] leafNumeric, final ColumnPredicate[] leaves,
      final PredicateTree tree, final int leaf, final int rowCount) {
    for (int i = 0; i < streams.length; i++) {
      final Stream st = streams[i];
      st.open(leafBytes[i][leaf], rowCount, leafNumeric[i]);
      boolean live = st.presenceMode != 1;
      if (leafNumeric[i]) {
        if (!st.plainWidth) {
          throw new IllegalStateException("Predicate column " + leaves[i].column
              + " has a non-plain width escape — kernel dispatched without eligibility check");
        }
        if (st.min > st.max || ProjectionIndexByteScan.zoneSkip(leaves[i], st.min, st.max)) {
          live = false;
        }
      }
      leafLive[i] = live;
    }
    // Program over liveness: can the root match at all?
    final boolean[] canMatch = new boolean[PredicateTree.MAX_LEAVES];
    int depth = 0;
    for (final byte insn : tree.program) {
      if (insn >= 0) {
        canMatch[depth++] = leafLive[insn];
      } else if (insn == PredicateTree.OP_AND) {
        depth--;
        canMatch[depth - 1] = canMatch[depth - 1] && canMatch[depth];
      } else {
        depth--;
        canMatch[depth - 1] = canMatch[depth - 1] || canMatch[depth];
      }
    }
    return canMatch[0];
  }

  /**
   * Interpret the tree program for one block: leaf pushes evaluate the leaf's mask over
   * the FULL (tail-masked) row domain; AND/OR combine word-wise in place on the stack.
   * Returns the root mask (stack slot 0 of the scratch).
   */
  private static long[] evaluateTreeBlock(final PredicateTree tree, final Stream[] streams,
      final boolean[] leafLive, final boolean[] leafNumeric, final ColumnPredicate[] leaves,
      final Scratch s, final int blockStart, final int rows, final int words, final int wordBase,
      final int presWords, final int rowCount) {
    int depth = 0;
    for (final byte insn : tree.program) {
      if (insn >= 0) {
        final long[] slot = s.stack[depth++];
        final Stream st = streams[insn];
        if (!leafLive[insn]) {
          Arrays.fill(slot, 0, words, 0L);
          continue;
        }
        fillAllTrue(slot, rows, words);
        if (leafNumeric[insn]) {
          st.unpackBlock(blockStart, rows, s.vals);
          evalNumericBlock(s.vals, leaves[insn], st, words, wordBase, presWords, rowCount, slot);
        } else {
          for (int w = 0; w < words; w++) {
            final long bw = st.boolWord(wordBase + w);
            final long match = leaves[insn].boolLit ? bw : ~bw;
            slot[w] &= match & st.presenceWord(wordBase + w, presWords, rowCount);
          }
        }
      } else if (insn == PredicateTree.OP_AND) {
        depth--;
        final long[] a = s.stack[depth - 1];
        final long[] b = s.stack[depth];
        for (int w = 0; w < words; w++) {
          a[w] &= b[w];
        }
      } else {
        depth--;
        final long[] a = s.stack[depth - 1];
        final long[] b = s.stack[depth];
        for (int w = 0; w < words; w++) {
          a[w] |= b[w];
        }
      }
    }
    return s.stack[0];
  }

  // ==================== shared evaluation ====================

  private static Stream[] newStreams(final int n) {
    final Stream[] streams = new Stream[n];
    for (int i = 0; i < n; i++) {
      streams[i] = new Stream();
    }
    return streams;
  }

  private static byte[][][] resolvePredicateBytes(final ProjectionColumnStore store,
      final ColumnPredicate[] predicates, final ColumnSegmentFetcher fetcher) {
    if (predicates == null) {
      throw new IllegalArgumentException("predicates must not be null");
    }
    final byte[][][] bytes = new byte[predicates.length][][];
    for (int i = 0; i < predicates.length; i++) {
      final ColumnPredicate p = predicates[i];
      if (p.stringLitBytes != null) {
        throw new IllegalStateException("String predicates are not foldable");
      }
      bytes[i] = store.columnBytes(p.column, fetcher);
    }
    return bytes;
  }

  private static boolean[] predicateNumeric(final ProjectionColumnStore store,
      final ColumnPredicate[] predicates) {
    final boolean[] numeric = new boolean[predicates.length];
    for (int i = 0; i < predicates.length; i++) {
      numeric[i] = ProjectionIndexRowGroupPage.isNumericKind(store.columnKind(predicates[i].column));
    }
    return numeric;
  }

  /**
   * Open every predicate stream for {@code leaf} and run the zone phase. Returns
   * {@code false} when the leaf is pruned outright (zone skip, {@code min > max}, or an
   * all-missing predicate column — parity: an all-missing presence ANDs every mask word to
   * zero, so skipping the leaf is exact).
   */
  private static boolean openRowGroup(final Stream[] streams, final byte[][][] predBytes,
      final boolean[] predNumeric, final ColumnPredicate[] predicates, final int leaf,
      final int rowCount) {
    for (int i = 0; i < streams.length; i++) {
      final Stream st = streams[i];
      st.open(predBytes[i][leaf], rowCount, predNumeric[i]);
      if (predNumeric[i]) {
        if (!st.plainWidth) {
          throw new IllegalStateException("Predicate column " + predicates[i].column
              + " has a non-plain width escape — kernel dispatched without eligibility check");
        }
        // Zone-map prune — numeric predicate columns only (byte-kernel policy).
        if (st.min > st.max || ProjectionIndexByteScan.zoneSkip(predicates[i], st.min, st.max)) {
          return false;
        }
      }
      if (st.presenceMode == 1) {
        return false;
      }
    }
    return true;
  }

  /**
   * Apply every predicate to the block's mask. Returns {@code false} when the mask emptied
   * (callers skip the fold/count for this block).
   */
  private static boolean evaluateBlock(final Stream[] streams, final ColumnPredicate[] predicates,
      final boolean[] predNumeric, final Scratch s, final int blockStart, final int rows,
      final int words, final int wordBase, final int presWords, final int rowCount) {
    for (int i = 0; i < streams.length; i++) {
      final Stream st = streams[i];
      if (predNumeric[i]) {
        st.unpackBlock(blockStart, rows, s.vals);
        evalNumericBlock(s.vals, predicates[i], st, words, wordBase, presWords, rowCount, s.mask);
      } else {
        for (int w = 0; w < words; w++) {
          final long bw = st.boolWord(wordBase + w);
          final long match = predicates[i].boolLit ? bw : ~bw;
          s.mask[w] &= match & st.presenceWord(wordBase + w, presWords, rowCount);
        }
      }
      boolean any = false;
      for (int w = 0; w < words; w++) {
        if (s.mask[w] != 0L) {
          any = true;
          break;
        }
      }
      if (!any) {
        return false;
      }
    }
    return true;
  }

  private static void evalNumericBlock(final long[] vals, final ColumnPredicate p,
      final Stream st, final int words, final int wordBase, final int presWords,
      final int rowCount, final long[] mask) {
    final long lit = p.longLit;
    final long high = p.highLit;
    for (int w = 0; w < words; w++) {
      final long m = mask[w] & st.presenceWord(wordBase + w, presWords, rowCount);
      if (m == 0L) {
        mask[w] = 0L;
        continue;
      }
      final int rowBase = w << 6;
      // The scratch is a fixed block-size array, so the vector kernel's 64-value window is
      // always readable; stale lanes beyond a tail word's rows are ANDed away by m.
      if (m == -1L) {
        mask[w] = ProjectionVectorKernels.compareWord(vals, rowBase, p.op, lit, high);
        continue;
      }
      if (Long.bitCount(m) > ProjectionVectorKernels.COMPARE_WALK_MAX_BITS) {
        mask[w] = m & ProjectionVectorKernels.compareWord(vals, rowBase, p.op, lit, high);
        continue;
      }
      long out = 0L;
      long candidates = m;
      while (candidates != 0L) {
        final int bit = Long.numberOfTrailingZeros(candidates);
        candidates &= candidates - 1L;
        final long v = vals[rowBase + bit];
        final boolean match = switch (p.op) {
          case GT -> v > lit;
          case LT -> v < lit;
          case GE -> v >= lit;
          case LE -> v <= lit;
          case EQ -> v == lit;
          case BETWEEN_GT_LT -> v > lit && v < high;
          case BETWEEN_GT_LE -> v > lit && v <= high;
          case BETWEEN_GE_LT -> v >= lit && v < high;
          case BETWEEN_GE_LE -> v >= lit && v <= high;
        };
        if (match) {
          out |= 1L << bit;
        }
      }
      mask[w] = out;
    }
  }

  /** Block-local all-true mask with the final word tail-masked to {@code rowCount} bits. */
  private static void fillAllTrue(final long[] mask, final int rows, final int words) {
    Arrays.fill(mask, 0, words, -1L);
    final int tailBits = rows & 63;
    if (tailBits != 0) {
      mask[words - 1] = (1L << tailBits) - 1L;
    }
  }
}
