/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.index.projection.ProjectionColumnStore.SegmentFetcher;
import io.sirix.index.projection.ProjectionIndexScan.ColumnPredicate;
import io.sirix.index.projection.ProjectionIndexScan.PredicateTree;

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
 * ({@link ProjectionIndexLeafCodec#unpackInto(byte[], int, int, int, long, long[], int)}),
 * and block-local masks align with 64-bit presence words ({@code 1024 = 16 × 64}).
 *
 * <p><b>Parity contract.</b> Semantics mirror {@link ProjectionColumnScan} (and therefore
 * {@code ProjectionIndexByteScan.evaluateLeafMask}) bit for bit: numeric zone-skip on
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
 */
public final class ProjectionSegmentFoldScan {

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
      this.min = ProjectionIndexLeafCodec.getLongLE(segment, 7);
      this.max = ProjectionIndexLeafCodec.getLongLE(segment, 15);
      int pos = 23;
      this.presenceMode = segment[pos] & 0xFF;
      pos++;
      if (presenceMode == 2) {
        this.presenceBase = pos;
        pos += ((rowCount + 63) >>> 6) << 3;
      }
      if (numericKind) {
        this.base = ProjectionIndexLeafCodec.getLongLE(segment, pos);
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
        case 0 -> ProjectionIndexLeafCodec.expectedFullWord(w, presWords, rowCount);
        case 1 -> 0L;
        case 2 -> ProjectionIndexLeafCodec.getLongLE(seg, presenceBase + (w << 3));
        default -> throw new IllegalStateException("Bad presence marker " + presenceMode);
      };
    }

    /** Boolean word {@code w} (leaf-global index), verbatim from the segment. */
    long boolWord(final int w) {
      return ProjectionIndexLeafCodec.getLongLE(seg, boolBase + (w << 3));
    }

    /** Unpack {@code count} values of the block starting at value {@code valueStart}. */
    void unpackBlock(final int valueStart, final int count, final long[] out) {
      final int byteOff = valuesBase
          + (width == 64 ? valueStart << 3 : (valueStart >>> 3) * width);
      ProjectionIndexLeafCodec.unpackInto(seg, byteOff, count, width, base, out, 0);
    }
  }

  private ProjectionSegmentFoldScan() {
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
      final ColumnPredicate[] predicates, final int aggColOrNegative, final SegmentFetcher fetcher) {
    for (final ColumnPredicate p : predicates) {
      if (p.stringLitBytes != null || !store.columnSliceable(p.column)) {
        return false;
      }
    }
    if (aggColOrNegative >= 0 && !store.columnSliceable(aggColOrNegative)) {
      return false;
    }
    final Stream probe = new Stream();
    for (int i = 0; i <= predicates.length; i++) {
      final int col = i < predicates.length ? predicates[i].column : aggColOrNegative;
      if (col < 0) {
        continue;
      }
      final boolean numericKind =
          ProjectionIndexLeafPage.isNumericKind(store.columnKind(col));
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
      final ColumnPredicate[] predicates, final SegmentFetcher fetcher) {
    return conjunctiveCount(store, predicates, 0, store.leafCount(), fetcher);
  }

  /** Ranged variant for the executor's chunked parallel dispatch — scratch is thread-local. */
  public static long conjunctiveCount(final ProjectionColumnStore store,
      final ColumnPredicate[] predicates, final int fromLeaf, final int toLeaf,
      final SegmentFetcher fetcher) {
    final byte[][][] predBytes = resolvePredicateBytes(store, predicates, fetcher);
    final boolean[] predNumeric = predicateNumeric(store, predicates);
    final Stream[] streams = newStreams(predicates.length);
    final Scratch s = SCRATCH.get();
    long total = 0;
    for (int leaf = fromLeaf; leaf < toLeaf; leaf++) {
      final int rowCount = store.rowCount(leaf);
      if (rowCount <= 0 || !openLeaf(streams, predBytes, predNumeric, predicates, leaf, rowCount)) {
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
      final SegmentFetcher fetcher) {
    conjunctiveAggregateNumeric(store, predicates, numericColumn, acc, 0, store.leafCount(), fetcher);
  }

  /** Ranged variant for chunked parallel dispatch. */
  public static void conjunctiveAggregateNumeric(final ProjectionColumnStore store,
      final ColumnPredicate[] predicates, final int numericColumn, final long[] acc,
      final int fromLeaf, final int toLeaf, final SegmentFetcher fetcher) {
    if (store.columnKind(numericColumn) != ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_LONG) {
      throw new IllegalStateException("aggregate column " + numericColumn + " is not NUMERIC_LONG");
    }
    final byte[][][] predBytes = resolvePredicateBytes(store, predicates, fetcher);
    final boolean[] predNumeric = predicateNumeric(store, predicates);
    final byte[][] aggBytes = store.columnBytes(numericColumn, fetcher);
    final Stream[] streams = newStreams(predicates.length);
    final Stream aggStream = new Stream();
    final Scratch s = SCRATCH.get();
    long count = acc[0];
    long sum = acc[1];
    long min = acc[2];
    long max = acc[3];
    for (int leaf = fromLeaf; leaf < toLeaf; leaf++) {
      final int rowCount = store.rowCount(leaf);
      if (rowCount <= 0 || !openLeaf(streams, predBytes, predNumeric, predicates, leaf, rowCount)) {
        continue;
      }
      aggStream.open(aggBytes[leaf], rowCount, true);
      if (!aggStream.plainWidth) {
        throw new IllegalStateException("Aggregate column " + numericColumn
            + " has a non-plain width escape — kernel dispatched without eligibility check");
      }
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
        boolean unpacked = false;
        for (int w = 0; w < words; w++) {
          long word = s.mask[w] & aggStream.presenceWord(wordBase + w, presWords, rowCount);
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
          if (word == -1L) {
            // Dense word — all 64 rows fold: linear loop, no per-bit bookkeeping. Long
            // addition is associative (wrapping) and min/max order-insensitive, so this is
            // bit-exact with the sparse path.
            for (int k = 0; k < 64; k++) {
              final long v = s.aggVals[rowBase + k];
              sum += v;
              if (v < min) min = v;
              if (v > max) max = v;
            }
            count += 64;
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
      }
    }
    acc[0] = count;
    acc[1] = sum;
    acc[2] = min;
    acc[3] = max;
  }

  // ==================== predicate-tree kernels (P5b stage 6) ====================

  /** {@link #eligible} for a predicate tree — same gates, over the tree's leaves. */
  public static boolean eligibleTree(final ProjectionColumnStore store, final PredicateTree tree,
      final int aggColOrNegative, final SegmentFetcher fetcher) {
    return eligible(store, tree.leaves, aggColOrNegative, fetcher);
  }

  /**
   * Count of rows matching an arbitrary AND/OR {@link PredicateTree}, folded straight from
   * segment bytes. Leaf masks encode missing ⇒ {@code false}; combinators are word-wise
   * intersection/union — see the tree type's semantics contract.
   */
  public static long treeCount(final ProjectionColumnStore store, final PredicateTree tree,
      final SegmentFetcher fetcher) {
    return treeCount(store, tree, 0, store.leafCount(), fetcher);
  }

  /** Ranged variant for chunked parallel dispatch. */
  public static long treeCount(final ProjectionColumnStore store, final PredicateTree tree,
      final int fromLeaf, final int toLeaf, final SegmentFetcher fetcher) {
    final ColumnPredicate[] leaves = tree.leaves;
    final byte[][][] leafBytes = resolvePredicateBytes(store, leaves, fetcher);
    final boolean[] leafNumeric = predicateNumeric(store, leaves);
    final Stream[] streams = newStreams(leaves.length);
    final boolean[] leafLive = new boolean[leaves.length];
    final Scratch s = SCRATCH.get();
    long total = 0;
    for (int leaf = fromLeaf; leaf < toLeaf; leaf++) {
      final int rowCount = store.rowCount(leaf);
      if (rowCount <= 0
          || !openTreeLeaf(streams, leafLive, leafBytes, leafNumeric, leaves, tree, leaf, rowCount)) {
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
      final SegmentFetcher fetcher) {
    treeAggregateNumeric(store, tree, numericColumn, acc, 0, store.leafCount(), fetcher);
  }

  /** Ranged variant for chunked parallel dispatch. */
  public static void treeAggregateNumeric(final ProjectionColumnStore store,
      final PredicateTree tree, final int numericColumn, final long[] acc,
      final int fromLeaf, final int toLeaf, final SegmentFetcher fetcher) {
    if (store.columnKind(numericColumn) != ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_LONG) {
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
    long count = acc[0];
    long sum = acc[1];
    long min = acc[2];
    long max = acc[3];
    for (int leaf = fromLeaf; leaf < toLeaf; leaf++) {
      final int rowCount = store.rowCount(leaf);
      if (rowCount <= 0
          || !openTreeLeaf(streams, leafLive, leafBytes, leafNumeric, leaves, tree, leaf, rowCount)) {
        continue;
      }
      aggStream.open(aggBytes[leaf], rowCount, true);
      if (!aggStream.plainWidth) {
        throw new IllegalStateException("Aggregate column " + numericColumn
            + " has a non-plain width escape — kernel dispatched without eligibility check");
      }
      final int presWords = (rowCount + 63) >>> 6;
      for (int blockStart = 0; blockStart < rowCount; blockStart += BLOCK_VALUES) {
        final int rows = Math.min(BLOCK_VALUES, rowCount - blockStart);
        final int words = (rows + 63) >>> 6;
        final int wordBase = blockStart >>> 6;
        final long[] root = evaluateTreeBlock(tree, streams, leafLive, leafNumeric, leaves, s,
            blockStart, rows, words, wordBase, presWords, rowCount);
        boolean unpacked = false;
        for (int w = 0; w < words; w++) {
          long word = root[w] & aggStream.presenceWord(wordBase + w, presWords, rowCount);
          if (word == 0L) {
            continue;
          }
          if (!unpacked) {
            aggStream.unpackBlock(blockStart, rows, s.aggVals);
            unpacked = true;
          }
          final int rowBase = w << 6;
          if (word == -1L) {
            for (int k = 0; k < 64; k++) {
              final long v = s.aggVals[rowBase + k];
              sum += v;
              if (v < min) min = v;
              if (v > max) max = v;
            }
            count += 64;
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
      }
    }
    acc[0] = count;
    acc[1] = sum;
    acc[2] = min;
    acc[3] = max;
  }

  /**
   * Open every tree-leaf stream for {@code leaf} and run the TREE-aware zone phase:
   * per-leaf EMPTY states (zone skip, {@code min > max}, all-missing presence) propagate
   * through the program — {@code AND(EMPTY, x) = EMPTY}, {@code OR(EMPTY, x) = x} — and
   * only a provably-EMPTY root prunes the whole leaf. Non-pruned EMPTY leaves contribute
   * all-zero masks in the block phase without touching their packed values.
   */
  private static boolean openTreeLeaf(final Stream[] streams, final boolean[] leafLive,
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
      final ColumnPredicate[] predicates, final SegmentFetcher fetcher) {
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
      numeric[i] = ProjectionIndexLeafPage.isNumericKind(store.columnKind(predicates[i].column));
    }
    return numeric;
  }

  /**
   * Open every predicate stream for {@code leaf} and run the zone phase. Returns
   * {@code false} when the leaf is pruned outright (zone skip, {@code min > max}, or an
   * all-missing predicate column — parity: an all-missing presence ANDs every mask word to
   * zero, so skipping the leaf is exact).
   */
  private static boolean openLeaf(final Stream[] streams, final byte[][][] predBytes,
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
      if (m == -1L) {
        // Dense word — all 64 rows are candidates: one op-specialized linear compare loop
        // (branch-predictable, no per-bit extraction) instead of the ntz walk. Result bits
        // are identical to the sparse path by construction.
        mask[w] = denseCompareWord(vals, rowBase, p.op, lit, high);
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

  /** Dense 64-value compare: the op switch is hoisted OUT of the loop so each arm is a tight, predictable scan. */
  private static long denseCompareWord(final long[] vals, final int rowBase,
      final ProjectionIndexScan.Op op, final long lit, final long high) {
    long out = 0L;
    switch (op) {
      case GT -> {
        for (int k = 0; k < 64; k++) {
          if (vals[rowBase + k] > lit) out |= 1L << k;
        }
      }
      case LT -> {
        for (int k = 0; k < 64; k++) {
          if (vals[rowBase + k] < lit) out |= 1L << k;
        }
      }
      case GE -> {
        for (int k = 0; k < 64; k++) {
          if (vals[rowBase + k] >= lit) out |= 1L << k;
        }
      }
      case LE -> {
        for (int k = 0; k < 64; k++) {
          if (vals[rowBase + k] <= lit) out |= 1L << k;
        }
      }
      case EQ -> {
        for (int k = 0; k < 64; k++) {
          if (vals[rowBase + k] == lit) out |= 1L << k;
        }
      }
      case BETWEEN_GT_LT -> {
        for (int k = 0; k < 64; k++) {
          final long v = vals[rowBase + k];
          if (v > lit && v < high) out |= 1L << k;
        }
      }
      case BETWEEN_GT_LE -> {
        for (int k = 0; k < 64; k++) {
          final long v = vals[rowBase + k];
          if (v > lit && v <= high) out |= 1L << k;
        }
      }
      case BETWEEN_GE_LT -> {
        for (int k = 0; k < 64; k++) {
          final long v = vals[rowBase + k];
          if (v >= lit && v < high) out |= 1L << k;
        }
      }
      case BETWEEN_GE_LE -> {
        for (int k = 0; k < 64; k++) {
          final long v = vals[rowBase + k];
          if (v >= lit && v <= high) out |= 1L << k;
        }
      }
    }
    return out;
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
