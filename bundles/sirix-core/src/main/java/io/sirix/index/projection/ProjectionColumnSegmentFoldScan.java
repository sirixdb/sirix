/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.index.projection.ProjectionColumnStore.ColumnSegmentFetcher;
import io.sirix.index.projection.ProjectionIndexScan.ColumnPredicate;
import io.sirix.index.projection.ProjectionIndexScan.PredicateTree;

import java.util.Arrays;

/**
 * Fold-during-decode scan kernels (P5b stage 4): conjunctive counts and numeric-long aggregates
 * evaluated STRAIGHT from the verified BODY segment bytes cached by
 * {@link ProjectionColumnStore#columnBytes(int)} — no {@code long[rowCount]} slice arrays are ever
 * materialized. Values stream through an L1-resident 1024-value scratch block (8&nbsp;KiB), the
 * vector-at-a-time model of the analytical engines this path is measured against: unpack a block,
 * mask it, fold it, move on.
 *
 * <p>
 * <b>Why 1024-value blocks are safe for ANY width.</b> Packed runs are little-endian LSB-first bit
 * streams; a block boundary at value {@code 1024·n} sits at bit offset {@code 1024·n·width}, which
 * is a whole number of bytes for every width — so each block decodes independently with the same
 * positional bulk unpacker the slice path uses
 * ({@link ProjectionIndexRowGroupCodec#unpackInto(byte[], int, int, int, long, long[], int)}), and
 * block-local masks align with 64-bit presence words ({@code 1024 = 16 × 64}).
 *
 * <p>
 * <b>Parity contract.</b> Semantics mirror {@link ProjectionColumnScan} (and therefore
 * {@code ProjectionIndexByteScan.evaluateRowGroupMask}) bit for bit: numeric zone-skip on
 * segment-truth min/max with the {@code min > max} all-missing prune, missing ⇒ false via the
 * presence AND, boolean bitmap equality, and the aggregate column's own presence AND before
 * folding. {@code ProjectionColumnScanParityTest} pins the equivalence against both the byte and
 * the slice kernels over randomized stores.
 *
 * <p>
 * <b>Eligibility.</b> Only plain-FOR numeric streams (width 0–56 or 64) and boolean streams are
 * foldable; an ALP-escaped double stream ({@code width == 65}) or any reserved escape routes the
 * query to the slice kernels via {@link #eligible} — never an error.
 *
 * <p>
 * <b>Exactness contract.</b> A numeric aggregate that includes {@link #AGG_SUM} is gated by
 * {@link #requireSumFitsLong} before any folding: {@code xs:integer} is arbitrary precision, so a
 * total that would wrap a {@code long} is DECLINED with an {@link ArithmeticException} rather than
 * served modulo 2^64. Counts and extrema are unaffected.
 *
 * <p>
 * Scratch is thread-local and fixed-size; per-leaf evaluation allocates nothing beyond the per-call
 * stream holders.
 *
 * <p>
 * <b>Compare arms are Vector API, the folds are scalar.</b> {@code ProjectionFoldKernelBenchmark}
 * (512-bit species) put the scalar compare-to-bitmask loop at ~4.1&nbsp;ns/row on dense words against
 * ~0.21 for the lane-compare kernel, and dispatch keeps that crossover
 * ({@link ProjectionVectorKernels#COMPARE_WALK_MAX_BITS}). The FOLD arms were Vector API too until
 * a 100M allocation profile showed their masked lanewise call running the Java fallback inside this
 * kernel's compile (see {@link #foldMaskedBlock}); they now sum dense blocks straight off the packed
 * bits and fold the rest with plain scalar accumulators.
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
   * Per-leaf parsed view over one column's BODY segment bytes — a mutable flyweight reused across
   * leaves (thread-confined to one kernel invocation). Wire form after the 6-byte PIXS header:
   * {@code flags; [rowCount > 0] min, max; presence marker [+ words];} then
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

    /**
     * Wrapped sum of {@code count} values from value {@code valueStart} straight off the packed
     * bits — {@code count · base + Σ packed}; exact whenever the true total fits a long, which the
     * caller's zone-map pre-flight guarantees.
     */
    long sumBlock(final int valueStart, final int count) {
      final int byteOff = valuesBase + (width == 64
          ? valueStart << 3
          : (valueStart >>> 3) * width);
      return base * count + ProjectionIndexRowGroupCodec.sumPacked(seg, byteOff, count, width);
    }

    /** Unpack {@code count} values of the block starting at value {@code valueStart}. */
    void unpackBlock(final int valueStart, final int count, final long[] out) {
      final int byteOff = valuesBase + (width == 64
          ? valueStart << 3
          : (valueStart >>> 3) * width);
      ProjectionIndexRowGroupCodec.unpackInto(seg, byteOff, count, width, base, out, 0);
    }
  }

  private ProjectionColumnSegmentFoldScan() {}

  /**
   * Whether the fused kernels can serve this query shape: every predicate column sliceable and
   * non-string, and every involved NUMERIC stream plain-FOR in every leaf (no ALP or reserved width
   * escapes). Fetches (and caches) the involved columns' bytes — so a {@code true} answer means the
   * kernels' substrate is already resident.
   *
   * @throws IllegalStateException on corrupt/missing segments (same contract as
   *         {@link ProjectionColumnStore#columnBytes(int)}) — callers decline through the established
   *         fail-soft flow
   */
  public static boolean eligible(final ProjectionColumnStore store, final ColumnPredicate[] predicates,
      final int aggColOrNegative, final ColumnSegmentFetcher fetcher) {
    for (final ColumnPredicate p : predicates) {
      if (p.stringLitBytes != null || !store.columnSliceable(p.column)) {
        return false;
      }
      // This kernel has exactly TWO lanes — packed longs and packed bits — and picks between them
      // by kind. A column that is neither (a per-leaf dictionary, a set) has no lane here, and the
      // bit arm would read its bytes as bits rather than decline. Sliceability alone stopped saying
      // so once it began admitting string kinds.
      if (!ProjectionIndexRowGroupPage.isLongLaneKind(store.columnKind(p.column))
          && store.columnKind(p.column) != ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN) {
        return false;
      }
    }
    // The aggregate must be a NUMERIC or TEMPORAL column, checked by KIND: columnSliceable now also
    // admits string and boolean columns, and a fold over their bytes would misparse them as numbers.
    // A temporal column folds for its extrema (its epoch orders as the text does); the caller is what
    // keeps a sum off it.
    if (aggColOrNegative >= 0 && !ProjectionIndexRowGroupPage.isNumericKind(store.columnKind(aggColOrNegative))
        && !ProjectionIndexRowGroupPage.isTemporalKind(store.columnKind(aggColOrNegative))) {
      return false;
    }
    final Stream probe = new Stream();
    for (int i = 0; i <= predicates.length; i++) {
      final int col = i < predicates.length
          ? predicates[i].column
          : aggColOrNegative;
      if (col < 0) {
        continue;
      }
      // Long-lane columns are the ones the plain-width probe applies to — the same set
      // predicateNumeric will send down the unpacking arm, so the probe must cover exactly them.
      final boolean longLaneKind = ProjectionIndexRowGroupPage.isLongLaneKind(store.columnKind(col));
      if (!longLaneKind) {
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
  public static long conjunctiveCount(final ProjectionColumnStore store, final ColumnPredicate[] predicates,
      final ColumnSegmentFetcher fetcher) {
    return conjunctiveCount(store, predicates, 0, store.rowGroupCount(), fetcher);
  }

  /** Ranged variant for the executor's chunked parallel dispatch — scratch is thread-local. */
  public static long conjunctiveCount(final ProjectionColumnStore store, final ColumnPredicate[] predicates,
      final int fromRowGroup, final int toRowGroup, final ColumnSegmentFetcher fetcher) {
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
        if (evaluateBlock(streams, predicates, predNumeric, s, blockStart, rows, words, wordBase, presWords,
            rowCount)) {
          for (int w = 0; w < words; w++) {
            total += Long.bitCount(s.mask[w]);
          }
        }
      }
    }
    return total;
  }

  /** {@code acc[0]}, the surviving row count — always folded; it is a popcount of the mask. */
  public static final int AGG_COUNT = 1;
  /** {@code acc[1]}, the sum over surviving rows. */
  public static final int AGG_SUM = 1 << 1;
  /** {@code acc[2]}, the minimum over surviving rows. */
  public static final int AGG_MIN = 1 << 2;
  /** {@code acc[3]}, the maximum over surviving rows. */
  public static final int AGG_MAX = 1 << 3;
  /** Every slot — the historical behaviour, and what the unmasked overloads request. */
  public static final int AGG_ALL = AGG_COUNT | AGG_SUM | AGG_MIN | AGG_MAX;

  /** Extremum slots. Their absence is what lets the fold skip the emulated 64-bit min/max. */
  private static final int AGG_EXTREMA = AGG_MIN | AGG_MAX;

  /**
   * Conjunctive numeric-long aggregate folded straight from segment bytes —
   * {@code acc = [count, sum, min, max]}, initialised by the caller to
   * {@code {0, 0, Long.MAX_VALUE, Long.MIN_VALUE}}.
   *
   * <p>
   * Folds every slot. Prefer the {@code aggMask} overload when the query wants only some of them: on
   * an ISA without 64-bit SIMD min/max the extrema cost ~35-40 % of the fold.
   *
   * @throws ArithmeticException when the sum could not be folded exactly — see
   *         {@link #requireSumFitsLong}; callers treat it as a DECLINE
   */
  public static void conjunctiveAggregateNumeric(final ProjectionColumnStore store, final ColumnPredicate[] predicates,
      final int numericColumn, final long[] acc, final ColumnSegmentFetcher fetcher) {
    conjunctiveAggregateNumeric(store, predicates, numericColumn, acc, 0, store.rowGroupCount(), fetcher, AGG_ALL);
  }

  /**
   * Fold only the slots named by {@code aggMask} (see {@link #AGG_COUNT} and friends).
   *
   * <p>
   * {@code count} and {@code sum} ride along free — a popcount and a masked lane add, both single
   * instructions. The extrema do not: AVX2 has no {@code vpminsq}/{@code vpmaxsq}, so a masked 64-bit
   * min/max compiles to a compare-and-blend emulation, and folding it for a query that asked for
   * {@code sum} is pure waste. Slots outside the mask are left exactly as the caller initialised
   * them, so a chunked merge over per-thread accumulators stays correct.
   *
   * <p>
   * Requesting {@link #AGG_SUM} (and therefore {@code avg}) arms the pre-flight exactness gate; an
   * extrema-only query keeps serving a column whose sum would not fit, because its answer does not
   * depend on the sum lane. A caller that PUBLISHES a masked accumulator to another query's
   * {@code func} must not do so — the slots outside the mask are non-answers.
   *
   * @throws ArithmeticException when the sum could not be folded exactly — see
   *         {@link #requireSumFitsLong}; callers treat it as a DECLINE
   */
  public static void conjunctiveAggregateNumeric(final ProjectionColumnStore store, final ColumnPredicate[] predicates,
      final int numericColumn, final long[] acc, final ColumnSegmentFetcher fetcher, final int aggMask) {
    conjunctiveAggregateNumeric(store, predicates, numericColumn, acc, 0, store.rowGroupCount(), fetcher, aggMask);
  }

  /** Ranged variant for chunked parallel dispatch. */
  public static void conjunctiveAggregateNumeric(final ProjectionColumnStore store, final ColumnPredicate[] predicates,
      final int numericColumn, final long[] acc, final int fromRowGroup, final int toRowGroup,
      final ColumnSegmentFetcher fetcher) {
    conjunctiveAggregateNumeric(store, predicates, numericColumn, acc, fromRowGroup, toRowGroup, fetcher, AGG_ALL);
  }

  /** Ranged variant for chunked parallel dispatch, folding only {@code aggMask}'s slots. */
  public static void conjunctiveAggregateNumeric(final ProjectionColumnStore store, final ColumnPredicate[] predicates,
      final int numericColumn, final long[] acc, final int fromRowGroup, final int toRowGroup,
      final ColumnSegmentFetcher fetcher, final int aggMask) {
    // Admitted for its ORDER, like the sliced fold: min/max over an epoch is exactly the extremum of
    // the text, while a SUM over one is not an answer any caller asks for (and requireSumFitsLong
    // below still refuses one it cannot compute).
    if (!ProjectionIndexRowGroupPage.isOrderedLongKind(store.columnKind(numericColumn))) {
      throw new IllegalStateException(
          "aggregate column " + numericColumn + " is not NUMERIC_LONG or a temporal kind");
    }
    final byte[][][] predBytes = resolvePredicateBytes(store, predicates, fetcher);
    final boolean[] predNumeric = predicateNumeric(store, predicates);
    final byte[][] aggBytes = store.columnBytes(numericColumn, fetcher);
    if ((aggMask & AGG_SUM) != 0) {
      requireSumFitsLong(store, numericColumn, aggBytes, fromRowGroup, toRowGroup, acc);
    }
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
        if (!evaluateBlock(streams, predicates, predNumeric, s, blockStart, rows, words, wordBase, presWords,
            rowCount)) {
          continue;
        }
        foldMaskedBlock(s.mask, aggStream, s, blockStart, rows, words, wordBase, presWords, rowCount, acc, aggMask);
      }
    }
  }

  /**
   * Pre-flight zone-map bound on the {@link #AGG_SUM} slot: refuse the fold BEFORE it starts when the
   * surviving rows could overflow {@code acc[1]}.
   *
   * <p>
   * <b>Why the sum needs a guard at all.</b> {@code xs:integer} is arbitrary precision and the
   * interpreter behind this route promotes an overflowing total to exact decimal, so a wrapped
   * {@code long} is not a fast answer — it is a wrong one that looks plausible. A column of 64-bit
   * ids (ClickBench's {@code UserID}, hashes, snowflake keys) wraps a long accumulator after a few
   * dozen rows; before this guard an installed projection turned a correct {@code avg} into
   * {@code 3.6e13} where the truth is {@code 5.7e17}.
   *
   * <p>
   * <b>Why not inside the fold loops.</b> The fold arms are Vector API kernels whose entire measured
   * advantage is that a masked lane add is a single instruction — there is no lanewise
   * {@code addExact}, and reconstructing one from an abs/max per lane group would reintroduce in the
   * sum-only arm precisely the extremum work that arm exists to avoid (35-40 % of the fold on an ISA
   * without {@code vpminsq}). The zone map bounds the total without reading a single value: every
   * surviving row of leaf {@code g} has magnitude at most {@code max(|min_g|, |max_g|)} and there are
   * at most {@code rows_g} of them, so {@code Σ rows_g · max(|min_g|, |max_g|)} bounds {@code |sum|}
   * from metadata the segment header already carries — one header read and two multiplications per
   * ROW GROUP, never per row.
   *
   * <p>
   * <b>Why exact integer math for the bound.</b> {@code Math.multiplyExact} / {@code Math.addExact}
   * in one try/catch declines if and only if the bound genuinely leaves the long range; accumulating
   * the bound in {@code double} and comparing against {@code 9.0e18} (what the page-region kernel
   * does, where no exact alternative was reachable) would additionally reject the 0.2e18-wide band
   * below {@link Long#MAX_VALUE} and could not represent the bound exactly anyway.
   * {@link Math#absExact} supplies the {@link Long#MIN_VALUE} guard for free: its magnitude is
   * unrepresentable, so a column holding it declines rather than folding a negated garbage bound.
   *
   * <p>
   * The bound is CONSERVATIVE — predicates and presence only ever remove contributions — so this may
   * decline a column whose actual sum would have fit. That is the intended trade: a decline costs a
   * slower correct answer from the generic pipeline, a wrap is a wrong one.
   *
   * @param acc the caller's accumulator; its incoming sum is folded into the bound so a REUSED
   *        accumulator stays covered
   * @throws ArithmeticException naming the column — callers treat it as a DECLINE, the same contract
   *         {@code ProjectionIndexByteScan}'s exact-math kernels already publish
   */
  private static void requireSumFitsLong(final ProjectionColumnStore store, final int numericColumn,
      final byte[][] aggBytes, final int fromRowGroup, final int toRowGroup, final long[] acc) {
    try {
      long bound = Math.absExact(acc[1]);
      for (int leaf = fromRowGroup; leaf < toRowGroup; leaf++) {
        final int rows = store.rowCount(leaf);
        if (rows <= 0) {
          continue;
        }
        final byte[] segment = aggBytes[leaf];
        final long min = ProjectionIndexRowGroupCodec.getLongLE(segment, 7);
        final long max = ProjectionIndexRowGroupCodec.getLongLE(segment, 15);
        // min > max is this format's all-missing marker (same prune as openRowGroup): the leaf
        // contributes nothing, and its sentinel min/max have no magnitude to bound.
        if (min > max) {
          continue;
        }
        final long magnitude = Math.max(Math.absExact(min), Math.absExact(max));
        bound = Math.addExact(bound, Math.multiplyExact((long) rows, magnitude));
      }
    } catch (final ArithmeticException overflow) {
      throw new ArithmeticException("Projection column " + numericColumn
          + " cannot be summed exactly in a long: the zone-map magnitude bound over row groups [" + fromRowGroup + ", "
          + toRowGroup + ") leaves the signed 64-bit range");
    }
  }

  /**
   * Open the aggregate column's stream for one row group, refusing a width escape the eligibility
   * gate should already have declined.
   */
  private static void openAggregateColumn(final Stream aggStream, final byte[] segment, final int rowCount,
      final int numericColumn) {
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
   * <p>
   * <b>Scalar on purpose — a measured verdict (100M, q2-shaped {@code sum(x)} over two columns).</b>
   * The previous arms folded through the Vector API: {@code vsum.add(fromArray(..))} on dense words
   * and {@code add(v, m)} / {@code lanewise(MIN, v, m)} on partial ones. Inside this kernel's C2
   * compile the MASKED lanewise call was a virtual call that never intrinsified — every call
   * allocated a {@code long[]} and a {@code Long256Vector} through the Java fallback, and its presence
   * in the compilation unit dragged the DENSE add down the same path: the async-profiler allocation
   * profile of a hot try was 100&nbsp;% {@code LongVector.add → lanewiseTemplate}, the fold ran at
   * 10-15&nbsp;ns/value per thread, and each try paid 1-3 young GCs for a kernel that "allocates
   * nothing". Dropping the masked arm alone halved the query with byte-identical answers. What
   * stays is code C2 compiles the same way every time: the dense block (every row survives — every
   * block of an unpredicated aggregate over a NOT NULL column) sums STRAIGHT from the packed bits
   * ({@link ProjectionIndexRowGroupCodec#sumPacked}) without writing the scratch at all, dense
   * words fold with independent scalar accumulators, and partial words walk their set bits.
   * {@link ProjectionVectorKernels} keeps the compare kernels, whose lane compares have no masked
   * virtual call in them.
   */
  private static void foldMaskedBlock(final long[] maskWords, final Stream aggStream, final Scratch s,
      final int blockStart, final int rows, final int words, final int wordBase, final int presWords,
      final int rowCount, final long[] acc, final int aggMask) {
    // Dispatched once per block, never per row: each arm is then a single fixed loop shape.
    if ((aggMask & AGG_EXTREMA) == 0) {
      foldMaskedBlockSum(maskWords, aggStream, s, blockStart, rows, words, wordBase, presWords, rowCount, acc);
    } else if ((aggMask & AGG_EXTREMA) == AGG_EXTREMA) {
      foldMaskedBlockFull(maskWords, aggStream, s, blockStart, rows, words, wordBase, presWords, rowCount, acc);
    } else {
      foldMaskedBlockOneExtremum(maskWords, aggStream, s, blockStart, rows, words, wordBase, presWords, rowCount, acc,
          (aggMask & AGG_MIN) != 0);
    }
  }

  /**
   * Whether every one of the block's {@code rows} rows survives both the mask and the aggregate
   * column's presence — the shape that lets the fold skip the scratch entirely. A tail word is dense
   * when exactly its {@code rows & 63} low bits are set, so the last block of a leaf qualifies too.
   */
  private static boolean blockDense(final long[] maskWords, final Stream aggStream, final int rows, final int words,
      final int wordBase, final int presWords, final int rowCount) {
    for (int w = 0; w < words; w++) {
      final long expect = ProjectionIndexRowGroupCodec.expectedFullWord(w, words, rows);
      if ((maskWords[w] & aggStream.presenceWord(wordBase + w, presWords, rowCount)) != expect) {
        return false;
      }
    }
    return true;
  }

  /**
   * One extremum, not two.
   *
   * <p>
   * {@code min(x)} and {@code max(x)} are separate queries and arrive as separate calls, so the full
   * fold was computing an extremum nobody asked for on each of them. The {@code wantMin} branch sits
   * outside the loops, so each call still presents the JIT one fixed loop shape per arm.
   */
  private static void foldMaskedBlockOneExtremum(final long[] maskWords, final Stream aggStream, final Scratch s,
      final int blockStart, final int rows, final int words, final int wordBase, final int presWords,
      final int rowCount, final long[] acc, final boolean wantMin) {
    long count = acc[0];
    long sum = acc[1];
    long ext = wantMin
        ? acc[2]
        : acc[3];
    final long[] vals = s.aggVals;
    if (blockDense(maskWords, aggStream, rows, words, wordBase, presWords, rowCount)) {
      aggStream.unpackBlock(blockStart, rows, vals);
      count += rows;
      sum += sumDense(vals, 0, rows);
      final long e = wantMin
          ? minDense(vals, 0, rows)
          : maxDense(vals, 0, rows);
      if (wantMin
          ? e < ext
          : e > ext) {
        ext = e;
      }
    } else {
      boolean unpacked = false;
      for (int w = 0; w < words; w++) {
        long word = maskWords[w] & aggStream.presenceWord(wordBase + w, presWords, rowCount);
        if (word == 0L) {
          continue;
        }
        if (!unpacked) {
          aggStream.unpackBlock(blockStart, rows, vals);
          unpacked = true;
        }
        final int rowBase = w << 6;
        if (word == -1L) {
          count += 64;
          sum += sumDense(vals, rowBase, 64);
          final long e = wantMin
              ? minDense(vals, rowBase, 64)
              : maxDense(vals, rowBase, 64);
          if (wantMin
              ? e < ext
              : e > ext) {
            ext = e;
          }
          continue;
        }
        while (word != 0L) {
          final int bit = Long.numberOfTrailingZeros(word);
          word &= word - 1L;
          final long v = vals[rowBase + bit];
          count++;
          sum += v;
          if (wantMin
              ? v < ext
              : v > ext) {
            ext = v;
          }
        }
      }
    }
    acc[0] = count;
    acc[1] = sum;
    // The unrequested extremum keeps the caller's identity, exactly as the sum-only arm leaves both.
    acc[wantMin
        ? 2
        : 3] = ext;
  }

  /**
   * Count and sum only, leaving {@code acc[2]} and {@code acc[3]} untouched at the caller's
   * identities. A dense block never touches the scratch: its total comes straight off the packed
   * bits.
   */
  private static void foldMaskedBlockSum(final long[] maskWords, final Stream aggStream, final Scratch s,
      final int blockStart, final int rows, final int words, final int wordBase, final int presWords,
      final int rowCount, final long[] acc) {
    long count = acc[0];
    long sum = acc[1];
    if (blockDense(maskWords, aggStream, rows, words, wordBase, presWords, rowCount)) {
      acc[0] = count + rows;
      acc[1] = sum + aggStream.sumBlock(blockStart, rows);
      return;
    }
    final long[] vals = s.aggVals;
    boolean unpacked = false;
    for (int w = 0; w < words; w++) {
      long word = maskWords[w] & aggStream.presenceWord(wordBase + w, presWords, rowCount);
      if (word == 0L) {
        continue;
      }
      if (!unpacked) {
        aggStream.unpackBlock(blockStart, rows, vals);
        unpacked = true;
      }
      final int rowBase = w << 6;
      if (word == -1L) {
        count += 64;
        sum += sumDense(vals, rowBase, 64);
        continue;
      }
      while (word != 0L) {
        final int bit = Long.numberOfTrailingZeros(word);
        word &= word - 1L;
        count++;
        sum += vals[rowBase + bit];
      }
    }
    acc[0] = count;
    acc[1] = sum;
  }

  private static void foldMaskedBlockFull(final long[] maskWords, final Stream aggStream, final Scratch s,
      final int blockStart, final int rows, final int words, final int wordBase, final int presWords,
      final int rowCount, final long[] acc) {
    long count = acc[0];
    long sum = acc[1];
    long min = acc[2];
    long max = acc[3];
    final long[] vals = s.aggVals;
    if (blockDense(maskWords, aggStream, rows, words, wordBase, presWords, rowCount)) {
      aggStream.unpackBlock(blockStart, rows, vals);
      count += rows;
      sum += sumDense(vals, 0, rows);
      min = Math.min(min, minDense(vals, 0, rows));
      max = Math.max(max, maxDense(vals, 0, rows));
    } else {
      boolean unpacked = false;
      for (int w = 0; w < words; w++) {
        long word = maskWords[w] & aggStream.presenceWord(wordBase + w, presWords, rowCount);
        if (word == 0L) {
          continue;
        }
        if (!unpacked) {
          // Unpack the aggregate block only once a bit actually survives the mask AND —
          // fully filtered/absent blocks never touch the packed values at all.
          aggStream.unpackBlock(blockStart, rows, vals);
          unpacked = true;
        }
        final int rowBase = w << 6;
        if (word == -1L) {
          count += 64;
          sum += sumDense(vals, rowBase, 64);
          min = Math.min(min, minDense(vals, rowBase, 64));
          max = Math.max(max, maxDense(vals, rowBase, 64));
          continue;
        }
        while (word != 0L) {
          final int bit = Long.numberOfTrailingZeros(word);
          word &= word - 1L;
          final long v = vals[rowBase + bit];
          count++;
          sum += v;
          if (v < min)
            min = v;
          if (v > max)
            max = v;
        }
      }
    }
    acc[0] = count;
    acc[1] = sum;
    acc[2] = min;
    acc[3] = max;
  }

  /**
   * Wrapped sum of {@code n} unpacked values with four independent accumulators — one add per
   * value with no cross-iteration chain, which C2 keeps in registers (and may superword) without
   * any intrinsic it can silently decline. {@code n} is a multiple of 4 for every caller except a
   * leaf's tail block, which the scalar remainder covers.
   */
  private static long sumDense(final long[] vals, final int from, final int n) {
    long s0 = 0L;
    long s1 = 0L;
    long s2 = 0L;
    long s3 = 0L;
    final int end4 = from + (n & ~3);
    int i = from;
    for (; i < end4; i += 4) {
      s0 += vals[i];
      s1 += vals[i + 1];
      s2 += vals[i + 2];
      s3 += vals[i + 3];
    }
    final int end = from + n;
    for (; i < end; i++) {
      s0 += vals[i];
    }
    return s0 + s1 + s2 + s3;
  }

  private static long minDense(final long[] vals, final int from, final int n) {
    long m0 = Long.MAX_VALUE;
    long m1 = Long.MAX_VALUE;
    final int end2 = from + (n & ~1);
    int i = from;
    for (; i < end2; i += 2) {
      m0 = Math.min(m0, vals[i]);
      m1 = Math.min(m1, vals[i + 1]);
    }
    if (i < from + n) {
      m0 = Math.min(m0, vals[i]);
    }
    return Math.min(m0, m1);
  }

  private static long maxDense(final long[] vals, final int from, final int n) {
    long m0 = Long.MIN_VALUE;
    long m1 = Long.MIN_VALUE;
    final int end2 = from + (n & ~1);
    int i = from;
    for (; i < end2; i += 2) {
      m0 = Math.max(m0, vals[i]);
      m1 = Math.max(m1, vals[i + 1]);
    }
    if (i < from + n) {
      m0 = Math.max(m0, vals[i]);
    }
    return Math.max(m0, m1);
  }

  // ==================== predicate-tree kernels (P5b stage 6) ====================

  /** {@link #eligible} for a predicate tree — same gates, over the tree's leaves. */
  public static boolean eligibleTree(final ProjectionColumnStore store, final PredicateTree tree,
      final int aggColOrNegative, final ColumnSegmentFetcher fetcher) {
    return eligible(store, tree.leaves, aggColOrNegative, fetcher);
  }

  /**
   * Count of rows matching an arbitrary AND/OR {@link PredicateTree}, folded straight from segment
   * bytes. Leaf masks encode missing ⇒ {@code false}; combinators are word-wise intersection/union —
   * see the tree type's semantics contract.
   */
  public static long treeCount(final ProjectionColumnStore store, final PredicateTree tree,
      final ColumnSegmentFetcher fetcher) {
    return treeCount(store, tree, 0, store.rowGroupCount(), fetcher);
  }

  /** Ranged variant for chunked parallel dispatch. */
  public static long treeCount(final ProjectionColumnStore store, final PredicateTree tree, final int fromRowGroup,
      final int toRowGroup, final ColumnSegmentFetcher fetcher) {
    final ColumnPredicate[] leaves = tree.leaves;
    final byte[][][] leafBytes = resolvePredicateBytes(store, leaves, fetcher);
    final boolean[] leafNumeric = predicateNumeric(store, leaves);
    final Stream[] streams = newStreams(leaves.length);
    final boolean[] leafLive = new boolean[leaves.length];
    final Scratch s = SCRATCH.get();
    long total = 0;
    for (int leaf = fromRowGroup; leaf < toRowGroup; leaf++) {
      final int rowCount = store.rowCount(leaf);
      if (rowCount <= 0 || !openTreeRowGroup(streams, leafLive, leafBytes, leafNumeric, leaves, tree, leaf, rowCount)) {
        continue;
      }
      final int presWords = (rowCount + 63) >>> 6;
      for (int blockStart = 0; blockStart < rowCount; blockStart += BLOCK_VALUES) {
        final int rows = Math.min(BLOCK_VALUES, rowCount - blockStart);
        final int words = (rows + 63) >>> 6;
        final int wordBase = blockStart >>> 6;
        final long[] root = evaluateTreeBlock(tree, streams, leafLive, leafNumeric, leaves, s, blockStart, rows, words,
            wordBase, presWords, rowCount);
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
   *
   * @throws ArithmeticException when the sum could not be folded exactly — see
   *         {@link #requireSumFitsLong}; callers treat it as a DECLINE
   */
  public static void treeAggregateNumeric(final ProjectionColumnStore store, final PredicateTree tree,
      final int numericColumn, final long[] acc, final ColumnSegmentFetcher fetcher) {
    treeAggregateNumeric(store, tree, numericColumn, acc, 0, store.rowGroupCount(), fetcher, AGG_ALL);
  }

  /** Fold only {@code aggMask}'s slots — see the flat kernel's overload for why that pays. */
  public static void treeAggregateNumeric(final ProjectionColumnStore store, final PredicateTree tree,
      final int numericColumn, final long[] acc, final ColumnSegmentFetcher fetcher, final int aggMask) {
    treeAggregateNumeric(store, tree, numericColumn, acc, 0, store.rowGroupCount(), fetcher, aggMask);
  }

  /** Ranged variant for chunked parallel dispatch. */
  public static void treeAggregateNumeric(final ProjectionColumnStore store, final PredicateTree tree,
      final int numericColumn, final long[] acc, final int fromRowGroup, final int toRowGroup,
      final ColumnSegmentFetcher fetcher) {
    treeAggregateNumeric(store, tree, numericColumn, acc, fromRowGroup, toRowGroup, fetcher, AGG_ALL);
  }

  /** Ranged variant for chunked parallel dispatch, folding only {@code aggMask}'s slots. */
  public static void treeAggregateNumeric(final ProjectionColumnStore store, final PredicateTree tree,
      final int numericColumn, final long[] acc, final int fromRowGroup, final int toRowGroup,
      final ColumnSegmentFetcher fetcher, final int aggMask) {
    // Admitted for its ORDER, like the sliced fold: min/max over an epoch is exactly the extremum of
    // the text, while a SUM over one is not an answer any caller asks for (and requireSumFitsLong
    // below still refuses one it cannot compute).
    if (!ProjectionIndexRowGroupPage.isOrderedLongKind(store.columnKind(numericColumn))) {
      throw new IllegalStateException(
          "aggregate column " + numericColumn + " is not NUMERIC_LONG or a temporal kind");
    }
    final ColumnPredicate[] leaves = tree.leaves;
    final byte[][][] leafBytes = resolvePredicateBytes(store, leaves, fetcher);
    final boolean[] leafNumeric = predicateNumeric(store, leaves);
    final byte[][] aggBytes = store.columnBytes(numericColumn, fetcher);
    if ((aggMask & AGG_SUM) != 0) {
      requireSumFitsLong(store, numericColumn, aggBytes, fromRowGroup, toRowGroup, acc);
    }
    final Stream[] streams = newStreams(leaves.length);
    final boolean[] leafLive = new boolean[leaves.length];
    final Stream aggStream = new Stream();
    final Scratch s = SCRATCH.get();
    for (int leaf = fromRowGroup; leaf < toRowGroup; leaf++) {
      final int rowCount = store.rowCount(leaf);
      if (rowCount <= 0 || !openTreeRowGroup(streams, leafLive, leafBytes, leafNumeric, leaves, tree, leaf, rowCount)) {
        continue;
      }
      openAggregateColumn(aggStream, aggBytes[leaf], rowCount, numericColumn);
      final int presWords = (rowCount + 63) >>> 6;
      for (int blockStart = 0; blockStart < rowCount; blockStart += BLOCK_VALUES) {
        final int rows = Math.min(BLOCK_VALUES, rowCount - blockStart);
        final int words = (rows + 63) >>> 6;
        final int wordBase = blockStart >>> 6;
        final long[] root = evaluateTreeBlock(tree, streams, leafLive, leafNumeric, leaves, s, blockStart, rows, words,
            wordBase, presWords, rowCount);
        foldMaskedBlock(root, aggStream, s, blockStart, rows, words, wordBase, presWords, rowCount, acc, aggMask);
      }
    }
  }

  /**
   * Open every tree-leaf stream for {@code leaf} and run the TREE-aware zone phase: per-leaf EMPTY
   * states (zone skip, {@code min > max}, all-missing presence) propagate through the program —
   * {@code AND(EMPTY, x) = EMPTY}, {@code OR(EMPTY, x) = x} — and only a provably-EMPTY root prunes
   * the whole leaf. Non-pruned EMPTY leaves contribute all-zero masks in the block phase without
   * touching their packed values.
   */
  private static boolean openTreeRowGroup(final Stream[] streams, final boolean[] leafLive, final byte[][][] leafBytes,
      final boolean[] leafNumeric, final ColumnPredicate[] leaves, final PredicateTree tree, final int leaf,
      final int rowCount) {
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
      } else if (insn == PredicateTree.OP_NOT) {
        // A dead child (zone-skipped, all-missing) matches NO row, so its negation matches
        // EVERY row — and a live child's negation is unknown. Either way the subtree can match.
        canMatch[depth - 1] = true;
      } else {
        depth--;
        canMatch[depth - 1] = canMatch[depth - 1] || canMatch[depth];
      }
    }
    return canMatch[0];
  }

  /**
   * Interpret the tree program for one block: leaf pushes evaluate the leaf's mask over the FULL
   * (tail-masked) row domain; AND/OR combine word-wise in place on the stack. Returns the root mask
   * (stack slot 0 of the scratch).
   */
  private static long[] evaluateTreeBlock(final PredicateTree tree, final Stream[] streams, final boolean[] leafLive,
      final boolean[] leafNumeric, final ColumnPredicate[] leaves, final Scratch s, final int blockStart,
      final int rows, final int words, final int wordBase, final int presWords, final int rowCount) {
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
            final long match = leaves[insn].boolLit
                ? bw
                : ~bw;
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
      } else if (insn == PredicateTree.OP_NOT) {
        // Complement within the block's valid rows: the trailing partial word must stay
        // zero past `rows` — downstream folds popcount raw words.
        final long[] a = s.stack[depth - 1];
        for (int w = 0; w < words; w++) {
          a[w] = ~a[w];
        }
        final int tail = rows & 63;
        if (tail != 0) {
          a[words - 1] &= (1L << tail) - 1;
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

  private static byte[][][] resolvePredicateBytes(final ProjectionColumnStore store, final ColumnPredicate[] predicates,
      final ColumnSegmentFetcher fetcher) {
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

  /**
   * Which predicate columns are read through the LONG lane rather than the bit lane.
   *
   * <p>
   * The question is about STORAGE, not about arithmetic, which is why it asks
   * {@link ProjectionIndexRowGroupPage#isLongLaneKind} and not {@code isNumericKind}: the false arm
   * below reads the column as a packed BOOLEAN word, so anything whose cells are longs must answer
   * {@code true} here or its bytes are interpreted as bits — a wrong answer with no symptom.
   * {@code NUMERIC_DOUBLE} is in for the same reason ({@code ProjectionDoubleEncoding}'s
   * order-preserving longs), and {@code STRING_GLOBAL} joins them: its cells are dictionary ids, and
   * the literal was resolved to an id before the predicate was built.
   */
  private static boolean[] predicateNumeric(final ProjectionColumnStore store, final ColumnPredicate[] predicates) {
    final boolean[] numeric = new boolean[predicates.length];
    for (int i = 0; i < predicates.length; i++) {
      numeric[i] = ProjectionIndexRowGroupPage.isLongLaneKind(store.columnKind(predicates[i].column));
    }
    return numeric;
  }

  /**
   * Open every predicate stream for {@code leaf} and run the zone phase. Returns {@code false} when
   * the leaf is pruned outright (zone skip, {@code min > max}, or an all-missing predicate column —
   * parity: an all-missing presence ANDs every mask word to zero, so skipping the leaf is exact).
   */
  private static boolean openRowGroup(final Stream[] streams, final byte[][][] predBytes, final boolean[] predNumeric,
      final ColumnPredicate[] predicates, final int leaf, final int rowCount) {
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
   * Apply every predicate to the block's mask. Returns {@code false} when the mask emptied (callers
   * skip the fold/count for this block).
   */
  private static boolean evaluateBlock(final Stream[] streams, final ColumnPredicate[] predicates,
      final boolean[] predNumeric, final Scratch s, final int blockStart, final int rows, final int words,
      final int wordBase, final int presWords, final int rowCount) {
    for (int i = 0; i < streams.length; i++) {
      final Stream st = streams[i];
      if (predNumeric[i]) {
        st.unpackBlock(blockStart, rows, s.vals);
        evalNumericBlock(s.vals, predicates[i], st, words, wordBase, presWords, rowCount, s.mask);
      } else {
        for (int w = 0; w < words; w++) {
          final long bw = st.boolWord(wordBase + w);
          final long match = predicates[i].boolLit
              ? bw
              : ~bw;
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

  private static void evalNumericBlock(final long[] vals, final ColumnPredicate p, final Stream st, final int words,
      final int wordBase, final int presWords, final int rowCount, final long[] mask) {
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
          case NE -> v != lit;
          case BETWEEN_GT_LT -> v > lit && v < high;
          case BETWEEN_GT_LE -> v > lit && v <= high;
          case BETWEEN_GE_LT -> v >= lit && v < high;
          case BETWEEN_GE_LE -> v >= lit && v <= high;
          // Unreachable by construction — `eligible` rejects any predicate carrying
          // stringLitBytes, which every string op does. Kept loud, not silent.
          case STR_LT, STR_LE, STR_GT, STR_GE, STR_CONTAINS ->
            throw new IllegalStateException("string op in the fold-scan kernel: " + p.op);
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
