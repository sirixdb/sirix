/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.path.summary;

import io.sirix.settings.Constants;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import org.jspecify.annotations.Nullable;

import java.math.BigInteger;
import java.util.Arrays;

/**
 * Per-path insert-side statistics delta — the associative partial aggregate behind BOTH the cursor
 * path's deferred statistics ({@code PathSummaryWriter}'s pending map) and the bulk loaders'
 * per-chunk collection.
 *
 * <p>
 * Extracted from {@code PathSummaryWriter.DeferredStats} verbatim so there is exactly ONE
 * implementation of the observation semantics — the numeric classifier, the NaN/infinity policy,
 * the overflow-guarded sum, the fraction carry, the byte-bound cloning and the HLL hashing. A bulk
 * build that accumulated through different code could diverge from the cursor in exactly the
 * corners the exactness rules depend on; sharing the class makes that divergence impossible by
 * construction.
 *
 * <p>
 * Fields track only the state needed to merge into a {@link PathNode}'s stats in one pass: count /
 * sum / min / max / null-count for numeric; bytes min/max for strings; HLL batched into a local
 * sketch that is unioned on flush; leaf-page witnesses for the page-skip bitmap. {@code kind} is
 * recorded so mixed-type updates (rare but defensible) keep the right lane.
 *
 * <p>
 * Not thread-safe: an instance belongs to one writer (the transaction's summary writer or one
 * bulk-import chunk worker). Chunk partials are merged single-threaded by the coordinator via
 * {@link #mergeFrom}. Per lane: count, nullCount, min, max, byte bounds, HLL, page witnesses AND the
 * integral sum (a 128-bit accumulator, so no intermediate can leave the representable range) are
 * associative and commutative — any merge order yields the same value, bit for bit. {@code
 * sumFraction} alone is not: it is a {@code double} accumulator, so its low bits depend on addition
 * order, which is why the coordinator drains chunks in DOCUMENT order and why the field is exempt
 * from byte identity (nothing serves it).
 *
 * <p>
 * That guarantee does not stop at the batch. {@link PathNode#mergeLongStats(long, long, long, long,
 * long)} takes both halves of the integral sum and {@link PathStats#sumHi} carries them across a
 * commit, so the persisted statistics are a function of the observed values alone — not of how many
 * flushes the load took, nor of how a bulk load chunked its input. Pinned end to end by
 * {@code PathStatsPersistedSumOrderIndependenceTest} and by the differential's three-commit arm.
 */
public final class PathStatsAccumulator {

  /** Kind marker: 0 = none, 1 = long/numeric, 2 = bytes. */
  byte kind;
  long count;
  long nullCount;
  /**
   * Low (unsigned) half of the 128-bit integral accumulator; also the exact {@code long} total
   * whenever {@link #sumFitsLong()} holds.
   */
  long sumLo;
  /** High (signed) half of the 128-bit integral accumulator. */
  long sumHi;
  /** Fractional remainder of the integral sum for non-integral observations in this batch. */
  double sumFraction;
  /** Whether any observation in this batch arrived as a floating-point value. */
  boolean doubleTyped;
  /**
   * Whether this batch saw an OBSERVATION that cannot be accumulated (NaN, an infinity). Sum
   * overflow is NOT recorded here — it is derived from the 128-bit accumulator by
   * {@link #isValueStatsUntrusted()}, so that the verdict depends only on the observation multiset
   * and not on the order or chunking it arrived in.
   */
  boolean untrustedObservation;
  long min;
  long max;
  byte @Nullable [] minBytes;
  byte @Nullable [] maxBytes;
  @Nullable
  HyperLogLogSketch hll;
  /**
   * Leaf-page keys witnessed during this batch. Small set — bounded by the flush cadence (cursor) or
   * the chunk size (bulk). Lazily allocated so paths that never feed the page-skip index pay no extra
   * state.
   */
  @Nullable
  IntOpenHashSet seenPages;

  public PathStatsAccumulator() {
    reset();
  }

  /** Clear in place for pooling — zero-alloc reuse across flushes. */
  public void reset() {
    kind = 0;
    count = 0L;
    nullCount = 0L;
    sumLo = 0L;
    sumHi = 0L;
    sumFraction = 0.0d;
    doubleTyped = false;
    untrustedObservation = false;
    min = Long.MAX_VALUE;
    max = Long.MIN_VALUE;
    minBytes = null;
    maxBytes = null;
    hll = null;
    if (seenPages != null) {
      seenPages.clear();
    }
  }

  /**
   * Whether nothing has been observed.
   *
   * <p>
   * Deliberately just {@code count == 0 && nullCount == 0} — the same gate the summary writer's own
   * flush applies before touching a PathNode. It is exact for every reachable state: each
   * {@code add*} increments {@code count} or {@code nullCount} before anything else it sets
   * ({@link #addDouble} bumps {@code count} before flagging NaN untrusted), and the bulk batches call
   * {@link #recordPage}/{@link #recordPageOfNode} only alongside an {@code add*} — so a partial
   * carrying ONLY flags or ONLY page witnesses cannot exist, and this gate can never drop state.
   */
  public boolean isEmpty() {
    return count == 0L && nullCount == 0L;
  }

  public void recordPage(final int pageKey) {
    if (pageKey < 0) {
      return;
    }
    if (seenPages == null) {
      seenPages = new IntOpenHashSet(4);
    }
    seenPages.add(pageKey);
  }

  public void addLong(final long v) {
    kind = 1;
    count++;
    addToSum(v);
    if (v < min) {
      min = v;
    }
    if (v > max) {
      max = v;
    }
    if (hll == null) {
      hll = new HyperLogLogSketch();
    }
    hll.add(v);
  }

  /**
   * Folds {@code delta} into the 128-bit integral accumulator.
   *
   * <p>
   * 128 bits rather than 64 because the observation multiset — not the order it is fed in — must
   * decide whether the total is representable. A 64-bit accumulator that drops an overflowing addend
   * answers differently for {@code [M, M, -M]} fed sequentially (untrusted, partial total) than for
   * the same values split into chunks that each stay in range (trusted, different total), so a
   * bulk-loaded resource would persist statistics a cursor-loaded one does not. Here every
   * intermediate is exact — 2^63 magnitude values would need 2^64 of them to leave 128 bits — and
   * {@link #sumFitsLong()} asks the only question that matters: does the TRUE total fit the
   * {@code long} the summary persists? Both halves are handed to
   * {@link PathNode#mergeLongStats(long, long, long, long, long)} at flush, and
   * {@link PathStats#sumHi} carries them across a commit, so the same guarantee holds end to end
   * rather than only within one batch.
   *
   * <p>
   * Two long adds, one unsigned compare, no allocation and no boxing: this runs once per ingested
   * numeric value on the bulk loaders' hot path.
   */
  void addToSum(final long delta) {
    add128(delta >> 63, delta);
  }

  /** Add the 128-bit value {@code (hi, lo)} into the accumulator; {@code lo} is unsigned. */
  private void add128(final long hi, final long lo) {
    final long updatedLo = sumLo + lo;
    sumHi = PathStats.addCarry(sumHi, hi, sumLo, updatedLo);
    sumLo = updatedLo;
  }

  /**
   * Whether the exact integral total is representable as a {@code long} — i.e. whether the high half
   * is nothing but the sign extension of the low half.
   */
  public boolean sumFitsLong() {
    return PathStats.sumFitsLong(sumHi, sumLo);
  }

  /**
   * The exact integral total. Only meaningful when {@link #sumFitsLong()} holds; otherwise it is the
   * true total modulo 2^64 and callers must decline to serve it.
   */
  public long sumAsLong() {
    return sumLo;
  }

  /**
   * Whether value statistics may be served from this batch: {@code false} once an unaccumulable
   * observation arrived (NaN, an infinity) OR the exact total left {@code long} range.
   *
   * <p>
   * Both terms are functions of the observation multiset alone, so a chunked merge and a sequential
   * feed of the same values always agree. Note this is the BATCH-level verdict; the persisted one is
   * {@link PathNode#isStatsSumTrustworthy()}, which asks the same question of the 128-bit total the
   * batches folded into.
   */
  public boolean isValueStatsUntrusted() {
    return untrustedObservation || !sumFitsLong();
  }

  /**
   * Non-integral observation: integral part into the 128-bit sum, remainder into the fraction.
   *
   * <p>
   * NaN and the infinities carry nothing to accumulate and nothing to subtract later, and folding
   * them in silently poisons the accumulators for good: {@code (long) NaN} is 0 while
   * {@code NaN - NaN} is NaN, so {@code sumFraction} would stay NaN forever, and casting the
   * infinities yields {@code Long.MIN_VALUE}/{@code Long.MAX_VALUE} straight into min/max. They mark
   * the observation untrusted instead, which is the same bargain a delete makes.
   */
  public void addDouble(final double v) {
    kind = 1;
    doubleTyped = true;
    count++;
    if (Double.isNaN(v) || Double.isInfinite(v)) {
      untrustedObservation = true;
      if (hll == null) {
        hll = new HyperLogLogSketch();
      }
      hll.add(Double.doubleToLongBits(v));
      return;
    }
    final double integral = v < 0
        ? Math.ceil(v)
        : Math.floor(v);
    addToSum((long) integral);
    sumFraction += v - integral;
    final long lower = (long) Math.floor(v);
    final long upper = (long) Math.ceil(v);
    if (lower < min) {
      min = lower;
    }
    if (upper > max) {
      max = upper;
    }
    if (hll == null) {
      hll = new HyperLogLogSketch();
    }
    hll.add(Double.doubleToLongBits(v));
  }

  public void addBytes(final byte[] v) {
    addBytes(v, 0, v.length);
  }

  /**
   * Range variant for callers holding a value inside a reused scanner buffer — the bulk parsers'
   * shape. Bounds are compared and (only on a new extremum) copied from the range; the sketch hashes
   * the range directly, identical to hashing an exact-length copy.
   */
  public void addBytes(final byte[] buf, final int offset, final int length) {
    kind = 2;
    count++;
    if (minBytes == null || Arrays.compareUnsigned(buf, offset, offset + length, minBytes, 0, minBytes.length) < 0) {
      minBytes = Arrays.copyOfRange(buf, offset, offset + length);
    }
    if (maxBytes == null || Arrays.compareUnsigned(buf, offset, offset + length, maxBytes, 0, maxBytes.length) > 0) {
      maxBytes = Arrays.copyOfRange(buf, offset, offset + length);
    }
    if (hll == null) {
      hll = new HyperLogLogSketch();
    }
    hll.add(buf, offset, length);
  }

  /**
   * Witness the leaf page of {@code nodeKey} for the page-skip bitmap — the same derivation the
   * cursor's deferred path uses ({@code nodeKey >>> INP_REFERENCE_COUNT_EXPONENT}; keys above the
   * int-keyed bitmap's range skip tracking). Pass a negative key to skip.
   */
  public void recordPageOfNode(final long nodeKey) {
    if (nodeKey < 0L) {
      return;
    }
    final long pk = nodeKey >>> Constants.INP_REFERENCE_COUNT_EXPONENT;
    if (pk > Integer.MAX_VALUE) {
      return;
    }
    recordPage((int) pk);
  }

  public void addBoolean(final boolean v) {
    addLong(v
        ? 1L
        : 0L);
  }

  public void addNull() {
    nullCount++;
  }

  /**
   * Record a numeric observation with the SAME integral-vs-floating dispatch the cursor path uses
   * ({@link #isExactLong}): integral-and-long-representable numbers keep the exact long accumulator,
   * everything else routes through {@link #addDouble}, whose {@code doubleTyped} flag makes value
   * aggregates decline to the scan.
   */
  public void addNumber(final Number number) {
    if (isExactLong(number)) {
      addLong(number.longValue());
    } else {
      addDouble(number.doubleValue());
    }
  }

  /**
   * Whether {@code number} is integral AND representable as a {@code long} without loss — the single
   * classifier shared by the cursor record/remove hooks and the bulk loaders. A {@link BigInteger}
   * that does not fit a long must NOT take the long lane: {@code longValue()} wraps modulo 2^64 and
   * the wrapped value would be SERVED, because nothing would mark the column untrustworthy.
   */
  public static boolean isExactLong(final Number number) {
    if (number instanceof Long || number instanceof Integer || number instanceof Short || number instanceof Byte) {
      return true;
    }
    if (number instanceof BigInteger bigInteger) {
      return bigInteger.bitLength() < Long.SIZE;
    }
    // Everything else — Double, Float, BigDecimal — takes the double lane even when the value
    // happens to be integral (2.0): the type says the column is floating-point, and the
    // doubleTyped flag must decline value aggregates for it. VERBATIM from the cursor's
    // classifier; a wider rule here would serve sums the scan computes differently.
    return false;
  }

  /**
   * Fold {@code other}'s observations into this accumulator — the chunk-merge operation.
   * {@code other} is left untouched.
   *
   * <p>
   * <b>Associativity, stated honestly.</b> count, nullCount, min, max, byte bounds, HLL, page
   * witnesses AND the integral sum (with it {@link #isValueStatsUntrusted()}, since the 128-bit
   * accumulator has no intermediate to overflow) are associative AND commutative — any merge order
   * yields exactly the same value, so a chunked merge and a sequential feed of the same observation
   * multiset are indistinguishable. Exactly ONE lane is not: {@code sumFraction} is a double
   * accumulator ({@code +=}), so its low bits depend on addition order — the coordinator therefore
   * merges chunks in DOCUMENT order, which makes the result deterministic (and the field is never
   * served: {@code PathStats#sumFraction}). Pinned by
   * {@code BulkPathStatsAccumulatorContractTest}.
   */
  public void mergeFrom(final PathStatsAccumulator other) {
    if (other.kind != 0) {
      // Last non-empty batch wins the kind — the coordinator merges chunks in document order, so
      // this reproduces the sequential feed's per-observation overwrite at flush granularity.
      kind = other.kind;
    }
    count += other.count;
    nullCount += other.nullCount;
    add128(other.sumHi, other.sumLo);
    sumFraction += other.sumFraction;
    doubleTyped |= other.doubleTyped;
    untrustedObservation |= other.untrustedObservation;
    if (other.min < min) {
      min = other.min;
    }
    if (other.max > max) {
      max = other.max;
    }
    if (other.minBytes != null && (minBytes == null || Arrays.compareUnsigned(other.minBytes, minBytes) < 0)) {
      minBytes = other.minBytes.clone();
    }
    if (other.maxBytes != null && (maxBytes == null || Arrays.compareUnsigned(other.maxBytes, maxBytes) > 0)) {
      maxBytes = other.maxBytes.clone();
    }
    if (other.hll != null) {
      if (hll == null) {
        hll = new HyperLogLogSketch();
      }
      hll.union(other.hll);
    }
    if (other.seenPages != null && !other.seenPages.isEmpty()) {
      if (seenPages == null) {
        seenPages = new IntOpenHashSet(other.seenPages.size());
      }
      seenPages.addAll(other.seenPages);
    }
  }
}
