/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page.pax;

import io.sirix.node.LE;
import jdk.incubator.vector.DoubleVector;
import org.jspecify.annotations.Nullable;
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

import java.lang.foreign.MemorySegment;
import java.nio.ByteOrder;

/**
 * SIMD kernels over {@link DoubleRegion}'s per-tag columns.
 *
 * <h2>ALP tags never materialize a double</h2>
 *
 * <p>ALP's decode is {@code I * 10^f / 10^e} — multiplication by a positive constant — so it is
 * strictly MONOTONIC in the packed integer. A range predicate over the doubles is therefore a
 * range predicate over the integers: {@code [dlo, dhi]} translates once per tag into
 * {@code [ilo, ihi]}, and the scan is the same FOR+bit-packed unsigned-compare kernel the long
 * column runs, straight over the encoded bytes. This is the BtrBlocks/DuckDB scan story landing
 * on the double column: the compressed form IS the scan format.
 *
 * <p>The translation computes candidate bounds in floating point and then FIXES them by decoding
 * the neighbouring integers — division rounds, and an off-by-one at the boundary is a wrong count,
 * not a slow one. Exception slots hold a placeholder (the FOR base); the vector pass counts them
 * as whatever the placeholder decides, and a scalar correction loop — proportional to the
 * exception count, not the column — subtracts the placeholder's verdict and adds the raw value's.
 *
 * <p>PLAIN tags take one load and one two-sided compare per eight lanes; NaN matches nothing,
 * which is both IEEE semantics and the conservative reading of a value JSON cannot produce.
 */
public final class DoubleRegionSimd {

  private static final VectorSpecies<Double> SPECIES = DoubleVector.SPECIES_PREFERRED;
  private static final int LANES = SPECIES.length();

  private DoubleRegionSimd() {
    throw new AssertionError("no instances");
  }

  /**
   * Count values of tag {@code t} within the inclusive bound {@code [dlo, dhi]}.
   *
   * <p>A bound, not operators, for the reason spelled out in {@link NumberRegionSimd}: parameters
   * do not constant-fold, and open bounds are pre-converted by the planner ({@code v > x} is
   * {@code v >= nextUp(x)} for finite doubles).
   */
  public static long countTagRange(final MemorySegment payload, final DoubleRegion.Header h,
      final int t, final double dlo, final double dhi) {
    return countTagRangeMasked(payload, h, t, dlo, dhi, null);
  }

  /**
   * The liveness-masked form, for a versioned merge in which a newer fragment shadows some of the
   * tag's values. {@code liveBits} is indexed TAG-LOCAL — bit {@code k} governs the tag's k-th
   * value — matching the projection {@code DoubleRegion.fieldOrdinalAt} makes possible; a
   * {@code null} mask means everything is live and this IS {@link #countTagRange}.
   */
  public static long countTagRangeMasked(final MemorySegment payload, final DoubleRegion.Header h,
      final int t, final double dlo, final double dhi, final long @Nullable [] liveBits) {
    if (!(dlo <= dhi)) {
      return 0L;  // an empty or NaN bound matches nothing
    }
    final int n = h.tagCount[t];
    if (n <= 0) {
      return 0L;
    }
    return switch (h.tagEnc[t]) {
      case DoubleRegion.ENC_ALP -> countAlp(payload, h, t, n, dlo, dhi, liveBits);
      case DoubleRegion.ENC_ALP_RD -> countAlpRd(payload, h, t, n, dlo, dhi, liveBits);
      case DoubleRegion.ENC_PLAIN -> countPlain(payload, h.tagDataOffset[t], n, dlo, dhi, liveBits);
      // ENC_DEC and anything unknown: REFUSED, never guessed. A decimal tag's integers are the
      // decimals themselves, not a recoding of doubles, and two distinct decimals can share a
      // double image — so a double-domain bound cannot decide it. Enumerated rather than left to
      // `default`, which used to route every unrecognised kind into countPlain and would have read
      // packed integers as raw IEEE bits.
      default -> REFUSED;
    };
  }

  /**
   * Returned when a tag cannot be decided from the bounds supplied. Not a count: callers MUST test
   * for it and fall back to the record path, which compares the stored values exactly.
   *
   * <p>{@link Long#MIN_VALUE} so that a caller which ignores it produces an obviously impossible
   * count rather than a plausible one.
   */
  public static final long REFUSED = Long.MIN_VALUE;

  /**
   * Count an {@link DoubleRegion#ENC_DEC} tag against bounds already expressed in ITS integer
   * domain — the tag's own decimal scale — so the comparison is exact decimal arithmetic.
   *
   * <p>This is the entry point that makes a column of real decimals (prices, rates) scannable.
   * {@link #countTagRangeMasked} refuses such a tag because a double bound cannot separate two
   * decimals that round to the same double; supplying the bound in decimal space removes the
   * ambiguity at its source rather than compensating for it.
   *
   * @param loInt inclusive lower bound, as an unscaled integer at {@code h.alpE[t]}'s scale
   * @param hiInt inclusive upper bound, same domain
   * @return the match count, or {@link #REFUSED} when the tag is not exact-decimal
   */
  public static long countDecTagRangeMasked(final MemorySegment payload,
      final DoubleRegion.Header h, final int t, final long loInt, final long hiInt,
      final long @Nullable [] liveBits) {
    if (h.tagEnc[t] != DoubleRegion.ENC_DEC) {
      return REFUSED;
    }
    final int n = h.tagCount[t];
    if (n <= 0 || loInt > hiInt) {
      return 0L;
    }
    final long base = h.alpForBase[t];
    final int width = h.alpBitWidth[t] & 0xFF;
    final long maxPacked = width == 0 ? 0L : BitUnpackSimd.maskFor(width);
    // Saturating, and ordered so no subtraction runs before its operands are known to be in range:
    // the packed values occupy exactly [base, base + maxPacked], and a bound outside it decides the
    // tag without arithmetic. base + maxPacked is computed only when it cannot overflow.
    if (hiInt < base) {
      return 0L;
    }
    final long domainHi = base > Long.MAX_VALUE - maxPacked ? Long.MAX_VALUE : base + maxPacked;
    if (loInt > domainHi) {
      return 0L;
    }
    // Both differences are now provably within [0, maxPacked], so neither can overflow.
    final long plo = loInt <= base ? 0L : loInt - base;
    final long phi = hiInt >= domainHi ? maxPacked : hiInt - base;
    return countPackedRange(payload, h.tagDataOffset[t], width, n, plo, phi, liveBits);
  }

  private static boolean live(final long @Nullable [] liveBits, final int k) {
    return ColumnLoad.isLive(liveBits, k);
  }

  /**
   * Count an ALP-RD tag by DECODING in registers — the split is a bit permutation, not a
   * monotonic map, so unlike ALP the predicate cannot move into integer space. Per eight lanes:
   * unpack the codes, gather their left parts from the dictionary with a single
   * {@code selectFrom} (the dictionary is at most {@value DoubleRegion#RD_MAX_DICT} longs — one
   * register), unpack the right bits, shift-OR the halves together, reinterpret the lanes as
   * doubles, and compare. No scalar decode, no materialization.
   *
   * <p>Exception slots pack code zero, so their vector verdict used dictionary entry zero's left
   * part with the REAL right bits; the correction loop recomputes both variants per exception.
   */
  private static long countAlpRd(final MemorySegment payload, final DoubleRegion.Header h,
      final int t, final int n, final double dlo, final double dhi,
      final long @Nullable [] liveBits) {
    final int rw = h.rdRightWidth[t] & 0xFF;
    final int cw = h.rdCodeWidth[t] & 0xFF;
    final int dictSize = h.rdDictSize[t] & 0xFF;
    long count = 0;
    int i = 0;
    final BitUnpackSimd.Plan rightPlan = BitUnpackSimd.planFor(rw);
    final BitUnpackSimd.Plan codePlan = cw == 0 ? null : BitUnpackSimd.planFor(cw);
    if (rightPlan != null && (cw == 0 || codePlan != null) && BitUnpackSimd.vectorProfitable(n)) {
      final long progress = countAlpRdVector(payload, h, t, n, dlo, dhi, liveBits, rw, cw, dictSize,
                                             rightPlan, codePlan);
      i = (int) (progress >>> 32);
      count = (int) progress;
    }
    count += countAlpRdScalarTail(payload, h, t, i, n, dlo, dhi, liveBits, rw, cw);
    return count + correctAlpRdExceptions(payload, h, t, dlo, dhi, liveBits, rw);
  }

  /**
   * Vector body of {@link #countAlpRd}: the resume index in the high word, the count in the low
   * one.
   *
   * <p>The dictionary is spread over as many registers as it needs, each padded with entry 0. A
   * dictionary within one register gathers with a single {@code selectFrom}; a wider one runs a
   * blend cascade — mask in the lanes whose code falls in each register's index window. The AND
   * with {@code LANES-1} keeps every lane a VALID index for {@code selectFrom} (which validates all
   * lanes, used or not); the window mask then decides which register's answer survives.
   */
  private static long countAlpRdVector(final MemorySegment payload, final DoubleRegion.Header h,
      final int t, final int n, final double dlo, final double dhi,
      final long @Nullable [] liveBits, final int rw, final int cw, final int dictSize,
      final BitUnpackSimd.Plan rightPlan, final BitUnpackSimd.Plan codePlan) {
    long count = 0;
    int i = 0;
    {
      final int registers = (dictSize + ColumnLoad.LANES - 1) / ColumnLoad.LANES;
      final LongVector[] dictV = new LongVector[registers];
      final long[] dictLanes = new long[ColumnLoad.LANES];
      for (int r = 0; r < registers; r++) {
        for (int d = 0; d < ColumnLoad.LANES; d++) {
          final int idx = r * ColumnLoad.LANES + d;
          dictLanes[d] = idx < dictSize
              ? payload.get(LE.LONG, h.rdDictOffset[t] + (long) idx * Long.BYTES)
              : payload.get(LE.LONG, h.rdDictOffset[t]);
        }
        dictV[r] = LongVector.fromArray(ColumnLoad.LONG_SPECIES, dictLanes, 0);
      }
      final DoubleVector loV = DoubleVector.broadcast(SPECIES, dlo);
      final DoubleVector hiV = DoubleVector.broadcast(SPECIES, dhi);
      final int lastRight =
          BitUnpackSimd.lastVectorGroupStart(payload.byteSize(), h.tagDataOffset[t], rw);
      final int lastCode = cw == 0
          ? Integer.MAX_VALUE
          : BitUnpackSimd.lastVectorGroupStart(payload.byteSize(), h.rdCodesOffset[t], cw);
      for (; i <= n - ColumnLoad.LANES && i <= lastRight && i <= lastCode; i += ColumnLoad.LANES) {
        final LongVector right = rightPlan.unpack(payload, h.tagDataOffset[t], i);
        final LongVector left;
        if (cw == 0) {
          left = dictV[0];  // one dictionary entry, already broadcast across the register
        } else {
          final LongVector codes = codePlan.unpack(payload, h.rdCodesOffset[t], i);
          final LongVector laneIdx =
              codes.lanewise(VectorOperators.AND, (long) (ColumnLoad.LANES - 1));
          LongVector gathered = laneIdx.selectFrom(dictV[0]);
          for (int r = 1; r < registers; r++) {
            final var window = codes
                .compare(VectorOperators.GE, (long) r * ColumnLoad.LANES)
                .and(codes.compare(VectorOperators.LT, (long) (r + 1) * ColumnLoad.LANES));
            gathered = gathered.blend(laneIdx.selectFrom(dictV[r]), window);
          }
          left = gathered;
        }
        final DoubleVector v = left.lanewise(VectorOperators.LSHL, rw).or(right)
                                   .viewAsFloatingLanes();
        // The gather above dominates this loop, so unlike the leaner kernels the mask branch is
        // not worth two copies of the body; it stays inline, biased and predictable.
        var m = v.compare(VectorOperators.GE, loV).and(v.compare(VectorOperators.LE, hiV));
        if (liveBits != null) {
          m = m.and(ColumnLoad.laneMaskDouble(ColumnLoad.liveWindow(liveBits, i)));
        }
        count += m.trueCount();
      }
    }
    return ((long) i << 32) | count;
  }

  /**
   * Scalar tail of {@link #countAlpRd}, decoding THROUGH the dictionary and deliberately ignoring
   * the exception list: the correction assumes every LIVE slot was judged by its packed code, tail
   * included.
   */
  private static long countAlpRdScalarTail(final MemorySegment payload,
      final DoubleRegion.Header h, final int t, final int from, final int n, final double dlo,
      final double dhi, final long @Nullable [] liveBits, final int rw, final int cw) {
    long count = 0;
    for (int i = from; i < n; i++) {
      if (!live(liveBits, i)) {
        continue;
      }
      final long right = BitUnpackSimd.decodeAt(payload, h.tagDataOffset[t], rw,
                                                BitUnpackSimd.maskFor(rw), i);
      final long code = cw == 0
          ? 0L
          : BitUnpackSimd.decodeAt(payload, h.rdCodesOffset[t], cw, BitUnpackSimd.maskFor(cw), i);
      final long left = payload.get(LE.LONG, h.rdDictOffset[t] + code * Long.BYTES);
      final double v = Double.longBitsToDouble((left << rw) | right);
      if (v >= dlo && v <= dhi) {
        count++;
      }
    }
    return count;
  }

  /**
   * Undo each exception's dictionary-decoded verdict and apply its real one; answers the net
   * adjustment.
   */
  private static long correctAlpRdExceptions(final MemorySegment payload,
      final DoubleRegion.Header h, final int t, final double dlo, final double dhi,
      final long @Nullable [] liveBits, final int rw) {
    final int exceptions = h.alpExceptionCount[t];
    if (exceptions == 0) {
      return 0L;
    }
    long delta = 0;
    final long posBase = h.alpExceptionOffset[t];
    final long leftBase = posBase + (long) exceptions * Short.BYTES;
    final long dict0 = payload.get(LE.LONG, h.rdDictOffset[t]);
    for (int x = 0; x < exceptions; x++) {
      final int pos = payload.get(LE.SHORT, posBase + (long) x * Short.BYTES) & 0xFFFF;
      if (!live(liveBits, pos)) {
        continue;  // a shadowed exception was never counted, so it corrects nothing
      }
      final long right = BitUnpackSimd.decodeAt(payload, h.tagDataOffset[t], rw,
                                                BitUnpackSimd.maskFor(rw), pos);
      final double judged = Double.longBitsToDouble((dict0 << rw) | right);
      if (judged >= dlo && judged <= dhi) {
        delta--;
      }
      final long left = payload.get(LE.SHORT, leftBase + (long) x * Short.BYTES) & 0xFFFFL;
      final double actual = Double.longBitsToDouble((left << rw) | right);
      if (actual >= dlo && actual <= dhi) {
        delta++;
      }
    }
    return delta;
  }

  // ─────────────────────────────────────────────────────────────── PLAIN

  private static long countPlain(final MemorySegment payload, final int valuesOffset, final int n,
      final double dlo, final double dhi, final long @Nullable [] liveBits) {
    long count = 0;
    int i = 0;
    if (BitUnpackSimd.vectorProfitable(n)) {
      final DoubleVector loV = DoubleVector.broadcast(SPECIES, dlo);
      final DoubleVector hiV = DoubleVector.broadcast(SPECIES, dhi);
      // Two loops, selected once: the unmasked path is what every non-merged scan runs, and it
      // stays exactly as branch-free as before liveness masking existed.
      if (liveBits == null) {
        for (; i <= n - LANES; i += LANES) {
          final DoubleVector v = DoubleVector.fromMemorySegment(SPECIES, payload,
              valuesOffset + (long) i * Double.BYTES, ByteOrder.LITTLE_ENDIAN);
          count += v.compare(VectorOperators.GE, loV)
                    .and(v.compare(VectorOperators.LE, hiV))
                    .trueCount();
        }
      } else {
        for (; i <= n - LANES; i += LANES) {
          final DoubleVector v = DoubleVector.fromMemorySegment(SPECIES, payload,
              valuesOffset + (long) i * Double.BYTES, ByteOrder.LITTLE_ENDIAN);
          count += v.compare(VectorOperators.GE, loV)
                    .and(v.compare(VectorOperators.LE, hiV))
                    .and(ColumnLoad.laneMaskDouble(ColumnLoad.liveWindow(liveBits, i)))
                    .trueCount();
        }
      }
    }
    for (; i < n; i++) {
      if (!live(liveBits, i)) {
        continue;
      }
      final double v =
          Double.longBitsToDouble(payload.get(LE.LONG, valuesOffset + (long) i * Double.BYTES));
      if (v >= dlo && v <= dhi) {
        count++;
      }
    }
    return count;
  }

  // ───────────────────────────────────────────────────────────────── ALP

  private static long countAlp(final MemorySegment payload, final DoubleRegion.Header h,
      final int t, final int n, final double dlo, final double dhi,
      final long @Nullable [] liveBits) {
    final int e = h.alpE[t] & 0xFF;
    final int f = h.alpF[t] & 0xFF;
    final long base = h.alpForBase[t];
    final int width = h.alpBitWidth[t] & 0xFF;
    final long maxPacked = width == 0 ? 0L : BitUnpackSimd.maskFor(width);

    // ---- translate the double bound into integer space, exactly ----
    // Candidates from floating-point division, then fixed by decoding neighbours inside the
    // clamped domain [base-1, base+maxPacked+1]: division rounds, and an off-by-one at the
    // boundary is a wrong count, not a slow one. Infinite or overflowing bounds land on the
    // clamp edges, where the fix-up degenerates to "everything" or "nothing" correctly.
    final long domainLo = base - 1;
    final long domainHi = base + maxPacked + 1;
    final long ilo = fixLowerBound(dlo, e, f, domainLo, domainHi);
    final long ihi = fixUpperBound(dhi, e, f, domainLo, domainHi);
    if (ilo == BOUND_UNRESOLVED || ihi == BOUND_UNRESOLVED) {
      // The fix-up walk exhausted its budget — pathological exponents. Decode scalar; correct is
      // the contract, fast is the common case.
      return countAlpByDecoding(payload, h, t, n, dlo, dhi, liveBits);
    }
    final long plo = Math.max(0L, ilo - base);
    final long phi = Math.min(maxPacked, ihi - base);
    final boolean encodedCanMatch = ilo <= ihi && ihi >= base && ilo <= base + maxPacked;

    final long count = encodedCanMatch
        ? countPackedRange(payload, h.tagDataOffset[t], width, n, plo, phi, liveBits)
        : 0L;
    // The placeholder packs as zero, i.e. decodes as the FOR base — its verdict under the integer
    // bound is one fixed boolean for the whole tag.
    return count + correctAlpExceptions(payload, h, t, dlo, dhi, liveBits,
                                        encodedCanMatch && plo <= 0L && 0L <= phi);
  }

  /** Fully scalar {@link #countAlp}, for a tag whose bound could not be translated exactly. */
  private static long countAlpByDecoding(final MemorySegment payload, final DoubleRegion.Header h,
      final int t, final int n, final double dlo, final double dhi,
      final long @Nullable [] liveBits) {
    long count = 0;
    for (int i = 0; i < n; i++) {
      if (!live(liveBits, i)) {
        continue;
      }
      final double v = DoubleRegion.decodeValueAt(payload, h, t, i);
      if (v >= dlo && v <= dhi) {
        count++;
      }
    }
    return count;
  }

  /**
   * Exception correction, scalar and proportional to the exception count; answers the net
   * adjustment.
   *
   * <p>Only LIVE exceptions correct anything: a shadowed slot was never counted by the masked
   * vector pass, so neither its placeholder verdict nor its raw value may touch the total.
   */
  private static long correctAlpExceptions(final MemorySegment payload,
      final DoubleRegion.Header h, final int t, final double dlo, final double dhi,
      final long @Nullable [] liveBits, final boolean placeholderCounted) {
    final int exceptions = h.alpExceptionCount[t];
    if (exceptions == 0) {
      return 0L;
    }
    long delta = 0;
    final long posBase = h.alpExceptionOffset[t];
    final long rawBase = posBase + (long) exceptions * Short.BYTES;
    for (int x = 0; x < exceptions; x++) {
      final int pos = payload.get(LE.SHORT, posBase + (long) x * Short.BYTES) & 0xFFFF;
      if (!live(liveBits, pos)) {
        continue;
      }
      if (placeholderCounted) {
        delta--;
      }
      final double raw =
          Double.longBitsToDouble(payload.get(LE.LONG, rawBase + (long) x * Long.BYTES));
      if (raw >= dlo && raw <= dhi) {
        delta++;
      }
    }
    return delta;
  }

  /**
   * Count packed entries in {@code [plo, phi]} — the same unsigned-span trick the long column's
   * kernels use, over {@link BitUnpackSimd}'s unpack.
   */
  /**
   * Write a tag's matches into a SELECTION BITMAP instead of counting them.
   *
   * <p>The composable half of this kernel family. A count cannot be combined — two counts cannot be
   * ANDed — so every conjunction had to be fused by hand into its own kernel, per encoding and per
   * masked/unmasked variant. A bitmap composes with {@code &}, {@code |} and {@code ~}, which is
   * how BtrBlocks/Umbra-style engines evaluate an arbitrary predicate WITHOUT ever decoding: each
   * leaf answers in its own encoded domain and the tree is bitmap algebra over the results.
   *
   * <p>Deliberately takes no liveness mask. Liveness is one more AND applied once by the evaluator,
   * which is what removes the {@code *Masked} twin of every kernel.
   *
   * @param out tag-local bitmap, at least {@link ColumnLoad#bitmapWords(int)} words, pre-zeroed
   * @return {@code false} when this tag cannot be answered in its encoded domain and the caller
   *         must fall back; {@code true} when {@code out} holds the answer
   */
  public static boolean selectTagRange(final MemorySegment payload, final DoubleRegion.Header h,
      final int t, final double dlo, final double dhi, final long[] out) {
    final int n = h.tagCount[t];
    if (n <= 0) {
      return true;  // nothing to select; an empty selection is a valid answer
    }
    if (!(dlo <= dhi)) {
      return true;  // empty or NaN bound matches nothing
    }
    return switch (h.tagEnc[t]) {
      case DoubleRegion.ENC_ALP -> selectAlp(payload, h, t, n, dlo, dhi, out);
      case DoubleRegion.ENC_PLAIN -> {
        selectPlain(payload, h.tagDataOffset[t], n, dlo, dhi, out);
        yield true;
      }
      // ALP-RD splits the bit pattern; the transform is NOT monotonic, so a range predicate has no
      // image in its encoded domain. Decoding one tag locally is the honest answer — and the
      // selection it produces composes exactly like an encoded one, so nothing above needs to know.
      case DoubleRegion.ENC_ALP_RD -> {
        selectByDecode(payload, h, t, n, dlo, dhi, out);
        yield true;
      }
      // A decimal tag cannot be decided from a double bound: two decimals can share a double image.
      case DoubleRegion.ENC_DEC -> false;
      default -> false;
    };
  }

  /**
   * Write an {@link DoubleRegion#ENC_DEC} tag's matches, from bounds already in ITS integer domain.
   *
   * @return {@code false} when the tag is not exact-decimal
   */
  public static boolean selectDecTagRange(final MemorySegment payload, final DoubleRegion.Header h,
      final int t, final long loInt, final long hiInt, final long[] out) {
    if (h.tagEnc[t] != DoubleRegion.ENC_DEC) {
      return false;
    }
    final int n = h.tagCount[t];
    if (n <= 0 || loInt > hiInt) {
      return true;
    }
    final long base = h.alpForBase[t];
    final int width = h.alpBitWidth[t] & 0xFF;
    final long maxPacked = width == 0 ? 0L : BitUnpackSimd.maskFor(width);
    if (hiInt < base) {
      return true;
    }
    final long domainHi = base > Long.MAX_VALUE - maxPacked ? Long.MAX_VALUE : base + maxPacked;
    if (loInt > domainHi) {
      return true;
    }
    final long plo = loInt <= base ? 0L : loInt - base;
    final long phi = hiInt >= domainHi ? maxPacked : hiInt - base;
    selectPackedRange(payload, h.tagDataOffset[t], width, n, plo, phi, out);
    return true;
  }

  /** ALP: the decimal transform is monotonic, so the bound maps into packed space exactly. */
  private static boolean selectAlp(final MemorySegment payload, final DoubleRegion.Header h,
      final int t, final int n, final double dlo, final double dhi, final long[] out) {
    final int e = h.alpE[t] & 0xFF;
    final int f = h.alpF[t] & 0xFF;
    final long base = h.alpForBase[t];
    final int width = h.alpBitWidth[t] & 0xFF;
    final long maxPacked = width == 0 ? 0L : BitUnpackSimd.maskFor(width);
    final long domainLo = base - 1;
    final long domainHi = base + maxPacked + 1;
    final long ilo = fixLowerBound(dlo, e, f, domainLo, domainHi);
    final long ihi = fixUpperBound(dhi, e, f, domainLo, domainHi);
    if (ilo == BOUND_UNRESOLVED || ihi == BOUND_UNRESOLVED) {
      selectByDecode(payload, h, t, n, dlo, dhi, out);  // pathological exponents; still correct
      return true;
    }
    if (ilo <= ihi && ihi >= base && ilo <= base + maxPacked) {
      final long plo = Math.max(0L, ilo - base);
      final long phi = Math.min(maxPacked, ihi - base);
      selectPackedRange(payload, h.tagDataOffset[t], width, n, plo, phi, out);
    }
    // Exceptions are stored verbatim and decode outside the packed domain, so each is judged on its
    // own value — and its packed placeholder must be cleared when it does not match.
    final int exceptions = h.alpExceptionCount[t];
    for (int k = 0; k < exceptions; k++) {
      final int pos = payload.get(LE.SHORT, h.alpExceptionOffset[t] + (long) k * Short.BYTES) & 0xFFFF;
      if (pos >= n) {
        continue;
      }
      final double v = DoubleRegion.decodeValueAt(payload, h, t, pos);
      final int word = pos >>> 6;
      final long bit = 1L << (pos & 63);
      if (v >= dlo && v <= dhi) {
        out[word] |= bit;
      } else {
        out[word] &= ~bit;
      }
    }
    return true;
  }

  /** Verbatim doubles: compare directly, no encoded domain to exploit. */
  private static void selectPlain(final MemorySegment payload, final int offset, final int n,
      final double dlo, final double dhi, final long[] out) {
    for (int i = 0; i < n; i++) {
      final double v = Double.longBitsToDouble(payload.get(LE.LONG, offset + (long) i * Double.BYTES));
      if (v >= dlo && v <= dhi) {
        ColumnLoad.setBit(out, i);
      }
    }
  }

  /** Last resort for a tag whose transform is not order-preserving: decode this tag alone. */
  private static void selectByDecode(final MemorySegment payload, final DoubleRegion.Header h,
      final int t, final int n, final double dlo, final double dhi, final long[] out) {
    for (int i = 0; i < n; i++) {
      final double v = DoubleRegion.decodeValueAt(payload, h, t, i);
      if (v >= dlo && v <= dhi) {
        ColumnLoad.setBit(out, i);
      }
    }
  }

  /** The selection twin of {@link #countPackedRange}, sharing its vector shape. */
  private static void selectPackedRange(final MemorySegment payload, final int packedOffset,
      final int width, final int n, final long plo, final long phi, final long[] out) {
    if (plo > phi) {
      return;
    }
    if (width == 0) {
      if (plo <= 0L && 0L <= phi) {
        final int words = ColumnLoad.bitmapWords(n);
        for (int w = 0; w < words; w++) {
          out[w] = -1L;
        }
        final int tail = n & 63;
        if (tail != 0) {
          out[words - 1] = (1L << tail) - 1L;
        }
      }
      return;
    }
    final BitUnpackSimd.Plan plan = BitUnpackSimd.planFor(width);
    int i = 0;
    if (plan != null && BitUnpackSimd.vectorProfitable(n)) {
      final LongVector loV = LongVector.broadcast(ColumnLoad.LONG_SPECIES, plo);
      final LongVector spanV = LongVector.broadcast(ColumnLoad.LONG_SPECIES, phi - plo);
      final int lastGroup =
          BitUnpackSimd.lastVectorGroupStart(payload.byteSize(), packedOffset, width);
      for (; i <= n - ColumnLoad.LANES && i <= lastGroup; i += ColumnLoad.LANES) {
        ColumnLoad.setWindow(out, i, plan.unpack(payload, packedOffset, i)
                                         .sub(loV)
                                         .compare(VectorOperators.ULE, spanV)
                                         .toLong());
      }
    }
    final long mask = BitUnpackSimd.maskFor(width);
    for (; i < n; i++) {
      final long v = BitUnpackSimd.decodeAt(payload, packedOffset, width, mask, i);
      if (v >= plo && v <= phi) {
        ColumnLoad.setBit(out, i);
      }
    }
  }

  private static long countPackedRange(final MemorySegment payload, final int packedOffset,
      final int width, final int n, final long plo, final long phi,
      final long @Nullable [] liveBits) {
    if (plo > phi) {
      return 0L;
    }
    if (width == 0) {
      // Every entry packs as zero; the bound includes them all or none of them.
      if (!(plo <= 0L && 0L <= phi)) {
        return 0L;
      }
      return liveBits == null ? n : ColumnLoad.countSetPrefix(liveBits, n);
    }
    final BitUnpackSimd.Plan plan = BitUnpackSimd.planFor(width);
    long count = 0;
    int i = 0;
    if (plan != null && BitUnpackSimd.vectorProfitable(n)) {
      final long progress =
          countPackedRangeVector(payload, packedOffset, width, n, plo, phi, liveBits, plan);
      i = (int) (progress >>> 32);
      count = (int) progress;
    }
    final long mask = BitUnpackSimd.maskFor(width);
    for (; i < n; i++) {
      if (!live(liveBits, i)) {
        continue;
      }
      final long v = BitUnpackSimd.decodeAt(payload, packedOffset, width, mask, i);
      if (v >= plo && v <= phi) {
        count++;
      }
    }
    return count;
  }

  /**
   * Vector body of {@link #countPackedRange}: the resume index in the high word, the count in the
   * low one. Two loop copies so the liveness mask is not re-tested per vector group.
   */
  private static long countPackedRangeVector(final MemorySegment payload, final int packedOffset,
      final int width, final int n, final long plo, final long phi,
      final long @Nullable [] liveBits, final BitUnpackSimd.Plan plan) {
    long count = 0;
    int i = 0;
    final LongVector loV = LongVector.broadcast(ColumnLoad.LONG_SPECIES, plo);
    final LongVector spanV = LongVector.broadcast(ColumnLoad.LONG_SPECIES, phi - plo);
    final int lastGroup =
        BitUnpackSimd.lastVectorGroupStart(payload.byteSize(), packedOffset, width);
    if (liveBits == null) {
      for (; i <= n - ColumnLoad.LANES && i <= lastGroup; i += ColumnLoad.LANES) {
        count += plan.unpack(payload, packedOffset, i)
                     .sub(loV)
                     .compare(VectorOperators.ULE, spanV)
                     .trueCount();
      }
    } else {
      for (; i <= n - ColumnLoad.LANES && i <= lastGroup; i += ColumnLoad.LANES) {
        count += plan.unpack(payload, packedOffset, i)
                     .sub(loV)
                     .compare(VectorOperators.ULE, spanV)
                     .and(ColumnLoad.laneMask(ColumnLoad.liveWindow(liveBits, i)))
                     .trueCount();
      }
    }
    return ((long) i << 32) | count;
  }

  /** Sentinel: the fix-up walk exhausted its budget and the caller must count scalar. */
  private static final long BOUND_UNRESOLVED = Long.MIN_VALUE;

  /** Walk budget for the boundary fix-up; candidates are off by rounding, not by distance. */
  private static final int MAX_FIX_STEPS = 64;

  /**
   * Smallest integer in {@code [min, max+1]} whose decode is {@code >= dlo}, established by
   * decoding rather than assumed from the division. {@code max + 1} means "past the domain":
   * nothing encodable reaches the bound.
   */
  private static long fixLowerBound(final double dlo, final int e, final int f, final long min,
      final long max) {
    final double scaled = dlo * DoubleRegion.EXP10[e] / DoubleRegion.EXP10[f];
    long i;
    if (!(scaled > min)) {
      i = min;          // covers -infinity and underflow: everything in the domain qualifies
    } else if (!(scaled < max)) {
      i = max;
    } else {
      i = (long) Math.ceil(scaled);
    }
    int steps = 0;
    while (i > min && DoubleRegion.alpDecode(i - 1, e, f) >= dlo) {
      if (++steps > MAX_FIX_STEPS) {
        return BOUND_UNRESOLVED;
      }
      i--;
    }
    while (i <= max && DoubleRegion.alpDecode(i, e, f) < dlo) {
      if (++steps > MAX_FIX_STEPS) {
        return BOUND_UNRESOLVED;
      }
      i++;
    }
    return i;
  }

  /** Largest integer in {@code [min-1, max]} whose decode is {@code <= dhi}; {@code min - 1} means
   *  nothing encodable stays within the bound. */
  private static long fixUpperBound(final double dhi, final int e, final int f, final long min,
      final long max) {
    final double scaled = dhi * DoubleRegion.EXP10[e] / DoubleRegion.EXP10[f];
    long i;
    if (!(scaled < max)) {
      i = max;          // covers +infinity and overflow
    } else if (!(scaled > min)) {
      i = min;
    } else {
      i = (long) Math.floor(scaled);
    }
    int steps = 0;
    while (i < max && DoubleRegion.alpDecode(i + 1, e, f) <= dhi) {
      if (++steps > MAX_FIX_STEPS) {
        return BOUND_UNRESOLVED;
      }
      i++;
    }
    while (i >= min && DoubleRegion.alpDecode(i, e, f) > dhi) {
      if (++steps > MAX_FIX_STEPS) {
        return BOUND_UNRESOLVED;
      }
      i--;
    }
    return i;
  }
}
