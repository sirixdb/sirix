/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page.pax;

import io.sirix.node.LE;
import jdk.incubator.vector.DoubleVector;
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
    if (!(dlo <= dhi)) {
      return 0L;  // an empty or NaN bound matches nothing
    }
    final int n = h.tagCount[t];
    if (n <= 0) {
      return 0L;
    }
    return switch (h.tagEnc[t]) {
      case DoubleRegion.ENC_ALP -> countAlp(payload, h, t, n, dlo, dhi);
      case DoubleRegion.ENC_ALP_RD -> countAlpRd(payload, h, t, n, dlo, dhi);
      default -> countPlain(payload, h.tagDataOffset[t], n, dlo, dhi);
    };
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
      final int t, final int n, final double dlo, final double dhi) {
    final int rw = h.rdRightWidth[t] & 0xFF;
    final int cw = h.rdCodeWidth[t] & 0xFF;
    final int dictSize = h.rdDictSize[t] & 0xFF;
    long count = 0;
    int i = 0;
    final BitUnpackSimd.Plan rightPlan = BitUnpackSimd.planFor(rw);
    final BitUnpackSimd.Plan codePlan = cw == 0 ? null : BitUnpackSimd.planFor(cw);
    if (rightPlan != null && (cw == 0 || codePlan != null) && BitUnpackSimd.vectorProfitable(n)) {
      // The dictionary spread over as many registers as it needs, each padded with entry 0. A
      // dictionary within one register gathers with a single selectFrom; a wider one runs a blend
      // cascade — mask in the lanes whose code falls in each register's index window. The AND with
      // LANES-1 keeps every lane a VALID index for selectFrom (which validates all lanes, used or
      // not); the window mask then decides which register's answer survives.
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
        count += v.compare(VectorOperators.GE, loV)
                  .and(v.compare(VectorOperators.LE, hiV))
                  .trueCount();
      }
    }
    for (; i < n; i++) {
      // The scalar tail decodes THROUGH the dictionary, deliberately ignoring the exception list:
      // the correction below assumes every slot was judged by its packed code, tail included.
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
    final int exceptions = h.alpExceptionCount[t];
    if (exceptions > 0) {
      final long posBase = h.alpExceptionOffset[t];
      final long leftBase = posBase + (long) exceptions * Short.BYTES;
      final long dict0 = payload.get(LE.LONG, h.rdDictOffset[t]);
      for (int x = 0; x < exceptions; x++) {
        final int pos = payload.get(LE.SHORT, posBase + (long) x * Short.BYTES) & 0xFFFF;
        final long right = BitUnpackSimd.decodeAt(payload, h.tagDataOffset[t], rw,
                                                  BitUnpackSimd.maskFor(rw), pos);
        final double judged = Double.longBitsToDouble((dict0 << rw) | right);
        if (judged >= dlo && judged <= dhi) {
          count--;
        }
        final long left =
            payload.get(LE.SHORT, leftBase + (long) x * Short.BYTES) & 0xFFFFL;
        final double actual = Double.longBitsToDouble((left << rw) | right);
        if (actual >= dlo && actual <= dhi) {
          count++;
        }
      }
    }
    return count;
  }

  // ─────────────────────────────────────────────────────────────── PLAIN

  private static long countPlain(final MemorySegment payload, final int valuesOffset, final int n,
      final double dlo, final double dhi) {
    long count = 0;
    int i = 0;
    if (BitUnpackSimd.vectorProfitable(n)) {
      final DoubleVector loV = DoubleVector.broadcast(SPECIES, dlo);
      final DoubleVector hiV = DoubleVector.broadcast(SPECIES, dhi);
      for (; i <= n - LANES; i += LANES) {
        final DoubleVector v = DoubleVector.fromMemorySegment(SPECIES, payload,
            valuesOffset + (long) i * Double.BYTES, ByteOrder.LITTLE_ENDIAN);
        count += v.compare(VectorOperators.GE, loV)
                  .and(v.compare(VectorOperators.LE, hiV))
                  .trueCount();
      }
    }
    for (; i < n; i++) {
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
      final int t, final int n, final double dlo, final double dhi) {
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
    long count;
    if (ilo == BOUND_UNRESOLVED || ihi == BOUND_UNRESOLVED) {
      // The fix-up walk exhausted its budget — pathological exponents. Decode scalar; correct is
      // the contract, fast is the common case.
      count = 0;
      for (int i = 0; i < n; i++) {
        final double v = DoubleRegion.decodeValueAt(payload, h, t, i);
        if (v >= dlo && v <= dhi) {
          count++;
        }
      }
      return count;
    }
    final long plo = Math.max(0L, ilo - base);
    final long phi = Math.min(maxPacked, ihi - base);
    final boolean encodedCanMatch = ilo <= ihi && ihi >= base && ilo <= base + maxPacked;

    count = 0;
    if (encodedCanMatch) {
      count = countPackedRange(payload, h.tagDataOffset[t], width, n, plo, phi);
    }

    // ---- exception correction, scalar and proportional to the exception count ----
    final int exceptions = h.alpExceptionCount[t];
    if (exceptions > 0) {
      // The placeholder packs as zero, i.e. decodes as the FOR base — its verdict under the
      // integer bound is one fixed boolean for the whole tag.
      final boolean placeholderCounted = encodedCanMatch && plo <= 0L && 0L <= phi;
      final long posBase = h.alpExceptionOffset[t];
      final long rawBase = posBase + (long) exceptions * Short.BYTES;
      for (int x = 0; x < exceptions; x++) {
        if (placeholderCounted) {
          count--;
        }
        final double raw =
            Double.longBitsToDouble(payload.get(LE.LONG, rawBase + (long) x * Long.BYTES));
        if (raw >= dlo && raw <= dhi) {
          count++;
        }
      }
    }
    return count;
  }

  /**
   * Count packed entries in {@code [plo, phi]} — the same unsigned-span trick the long column's
   * kernels use, over {@link BitUnpackSimd}'s unpack.
   */
  private static long countPackedRange(final MemorySegment payload, final int packedOffset,
      final int width, final int n, final long plo, final long phi) {
    if (plo > phi) {
      return 0L;
    }
    if (width == 0) {
      // Every entry packs as zero; the bound already includes it or excludes it wholesale.
      return plo <= 0L && 0L <= phi ? n : 0L;
    }
    final BitUnpackSimd.Plan plan = BitUnpackSimd.planFor(width);
    long count = 0;
    int i = 0;
    if (plan != null && BitUnpackSimd.vectorProfitable(n)) {
      final LongVector loV = LongVector.broadcast(ColumnLoad.LONG_SPECIES, plo);
      final LongVector spanV = LongVector.broadcast(ColumnLoad.LONG_SPECIES, phi - plo);
      final int lastGroup =
          BitUnpackSimd.lastVectorGroupStart(payload.byteSize(), packedOffset, width);
      for (; i <= n - ColumnLoad.LANES && i <= lastGroup; i += ColumnLoad.LANES) {
        count += plan.unpack(payload, packedOffset, i)
                     .sub(loV)
                     .compare(VectorOperators.ULE, spanV)
                     .trueCount();
      }
    }
    final long mask = BitUnpackSimd.maskFor(width);
    for (; i < n; i++) {
      final long v = BitUnpackSimd.decodeAt(payload, packedOffset, width, mask, i);
      if (v >= plo && v <= phi) {
        count++;
      }
    }
    return count;
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
