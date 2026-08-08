/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page.pax;

import io.sirix.node.LE;
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorShuffle;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * Vectorized unpack of a fixed-width bit-packed column, straight out of the encoded form.
 *
 * <p>This is the primitive every bit-packed column kernel in the PAX layout stands on:
 * {@link NumberRegion}'s {@code BIT_PACKED} / {@code COMPACT_ZM} values, {@link StringRegion}'s
 * dict-id column, and the residual stream behind {@link NumberRegionDeltaSimd}. It turns
 * {@link #lanes()} consecutive packed values into a {@link LongVector} without materializing
 * anything to memory first, and without a per-lane scalar load.
 *
 * <h2>What it replaces</h2>
 *
 * <p>The obvious way to feed a vector from a bit-packed column is to decode each lane with the
 * scalar unpacker and stage the results in a {@code long[]}. That was this codebase's first cut,
 * and it is only half a kernel: the compare and the popcount vectorize, but the unpack — the part
 * that actually touches memory — stays a scalar loop of one load, one shift and one mask per
 * value. On an eight-lane machine seven eighths of the work never left the scalar unit. BtrBlocks
 * and FastLanes make the same point from the other side: bit-unpacking <em>is</em> the hot loop of
 * a compressed scan, and it is worth vectorizing on its own terms.
 *
 * <h2>How a lane finds its bits</h2>
 *
 * <p>Value {@code i} of a {@code w}-bit column starts at bit {@code i * w}. Take the group of
 * {@code L} values starting at {@code i}, let {@code baseWord} be the 64-bit word holding the
 * group's first bit, and let {@code phase = (i * w) mod 64} be where in that word it starts. Lane
 * {@code j} then reads its value out of word {@code baseWord + (phase + j*w)/64}, shifted right by
 * {@code (phase + j*w) mod 64}, possibly continuing into the following word.
 *
 * <p>Two facts make that addressable with vector instructions rather than a gather:
 *
 * <ul>
 *   <li>For {@code w <= }{@value #MAX_BIT_WIDTH} the highest word any lane needs is
 *       {@code baseWord + L}. So two ordinary contiguous vector loads — one at {@code baseWord},
 *       one at {@code baseWord + 1} — between them hold every word the group can want, with lane
 *       {@code j}'s <em>low</em> word at index {@code (phase + j*w)/64} of the first load and its
 *       <em>high</em> word at the same index of the second. One shuffle serves both.</li>
 *   <li>{@code phase} takes at most 64 values, and it alone determines the shuffle and the two
 *       shift vectors, so they are computed once per width and looked up rather than recomputed in
 *       the loop.</li>
 * </ul>
 *
 * <p>What remains per group is two loads, two permutes, three variable shifts, an OR and an AND —
 * nine vector instructions for {@code L} values against {@code 3L} scalar ones. The shifts are
 * per-lane variable ({@code vpsrlvq} / {@code vpsllvq}), the permutes are {@code vpermq}, and both
 * measure free next to the loads. Nothing spills to memory and nothing allocates.
 *
 * <h2>Straddles are not a special case</h2>
 *
 * <p>A value crossing a word boundary is assembled as {@code (lo >>> s) | (hi << (64 - s))}. A
 * value that does not cross one is assembled by the exact same expression: the {@code hi} term
 * lands entirely at bit positions {@code >= 64 - s}, and when the value does not straddle,
 * {@code 64 - s >= w}, so the final mask deletes it. No branch, no second code path — which is the
 * point, because a per-value straddle branch on a vector path costs more than the straddle.
 *
 * <p>The shift by {@code 64 - s} is spelled {@code (hi << 1) << (63 - s)} because Java takes shift
 * counts modulo 64, so a literal {@code << 64} would be an identity rather than a wipe.
 *
 * <h2>Width bound</h2>
 *
 * <p>Widths run 1..{@value #MAX_BIT_WIDTH}. Above that a group can reach past {@code baseWord + L}
 * and the two-load window stops covering it, so callers fall back to the scalar unpacker. The
 * bound is not a practical restriction: a column whose residuals need 57+ bits is one the encoder
 * leaves in {@code PLAIN_LONG}, and dict ids are capped at 32 bits.
 */
public final class BitUnpackSimd {

  private static final int LANES = ColumnLoad.LANES;

  /**
   * Widest column the two-load window covers. Derived rather than asserted: the last lane's low
   * word index is {@code (63 + (L-1)*w) / 64}, which stays {@code <= L-1} exactly while
   * {@code w <= 56}, for every lane count a JVM reports.
   */
  public static final int MAX_BIT_WIDTH = 56;

  /** Number of distinct bit offsets a group start can have within its first word. */
  private static final int PHASES = 64;

  /**
   * Smallest range worth entering the vector path for. Below it the plan lookup, the broadcast
   * constants and the loop setup cost more than decoding the values one at a time — and a point
   * lookup or a single-row answer is made of exactly such ranges, so this threshold is what keeps
   * a scan kernel from taxing a query that never wanted one.
   */
  public static final int MIN_VECTOR_RANGE = 32;

  /**
   * Per-width plans, built on first touch of that width.
   *
   * <p>Lazily rather than in a static initializer, because eager construction is 56 widths x 64
   * phases x 3 vector constants of work that a cold JVM does interpreted, on the critical path of
   * the first query, to build tables for 55 widths the page does not use. A page touches one or
   * two widths; it should pay for one or two.
   *
   * <p>The array is written racily and that is intentional: {@link Plan} is immutable with final
   * fields, so two threads that build the same width produce interchangeable objects and
   * publishing either is correct. Making the element read {@code volatile} would cost the scan
   * loop a fence per lookup for no benefit — the {@code final} fields inside {@code Plan} already
   * carry the freeze that safe publication needs.
   */
  private static final Plan[] PLANS = new Plan[MAX_BIT_WIDTH + 1];

  private BitUnpackSimd() {
  }

  /** Whether the vector path serves this width. */
  public static boolean supports(final int bitWidth) {
    return bitWidth >= 1 && bitWidth <= MAX_BIT_WIDTH;
  }

  /** Lane count of the unpack, i.e. values produced per {@link Plan#unpack} call. */
  public static int lanes() {
    return LANES;
  }

  /**
   * Plan for {@code bitWidth}, or {@code null} when the width is out of range.
   *
   * <p>Hoist this out of the scan loop: the whole point of the phase tables is that the loop does
   * an array read instead of a recomputation.
   */
  public static Plan planFor(final int bitWidth) {
    if (!supports(bitWidth)) {
      return null;
    }
    final Plan cached = PLANS[bitWidth];
    if (cached != null) {
      return cached;
    }
    final Plan built = new Plan(bitWidth);
    PLANS[bitWidth] = built;
    return built;
  }

  /**
   * Values this class lets the kernels decode scalar-wise before it starts offering the vector
   * path. See {@link #vectorProfitable}. Overridable for benchmarks that want to measure the vector
   * path directly; {@code 0} disables the warmup hold entirely.
   */
  private static final int WARMUP_VALUES =
      Integer.getInteger("sirix.simd.warmupValues", 1 << 19);

  /**
   * How much of the warmup budget is left.
   *
   * <p>Deliberately a plain {@code int} with racy updates. Its only job is to be roughly right:
   * a lost increment delays the switch to vector code by one range, which is unobservable, and
   * making it atomic would put a contended write on the hot path of every scan to protect a
   * heuristic.
   */
  private static int warmupRemaining = WARMUP_VALUES;

  /**
   * Whether a range of {@code n} values should actually go through the vector path.
   *
   * <p>Two separate reasons to say no, and both are about latency rather than throughput.
   *
   * <p><b>The range is too short.</b> Point lookups, {@code LIMIT 1} and the first page of a cursor
   * walk reach the same kernels a full scan does. Below {@link #MIN_VECTOR_RANGE} the broadcasts
   * and the plan lookup cost more than just decoding the values.
   *
   * <p><b>The JVM has not compiled anything yet.</b> This is the larger effect and the less obvious
   * one: vector code is <em>catastrophically</em> slow in the interpreter. Measured on a cold JVM,
   * the first scan of 4096 bit-packed values costs 125 ms through the vector path and 35 ms through
   * the scalar one — the SIMD kernels are 3.6x <em>worse</em> until C2 compiles them, on top of the
   * ~46 ms the Vector API spends bootstrapping itself on its first operation. A process answering
   * one query and exiting, or a server answering its first, would pay that in full.
   *
   * <p>So the kernels decode scalar-wise until they have seen enough values that the JIT has
   * certainly compiled them, then switch over for good. The budget is spent in whatever units of
   * work arrive; a long-running server crosses it during its first analytical query and never
   * thinks about it again, while a short-lived one never pays the interpreted-vector penalty at all.
   */
  public static boolean vectorProfitable(final int n) {
    if (n < MIN_VECTOR_RANGE) {
      return false;
    }
    final int remaining = warmupRemaining;
    if (remaining <= 0) {
      return true;
    }
    warmupRemaining = remaining - n;
    return false;
  }

  /**
   * Test hook: set how many more values the kernels will decode scalar-wise before switching to the
   * vector path. Pass {@code 0} to take the vector path immediately.
   *
   * <p>Exists because the warmup hold otherwise silently decides which code an equivalence test
   * covers: a test suite that never spends the budget would assert only against the scalar loops
   * and leave the vector ones unexercised, which is precisely backwards.
   */
  public static void setWarmupRemainingForTesting(final int values) {
    warmupRemaining = values;
  }

  /** Test hook: restore the default warmup budget. */
  public static void resetWarmupForTesting() {
    warmupRemaining = WARMUP_VALUES;
  }

  /**
   * Highest value index at which a vector group may start without the two-word window reading past
   * the end of {@code payload}.
   *
   * <p>Returns {@code -1} when the column is too short for even one group, in which case the caller
   * runs entirely on the scalar tail. Callers bound their vector loop by this <em>and</em> by
   * {@code end - LANES}: this bound is about the buffer, the other about the range.
   *
   * @param payloadLength length of the buffer holding the column
   * @param valuesByteOffset byte offset at which the packed values begin
   * @param bitWidth bits per value
   */
  public static int lastVectorGroupStart(final long payloadLength, final long valuesByteOffset,
      final int bitWidth) {
    if (!supports(bitWidth)) {
      return -1;
    }
    // A group starting at value i loads bytes [base, base + 2*BYTES_PER_VECTOR) where
    // base = valuesByteOffset + baseWord*8 — two vector loads, the second offset by one word.
    final long availableForBase =
        payloadLength - valuesByteOffset - (long) ColumnLoad.BYTES_PER_VECTOR - Long.BYTES;
    if (availableForBase < 0) {
      return -1;
    }
    final long maxBaseWord = availableForBase >>> 3;
    // Any start bit whose word index is <= maxBaseWord is safe, so take the last bit of that word.
    final long maxStartBit = (maxBaseWord << 6) + 63L;
    return (int) Math.min(Integer.MAX_VALUE, maxStartBit / bitWidth);
  }

  /** Value mask for a width, i.e. {@code w} low bits set. */
  public static long maskFor(final int bitWidth) {
    return bitWidth >= 64 ? ~0L : (1L << bitWidth) - 1L;
  }

  /**
   * Scalar unpack of one value, for loop tails and for widths the vector path declines.
   *
   * <p>Deliberately shares its word load with nothing else: two unpackers that disagree about a
   * tail byte is a class of bug that only shows up on the last few values of a page, which is
   * exactly where nobody looks.
   */
  public static long decodeAt(final MemorySegment payload, final long valuesByteOffset,
      final int bitWidth, final long mask, final int index) {
    final long bitOff = (long) index * bitWidth;
    final long byteOff = valuesByteOffset + (bitOff >>> 3);
    final int shift = (int) (bitOff & 7L);
    final long low = readWordSafe(payload, byteOff);
    long value = low >>> shift;
    // A byte-aligned load leaves 64 - shift usable bits, so anything wider straddles into the next
    // word. This matters precisely for the widths the vector path declines — those above
    // MAX_BIT_WIDTH, where this method is the ONLY decoder — and a single-word read silently
    // returned the value with its top bits zeroed for widths past 57.
    if (bitWidth > 64 - shift) {
      value |= readWordSafe(payload, byteOff + Long.BYTES) << (64 - shift);
    }
    return value & mask;
  }

  /**
   * Little-endian 64-bit load that tolerates running into the end of the buffer, composing the
   * final partial word byte by byte and zeroing what lies past it.
   *
   * <p>Safe for the caller because the bits of the value being decoded always lie inside the
   * buffer; only the padding above them can be missing, and the caller masks that away.
   */
  public static long readWordSafe(final MemorySegment payload, final long byteOff) {
    final long size = payload.byteSize();
    if (byteOff + Long.BYTES <= size) {
      return payload.get(LE.LONG, byteOff);
    }
    long word = 0L;
    final long remaining = Math.max(0L, size - byteOff);
    for (long k = 0; k < remaining; k++) {
      word |= (payload.get(ValueLayout.JAVA_BYTE, byteOff + k) & 0xFFL) << (k << 3);
    }
    return word;
  }

  /**
   * Per-width unpack tables: for each of the 64 possible group phases, the lane permutation that
   * puts each lane's containing word into its own lane, and the two shift vectors that slide the
   * value down to bit zero.
   *
   * <p>Immutable after construction, so the scan loop reads the tables with no synchronization.
   */
  public static final class Plan {

    private final int bitWidth;
    private final long mask;
    private final VectorShuffle<Long>[] wordSelect;
    private final VectorShuffle<Long>[] carrySelect;
    private final LongVector[] downShift;
    private final LongVector[] carryShift;
    /**
     * Whether one vector load covers the whole group, carry words included.
     *
     * <p>The carry word a lane may need is one past its low word, so a single load suffices exactly
     * when the highest low-word index stays below the last lane: then the carry is still inside the
     * register that was already loaded, and a second shuffle of it is cheaper than a second load.
     * On a 512-bit machine that covers widths up to 54, which is nearly all of them; on a 256-bit
     * machine it covers up to 21 and the wider columns take the two-load path.
     *
     * <p>The branch it guards is per-plan, so it resolves the same way on every iteration of a
     * given scan and costs nothing after the first.
     */
    private final boolean singleLoad;

    @SuppressWarnings("unchecked")
    private Plan(final int bitWidth) {
      this.bitWidth = bitWidth;
      this.mask = maskFor(bitWidth);
      this.wordSelect = new VectorShuffle[PHASES];
      this.carrySelect = new VectorShuffle[PHASES];
      this.downShift = new LongVector[PHASES];
      this.carryShift = new LongVector[PHASES];
      // Worst case over all phases is phase 63, the latest a group can start inside its word.
      this.singleLoad = ((63 + (LANES - 1) * bitWidth) >>> 6) + 1 <= LANES - 1;

      final int[] words = new int[LANES];
      final int[] carryWords = new int[LANES];
      final long[] shifts = new long[LANES];
      final long[] carries = new long[LANES];
      for (int phase = 0; phase < PHASES; phase++) {
        for (int lane = 0; lane < LANES; lane++) {
          final int bitOff = phase + lane * bitWidth;
          words[lane] = bitOff >>> 6;
          // Under singleLoad the carry comes from the same register, so it is indexed one word up;
          // otherwise it comes from a register already shifted by one word, at the same index.
          carryWords[lane] = singleLoad ? (bitOff >>> 6) + 1 : bitOff >>> 6;
          shifts[lane] = bitOff & 63;
          // 63 - s rather than 64 - s: the caller pre-shifts by one, because Java reduces shift
          // counts modulo 64 and a literal 64 would be a no-op instead of a wipe.
          carries[lane] = 63 - (bitOff & 63);
        }
        wordSelect[phase] = VectorShuffle.fromArray(ColumnLoad.LONG_SPECIES, words, 0);
        carrySelect[phase] = VectorShuffle.fromArray(ColumnLoad.LONG_SPECIES, carryWords, 0);
        downShift[phase] = LongVector.fromArray(ColumnLoad.LONG_SPECIES, shifts, 0);
        carryShift[phase] = LongVector.fromArray(ColumnLoad.LONG_SPECIES, carries, 0);
      }
    }

    /** Bits per value this plan unpacks. */
    public int bitWidth() {
      return bitWidth;
    }

    /** Mask of {@link #bitWidth()} low bits. */
    public long mask() {
      return mask;
    }

    /**
     * Unpack the {@link BitUnpackSimd#lanes()} values starting at {@code index}, lane {@code j}
     * receiving value {@code index + j} — order preserved, which is what lets masked kernels line
     * a liveness bitmap up with lanes and what lets a selection kernel emit indices.
     *
     * <p>The caller must have checked {@code index <= }
     * {@link BitUnpackSimd#lastVectorGroupStart}; this does not re-check, because the check is
     * loop-invariant and belongs in the loop bound.
     *
     * @param payload buffer holding the column
     * @param valuesByteOffset byte offset at which the packed values begin
     * @param index index of the first value in the group
     */
    public LongVector unpack(final MemorySegment payload, final long valuesByteOffset,
        final int index) {
      final long startBit = (long) index * bitWidth;
      final int phase = (int) (startBit & 63L);
      final long byteBase = valuesByteOffset + ((startBit >>> 6) << 3);

      final LongVector w0 = ColumnLoad.loadWords(payload, byteBase);
      final LongVector lo = w0.rearrange(wordSelect[phase]);
      final LongVector hi = singleLoad
          ? w0.rearrange(carrySelect[phase])
          : ColumnLoad.loadWords(payload, byteBase + Long.BYTES).rearrange(carrySelect[phase]);

      // (lo >>> s) | (hi << (64 - s)), then mask. When the value does not straddle a word the
      // carry term lands above bit w and the mask deletes it, so there is no straddle branch.
      return lo.lanewise(VectorOperators.LSHR, downShift[phase])
               .or(hi.lanewise(VectorOperators.LSHL, 1L)
                     .lanewise(VectorOperators.LSHL, carryShift[phase]))
               .and(mask);
    }

    /** Scalar unpack of one value under this plan's width, for loop tails. */
    public long decodeAt(final MemorySegment payload, final long valuesByteOffset,
        final int index) {
      return BitUnpackSimd.decodeAt(payload, valuesByteOffset, bitWidth, mask, index);
    }
  }
}
