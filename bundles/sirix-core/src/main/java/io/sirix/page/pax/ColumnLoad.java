/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page.pax;

import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorShape;
import jdk.incubator.vector.VectorSpecies;

import java.lang.foreign.MemorySegment;
import java.nio.ByteOrder;

/**
 * The single place a column kernel pulls a vector register's worth of encoded bytes out of a
 * region payload.
 *
 * <h2>Why this is worth its own file</h2>
 *
 * <p>Every column kernel in this package is memory-bound: it loads encoded bytes, does a handful of
 * register-resident operations on them, and reduces. Which load instruction it gets is therefore
 * not an implementation detail — it is the kernel's speed. Measured on this hardware, a 64-byte
 * load costs:
 *
 * <ul>
 *   <li>0.44 ns through {@code ByteVector.fromArray} on a heap {@code byte[]} — 147 GB/s;</li>
 *   <li>0.50 ns through {@link LongVector#fromMemorySegment} on a <em>native</em> segment —
 *       128 GB/s;</li>
 *   <li>3.95 ns through {@code fromMemorySegment} on a <em>heap</em> segment — 16 GB/s.</li>
 * </ul>
 *
 * <p>The third is the one to avoid, and it is the one the kernels used to take: region payloads
 * were heap {@code byte[]} wrapped by {@code MemorySegment.ofArray}, and vector loads against a
 * heap segment do not intrinsify. The compare and the popcount vectorized; the load did not, so a
 * scan over a bit-packed column ran at roughly the speed of the scalar loop it was meant to
 * replace. Confirmed unchanged on JDK 26, so this is a constraint to design around rather than
 * wait out.
 *
 * <p>Payloads are now native segments ({@link RegionTable} allocates them off-heap), which is the
 * second row: within 13% of the array path and eight times the heap-segment one. The 13% buys
 * three things an on-heap {@code byte[]} cannot — a decompression that writes straight into the
 * payload instead of into a scratch buffer that is then copied, no per-page garbage from a scan
 * that crosses thousands of pages, and the option of scanning an uncompressed region in place in
 * the memory-mapped file rather than copying it to the heap first.
 *
 * <h2>Byte order</h2>
 *
 * <p>The on-disk format is pinned little-endian, and {@link LongVector#fromMemorySegment} takes the
 * order explicitly, so nothing here is an accident of the host CPU — the same rule
 * {@link io.sirix.node.LE} enforces for scalar access. On a little-endian host the parameter costs
 * nothing; on a big-endian one it byte-swaps the lanes.
 */
public final class ColumnLoad {

  /** Lane species every kernel in this package computes in. */
  public static final VectorSpecies<Long> LONG_SPECIES = LongVector.SPECIES_PREFERRED;

  /** Byte species of the same register width, for kernels that reason in bytes. */
  public static final VectorSpecies<Byte> BYTE_SPECIES =
      VectorSpecies.of(byte.class, VectorShape.forBitSize(LONG_SPECIES.vectorBitSize()));

  /**
   * Double species of the same register width. Same lane count as {@link #LONG_SPECIES} — both are
   * 64-bit elements — so the two share a lane index and {@link #LANE_WINDOW_MASK}.
   */
  public static final VectorSpecies<Double> DOUBLE_SPECIES =
      VectorSpecies.of(double.class, VectorShape.forBitSize(LONG_SPECIES.vectorBitSize()));

  /** Lanes per vector, i.e. values per group for a 64-bit-lane kernel. */
  public static final int LANES = LONG_SPECIES.length();

  /** Bytes consumed by one vector load. */
  public static final int BYTES_PER_VECTOR = BYTE_SPECIES.length();

  private ColumnLoad() {
  }

  /**
   * Load {@link #LANES} consecutive little-endian 64-bit words starting at {@code byteOffset}.
   *
   * <p>The caller must guarantee {@code byteOffset + }{@link #BYTES_PER_VECTOR}{@code  <=
   * payload.byteSize()}; this does not check, because the check is loop-invariant and belongs in
   * the loop bound where it costs nothing.
   */
  public static LongVector loadWords(final MemorySegment payload, final long byteOffset) {
    return LongVector.fromMemorySegment(LONG_SPECIES, payload, byteOffset, ByteOrder.LITTLE_ENDIAN);
  }

  /**
   * Whether a vector load starting at {@code byteOffset} stays inside {@code payload}.
   *
   * <p>For loop bounds, not for per-iteration use.
   */
  public static boolean canLoad(final MemorySegment payload, final long byteOffset) {
    return byteOffset >= 0 && byteOffset + BYTES_PER_VECTOR <= payload.byteSize();
  }

  /**
   * Bit {@code k} of a relative liveness bitmap; {@code null} means everything is live. The single
   * definition of the convention every masked kernel in this package shares.
   */
  public static boolean isLive(final long[] liveBits, final int k) {
    return liveBits == null || (liveBits[k >>> 6] & (1L << (k & 63))) != 0L;
  }

  /**
   * The 64-bit liveness window whose low lanes govern group {@code i}. Valid because every vector
   * group start is a multiple of the lane count, which divides 64 — a group never straddles two
   * words, so one shift positions it.
   */
  public static long liveWindow(final long[] liveBits, final int i) {
    return liveBits[i >>> 6] >>> (i & 63);
  }

  /** Low {@link #LANES} bits set; the lane window {@link #liveWindow} reads and this writes. */
  public static final long LANE_WINDOW_MASK = LANES == Long.SIZE ? -1L : (1L << LANES) - 1L;

  /**
   * Every lane mask, indexed by its own bit pattern — one table per element species.
   *
   * <p>Turning a {@link #liveWindow} into a {@link VectorMask} was the single most expensive thing
   * the masked column kernels did. {@link VectorMask#fromLong} is cheap only where the ISA has
   * mask registers: AVX-512 drops the bits into a k-register, AVX2 must materialise a full-width
   * lane mask from them. Measured in the projection fold, the identical lane adds cost
   * 0.117&nbsp;ns/row unmasked and 4.9-10.9&nbsp;ns/row once the mask was built per lane group.
   *
   * <p>{@link #LANES} lanes admit exactly {@code 1 << LANES} masks — 16 at 256-bit, 256 at 512-bit
   * — so the domain is enumerable and the per-group cost collapses to an array load. Measured
   * 0.39-0.41&nbsp;ns/row there, and flat in selectivity where the constructed form degraded with
   * it. Liveness masking is now close to free rather than the dominant term.
   */
  private static final VectorMask<Long>[] LONG_LANE_MASKS = buildLongLaneMasks();

  private static final VectorMask<Double>[] DOUBLE_LANE_MASKS = buildDoubleLaneMasks();

  @SuppressWarnings("unchecked")
  private static VectorMask<Long>[] buildLongLaneMasks() {
    final VectorMask<Long>[] masks = new VectorMask[1 << LANES];
    for (int i = 0; i < masks.length; i++) {
      masks[i] = VectorMask.fromLong(LONG_SPECIES, i);
    }
    return masks;
  }

  @SuppressWarnings("unchecked")
  private static VectorMask<Double>[] buildDoubleLaneMasks() {
    final VectorMask<Double>[] masks = new VectorMask[1 << LANES];
    for (int i = 0; i < masks.length; i++) {
      masks[i] = VectorMask.fromLong(DOUBLE_SPECIES, i);
    }
    return masks;
  }

  /**
   * The long-lane mask for the low {@link #LANES} bits of {@code bits} — a load, not a build.
   *
   * <p>Drop-in for {@code VectorMask.fromLong(LONG_SPECIES, bits)}: that already ignores bits above
   * the lane count, which is what masking by {@link #LANE_WINDOW_MASK} reproduces.
   */
  public static VectorMask<Long> laneMask(final long bits) {
    return LONG_LANE_MASKS[(int) (bits & LANE_WINDOW_MASK)];
  }

  /** {@link #laneMask} for double-element kernels; same lane count, different element species. */
  public static VectorMask<Double> laneMaskDouble(final long bits) {
    return DOUBLE_LANE_MASKS[(int) (bits & LANE_WINDOW_MASK)];
  }

  /**
   * The inverse of {@link #liveWindow}: OR one vector's worth of lane bits into a selection bitmap
   * at tag-local position {@code i}.
   *
   * <p>Never straddles a word. {@link #LANES} is a power of two dividing 64 and callers advance
   * {@code i} by {@code LANES} from zero, so {@code (i & 63) + LANES <= 64} always — the same
   * property {@link #liveWindow} silently relies on when it reads.
   */
  public static void setWindow(final long[] out, final int i, final long laneBits) {
    out[i >>> 6] |= (laneBits & LANE_WINDOW_MASK) << (i & 63);
  }

  /** Set one bit of a selection bitmap. */
  public static void setBit(final long[] out, final int k) {
    out[k >>> 6] |= 1L << (k & 63);
  }

  /** Number of {@code long} words a tag-local bitmap of {@code n} values needs. */
  public static int bitmapWords(final int n) {
    return (n + 63) >>> 6;
  }

  /** Set bits among the first {@code n} of {@code bits} — the shared prefix popcount. */
  public static long countSetPrefix(final long[] bits, final int n) {
    final int fullWords = n >>> 6;
    long count = 0;
    for (int w = 0; w < fullWords; w++) {
      count += Long.bitCount(bits[w]);
    }
    final int rest = n & 63;
    if (rest != 0) {
      count += Long.bitCount(bits[fullWords] & ((1L << rest) - 1L));
    }
    return count;
  }
}
