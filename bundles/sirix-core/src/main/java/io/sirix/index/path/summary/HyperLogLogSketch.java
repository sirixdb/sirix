package io.sirix.index.path.summary;

import net.openhft.hashing.LongHashFunction;

import java.util.Arrays;
import java.util.Objects;

/**
 * Minimal fixed-precision HyperLogLog cardinality sketch for per-path distinct-value
 * estimation in the PathSummary.
 *
 * <p>Precision p = 11 → m = 2048 registers, each held in a full byte (only the low 6
 * bits are used). Total footprint: 2048 bytes. Theoretical standard error ≈ 1.04/√m
 * ≈ 2.3% for large cardinalities; small cardinalities use linear counting and are
 * near-exact.
 *
 * <p>Hash: xxHash3 via {@link LongHashFunction#xx3()} — already a project dependency.
 * HLL registers are monotonic (max-combined), so the sketch can union with another
 * but cannot subtract — deletes drift over time. Acceptable for analytical
 * read-heavy workloads; for high-churn cases a periodic rebuild from the true value
 * set is the intended fix.
 *
 * <p>Not thread-safe on write. External synchronization required when adding from
 * multiple threads; reads are safe once the sketch is frozen.
 */
public final class HyperLogLogSketch {

  /** Precision parameter: 2^P = number of registers. */
  public static final int P = 11;

  /** Number of registers: 2^P. */
  public static final int M = 1 << P;

  /** Total serialized size in bytes. */
  public static final int BYTES = M;

  private static final LongHashFunction HASH = LongHashFunction.xx3();

  /**
   * Bias-correction constant for m ≥ 128 (Flajolet-Fusy-Gandouet-Meunier 2007).
   */
  private static final double ALPHA = 0.7213 / (1.0 + 1.079 / M);

  /** One byte per register. Low 6 bits hold the register value (0-63). */
  private final byte[] registers;

  public HyperLogLogSketch() {
    this.registers = new byte[M];
  }

  private HyperLogLogSketch(byte[] regs) {
    this.registers = regs;
  }

  /** Add a long value. Zero-allocation on the hot path. */
  public void add(long value) {
    update(HASH.hashLong(value));
  }

  /** Add a byte range (e.g., UTF-8 bytes of a string). Zero-allocation. */
  public void add(byte[] bytes, int offset, int length) {
    Objects.requireNonNull(bytes);
    update(HASH.hashBytes(bytes, offset, length));
  }

  public void add(byte[] bytes) {
    add(bytes, 0, bytes.length);
  }

  private void update(long hash) {
    final int idx = (int) (hash & (M - 1));
    final long w = hash >>> P;
    // rho = position of leftmost 1-bit in w (1-indexed).
    // Long.numberOfLeadingZeros(w) = 64 for w=0; w has P leading zeros by
    // construction, so we subtract P.
    final int rho = (w == 0L) ? (64 - P + 1) : (Long.numberOfLeadingZeros(w) - P + 1);
    if ((registers[idx] & 0xFF) < rho) {
      registers[idx] = (byte) rho;
    }
  }

  /** Register-wise max. Semantically a multiset union. */
  public void union(HyperLogLogSketch other) {
    final byte[] o = other.registers;
    for (int i = 0; i < M; i++) {
      if ((o[i] & 0xFF) > (registers[i] & 0xFF)) {
        registers[i] = o[i];
      }
    }
  }

  /** Estimate the number of distinct values added to this sketch. */
  public long estimate() {
    double sum = 0.0;
    int zeroCount = 0;
    for (byte register : registers) {
      final int r = register & 0xFF;
      sum += Math.scalb(1.0, -r); // 2^-r
      if (r == 0) {
        zeroCount++;
      }
    }
    final double rawEstimate = ALPHA * M * M / sum;

    // Small-range correction: linear counting when there are still empty registers
    // and the raw estimate would be noisy.
    if (rawEstimate <= 2.5 * M && zeroCount != 0) {
      return Math.round(M * Math.log((double) M / zeroCount));
    }

    // Large-range correction is only needed for 32-bit hashes. xxHash3 is 64-bit,
    // so the raw (bias-corrected) estimate is sufficient.
    return Math.round(rawEstimate);
  }

  /** Serialize to a freshly allocated byte array of size {@link #BYTES}. */
  public byte[] serialize() {
    return Arrays.copyOf(registers, M);
  }

  /** Deserialize from the byte array produced by {@link #serialize()}. */
  public static HyperLogLogSketch deserialize(byte[] bytes) {
    if (bytes.length != M) {
      throw new IllegalArgumentException(
          "Expected " + M + " bytes, got " + bytes.length);
    }
    return new HyperLogLogSketch(Arrays.copyOf(bytes, M));
  }

  /** Package-private accessor for tests. */
  byte[] rawRegisters() {
    return registers;
  }
}
