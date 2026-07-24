/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

/**
 * ALP (Adaptive Lossless floating-Point) encoding for {@code NUMERIC_DOUBLE} BODY streams
 * (docs/PROJECTION_INDEX_STORAGE_REDESIGN.md §2.6, §11-6), landed behind the reserved
 * width-byte escape {@link #WIDTH_ESCAPE_ALP} in the shared FOR wire form — escape bytes
 * 66..255 stay reserved and reject loudly exactly as before, and pre-ALP double stores
 * decode unchanged (their width bytes are ≤ 64).
 *
 * <h2>Scheme</h2>
 *
 * Most real-world doubles are decimals (prices, ratings, measurements). For one leaf-column
 * vector (≤ {@link ProjectionIndexRowGroupPage#MAX_ROWS} cells) pick a decimal scale pair
 * {@code (e, f)} and encode each value {@code v} as the integer
 * {@code digits = round(v · 10^e / 10^f)}; decode is {@code (digits · 10^f) / 10^e} — the
 * trailing DIVISION is load-bearing: IEEE division is correctly rounded, so integer/10^k
 * decimals verify bit-exact where a reciprocal multiply ({@code digits · 0.1}) misses
 * about half of them. Every cell is verified at encode time against that exact decode
 * expression —
 * cells that fail (true binary fractions, huge magnitudes) go verbatim into an exceptions
 * list. The digits stream is FOR bit-packed with the codec's shared primitives, which is
 * the size win: a transform-domain double pattern needs ~50–64 bits under plain FOR, while
 * {@code 12.25} at {@code (e=2, f=0)} is the integer {@code 1225}.
 *
 * <p>Cells arrive and leave in the {@link ProjectionDoubleEncoding} transform domain (the
 * raw scan form's representation); ALP works on the decoded doubles and re-encodes on
 * decode. Byte-identity of the assembled raw form holds end-to-end because every non-
 * exception cell is verified bit-exact at encode time and exceptions carry raw
 * transform-domain bits.
 *
 * <h2>Wire form (after the shared {@code long base; byte width} head)</h2>
 *
 * <pre>
 *   long  base            = 0 (reserved; the shared decoder reads it unconditionally)
 *   byte  width           = 65 (WIDTH_ESCAPE_ALP)
 *   byte  e; byte f       // decimal scale pair, 0 ≤ f ≤ e ≤ 18
 *   int   exceptionCount
 *   long  forBase; byte forWidth; packed digits[rowCount]   // plain FOR, never an escape
 *   per exception: int rowIdx; long transformBits
 * </pre>
 *
 * <p><b>Determinism</b> (required by the descriptor-hash no-op carry-forward, §3): scale
 * selection samples a fixed stride, walks candidates in a fixed order, and breaks ties
 * first-best — identical cells always produce identical bytes.
 */
final class ProjectionAlpEncoding {

  /** Reserved width-byte escape selecting the ALP branch in the shared numeric wire form. */
  static final int WIDTH_ESCAPE_ALP = 65;

  /** Exact powers of ten for the scale range 0..18 ({@code 10^i} is double-exact to 10^22). */
  private static final double[] EXP10 = new double[19];

  static {
    double d = 1.0;
    for (int i = 0; i <= 18; i++) {
      EXP10[i] = d;
      d *= 10.0;
    }
  }

  /**
   * Digits whose magnitude reaches 2^53 cannot round-trip anyway (the integer grid is
   * coarser than 1 beyond it) — bail before {@code Math.round} can misbehave.
   */
  private static final double MAX_DIGITS = 0x1p53;

  /** Exceptions beyond rowCount/8 never pay for themselves — reject the pair early. */
  private static final int MAX_EXCEPTION_DIVISOR = 8;

  /** Stage-1 sample budget; 32 cells bound the 190-pair search at ~6k probe ops per vector. */
  private static final int SAMPLE_TARGET = 32;

  /** Fixed per-exception wire cost: int rowIdx + long transformBits. */
  private static final int EXCEPTION_BYTES = 4 + 8;

  private ProjectionAlpEncoding() {
  }

  /**
   * Result of a successful ALP probe: the chosen scale pair, per-row digits (exception rows
   * hold the first round-trippable digits value, keeping the FOR range tight), and the
   * exception rows carrying raw transform-domain bits; {@code sizeBytes} is the exact wire
   * size including the shared 9-byte head.
   */
  record Encoded(int e, int f, long[] digits, int[] exceptionRows, long[] exceptionBits, int sizeBytes) {
  }

  /**
   * Probe ALP for one double column's transform-domain cells. Returns {@code null} when no
   * scale pair round-trips at least {@code 7/8} of the rows with a strictly smaller wire
   * size than {@code plainForSizeBytes} (the caller's plain FOR form, shared head included).
   */
  static Encoded tryEncode(final long[] transformCells, final int rowCount, final int plainForSizeBytes) {
    if (rowCount <= 0 || rowCount > ProjectionIndexRowGroupPage.MAX_ROWS) {
      return null;
    }
    final double[] values = new double[rowCount];
    for (int i = 0; i < rowCount; i++) {
      values[i] = ProjectionDoubleEncoding.decode(transformCells[i]);
    }

    // Stage 1 — shortlist the scale pair on a small deterministic sample.
    final int sampleCount = Math.min(SAMPLE_TARGET, rowCount);
    final double[] sample;
    if (sampleCount == rowCount) {
      sample = values;
    } else {
      sample = new double[sampleCount];
      final int stride = rowCount / sampleCount;
      for (int i = 0; i < sampleCount; i++) {
        sample[i] = values[i * stride];
      }
    }
    int bestE = -1;
    int bestF = -1;
    int bestSampleSize = Integer.MAX_VALUE;
    // Fixed candidate order, first-best tie-break: descending e (more precision first),
    // ascending f — deterministic choice, deterministic bytes.
    for (int e = 18; e >= 0; e--) {
      for (int f = 0; f <= e; f++) {
        final int size = probeSize(sample, sampleCount, e, f);
        if (size >= 0 && size < bestSampleSize) {
          bestSampleSize = size;
          bestE = e;
          bestF = f;
        }
      }
    }
    if (bestE < 0) {
      return null;
    }
    // Stage 2 — the winning pair over the FULL vector decides against the plain form. A
    // pair that looked good on the sample but fails the full pass rejects ALP for this
    // vector (deterministically); it never degrades the encoding.
    final int fullSize = probeSize(values, rowCount, bestE, bestF);
    if (fullSize < 0 || fullSize >= plainForSizeBytes) {
      return null;
    }
    return materialize(values, transformCells, rowCount, bestE, bestF, fullSize);
  }

  /**
   * Exact wire size (shared head included) under {@code (e, f)}, or {@code -1} when the
   * pair fails too many cells. One pass, allocation-free.
   */
  private static int probeSize(final double[] values, final int rowCount, final int e, final int f) {
    final int maxExceptions = Math.max(1, rowCount / MAX_EXCEPTION_DIVISOR);
    final double scale = EXP10[e] / EXP10[f];
    final double expF = EXP10[f];
    final double expE = EXP10[e];
    long minDigits = Long.MAX_VALUE;
    long maxDigits = Long.MIN_VALUE;
    int exceptions = 0;
    boolean sawHit = false;
    for (int i = 0; i < rowCount; i++) {
      final double v = values[i];
      final double scaled = v * scale;
      if (!(Math.abs(scaled) < MAX_DIGITS)) { // NaN-safe: also rejects non-finite scaled
        if (++exceptions > maxExceptions) {
          return -1;
        }
        continue;
      }
      final long digits = Math.round(scaled);
      if (Double.doubleToRawLongBits(digits * expF / expE) != Double.doubleToRawLongBits(v)) {
        if (++exceptions > maxExceptions) {
          return -1;
        }
        continue;
      }
      sawHit = true;
      if (digits < minDigits) {
        minDigits = digits;
      }
      if (digits > maxDigits) {
        maxDigits = digits;
      }
    }
    if (!sawHit) {
      return -1;
    }
    final int forWidth = ProjectionIndexRowGroupCodec.widthOf(maxDigits - minDigits);
    // head: base(8) + width(1); ALP: e(1) + f(1) + excCount(4) + forBase(8) + forWidth(1)
    //   + packed digits + EXCEPTION_BYTES per exception.
    return 8 + 1 + 1 + 1 + 4 + 8 + 1 + ((rowCount * forWidth + 7) >>> 3)
        + exceptions * EXCEPTION_BYTES;
  }

  /** Second pass with the winning pair: collect digits and exceptions exactly. */
  private static Encoded materialize(final double[] values, final long[] transformCells,
      final int rowCount, final int e, final int f, final int sizeBytes) {
    final double scale = EXP10[e] / EXP10[f];
    final double expF = EXP10[f];
    final double expE = EXP10[e];
    final long[] digits = new long[rowCount];
    final boolean[] isException = new boolean[rowCount];
    int exceptionCount = 0;
    long fill = 0L;
    boolean fillChosen = false;
    for (int i = 0; i < rowCount; i++) {
      final double v = values[i];
      final double scaled = v * scale;
      long d = 0L;
      boolean ok = Math.abs(scaled) < MAX_DIGITS;
      if (ok) {
        d = Math.round(scaled);
        ok = Double.doubleToRawLongBits(d * expF / expE) == Double.doubleToRawLongBits(v);
      }
      if (ok) {
        digits[i] = d;
        if (!fillChosen) {
          fill = d;
          fillChosen = true;
        }
      } else {
        isException[i] = true;
        exceptionCount++;
      }
    }
    final int[] exceptionRows = new int[exceptionCount];
    final long[] exceptionBits = new long[exceptionCount];
    int x = 0;
    for (int i = 0; i < rowCount; i++) {
      if (isException[i]) {
        exceptionRows[x] = i;
        exceptionBits[x] = transformCells[i];
        digits[i] = fill; // keep the FOR range tight; real bits live in the exception list
        x++;
      }
    }
    return new Encoded(e, f, digits, exceptionRows, exceptionBits, sizeBytes);
  }

  /**
   * Decode the ALP payload (cursor positioned immediately AFTER the shared
   * {@code base + width} head) back to transform-domain cells — the exact inverse of the
   * wire form above. The digits stream decodes through the PLAIN FOR decoder (an escape
   * byte inside it is corruption and rejects loudly).
   */
  static long[] decode(final ProjectionIndexRowGroupCodec.Cursor in, final int rowCount) {
    final int e = in.readByte() & 0xFF;
    final int f = in.readByte() & 0xFF;
    if (e > 18 || f > e) {
      throw new IllegalStateException("Corrupt ALP scale pair e=" + e + " f=" + f);
    }
    final int exceptionCount = in.readInt();
    if (exceptionCount < 0 || exceptionCount > rowCount) {
      throw new IllegalStateException("Corrupt ALP exception count " + exceptionCount
          + " for rowCount " + rowCount);
    }
    final long[] cells = ProjectionIndexRowGroupCodec.decodePlainForBitPacked(in, rowCount);
    final double expF = EXP10[f];
    final double expE = EXP10[e];
    for (int i = 0; i < rowCount; i++) {
      cells[i] = ProjectionDoubleEncoding.encode(cells[i] * expF / expE);
    }
    for (int i = 0; i < exceptionCount; i++) {
      final int row = in.readInt();
      if (row < 0 || row >= rowCount) {
        throw new IllegalStateException("Corrupt ALP exception row " + row);
      }
      cells[row] = in.readLong();
    }
    return cells;
  }
}
