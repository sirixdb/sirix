/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.index.projection.SchemeSelector.Candidate;
import io.sirix.index.projection.SchemeSelector.SchemePool;
import io.sirix.index.projection.SchemeSelector.Stats;

/**
 * The scheme pool a projection column chooses from, with BtrBlocks' §3.1 viability rules.
 *
 * <h2>Scheme codes</h2>
 *
 * <p>
 * Deliberately separate from {@code ProjectionIndexRowGroupPage}'s COLUMN_KIND bytes: a kind says
 * what a column HOLDS (longs, a string set), a scheme says how those values are LAID OUT. The
 * paper's cascade needs the second to vary per block while the first stays fixed for the column.
 */
public final class ProjectionSchemePool implements SchemePool {

  /** Store the values as they are. */
  public static final byte SCHEME_UNCOMPRESSED = 0;

  /** Frame of reference plus bit-packing — what the projection codec already writes. */
  public static final byte SCHEME_FOR_BITPACK = 1;

  /** Every value in the block is the same; the block stores it once. */
  public static final byte SCHEME_ONE_VALUE = 2;

  /** Run-length encoded: one value and one length per run. */
  public static final byte SCHEME_RLE = 3;

  /**
   * The pool in preference order. Ties in estimated size go to the earlier entry, so a block that One
   * Value and RLE would encode identically — a constant block, where RLE degenerates to a single run
   * — takes One Value, which is both smaller in practice and scannable with one compare.
   */
  private static final Candidate[] CANDIDATES = {new Candidate(SCHEME_ONE_VALUE, LightweightSchemes::oneValueBytes),
      new Candidate(SCHEME_RLE, LightweightSchemes::rleBytes),
      new Candidate(SCHEME_FOR_BITPACK, ProjectionSchemePool::forBitPackBytes),};

  /**
   * Whether RLE may be offered at all.
   *
   * <p>
   * Off by default, and this is NOT a ratio judgement. Our scan kernels evaluate predicates over the
   * encoded bytes positionally — value {@code i} lives at bit offset {@code i * width}. Under RLE the
   * value at a row is only reachable by walking the run lengths, so a column encoded this way would
   * fall off the vectorized path onto a decode-then-scan fallback. BtrBlocks optimises decompression
   * and can take that trade; we optimise scanning and cannot, until a run-aware kernel exists.
   *
   * <p>
   * Left switchable so the run-aware kernel can be measured against the positional one on real
   * columns rather than argued about.
   */
  public static boolean RLE_ENABLED = Boolean.getBoolean("sirix.projection.rle");

  private static final ProjectionSchemePool INSTANCE = new ProjectionSchemePool();

  public static ProjectionSchemePool get() {
    return INSTANCE;
  }

  private ProjectionSchemePool() {}

  @Override
  public Candidate[] candidates() {
    return CANDIDATES;
  }

  @Override
  public boolean viable(final byte schemeCode, final Stats stats) {
    return switch (schemeCode) {
      // Exact precondition, not a heuristic: One Value is representable iff the block is constant.
      case SCHEME_ONE_VALUE -> stats.isConstant();
      // BtrBlocks §3.1: excluded below an average run of 2, where RLE is strictly larger than its
      // input. The scan-shape gate above is ours, and independent of the paper's rule.
      case SCHEME_RLE -> RLE_ENABLED && stats.averageRunLength() >= 2.0;
      case SCHEME_FOR_BITPACK -> stats.max() - stats.min() >= 0;
      default -> false;
    };
  }

  @Override
  public byte uncompressedCode() {
    return SCHEME_UNCOMPRESSED;
  }

  /**
   * Size of the frame-of-reference bit-packed form: one reference, then {@code count} values at the
   * width the block's spread needs.
   */
  static long forBitPackBytes(final long[] values, final int count) {
    if (values == null || count <= 0) {
      return -1;
    }
    long min = Long.MAX_VALUE;
    long max = Long.MIN_VALUE;
    for (int i = 0; i < count; i++) {
      final long v = values[i];
      if (v < min) {
        min = v;
      }
      if (v > max) {
        max = v;
      }
    }
    final long range = max - min;
    if (range < 0) {
      // Wraparound: the spread does not fit an unsigned 64-bit delta, so there is no reference that
      // makes these values narrower. Reporting a width from the wrapped value would size the block
      // at nearly nothing and win every bake-off for data it cannot encode.
      return -1;
    }
    final int width = LightweightSchemes.widthOf(range);
    return 8 /* reference */ + 1 /* width */ + ((long) count * width + 7) / 8;
  }
}
