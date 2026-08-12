/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

/**
 * Sampling-based encoding-scheme selection, after BtrBlocks §3 (Kuschewski, Sauerwein, Alhomssi,
 * Leis, SIGMOD '23).
 *
 * <h2>Why sampling rather than statistics</h2>
 *
 * <p>
 * Our region encoders pick their scheme from full-column statistics and a fixed two-way bake-off
 * (frame-of-reference bit-packing versus delta-of-delta). That is the "simple heuristics" the paper
 * argues against in §3: min/max/unique-count are enough to choose among a handful of encodings, but
 * they cannot rank encodings whose benefit depends on the ARRANGEMENT of values rather than their
 * range — run-length above all, where a column with an ideal min/max spread may still be a hundred
 * repeats of the same value.
 *
 * <p>
 * The paper's answer is to compress a small sample with each viable scheme and compare the ratios
 * it actually achieves. That costs 1.2 % of total compression time in their measurements, and it
 * extends to new schemes without a cost model per scheme.
 *
 * <h2>The sample</h2>
 *
 * <p>
 * Not a prefix, and not a uniform stride. §3.1: the sample must preserve the LOCALITY of
 * neighbouring tuples — a run-length encoder judged on every 64th value sees no runs at all — while
 * still covering the whole block, because the first {@code k} tuples are a biased sample. So the
 * sample is several short CONTIGUOUS runs taken from random positions in non-overlapping parts of
 * the block. The paper uses ten runs of 64 values over a 64,000-value block, i.e. 1 %.
 *
 * <p>
 * Deterministic here, unlike the paper's {@code rand()}: the part is divided into a fixed number of
 * slots and the run is taken from the middle of its part. A compressor that picks a different
 * scheme for the same input on two runs produces two different byte images for one logical page,
 * which breaks page-image comparison in tests and makes a corruption report unreproducible. The
 * point of randomness in the paper is to avoid a biased offset within the block, and taking the
 * midpoint of each part achieves that without the irreproducibility.
 *
 * <h2>What this class does NOT do</h2>
 *
 * <p>
 * It ranks schemes; it does not encode. The caller owns the byte formats, and the estimator a
 * scheme supplies is free to be exact (compress the sample and measure) or analytic (compute the
 * size the scheme would produce). It also does not cascade — {@link #select} answers one level, and
 * a caller that cascades re-enters it on the scheme's output with {@code depth - 1}.
 */
public final class SchemeSelector {

  /** Runs per block, per BtrBlocks §3.1. */
  public static final int SAMPLE_RUNS = 10;

  /** Values per run, per BtrBlocks §3.1. Ten of these over 64,000 values is the paper's 1 %. */
  public static final int SAMPLE_RUN_LENGTH = 64;

  /**
   * Maximum cascade depth; the paper's default (§3.2). Past it the output is left as it is.
   *
   * <p>
   * Not a tuning knob so much as a termination guarantee: every level costs another selection pass,
   * and the paper measured the ratio gain flattening well before the cost does.
   */
  public static final int MAX_CASCADE_DEPTH = 3;

  private SchemeSelector() {}

  /**
   * One scheme's ability to size its own output.
   *
   * <p>
   * Deliberately narrow: a scheme that can estimate a sample can be ranked, whether or not it can
   * encode yet. That keeps the selector independent of every byte format in the codebase.
   */
  @FunctionalInterface
  public interface RatioEstimator {

    /**
     * Bytes this scheme would produce for {@code sample[0..length)}, or a negative value when the
     * scheme cannot represent this input at all.
     *
     * <p>
     * Sizing the SAMPLE, not the block: ratios compare across schemes on identical input, so a scheme
     * that is asked for an absolute block size instead would have to model the block's distribution,
     * which is the cost model the sampling approach exists to avoid.
     */
    long estimateBytes(long[] sample, int length);
  }

  /** A candidate scheme: an identifier the caller understands, plus its estimator. */
  public record Candidate(byte schemeCode, RatioEstimator estimator) {
  }

  /**
   * Column statistics gathered in ONE pass, per BtrBlocks §3.1.
   *
   * <p>
   * These drive the VIABILITY filter, not the ranking: their job is to exclude schemes that cannot
   * pay off, so the sample is only compressed with the ones that might.
   */
  public record Stats(long min, long max, int uniqueApprox, double averageRunLength, int count) {

    /** Whether every value is the same — One Value's exact precondition. */
    public boolean isConstant() {
      return count > 0 && min == max;
    }
  }

  /**
   * Single-pass statistics over {@code values[0..count)}.
   *
   * <p>
   * {@code uniqueApprox} counts distinct values only while they stay few, then stops counting and
   * reports the cap. Exactness past that point buys nothing — every rule that reads it is a threshold
   * ("at least half the values are unique"), and an exact distinct count over a whole block is a hash
   * set the single pass exists to avoid.
   */
  public static Stats stats(final long[] values, final int count) {
    if (values == null || count <= 0) {
      return new Stats(0, 0, 0, 0, 0);
    }
    final int uniqueCap = 64;
    final long[] seen = new long[uniqueCap];
    int unique = 0;
    boolean uniqueExact = true;
    long min = Long.MAX_VALUE;
    long max = Long.MIN_VALUE;
    int runs = 1;
    long prev = values[0];
    for (int i = 0; i < count; i++) {
      final long v = values[i];
      if (v < min) {
        min = v;
      }
      if (v > max) {
        max = v;
      }
      if (i > 0 && v != prev) {
        runs++;
      }
      prev = v;
      if (uniqueExact) {
        boolean found = false;
        for (int u = 0; u < unique; u++) {
          if (seen[u] == v) {
            found = true;
            break;
          }
        }
        if (!found) {
          if (unique == uniqueCap) {
            uniqueExact = false;
          } else {
            seen[unique++] = v;
          }
        }
      }
    }
    return new Stats(min, max, uniqueExact
        ? unique
        : uniqueCap, (double) count / runs, count);
  }

  /**
   * Draw the paper's sample: {@link #SAMPLE_RUNS} contiguous runs of {@link #SAMPLE_RUN_LENGTH}, one
   * from the middle of each of that many equal parts.
   *
   * <p>
   * Short blocks return a copy of the whole block rather than a sample — below roughly one run per
   * part there is nothing to sample, and estimating on the real thing is both cheaper and exact.
   *
   * @param out receives the sample; must hold {@code SAMPLE_RUNS * SAMPLE_RUN_LENGTH}
   * @return the number of values written
   */
  public static int sample(final long[] values, final int count, final long[] out) {
    if (values == null || count <= 0) {
      return 0;
    }
    final int wanted = SAMPLE_RUNS * SAMPLE_RUN_LENGTH;
    if (count <= wanted) {
      final int n = Math.min(count, out.length);
      System.arraycopy(values, 0, out, 0, n);
      return n;
    }
    final int partLength = count / SAMPLE_RUNS;
    int written = 0;
    for (int part = 0; part < SAMPLE_RUNS && written + SAMPLE_RUN_LENGTH <= out.length; part++) {
      // Midpoint of the part, clamped so the run stays inside it: contiguous, so run structure
      // survives, and spread across the block, so the sample is not the prefix.
      final int partStart = part * partLength;
      int runStart = partStart + (partLength - SAMPLE_RUN_LENGTH) / 2;
      if (runStart < partStart) {
        runStart = partStart;
      }
      final int maxStart = Math.min(partStart + partLength, count) - SAMPLE_RUN_LENGTH;
      if (runStart > maxStart) {
        runStart = maxStart;
      }
      if (runStart < 0) {
        break;
      }
      System.arraycopy(values, runStart, out, written, SAMPLE_RUN_LENGTH);
      written += SAMPLE_RUN_LENGTH;
    }
    return written;
  }

  /**
   * Whether a scheme is worth estimating at all, per the paper's §3.1 exclusions.
   *
   * <p>
   * Both rules exclude a scheme whose best case cannot occur given the statistics, so the sample is
   * never compressed with it:
   *
   * <ul>
   * <li>RLE needs an average run of at least 2 — at one value per run it stores a length beside every
   * value and is strictly larger than the input.</li>
   * <li>Frequency needs a dominant value — at half the values distinct there is no top value frequent
   * enough to pay for the exception bitmap.</li>
   * </ul>
   */
  public static boolean viable(final byte schemeCode, final Stats stats, final SchemePool pool) {
    return pool.viable(schemeCode, stats);
  }

  /** The caller's scheme set, with the viability rules that belong to it. */
  public interface SchemePool {

    /** Candidates in preference order; ties in estimated size resolve to the earlier entry. */
    Candidate[] candidates();

    /** Whether {@code schemeCode} can pay off given {@code stats} — see {@link #viable}. */
    boolean viable(byte schemeCode, Stats stats);

    /** Code meaning "store as-is"; returned when nothing beats the input. */
    byte uncompressedCode();
  }

  /**
   * The scheme with the smallest estimated output, or the pool's uncompressed code when none beats
   * storing the values as they are.
   *
   * <p>
   * The comparison is against {@code 8 * count} — the plain long array the caller would otherwise
   * write. A scheme that cannot beat that is not worth its decode branch, which is the asymmetry that
   * matters here: compression happens once and scanning happens forever.
   *
   * @param scratch reusable sample buffer, at least {@code SAMPLE_RUNS * SAMPLE_RUN_LENGTH} long
   */
  public static byte select(final long[] values, final int count, final SchemePool pool, final long[] scratch) {
    if (values == null || count <= 0 || pool == null) {
      return pool == null
          ? 0
          : pool.uncompressedCode();
    }
    final Stats stats = stats(values, count);
    final int sampleLength = sample(values, count, scratch);
    if (sampleLength <= 0) {
      return pool.uncompressedCode();
    }
    byte best = pool.uncompressedCode();
    // The sample's own plain size, so ratios compare like with like: estimators size the SAMPLE,
    // and the baseline has to be the sample's plain form rather than the block's.
    long bestBytes = 8L * sampleLength;
    for (final Candidate candidate : pool.candidates()) {
      if (!pool.viable(candidate.schemeCode(), stats)) {
        continue;
      }
      final long bytes = candidate.estimator().estimateBytes(scratch, sampleLength);
      if (bytes >= 0 && bytes < bestBytes) {
        bestBytes = bytes;
        best = candidate.schemeCode();
      }
    }
    return best;
  }
}
