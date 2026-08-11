package io.sirix.index.projection;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Sampling-based scheme selection, against the behaviour BtrBlocks §3 specifies.
 *
 * <h2>What is worth asserting</h2>
 *
 * <p>
 * Not that a particular block picks a particular scheme — that is the estimator's arithmetic and
 * would pin an implementation detail. What matters is the paper's two claims about the SAMPLE, both
 * of which are properties a wrong sampler silently violates while still returning plausible
 * schemes:
 *
 * <ul>
 * <li>It preserves LOCALITY, so run-structure survives. A stride sampler destroys exactly this, and
 * the symptom is that RLE is never chosen on data made of long runs.</li>
 * <li>It SPREADS across the block, so a block whose head and tail differ is not judged on its head.
 * A prefix sampler destroys this, and the symptom appears only on data whose distribution shifts —
 * which is most real data, and no synthetic uniform block.</li>
 * </ul>
 */
final class SchemeSelectorTest {

  private static final long[] SCRATCH = new long[SchemeSelector.SAMPLE_RUNS * SchemeSelector.SAMPLE_RUN_LENGTH];

  @Test
  @DisplayName("the sample preserves runs, so a run-encoded column is still recognisable")
  void sampleKeepsLocality() {
    // 64,000 values in runs of 100 — the paper's block size, and an average run far above the
    // viability threshold. A stride sampler would see 64,000 unrelated values and report runs of 1.
    final int count = 64_000;
    final long[] values = new long[count];
    for (int i = 0; i < count; i++) {
      values[i] = i / 100;
    }
    final int n = SchemeSelector.sample(values, count, SCRATCH);
    assertEquals(SchemeSelector.SAMPLE_RUNS * SchemeSelector.SAMPLE_RUN_LENGTH, n,
        "sample length is not the paper's 10 runs of 64");

    final SchemeSelector.Stats sampled = SchemeSelector.stats(SCRATCH, n);
    assertTrue(sampled.averageRunLength() > 10.0, "the sample's average run length is " + sampled.averageRunLength()
        + " — runs did not survive sampling, so the sample is not contiguous");
  }

  @Test
  @DisplayName("the sample spreads across the block, so a shifting distribution is seen")
  void sampleSpreadsAcrossTheBlock() {
    // Constant for the first 90 %, then different. A prefix sampler reports a constant block and
    // One Value would be chosen for something that is not constant at all.
    final int count = 64_000;
    final long[] values = new long[count];
    for (int i = 0; i < count; i++) {
      values[i] = i < (count * 9) / 10
          ? 7L
          : 12345L;
    }
    final int n = SchemeSelector.sample(values, count, SCRATCH);
    final SchemeSelector.Stats sampled = SchemeSelector.stats(SCRATCH, n);
    assertNotEquals(sampled.min(), sampled.max(), "the sample saw only one value — it did not reach the block's tail");
  }

  @Test
  @DisplayName("a constant column selects One Value")
  void constantColumnPicksOneValue() {
    final int count = 4_096;
    final long[] values = new long[count];
    java.util.Arrays.fill(values, 3L);
    assertEquals(ProjectionSchemePool.SCHEME_ONE_VALUE,
        SchemeSelector.select(values, count, ProjectionSchemePool.get(), SCRATCH),
        "a block with one distinct value did not select One Value");
  }

  @Test
  @DisplayName("RLE is excluded below the paper's average-run-length threshold")
  void rleExcludedWithoutRuns() {
    final boolean previous = ProjectionSchemePool.RLE_ENABLED;
    ProjectionSchemePool.RLE_ENABLED = true;
    try {
      final int count = 4_096;
      final long[] values = new long[count];
      for (int i = 0; i < count; i++) {
        values[i] = i; // every run is length 1
      }
      final SchemeSelector.Stats stats = SchemeSelector.stats(values, count);
      assertTrue(stats.averageRunLength() < 2.0, "test data has runs it should not have");
      assertTrue(!ProjectionSchemePool.get().viable(ProjectionSchemePool.SCHEME_RLE, stats),
          "RLE was offered on data with no runs — it is strictly larger than its input "
              + "there, which is why the paper excludes it before estimating");
      assertEquals(ProjectionSchemePool.SCHEME_FOR_BITPACK,
          SchemeSelector.select(values, count, ProjectionSchemePool.get(), SCRATCH),
          "a dense ascending column should bit-pack");
    } finally {
      ProjectionSchemePool.RLE_ENABLED = previous;
    }
  }

  @Test
  @DisplayName("RLE wins on long runs once it is allowed to compete")
  void rleWinsOnLongRuns() {
    final boolean previous = ProjectionSchemePool.RLE_ENABLED;
    ProjectionSchemePool.RLE_ENABLED = true;
    try {
      final int count = 4_096;
      final long[] values = new long[count];
      for (int i = 0; i < count; i++) {
        values[i] = (i / 256) * 1_000_000L; // 16 long runs, values far apart
      }
      assertEquals(ProjectionSchemePool.SCHEME_RLE,
          SchemeSelector.select(values, count, ProjectionSchemePool.get(), SCRATCH),
          "RLE did not win on 16 runs of 256 — bit-packing pays the full spread on every "
              + "row here, RLE pays it 16 times");
    } finally {
      ProjectionSchemePool.RLE_ENABLED = previous;
    }
  }

  @Test
  @DisplayName("RLE is not offered by default, because our scans are positional")
  void rleOffByDefault() {
    final int count = 1_024;
    final long[] values = new long[count];
    for (int i = 0; i < count; i++) {
      values[i] = i / 128; // ideal RLE data
    }
    assertNotEquals(ProjectionSchemePool.SCHEME_RLE,
        SchemeSelector.select(values, count, ProjectionSchemePool.get(), SCRATCH),
        "RLE was selected by default — a positional SIMD kernel cannot address a "
            + "run-encoded column, so the ratio would cost scan speed");
  }

  @Test
  @DisplayName("the selector declines when nothing beats storing the values plainly")
  void declinesWhenNothingHelps() {
    final int count = 512;
    final long[] values = new long[count];
    for (int i = 0; i < count; i++) {
      values[i] = i % 2 == 0
          ? Long.MIN_VALUE + i
          : Long.MAX_VALUE - i; // full 64-bit spread
    }
    assertEquals(ProjectionSchemePool.SCHEME_UNCOMPRESSED,
        SchemeSelector.select(values, count, ProjectionSchemePool.get(), SCRATCH),
        "a column spanning the full long range should stay uncompressed rather than pay "
            + "a reference and a width for nothing");
  }
}
