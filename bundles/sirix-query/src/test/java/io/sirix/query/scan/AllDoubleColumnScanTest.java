package io.sirix.query.scan;

import io.brackit.query.Query;
import io.brackit.query.atomic.Int64;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.sirix.access.Databases;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A numeric field that is PREDOMINANTLY OR ENTIRELY fractional must still be answered from the
 * typed double column, not from the record heap.
 *
 * <p>This is the workload the ALP double column exists for, and it was measured never reaching it:
 * holding {@code $u.price gt 100.25} constant over 20,000 records and varying only the field's type
 * mix, a mostly-integral field was served on every page, while 50 %, 99.98 % and 100 % fractional
 * fields fell back on every page.
 *
 * <h2>Why the counters are the assertion</h2>
 * The count is exact either way — a page that falls back decodes its records and returns the same
 * number. So a result-only test cannot tell "answered from the ALP column in registers" from
 * "answered by decoding every record", which is precisely how this gap survived. Only
 * {@code regionOnlyPagesServed} / {@code regionOnlyPageFallbacks} discriminate, so both are
 * asserted here, and the exact count is asserted alongside them to catch a served-but-wrong answer.
 *
 * <h2>Why tenths and not quarters</h2>
 * A JSON number is stored as a {@code double} only when its double image is EXACT; otherwise it
 * stays a {@code BigDecimal}, which the column path calls "a value of some third type" and which
 * makes the summed completeness oracle refuse the page. {@code 0.25} is exact in binary and
 * {@code 0.1} is not, so a corpus of quarters lands in the double column and is served — passing
 * this test without exercising the reported gap at all. An earlier revision of this test used
 * quarters and passed against BOTH the fixed and the unfixed engine, which is the definition of a
 * test that proves nothing. Tenths reproduce the shape the gap was reported on: "100 % doubles with
 * one or two decimals".
 */
public final class AllDoubleColumnScanTest {

  private static final int N = 20_000;
  private static final String DB = "all-double-scan-db";
  private static final String RES = "records.jn";

  /**
   * Never {@code .0}: a whole-numbered literal is stored as an integer and defeats the point.
   *
   * <p>Tenths, NOT quarters, and that is the whole experiment. A JSON number becomes a
   * {@code double} only when its double image is exact — {@code SirixVectorizedExecutor} keeps the
   * value as a {@code BigDecimal} otherwise — and {@code 0.25} is exact in binary while
   * {@code 0.1} is not. A corpus of quarters therefore lands in the double column and proves
   * nothing about the reported gap, which was measured on "doubles with one or two decimals".
   */
  private static final double[] TENTHS = { 0.1d, 0.2d, 0.3d };

  private Path dbDir;
  private double[] price;   // 100 % fractional
  private double[] mixed;   // 50 % fractional, 50 % integral — the other half of the reported gap
  private boolean[] mixedIsDouble;

  @BeforeEach
  void setUp() throws Exception {
    dbDir = Files.createTempDirectory("sirix-all-double-scan-");
    price = new double[N];
    mixed = new double[N];
    mixedIsDouble = new boolean[N];

    final StringBuilder sb = new StringBuilder(N * 48);
    sb.append('[');
    for (int i = 0; i < N; i++) {
      if (i > 0) {
        sb.append(',');
      }
      price[i] = 50 + (i % 500) + TENTHS[i % TENTHS.length];
      mixedIsDouble[i] = (i & 1) == 0;
      mixed[i] = mixedIsDouble[i] ? 50 + (i % 500) + TENTHS[i % TENTHS.length] : 50 + (i % 500);
      sb.append("{\"id\":").append(i).append(",\"price\":").append(price[i]).append(",\"mixed\":");
      if (mixedIsDouble[i]) {
        sb.append(mixed[i]);
      } else {
        sb.append((long) mixed[i]);
      }
      sb.append('}');
    }
    sb.append(']');

    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).buildPathSummary(true).build();
         var ctx = SirixQueryContext.createWithJsonStore(store);
         var chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain,
                "jn:store('" + DB + "','" + RES + "','" + sb.toString().replace("'", "''") + "')")
          .evaluate(ctx);
    }
  }

  @AfterEach
  void tearDown() {
    SequentialPipelineStrategy.setVectorizedExecutor(null);
    if (dbDir != null) {
      // The database lives at location.resolve(name), not at the location itself — passing dbDir
      // makes this a silent no-op that leaks a 20k-record database per test.
      Databases.removeDatabase(dbDir.resolve(DB));
    }
  }

  @Test
  @DisplayName("an entirely fractional field is served from the double column, not the record heap")
  void allDoubleFieldIsServedFromTheDoubleColumn() throws Exception {
    assertServedFromColumns("$u.price gt 100.25", groundTruth(price, 100.25d));
  }

  @Test
  @DisplayName("a half-fractional field is served from both typed columns")
  void halfDoubleFieldIsServedFromBothColumns() throws Exception {
    assertServedFromColumns("$u.mixed gt 100.25", groundTruth(mixed, 100.25d));
  }

  @Test
  @DisplayName("an inexactly-representable threshold is served from the decimal column")
  void inexactThresholdIsServedFromTheDecimalColumn() throws Exception {
    // `19.99` has no exact double image, which used to abandon the plan outright — every query a
    // human would actually write against a price column. The decimal side folds from the literal
    // itself, so it serves; the double side stays unservable, and a tag that recodes doubles
    // refuses rather than being answered from a threshold it cannot represent.
    assertServedFromColumns("$u.price gt 19.99", groundTruth(price, 19.99d));
    assertServedFromColumns("$u.price le 300.05", groundTruth(price, Double.NaN, 300.05d));
  }

  @Test
  @DisplayName("a disjunction over a fractional field is served as a decimal union")
  void disjunctionOverFractionalFieldIsServed() throws Exception {
    // Two disjoint branches, both with inexact thresholds. The union is merged in the TAG's
    // integer domain, so the count is exact even though neither bound has a faithful double.
    long expected = 0;
    for (final double v : price) {
      if (v < 19.99d || v > 500.05d) {
        expected++;
      }
    }
    assertServedFromColumns("$u.price lt 19.99 or $u.price gt 500.05", expected);
  }

  @Test
  @DisplayName("an OR branch that is itself a conjunction is served as a decimal union")
  void conjunctionInsideDisjunctionIsServed() throws Exception {
    // The branch `(ge 100.05 and le 200.05)` is a CONJUNCTION, so its exact bounds come from
    // intersecting two thresholds before the union is formed. Every literal here is inexact as a
    // double, so nothing in this query could be answered in double space.
    long expected = 0;
    for (final double v : price) {
      if ((v >= 100.05d && v <= 200.05d) || v > 500.05d) {
        expected++;
      }
    }
    assertServedFromColumns(
        "($u.price ge 100.05 and $u.price le 200.05) or $u.price gt 500.05", expected);
  }

  @Test
  @DisplayName("the column path and the record path agree on an all-double field")
  void columnPathAgreesWithRecordPath() throws Exception {
    final String predicate = "$u.price gt 100.25";
    assertEquals(count(predicate, false), count(predicate, true),
                 "column path and record path disagree on an all-double field");
  }

  /** Exact count, at least one page served from the columns, and nothing falling back. */
  private void assertServedFromColumns(final String predicate, final long expected) throws Exception {
    // The record path first: if THIS disagrees with the oracle the corpus is wrong, not the column
    // path, and the counter assertions below would be diagnosing the wrong thing.
    assertEquals(expected, count(predicate, false), "record path: " + predicate);

    SirixVectorizedExecutor.resetRegionOnlyCounters();
    final long actual = count(predicate, true);
    final long served = SirixVectorizedExecutor.regionOnlyPagesServed();
    final long fellBack = SirixVectorizedExecutor.regionOnlyPageFallbacks();
    final String seen = " (served=" + served + ", fellBack=" + fellBack + ", unavailable="
        + SirixVectorizedExecutor.regionOnlyPagesUnavailable() + ") for " + predicate;

    assertEquals(expected, actual, "column path returned a wrong count" + seen);
    assertTrue(served > 0, "no page served from the typed columns" + seen);
    assertEquals(0, fellBack, "a fractional field must not send pages to the record heap" + seen);
  }

  private static long groundTruth(final double[] values, final double threshold) {
    long c = 0;
    for (final double v : values) {
      if (v > threshold) {
        c++;
      }
    }
    return c;
  }

  /** Ground truth for {@code v <= hi}; {@code lo} is NaN when there is no lower bound. */
  private static long groundTruth(final double[] values, final double lo, final double hi) {
    long c = 0;
    for (final double v : values) {
      if ((Double.isNaN(lo) || v > lo) && v <= hi) {
        c++;
      }
    }
    return c;
  }

  private long count(final String predicate, final boolean regionOnly) throws Exception {
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).buildPathSummary(true).build();
         var ctx = SirixQueryContext.createWithJsonStore(store);
         var chain = SirixCompileChain.createWithJsonStore(store)) {
      final var coll = store.lookup(DB);
      final var resourceSession = coll.getDatabase().beginResourceSession(RES);
      try {
        final var exec =
            new SirixVectorizedExecutor(resourceSession, resourceSession.getMostRecentRevisionNumber());
        exec.setRegionOnlyCountEnabled(regionOnly);
        SequentialPipelineStrategy.setVectorizedExecutor(exec);
        try {
          final String q = "count(for $u in jn:doc('" + DB + "','" + RES + "')[] where " + predicate
              + " return $u)";
          return ((Int64) new Query(chain, q).evaluate(ctx)).longValue();
        } finally {
          exec.close();
          SequentialPipelineStrategy.setVectorizedExecutor(null);
        }
      } finally {
        resourceSession.close();
      }
    }
  }
}
