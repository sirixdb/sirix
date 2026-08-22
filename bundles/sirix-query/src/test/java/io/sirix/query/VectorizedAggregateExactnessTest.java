package io.sirix.query;

import io.brackit.query.Query;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.sirix.JsonTestHelper;
import io.sirix.index.projection.ProjectionIndexRegistry;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.scan.SirixVectorizedExecutor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exactness of the vectorized value aggregates, for the two shapes the ClickBench port ran into.
 *
 * <ol>
 * <li>{@code fn:min}/{@code fn:max} over a STRING column. The numeric kernels contribute nothing
 * for such a column, and the "no numeric value" branch used to be terminal — every
 * {@code min(EventDate)} died with an internal error although the interpreter answers it
 * (ClickBench Q6, and Q21/Q22 via {@code MIN(URL)}).</li>
 * <li>{@code fn:sum}/{@code fn:avg} over a column of large 64-bit integers. {@code xs:integer} is
 * arbitrary precision; a long accumulator wraps after a few dozen 1e18-scale values and returned a
 * silently wrong answer (ClickBench Q3 averages {@code UserID}). The expected values here are
 * computed with {@link BigInteger}, not by the engine.</li>
 * </ol>
 */
public final class VectorizedAggregateExactnessTest {

  /** Values around 1.1e18: nine of them overflow a signed 64-bit accumulator. */
  private static final long[] BIG_IDS = {1152908451924884486L, 1141889425024884481L, 1052908451924884482L,
      1152908451924884483L, 1052908451924884484L, 1152908451924884485L, 1052908451924884486L, 1152908451924884487L,
      1052908451924884488L, 1152908451924884489L, 1052908451924884490L, 1152908451924884491L, 1052908451924884492L,
      1152908451924884493L, 1052908451924884494L, 1152908451924884495L};

  private static final String[] DATES = {"2013-07-15", "2013-07-02", "2013-07-31", "2013-07-14"};

  @BeforeEach
  public void setUp() {
    JsonTestHelper.deleteEverything();
    ProjectionIndexRegistry.clear();
    SequentialPipelineStrategy.setVectorizedExecutor(null);
  }

  @AfterEach
  public void tearDown() {
    ProjectionIndexRegistry.clear();
    SequentialPipelineStrategy.setVectorizedExecutor(null);
    JsonTestHelper.deleteEverything();
  }

  @Test
  public void sumAndAvgOverLargeIntegersStayExact() throws Exception {
    BigInteger expectedSum = BigInteger.ZERO;
    for (final long id : BIG_IDS) {
      expectedSum = expectedSum.add(BigInteger.valueOf(id));
    }
    // Nine values already exceed Long.MAX_VALUE, so a wrapping accumulator cannot pass this.
    assertEquals(1, expectedSum.compareTo(BigInteger.valueOf(Long.MAX_VALUE)),
        "the fixture must overflow a long accumulator, otherwise it proves nothing");

    try (final BasicJsonDBStore store = newStore();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain, "jn:store('agg-db','records.jn','" + document() + "')").evaluate(ctx);

      assertEquals(expectedSum.toString(),
          run(chain, ctx, "sum(for $r in jn:doc('agg-db','records.jn')[] return $r.id)"),
          "sum over 64-bit ids must not wrap");
      // fn:avg over integers is xs:decimal when the quotient is not exact, so compare against the
      // exact decimal rather than a truncating integer division.
      final BigDecimal expectedAvg =
          new BigDecimal(expectedSum).divide(BigDecimal.valueOf(BIG_IDS.length), MathContext.DECIMAL128)
                                     .stripTrailingZeros();
      assertEquals(expectedAvg,
          new BigDecimal(
              run(chain, ctx, "avg(for $r in jn:doc('agg-db','records.jn')[] return $r.id)")).stripTrailingZeros(),
          "avg over 64-bit ids must not wrap");
      assertEquals(Long.toString(min(BIG_IDS)),
          run(chain, ctx, "min(for $r in jn:doc('agg-db','records.jn')[] return $r.id)"));
      assertEquals(Long.toString(max(BIG_IDS)),
          run(chain, ctx, "max(for $r in jn:doc('agg-db','records.jn')[] return $r.id)"));
    }
  }

  /**
   * The same exactness, with a PROJECTION INDEX installed over the big-integer column.
   *
   * <p>
   * A maintained projection is consulted BEFORE the path summary and before any scan, so an installed
   * one does not merely add a faster route — it REPLACES the answer. Its aggregate kernels had their
   * own bare {@code sum += v}, in a SIMD fold where no per-add carry is visible, so installing an
   * index over {@code UserID} turned the exact avg the scan path had just been taught to produce back
   * into the wrapped one (5.7e17 → 3.6e13). Nothing in the suite installed a projection while
   * checking exactness, which is the gap that let it ship.
   */
  @Test
  public void sumAndAvgOverLargeIntegersStayExactWithAProjectionInstalled() throws Exception {
    BigInteger expectedSum = BigInteger.ZERO;
    for (final long id : BIG_IDS) {
      expectedSum = expectedSum.add(BigInteger.valueOf(id));
    }
    assertEquals(1, expectedSum.compareTo(BigInteger.valueOf(Long.MAX_VALUE)),
        "the fixture must overflow a long accumulator, otherwise it proves nothing");

    try (final BasicJsonDBStore store = newStore();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain, "jn:store('agg-proj-db','records.jn','" + document() + "')").evaluate(ctx);
      final String install = "let $doc := jn:doc('agg-proj-db','records.jn')"
          + " let $i := jn:create-projection-index($doc, '/[]', ('/[]/id'), ('long'))" + " return sdb:commit($doc)";
      new Query(chain, install).evaluate(ctx);

      SirixVectorizedExecutor.resetProjectionAggregateCounters();
      assertEquals(expectedSum.toString(),
          run(chain, ctx, "sum(for $r in jn:doc('agg-proj-db','records.jn')[] return $r.id)"),
          "sum over 64-bit ids must not wrap when a projection covers the column");
      final BigDecimal expectedAvg =
          new BigDecimal(expectedSum).divide(BigDecimal.valueOf(BIG_IDS.length), MathContext.DECIMAL128)
                                     .stripTrailingZeros();
      assertEquals(expectedAvg,
          new BigDecimal(
              run(chain, ctx, "avg(for $r in jn:doc('agg-proj-db','records.jn')[] return $r.id)")).stripTrailingZeros(),
          "avg over 64-bit ids must not wrap when a projection covers the column");
      assertEquals(Long.toString(min(BIG_IDS)),
          run(chain, ctx, "min(for $r in jn:doc('agg-proj-db','records.jn')[] return $r.id)"));
      assertEquals(Long.toString(max(BIG_IDS)),
          run(chain, ctx, "max(for $r in jn:doc('agg-proj-db','records.jn')[] return $r.id)"));

      // Non-vacuity: a route that declines is indistinguishable from one that never ran, because
      // the fallback returns the same (correct) answer. The decline counter is what tells them
      // apart — without it this test would still pass if the projection silently stopped covering
      // the column, and would then prove nothing about the kernels.
      assertTrue(SirixVectorizedExecutor.projectionAggregateOverflowDeclines() > 0,
          "the projection never reached its aggregate kernels — the exactness gate was not exercised");
    }
  }

  @Test
  public void minAndMaxOverAStringColumnAreServed() throws Exception {
    try (final BasicJsonDBStore store = newStore();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain, "jn:store('agg-db','records.jn','" + document() + "')").evaluate(ctx);

      // Compared unquoted: a top-level string atomic is serialized bare by the fast path and
      // quoted by the interpreter (a pre-existing brackit serialization difference between a lone
      // Atomic and a one-item sequence). The VALUE is what this test is about.
      assertEquals("2013-07-02",
          unquoted(run(chain, ctx, "min(for $r in jn:doc('agg-db','records.jn')[] return $r.day)")));
      assertEquals("2013-07-31",
          unquoted(run(chain, ctx, "max(for $r in jn:doc('agg-db','records.jn')[] return $r.day)")));
    }
  }

  private static BasicJsonDBStore newStore() {
    return BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
  }

  /** {@code [{"id": <big>, "day": "<iso>"}, ...]} — one record per big id. */
  private static String document() {
    final StringBuilder sb = new StringBuilder(512).append('[');
    for (int i = 0; i < BIG_IDS.length; i++) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append("{\"id\":").append(BIG_IDS[i]).append(",\"day\":\"").append(DATES[i % DATES.length]).append("\"}");
    }
    return sb.append(']').toString();
  }

  private static String run(final SirixCompileChain chain, final SirixQueryContext ctx, final String query)
      throws Exception {
    try (final ByteArrayOutputStream out = new ByteArrayOutputStream(); final PrintWriter pw = new PrintWriter(out)) {
      new Query(chain, query).serialize(ctx, pw);
      pw.flush();
      return out.toString().trim();
    }
  }

  private static String unquoted(final String serialized) {
    return serialized.length() >= 2 && serialized.startsWith("\"") && serialized.endsWith("\"")
        ? serialized.substring(1, serialized.length() - 1)
        : serialized;
  }

  private static long min(final long[] values) {
    long best = Long.MAX_VALUE;
    for (final long v : values) {
      best = Math.min(best, v);
    }
    return best;
  }

  private static long max(final long[] values) {
    long best = Long.MIN_VALUE;
    for (final long v : values) {
      best = Math.max(best, v);
    }
    return best;
  }
}
