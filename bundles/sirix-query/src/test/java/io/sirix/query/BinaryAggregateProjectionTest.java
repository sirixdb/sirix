package io.sirix.query;

import io.brackit.query.Query;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.sirix.access.Databases;
import io.sirix.index.projection.ProjectionIndexRegistry;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.scan.SirixVectorizedExecutor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * {@code sum($m.a * $m.b)} — an aggregate over an ARITHMETIC expression, served from two projection
 * columns.
 *
 * <h2>Why the shape needed its own route</h2>
 *
 * <p>
 * The vectorized SPI aggregated over ONE field, so a product of two fields was not claimed at all
 * and the record pipeline answered: it materializes every record to reach two of its fields, which
 * on a 3.48M-record corpus measured 530 ms against the ~3 ns per row a fused pass over two long
 * columns costs. It was the last shape in the DuckDB comparison still an order of magnitude behind
 * — 26x, where the others sat between 1.5x and 4x.
 *
 * <h2>What these tests hold it to</h2>
 *
 * <p>
 * Ground truth is the SAME query with no projection installed, which is the record pipeline. That
 * is the only comparison that cannot drift with the kernels, and it is what catches the two things
 * a columnar fold gets wrong: reading a missing operand as a zero, and pairing values across rows.
 * The corpus therefore makes both operands optional and independent of each other, so plenty of
 * rows carry exactly one of them — under the interpreter those contribute nothing, because
 * {@code () * 3} is the empty sequence.
 */
final class BinaryAggregateProjectionTest {

  private static final int N = 4_000;
  private static final String DB_WITH = "binary-agg-with-projection";
  private static final String DB_WITHOUT = "binary-agg-without-projection";
  private static final String RES = "records.jn";

  /** Every arithmetic form the route claims, plus one it must decline (division). */
  private static final List<String> SHAPES =
      List.of("sum(for $m in %s return $m.width * $m.height)", "sum(for $m in %s return $m.width + $m.height)",
          "sum(for $m in %s return $m.width - $m.height)", "sum(for $m in %s return $m.height - $m.width)",
          // Not claimed: over the integers a JSON document holds, division is not closed, so the
          // columnar fold and the interpreter would not agree. It must still answer, via the records.
          "sum(for $m in %s return $m.width div $m.height)",
          // The single-field route must keep working beside the binary one.
          "sum(for $m in %s return $m.width)");

  private Path dbDir;

  @BeforeEach
  void setUp() throws Exception {
    ProjectionIndexRegistry.clear();
    dbDir = Files.createTempDirectory("sirix-binary-agg-");
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      final String corpus = corpus();
      new Query(chain, "jn:store('" + DB_WITH + "','" + RES + "','" + corpus + "')").evaluate(ctx);
      new Query(chain, "jn:store('" + DB_WITHOUT + "','" + RES + "','" + corpus + "')").evaluate(ctx);
      new Query(chain,
          "let $doc := jn:doc('" + DB_WITH + "','" + RES + "')" + " let $i := jn:create-projection-index($doc, '/[]',"
              + " ('/[]/width', '/[]/height'), ('long', 'long'))" + " return sdb:commit($doc)").evaluate(ctx);
    }
  }

  /**
   * Both operands optional and independent: about a third of the records carry only one of them, and
   * a ninth carry neither. Values are negative on part of the range so a sum cannot pass by accident
   * on magnitudes alone.
   */
  private static String corpus() {
    final StringBuilder sb = new StringBuilder(N * 32);
    sb.append('[');
    for (int i = 0; i < N; i++) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append("{\"id\":").append(i);
      if (i % 3 != 0) {
        sb.append(",\"width\":")
          .append(i % 7 == 0
              ? -(i % 500) - 1
              : (i % 500) + 1);
      }
      if (i % 3 != 1) {
        sb.append(",\"height\":").append((i % 311) + 1);
      }
      sb.append('}');
    }
    return sb.append(']').toString();
  }

  @AfterEach
  void tearDown() {
    ProjectionIndexRegistry.clear();
    SequentialPipelineStrategy.setVectorizedExecutor(null);
    if (dbDir != null) {
      Databases.removeDatabase(dbDir.resolve(DB_WITH));
      Databases.removeDatabase(dbDir.resolve(DB_WITHOUT));
    }
  }

  @Test
  @DisplayName("the projection actually serves the products, rather than agreeing vacuously")
  void theProjectionServesTheArithmeticShapes() throws Exception {
    SirixVectorizedExecutor.resetRegionOnlyCounters();
    evaluate("sum(for $m in jn:doc('" + DB_WITH + "','" + RES + "')[]" + " return $m.width * $m.height)");
    assertEquals(1L, SirixVectorizedExecutor.binaryAggregatesServed(),
        "the product was not served from the projection — agreement with the record "
            + "pipeline then proves nothing, because the record pipeline answered both");
  }

  @Test
  @DisplayName("a shape outside the closed operator set is declined, not miscomputed")
  void divisionIsDeclined() throws Exception {
    SirixVectorizedExecutor.resetRegionOnlyCounters();
    evaluate("sum(for $m in jn:doc('" + DB_WITH + "','" + RES + "')[]" + " return $m.width div $m.height)");
    assertEquals(0L, SirixVectorizedExecutor.binaryAggregatesServed(),
        "division was served from integer columns — over the integers a JSON document "
            + "holds it is not closed, so the fold and the interpreter would disagree");
  }

  @Test
  @DisplayName("every arithmetic aggregate agrees with the record pipeline")
  void arithmeticAggregatesAgreeWithTheRecordPipeline() throws Exception {
    for (final String shape : SHAPES) {
      final String withProjection = shape.formatted("jn:doc('" + DB_WITH + "','" + RES + "')[]");
      final String withoutProjection = shape.formatted("jn:doc('" + DB_WITHOUT + "','" + RES + "')[]");
      assertEquals(evaluate(withoutProjection), evaluate(withProjection),
          "the projection-served answer differs from the record pipeline's for: " + shape
              + " — a row carrying only ONE of the two operands contributes nothing under "
              + "the interpreter, and a columnar fold that reads its missing operand as "
              + "zero, or pairs values across rows, disagrees here");
    }
  }

  private String evaluate(final String query) throws Exception {
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      final ByteArrayOutputStream sink = new ByteArrayOutputStream();
      try (final PrintStream out = new PrintStream(sink)) {
        new Query(chain, query).serialize(ctx, out);
      }
      return sink.toString().trim();
    }
  }
}
