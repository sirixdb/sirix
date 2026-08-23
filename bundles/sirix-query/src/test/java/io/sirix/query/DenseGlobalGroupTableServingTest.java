package io.sirix.query;

import io.brackit.query.Query;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.sirix.JsonTestHelper;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.projection.ProjectionIndexBuilder;
import io.sirix.index.projection.ProjectionIndexCatalog;
import io.sirix.index.projection.ProjectionIndexRegistry;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.scan.SirixVectorizedExecutor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * The DENSE global-id group table: a group-by whose key is a resource-wide dictionary id addresses
 * its accumulator by the id instead of probing a per-worker hash table, which deletes the probe,
 * the table growth and the whole partition merge.
 *
 * <p>
 * Both arms produce the same answer — the hash arm is the fallback and is correct — so a
 * differential alone proves nothing about which one ran. Every case here therefore asserts a delta
 * on {@link SirixVectorizedExecutor#groupDenseServedCount()} beside the generic pipeline's answer,
 * and the arms are compared against EACH OTHER in one JVM through the
 * {@code sirix.projection.groupDense} switch.
 *
 * <p>
 * What is pinned beyond "it serves":
 * <ul>
 * <li>the {@code count} lane is folded only when something reads it — an atomic add per matching
 * row that a top-K by {@code min} does not need. Both shapes are here, because skipping it wrongly
 * would emit zeros and skipping it for group EXISTENCE would drop groups outright (existence is
 * read off the first-seen lane precisely so it cannot);</li>
 * <li>a group whose every matching row LACKS the aggregate operand must answer the empty sequence,
 * which only the per-column present-count lane can distinguish from a real extremum;</li>
 * <li>rows whose GROUP field is absent fold into the missing-key group, which has no id and stays a
 * thread-confined accumulator;</li>
 * <li>over the byte budget the arm declines to the hash tables rather than allocating — with the
 * answer unchanged.</li>
 * </ul>
 */
public final class DenseGlobalGroupTableServingTest extends AbstractJsonTest {

  private static final String DENSE_SWITCH = "sirix.projection.groupDense";
  private static final String DENSE_BUDGET = "sirix.projection.groupDense.maxBytes";
  /**
   * The speculative whole-leaf promotion fires after the second sliced serve and takes the column
   * store away from every sliced arm, dense included — so a suite that runs four shapes in one
   * session would see the last two decline for a reason that has nothing to do with what it tests.
   * Declining the promotion is also what the 100M measurements run with.
   */
  private static final String PROMOTE_BUDGET = "sirix.projection.promoteMaxBytes";

  /**
   * Enough rows and distinct {@code did}s that the AUTO heuristic mints a resource-wide dictionary
   * for that column — the encoding this table exists for. Forcing the mode would test the route but
   * not the decision that ships.
   *
   * <p>
   * Three shapes are deliberate: every third row has no {@code n} at all (so one whole group's
   * aggregate operand is missing), every 400th row has no {@code did} (the missing-KEY group), and
   * {@code kind} repeats so the corpus also carries a low-cardinality column.
   */
  private static final int ROWS = 12_000;

  private static String corpus() {
    final StringBuilder sb = new StringBuilder(ROWS * 70).append('[');
    for (int i = 0; i < ROWS; i++) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append("{\"kind\":\"")
        .append(i % 3 == 0
            ? "commit"
            : i % 3 == 1
                ? "identity"
                : "account")
        .append('"');
      if (i % 400 != 7) {
        sb.append(",\"did\":\"did:plc:").append(i).append('"');
      }
      // Group "did:plc:3" keeps its rows but never carries n — its min must be the empty sequence.
      if (i != 3) {
        sb.append(",\"n\":").append(i % 977);
      }
      sb.append(",\"t\":").append(1_700_000_000_000_000L + (long) i * 1000).append('}');
    }
    return sb.append(']').toString();
  }

  /** JSONBench Q4's shape: high-cardinality global key, ordered by an aggregate, capped. */
  private static final String MIN_TOP_K = """
        subsequence(
          for $e in jn:doc('json-path1','dense.jn')[]
          where $e.kind eq 'commit'
          let $k := $e.did
          group by $k
          let $first := min($e.t)
          order by $first
          return {"did": $k, "first": $first}, 1, 3)
      """;

  /** The same key, but the emission READS the count lane — which must therefore be folded. */
  private static final String COUNT_TOP_K = """
        subsequence(
          for $e in jn:doc('json-path1','dense.jn')[]
          where $e.kind eq 'commit'
          let $k := $e.did
          group by $k
          let $c := count($e)
          let $first := min($e.t)
          order by $c descending
          return {"did": $k, "rows": $c, "first": $first}, 1, 3)
      """;

  /** JSONBench Q5's shape: a span over one column's two extrema. */
  private static final String SPAN_TOP_K = """
        subsequence(
          for $e in jn:doc('json-path1','dense.jn')[]
          where $e.kind eq 'commit'
          let $k := $e.did
          group by $k
          let $first := min($e.t)
          let $last := max($e.t)
          let $span := $last - $first
          order by $span descending
          return {"did": $k, "span": $span}, 1, 3)
      """;

  /**
   * An UNCAPPED group-by over the global key: selection is bounded by the LIVE groups rather than by
   * K, so the roster sizes the heap and every group is emitted and reverse-mapped.
   *
   * <p>
   * The disjunction is what makes this shape reachable at all: an ordered group-by without a cap
   * resolves an order plan only when its predicate is a TREE (or it carries a distinct/transform),
   * and without a plan the flat arms — dense and hash alike — are not entered. Ordering is by the
   * AGGREGATE, never by the key: a global column's ids are mint order, not value order, and a plan
   * over them is declined by design.
   */
  private static final String UNCAPPED_MIN = """
        for $e in jn:doc('json-path1','dense.jn')[]
        where $e.kind eq 'commit' and ($e.t le 1700000000010000 or $e.t gt 1799999999999999)
        let $k := $e.did
        group by $k
        let $first := min($e.t)
        order by $first
        return {"did": $k, "first": $first}
      """;

  @BeforeEach
  public void clearBefore() {
    System.clearProperty(DENSE_SWITCH);
    System.clearProperty(DENSE_BUDGET);
    System.setProperty(PROMOTE_BUDGET, "0");
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    SequentialPipelineStrategy.setVectorizedExecutor(null);
  }

  @AfterEach
  public void clearAfter() {
    System.clearProperty(DENSE_SWITCH);
    System.clearProperty(DENSE_BUDGET);
    System.clearProperty(PROMOTE_BUDGET);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    SequentialPipelineStrategy.setVectorizedExecutor(null);
  }

  private void build() throws IOException {
    JsonTestHelper.deleteEverything();
    query("jn:store('json-path1','dense.jn','" + corpus() + "')");
    query("""
          let $doc := jn:doc('json-path1','dense.jn')
          let $stats := jn:create-projection-index($doc, '/[]',
              ('/[]/kind', '/[]/did', '/[]/n', '/[]/t'), ('string', 'string', 'long', 'long'))
          return {"revision": sdb:commit($doc)}
        """);
    Assertions.assertTrue(ProjectionIndexBuilder.globalDictionaryColumnsBuilt() > 0,
        "the fixture must build at least one resource-wide dictionary column, or this suite tests "
            + "the hash arm twice");
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
  }

  @Test
  public void theFourGlobalKeyShapesServeThroughTheDenseTable() throws IOException {
    build();
    withExecutor(evaluator -> {
      assertDenseServes(evaluator, MIN_TOP_K, "a top-K by min over a global key");
      assertDenseServes(evaluator, COUNT_TOP_K, "a top-K by count over a global key");
      assertDenseServes(evaluator, SPAN_TOP_K, "a top-K by span over a global key");
      assertDenseServes(evaluator, UNCAPPED_MIN, "an uncapped group-by over a global key");
    });
  }

  /**
   * The dense arm and the hash arm must agree EXACTLY, including on the count lane the dense arm
   * skips folding when nothing reads it: {@code MIN_TOP_K} skips it, {@code COUNT_TOP_K} does not,
   * and a mix-up in either direction shows up here as a differing answer rather than as a slow one.
   */
  @Test
  public void theDenseArmAgreesWithTheHashArmOnEveryShape() throws IOException {
    build();
    for (final String q : new String[] {MIN_TOP_K, COUNT_TOP_K, SPAN_TOP_K, UNCAPPED_MIN}) {
      final String dense;
      final String hash;
      System.clearProperty(DENSE_SWITCH);
      dense = servedAnswer(q, true);
      System.setProperty(DENSE_SWITCH, "false");
      hash = servedAnswer(q, false);
      System.clearProperty(DENSE_SWITCH);
      Assertions.assertEquals(hash, dense, "the dense and hash group tables disagreed on: " + q);
    }
  }

  /**
   * A group with rows but no aggregate operand answers the empty sequence, and a row without the
   * group FIELD lands in the missing-key group. The dense table addresses blocks by id; both of these
   * are groups it cannot address that way, and both used to be the hash table's side slots.
   */
  @Test
  public void theAllMissingOperandAndMissingKeyGroupsSurviveTheDenseTable() throws IOException {
    build();
    // did:plc:3 is a commit row (3 % 3 == 0) whose n is absent, so min(n) over its single row is
    // empty — NOT 0, which is what fn:count would give.
    final String allMissing = """
          for $e in jn:doc('json-path1','dense.jn')[]
          where $e.kind eq 'commit' and ($e.t le 1700000000004000 or $e.t gt 1799999999999999)
          let $k := $e.did
          group by $k
          let $first := min($e.n)
          order by $first
          return {"did": $k, "first": $first}
        """;
    // Rows 7 and 407 carry no did at all and are 'identity' rows, so the filter has to admit that
    // kind for the missing-key group to exist at all.
    final String missingKey = """
          for $e in jn:doc('json-path1','dense.jn')[]
          where $e.t le 1700000000407000 and ($e.kind eq 'commit' or $e.kind eq 'identity')
          let $k := $e.did
          group by $k
          let $rows := count($e)
          order by $rows descending
          return {"did": $k, "rows": $rows}
        """;
    withExecutor(evaluator -> {
      final String missingOperand = assertDenseServes(evaluator, allMissing, "a group whose operand is all-missing");
      Assertions.assertTrue(missingOperand.contains("{\"did\":\"did:plc:3\",\"first\":null}"),
          "a group whose every row lacks the aggregate operand must answer the empty sequence, got: " + missingOperand);
      final String withMissingKey =
          assertDenseServes(evaluator, missingKey, "a corpus with rows lacking the group key");
      Assertions.assertTrue(withMissingKey.contains("\"did\":null"),
          "the rows without a did must be emitted as the missing-key group, got: " + withMissingKey);
    });
  }

  /**
   * Over the byte budget the arm must DECLINE to the hash tables — the fallback whose memory scales
   * with the group count rather than with the id space — and the answer must not move.
   */
  @Test
  public void overTheByteBudgetTheArmDeclinesToTheHashTables() throws IOException {
    build();
    final String withDense = servedAnswer(MIN_TOP_K, true);
    System.setProperty(DENSE_BUDGET, "8");
    try {
      withExecutor(evaluator -> {
        final long before = SirixVectorizedExecutor.groupDenseServedCount();
        final long served = SirixVectorizedExecutor.groupAggServedCount();
        final String answer = evaluator.evaluate(MIN_TOP_K);
        Assertions.assertEquals(before, SirixVectorizedExecutor.groupDenseServedCount(),
            "an eight-byte budget must decline the dense table");
        Assertions.assertEquals(1L, SirixVectorizedExecutor.groupAggServedCount() - served,
            "the decline must fall back to the HASH group tables, not off the served path entirely");
        Assertions.assertEquals(withDense, answer, "the fallback answered differently from the dense arm");
      });
    } finally {
      System.clearProperty(DENSE_BUDGET);
    }
  }

  /**
   * Run {@code q} with an executor installed, asserting the dense arm took it; returns the answer.
   */
  private static String assertDenseServes(final Evaluator evaluator, final String q, final String what)
      throws IOException {
    final long before = SirixVectorizedExecutor.groupDenseServedCount();
    final String answer = evaluator.evaluate(q);
    Assertions.assertEquals(1L, SirixVectorizedExecutor.groupDenseServedCount() - before,
        what + " must be served through the DENSE global-id table");
    return answer;
  }

  /**
   * The answer of {@code q} with an executor installed, checking WHICH arm produced it — so that two
   * equal answers are evidence about the arms rather than about the fallback being correct twice.
   */
  private String servedAnswer(final String q, final boolean expectDense) throws IOException {
    final String[] out = new String[1];
    final long before = SirixVectorizedExecutor.groupDenseServedCount();
    withExecutor(evaluator -> out[0] = evaluator.evaluate(q));
    final boolean dense = SirixVectorizedExecutor.groupDenseServedCount() > before;
    Assertions.assertEquals(expectDense, dense, expectDense
        ? "expected the dense arm to serve this shape"
        : "expected the HASH arm here, or the comparison proves nothing");
    return out[0];
  }

  /** Evaluates a query against the installed executor and context. */
  @FunctionalInterface
  private interface Evaluator {
    String evaluate(String query) throws IOException;
  }

  @FunctionalInterface
  private interface WithEvaluator {
    void run(Evaluator evaluator) throws IOException;
  }

  /**
   * Install a vectorized executor over the fixture's most recent revision and run {@code body}.
   * Without the install every group query takes the generic pipeline, the answers are right, and the
   * serving counters say nothing — the trap this suite exists to avoid.
   */
  private void withExecutor(final WithEvaluator body) throws IOException {
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup("json-path1");
      final JsonResourceSession session = collection.getDatabase().beginResourceSession("dense.jn");
      final SirixVectorizedExecutor executor =
          new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 4);
      SequentialPipelineStrategy.setVectorizedExecutor(executor);
      try {
        body.run(queryStr -> {
          try (final ByteArrayOutputStream out = new ByteArrayOutputStream();
              final PrintWriter printWriter = new PrintWriter(out)) {
            new Query(chain, queryStr).serialize(ctx, printWriter);
            printWriter.flush();
            return out.toString();
          }
        });
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        executor.close();
      }
    }
  }
}
