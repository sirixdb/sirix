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
 * The resource-wide dictionary must be SERVED, not merely correct.
 *
 * <p>
 * {@link GlobalValueDictionaryParityTest} shows the two encodings answer identically. That is
 * necessary and, on its own, proves nothing about the routes: a global column that declines every
 * fast path also answers identically, because the generic pipeline is correct. So this suite
 * asserts a delta on the serving counters — the only signal that distinguishes a route that works
 * from one that silently is not taken — with the generic pipeline's own answer as the oracle beside
 * it.
 *
 * <p>
 * The corpus is sized so that the AUTO heuristic itself picks the encoding for {@code did} and
 * rejects it for {@code kind}: {@code did} is distinct per row (per-leaf dedup factor 1, so a
 * per-leaf dictionary stores every value once per leaf and compresses nothing), {@code kind}
 * repeats across three labels. Forcing the mode would test the routes but not the decision, and the
 * decision is what ships.
 */
public final class GlobalValueDictionaryServingTest extends AbstractJsonTest {

  private static final String GLOBAL_DICT_PROPERTY = "sirix.projection.globalDict";

  /** Enough rows and enough distinct values that AUTO's minimum-entries floor is cleared. */
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
        .append("\",\"did\":\"did:plc:")
        .append(i)
        .append("\",\"n\":")
        .append(i % 977)
        .append('}');
    }
    return sb.append(']').toString();
  }

  @BeforeEach
  public void clearBefore() {
    System.clearProperty(GLOBAL_DICT_PROPERTY);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    SequentialPipelineStrategy.setVectorizedExecutor(null);
  }

  @AfterEach
  public void clearAfter() {
    System.clearProperty(GLOBAL_DICT_PROPERTY);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    SequentialPipelineStrategy.setVectorizedExecutor(null);
  }

  /** Build the corpus + index under the DEFAULT mode and report how many columns went global. */
  private int buildUnderDefault() throws IOException {
    JsonTestHelper.deleteEverything();
    query("jn:store('json-path1','serving.jn','" + corpus() + "')");
    query("""
          let $doc := jn:doc('json-path1','serving.jn')
          let $stats := jn:create-projection-index($doc, '/[]',
              ('/[]/kind', '/[]/did', '/[]/n'), ('string', 'string', 'long'))
          return {"revision": sdb:commit($doc)}
        """);
    final int global = ProjectionIndexBuilder.globalDictionaryColumnsBuilt();
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    return global;
  }

  @Test
  public void theHeuristicPicksGlobalForTheHighCardinalityColumnOnly() throws IOException {
    Assertions.assertEquals(1, buildUnderDefault(),
        "AUTO should have chosen a resource-wide dictionary for exactly one column — did, whose "
            + "per-leaf dictionaries store every value once per leaf and compress nothing — and left "
            + "kind's three repeated labels alone");
  }

  /** JSONBench Q4's shape: a high-cardinality group key, ordered by an aggregate and capped. */
  private static final String TOP_K = """
        subsequence(
          for $e in jn:doc('json-path1','serving.jn')[]
          where $e.kind eq 'commit'
          let $k := $e.did
          group by $k
          let $first := min($e.n)
          order by $first
          return {"did": $k, "first": $first}, 1, 3)
      """;

  /** JSONBench Q2's shape: a low-cardinality group key with a high-cardinality distinct operand. */
  private static final String PER_GROUP_DISTINCT = """
        for $e in jn:doc('json-path1','serving.jn')[]
        where $e.n ge 0
        let $k := $e.kind
        group by $k
        let $c := count($e)
        let $u := count(distinct-values($e.did))
        order by $c descending
        return {"kind": $k, "count": $c, "users": $u}
      """;

  @Test
  public void groupByAGlobalColumnIsServedByTheIntegerKernel() throws IOException {
    buildUnderDefault();
    withExecutor(TOP_K, (chain, ctx, generic) -> {
      SirixVectorizedExecutor.resetNumericGroupByServed();
      Assertions.assertEquals(generic, evaluateQuery(chain, ctx, TOP_K),
          "the integer-kernel group-by disagreed with the generic pipeline");
      Assertions.assertTrue(SirixVectorizedExecutor.numericGroupByServedCount() > 0,
          "a group-by on a global column must run through the integer kernel — a decline here means "
              + "the id key never reached the numeric arm, and the query silently costs what it did "
              + "before the encoding existed");
      // The keys must come back as the STRINGS they were, never as the ids the kernel grouped on.
      Assertions.assertTrue(generic.contains("did:plc:"),
          "the group keys were emitted as ids rather than reverse-mapped to their values: " + generic);
    });
  }

  @Test
  public void countDistinctOverAGlobalColumnIsServed() throws IOException {
    buildUnderDefault();
    withExecutor(PER_GROUP_DISTINCT, (chain, ctx, generic) -> {
      final long before = SirixVectorizedExecutor.groupDistinctServedCount();
      Assertions.assertEquals(generic, evaluateQuery(chain, ctx, PER_GROUP_DISTINCT),
          "distinct-over-ids disagreed with the generic pipeline");
      Assertions.assertTrue(SirixVectorizedExecutor.groupDistinctServedCount() > before,
          "the JSONBench Q2 shape — a per-leaf-dict group key with a GLOBAL distinct operand — must "
              + "be served; its ids go straight into the set, with no dictionary bytes read at all");
      // Every did is distinct, so each group's distinct count equals its row count. Exact by
      // construction here, unlike the per-leaf arm's content hashes, which are exact only up to a
      // hash collision.
      Assertions.assertTrue(generic.contains("\"count\":4000") && generic.contains("\"users\":4000"),
          "distinct counts disagreed with the row counts, so ids are not behaving as identities: " + generic);
    });
  }

  /**
   * ClickBench Q27's shape: a numeric group key, a string-length aggregate over a GLOBAL operand and
   * a non-empty predicate on that same operand — {@code AVG(length(URL)) … WHERE URL <> '' GROUP BY
   * CounterID}.
   */
  private static final String GLOBAL_LENGTH_AVG = """
        subsequence(
          for $e in jn:doc('json-path1','serving.jn')[]
          where $e.did != ""
          let $k := $e.n, $len := jn:utf8-length($e.did)
          group by $k
          let $c := count($e)
          let $l := xs:double(avg($len))
          order by $l descending, $k
          return {"n": $k, "l": $l, "c": $c}, 1, 5)
      """;

  @Test
  public void stringLengthOverAGlobalOperandIsServedFromColumnSlices() throws IOException {
    buildUnderDefault();
    withExecutor(GLOBAL_LENGTH_AVG, (chain, ctx, generic) -> {
      final long slicedBefore = SirixVectorizedExecutor.groupAggSlicedServedCount();
      final long servedBefore = SirixVectorizedExecutor.groupAggServedCount();
      Assertions.assertEquals(generic, evaluateQuery(chain, ctx, GLOBAL_LENGTH_AVG),
          "the global length fold disagreed with the generic pipeline");
      Assertions.assertTrue(SirixVectorizedExecutor.groupAggServedCount() > servedBefore,
          "a string-length aggregate over a global operand must be served by the group arm");
      // The length of a GLOBAL operand is a per-query id → length table indexed by the row's id lane;
      // nothing about it needs the whole-leaf payload. A tick missing here means the numeric arm fell
      // back to assembling every leaf — the 58 % of ClickBench Q27 this route exists to remove.
      Assertions.assertTrue(SirixVectorizedExecutor.groupAggSlicedServedCount() > slicedBefore,
          "the numeric arm read whole-leaf payloads for a global string-length operand instead of "
              + "folding the id→length table over column slices");
      Assertions.assertTrue(generic.contains("\"l\":"), "the average length lane is missing: " + generic);
    });
  }

  @Test
  public void equalityAgainstAGlobalColumnIsServedByOneProbe() throws IOException {
    buildUnderDefault();
    SirixVectorizedExecutor.resetProjectionCountsServed();
    final String hit =
        answer("count(for $e in jn:doc('json-path1','serving.jn')[] " + "where $e.did eq \"did:plc:4711\" return $e)");
    final String miss =
        answer("count(for $e in jn:doc('json-path1','serving.jn')[] " + "where $e.did eq \"did:plc:nope\" return $e)");
    Assertions.assertEquals("1", hit.trim());
    Assertions.assertEquals("0", miss.trim(),
        "a literal the dictionary provably lacks must answer zero exactly — not decline, and above " + "all not match");
    Assertions.assertTrue(SirixVectorizedExecutor.projectionCountsServed() > 0,
        "an equality over a global column must be served: the literal resolves to an id once and "
            + "every row is then an integer compare");
  }

  /**
   * Ordering by a global column must fall back, because ids are not the values' order.
   *
   * <p>
   * Ids are minted in first-seen order, which for this corpus is {@code did:plc:0, 1, 2 …}; string
   * order is {@code did:plc:0, did:plc:1, did:plc:10 …}. The two disagree at the third row, which is
   * what makes this a test of the decline rather than a coincidence.
   */
  @Test
  public void orderingByAGlobalColumnAnswersInValueOrder() throws IOException {
    buildUnderDefault();
    final String answer = answer("""
          subsequence(
            for $e in jn:doc('json-path1','serving.jn')[]
            order by $e.did
            return $e.did, 1, 3)
        """);
    Assertions.assertEquals("\"did:plc:0\" \"did:plc:1\" \"did:plc:10\"", answer.trim(),
        "ordering fell back to the ids' mint order instead of the values' string order");
  }

  /** What a test body does once an executor is installed, with the generic answer in hand. */
  @FunctionalInterface
  private interface ServedCheck {
    void run(SirixCompileChain chain, SirixQueryContext ctx, String generic) throws IOException;
  }

  /**
   * Evaluate {@code oracleQuery} with NO executor installed (the generic pipeline is the oracle),
   * then install one and hand both to {@code check}.
   *
   * <p>
   * The install is what wires the analytical routes in at all. Without it every group query takes the
   * generic pipeline, the answers are right, and a serving counter that never moves says nothing
   * about the route — which is exactly the trap this suite exists to avoid.
   */
  private void withExecutor(final String oracleQuery, final ServedCheck check) throws IOException {
    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup("json-path1");
      final JsonResourceSession session = collection.getDatabase().beginResourceSession("serving.jn");
      final String generic = evaluateQuery(chain, ctx, oracleQuery);
      final SirixVectorizedExecutor executor =
          new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 2);
      SequentialPipelineStrategy.setVectorizedExecutor(executor);
      try {
        check.run(chain, ctx, generic);
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        executor.close();
      }
    }
  }

  private static String evaluateQuery(final SirixCompileChain chain, final SirixQueryContext ctx, final String queryStr)
      throws IOException {
    try (final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final PrintWriter printWriter = new PrintWriter(out)) {
      new Query(chain, queryStr).serialize(ctx, printWriter);
      printWriter.flush();
      return out.toString();
    }
  }

  private String answer(final String q) throws IOException {
    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      return evaluateQuery(chain, ctx, q);
    }
  }
}
