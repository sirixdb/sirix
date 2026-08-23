package io.sirix.query;

import io.brackit.query.Query;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.sirix.JsonTestHelper;
import io.sirix.api.json.JsonResourceSession;
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
 * A group's accumulator block is {@code [count, sum, min, max]} per aggregate column whatever the
 * query asked for. Folding and merging the SUM lane under {@link Math#addExact} therefore used to
 * decline a {@code min}-only query the moment some group's unrequested sum passed {@code 2^63} —
 * the query's own answer being perfectly representable.
 *
 * <p>
 * That is not a hypothetical. It took JSONBench Q4/Q5 ({@code group by did order by min(time_us)}
 * over Bluesky microsecond timestamps, ~1.75e15 each) off the served path at 100M rows and onto a
 * generic fallback that could not finish, while the same queries served at 10M: the threshold is
 * ~5,270 rows in one group, and the partition merge — which re-adds the workers' partial sums — is
 * where it was first crossed.
 *
 * <p>
 * The fixture puts three values of {@code 4e18} in one group, so its sum is {@code 1.2e19} and
 * overflows, while {@code min}, {@code max} and {@code count} over it are ordinary numbers. What is
 * pinned here is BOTH directions: those three serve, and a query that really does read the sum
 * still declines to the interpreter's promoting arithmetic rather than emitting a wrapped total.
 */
public final class UnreadSumLaneOverflowServingTest extends AbstractJsonTest {

  /**
   * {@code 4e18} is under {@link Long#MAX_VALUE} ({@code ~9.22e18}) so every individual cell is a
   * plain long, but any two of them already sum past it.
   */
  private static final String STORE = """
        jn:store('json-path1','overflow.jn','[
          {"kind":"commit","did":"u:a","bucket":1,"time_us":4000000000000000000,"n":1},
          {"kind":"commit","did":"u:a","bucket":1,"time_us":4000000000000000001,"n":2},
          {"kind":"commit","did":"u:a","bucket":1,"time_us":4000000000000000002,"n":3},
          {"kind":"commit","did":"u:b","bucket":2,"time_us":1000,"n":4},
          {"kind":"commit","did":"u:b","bucket":2,"time_us":2000,"n":5},
          {"kind":"commit","did":"u:c","bucket":3,"time_us":7000,"n":6},
          {"kind":"identity","did":"u:d","bucket":4,"time_us":9000,"n":7}
        ]')
      """;

  private static final String INDEX = """
        let $doc := jn:doc('json-path1','overflow.jn')
        let $stats := jn:create-projection-index($doc, '/[]',
            ('/[]/kind', '/[]/did', '/[]/bucket', '/[]/time_us', '/[]/n'),
            ('string', 'string', 'long', 'long', 'long'))
        return {"revision": sdb:commit($doc)}
      """;

  @BeforeEach
  public void clearProjectionStateBefore() {
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    SequentialPipelineStrategy.setVectorizedExecutor(null);
  }

  @AfterEach
  public void clearProjectionStateAfter() {
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    SequentialPipelineStrategy.setVectorizedExecutor(null);
  }

  @Test
  public void extremaOverAnOverflowingColumnServe() throws IOException {
    query(STORE);
    query(INDEX);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();

    // The JSONBench Q4 shape over a STRING group key (the per-leaf dict arm).
    final String minOnly = """
          subsequence(
            for $e in jn:doc('json-path1','overflow.jn')[]
            where $e.kind eq 'commit'
            let $k := $e.did
            group by $k
            let $first := min($e.time_us)
            order by $first
            return {"user_id": $k, "first": $first}, 1, 3)
        """;
    // The same over a NUMERIC group key, so the numeric single-key arm folds it too.
    final String minOnlyNumericKey = """
          subsequence(
            for $e in jn:doc('json-path1','overflow.jn')[]
            where $e.kind eq 'commit'
            let $k := $e.bucket
            group by $k
            let $first := min($e.time_us)
            order by $first
            return {"bucket": $k, "first": $first}, 1, 3)
        """;
    // Q5's span: max and min of the overflowing column, neither of which adds anything.
    final String span = """
          subsequence(
            for $e in jn:doc('json-path1','overflow.jn')[]
            where $e.kind eq 'commit'
            let $k := $e.did
            group by $k
            let $first := min($e.time_us)
            let $last := max($e.time_us)
            let $s := $last - $first
            order by $s descending
            return {"user_id": $k, "span": $s}, 1, 3)
        """;
    // count() reads the presence lane, never the sum.
    final String countOnly = """
          subsequence(
            for $e in jn:doc('json-path1','overflow.jn')[]
            where $e.kind eq 'commit'
            let $k := $e.did
            group by $k
            let $c := count($e)
            let $first := min($e.time_us)
            order by $c descending
            return {"user_id": $k, "rows": $c, "first": $first}, 1, 3)
        """;
    // MIXED: the overflowing column is read only by min, while a second column really is summed.
    // The summed lane stays exact — the skip is per column, not per query.
    final String mixed = """
          subsequence(
            for $e in jn:doc('json-path1','overflow.jn')[]
            where $e.kind eq 'commit'
            let $k := $e.did
            group by $k
            let $first := min($e.time_us)
            let $total := sum($e.n)
            order by $first
            return {"user_id": $k, "first": $first, "total": $total}, 1, 3)
        """;

    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup("json-path1");
      final JsonResourceSession session = collection.getDatabase().beginResourceSession("overflow.jn");
      final String genericMin = evaluateQuery(chain, ctx, minOnly);
      final String genericMinNumeric = evaluateQuery(chain, ctx, minOnlyNumericKey);
      final String genericSpan = evaluateQuery(chain, ctx, span);
      final String genericCount = evaluateQuery(chain, ctx, countOnly);
      final String genericMixed = evaluateQuery(chain, ctx, mixed);

      final SirixVectorizedExecutor executor =
          new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 2);
      SequentialPipelineStrategy.setVectorizedExecutor(executor);
      try {
        assertServed(chain, ctx, minOnly, genericMin, "min over a column whose group sum overflows");
        assertServed(chain, ctx, minOnlyNumericKey, genericMinNumeric, "the same under a numeric group key");
        assertServed(chain, ctx, span, genericSpan, "a span over a column whose group sum overflows");
        assertServed(chain, ctx, countOnly, genericCount, "count beside a min over an overflowing column");
        assertServed(chain, ctx, mixed, genericMixed, "a real sum beside a min over an overflowing column");
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        executor.close();
      }

      // The answers themselves, so a "served" that quietly returned the wrong extremum cannot pass.
      Assertions.assertEquals("{\"user_id\":\"u:b\",\"first\":1000} {\"user_id\":\"u:c\",\"first\":7000}"
          + " {\"user_id\":\"u:a\",\"first\":4000000000000000000}", genericMin);
      // u:a's sum is 1.2e19 — the very number the fold used to raise on — yet its span is 2.
      Assertions.assertEquals(
          "{\"user_id\":\"u:b\",\"span\":1000} {\"user_id\":\"u:a\",\"span\":2}" + " {\"user_id\":\"u:c\",\"span\":0}",
          genericSpan);
      Assertions.assertEquals(
          "{\"user_id\":\"u:b\",\"first\":1000,\"total\":9}" + " {\"user_id\":\"u:c\",\"first\":7000,\"total\":6}"
              + " {\"user_id\":\"u:a\",\"first\":4000000000000000000,\"total\":6}",
          genericMixed);
    }
  }

  @Test
  public void aSumThatReallyOverflowsStillDeclines() throws IOException {
    query(STORE);
    query(INDEX);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();

    // The guard is not gone, only narrowed to the lanes an answer reads: this one reads the sum,
    // so it must reach the interpreter's promoting arithmetic instead of a wrapped long.
    final String sumOverflows = """
          subsequence(
            for $e in jn:doc('json-path1','overflow.jn')[]
            where $e.kind eq 'commit'
            let $k := $e.did
            group by $k
            let $total := sum($e.time_us)
            order by $total descending
            return {"user_id": $k, "total": $total}, 1, 3)
        """;
    final String avgOverflows = """
          subsequence(
            for $e in jn:doc('json-path1','overflow.jn')[]
            where $e.kind eq 'commit'
            let $k := $e.did
            group by $k
            let $mean := avg($e.time_us)
            order by $mean descending
            return {"user_id": $k, "mean": $mean}, 1, 3)
        """;
    // Same fixture, same executor: min still serves, so the declines above are the guard and not
    // an unservable fixture.
    final String served = """
          subsequence(
            for $e in jn:doc('json-path1','overflow.jn')[]
            where $e.kind eq 'commit'
            let $k := $e.did
            group by $k
            let $first := min($e.time_us)
            order by $first
            return {"user_id": $k, "first": $first}, 1, 3)
        """;
    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup("json-path1");
      final JsonResourceSession session = collection.getDatabase().beginResourceSession("overflow.jn");
      final String genericSum = evaluateQuery(chain, ctx, sumOverflows);
      final String genericAvg = evaluateQuery(chain, ctx, avgOverflows);
      final String genericServed = evaluateQuery(chain, ctx, served);
      final SirixVectorizedExecutor executor =
          new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 2);
      SequentialPipelineStrategy.setVectorizedExecutor(executor);
      try {
        assertDeclined(chain, ctx, sumOverflows, genericSum, "a sum that really does overflow a long");
        assertDeclined(chain, ctx, avgOverflows, genericAvg, "an avg whose sum overflows a long");
        assertServed(chain, ctx, served, genericServed, "the min-only sibling");
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        executor.close();
      }
      // The interpreter answers with the promoted value, which is what declining buys.
      Assertions.assertTrue(genericSum.contains("12000000000000000003"),
          "the generic pipeline must answer the promoted sum, got: " + genericSum);
    }
  }

  private static void assertServed(final SirixCompileChain chain, final SirixQueryContext ctx, final String queryStr,
      final String expected, final String what) throws IOException {
    final long before = SirixVectorizedExecutor.groupAggServedCount();
    Assertions.assertEquals(expected, evaluateQuery(chain, ctx, queryStr),
        what + " must fold exactly like the generic pipeline");
    Assertions.assertEquals(1L, SirixVectorizedExecutor.groupAggServedCount() - before,
        what + " must be SERVED from the projection");
  }

  private static void assertDeclined(final SirixCompileChain chain, final SirixQueryContext ctx, final String queryStr,
      final String expected, final String what) throws IOException {
    final long before = SirixVectorizedExecutor.groupAggServedCount();
    Assertions.assertEquals(expected, evaluateQuery(chain, ctx, queryStr),
        what + " must still be answered, by the generic pipeline");
    Assertions.assertEquals(0L, SirixVectorizedExecutor.groupAggServedCount() - before,
        what + " must DECLINE rather than emit a wrapped total");
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
}
