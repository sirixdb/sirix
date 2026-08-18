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
 * Serving coverage for the SPAN aggregate — {@code max(f) - min(f)}, and the whole-unit form
 * {@code (max(f) idiv k) - (min(f) idiv k)} that ClickHouse's
 * {@code dateDiff('millisecond', min(t), max(t))} over microsecond timestamps compiles to. This is
 * the JSONBench Q5 shape: {@code ORDER BY activity_span DESC LIMIT 3}. Q4's {@code ORDER BY min(t)}
 * already served; the difference needed its own order kind because no accumulator lane holds it.
 *
 * <p>
 * The fixture is built so that ordering by the MILLISECOND span and ordering by the RAW microsecond
 * span disagree — {@code u:a} spans 999_999 µs (999 ms) and {@code u:b} spans 999_001 µs (1000 ms),
 * because the divisor truncates each extremum, not the difference. Rewriting
 * {@code (max idiv k) - (min idiv k)} as {@code (max - min) idiv k} would therefore reorder the
 * answer, and both orders are pinned below so the two can never be conflated.
 */
public final class SpanOrderTopKServingTest extends AbstractJsonTest {

  private static final String STORE = """
        jn:store('json-path1','span.jn','[
          {"kind":"commit","did":"u:a","time_us":1000000,"n":1},
          {"kind":"commit","did":"u:a","time_us":1999999,"n":2},
          {"kind":"commit","did":"u:b","time_us":1000999,"n":3},
          {"kind":"commit","did":"u:b","time_us":2000000,"n":4},
          {"kind":"commit","did":"u:c","time_us":5000000,"n":5},
          {"kind":"commit","did":"u:d","time_us":7000000,"n":6},
          {"kind":"commit","did":"u:d","time_us":8000000,"n":7},
          {"kind":"identity","did":"u:e","time_us":9000000,"n":8},
          {"kind":"commit","did":"u:f","n":9}
        ]')
      """;

  private static final String INDEX = """
        let $doc := jn:doc('json-path1','span.jn')
        let $stats := jn:create-projection-index($doc, '/[]',
            ('/[]/kind', '/[]/did', '/[]/time_us', '/[]/n'),
            ('string', 'string', 'long', 'long'))
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
  public void spanOrderedTopKServesAndMatchesTheGenericPipeline() throws IOException {
    query(STORE);
    query(INDEX);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();

    // The JSONBench Q5 shape: min/max bound to their own post-group lets, the span taken over the
    // whole-unit divisor, ordered descending under a subsequence cap. The predicate is TOP-LEVEL
    // because this fixture is flat (nested-deref predicates have their own coverage in
    // JsonBenchShapeServingTest); its second clause keeps the operand-less `u:f` group out so the
    // pinned top-3 reaches the two groups whose millisecond and microsecond orders disagree. The
    // variants below keep `u:f` in, which is where the empty-span placement gets covered.
    final String msSpan = """
          subsequence(
            for $e in jn:doc('json-path1','span.jn')[]
            where $e.kind eq 'commit' and $e.time_us gt 0
            let $k := $e.did
            group by $k
            let $first := min($e.time_us)
            let $last := max($e.time_us)
            let $span := ($last idiv 1000) - ($first idiv 1000)
            order by $span descending
            return {"user_id": $k, "activity_span": $span}, 1, 3)
        """;
    // The RAW difference, written directly rather than through min/max lets.
    final String rawSpan = """
          subsequence(
            for $e in jn:doc('json-path1','span.jn')[]
            where $e.kind eq 'commit' and $e.time_us gt 0
            let $k := $e.did
            group by $k
            let $span := max($e.time_us) - min($e.time_us)
            order by $span descending
            return {"user_id": $k, "activity_span": $span}, 1, 3)
        """;
    // ASCENDING, with the operand-less `u:f` group included: its span is the empty sequence, and
    // empty placement follows the declared empty order, which the descending flag does NOT flip —
    // so the same end holds in both directions and the generic pipeline is the oracle for which.
    final String ascSpan = """
          subsequence(
            for $e in jn:doc('json-path1','span.jn')[]
            where $e.kind eq 'commit'
            let $k := $e.did
            group by $k
            let $span := max($e.time_us) - min($e.time_us)
            order by $span
            return {"user_id": $k, "activity_span": $span}, 1, 3)
        """;
    // UNCAPPED: no subsequence, so the in-kernel plan does not resolve and the legacy emission
    // arm answers — the span still has to be emitted correctly there and sorted downstream.
    final String uncapped = """
          for $e in jn:doc('json-path1','span.jn')[]
          where $e.kind eq 'commit'
          let $k := $e.did
          group by $k
          let $span := max($e.time_us) - min($e.time_us)
          order by $span descending
          return {"user_id": $k, "activity_span": $span}
        """;
    // The span written DIRECTLY in the return record, ordered by a different entry.
    final String inRecord = """
          subsequence(
            for $e in jn:doc('json-path1','span.jn')[]
            where $e.kind eq 'commit'
            let $k := $e.did
            group by $k
            let $c := count($e)
            order by $c descending
            return {"user_id": $k, "rows": $c, "activity_span": max($e.time_us) - min($e.time_us)}, 1, 3)
        """;
    // A span alongside the extrema it is built from: min/max entries read the SAME lanes.
    final String withExtrema = """
          subsequence(
            for $e in jn:doc('json-path1','span.jn')[]
            where $e.kind eq 'commit'
            let $k := $e.did
            group by $k
            let $first := min($e.time_us)
            let $last := max($e.time_us)
            let $span := $last - $first
            order by $span descending
            return {"user_id": $k, "first": $first, "last": $last, "activity_span": $span}, 1, 3)
        """;

    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup("json-path1");
      final JsonResourceSession session = collection.getDatabase().beginResourceSession("span.jn");
      final String genericMs = evaluateQuery(chain, ctx, msSpan);
      final String genericRaw = evaluateQuery(chain, ctx, rawSpan);
      final String genericAsc = evaluateQuery(chain, ctx, ascSpan);
      final String genericUncapped = evaluateQuery(chain, ctx, uncapped);
      final String genericInRecord = evaluateQuery(chain, ctx, inRecord);
      final String genericWithExtrema = evaluateQuery(chain, ctx, withExtrema);

      final SirixVectorizedExecutor executor =
          new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 2);
      SequentialPipelineStrategy.setVectorizedExecutor(executor);
      try {
        assertServed(chain, ctx, msSpan, genericMs, "the JSONBench Q5 millisecond span");
        assertServed(chain, ctx, rawSpan, genericRaw, "a raw microsecond span");
        assertServed(chain, ctx, ascSpan, genericAsc, "an ascending span order");
        assertServed(chain, ctx, uncapped, genericUncapped, "an uncapped span order");
        assertServed(chain, ctx, inRecord, genericInRecord, "a span written directly in the record");
        assertServed(chain, ctx, withExtrema, genericWithExtrema, "a span beside its own min and max");
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        executor.close();
      }

      // The two orders DISAGREE, which is the whole point of dividing each extremum: u:b spans
      // 999_001 us = 1000 ms while u:a spans 999_999 us = 999 ms. u:d spans exactly 1000 ms too,
      // so the millisecond order has a TIE at the top, broken by first appearance (u:b before u:d).
      Assertions.assertEquals("{\"user_id\":\"u:b\",\"activity_span\":1000}"
          + " {\"user_id\":\"u:d\",\"activity_span\":1000}" + " {\"user_id\":\"u:a\",\"activity_span\":999}",
          genericMs);
      Assertions.assertEquals("{\"user_id\":\"u:d\",\"activity_span\":1000000}"
          + " {\"user_id\":\"u:a\",\"activity_span\":999999}" + " {\"user_id\":\"u:b\",\"activity_span\":999001}",
          genericRaw);
    }
  }

  @Test
  public void singleGroupAndEmptyResultServe() throws IOException {
    query(STORE);
    query(INDEX);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();

    // One matching group, and the group whose operand is entirely absent: max(()) - min(()) is the
    // EMPTY sequence (serialized null), not the 0 an fn:count-shaped aggregate would give.
    final String oneGroup = """
          subsequence(
            for $e in jn:doc('json-path1','span.jn')[]
            where $e.did eq 'u:d'
            let $k := $e.did
            group by $k
            let $span := max($e.time_us) - min($e.time_us)
            order by $span descending
            return {"user_id": $k, "activity_span": $span}, 1, 3)
        """;
    final String allMissingOperand = """
          subsequence(
            for $e in jn:doc('json-path1','span.jn')[]
            where $e.did eq 'u:f'
            let $k := $e.did
            group by $k
            let $span := max($e.time_us) - min($e.time_us)
            order by $span descending
            return {"user_id": $k, "activity_span": $span}, 1, 3)
        """;
    // A predicate that matches nothing: the served sequence must be empty, not a phantom group.
    final String noRows = """
          subsequence(
            for $e in jn:doc('json-path1','span.jn')[]
            where $e.kind eq 'no-such-kind'
            let $k := $e.did
            group by $k
            let $span := max($e.time_us) - min($e.time_us)
            order by $span descending
            return {"user_id": $k, "activity_span": $span}, 1, 3)
        """;
    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup("json-path1");
      final JsonResourceSession session = collection.getDatabase().beginResourceSession("span.jn");
      final String genericOne = evaluateQuery(chain, ctx, oneGroup);
      final String genericMissing = evaluateQuery(chain, ctx, allMissingOperand);
      final String genericNone = evaluateQuery(chain, ctx, noRows);
      final SirixVectorizedExecutor executor =
          new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 2);
      SequentialPipelineStrategy.setVectorizedExecutor(executor);
      try {
        assertServed(chain, ctx, oneGroup, genericOne, "a single-group span");
        assertServed(chain, ctx, allMissingOperand, genericMissing, "a span over an all-missing operand");
        assertServed(chain, ctx, noRows, genericNone, "a span under an empty filter");
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        executor.close();
      }
      Assertions.assertEquals("{\"user_id\":\"u:d\",\"activity_span\":1000000}", genericOne);
      Assertions.assertEquals("{\"user_id\":\"u:f\",\"activity_span\":null}", genericMissing);
      Assertions.assertEquals("", genericNone);
    }
  }

  @Test
  public void differentOperandsAndDivisorsDecline() throws IOException {
    query(STORE);
    query(INDEX);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();

    // max over one field minus min over ANOTHER is not a span of anything the accumulator holds.
    final String crossField = """
          subsequence(
            for $e in jn:doc('json-path1','span.jn')[]
            where $e.kind eq 'commit'
            let $k := $e.did
            group by $k
            let $span := max($e.time_us) - min($e.n)
            order by $span descending
            return {"user_id": $k, "activity_span": $span}, 1, 3)
        """;
    // Two DIFFERENT divisors do not reduce to one scaled span.
    final String mixedDivisors = """
          subsequence(
            for $e in jn:doc('json-path1','span.jn')[]
            where $e.kind eq 'commit'
            let $k := $e.did
            group by $k
            let $first := min($e.time_us)
            let $last := max($e.time_us)
            let $span := ($last idiv 1000) - ($first idiv 100)
            order by $span descending
            return {"user_id": $k, "activity_span": $span}, 1, 3)
        """;
    // min - max is not a span: the sides are swapped, so the value is the negated one.
    final String reversed = """
          subsequence(
            for $e in jn:doc('json-path1','span.jn')[]
            where $e.kind eq 'commit'
            let $k := $e.did
            group by $k
            let $span := min($e.time_us) - max($e.time_us)
            order by $span descending
            return {"user_id": $k, "activity_span": $span}, 1, 3)
        """;
    // A cast-avg ORDER spec precomputes its double into the block's min lane — the very lane a
    // span over the same field reads. Serving both would emit a span computed from a double's raw
    // bits, so the plan must decline while the cast-avg-only sibling below still serves.
    final String castAvgSharingTheLane = """
          subsequence(
            for $e in jn:doc('json-path1','span.jn')[]
            where $e.kind eq 'commit'
            let $k := $e.did
            group by $k
            let $a := xs:double(avg($e.time_us))
            let $span := max($e.time_us) - min($e.time_us)
            order by $a descending
            return {"user_id": $k, "mean": $a, "activity_span": $span}, 1, 3)
        """;
    final String castAvgAlone = """
          subsequence(
            for $e in jn:doc('json-path1','span.jn')[]
            where $e.kind eq 'commit'
            let $k := $e.did
            group by $k
            let $a := xs:double(avg($e.time_us))
            order by $a descending
            return {"user_id": $k, "mean": $a}, 1, 3)
        """;
    final String served = """
          subsequence(
            for $e in jn:doc('json-path1','span.jn')[]
            where $e.kind eq 'commit'
            let $k := $e.did
            group by $k
            let $span := max($e.time_us) - min($e.time_us)
            order by $span descending
            return {"user_id": $k, "activity_span": $span}, 1, 3)
        """;
    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup("json-path1");
      final JsonResourceSession session = collection.getDatabase().beginResourceSession("span.jn");
      final String genericCross = evaluateQuery(chain, ctx, crossField);
      final String genericMixed = evaluateQuery(chain, ctx, mixedDivisors);
      final String genericReversed = evaluateQuery(chain, ctx, reversed);
      final String genericCastAvgShared = evaluateQuery(chain, ctx, castAvgSharingTheLane);
      final String genericCastAvgAlone = evaluateQuery(chain, ctx, castAvgAlone);
      final String genericServed = evaluateQuery(chain, ctx, served);
      final SirixVectorizedExecutor executor =
          new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 2);
      SequentialPipelineStrategy.setVectorizedExecutor(executor);
      try {
        assertDeclined(chain, ctx, crossField, genericCross, "max over one field minus min over another");
        assertDeclined(chain, ctx, mixedDivisors, genericMixed, "two different span divisors");
        assertDeclined(chain, ctx, reversed, genericReversed, "min minus max");
        // This one SERVES — but with the ORDER PLAN declined, not the route: a string group key
        // never qualifies for the in-kernel order plan, so grouping+aggregation serve and the
        // sort runs downstream in the generic pipeline. With no order plan there is no
        // writeCastAvgLanes call (every call site is orderPlan.-qualified), so the span's
        // min/max lanes stay extrema and the record is exact — which is what the equality
        // assertion proves. The lane-sharing hazard this test was written against only exists
        // WHEN a plan resolves, and GroupOrderPlan.resolve declines exactly that combination
        // (the castAvg guard scanning min/max/span entries over the same field).
        assertServed(chain, ctx, castAvgSharingTheLane, genericCastAvgShared,
            "a cast-avg order spec sharing the span's min lane (ordering deferred downstream)");
        assertServed(chain, ctx, castAvgAlone, genericCastAvgAlone, "the same cast-avg order without a span");
        // Same fixture, same executor: the real span still serves, so the declines above are the
        // recognizer's guards and not an unservable fixture.
        assertServed(chain, ctx, served, genericServed, "the well-formed span");
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        executor.close();
      }
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
        what + " must DECLINE rather than serve a value the accumulator does not hold");
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
