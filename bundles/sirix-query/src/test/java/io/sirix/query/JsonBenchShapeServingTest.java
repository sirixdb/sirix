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
import java.util.HashMap;
import java.util.Map;

/**
 * Serving coverage for the three shapes the JSONBench queries need beyond the group-aggregate route
 * as it stood — each one a place where the whole pipeline used to decline:
 *
 * <ul>
 * <li><b>A nested-deref {@code where}.</b> Brackit's predicate leaves name a DIRECT
 * {@code $r.field}, so {@code $e.commit.operation = "create"} left the pipeline with no
 * representable predicate at all and the group-aggregate route declined on the filter-safety rule.
 * The chain-aware tree the detection stage now builds carries the same leaf kinds over
 * {@code '/'}-joined column paths.</li>
 * <li><b>{@code fn:string(<chain>)} as the group key.</b> The wrapper is how a SQL
 * {@code LowCardinality(String)} column is ported: an absent path reads as {@code ''}, not as no
 * value. The byte-identity-critical case is a STORED empty string beside absent rows — the
 * interpreter puts both in ONE group, so the kernel hashes {@code ""} in the dictionary's own
 * domain rather than keeping a side group.</li>
 * <li><b>{@code ($e.f idiv D) mod M} as a group key.</b> Integer date-part extraction, grouped on
 * the TRANSFORMED value (deliberately non-injective) and re-applied at emission.</li>
 * </ul>
 *
 * <p>
 * The fixture keeps four kinds of key-column absence apart, because they are exactly what the
 * {@code fn:string} substitution has to reproduce: a stored {@code ""} ({@code u:d}), a record with
 * a {@code commit} object but no {@code collection} in it ({@code u:g}), and two records with no
 * {@code commit} at all ({@code u:e}, {@code u:f}). All four belong to one {@code ''} group, and
 * the generic pipeline is the oracle for every assertion here.
 */
public final class JsonBenchShapeServingTest extends AbstractJsonTest {

  private static final String STORE =
      """
            jn:store('json-path1','jbshape.jn','[
              {"kind":"commit","commit":{"collection":"posts","operation":"create","rev":7},"did":"u:a","time_us":3600000001},
              {"kind":"commit","commit":{"collection":"posts","operation":"create","rev":9},"did":"u:b","time_us":7200000002},
              {"kind":"commit","commit":{"collection":"likes","operation":"create","rev":7},"did":"u:a","time_us":7200000003},
              {"kind":"commit","commit":{"collection":"likes","operation":"delete","rev":5},"did":"u:c","time_us":10800000004},
              {"kind":"commit","commit":{"collection":"","operation":"create","rev":7},"did":"u:d","time_us":3600000005},
              {"kind":"identity","did":"u:e","time_us":3600000006},
              {"kind":"account","did":"u:f","time_us":7200000007},
              {"kind":"commit","commit":{"operation":"create","rev":9},"did":"u:g","time_us":10800000008}
            ]')
          """;

  private static final String INDEX = """
        let $doc := jn:doc('json-path1','jbshape.jn')
        let $stats := jn:create-projection-index($doc, '/[]',
            ('/[]/kind', '/[]/did', '/[]/time_us', '/[]/commit/collection', '/[]/commit/operation', '/[]/commit/rev'),
            ('string', 'string', 'long', 'string', 'string', 'long'))
        return {"revision": sdb:commit($doc)}
      """;

  /** One microsecond hour, the divisor JSONBench's hour-of-day extraction uses. */
  private static final long MICROS_PER_HOUR = 3_600_000_000L;

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
  public void nestedDerefPredicatesServeAtEverySelectivity() throws IOException {
    query(STORE);
    query(INDEX);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();

    // COMMON (5 of 8 rows), the JSONBench Q2 filter verbatim.
    final String common = group("""
          where $e.kind = "commit" and $e.commit.operation = "create"
        """);
    // MID: one nested equality alone.
    final String mid = group("""
          where $e.commit.collection = "likes"
        """);
    // RARE: one row.
    final String rare = group("""
          where $e.commit.operation = "delete"
        """);
    // NONE: a literal no row carries — the answer is the empty sequence, and a route that dropped
    // the filter would answer every group instead. A wrong-answer bug that hides behind a common
    // literal shows up here.
    final String none = group("""
          where $e.commit.operation = "update"
        """);
    // An OR over the nested column (SQL's IN list) — a predicate TREE rather than a conjunction.
    final String orList = group("""
          where $e.kind = "commit"
            and ($e.commit.collection = "posts" or $e.commit.collection = "likes")
        """);
    // A NUMERIC leaf over a nested column, mixed with a string one.
    final String numeric = group("""
          where $e.commit.rev ge 7 and $e.commit.operation = "create"
        """);
    // Nested and DIRECT derefs in one predicate: Brackit represents the direct conjunct alone, so
    // this shape is the one where its tree must NOT be preferred over the chain-aware one.
    final String mixedDepth = group("""
          where $e.kind = "commit" and $e.commit.rev = 9
        """);

    withFixture((chain, ctx, executor) -> {
      assertServed(chain, ctx, common, "a nested create-commit filter");
      assertServed(chain, ctx, mid, "a single nested equality");
      assertServed(chain, ctx, rare, "a nested equality matching one row");
      assertServed(chain, ctx, none, "a nested equality matching no row");
      assertServed(chain, ctx, orList, "an OR list over a nested column");
      assertServed(chain, ctx, numeric, "a numeric comparison over a nested column");
      assertServed(chain, ctx, mixedDepth, "a nested and a direct conjunct together");
    });
  }

  @Test
  public void nestedDerefPredicateAnswersAreExact() throws IOException {
    query(STORE);
    query(INDEX);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();

    // Pinned answers, not just generic-parity: a filter that silently vanished would still agree
    // with a generic pipeline compiled from the SAME wrong annotation.
    withFixture((chain, ctx, executor) -> {
      Assertions.assertEquals("{\"event\":\"likes\",\"count\":1}", evaluateQuery(chain, ctx, group("""
            where $e.commit.operation = "delete"
          """)), "the delete commit is the only matching row");
      Assertions.assertEquals("", evaluateQuery(chain, ctx, group("""
            where $e.commit.operation = "update"
          """)), "no row carries operation=update");
      Assertions.assertEquals(
          "{\"event\":\"posts\",\"count\":1} {\"event\":\"likes\",\"count\":1} {\"event\":\"\",\"count\":1}",
          evaluateQuery(chain, ctx, group("""
                where $e.kind = "commit" and $e.commit.rev = 7 and $e.commit.operation = "create"
              """)), "the three rev=7 creates are u:a/posts, u:a/likes and u:d's stored empty collection — "
              + "equal counts, so the order is first appearance");
    });
  }

  @Test
  public void stringifiedKeyMergesAbsentRowsWithTheStoredEmptyString() throws IOException {
    query(STORE);
    query(INDEX);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();

    // The JSONBench Q1 shape: the whole corpus, no filter, key wrapped in fn:string.
    final String all = """
          for $e in jn:doc('json-path1','jbshape.jn')[]
          let $k := string($e.commit.collection)
          group by $k
          let $c := count($e)
          order by $c descending
          return {"event": $k, "count": $c}
        """;
    // SOME absent: the create-commit filter keeps u:d (stored "") and u:g (no collection) but drops
    // the two records with no commit object.
    final String someAbsent = """
          for $e in jn:doc('json-path1','jbshape.jn')[]
          where $e.commit.operation = "create"
          let $k := string($e.commit.collection)
          group by $k
          let $c := count($e)
          order by $c descending
          return {"event": $k, "count": $c}
        """;
    // NONE absent: every matching row has a non-empty collection, so the substitution never fires.
    final String noneAbsent = """
          for $e in jn:doc('json-path1','jbshape.jn')[]
          where $e.commit.collection = "posts" or $e.commit.collection = "likes"
          let $k := string($e.commit.collection)
          group by $k
          let $c := count($e)
          order by $c descending
          return {"event": $k, "count": $c}
        """;
    // ALL absent: one group, keyed "", over rows that have no collection at all.
    final String allAbsent = """
          for $e in jn:doc('json-path1','jbshape.jn')[]
          where $e.kind = "identity" or $e.kind = "account"
          let $k := string($e.commit.collection)
          group by $k
          let $c := count($e)
          order by $c descending
          return {"event": $k, "count": $c}
        """;
    // The stringified key beside a second key and an aggregate over a third column.
    final String withSecondKey = """
          for $e in jn:doc('json-path1','jbshape.jn')[]
          let $k := string($e.commit.collection), $op := $e.commit.operation
          group by $k, $op
          let $c := count($e)
          let $latest := max($e.time_us)
          order by $c descending
          return {"event": $k, "operation": $op, "count": $c, "latest": $latest}
        """;

    withFixture((chain, ctx, executor) -> {
      assertServed(chain, ctx, all, "a stringified key over the whole corpus");
      assertServed(chain, ctx, someAbsent, "a stringified key with some rows absent");
      assertServed(chain, ctx, noneAbsent, "a stringified key with no row absent");
      assertServed(chain, ctx, allAbsent, "a stringified key with every row absent");
      assertServed(chain, ctx, withSecondKey, "a stringified key beside a second key");
      // The merge itself: FOUR rows carry the empty key — a stored "", a commit without the field,
      // and two records without a commit object — and they form ONE group, exactly as the
      // interpreter's fn:string does. A side group for the absent rows would print two '' rows.
      Assertions.assertEquals(
          "{\"event\":\"\",\"count\":4} {\"event\":\"posts\",\"count\":2}" + " {\"event\":\"likes\",\"count\":2}",
          evaluateQuery(chain, ctx, all), "absent rows and the stored empty string must share one group");
    });
  }

  @Test
  public void divModKeysServeAndReApplyTheTransformAtEmission() throws IOException {
    query(STORE);
    query(INDEX);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();

    // The JSONBench Q3 shape: two group keys, the second computed, ordered by (hour, event) with
    // NO limit — the order specs read key components, which no in-kernel plan can compare, so the
    // route emits first-appearance order and the wrapper's stable sort finishes the job.
    final String hourAndCollection = """
          for $e in jn:doc('json-path1','jbshape.jn')[]
          where $e.kind = "commit" and $e.commit.operation = "create"
          let $k := $e.commit.collection, $hour := ($e.time_us idiv %d) mod 24
          group by $k, $hour
          let $c := count($e)
          order by $hour, $k
          return {"event": $k, "hour_of_day": $hour, "count": $c}
        """.formatted(MICROS_PER_HOUR);
    // The single-operation forms: a bare idiv, and a bare mod.
    final String bareIdiv = """
          for $e in jn:doc('json-path1','jbshape.jn')[]
          let $bucket := $e.time_us idiv %d
          group by $bucket
          let $c := count($e)
          order by $c descending
          return {"bucket": $bucket, "count": $c}
        """.formatted(MICROS_PER_HOUR);
    final String bareMod = """
          for $e in jn:doc('json-path1','jbshape.jn')[]
          let $parity := $e.time_us mod 2
          group by $parity
          let $c := count($e)
          order by $c descending
          return {"parity": $parity, "count": $c}
        """;
    // A NON-INJECTIVE transform whose collisions matter: `mod 2` over the hour buckets folds hours
    // 1 and 3 together, so grouping on the RAW column and transforming at emission would produce
    // more groups than the interpreter does.
    final String collidingBuckets = """
          for $e in jn:doc('json-path1','jbshape.jn')[]
          let $odd := ($e.time_us idiv %d) mod 2
          group by $odd
          let $c := count($e)
          order by $c descending
          return {"odd_hour": $odd, "count": $c}
        """.formatted(MICROS_PER_HOUR);
    // Under a LIMIT the order specs still read a key, so no plan resolves and the deferred-order
    // arm is unavailable (a top-K by first appearance would truncate by the wrong order): this one
    // must DECLINE rather than answer from the wrong prefix.
    final String cappedKeyOrder = """
          subsequence(
            for $e in jn:doc('json-path1','jbshape.jn')[]
            let $hour := ($e.time_us idiv %d) mod 24
            group by $hour
            let $c := count($e)
            order by $hour descending
            return {"hour_of_day": $hour, "count": $c}, 1, 2)
        """.formatted(MICROS_PER_HOUR);
    // Ordered by the COUNT under a cap: that plan does resolve, so the transform serves top-K.
    final String cappedCountOrder = """
          subsequence(
            for $e in jn:doc('json-path1','jbshape.jn')[]
            let $hour := ($e.time_us idiv %d) mod 24
            group by $hour
            let $c := count($e)
            order by $c descending
            return {"hour_of_day": $hour, "count": $c}, 1, 2)
        """.formatted(MICROS_PER_HOUR);

    withFixture((chain, ctx, executor) -> {
      assertServed(chain, ctx, hourAndCollection, "the JSONBench Q3 hour-of-day grouping");
      assertServed(chain, ctx, bareIdiv, "a bare idiv key");
      assertServed(chain, ctx, bareMod, "a bare mod key");
      assertServed(chain, ctx, collidingBuckets, "a colliding divmod key");
      assertDeclined(chain, ctx, cappedKeyOrder, "a capped order by a transformed key");
      assertServed(chain, ctx, cappedCountOrder, "a capped order by the count over a transformed key");
      // Pinned: hours 1, 2, 3 hold 2, 2, 1 create-commits, and the record carries the TRANSFORMED
      // hour, not the raw microsecond timestamp it was derived from.
      Assertions.assertEquals(
          "{\"event\":\"\",\"hour_of_day\":1,\"count\":1}" + " {\"event\":\"posts\",\"hour_of_day\":1,\"count\":1}"
              + " {\"event\":\"likes\",\"hour_of_day\":2,\"count\":1}"
              + " {\"event\":\"posts\",\"hour_of_day\":2,\"count\":1}"
              + " {\"event\":null,\"hour_of_day\":3,\"count\":1}",
          evaluateQuery(chain, ctx, hourAndCollection), "each create-commit falls in the hour its timestamp names");
    });
  }

  @Test
  public void unclaimedKeyWrappersStillAnswerFromTheGenericPipeline() throws IOException {
    query(STORE);
    query(INDEX);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();

    // xs:string is the CONSTRUCTOR, not fn:string: over a missing operand it yields the empty
    // sequence rather than "", so claiming it as the stringify transform would invent a group.
    final String xsString = """
          for $e in jn:doc('json-path1','jbshape.jn')[]
          let $k := xs:string($e.commit.collection)
          group by $k
          let $c := count($e)
          order by $c descending
          return {"event": $k, "count": $c}
        """;
    // A NEGATIVE divisor is not a date-part extraction, and a zero one raises in the interpreter.
    final String negativeDivisor = """
          for $e in jn:doc('json-path1','jbshape.jn')[]
          let $bucket := $e.time_us idiv -3600000000
          group by $bucket
          let $c := count($e)
          order by $c descending
          return {"bucket": $bucket, "count": $c}
        """;
    // fn:string over a NUMERIC column: the interpreter emits the lexical form ("3600000001"), the
    // kernel's numeric component emits the long — so the transform declines on the column kind.
    final String stringOfNumeric = """
          for $e in jn:doc('json-path1','jbshape.jn')[]
          let $k := string($e.time_us)
          group by $k
          let $c := count($e)
          order by $c descending
          return {"stamp": $k, "count": $c}
        """;

    withFixture((chain, ctx, executor) -> {
      assertDeclined(chain, ctx, xsString, "an xs:string key constructor");
      assertDeclined(chain, ctx, negativeDivisor, "a negative idiv divisor");
      assertDeclined(chain, ctx, stringOfNumeric, "fn:string over a numeric column");
    });
  }

  /** The group-by body every predicate case shares: one nested string key plus a count. */
  private static String group(final String where) {
    return """
        for $e in jn:doc('json-path1','jbshape.jn')[]
        %s
        let $k := $e.commit.collection
        group by $k
        let $c := count($e)
        order by $c descending
        return {"event": $k, "count": $c}
        """.formatted(where);
  }

  /** What a fixture case does with an open store, context and a bound executor. */
  private interface FixtureCase {
    void run(SirixCompileChain chain, SirixQueryContext ctx, SirixVectorizedExecutor executor) throws IOException;
  }

  /**
   * Runs {@code body} twice over the same fixture: first with NO executor bound, recording the
   * generic pipeline's answer for every query it evaluates, then with one bound, requiring the same
   * answer. Serving assertions compare against the recording, so the oracle is the interpreter and
   * never the annotation under test.
   */
  private void withFixture(final FixtureCase body) throws IOException {
    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup("json-path1");
      final JsonResourceSession session = collection.getDatabase().beginResourceSession("jbshape.jn");
      genericAnswers.clear();
      recording = true;
      try {
        body.run(chain, ctx, null);
      } finally {
        recording = false;
      }
      final SirixVectorizedExecutor executor =
          new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 2);
      SequentialPipelineStrategy.setVectorizedExecutor(executor);
      try {
        body.run(chain, ctx, executor);
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        executor.close();
      }
    }
  }

  /** Generic answers recorded in the first pass, keyed by query text. */
  private final Map<String, String> genericAnswers = new HashMap<>();

  /** True during the first pass, when no executor is bound and answers are being recorded. */
  private boolean recording;

  private void assertServed(final SirixCompileChain chain, final SirixQueryContext ctx, final String queryStr,
      final String what) throws IOException {
    if (recording) {
      genericAnswers.put(queryStr, evaluateQuery(chain, ctx, queryStr));
      return;
    }
    final long before = SirixVectorizedExecutor.groupAggServedCount();
    Assertions.assertEquals(genericAnswers.get(queryStr), evaluateQuery(chain, ctx, queryStr),
        what + " must answer exactly like the generic pipeline");
    Assertions.assertEquals(1L, SirixVectorizedExecutor.groupAggServedCount() - before,
        what + " must be SERVED from the projection");
  }

  private void assertDeclined(final SirixCompileChain chain, final SirixQueryContext ctx, final String queryStr,
      final String what) throws IOException {
    if (recording) {
      genericAnswers.put(queryStr, evaluateQuery(chain, ctx, queryStr));
      return;
    }
    final long before = SirixVectorizedExecutor.groupAggServedCount();
    Assertions.assertEquals(genericAnswers.get(queryStr), evaluateQuery(chain, ctx, queryStr),
        what + " must still be answered, by the generic pipeline");
    Assertions.assertEquals(0L, SirixVectorizedExecutor.groupAggServedCount() - before,
        what + " must DECLINE rather than serve a shape the kernel does not model");
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
