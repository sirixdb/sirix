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
 * the generic pipeline is the oracle for every assertion here but one: a single plain key ordered
 * by its count alone orders equal counts by key on the projection, an order the interpreter leaves
 * open, so those answers are pinned instead.
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

  /**
   * Collection counts that stay pairwise distinct, with and without {@code fn:string}, before and
   * after the first record's {@code posts} becomes {@code likes}: posts 7, likes 4, stored "" 1,
   * absent 2.
   */
  private static final String UNTIED_STORE = """
        jn:store('json-path1','jbshape.jn','[
          {"commit":{"collection":"posts"}}, {"commit":{"collection":"likes"}}, {"commit":{"collection":"posts"}},
          {}, {"commit":{"collection":"posts"}}, {"commit":{"collection":"likes"}}, {"commit":{"collection":""}},
          {"commit":{"collection":"posts"}}, {"commit":{}}, {"commit":{"collection":"posts"}},
          {"commit":{"collection":"likes"}}, {"commit":{"collection":"posts"}}, {"commit":{"collection":"likes"}},
          {"commit":{"collection":"posts"}}
        ]')
      """;

  /**
   * Equal counts whose first appearances are out of key order. Plain key: likes 3, absent 2, posts 2,
   * then "", a, b, U+FF21 and U+1F600 once each. With {@code fn:string} the two absent records join
   * the stored "" for 3 beside likes 3. U+1F600 is a surrogate pair, so UTF-16 unit order would put
   * it before U+FF21, where codepoint order puts it after.
   */
  private static final String TIED_STORE = """
        jn:store('json-path1','jbshape.jn','[
          {"commit":{"collection":"likes"}}, {"commit":{"collection":"b"}}, {"commit":{"collection":"posts"}},
          {"commit":{"collection":"\\uFF21"}}, {}, {"commit":{"collection":"likes"}}, {"commit":{"collection":""}},
          {"commit":{"collection":"\\uD83D\\uDE00"}}, {"commit":{"collection":"a"}}, {"commit":{}},
          {"commit":{"collection":"posts"}}, {"commit":{"collection":"likes"}}
        ]')
      """;

  /**
   * {@link #TIED_STORE} by count descending, then by key: the absent group first, then codepoints.
   */
  private static final String TIED_ORDER = "{\"event\":\"likes\",\"count\":3} {\"event\":null,\"count\":2}"
      + " {\"event\":\"posts\",\"count\":2} {\"event\":\"\",\"count\":1} {\"event\":\"a\",\"count\":1}"
      + " {\"event\":\"b\",\"count\":1} {\"event\":\"\uFF21\",\"count\":1} {\"event\":\"\uD83D\uDE00\",\"count\":1}";

  /**
   * {@link #TIED_STORE} under {@code fn:string}: the merged "" is an ordinary string among its ties.
   */
  private static final String TIED_STRINGIFIED_ORDER = "{\"event\":\"\",\"count\":3} {\"event\":\"likes\",\"count\":3}"
      + " {\"event\":\"posts\",\"count\":2} {\"event\":\"a\",\"count\":1} {\"event\":\"b\",\"count\":1}"
      + " {\"event\":\"\uFF21\",\"count\":1} {\"event\":\"\uD83D\uDE00\",\"count\":1}";

  private static final String STRINGIFIED_GROUP = """
        for $e in jn:doc('json-path1','jbshape.jn')[]
        let $k := string($e.commit.collection)
        group by $k
        let $c := count($e)
        order by $c descending
        return {"event": $k, "count": $c}
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
  public void unfilteredCollectionCountUsesRevisionedScalarSummary() throws IOException {
    query(UNTIED_STORE);
    query(INDEX);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    assertSummaryServesUnfilteredCount();

    query("""
          let $doc := jn:doc('json-path1','jbshape.jn')
          return replace json value of $doc[0].commit.collection with "likes"
        """);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    assertSummaryServesUnfilteredCount();
  }

  private void assertSummaryServesUnfilteredCount() throws IOException {
    withFixture((chain, ctx, executor) -> {
      final long before = SirixVectorizedExecutor.groupAggSummaryServedCount();
      assertServed(chain, ctx, group(""), "unfiltered collection count");
      if (!recording) {
        Assertions.assertEquals(1L, SirixVectorizedExecutor.groupAggSummaryServedCount() - before,
            "unfiltered count should read the persisted summary");
        Assertions.assertEquals(0L, ProjectionIndexCatalog.dataCacheSize(),
            "summary serving must not hydrate the row-group handle");
      }
      final long beforeStringified = SirixVectorizedExecutor.groupAggSummaryServedCount();
      assertServed(chain, ctx, STRINGIFIED_GROUP, "stringified unfiltered collection count");
      if (!recording) {
        Assertions.assertEquals(1L, SirixVectorizedExecutor.groupAggSummaryServedCount() - beforeStringified,
            "missing and empty values should merge from the persisted summary");
        Assertions.assertEquals(0L, ProjectionIndexCatalog.dataCacheSize(),
            "stringified summary serving must not hydrate the row-group handle");
      }
    });
  }

  @Test
  public void tiedScalarSummaryCountsOrderByKey() throws IOException {
    query(TIED_STORE);
    query(INDEX);
    final int revision = mostRecentRevision();
    Assertions.assertEquals(TIED_ORDER, servedAt(revision, group(""), true, "tied unfiltered collection count"),
        "equal counts order by key, the absent group first");
    Assertions.assertEquals(TIED_STRINGIFIED_ORDER,
        servedAt(revision, STRINGIFIED_GROUP, true, "tied stringified unfiltered collection count"),
        "the absent records' \"\" group orders among equal counts as the empty string");
  }

  /**
   * The same records answered once from the persisted per-value counts and once by scanning the row
   * groups. A value too long for the summary's bounded encoding withdraws the summary for good, so
   * writing one and then restoring the original value leaves a later revision with the original
   * records and no summary.
   */
  @Test
  public void tiedCountsOrderAlikeFromTheSummaryAndTheRowGroups() throws IOException {
    query(TIED_STORE);
    query(INDEX);
    final int summaryRevision = mostRecentRevision();
    replaceFirstCollection("x".repeat(4096));
    replaceFirstCollection("likes");
    final int rowGroupRevision = mostRecentRevision();

    for (final boolean stringified : new boolean[] {false, true}) {
      final String what = stringified
          ? "tied stringified collection count"
          : "tied collection count";
      Assertions.assertEquals(evaluateGeneric(countAt(summaryRevision, stringified)),
          evaluateGeneric(countAt(rowGroupRevision, stringified)), what + ": both revisions hold the same records");
      final String fromSummary = servedAt(summaryRevision, countAt(summaryRevision, stringified), true, what);
      final String fromRowGroups = servedAt(rowGroupRevision, countAt(rowGroupRevision, stringified), false, what);
      Assertions.assertEquals(fromSummary, fromRowGroups, what + ": the summary and the row groups order ties alike");
      Assertions.assertEquals(stringified
          ? TIED_STRINGIFIED_ORDER
          : TIED_ORDER, fromRowGroups, what + ": count descending, then key ascending");
    }
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
      // The merge itself: FOUR rows carry the empty key — a stored "", a commit without the field,
      // and two records without a commit object — and they form ONE group, exactly as the
      // interpreter's fn:string does. A side group for the absent rows would print two '' rows.
      assertServedAs(chain, ctx, all,
          "{\"event\":\"\",\"count\":4} {\"event\":\"likes\",\"count\":2} {\"event\":\"posts\",\"count\":2}",
          "absent rows and the stored empty string must share one group; equal counts order by key");
      assertServed(chain, ctx, someAbsent, "a stringified key with some rows absent");
      assertServed(chain, ctx, noneAbsent, "a stringified key with no row absent");
      assertServed(chain, ctx, allAbsent, "a stringified key with every row absent");
      assertServed(chain, ctx, withSecondKey, "a stringified key beside a second key");
    });
  }

  @Test
  public void lowCardinalityStringifiedKeyPreservesExactMissingAndStringGroups() throws IOException {
    final String[] records =
        {"{}", "{\"commit\":{}}", "{\"commit\":{\"collection\":\"\"}}", "{\"commit\":{\"collection\":\"null\"}}",
            "{\"commit\":{\"collection\":\"β\"}}", "{\"commit\":{\"collection\":\"alpha\"}}"};
    final StringBuilder data = new StringBuilder(8192).append('[');
    for (int row = 0; row < 240; row++) {
      if (row != 0) {
        data.append(',');
      }
      data.append(records[row % records.length]);
    }
    data.append(']');
    query("jn:store('json-path1','jbshape.jn','" + data + "')");
    query(INDEX);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    final String all = """
        for $e in jn:doc('json-path1','jbshape.jn')[]
        let $k := string($e.commit.collection)
        group by $k
        let $c := count($e)
        order by $c descending
        return {"event": $k, "count": $c}
        """;
    withFixture((chain, ctx, executor) -> assertServedAs(chain, ctx, all,
        "{\"event\":\"\",\"count\":120} {\"event\":\"alpha\",\"count\":40}"
            + " {\"event\":\"null\",\"count\":40} {\"event\":\"β\",\"count\":40}",
        "the low-cardinality Q1 compiler path"));
  }

  @Test
  public void lowCardinalityDictionaryTransformsKeepTheirComputedIdentity() throws IOException {
    final String[] values = {"10", "11", "20", "21"};
    final StringBuilder data = new StringBuilder(8192).append('[');
    for (int row = 0; row < 240; row++) {
      if (row != 0) {
        data.append(',');
      }
      data.append("{\"commit\":{\"collection\":\"").append(values[row % values.length]).append("\"}}");
    }
    data.append(']');
    query("jn:store('json-path1','jbshape.jn','" + data + "')");
    query(INDEX);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    withFixture((chain, ctx, executor) -> {
      for (final int offset : new int[] {0, 3}) {
        final String key = "xs:integer(substring($e.commit.collection, 1, 1))" + (offset == 0
            ? ""
            : " + 3");
        final String transformed = """
            for $e in jn:doc('json-path1','jbshape.jn')[]
            let $k := %s
            group by $k
            let $c := count($e)
            order by $c descending
            return {"event": $k, "count": $c}
            """.formatted(key);
        if (offset == 0) {
          assertServed(chain, ctx, transformed, "a nonzero substring transform");
        } else {
          assertDeclined(chain, ctx, transformed, "the unsupported arithmetic over a substring cast");
        }
        Assertions.assertEquals(
            "{\"event\":" + (1 + offset) + ",\"count\":120} {\"event\":" + (2 + offset) + ",\"count\":120}",
            evaluateQuery(chain, ctx, transformed));
      }
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

  /**
   * The JSONBench Q1 count over one pinned revision, the key wrapped in {@code fn:string} if asked.
   */
  private static String countAt(final int revision, final boolean stringified) {
    return """
        for $e in jn:doc('json-path1','jbshape.jn',%d)[]
        let $k := %s
        group by $k
        let $c := count($e)
        order by $c descending
        return {"event": $k, "count": $c}
        """.formatted(revision, stringified
        ? "string($e.commit.collection)"
        : "$e.commit.collection");
  }

  private void replaceFirstCollection(final String value) {
    query("""
        let $doc := jn:doc('json-path1','jbshape.jn')
        return replace json value of $doc[0].commit.collection with "%s"
        """.formatted(value));
  }

  private static int mostRecentRevision() {
    try (final BasicJsonDBStore store =
        BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build()) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup("json-path1");
      try (final JsonResourceSession session = collection.getDatabase().beginResourceSession("jbshape.jn")) {
        return session.getMostRecentRevisionNumber();
      }
    }
  }

  /**
   * Evaluates {@code queryStr} with an executor bound to {@code revision}, requiring the group
   * aggregate to be served from the projection: from the persisted per-value counts when
   * {@code fromSummary}, from the row groups otherwise.
   */
  private static String servedAt(final int revision, final String queryStr, final boolean fromSummary,
      final String what) throws IOException {
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup("json-path1");
      try (final JsonResourceSession session = collection.getDatabase().beginResourceSession("jbshape.jn")) {
        final SirixVectorizedExecutor executor = new SirixVectorizedExecutor(session, revision, 2);
        SequentialPipelineStrategy.setVectorizedExecutor(executor);
        try {
          final long served = SirixVectorizedExecutor.groupAggServedCount();
          final long summary = SirixVectorizedExecutor.groupAggSummaryServedCount();
          final String answer = evaluateQuery(chain, ctx, queryStr);
          Assertions.assertEquals(1L, SirixVectorizedExecutor.groupAggServedCount() - served,
              what + " must be SERVED from the projection");
          Assertions.assertEquals(fromSummary
              ? 1L
              : 0L, SirixVectorizedExecutor.groupAggSummaryServedCount() - summary,
              what + (fromSummary
                  ? " must read the persisted per-value counts"
                  : " must scan the row groups"));
          return answer;
        } finally {
          SequentialPipelineStrategy.setVectorizedExecutor(null);
          executor.close();
        }
      }
    }
  }

  /** The interpreter's answer, with no executor bound or auto-wired. */
  private static String evaluateGeneric(final String queryStr) throws IOException {
    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store)) {
      return evaluateQuery(chain, ctx, queryStr);
    }
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

  /**
   * Requires {@code queryStr} to be SERVED with exactly {@code expected}, for a count order whose
   * ties the projection orders by key. The interpreter leaves the order of equal counts open, so its
   * answer is no oracle for them.
   */
  private void assertServedAs(final SirixCompileChain chain, final SirixQueryContext ctx, final String queryStr,
      final String expected, final String what) throws IOException {
    if (recording) {
      return;
    }
    final long before = SirixVectorizedExecutor.groupAggServedCount();
    Assertions.assertEquals(expected, evaluateQuery(chain, ctx, queryStr), what);
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
