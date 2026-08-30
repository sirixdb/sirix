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
 * Serving coverage for grouped {@code count(distinct-values(<STRING field>))} — the JSONBench Q2
 * shape ({@code uniqExact(did)} per collection). The numeric distinct operand already served; a
 * STRING_DICT one used to decline because dict ids are LEAF-LOCAL and unioning them across leaves
 * would count nonsense. The kernels now feed the set each entry's 64-bit CONTENT hash instead, so
 * identity is exact up to a hash collision — the same standard the composite group-key identity
 * already accepts.
 *
 * <p>
 * Every case is differential against the generic pipeline AND counter-proven, because a wrong
 * distinct identity produces a plausible-looking number: too small (ids colliding across leaves) or
 * too large (the same value counted once per leaf). Cardinality is swept deliberately — a
 * leaf-local-id bug is invisible when every group has one distinct value.
 */
public final class StringDistinctGroupServingTest extends AbstractJsonTest {

  /**
   * JSONBench-shaped: a nested string group key ({@code commit.collection}), a top-level string
   * distinct operand ({@code did}), a NESTED one ({@code commit.rkey}) and a numeric one ({@code n}).
   * The three collections deliberately span the cardinality regimes — post has 4 rows over 3 distinct
   * dids (repeats), like has 3 rows over 3 (all distinct), follow has 1 row. {@code rkey} rather than
   * a second {@code did}: column lookup is by trailing field name, so a projection cannot declare two
   * columns whose last path step is the same.
   */
  private static final String STORE =
      """
            jn:store('json-path1','dids.jn','[
              {"kind":"commit","commit":{"collection":"app.bsky.feed.post","operation":"create","rkey":"r1"},"did":"did:a","n":1},
              {"kind":"commit","commit":{"collection":"app.bsky.feed.post","operation":"create","rkey":"r2"},"did":"did:b","n":2},
              {"kind":"commit","commit":{"collection":"app.bsky.feed.post","operation":"create","rkey":"r1"},"did":"did:a","n":3},
              {"kind":"commit","commit":{"collection":"app.bsky.feed.post","operation":"delete","rkey":"r3"},"did":"did:c","n":4},
              {"kind":"commit","commit":{"collection":"app.bsky.feed.like","operation":"create","rkey":"r4"},"did":"did:a","n":5},
              {"kind":"commit","commit":{"collection":"app.bsky.feed.like","operation":"create","rkey":"r4"},"did":"did:d","n":6},
              {"kind":"commit","commit":{"collection":"app.bsky.feed.like","operation":"create","rkey":"r5"},"did":"did:e","n":7},
              {"kind":"identity","commit":{"collection":"app.bsky.graph.follow","operation":"create","rkey":"r6"},"did":"did:a","n":8}
            ]')
          """;

  private static final String INDEX = """
        let $doc := jn:doc('json-path1','dids.jn')
        let $stats := jn:create-projection-index($doc, '/[]',
            ('/[]/kind', '/[]/did', '/[]/n', '/[]/commit/collection', '/[]/commit/operation', '/[]/commit/rkey'),
            ('string', 'string', 'long', 'string', 'string', 'string'))
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
  public void stringDistinctOverAStringGroupKeyServesAndMatchesTheGenericPipeline() throws IOException {
    query(STORE);
    query(INDEX);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();

    // The JSONBench Q2 shape: nested string group key, top-level string distinct operand, a
    // predicate, order by count descending. The nested-predicate spelling of the same shape is
    // covered by nestedPredicateServesWithAndWithoutADistinctEntry below.
    final String q2 = """
          for $e in jn:doc('json-path1','dids.jn')[]
          where $e.kind eq 'commit'
          let $k := $e.commit.collection
          group by $k
          let $c := count($e)
          let $u := count(distinct-values($e.did))
          order by $c descending
          return {"event": $k, "count": $c, "users": $u}
        """;
    // Same, capped: the flat route's heap-select arm rather than its emit-everything arm.
    final String capped = """
          subsequence(
            for $e in jn:doc('json-path1','dids.jn')[]
            where $e.kind eq 'commit'
            let $k := $e.commit.collection
            group by $k
            let $c := count($e)
            let $u := count(distinct-values($e.did))
            order by $u descending
            return {"event": $k, "count": $c, "users": $u}, 1, 2)
        """;
    // Unpredicated, ordered by the DISTINCT count itself — the order plan reads the distinct
    // block's sum lane, so a wrong set size would also reorder the answer.
    final String byDistinct = """
          for $e in jn:doc('json-path1','dids.jn')[]
          let $k := $e.commit.collection
          group by $k
          let $u := count(distinct-values($e.did))
          order by $u descending
          return {"event": $k, "users": $u}
        """;
    // A NESTED distinct operand chain: $e.commit.rkey resolves to /[]/commit/rkey.
    final String nestedOperand = """
          for $e in jn:doc('json-path1','dids.jn')[]
          let $k := $e.commit.collection
          group by $k
          let $u := count(distinct-values($e.commit.rkey))
          order by $u descending
          return {"event": $k, "rkeys": $u}
        """;
    // A string distinct operand alongside NUMERIC aggregates: the distinct block rides as an
    // extra accumulator block whose lanes stay zero, so the sum/min lanes must be untouched.
    final String withNumericAggs = """
          for $e in jn:doc('json-path1','dids.jn')[]
          let $k := $e.commit.collection
          group by $k
          let $u := count(distinct-values($e.did))
          order by $u descending
          return {"event": $k, "users": $u, "total": sum($e.n), "lowest": min($e.n)}
        """;
    // The group key IS the distinct operand: the two dict memos must not share state.
    final String keyIsOperand = """
          for $e in jn:doc('json-path1','dids.jn')[]
          let $k := $e.commit.collection
          group by $k
          let $u := count(distinct-values($e.commit.collection))
          order by $u descending
          return {"event": $k, "users": $u}
        """;

    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup("json-path1");
      final JsonResourceSession session = collection.getDatabase().beginResourceSession("dids.jn");
      final String genericQ2 = evaluateQuery(chain, ctx, q2);
      final String genericCapped = evaluateQuery(chain, ctx, capped);
      final String genericByDistinct = evaluateQuery(chain, ctx, byDistinct);
      final String genericNested = evaluateQuery(chain, ctx, nestedOperand);
      final String genericNumeric = evaluateQuery(chain, ctx, withNumericAggs);
      final String genericKeyIsOperand = evaluateQuery(chain, ctx, keyIsOperand);

      final SirixVectorizedExecutor executor =
          new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 2);
      SequentialPipelineStrategy.setVectorizedExecutor(executor);
      try {
        assertDistinctServed(chain, ctx, q2, genericQ2, "the JSONBench Q2 shape");
        assertDistinctServed(chain, ctx, capped, genericCapped, "a capped string count-distinct");
        assertDistinctServed(chain, ctx, byDistinct, genericByDistinct, "ordering ON the distinct count");
        assertDistinctServed(chain, ctx, nestedOperand, genericNested, "a nested distinct operand chain");
        assertDistinctServed(chain, ctx, withNumericAggs, genericNumeric,
            "a string distinct operand beside numeric aggregates");
        assertDistinctServed(chain, ctx, keyIsOperand, genericKeyIsOperand,
            "the group key used as its own distinct operand");
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        executor.close();
      }

      // Pin the values, not just the parity: over the 7 `commit` rows post has 4 rows spanning
      // {a,b,c} = 3 distinct dids and like 3 rows spanning {a,d,e} = 3. A cross-leaf id union
      // would undercount here, one-set-per-leaf would overcount.
      Assertions.assertEquals("{\"event\":\"app.bsky.feed.post\",\"count\":4,\"users\":3}"
          + " {\"event\":\"app.bsky.feed.like\",\"count\":3,\"users\":3}", genericQ2);
      // Unpredicated: post has 4 rows over {a,b,c} = 3, like 3 over {a,d,e} = 3, follow 1 over {a}.
      Assertions.assertEquals("{\"event\":\"app.bsky.feed.post\",\"users\":3}"
          + " {\"event\":\"app.bsky.feed.like\",\"users\":3}" + " {\"event\":\"app.bsky.graph.follow\",\"users\":1}",
          genericByDistinct);
      // A group's own key is ONE distinct value, whatever the row count.
      Assertions.assertEquals("{\"event\":\"app.bsky.feed.post\",\"users\":1}"
          + " {\"event\":\"app.bsky.feed.like\",\"users\":1}" + " {\"event\":\"app.bsky.graph.follow\",\"users\":1}",
          genericKeyIsOperand);
    }
  }

  @Test
  public void stringDistinctOverANumericGroupKeyServes() throws IOException {
    // The NUMERIC single-key flat arm's twin of the same extension: a long group key with a
    // string distinct operand. Cardinality is swept across the three groups (1 / 2 / 3 distinct).
    query("""
          jn:store('json-path1','numkey.jn','[
            {"g": 1, "tag": "x"},
            {"g": 1, "tag": "x"},
            {"g": 1, "tag": "x"},
            {"g": 2, "tag": "x"},
            {"g": 2, "tag": "y"},
            {"g": 3, "tag": "x"},
            {"g": 3, "tag": "y"},
            {"g": 3, "tag": "z"}
          ]')
        """);
    query("""
          let $doc := jn:doc('json-path1','numkey.jn')
          let $stats := jn:create-projection-index($doc, '/[]', ('/[]/g', '/[]/tag'), ('long', 'string'))
          return {"revision": sdb:commit($doc)}
        """);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();

    final String q = """
          for $r in jn:doc('json-path1','numkey.jn')[]
          let $g := $r.g
          group by $g
          let $u := count(distinct-values($r.tag))
          order by $u descending
          return {"g": $g, "u": $u}
        """;
    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup("json-path1");
      final JsonResourceSession session = collection.getDatabase().beginResourceSession("numkey.jn");
      final String generic = evaluateQuery(chain, ctx, q);
      final SirixVectorizedExecutor executor =
          new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 2);
      SequentialPipelineStrategy.setVectorizedExecutor(executor);
      try {
        assertDistinctServed(chain, ctx, q, generic, "a string distinct operand under a numeric key");
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        executor.close();
      }
      Assertions.assertEquals("{\"g\":3,\"u\":3} {\"g\":2,\"u\":2} {\"g\":1,\"u\":1}", generic);
    }
  }

  @Test
  public void anAbsentOperandContributesNothingAndStillServes() throws IOException {
    // A row MISSING the operand field: distinct-values(()) contributes nothing, which is exactly
    // what the kernels' presence skip does — so a presence hole SERVES rather than declining.
    // (Absence is not the same as an unrepresentable cell; the null case below is.)
    query("""
          jn:store('json-path1','holes.jn','[
            {"g": "a", "tag": "x"},
            {"g": "a"},
            {"g": "a", "tag": "y"},
            {"g": "b"},
            {"g": "b"},
            {"g": "c", "tag": "x"}
          ]')
        """);
    query("""
          let $doc := jn:doc('json-path1','holes.jn')
          let $stats := jn:create-projection-index($doc, '/[]', ('/[]/g', '/[]/tag'), ('string', 'string'))
          return {"revision": sdb:commit($doc)}
        """);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();

    final String q = """
          for $r in jn:doc('json-path1','holes.jn')[]
          let $g := $r.g
          group by $g
          let $u := count(distinct-values($r.tag))
          order by $u descending
          return {"g": $g, "u": $u}
        """;
    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup("json-path1");
      final JsonResourceSession session = collection.getDatabase().beginResourceSession("holes.jn");
      final String generic = evaluateQuery(chain, ctx, q);
      final SirixVectorizedExecutor executor =
          new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 2);
      SequentialPipelineStrategy.setVectorizedExecutor(executor);
      try {
        assertDistinctServed(chain, ctx, q, generic, "an operand absent on some rows");
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        executor.close();
      }
      // Group b has no present operand at all: fn:count over the empty distinct-values sequence
      // is 0, never the empty sequence min/avg would produce.
      Assertions.assertEquals("{\"g\":\"a\",\"u\":2} {\"g\":\"c\",\"u\":1} {\"g\":\"b\",\"u\":0}", generic);
    }
  }

  @Test
  public void aNullBearingOperandDeclinesAndTheGenericPipelineAnswers() throws IOException {
    // A JSON null in the operand column is an UNREPRESENTABLE cell: distinct-values counts null as
    // a value, the kernels read it as missing. columnSparseClean is the gate that keeps that
    // divergence out — so the route must DECLINE, and the same fixture's null-free sibling column
    // must still serve (proving the decline is the gate, not an unservable fixture).
    query("""
          jn:store('json-path1','nulls.jn','[
            {"g": "a", "tag": "x", "clean": "x"},
            {"g": "a", "tag": null, "clean": "y"},
            {"g": "a", "tag": "y", "clean": "y"},
            {"g": "b", "tag": "x", "clean": "x"}
          ]')
        """);
    query("""
          let $doc := jn:doc('json-path1','nulls.jn')
          let $stats := jn:create-projection-index($doc, '/[]',
              ('/[]/g', '/[]/tag', '/[]/clean'), ('string', 'string', 'string'))
          return {"revision": sdb:commit($doc)}
        """);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();

    final String nullBearing = """
          for $r in jn:doc('json-path1','nulls.jn')[]
          let $g := $r.g
          group by $g
          let $u := count(distinct-values($r.tag))
          order by $u descending
          return {"g": $g, "u": $u}
        """;
    final String cleanColumn = """
          for $r in jn:doc('json-path1','nulls.jn')[]
          let $g := $r.g
          group by $g
          let $u := count(distinct-values($r.clean))
          order by $u descending
          return {"g": $g, "u": $u}
        """;
    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup("json-path1");
      final JsonResourceSession session = collection.getDatabase().beginResourceSession("nulls.jn");
      final String genericNull = evaluateQuery(chain, ctx, nullBearing);
      final String genericClean = evaluateQuery(chain, ctx, cleanColumn);
      final SirixVectorizedExecutor executor =
          new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 2);
      SequentialPipelineStrategy.setVectorizedExecutor(executor);
      try {
        final long before = SirixVectorizedExecutor.groupDistinctServedCount();
        Assertions.assertEquals(genericNull, evaluateQuery(chain, ctx, nullBearing),
            "the generic pipeline must answer the null-bearing operand identically");
        Assertions.assertEquals(0L, SirixVectorizedExecutor.groupDistinctServedCount() - before,
            "a null-bearing distinct operand must DECLINE, not be served");
        assertDistinctServed(chain, ctx, cleanColumn, genericClean, "the null-free sibling column in the same fixture");
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        executor.close();
      }
    }
  }

  @Test
  public void theByteKernelTwinServesTheSameStringDistinctAnswer() throws IOException {
    // The sliced kernel is only HALF the route: after SLICED_PROMOTE_AFTER serves the handle
    // materializes its leaves and the contiguous BYTE kernels take over for good. Both twins
    // therefore have to carry the string-distinct identity — a sliced-only implementation would
    // start answering differently mid-suite, which is the worst possible failure mode. Repeat the
    // query until a serve comes back NOT sliced (promotion is asynchronous) and check parity on
    // every single run, not only the last.
    query(STORE);
    query(INDEX);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();

    final String q = """
          for $e in jn:doc('json-path1','dids.jn')[]
          let $k := $e.commit.collection
          group by $k
          let $u := count(distinct-values($e.did))
          order by $u descending
          return {"event": $k, "users": $u}
        """;
    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup("json-path1");
      final JsonResourceSession session = collection.getDatabase().beginResourceSession("dids.jn");
      final String generic = evaluateQuery(chain, ctx, q);
      final SirixVectorizedExecutor executor =
          new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 2);
      SequentialPipelineStrategy.setVectorizedExecutor(executor);
      try {
        boolean sawByteKernel = false;
        for (int i = 0; i < 40 && !sawByteKernel; i++) {
          final long aggBefore = SirixVectorizedExecutor.groupAggServedCount();
          final long slicedBefore = SirixVectorizedExecutor.groupAggSlicedServedCount();
          Assertions.assertEquals(generic, evaluateQuery(chain, ctx, q),
              "run " + i + " must fold exactly like the generic pipeline");
          Assertions.assertEquals(1L, SirixVectorizedExecutor.groupAggServedCount() - aggBefore,
              "run " + i + " must be served from the projection");
          sawByteKernel = SirixVectorizedExecutor.groupAggSlicedServedCount() - slicedBefore == 0L;
        }
        Assertions.assertTrue(sawByteKernel,
            "the handle never promoted past the sliced kernel, so the BYTE twin went untested");
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        executor.close();
      }
    }
  }

  @Test
  public void nestedPredicateServesWithAndWithoutADistinctEntry() throws IOException {
    // A NESTED deref in a `where` clause used to decline the whole group-aggregate serve: nested
    // chains resolved for group keys and aggregate operands but not for PREDICATE fields, because
    // Brackit's predicate leaves name a direct `$r.field` and its absent annotation is what the
    // filter-safety rule keys on. The detection stage now builds its own chain-aware tree, so the
    // verbatim JSONBench Q2/Q3/Q5 predicates (`data.commit.operation = 'create'`) serve — with a
    // distinct entry and without one, since the predicate was never a property of the operand.
    query(STORE);
    query(INDEX);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();

    final String nestedPredicateWithDistinct = """
          for $e in jn:doc('json-path1','dids.jn')[]
          where $e.commit.operation eq 'create'
          let $k := $e.commit.collection
          group by $k
          let $u := count(distinct-values($e.did))
          order by $u descending
          return {"event": $k, "users": $u}
        """;
    final String nestedPredicateNoDistinct = """
          for $e in jn:doc('json-path1','dids.jn')[]
          where $e.commit.operation eq 'create'
          let $k := $e.commit.collection
          group by $k
          let $c := count($e)
          order by $c descending
          return {"event": $k, "count": $c}
        """;
    final String topLevelPredicate = """
          for $e in jn:doc('json-path1','dids.jn')[]
          where $e.kind eq 'commit'
          let $k := $e.commit.collection
          group by $k
          let $u := count(distinct-values($e.did))
          order by $u descending
          return {"event": $k, "users": $u}
        """;
    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup("json-path1");
      final JsonResourceSession session = collection.getDatabase().beginResourceSession("dids.jn");
      final String genericDistinct = evaluateQuery(chain, ctx, nestedPredicateWithDistinct);
      final String genericNoDistinct = evaluateQuery(chain, ctx, nestedPredicateNoDistinct);
      final String genericTopLevel = evaluateQuery(chain, ctx, topLevelPredicate);
      final SirixVectorizedExecutor executor =
          new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 2);
      SequentialPipelineStrategy.setVectorizedExecutor(executor);
      try {
        assertDistinctServed(chain, ctx, nestedPredicateWithDistinct, genericDistinct,
            "a nested predicate field beside a distinct entry");
        final long aggBefore = SirixVectorizedExecutor.groupAggServedCount();
        Assertions.assertEquals(genericNoDistinct, evaluateQuery(chain, ctx, nestedPredicateNoDistinct),
            "the nested-predicate count query must answer exactly like the generic pipeline");
        Assertions.assertEquals(1L, SirixVectorizedExecutor.groupAggServedCount() - aggBefore,
            "a nested predicate field serves with no distinct entry either");
        // Same fixture, same executor, same nested key and operand — only the predicate differs.
        assertDistinctServed(chain, ctx, topLevelPredicate, genericTopLevel, "a TOP-LEVEL predicate field");
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        executor.close();
      }
    }
  }

  /**
   * Run {@code queryStr} under the wired executor: it must match {@code expected}, be served, and be
   * served through the DISTINCT route specifically (not some other arm that happened to fire).
   */
  private static void assertDistinctServed(final SirixCompileChain chain, final SirixQueryContext ctx,
      final String queryStr, final String expected, final String what) throws IOException {
    final long aggBefore = SirixVectorizedExecutor.groupAggServedCount();
    final long cdBefore = SirixVectorizedExecutor.groupDistinctServedCount();
    Assertions.assertEquals(expected, evaluateQuery(chain, ctx, queryStr),
        what + " must fold exactly like the generic pipeline");
    Assertions.assertEquals(1L, SirixVectorizedExecutor.groupAggServedCount() - aggBefore,
        what + " must be SERVED from the projection");
    Assertions.assertEquals(1L, SirixVectorizedExecutor.groupDistinctServedCount() - cdBefore,
        what + " must be served through the COUNT(DISTINCT) route");
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
