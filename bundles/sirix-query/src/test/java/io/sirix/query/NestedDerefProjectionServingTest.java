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
 * Serving coverage for group-aggregate pipelines whose group keys / aggregate operands are NESTED
 * derefs off the loop variable ({@code $r.commit.collection}, {@code $r.record.reply.root}) — the
 * JSONBench shape. Two properties are under test, and the second is the reason the first is safe:
 *
 * <ol>
 * <li>a nested deref chain is DETECTED and SERVED from the projection column declared at that path
 * (counter-proven, byte-identical to the generic pipeline), and
 * <li>the match is PATH-aware: a column's declared path — not its trailing name — is what a chain
 * is matched against, so a nested {@code /[]/commit/collection} column can never answer a top-level
 * {@code $r.collection} deref, nor a top-level {@code /[]/collection} column a nested
 * {@code $r.commit.collection} one. Both must fall back to the generic pipeline; serving either
 * would be a silent wrong answer.
 * </ol>
 *
 * Each decline case runs on a fixture whose MATCHING query serves, so the assertion proves the path
 * guard declined and not that the fixture was unservable to begin with.
 */
public final class NestedDerefProjectionServingTest extends AbstractJsonTest {

  /** Records with the JSONBench nesting: kind + commit.{collection,operation}, mixed selectivity. */
  private static final String NESTED_STORE =
      """
            jn:store('json-path1','nested.jn','[
              {"kind": "commit",   "commit": {"collection": "app.bsky.feed.post",   "operation": "create", "size": 10}, "n": 1},
              {"kind": "commit",   "commit": {"collection": "app.bsky.feed.like",   "operation": "create", "size": 20}, "n": 2},
              {"kind": "commit",   "commit": {"collection": "app.bsky.feed.post",   "operation": "delete", "size": 30}, "n": 3},
              {"kind": "identity", "commit": {"collection": "app.bsky.graph.follow","operation": "create", "size": 40}, "n": 4},
              {"kind": "commit",   "commit": {"collection": "app.bsky.feed.post",   "operation": "create", "size": 50}, "n": 5},
              {"kind": "commit",   "n": 6}
            ]')
          """;

  private static final String NESTED_INDEX = """
        let $doc := jn:doc('json-path1','nested.jn')
        let $stats := jn:create-projection-index($doc, '/[]',
            ('/[]/kind', '/[]/commit/collection', '/[]/commit/operation', '/[]/commit/size', '/[]/n'),
            ('string', 'string', 'string', 'long', 'long'))
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
  public void nestedGroupKeysServeAndMatchTheGenericPipeline() throws IOException {
    query(NESTED_STORE);
    query(NESTED_INDEX);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();

    // Unfiltered, filtered by a TOP-LEVEL field (the JSONBench shape), and a two-key nested
    // grouping. Selectivity varies across the corpus (3/1/1 plus a record missing the nested
    // field, which is the null-key group).
    final String plain = """
          let $doc := jn:doc('json-path1','nested.jn')
          for $r in $doc[]
          let $c := $r.commit.collection
          group by $c
          return {"collection": $c, "n": count($r), "total": sum($r.n)}
        """;
    final String filtered = """
          let $doc := jn:doc('json-path1','nested.jn')
          for $r in $doc[]
          where $r.kind eq 'commit'
          let $c := $r.commit.collection
          group by $c
          return {"collection": $c, "n": count($r), "total": sum($r.n)}
        """;
    final String twoKeys = """
          let $doc := jn:doc('json-path1','nested.jn')
          for $r in $doc[]
          where $r.kind eq 'commit'
          let $c := $r.commit.collection
          let $o := $r.commit.operation
          group by $c, $o
          return {"collection": $c, "operation": $o, "n": count($r), "total": sum($r.n)}
        """;
    // A nested AGGREGATE operand with a top-level key: the operand side of the same extension.
    final String nestedAggregate = """
          let $doc := jn:doc('json-path1','nested.jn')
          for $r in $doc[]
          let $k := $r.kind
          group by $k
          return {"kind": $k, "n": count($r), "bytes": sum($r.commit.size), "biggest": max($r.commit.size)}
        """;
    // A post-group count let + order by over a nested key: the ordered emission arm.
    final String ordered = """
          let $doc := jn:doc('json-path1','nested.jn')
          for $r in $doc[]
          let $c := $r.commit.collection
          group by $c
          let $cnt := count($r)
          order by $cnt descending
          return {"collection": $c, "cnt": $cnt}
        """;

    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup("json-path1");
      final JsonResourceSession session = collection.getDatabase().beginResourceSession("nested.jn");
      final String genericPlain = evaluateQuery(chain, ctx, plain);
      final String genericFiltered = evaluateQuery(chain, ctx, filtered);
      final String genericTwoKeys = evaluateQuery(chain, ctx, twoKeys);
      final String genericNestedAgg = evaluateQuery(chain, ctx, nestedAggregate);
      final String genericOrdered = evaluateQuery(chain, ctx, ordered);

      final SirixVectorizedExecutor executor =
          new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 2);
      SequentialPipelineStrategy.setVectorizedExecutor(executor);
      try {
        assertServed(chain, ctx, plain, genericPlain, "nested group key");
        assertServed(chain, ctx, filtered, genericFiltered, "nested group key under a top-level predicate");
        assertServed(chain, ctx, twoKeys, genericTwoKeys, "two nested group keys");
        assertServed(chain, ctx, nestedAggregate, genericNestedAgg, "nested aggregate operand");
        assertServed(chain, ctx, ordered, genericOrdered, "ordered nested group key");
        // Document first-appearance order, null-key group last (the record without `commit`).
        Assertions.assertEquals("{\"collection\":\"app.bsky.feed.post\",\"n\":3,\"total\":9}"
            + " {\"collection\":\"app.bsky.feed.like\",\"n\":1,\"total\":2}"
            + " {\"collection\":\"app.bsky.graph.follow\",\"n\":1,\"total\":4}"
            + " {\"collection\":null,\"n\":1,\"total\":6}", genericPlain);
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        executor.close();
      }
    }
  }

  @Test
  public void threeStepDerefChainServes() throws IOException {
    query("""
          jn:store('json-path1','deep.jn','[
            {"record": {"reply": {"root": "aaa"}}, "n": 1},
            {"record": {"reply": {"root": "bbb"}}, "n": 2},
            {"record": {"reply": {"root": "aaa"}}, "n": 3},
            {"record": {"reply": {}}, "n": 4},
            {"n": 5}
          ]')
        """);
    query("""
          let $doc := jn:doc('json-path1','deep.jn')
          let $stats := jn:create-projection-index($doc, '/[]',
              ('/[]/record/reply/root', '/[]/n'), ('string', 'long'))
          return {"revision": sdb:commit($doc)}
        """);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();

    final String deep = """
          let $doc := jn:doc('json-path1','deep.jn')
          for $r in $doc[]
          let $k := $r.record.reply.root
          group by $k
          return {"root": $k, "c": count($r), "s": sum($r.n)}
        """;
    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup("json-path1");
      final JsonResourceSession session = collection.getDatabase().beginResourceSession("deep.jn");
      final String generic = evaluateQuery(chain, ctx, deep);
      final SirixVectorizedExecutor executor =
          new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 2);
      SequentialPipelineStrategy.setVectorizedExecutor(executor);
      try {
        final long servedBefore = SirixVectorizedExecutor.groupAggServedCount();
        Assertions.assertEquals(generic, evaluateQuery(chain, ctx, deep),
            "a three-step deref chain must fold exactly like the generic pipeline");
        Assertions.assertEquals(1L, SirixVectorizedExecutor.groupAggServedCount() - servedBefore,
            "the three-step chain must be SERVED from the projection");
        // A missing LEAF and a missing INTERMEDIATE both yield the empty key: one null group.
        Assertions.assertEquals("{\"root\":\"aaa\",\"c\":2,\"s\":4} {\"root\":\"bbb\",\"c\":1,\"s\":2}"
            + " {\"root\":null,\"c\":2,\"s\":9}", generic);
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        executor.close();
      }
    }
  }

  @Test
  public void topLevelDerefIsNotServedFromANestedColumn() throws IOException {
    query(NESTED_STORE);
    query(NESTED_INDEX);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();

    // The projection's only `collection` column is declared at /[]/commit/collection. A query
    // grouping by the TOP-LEVEL $r.collection asks for a field no record has: every row belongs
    // to the null-key group. Serving it from the nested column would answer with the nested
    // values — the wrong answer this guard exists to prevent.
    final String topLevel = """
          let $doc := jn:doc('json-path1','nested.jn')
          for $r in $doc[]
          let $c := $r.collection
          group by $c
          return {"collection": $c, "n": count($r), "total": sum($r.n)}
        """;
    final String nested = """
          let $doc := jn:doc('json-path1','nested.jn')
          for $r in $doc[]
          let $c := $r.commit.collection
          group by $c
          return {"collection": $c, "n": count($r), "total": sum($r.n)}
        """;
    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup("json-path1");
      final JsonResourceSession session = collection.getDatabase().beginResourceSession("nested.jn");
      final String generic = evaluateQuery(chain, ctx, topLevel);
      final SirixVectorizedExecutor executor =
          new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 2);
      SequentialPipelineStrategy.setVectorizedExecutor(executor);
      try {
        final long servedBefore = SirixVectorizedExecutor.groupAggServedCount();
        Assertions.assertEquals(generic, evaluateQuery(chain, ctx, topLevel),
            "a top-level deref must not be answered from the same-named NESTED column");
        Assertions.assertEquals(0L, SirixVectorizedExecutor.groupAggServedCount() - servedBefore,
            "the path guard must DECLINE, not serve, the top-level deref");
        Assertions.assertEquals("{\"collection\":null,\"n\":6,\"total\":21}", generic,
            "no record carries a top-level `collection`: one null-key group over all six rows");
        // Same fixture, same executor: the matching NESTED query does serve — so the decline
        // above is the path guard, not an unservable fixture.
        evaluateQuery(chain, ctx, nested);
        Assertions.assertEquals(1L, SirixVectorizedExecutor.groupAggServedCount() - servedBefore,
            "the matching nested query must still be served from the same projection");
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        executor.close();
      }
    }
  }

  @Test
  public void nestedDerefIsNotServedFromATopLevelColumn() throws IOException {
    query("""
          jn:store('json-path1','flat.jn','[
            {"kind": "commit", "collection": "app.bsky.feed.post", "n": 1},
            {"kind": "commit", "collection": "app.bsky.feed.like", "n": 2},
            {"kind": "commit", "collection": "app.bsky.feed.post", "n": 3}
          ]')
        """);
    query("""
          let $doc := jn:doc('json-path1','flat.jn')
          let $stats := jn:create-projection-index($doc, '/[]',
              ('/[]/kind', '/[]/collection', '/[]/n'), ('string', 'string', 'long'))
          return {"revision": sdb:commit($doc)}
        """);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();

    final String nested = """
          let $doc := jn:doc('json-path1','flat.jn')
          for $r in $doc[]
          let $c := $r.commit.collection
          group by $c
          return {"collection": $c, "n": count($r), "total": sum($r.n)}
        """;
    final String topLevel = """
          let $doc := jn:doc('json-path1','flat.jn')
          for $r in $doc[]
          let $c := $r.collection
          group by $c
          return {"collection": $c, "n": count($r), "total": sum($r.n)}
        """;
    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup("json-path1");
      final JsonResourceSession session = collection.getDatabase().beginResourceSession("flat.jn");
      final String generic = evaluateQuery(chain, ctx, nested);
      final SirixVectorizedExecutor executor =
          new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 2);
      SequentialPipelineStrategy.setVectorizedExecutor(executor);
      try {
        final long servedBefore = SirixVectorizedExecutor.groupAggServedCount();
        Assertions.assertEquals(generic, evaluateQuery(chain, ctx, nested),
            "a nested deref must not be answered from the same-named TOP-LEVEL column");
        Assertions.assertEquals(0L, SirixVectorizedExecutor.groupAggServedCount() - servedBefore,
            "the path guard must DECLINE, not serve, the nested deref");
        Assertions.assertEquals("{\"collection\":null,\"n\":3,\"total\":6}", generic,
            "no record carries a `commit` object: one null-key group over all three rows");
        evaluateQuery(chain, ctx, topLevel);
        Assertions.assertEquals(1L, SirixVectorizedExecutor.groupAggServedCount() - servedBefore,
            "the matching top-level query must still be served from the same projection");
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        executor.close();
      }
    }
  }

  /** Run {@code queryStr} under the wired executor: it must match {@code expected} AND be served. */
  private static void assertServed(final SirixCompileChain chain, final SirixQueryContext ctx, final String queryStr,
      final String expected, final String what) throws IOException {
    final long before = SirixVectorizedExecutor.groupAggServedCount();
    Assertions.assertEquals(expected, evaluateQuery(chain, ctx, queryStr),
        what + " must fold exactly like the generic pipeline");
    Assertions.assertEquals(1L, SirixVectorizedExecutor.groupAggServedCount() - before,
        what + " must be SERVED from the projection");
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
