package io.sirix.query;

import io.brackit.query.Query;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.sirix.JsonTestHelper;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.projection.ProjectionIndexCatalog;
import io.sirix.index.projection.ProjectionIndexRegistry;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.node.BasicXmlDBStore;
import io.sirix.query.scan.SirixVectorizedExecutor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * End-to-end coverage of the EXECUTOR-SIDE catalog serving path: the plain
 * function tests run the generic pipeline (no vectorized executor is wired
 * through {@code AbstractJsonTest}), so this test wires a
 * {@link SirixVectorizedExecutor} explicitly and asserts — via
 * {@link ProjectionIndexCatalog#servedCount()} — that a query is actually
 * SERVED from the catalogued projection after re-open, not merely answered
 * correctly by the fallback pipeline.
 */
public final class ProjectionIndexCatalogServingTest extends AbstractJsonTest {

  @BeforeEach
  public void clearProjectionStateBefore() {
    ProjectionIndexRegistry.clear();
    SequentialPipelineStrategy.setVectorizedExecutor(null);
  }

  @AfterEach
  public void clearProjectionStateAfter() {
    ProjectionIndexRegistry.clear();
    SequentialPipelineStrategy.setVectorizedExecutor(null);
  }

  @Test
  public void catalogServesAggregateThroughTheVectorizedExecutor() throws IOException {
    query("""
          jn:store('json-path1','sales.jn','[
            {"age": 30, "active": true,  "dept": "Eng"},
            {"age": 45, "active": false, "dept": "Sales"},
            {"age": 52, "active": true,  "dept": "Eng"},
            {"age": 23, "active": true,  "dept": "HR"},
            {"age": 61, "active": false, "dept": "Eng"}
          ]')
        """);
    query("""
          let $doc := jn:doc('json-path1','sales.jn')
          let $stats := jn:create-projection-index($doc, '/[]',
              ('/[]/age', '/[]/active', '/[]/dept'),
              ('long', 'boolean', 'string'))
          return {"revision": sdb:commit($doc)}
        """);
    // Simulate a fresh process: no in-memory state — discovery must go
    // through the revision-scoped catalog + pages.
    ProjectionIndexRegistry.clear();

    try (final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
         final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
         final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup("json-path1");
      final JsonResourceSession session = collection.getDatabase().beginResourceSession("sales.jn");
      final SirixVectorizedExecutor executor =
          new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 2);
      SequentialPipelineStrategy.setVectorizedExecutor(executor);
      try {
        final long servedBefore = ProjectionIndexCatalog.servedCount();
        try (final ByteArrayOutputStream out = new ByteArrayOutputStream();
             final PrintWriter printWriter = new PrintWriter(out)) {
          new Query(chain, """
                let $doc := jn:doc('json-path1','sales.jn')
                return sum(for $r in $doc[] return $r.age)
              """).serialize(ctx, printWriter);
          printWriter.flush();
          Assertions.assertEquals("211", out.toString());
        }
        Assertions.assertTrue(ProjectionIndexCatalog.servedCount() > servedBefore,
            "the aggregate must be SERVED from the catalogued projection, not the fallback");
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        executor.close();
      }
    }
  }

  @Test
  public void incrementallyMaintainedProjectionStillServesAfterUpdateInsertDelete() throws IOException {
    // The change listener patches the persisted leaves at commit time
    // (update → leaf rebuild, insert → tail append, delete → row drop), so
    // after three maintenance commits the SAME catalogued projection —
    // never re-created — must still SERVE the aggregate with the compounded
    // values, proven by the servedCount delta (a tombstoned projection
    // would fall back and leave the counter unchanged).
    query("""
          jn:store('json-path1','sales.jn','[
            {"age": 30, "active": true,  "dept": "Eng"},
            {"age": 45, "active": false, "dept": "Sales"},
            {"age": 52, "active": true,  "dept": "Eng"},
            {"age": 23, "active": true,  "dept": "HR"},
            {"age": 61, "active": false, "dept": "Eng"}
          ]')
        """);
    query("""
          let $doc := jn:doc('json-path1','sales.jn')
          let $stats := jn:create-projection-index($doc, '/[]',
              ('/[]/age', '/[]/active', '/[]/dept'),
              ('long', 'boolean', 'string'))
          return {"revision": sdb:commit($doc)}
        """);
    query("""
          let $doc := jn:doc('json-path1','sales.jn')
          return replace json value of $doc[0].age with 99
        """);
    query("""
          let $doc := jn:doc('json-path1','sales.jn')
          return append json {"age": 7, "active": true, "dept": "Eng"} into $doc
        """);
    query("""
          let $doc := jn:doc('json-path1','sales.jn')
          return delete json $doc[1]
        """);
    ProjectionIndexRegistry.clear();

    try (final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
         final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
         final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup("json-path1");
      final JsonResourceSession session = collection.getDatabase().beginResourceSession("sales.jn");
      final SirixVectorizedExecutor executor =
          new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 2);
      SequentialPipelineStrategy.setVectorizedExecutor(executor);
      try {
        final long servedBefore = ProjectionIndexCatalog.servedCount();
        try (final ByteArrayOutputStream out = new ByteArrayOutputStream();
             final PrintWriter printWriter = new PrintWriter(out)) {
          new Query(chain, """
                let $doc := jn:doc('json-path1','sales.jn')
                return sum(for $r in $doc[] return $r.age)
              """).serialize(ctx, printWriter);
          printWriter.flush();
          // 211 - 30 + 99 + 7 - 45 = 242.
          Assertions.assertEquals("242", out.toString());
        }
        Assertions.assertTrue(ProjectionIndexCatalog.servedCount() > servedBefore,
            "the aggregate must be SERVED from the incrementally maintained projection, not the fallback");
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        executor.close();
      }
    }
  }

  @Test
  public void recordSetReplacementIsMaintainedWithoutRecreate() throws IOException {
    // The parity guarantee with the other index families: deleting the
    // whole record set (rows drop out via the records' own post-order
    // notifications) and inserting a fresh one at the same path (a NEW
    // path class the listener reseeds as a root; its records append) keeps
    // the SAME projection continuously maintained — no tombstone, no
    // re-creation call — and it must SERVE the new values (servedCount
    // delta; value-only assertions cannot distinguish serving from
    // fallback).
    query("""
          jn:store('json-path1','two.jn','{
            "a": [{"age": 10}, {"age": 20}],
            "b": [{"age": 1}, {"age": 2}]
          }')
        """);
    query("""
          let $doc := jn:doc('json-path1','two.jn')
          let $stats := jn:create-projection-index($doc, '/a/[]', ('/a/[]/age'), ('long'))
          return {"revision": sdb:commit($doc)}
        """);
    query("""
          let $doc := jn:doc('json-path1','two.jn')
          return delete json $doc.a
        """);
    query("""
          let $doc := jn:doc('json-path1','two.jn')
          return insert json {"a": [{"age": 5}, {"age": 7}]} into $doc
        """);
    ProjectionIndexRegistry.clear();

    try (final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
         final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
         final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup("json-path1");
      final JsonResourceSession session = collection.getDatabase().beginResourceSession("two.jn");
      final SirixVectorizedExecutor executor =
          new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 2);
      SequentialPipelineStrategy.setVectorizedExecutor(executor);
      try {
        final long servedBefore = ProjectionIndexCatalog.servedCount();
        try (final ByteArrayOutputStream out = new ByteArrayOutputStream();
             final PrintWriter printWriter = new PrintWriter(out)) {
          new Query(chain, """
                let $doc := jn:doc('json-path1','two.jn')
                return sum(for $r in $doc.a[] return $r.age)
              """).serialize(ctx, printWriter);
          printWriter.flush();
          Assertions.assertEquals("12", out.toString());
        }
        Assertions.assertTrue(ProjectionIndexCatalog.servedCount() > servedBefore,
            "the continuously maintained projection must serve the replaced record set — a "
                + "tombstone or silent fallback would leave the counter unchanged");
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        executor.close();
      }
    }
  }

  @Test
  public void sessionBoundNodeAndJsonStoreChainServesAnalyticsAutomatically() throws IOException {
    // The REST layer's compile-chain shape: BOTH stores plus a session pinned to an explicit
    // revision. Queries compiled through it must receive the analytical fast paths WITHOUT any
    // manual executor registration — the incrementally maintained projection serves the
    // post-update aggregate (proven by the servedCount delta).
    query("""
          jn:store('json-path1','sales.jn','[
            {"age": 30, "active": true,  "dept": "Eng"},
            {"age": 45, "active": false, "dept": "Sales"},
            {"age": 52, "active": true,  "dept": "Eng"},
            {"age": 23, "active": true,  "dept": "HR"},
            {"age": 61, "active": false, "dept": "Eng"}
          ]')
        """);
    query("""
          let $doc := jn:doc('json-path1','sales.jn')
          let $stats := jn:create-projection-index($doc, '/[]',
              ('/[]/age', '/[]/active', '/[]/dept'),
              ('long', 'boolean', 'string'))
          return {"revision": sdb:commit($doc)}
        """);
    query("""
          let $doc := jn:doc('json-path1','sales.jn')
          return replace json value of $doc[0].age with 99
        """);
    ProjectionIndexRegistry.clear();

    try (final BasicJsonDBStore jsonStore =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
         final BasicXmlDBStore xmlStore = BasicXmlDBStore.newBuilder().build();
         final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(jsonStore)) {
      final JsonDBCollection collection = (JsonDBCollection) jsonStore.lookup("json-path1");
      final JsonResourceSession session = collection.getDatabase().beginResourceSession("sales.jn");
      final int mostRecent = session.getMostRecentRevisionNumber();

      Assertions.assertThrows(IllegalArgumentException.class,
          () -> SirixCompileChain.createWithNodeAndJsonStore(xmlStore, jsonStore, session, 0));
      Assertions.assertThrows(IllegalArgumentException.class,
          () -> SirixCompileChain.createWithNodeAndJsonStore(xmlStore, jsonStore, null, mostRecent));

      try (final SirixCompileChain chain =
              SirixCompileChain.createWithNodeAndJsonStore(xmlStore, jsonStore, session, mostRecent)) {
        final long servedBefore = ProjectionIndexCatalog.servedCount();
        try (final ByteArrayOutputStream out = new ByteArrayOutputStream();
             final PrintWriter printWriter = new PrintWriter(out)) {
          new Query(chain, """
                let $doc := jn:doc('json-path1','sales.jn')
                return sum(for $r in $doc[] return $r.age)
              """).serialize(ctx, printWriter);
          printWriter.flush();
          // 211 - 30 + 99 = 280.
          Assertions.assertEquals("280", out.toString());
        }
        Assertions.assertTrue(ProjectionIndexCatalog.servedCount() > servedBefore,
            "the session-bound chain must auto-wire the vectorized executor and SERVE the "
                + "aggregate from the maintained projection");
      }
    }
  }

  @Test
  public void singleProjectionOverDifferentRecordSetIsNotServed() throws IOException {
    // ONE projection over /a/[] must never answer for /b/[] — root matching
    // is exact, so the /b query falls back to the generic pipeline and
    // returns the correct sum.
    query("""
          jn:store('json-path1','two.jn','{
            "a": [{"age": 10}, {"age": 20}],
            "b": [{"age": 1}, {"age": 2}]
          }')
        """);
    query("""
          let $doc := jn:doc('json-path1','two.jn')
          let $stats := jn:create-projection-index($doc, '/a/[]', ('/a/[]/age'), ('long'))
          return {"revision": sdb:commit($doc)}
        """);
    ProjectionIndexRegistry.clear();

    try (final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
         final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
         final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup("json-path1");
      final JsonResourceSession session = collection.getDatabase().beginResourceSession("two.jn");
      final SirixVectorizedExecutor executor =
          new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 2);
      SequentialPipelineStrategy.setVectorizedExecutor(executor);
      try {
        try (final ByteArrayOutputStream out = new ByteArrayOutputStream();
             final PrintWriter printWriter = new PrintWriter(out)) {
          new Query(chain, """
                let $doc := jn:doc('json-path1','two.jn')
                return {"a": sum(for $r in $doc.a[] return $r.age),
                        "b": sum(for $r in $doc.b[] return $r.age)}
              """).serialize(ctx, printWriter);
          printWriter.flush();
          Assertions.assertEquals("{\"a\":30,\"b\":3}", out.toString());
        }
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        executor.close();
      }
    }
  }
}
