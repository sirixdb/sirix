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
  public void staleTombstoneRecreateRebuildsUnderSameDefAndServes() throws IOException {
    // The recovery ladder every listener fallback depends on: a structural
    // change tombstones the projection while its definition STAYS catalogued;
    // re-running jn:create-projection-index with the same shape must refuse
    // the stale snapshot, REBUILD under the same definition id, and the
    // rebuilt projection must SERVE (servedCount delta — value-only
    // assertions cannot distinguish rebuild from fallback).
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
    // Structural fallback: removing the record-set array tombstones the
    // projection; the definition remains catalogued.
    query("""
          let $doc := jn:doc('json-path1','two.jn')
          return delete json $doc.a
        """);
    // New record set at the same path, then a same-shape re-create — must
    // rebuild over the stale tombstone (id 0 reused).
    query("""
          let $doc := jn:doc('json-path1','two.jn')
          return insert json {"a": [{"age": 5}, {"age": 7}]} into $doc
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
            "the REBUILT projection must serve — a short-circuit on the stale snapshot or a "
                + "silent fallback would leave the counter unchanged");
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        executor.close();
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
