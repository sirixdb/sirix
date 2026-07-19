package io.sirix.query;

import io.brackit.query.Query;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.sirix.JsonTestHelper;
import io.sirix.access.Databases;
import io.sirix.access.trx.node.json.objectvalue.ArrayValue;
import io.sirix.access.trx.node.json.objectvalue.ObjectValue;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.projection.ProjectionIndexCatalog;
import io.sirix.index.projection.ProjectionIndexRegistry;
import io.sirix.node.NodeKind;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.scan.SirixVectorizedExecutor;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;

/**
 * Pattern-aware serving for DESCENDANT-rooted projections ({@code //...}):
 * the catalog resolves the pattern against the queried revision's path
 * summary — a pattern matching exactly ONE path class serves under that
 * concrete path, a pattern matching several subtrees (the projection
 * aggregates across all of them, which a path-specific query must not be
 * served from) fails closed to the generic pipeline, and a NEW matching
 * subtree appearing mid-transaction RESEEDS the listener's root set so the
 * projection stays exactly maintained across widening and shrinking.
 */
public final class ProjectionIndexDescendantRootServingTest extends AbstractJsonTest {

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

  private void storeSingleSubtreeAndCreateProjection() {
    query("""
          jn:store('json-path1','desc.jn','{
            "x": {"records": [{"age": 30}, {"age": 45}, {"age": 52}]},
            "meta": "untouched"
          }')
        """);
    query("""
          let $doc := jn:doc('json-path1','desc.jn')
          let $stats := jn:create-projection-index($doc, '//records/[]',
              ('//records/[]/age'), ('long'))
          return {"revision": sdb:commit($doc)}
        """);
  }

  /** Run {@code queryString} through a catalog-backed executor; returns the serialized result. */
  private String executeServed(final String resource, final String queryString) throws IOException {
    try (final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
         final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
         final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup("json-path1");
      final JsonResourceSession session = collection.getDatabase().beginResourceSession(resource);
      final SirixVectorizedExecutor executor =
          new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 2);
      SequentialPipelineStrategy.setVectorizedExecutor(executor);
      try (final ByteArrayOutputStream out = new ByteArrayOutputStream();
           final PrintWriter printWriter = new PrintWriter(out)) {
        new Query(chain, queryString).serialize(ctx, printWriter);
        printWriter.flush();
        return out.toString();
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        executor.close();
      }
    }
  }

  @Test
  public void singleMatchingSubtreeResolvesAndServes() throws IOException {
    storeSingleSubtreeAndCreateProjection();
    ProjectionIndexRegistry.clear();

    final long servedBefore = ProjectionIndexCatalog.servedCount();
    Assertions.assertEquals("127", executeServed("desc.jn", """
          let $doc := jn:doc('json-path1','desc.jn')
          return sum(for $r in $doc.x.records[] return $r.age)
        """));
    Assertions.assertTrue(ProjectionIndexCatalog.servedCount() > servedBefore,
        "a descendant pattern matching exactly one subtree must resolve to its concrete "
            + "path and SERVE, not fall back");
  }

  @Test
  public void ambiguousPatternFailsClosed() throws IOException {
    // TWO matching subtrees: the projection aggregates across both, so a
    // path-specific query must not be served from it — fail closed, generic
    // pipeline answers per subtree.
    query("""
          jn:store('json-path1','desc2.jn','{
            "x": {"records": [{"age": 1}, {"age": 2}]},
            "y": {"records": [{"age": 10}]}
          }')
        """);
    query("""
          let $doc := jn:doc('json-path1','desc2.jn')
          let $stats := jn:create-projection-index($doc, '//records/[]',
              ('//records/[]/age'), ('long'))
          return {"revision": sdb:commit($doc)}
        """);
    ProjectionIndexRegistry.clear();

    final long servedBefore = ProjectionIndexCatalog.servedCount();
    Assertions.assertEquals("{\"x\":3,\"y\":10}", executeServed("desc2.jn", """
          let $doc := jn:doc('json-path1','desc2.jn')
          return {"x": sum(for $r in $doc.x.records[] return $r.age),
                  "y": sum(for $r in $doc.y.records[] return $r.age)}
        """));
    Assertions.assertEquals(servedBefore, ProjectionIndexCatalog.servedCount(),
        "an ambiguous descendant pattern must never serve a path-specific query");
  }

  @Test
  public void incrementallyMaintainedDescendantProjectionStillServes() throws IOException {
    // The change listener seeds its record-set roots from the PATTERN, so
    // update/insert/delete maintenance under the (single) matching subtree
    // must keep the projection serving with the compounded values.
    storeSingleSubtreeAndCreateProjection();
    query("""
          let $doc := jn:doc('json-path1','desc.jn')
          return replace json value of $doc.x.records[0].age with 99
        """);
    query("""
          let $doc := jn:doc('json-path1','desc.jn')
          return append json {"age": 7} into $doc.x.records
        """);
    query("""
          let $doc := jn:doc('json-path1','desc.jn')
          return delete json $doc.x.records[1]
        """);
    ProjectionIndexRegistry.clear();

    final long servedBefore = ProjectionIndexCatalog.servedCount();
    // 127 - 30 + 99 + 7 - 45 = 158.
    Assertions.assertEquals("158", executeServed("desc.jn", """
          let $doc := jn:doc('json-path1','desc.jn')
          return sum(for $r in $doc.x.records[] return $r.age)
        """));
    Assertions.assertTrue(ProjectionIndexCatalog.servedCount() > servedBefore,
        "the incrementally maintained descendant-rooted projection must still be SERVED");
  }

  @Test
  public void newMatchingSubtreeMidTransactionIsMaintainedExactly() throws IOException {
    // A brand-new subtree matching the pattern widens the record set — the
    // listener RESEEDS its root PCRs so the new subtree's records attribute
    // normally. Here the subtree is removed again before the commit, so the
    // maintained projection must end up exactly where it started: still
    // valid, still serving the original rows.
    storeSingleSubtreeAndCreateProjection();
    try (final Database<JsonResourceSession> database = openDatabase();
         final JsonResourceSession session = database.beginResourceSession("desc.jn")) {
      try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
        Assertions.assertTrue(wtx.moveToDocumentRoot());
        Assertions.assertTrue(wtx.moveToFirstChild());        // top-level OBJECT
        final long topObjectKey = wtx.getNodeKey();
        // Build {"z": {"records": [{"age": 100}]}} — a SECOND subtree
        // matching //records/[] — inside this transaction.
        wtx.insertObjectRecordAsLastChild("z", new ObjectValue());
        wtx.insertObjectRecordAsLastChild("records", new ArrayValue());
        wtx.insertSubtreeAsLastChild(JsonShredder.createStringReader("{\"age\": 100}"),
            JsonNodeTrx.Commit.NO);
        // ... and remove it again before committing.
        Assertions.assertTrue(wtx.moveTo(topObjectKey));
        Assertions.assertTrue(wtx.moveToLastChild());
        Assertions.assertEquals(NodeKind.OBJECT_NAMED_OBJECT, wtx.getKind());
        wtx.remove();
        wtx.commit();
      }
      // The pattern resolves to ONE path class again and the maintained
      // projection serves the unchanged rows — no tombstone.
      final int mostRecent = session.getMostRecentRevisionNumber();
      try (final var rtx = session.beginNodeReadOnlyTrx(mostRecent)) {
        Assertions.assertNotNull(session.getRtxIndexController(mostRecent)
                .openProjectionIndex(rtx.getStorageEngineReader(),
                    new String[] { "x", "records", "[]" }, new String[] { "age" }),
            "a reverted mid-transaction widening must leave the projection maintained and servable");
      }
    }
    final long servedBefore = ProjectionIndexCatalog.servedCount();
    Assertions.assertEquals("127", executeServed("desc.jn", """
          let $doc := jn:doc('json-path1','desc.jn')
          return sum(for $r in $doc.x.records[] return $r.age)
        """));
    Assertions.assertTrue(ProjectionIndexCatalog.servedCount() > servedBefore);
  }

  @Test
  public void widenThenShrinkStaysMaintainedAndServingResumes() throws IOException {
    // The full life cycle of a descendant pattern's record set: a SECOND
    // matching subtree appears (reseed — its records append to the
    // projection; serving pauses on ambiguity), then the FIRST subtree is
    // deleted (its rows drop out incrementally) — after which the pattern
    // resolves to one path class again and serving RESUMES from the
    // continuously maintained projection, with the surviving subtree's
    // values. No tombstone, no re-creation call anywhere.
    storeSingleSubtreeAndCreateProjection();
    query("""
          let $doc := jn:doc('json-path1','desc.jn')
          return insert json {"y": {"records": [{"age": 100}]}} into $doc
        """);
    // Ambiguous while both subtrees exist — fail closed, values correct.
    final long servedWhileAmbiguous = ProjectionIndexCatalog.servedCount();
    Assertions.assertEquals("227", executeServed("desc.jn", """
          let $doc := jn:doc('json-path1','desc.jn')
          return sum(for $r in $doc.x.records[] return $r.age)
             + sum(for $r in $doc.y.records[] return $r.age)
        """));
    Assertions.assertEquals(servedWhileAmbiguous, ProjectionIndexCatalog.servedCount(),
        "two matching subtrees must not serve a path-specific query");
    // Shrink back to one subtree.
    query("""
          let $doc := jn:doc('json-path1','desc.jn')
          return delete json $doc.x
        """);
    final long servedBefore = ProjectionIndexCatalog.servedCount();
    Assertions.assertEquals("100", executeServed("desc.jn", """
          let $doc := jn:doc('json-path1','desc.jn')
          return sum(for $r in $doc.y.records[] return $r.age)
        """));
    Assertions.assertTrue(ProjectionIndexCatalog.servedCount() > servedBefore,
        "after shrinking back to one subtree the maintained projection must SERVE again");
  }

  private static Database<JsonResourceSession> openDatabase() {
    final Path dbPath =
        Path.of(JsonTestHelper.PATHS.PATH1.getFile().getParent().toString(), "json-path1");
    return Databases.openJsonDatabase(dbPath);
  }
}
