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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * End-to-end coverage of the canonical resource-confinement contract
 * ({@link io.sirix.query.scan.SirixVectorizedExecutor#acceptsSource}). A compile chain that auto-wires a
 * vectorized executor bound to one resource must serve analytical queries over THAT resource from its
 * projection, yet must never answer a query over a DIFFERENT resource (or a revision the executor isn't
 * pinned to) from the bound resource's columns. Brackit's optimizer lifts each scan's source identity
 * into a {@code SourceRef} and asks the executor's {@code acceptsSource} at translate time; a mismatch
 * declines and the generic (always-correct) pipeline runs. Proven by
 * {@link ProjectionIndexCatalog#servedCount()} (did the projection serve?) together with the result
 * value (whose data was returned?).
 */
public final class VectorizedSourceRefServingTest extends AbstractJsonTest {

  // Two DISTINCT databases: jn:store recreates the whole database it targets (removeIfExisting),
  // so a single database cannot hold two independently-stored resources. json-path1/json-path2 are
  // the two managed test paths JsonTestHelper.deleteEverything() cleans between tests.
  private static final String DB_A = "json-path1";

  @BeforeEach
  public void clearBefore() {
    ProjectionIndexRegistry.clear();
    SequentialPipelineStrategy.setVectorizedExecutor(null);
  }

  @AfterEach
  public void clearAfter() {
    ProjectionIndexRegistry.clear();
    SequentialPipelineStrategy.setVectorizedExecutor(null);
  }

  @Test
  public void boundResourceStillServesThroughAutoWiredChain() throws IOException {
    storeTwoResourcesWithProjections();
    ProjectionIndexRegistry.clear();

    try (final BasicJsonDBStore store = newStore();
         final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store)) {
      final JsonResourceSession sessionA = openResource(store, DB_A, "a.jn");
      try (final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store, sessionA)) {
        final long servedBefore = ProjectionIndexCatalog.servedCount();
        // sum over the BOUND resource — must be served from a.jn's projection. The scan's SourceRef
        // is jn:doc('json-path1','a.jn'); the a.jn-bound executor accepts it.
        Assertions.assertEquals("60", evaluate(chain, ctx,
            "let $doc := jn:doc('json-path1','a.jn') return sum(for $r in $doc[] return $r.age)"));
        Assertions.assertTrue(ProjectionIndexCatalog.servedCount() > servedBefore,
            "a query over the bound resource must be served from its projection");
      }
    }
  }

  @Test
  public void crossResourceQueryIsNotServedFromBoundResource() throws IOException {
    storeTwoResourcesWithProjections();
    ProjectionIndexRegistry.clear();

    try (final BasicJsonDBStore store = newStore();
         final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store)) {
      final JsonResourceSession sessionA = openResource(store, DB_A, "a.jn");
      // Chain bound to json-path1/a.jn (sum 60), but the query ranges over json-path2/b.jn (sum 10).
      // Without acceptsSource the a.jn-bound executor would answer from a.jn's projection over the
      // shared '[]' shape and return 60 — the wrong resource's data. The executor declines the
      // json-path2/b.jn SourceRef, so the generic pipeline runs and returns b.jn's correct 10, and the
      // projection never serves.
      try (final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store, sessionA)) {
        final long servedBefore = ProjectionIndexCatalog.servedCount();
        Assertions.assertEquals("10", evaluate(chain, ctx,
            "let $doc := jn:doc('json-path2','b.jn') return sum(for $r in $doc[] return $r.age)"),
            "a query over another resource must return THAT resource's data, never the bound one's");
        Assertions.assertEquals(servedBefore, ProjectionIndexCatalog.servedCount(),
            "the bound resource's projection must not serve a query over a different resource");
      }
    }
  }

  @Test
  public void nonLatestRevisionJnDocIsNotServed() throws IOException {
    // a.jn: build a projection (rev 2), then update it (rev 3). Bind the chain to the OLD rev 2.
    // A bare jn:doc opens the MOST-RECENT revision (3), so serving rev 2's projection would answer
    // with stale data. The executor's acceptsSource refuses (bound revision is not the latest) and the
    // generic pipeline returns rev-3 data.
    query("jn:store('json-path1','a.jn','[{\"age\":10},{\"age\":20},{\"age\":30}]')");
    query("""
          let $doc := jn:doc('json-path1','a.jn')
          let $stats := jn:create-projection-index($doc, '/[]', ('/[]/age'), ('long'))
          return {"revision": sdb:commit($doc)}
        """);
    query("let $doc := jn:doc('json-path1','a.jn') return insert json {\"age\":40} into $doc");
    ProjectionIndexRegistry.clear();

    try (final BasicJsonDBStore store = newStore();
         final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
         final BasicXmlDBStore xmlStore = BasicXmlDBStore.newBuilder().build()) {
      final JsonResourceSession sessionA = openResource(store, DB_A, "a.jn");
      final int mostRecent = sessionA.getMostRecentRevisionNumber();
      final int oldRevision = mostRecent - 1;
      try (final SirixCompileChain chain =
              SirixCompileChain.createWithNodeAndJsonStore(xmlStore, store, sessionA, oldRevision)) {
        final long servedBefore = ProjectionIndexCatalog.servedCount();
        // Live (rev 3) sum is 10+20+30+40 = 100. Serving the rev-2 projection would give 60.
        Assertions.assertEquals("100", evaluate(chain, ctx,
            "let $doc := jn:doc('json-path1','a.jn') return sum(for $r in $doc[] return $r.age)"),
            "a bare jn:doc opens the most-recent revision; a pinned older executor must not serve");
        Assertions.assertEquals(servedBefore, ProjectionIndexCatalog.servedCount(),
            "an executor pinned to a non-latest revision must not serve a bare jn:doc query");
      }
    }
  }

  // ---------------------------------------------------------------------------------------------

  private void storeTwoResourcesWithProjections() {
    // json-path1/a.jn: ages 10,20,30 (sum 60) — json-path2/b.jn: ages 1,2,3,4 (sum 10). Distinct
    // sums make "which resource answered?" decisive; distinct databases avoid the jn:store wipe.
    query("jn:store('json-path1','a.jn','[{\"age\":10},{\"age\":20},{\"age\":30}]')");
    query("""
          let $doc := jn:doc('json-path1','a.jn')
          let $stats := jn:create-projection-index($doc, '/[]', ('/[]/age'), ('long'))
          return {"revision": sdb:commit($doc)}
        """);
    query("jn:store('json-path2','b.jn','[{\"age\":1},{\"age\":2},{\"age\":3},{\"age\":4}]')");
    query("""
          let $doc := jn:doc('json-path2','b.jn')
          let $stats := jn:create-projection-index($doc, '/[]', ('/[]/age'), ('long'))
          return {"revision": sdb:commit($doc)}
        """);
  }

  private static JsonResourceSession openResource(final BasicJsonDBStore store, final String db,
      final String resource) {
    final JsonDBCollection collection = (JsonDBCollection) store.lookup(db);
    return collection.getDatabase().beginResourceSession(resource);
  }

  private static BasicJsonDBStore newStore() {
    return BasicJsonDBStore.newBuilder()
                           .location(JsonTestHelper.PATHS.PATH1.getFile().getParent())
                           .build();
  }

  private static String evaluate(final SirixCompileChain chain, final SirixQueryContext ctx,
      final String query) throws IOException {
    try (final ByteArrayOutputStream out = new ByteArrayOutputStream();
         final PrintWriter printWriter = new PrintWriter(out)) {
      new Query(chain, query).serialize(ctx, printWriter);
      printWriter.flush();
      return out.toString();
    }
  }
}
