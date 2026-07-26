package io.sirix.query;

import io.brackit.query.Query;
import io.brackit.query.atomic.QNm;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Iter;
import io.brackit.query.jdm.Sequence;
import io.sirix.JsonTestHelper;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.projection.ProjectionIndexCatalog;
import io.sirix.index.projection.ProjectionIndexRegistry;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBItem;
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

  /**
   * A variable bound to a NESTED item must never be served as the whole document.
   *
   * <p>The source path the scan serves is written relative to the binding but resolved ABSOLUTELY
   * (against the projection's root path and the whole-resource path summary), so serving a nested
   * binding as the document root aggregates every matching row in the resource instead of only
   * those beneath the bound sub-tree. That is a wrong answer, not a slow one.
   *
   * <p>The gate that prevents it reads the item's parent key. {@code JsonDBItem.getTrx()} hands back
   * the SHARED cursor without repositioning it, so reading the parent straight off it reports the
   * parent of whatever node the cursor last visited — for a nested item whose cursor happens to sit
   * on the document's top-level node, that wrongly says "root".
   */
  @Test
  public void nestedVariableBindingIsNotServedAsWholeDocument() throws IOException {
    storeNestedResourceWithProjection();
    ProjectionIndexRegistry.clear();

    try (final BasicJsonDBStore store = newStore();
         final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store)) {
      final JsonResourceSession session = openResource(store, DB_A, "nested.jn");
      try (final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store, session)) {
        // Bind $sub to department id 1 only. Its ages are 10+20 = 30; the whole document is
        // 10+20+100+200 = 330, so the two answers are decisively different. Selected by predicate
        // rather than positionally: in this dialect both [[0]] and a trailing [1] parse as array
        // ACCESS (the former reading its inner [0] as an array literal), not as a position filter.
        final Sequence nested =
            new Query(chain,
                "for $d in jn:doc('json-path1','nested.jn').departments[] "
                    + "where $d.id eq 1 return $d").execute(ctx);
        // Materialize: the pipe returns a lazy sequence, and the binding must be the item itself.
        final Item subItem;
        try (final Iter iter = nested.iterate()) {
          subItem = iter.next();
        }
        Assertions.assertNotNull(subItem, "the nested department must resolve to exactly one item");
        ctx.bind(new QNm("sub"), subItem);

        // Park the SHARED cursor on the document's top-level node — the state it is legitimately
        // in right after any $doc-level access, and the one that makes the unguarded parent-key
        // read report "root" for this nested item. Without repositioning onto the item, the gate
        // now says "whole document" and the aggregate runs over BOTH departments (330).
        final JsonNodeReadOnlyTrx sharedTrx = ((JsonDBItem) subItem).getTrx();
        sharedTrx.moveToDocumentRoot();
        sharedTrx.moveToFirstChild();

        Assertions.assertEquals("30", evaluate(chain, ctx,
            "declare variable $sub external; "
                + "sum(for $r in $sub.records[] return $r.age)"),
            "a variable bound to a NESTED item must aggregate only that sub-tree, never the "
                + "whole resource");
      }
    }
  }

  // ---------------------------------------------------------------------------------------------

  private void storeNestedResourceWithProjection() {
    query("jn:store('json-path1','nested.jn','"
        + "{\"departments\":["
        + "{\"id\":1,\"records\":[{\"age\":10},{\"age\":20}]},"
        + "{\"id\":2,\"records\":[{\"age\":100},{\"age\":200}]}"
        + "]}')");
    query("""
          let $doc := jn:doc('json-path1','nested.jn')
          let $stats := jn:create-projection-index($doc, '/departments/[]/records/[]',
                                                   ('/departments/[]/records/[]/age'), ('long'))
          return {"revision": sdb:commit($doc)}
        """);
  }

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
