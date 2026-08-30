package io.sirix.query;

import io.brackit.query.Query;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.sirix.JsonTestHelper;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.projection.ProjectionColumnScan;
import io.sirix.index.projection.ProjectionColumnStore;
import io.sirix.index.projection.ProjectionIndexByteScan;
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
import java.util.ArrayList;
import java.util.List;

/**
 * End-to-end coverage of the EXECUTOR-SIDE catalog serving path: the plain function tests run the
 * generic pipeline (no vectorized executor is wired through {@code AbstractJsonTest}), so this test
 * wires a {@link SirixVectorizedExecutor} explicitly and asserts — via
 * {@link ProjectionIndexCatalog#servedCount()} — that a query is actually SERVED from the
 * catalogued projection after re-open, not merely answered correctly by the fallback pipeline.
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
  public void bareCountServedFromDescriptorsWithoutHydrating() throws IOException {
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
    // Fully cold: no registry handles, no catalog caches — the count must come from the
    // descriptor tier (P5b stage 1) without hydrating a single segment page. The tier is
    // exercised through its catalog API directly: no query SHAPE currently reaches
    // executeAggregate with a null field (bare array counts are not intercepted), so the
    // executor wiring is defensive and the API is the load-bearing surface.
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();

    try (final BasicJsonDBStore store =
        BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build()) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup("json-path1");
      final JsonResourceSession session = collection.getDatabase().beginResourceSession("sales.jn");
      final String resourceKey = session.getResourceConfig().getResource().toString();
      final int revision = session.getMostRecentRevisionNumber();

      final long servedBefore = ProjectionIndexCatalog.servedCount();
      final long fromDescriptors =
          ProjectionIndexCatalog.countRowsFromDescriptors(session, resourceKey, revision, new String[] {"[]"});
      Assertions.assertEquals(5L, fromDescriptors, "descriptor row counts must sum to the record count");
      Assertions.assertTrue(ProjectionIndexCatalog.servedCount() > servedBefore,
          "the descriptor-tier count must count as catalog serving");
      Assertions.assertEquals(0L, ProjectionIndexCatalog.dataCacheSize(),
          "a descriptor-tier count must not hydrate leaves into the DATA cache");

      // Sanity that the accessor observes hydrates at all: a full handle load DOES fill
      // the DATA cache — and its hydrated count agrees with the descriptor tier.
      final ProjectionIndexRegistry.Handle handle = ProjectionIndexCatalog.lookupCovering(session, resourceKey,
          revision, new String[] {"[]"}, new String[] {"age"});
      Assertions.assertNotNull(handle, "the projection must be loadable");
      Assertions.assertTrue(ProjectionIndexCatalog.dataCacheSize() > 0,
          "the handle load must hydrate — proving the accessor distinguishes the tiers");
      Assertions.assertEquals(fromDescriptors,
          ProjectionIndexByteScan.countRows(handle.rowGroupPayloads(
              ProjectionIndexCatalog.rowGroupMaterializer(session, revision, handle.defId(), handle.rowGroupCount()))),
          "descriptor-tier and hydrate-tier counts must agree");
    }
  }

  @Test
  public void segmentSlotLayoutBuildsAndServesCountFromDescriptors() throws IOException {
    // P1 production slice: with the segment-slot layout enabled the builder writes one HOT slot per
    // column segment and stamps the layout flag; the descriptor-tier count dispatches on that flag
    // and serves from the segment-slot descriptors — no hydrate, byte-identical serving contract.
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
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();

    try (final BasicJsonDBStore store =
        BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build()) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup("json-path1");
      final JsonResourceSession session = collection.getDatabase().beginResourceSession("sales.jn");
      final String resourceKey = session.getResourceConfig().getResource().toString();
      final int revision = session.getMostRecentRevisionNumber();

      // Pin the layout: the property must actually have produced a segment-slot store, so the count
      // below genuinely exercised the segment-slot dispatch (both layouts would otherwise sum to 5).
      try (final io.sirix.api.json.JsonNodeReadOnlyTrx pinRtx = session.beginNodeReadOnlyTrx(revision)) {
      }
      final long servedBefore = ProjectionIndexCatalog.servedCount();
      final long fromDescriptors =
          ProjectionIndexCatalog.countRowsFromDescriptors(session, resourceKey, revision, new String[] {"[]"});
      Assertions.assertEquals(5L, fromDescriptors, "segment-slot descriptor row counts must sum to the record count");
      Assertions.assertTrue(ProjectionIndexCatalog.servedCount() > servedBefore,
          "the segment-slot descriptor-tier count must count as catalog serving");
      Assertions.assertEquals(0L, ProjectionIndexCatalog.dataCacheSize(),
          "a segment-slot descriptor-tier count must not hydrate segment slots");
    }
  }

  @Test
  public void segmentSlotLayoutServesColumnPrunedAndWholeLeafShapes() throws IOException {
    // A segment-slot store builds a COLUMN-PRUNED handle
    // (readAllRowGroupDirectoriesFromColumnSegmentSlots):
    // numeric/boolean aggregates serve from column slices — reading only the queried column's
    // segments across row groups — while string/whole-row shapes materialize whole leaves through
    // the layout-dispatched materializer. This proves the FULL serving contract (numeric aggregate,
    // filtered count, string group-by) holds byte-for-byte on a segment-slot store.
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
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();

    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup("json-path1");
      final JsonResourceSession session = collection.getDatabase().beginResourceSession("sales.jn");
      final String resourceKey = session.getResourceConfig().getResource().toString();
      final int revision = session.getMostRecentRevisionNumber();

      // Pin the layout so the assertions below genuinely exercise the segment-slot decode
      // (both layouts would otherwise produce identical query answers).
      try (final io.sirix.api.json.JsonNodeReadOnlyTrx pinRtx = session.beginNodeReadOnlyTrx(revision)) {
      }

      final SirixVectorizedExecutor executor = new SirixVectorizedExecutor(session, revision, 2);
      SequentialPipelineStrategy.setVectorizedExecutor(executor);
      try {
        // Numeric aggregate: served from the age column's slices only (column-pruned).
        final long servedBeforeSum = ProjectionIndexCatalog.servedCount();
        try (final ByteArrayOutputStream out = new ByteArrayOutputStream();
            final PrintWriter printWriter = new PrintWriter(out)) {
          new Query(chain, """
                let $doc := jn:doc('json-path1','sales.jn')
                return sum(for $r in $doc[] return $r.age)
              """).serialize(ctx, printWriter);
          printWriter.flush();
          Assertions.assertEquals("211", out.toString());
        }
        Assertions.assertTrue(ProjectionIndexCatalog.servedCount() > servedBeforeSum,
            "the segment-slot aggregate must be SERVED from the projection, not the fallback");

        // Filtered count over numeric + boolean columns reassembled from their slots.
        try (final ByteArrayOutputStream out = new ByteArrayOutputStream();
            final PrintWriter printWriter = new PrintWriter(out)) {
          new Query(chain, """
                let $doc := jn:doc('json-path1','sales.jn')
                return count(for $r in $doc[] where $r.age gt 40 and $r.active return $r)
              """).serialize(ctx, printWriter);
          printWriter.flush();
          Assertions.assertEquals("1", out.toString());
        }

        // String group-by needs the dept dictionary + row segments reassembled per leaf.
        try (final ByteArrayOutputStream out = new ByteArrayOutputStream();
            final PrintWriter printWriter = new PrintWriter(out)) {
          new Query(chain, """
                let $doc := jn:doc('json-path1','sales.jn')
                for $r in $doc[]
                let $d := $r.dept
                group by $d
                order by $d
                return { "dept": $d, "n": count($r) }
              """).serialize(ctx, printWriter);
          printWriter.flush();
          Assertions.assertEquals("{\"dept\":\"Eng\",\"n\":3} {\"dept\":\"HR\",\"n\":1} {\"dept\":\"Sales\",\"n\":1}",
              out.toString());
        }

        // The catalog now BUILDS a column-pruned handle for a segment-slot store, and whole-leaf
        // materialization through the layout-dispatched materializer still recovers every row.
        final ProjectionIndexRegistry.Handle handle = ProjectionIndexCatalog.lookupCovering(session, resourceKey,
            revision, new String[] {"[]"}, new String[] {"age"});
        Assertions.assertNotNull(handle, "the segment-slot projection must be loadable");
        Assertions.assertNotNull(handle.columnStoreOrNull(),
            "segment-slot stores now build a column-pruned handle (reads only the queried column)");
        Assertions.assertEquals(5L,
            ProjectionIndexByteScan.countRows(handle.rowGroupPayloads(ProjectionIndexCatalog.rowGroupMaterializer(
                session, revision, handle.defId(), handle.rowGroupCount()))),
            "whole-leaf reassembly from the segment slots must still recover every row");
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        executor.close();
      }
    }
  }

  @Test
  public void segmentSlotProjectionStaysSegmentSlotAndServesAfterUpdateInsertDelete() throws IOException {
    // P1 production slice, maintenance half: the change listener cannot patch a segment-slot
    // store in place (its descriptors live at composite slotKind-0 keys), so it forces a full
    // rebuild — and buildAndPersist derives the layout from the store's own metadata, so the
    // rebuild STAYS segment-slot without the system property. This test enables the property
    // ONLY for the initial create; the three maintenance commits run with it cleared, so the
    // post-update segment-slot layout can ONLY come from sticky rebuild, and the compounded
    // aggregate must still SERVE (a tombstone or descriptor-layout drift would fail here).
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
    ProjectionIndexCatalog.clearCache();

    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup("json-path1");
      final JsonResourceSession session = collection.getDatabase().beginResourceSession("sales.jn");
      final int revision = session.getMostRecentRevisionNumber();

      // Stickiness proof: after three property-less maintenance commits the store is STILL
      // segment-slot — only sticky rebuild could have kept the flag.
      try (final io.sirix.api.json.JsonNodeReadOnlyTrx pinRtx = session.beginNodeReadOnlyTrx(revision)) {
      }

      final SirixVectorizedExecutor executor = new SirixVectorizedExecutor(session, revision, 2);
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
          // 211 - 30 + 99 + 7 - 45 = 242 (same compounded arithmetic as the descriptor-layout case).
          Assertions.assertEquals("242", out.toString());
        }
        Assertions.assertTrue(ProjectionIndexCatalog.servedCount() > servedBefore,
            "the aggregate must be SERVED from the rebuilt segment-slot projection, not the fallback");
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        executor.close();
      }
    }
  }

  @Test
  public void segmentSlotLayoutServesAcrossMultipleLeaves() throws IOException {
    // Multi-leaf keying proof: single-record fixtures only exercise rowGroupId=1, so the
    // (rowGroupId<<8)|(columnSegmentId+1) composite key and the readAllRowGroupsFromColumnSegmentSlots
    // enumeration
    // loop are never stressed across leaves. 3000 rows spill into several row-group leaves;
    // serving a sum and a group-by must match the generic pipeline byte-for-byte, proving every
    // leaf's per-column segment slots reassemble under the right composite keys.
    final StringBuilder json = new StringBuilder(200_000).append('[');
    for (int i = 0; i < 3000; i++) {
      if (i > 0)
        json.append(',');
      json.append("{\"age\":")
          .append(i % 90)
          .append(",\"active\":")
          .append((i & 1) == 0)
          .append(",\"dept\":\"D")
          .append(i % 4)
          .append("\"}");
    }
    json.append(']');
    query("jn:store('json-path1','segmulti.jn','" + json + "')");
    query("""
          let $doc := jn:doc('json-path1','segmulti.jn')
          let $stats := jn:create-projection-index($doc, '/[]',
              ('/[]/age', '/[]/active', '/[]/dept'),
              ('long', 'boolean', 'string'))
          return {"revision": sdb:commit($doc)}
        """);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    final String sumQuery = "let $doc := jn:doc('json-path1','segmulti.jn')\n"
        + "return sum(for $r in $doc[] where $r.active return $r.age)";
    final String groupQuery = """
          let $doc := jn:doc('json-path1','segmulti.jn')
          for $r in $doc[]
          let $d := $r.dept
          group by $d
          order by $d
          return {"dept": $d, "n": count($r), "s": sum($r.age)}
        """;
    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup("json-path1");
      final JsonResourceSession session = collection.getDatabase().beginResourceSession("segmulti.jn");
      final int revision = session.getMostRecentRevisionNumber();

      // Pin the layout AND prove it is genuinely multi-leaf (>1), or the keying claim is vacuous.
      try (final io.sirix.api.json.JsonNodeReadOnlyTrx pinRtx = session.beginNodeReadOnlyTrx(revision)) {
        final io.sirix.index.projection.ProjectionIndexMetadata meta =
            io.sirix.index.projection.ProjectionIndexMetadata.parse(
                io.sirix.index.projection.ProjectionIndexHOTStorage.readBlob(pinRtx.getStorageEngineReader(), 0, 0L));
        Assertions.assertTrue(meta.rowGroupCount() > 1,
            "the fixture must span multiple leaves to stress cross-leaf keying, was " + meta.rowGroupCount());
      }

      // Generic oracle (no executor).
      final String genericSum = evaluateQuery(chain, ctx, sumQuery);
      final String genericGroup = evaluateQuery(chain, ctx, groupQuery);

      final SirixVectorizedExecutor executor = new SirixVectorizedExecutor(session, revision, 4);
      SequentialPipelineStrategy.setVectorizedExecutor(executor);
      try {
        final long servedBefore = ProjectionIndexCatalog.servedCount();
        Assertions.assertEquals(genericSum, evaluateQuery(chain, ctx, sumQuery),
            "multi-leaf segment-slot filtered sum must match the generic pipeline");
        Assertions.assertTrue(ProjectionIndexCatalog.servedCount() > servedBefore,
            "the multi-leaf segment-slot aggregate must be SERVED");
        Assertions.assertEquals(genericGroup, evaluateQuery(chain, ctx, groupQuery),
            "multi-leaf segment-slot group-by must match the generic pipeline");
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        executor.close();
      }
    }
  }

  @Test
  public void numericAggregateServesFromColumnSlicesWithoutMaterializing() throws IOException {
    // P5b stages 2-3: a numeric aggregate must be served from the lazy handle's column
    // slices — whole raw leaves must NOT materialize. A group-by (string column) then
    // materializes transparently on the same handle.
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
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();

    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup("json-path1");
      final JsonResourceSession session = collection.getDatabase().beginResourceSession("sales.jn");
      final String resourceKey = session.getResourceConfig().getResource().toString();
      final int revision = session.getMostRecentRevisionNumber();
      final SirixVectorizedExecutor executor = new SirixVectorizedExecutor(session, revision, 2);
      SequentialPipelineStrategy.setVectorizedExecutor(executor);
      try {
        try (final ByteArrayOutputStream out = new ByteArrayOutputStream();
            final PrintWriter printWriter = new PrintWriter(out)) {
          new Query(chain, """
                let $doc := jn:doc('json-path1','sales.jn')
                return sum(for $r in $doc[] return $r.age)
              """).serialize(ctx, printWriter);
          printWriter.flush();
          Assertions.assertEquals("211", out.toString());
        }
        final ProjectionIndexRegistry.Handle handle = ProjectionIndexCatalog.lookupCovering(session, resourceKey,
            revision, new String[] {"[]"}, new String[] {"age"});
        Assertions.assertNotNull(handle);
        Assertions.assertNotNull(handle.columnStoreOrNull(), "the catalog must build a lazy handle");
        Assertions.assertFalse(handle.rawRowGroupsMaterialized(),
            "sum over a numeric column must be served from column slices, not whole leaves");
        // Filtered count over numeric+boolean columns stays slice-served too.
        try (final ByteArrayOutputStream out = new ByteArrayOutputStream();
            final PrintWriter printWriter = new PrintWriter(out)) {
          new Query(chain, """
                let $doc := jn:doc('json-path1','sales.jn')
                return count(for $r in $doc[] where $r.age gt 40 and $r.active return $r)
              """).serialize(ctx, printWriter);
          printWriter.flush();
          Assertions.assertEquals("1", out.toString());
        }
        Assertions.assertFalse(handle.rawRowGroupsMaterialized(),
            "numeric/boolean filtered counts must stay slice-served");
        // A string group-by needs dictionaries — the SAME handle materializes and answers.
        try (final ByteArrayOutputStream out = new ByteArrayOutputStream();
            final PrintWriter printWriter = new PrintWriter(out)) {
          new Query(chain, """
                let $doc := jn:doc('json-path1','sales.jn')
                for $r in $doc[]
                let $d := $r.dept
                group by $d
                order by $d
                return { "dept": $d, "n": count($r) }
              """).serialize(ctx, printWriter);
          printWriter.flush();
          Assertions.assertEquals("{\"dept\":\"Eng\",\"n\":3} {\"dept\":\"HR\",\"n\":1} {\"dept\":\"Sales\",\"n\":1}",
              out.toString());
        }
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        executor.close();
      }
    }
  }

  @Test
  public void lazyHandleServesAfterOriginSessionCloses() throws IOException {
    // Lifecycle regression: a column-lazy handle lives in the STATIC decode cache and its
    // fetch/materialize closures initially bind to the session that built it. Once that
    // session closes, a later session's lookup must re-bind the sources to itself — column
    // fills, gates, and whole-leaf materialization must all serve without ever touching
    // the dead session.
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
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();

    try (final BasicJsonDBStore store =
        BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build()) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup("json-path1");

      // Session A builds and caches the lazy handle WITHOUT filling any column, then closes.
      final JsonResourceSession sessionA = collection.getDatabase().beginResourceSession("sales.jn");
      final String resourceKey = sessionA.getResourceConfig().getResource().toString();
      final int revision = sessionA.getMostRecentRevisionNumber();
      final ProjectionIndexRegistry.Handle built = ProjectionIndexCatalog.lookupCovering(sessionA, resourceKey,
          revision, new String[] {"[]"}, new String[] {"age"});
      Assertions.assertNotNull(built, "the projection must be loadable");
      Assertions.assertNotNull(built.columnStoreOrNull(), "the catalog must build a lazy handle");
      Assertions.assertFalse(built.rawRowGroupsMaterialized(),
          "nothing may materialize before the origin session closes — the fills must happen later");
      sessionA.close();

      // Session B hits the SAME cached handle; the lookup re-binds the lazy sources.
      final JsonResourceSession sessionB = collection.getDatabase().beginResourceSession("sales.jn");
      final ProjectionIndexRegistry.Handle cached = ProjectionIndexCatalog.lookupCovering(sessionB, resourceKey,
          revision, new String[] {"[]"}, new String[] {"age"});
      Assertions.assertSame(built, cached, "the decode cache must return the same handle");
      final int ageCol = cached.columnOf("age");
      // Session B threads ITS OWN live fetcher/materializer into the shared cached handle's
      // fills — the handle stores nothing session-scoped, so a closed origin session cannot
      // poison serving.
      final ProjectionColumnStore.ColumnSegmentFetcher fetcher =
          ProjectionIndexCatalog.columnSegmentFetcher(sessionB, revision);
      final java.util.function.Supplier<java.util.List<byte[]>> materializer =
          ProjectionIndexCatalog.rowGroupMaterializer(sessionB, revision, cached.defId(),
              cached.columnStoreOrNull().rowGroupCount());
      Assertions.assertTrue(cached.numericColumnIsIntegral(ageCol, fetcher),
          "the gate's column fill must succeed through session B's own fetcher");
      Assertions.assertTrue(cached.columnSparseClean(ageCol, fetcher, materializer),
          "slice evidence must resolve through session B's own fetcher");
      Assertions.assertEquals(cached.columnStoreOrNull().rowGroupCount(),
          cached.columnStoreOrNull().column(ageCol, fetcher).length,
          "the column fill must decode every leaf's slice through session B's own fetcher");
      Assertions.assertEquals(5L, ProjectionIndexByteScan.countRows(cached.rowGroupPayloads(materializer)),
          "whole-leaf materialization must succeed through session B's own materializer");
    }
  }

  @Test
  public void orPredicateTreesServeCountsAndAggregates() throws IOException {
    // P5b stage 6: AND/OR predicate trees serve through the fold kernels' tree path;
    // NOT falls back to the generic pipeline (missing-semantics flip) but stays correct;
    // several aggregate functions over the same (field, predicate) share ONE kernel scan.
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
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();

    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup("json-path1");
      final JsonResourceSession session = collection.getDatabase().beginResourceSession("sales.jn");
      final String resourceKey = session.getResourceConfig().getResource().toString();
      final int revision = session.getMostRecentRevisionNumber();
      final SirixVectorizedExecutor executor = new SirixVectorizedExecutor(session, revision, 2);
      SequentialPipelineStrategy.setVectorizedExecutor(executor);
      try {
        // OR count: age>50 → {52, 61}; active → {30, 52, 23}; union = 4 rows.
        try (final ByteArrayOutputStream out = new ByteArrayOutputStream();
            final PrintWriter printWriter = new PrintWriter(out)) {
          new Query(chain, """
                let $doc := jn:doc('json-path1','sales.jn')
                return count(for $r in $doc[] where $r.age gt 50 or $r.active return $r)
              """).serialize(ctx, printWriter);
          printWriter.flush();
          Assertions.assertEquals("4", out.toString());
        }
        // OR aggregate: age<25 → {23}; active → {30, 52, 23}; union sums to 105.
        try (final ByteArrayOutputStream out = new ByteArrayOutputStream();
            final PrintWriter printWriter = new PrintWriter(out)) {
          new Query(chain, """
                let $doc := jn:doc('json-path1','sales.jn')
                return sum(for $r in $doc[] where $r.age lt 25 or $r.active return $r.age)
              """).serialize(ctx, printWriter);
          printWriter.flush();
          Assertions.assertEquals("105", out.toString());
        }
        final ProjectionIndexRegistry.Handle handle = ProjectionIndexCatalog.lookupCovering(session, resourceKey,
            revision, new String[] {"[]"}, new String[] {"age"});
        Assertions.assertNotNull(handle);
        Assertions.assertFalse(handle.rawRowGroupsMaterialized(),
            "tree serving must stay on the fold kernels — no whole-leaf materialization");
        // NOT is excluded from the mask algebra — the generic pipeline answers, correctly.
        try (final ByteArrayOutputStream out = new ByteArrayOutputStream();
            final PrintWriter printWriter = new PrintWriter(out)) {
          new Query(chain, """
                let $doc := jn:doc('json-path1','sales.jn')
                return count(for $r in $doc[] where not($r.active) return $r)
              """).serialize(ctx, printWriter);
          printWriter.flush();
          Assertions.assertEquals("2", out.toString());
        }
        // Multi-aggregate single pass: min/max/avg over the same (field, predicate) must
        // run ONE kernel scan — the stats cache serves the second and third functions.
        final long scansBefore = SirixVectorizedExecutor.predicatedAggScanCount();
        try (final ByteArrayOutputStream out = new ByteArrayOutputStream();
            final PrintWriter printWriter = new PrintWriter(out)) {
          new Query(chain, """
                let $doc := jn:doc('json-path1','sales.jn')
                return {"min": min(for $r in $doc[] where $r.active return $r.age),
                        "max": max(for $r in $doc[] where $r.active return $r.age),
                        "avg": avg(for $r in $doc[] where $r.active return $r.age)}
              """).serialize(ctx, printWriter);
          printWriter.flush();
          Assertions.assertEquals("{\"min\":23,\"max\":52,\"avg\":35}", out.toString());
        }
        Assertions.assertEquals(1L, SirixVectorizedExecutor.predicatedAggScanCount() - scansBefore,
            "min+max+avg over one (field, predicate) must share a single kernel scan");
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        executor.close();
      }
    }
  }

  @Test
  public void perGroupAggregatesServeAndMatchTheGenericPipeline() throws IOException {
    // P5b stage 7a: group-by with per-group aggregates. The SAME compiled shape runs
    // twice — once with the executor registered (projection-served, counter-proven) and
    // once without (generic pipeline) — and the outputs must be identical.
    query("""
          jn:store('json-path1','groupagg.jn','[
            {"age": 30, "active": true,  "dept": "Eng"},
            {"age": 45, "active": false, "dept": "Sales"},
            {"age": 52, "active": true,  "dept": "Eng"},
            {"age": 23, "active": true,  "dept": "HR"},
            {"age": 44, "active": false, "dept": "Eng"},
            {"age": 99, "active": true}
          ]')
        """);
    query("""
          let $doc := jn:doc('json-path1','groupagg.jn')
          let $stats := jn:create-projection-index($doc, '/[]',
              ('/[]/age', '/[]/active', '/[]/dept'),
              ('long', 'boolean', 'string'))
          return {"revision": sdb:commit($doc)}
        """);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();

    final String groupQuery = """
          let $doc := jn:doc('json-path1','groupagg.jn')
          for $r in $doc[]
          let $d := $r.dept
          group by $d
          return {"dept": $d, "n": count($r), "total": sum($r.age), "top": max($r.age)}
        """;
    final String filteredGroupQuery = """
          let $doc := jn:doc('json-path1','groupagg.jn')
          for $r in $doc[]
          where $r.age gt 25
          let $d := $r.dept
          group by $d
          return {"dept": $d, "n": count($r), "total": sum($r.age), "top": max($r.age)}
        """;

    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup("json-path1");
      final JsonResourceSession session = collection.getDatabase().beginResourceSession("groupagg.jn");
      // Generic (no executor): the parity oracle.
      final String genericFull = evaluateQuery(chain, ctx, groupQuery);
      final String genericFiltered = evaluateQuery(chain, ctx, filteredGroupQuery);
      final SirixVectorizedExecutor executor =
          new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 2);
      SequentialPipelineStrategy.setVectorizedExecutor(executor);
      try {
        final long servedBefore = SirixVectorizedExecutor.groupAggServedCount();
        final String servedFull = evaluateQuery(chain, ctx, groupQuery);
        final String servedFiltered = evaluateQuery(chain, ctx, filteredGroupQuery);
        Assertions.assertEquals(genericFull, servedFull,
            "served per-group aggregates must match the generic pipeline exactly");
        Assertions.assertEquals(genericFiltered, servedFiltered,
            "served filtered per-group aggregates must match the generic pipeline exactly");
        Assertions.assertEquals(2L, SirixVectorizedExecutor.groupAggServedCount() - servedBefore,
            "both group-aggregate queries must be SERVED from the projection");
        // Sanity on the actual values (doc first-appearance order; record without dept
        // forms the null-key group; missing-age semantics never arise here).
        Assertions.assertEquals("{\"dept\":\"Eng\",\"n\":3,\"total\":126,\"top\":52}"
            + " {\"dept\":\"Sales\",\"n\":1,\"total\":45,\"top\":45}"
            + " {\"dept\":\"HR\",\"n\":1,\"total\":23,\"top\":23}" + " {\"dept\":null,\"n\":1,\"total\":99,\"top\":99}",
            servedFull);
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        executor.close();
      }
    }

    // Null-key group FIRST in document order (plus a partially-missing agg field): the
    // served emission order and per-group folds must still match the generic pipeline.
    query("""
          jn:store('json-path1','groupagg2.jn','[
            {"active": true,  "dept2": "zzz", "age": 7},
            {"age": 10, "active": true,  "dept": "B"},
            {"active": false, "dept": "A"},
            {"age": 20, "active": false, "dept": "B"}
          ]')
        """);
    query("""
          let $doc := jn:doc('json-path1','groupagg2.jn')
          let $stats := jn:create-projection-index($doc, '/[]',
              ('/[]/age', '/[]/active', '/[]/dept'),
              ('long', 'boolean', 'string'))
          return {"revision": sdb:commit($doc)}
        """);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    final String edgeQuery = """
          let $doc := jn:doc('json-path1','groupagg2.jn')
          for $r in $doc[]
          let $d := $r.dept
          group by $d
          return {"dept": $d, "n": count($r), "total": sum($r.age), "m": min($r.age)}
        """;
    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup("json-path1");
      final JsonResourceSession session = collection.getDatabase().beginResourceSession("groupagg2.jn");
      final String generic = evaluateQuery(chain, ctx, edgeQuery);
      final SirixVectorizedExecutor executor =
          new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 2);
      SequentialPipelineStrategy.setVectorizedExecutor(executor);
      try {
        final long servedBefore = SirixVectorizedExecutor.groupAggServedCount();
        Assertions.assertEquals(generic, evaluateQuery(chain, ctx, edgeQuery),
            "null-group-first + partial agg presence must match the generic pipeline exactly");
        Assertions.assertEquals(1L, SirixVectorizedExecutor.groupAggServedCount() - servedBefore,
            "the edge query must be SERVED, not silently fallen back");
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        executor.close();
      }
    }
  }

  @Test
  public void numericKeyGroupAggregatesServeInDocumentFirstAppearanceOrder() throws IOException {
    // The NUMERIC_LONG twin of perGroupAggregatesServeAndMatchTheGenericPipeline. The exact-string
    // assertion is the point: the differential harnesses elsewhere normalize by sorting and are
    // therefore blind to emission ORDER, and order (document first appearance, with the null-key
    // group interleaved at its own ordinal) is what the served records have to reproduce.
    // The `region` key is also the trap for the key's TYPE: a numeric key wrapped in Str would
    // serialize as "3" instead of 3 — a wrong result, not a degradation.
    query("""
          jn:store('json-path1','numgroupagg.jn','[
            {"region": 3, "amount": 10},
            {"region": 1, "amount": 20},
            {"amount": 30},
            {"region": 3, "amount": 5},
            {"region": 1},
            {"region": -2, "amount": 7}
          ]')
        """);
    query("""
          let $doc := jn:doc('json-path1','numgroupagg.jn')
          let $stats := jn:create-projection-index($doc, '/[]',
              ('/[]/region', '/[]/amount'),
              ('long', 'long'))
          return {"revision": sdb:commit($doc)}
        """);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();

    final String groupQuery = """
          let $doc := jn:doc('json-path1','numgroupagg.jn')
          for $r in $doc[]
          let $k := $r.region
          group by $k
          return {"region": $k, "n": count($r), "total": sum($r.amount), "top": max($r.amount)}
        """;
    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup("json-path1");
      final JsonResourceSession session = collection.getDatabase().beginResourceSession("numgroupagg.jn");
      final String generic = evaluateQuery(chain, ctx, groupQuery);
      final SirixVectorizedExecutor executor =
          new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 2);
      SequentialPipelineStrategy.setVectorizedExecutor(executor);
      try {
        final long aggBefore = SirixVectorizedExecutor.groupAggServedCount();
        final long numericBefore = SirixVectorizedExecutor.numericGroupByServedCount();
        final String served = evaluateQuery(chain, ctx, groupQuery);
        Assertions.assertEquals(generic, served,
            "served per-group aggregates under a numeric key must match the generic pipeline exactly");
        Assertions.assertEquals(1L, SirixVectorizedExecutor.groupAggServedCount() - aggBefore,
            "the numeric group-aggregate query must be SERVED from the projection");
        Assertions.assertEquals(1L, SirixVectorizedExecutor.numericGroupByServedCount() - numericBefore,
            "the NUMERIC route specifically must be the one that served it");
        // Document first-appearance order: 3, 1, null (record 3), -2. The key renders as an
        // xs:integer, and the group whose only member lacks `amount` emits the empty sequence.
        Assertions.assertEquals("{\"region\":3,\"n\":2,\"total\":15,\"top\":10}"
            + " {\"region\":1,\"n\":2,\"total\":20,\"top\":20}" + " {\"region\":null,\"n\":1,\"total\":30,\"top\":30}"
            + " {\"region\":-2,\"n\":1,\"total\":7,\"top\":7}", served);
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        executor.close();
      }
    }
  }

  @Test
  public void numericTopKGroupAggregatesServeFromColumnSlices() throws IOException {
    // The SLICED twin of the numeric group test above: an ordered + capped (top-K) numeric
    // single-key grouping over a PERSISTED catalog must be served by the column-slice group
    // kernel — no whole-leaf payload assembly. The dedicated counter is the proof: the byte
    // route would serve the identical bytes, so equality alone cannot tell the routes apart.
    // Fixture covers the three key regimes the kernel special-cases: a zero-valued key (the
    // table's zero side slot), a record MISSING the key (the missing-key side accumulator),
    // and a negative key, plus a predicate so the mask path is exercised.
    query("""
          jn:store('json-path1','slicedgroup.jn','[
            {"grp": 7,  "age": 30, "tag": "aa"},
            {"grp": 7,  "age": 45, "tag": "bbbb"},
            {"grp": 0,  "age": 20, "tag": "c"},
            {"grp": 3,  "age": 8},
            {"grp": 7,  "age": 12, "tag": "dd"},
            {"grp": 3,  "age": 15, "tag": "ee"},
            {"age": 99, "tag": "fff"},
            {"grp": -5, "age": 60, "tag": "g"},
            {"grp": 4,  "age": 2,  "tag": "hh"}
          ]')
        """);
    query("""
          let $doc := jn:doc('json-path1','slicedgroup.jn')
          let $stats := jn:create-projection-index($doc, '/[]',
              ('/[]/grp', '/[]/age', '/[]/tag'),
              ('long', 'long', 'string'))
          return {"revision": sdb:commit($doc)}
        """);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();

    // `age gt 5` also drops the grp-4 record entirely — a group that must never surface.
    final String topKQuery = """
          subsequence(
            for $r in jn:doc('json-path1','slicedgroup.jn')[]
            where $r.age gt 5
            let $g := $r.grp
            group by $g
            let $c := count($r)
            order by $c descending
            return {"g": $g, "c": $c, "total": sum($r.age), "hi": max($r.age)}, 1, 3)
        """;
    // strlen operand on the NUMERIC arm: the agg slice is the STRING dict, codepoint counts
    // fold with missing-is-0 (the grp-3/age-8 record has no tag).
    final String strlenQuery = """
          subsequence(
            for $r in jn:doc('json-path1','slicedgroup.jn')[]
            where $r.age gt 5
            let $g := $r.grp, $len := fn:string-length($r.tag)
            group by $g
            let $c := count($r)
            order by $c descending
            return {"g": $g, "c": $c, "slen": sum($len)}, 1, 3)
        """;
    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup("json-path1");
      final JsonResourceSession session = collection.getDatabase().beginResourceSession("slicedgroup.jn");
      // Generic: the parity oracle. It MUST run on a chain that cannot auto-wire an executor.
      // createWithJsonStore auto-wires one PER QUERY, so computing the oracle on `chain` gets the
      // oracle served by the very route under test, and that breaks this test twice over:
      // 1. the equality below stops being a cross-check and compares served-to-served, so a
      // sliced kernel that is wrong in exactly the same way as itself still passes;
      // 2. each oracle leg is a route arrival, so it spends this handle's promotion budget
      // (SLICED_PROMOTE_AFTER) before the measured pair runs — the handle promotes to the
      // whole-leaf byte kernel early and the sliced counter below reads 1 instead of 2.
      // (2) is silent and looks exactly like a product regression, which is what it was mistaken
      // for; do NOT "simplify" this back to `chain`. Deliberately NOT closed: SirixCompileChain
      // #close tears down the SHARED store's sessions, which the measured legs and the enclosing
      // try-with-resources still need.
      final SirixCompileChain genericChain = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store);
      final String generic = evaluateQuery(genericChain, ctx, topKQuery);
      final String genericStrlen = evaluateQuery(genericChain, ctx, strlenQuery);
      final SirixVectorizedExecutor executor =
          new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 2);
      SequentialPipelineStrategy.setVectorizedExecutor(executor);
      try {
        final long aggBefore = SirixVectorizedExecutor.groupAggServedCount();
        final long slicedBefore = SirixVectorizedExecutor.groupAggSlicedServedCount();
        final String served = evaluateQuery(chain, ctx, topKQuery);
        final String servedStrlen = evaluateQuery(chain, ctx, strlenQuery);
        Assertions.assertEquals(generic, served,
            "sliced-served top-K numeric grouping must match the generic pipeline exactly");
        Assertions.assertEquals(genericStrlen, servedStrlen,
            "sliced-served strlen-operand grouping must match the generic pipeline exactly");
        Assertions.assertEquals(2L, SirixVectorizedExecutor.groupAggServedCount() - aggBefore,
            "both numeric group-aggregate queries must be SERVED from the projection");
        Assertions.assertEquals(2L, SirixVectorizedExecutor.groupAggSlicedServedCount() - slicedBefore,
            "the COLUMN-SLICE group kernel specifically must have served both (whole-leaf would "
                + "produce identical bytes, so this counter is the only route-level evidence)");
        // Stable count-descending over first-appearance order 7, 0, 3, null, -5:
        // 7 (3 rows) > 3 (2 rows) > 0 (1 row, first-seen before the other singletons).
        Assertions.assertEquals("{\"g\":7,\"c\":3,\"total\":87,\"hi\":45}" + " {\"g\":3,\"c\":2,\"total\":23,\"hi\":15}"
            + " {\"g\":0,\"c\":1,\"total\":20,\"hi\":20}", served);
        // 7: aa+bbbb+dd = 8; 3: (missing→0)+ee = 2; 0: c = 1.
        Assertions.assertEquals(
            "{\"g\":7,\"c\":3,\"slen\":8}" + " {\"g\":3,\"c\":2,\"slen\":2}" + " {\"g\":0,\"c\":1,\"slen\":1}",
            servedStrlen);
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        executor.close();
      }
    }
  }

  @Test
  public void stringTopKGroupAggregatesServeFromColumnSlices() throws IOException {
    // The STRING flat twin: group identity is the FNV of the slice dict entry's bytes, winners
    // materialize from the slice dict (no payload assembly), strlen operands fold codepoint
    // counts from the slice dict, and a regex-transformed key hashes the TRANSFORMED bytes.
    query("""
          jn:store('json-path1','slicedgroupstr.jn','[
            {"dept": "Eng",   "age": 30, "city": "Berlin"},
            {"dept": "Eng",   "age": 45, "city": "Muc"},
            {"dept": "Sales", "age": 50, "city": "Ulm"},
            {"age": 99, "city": "Bonn"},
            {"dept": "HR",    "age": 23},
            {"dept": "Eng",   "age": 4,  "city": "X"}
          ]')
        """);
    query("""
          let $doc := jn:doc('json-path1','slicedgroupstr.jn')
          let $stats := jn:create-projection-index($doc, '/[]',
              ('/[]/dept', '/[]/age', '/[]/city'),
              ('string', 'long', 'string'))
          return {"revision": sdb:commit($doc)}
        """);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();

    // Missing dept (99) forms the null-key group; missing city (HR) folds strlen 0; age gt 5
    // drops the Eng/4 record entirely.
    final String topKQuery = """
          subsequence(
            for $r in jn:doc('json-path1','slicedgroupstr.jn')[]
            where $r.age gt 5
            let $d := $r.dept, $len := fn:string-length($r.city)
            group by $d
            let $c := count($r)
            order by $c descending
            return {"d": $d, "c": $c, "total": sum($r.age),
                    "slen": sum($len)}, 1, 3)
        """;
    // Regex key: the where filters the missing-dept record OUT — fn:replace over a missing key
    // is "" (a real key), which the kernel declines; this query must stay servable.
    final String regexQuery = """
          subsequence(
            for $r in jn:doc('json-path1','slicedgroupstr.jn')[]
            where $r.age lt 60
            let $k := fn:replace($r.dept, 'ng', 'X')
            group by $k
            let $c := count($r)
            order by $c descending
            return {"k": $k, "c": $c}, 1, 2)
        """;
    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup("json-path1");
      final JsonResourceSession session = collection.getDatabase().beginResourceSession("slicedgroupstr.jn");
      // Oracle on a NON-auto-wiring chain — see the numeric twin above for the full reasoning:
      // `chain` auto-wires an executor per query, so the oracle would be served by the very route
      // under test, comparing served-to-served AND burning the handle's promotion budget, which
      // silently drops the sliced counter below from 2 to 1 and reads as a product regression.
      final SirixCompileChain genericChain = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store);
      final String genericTopK = evaluateQuery(genericChain, ctx, topKQuery);
      final String genericRegex = evaluateQuery(genericChain, ctx, regexQuery);
      final SirixVectorizedExecutor executor =
          new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 2);
      SequentialPipelineStrategy.setVectorizedExecutor(executor);
      try {
        final long aggBefore = SirixVectorizedExecutor.groupAggServedCount();
        final long slicedBefore = SirixVectorizedExecutor.groupAggSlicedServedCount();
        final String servedTopK = evaluateQuery(chain, ctx, topKQuery);
        final String servedRegex = evaluateQuery(chain, ctx, regexQuery);
        Assertions.assertEquals(genericTopK, servedTopK,
            "sliced-served string top-K grouping must match the generic pipeline exactly");
        Assertions.assertEquals(genericRegex, servedRegex,
            "sliced-served regex-keyed grouping must match the generic pipeline exactly");
        Assertions.assertEquals(2L, SirixVectorizedExecutor.groupAggServedCount() - aggBefore,
            "both string group-aggregate queries must be SERVED from the projection");
        Assertions.assertEquals(2L, SirixVectorizedExecutor.groupAggSlicedServedCount() - slicedBefore,
            "the COLUMN-SLICE string kernel specifically must have served both");
        Assertions.assertEquals("{\"d\":\"Eng\",\"c\":2,\"total\":75,\"slen\":9}"
            + " {\"d\":\"Sales\",\"c\":1,\"total\":50,\"slen\":3}" + " {\"d\":null,\"c\":1,\"total\":99,\"slen\":4}",
            servedTopK);
        Assertions.assertEquals("{\"k\":\"EX\",\"c\":3} {\"k\":\"Sales\",\"c\":1}", servedRegex);
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        executor.close();
      }
    }
  }

  @Test
  public void anOverBudgetColumnServesThroughWindowedSlicesInsteadOfDecliningOrFailing() throws IOException {
    // A whole-column slice fill declines through the store's budget door. Kind-sliceability knows
    // nothing about that budget, so a planner that resolved resident arrays on kind alone would
    // throw inside the arm, land in the group route's fail-soft catch — GROUP_AGG_FAILED, documented
    // to mean a defect or corruption — and drop the query to the GENERIC pipeline. The route is
    // decided by KIND and residency by FIT: an over-budget column keeps the sliced kernels, fed
    // per-sub-chunk WINDOWED slices by every worker (the 100M fat-column shape), served from the
    // projection with no failure signal. The pre-lever contract (whole-leaf byte kernel) is gone.
    query("""
          jn:store('json-path1','budgetgroupstr.jn','[
            {"dept": "Eng",   "age": 30, "city": "Berlin"},
            {"dept": "Eng",   "age": 45, "city": "Muc"},
            {"dept": "Sales", "age": 50, "city": "Ulm"},
            {"age": 99, "city": "Bonn"},
            {"dept": "HR",    "age": 23},
            {"dept": "Eng",   "age": 4,  "city": "X"}
          ]')
        """);
    query("""
          let $doc := jn:doc('json-path1','budgetgroupstr.jn')
          let $stats := jn:create-projection-index($doc, '/[]',
              ('/[]/dept', '/[]/age', '/[]/city'),
              ('string', 'long', 'string'))
          return {"revision": sdb:commit($doc)}
        """);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();

    final String topKQuery = """
          subsequence(
            for $r in jn:doc('json-path1','budgetgroupstr.jn')[]
            where $r.age gt 5
            let $d := $r.dept, $len := fn:string-length($r.city)
            group by $d
            let $c := count($r)
            order by $c descending
            return {"d": $d, "c": $c, "total": sum($r.age),
                    "slen": sum($len)}, 1, 3)
        """;
    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup("json-path1");
      final JsonResourceSession session = collection.getDatabase().beginResourceSession("budgetgroupstr.jn");
      // Oracle on a NON-auto-wiring chain, computed BEFORE the budget is shrunk — see the string
      // twin above for the full reasoning.
      final SirixCompileChain genericChain = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store);
      final String generic = evaluateQuery(genericChain, ctx, topKQuery);
      final SirixVectorizedExecutor executor =
          new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 2);
      SequentialPipelineStrategy.setVectorizedExecutor(executor);
      final long priorBudget = ProjectionColumnStore.setColumnFillBudgetBytesForTesting(1L);
      try {
        final long aggBefore = SirixVectorizedExecutor.groupAggServedCount();
        final long slicedBefore = SirixVectorizedExecutor.groupAggSlicedServedCount();
        final long windowedBefore = SirixVectorizedExecutor.groupWindowedSlicesCount();
        final long failedBefore = SirixVectorizedExecutor.groupAggFailedCount();
        final String served = evaluateQuery(chain, ctx, topKQuery);
        Assertions.assertEquals(generic, served, "the answer must not change with the fill budget");
        Assertions.assertEquals(1L, SirixVectorizedExecutor.groupAggServedCount() - aggBefore,
            "an over-budget column must still be SERVED from the projection");
        Assertions.assertEquals(1L, SirixVectorizedExecutor.groupAggSlicedServedCount() - slicedBefore,
            "the sliced string arm must serve the refused column through windowed slices");
        Assertions.assertEquals(1L, SirixVectorizedExecutor.groupWindowedSlicesCount() - windowedBefore,
            "the windowed-slice route must have engaged exactly once");
        Assertions.assertEquals(0L, SirixVectorizedExecutor.groupAggFailedCount() - failedBefore,
            "routing into a fill that declines raises a false defect signal");
      } finally {
        ProjectionColumnStore.setColumnFillBudgetBytesForTesting(priorBudget);
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        executor.close();
      }
    }
  }

  @Test
  public void compositeTopKGroupAggregatesServeFromColumnSlices() throws IOException {
    // Unit 4 twin: the COMPOSITE flat arm (Q11 shape — numeric + string 2-key, COUNT(DISTINCT),
    // ordered + capped) must serve from column slices. The record missing `model` exercises the
    // MISSING-component sentinel as part of the composite identity (not a side group), and the
    // winner re-read materializes key parts from slices, not payloads.
    query("""
          jn:store('json-path1','slicedcomp.jn','[
            {"phone": 1, "model": "A", "uid": 10},
            {"phone": 1, "model": "A", "uid": 11},
            {"phone": 1, "model": "B", "uid": 10},
            {"phone": 2, "model": "A", "uid": 12},
            {"phone": 1, "uid": 13},
            {"phone": 1, "model": "A", "uid": 10},
            {"phone": 2, "model": "A", "uid": 14}
          ]')
        """);
    query("""
          let $doc := jn:doc('json-path1','slicedcomp.jn')
          let $stats := jn:create-projection-index($doc, '/[]',
              ('/[]/phone', '/[]/model', '/[]/uid'),
              ('long', 'string', 'long'))
          return {"revision": sdb:commit($doc)}
        """);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();

    final String topKQuery = """
          subsequence(
            for $r in jn:doc('json-path1','slicedcomp.jn')[]
            let $p := $r.phone, $m := $r.model
            group by $p, $m
            let $u := count(distinct-values($r.uid))
            order by $u descending
            return {"p": $p, "m": $m, "u": $u, "n": count($r)}, 1, 3)
        """;
    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup("json-path1");
      final JsonResourceSession session = collection.getDatabase().beginResourceSession("slicedcomp.jn");
      final String generic = evaluateQuery(chain, ctx, topKQuery);
      final SirixVectorizedExecutor executor =
          new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 2);
      SequentialPipelineStrategy.setVectorizedExecutor(executor);
      try {
        final long aggBefore = SirixVectorizedExecutor.groupAggServedCount();
        final long slicedBefore = SirixVectorizedExecutor.groupAggSlicedServedCount();
        final String served = evaluateQuery(chain, ctx, topKQuery);
        Assertions.assertEquals(generic, served,
            "sliced-served composite top-K grouping must match the generic pipeline exactly");
        Assertions.assertEquals(1L, SirixVectorizedExecutor.groupAggServedCount() - aggBefore,
            "the composite group-aggregate query must be SERVED from the projection");
        Assertions.assertEquals(1L, SirixVectorizedExecutor.groupAggSlicedServedCount() - slicedBefore,
            "the COLUMN-SLICE composite kernel specifically must have served it");
        // (1,A): uids {10,11} u=2 n=3; (1,B): {10} u=1 n=1; (2,A): {12,14} u=2 n=2;
        // (1,missing): {13} u=1 n=1. Count-distinct desc, first-appearance tiebreak:
        // (1,A) u=2, (2,A) u=2, then (1,B) u=1 (seen before the missing-model group).
        Assertions.assertEquals("{\"p\":1,\"m\":\"A\",\"u\":2,\"n\":3}" + " {\"p\":2,\"m\":\"A\",\"u\":2,\"n\":2}"
            + " {\"p\":1,\"m\":\"B\",\"u\":1,\"n\":1}", served);
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        executor.close();
      }
    }
  }

  @Test
  public void multiKeyGroupAggregatesServeAndMatchTheGenericPipeline() throws IOException {
    // Gap item 1a: N group keys. Fixture exercises every composite-missing combination
    // (both present / missing city / missing dept / missing both), plus a filtered
    // variant, reversed key order in the record, and a 3-key grouping. All differential
    // against the generic pipeline; all must be SERVED.
    query("""
          jn:store('json-path1','mkgroup.jn','[
            {"age": 30, "dept": "Eng",   "city": "Berlin", "grade": "A"},
            {"age": 45, "dept": "Sales", "city": "Munich", "grade": "B"},
            {"age": 52, "dept": "Eng",   "city": "Berlin", "grade": "B"},
            {"age": 23, "dept": "HR",    "city": "Berlin", "grade": "A"},
            {"age": 44, "dept": "Eng",   "city": "Munich"},
            {"age": 99,                  "city": "Berlin", "grade": "A"},
            {"age": 61, "dept": "Eng",   "grade": "A"},
            {"age": 77}
          ]')
        """);
    query("""
          let $doc := jn:doc('json-path1','mkgroup.jn')
          let $stats := jn:create-projection-index($doc, '/[]',
              ('/[]/age', '/[]/dept', '/[]/city', '/[]/grade'),
              ('long', 'string', 'string', 'string'))
          return {"revision": sdb:commit($doc)}
        """);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    final String[] queries = {"""
          let $doc := jn:doc('json-path1','mkgroup.jn')
          for $r in $doc[]
          let $d := $r.dept
          let $c := $r.city
          group by $d, $c
          return {"dept": $d, "city": $c, "n": count($r), "total": sum($r.age)}
        """, """
          let $doc := jn:doc('json-path1','mkgroup.jn')
          for $r in $doc[]
          where $r.age gt 25
          let $d := $r.dept
          let $c := $r.city
          group by $d, $c
          return {"dept": $d, "city": $c, "n": count($r), "total": sum($r.age)}
        """,
        // Key entries in the record REVERSED relative to the group-spec order.
        """
              let $doc := jn:doc('json-path1','mkgroup.jn')
              for $r in $doc[]
              let $d := $r.dept
              let $c := $r.city
              group by $d, $c
              return {"city": $c, "dept": $d, "n": count($r), "top": max($r.age)}
            """,
        // Three keys.
        """
              let $doc := jn:doc('json-path1','mkgroup.jn')
              for $r in $doc[]
              let $d := $r.dept
              let $c := $r.city
              let $g := $r.grade
              group by $d, $c, $g
              return {"dept": $d, "city": $c, "grade": $g, "n": count($r), "m": min($r.age)}
            """};
    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup("json-path1");
      final JsonResourceSession session = collection.getDatabase().beginResourceSession("mkgroup.jn");
      final String[] generic = new String[queries.length];
      for (int i = 0; i < queries.length; i++) {
        generic[i] = evaluateQuery(chain, ctx, queries[i]);
      }
      final SirixVectorizedExecutor executor =
          new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 2);
      SequentialPipelineStrategy.setVectorizedExecutor(executor);
      try {
        for (int i = 0; i < queries.length; i++) {
          final long servedBefore = SirixVectorizedExecutor.groupAggServedCount();
          Assertions.assertEquals(generic[i], evaluateQuery(chain, ctx, queries[i]),
              "multi-key group-aggregate parity, query " + i);
          Assertions.assertEquals(1L, SirixVectorizedExecutor.groupAggServedCount() - servedBefore,
              "multi-key group-aggregate query " + i + " must be SERVED from the projection");
        }
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        executor.close();
      }
    }
  }

  @Test
  public void groupAggregateShadowingAndRegroupShapesDecline() throws IOException {
    // Review findings (gap 1). Shadowed let: Brackit RENAMES shadowed variables at parse
    // time, so `let $d := ... let $d := ...` reaches detection as two DISTINCT vars and
    // the group spec resolves to the LAST binding — served correctly (the stage's
    // duplicate-name decline stays as belt-and-braces for front-ends that don't rename).
    // Double group-by: re-groups the grouped stream (type-erroring on >1-item keys) —
    // MUST decline; the outcome (error included) must match the interpreter exactly.
    query("""
          jn:store('json-path1','shadow.jn','[
            {"dept": "x", "city": "y", "age": 1},
            {"dept": "x", "city": "z", "age": 2}
          ]')
        """);
    query("""
          let $doc := jn:doc('json-path1','shadow.jn')
          let $stats := jn:create-projection-index($doc, '/[]',
              ('/[]/dept', '/[]/city', '/[]/age'),
              ('string', 'string', 'long'))
          return {"revision": sdb:commit($doc)}
        """);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    // Interpreter groups by CITY (the last $d binding shadows the first) -> two groups.
    final String shadowedQuery = """
          let $doc := jn:doc('json-path1','shadow.jn')
          for $r in $doc[]
          let $d := $r.dept
          let $d := $r.city
          group by $d
          return {"d": $d, "n": count($r)}
        """;
    // Re-grouping the grouped stream: $b is a 2-item sequence for group x -> XPTY0004.
    final String regroupQuery = """
          let $doc := jn:doc('json-path1','shadow.jn')
          for $r in $doc[]
          let $a := $r.dept
          let $b := $r.city
          group by $a
          group by $b
          return {"a": $a, "b": $b, "s": sum($r.age)}
        """;
    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup("json-path1");
      final JsonResourceSession session = collection.getDatabase().beginResourceSession("shadow.jn");
      String genericShadowed;
      try {
        genericShadowed = evaluateQuery(chain, ctx, shadowedQuery);
      } catch (final RuntimeException e) {
        genericShadowed = "ERROR:" + e.getClass().getSimpleName();
      }
      String genericRegroup;
      try {
        genericRegroup = evaluateQuery(chain, ctx, regroupQuery);
      } catch (final RuntimeException e) {
        genericRegroup = "ERROR:" + e.getClass().getSimpleName();
      }
      final SirixVectorizedExecutor executor =
          new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 2);
      SequentialPipelineStrategy.setVectorizedExecutor(executor);
      try {
        final long servedBeforeShadowed = SirixVectorizedExecutor.groupAggServedCount();
        String servedShadowed;
        try {
          servedShadowed = evaluateQuery(chain, ctx, shadowedQuery);
        } catch (final RuntimeException e) {
          servedShadowed = "ERROR:" + e.getClass().getSimpleName();
        }
        Assertions.assertEquals(genericShadowed, servedShadowed,
            "shadowed let bindings must reproduce the interpreter outcome (last binding wins)");
        Assertions.assertEquals(1L, SirixVectorizedExecutor.groupAggServedCount() - servedBeforeShadowed,
            "renamed shadowed-let shape resolves to the LAST binding and must be SERVED");
        final long servedBeforeRegroup = SirixVectorizedExecutor.groupAggServedCount();
        String servedRegroup;
        try {
          servedRegroup = evaluateQuery(chain, ctx, regroupQuery);
        } catch (final RuntimeException e) {
          servedRegroup = "ERROR:" + e.getClass().getSimpleName();
        }
        Assertions.assertEquals(genericRegroup, servedRegroup,
            "double group-by must reproduce the interpreter outcome, error included");
        Assertions.assertEquals(0L, SirixVectorizedExecutor.groupAggServedCount() - servedBeforeRegroup,
            "double-group-by must DECLINE serving");
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        executor.close();
      }
    }
  }

  @Test
  public void groupAggregateSumOverflowDeclinesToPromotingInterpreter() throws IOException {
    // Review finding (gap 1): a per-group long sum that overflows must DECLINE — the
    // interpreter promotes to exact decimal; a wrapped long would silently serve a
    // wrong total. Applies to single- AND multi-key paths (shared exact-sum kernels).
    query("""
          jn:store('json-path1','ovf.jn','[
            {"d": "a", "c": "x", "v": 9223372036854775000},
            {"d": "a", "c": "x", "v": 10000},
            {"d": "b", "c": "y", "v": 7}
          ]')
        """);
    query("""
          let $doc := jn:doc('json-path1','ovf.jn')
          let $stats := jn:create-projection-index($doc, '/[]',
              ('/[]/d', '/[]/c', '/[]/v'),
              ('string', 'string', 'long'))
          return {"revision": sdb:commit($doc)}
        """);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    final String[] queries = {"""
          let $doc := jn:doc('json-path1','ovf.jn')
          for $r in $doc[]
          let $d := $r.d
          group by $d
          return {"d": $d, "total": sum($r.v)}
        """, """
          let $doc := jn:doc('json-path1','ovf.jn')
          for $r in $doc[]
          let $d := $r.d
          let $c := $r.c
          group by $d, $c
          return {"d": $d, "c": $c, "total": sum($r.v)}
        """};
    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup("json-path1");
      final JsonResourceSession session = collection.getDatabase().beginResourceSession("ovf.jn");
      final String[] generic = new String[queries.length];
      for (int i = 0; i < queries.length; i++) {
        generic[i] = evaluateQuery(chain, ctx, queries[i]);
      }
      final SirixVectorizedExecutor executor =
          new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 2);
      SequentialPipelineStrategy.setVectorizedExecutor(executor);
      try {
        final long servedBefore = SirixVectorizedExecutor.groupAggServedCount();
        final long failedBefore = SirixVectorizedExecutor.groupAggFailedCount();
        for (int i = 0; i < queries.length; i++) {
          Assertions.assertEquals(generic[i], evaluateQuery(chain, ctx, queries[i]),
              "overflowing group sum must fall back to the promoting interpreter, query " + i);
        }
        Assertions.assertEquals(0L, SirixVectorizedExecutor.groupAggServedCount() - servedBefore,
            "overflowing group sums must DECLINE, never serve a wrapped total");
        Assertions.assertEquals(0L, SirixVectorizedExecutor.groupAggFailedCount() - failedBefore,
            "overflow is an expected decline, not a serving failure");
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        executor.close();
      }
    }
  }

  @Test
  public void computedExpressionAggregatesServeAndMatchTheGenericPipeline() throws IOException {
    // Gap item 2: aggregates over +/-/* trees of covered numeric fields. Rows missing any
    // operand contribute nothing (empty arithmetic); count counts only fully-present
    // rows. All differential; all SERVED.
    query("""
          jn:store('json-path1','computed.jn','[
            {"a": 3, "b": 4,  "f": 1},
            {"a": 5,          "f": 2},
            {"b": 7,          "f": 3},
            {"a": 2, "b": 10, "f": 4},
            {"f": 5}
          ]')
        """);
    query("""
          let $doc := jn:doc('json-path1','computed.jn')
          let $stats := jn:create-projection-index($doc, '/[]',
              ('/[]/a', '/[]/b', '/[]/f'),
              ('long', 'long', 'long'))
          return {"revision": sdb:commit($doc)}
        """);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    final String[] queries =
        {"let $doc := jn:doc('json-path1','computed.jn')\n" + "return sum(for $r in $doc[] return $r.a * $r.b)",
            "let $doc := jn:doc('json-path1','computed.jn')\n" + "return avg(for $r in $doc[] return $r.a + $r.b - 1)",
            "let $doc := jn:doc('json-path1','computed.jn')\n"
                + "return max(for $r in $doc[] where $r.f gt 1 return $r.a * 2)",
            "let $doc := jn:doc('json-path1','computed.jn')\n" + "return count(for $r in $doc[] return $r.a * $r.b)",
            "let $doc := jn:doc('json-path1','computed.jn')\n" + "return min(for $r in $doc[] return $r.b - $r.a)"};
    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup("json-path1");
      final JsonResourceSession session = collection.getDatabase().beginResourceSession("computed.jn");
      final String[] generic = new String[queries.length];
      for (int i = 0; i < queries.length; i++) {
        generic[i] = evaluateQuery(chain, ctx, queries[i]);
      }
      final SirixVectorizedExecutor executor =
          new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 2);
      SequentialPipelineStrategy.setVectorizedExecutor(executor);
      try {
        for (int i = 0; i < queries.length; i++) {
          // A pure two-field product/sum is deliberately served by the binary SLICE FOLD (body
          // chains + presence intersection) instead of the whole-leaf computed route — both are
          // projection-served, so either counter satisfies the assertion.
          final long servedBefore =
              SirixVectorizedExecutor.computedAggServedCount() + SirixVectorizedExecutor.binaryAggregatesServed();
          Assertions.assertEquals(generic[i], evaluateQuery(chain, ctx, queries[i]),
              "computed-aggregate parity, query " + i);
          Assertions.assertEquals(1L, SirixVectorizedExecutor.computedAggServedCount()
              + SirixVectorizedExecutor.binaryAggregatesServed() - servedBefore,
              "computed-aggregate query " + i + " must be SERVED from the projection");
        }
        // Sanity on real values: products over fully-present rows only (12 + 20).
        Assertions.assertEquals("32", evaluateQuery(chain, ctx, queries[0]));
        Assertions.assertEquals("2", evaluateQuery(chain, ctx, queries[3]));
        // ALL-CONSTANT computed return: no covered column — must DECLINE cleanly at
        // detection (no serve, no failure-counter pollution), interpreter answers.
        final String allConst =
            "let $doc := jn:doc('json-path1','computed.jn')\n" + "return sum(for $r in $doc[] return 2 * 3)";
        final long constServedBefore = SirixVectorizedExecutor.computedAggServedCount();
        final long constFailedBefore = SirixVectorizedExecutor.computedAggFailedCount();
        final String constGeneric;
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        try {
          constGeneric = evaluateQuery(chain, ctx, allConst);
        } finally {
          SequentialPipelineStrategy.setVectorizedExecutor(executor);
        }
        Assertions.assertEquals(constGeneric, evaluateQuery(chain, ctx, allConst),
            "all-constant computed aggregate must match the interpreter");
        Assertions.assertEquals(0L, SirixVectorizedExecutor.computedAggServedCount() - constServedBefore,
            "all-constant computed aggregate must not be served");
        Assertions.assertEquals(0L, SirixVectorizedExecutor.computedAggFailedCount() - constFailedBefore,
            "all-constant decline must not pollute the failure counter");
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        executor.close();
      }
    }

    // OVERFLOW parity: 2^62 * 4 overflows a long. The interpreter PROMOTES to exact
    // decimal; exact-math serving must DECLINE (never wrap) and the generic answer flows
    // through — differential equality plus an unchanged served counter prove it.
    query("""
          jn:store('json-path1','computed2.jn','[
            {"a": 4611686018427387904, "b": 4},
            {"a": 10, "b": 3}
          ]')
        """);
    query("""
          let $doc := jn:doc('json-path1','computed2.jn')
          let $stats := jn:create-projection-index($doc, '/[]',
              ('/[]/a', '/[]/b'),
              ('long', 'long'))
          return {"revision": sdb:commit($doc)}
        """);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    final String overflowQuery =
        "let $doc := jn:doc('json-path1','computed2.jn')\n" + "return sum(for $r in $doc[] return $r.a * $r.b)";
    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup("json-path1");
      final JsonResourceSession session = collection.getDatabase().beginResourceSession("computed2.jn");
      final String generic = evaluateQuery(chain, ctx, overflowQuery);
      final SirixVectorizedExecutor executor =
          new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 2);
      SequentialPipelineStrategy.setVectorizedExecutor(executor);
      try {
        final long servedBefore = SirixVectorizedExecutor.computedAggServedCount();
        final long failedBefore = SirixVectorizedExecutor.computedAggFailedCount();
        Assertions.assertEquals(generic, evaluateQuery(chain, ctx, overflowQuery),
            "overflow must fall back to the interpreter's decimal-promoting arithmetic");
        Assertions.assertEquals(0L, SirixVectorizedExecutor.computedAggServedCount() - servedBefore,
            "overflowing computed aggregate must DECLINE, not serve a wrapped value");
        Assertions.assertEquals(0L, SirixVectorizedExecutor.computedAggFailedCount() - failedBefore,
            "overflow is an expected decline, not a serving failure");
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        executor.close();
      }
    }
  }

  @Test
  public void perGroupAggregatesSurviveMultiLeafParallelMerge() throws IOException {
    // 70k rows ≈ 69 leaves → chunked parallel kernels + cross-thread merge + global
    // first-seen ordinals actually engage (single-leaf fixtures never reach them).
    // Deterministic data; covers avg, duplicate (func, field) pairs under two names,
    // empty-string group keys, missing group keys, partially-missing agg cells, and a
    // predicate that filters out entire groups — all differentially against the
    // generic pipeline.
    final StringBuilder json = new StringBuilder(3_000_000).append('[');
    for (int i = 0; i < 70_000; i++) {
      if (i > 0)
        json.append(',');
      json.append("{\"active\":").append((i & 1) == 0);
      if (i % 11 != 3) {
        json.append(",\"age\":").append(i % 97);
      }
      final int deptSel = i % 7;
      if (deptSel != 6) {
        json.append(",\"dept\":\"")
            .append(deptSel == 5
                ? ""
                : "D" + deptSel)
            .append('"');
      }
      json.append('}');
    }
    json.append(']');
    query("jn:store('json-path1','groupagg3.jn','" + json + "')");
    query("""
          let $doc := jn:doc('json-path1','groupagg3.jn')
          let $stats := jn:create-projection-index($doc, '/[]',
              ('/[]/age', '/[]/active', '/[]/dept'),
              ('long', 'boolean', 'string'))
          return {"revision": sdb:commit($doc)}
        """);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    final String[] queries = {"""
          let $doc := jn:doc('json-path1','groupagg3.jn')
          for $r in $doc[]
          let $d := $r.dept
          group by $d
          return {"dept": $d, "n": count($r), "s": sum($r.age), "a": avg($r.age),
                  "lo": min($r.age), "hi": max($r.age), "s2": sum($r.age)}
        """, """
          let $doc := jn:doc('json-path1','groupagg3.jn')
          for $r in $doc[]
          where $r.age gt 90
          let $d := $r.dept
          group by $d
          return {"dept": $d, "n": count($r), "s": sum($r.age)}
        """,
        // NOTE: a {"dept": $d, "n": count($r)} return here would be the brackit-canonical
        // count shape and route through the PRE-EXISTING count path (the overlap rule) —
        // the sum entry keeps this filter-all query on the stage-7a path under test.
        """
              let $doc := jn:doc('json-path1','groupagg3.jn')
              for $r in $doc[]
              where $r.age gt 200
              let $d := $r.dept
              group by $d
              return {"dept": $d, "n": count($r), "s": sum($r.age)}
            """};
    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup("json-path1");
      final JsonResourceSession session = collection.getDatabase().beginResourceSession("groupagg3.jn");
      final String[] generic = new String[queries.length];
      for (int i = 0; i < queries.length; i++) {
        generic[i] = evaluateQuery(chain, ctx, queries[i]);
      }
      final SirixVectorizedExecutor executor =
          new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 4);
      SequentialPipelineStrategy.setVectorizedExecutor(executor);
      try {
        final long servedBefore = SirixVectorizedExecutor.groupAggServedCount();
        final long failedBefore = SirixVectorizedExecutor.groupAggFailedCount();
        for (int i = 0; i < queries.length; i++) {
          Assertions.assertEquals(generic[i], evaluateQuery(chain, ctx, queries[i]),
              "multi-leaf group-aggregate parity, query " + i);
        }
        Assertions.assertEquals(queries.length, SirixVectorizedExecutor.groupAggServedCount() - servedBefore,
            "every multi-leaf group-aggregate query must be SERVED");
        Assertions.assertEquals(0L, SirixVectorizedExecutor.groupAggFailedCount() - failedBefore,
            "no serving attempt may fail with an exception");
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        executor.close();
      }
    }
  }

  @Test
  public void sortedScansServeAndMatchTheGenericPipeline() throws IOException {
    // P5b stage 7b: `order by $r.f [descending] return $r` served as a pre-sorted
    // record-key stream. Fixture covers duplicate sort values (stability!), predicates
    // (asc and desc), and top-K via subsequence. Missing-order-field declines and
    // multi-leaf cross-boundary stability are covered by the dedicated blocks below.
    query("""
          jn:store('json-path1','sorted.jn','[
            {"age": 30, "active": true,  "dept": "Eng"},
            {"age": 10, "active": false, "dept": "Sales"},
            {"age": 30, "active": false, "dept": "Ops"},
            {"age": 5,  "active": true,  "dept": "Eng"},
            {"age": 22, "active": true,  "dept": "QA"}
          ]')
        """);
    query("""
          let $doc := jn:doc('json-path1','sorted.jn')
          let $stats := jn:create-projection-index($doc, '/[]',
              ('/[]/age', '/[]/active', '/[]/dept'),
              ('long', 'boolean', 'string'))
          return {"revision": sdb:commit($doc)}
        """);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    final String[] queries = {
        "let $doc := jn:doc('json-path1','sorted.jn')\n" + "for $r in $doc[] order by $r.age return $r",
        "let $doc := jn:doc('json-path1','sorted.jn')\n" + "for $r in $doc[] order by $r.age descending return $r",
        "let $doc := jn:doc('json-path1','sorted.jn')\n" + "for $r in $doc[] where $r.active order by $r.age return $r",
        "subsequence(let $doc := jn:doc('json-path1','sorted.jn')\n"
            + "for $r in $doc[] order by $r.age descending return $r, 1, 3)",
        "let $doc := jn:doc('json-path1','sorted.jn')\n"
            + "for $r in $doc[] where $r.age gt 8 order by $r.age descending return $r"};
    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup("json-path1");
      final JsonResourceSession session = collection.getDatabase().beginResourceSession("sorted.jn");
      final String[] generic = new String[queries.length];
      for (int i = 0; i < queries.length; i++) {
        generic[i] = evaluateQuery(chain, ctx, queries[i]);
      }
      final SirixVectorizedExecutor executor =
          new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 2);
      SequentialPipelineStrategy.setVectorizedExecutor(executor);
      try {
        final long servedBefore = SirixVectorizedExecutor.sortedScanServedCount();
        for (int i = 0; i < queries.length; i++) {
          Assertions.assertEquals(generic[i], evaluateQuery(chain, ctx, queries[i]), "sorted-scan parity, query " + i);
        }
        Assertions.assertEquals(queries.length, SirixVectorizedExecutor.sortedScanServedCount() - servedBefore,
            "every sorted-scan query must be SERVED from the projection");
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        executor.close();
      }
    }
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

    try (
        final BasicJsonDBStore store =
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

    try (
        final BasicJsonDBStore store =
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

    try (
        final BasicJsonDBStore store =
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

    try (
        final BasicJsonDBStore jsonStore =
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

    try (
        final BasicJsonDBStore store =
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


  @Test
  public void sortedScanMultiLeafStabilityMatchesGenericPipeline() throws IOException {
    // 3000 rows = 3 leaves with duplicate sort values STRADDLING leaf boundaries —
    // cross-leaf document-order stability differentially proven, parallel collect engaged.
    final StringBuilder big = new StringBuilder(200_000).append('[');
    for (int i = 0; i < 3000; i++) {
      if (i > 0)
        big.append(',');
      big.append("{\"age\":")
         .append(i % 7)
         .append(",\"active\":")
         .append((i & 1) == 0)
         .append(",\"dept\":\"D")
         .append(i % 3)
         .append("\"}");
    }
    big.append(']');
    query("jn:store('json-path1','sorted2.jn','" + big + "')");
    query("""
          let $doc := jn:doc('json-path1','sorted2.jn')
          let $stats := jn:create-projection-index($doc, '/[]',
              ('/[]/age', '/[]/active', '/[]/dept'),
              ('long', 'boolean', 'string'))
          return {"revision": sdb:commit($doc)}
        """);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    final String bigQuery = "let $doc := jn:doc('json-path1','sorted2.jn')\n"
        + "for $r in $doc[] where $r.active order by $r.age return $r";
    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup("json-path1");
      final JsonResourceSession session = collection.getDatabase().beginResourceSession("sorted2.jn");
      final String generic = evaluateQuery(chain, ctx, bigQuery);
      final SirixVectorizedExecutor executor =
          new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 4);
      SequentialPipelineStrategy.setVectorizedExecutor(executor);
      try {
        final long servedBefore = SirixVectorizedExecutor.sortedScanServedCount();
        Assertions.assertEquals(generic, evaluateQuery(chain, ctx, bigQuery),
            "cross-leaf duplicate-value stability must match the generic pipeline");
        Assertions.assertEquals(1L, SirixVectorizedExecutor.sortedScanServedCount() - servedBefore,
            "the multi-leaf sorted scan must be SERVED");
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        executor.close();
      }
    }
  }

  @Test
  public void stringSortedScansServeAndMatchTheGenericPipeline() throws IOException {
    // String ORDER BY keys (gap 1b widened to STRING_DICT) + `return $r.field` (gap 1c).
    // The corpus plants the collation adversary pair U+FF01 (3-byte UTF-8, high UTF-16
    // unit) vs U+10400 (4-byte UTF-8, surrogate pair): raw unsigned UTF-8 order and the
    // interpreter's UTF-16 order DISAGREE on it, so a comparator missing the decoded
    // fallback fails the differential — descending puts the pair at the FRONT of the
    // window. Duplicate names prove document-order stability; the "" phrase exercises
    // the empty string as an order value and as an NE literal.
    query("""
          jn:store('json-path1','strsort.jn','[
            {"name": "delta",   "age": 1, "phrase": "x"},
            {"name": "alpha",   "age": 2, "phrase": ""},
            {"name": "charlie", "age": 3, "phrase": "y"},
            {"name": "alpha",   "age": 2, "phrase": "z"},
            {"name": "！",  "age": 5, "phrase": "w"},
            {"name": "𐐀", "age": 2, "phrase": "v"},
            {"name": "bravo",   "age": 7, "phrase": "q"}
          ]')
        """);
    query("""
          let $doc := jn:doc('json-path1','strsort.jn')
          let $stats := jn:create-projection-index($doc, '/[]',
              ('/[]/name', '/[]/age', '/[]/phrase'),
              ('string', 'long', 'string'))
          return {"revision": sdb:commit($doc)}
        """);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    final String src = "let $doc := jn:doc('json-path1','strsort.jn')\n";
    final String[] queries = {
        // full ascending window over one string key
        "subsequence(" + src + "for $r in $doc[] order by $r.name return $r, 1, 7)",
        // DESCENDING: the collation adversaries are the top-2 — untested by ClickBench
        "subsequence(" + src + "for $r in $doc[] order by $r.name descending return $r, 1, 3)",
        // fn:contains predicate + string key (the Q23 shape; the predsSliceable widening)
        "subsequence(" + src + "for $r in $doc[] where contains($r.name, \"a\") order by $r.name return $r, 1, 5)",
        // NE '' predicate + return-field projection (the Q24 shape)
        "subsequence(" + src + "for $r in $doc[] where $r.phrase != \"\" order by $r.name return $r.phrase, 1, 6)",
        // two string keys; the alpha tie resolves on phrase "" vs "z" (the Q26 shape)
        "subsequence(" + src + "for $r in $doc[] order by $r.name, $r.phrase return $r, 1, 7)",
        // numeric first key, string second key: the age-2 plateau orders by name
        "subsequence(" + src + "for $r in $doc[] order by $r.age, $r.name return $r, 1, 5)",
        // return-field equal to the order key (the Q25 shape)
        "subsequence(" + src + "for $r in $doc[] order by $r.name return $r.name, 1, 4)"};
    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup("json-path1");
      final JsonResourceSession session = collection.getDatabase().beginResourceSession("strsort.jn");
      final String[] generic = new String[queries.length];
      for (int i = 0; i < queries.length; i++) {
        generic[i] = evaluateQuery(chain, ctx, queries[i]);
      }
      final SirixVectorizedExecutor executor =
          new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 2);
      SequentialPipelineStrategy.setVectorizedExecutor(executor);
      try {
        final long servedBefore = SirixVectorizedExecutor.sortedScanServedCount();
        for (int i = 0; i < queries.length; i++) {
          Assertions.assertEquals(generic[i], evaluateQuery(chain, ctx, queries[i]),
              "string sorted-scan parity, query " + i);
        }
        Assertions.assertEquals(queries.length, SirixVectorizedExecutor.sortedScanServedCount() - servedBefore,
            "every string sorted-scan query must be SERVED from the projection");
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        executor.close();
      }
    }
  }

  @Test
  public void overflowingSumServesThroughTheWideAccumulator() throws IOException {
    // 64-bit-id-scale values: the exact long sum overflows after two rows, where the
    // interpreter promotes to big-integer arithmetic. The 128-bit-sum fallback must serve
    // the identical digits for sum and avg (and untouched min/max/count) instead of
    // declining to the row path — the cold-regime whale this exists for.
    query("""
          jn:store('json-path1','wide.jn','[
            {"uid": 4611686018427387904},
            {"uid": 4611686018427387903},
            {"uid": 4611686018427387902},
            {"uid": 1},
            {"uid": -4611686018427387900}
          ]')
        """);
    query("""
          let $doc := jn:doc('json-path1','wide.jn')
          let $stats := jn:create-projection-index($doc, '/[]',
              ('/[]/uid'),
              ('long'))
          return {"revision": sdb:commit($doc)}
        """);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    final String[] queries = {"avg(let $doc := jn:doc('json-path1','wide.jn') for $r in $doc[] return $r.uid)",
        "sum(let $doc := jn:doc('json-path1','wide.jn') for $r in $doc[] return $r.uid)",
        "min(let $doc := jn:doc('json-path1','wide.jn') for $r in $doc[] return $r.uid)",
        "max(let $doc := jn:doc('json-path1','wide.jn') for $r in $doc[] return $r.uid)"};
    // HAND-COMPUTED oracles, not a "generic" leg: aggregate claims serve through the
    // AUTO-WIRED executor even before this test installs its own, so a generic run would
    // take the same projection route and agree with any bug vacuously (the trap
    // auto-wired-vectorized-executor already documented). sum = 2*2^62 + 2; avg divides
    // exactly (ends in 0/5), so the interpreter emits the bare integer.
    final String[] expected =
        {"1844674407370955162", "9223372036854775810", "-4611686018427387900", "4611686018427387904"};
    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup("json-path1");
      final JsonResourceSession session = collection.getDatabase().beginResourceSession("wide.jn");
      final SirixVectorizedExecutor executor =
          new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 2);
      SequentialPipelineStrategy.setVectorizedExecutor(executor);
      try {
        final long servedBefore = SirixVectorizedExecutor.projectionAggregatesServed();
        for (int i = 0; i < queries.length; i++) {
          Assertions.assertEquals(expected[i], evaluateQuery(chain, ctx, queries[i]).trim(),
              "wide-accumulator exact value, query " + i);
        }
        Assertions.assertTrue(SirixVectorizedExecutor.projectionAggregatesServed() > servedBefore,
            "the overflowing aggregate must SERVE through the wide accumulator, not fall back");
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        executor.close();
      }
    }
  }

  @Test
  public void sortedScanReturnFieldGuardsTheSubsequenceWindow() throws IOException {
    // fn:subsequence counts ITEMS, not rows: a top-K winner whose return field is
    // MISSING yields empty and shifts the window into rows the scan never fetched.
    // The expr must fail LOUD into the generic fallback there — a silent skip returns
    // row K+1's value as item K. Unbounded scans may skip empties (exact semantics).
    query("""
          jn:store('json-path1','strsort4.jn','[
            {"age": 2, "name": "b", "phrase": "p"},
            {"age": 1, "name": "a"},
            {"age": 3, "name": "c", "phrase": "r"}
          ]')
        """);
    query("""
          let $doc := jn:doc('json-path1','strsort4.jn')
          let $stats := jn:create-projection-index($doc, '/[]',
              ('/[]/name', '/[]/age', '/[]/phrase'),
              ('string', 'long', 'string'))
          return {"revision": sdb:commit($doc)}
        """);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    final String src = "let $doc := jn:doc('json-path1','strsort4.jn')\n";
    final String windowHasMissing = "subsequence(" + src + "for $r in $doc[] order by $r.name return $r.phrase, 1, 2)";
    final String unboundedSkips = src + "for $r in $doc[] order by $r.age return $r.phrase";
    final String windowClean =
        "subsequence(" + src + "for $r in $doc[] order by $r.name descending return $r.phrase, 1, 2)";
    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup("json-path1");
      final JsonResourceSession session = collection.getDatabase().beginResourceSession("strsort4.jn");
      final String genericMissing = evaluateQuery(chain, ctx, windowHasMissing);
      final String genericUnbounded = evaluateQuery(chain, ctx, unboundedSkips);
      final String genericClean = evaluateQuery(chain, ctx, windowClean);
      final SirixVectorizedExecutor executor =
          new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 2);
      SequentialPipelineStrategy.setVectorizedExecutor(executor);
      try {
        long servedBefore = SirixVectorizedExecutor.sortedScanServedCount();
        final long failedBefore = SirixVectorizedExecutor.sortedScanFailedCount();
        Assertions.assertEquals(genericMissing, evaluateQuery(chain, ctx, windowHasMissing),
            "missing-field window must still answer correctly (via fallback)");
        Assertions.assertEquals(0L, SirixVectorizedExecutor.sortedScanServedCount() - servedBefore,
            "a window containing a missing return field must NOT serve");
        Assertions.assertEquals(1L, SirixVectorizedExecutor.sortedScanFailedCount() - failedBefore,
            "the miss must take the loud-fail path, not a silent decline");
        servedBefore = SirixVectorizedExecutor.sortedScanServedCount();
        Assertions.assertEquals(genericUnbounded, evaluateQuery(chain, ctx, unboundedSkips),
            "unbounded return-field scan must skip empty derefs exactly");
        Assertions.assertEquals(1L, SirixVectorizedExecutor.sortedScanServedCount() - servedBefore,
            "the unbounded return-field scan must be SERVED");
        servedBefore = SirixVectorizedExecutor.sortedScanServedCount();
        Assertions.assertEquals(genericClean, evaluateQuery(chain, ctx, windowClean),
            "a clean window over the same corpus must serve");
        Assertions.assertEquals(1L, SirixVectorizedExecutor.sortedScanServedCount() - servedBefore,
            "the guard must fire only when the window actually contains a miss");
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        executor.close();
      }
    }
  }

  @Test
  public void anOverBudgetSortedScanReEntersTheWholeLeafArmInsteadOfCountingAsCorruption() throws IOException {
    // The sorted scan performs an unmasked whole-column fill of its ORDER columns, so an
    // over-budget column reaches the store's decline door. That decline used to escape to the
    // route's generic RuntimeException handler, which ticks SORTED_SCAN_FAILED — a defect signal —
    // and dropped the query to the generic navigational pipeline, even though the route has a
    // whole-leaf byte-scan arm right beside the sliced one.
    final StringBuilder corpus = new StringBuilder(4096).append('[');
    for (int i = 0; i < 64; i++) {
      if (i > 0) {
        corpus.append(',');
      }
      corpus.append("{\"age\":").append((i * 37) % 101).append(",\"tag\":\"t").append(i % 5).append("\"}");
    }
    query("jn:store('json-path1','budgetsorted.jn','" + corpus.append(']') + "')");
    query("""
          let $doc := jn:doc('json-path1','budgetsorted.jn')
          let $stats := jn:create-projection-index($doc, '/[]',
              ('/[]/age', '/[]/tag'),
              ('long', 'string'))
          return {"revision": sdb:commit($doc)}
        """);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    final String sortedQuery =
        "let $doc := jn:doc('json-path1','budgetsorted.jn')\n" + "for $r in $doc[] order by $r.age return $r";
    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup("json-path1");
      final JsonResourceSession session = collection.getDatabase().beginResourceSession("budgetsorted.jn");
      final int revision = session.getMostRecentRevisionNumber();
      final long ageBytes;
      try (final var rtx = session.beginNodeReadOnlyTrx(revision)) {
        final ProjectionIndexRegistry.Handle handle =
            session.getRtxIndexController(revision)
                   .openProjectionIndex(rtx.getStorageEngineReader(), new String[] {"[]"}, new String[] {"age", "tag"});
        Assertions.assertNotNull(handle, "the projection must be servable");
        final ProjectionColumnStore columnStore = handle.columnStoreOrNull();
        Assertions.assertNotNull(columnStore);
        ageBytes = columnStore.projectedColumnFillBytes(handle.columnOf("age"));
      }
      final SirixCompileChain genericChain = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store);
      final String generic = evaluateQuery(genericChain, ctx, sortedQuery);
      final SirixVectorizedExecutor executor = new SirixVectorizedExecutor(session, revision, 2);
      SequentialPipelineStrategy.setVectorizedExecutor(executor);
      // Just under the ORDER column's own projection: the sliced arm cannot fill it, the whole-leaf
      // arm can.
      final long priorBudget = ProjectionColumnStore.setColumnFillBudgetBytesForTesting(ageBytes - 1);
      try {
        final long failedBefore = SirixVectorizedExecutor.sortedScanFailedCount();
        final long declinedBefore = SirixVectorizedExecutor.sortedScanDeclinedCount();
        final long servedBefore = SirixVectorizedExecutor.sortedScanServedCount();
        Assertions.assertEquals(generic, evaluateQuery(chain, ctx, sortedQuery),
            "the answer must not change with the fill budget");
        Assertions.assertEquals(0L, SirixVectorizedExecutor.sortedScanFailedCount() - failedBefore,
            "a priced fill refusal is a routing decision, not a corruption signal");
        Assertions.assertEquals(1L, SirixVectorizedExecutor.sortedScanServedCount() - servedBefore,
            "the sorted scan must still be SERVED, by the whole-leaf arm it already has");
        Assertions.assertEquals(0L, SirixVectorizedExecutor.sortedScanDeclinedCount() - declinedBefore,
            "a decline that re-routes successfully must not count as a route decline");
      } finally {
        ProjectionColumnStore.setColumnFillBudgetBytesForTesting(priorBudget);
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        executor.close();
      }
    }
  }

  @Test
  public void anOverBudgetPredicateColumnServesWindowedInsteadOfCountingAsCorruption() throws IOException {
    // A CONTAINS literal cannot be bloom-pruned (fingerprints hash whole values), so the predicate
    // column takes the UNMASKED door and the store prices it against the fill budget. That decline
    // used to land in the group arm's generic RuntimeException handler, whose own comment says an
    // exception there "means a defect or corruption": every such query ticked GROUP_AGG_FAILED. A
    // priced refusal is a routing decision, not a defect, and must be distinguishable from one.
    final StringBuilder corpus = new StringBuilder(4096).append('[');
    for (int i = 0; i < 64; i++) {
      if (i > 0) {
        corpus.append(',');
      }
      // A SHORT group key beside a LONG predicate column, so one column's fill fits a budget the
      // other's does not — which is the whole point: the arm is selected, then the door refuses.
      corpus.append("{\"dept\":\"d")
            .append(i % 4)
            .append("\",\"age\":")
            .append(20 + i)
            .append(",\"city\":\"")
            .append("metropolis-")
            .append("qwertyuiop".repeat(12))
            .append(i)
            .append("\"}");
    }
    query("jn:store('json-path1','budgetcontains.jn','" + corpus.append(']') + "')");
    query("""
          let $doc := jn:doc('json-path1','budgetcontains.jn')
          let $stats := jn:create-projection-index($doc, '/[]',
              ('/[]/dept', '/[]/age', '/[]/city'),
              ('string', 'long', 'string'))
          return {"revision": sdb:commit($doc)}
        """);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    final String containsGroupBy = """
          for $r in jn:doc('json-path1','budgetcontains.jn')[]
          where contains($r.city, "metropolis")
          let $d := $r.dept
          group by $d
          let $c := count($r)
          order by $c descending
          return {"d": $d, "c": $c, "total": sum($r.age)}
        """;
    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup("json-path1");
      final JsonResourceSession session = collection.getDatabase().beginResourceSession("budgetcontains.jn");
      final int revision = session.getMostRecentRevisionNumber();
      final long deptBytes;
      final long cityBytes;
      try (final var rtx = session.beginNodeReadOnlyTrx(revision)) {
        final ProjectionIndexRegistry.Handle handle =
            session.getRtxIndexController(revision)
                   .openProjectionIndex(rtx.getStorageEngineReader(), new String[] {"[]"},
                       new String[] {"dept", "age", "city"});
        Assertions.assertNotNull(handle, "the projection must be servable");
        final ProjectionColumnStore columnStore = handle.columnStoreOrNull();
        Assertions.assertNotNull(columnStore, "the handle must be column-lazy for this test to price columns");
        deptBytes = columnStore.projectedColumnFillBytes(handle.columnOf("dept"));
        cityBytes = columnStore.projectedColumnFillBytes(handle.columnOf("city"));
      }
      Assertions.assertTrue(cityBytes > deptBytes,
          "the predicate column must project larger than the group key: " + cityBytes + " vs " + deptBytes);

      final SirixCompileChain genericChain = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store);
      final String generic = evaluateQuery(genericChain, ctx, containsGroupBy);
      final SirixVectorizedExecutor executor = new SirixVectorizedExecutor(session, revision, 2);
      SequentialPipelineStrategy.setVectorizedExecutor(executor);
      // Between the two: the group key's fill is affordable so the sliced arm IS selected, and the
      // predicate column's fill is not so the store refuses it once the arm is under way.
      final long priorBudget = ProjectionColumnStore.setColumnFillBudgetBytesForTesting(cityBytes - 1);
      try {
        Assertions.assertTrue(deptBytes <= cityBytes - 1, "the group key must stay within the chosen budget");
        final long failedBefore = SirixVectorizedExecutor.groupAggFailedCount();
        final long declinedBefore = SirixVectorizedExecutor.groupAggregateDeclinedCount();
        final long servedBefore = SirixVectorizedExecutor.groupAggServedCount();
        final long slicedBefore = SirixVectorizedExecutor.groupAggSlicedServedCount();
        Assertions.assertEquals(generic, evaluateQuery(chain, ctx, containsGroupBy),
            "the answer must not change with the fill budget");
        Assertions.assertEquals(0L, SirixVectorizedExecutor.groupAggFailedCount() - failedBefore,
            "a priced fill refusal is a routing decision, not a corruption signal");
        // The refusal must land on the WHOLE-LEAF arm, which for this handle is the byte-kernel
        // scan over projection leaves — not on the generic navigational pipeline. Returning null
        // here would leave both of these flat and walk every record instead.
        Assertions.assertEquals(1L, SirixVectorizedExecutor.groupAggServedCount() - servedBefore,
            "the query must still be SERVED from the projection");
        // KIND versus FIT: the refused residency feeds the sliced kernels windowed slices instead of
        // sending the query to the whole-leaf arm (the pre-lever contract asserted sliced == 0 here).
        Assertions.assertEquals(1L, SirixVectorizedExecutor.groupAggSlicedServedCount() - slicedBefore,
            "the sliced arm serves the refused column through windowed slices");
        Assertions.assertEquals(0L, SirixVectorizedExecutor.groupAggregateDeclinedCount() - declinedBefore,
            "a refused residency is a routing decision, not a route decline");
      } finally {
        ProjectionColumnStore.setColumnFillBudgetBytesForTesting(priorBudget);
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        executor.close();
      }
    }
  }

  @Test
  public void aBloomPrunedPointLookupStillServesWhenTheWholeColumnFillIsOverBudget() throws IOException {
    // A predicate column is fetched MASKED once the bloom fingerprints prune leaves — a fraction of
    // the column, and no budget door stands in front of it. Gating that route on the WHOLE-column
    // fill projection therefore refuses a point lookup that would have read almost nothing, and the
    // query falls all the way to the generic navigational pipeline. The route gate has to ask about
    // the fill the route will actually perform.
    // LONG grp values: the point of the budget here is that the whole-COLUMN fill is refused while
    // the pruned fetch is not, so the column has to be the expensive thing in the store.
    final StringBuilder predCorpus = new StringBuilder(4096).append('[');
    for (int i = 0; i < 6; i++) {
      if (i > 0) {
        predCorpus.append(',');
      }
      predCorpus.append("{\"uid\":")
                .append(i % 3 == 0
                    ? 7
                    : 3)
                .append(",\"name\":\"n")
                .append(i)
                .append("\",\"grp\":\"")
                .append("groupvalue-".repeat(24))
                .append(i % 3)
                .append("\"}");
    }
    query("jn:store('json-path1','budgetpredscan.jn','" + predCorpus.append(']') + "')");
    query("""
          let $doc := jn:doc('json-path1','budgetpredscan.jn')
          let $stats := jn:create-projection-index($doc, '/[]',
              ('/[]/uid', '/[]/name', '/[]/grp'),
              ('long', 'string', 'string'))
          return {"revision": sdb:commit($doc)}
        """);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    // An ABSENT literal: every leaf's fingerprint misses, so the prune drops them all and the
    // masked fetch reads no segment at all — the cheapest possible serve, and the one the
    // whole-column budget was rejecting.
    final String absentLookup = "let $doc := jn:doc('json-path1','budgetpredscan.jn')\n"
        + "for $r in $doc[] where $r.grp = \"nowhere\" return $r";
    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup("json-path1");
      final JsonResourceSession session = collection.getDatabase().beginResourceSession("budgetpredscan.jn");
      final int revision = session.getMostRecentRevisionNumber();
      // A budget that refuses the WHOLE-COLUMN fill of the predicate column but admits the route's
      // own record-key fill. Budgeting below the keys too would refuse the route for a reason that
      // has nothing to do with the pruning under test — the keys are its result, not its cost.
      final long grpBytes;
      final long keysBytes;
      try (final var rtx = session.beginNodeReadOnlyTrx(revision)) {
        final ProjectionIndexRegistry.Handle handle =
            session.getRtxIndexController(revision)
                   .openProjectionIndex(rtx.getStorageEngineReader(), new String[] {"[]"},
                       new String[] {"uid", "name", "grp"});
        Assertions.assertNotNull(handle, "the projection must be servable");
        final ProjectionColumnStore columnStore = handle.columnStoreOrNull();
        Assertions.assertNotNull(columnStore);
        grpBytes = columnStore.projectedColumnFillBytes(handle.columnOf("grp"));
        keysBytes = columnStore.projectedRecordKeysFillBytes();
      }
      Assertions.assertTrue(keysBytes < grpBytes,
          "the record keys must be cheaper than the whole predicate column: " + keysBytes + " vs " + grpBytes);
      final SirixCompileChain genericChain = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store);
      final String generic = evaluateQuery(genericChain, ctx, absentLookup);
      final SirixVectorizedExecutor executor = new SirixVectorizedExecutor(session, revision, 2);
      SequentialPipelineStrategy.setVectorizedExecutor(executor);
      final long priorBudget = ProjectionColumnStore.setColumnFillBudgetBytesForTesting(grpBytes - 1);
      try {
        final long servedBefore = SirixVectorizedExecutor.predicateScanServedCount();
        Assertions.assertEquals(generic, evaluateQuery(chain, ctx, absentLookup),
            "the answer must not change with the fill budget");
        Assertions.assertEquals(1L, SirixVectorizedExecutor.predicateScanServedCount() - servedBefore,
            "a bloom-pruned point lookup must still be SERVED by the predicate-scan route");
      } finally {
        ProjectionColumnStore.setColumnFillBudgetBytesForTesting(priorBudget);
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        executor.close();
      }
    }
  }

  @Test
  public void predicateScansServeFilteredRowsInDocumentOrder() throws IOException {
    // Stage 7d: `for $r in P where p return $r [.field]` — record keys straight from the
    // predicate mask, materialized per record. Document order is the contract; multi-match
    // predicates prove it, zero-match proves the empty serve, and the unfiltered shape must
    // NOT serve (full-table per-record navigation loses to the row path).
    query("""
          jn:store('json-path1','predscan.jn','[
            {"uid": 7, "name": "a", "grp": "x"},
            {"uid": 3, "name": "b", "grp": "y"},
            {"uid": 7, "name": "c", "grp": "x"},
            {"uid": 9, "name": "d"},
            {"uid": 7, "name": "e", "grp": "z"},
            {"uid": 3, "name": "f", "grp": "x"}
          ]')
        """);
    query("""
          let $doc := jn:doc('json-path1','predscan.jn')
          let $stats := jn:create-projection-index($doc, '/[]',
              ('/[]/uid', '/[]/name', '/[]/grp'),
              ('long', 'string', 'string'))
          return {"revision": sdb:commit($doc)}
        """);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    final String src = "let $doc := jn:doc('json-path1','predscan.jn')\n";
    final String[] queries = {
        // numeric point lookup, multiple matches straddling non-matches
        src + "for $r in $doc[] where $r.uid = 7 return $r",
        // conjunction with a string predicate
        src + "for $r in $doc[] where $r.uid = 7 and $r.grp = \"x\" return $r",
        // contains + field projection
        src + "for $r in $doc[] where contains($r.grp, \"x\") return $r.name",
        // zero matches: the served answer is the empty sequence
        src + "for $r in $doc[] where $r.uid = 999 return $r",
        // subsequence cap: the document-order prefix is exact
        "subsequence(" + src + "for $r in $doc[] where $r.uid = 7 return $r, 1, 2)"};
    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup("json-path1");
      final JsonResourceSession session = collection.getDatabase().beginResourceSession("predscan.jn");
      final String unfiltered = src + "for $r in $doc[] return $r";
      final String[] generic = new String[queries.length];
      for (int i = 0; i < queries.length; i++) {
        generic[i] = evaluateQuery(chain, ctx, queries[i]);
      }
      final String genericAll = evaluateQuery(chain, ctx, unfiltered);
      final SirixVectorizedExecutor executor =
          new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 2);
      SequentialPipelineStrategy.setVectorizedExecutor(executor);
      try {
        long servedBefore = SirixVectorizedExecutor.predicateScanServedCount();
        final long emittedBefore = SirixVectorizedExecutor.predicateValueEmissionsServedCount();
        for (int i = 0; i < queries.length; i++) {
          Assertions.assertEquals(generic[i], evaluateQuery(chain, ctx, queries[i]),
              "predicate-scan parity, query " + i);
        }
        // Query 2 returns ONE projected field, so the value-emission route (stage 7e) claims it
        // before any record is materialized; the other four still materialize records.
        Assertions.assertEquals(1L, SirixVectorizedExecutor.predicateValueEmissionsServedCount() - emittedBefore,
            "the single-field return must be SERVED by value emission");
        Assertions.assertEquals(queries.length - 1, SirixVectorizedExecutor.predicateScanServedCount() - servedBefore,
            "every remaining predicate-scan query must be SERVED from the projection");
        // The unfiltered shape must decline (and still answer identically via the fallback).
        servedBefore = SirixVectorizedExecutor.predicateScanServedCount();
        Assertions.assertEquals(genericAll, evaluateQuery(chain, ctx, unfiltered),
            "unfiltered scan must answer identically via the fallback");
        Assertions.assertEquals(0L, SirixVectorizedExecutor.predicateScanServedCount() - servedBefore,
            "an unfiltered scan must NOT serve through the predicate-scan route");
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        executor.close();
      }
    }
  }

  @Test
  public void predicateValueEmissionServesEveryProjectedKindAtEverySelectivity() throws IOException {
    // Stage 7e: `for $r in P where p return $r.field` emits the FIELD's values straight off the
    // predicate mask — no record key, no record page, no deref. Three things this corpus is
    // built to catch:
    //
    // 1. SELECTIVITY SWEEP — common (~1/3), mid (~1/25), rare (exactly 1) and zero matches. A
    // wrong-answer bug once hid behind a common literal for a whole session; the error scaled
    // with rarity, and rarity is also what makes the zone/fingerprint prune drop leaves, which
    // is where a value column resolved WITHOUT the prune mask would read the wrong leaf.
    // 2. EVERY EMITTABLE KIND — long, string (dict-encoded), boolean and double each construct a
    // different atomic, and the bar is what the record deref produces, not what is convenient.
    // 3. MULTIPLE LEAVES — 3000 rows is three row groups, so leaf order vs row order shows up in
    // the emission order; a single-leaf corpus cannot tell document order from anything.
    final int rows = 3000;
    final StringBuilder corpus = new StringBuilder(rows * 90).append('[');
    for (int i = 0; i < rows; i++) {
      if (i > 0) {
        corpus.append(',');
      }
      // ratio alternates a fractional and an INTEGRAL double value: `3.0` vs `3` is exactly the
      // kind of drift an atomic swap produces while every value stays numerically right. The
      // exponent form is load-bearing: {@code JsonNumber} keeps a plain `3.5` as a BigDecimal and
      // only an exactly-round-tripping exponent literal becomes a Double, which is what the
      // column's pure-source bit — and therefore the Dbl emission arm — requires.
      corpus.append("{\"uid\":")
            .append(i % 3 == 0
                ? 7
                : i % 25 == 4
                    ? 3
                    : 100 + i)
            .append(",\"name\":\"n")
            .append(i % 137)
            .append("\",\"flag\":")
            .append(i % 2 == 0)
            .append(",\"ratio\":")
            .append(i % 7)
            .append(i % 2 == 0
                ? ".0e0"
                : ".5e0")
            .append('}');
    }
    corpus.append(']');
    query("jn:store('json-path1','predemit.jn','" + corpus + "')");
    query("""
          let $doc := jn:doc('json-path1','predemit.jn')
          let $stats := jn:create-projection-index($doc, '/[]',
              ('/[]/uid', '/[]/name', '/[]/flag', '/[]/ratio'),
              ('long', 'string', 'boolean', 'double'))
          return {"revision": sdb:commit($doc)}
        """);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    final String src = "let $doc := jn:doc('json-path1','predemit.jn')\n";
    // Common, mid, rare (uid 1100 is row 1000 and nothing else), zero, and two string predicates.
    final String[] wheres =
        {"$r.uid = 7", "$r.uid = 3", "$r.uid = 1100", "$r.uid = -1", "$r.name = \"n5\"", "contains($r.name, \"n12\")"};
    final List<String> queries = new ArrayList<>();
    for (final String where : wheres) {
      for (final String field : new String[] {"uid", "name", "flag", "ratio"}) {
        queries.add(src + "for $r in $doc[] where " + where + " return $r." + field);
      }
      // Sole-consumer cap: the document-order prefix must be the same prefix.
      queries.add("subsequence(" + src + "for $r in $doc[] where " + where + " return $r.name, 1, 3)");
    }
    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup("json-path1");
      final JsonResourceSession session = collection.getDatabase().beginResourceSession("predemit.jn");
      final String[] generic = new String[queries.size()];
      for (int i = 0; i < queries.size(); i++) {
        generic[i] = evaluateQuery(chain, ctx, queries.get(i));
      }
      final SirixVectorizedExecutor executor =
          new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 2);
      SequentialPipelineStrategy.setVectorizedExecutor(executor);
      try {
        final long failedBefore = SirixVectorizedExecutor.predicateValueEmissionsFailedCount();
        final List<String> declined = new ArrayList<>();
        for (int i = 0; i < queries.size(); i++) {
          final long before = SirixVectorizedExecutor.predicateValueEmissionsServedCount();
          Assertions.assertEquals(generic[i], evaluateQuery(chain, ctx, queries.get(i)),
              "value-emission parity, query " + i + ": " + queries.get(i));
          if (SirixVectorizedExecutor.predicateValueEmissionsServedCount() == before) {
            declined.add(queries.get(i));
          }
        }
        // Equality alone passes vacuously when both legs run the same pipeline, so every query
        // has to be shown to have taken the route under test.
        Assertions.assertEquals(List.of(), declined, "every single-field predicate scan must be SERVED");
        Assertions.assertEquals(0L, SirixVectorizedExecutor.predicateValueEmissionsFailedCount() - failedBefore,
            "no value-emission attempt may fail with an exception");
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        executor.close();
      }
    }
  }

  @Test
  public void predicateValueEmissionSkipsAbsentCellsAndDeclinesUnderACap() throws IOException {
    // An emitted field the matching row does NOT have: the interpreter's deref is empty, so the
    // row contributes nothing. Unbounded, emission reproduces that by skipping the row. Under a
    // fn:subsequence cap it cannot — the window counts ITEMS, and a skipped row shifts it past
    // the rows the mask fetched — so the route declines and the record path (which throws on the
    // same shape) hands the query to the interpreter.
    query("""
          jn:store('json-path1','predemitgap.jn','[
            {"uid": 7, "name": "a"},
            {"uid": 7},
            {"uid": 7, "name": "c"},
            {"uid": 3, "name": "d"},
            {"uid": 7, "name": "e"}
          ]')
        """);
    query("""
          let $doc := jn:doc('json-path1','predemitgap.jn')
          let $stats := jn:create-projection-index($doc, '/[]',
              ('/[]/uid', '/[]/name'),
              ('long', 'string'))
          return {"revision": sdb:commit($doc)}
        """);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    final String src = "let $doc := jn:doc('json-path1','predemitgap.jn')\n";
    final String unbounded = src + "for $r in $doc[] where $r.uid = 7 return $r.name";
    final String capped = "subsequence(" + src + "for $r in $doc[] where $r.uid = 7 return $r.name, 1, 2)";
    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup("json-path1");
      final JsonResourceSession session = collection.getDatabase().beginResourceSession("predemitgap.jn");
      final String genericUnbounded = evaluateQuery(chain, ctx, unbounded);
      final String genericCapped = evaluateQuery(chain, ctx, capped);
      final SirixVectorizedExecutor executor =
          new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 2);
      SequentialPipelineStrategy.setVectorizedExecutor(executor);
      try {
        long emittedBefore = SirixVectorizedExecutor.predicateValueEmissionsServedCount();
        Assertions.assertEquals(genericUnbounded, evaluateQuery(chain, ctx, unbounded),
            "an absent cell must contribute nothing, exactly as the empty deref does");
        Assertions.assertEquals(1L, SirixVectorizedExecutor.predicateValueEmissionsServedCount() - emittedBefore,
            "the unbounded shape must still SERVE across the hole");
        emittedBefore = SirixVectorizedExecutor.predicateValueEmissionsServedCount();
        Assertions.assertEquals(genericCapped, evaluateQuery(chain, ctx, capped),
            "the capped shape must answer identically through the fallback");
        Assertions.assertEquals(0L, SirixVectorizedExecutor.predicateValueEmissionsServedCount() - emittedBefore,
            "a cap over a hole must DECLINE — the window would be off by the skipped row");
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        executor.close();
      }
    }
  }

  @Test
  public void predicateValueEmissionDeclinesOnNullCellsAndUnprojectedFields() throws IOException {
    // Two declines that must leave the RECORD-materializing predicate scan in charge:
    //
    // 1. A JSON null is present-but-unrepresentable, which clears the column's sparse-clean bit.
    // Without that gate an absent presence bit could mean EITHER "no field" (emit nothing) or
    // "null" (emit null), and skipping would silently drop the nulls.
    // 2. A return field outside the projection has no column to read at all.
    query("""
          jn:store('json-path1','predemitnull.jn','[
            {"uid": 7, "name": "a", "extra": "p"},
            {"uid": 7, "name": null, "extra": "q"},
            {"uid": 7, "name": "c", "extra": "r"},
            {"uid": 3, "name": "d", "extra": "s"}
          ]')
        """);
    query("""
          let $doc := jn:doc('json-path1','predemitnull.jn')
          let $stats := jn:create-projection-index($doc, '/[]',
              ('/[]/uid', '/[]/name'),
              ('long', 'string'))
          return {"revision": sdb:commit($doc)}
        """);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    final String src = "let $doc := jn:doc('json-path1','predemitnull.jn')\n";
    final String nullBearing = src + "for $r in $doc[] where $r.uid = 7 return $r.name";
    final String unprojected = src + "for $r in $doc[] where $r.uid = 7 return $r.extra";
    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup("json-path1");
      final JsonResourceSession session = collection.getDatabase().beginResourceSession("predemitnull.jn");
      final String genericNull = evaluateQuery(chain, ctx, nullBearing);
      final String genericExtra = evaluateQuery(chain, ctx, unprojected);
      final SirixVectorizedExecutor executor =
          new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 2);
      SequentialPipelineStrategy.setVectorizedExecutor(executor);
      try {
        final long emittedBefore = SirixVectorizedExecutor.predicateValueEmissionsServedCount();
        final long scannedBefore = SirixVectorizedExecutor.predicateScanServedCount();
        Assertions.assertEquals(genericNull, evaluateQuery(chain, ctx, nullBearing),
            "a null-bearing column must answer identically through the record route");
        Assertions.assertEquals(genericExtra, evaluateQuery(chain, ctx, unprojected),
            "an unprojected return field must answer identically through the record route");
        Assertions.assertEquals(0L, SirixVectorizedExecutor.predicateValueEmissionsServedCount() - emittedBefore,
            "neither shape may be served by value emission");
        Assertions.assertEquals(2L, SirixVectorizedExecutor.predicateScanServedCount() - scannedBefore,
            "both must fall through to the record-materializing predicate scan, not to the row path");
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        executor.close();
      }
    }
  }

  @Test
  public void stringSortedScanDeclinesOnMissingOrderCells() throws IOException {
    // A matched row without the string order key can only be placed by the interpreter
    // (empty-least semantics) — the scan must return null, never invent a position.
    query("""
          jn:store('json-path1','strsort3.jn','[
            {"name": "x", "v": 1},
            {"v": 2}
          ]')
        """);
    query("""
          let $doc := jn:doc('json-path1','strsort3.jn')
          let $stats := jn:create-projection-index($doc, '/[]',
              ('/[]/name', '/[]/v'),
              ('string', 'long'))
          return {"revision": sdb:commit($doc)}
        """);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    final String q = "subsequence(let $doc := jn:doc('json-path1','strsort3.jn')\n"
        + "for $r in $doc[] order by $r.name return $r, 1, 2)";
    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup("json-path1");
      final JsonResourceSession session = collection.getDatabase().beginResourceSession("strsort3.jn");
      final String generic = evaluateQuery(chain, ctx, q);
      final SirixVectorizedExecutor executor =
          new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 2);
      SequentialPipelineStrategy.setVectorizedExecutor(executor);
      try {
        final long servedBefore = SirixVectorizedExecutor.sortedScanServedCount();
        Assertions.assertEquals(generic, evaluateQuery(chain, ctx, q),
            "missing string order cell must fall back with an identical answer");
        Assertions.assertEquals(0L, SirixVectorizedExecutor.sortedScanServedCount() - servedBefore,
            "a corpus with a missing string order cell must DECLINE");
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        executor.close();
      }
    }
  }

  @Test
  public void multiLeafStringSortedScansMatchTheGenericPipeline() throws IOException {
    // 2500 rows = 3 leaves: the same string interns to DIFFERENT dict ids per leaf, so
    // packed (leaf,dictId) heap keys for equal values differ as longs and MUST compare
    // equal through entry bytes, then fall to the document-order rank — cross-leaf
    // stability differentially proven with the parallel collector engaged.
    final StringBuilder big = new StringBuilder(120_000).append('[');
    for (int i = 0; i < 2500; i++) {
      if (i > 0) {
        big.append(',');
      }
      big.append("{\"name\":\"N").append(i % 5).append("\",\"val\":").append(i).append('}');
    }
    big.append(']');
    query("jn:store('json-path1','strsort2.jn','" + big + "')");
    query("""
          let $doc := jn:doc('json-path1','strsort2.jn')
          let $stats := jn:create-projection-index($doc, '/[]',
              ('/[]/name', '/[]/val'),
              ('string', 'long'))
          return {"revision": sdb:commit($doc)}
        """);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    final String src = "let $doc := jn:doc('json-path1','strsort2.jn')\n";
    final String[] queries = {
        // 30 winners, all name "N0", straddling leaves — pure rank-tiebreak territory
        "subsequence(" + src + "for $r in $doc[] order by $r.name return $r, 1, 30)",
        "subsequence(" + src + "for $r in $doc[] order by $r.name descending, $r.val descending return $r, 1, 15)",
        // string NE predicate + two keys through the sliced path
        "subsequence(" + src + "for $r in $doc[] where $r.name != \"N3\" order by $r.name, $r.val return $r, 1, 20)",
        // numeric return field under a string order key
        "subsequence(" + src + "for $r in $doc[] order by $r.name return $r.val, 1, 25)"};
    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup("json-path1");
      final JsonResourceSession session = collection.getDatabase().beginResourceSession("strsort2.jn");
      final String[] generic = new String[queries.length];
      for (int i = 0; i < queries.length; i++) {
        generic[i] = evaluateQuery(chain, ctx, queries[i]);
      }
      final SirixVectorizedExecutor executor =
          new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 4);
      SequentialPipelineStrategy.setVectorizedExecutor(executor);
      try {
        final long servedBefore = SirixVectorizedExecutor.sortedScanServedCount();
        for (int i = 0; i < queries.length; i++) {
          Assertions.assertEquals(generic[i], evaluateQuery(chain, ctx, queries[i]),
              "multi-leaf string sorted-scan parity, query " + i);
        }
        Assertions.assertEquals(queries.length, SirixVectorizedExecutor.sortedScanServedCount() - servedBefore,
            "every multi-leaf string sorted-scan query must be SERVED");
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        executor.close();
      }
    }
  }

  @Test
  public void predicatedTopKSweepsSelectivityAndSkipsLeaves() throws IOException {
    // The ClickBench `SELECT * WHERE <p> ORDER BY <col> LIMIT k` family. Four things have to hold
    // at once, and each has burned this route before:
    //
    // 1. SELECTIVITY SWEEP — the same shape at four match rates. A wrong-answer bug once hid
    // behind a common literal for a whole session; the error scaled with RARITY, because a rare
    // predicate is what lets the bounded heap stop early.
    // 2. LEAF ORDER != DOCUMENT ORDER — the four 1024-row leaves get key bands 3000/1005/5010/2010,
    // so the best leaf is the SECOND one and the plan must reorder. A corpus whose leaf minima
    // already ascend cannot tell best-first from the document-order walk.
    // 3. TIES, INCLUDING ACROSS LEAVES — 100 distinct keys per leaf over 1024 rows (~10 rows each)
    // and deliberately OVERLAPPING bands on a shared step-15 grid, so limits land mid-tie and
    // only the DOCUMENT-order tiebreak separates the winners.
    // 4. THE PRUNE ACTUALLY RAN — a route that quietly stops pruning returns the same answer, so
    // equality alone cannot tell the two apart; the skipped-leaf counter has to move.
    final int rows = 4000;
    final int[] band = {3000, 1005, 5010, 2010};
    final StringBuilder corpus = new StringBuilder(rows * 90).append('[');
    for (int i = 0; i < rows; i++) {
      if (i > 0) {
        corpus.append(',');
      }
      final int ord = band[i / 1024] + i * 977 % 100 * 15;
      final String tag = i % 300 == 7
          ? "few"
          : i % 3 == 0
              ? "hit"
              : "other";
      corpus.append("{\"ts\":\"T")
            .append(String.format("%05d", ord))
            .append("\",\"n\":")
            .append(ord)
            .append(",\"tag\":\"")
            .append(tag)
            .append("\",\"url\":\"http://")
            .append(i % 7 == 0
                ? "google"
                : "example")
            .append(".com/")
            .append(i)
            .append("\"}");
    }
    corpus.append(']');
    query("jn:store('json-path1','topksweep.jn','" + corpus + "')");
    query("""
          let $doc := jn:doc('json-path1','topksweep.jn')
          let $stats := jn:create-projection-index($doc, '/[]',
              ('/[]/ts', '/[]/n', '/[]/tag', '/[]/url'),
              ('string', 'long', 'string', 'string'))
          return {"revision": sdb:commit($doc)}
        """);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    final String src = "let $doc := jn:doc('json-path1','topksweep.jn')\n";
    // where-clauses at four match rates: all 4000 rows, 1333, ~14, and none.
    final String[] wheres = {"$r.tag != \"zzz\"", "$r.tag = \"hit\"", "$r.tag = \"few\"", "$r.tag = \"zzz\"",
        "contains($r.url, \"google\")"};
    final List<String> queries = new ArrayList<>();
    for (final String where : wheres) {
      for (final int limit : new int[] {1, 25}) {
        // String order key, both directions; numeric order key; and a two-key sort whose second
        // key only ever decides rows tied on the first.
        queries.add(
            "subsequence(" + src + "for $r in $doc[] where " + where + " order by $r.ts return $r, 1, " + limit + ")");
        queries.add("subsequence(" + src + "for $r in $doc[] where " + where
            + " order by $r.ts descending return $r, 1, " + limit + ")");
        queries.add("subsequence(" + src + "for $r in $doc[] where " + where + " order by $r.n return $r.url, 1, "
            + limit + ")");
        queries.add("subsequence(" + src + "for $r in $doc[] where " + where + " order by $r.n descending, $r.url"
            + " return $r, 1, " + limit + ")");
      }
    }
    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup("json-path1");
      final JsonResourceSession session = collection.getDatabase().beginResourceSession("topksweep.jn");
      final String[] generic = new String[queries.size()];
      for (int i = 0; i < queries.size(); i++) {
        generic[i] = evaluateQuery(chain, ctx, queries.get(i));
      }
      final SirixVectorizedExecutor executor =
          new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 4);
      SequentialPipelineStrategy.setVectorizedExecutor(executor);
      try {
        final long servedBefore = SirixVectorizedExecutor.sortedScanServedCount();
        final long skippedBefore = ProjectionColumnScan.topKLeavesSkippedCount();
        for (int i = 0; i < queries.size(); i++) {
          Assertions.assertEquals(generic[i], evaluateQuery(chain, ctx, queries.get(i)),
              "predicated top-K parity, query " + i + ": " + queries.get(i));
        }
        Assertions.assertEquals(queries.size(), SirixVectorizedExecutor.sortedScanServedCount() - servedBefore,
            "every predicated top-K query must be SERVED — equality over two fallbacks proves nothing");
        Assertions.assertTrue(ProjectionColumnScan.topKLeavesSkippedCount() > skippedBefore,
            "the bounded scan must SKIP leaves — otherwise the sweep only proved the unpruned walk");
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        executor.close();
      }
    }
  }

  @Test
  public void predicatedTopKDeclinesWhenTheOrderKeyIsMissingInALateLeaf() throws IOException {
    // The hazard the best-first stop introduces: a matching row whose order key is ABSENT sorts
    // FIRST in ascending order (empty least), and it sits in the LAST leaf — precisely the leaf an
    // early stop would never open. Serving must decline outright and let the interpreter place it.
    final int rows = 3000;
    final StringBuilder corpus = new StringBuilder(rows * 40).append('[');
    for (int i = 0; i < rows; i++) {
      if (i > 0) {
        corpus.append(',');
      }
      // Row 2900 (last leaf) matches the predicate but carries NO order key.
      if (i == 2900) {
        corpus.append("{\"tag\":\"hit\"}");
      } else {
        corpus.append("{\"ts\":\"T").append(String.format("%04d", i * 977 % 300)).append("\",\"tag\":\"hit\"}");
      }
    }
    corpus.append(']');
    query("jn:store('json-path1','topkgap.jn','" + corpus + "')");
    query("""
          let $doc := jn:doc('json-path1','topkgap.jn')
          let $stats := jn:create-projection-index($doc, '/[]',
              ('/[]/ts', '/[]/tag'),
              ('string', 'string'))
          return {"revision": sdb:commit($doc)}
        """);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    final String q = "subsequence(let $doc := jn:doc('json-path1','topkgap.jn')\n"
        + "for $r in $doc[] where $r.tag = \"hit\" order by $r.ts return $r, 1, 10)";
    // Control over the SAME corpus: `tag` has no absent cell, so this one must serve. Without it a
    // decline assertion also passes when the projection was never built or never reached.
    final String control = "subsequence(let $doc := jn:doc('json-path1','topkgap.jn')\n"
        + "for $r in $doc[] where $r.tag = \"hit\" order by $r.tag return $r, 1, 10)";
    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup("json-path1");
      final JsonResourceSession session = collection.getDatabase().beginResourceSession("topkgap.jn");
      final String generic = evaluateQuery(chain, ctx, q);
      final String genericControl = evaluateQuery(chain, ctx, control);
      final SirixVectorizedExecutor executor =
          new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 4);
      SequentialPipelineStrategy.setVectorizedExecutor(executor);
      try {
        long servedBefore = SirixVectorizedExecutor.sortedScanServedCount();
        Assertions.assertEquals(genericControl, evaluateQuery(chain, ctx, control), "control query parity");
        Assertions.assertEquals(1L, SirixVectorizedExecutor.sortedScanServedCount() - servedBefore,
            "the control must SERVE — otherwise the decline below proves nothing about this route");
        servedBefore = SirixVectorizedExecutor.sortedScanServedCount();
        Assertions.assertEquals(generic, evaluateQuery(chain, ctx, q),
            "a missing order key in a late leaf must not change the answer");
        Assertions.assertEquals(0L, SirixVectorizedExecutor.sortedScanServedCount() - servedBefore,
            "an order column with an absent cell must DECLINE, never be skipped past");
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        executor.close();
      }
    }
  }

  @Test
  public void multiKeySortedScansServeAndMatchTheGenericPipeline() throws IOException {
    // Gap item 1b: N order-by keys with per-key direction. Ties on the primary key
    // exercise the secondary key; exact ties on BOTH keys exercise document-order
    // stability. All differential against the generic pipeline; all must be SERVED.
    query("""
          jn:store('json-path1','mksort.jn','[
            {"age": 30, "bonus": 5, "name": "a"},
            {"age": 30, "bonus": 2, "name": "b"},
            {"age": 25, "bonus": 9, "name": "c"},
            {"age": 30, "bonus": 2, "name": "d"},
            {"age": 25, "bonus": 1, "name": "e"},
            {"age": 40, "bonus": 7, "name": "f"}
          ]')
        """);
    query("""
          let $doc := jn:doc('json-path1','mksort.jn')
          let $stats := jn:create-projection-index($doc, '/[]',
              ('/[]/age', '/[]/bonus', '/[]/name'),
              ('long', 'long', 'string'))
          return {"revision": sdb:commit($doc)}
        """);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    final String[] queries =
        {"let $doc := jn:doc('json-path1','mksort.jn')\n" + "for $r in $doc[] order by $r.age, $r.bonus return $r",
            "let $doc := jn:doc('json-path1','mksort.jn')\n"
                + "for $r in $doc[] order by $r.age descending, $r.bonus return $r",
            "let $doc := jn:doc('json-path1','mksort.jn')\n"
                + "for $r in $doc[] order by $r.age, $r.bonus descending return $r",
            "let $doc := jn:doc('json-path1','mksort.jn')\n"
                + "for $r in $doc[] where $r.age lt 40 order by $r.age descending, $r.bonus descending return $r",
            // An intermediate let (not shadowing $r) must NOT lose serving — the specs and
            // return only touch the loop var (review finding: the pre-1b path served this).
            "let $doc := jn:doc('json-path1','mksort.jn')\n"
                + "for $r in $doc[] where $r.age gt 20 let $x := $r.bonus order by $r.age return $r"};
    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup("json-path1");
      final JsonResourceSession session = collection.getDatabase().beginResourceSession("mksort.jn");
      final String[] generic = new String[queries.length];
      for (int i = 0; i < queries.length; i++) {
        generic[i] = evaluateQuery(chain, ctx, queries[i]);
      }
      final SirixVectorizedExecutor executor =
          new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 2);
      SequentialPipelineStrategy.setVectorizedExecutor(executor);
      try {
        for (int i = 0; i < queries.length; i++) {
          final long servedBefore = SirixVectorizedExecutor.sortedScanServedCount();
          Assertions.assertEquals(generic[i], evaluateQuery(chain, ctx, queries[i]),
              "multi-key sorted-scan parity, query " + i);
          Assertions.assertEquals(1L, SirixVectorizedExecutor.sortedScanServedCount() - servedBefore,
              "multi-key sorted-scan query " + i + " must be SERVED from the projection");
        }
        // Review finding: a let whose bound expression can RAISE must decline — the
        // generic pipeline evaluates lets per row, so serving would replace the
        // interpreter's error with an answer.
        final String errorLetQuery = "let $doc := jn:doc('json-path1','mksort.jn')\n"
            + "for $r in $doc[] let $x := $r.age idiv 0 order by $r.age return $r";
        final long servedBeforeErrLet = SirixVectorizedExecutor.sortedScanServedCount();
        String servedErrLet;
        try {
          servedErrLet = evaluateQuery(chain, ctx, errorLetQuery);
        } catch (final RuntimeException e) {
          servedErrLet = "ERROR:" + e.getClass().getSimpleName();
        }
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        String genericErrLet;
        try {
          genericErrLet = evaluateQuery(chain, ctx, errorLetQuery);
        } catch (final RuntimeException e) {
          genericErrLet = "ERROR:" + e.getClass().getSimpleName();
        } finally {
          SequentialPipelineStrategy.setVectorizedExecutor(executor);
        }
        Assertions.assertEquals(genericErrLet, servedErrLet,
            "an erroring let must reproduce the interpreter outcome, error included");
        Assertions.assertEquals(0L, SirixVectorizedExecutor.sortedScanServedCount() - servedBeforeErrLet,
            "a non-deref let must DECLINE sorted serving");
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        executor.close();
      }
    }
  }

  @Test
  public void lossyLongColumnsDeclineValueServing() throws IOException {
    // Review findings: (a) out-of-long-range integers WRAP through longValue() at
    // extraction — 2^63 stored as Long.MIN_VALUE would serve wrong min/sum/order; (b) a
    // Double-typed but integral-valued cell (6.0E6) types the interpreter's fold in
    // DOUBLE space ("1.1E7" serialization) while a served long fold prints "11000000".
    // Both now poison the value-exactness bit at extraction: value serving must DECLINE
    // and every outcome must match the interpreter byte-for-byte.
    query("""
          jn:store('json-path1','lossy.jn','[
            {"a": 9223372036854775808, "b": 1},
            {"a": 5, "b": 2}
          ]')
        """);
    query("""
          let $doc := jn:doc('json-path1','lossy.jn')
          let $stats := jn:create-projection-index($doc, '/[]',
              ('/[]/a', '/[]/b'), ('long', 'long'))
          return {"revision": sdb:commit($doc)}
        """);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    final String[] queries =
        {"let $doc := jn:doc('json-path1','lossy.jn')\n" + "return min(for $r in $doc[] return $r.a + 0)",
            "let $doc := jn:doc('json-path1','lossy.jn')\n" + "for $r in $doc[] order by $r.a return $r"};
    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup("json-path1");
      final JsonResourceSession session = collection.getDatabase().beginResourceSession("lossy.jn");
      final String[] generic = new String[queries.length];
      for (int i = 0; i < queries.length; i++) {
        generic[i] = evaluateQuery(chain, ctx, queries[i]);
      }
      final SirixVectorizedExecutor executor =
          new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 2);
      SequentialPipelineStrategy.setVectorizedExecutor(executor);
      try {
        final long computedBefore = SirixVectorizedExecutor.computedAggServedCount();
        final long sortedBefore = SirixVectorizedExecutor.sortedScanServedCount();
        for (int i = 0; i < queries.length; i++) {
          Assertions.assertEquals(generic[i], evaluateQuery(chain, ctx, queries[i]),
              "lossy long column parity, query " + i);
        }
        Assertions.assertEquals(0L, SirixVectorizedExecutor.computedAggServedCount() - computedBefore,
            "computed aggregate over a wrapped 2^63 column must DECLINE");
        Assertions.assertEquals(0L, SirixVectorizedExecutor.sortedScanServedCount() - sortedBefore,
            "sorted scan over a wrapped 2^63 column must DECLINE");
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        executor.close();
      }
    }

    // Double-typed integral value: 6.0E6 shreds as Double; the interpreter folds in
    // double space and serializes "1.1E7" — a served long fold would print "11000000".
    query("""
          jn:store('json-path1','dblsrc.jn','[
            {"a": 6.0E6},
            {"a": 5000000}
          ]')
        """);
    query("""
          let $doc := jn:doc('json-path1','dblsrc.jn')
          let $stats := jn:create-projection-index($doc, '/[]',
              ('/[]/a'), ('long'))
          return {"revision": sdb:commit($doc)}
        """);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    final String dblQuery =
        "let $doc := jn:doc('json-path1','dblsrc.jn')\n" + "return sum(for $r in $doc[] return $r.a * 1)";
    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup("json-path1");
      final JsonResourceSession session = collection.getDatabase().beginResourceSession("dblsrc.jn");
      final String generic = evaluateQuery(chain, ctx, dblQuery);
      final SirixVectorizedExecutor executor =
          new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 2);
      SequentialPipelineStrategy.setVectorizedExecutor(executor);
      try {
        final long computedBefore = SirixVectorizedExecutor.computedAggServedCount();
        Assertions.assertEquals(generic, evaluateQuery(chain, ctx, dblQuery),
            "double-typed integral cells must fold in the interpreter's double space");
        Assertions.assertEquals(0L, SirixVectorizedExecutor.computedAggServedCount() - computedBefore,
            "computed aggregate over a double-sourced long column must DECLINE");
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        executor.close();
      }
    }
  }

  @Test
  public void topKSubsequencePushdownServesViaBoundedSelection() throws IOException {
    // Gap item 3: a sole-consumer fn:subsequence with literal bounds caps the sorted
    // pipe at start+len-1 rows — served via bounded heap selection, not a full sort.
    // 2000 rows across multiple leaves with heavy duplicate keys prove the kept set
    // equals the full stable sort's prefix (ties resolved by document order).
    final StringBuilder big = new StringBuilder(120_000).append('[');
    for (int i = 0; i < 2000; i++) {
      if (i > 0)
        big.append(',');
      big.append("{\"age\":")
         .append(i % 7)
         .append(",\"bonus\":")
         .append(i % 5)
         .append(",\"active\":")
         .append((i & 1) == 0)
         .append('}');
    }
    big.append(']');
    query("jn:store('json-path1','topk.jn','" + big + "')");
    query("""
          let $doc := jn:doc('json-path1','topk.jn')
          let $stats := jn:create-projection-index($doc, '/[]',
              ('/[]/age', '/[]/bonus', '/[]/active'),
              ('long', 'long', 'boolean'))
          return {"revision": sdb:commit($doc)}
        """);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    final String[] cappedQueries = {
        "let $doc := jn:doc('json-path1','topk.jn')\n"
            + "return subsequence(for $r in $doc[] where $r.active order by $r.age return $r, 1, 5)",
        // start > 1 and a secondary key: limit = 3+4-1 = 6 kept rows, emit rows 3..6.
        "let $doc := jn:doc('json-path1','topk.jn')\n"
            + "return subsequence(for $r in $doc[] order by $r.age descending, $r.bonus return $r, 3, 4)"};
    // 2-arg subsequence needs the unbounded tail — served, but NO top-K cap may apply.
    final String uncappedQuery = "let $doc := jn:doc('json-path1','topk.jn')\n"
        + "return count(subsequence(for $r in $doc[] where $r.active order by $r.age return $r, 1990))";
    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup("json-path1");
      final JsonResourceSession session = collection.getDatabase().beginResourceSession("topk.jn");
      final String[] generic = new String[cappedQueries.length];
      for (int i = 0; i < cappedQueries.length; i++) {
        generic[i] = evaluateQuery(chain, ctx, cappedQueries[i]);
      }
      final String genericUncapped = evaluateQuery(chain, ctx, uncappedQuery);
      final SirixVectorizedExecutor executor =
          new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 2);
      SequentialPipelineStrategy.setVectorizedExecutor(executor);
      try {
        for (int i = 0; i < cappedQueries.length; i++) {
          final long servedBefore = SirixVectorizedExecutor.sortedScanServedCount();
          final long topKBefore = SirixVectorizedExecutor.sortedTopKAppliedCount();
          Assertions.assertEquals(generic[i], evaluateQuery(chain, ctx, cappedQueries[i]), "top-K parity, query " + i);
          Assertions.assertEquals(1L, SirixVectorizedExecutor.sortedScanServedCount() - servedBefore,
              "top-K query " + i + " must be SERVED");
          Assertions.assertEquals(1L, SirixVectorizedExecutor.sortedTopKAppliedCount() - topKBefore,
              "top-K query " + i + " must use BOUNDED selection, not the full sort");
        }
        final long servedBefore = SirixVectorizedExecutor.sortedScanServedCount();
        final long topKBefore = SirixVectorizedExecutor.sortedTopKAppliedCount();
        Assertions.assertEquals(genericUncapped, evaluateQuery(chain, ctx, uncappedQuery), "2-arg subsequence parity");
        Assertions.assertEquals(1L, SirixVectorizedExecutor.sortedScanServedCount() - servedBefore,
            "2-arg subsequence sorted scan must still be SERVED (full sort)");
        Assertions.assertEquals(0L, SirixVectorizedExecutor.sortedTopKAppliedCount() - topKBefore,
            "2-arg subsequence must NOT be capped — it needs the unbounded tail");
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        executor.close();
      }
    }
  }

  @Test
  public void sortedScanDeclinesOnMissingOrderCells() throws IOException {
    // Rows lacking the order field: the interpreter sorts empty order keys empty-least
    // (no error) — the served path cannot represent that placement, so it must DECLINE,
    // reproduce the interpreter outcome exactly, and must NOT be counted as served.
    query("""
          jn:store('json-path1','sorted3.jn','[
            {"age": 4, "active": true, "dept": "A"},
            {"active": false, "dept": "B"},
            {"age": 2, "active": true, "dept": "C"}
          ]')
        """);
    query("""
          let $doc := jn:doc('json-path1','sorted3.jn')
          let $stats := jn:create-projection-index($doc, '/[]',
              ('/[]/age', '/[]/active', '/[]/dept'),
              ('long', 'boolean', 'string'))
          return {"revision": sdb:commit($doc)}
        """);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    final String sparseQuery =
        "let $doc := jn:doc('json-path1','sorted3.jn')\n" + "for $r in $doc[] order by $r.age return $r";
    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup("json-path1");
      final JsonResourceSession session = collection.getDatabase().beginResourceSession("sorted3.jn");
      String generic;
      try {
        generic = evaluateQuery(chain, ctx, sparseQuery);
      } catch (final RuntimeException interpreterError) {
        generic = "ERROR:" + interpreterError.getClass().getSimpleName();
      }
      final SirixVectorizedExecutor executor =
          new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 2);
      SequentialPipelineStrategy.setVectorizedExecutor(executor);
      try {
        final long servedBefore = SirixVectorizedExecutor.sortedScanServedCount();
        String served;
        try {
          served = evaluateQuery(chain, ctx, sparseQuery);
        } catch (final RuntimeException servedError) {
          served = "ERROR:" + servedError.getClass().getSimpleName();
        }
        Assertions.assertEquals(generic, served,
            "missing order cells: served path must reproduce the interpreter outcome, error included");
        Assertions.assertEquals(0L, SirixVectorizedExecutor.sortedScanServedCount() - servedBefore,
            "a store with missing order cells must NOT be served");
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        executor.close();
      }
    }
  }

  @Test
  public void coveredRowsServeAndMatchTheGenericPipeline() throws IOException {
    // P5b stage 7c: record-constructor returns over covered fields serve straight from
    // projection segments. Fixture covers missing fields (JSON null emission), booleans,
    // longs, doubles, strings, and a predicate; differential vs the generic pipeline.
    // score uses exponent-form literals: plain decimals ("1.5") shred as BigDecimal and the
    // pure-double-source gate rightly declines them (interpreter would type them Dec, the
    // projection would emit Dbl). Exponent form shreds as Double — the servable shape.
    query("""
          jn:store('json-path1','rows.jn','[
            {"age": 30, "active": true,  "dept": "Eng",   "score": 1.5E0},
            {"age": 45, "active": false, "dept": "Sales", "score": 2.25E0},
            {"active": true,  "dept": "HR", "score": -0.5E0},
            {"age": 23, "active": true},
            {"age": 61, "active": false, "dept": "Eng", "score": 12.75E0}
          ]')
        """);
    query("""
          let $doc := jn:doc('json-path1','rows.jn')
          let $stats := jn:create-projection-index($doc, '/[]',
              ('/[]/age', '/[]/active', '/[]/dept', '/[]/score'),
              ('long', 'boolean', 'string', 'double'))
          return {"revision": sdb:commit($doc)}
        """);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    final String[] queries = {
        "let $doc := jn:doc('json-path1','rows.jn')\n"
            + "for $r in $doc[] return {\"a\": $r.age, \"d\": $r.dept, \"on\": $r.active}",
        "let $doc := jn:doc('json-path1','rows.jn')\n"
            + "for $r in $doc[] where $r.age gt 25 return {\"a\": $r.age, \"d\": $r.dept}",
        // Same source field twice under distinct output names — column reuse in one record.
        "let $doc := jn:doc('json-path1','rows.jn')\n" + "for $r in $doc[] return {\"a\": $r.age, \"b\": $r.age}",
        // Double column (pure-double-source gate) alongside a long column.
        "let $doc := jn:doc('json-path1','rows.jn')\n" + "for $r in $doc[] return {\"s\": $r.score, \"a\": $r.age}",
        // COMPOSITION over the served records: a missing field must be the EMPTY sequence
        // inside the record (count skips it), not a JSON null item (count would include
        // it). 5 rows, one missing age -> 4. Catches Null.INSTANCE-vs-empty divergence
        // that plain serialization cannot see.
        "let $doc := jn:doc('json-path1','rows.jn')\n"
            + "return count(for $o in (for $r in $doc[] return {\"a\": $r.age}) return $o.a)",
        // Same composition through a DIRECT deref over the parenthesized pipe. Serving-safe
        // only because SirixDerefExpr fixed Brackit's DerefExpr, which derefed any
        // FlatteningSequence base (what parens evaluate to) to empty without evaluating
        // the pipe at all — this query used to be 0 = 0 vacuously.
        "let $doc := jn:doc('json-path1','rows.jn')\n" + "return count((for $r in $doc[] return {\"a\": $r.age}).a)",
        // COMPUTED record entries (gap 2): +/-/* trees alongside direct fields. Rows
        // missing an operand store the EMPTY sequence (JSON null when serialized).
        "let $doc := jn:doc('json-path1','rows.jn')\n"
            + "for $r in $doc[] return {\"twice\": $r.age * 2, \"d\": $r.dept}",
        // Computed-entry composition: missing-operand rows contribute NO item to .t.
        "let $doc := jn:doc('json-path1','rows.jn')\n"
            + "return count((for $r in $doc[] return {\"t\": $r.age + $r.age - 1}).t)"};
    // OR predicates have no conjunctive extraction — the executor must DECLINE and the
    // generic pipeline must answer (served counter unchanged, result identical).
    final String orQuery = "let $doc := jn:doc('json-path1','rows.jn')\n"
        + "for $r in $doc[] where $r.age gt 55 or $r.age lt 25 return {\"a\": $r.age, \"d\": $r.dept}";
    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      final JsonDBCollection collection = (JsonDBCollection) store.lookup("json-path1");
      final JsonResourceSession session = collection.getDatabase().beginResourceSession("rows.jn");
      final String[] generic = new String[queries.length];
      for (int i = 0; i < queries.length; i++) {
        generic[i] = evaluateQuery(chain, ctx, queries[i]);
      }
      final String genericOr = evaluateQuery(chain, ctx, orQuery);
      final SirixVectorizedExecutor executor =
          new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber(), 2);
      SequentialPipelineStrategy.setVectorizedExecutor(executor);
      try {
        for (int i = 0; i < queries.length; i++) {
          final long servedBefore = SirixVectorizedExecutor.rowMaterializeServedCount();
          Assertions.assertEquals(generic[i], evaluateQuery(chain, ctx, queries[i]), "covered-row parity, query " + i);
          Assertions.assertEquals(1L, SirixVectorizedExecutor.rowMaterializeServedCount() - servedBefore,
              "covered-row query " + i + " must be SERVED from the projection");
        }
        final long servedBeforeOr = SirixVectorizedExecutor.rowMaterializeServedCount();
        Assertions.assertEquals(genericOr, evaluateQuery(chain, ctx, orQuery),
            "OR-predicate covered-row query must still answer correctly (generic fallback)");
        Assertions.assertEquals(0L, SirixVectorizedExecutor.rowMaterializeServedCount() - servedBeforeOr,
            "OR predicates must DECLINE covered-row serving, not serve");
        // Review finding (kind-blind predicates): a type-mismatched literal — string-EQ
        // over the NUMERIC age column — must DECLINE; the interpreter type-errors and
        // that outcome (error included) must flow through unchanged.
        final String mismatchQuery = "let $doc := jn:doc('json-path1','rows.jn')\n"
            + "for $r in $doc[] where $r.age = \"x\" return {\"a\": $r.age}";
        final long servedBeforeMismatch = SirixVectorizedExecutor.rowMaterializeServedCount();
        String servedMismatch;
        try {
          servedMismatch = evaluateQuery(chain, ctx, mismatchQuery);
        } catch (final RuntimeException e) {
          servedMismatch = "ERROR:" + e.getClass().getSimpleName();
        }
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        String genericMismatch;
        try {
          genericMismatch = evaluateQuery(chain, ctx, mismatchQuery);
        } catch (final RuntimeException e) {
          genericMismatch = "ERROR:" + e.getClass().getSimpleName();
        } finally {
          SequentialPipelineStrategy.setVectorizedExecutor(executor);
        }
        Assertions.assertEquals(genericMismatch, servedMismatch,
            "type-mismatched predicate must reproduce the interpreter outcome exactly");
        Assertions.assertEquals(0L, SirixVectorizedExecutor.rowMaterializeServedCount() - servedBeforeMismatch,
            "string-EQ over a numeric column must DECLINE, never run a kind-blind scan");
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        executor.close();
      }
    }
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
