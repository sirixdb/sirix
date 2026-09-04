package io.sirix.query;

import io.brackit.query.Query;
import io.brackit.query.QueryException;
import io.sirix.JsonTestHelper;
import io.sirix.access.Databases;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexType;
import io.sirix.index.projection.ProjectionIndexCatalog;
import io.sirix.index.projection.ProjectionIndexHOTStorage;
import io.sirix.index.projection.ProjectionIndexMetadata;
import io.sirix.index.projection.ProjectionIndexRegistry;
import io.sirix.query.json.BasicJsonDBStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;

/**
 * End-to-end coverage of {@code jn:create-projection-index}: create a columnar projection over
 * stored records via JSONiq and run the analytical query shapes it serves. Results must be
 * identical with and without the projection installed — the function's job is acceleration, not
 * semantics.
 */
public final class ProjectionIndexFunctionTest extends AbstractJsonTest {

  private static final String STORE_QUERY = """
        jn:store('json-path1','sales.jn','[
          {"age": 30, "active": true,  "dept": "Eng"},
          {"age": 45, "active": false, "dept": "Sales"},
          {"age": 52, "active": true,  "dept": "Eng"},
          {"age": 23, "active": true,  "dept": "HR"},
          {"age": 61, "active": false, "dept": "Eng"}
        ]')
      """;

  private static final String CREATE_INDEX_QUERY = """
        let $doc := jn:doc('json-path1','sales.jn')
        let $stats := jn:create-projection-index($doc, '/[]',
            ('/[]/age', '/[]/active', '/[]/dept'),
            ('long', 'boolean', 'string'))
        return {"revision": sdb:commit($doc)}
      """;

  // The registry is a JVM-wide static — the on-disk stores are wiped between
  // tests, so a surviving entry would serve stale leaves for a fresh
  // same-named resource.
  @BeforeEach
  public void clearProjectionRegistryBefore() {
    ProjectionIndexRegistry.clear();
  }

  @AfterEach
  public void clearProjectionRegistryAfter() {
    ProjectionIndexRegistry.clear();
  }

  private static final String STORE_DOUBLES_QUERY = """
        jn:store('json-path1','prices.jn','[
          {"price": 0.5,   "qty": 3},
          {"price": 2.25,  "qty": 1},
          {"price": 4.125, "qty": 7},
          {"price": 8.0,   "qty": 2},
          {"price": 1.5,   "qty": 5}
        ]')
      """;

  private static final String CREATE_DOUBLE_INDEX_QUERY = """
        let $doc := jn:doc('json-path1','prices.jn')
        let $stats := jn:create-projection-index($doc, '/[]',
            ('/[]/price', '/[]/qty'),
            ('double', 'long'))
        return {"revision": sdb:commit($doc)}
      """;

  // Dyadic-exact values (0.5, 2.25, 4.125, 8.0, 1.5): sums and extrema are exact doubles, so
  // the projection fast path and the interpreted pipeline must print identical results —
  // regardless of whether the value-exactness gate lets the fast path serve (JSON decimals
  // shredded as BigDecimal mark the column not-value-exact and fall back; parity holds
  // either way, which is exactly the fail-closed contract).
  @Test
  public void doubleColumnSumMatchesInterpretedPipeline() throws IOException {
    final String query = """
          let $doc := jn:doc('json-path1','prices.jn')
          return sum(for $r in $doc[] return $r.price)
        """;
    test(STORE_DOUBLES_QUERY, CREATE_DOUBLE_INDEX_QUERY, query, "16.375");
  }

  @Test
  public void doubleColumnMinMaxMatchInterpretedPipeline() throws IOException {
    final String minQuery = """
          let $doc := jn:doc('json-path1','prices.jn')
          return min(for $r in $doc[] return $r.price)
        """;
    test(STORE_DOUBLES_QUERY, CREATE_DOUBLE_INDEX_QUERY, minQuery, "0.5");
  }

  @Test
  public void doubleColumnPredicatesUseTransformedLiterals() throws IOException {
    // Regression for the plan-time literal transform: predicates on a NUMERIC_DOUBLE column
    // compare against TRANSFORMED cells — an untransformed literal would silently match the
    // wrong rows. Both double and integer literals; parity with the interpreted pipeline is
    // the oracle (fail-closed fallback produces the same answer by construction).
    final String fpFiltered = """
          let $doc := jn:doc('json-path1','prices.jn')
          return sum(for $r in $doc[] where $r.price > 2.0 return $r.price)
        """;
    test(STORE_DOUBLES_QUERY, CREATE_DOUBLE_INDEX_QUERY, fpFiltered, "14.375");
    final String longFiltered = """
          let $doc := jn:doc('json-path1','prices.jn')
          return count(for $r in $doc[] where $r.price >= 8 return $r)
        """;
    test(STORE_DOUBLES_QUERY, CREATE_DOUBLE_INDEX_QUERY, longFiltered, "1");
  }

  @Test
  public void doubleColumnFractionalDecimalPredicateMatchesInterpretedPipeline() throws IOException {
    // Regression for the OP_DEC_CMP hole: '2.5' is a DECIMAL literal (not xs:double), whose
    // long-space arm (x >= 3) compared untransformed against transformed cells matched ALL
    // rows. XQuery promotes the decimal to double against double values, so the transformed
    // promoted literal is exact parity.
    final String query = """
          let $doc := jn:doc('json-path1','prices.jn')
          return sum(for $r in $doc[] where $r.price > 2.5 return $r.price)
        """;
    test(STORE_DOUBLES_QUERY, CREATE_DOUBLE_INDEX_QUERY, query, "12.125");
    final String filteredCount = """
          let $doc := jn:doc('json-path1','prices.jn')
          return count(for $r in $doc[] where $r.price > 2.5 return $r)
        """;
    test(STORE_DOUBLES_QUERY, CREATE_DOUBLE_INDEX_QUERY, filteredCount, "2");
  }

  @Test
  public void doubleColumnFilteredCountViaLongPredicate() throws IOException {
    final String query = """
          let $doc := jn:doc('json-path1','prices.jn')
          return count(for $r in $doc[] where $r.qty > 2 return $r)
        """;
    test(STORE_DOUBLES_QUERY, CREATE_DOUBLE_INDEX_QUERY, query, "3");
  }

  @Test
  public void createProjectionIndexAndAggregate() throws IOException {
    final String query = """
          let $doc := jn:doc('json-path1','sales.jn')
          return sum(for $r in $doc[] return $r.age)
        """;
    test(STORE_QUERY, CREATE_INDEX_QUERY, query, "211");
  }

  @Test
  public void createProjectionIndexAndFilterCount() throws IOException {
    final String query = """
          let $doc := jn:doc('json-path1','sales.jn')
          return count(for $r in $doc[] where $r.age > 40 and $r.active return $r)
        """;
    test(STORE_QUERY, CREATE_INDEX_QUERY, query, "1");
  }

  @Test
  public void createProjectionIndexAndGroupBy() throws IOException {
    final String query = """
          let $doc := jn:doc('json-path1','sales.jn')
          for $r in $doc[]
          let $d := $r.dept
          group by $d
          order by $d
          return {"dept": $d, "count": count($r)}
        """;
    test(STORE_QUERY, CREATE_INDEX_QUERY, query,
        "{\"dept\":\"Eng\",\"count\":3} {\"dept\":\"HR\",\"count\":1} {\"dept\":\"Sales\",\"count\":1}");
  }

  @Test
  public void createProjectionIndexIsIdempotent() throws IOException {
    // Second call short-circuits on the already-installed registry entry.
    final String query = """
          let $doc := jn:doc('json-path1','sales.jn')
          let $stats := jn:create-projection-index($doc, '/[]',
              ('/[]/age', '/[]/active', '/[]/dept'),
              ('long', 'boolean', 'string'))
          return count($doc[])
        """;
    test(STORE_QUERY, CREATE_INDEX_QUERY, query, "5");
  }

  @Test
  public void hydratesFromPersistedMetadataAfterRegistryClear() throws IOException {
    // Simulates a fresh process: the in-memory registry is empty but the
    // projection (metadata at slot 0 + compact leaves) is persisted —
    // re-creating with the same shape must hydrate, not rebuild, and the
    // hydrated projection must serve correct results.
    query(STORE_QUERY);
    query(CREATE_INDEX_QUERY);
    ProjectionIndexRegistry.clear();
    final String hydrateAndAggregate = """
          let $doc := jn:doc('json-path1','sales.jn')
          let $stats := jn:create-projection-index($doc, '/[]',
              ('/[]/age', '/[]/active', '/[]/dept'),
              ('long', 'boolean', 'string'))
          return sum(for $r in $doc[] return $r.age)
        """;
    test(hydrateAndAggregate, "211");
  }

  @Test
  public void secondProjectionWithDifferentShapeCoexists() throws IOException {
    // A different shape is a NEW projection with its own catalogued id and
    // HOT sub-tree — analogous to the other index families — not an error.
    query(STORE_QUERY);
    query(CREATE_INDEX_QUERY);
    final String createSecondAndQuery = """
          let $doc := jn:doc('json-path1','sales.jn')
          let $stats := jn:create-projection-index($doc, '/[]',
              ('/[]/age', '/[]/dept'), ('long', 'string'))
          let $rev := sdb:commit($doc)
          let $doc2 := jn:doc('json-path1','sales.jn')
          return {"sum": sum(for $r in $doc2[] return $r.age),
                  "distinct": count(for $r in $doc2[] let $d := $r.dept group by $d return $d)}
        """;
    test(createSecondAndQuery, "{\"sum\":211,\"distinct\":3}");
  }

  @Test
  public void bothProjectionsHydrateAfterRegistryClear() throws IOException {
    // Both catalogued projections must survive close/re-open: each hydrates
    // from its own sub-tree via its own metadata payload and serves queries.
    query(STORE_QUERY);
    query(CREATE_INDEX_QUERY);
    query("""
          let $doc := jn:doc('json-path1','sales.jn')
          let $stats := jn:create-projection-index($doc, '/[]',
              ('/[]/age', '/[]/dept'), ('long', 'string'))
          return {"revision": sdb:commit($doc)}
        """);
    ProjectionIndexRegistry.clear();
    final String hydrateBothAndQuery = """
          let $doc := jn:doc('json-path1','sales.jn')
          let $s1 := jn:create-projection-index($doc, '/[]',
              ('/[]/age', '/[]/active', '/[]/dept'),
              ('long', 'boolean', 'string'))
          let $s2 := jn:create-projection-index($doc, '/[]',
              ('/[]/age', '/[]/dept'), ('long', 'string'))
          return {"filtered": count(for $r in $doc[] where $r.age > 40 and $r.active return $r),
                  "sum": sum(for $r in $doc[] return $r.age)}
        """;
    test(hydrateBothAndQuery, "{\"filtered\":1,\"sum\":211}");
  }

  @Test
  public void duplicateFieldNamesAreRejected() {
    query(STORE_QUERY);
    final String duplicate = """
          let $doc := jn:doc('json-path1','sales.jn')
          return jn:create-projection-index($doc, '/[]',
              ('/[]/age', '/[]/nested/age'),
              ('long', 'long'))
        """;
    final QueryException e = Assertions.assertThrows(QueryException.class, () -> query(duplicate));
    Assertions.assertTrue(e.getMessage().contains("Duplicate projected field name"),
        () -> "unexpected message: " + e.getMessage());
  }

  @Test
  public void unsupportedColumnTypesAreRejectedButDoubleIsAccepted() throws IOException {
    query(STORE_QUERY);
    // Double columns are supported since the storage redesign's P6
    // (NUMERIC_DOUBLE, docs/PROJECTION_INDEX_STORAGE_REDESIGN.md §2.6)…
    final String floating = """
          let $doc := jn:doc('json-path1','sales.jn')
          let $stats := jn:create-projection-index($doc, '/[]',
              ('/[]/age'), ('double'))
          return {"revision": sdb:commit($doc)}
        """;
    query(floating);
    // …while genuinely unsupported types still fail loudly. ('datetime' and 'date' used to stand
    // here; they are DECLARABLE since the temporal column kinds landed, so the guard moved to a type
    // the vocabulary still does not know — otherwise it would have gone quietly vacuous.)
    final String unsupported = """
          let $doc := jn:doc('json-path1','sales.jn')
          return jn:create-projection-index($doc, '/[]',
              ('/[]/age'), ('time'))
        """;
    final QueryException e = Assertions.assertThrows(QueryException.class, () -> query(unsupported));
    Assertions.assertTrue(e.getMessage().contains("Unsupported projection column type"),
        () -> "unexpected message: " + e.getMessage());
    // The message must name every type that IS declarable, or a caller cannot discover the temporal
    // ones from the failure alone.
    for (final String declarable : new String[] {"long", "double", "decimal", "boolean", "string", "timestamp",
        "date"}) {
      Assertions.assertTrue(e.getMessage().contains(declarable),
          () -> "the rejection must list '" + declarable + "': " + e.getMessage());
    }
  }

  @Test
  public void updateIsMaintainedIncrementallyWithoutRecreate() throws IOException {
    // The projection change listener (IndexController lifecycle) maintains
    // the columns INCREMENTALLY: the commit patches the touched leaf by
    // re-extraction, so queries see the update from the same catalogued
    // projection — no re-create required, and the definition stays live.
    query(STORE_QUERY);
    query(CREATE_INDEX_QUERY);
    query("""
          let $doc := jn:doc('json-path1','sales.jn')
          return replace json value of $doc[0].age with 99
        """);
    // 211 - 30 + 99 = 280; the maintained projection must NOT serve the old 211.
    final String sumQuery = """
          let $doc := jn:doc('json-path1','sales.jn')
          return sum(for $r in $doc[] return $r.age)
        """;
    test(sumQuery, "280");
    // The definition is still catalogued under its original id, and a
    // same-shape re-create short-circuits on the maintained snapshot.
    final String recreateAndSum = """
          let $doc := jn:doc('json-path1','sales.jn')
          let $idx := jn:find-projection-index($doc, '/[]', ('/[]/age', '/[]/active', '/[]/dept'))
          let $stats := jn:create-projection-index($doc, '/[]',
              ('/[]/age', '/[]/active', '/[]/dept'),
              ('long', 'boolean', 'string'))
          let $rev := sdb:commit($doc)
          let $doc2 := jn:doc('json-path1','sales.jn')
          return {"idx": $idx, "sum": sum(for $r in $doc2[] return $r.age)}
        """;
    test(recreateAndSum, "{\"idx\":0,\"sum\":280}");
  }

  @Test
  public void insertIsMaintainedIncrementally() throws IOException {
    // New records append to the projection's tail leaf at commit time —
    // including a head-position insert (node keys are monotone, so the new
    // record sorts after every indexed key regardless of document position).
    query(STORE_QUERY);
    query(CREATE_INDEX_QUERY);
    query("""
          let $doc := jn:doc('json-path1','sales.jn')
          return append json {"age": 7, "active": true, "dept": "Eng"} into $doc
        """);
    query("""
          let $doc := jn:doc('json-path1','sales.jn')
          return insert json {"age": 100, "active": false, "dept": "HR"} into $doc at position 0
        """);
    // 211 + 7 + 100 = 318, served without re-creating the projection.
    test("""
          let $doc := jn:doc('json-path1','sales.jn')
          return sum(for $r in $doc[] return $r.age)
        """, "318");
  }

  @Test
  public void deleteIsMaintainedIncrementally() throws IOException {
    // Deleting a record drops its row during the commit-time leaf rebuild.
    query(STORE_QUERY);
    query(CREATE_INDEX_QUERY);
    query("""
          let $doc := jn:doc('json-path1','sales.jn')
          return delete json $doc[1]
        """);
    // 211 - 45 = 166.
    test("""
          let $doc := jn:doc('json-path1','sales.jn')
          return {"sum": sum(for $r in $doc[] return $r.age),
                  "count": count($doc[])}
        """, "{\"sum\":166,\"count\":4}");
  }

  @Test
  public void mixedUpdateInsertDeleteAcrossCommitsStaysCorrect() throws IOException {
    // Three maintenance commits in sequence — each patches the previous
    // commit's leaves, so the snapshot keeps compounding correctly.
    query(STORE_QUERY);
    query(CREATE_INDEX_QUERY);
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
    // 211 - 30 + 99 + 7 - 45 = 242.
    test("""
          let $doc := jn:doc('json-path1','sales.jn')
          return sum(for $r in $doc[] return $r.age)
        """, "242");
  }

  @Test
  public void structuralRecordSetDeleteIsMaintainedExactly() throws IOException {
    // Removing the record-set array itself drops every record: the records
    // fire their own post-order delete notifications, so the maintained
    // projection ends up truthfully EMPTY (no tombstone) and queries stay
    // correct.
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
    test("""
          let $doc := jn:doc('json-path1','two.jn')
          return {"a": sum(for $r in $doc.a[] return $r.age),
                  "b": sum(for $r in $doc.b[] return $r.age)}
        """, "{\"a\":0,\"b\":3}");
  }

  @Test
  public void queriesUsePersistedProjectionWithoutRecreate() throws IOException {
    // Analogous to the other index families: after re-open (simulated by
    // clearing all in-memory projection state) queries discover the
    // catalogued projection through the revision-scoped catalog + pages —
    // no re-run of jn:create-projection-index required.
    query(STORE_QUERY);
    query(CREATE_INDEX_QUERY);
    ProjectionIndexRegistry.clear();
    final String query = """
          let $doc := jn:doc('json-path1','sales.jn')
          return sum(for $r in $doc[] return $r.age)
        """;
    test(query, "211");
  }

  @Test
  public void twoRecordSetsWithSameFieldNameStayCorrect() throws IOException {
    // Two projections over DIFFERENT record sets sharing the trailing field
    // name "age" must never answer for each other — selection is ambiguous
    // by trailing name alone, so queries fall back to the generic pipeline
    // and stay correct.
    query("""
          jn:store('json-path1','two.jn','{
            "a": [{"age": 10}, {"age": 20}],
            "b": [{"age": 1}, {"age": 2}]
          }')
        """);
    query("""
          let $doc := jn:doc('json-path1','two.jn')
          let $s1 := jn:create-projection-index($doc, '/a/[]', ('/a/[]/age'), ('long'))
          let $s2 := jn:create-projection-index($doc, '/b/[]', ('/b/[]/age'), ('long'))
          return {"revision": sdb:commit($doc)}
        """);
    final String query = """
          let $doc := jn:doc('json-path1','two.jn')
          return {"a": sum(for $r in $doc.a[] return $r.age),
                  "b": sum(for $r in $doc.b[] return $r.age)}
        """;
    test(query, "{\"a\":30,\"b\":3}");
  }

  @Test
  public void sameShapeTwiceBeforeCommitCataloguesOnce() throws IOException {
    // Two identical creates in one (uncommitted) transaction must reuse one
    // definition — the wtx-side catalogue is consulted, not just the
    // committed one.
    query(STORE_QUERY);
    final String doubleCreate = """
          let $doc := jn:doc('json-path1','sales.jn')
          let $s1 := jn:create-projection-index($doc, '/[]',
              ('/[]/age', '/[]/active', '/[]/dept'), ('long', 'boolean', 'string'))
          let $s2 := jn:create-projection-index($doc, '/[]',
              ('/[]/age', '/[]/active', '/[]/dept'), ('long', 'boolean', 'string'))
          let $rev := sdb:commit($doc)
          let $doc2 := jn:doc('json-path1','sales.jn')
          return sum(for $r in $doc2[] return $r.age)
        """;
    test(doubleCreate, "211");
    final Path dbPath = Path.of(JsonTestHelper.PATHS.PATH1.getFile().getParent().toString(), "json-path1");
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath);
        final JsonResourceSession session = database.beginResourceSession("sales.jn")) {
      final int mostRecent = session.getMostRecentRevisionNumber();
      Assertions.assertEquals(1,
          session.getRtxIndexController(mostRecent).getIndexes().getNrOfIndexDefsWithType(IndexType.PROJECTION));
    }
  }

  @Test
  public void findAndDropProjectionIndex() throws IOException {
    query(STORE_QUERY);
    query(CREATE_INDEX_QUERY);
    test("""
          let $doc := jn:doc('json-path1','sales.jn')
          return jn:find-projection-index($doc, '/[]', ('/[]/age', '/[]/active', '/[]/dept'))
        """, "0");
    query("""
          let $doc := jn:doc('json-path1','sales.jn')
          let $dropped := jn:drop-projection-index($doc, 0)
          return {"revision": sdb:commit($doc)}
        """);
    // Catalogue no longer lists the definition; queries stay correct via
    // the generic pipeline.
    test("""
          let $doc := jn:doc('json-path1','sales.jn')
          return {"idx": jn:find-projection-index($doc, '/[]', ('/[]/age', '/[]/active', '/[]/dept')),
                  "sum": sum(for $r in $doc[] return $r.age)}
        """, "{\"idx\":-1,\"sum\":211}");
  }

  @Test
  public void droppingWhenNothingIsCataloguedStrandsNoWriteTransaction() {
    // jn:drop-projection-index($doc) with nothing to drop is a successful no-op — and has to stay a
    // no-op all the way down. It opens a write transaction to read the catalogue, and the writer
    // permit is shared across every ResourceSession for the resource (WriteLocksRegistry), so
    // leaving that transaction open on the no-change path strands the permit: the next beginNodeTrx
    // anywhere fails with "No read-write transaction available" after its 5s tryAcquire.
    //
    // The second session below is what makes this observable — within ONE session a leaked wtx is
    // simply reused by the next writer, which hides the leak entirely.
    query(STORE_QUERY);
    try (
        final BasicJsonDBStore store =
            BasicJsonDBStore.newBuilder().location(JsonTestHelper.PATHS.PATH1.getFile().getParent()).build();
        final SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        final SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain, """
            let $doc := jn:doc('json-path1','sales.jn')
            return jn:drop-projection-index($doc)
          """).evaluate(ctx);
      // The store above is still open, so a stranded permit is still held here.
      try (
          final Database<JsonResourceSession> database =
              Databases.openJsonDatabase(JsonTestHelper.PATHS.PATH1.getFile());
          final JsonResourceSession session = database.beginResourceSession("sales.jn");
          final var wtx = session.beginNodeTrx()) {
        Assertions.assertNotNull(wtx, "a no-op drop must leave the resource writable");
      }
    }
  }

  @Test
  public void recreateAfterDropAndUpdateUsesANewTree() throws IOException {
    // After a drop no listener maintains tree 0. The old tree stays immutable for historical
    // revisions and can never be an initializer target again. A same-shape re-creation therefore
    // receives tree 1 and builds the current records into that virgin tree.
    query(STORE_QUERY);
    query(CREATE_INDEX_QUERY);
    final byte[] retiredTreeMetadata = readProjectionMetadata(0);
    query("""
          let $doc := jn:doc('json-path1','sales.jn')
          let $dropped := jn:drop-projection-index($doc)
          return {"revision": sdb:commit($doc)}
        """);
    Assertions.assertArrayEquals(retiredTreeMetadata, readProjectionMetadata(0),
        "drop must not mutate the historical tree");
    query("""
          let $doc := jn:doc('json-path1','sales.jn')
          return replace json value of $doc[0].age with 99
        """);
    Assertions.assertArrayEquals(retiredTreeMetadata, readProjectionMetadata(0),
        "updates after drop must not mutate the retired tree");
    query(CREATE_INDEX_QUERY);
    // 211 - 30 + 99 = 280 — served from the new projection, while the physical existence of tree 0
    // proves its id was not recycled after the catalogue entry disappeared.
    test("""
          let $doc := jn:doc('json-path1','sales.jn')
          return {"idx": jn:find-projection-index($doc, '/[]',
                      ('/[]/age', '/[]/active', '/[]/dept')),
                  "sum": sum(for $r in $doc[] return $r.age)}
        """, "{\"idx\":1,\"sum\":280}");
  }

  @Test
  public void unusableCataloguedProjectionCannotBeRebuiltInPlace() {
    query(STORE_QUERY);
    query(CREATE_INDEX_QUERY);
    final byte[] staleMetadata = ProjectionIndexMetadata.staleTombstone().serialize();

    final Path dbPath = Path.of(JsonTestHelper.PATHS.PATH1.getFile().getParent().toString(), "json-path1");
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath);
        final JsonResourceSession session = database.beginResourceSession("sales.jn");
        final JsonNodeTrx wtx = session.beginNodeTrx()) {
      new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), 0).putBlob(0, staleMetadata);
      wtx.commit();
    }
    ProjectionIndexCatalog.clearCache();

    final QueryException failure = Assertions.assertThrows(QueryException.class, () -> query("""
          let $doc := jn:doc('json-path1','sales.jn')
          return jn:create-projection-index($doc, '/[]',
              ('/[]/age', '/[]/active', '/[]/dept'),
              ('long', 'boolean', 'string'))
        """));
    Assertions.assertTrue(failure.getMessage().contains("cannot be rebuilt in place"), failure::getMessage);
    Assertions.assertArrayEquals(staleMetadata, readProjectionMetadata(0),
        "refusing creation must not alter the unusable tree");
  }

  @Test
  public void metadataLessCataloguedProjectionRejectsIncrementalMaintenance() throws IOException {
    query(STORE_QUERY);
    query(CREATE_INDEX_QUERY);

    final Path dbPath = Path.of(JsonTestHelper.PATHS.PATH1.getFile().getParent().toString(), "json-path1");
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath);
        final JsonResourceSession session = database.beginResourceSession("sales.jn");
        final JsonNodeTrx wtx = session.beginNodeTrx()) {
      new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), 0).tombstoneBlob(0);
      wtx.commit();
    }
    ProjectionIndexCatalog.clearCache();

    final RuntimeException failure = Assertions.assertThrows(RuntimeException.class, () -> query("""
          let $doc := jn:doc('json-path1','sales.jn')
          return replace json value of $doc[0].age with 99
        """));
    Assertions.assertTrue(causeChainContains(failure, "has no live metadata"), failure::toString);
    Assertions.assertNull(readProjectionMetadata(0), "failed maintenance must not manufacture replacement metadata");
    test("""
          let $doc := jn:doc('json-path1','sales.jn')
          return sum(for $r in $doc[] return $r.age)
        """, "211");
  }

  @Test
  public void explicitlyAbandonedProjectionMakesLaterMaintenanceANoOp() throws IOException {
    query(STORE_QUERY);
    query(CREATE_INDEX_QUERY);
    final byte[] staleMetadata =
        ProjectionIndexMetadata.staleTombstone(ProjectionIndexMetadata.StaleReason.GLOBAL_DICTIONARY_BUDGET_EXCEEDED)
                               .serialize();

    final Path dbPath = Path.of(JsonTestHelper.PATHS.PATH1.getFile().getParent().toString(), "json-path1");
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath);
        final JsonResourceSession session = database.beginResourceSession("sales.jn");
        final JsonNodeTrx wtx = session.beginNodeTrx()) {
      new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), 0).putBlob(0, staleMetadata);
      wtx.commit();
    }
    ProjectionIndexCatalog.clearCache();

    query("""
          let $doc := jn:doc('json-path1','sales.jn')
          return append json {"age": 7, "active": true, "dept": "Eng"} into $doc
        """);
    Assertions.assertArrayEquals(staleMetadata, readProjectionMetadata(0),
        "ordinary writes must not touch an explicitly abandoned projection tree");
    test("""
          let $doc := jn:doc('json-path1','sales.jn')
          return sum(for $r in $doc[] return $r.age)
        """, "218");
  }

  private static byte[] readProjectionMetadata(final int indexNumber) {
    final Path dbPath = Path.of(JsonTestHelper.PATHS.PATH1.getFile().getParent().toString(), "json-path1");
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath);
        final JsonResourceSession session = database.beginResourceSession("sales.jn");
        final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
      return ProjectionIndexHOTStorage.readBlob(rtx.getStorageEngineReader(), indexNumber, 0L);
    }
  }

  private static boolean causeChainContains(final Throwable failure, final String text) {
    Throwable cursor = failure;
    for (int depth = 0; cursor != null && depth < 16; depth++, cursor = cursor.getCause()) {
      if (cursor.getMessage() != null && cursor.getMessage().contains(text)) {
        return true;
      }
    }
    return false;
  }

  @Test
  public void recurringTrailingNameUnderRecordSetIsAcceptedAndServesTheDeclaredPath() throws IOException {
    // "age" exists both at /[]/age and /[]/addr/age. Column lookup compares the DECLARED PATH
    // relative to the record set, so the two are distinguishable and creation must succeed —
    // rejecting this would make any real nested corpus unprojectable. The sums prove the
    // disambiguation is real and not merely tolerated: the top-level column answers the top-level
    // deref, and the nested deref (which no column covers) still answers from the generic pipeline
    // rather than being served the projected column's values.
    query("""
          jn:store('json-path1','amb.jn','[
            {"age": 30, "addr": {"age": 99}},
            {"age": 45, "addr": {"age": 12}}
          ]')
        """);
    query("""
          let $doc := jn:doc('json-path1','amb.jn')
          let $stats := jn:create-projection-index($doc, '/[]',
              ('/[]/age'), ('long'))
          return {"revision": sdb:commit($doc)}
        """);
    test("""
          let $doc := jn:doc('json-path1','amb.jn')
          return sum(for $r in $doc[] return $r.age)
        """, "75");
    test("""
          let $doc := jn:doc('json-path1','amb.jn')
          return sum(for $r in $doc[] return $r.addr.age)
        """, "111");
  }

  @Test
  public void fieldNameAmbiguousWithoutARelativizablePathIsRejected() {
    // A declared path that does not sit strictly under the declared root has no relative chain, so
    // lookup falls back to comparing the bare trailing name — the one shape where a recurring name
    // really is ambiguous, and the one this guard still has to refuse. Here the record set is
    // '/[]/addr' while the column is declared at '/[]/age', outside it: no chain can be formed, and
    // '/[]/addr/age' is a second 'age' under the record set that name matching would confuse it with.
    query("""
          jn:store('json-path1','amb2.jn','[
            {"age": 30, "addr": {"age": 99}},
            {"age": 45, "addr": {"age": 12}}
          ]')
        """);
    final String create = """
          let $doc := jn:doc('json-path1','amb2.jn')
          return jn:create-projection-index($doc, '/[]/addr',
              ('/[]/age'), ('long'))
        """;
    final QueryException e = Assertions.assertThrows(QueryException.class, () -> query(create));
    Assertions.assertTrue(e.getMessage().contains("ambiguous"), () -> "unexpected message: " + e.getMessage());
  }

  @Test
  public void duplicateProjectedColumnNameIsRejected() {
    // Distinct paths, same trailing name: lookup could tell them apart, but the column NAME is part
    // of the projection's identity, so two columns may not share one.
    query("""
          jn:store('json-path1','dup.jn','[
            {"age": 30, "addr": {"age": 99}},
            {"age": 45, "addr": {"age": 12}}
          ]')
        """);
    final String create = """
          let $doc := jn:doc('json-path1','dup.jn')
          return jn:create-projection-index($doc, '/[]',
              ('/[]/age', '/[]/addr/age'), ('long', 'long'))
        """;
    final QueryException e = Assertions.assertThrows(QueryException.class, () -> query(create));
    Assertions.assertTrue(e.getMessage().contains("Duplicate projected field name"),
        () -> "unexpected message: " + e.getMessage());
  }
}
