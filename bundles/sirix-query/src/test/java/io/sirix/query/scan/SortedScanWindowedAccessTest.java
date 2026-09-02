package io.sirix.query.scan;

import io.brackit.query.Query;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.util.serialize.StringSerializer;
import io.sirix.access.Databases;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.projection.ProjectionColumnStore;
import io.sirix.index.projection.ProjectionIndexCatalog;
import io.sirix.index.projection.ProjectionIndexRegistry;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The sorted top-k scan over a non-retaining (per-leaf-set) column access, against the interpreter.
 *
 * <p>
 * A whole-column slice fill of a fat string column at 100M rows is several GB of per-leaf
 * dictionaries and can never be resident, so the kernel never fills a column for itself: it serves
 * whatever the store already holds resident and decodes exactly the leaves it visits otherwise. The
 * two must answer identically — including string sort keys resolved across leaves for the heap
 * comparisons, zone-map pruned leaves, and the record keys of the winners. The first arm pins the
 * fill budget to one byte so nothing can be resident and asserts the non-retaining access engaged;
 * the second pre-fills every column and the record keys through the catalog's store and asserts the
 * scan then took the resident route (the witness must NOT move).
 * </p>
 */
final class SortedScanWindowedAccessTest {
  private static final String DB = "sorted-windowed-db";
  private static final String RES = "records.jn";
  private static final int N = 30_000;
  private static final String DOC = "jn:doc('" + DB + "','" + RES + "')[]";

  private static final List<String> QUERIES = List.of(
      // string sort key (ISO timestamps, non-monotonic in document order), string predicate
      "subsequence(for $h in " + DOC + " where contains($h.url, 'google') order by $h.t "
          + "return $h, 1, 10)",
      // descending, with a numeric predicate beside the string one
      "subsequence(for $h in " + DOC + " where contains($h.url, 'google') and $h.v ge 1000 "
          + "order by $h.t descending return $h, 1, 10)",
      // numeric sort key
      "subsequence(for $h in " + DOC + " where contains($h.url, 'bing') order by $h.v descending "
          + "return $h, 1, 7)");

  private Path dbDir;
  private long previousBudget = -1L;

  @BeforeEach
  void setUp() throws Exception {
    SirixVectorizedExecutor.STRICT_SERVING = true;
    dbDir = Files.createTempDirectory("sirix-sorted-windowed-");
    final StringBuilder sb = new StringBuilder(N * 96);
    sb.append('[');
    for (int i = 0; i < N; i++) {
      if (i > 0) {
        sb.append(',');
      }
      // Timestamps scattered over a year so the best-first leaf order differs from document order;
      // every value distinct so the top-k is deterministic.
      final int minute = (i * 7919) % 525_600;
      final int day = minute / 1440;
      final int hh = (minute % 1440) / 60;
      final int mm = minute % 60;
      sb.append("{\"t\":\"2024-").append(String.format("%02d", 1 + day / 28)).append('-')
        .append(String.format("%02d", 1 + day % 28)).append('T').append(String.format("%02d", hh)).append(':')
        .append(String.format("%02d", mm)).append(":00\",\"url\":\"http://").append(i % 23 == 0
            ? "www.google.com/q"
            : i % 31 == 0
                ? "www.bing.com/s"
                : "site").append(i % 977).append(".example/p").append(i).append("\",\"v\":").append((i * 31) % 20_011)
        .append('}');
    }
    sb.append(']');
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain, "jn:store('" + DB + "','" + RES + "','" + sb + "')").evaluate(ctx);
      new Query(chain, """
          let $doc := jn:doc('%s','%s')
          let $stats := jn:create-projection-index($doc, '/[]', ('/[]/t', '/[]/url', '/[]/v'),
            ('string', 'string', 'long'))
          return sdb:commit($doc)
          """.formatted(DB, RES)).evaluate(ctx);
    }
  }

  @AfterEach
  void tearDown() {
    if (previousBudget >= 0L) {
      ProjectionColumnStore.setColumnFillBudgetBytesForTesting(previousBudget);
    }
    SirixVectorizedExecutor.STRICT_SERVING = false;
    SequentialPipelineStrategy.setVectorizedExecutor(null);
    if (dbDir != null) {
      Databases.removeDatabase(dbDir.resolve(DB));
    }
  }

  @Test
  @DisplayName("resident and windowed leaf access answer the interpreter's top-k exactly")
  void windowedAccessAgreesWithResidentAndInterpreter() throws Exception {
    final String[] generic = new String[QUERIES.size()];
    for (int i = 0; i < QUERIES.size(); i++) {
      generic[i] = run(QUERIES.get(i), false);
    }
    // Arm 1 FIRST: a one-byte fill budget — no column can be resident, the windowed access serves.
    // (Resident fills persist in the catalog's store, so this arm must run before any resident one.)
    previousBudget = ProjectionColumnStore.setColumnFillBudgetBytesForTesting(1L);
    final long windowedBefore = ProjectionColumnStore.windowedLeafAccessCount();
    for (int i = 0; i < QUERIES.size(); i++) {
      final long servedBefore = SirixVectorizedExecutor.sortedScanServedCount();
      assertEquals(generic[i], run(QUERIES.get(i), true), "windowed sorted scan diverges for: " + QUERIES.get(i));
      assertTrue(SirixVectorizedExecutor.sortedScanServedCount() > servedBefore,
          "not served by the sorted scan under the one-byte budget: " + QUERIES.get(i));
    }
    final long windowedAfter = ProjectionColumnStore.windowedLeafAccessCount();
    // Arm 2: the default budget, every column and the record keys RESIDENT — the same answers from
    // the retained slices. The scan observes residency but never creates it, so the fills are made
    // here through the catalog's shared store, exactly as an earlier query would have left them.
    ProjectionColumnStore.setColumnFillBudgetBytesForTesting(previousBudget);
    previousBudget = -1L;
    try (var db = Databases.openJsonDatabase(dbDir.resolve(DB)); var session = db.beginResourceSession(RES)) {
      final int revision = session.getMostRecentRevisionNumber();
      final ProjectionIndexRegistry.Handle handle = ProjectionIndexCatalog.lookupCovering(session,
          session.getResourceConfig().getResource().toString(), revision, new String[] {"[]"},
          new String[] {"t", "url", "v"});
      assertNotNull(handle, "the projection must be loadable");
      final ProjectionColumnStore store = handle.columnStoreOrNull();
      assertNotNull(store, "the catalog must build a column store");
      final ProjectionColumnStore.ColumnSegmentFetcher fetcher =
          ProjectionIndexCatalog.columnSegmentFetcher(session, revision);
      for (final String name : new String[] {"t", "url", "v"}) {
        final int col = handle.columnOf(name);
        assertTrue(col >= 0, "column " + name + " must be projected");
        assertNotNull(store.column(col, fetcher), "column " + name + " must fill");
        assertTrue(store.columnFilled(col), "column " + name + " must be resident after its fill");
      }
      assertNotNull(store.recordKeys(fetcher), "the record keys must fill");
      assertTrue(store.recordKeysFilled(), "the record keys must be resident after their fill");
    }
    for (int i = 0; i < QUERIES.size(); i++) {
      final long servedBefore = SirixVectorizedExecutor.sortedScanServedCount();
      assertEquals(generic[i], run(QUERIES.get(i), true), "resident sorted scan diverges for: " + QUERIES.get(i));
      assertTrue(SirixVectorizedExecutor.sortedScanServedCount() > servedBefore, "not served by the sorted scan: "
          + QUERIES.get(i));
    }
    assertEquals(windowedAfter, ProjectionColumnStore.windowedLeafAccessCount(),
        "the resident arm must serve from the retained slices, not decode leaves for itself");
    assertTrue(ProjectionColumnStore.windowedLeafAccessCount() > windowedBefore,
        "the windowed access never engaged: the budget seam did not take, the agreement above is vacuous");
  }

  @Test
  @DisplayName("predicate value emission serves through the windowed access as well")
  void valueEmissionServesWindowed() throws Exception {
    // Point lookups emitting another field, and the same field (Q20's shape).
    final List<String> queries = List.of(
        "for $h in " + DOC + " where $h.v eq 12345 return $h.t",
        "for $h in " + DOC + " where $h.v eq 12345 return $h.v",
        "for $h in " + DOC + " where $h.t eq '2024-03-05T10:13:00' return $h.url");
    final String[] generic = new String[queries.size()];
    for (int i = 0; i < queries.size(); i++) {
      generic[i] = run(queries.get(i), false);
    }
    previousBudget = ProjectionColumnStore.setColumnFillBudgetBytesForTesting(1L);
    final long windowedBefore = ProjectionColumnStore.windowedLeafAccessCount();
    for (int i = 0; i < queries.size(); i++) {
      final long servedBefore = SirixVectorizedExecutor.predicateValueEmissionsServedCount();
      assertEquals(generic[i], run(queries.get(i), true), "windowed value emission diverges for: " + queries.get(i));
      assertTrue(SirixVectorizedExecutor.predicateValueEmissionsServedCount() > servedBefore,
          "not served by value emission under the one-byte budget: " + queries.get(i));
    }
    assertTrue(ProjectionColumnStore.windowedLeafAccessCount() > windowedBefore, "the windowed access never engaged");
  }

  @Test
  @DisplayName("a predicate column already resident is masked in place — no second fetch, no second budget charge")
  void valueEmissionReusesAResidentPredicateColumnUnderAFullBudget() throws Exception {
    // q19 at 100M/8 GB inside a leg: UserID's body was retained by an earlier query, the residency
    // decision priced it at zero (resident), then the predicate path fetched it masked and priced the
    // masked bytes against a budget the column's own body already filled — declined every try
    // ("masked slice fill adds 117 MB beside 2,118 MB already retained"), while alone it served.
    final String query = "for $h in " + DOC + " where $h.v eq 12345 return $h.t";
    final String generic = run(query, false);
    // Make `v` resident through a route that PUBLISHES its fill (the value-emission route masks its
    // columns and masked fills are never published): a sliced group-by keyed on it.
    final String fill = "subsequence(for $h in " + DOC + " let $k := $h.v group by $k let $c := count($h) "
        + "order by $c descending return {\"v\": $k, \"c\": $c}, 1, 5)";
    assertEquals(run(fill, false), run(fill, true), "the filling group query diverges");
    final ProjectionColumnStore store;
    final int v;
    try (var db = Databases.openJsonDatabase(dbDir.resolve(DB)); var session = db.beginResourceSession(RES)) {
      final ProjectionIndexRegistry.Handle handle = ProjectionIndexCatalog.lookupCovering(session,
          session.getResourceConfig().getResource().toString(), session.getMostRecentRevisionNumber(),
          new String[] {"[]"}, new String[] {"v", "t"});
      assertNotNull(handle, "the projection must be loadable");
      store = handle.columnStoreOrNull();
      assertNotNull(store, "the catalog must build a column store");
      v = handle.columnOf("v");
    }
    assertTrue(v >= 0 && store.columnFilled(v), "the first (resident) serve must have retained the predicate column");
    // No headroom at all: any charge for the already-resident column would trip the door.
    previousBudget = ProjectionColumnStore.setColumnFillBudgetBytesForTesting(store.retainedFillBytes());
    final long servedBefore = SirixVectorizedExecutor.predicateValueEmissionsServedCount();
    final long declinedBefore = SirixVectorizedExecutor.predicateValueEmissionDeclinedCount();
    final long retainedBefore = store.retainedFillBytes();
    final String served = run(query, true);
    ProjectionColumnStore.setColumnFillBudgetBytesForTesting(previousBudget);
    previousBudget = -1L;
    assertEquals(generic, served, "value emission over the masked resident view diverges");
    assertEquals(servedBefore + 1, SirixVectorizedExecutor.predicateValueEmissionsServedCount(),
        "the route must serve the resident column under a full budget");
    assertEquals(declinedBefore, SirixVectorizedExecutor.predicateValueEmissionDeclinedCount(),
        "a resident predicate column must not be re-priced against the budget");
    assertEquals(retainedBefore, store.retainedFillBytes(), "nothing new may be retained");
  }

  private String run(final String query, final boolean vectorized) throws Exception {
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = vectorized
            ? SirixCompileChain.createWithJsonStore(store)
            : SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store)) {
      SirixVectorizedExecutor exec = null;
      try {
        if (vectorized) {
          final var db = Databases.openJsonDatabase(dbDir.resolve(DB));
          final JsonResourceSession session = db.beginResourceSession(RES);
          exec = new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber());
          SequentialPipelineStrategy.setVectorizedExecutor(exec);
        }
        final Sequence result = new Query(chain, query).execute(ctx);
        final StringWriter out = new StringWriter();
        try (PrintWriter pw = new PrintWriter(out)) {
          new StringSerializer(pw).serialize(result);
        }
        return out.toString();
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        if (exec != null) {
          exec.close();
        }
      }
    }
  }
}
