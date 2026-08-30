package io.sirix.query.scan;

import io.brackit.query.Query;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.util.serialize.StringSerializer;
import io.sirix.access.Databases;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.projection.ProjectionColumnStore;
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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The sorted top-k scan over a leaf-at-a-time (windowed) column access, against the interpreter.
 *
 * <p>
 * A whole-column slice fill of a fat string column at 100M rows is several GB of per-leaf
 * dictionaries and can never be resident, so the kernel now takes a {@code LeafColumnAccess}: the
 * store hands out resident slices when every needed column fits the fill budget and decodes leaves
 * per window otherwise. The two must answer identically — including string sort keys resolved
 * across leaves for the heap comparisons, zone-map pruned leaves, and the record keys of the winners.
 * The second arm pins the fill budget to one byte so the windowed access is what serves, and asserts
 * that it did.
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
    // Arm 2: the default budget — resident slices, the same answers.
    ProjectionColumnStore.setColumnFillBudgetBytesForTesting(previousBudget);
    previousBudget = -1L;
    for (int i = 0; i < QUERIES.size(); i++) {
      final long servedBefore = SirixVectorizedExecutor.sortedScanServedCount();
      assertEquals(generic[i], run(QUERIES.get(i), true), "resident sorted scan diverges for: " + QUERIES.get(i));
      assertTrue(SirixVectorizedExecutor.sortedScanServedCount() > servedBefore, "not served by the sorted scan: "
          + QUERIES.get(i));
    }
    assertEquals(windowedAfter, ProjectionColumnStore.windowedLeafAccessCount(),
        "the resident arm must not take the windowed access");
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
