package io.sirix.query.scan;

import io.brackit.query.Query;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.util.serialize.StringSerializer;
import io.sirix.access.Databases;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.projection.ProjectionColumnScan;
import io.sirix.index.projection.ProjectionIndexBuilder;
import io.sirix.index.projection.ProjectionIndexCatalog;
import io.sirix.index.projection.ProjectionIndexHOTStorage;
import io.sirix.index.projection.ProjectionIndexMetadata;
import io.sirix.index.projection.ProjectionIndexRegistry;
import io.sirix.index.projection.ProjectionIndexRowGroupPage;
import io.sirix.index.projection.ProjectionRankPass;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression witness for ClickBench q25
 * ({@code WHERE SearchPhrase <> '' ORDER BY SearchPhrase LIMIT
 * 10}) on a build whose sort key is STRING_GLOBAL with a rank-ordered dictionary (2026-09-03). The
 * bounded top-k planned its leaf bounds from the per-leaf string extrema (a per-leaf dictionary
 * kind) or from the zone (numeric kinds) and left a global key unbounded, so every leaf was visited
 * and the stop rule never fired: 0.056 s became 0.26 s hot at 100M. Under a fully-ordered
 * dictionary the id lane IS the value order, so a leaf's smallest present id bounds it — and,
 * because the empty string's id is every leaf's minimum, the {@code <>} refinement to the SECOND
 * extremum ({@link io.sirix.index.projection.ProjectionColumnStore#longValueExtrema}) is what lets
 * the walk skip.
 *
 * <p>
 * The fixture: 200 leaves, every leaf holds the empty phrase, the non-empty phrases are a
 * permutation of zero-padded numbers so each leaf's second-smallest value differs and the ten
 * smallest values sit in a handful of leaves. The witness asserts the dictionary is global and
 * rank-ordered, that the sorted route served with the bounded heap, that leaves were actually
 * SKIPPED (the plan was neither unbounded nor tied), and the answer's equality with the
 * interpreter.
 */
final class SortedGlobalKeyExcludingItsMinimumQueryTest {
  private static final String DB = "sorted-global-key-db";
  private static final String RES = "records.jn";
  private static final String DOC = "jn:doc('" + DB + "','" + RES + "')[]";
  private static final int LEAF_ROWS = 1_024;
  private static final int LEAVES = 200;
  private static final int N = LEAVES * LEAF_ROWS;
  private static final int INDEX_NUMBER = 0;
  private static final String MODE_PROPERTY = "sirix.projection.globalDict";
  private static final String RANK_PROPERTY = "sirix.projection.globalDict.rank";

  private Path dbDir;
  private Path spillDir;
  private String priorMode;
  private String priorRank;

  @BeforeEach
  void setUp() throws Exception {
    priorMode = System.getProperty(MODE_PROPERTY);
    priorRank = System.getProperty(RANK_PROPERTY);
    // The rank pass converts a PER-LEAF dictionary into a rank-ordered global one; the load itself
    // must therefore not elect a global dictionary.
    System.setProperty(MODE_PROPERTY, "never");
    System.setProperty(RANK_PROPERTY, "true");
    SirixVectorizedExecutor.STRICT_SERVING = true;
    dbDir = Files.createTempDirectory("sirix-sorted-global-key-");
    spillDir = Files.createTempDirectory("sirix-sorted-global-key-spill-");
    final StringBuilder sb = new StringBuilder(N * 40);
    sb.append('[');
    for (int i = 0; i < N; i++) {
      if (i > 0) {
        sb.append(',');
      }
      // Every third row is the empty phrase — the minimum of EVERY leaf under collation. The rest
      // are a permutation of 0..N-1 (7919 is coprime to N), zero-padded so byte order is numeric
      // order: each leaf's second-smallest value is distinct and the ten smallest are scattered.
      final String phrase = i % 3 == 0
          ? ""
          : String.format("p-%06d", (int) (((long) i * 7919L) % N));
      sb.append("{\"phrase\":\"").append(phrase).append("\",\"n\":").append(i).append('}');
    }
    sb.append(']');
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain, "jn:store('" + DB + "','" + RES + "','" + sb + "')").evaluate(ctx);
      new Query(chain, """
          let $doc := jn:doc('%s','%s')
          let $stats := jn:create-projection-index($doc, '/[]', ('/[]/phrase', '/[]/n'), ('string', 'long'))
          return sdb:commit($doc)
          """.formatted(DB, RES)).evaluate(ctx);
    }
    assertEquals(0, ProjectionIndexBuilder.globalDictionaryColumnsBuilt(),
        "the fixture must start from a per-leaf dictionary or the rank pass has nothing to convert");
    try (var db = Databases.openJsonDatabase(dbDir.resolve(DB));
        JsonResourceSession session = db.beginResourceSession(RES)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        ProjectionRankPass.run(wtx, INDEX_NUMBER, 0, spillDir, 1 << 20);
      }
      final ProjectionIndexMetadata metadata = metadata(session);
      assertEquals(ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL, metadata.columnKinds()[0],
          "the rank pass must have converted the phrase column");
    }
    ProjectionIndexCatalog.clearCache();
    ProjectionIndexRegistry.clear();
  }

  @AfterEach
  void tearDown() {
    restore(MODE_PROPERTY, priorMode);
    restore(RANK_PROPERTY, priorRank);
    SirixVectorizedExecutor.STRICT_SERVING = false;
    SequentialPipelineStrategy.setVectorizedExecutor(null);
    ProjectionIndexCatalog.clearCache();
    ProjectionIndexRegistry.clear();
    if (dbDir != null) {
      Databases.removeDatabase(dbDir.resolve(DB));
    }
  }

  private static void restore(final String key, final String value) {
    if (value == null) {
      System.clearProperty(key);
    } else {
      System.setProperty(key, value);
    }
  }

  @Test
  void aGlobalKeyExcludingItsMinimumIsBoundedBySecondExtremaAndSkipsLeaves() throws Exception {
    final String query = "subsequence(\nfor $h in " + DOC + "\n" + "where $h.phrase != \"\"\n" + "order by $h.phrase\n"
        + "return $h.phrase, 1, 10)";
    final String generic = run(query, false);
    assertTrue(generic.contains("\"p-0000"), "the interpreter answered nothing: " + generic);

    final long servedBefore = SirixVectorizedExecutor.sortedScanServedCount();
    final long topKBefore = SirixVectorizedExecutor.sortedTopKAppliedCount();
    final long skippedBefore = ProjectionColumnScan.topKLeavesSkippedCount();
    final long tiedBefore = ProjectionColumnScan.topKPlanTiedCount();
    final String served = run(query, true);
    assertEquals(generic, served, "the sorted arm diverges from the interpreter");
    assertTrue(SirixVectorizedExecutor.sortedScanServedCount() > servedBefore, "not served by the sorted route");
    assertTrue(SirixVectorizedExecutor.sortedTopKAppliedCount() > topKBefore, "the bounded heap never applied");
    assertEquals(tiedBefore, ProjectionColumnScan.topKPlanTiedCount(),
        "every leaf tied on its first extremum: the <> refinement to the second one did not run");
    final long skipped = ProjectionColumnScan.topKLeavesSkippedCount() - skippedBefore;
    assertTrue(skipped >= LEAVES / 2,
        "a bounded plan over a rank-ordered global key must skip most leaves, skipped " + skipped);

    // Descending: the bound is the largest present id; no leaf's maximum is the excluded empty
    // string, so the refinement stays on the first extremum and the plan still skips.
    final String descQuery = "subsequence(\nfor $h in " + DOC + "\n" + "where $h.phrase != \"\"\n"
        + "order by $h.phrase descending\n" + "return $h.phrase, 1, 10)";
    final String descGeneric = run(descQuery, false);
    final long descSkippedBefore = ProjectionColumnScan.topKLeavesSkippedCount();
    assertEquals(descGeneric, run(descQuery, true), "the descending sorted arm diverges from the interpreter");
    assertTrue(ProjectionColumnScan.topKLeavesSkippedCount() - descSkippedBefore >= LEAVES / 2,
        "the descending plan must skip most leaves");
  }

  private static ProjectionIndexMetadata metadata(final JsonResourceSession session) {
    try (JsonNodeTrx wtx = session.beginNodeTrx()) {
      final byte[] raw = new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER).getBlob(0L);
      assertNotNull(raw, "the projection index must publish metadata");
      return ProjectionIndexMetadata.parse(raw);
    }
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
