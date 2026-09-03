package io.sirix.query.scan;

import io.brackit.query.Query;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.util.serialize.StringSerializer;
import io.sirix.access.Databases;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.projection.ProjectionColumnScan;
import io.sirix.index.projection.ProjectionColumnStore;
import io.sirix.index.projection.ProjectionIndexBuilder;
import io.sirix.index.projection.ProjectionIndexCatalog;
import io.sirix.index.projection.ProjectionIndexRegistry;
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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression witness for ClickBench q22 on the 100M build whose SearchPhrase column is STRING_GLOBAL
 * (2026-09-03). Three levers met on one leaf: the global column's {@code != ""} became zone-prunable
 * (a whole leaf of empty phrases is a collapsed zone), the WHERE carries a {@code not(contains(URL))}
 * so the predicate is a TREE with a NOT — which disarms the tree evaluator's "every operand pruned ⇒
 * no rows" shortcut — and the combined fit refused residency, so the windowed access handed the
 * kernel the zero-length pruned sentinel for the group column too. The exact evaluation produced an
 * all-zero mask but reported the leaf's row count, and the kernel read the group presence word of an
 * empty array before testing the mask: an {@code ArrayIndexOutOfBoundsException} that fell to the
 * generic pipeline for 571 s.
 *
 * <p>
 * The fixture makes every one of those conditions hold at 200 leaves: even leaves are all-empty
 * phrases (dropped by the NE zone), the query is q22's WHERE verbatim over its key and its count /
 * count-distinct aggregates (the string extrema are orthogonal to the seam and need a rank-ordered
 * dictionary to serve), three string columns are global, and a one-byte fill budget forces the
 * windowed route. The witnesses assert the preconditions
 * (global columns built, leaves actually pruned, windowed route engaged) and that the arm SERVED under
 * strict serving, beside the answer's equality with the interpreter's.
 */
final class NegatedTreeOverPrunedGlobalLeavesQueryTest {
  private static final String DB = "negated-tree-pruned-db";
  private static final String RES = "records.jn";
  private static final String DOC = "jn:doc('" + DB + "','" + RES + "')[]";
  private static final int LEAF_ROWS = 1_024;
  private static final int LEAVES = 200;
  private static final int N = LEAVES * LEAF_ROWS;
  private static final String MODE_PROPERTY = "sirix.projection.globalDict";

  private Path dbDir;
  private String priorMode;
  private String priorThreads;
  private long priorBudget = -1L;

  @BeforeEach
  void setUp() throws Exception {
    priorMode = System.getProperty(MODE_PROPERTY);
    System.setProperty(MODE_PROPERTY, "always");
    // One worker walks every window, so a pruned leaf's sentinel is seen beside decoded neighbours
    // in the SAME morsel rather than hidden behind a per-worker cache that never evicts.
    priorThreads = System.getProperty("sirix.vec.threads");
    System.setProperty("sirix.vec.threads", "1");
    SirixVectorizedExecutor.STRICT_SERVING = true;
    dbDir = Files.createTempDirectory("sirix-negated-tree-pruned-");
    final StringBuilder sb = new StringBuilder(N * 96);
    sb.append('[');
    for (int i = 0; i < N; i++) {
      if (i > 0) {
        sb.append(',');
      }
      final int leaf = i / LEAF_ROWS;
      // Even leaves: every phrase empty — the NE zone drops the whole leaf. Odd leaves: a mix, with
      // empties inside so the row-level NE is exercised where the zone cannot decide.
      final String phrase = (leaf & 1) == 0 || i % 7 == 0
          ? ""
          : "phrase-" + (i % 37);
      final String title = i % 3 == 0
          ? "Google Search " + (i % 5)
          : "Bing " + (i % 5);
      final String url = i % 5 == 0
          ? "http://mail.google.com/" + (i % 4)
          : "http://example.org/" + (i % 11);
      sb.append("{\"phrase\":\"").append(phrase).append("\",\"title\":\"").append(title)
        .append("\",\"url\":\"").append(url).append("\",\"uid\":").append(i % 101).append('}');
    }
    sb.append(']');
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain, "jn:store('" + DB + "','" + RES + "','" + sb + "')").evaluate(ctx);
      new Query(chain, """
          let $doc := jn:doc('%s','%s')
          let $stats := jn:create-projection-index($doc, '/[]',
            ('/[]/phrase', '/[]/title', '/[]/url', '/[]/uid'), ('string', 'string', 'string', 'long'))
          return sdb:commit($doc)
          """.formatted(DB, RES)).evaluate(ctx);
    }
    ProjectionIndexCatalog.clearCache();
    ProjectionIndexRegistry.clear();
    assertEquals(3, ProjectionIndexBuilder.globalDictionaryColumnsBuilt(),
        "the fixture must encode phrase, title and url with a global dictionary");
  }

  @AfterEach
  void tearDown() {
    if (priorMode == null) {
      System.clearProperty(MODE_PROPERTY);
    } else {
      System.setProperty(MODE_PROPERTY, priorMode);
    }
    if (priorThreads == null) {
      System.clearProperty("sirix.vec.threads");
    } else {
      System.setProperty("sirix.vec.threads", priorThreads);
    }
    if (priorBudget >= 0L) {
      ProjectionColumnStore.setColumnFillBudgetBytesForTesting(priorBudget);
    }
    SirixVectorizedExecutor.STRICT_SERVING = false;
    SequentialPipelineStrategy.setVectorizedExecutor(null);
    ProjectionIndexCatalog.clearCache();
    ProjectionIndexRegistry.clear();
    if (dbDir != null) {
      Databases.removeDatabase(dbDir.resolve(DB));
    }
  }

  @Test
  void aNegatedTreeOverKeepMaskedLeavesIsServedWindowed() throws Exception {
    final String query = "subsequence(\nfor $h in " + DOC + "\n"
        + "where contains($h.title, \"Google\") and not(contains($h.url, \".google.\")) and $h.phrase != \"\"\n"
        + "let $g0 := $h.phrase\ngroup by $g0\n"
        + "let $f0 := count($h)\nlet $f1 := count(distinct-values($h.uid))\n"
        + "order by $f0 descending\n"
        + "return {\"phrase\": $g0, \"c\": $f0, \"u\": $f1}, 1, 10)";
    final String generic = run(query, false);
    assertTrue(generic.contains("\"c\":"), "the interpreter answered nothing: " + generic);

    final long residentPrunedBefore = ProjectionColumnScan.treeLeavesPrunedCount();
    final String resident = run(query, true);
    assertEquals(generic, resident, "the resident arm diverges from the interpreter");
    assertTrue(ProjectionColumnScan.treeLeavesPrunedCount() - residentPrunedBefore >= LEAVES / 2,
        "the NE zone must drop every all-empty leaf of the global column");

    priorBudget = ProjectionColumnStore.setColumnFillBudgetBytesForTesting(1L);
    final long prunedBefore = ProjectionColumnScan.treeLeavesPrunedCount();
    final long servedBefore = SirixVectorizedExecutor.groupAggServedCount();
    final long windowedBefore = SirixVectorizedExecutor.groupWindowedSlicesCount();
    final String windowed = run(query, true);
    assertTrue(SirixVectorizedExecutor.groupAggServedCount() > servedBefore, "not served by the group arm");
    assertTrue(SirixVectorizedExecutor.groupWindowedSlicesCount() > windowedBefore, "the windowed route never engaged");
    assertTrue(ProjectionColumnScan.treeLeavesPrunedCount() - prunedBefore >= LEAVES / 2,
        "the windowed pass must see the same pruned leaves");
    assertEquals(generic, windowed, "the windowed arm diverges from the interpreter");
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
