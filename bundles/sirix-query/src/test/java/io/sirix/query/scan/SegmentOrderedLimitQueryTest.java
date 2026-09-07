package io.sirix.query.scan;

import io.brackit.query.Query;
import io.brackit.query.atomic.QNm;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.Path;
import io.brackit.query.util.path.PathParser;
import io.brackit.query.util.serialize.StringSerializer;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.AfterCommitState;
import io.sirix.access.trx.node.HashType;
import io.sirix.access.trx.node.json.JsonIndexController;
import io.sirix.access.trx.node.json.ParallelBulkJsonImporter;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.projection.ProjectionBulkLoad;
import io.sirix.index.projection.ProjectionColumnScan;
import io.sirix.index.projection.ProjectionIndexCatalog;
import io.sirix.index.projection.ProjectionIndexRegistry;
import io.sirix.index.projection.ProjectionIndexRowGroupPage;
import io.sirix.index.projection.SegmentDictionaryLane;
import io.sirix.page.ChunkedBodyConfig;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end witness for ClickBench q25's shape — {@code WHERE SearchPhrase <> '' ORDER BY
 * SearchPhrase LIMIT 10} — over a SEGMENT-scoped string key, the kind every string column takes
 * under the segment dictionary lane the 100M database is built with.
 *
 * <p>
 * Before the segment bound existed the planner declared a segment-scoped key unboundable (a mint id
 * means nothing outside its own segment), so every admitted leaf carried an unknown bound, none was
 * ever skipped, and the whole column was decoded into a ten-entry heap. The lever gives each segment
 * a bound read from its dictionary's first/last COLLATION position — refined past the excluded
 * literal — and compares those bounds through their dictionary VALUES, so best-first selection can
 * stop.
 *
 * <p>
 * The fixture loads 64 regions × 2,048 rows through the real parallel bulk import with the lane
 * armed and the segment span cap lowered, so the corpus closes SEVERAL segments — a single-segment
 * fixture would give every leaf the same bound and could not tell pruning from a tie. Every third
 * row carries the empty phrase (the minimum of every segment, exactly as in ClickBench), and the
 * non-empty phrases are ordered by region, so the ten smallest live in the first segment and every
 * later segment is provably irrelevant without being decoded.
 *
 * <p>
 * Each witness asserts the PRECONDITION (the phrase column really is segment-scoped, several
 * segments were sealed) and then that the answer equals the interpreter's AND that the lever
 * engaged, so a fixture that silently fell back to another column kind cannot pass by answering
 * correctly.
 */
public final class SegmentOrderedLimitQueryTest {

  private static final int REGIONS = 64;
  private static final int ROWS_PER_REGION = 2_048;
  private static final int LEAF_ROWS = 1_024;
  private static final int MIN_LEAVES = REGIONS * ROWS_PER_REGION / LEAF_ROWS;
  private static final int RECORDS = REGIONS * ROWS_PER_REGION;
  private static final int K = 10;
  private static final String DB = "seg-order-db";
  private static final String RES = "records.jn";
  private static final String SRC = "jn:doc('" + DB + "','" + RES + "')[]";
  private static final String GLOBAL_MODE_PROPERTY = "sirix.projection.globalDict";
  private static final String SPAN_PROPERTY = "sirix.segmentDict.maxLeaves";
  /** Record pages per segment: this corpus closes several segments at this span. */
  private static final String SPAN_PAGES = "128";

  private File location;
  private String priorLane;
  private String priorGlobalMode;
  private String priorSpan;
  private boolean priorChunked;
  private int priorChunkTarget;

  @BeforeEach
  void setUp() throws Exception {
    priorLane = System.getProperty(SegmentDictionaryLane.ENABLED_PROPERTY);
    priorGlobalMode = System.getProperty(GLOBAL_MODE_PROPERTY);
    priorSpan = System.getProperty(SPAN_PROPERTY);
    System.setProperty(SegmentDictionaryLane.ENABLED_PROPERTY, "true");
    // The projection's own global column dictionary is not under test; with it out of the way every
    // string column of a leaf is converted to the segment kind.
    System.setProperty(GLOBAL_MODE_PROPERTY, "never");
    System.setProperty(SPAN_PROPERTY, SPAN_PAGES);
    // A converted page is readable only on the lazy route, which needs a chunk-framed body; the lane
    // refuses to arm without one.
    priorChunked = ChunkedBodyConfig.setEnabledForTesting(true);
    priorChunkTarget = ChunkedBodyConfig.setTargetChunkBytesForTesting(4096);
    ProjectionBulkLoad.clearActive();
    ProjectionIndexCatalog.clearCache();
    ProjectionIndexRegistry.clear();

    location = Files.createTempDirectory("sirix-seg-order-").toFile();
    Databases.createJsonDatabase(new DatabaseConfiguration(dbFile().toPath()));
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(dbFile().toPath())) {
      db.createResource(ResourceConfiguration.newBuilder(RES)
                                             .useDeweyIDs(false)
                                             .hashKind(HashType.NONE)
                                             .storeNodeHistory(false)
                                             .buildPathSummary(true)
                                             .versioningApproach(VersioningType.FULL)
                                             .build());
      try (JsonResourceSession session = db.beginResourceSession(RES);
          JsonNodeTrx wtx = session.beginNodeTrx(2048, AfterCommitState.KEEP_OPEN_ASYNC_FLUSH)) {
        assertTrue(wtx.moveToDocumentRoot());
        final JsonIndexController controller = session.getWtxIndexController(wtx.getRevisionNumber());
        controller.createProjectionIndexAtLoadStart(projectionDef(), wtx, RECORDS);
        ParallelBulkJsonImporter.assembleBytes(wtx, new ByteArrayInputStream(corpus()), 1 << 20, 4);
        wtx.commit();
      }
    }
    ProjectionIndexCatalog.clearCache();
    ProjectionIndexRegistry.clear();
    Databases.getGlobalBufferManager().clearAllCaches();
  }

  @AfterEach
  void tearDown() {
    ChunkedBodyConfig.setTargetChunkBytesForTesting(priorChunkTarget);
    ChunkedBodyConfig.setEnabledForTesting(priorChunked);
    restore(SegmentDictionaryLane.ENABLED_PROPERTY, priorLane);
    restore(GLOBAL_MODE_PROPERTY, priorGlobalMode);
    restore(SPAN_PROPERTY, priorSpan);
    SequentialPipelineStrategy.setVectorizedExecutor(null);
    ProjectionBulkLoad.clearActive();
    ProjectionIndexCatalog.clearCache();
    ProjectionIndexRegistry.clear();
    if (location != null) {
      Databases.removeDatabase(dbFile().toPath());
    }
    Databases.getGlobalBufferManager().clearAllCaches();
  }

  /** The database directory under the store's location ({@code Path} in this file is brackit's). */
  private File dbFile() {
    return new File(location, DB);
  }

  private static void restore(final String property, final String value) {
    if (value == null) {
      System.clearProperty(property);
    } else {
      System.setProperty(property, value);
    }
  }

  private static IndexDef projectionDef() {
    final List<Path<QNm>> fieldPaths = List.of(Path.parse("/[]/phrase", PathParser.Type.JSON),
        Path.parse("/[]/n", PathParser.Type.JSON));
    return IndexDefs.createProjectionIdxDef(Path.parse("/[]", PathParser.Type.JSON), fieldPaths,
        List.of(Type.STR, Type.LON), 0, IndexDef.DbType.JSON);
  }

  private static int regionOf(final int i) {
    return i / ROWS_PER_REGION;
  }

  /**
   * Every third row is the empty phrase — the COLLATION minimum of every segment, and what the
   * {@code <>} refines the endpoint past. The rest are distinct and ordered by region, so a
   * segment's smallest non-empty value is strictly larger than the previous segment's.
   */
  private static String phraseOf(final int i) {
    return i % 3 == 0
        ? ""
        : String.format("p-%02d-%04d", regionOf(i), i % ROWS_PER_REGION);
  }

  private static byte[] corpus() {
    final StringBuilder sb = new StringBuilder(RECORDS * 40);
    sb.append('[');
    for (int i = 0; i < RECORDS; i++) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append("{\"phrase\":\"").append(phraseOf(i)).append("\",\"n\":").append(i).append('}');
    }
    sb.append(']');
    return sb.toString().getBytes(StandardCharsets.UTF_8);
  }

  /** The oracle: the k smallest (or largest) phrases, {@code excluded} dropped, ties in value order. */
  private static List<String> expected(final String excluded, final boolean descending) {
    final List<String> values = new ArrayList<>(RECORDS);
    for (int i = 0; i < RECORDS; i++) {
      final String phrase = phraseOf(i);
      if (!phrase.equals(excluded)) {
        values.add(phrase);
      }
    }
    values.sort(descending
        ? Comparator.reverseOrder()
        : Comparator.naturalOrder());
    return values.subList(0, K);
  }

  private static String orderedLimitQuery(final boolean excludeEmpty, final boolean descending) {
    return "subsequence(\nfor $h in " + SRC + "\n" + (excludeEmpty
        ? "where $h.phrase != \"\"\n"
        : "") + "order by $h.phrase" + (descending
            ? " descending"
            : "") + "\nreturn $h.phrase, 1, " + K + ")";
  }

  @Test
  void aSegmentKeyExcludingItsMinimumIsBoundedByCollationEndpointsAndSkipsLeaves() throws Exception {
    for (final boolean descending : new boolean[] {false, true}) {
      final String query = orderedLimitQuery(true, descending);
      final String interpreted = run(query, false);
      assertPhrases(interpreted, expected("", descending), "interpreter, descending=" + descending);

      final long servedBefore = SirixVectorizedExecutor.sortedScanServedCount();
      final long topKBefore = SirixVectorizedExecutor.sortedTopKAppliedCount();
      final long skippedBefore = ProjectionColumnScan.topKLeavesSkippedCount();
      final long tiedBefore = ProjectionColumnScan.topKPlanTiedCount();
      final String served = run(query, true);

      assertEquals(interpreted, served, "the sorted arm diverges from the interpreter: " + query);
      assertTrue(SirixVectorizedExecutor.sortedScanServedCount() > servedBefore,
          "the query was NOT served by the sorted route: " + query);
      assertTrue(SirixVectorizedExecutor.sortedTopKAppliedCount() > topKBefore, "the bounded heap never applied");
      assertEquals(tiedBefore, ProjectionColumnScan.topKPlanTiedCount(),
          "every leaf tied on its bound: the segment collation endpoints did not differentiate them");
      final long skipped = ProjectionColumnScan.topKLeavesSkippedCount() - skippedBefore;
      assertTrue(skipped >= MIN_LEAVES / 2,
          "a bounded plan over a segment-scoped key must skip most leaves, skipped " + skipped);
      System.out.printf("[witness] %s k=%d leaves>=%d skipped=%d%n  answer=%s%n", descending
          ? "ORDER BY phrase DESCENDING"
          : "ORDER BY phrase ASCENDING", K, MIN_LEAVES, skipped, served.replace('\n', ' ').trim());
    }
  }

  /**
   * The deliberately refused shape: without a predicate naming the key a leaf could hide a row whose
   * sort key is MISSING, which only the interpreter can place, and the only proof otherwise
   * available costs a whole-column pass inside planning. The scan therefore evaluates unbounded — a
   * correct answer, no leaf skipped.
   */
  @Test
  void anUnrefinedSegmentOrderingAnswersUnboundedWithoutSkippingALeaf() throws Exception {
    final String query = orderedLimitQuery(false, false);
    final String interpreted = run(query, false);
    assertPhrases(interpreted, expected(null, false), "interpreter, unrefined");

    final long servedBefore = SirixVectorizedExecutor.sortedScanServedCount();
    final long skippedBefore = ProjectionColumnScan.topKLeavesSkippedCount();
    final String served = run(query, true);

    assertEquals(interpreted, served, "the unbounded sorted arm diverges from the interpreter: " + query);
    assertTrue(SirixVectorizedExecutor.sortedScanServedCount() > servedBefore,
        "the query was NOT served by the sorted route: " + query);
    assertEquals(skippedBefore, ProjectionColumnScan.topKLeavesSkippedCount(),
        "no predicate names the key, so no bound is produced and no leaf may be skipped");
    System.out.printf("[witness] ORDER BY phrase (no key predicate) k=%d skipped=%d%n  answer=%s%n", K,
        ProjectionColumnScan.topKLeavesSkippedCount() - skippedBefore, served.replace('\n', ' ').trim());
  }

  /** Every serialized phrase, in order, against the oracle. */
  private static void assertPhrases(final String out, final List<String> expected, final String what) {
    final List<String> actual = new ArrayList<>(K);
    for (final String token : out.trim().split("\\s+")) {
      if (!token.isEmpty()) {
        actual.add(token.replace("\"", ""));
      }
    }
    assertEquals(expected, actual, what + ": " + out);
  }

  /** The precondition: the phrase column is segment-scoped and the corpus closed several segments. */
  private static void assertSegmentFixture(final JsonResourceSession session) {
    final String registryKey = session.getResourceConfig().getResource().toString();
    final ProjectionIndexRegistry.Handle handle = ProjectionIndexCatalog.lookupCovering(session, registryKey,
        session.getMostRecentRevisionNumber(), new String[] {"[]"}, new String[] {"phrase", "n"});
    assertNotNull(handle, "the covering projection handle");
    final int col = handle.columnOf("phrase");
    assertEquals(ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SEGMENT, handle.columnKindOf(col),
        "phrase must be a SEGMENT-scoped column, or the lever under test is never reached");
    final int segments = handle.segmentDictionarySegmentCount();
    assertTrue(segments >= 2, "the span cap must close several segments, got " + segments);
    for (int segment = 0; segment < segments; segment++) {
      assertTrue(handle.segmentDictionaryHeaderKey(segment, col) > 0L, "segment " + segment + " has a dictionary");
    }
    assertNotNull(handle.columnStoreOrNull(), "column store");
    assertTrue(handle.columnStoreOrNull().leafCount() >= MIN_LEAVES,
        "at least " + MIN_LEAVES + " leaves, got " + handle.columnStoreOrNull().leafCount());
  }

  private String run(final String query, final boolean vectorized) throws Exception {
    try (BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(location.toPath()).build();
        SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        SirixCompileChain chain = vectorized
            ? SirixCompileChain.createWithJsonStore(store)
            : SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store)) {
      SirixVectorizedExecutor exec = null;
      try {
        if (vectorized) {
          final Database<JsonResourceSession> db = Databases.openJsonDatabase(dbFile().toPath());
          final JsonResourceSession session = db.beginResourceSession(RES);
          // Assert the fixture BEFORE the query runs, so a wrong column kind fails the precondition,
          // not the lever.
          assertSegmentFixture(session);
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
