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
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.projection.GlobalValueDictionary;
import io.sirix.index.projection.ProjectionBulkLoad;
import io.sirix.index.projection.ProjectionIndexByteScan;
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
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end witness that a string-length aggregate over a SEGMENT-scoped column is served from
 * per-segment length tables — one dictionary walk per segment, memoised on the handle — and never
 * canonicalises the column.
 *
 * <p>
 * ClickBench q27 ({@code AVG(length(URL)) … GROUP BY CounterID}) spent 47 s at 100M sealing the URL
 * operand: the sliced group arm resolved and ranked all 18.3M distinct URLs so the kernel could
 * fold their lengths, though a length never compares two cells. The lever hands the kernel the raw
 * packed-cell lane and a table per segment; the kernel picks the leaf's table from its zone bounds.
 * Nothing in the answer shows which path produced it, so every test here asserts the path through
 * the executor's counters as well as the values.
 *
 * <p>
 * The fixture loads 64 regions × 2,048 rows through the real parallel bulk import with the lane
 * armed. {@code name} is a multi-byte salt plus the region ("😀-17"), so code points and UTF-8
 * bytes disagree on every region and the two length modes need two tables; some rows of the even
 * regions carry no {@code name} at all, so the absent-operand rule (folds 0, still counted) has
 * rows to bite. The segment span cap is lowered so the corpus closes SEVERAL segments: a fixture
 * with one segment could not tell a per-segment table from a global one.
 */
public final class SegmentLengthLaneQueryTest {

  private static final int REGIONS = 64;
  private static final int ROWS_PER_REGION = 2_048;
  private static final int LEAF_ROWS = 1_024;
  private static final int MIN_LEAVES = REGIONS * ROWS_PER_REGION / LEAF_ROWS;
  private static final int RECORDS = REGIONS * ROWS_PER_REGION;
  /** Code points 1, 1, 1, 2; UTF-8 bytes 1, 2, 4, 3 — the two modes disagree on every region. */
  private static final String[] SALTS = {"a", "é", "😀", "ñu"};
  private static final String DB = "seglen-db";
  private static final String RES = "records.jn";
  private static final String SRC = "jn:doc('" + DB + "','" + RES + "')[]";
  private static final String GLOBAL_MODE_PROPERTY = "sirix.projection.globalDict";
  private static final String SPAN_PROPERTY = "sirix.segmentDict.maxLeaves";
  /** Record pages per segment: ~900 pages of corpus close about seven segments. */
  private static final String SPAN_PAGES = "128";

  private static final Pattern AVG_LINE = Pattern.compile("\"r\":(\\d+),\"l\":([-+0-9.Ee]+),\"c\":(\\d+)");
  private static final Pattern EXTREMA_LINE = Pattern.compile("\"r\":(\\d+),\"lo\":(\\d+),\"hi\":(\\d+),\"c\":(\\d+)");

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

    location = Files.createTempDirectory("sirix-seglen-").toFile();
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
    final List<Path<QNm>> fieldPaths = List.of(Path.parse("/[]/region", PathParser.Type.JSON),
        Path.parse("/[]/name", PathParser.Type.JSON), Path.parse("/[]/amount", PathParser.Type.JSON));
    return IndexDefs.createProjectionIdxDef(Path.parse("/[]", PathParser.Type.JSON), fieldPaths,
        List.of(Type.LON, Type.STR, Type.LON), 0, IndexDef.DbType.JSON);
  }

  private static int regionOf(final int i) {
    return i / ROWS_PER_REGION;
  }

  /** Every name of a region is its own value: no two segments share a dictionary entry. */
  private static String nameOf(final int i) {
    return SALTS[i % SALTS.length] + "-" + regionOf(i);
  }

  /**
   * Rows of the EVEN regions drop {@code name} every 1,000th row; the odd regions keep every name.
   */
  private static boolean namePresent(final int i) {
    return regionOf(i) % 2 != 0 || i % 1_000 != 500;
  }

  private static byte[] corpus() {
    final StringBuilder sb = new StringBuilder(RECORDS * 48);
    sb.append('[');
    for (int i = 0; i < RECORDS; i++) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append("{\"region\":").append(regionOf(i));
      if (namePresent(i)) {
        sb.append(",\"name\":\"").append(nameOf(i)).append('"');
      }
      sb.append(",\"amount\":").append(i % 13).append('}');
    }
    sb.append(']');
    return sb.toString().getBytes(StandardCharsets.UTF_8);
  }

  private static int lengthOf(final String value, final byte mode) {
    return mode == ProjectionIndexByteScan.STRING_LENGTH_UTF8_BYTES
        ? value.getBytes(StandardCharsets.UTF_8).length
        : value.codePointCount(0, value.length());
  }

  /**
   * Per region {@code {count, sum, min, max}} of the length lane; an absent name folds 0 and counts.
   */
  private static long[][] expected(final byte mode) {
    final long[][] out = new long[REGIONS][];
    for (int r = 0; r < REGIONS; r++) {
      out[r] = new long[] {0L, 0L, Long.MAX_VALUE, Long.MIN_VALUE};
    }
    for (int i = 0; i < RECORDS; i++) {
      final long[] acc = out[regionOf(i)];
      final int len = namePresent(i)
          ? lengthOf(nameOf(i), mode)
          : 0;
      acc[0]++;
      acc[1] += len;
      acc[2] = Math.min(acc[2], len);
      acc[3] = Math.max(acc[3], len);
    }
    return out;
  }

  /**
   * q27's shape. A length aggregate serves only under an order plan, and the plan needs a LIMIT, so
   * the groups order by the average under a window that keeps every one of them. The extrema go in a
   * query of their own: ordering on {@code xs:double(avg(f))} borrows the lane a real min/max of the
   * same field would need, and the plan declines the mix.
   */
  private static String avgQuery(final String lengthFunction) {
    return "subsequence(for $u in " + SRC + " let $r := $u.region, $len := " + lengthFunction
        + "($u.name) group by $r let $c := count($u) let $l := xs:double(avg($len)) order by $l descending "
        + "return {\"r\": $r, \"l\": $l, \"c\": $c}, 1, " + REGIONS + ")";
  }

  private static String extremaQuery(final String lengthFunction) {
    return "subsequence(for $u in " + SRC + " let $r := $u.region, $len := " + lengthFunction
        + "($u.name) group by $r let $c := count($u) order by $c descending "
        + "return {\"r\": $r, \"lo\": min($len), \"hi\": max($len), \"c\": $c}, 1, " + REGIONS + ")";
  }

  @Test
  void codePointLengthsComeFromOneTablePerSegmentAndNeverSeal() throws Exception {
    assertLengthLaneServed("string-length", ProjectionIndexByteScan.STRING_LENGTH_CODE_POINTS);
  }

  @Test
  void utf8LengthsComeFromTheirOwnTables() throws Exception {
    assertLengthLaneServed("jn:utf8-length", ProjectionIndexByteScan.STRING_LENGTH_UTF8_BYTES);
  }

  @Test
  void theSecondModeBuildsItsOwnTablesAndTheFirstStaysMemoised() throws Exception {
    final int segments = segmentCount();
    run(avgQuery("string-length"), true);
    final long buildsBefore = SirixVectorizedExecutor.projectionStringLengthTableBuildCount();
    final long hitsBefore = SirixVectorizedExecutor.projectionStringLengthTableMemoHitCount();
    run(avgQuery("jn:utf8-length"), true);
    assertEquals(segments, SirixVectorizedExecutor.projectionStringLengthTableBuildCount() - buildsBefore,
        "a byte-length table is not a code-point table: the second mode must walk every segment once");
    assertEquals(0L, SirixVectorizedExecutor.projectionStringLengthTableMemoHitCount() - hitsBefore,
        "the code-point tables must not be handed to the byte-length lane");
    run(avgQuery("string-length"), true);
    assertEquals(segments, SirixVectorizedExecutor.projectionStringLengthTableMemoHitCount() - hitsBefore,
        "the first mode's tables must still be memoised beside the second's");
  }

  @Test
  void regexGroupsPreserveLengthsMinimaMultiplicityAndStableCountTies() throws Exception {
    final int segments = segmentCount();
    assertTrue(segments > 1, "MIN must compare values from different segment dictionaries");
    final String prefix =
        "subsequence(for $u in " + SRC + " where $u.name != '' let $k := replace($u.name, '^(.*)-[0-9]+$', '$1'), "
            + "$len := jn:utf8-length($u.name) group by $k let $c := count($u) ";
    final String[] queries = {
        prefix + "let $l := xs:double(avg($len)) where $c > 10 order by $l descending "
            + "return {\"k\": $k, \"l\": $l, \"c\": $c, \"m\": min($u.name)}, 1, 4)",
        // Three salts have equal counts. LIMIT cuts that tie; document order decides the survivor.
        prefix + "order by $c descending return {\"k\": $k, \"c\": $c, \"m\": min($u.name)}, 1, 2)"};
    for (final String query : queries) {
      final String expected = run(query, false);
      final long servedBefore = SirixVectorizedExecutor.groupAggServedCount();
      final long sealsBefore = SirixVectorizedExecutor.segmentOperandSealCount();
      assertEquals(expected, run(query, true), "regex grouping must agree with the interpreter including order");
      assertTrue(SirixVectorizedExecutor.groupAggServedCount() > servedBefore, "the projection route must serve");
      assertEquals(1, SirixVectorizedExecutor.segmentOperandSealCount() - sealsBefore,
          "MIN uses one ordered value space for the original column");
    }
  }

  private void assertLengthLaneServed(final String lengthFunction, final byte mode) throws Exception {
    final int segments = segmentCount();
    final String query = avgQuery(lengthFunction);
    final String interpreted = run(query, false);
    final long servedBefore = SirixVectorizedExecutor.groupAggServedCount();
    final long buildsBefore = SirixVectorizedExecutor.projectionStringLengthTableBuildCount();
    final long hitsBefore = SirixVectorizedExecutor.projectionStringLengthTableMemoHitCount();
    final long sealsBefore = SirixVectorizedExecutor.segmentOperandSealCount();
    final String vectorized = run(query, true);
    assertTrue(SirixVectorizedExecutor.groupAggServedCount() > servedBefore,
        "the query was NOT served by the group-aggregate route: " + query);
    assertEquals(segments, SirixVectorizedExecutor.projectionStringLengthTableBuildCount() - buildsBefore,
        "the first run builds exactly one length table per segment dictionary");
    assertEquals(0L, SirixVectorizedExecutor.projectionStringLengthTableMemoHitCount() - hitsBefore,
        "nothing was memoised before the first run");
    assertEquals(0L, SirixVectorizedExecutor.segmentOperandSealCount() - sealsBefore,
        "a length lane must not canonicalise its column");
    assertAverages(vectorized, expected(mode));
    assertAverages(interpreted, expected(mode));

    final long buildsAfterFirst = SirixVectorizedExecutor.projectionStringLengthTableBuildCount();
    final String again = run(query, true);
    assertEquals(0L, SirixVectorizedExecutor.projectionStringLengthTableBuildCount() - buildsAfterFirst,
        "the second run must not walk a dictionary again");
    assertEquals(segments, SirixVectorizedExecutor.projectionStringLengthTableMemoHitCount() - hitsBefore,
        "the second run takes every segment's table from the handle memo");
    assertEquals(0L, SirixVectorizedExecutor.segmentOperandSealCount() - sealsBefore,
        "a memoised length lane must not canonicalise its column either");
    assertAverages(again, expected(mode));
    assertMemoisedTablesMatchTheDictionaries(mode);

    // The extrema lanes read the same tables (memoised by now) through the same kernel fold.
    final long buildsBeforeExtrema = SirixVectorizedExecutor.projectionStringLengthTableBuildCount();
    final long servedBeforeExtrema = SirixVectorizedExecutor.groupAggServedCount();
    final String extrema = run(extremaQuery(lengthFunction), true);
    assertTrue(SirixVectorizedExecutor.groupAggServedCount() > servedBeforeExtrema,
        "the extrema query was NOT served by the group-aggregate route");
    assertEquals(0L, SirixVectorizedExecutor.projectionStringLengthTableBuildCount() - buildsBeforeExtrema,
        "the extrema lanes take the memoised tables");
    assertEquals(0L, SirixVectorizedExecutor.segmentOperandSealCount() - sealsBefore,
        "a min/max over a LENGTH is numeric and must not seal the string column");
    assertExtrema(extrema, expected(mode));
    assertExtrema(run(extremaQuery(lengthFunction), false), expected(mode));
  }

  /** Parse every group line and check it against the per-region expectation; every region reports. */
  private static void assertAverages(final String out, final long[][] expected) {
    final boolean[] seen = new boolean[REGIONS];
    final Matcher m = AVG_LINE.matcher(out);
    int lines = 0;
    while (m.find()) {
      lines++;
      final int r = Integer.parseInt(m.group(1));
      assertTrue(r >= 0 && r < REGIONS && !seen[r], "region " + r + " reported once: " + out);
      seen[r] = true;
      final long[] e = expected[r];
      assertEquals(e[0], Long.parseLong(m.group(3)), "count of region " + r);
      assertEquals((double) e[1] / e[0], Double.parseDouble(m.group(2)), 1e-9, "avg length of region " + r);
    }
    assertEquals(REGIONS, lines, "one group per region: " + out);
  }

  private static void assertExtrema(final String out, final long[][] expected) {
    final boolean[] seen = new boolean[REGIONS];
    final Matcher m = EXTREMA_LINE.matcher(out);
    int lines = 0;
    while (m.find()) {
      lines++;
      final int r = Integer.parseInt(m.group(1));
      assertTrue(r >= 0 && r < REGIONS && !seen[r], "region " + r + " reported once: " + out);
      seen[r] = true;
      final long[] e = expected[r];
      assertEquals(e[2], Long.parseLong(m.group(2)), "min length of region " + r);
      assertEquals(e[3], Long.parseLong(m.group(3)), "max length of region " + r);
      assertEquals(e[0], Long.parseLong(m.group(4)), "count of region " + r);
    }
    assertEquals(REGIONS, lines, "one group per region: " + out);
  }

  /**
   * The oracle that needs no knowledge of where the loader cut the segments: every segment's memoised
   * table maps each of ITS ids to the length of ITS value, read back through the dictionary itself.
   */
  private void assertMemoisedTablesMatchTheDictionaries(final byte mode) throws Exception {
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(dbFile().toPath());
        JsonResourceSession session = db.beginResourceSession(RES);
        JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
      final ProjectionIndexRegistry.Handle handle = handle(session);
      final int col = handle.columnOf("name");
      final int segments = handle.segmentDictionarySegmentCount();
      for (int segment = 0; segment < segments; segment++) {
        final long headerKey = handle.segmentDictionaryHeaderKey(segment, col);
        assertTrue(headerKey > 0L, "segment " + segment + " holds a name dictionary");
        final int[] table = handle.stringLengthTable(headerKey, mode);
        assertNotNull(table, "segment " + segment + " has a memoised table for mode " + mode);
        final GlobalValueDictionary.ReadView view =
            GlobalValueDictionary.readView(headerKey, rtx.getStorageEngineReader());
        assertNotNull(view, "segment " + segment + " dictionary is readable");
        assertEquals(view.entryCount() + 1, table.length, "table of segment " + segment + " covers its ids");
        for (int id = 1; id <= view.entryCount(); id++) {
          final String value = view.valueAsString(id);
          assertEquals(lengthOf(value, mode), table[id],
              "segment " + segment + " id " + id + " (" + value + ") in mode " + mode);
        }
      }
    }
  }

  private int segmentCount() throws Exception {
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(dbFile().toPath());
        JsonResourceSession session = db.beginResourceSession(RES)) {
      return assertSegmentFixture(session).segmentDictionarySegmentCount();
    }
  }

  /**
   * The handle the EXECUTOR serves from: the catalog caches handles by resource key, and the executor
   * keys by the resource's path, not its name — a lookup by name would return a second handle with an
   * empty memo and prove nothing about the tables the queries used.
   */
  private static ProjectionIndexRegistry.Handle handle(final JsonResourceSession session) {
    final String registryKey = session.getResourceConfig().getResource().toString();
    final ProjectionIndexRegistry.Handle handle = ProjectionIndexCatalog.lookupCovering(session, registryKey,
        session.getMostRecentRevisionNumber(), new String[] {"[]"}, new String[] {"region", "name", "amount"});
    assertNotNull(handle, "the covering projection handle");
    return handle;
  }

  /** The precondition: the name column is segment-scoped and the corpus closed several segments. */
  private static ProjectionIndexRegistry.Handle assertSegmentFixture(final JsonResourceSession session) {
    final ProjectionIndexRegistry.Handle handle = handle(session);
    final int col = handle.columnOf("name");
    assertEquals(ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SEGMENT, handle.columnKindOf(col),
        "name must be a SEGMENT-scoped column, or the lever under test is never reached");
    final int segments = handle.segmentDictionarySegmentCount();
    assertTrue(segments >= 2, "the span cap must close several segments, got " + segments);
    for (int segment = 0; segment < segments; segment++) {
      assertTrue(handle.segmentDictionaryHeaderKey(segment, col) > 0L, "segment " + segment + " has a dictionary");
    }
    assertNotNull(handle.columnStoreOrNull(), "column store");
    assertTrue(handle.columnStoreOrNull().leafCount() >= MIN_LEAVES,
        "at least " + MIN_LEAVES + " leaves, got " + handle.columnStoreOrNull().leafCount());
    return handle;
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
