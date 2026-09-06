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
import io.sirix.index.projection.ProjectionColumnStore;
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The any-k group selection and leaf pruning over a
 * {@link ProjectionIndexRowGroupPage#COLUMN_KIND_STRING_SEGMENT} key — the shape every string column
 * takes under the segment dictionary lane, which is the lane the 100M database is built with. At
 * 100M the any-k planner declined {@code GROUP BY UserID, SearchPhrase LIMIT 10} ("key SearchPhrase
 * … kind=8 has no leaf evidence") and read the whole column, ~10 s against a board best of
 * milliseconds; and a segment-scoped equality skipped leaves only AFTER their slices were fetched.
 * A segment column's zones ARE packed-cell bounds, a leaf never straddles a segment, and a cell of
 * another segment stabs nothing there — so a cell equality is containment exactly as a global id's
 * is, and the planner can price a segment key the way it prices a global one.
 *
 * <p>
 * The fixture is {@link AnyKGroupsGlobalKeyRewriteTest}'s shape, loaded through the real parallel
 * bulk import with the lane armed (the lane mints as document pages encode; there is no other way to
 * build it): 64 regions × 2,048 rows — exactly TWO 1,024-row leaves per region — four tags
 * round-robin (512 rows per (region, tag) group), plus a {@code label} column that names its region
 * ("L7"). Every leaf's label zone COLLAPSES onto one cell, which gives the {@code !=} rule a positive
 * witness. The corpus is small enough for the default segment boundaries to seal ONE segment, so a
 * leaf's cells are one dictionary's ids and the leaf arithmetic below is exact.
 *
 * <p>
 * Each witness first asserts the PRECONDITION (the columns really are segment-scoped, the store holds
 * the expected leaves) and then that the lever ENGAGED (rewrite counter, prune counter), so a fixture
 * that silently fell back to another column kind cannot pass by answering correctly.
 */
public final class AnyKGroupsSegmentKeyRewriteTest {

  private static final int REGIONS = 64;
  private static final int ROWS_PER_REGION = 2_048;
  private static final int LEAF_ROWS = 1_024;
  private static final int LEAVES = REGIONS * ROWS_PER_REGION / LEAF_ROWS;
  private static final int RECORDS = REGIONS * ROWS_PER_REGION;
  private static final String[] TAGS = {"alpha", "beta", "gamma", "delta"};
  private static final String DB = "anyk-segment-db";
  private static final String RES = "records.jn";
  private static final String SRC = "jn:doc('" + DB + "','" + RES + "')[]";
  private static final String GLOBAL_MODE_PROPERTY = "sirix.projection.globalDict";

  private static final Pattern GROUP_LINE = Pattern.compile("\"r\":(-?\\d+),\"t\":\"([^\"]*)\",\"c\":(\\d+)");
  private static final Pattern LABEL_LINE = Pattern.compile("\"l\":\"L(\\d+)\",\"c\":(\\d+)");
  private static final Pattern TAG_LINE = Pattern.compile("\"t\":\"([^\"]*)\",\"c\":(\\d+)");

  private File location;
  private String priorLane;
  private String priorGlobalMode;
  private boolean priorChunked;
  private int priorChunkTarget;

  @BeforeEach
  void setUp() throws Exception {
    priorLane = System.getProperty(SegmentDictionaryLane.ENABLED_PROPERTY);
    priorGlobalMode = System.getProperty(GLOBAL_MODE_PROPERTY);
    System.setProperty(SegmentDictionaryLane.ENABLED_PROPERTY, "true");
    // The projection's own global column dictionary is not under test; with it out of the way every
    // string column of a leaf is converted to the segment kind.
    System.setProperty(GLOBAL_MODE_PROPERTY, "never");
    // A converted page is readable only on the lazy route, which needs a chunk-framed body; the lane
    // refuses to arm without one.
    priorChunked = ChunkedBodyConfig.setEnabledForTesting(true);
    priorChunkTarget = ChunkedBodyConfig.setTargetChunkBytesForTesting(4096);
    ProjectionBulkLoad.clearActive();
    ProjectionIndexCatalog.clearCache();
    ProjectionIndexRegistry.clear();

    location = Files.createTempDirectory("sirix-anyk-segment-").toFile();
    Databases.createJsonDatabase(new DatabaseConfiguration(dbFile().toPath()));
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(dbFile().toPath())) {
      db.createResource(ResourceConfiguration.newBuilder(RES)
                                             .useDeweyIDs(false)
                                             .hashKind(HashType.NONE)
                                             .storeNodeHistory(false)
                                             .buildPathSummary(true)
                                             // A page whose values are dictionary ids expands only on
                                             // the lazy route; FULL keeps one complete page per
                                             // revision, so there is no eager fragment combine.
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
        Path.parse("/[]/tag", PathParser.Type.JSON), Path.parse("/[]/label", PathParser.Type.JSON),
        Path.parse("/[]/amount", PathParser.Type.JSON));
    return IndexDefs.createProjectionIdxDef(Path.parse("/[]", PathParser.Type.JSON), fieldPaths,
        List.of(Type.LON, Type.STR, Type.STR, Type.LON), 0, IndexDef.DbType.JSON);
  }

  private static byte[] corpus() {
    final StringBuilder sb = new StringBuilder(RECORDS * 64);
    sb.append('[');
    for (int i = 0; i < RECORDS; i++) {
      if (i > 0) {
        sb.append(',');
      }
      final int region = i / ROWS_PER_REGION;
      sb.append("{\"region\":")
        .append(region)
        .append(",\"tag\":\"")
        .append(TAGS[i % TAGS.length])
        .append("\",\"label\":\"L")
        .append(region)
        .append("\",\"amount\":")
        .append(i % 13)
        .append('}');
    }
    sb.append(']');
    return sb.toString().getBytes(StandardCharsets.UTF_8);
  }

  /**
   * The precondition of every witness: the string columns are SEGMENT-scoped (not global, not
   * per-leaf), the revision sealed at least one segment dictionary, and the store holds exactly the
   * leaves the arithmetic below assumes.
   */
  private static void assertSegmentFixture(final JsonResourceSession session) {
    final ProjectionIndexRegistry.Handle handle = ProjectionIndexCatalog.lookupCovering(session,
        session.getResourceConfig().getResource().toString(), session.getMostRecentRevisionNumber(),
        new String[] {"[]"}, new String[] {"region", "tag", "label", "amount"});
    assertNotNull(handle, "the projection index covers the four fields");
    for (final String column : new String[] {"tag", "label"}) {
      assertEquals(ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SEGMENT, handle.columnKindOf(handle.columnOf(column)),
          "the lane must have converted " + column + " to the segment kind");
    }
    assertTrue(handle.segmentDictionarySegmentCount() >= 1, "the revision sealed a segment dictionary");
    final ProjectionColumnStore store = handle.columnStoreOrNull();
    assertNotNull(store, "the column store is open");
    assertEquals(LEAVES, store.leafCount(), "two 1,024-row leaves per region");
  }

  @Test
  void numericAndSegmentStringKeysAreRewrittenWithExactAggregates() throws Exception {
    final long rewrittenBefore = SirixVectorizedExecutor.anyKGroupsRewriteCount();
    final long servedBefore = SirixVectorizedExecutor.groupAggServedCount();
    final long prunedBefore = ProjectionColumnScan.treeLeavesPrunedCount();
    final String out = run("subsequence(for $u in " + SRC + " let $r := $u.region let $t := $u.tag "
        + "group by $r, $t return {\"r\": $r, \"t\": $t, \"c\": count($u)}, 1, 10)");
    assertEquals(rewrittenBefore + 1, SirixVectorizedExecutor.anyKGroupsRewriteCount(),
        "the any-k rewrite must ENGAGE with a segment-string key: " + out);
    assertTrue(SirixVectorizedExecutor.groupAggServedCount() > servedBefore, "the rewritten request was served");
    // Ten groups over four tags touch three regions = six leaves of 128; the pass must skip the rest.
    final long pruned = ProjectionColumnScan.treeLeavesPrunedCount() - prunedBefore;
    assertTrue(pruned >= LEAVES - 6,
        "the rewritten pass must prune the leaves the chosen groups cannot touch: pruned=" + pruned);
    final Matcher m = GROUP_LINE.matcher(out);
    final Set<String> groups = new HashSet<>();
    final Set<String> tags = Set.of(TAGS);
    int lines = 0;
    while (m.find()) {
      lines++;
      final int region = Integer.parseInt(m.group(1));
      final String tag = m.group(2);
      assertTrue(region >= 0 && region < REGIONS, "region out of range: " + region);
      assertTrue(tags.contains(tag), "unknown tag: " + tag);
      assertEquals(ROWS_PER_REGION / TAGS.length, Long.parseLong(m.group(3)), "count of (" + region + ", " + tag + ")");
      assertTrue(groups.add(region + "/" + tag), "duplicate group: " + region + "/" + tag);
    }
    assertEquals(10, lines, "exactly k groups: " + out);
    // Every group costs exactly two leaves, so the tie-break is first appearance: regions 0 and 1
    // in full, then the first two tags of region 2 — never a region the sample did not see.
    for (final String group : groups) {
      final int region = Integer.parseInt(group.substring(0, group.indexOf('/')));
      assertTrue(region <= 2, "a chosen group must come from the sampled leading leaves: " + group);
    }
  }

  @Test
  void aSingleSegmentStringKeyIsPricedByItsCellZones() throws Exception {
    final long rewrittenBefore = SirixVectorizedExecutor.anyKGroupsRewriteCount();
    final long prunedBefore = ProjectionColumnScan.treeLeavesPrunedCount();
    // The sample is eight leaves = four labels; k must stay below that or the planner (rightly)
    // declines for want of candidates.
    final String out = run("subsequence(for $u in " + SRC + " let $l := $u.label "
        + "group by $l return {\"l\": $l, \"c\": count($u)}, 1, 3)");
    assertEquals(rewrittenBefore + 1, SirixVectorizedExecutor.anyKGroupsRewriteCount(),
        "a lone segment-string key must be priced by zone stabbing on its cells: " + out);
    // Three labels cover six leaves of 128: the pass must skip the rest.
    final long pruned = ProjectionColumnScan.treeLeavesPrunedCount() - prunedBefore;
    assertEquals(LEAVES - 6, pruned, "the rewritten pass must prune every leaf the chosen labels cannot touch");
    final Matcher m = LABEL_LINE.matcher(out);
    final Set<Integer> labels = new HashSet<>();
    int lines = 0;
    while (m.find()) {
      lines++;
      final int region = Integer.parseInt(m.group(1));
      assertTrue(region >= 0 && region < REGIONS, "label out of range: L" + region);
      assertEquals(ROWS_PER_REGION, Long.parseLong(m.group(2)), "count of L" + region);
      assertTrue(labels.add(region), "duplicate group: L" + region);
    }
    assertEquals(3, lines, "exactly k groups: " + out);
    // Every label costs two leaves; the planner must have chosen only labels it sampled (the first
    // eight leaves), never invented one.
    for (final int region : labels) {
      assertTrue(region <= 3, "a chosen label must come from the sampled leading leaves: L" + region);
    }
  }

  @Test
  void anEqualityTreeOverASegmentStringColumnPrunesLeavesBeforeTheyAreRead() throws Exception {
    final long prunedBefore = ProjectionColumnScan.treeLeavesPrunedCount();
    // An OR makes the WHERE a predicate tree, whose keep mask is the store-evidence prune of each
    // leaf predicate: `amount > 1000` never holds (amount < 13) and its zones drop every leaf, so the
    // union keeps exactly what the cell equality keeps — L7's two leaves; every other label zone
    // excludes L7's cell.
    final String out = run("for $u in " + SRC + " where $u.label = \"L7\" or $u.amount > 1000 "
        + "let $t := $u.tag group by $t return {\"t\": $t, \"c\": count($u)}");
    final long pruned = ProjectionColumnScan.treeLeavesPrunedCount() - prunedBefore;
    assertEquals(LEAVES - 2, pruned, "the cell equality must prune every leaf but L7's two from the descriptors");
    final Matcher m = TAG_LINE.matcher(out);
    int lines = 0;
    while (m.find()) {
      lines++;
      assertEquals(ROWS_PER_REGION / TAGS.length, Long.parseLong(m.group(2)), "count of " + m.group(1));
    }
    assertEquals(TAGS.length, lines, "one group per tag: " + out);
  }

  @Test
  void anInequalityTreeOverASegmentStringColumnPrunesCollapsedZones() throws Exception {
    final long prunedBefore = ProjectionColumnScan.treeLeavesPrunedCount();
    // A leaf whose label zone collapsed onto L7's cell holds nothing else: `label != "L7"` can keep
    // no row there and the zone alone proves it — exactly L7's two leaves are dropped.
    final String out = run("for $u in " + SRC + " where $u.label != \"L7\" or $u.amount > 1000 "
        + "let $t := $u.tag group by $t return {\"t\": $t, \"c\": count($u)}");
    final long pruned = ProjectionColumnScan.treeLeavesPrunedCount() - prunedBefore;
    assertEquals(2, pruned, "NE prunes exactly the zones collapsed onto the literal's cell");
    final Matcher m = TAG_LINE.matcher(out);
    int lines = 0;
    while (m.find()) {
      lines++;
      assertEquals((REGIONS - 1) * ROWS_PER_REGION / TAGS.length, Long.parseLong(m.group(2)),
          "count of " + m.group(1) + " without L7");
    }
    assertEquals(TAGS.length, lines, "one group per tag: " + out);
  }

  @Test
  void anInequalityNeverPrunesAZoneThatMerelyStartsAtTheLiteral() throws Exception {
    final long prunedBefore = ProjectionColumnScan.treeLeavesPrunedCount();
    // The tag column's four values sit on EVERY leaf, so no tag zone collapses; `tag != "alpha"` has
    // no leaf it can prove empty and must prune nothing — a zone that merely STARTS (or ends) at the
    // literal's cell still holds the other tags.
    final String out = run("for $u in " + SRC + " where $u.tag != \"alpha\" or $u.amount > 1000 "
        + "let $l := $u.label group by $l return {\"l\": $l, \"c\": count($u)}");
    final long pruned = ProjectionColumnScan.treeLeavesPrunedCount() - prunedBefore;
    assertEquals(0, pruned, "NE must not prune a zone that holds other values");
    final Matcher m = LABEL_LINE.matcher(out);
    int lines = 0;
    while (m.find()) {
      lines++;
      assertEquals(ROWS_PER_REGION - ROWS_PER_REGION / TAGS.length, Long.parseLong(m.group(2)),
          "count of L" + m.group(1) + " without alpha");
    }
    assertEquals(REGIONS, lines, "one group per label: " + out);
  }

  private String run(final String query) throws Exception {
    try (BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(location.toPath()).build();
        SirixQueryContext ctx = SirixQueryContext.createWithJsonStore(store);
        SirixCompileChain chain = SirixCompileChain.createWithJsonStore(store)) {
      SirixVectorizedExecutor exec = null;
      try {
        final Database<JsonResourceSession> db = Databases.openJsonDatabase(dbFile().toPath());
        final JsonResourceSession session = db.beginResourceSession(RES);
        // Assert the fixture BEFORE the query runs, so a wrong column kind fails the precondition,
        // not the lever.
        assertSegmentFixture(session);
        exec = new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber());
        SequentialPipelineStrategy.setVectorizedExecutor(exec);
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
