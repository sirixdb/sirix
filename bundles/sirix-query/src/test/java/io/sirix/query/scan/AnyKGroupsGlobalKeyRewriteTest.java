package io.sirix.query.scan;

import io.brackit.query.Query;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.util.serialize.StringSerializer;
import io.sirix.access.Databases;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.projection.ProjectionColumnScan;
import io.sirix.index.projection.ProjectionIndexBuilder;
import io.sirix.index.projection.ProjectionIndexCatalog;
import io.sirix.index.projection.ProjectionIndexRegistry;
import io.sirix.index.projection.ProjectionIndexRowGroupPage;
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
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The any-k group selection and leaf pruning over a
 * {@link ProjectionIndexRowGroupPage#COLUMN_KIND_STRING_GLOBAL} key. When SearchPhrase became a
 * global-dictionary column at 100M, the any-k planner declined it ("has no leaf evidence") and the
 * predicate scan gave the column no leaf pruning at all: both priced only STRING_DICT (fingerprints)
 * and NUMERIC_LONG (zones), although a global column's zones ARE id bounds and an id equality is
 * containment — no value order needed. The fixture is {@link AnyKGroupsRewriteTest}'s shape built
 * with {@code -Dsirix.projection.globalDict=always} so every string column is global: 64 regions ×
 * 2,048 rows — exactly TWO 1,024-row leaves per region — four tags round-robin (512 rows per
 * (region, tag) group), plus a {@code label} column that names its region ("L7"). Every leaf's
 * label zone therefore COLLAPSES onto one id, which is what gives the {@code !=} rule a positive
 * witness, and a label's rows live on exactly two adjacent leaves — the shape a global key must be
 * priced on.
 *
 * <p>
 * Each witness first asserts the PRECONDITION (the columns really are global — the build reports
 * two global dictionaries) and then that the lever ENGAGED (rewrite counter, prune counter), so a
 * fixture that silently fell back to leaf dictionaries cannot pass by answering correctly.
 */
public final class AnyKGroupsGlobalKeyRewriteTest {

  private static final int REGIONS = 64;
  private static final int ROWS_PER_REGION = 2_048;
  private static final int LEAF_ROWS = 1_024;
  private static final int LEAVES = REGIONS * ROWS_PER_REGION / LEAF_ROWS;
  private static final String[] TAGS = {"alpha", "beta", "gamma", "delta"};
  private static final String DB = "anyk-global-db";
  private static final String RES = "records.jn";
  private static final String SRC = "jn:doc('" + DB + "','" + RES + "')[]";
  private static final String MODE_PROPERTY = "sirix.projection.globalDict";

  private static final Pattern GROUP_LINE = Pattern.compile("\"r\":(-?\\d+),\"t\":\"([^\"]*)\",\"c\":(\\d+)");
  private static final Pattern LABEL_LINE = Pattern.compile("\"l\":\"L(\\d+)\",\"c\":(\\d+)");
  private static final Pattern TAG_LINE = Pattern.compile("\"t\":\"([^\"]*)\",\"c\":(\\d+)");

  private Path dbDir;
  private String priorMode;

  @BeforeEach
  void setUp() throws Exception {
    priorMode = System.getProperty(MODE_PROPERTY);
    System.setProperty(MODE_PROPERTY, "always");
    dbDir = Files.createTempDirectory("sirix-anyk-global-");
    final int n = REGIONS * ROWS_PER_REGION;
    final StringBuilder sb = new StringBuilder(n * 64);
    sb.append('[');
    for (int i = 0; i < n; i++) {
      if (i > 0) {
        sb.append(',');
      }
      final int region = i / ROWS_PER_REGION;
      sb.append("{\"region\":").append(region)
          .append(",\"tag\":\"").append(TAGS[i % TAGS.length])
          .append("\",\"label\":\"L").append(region)
          .append("\",\"amount\":").append(i % 13).append('}');
    }
    sb.append(']');
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain, "jn:store('" + DB + "','" + RES + "','" + sb + "')").evaluate(ctx);
      new Query(chain, """
          let $doc := jn:doc('%s','%s')
          let $stats := jn:create-projection-index($doc, '/[]',
            ('/[]/region', '/[]/tag', '/[]/label', '/[]/amount'), ('long', 'string', 'string', 'long'))
          return sdb:commit($doc)
          """.formatted(DB, RES)).evaluate(ctx);
    }
    ProjectionIndexCatalog.clearCache();
    ProjectionIndexRegistry.clear();
    // The precondition of every witness below: the build produced GLOBAL columns for tag and label.
    assertEquals(2, ProjectionIndexBuilder.globalDictionaryColumnsBuilt(),
        "the fixture must encode both string columns with a global dictionary");
  }

  @AfterEach
  void tearDown() {
    if (priorMode == null) {
      System.clearProperty(MODE_PROPERTY);
    } else {
      System.setProperty(MODE_PROPERTY, priorMode);
    }
    ProjectionIndexCatalog.clearCache();
    ProjectionIndexRegistry.clear();
    if (dbDir != null) {
      Databases.removeDatabase(dbDir);
    }
  }

  @Test
  void numericAndGlobalStringKeysAreRewrittenWithExactAggregates() throws Exception {
    final long rewrittenBefore = SirixVectorizedExecutor.anyKGroupsRewriteCount();
    final long servedBefore = SirixVectorizedExecutor.groupAggServedCount();
    final long prunedBefore = ProjectionColumnScan.treeLeavesPrunedCount();
    final String out = run("subsequence(for $u in " + SRC + " let $r := $u.region let $t := $u.tag "
        + "group by $r, $t return {\"r\": $r, \"t\": $t, \"c\": count($u)}, 1, 10)");
    assertEquals(rewrittenBefore + 1, SirixVectorizedExecutor.anyKGroupsRewriteCount(),
        "the any-k rewrite must ENGAGE with a global-string key: " + out);
    assertTrue(SirixVectorizedExecutor.groupAggServedCount() > servedBefore, "the rewritten request was served");
    // Ten groups over four tags touch three regions = six leaves of 128; the pass must skip the rest.
    final long pruned = ProjectionColumnScan.treeLeavesPrunedCount() - prunedBefore;
    assertTrue(pruned >= LEAVES - 6, "the rewritten pass must prune the leaves the chosen groups cannot touch: pruned=" + pruned);
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
  void aSingleGlobalStringKeyIsPricedByItsIdZones() throws Exception {
    final long rewrittenBefore = SirixVectorizedExecutor.anyKGroupsRewriteCount();
    final long prunedBefore = ProjectionColumnScan.treeLeavesPrunedCount();
    // The sample is eight leaves = four labels; k must stay below that or the planner (rightly)
    // declines for want of candidates.
    final String out = run("subsequence(for $u in " + SRC + " let $l := $u.label "
        + "group by $l return {\"l\": $l, \"c\": count($u)}, 1, 3)");
    assertEquals(rewrittenBefore + 1, SirixVectorizedExecutor.anyKGroupsRewriteCount(),
        "a lone global-string key must be priced by zone stabbing on its ids: " + out);
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
  void anEqualityTreeOverAGlobalStringColumnPrunesLeavesBeforeTheyAreRead() throws Exception {
    final long prunedBefore = ProjectionColumnScan.treeLeavesPrunedCount();
    // An OR makes the WHERE a predicate tree, whose keep mask is the store-evidence prune of each
    // leaf predicate: `amount > 1000` never holds (amount < 13) and its zones drop every leaf, so the
    // union keeps exactly what the id equality keeps — L7's leaves 14 and 15; every other label zone
    // excludes L7's id.
    final String out = run("for $u in " + SRC + " where $u.label = \"L7\" or $u.amount > 1000 let $t := $u.tag "
        + "group by $t return {\"t\": $t, \"c\": count($u)}");
    assertTagGroups(out, ROWS_PER_REGION / TAGS.length);
    assertEquals(LEAVES - 2, ProjectionColumnScan.treeLeavesPrunedCount() - prunedBefore,
        "an id equality over a global column must prune on the descriptor zones");
  }

  @Test
  void anInequalityOverAGlobalStringColumnPrunesOnlyCollapsedZones() throws Exception {
    final long prunedBefore = ProjectionColumnScan.treeLeavesPrunedCount();
    // `amount > 1000` never holds (amount < 13), so the OR keeps exactly what `label != "L0"` keeps:
    // the two leaves whose zone collapses onto L0's id are the only ones the NE rule may drop.
    final String out = run("for $u in " + SRC + " where $u.label != \"L0\" or $u.amount > 1000 let $t := $u.tag "
        + "group by $t return {\"t\": $t, \"c\": count($u)}");
    assertTagGroups(out, (REGIONS - 1) * ROWS_PER_REGION / TAGS.length);
    assertEquals(2L, ProjectionColumnScan.treeLeavesPrunedCount() - prunedBefore,
        "a NE over a global column prunes exactly the zones collapsed onto the literal's id");
  }

  @Test
  void anInequalityNeverPrunesAZoneThatMerelyStartsAtTheLiteral() throws Exception {
    final long prunedBefore = ProjectionColumnScan.treeLeavesPrunedCount();
    // Every leaf holds all four tags, so every tag zone STARTS at the smallest id but is not
    // collapsed: `!= "alpha"` may drop nothing (a rule that tests min alone would drop every leaf).
    final String out = run("for $u in " + SRC + " where $u.tag != \"alpha\" or $u.amount > 1000 let $l := $u.label "
        + "group by $l return {\"l\": $l, \"c\": count($u)}");
    final Matcher m = LABEL_LINE.matcher(out);
    int lines = 0;
    while (m.find()) {
      lines++;
      assertEquals(ROWS_PER_REGION - ROWS_PER_REGION / TAGS.length, Long.parseLong(m.group(2)),
          "non-alpha rows of L" + m.group(1));
    }
    assertEquals(REGIONS, lines, out);
    assertEquals(0L, ProjectionColumnScan.treeLeavesPrunedCount() - prunedBefore,
        "a NE must not prune a zone that holds other ids beside the literal's");
  }

  private static void assertTagGroups(final String out, final long expectedCount) {
    final Matcher m = TAG_LINE.matcher(out);
    final Set<String> seen = new HashSet<>();
    final Set<String> tags = Set.of(TAGS);
    int lines = 0;
    while (m.find()) {
      lines++;
      assertTrue(tags.contains(m.group(1)), "unknown tag: " + m.group(1));
      assertEquals(expectedCount, Long.parseLong(m.group(2)), "count of " + m.group(1));
      assertTrue(seen.add(m.group(1)), "duplicate tag group " + m.group(1));
    }
    assertEquals(TAGS.length, lines, out);
  }

  private String run(final String query) throws Exception {
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      SirixVectorizedExecutor exec = null;
      try {
        final var db = Databases.openJsonDatabase(dbDir.resolve(DB));
        final JsonResourceSession session = db.beginResourceSession(RES);
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
