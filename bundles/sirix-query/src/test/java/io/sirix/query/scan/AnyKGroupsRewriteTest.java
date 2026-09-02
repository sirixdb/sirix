package io.sirix.query.scan;

import io.brackit.query.Query;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.util.serialize.StringSerializer;
import io.sirix.access.Databases;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.projection.ProjectionColumnScan;
import io.sirix.index.projection.ProjectionIndexCatalog;
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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Witness for the any-k group selection: {@code GROUP BY keys LIMIT k} without ORDER BY may answer
 * with ANY k groups, so the executor picks k groups whose rows live on the fewest leaves (bloom +
 * zone evidence over a sample), rewrites the request as a predicate on exactly those groups and
 * aggregates only the leaves that can hold them. The witness asserts the lever ENGAGED (the rewrite
 * counter moved), that the answer is exactly k distinct groups, and that every aggregate equals the
 * generator's oracle — a wrong predicate leaf (wrong literal, wrong operator, aliased mask) would
 * produce a group that does not exist or a count that is not 200.
 *
 * <p>
 * The fixture: 64 regions × 1,000 rows, five tags round-robin per region, so each (region, tag)
 * group has exactly 200 rows on one or two adjacent leaves (leaves hold 1,024 rows), while every
 * tag alone lives on every leaf — the union gate must therefore refuse a single-key {@code tag}
 * request, and a {@code LIMIT} larger than the number of groups must decline to the full pass.
 */
public final class AnyKGroupsRewriteTest {

  private static final int REGIONS = 64;
  private static final int ROWS_PER_REGION = 1_000;
  private static final String[] TAGS = {"alpha", "beta", "gamma", "delta", "epsilon"};
  private static final String DB = "anyk-gb-db";
  private static final String RES = "records.jn";
  private static final String SRC = "jn:doc('" + DB + "','" + RES + "')[]";

  private static final Pattern GROUP_LINE = Pattern.compile("\"r\":(-?\\d+),\"t\":\"([^\"]*)\",\"c\":(\\d+)");
  private static final Pattern TAG_LINE = Pattern.compile("\"t\":\"([^\"]*)\",\"c\":(\\d+)");

  private Path dbDir;

  @BeforeEach
  void setUp() throws Exception {
    dbDir = Files.createTempDirectory("sirix-anyk-gb-");
    final int n = REGIONS * ROWS_PER_REGION;
    final StringBuilder sb = new StringBuilder(n * 48);
    sb.append('[');
    for (int i = 0; i < n; i++) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append("{\"region\":").append(i / ROWS_PER_REGION)
          .append(",\"tag\":\"").append(TAGS[i % TAGS.length])
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
            ('/[]/region', '/[]/tag', '/[]/amount'), ('long', 'string', 'long'))
          return sdb:commit($doc)
          """.formatted(DB, RES)).evaluate(ctx);
    }
    ProjectionIndexCatalog.clearCache();
  }

  @AfterEach
  void tearDown() {
    ProjectionIndexCatalog.clearCache();
    if (dbDir != null) {
      Databases.removeDatabase(dbDir);
    }
  }

  @Test
  void twoKeyLimitIsServedByTheRewriteWithExactAggregates() throws Exception {
    final long rewrittenBefore = SirixVectorizedExecutor.anyKGroupsRewriteCount();
    final long servedBefore = SirixVectorizedExecutor.groupAggServedCount();
    final long prunedBefore = ProjectionColumnScan.treeLeavesPrunedCount();
    final String out = run("subsequence(for $u in " + SRC + " let $r := $u.region let $t := $u.tag "
        + "group by $r, $t return {\"r\": $r, \"t\": $t, \"c\": count($u)}, 1, 10)");
    assertEquals(rewrittenBefore + 1, SirixVectorizedExecutor.anyKGroupsRewriteCount(),
        "the any-k rewrite must ENGAGE on a two-key LIMIT without ORDER BY: " + out);
    assertTrue(SirixVectorizedExecutor.groupAggServedCount() > servedBefore, "the rewritten request was served");
    // The point of the rewrite is the pass it saves: the ten chosen groups live on leaves 0 and 1,
    // so the rewritten pass must have pruned (almost) every other leaf of the 63. A predicate that
    // admits more rows than the chosen groups still answers correctly — and prunes nothing.
    final long pruned = ProjectionColumnScan.treeLeavesPrunedCount() - prunedBefore;
    assertTrue(pruned >= 55, "the rewritten pass must prune the leaves the chosen groups cannot touch: pruned=" + pruned);
    final Matcher m = GROUP_LINE.matcher(out);
    final Set<String> groups = new HashSet<>();
    final Set<String> tags = Set.of(TAGS);
    int lines = 0;
    while (m.find()) {
      lines++;
      final int region = Integer.parseInt(m.group(1));
      final String tag = m.group(2);
      final long count = Long.parseLong(m.group(3));
      assertTrue(region >= 0 && region < REGIONS, "region out of range: " + region);
      assertTrue(tags.contains(tag), "unknown tag: " + tag);
      assertEquals(ROWS_PER_REGION / TAGS.length, count, "count of (" + region + ", " + tag + ")");
      assertTrue(groups.add(region + "/" + tag), "duplicate group: " + region + "/" + tag);
    }
    assertEquals(10, lines, "exactly k groups: " + out);
    // The planner's choice is the k CHEAPEST groups, ties by first appearance: region 0 lives on
    // leaf 0 alone (five groups at one leaf), region 1 straddles leaves 0 and 1 and is seen next.
    // A predicate that admits more rows than the chosen groups (a wrong operator, say) still
    // answers with k exact groups — but not these.
    final Set<String> expected = new HashSet<>();
    for (final String tag : TAGS) {
      expected.add("0/" + tag);
      expected.add("1/" + tag);
    }
    assertEquals(expected, groups, "the served groups are the ten cheapest by leaf evidence");
  }

  @Test
  void keysThatLiveOnEveryLeafRefuseTheRewrite() throws Exception {
    // Every leaf holds every tag: the union of any three groups' leaves is the whole index and the
    // rewrite would only add a pass — the gate must refuse and the full pass serve exactly 3 groups.
    final long rewrittenBefore = SirixVectorizedExecutor.anyKGroupsRewriteCount();
    final String out = run("subsequence(for $u in " + SRC + " let $t := $u.tag group by $t "
        + "return {\"t\": $t, \"c\": count($u)}, 1, 3)");
    assertEquals(rewrittenBefore, SirixVectorizedExecutor.anyKGroupsRewriteCount(),
        "the union gate must refuse groups that cover every leaf: " + out);
    assertTagGroups(out, 3);
  }

  @Test
  void fewerGroupsThanTheLimitDeclinesToTheFullPass() throws Exception {
    // Five tags, LIMIT 10: the sample cannot name ten groups, so the rewrite declines and the full
    // pass answers with all five.
    final long rewrittenBefore = SirixVectorizedExecutor.anyKGroupsRewriteCount();
    final String out = run("subsequence(for $u in " + SRC + " let $t := $u.tag group by $t "
        + "return {\"t\": $t, \"c\": count($u)}, 1, 10)");
    assertEquals(rewrittenBefore, SirixVectorizedExecutor.anyKGroupsRewriteCount(),
        "fewer candidates than k must decline the rewrite: " + out);
    assertTagGroups(out, TAGS.length);
  }

  @Test
  void orderedLimitNeverRewrites() throws Exception {
    // ORDER BY pins WHICH groups the window holds — the any-k selection is only legal without it.
    final long rewrittenBefore = SirixVectorizedExecutor.anyKGroupsRewriteCount();
    final String out = run("subsequence(for $u in " + SRC + " let $r := $u.region let $t := $u.tag "
        + "group by $r, $t let $c := count($u) order by $c descending, $r, $t "
        + "return {\"r\": $r, \"t\": $t, \"c\": $c}, 1, 10)");
    assertEquals(rewrittenBefore, SirixVectorizedExecutor.anyKGroupsRewriteCount(),
        "an ORDER BY window must never take the any-k route: " + out);
    final Matcher m = GROUP_LINE.matcher(out);
    final List<String> seen = new ArrayList<>();
    while (m.find()) {
      assertEquals(ROWS_PER_REGION / TAGS.length, Long.parseLong(m.group(3)));
      seen.add(m.group(1) + "/" + m.group(2));
    }
    assertEquals(10, seen.size(), out);
    // Ties on the count resolve by (region, tag) ascending: regions 0 and 1, tags alphabetical.
    assertEquals(List.of("0/alpha", "0/beta", "0/delta", "0/epsilon", "0/gamma",
        "1/alpha", "1/beta", "1/delta", "1/epsilon", "1/gamma"), seen);
  }

  private static void assertTagGroups(final String out, final int expectedGroups) {
    final Matcher m = TAG_LINE.matcher(out);
    final Set<String> seen = new HashSet<>();
    final Set<String> tags = Set.of(TAGS);
    int lines = 0;
    while (m.find()) {
      lines++;
      assertTrue(tags.contains(m.group(1)), "unknown tag: " + m.group(1));
      assertEquals(REGIONS * ROWS_PER_REGION / TAGS.length, Long.parseLong(m.group(2)), "count of " + m.group(1));
      assertTrue(seen.add(m.group(1)), "duplicate tag group " + m.group(1));
    }
    assertEquals(expectedGroups, lines, out);
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
