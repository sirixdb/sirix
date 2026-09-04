package io.sirix.query.scan;

import io.brackit.query.Query;
import io.brackit.query.atomic.Int64;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.sirix.access.Databases;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The predicate-tree route must derive each leaf's alignment from that leaf's OWN column.
 *
 * <p>
 * The per-page geometry pass leaves the ANCHOR field's occurrence indices in a shared scratch
 * array, and every leaf derives its record-ordinal alignment lead from whatever is in there. The
 * hand-fused kernel may reuse that for its first leaf, because its leaves are validated
 * one-per-field IN FIELD ORDER with the anchor first. The tree route walks the TREE, and tree order
 * is not field order: the compiler moves the sound anchor to field 0, so
 * {@code not($u.active) and $u.year gt
 * 1950} anchors on {@code year} while {@code active} is the leaf evaluated first.
 *
 * <h2>The corpus is the test</h2> A stale-scratch read usually FAILS the alignment check and the
 * page falls back — harmless. It produces a wrong ANSWER only in one geometry: the page's leading
 * spanning record must carry the anchor but not the other field, and its trailing partial record
 * the other field but not the anchor. Then both fields report the same occurrence count while their
 * leads are 1 and 0, the check over the wrong array verifies cleanly, and every anchor value is
 * read one slot too early.
 *
 * <p>
 * That geometry is arithmetic, not luck. Records here are four fields plus their object node — five
 * slots — against a page of {@code Constants.NDP_NODE_COUNT} slots, and {@code 1024 mod 5 = 4}, so
 * the seam offset advances by four slots per page and cycles through every position within a
 * record. {@code active} is written BEFORE {@code year} so that a page beginning inside a record
 * can hold that record's {@code year} without its {@code active}, while the record opening at the
 * page's tail contributes the opposite pair. Reordering these fields, or adding a fifth, moves the
 * seam off the one position that matters and the test goes quiet.
 *
 * <p>
 * Ground truth is the same query with the column path switched off, so the two arms differ in
 * exactly one variable, and {@link #theRouteIsReached()} keeps the agreement from passing
 * vacuously.
 */
@DisplayName("predicate tree route regressions")
final class TreeRouteRegressionTest {

  private static final int N = 8_000;
  private static final String DB = "tree-route-regression-db";
  private static final String RES = "records.jn";

  /** Anchor NOT first in tree order: another leaf writes the scratch before the anchor reads it. */
  private static final List<String> ANCHOR_LAST_SHAPES = List.of("not($u.active) and $u.year gt 1950",
      "not($u.active) and $u.year gt 1950 and $u.note lt 40", "not($u.active) and $u.year ge 1980",
      "($u.year gt 1950 and $u.note eq 3) or $u.year lt 1910", "not($u.note lt 40) and $u.year ge 1980");

  private Path dbDir;

  @BeforeEach
  void setUp() throws Exception {
    dbDir = Files.createTempDirectory("sirix-tree-route-regression-");
    final StringBuilder sb = new StringBuilder(N * 48);
    sb.append('[');
    for (int i = 0; i < N; i++) {
      if (i > 0) {
        sb.append(',');
      }
      // Four fields, in this order, for the seam arithmetic in the class comment.
      sb.append("{\"active\":")
        .append(i % 3 != 0)
        .append(",\"id\":")
        .append(i)
        .append(",\"note\":")
        .append(i % 97)
        .append(",\"year\":")
        .append(1900 + i % 124)
        .append('}');
    }
    sb.append(']');
    // A PATH SUMMARY IS REQUIRED, and this test is the reason to say so explicitly. Without one,
    // resolveTargetPathNodeKey returns -1, structuralSourcePathMatcher(sourcePath, -1) is non-null,
    // and the executor then forces regionPlan to null on purpose: raw page columns cannot prove
    // exact source ancestry, so the page-only routes fail closed rather than answer from a scope
    // they cannot establish (NoPathSummarySourceScopeDifferentialTest pins that across every
    // VersioningType). An earlier revision of this test disabled the path summary and then asserted
    // the page-only tree route runs — two incompatible demands, which is why theRouteIsReached()
    // reported the route as never reached.
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).buildPathSummary(true).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain, "jn:store('" + DB + "','" + RES + "','" + sb + "')").evaluate(ctx);
    }
  }

  @AfterEach
  void tearDown() {
    SequentialPipelineStrategy.setVectorizedExecutor(null);
    if (dbDir != null) {
      Databases.removeDatabase(dbDir.resolve(DB));
    }
  }

  @Test
  @DisplayName("the anchor leaf aligns against its OWN column, not the leaf evaluated before it")
  void anchorLeafAlignsAgainstItsOwnColumn() throws Exception {
    for (final String predicate : ANCHOR_LAST_SHAPES) {
      assertEquals(count(predicate, false), count(predicate, true),
          "column path disagrees with the record path for: " + predicate
              + " — the anchor leaf took its alignment lead from another field's "
              + "occurrence indices, so its column was read one value too early");
    }
  }

  @Test
  @DisplayName("a warm repeat of the column path still agrees with the record path")
  void aWarmRepeatStillAgreesWithTheRecordPath() throws Exception {
    // A WARM/REPEAT correctness regression, and nothing more than that. Running the column path a
    // second time exercises it against resident and cached state left by the first run, which is a
    // real and distinct failure surface from a cold first scan.
    //
    // It asserts NOTHING about scheduling provenance, because this setup cannot observe any. With a
    // path summary present, planPageScan's persisted PathNode page-key array can serve the scan
    // before the first run ever happens; recordBuffers is then null and PageScanSchedule.publish is
    // a no-op, so there is no "set the first scan published" here to reason about. Registry
    // publication and reuse are covered where they are actually exercised — PageSkipNegativeHashTest
    // drives them through the generic record path with no summary.
    for (final String predicate : ANCHOR_LAST_SHAPES) {
      final long viaRecords = count(predicate, false);
      assertEquals(viaRecords, count(predicate, true), "column path disagrees with the record path for: " + predicate);
      assertEquals(viaRecords, count(predicate, true), "the SECOND (warm) column scan disagrees for: " + predicate
          + " — the repeat read resident or cached state differently from the first");
    }
  }

  @Test
  @DisplayName("the route is actually reached on these shapes, not vacuously correct")
  void theRouteIsReached() throws Exception {
    long reached = 0;
    for (final String predicate : ANCHOR_LAST_SHAPES) {
      SirixVectorizedExecutor.resetRegionOnlyCounters();
      count(predicate, true);
      final long pages = SirixVectorizedExecutor.regionTreePages();
      System.out.println("[tree-route] " + predicate + " -> pages=" + pages);
      reached += pages;
    }
    assertTrue(reached > 0,
        "the predicate-tree route answered no page at all — every assertion in this class " + "is passing vacuously");
  }

  private long count(final String predicate, final boolean regionOnly) throws Exception {
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).buildPathSummary(true).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      final var coll = store.lookup(DB);
      final var resourceSession = coll.getDatabase().beginResourceSession(RES);
      try {
        final var exec = new SirixVectorizedExecutor(resourceSession, resourceSession.getMostRecentRevisionNumber());
        exec.setRegionOnlyCountEnabled(regionOnly);
        SequentialPipelineStrategy.setVectorizedExecutor(exec);
        try {
          return ((Int64) new Query(chain,
              "count(for $u in jn:doc('" + DB + "','" + RES + "')[] where " + predicate + " return $u)").evaluate(
                  ctx)).longValue();
        } finally {
          exec.close();
          SequentialPipelineStrategy.setVectorizedExecutor(null);
        }
      } finally {
        resourceSession.close();
      }
    }
  }
}
