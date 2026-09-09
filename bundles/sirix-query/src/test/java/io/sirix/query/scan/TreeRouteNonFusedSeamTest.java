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
 * The last-resort predicate-tree route, on plans that are NOT fused, over a corpus whose records
 * straddle every page seam.
 *
 * <p>
 * The route was wired behind the shape kernels as a general fallback, and that changed who may hand
 * a boundary record back. The fused kernel was the only path that did, so the batch worker
 * allocated the record scratch that settles such a record only for fused plans; the route runs for
 * any plan and follows the same protocol, which left a single-field scan able to stand a pending
 * slot with nothing to evaluate it against. The route also has to reach the same verdict the
 * kernels would have: it is entered exactly where they refused, so a page it answers is a page
 * nobody else checked.
 *
 * <h2>Reaching it on purpose</h2> A single-field fractional predicate is served by the double
 * kernel, not by the route — unless the kernel refuses. {@code 0.500000000000001} has scale 15, one
 * past {@link io.sirix.page.pax.DoubleRegion#MAX_DECIMAL_SCALE}, so no exact-decimal interval can
 * be folded for it, and its double image is inexact, so the plan carries no usable double bounds
 * either: {@code countPageCombiningDoubles} declines every page and the route is what sees them.
 *
 * <p>
 * The prices are genuine doubles — a JSON number keeps its exponent form only when it round-trips —
 * so the tag is ALP over doubles rather than the exact-decimal column, which is the encoding the
 * route must refuse to answer from a rounded threshold. Five slots per record against a 1024-slot
 * page means the seam offset advances by four slots per page and cycles through every position
 * inside a record, so pages with a record spanning in from the previous one are not a corner case
 * here but the common case.
 *
 * <p>
 * Ground truth is the same query with the column path switched off. Agreement alone would pass
 * vacuously — a page nobody serves columnar agrees for free — so
 * {@link #theRouteDeclinesAnUnrepresentableThreshold()} asserts the mechanism as well: these pages
 * reach the last-resort call site and are REFUSED there. They were not, before: the route folded
 * the decimal threshold to its nearest double and answered from that, so the same three shapes came
 * back short while the plan that fed them had already declined for exactly that reason.
 */
final class TreeRouteNonFusedSeamTest {

  private static final int N = 8_000;
  private static final String DB = "tree-route-non-fused-db";
  private static final String RES = "records.jn";

  /**
   * Single-field, non-fused shapes whose literal no column domain can serve exactly: scale 15 is past
   * the exact-decimal column's reach and the image is inexact.
   */
  private static final List<String> NON_FUSED_SHAPES =
      List.of("$u.price gt 0.500000000000001", "$u.price lt 60.500000000000001", "$u.price ge 25.500000000000001",
          "$u.price lt 20.500000000000001 or $u.price gt 60.500000000000001");

  private Path dbDir;

  @BeforeEach
  void setUp() throws Exception {
    dbDir = Files.createTempDirectory("sirix-tree-route-non-fused-");
    final StringBuilder sb = new StringBuilder(N * 56);
    sb.append('[');
    for (int i = 0; i < N; i++) {
      if (i > 0) {
        sb.append(',');
      }
      // Four fields plus the object node — five slots — for the seam arithmetic above. The price is
      // written in exponent form so the shredder keeps it a double instead of a BigDecimal.
      sb.append("{\"active\":")
        .append(i % 3 != 0)
        .append(",\"id\":")
        .append(i)
        .append(",\"note\":")
        .append(i % 97)
        .append(",\"price\":")
        .append(1 + i % 128)
        .append('.')
        .append(i % 8)
        .append("e1")
        .append('}');
    }
    sb.append(']');
    // A path summary is REQUIRED: without one resolveTargetPathNodeKey returns -1, the structural
    // source matcher becomes non-null, and the executor forces regionPlan to null on purpose,
    // because raw page columns cannot prove exact source ancestry. The page-only routes this test
    // is about then never run at all — which is exactly what an earlier revision, built with
    // buildPathSummary(false), was unknowingly asserting against.
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
  @DisplayName("a non-fused plan served by the tree route agrees with the record path at every seam")
  void nonFusedTreeRoutePagesAgreeAcrossSeams() throws Exception {
    for (final String predicate : NON_FUSED_SHAPES) {
      assertEquals(count(predicate, false), count(predicate, true),
          "column path disagrees with the record path for: " + predicate
              + " — a single-field plan reached the last-resort tree route, which either "
              + "answered a threshold no double represents or mishandled the record "
              + "spanning in from the previous page");
    }
  }

  @Test
  @DisplayName("a warm repeat of the column path still agrees")
  void repeatScanAgrees() throws Exception {
    for (final String predicate : NON_FUSED_SHAPES) {
      final long viaRecords = count(predicate, false);
      assertEquals(viaRecords, count(predicate, true), "first column scan disagrees: " + predicate);
      // The second scan reads pages the first left RESIDENT — the arrangement under which a plan
      // whose mask never asked for the record linkage is nonetheless handed a page that carries it.
      // No claim is made about what scheduled the repeat: with a path summary the persisted
      // page-key array can serve the scan outright, leaving nothing published to schedule from.
      assertEquals(viaRecords, count(predicate, true), "the SECOND column scan disagrees for: " + predicate);
    }
  }

  @Test
  @DisplayName("the route DECLINES these pages rather than answering them from a rounded threshold")
  void theRouteDeclinesAnUnrepresentableThreshold() throws Exception {
    for (final String predicate : NON_FUSED_SHAPES) {
      SirixVectorizedExecutor.resetRegionOnlyCounters();
      count(predicate, true);
      final long viaTree = SirixVectorizedExecutor.regionTreePages();
      final long fellBack = SirixVectorizedExecutor.regionOnlyPageFallbacks();
      // Not a vacuous assertion: before the route carried the fold's soundness guard it answered
      // every one of these pages, from bounds rounded to the nearest double, and undercounted.
      assertEquals(0L, viaTree,
          "the tree route answered pages for: " + predicate
              + " — its threshold is a decimal no double represents, so the double column "
              + "cannot decide it and the shape kernels already refused for that reason");
      assertTrue(fellBack > 0,
          "no page fell back at all for: " + predicate
              + " — the shapes here are chosen because every column domain must refuse "
              + "them, so this test is not exercising the last-resort call site");
    }
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
