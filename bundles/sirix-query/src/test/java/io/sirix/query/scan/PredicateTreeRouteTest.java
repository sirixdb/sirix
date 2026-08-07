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

/**
 * The predicate-tree route: answers must match the record path, on every shape.
 *
 * <p>The route is wired as a LAST RESORT — it runs only where the shape-specific kernels already
 * refused, so it can never change an answer a kernel produced, only replace a record-heap
 * reconstruction with a column read. This pins the part that matters: that what it produces instead
 * is the same number.
 *
 * <p>Ground truth is the SAME query with the column path switched off, so the two arms differ in
 * exactly one variable. A shape neither arm can serve simply agrees trivially, which is why the
 * companion assertion on {@code regionTreePages()} exists — without it, a route that never runs
 * would pass every case here.
 */
@DisplayName("predicate tree route")
final class PredicateTreeRouteTest {

  private static final int N = 8_000;
  private static final String DB = "tree-route-db";
  private static final String RES = "records.jn";

  private Path dbDir;

  @BeforeEach
  void setUp() throws Exception {
    dbDir = Files.createTempDirectory("sirix-tree-route-");
    final StringBuilder sb = new StringBuilder(N * 64);
    sb.append('[');
    for (int i = 0; i < N; i++) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append("{\"id\":").append(i)
        .append(",\"year\":").append(1900 + i % 124)
        .append(",\"note\":").append(i % 97)
        .append(",\"active\":").append(i % 3 != 0)
        .append(",\"price\":").append(50 + i % 500).append('.').append(i % 10).append(i % 10)
        .append(",\"title\":\"").append(new String[] { "Alpha", "Beta", "Gamma" }[i % 3]).append('"')
        .append('}');
    }
    sb.append(']');
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
  @DisplayName("every shape agrees with the record path")
  void everyShapeAgreesWithTheRecordPath() throws Exception {
    final List<String> shapes = List.of(
        "$u.year gt 1950",
        "$u.year gt 1950 and $u.note lt 40",
        "$u.active",
        "not($u.active)",
        "$u.year gt 1950 and not($u.active)",
        "$u.year gt 2000 or $u.note gt 90",
        "($u.year ge 1940 and $u.year le 1950) or $u.year gt 2000",
        "$u.title eq \"Beta\"",
        "$u.title eq \"Beta\" and $u.year gt 1950",
        "$u.title eq \"Beta\" or $u.note lt 10",
        "not($u.title eq \"Beta\")",
        "$u.year gt 1950 and $u.note lt 40 and $u.active",
        "($u.title eq \"Alpha\" or $u.title eq \"Gamma\") and $u.year lt 1960",
        // Fractional leaves: inexact-as-double literals over a decimal column, alone and composed.
        "$u.price gt 100.25",
        "$u.price gt 19.99 and $u.year gt 1950",
        "$u.price lt 60.5 or $u.price gt 500.05",
        "not($u.price gt 300.33)",
        "$u.price ge 100.10 and $u.price le 200.90 and $u.active");
    for (final String predicate : shapes) {
      assertEquals(count(predicate, false), count(predicate, true),
                   "column path disagrees with the record path for: " + predicate);
    }
  }

  @Test
  @DisplayName("the route is actually reached, not merely correct-by-never-running")
  void theRouteIsReached() throws Exception {
    // Without this, every assertion above would still pass if countPageViaTree were dead code —
    // and it DID, until the route became primary for fused plans. Asserted as "> 0" rather than at
    // a fixed number: which shapes reach it shifts as the shape-specific kernels are retired, but
    // "never reached" must always fail.
    long reached = 0;
    for (final String predicate : List.of("$u.year gt 1950 and not($u.active)",
                                          "not($u.title eq \"Beta\")",
                                          "$u.title eq \"Beta\" or $u.note lt 10",
                                          "not($u.active)")) {
      SirixVectorizedExecutor.resetRegionOnlyCounters();
      count(predicate, true);
      reached += SirixVectorizedExecutor.regionTreePages();
    }
    System.out.println("[tree-route] pages answered by the predicate tree: " + reached);
    org.junit.jupiter.api.Assertions.assertTrue(reached > 0,
        "the predicate-tree route answered no page at all — it is dead code, and every agreement "
            + "assertion in this class is passing vacuously");
  }

  private long count(final String predicate, final boolean regionOnly) throws Exception {
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).buildPathSummary(true).build();
         var ctx = SirixQueryContext.createWithJsonStore(store);
         var chain = SirixCompileChain.createWithJsonStore(store)) {
      final var coll = store.lookup(DB);
      final var resourceSession = coll.getDatabase().beginResourceSession(RES);
      try {
        final var exec =
            new SirixVectorizedExecutor(resourceSession, resourceSession.getMostRecentRevisionNumber());
        exec.setRegionOnlyCountEnabled(regionOnly);
        SequentialPipelineStrategy.setVectorizedExecutor(exec);
        try {
          return ((Int64) new Query(chain,
                                    "count(for $u in jn:doc('" + DB + "','" + RES + "')[] where "
                                        + predicate + " return $u)").evaluate(ctx)).longValue();
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
