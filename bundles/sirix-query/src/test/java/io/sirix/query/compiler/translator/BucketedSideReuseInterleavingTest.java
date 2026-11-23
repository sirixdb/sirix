package io.sirix.query.compiler.translator;

import io.brackit.query.Query;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.budget.WorkCapture;
import io.sirix.budget.WorkCounter;
import io.sirix.cache.Allocators;
import io.sirix.index.projection.ProjectionIndexCatalog;
import io.sirix.index.projection.ProjectionIndexRegistry;
import io.sirix.io.StorageType;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.bench.bitemporal.BitemporalProjections;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.ValidTimeIndexes;
import io.sirix.query.scan.DerivedCacheBudget;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * A background packing publishes its side before it leaves the in-flight map, so a reader that
 * found no resident side must re-read the published sides once the map says nothing is in flight.
 * The interleaving this test drives is the one a loaded runner produced by itself: the packing
 * completed between a reader's two lookups, and the reader packed the resident side a second time.
 * See {@code docs/postmortems/2026-11-bucketed-side-reuse-interleaving.md}.
 */
@Isolated
final class BucketedSideReuseInterleavingTest {
  private static final int ROWS = 1000;
  private static final long BUDGET_BYTES = 8L << 20;
  private static final WorkCounter BUCKETED_SIDES =
      WorkCounter.alwaysOn("joinGroup.bucketedSidesBuilt", "join sides bucketed by key for the dense extrema kernel",
          SirixJoinedGroupAggregateExpr::bucketedSidesBuiltCount);
  private static final String QUERY = """
      declare variable $L := xs:dateTime('2024-04-01T00:00:00Z');
      declare variable $U := xs:dateTime('2024-07-01T00:00:00Z');
      for $c in jn:doc('ranges','contracts',1)[]
      for $p in jn:doc('ranges','products',1)[]
      where $c.pid eq $p.id
        and xs:dateTime($c.vf) lt $U and $L lt xs:dateTime($c.vt)
        and xs:dateTime($p.vf) lt $U and $L lt xs:dateTime($p.vt)
        and xs:dateTime($c.vf) lt xs:dateTime($p.vt)
        and xs:dateTime($p.vf) lt xs:dateTime($c.vt)
      let $category := $p.category, $cid := $c.id, $margin := $p.retail - $c.cost
      group by $category
      let $n := count(distinct-values($cid)), $minimum := min($margin), $maximum := max($margin)
      order by $category
      return {"category":$category,"n":$n,"minimum":$minimum,"maximum":$maximum}
      """;

  @TempDir
  Path directory;

  /**
   * The repeat must reuse the side whatever the packing's completion order is: here the packing
   * publishes and leaves the in-flight map exactly between the repeat's resident lookup and its
   * in-flight lookup, the window that made the work budget depend on the machine.
   */
  @Test
  void aSidePublishedBetweenTheTwoLookupsIsReusedRatherThanPackedAgain() throws Exception {
    build(VersioningType.FULL, ROWS);
    final DerivedCacheBudget budget =
        new DerivedCacheBudget(Allocators.getInstance(), BUDGET_BYTES * 8, BUDGET_BYTES);
    final DerivedCacheBudget previousBudget = DerivedCacheBudget.setForTesting(budget);
    final AtomicReference<Runnable> pending = new AtomicReference<>();
    final Executor previousExecutor =
        SirixJoinedGroupAggregateExpr.setPackingExecutorForTesting(pending::set);
    final AtomicInteger residentMisses = new AtomicInteger();
    try (var store = BasicJsonDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      final String expected = "{\"category\":1,\"n\":" + ROWS + ",\"minimum\":16,\"maximum\":16}";
      assertEquals(expected, run(chain, ctx), "the first execution packs the build side and streams the probe");
      final Runnable packing = pending.getAndSet(null);
      assertNotNull(packing, "the first execution submits the probe side's packing");
      // The repeat's build side is resident, so its lookup never reaches this point; the probe
      // side's lookup does, and lets the packing publish and leave the in-flight map right there.
      SirixJoinedGroupAggregateExpr.handoffHookForTesting = stage -> {
        if ("resident-miss".equals(stage) && residentMisses.getAndIncrement() == 0) {
          packing.run();
        }
      };
      final WorkCapture.Captured<String> repeated = WorkCapture.of(BUCKETED_SIDES).call(() -> run(chain, ctx));
      SirixJoinedGroupAggregateExpr.handoffHookForTesting = null;
      assertEquals(expected, repeated.result(), "the reused side must answer exactly as the packed one does");
      assertEquals(1, residentMisses.get(),
          "the repeat must reach the in-flight lookup once, for the side the packing was publishing");
      repeated.work()
              .assertExactly(BUCKETED_SIDES, 0,
                  "a side that published between the resident and the in-flight lookup must be reused, not packed"
                      + " again: the work this cache saves must not depend on the packing's completion order");
      assertNull(pending.get(), "the repeat must not submit a second packing of the same side");
    } finally {
      SirixJoinedGroupAggregateExpr.handoffHookForTesting = null;
      SirixJoinedGroupAggregateExpr.setPackingExecutorForTesting(previousExecutor);
      DerivedCacheBudget.setForTesting(previousBudget);
      budget.numeric().clear();
      budget.packed().clear();
      ProjectionIndexRegistry.clear();
      ProjectionIndexCatalog.clearCache();
      Databases.clearGlobalCaches();
      Databases.removeDatabase(directory.resolve("ranges"));
    }
  }

  private void build(final VersioningType versioning, final int rows) {
    final Path path = directory.resolve("ranges");
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(path)) {
      for (final String resource : new String[] {"contracts", "products"}) {
        db.createResource(ResourceConfiguration.newBuilder(resource)
                                               .validTimePaths("vf", "vt")
                                               .versioningApproach(versioning)
                                               .storageType(StorageType.FILE_CHANNEL)
                                               .storeDiffs(false)
                                               .build());
        try (var session = db.beginResourceSession(resource); var wtx = session.beginNodeTrx()) {
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(resource.equals("contracts")
              ? rows("contracts", rows)
              : rows("products", rows / 2)), JsonNodeTrx.Commit.NO);
          wtx.moveToDocumentRoot();
          wtx.moveToFirstChild();
          ValidTimeIndexes.createValidTimeIndexesIfConfigured(session, wtx, "ranges");
          BitemporalProjections.declare(session, wtx, resource);
          wtx.commit();
        }
      }
    }
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    Databases.clearGlobalCaches();
  }

  /** {@code rowCount} dense rows of {@code resource} over one year, two contracts per product. */
  private static String rows(final String resource, final int rowCount) {
    final boolean contracts = resource.equals("contracts");
    final StringBuilder json = new StringBuilder(rowCount * 160).append('[');
    for (int row = 0; row < rowCount; row++) {
      if (row > 0) {
        json.append(',');
      }
      json.append("{\"id\":").append(row + 1);
      if (contracts) {
        json.append(",\"pid\":").append(row % (rowCount / 2) + 1).append(",\"cost\":4");
      } else {
        json.append(",\"category\":1,\"retail\":20");
      }
      json.append(",\"vf\":\"2024-01-01T00:00:00Z\",\"vt\":\"2024-12-31T00:00:00Z\"}");
    }
    return json.append(']').toString();
  }

  private static String run(final SirixCompileChain chain, final SirixQueryContext ctx) throws Exception {
    try (var bytes = new ByteArrayOutputStream(); var writer = new PrintWriter(bytes, false, StandardCharsets.UTF_8)) {
      new Query(chain, QUERY).serialize(ctx, writer);
      writer.flush();
      return bytes.toString(StandardCharsets.UTF_8).trim();
    }
  }
}
