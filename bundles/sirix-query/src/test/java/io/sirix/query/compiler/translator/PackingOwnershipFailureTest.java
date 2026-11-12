package io.sirix.query.compiler.translator;

import io.brackit.query.Query;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Isolated
final class PackingOwnershipFailureTest {
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

  @ParameterizedTest
  @ValueSource(strings = {"packing", "submission", "side-handle"})
  void failedHandoffReleasesTheLeaseAndCompletesPublishedFutures(final String point) throws Exception {
    build(VersioningType.FULL, true, 1000);
    final long bytes = 8L << 20;
    final DerivedCacheBudget budget = new DerivedCacheBudget(Allocators.getInstance(), bytes * 8, bytes);
    final DerivedCacheBudget previous = DerivedCacheBudget.setForTesting(budget);
    final Executor previousExecutor = SirixJoinedGroupAggregateExpr.setPackingExecutorForTesting(Runnable::run);
    final AtomicReference<CompletableFuture<?>> future = new AtomicReference<>();
    final OutOfMemoryError failure = new OutOfMemoryError("injected " + point);
    SirixJoinedGroupAggregateExpr.handoffHookForTesting = stage -> {
      if (point.equals(stage)) {
        future.set(SirixJoinedGroupAggregateExpr.inFlightPackingForTesting());
        throw failure;
      }
    };
    try (var store = BasicJsonDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      assertSame(failure, assertThrows(OutOfMemoryError.class, () -> run(chain, ctx)));
      SirixJoinedGroupAggregateExpr.handoffHookForTesting = null;
      assertEquals(0, SirixJoinedGroupAggregateExpr.inFlightPackingCountForTesting());
      if (!point.equals("side-handle")) {
        assertNotNull(future.get());
        assertTrue(future.get().isCompletedExceptionally(), "a failed producer must wake every waiter");
      }
      assertEquals(0, budget.metrics().inFlightBytes());
      budget.numeric().clear();
      budget.packed().clear();
      assertEquals(0, budget.metrics().retainedBytes());
      assertEquals(0, budget.metrics().retiredLeasedBytes(), "no packing or side-handle lease outlives its failure");
      assertEquals("{\"category\":1,\"n\":1000,\"minimum\":16,\"maximum\":16}", run(chain, ctx));
    } finally {
      SirixJoinedGroupAggregateExpr.handoffHookForTesting = null;
      SirixJoinedGroupAggregateExpr.setPackingExecutorForTesting(previousExecutor);
      DerivedCacheBudget.setForTesting(previous);
      budget.numeric().clear();
      budget.packed().clear();
      ProjectionIndexCatalog.clearCache();
      ProjectionIndexRegistry.clear();
      Databases.clearGlobalCaches();
      Databases.removeDatabase(directory.resolve("ranges"));
    }
  }

  private void build(final VersioningType versioning, final boolean dense, final int rows) {
    build(versioning, rows("contracts", dense, rows), rows("products", dense, dense
        ? rows / 2
        : rows));
  }

  private void build(final VersioningType versioning, final String contracts, final String products) {
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
              ? contracts
              : products), JsonNodeTrx.Commit.NO);
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

  private static String rows(final String resource, final boolean dense, final int rowCount) {
    final boolean contracts = resource.equals("contracts");
    final StringBuilder json = new StringBuilder(rowCount * 160).append('[');
    for (int row = 0; row < rowCount; row++) {
      if (row > 0) {
        json.append(',');
      }
      json.append("{\"id\":").append(row + 1);
      if (contracts) {
        json.append(",\"pid\":")
            .append(dense
                ? row % (rowCount / 2) + 1
                : row + 1)
            .append(",\"cost\":4");
      } else {
        json.append(",\"category\":1,\"retail\":20");
      }
      final String year = dense || row == 0
          ? "2024"
          : "2025";
      json.append(",\"vf\":\"")
          .append(year)
          .append("-01-01T00:00:00Z\",\"vt\":\"")
          .append(year)
          .append("-12-31T00:00:00Z\"}");
    }
    return json.append(']').toString();
  }

  private static String run(final SirixCompileChain chain, final SirixQueryContext ctx) throws Exception {
    return run(chain, ctx, QUERY);
  }

  private static String run(final SirixCompileChain chain, final SirixQueryContext ctx, final String query)
      throws Exception {
    try (var bytes = new ByteArrayOutputStream(); var writer = new PrintWriter(bytes, false, StandardCharsets.UTF_8)) {
      new Query(chain, query).serialize(ctx, writer);
      writer.flush();
      return bytes.toString(StandardCharsets.UTF_8).trim();
    }
  }
}
