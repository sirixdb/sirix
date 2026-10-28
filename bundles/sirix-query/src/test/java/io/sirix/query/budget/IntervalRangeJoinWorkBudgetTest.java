package io.sirix.query.budget;

import io.brackit.query.Query;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.budget.EngineWorkCounters;
import io.sirix.budget.WorkCapture;
import io.sirix.budget.WorkCounter;
import io.sirix.io.StorageType;
import io.sirix.index.projection.ProjectionIndexCatalog;
import io.sirix.index.projection.ProjectionIndexRegistry;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.compiler.translator.SirixJoinedGroupAggregateExpr;
import io.sirix.query.bench.bitemporal.BitemporalProjections;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.ValidTimeIndexes;
import io.sirix.query.scan.SirixVectorizedExecutor;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** A selective overlap join must avoid both array scans and unselected projection column bodies. */
@Isolated
final class IntervalRangeJoinWorkBudgetTest {
  private static final int ROWS = 2200;
  private static final WorkCounter JOINED = WorkCounter.alwaysOn("served.joinGroup", "one joined column route",
      SirixVectorizedExecutor::joinGroupServedCount);
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

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void coldSelectiveRangeJoinReadsOnlyCandidateColumns(final VersioningType versioning) throws Exception {
    build(versioning, false);
    try (var store = BasicJsonDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store);
        var generic = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store)) {
      final WorkCapture.Captured<String> routed = WorkCapture.of(JOINED)
                                                             .and(EngineWorkCounters.VALID_TIME_INTERVAL_REFS)
                                                             .and(EngineWorkCounters.PROJECTION_BODY_SEGMENTS)
                                                             .and(EngineWorkCounters.PROJECTION_LEAVES_PRUNED)
                                                             .call(() -> run(chain, ctx));
      assertEquals("{\"category\":1,\"n\":1,\"minimum\":16,\"maximum\":16}", routed.result());
      assertEquals(run(generic, ctx), routed.result());
      routed.work()
            .assertExactly(JOINED, 1, "overlap residuals and distinct grouping must stay on columns")
            .assertExactly(EngineWorkCounters.VALID_TIME_INTERVAL_REFS, 2,
                "range intersection emits only selective candidates and ties")
            .assertBetween(EngineWorkCounters.PROJECTION_BODY_SEGMENTS, 1, 10,
                "only one leaf per side supplies the needed fields")
            .assertBetween(EngineWorkCounters.PROJECTION_LEAVES_PRUNED, 4, 4,
                "both range masks prune unrelated leaves");
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void repeatedDenseRangeJoinReusesOnlyItsImmutableRequestedNumericColumns(final VersioningType versioning)
      throws Exception {
    final int rows = 5000;
    build(versioning, true, rows);
    try (var store = BasicJsonDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      final WorkCapture.Captured<String> initial =
          WorkCapture.of(JOINED).and(BUCKETED_SIDES).call(() -> run(chain, ctx));
      final String expected = "{\"category\":1,\"n\":" + rows + ",\"minimum\":16,\"maximum\":16}";
      assertEquals(expected, initial.result());
      initial.work()
             .assertExactly(JOINED, 1, "the initial query must use the joined column kernel")
             .assertExactly(BUCKETED_SIDES, 2, "the dense extrema kernel must build both bucketed sides");
      final WorkCapture.Captured<String> repeated = WorkCapture.of(JOINED)
                                                               .and(BUCKETED_SIDES)
                                                               .and(EngineWorkCounters.VALID_TIME_INTERVAL_REFS)
                                                               .and(EngineWorkCounters.PROJECTION_BODY_SEGMENTS)
                                                               .and(EngineWorkCounters.PROJECTION_DENSE_ROWS)
                                                               .and(EngineWorkCounters.PROJECTION_KEY_SET_ADVANCES)
                                                               .call(() -> run(chain, ctx));
      assertEquals(expected, repeated.result());
      repeated.work()
              .assertExactly(JOINED, 1, "a fresh compiled query still performs the joined column kernel")
              .assertExactly(BUCKETED_SIDES, 0, "an immutable revision reuses both bucketed join sides")
              .assertExactly(EngineWorkCounters.VALID_TIME_INTERVAL_REFS, 0,
                  "immutable revision and bounds reuse the membership-proved range keys")
              .assertExactly(EngineWorkCounters.PROJECTION_BODY_SEGMENTS, 0,
                  "immutable dense numeric masks reuse only previously requested column bodies")
              .assertExactly(EngineWorkCounters.PROJECTION_DENSE_ROWS, 0,
                  "reusing an immutable mask must avoid revisiting its dense rows")
              .assertExactly(EngineWorkCounters.PROJECTION_KEY_SET_ADVANCES, 0,
                  "reusing an immutable mask must avoid rebuilding sorted source-key membership");
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void bucketedSidesAreScopedToTheValidTimeSnapshot(final VersioningType versioning) throws Exception {
    build(versioning, """
        [{"id":1,"pid":1,"cost":4,"vf":"2024-01-01T00:00:00Z","vt":"2024-07-01T00:00:00Z"},
         {"id":2,"pid":1,"cost":3,"vf":"2024-01-01T00:00:00Z","vt":"2024-07-01T00:00:00Z"},
         {"id":3,"pid":1,"cost":8,"vf":"2024-07-01T00:00:00Z","vt":"2025-01-01T00:00:00Z"},
         {"id":4,"pid":1,"cost":9,"vf":"2024-07-01T00:00:00Z","vt":"2025-01-01T00:00:00Z"}]
        """, """
        [{"id":1,"category":1,"retail":20,"vf":"2024-01-01T00:00:00Z","vt":"2025-01-01T00:00:00Z"}]
        """);
    final String laterQuery = QUERY.replace("2024-04-01", "2024-08-01").replace("2024-07-01", "2024-10-01");
    final String early = "{\"category\":1,\"n\":2,\"minimum\":16,\"maximum\":17}";
    final String later = "{\"category\":1,\"n\":2,\"minimum\":11,\"maximum\":12}";
    try (var store = BasicJsonDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store);
        var generic = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store)) {
      final WorkCapture.Captured<String> initial = WorkCapture.of(BUCKETED_SIDES).call(() -> run(chain, ctx));
      assertEquals(early, initial.result());
      initial.work().assertExactly(BUCKETED_SIDES, 2, "the first window must build both sides");
      final WorkCapture.Captured<String> warm = WorkCapture.of(BUCKETED_SIDES).call(() -> run(chain, ctx));
      assertEquals(early, warm.result());
      warm.work().assertExactly(BUCKETED_SIDES, 0, "the first window must reuse its completed packing");
      final WorkCapture.Captured<String> changed =
          WorkCapture.of(JOINED).and(BUCKETED_SIDES).call(() -> run(chain, ctx, laterQuery));
      assertEquals(later, changed.result());
      assertEquals(run(generic, ctx, laterQuery), changed.result());
      changed.work()
             .assertExactly(JOINED, 1, "the second window must use the same kernel")
             .assertExactly(BUCKETED_SIDES, 1, "only the changed contracts snapshot needs a new side");
      final WorkCapture.Captured<String> restored = WorkCapture.of(BUCKETED_SIDES).call(() -> run(chain, ctx));
      assertEquals(early, restored.result());
      assertEquals(run(generic, ctx), restored.result());
      restored.work().assertExactly(BUCKETED_SIDES, 0, "returning to the first window must reuse both of its sides");
    }
  }

  private void build(final VersioningType versioning, final boolean dense) {
    build(versioning, dense, ROWS);
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
