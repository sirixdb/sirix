package io.sirix.query.scan;

import io.brackit.query.Query;
import io.brackit.query.compiler.optimizer.PredicateNode;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.util.serialize.StringSerializer;
import io.sirix.access.Databases;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Structural source-path differential for resources without a path summary.
 *
 * <p>
 * Every selected record has same-named decoys both above and below the selected JSON depth. The
 * direct executor is compared with a compile chain that cannot auto-wire a vectorized executor, so
 * agreement cannot be obtained by accidentally running the fast path in both arms. All revisions
 * are queried only after the update and delete/insert commits have completed, which also exercises
 * historical page reconstruction for every versioning strategy. Deletes and inserts use distinct
 * commits so the fixture does not depend on pending-update ordering within one array.
 */
final class NoPathSummarySourceScopeDifferentialTest {

  private static final String DB = "no-path-summary-scope-db";
  private static final String RES = "records.jn";
  private static final PredicateNode ACTIVE = new PredicateNode.BoolRef("active");

  private Path dbDir;

  private record SourceCase(String label, String[] path, Expected[] expected) {
  }

  private record Expected(long count, long sum, long predicateCount, long predicateSum, long distinct) {
  }

  private record Snapshot(String count, String sum, String predicateCount, String predicateSum, String groups,
      String predicateGroups, String distinct, String multiGroups) {
  }

  @AfterEach
  void tearDown() {
    SequentialPipelineStrategy.setVectorizedExecutor(null);
    if (dbDir != null) {
      Databases.removeDatabase(dbDir);
      dbDir = null;
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void exactSourceDepthSurvivesUpdatesDeletesInsertsAndHistory(final VersioningType versioning) throws Exception {
    dbDir = Files.createTempDirectory("sirix-no-path-summary-scope-");
    storeRevisionOne(versioning);
    commitRevisionTwo(versioning);
    commitRevisionThree(versioning);
    commitRevisionFour(versioning);
    commitRevisionFive(versioning);

    final SourceCase[] sources = {
        new SourceCase("top-level", new String[] {"[]"},
            new Expected[] {new Expected(2, 300, 1, 100, 2), new Expected(2, 310, 1, 110, 2),
                new Expected(2, 310, 1, 110, 2), new Expected(2, 310, 1, 110, 2), new Expected(2, 310, 1, 110, 2)}),
        new SourceCase("nested rows", new String[] {"[]", "rows", "[]"},
            new Expected[] {new Expected(3, 60, 3, 40, 2), new Expected(3, 61, 3, 41, 3),
                new Expected(4, 101, 3, 81, 3), new Expected(4, 101, 3, 81, 3), new Expected(3, 81, 3, 81, 3)})};

    for (int revision = 1; revision <= 5; revision++) {
      for (final SourceCase source : sources) {
        final Snapshot interpreted = interpretedSnapshot(versioning, revision, source.path());
        final Snapshot vectorized = vectorizedSnapshot(versioning, revision, source.path());
        final String context = versioning + " revision " + revision + " " + source.label();
        assertEquals(interpreted, vectorized, context + " differs from the interpreter");
        assertExpected(source.expected()[revision - 1], vectorized, context);
      }
    }
  }

  private void storeRevisionOne(final VersioningType versioning) throws Exception {
    final String json = """
        [
          {
            "outerId": 1, "value": 100, "group": "outer-a", "active": true,
            "rows": [
              {"id": 1, "value": 10, "group": "a", "active": true,
               "nested": {"id": 901, "value": 1000, "group": "decoy-a", "active": true}},
              {"id": 2, "value": 20, "group": "b", "active": false,
               "nested": {"id": 902, "value": 2000, "group": "decoy-b", "active": true}},
              {"id": 4, "group": "b", "active": true,
               "nested": {"id": 904, "value": 4000, "group": "decoy-d", "active": true}}
            ],
            "nested": {"id": 801, "value": 8000, "group": "outer-decoy-a", "active": true}
          },
          {
            "outerId": 2, "value": 200, "group": "outer-b", "active": false,
            "rows": [
              {"id": 3, "value": 30, "group": "a", "active": true,
               "nested": {"id": 903, "value": 3000, "group": "decoy-c", "active": true}}
            ],
            "nested": {"id": 802, "value": 9000, "group": "outer-decoy-b", "active": true}
          }
        ]
        """;
    try (var store = newStore(versioning);
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain, "jn:store('" + DB + "','" + RES + "','" + json + "')").evaluate(ctx);
    }
  }

  private void commitRevisionTwo(final VersioningType versioning) throws Exception {
    try (var store = newStore(versioning);
        var ctx =
            SirixQueryContext.createWithJsonStoreAndCommitStrategy(store, SirixQueryContext.CommitStrategy.EXPLICIT);
        // The store-aware index-rewrite optimizer requires a path summary for update-path
        // matching. This resource deliberately has none; the context still supplies the JSON
        // store to the update functions, while the plain chain keeps the mutations generic.
        var chain = SirixCompileChain.create()) {
      update(chain, ctx,
          "for $o in " + latestSource("[]") + " where $o.outerId eq 1 return replace json value of $o.value with 110");
      update(chain, ctx, "for $o in " + latestSource("[]")
          + " where $o.outerId eq 1 return replace json value of $o.group with \"outer-c\"");
      update(chain, ctx,
          "for $r in " + latestSource("[].rows[]") + " where $r.id eq 1 return replace json value of $r.value with 11");
      update(chain, ctx, "for $r in " + latestSource("[].rows[]")
          + " where $r.id eq 1 return replace json value of $r.group with \"c\"");
      update(chain, ctx, "for $r in " + latestSource("[].rows[]")
          + " where $r.id eq 1 return replace json value of $r.nested.value with 10000");
      ctx.applyUpdates();
    }
  }

  private void commitRevisionThree(final VersioningType versioning) throws Exception {
    try (var store = newStore(versioning);
        var ctx =
            SirixQueryContext.createWithJsonStoreAndCommitStrategy(store, SirixQueryContext.CommitStrategy.EXPLICIT);
        var chain = SirixCompileChain.create()) {
      update(chain, ctx,
          "for $r in " + latestSource("[].rows[]") + " where $r.id eq 4 return insert json {\"value\":40} into $r");
      ctx.applyUpdates();
    }
  }

  private void commitRevisionFour(final VersioningType versioning) throws Exception {
    try (var store = newStore(versioning);
        var ctx =
            SirixQueryContext.createWithJsonStoreAndCommitStrategy(store, SirixQueryContext.CommitStrategy.EXPLICIT);
        var chain = SirixCompileChain.create()) {
      // Insert a fresh same-named decoy. The selected top-level and row sources must remain
      // unchanged even though their anchor names gain new, live slots in this revision.
      update(chain, ctx, "for $o in " + latestSource("[]") + " where $o.outerId eq 1 return insert json "
          + "{\"lateNested\":{\"id\":905,\"value\":7000,\"group\":\"decoy-e\",\"active\":true}} " + "into $o");
      ctx.applyUpdates();
    }
  }

  private void commitRevisionFive(final VersioningType versioning) throws Exception {
    try (var store = newStore(versioning);
        var ctx =
            SirixQueryContext.createWithJsonStoreAndCommitStrategy(store, SirixQueryContext.CommitStrategy.EXPLICIT);
        var chain = SirixCompileChain.create()) {
      update(chain, ctx, "for $r in " + latestSource("[].rows[]") + " where $r.id eq 2 return delete json $r.value");
      ctx.applyUpdates();
    }
  }

  private Snapshot interpretedSnapshot(final VersioningType versioning, final int revision, final String[] sourcePath)
      throws Exception {
    try (var store = newStore(versioning);
        var ctx = SirixQueryContext.createWithJsonStore(store);
        // As for the update harness, keep the no-summary resource away from index rewriting. A
        // plain chain has no executor to substitute, while the query context remains the store
        // used by jn:doc, making this an independent generic-pipeline oracle.
        var chain = SirixCompileChain.create()) {
      final String source = revisionSource(revision, sourcePath);
      return new Snapshot(run(chain, ctx, "count(for $r in " + source + " return $r.value)"),
          run(chain, ctx, "sum(for $r in " + source + " return $r.value)"),
          run(chain, ctx, "count(for $r in " + source + " where $r.active return $r)"),
          run(chain, ctx, "sum(for $r in " + source + " where $r.active return $r.value)"),
          normalizeGroups(run(chain, ctx,
              "for $r in " + source + " let $g := $r.group group by $g return {\"group\":$g,\"count\":count($r)}")),
          normalizeGroups(run(chain, ctx,
              "for $r in " + source + " where $r.active let $g := $r.group group by $g "
                  + "return {\"group\":$g,\"count\":count($r)}")),
          run(chain, ctx, "count(distinct-values(for $r in " + source + " return $r.group))"),
          normalizeGroups(
              run(chain, ctx, "for $r in " + source + " let $g := $r.group, $a := $r.active group by $g, $a "
                  + "return {\"group\":$g,\"active\":$a,\"count\":count($r)}")));
    }
  }

  private Snapshot vectorizedSnapshot(final VersioningType versioning, final int revision, final String[] sourcePath)
      throws Exception {
    try (var store = newStore(versioning);
        var resourceSession = store.lookup(DB).getDatabase().beginResourceSession(RES)) {
      final SirixVectorizedExecutor executor = new SirixVectorizedExecutor(resourceSession, revision);
      try {
        return new Snapshot(serialize(executor.executeAggregate(null, sourcePath, "count", "value")),
            serialize(executor.executeAggregate(null, sourcePath, "sum", "value")),
            serialize(executor.executePredicateCount(null, sourcePath, ACTIVE)),
            serialize(executor.executePredicateAggregate(null, sourcePath, ACTIVE, "sum", "value")),
            normalizeGroups(serialize(executor.executeGroupByCount(null, sourcePath, "group"))),
            normalizeGroups(serialize(executor.executePredicateGroupByCount(null, sourcePath, ACTIVE, "group"))),
            serialize(executor.executeCountDistinct(null, sourcePath, "group")),
            normalizeGroups(serialize(executor.executeGroupByCountMulti(null, sourcePath,
                new String[] {"group", "active"}, new String[] {"group", "active"}, "count", null))));
      } finally {
        executor.close();
      }
    }
  }

  private BasicJsonDBStore newStore(final VersioningType versioning) {
    return BasicJsonDBStore.newBuilder()
                           .location(dbDir)
                           .buildPathSummary(false)
                           .buildPathStatistics(false)
                           .versioningType(versioning)
                           .build();
  }

  private static void update(final SirixCompileChain chain, final SirixQueryContext ctx, final String query)
      throws Exception {
    new Query(chain, query).evaluate(ctx);
  }

  private static String run(final SirixCompileChain chain, final SirixQueryContext ctx, final String query)
      throws Exception {
    return serialize(new Query(chain, query).execute(ctx));
  }

  private static String serialize(final Sequence result) throws Exception {
    final StringWriter out = new StringWriter();
    try (PrintWriter writer = new PrintWriter(out)) {
      new StringSerializer(writer).serialize(result);
    }
    return out.toString();
  }

  private static String normalizeGroups(final String serialized) {
    return Arrays.stream(serialized.replace("} {", "}\n{").split("\\R"))
                 .map(String::strip)
                 .filter(line -> !line.isEmpty())
                 .sorted()
                 .collect(Collectors.joining("\n"));
  }

  private static void assertExpected(final Expected expected, final Snapshot actual, final String context) {
    assertEquals(Long.toString(expected.count()), actual.count(), context + " field count");
    assertEquals(Long.toString(expected.sum()), actual.sum(), context + " sum");
    assertEquals(Long.toString(expected.predicateCount()), actual.predicateCount(), context + " predicate count");
    assertEquals(Long.toString(expected.predicateSum()), actual.predicateSum(), context + " predicate sum");
    assertEquals(Long.toString(expected.distinct()), actual.distinct(), context + " distinct groups");
  }

  private static String latestDocument() {
    return "jn:doc('" + DB + "','" + RES + "')";
  }

  private static String latestSource(final String suffix) {
    return latestDocument() + suffix;
  }

  private static String revisionSource(final int revision, final String[] sourcePath) {
    final StringBuilder source =
        new StringBuilder("jn:doc('").append(DB).append("','").append(RES).append("',").append(revision).append(')');
    for (final String step : sourcePath) {
      if ("[]".equals(step)) {
        source.append("[]");
      } else {
        source.append('.').append(step);
      }
    }
    return source.toString();
  }
}
