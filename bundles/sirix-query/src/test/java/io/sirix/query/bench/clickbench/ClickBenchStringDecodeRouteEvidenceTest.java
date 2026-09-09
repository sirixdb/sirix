/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.query.bench.clickbench;

import io.brackit.query.Query;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.util.serialize.StringSerializer;
import io.sirix.access.Databases;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.projection.ProjectionBulkLoad;
import io.sirix.index.projection.ProjectionIndexCatalog;
import io.sirix.index.projection.ProjectionIndexRegistry;
import io.sirix.index.projection.ProjectionIndexRowGroupPage;
import io.sirix.index.projection.SegmentDictionaryLane;
import io.sirix.page.ChunkedBodyConfig;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.scan.SirixVectorizedExecutor;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end route witness for the string DECODE and canonicalisation path: the queries that group
 * by a {@link ProjectionIndexRowGroupPage#COLUMN_KIND_STRING_SEGMENT} column, which is the shape
 * the 100M ClickBench database is built with and the only shape that reaches
 * {@code SegmentGroupCanonicaliser}.
 *
 * <p>
 * {@link ClickBenchQ21Q22SegmentRouteEvidenceTest} witnesses the string EXTREMUM half of that path
 * (a {@code MIN} folded on sealed canonical ids). This one witnesses the other half, over the three
 * shapes the canonicaliser actually has:
 *
 * <ol>
 * <li><b>q33</b> groups an ENTIRE segment string column with no predicate — the whole-column route,
 * where the row loop rewrites every leaf's cells to canonical ids.</li>
 * <li><b>q13</b> groups the same kind of column under a predicate and adds {@code COUNT(DISTINCT
 * UserID)}, so only the kept rows are canonicalised.</li>
 * <li><b>q28</b> groups by a TRANSFORMED key — {@code REGEXP_REPLACE} over {@code Referer} — which
 * is the arm that decodes every selected dictionary value, applies the transform on the segment
 * worker, and publishes one canonical id per transformed value. It is the largest single block of
 * time in the ClickBench suite.</li>
 * </ol>
 *
 * <p>
 * The fixture is deliberately WIDER than the 60,000 rows q21/q22 use: at
 * {@link ProjectionIndexRowGroupPage#MAX_ROWS} rows per leaf, {@value #ROWS} rows give more than
 * 256 leaves, which is the threshold above which a whole-column mapping is split into disjoint
 * parallel ranges. A narrower fixture would map serially and would witness nothing about that
 * split.
 *
 * <p>
 * Every leg asserts the PRECONDITION (the columns really are segment-scoped, the query really was
 * served by the group-aggregate route) before it asserts the ANSWER, because a silent decline to
 * the generic interpreter would make an equality against the interpreter vacuous. The query text is
 * the SHIPPED ClickBench text ({@link ClickBenchQueries#byIndex(int)}), so a query edit cannot
 * drift away from what the campaign measures.
 */
public final class ClickBenchStringDecodeRouteEvidenceTest {

  /**
   * More than 256 leaves of {@link ProjectionIndexRowGroupPage#MAX_ROWS} rows, so a whole-column
   * canonical mapping is split into parallel ranges rather than mapped serially, and still small
   * enough that the generic interpreter can answer the same queries as an oracle in seconds.
   */
  private static final int ROWS = 300_000;

  /** The leaf count above which {@code canonicaliseMemoised} maps a whole column in parallel. */
  private static final int PARALLEL_MAPPING_LEAVES = 256;

  private static final int Q13 = 13;
  private static final int Q28 = 28;
  private static final int Q33 = 33;

  /** The string columns these three queries group by or transform. */
  private static final String[] SEGMENT_STRING_COLUMNS = {"SearchPhrase", "Referer", "URL"};

  private static final String GLOBAL_DICT_PROPERTY = "sirix.projection.globalDict";

  /**
   * q28's shipped {@code HAVING COUNT(*) > 100000} keeps nothing on a fixture this size, so the
   * shipped text alone would compare two empty answers and witness no decoded key. The same text with
   * the threshold scaled to the fixture returns the real regex-keyed groups; both are run.
   */
  private static final String Q28_SHIPPED_HAVING = "$c > 100000";

  private static final String Q28_SCALED_HAVING = "$c > 250";

  private static Path dbDir;
  private static String priorLane;
  private static String priorGlobalDict;
  private static boolean priorChunked;
  private static int priorChunkTarget;

  @BeforeAll
  static void loadSegmentScopedHits() throws Exception {
    priorLane = System.getProperty(SegmentDictionaryLane.ENABLED_PROPERTY);
    priorGlobalDict = System.getProperty(GLOBAL_DICT_PROPERTY);
    // The rig's own load flags (bench/clickbench/rig/rig.env LOADFLAGS): arm the segment dictionary
    // lane and keep the projection's global column dictionary out of the way, so every string column
    // of a leaf is converted to the segment kind rather than served globally.
    System.setProperty(SegmentDictionaryLane.ENABLED_PROPERTY, "true");
    System.setProperty(GLOBAL_DICT_PROPERTY, "never");
    // A page whose values are segment dictionary ids is readable only on the lazy route, which needs
    // a chunk-framed body; the lane refuses to arm without one. 16 KiB is the rig's target.
    priorChunked = ChunkedBodyConfig.setEnabledForTesting(true);
    priorChunkTarget = ChunkedBodyConfig.setTargetChunkBytesForTesting(16384);
    ProjectionBulkLoad.clearActive();
    ProjectionIndexCatalog.clearCache();
    ProjectionIndexRegistry.clear();

    dbDir = Files.createTempDirectory("sirix-cb-strdec-");
    try (
        BasicJsonDBStore store = BasicJsonDBStore.newBuilder()
                                                 .location(dbDir)
                                                 .buildPathSummary(true)
                                                 .versioningType(VersioningType.FULL)
                                                 .hashType(HashType.NONE)
                                                 .storeNodeHistory(false)
                                                 .build();
        Reader source = ClickBenchSource.open("generate:" + ROWS + ":42")) {
      store.createParallel(ClickBenchSchema.DATABASE, ClickBenchSchema.RESOURCE, source,
          ClickBenchProjection.spec(ROWS));
    }
    ProjectionIndexCatalog.clearCache();
    ProjectionIndexRegistry.clear();
    Databases.getGlobalBufferManager().clearAllCaches();
  }

  @AfterAll
  static void tearDown() throws IOException {
    ChunkedBodyConfig.setTargetChunkBytesForTesting(priorChunkTarget);
    ChunkedBodyConfig.setEnabledForTesting(priorChunked);
    restore(SegmentDictionaryLane.ENABLED_PROPERTY, priorLane);
    restore(GLOBAL_DICT_PROPERTY, priorGlobalDict);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    ProjectionBulkLoad.clearActive();
    Databases.getGlobalBufferManager().clearAllCaches();
    if (dbDir != null && Files.exists(dbDir)) {
      try (Stream<Path> paths = Files.walk(dbDir)) {
        paths.sorted(Comparator.reverseOrder()).forEach(path -> {
          try {
            Files.deleteIfExists(path);
          } catch (final IOException e) {
            // best effort: a temp directory left behind must not fail the build
          }
        });
      }
    }
  }

  private static void restore(final String property, final String value) {
    if (value == null) {
      System.clearProperty(property);
    } else {
      System.setProperty(property, value);
    }
  }

  /**
   * The precondition of every witness below: the columns really are segment-scoped, and the column is
   * wide enough that a whole-column mapping is split into parallel ranges.
   */
  @Test
  void theFixtureIsSegmentScopedAndWideEnoughForAParallelColumnMapping() throws Exception {
    try (var database = Databases.openJsonDatabase(dbDir.resolve(ClickBenchSchema.DATABASE));
        JsonResourceSession session = database.beginResourceSession(ClickBenchSchema.RESOURCE)) {
      final ProjectionIndexRegistry.Handle handle =
          ProjectionIndexCatalog.lookupCovering(session, session.getResourceConfig().getResource().toString(),
              session.getMostRecentRevisionNumber(), new String[] {"[]"}, SEGMENT_STRING_COLUMNS);
      assertNotNull(handle, "the ClickBench projection covers SearchPhrase/Referer/URL");
      for (final String column : SEGMENT_STRING_COLUMNS) {
        assertEquals(ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SEGMENT,
            handle.columnKindOf(handle.columnOf(column)),
            "the lane must have converted " + column + " to the segment kind");
      }
      assertTrue(handle.segmentDictionarySegmentCount() >= 1, "the load sealed at least one segment dictionary");
      assertTrue(handle.rowGroupCount() >= PARALLEL_MAPPING_LEAVES,
          "a whole-column mapping is only split into parallel ranges above " + PARALLEL_MAPPING_LEAVES
              + " leaves, the fixture has " + handle.rowGroupCount());
      System.out.println("=== fixture: " + ROWS + " generated ClickBench hits, segment dictionary lane armed ===");
      System.out.println("leaves (row groups): " + handle.rowGroupCount() + " (parallel column mapping engages above "
          + PARALLEL_MAPPING_LEAVES + ")");
      System.out.println("segments sealed: " + handle.segmentDictionarySegmentCount());
      for (final String column : SEGMENT_STRING_COLUMNS) {
        System.out.println("column " + column + " kind=" + handle.columnKindOf(handle.columnOf(column))
            + " (COLUMN_KIND_STRING_SEGMENT)");
      }
    }
  }

  @Test
  void q33GroupsAnEntireSegmentStringColumnAndAnswersLikeTheInterpreter() throws Exception {
    final String interpreted = run(ClickBenchQueries.byIndex(Q33).jsoniq(), false);
    final String served = run(ClickBenchQueries.byIndex(Q33).jsoniq(), true, "q33");

    assertEquals(interpreted, served, "q33's served answer differs from the interpreter's");
    report(Q33, ClickBenchQueries.byIndex(Q33).jsoniq(), served);
  }

  @Test
  void q13GroupsAPredicatedSegmentStringColumnAndAnswersLikeTheInterpreter() throws Exception {
    final String interpreted = run(ClickBenchQueries.byIndex(Q13).jsoniq(), false);
    final String served = run(ClickBenchQueries.byIndex(Q13).jsoniq(), true, "q13");

    assertEquals(interpreted, served, "q13's served answer differs from the interpreter's");
    report(Q13, ClickBenchQueries.byIndex(Q13).jsoniq(), served);
  }

  /**
   * The transformed-key arm: every selected {@code Referer} is decoded from its segment dictionary,
   * run through the regex on a segment worker, and published as one canonical id per distinct
   * transformed value. The interpreter applies the same {@code replace} per record, so agreement pins
   * both the decode and the transformed grouping.
   */
  @Test
  void q28GroupsATransformedRegexKeyOverASegmentStringColumnAndAnswersLikeTheInterpreter() throws Exception {
    final String shipped = ClickBenchQueries.byIndex(Q28).jsoniq();
    assertEquals(run(shipped, false), run(shipped, true, "q28"),
        "q28's shipped text: the served answer differs from the interpreter's");

    final String scaled = shipped.replace(Q28_SHIPPED_HAVING, Q28_SCALED_HAVING);
    assertNotEquals(shipped, scaled,
        "q28 no longer carries its " + Q28_SHIPPED_HAVING + " HAVING clause; rescale this witness");
    final String interpreted = run(scaled, false);
    final String served = run(scaled, true, "q28");

    assertEquals(interpreted, served, "q28's served answer differs from the interpreter's");
    assertTrue(served.contains("\"k\":"), "q28 returned no regex-keyed group at all: " + served);
    report(Q28, scaled, served);
  }

  private static void report(final int index, final String jsoniq, final String result) {
    final ClickBenchQueries.Query query = ClickBenchQueries.byIndex(index);
    System.out.println("=== ClickBench q" + index + " over " + ROWS + " generated hits (segment-scoped columns) ===");
    System.out.println("SQL: " + query.sql());
    System.out.println("JSONiq:\n" + jsoniq);
    System.out.println("result (vectorized, byte-identical to the interpreter's):");
    System.out.println(result);
  }

  private static String run(final String jsoniq, final boolean vectorized) throws Exception {
    return run(jsoniq, vectorized, null);
  }

  private static String run(final String jsoniq, final boolean vectorized, final String requireServedAs)
      throws Exception {
    final String text = ClickBenchQueries.wrap(ClickBenchSchema.DATABASE, ClickBenchSchema.RESOURCE, jsoniq);
    final long groupAggBefore = SirixVectorizedExecutor.groupAggServedCount();
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = vectorized
            ? SirixCompileChain.createWithJsonStore(store)
            : SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store)) {
      SirixVectorizedExecutor executor = null;
      try {
        if (vectorized) {
          final var database = Databases.openJsonDatabase(dbDir.resolve(ClickBenchSchema.DATABASE));
          final JsonResourceSession session = database.beginResourceSession(ClickBenchSchema.RESOURCE);
          executor = new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber());
          SequentialPipelineStrategy.setVectorizedExecutor(executor);
        }
        final Sequence result = new Query(chain, text).execute(ctx);
        final StringWriter out = new StringWriter();
        try (PrintWriter writer = new PrintWriter(out)) {
          new StringSerializer(writer).serialize(result);
        }
        if (requireServedAs != null) {
          assertTrue(SirixVectorizedExecutor.groupAggServedCount() > groupAggBefore,
              requireServedAs + " was NOT served by the group-aggregate route; the comparison would be vacuous");
        }
        return out.toString();
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        if (executor != null) {
          executor.close();
        }
      }
    }
  }
}
