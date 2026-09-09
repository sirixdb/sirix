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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end route witness for the campaign's two remaining string-predicate levers, q21 and q22,
 * over SEGMENT-scoped string columns — the shape the 100M database is built with, and the only
 * shape that reaches {@code SegmentGroupCanonicaliser}.
 *
 * <p>
 * Both queries are {@code LIKE} predicates combined with {@code MIN} over a string column; q22 adds
 * a second {@code MIN}, a {@code NOT LIKE} inside its predicate tree, and {@code COUNT(DISTINCT
 * UserID)}. The mechanism under test is that the minimum of a group is the smallest CANONICAL id:
 * the executor seals each string extremum operand into a collation-ordered id space and the numeric
 * group kernel folds integers, so no string comparison happens in the fold at all. That is
 * invisible from the answer alone, which is why each leg asserts on serving counters as well as on
 * the answer.
 *
 * <h2>What each leg establishes</h2>
 *
 * <ol>
 * <li>The fixture really is segment-scoped: {@code URL}, {@code Title} and {@code SearchPhrase} all
 * carry {@link ProjectionIndexRowGroupPage#COLUMN_KIND_STRING_SEGMENT} and the load sealed at least
 * one segment dictionary. Without this the queries would run over some other column kind and prove
 * nothing about this lane.</li>
 * <li>The query is SERVED by the group-aggregate route — a silent decline to the interpreter would
 * make the equality below vacuous.</li>
 * <li>Every string extremum went through a segment operand SEAL, i.e. through the canonicaliser, so
 * the {@code MIN}s were folded on canonical ids. q21 seals one operand ({@code URL}), q22 seals two
 * ({@code URL} and {@code Title}).</li>
 * <li>q22 additionally takes the {@code group-distinct} arm for its {@code COUNT(DISTINCT
 * UserID)}.</li>
 * <li>The served answer is byte-identical to the generic interpreter's over the same corpus. The
 * interpreter compares strings the ordinary way, so agreement is what pins the canonical id order
 * to real collation order.</li>
 * </ol>
 *
 * <p>
 * The query text is the SHIPPED ClickBench text ({@link ClickBenchQueries#byIndex(int)}), not a
 * hand-written approximation, so a query edit cannot drift away from what the campaign measures.
 */
public final class ClickBenchQ21Q22SegmentRouteEvidenceTest {

  /**
   * Enough generated hits that both predicates select a few thousand rows spread over dozens of
   * leaves, and the corpus still loads in seconds. The generator plants {@code google} hosts on ~15 %
   * of rows (a subset of those {@code .google.}) and {@code Google} titles on ~10 %, with
   * {@code SearchPhrase} non-empty on ~15 % — so q21 and q22 both select a non-trivial slice.
   */
  private static final int ROWS = 60_000;

  private static final int Q21 = 21;
  private static final int Q22 = 22;

  /** The three string columns q21/q22 read: the group key and the two extremum operands. */
  private static final String[] SEGMENT_STRING_COLUMNS = {"SearchPhrase", "Title", "URL"};

  private static final String GLOBAL_DICT_PROPERTY = "sirix.projection.globalDict";

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

    dbDir = Files.createTempDirectory("sirix-cb-q21q22-");
    try (BasicJsonDBStore store = BasicJsonDBStore.newBuilder()
                                                  .location(dbDir)
                                                  .buildPathSummary(true)
                                                  // FULL keeps one complete page per revision, so a
                                                  // converted page is never eagerly fragment-combined.
                                                  .versioningType(VersioningType.FULL)
                                                  // The rig's own load settings: the parallel bulk
                                                  // importer supports hashType=NONE only, and
                                                  // ClickBench reads one immutable snapshot, so
                                                  // per-insert node history is dead weight.
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
   * The precondition of both witnesses: the columns q21/q22 read really are segment-scoped, and the
   * load sealed a segment dictionary for them to be resolved against.
   */
  @Test
  void theHitsProjectionIsSegmentScopedForEveryStringColumnQ21AndQ22Read() throws Exception {
    try (var database = Databases.openJsonDatabase(dbDir.resolve(ClickBenchSchema.DATABASE));
        JsonResourceSession session = database.beginResourceSession(ClickBenchSchema.RESOURCE)) {
      final ProjectionIndexRegistry.Handle handle =
          ProjectionIndexCatalog.lookupCovering(session, session.getResourceConfig().getResource().toString(),
              session.getMostRecentRevisionNumber(), new String[] {"[]"}, SEGMENT_STRING_COLUMNS);
      assertNotNull(handle, "the ClickBench projection covers SearchPhrase/Title/URL");
      for (final String column : SEGMENT_STRING_COLUMNS) {
        assertEquals(ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SEGMENT,
            handle.columnKindOf(handle.columnOf(column)),
            "the lane must have converted " + column + " to the segment kind");
      }
      assertTrue(handle.segmentDictionarySegmentCount() >= 1, "the load sealed at least one segment dictionary");
      System.out.println("=== fixture: " + ROWS + " generated ClickBench hits, segment dictionary lane armed ===");
      System.out.println("segments sealed: " + handle.segmentDictionarySegmentCount());
      for (final String column : SEGMENT_STRING_COLUMNS) {
        System.out.println("column " + column + " kind=" + handle.columnKindOf(handle.columnOf(column))
            + " (COLUMN_KIND_STRING_SEGMENT)");
      }
    }
  }

  @Test
  void q21FoldsItsMinUrlOnCanonicalIdsAndAnswersLikeTheInterpreter() throws Exception {
    final long sealsBefore = SirixVectorizedExecutor.segmentOperandSealCount();

    final String interpreted = run(Q21, false);
    final String served = run(Q21, true, true);

    assertEquals(sealsBefore + 1, SirixVectorizedExecutor.segmentOperandSealCount(),
        "q21's MIN(URL) must be folded through exactly one sealed segment operand (canonical ids)");
    assertEquals(interpreted, served, "q21's served answer differs from the interpreter's");
    report(Q21, served);
  }

  @Test
  void q22FoldsTwoStringMinimaAndItsCountDistinctAndAnswersLikeTheInterpreter() throws Exception {
    final long sealsBefore = SirixVectorizedExecutor.segmentOperandSealCount();
    final long distinctBefore = SirixVectorizedExecutor.groupDistinctServedCount();

    final String interpreted = run(Q22, false);
    final String served = run(Q22, true, true);

    assertEquals(sealsBefore + 2, SirixVectorizedExecutor.segmentOperandSealCount(),
        "q22's MIN(URL) and MIN(Title) must each be folded through a sealed segment operand");
    assertTrue(SirixVectorizedExecutor.groupDistinctServedCount() > distinctBefore,
        "q22's COUNT(DISTINCT UserID) must take the group-distinct arm");
    assertEquals(interpreted, served, "q22's served answer differs from the interpreter's");
    report(Q22, served);
  }

  private static void report(final int index, final String result) {
    final ClickBenchQueries.Query query = ClickBenchQueries.byIndex(index);
    System.out.println("=== ClickBench q" + index + " over " + ROWS + " generated hits (segment-scoped columns) ===");
    System.out.println("SQL: " + query.sql());
    System.out.println("JSONiq:\n" + query.jsoniq());
    System.out.println("result (vectorized, byte-identical to the interpreter's):");
    System.out.println(result);
  }

  private static String run(final int index, final boolean vectorized) throws Exception {
    return run(index, vectorized, false);
  }

  private static String run(final int index, final boolean vectorized, final boolean requireServed) throws Exception {
    final String text = ClickBenchQueries.wrap(ClickBenchSchema.DATABASE, ClickBenchSchema.RESOURCE,
        ClickBenchQueries.byIndex(index).jsoniq());
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
        if (requireServed) {
          assertTrue(SirixVectorizedExecutor.groupAggServedCount() > groupAggBefore,
              "q" + index + " was NOT served by the group-aggregate route; the comparison would be vacuous");
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
