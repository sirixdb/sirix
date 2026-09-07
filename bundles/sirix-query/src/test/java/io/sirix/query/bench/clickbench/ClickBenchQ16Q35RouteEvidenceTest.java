/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.query.bench.clickbench;

import com.google.gson.stream.JsonReader;
import io.brackit.query.Query;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.util.serialize.StringSerializer;
import io.sirix.access.Databases;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.projection.ProjectionBulkLoad;
import io.sirix.index.projection.ProjectionIndexCatalog;
import io.sirix.index.projection.ProjectionIndexRegistry;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.scan.SirixVectorizedExecutor;
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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end route witness for the two campaign levers, exercised through the SHIPPED ClickBench
 * query text rather than a hand-written approximation of it.
 *
 * <p>
 * Q35 ({@code GROUP BY ClientIP, ClientIP - 1, ClientIP - 2, ClientIP - 3}) is the shape the
 * dependent-numeric-key fold exists for: four components of one {@code NUMERIC_LONG} column. Q16
 * ({@code GROUP BY UserID, SearchPhrase}) is the composite string/number shape it must NOT touch —
 * it is documentation-only in this change, so its route has to stay exactly where it was.
 *
 * <p>
 * Both queries are answered twice over the same projected resource: once through the vectorized
 * executor and once through the generic interpreter. Equality alone would be vacuous if the fast
 * path silently declined, so each leg also asserts on the serving counters — the group-aggregate
 * route must have served the vectorized leg, and the fold counter must move for Q35 and stand still
 * for Q16.
 */
public final class ClickBenchQ16Q35RouteEvidenceTest {

  /** Enough hits for several thousand distinct ClientIP groups and a real partitioned merge. */
  private static final int ROWS = 20_000;

  private static final int Q16 = 16;
  private static final int Q35 = 35;

  private static Path dbDir;

  @BeforeAll
  static void loadProjectedHits() throws Exception {
    dbDir = Files.createTempDirectory("sirix-cb-q16q35-");
    try (BasicJsonDBStore store = BasicJsonDBStore.newBuilder().location(dbDir).buildPathSummary(true).build();
        Reader source = ClickBenchSource.open("generate:" + ROWS + ":42");
        JsonReader reader = new JsonReader(source)) {
      store.create(ClickBenchSchema.DATABASE, ClickBenchSchema.RESOURCE, reader, ClickBenchProjection.spec(ROWS));
    }
  }

  @AfterAll
  static void tearDown() throws IOException {
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    ProjectionBulkLoad.clearActive();
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

  @Test
  void q35FoldsItsDependentClientIpKeysAndStillAnswersLikeTheInterpreter() throws Exception {
    final long foldsBefore = SirixVectorizedExecutor.offsetCountGroupsRewriteCount();

    final String interpreted = run(Q35, false);
    final String served = run(Q35, true, true);

    assertEquals(foldsBefore + 1, SirixVectorizedExecutor.offsetCountGroupsRewriteCount(),
        "ClickBench Q35 must take the dependent-numeric-key fold exactly once");
    assertEquals(interpreted, served, "Q35's folded answer differs from the interpreter's");
    report(Q35, served);
  }

  @Test
  void q16KeepsItsCompositeRouteAndAnswersLikeTheInterpreter() throws Exception {
    final long foldsBefore = SirixVectorizedExecutor.offsetCountGroupsRewriteCount();

    final String interpreted = run(Q16, false);
    final String served = run(Q16, true, true);

    assertEquals(foldsBefore, SirixVectorizedExecutor.offsetCountGroupsRewriteCount(),
        "ClickBench Q16's composite (UserID, SearchPhrase) key is not a same-column offset shape");
    assertEquals(interpreted, served, "Q16's composite answer differs from the interpreter's");
    report(Q16, served);
  }

  private static void report(final int index, final String result) {
    final ClickBenchQueries.Query query = ClickBenchQueries.byIndex(index);
    System.out.println("=== ClickBench q" + index + " over " + ROWS + " generated hits ===");
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
