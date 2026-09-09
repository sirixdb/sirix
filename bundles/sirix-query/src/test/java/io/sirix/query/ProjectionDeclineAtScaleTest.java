package io.sirix.query;

import io.brackit.query.Query;
import io.brackit.query.atomic.Int64;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.sirix.access.Databases;
import io.sirix.index.projection.ProjectionIndexRegistry;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.scan.SirixVectorizedExecutor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A projection route that DECLINES must fall back at every corpus size.
 *
 * <h2>The bug this pins</h2>
 *
 * <p>
 * The projection kernels signal "not mine to serve" by throwing {@link IllegalStateException} — a
 * group column that is not a string dictionary is one such case — and the callers catch exactly
 * that to compile the generic pipeline instead. The parallel drivers ran each chunk on the worker
 * pool and wrapped whatever came back in a plain {@code RuntimeException}, which put the decline
 * where no caller looks: the fallback became a failed query.
 *
 * <p>
 * What makes it worth a test of this size is WHERE it hid. Below 64 row groups those drivers run
 * the scan inline, and the decline propagates unwrapped and is caught. So the same projection, the
 * same query and the same declining kernel answer correctly on a small corpus and throw on a large
 * one — the shape of bug a suite of five-record fixtures cannot see. Found on a 3.48M-record
 * corpus, where {@code count(... group by $year)} against a projection carrying a numeric
 * {@code year} column failed outright where it used to answer in 1.5 ms.
 *
 * <p>
 * The corpus is therefore deliberately past one row group per 1024 records × 64 — anything smaller
 * passes without the fix and proves nothing.
 */
final class ProjectionDeclineAtScaleTest {

  /**
   * Past 64 row groups at 1024 rows each, which is where the parallel driver takes over from the
   * inline scan. Below that the decline propagates unwrapped and this test cannot fail.
   */
  private static final int N = 100_000;

  private static final String DB = "projection-decline-db";
  private static final String RES = "records.jn";

  private Path dbDir;

  @BeforeEach
  void setUp() throws Exception {
    // The registry is a JVM-wide static keyed by resource path; a surviving entry from another
    // test would serve another corpus's leaves for this one.
    ProjectionIndexRegistry.clear();
    dbDir = Files.createTempDirectory("sirix-projection-decline-");
    final StringBuilder sb = new StringBuilder(N * 28);
    sb.append('[');
    for (int i = 0; i < N; i++) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append("{\"year\":").append(1900 + i % 120).append(",\"title\":\"t").append(i % 997).append("\"}");
    }
    sb.append(']');
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain, "jn:store('" + DB + "','" + RES + "','" + sb + "')").evaluate(ctx);
      // A NUMERIC column beside a string one: the group-by kernel serves string dictionaries and
      // declines the numeric column, which is the decline this test rides.
      new Query(chain,
          "let $doc := jn:doc('" + DB + "','" + RES + "')" + " let $i := jn:create-projection-index($doc, '/[]',"
              + " ('/[]/year', '/[]/title'), ('long', 'string'))" + " return sdb:commit($doc)").evaluate(ctx);
    }
  }

  @AfterEach
  void tearDown() {
    ProjectionIndexRegistry.clear();
    SequentialPipelineStrategy.setVectorizedExecutor(null);
    if (dbDir != null) {
      Databases.removeDatabase(dbDir.resolve(DB));
    }
  }

  @Test
  @DisplayName("a group-by emitting key/count pairs over a numeric column answers")
  void numericGroupByAnswers() throws Exception {
    // 120 distinct years by construction, so 120 groups. The decline-preservation contract this
    // corpus was originally built for is stated directly in ProjectionWorkerFailureTest: every
    // query shape that reached the throwing kernel has since grown a route of its own, so an
    // end-to-end assertion of it would pass while proving nothing.
    assertEquals(120L, countItems("for $m in jn:doc('" + DB + "','" + RES + "')[]" + " let $y := $m.year group by $y"
        + " return {\"year\": $y, \"n\": count($m)}"));
  }

  @Test
  @DisplayName("count-distinct over a numeric column IS served, exactly")
  void numericCountDistinctIsServedFromTheProjection() throws Exception {
    // The other side of the same column kind. Every string route needs a dictionary, so this used
    // to leave the projection and rescan the corpus — invisible warm, where a memo answered, and
    // the whole document one-shot.
    final long servedBefore = SirixVectorizedExecutor.projectionCountDistinctServedCount();
    assertEquals(120L,
        count("count(for $m in jn:doc('" + DB + "','" + RES + "')[]" + " let $y := $m.year group by $y return $y)"));
    assertTrue(SirixVectorizedExecutor.projectionCountDistinctServedCount() > servedBefore,
        "a correct answer is not route evidence: the projection count-distinct outcome counter must move");
  }

  @Test
  @DisplayName("the shapes the projection DOES serve still answer, on the same corpus")
  void servedShapesStillAnswer() throws Exception {
    // Guards the fix from the other side: making declines fall back must not turn a served shape
    // into a fallback. Years cycle 1900..2019, so `> 1969` takes 50 of every 120 records; 100,000
    // records are 833 whole cycles plus a 40-record tail that contributes none.
    assertEquals(833L * 50L,
        count("count(for $m in jn:doc('" + DB + "','" + RES + "')[]" + " where $m.year > 1969 return $m)"));
  }

  /**
   * How many ITEMS a query emits. The group-by shape above returns records rather than a scalar, and
   * what it must not do is fail; counting the emitted groups is the cheapest way to say that while
   * still checking the answer.
   */
  private long countItems(final String query) throws Exception {
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      final var result = new Query(chain, query).evaluate(ctx);
      if (result instanceof io.brackit.query.jdm.Sequence sequence) {
        long items = 0;
        try (final var iter = sequence.iterate()) {
          while (iter.next() != null) {
            items++;
          }
        }
        return items;
      }
      return result == null
          ? 0
          : 1;
    }
  }

  private long count(final String query) throws Exception {
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      return ((Int64) new Query(chain, query).evaluate(ctx)).longValue();
    }
  }
}
