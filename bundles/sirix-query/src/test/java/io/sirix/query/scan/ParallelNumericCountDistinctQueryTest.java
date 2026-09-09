package io.sirix.query.scan;

import io.brackit.query.Query;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.util.serialize.StringSerializer;
import io.sirix.access.Databases;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end witness for the parallel numeric count-distinct arms: a projection of 100,000 rows
 * (past the row floor below which the count stays on one thread) with a WIDE column (a 52-bit value
 * per row — the shared hash-set arm), a NARROW one (the per-worker bitset arm) and a narrow one
 * MISSING on every other row. Every answer must equal the truth kept while the rows were written
 * and the interpreter's, and the parallel-arm counter must have moved — the one-thread arms give
 * the same answers, so the answers alone would not notice a fan-out that never engaged.
 */
final class ParallelNumericCountDistinctQueryTest {
  private static final String DB = "parallel-count-distinct-db";
  private static final String RES = "records.jn";
  private static final String DOC = "jn:doc('" + DB + "','" + RES + "')[]";
  /** 98 leaves of 1,024 rows: several slices per worker, well past the 32,768-row floor. */
  private static final int N = 100_000;

  private Path dbDir;
  private String previousGlobalDictMode;
  private long wideTruth;
  private long narrowTruth;
  private long mixedTruth;

  @BeforeEach
  void setUp() throws Exception {
    previousGlobalDictMode = System.getProperty("sirix.projection.globalDict");
    System.setProperty("sirix.projection.globalDict", "never");
    SirixVectorizedExecutor.STRICT_SERVING = true;
    dbDir = Files.createTempDirectory("sirix-parallel-count-distinct-");
    final Random random = new Random(20260903L);
    final LongOpenHashSet wide = new LongOpenHashSet(N);
    final LongOpenHashSet narrow = new LongOpenHashSet();
    final LongOpenHashSet mixed = new LongOpenHashSet();
    final StringBuilder sb = new StringBuilder(N * 64);
    sb.append('[');
    for (int i = 0; i < N; i++) {
      if (i > 0) {
        sb.append(',');
      }
      // Two thirds of the rows repeat one of 30,000 wide values, the rest are fresh 52-bit values:
      // 56k distinct over a span of 2^52 — the hash arm, with real repeats to dedupe.
      final long w = (i % 3 == 0)
          ? random.nextLong() >>> 12
          : 1_000_000_000_000L + random.nextInt(30_000) * 7_919L;
      final long n = random.nextInt(50_000) - 25_000L;
      wide.add(w);
      narrow.add(n);
      sb.append("{\"id\":").append(i).append(",\"wide\":").append(w).append(",\"narrow\":").append(n);
      if ((i & 1) == 0) {
        final long m = random.nextInt(3_000);
        mixed.add(m);
        sb.append(",\"m\":").append(m);
      }
      sb.append('}');
    }
    sb.append(']');
    wideTruth = wide.size();
    narrowTruth = narrow.size();
    mixedTruth = mixed.size();
    assertTrue(wideTruth > 50_000L, "the wide column has the cardinality the hash arm is for: " + wideTruth);
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain, "jn:store('" + DB + "','" + RES + "','" + sb + "')").evaluate(ctx);
      new Query(chain, """
          let $doc := jn:doc('%s','%s')
          let $stats := jn:create-projection-index($doc, '/[]',
            ('/[]/id', '/[]/wide', '/[]/narrow', '/[]/m'),
            ('long', 'long', 'long', 'long'))
          return sdb:commit($doc)
          """.formatted(DB, RES)).evaluate(ctx);
    }
  }

  @AfterEach
  void tearDown() {
    if (previousGlobalDictMode == null) {
      System.clearProperty("sirix.projection.globalDict");
    } else {
      System.setProperty("sirix.projection.globalDict", previousGlobalDictMode);
    }
    SirixVectorizedExecutor.STRICT_SERVING = false;
    SequentialPipelineStrategy.setVectorizedExecutor(null);
    if (dbDir != null) {
      Databases.removeDatabase(dbDir.resolve(DB));
    }
  }

  @Test
  void aWideColumnIsCountedByTheSharedSetOnEveryWorker() throws Exception {
    assertCountDistinct("wide", wideTruth);
  }

  @Test
  void aNarrowColumnIsCountedByPerWorkerBitsets() throws Exception {
    assertCountDistinct("narrow", narrowTruth);
  }

  @Test
  void missingRowsAreNotAValue() throws Exception {
    assertCountDistinct("m", mixedTruth);
  }

  private void assertCountDistinct(final String field, final long truth) throws Exception {
    final String query = "count(distinct-values(for $h in " + DOC + " return $h." + field + "))";
    final String generic = run(query, false);
    assertEquals(Long.toString(truth), generic.trim(), "the interpreter disagrees with the truth");
    final long servedBefore = SirixVectorizedExecutor.projectionCountDistinctServedCount();
    final long parallelBefore = SirixVectorizedExecutor.projectionCountDistinctNumericParallelServedCount();
    final String vectorized = run(query, true);
    assertEquals(generic, vectorized, "the projection arm diverges from the interpreter");
    assertEquals(servedBefore + 1, SirixVectorizedExecutor.projectionCountDistinctServedCount(),
        "not served from the projection");
    assertEquals(parallelBefore + 1, SirixVectorizedExecutor.projectionCountDistinctNumericParallelServedCount(),
        "the parallel arm never engaged for " + field);
  }

  private String run(final String query, final boolean vectorized) throws Exception {
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = vectorized
            ? SirixCompileChain.createWithJsonStore(store)
            : SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store)) {
      SirixVectorizedExecutor exec = null;
      try {
        if (vectorized) {
          final var db = Databases.openJsonDatabase(dbDir.resolve(DB));
          final JsonResourceSession session = db.beginResourceSession(RES);
          exec = new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber());
          SequentialPipelineStrategy.setVectorizedExecutor(exec);
        }
        final Sequence result = new Query(chain, query).execute(ctx);
        final StringWriter out = new StringWriter();
        try (PrintWriter pw = new PrintWriter(out)) {
          new StringSerializer(pw).serialize(result);
        }
        return out.toString();
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        if (exec != null) {
          exec.close();
        }
      }
    }
  }
}
