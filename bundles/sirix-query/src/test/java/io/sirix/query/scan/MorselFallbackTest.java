package io.sirix.query.scan;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.brackit.query.Query;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Iter;
import io.brackit.query.jdm.Sequence;
import io.sirix.access.Databases;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Verifies that the Morsel fan-out wrapper is transparent: sequential (morsel-off) and
 * morsel-on execution produce identical tuple counts for a projection query that does
 * NOT match any vectorized fast path (projects a fresh object per tuple, which bypasses
 * {@code SirixVectorizedExecutor}'s aggregate/count/groupBy/orderBy paths).
 * <p>
 * Also logs wall-clock timings so morsel speedup can be eyeballed.
 */
public final class MorselFallbackTest {

  private static final int N = 20_000;
  private static final String DB = "morsel-fallback-db";
  private static final String RES = "records.jn";

  private Path dbDir;
  private int expectedBig;

  @BeforeEach
  void setUp() throws Exception {
    dbDir = Files.createTempDirectory("sirix-morsel-");
    final Random rng = new Random(42);
    final StringBuilder sb = new StringBuilder(N * 48);
    sb.append('[');
    int big = 0;
    for (int i = 0; i < N; i++) {
      if (i > 0) sb.append(',');
      final int age = 18 + rng.nextInt(48);
      if (age > 40) big++;
      sb.append("{\"id\":").append(i)
        .append(",\"age\":").append(age)
        .append('}');
    }
    sb.append(']');
    expectedBig = big;

    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).build();
         var ctx = SirixQueryContext.createWithJsonStore(store);
         var chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain, "jn:store('" + DB + "','" + RES + "','" + sb.toString().replace("'", "''") + "')")
          .evaluate(ctx);
    }
  }

  @AfterEach
  void tearDown() {
    // Leave the morsel toggle off for downstream tests — JVM-wide flag.
    SequentialPipelineStrategy.setMorselEnabled(false);
    if (dbDir != null) {
      Databases.removeDatabase(dbDir);
    }
  }

  /**
   * A projection-with-filter query — returns a new object literal per match. The
   * vectorized executor only recognises count / aggregate / group-by / order-by
   * shapes, so this query compiles through the generic operator pipeline and is
   * therefore a valid morsel-wrap target.
   */
  private static final String QUERY =
      "for $u in jn:doc('" + DB + "','" + RES + "')[] where $u.age gt 40 "
          + "return {\"id\": $u.id, \"tag\": \"big\"}";

  @Test
  void morselOffVsOn_sameCount() throws Exception {
    SequentialPipelineStrategy.setMorselEnabled(false);
    final long t0 = System.nanoTime();
    final long countOff = runCount(QUERY);
    final long offMs = (System.nanoTime() - t0) / 1_000_000L;

    SequentialPipelineStrategy.setMorselEnabled(true);
    final long t1 = System.nanoTime();
    final long countOn = runCount(QUERY);
    final long onMs = (System.nanoTime() - t1) / 1_000_000L;

    System.out.printf("[MorselFallbackTest] expected=%d  morselOff=%d (%d ms)  morselOn=%d (%d ms)%n",
        expectedBig, countOff, offMs, countOn, onMs);

    assertEquals(expectedBig, countOff, "sequential count mismatch");
    assertEquals(expectedBig, countOn, "morsel-wrapped count mismatch");
    assertEquals(countOff, countOn, "morsel on/off counts must match");
  }

  private long runCount(final String query) throws Exception {
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).build();
         var ctx = SirixQueryContext.createWithJsonStore(store);
         var chain = SirixCompileChain.createWithJsonStore(store)) {
      final Sequence res = new Query(chain, query).evaluate(ctx);
      long n = 0;
      final Iter it = res.iterate();
      try {
        Item item;
        while ((item = it.next()) != null) {
          n++;
        }
      } finally {
        it.close();
      }
      return n;
    }
  }
}
