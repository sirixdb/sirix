package io.sirix.query.scan;

import io.brackit.query.Query;
import io.brackit.query.atomic.Int64;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.sirix.access.Databases;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A threshold too large to express at an exact-decimal tag's scale must saturate by its own SIGN.
 *
 * <p>Comparing against an {@code ENC_DEC} tag means lifting the threshold to the tag's scale, and a
 * threshold far outside the tag's range overflows that multiplication. The saturating answer is
 * fixed by where the threshold SITS, not by which side of the interval is being computed: a bound
 * above every stored value is {@code +inf} for both sides — nothing is {@code >=} it, everything is
 * {@code <=} it — and one below every stored value is {@code -inf} for both.
 *
 * <p>Choosing the extreme by side instead gets two of the four combinations backwards, and both
 * wrong ones read as "no row matches" on a predicate every row satisfies. A count-only test on
 * ordinary thresholds cannot see it, because ordinary thresholds never overflow the lift.
 *
 * <p>The corpus is at scale 8 so the lift from a scale-1 literal is {@code 10^7}: any unscaled
 * threshold past {@code Long.MAX_VALUE / 10^7 ≈ 9.2e11} overflows, which
 * {@code 100000000000.5} does.
 */
@DisplayName("decimal bound saturation")
final class DecimalBoundSaturationTest {

  private static final int N = 2_000;
  private static final String DB = "decimal-bound-db";
  private static final String RES = "records.jn";

  private Path dbDir;

  @BeforeEach
  void setUp() throws Exception {
    dbDir = Files.createTempDirectory("sirix-decimal-bound-");
    final StringBuilder sb = new StringBuilder(N * 48);
    sb.append('[');
    for (int i = 0; i < N; i++) {
      if (i > 0) {
        sb.append(',');
      }
      // Scale 8, narrow range: an exact-decimal tag that packs, and whose every value is far below
      // the thresholds below.
      sb.append("{\"id\":").append(i)
        .append(",\"price\":10.").append(String.format("%08d", 1 + i)).append('}');
    }
    sb.append(']');
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).buildPathSummary(true).build();
         var ctx = SirixQueryContext.createWithJsonStore(store);
         var chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain, "jn:store('" + DB + "','" + RES + "','" + sb + "')").evaluate(ctx);
    }
  }

  @AfterEach
  void tearDown() {
    SequentialPipelineStrategy.setVectorizedExecutor(null);
    if (dbDir != null) {
      Databases.removeDatabase(dbDir.resolve(DB));
    }
  }

  @Test
  @DisplayName("an unrepresentably HIGH upper bound admits every row")
  void unrepresentablyHighUpperBoundAdmitsEveryRow() throws Exception {
    for (final String predicate : new String[] { "$u.price le 100000000000.5",
                                                 "$u.price lt 100000000000.5" }) {
      assertEquals(N, count(predicate, false), "record path: every row is below the threshold");
      assertEquals(N, count(predicate, true),
                   "column path returned a short count for " + predicate
                       + " — the lift overflowed and saturated to -inf on an upper bound");
    }
  }

  @Test
  @DisplayName("an unrepresentably LOW lower bound admits every row")
  void unrepresentablyLowLowerBoundAdmitsEveryRow() throws Exception {
    for (final String predicate : new String[] { "$u.price ge -100000000000.5",
                                                 "$u.price gt -100000000000.5" }) {
      assertEquals(N, count(predicate, false), "record path: every row is above the threshold");
      assertEquals(N, count(predicate, true),
                   "column path returned a short count for " + predicate
                       + " — the lift overflowed and saturated to +inf on a lower bound");
    }
  }

  @Test
  @DisplayName("the sides that already saturated correctly still exclude every row")
  void unsatisfiableSidesStillExcludeEveryRow() throws Exception {
    for (final String predicate : new String[] { "$u.price ge 100000000000.5",
                                                 "$u.price le -100000000000.5" }) {
      assertEquals(0L, count(predicate, false), "record path: no row can satisfy this");
      assertEquals(0L, count(predicate, true), "column path over-counted for " + predicate);
    }
  }

  private long count(final String predicate, final boolean regionOnly) throws Exception {
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).buildPathSummary(true).build();
         var ctx = SirixQueryContext.createWithJsonStore(store);
         var chain = SirixCompileChain.createWithJsonStore(store)) {
      final var coll = store.lookup(DB);
      final var resourceSession = coll.getDatabase().beginResourceSession(RES);
      try {
        final var exec =
            new SirixVectorizedExecutor(resourceSession, resourceSession.getMostRecentRevisionNumber());
        exec.setRegionOnlyCountEnabled(regionOnly);
        SequentialPipelineStrategy.setVectorizedExecutor(exec);
        try {
          return ((Int64) new Query(chain,
                                    "count(for $u in jn:doc('" + DB + "','" + RES + "')[] where "
                                        + predicate + " return $u)").evaluate(ctx)).longValue();
        } finally {
          exec.close();
          SequentialPipelineStrategy.setVectorizedExecutor(null);
        }
      } finally {
        resourceSession.close();
      }
    }
  }
}
