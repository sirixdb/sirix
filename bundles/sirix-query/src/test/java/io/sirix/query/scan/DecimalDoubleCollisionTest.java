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
 * A decimal that is strictly greater than the threshold but whose NEAREST DOUBLE equals it must not
 * be answered from a double-domain column.
 *
 * <p>{@code 1000.25000000000001} is greater than {@code 1000.25} in decimal space, and both map to
 * the same {@code double}: at that magnitude one ulp is about {@code 1.1e-13}, so a scale-14
 * decimal one step above the threshold sits well inside half an ulp. Its unscaled value
 * ({@code 100025000000000001}) fits a {@code long} and its scale is 14, so an admission test
 * bounded only by "unscaled fits a long, scale &lt;= 14" lets it into the column.
 *
 * <p>Once it is there, the answer comes from the ALP kernel, whose bounds are derived from the
 * DOUBLE threshold ({@code DoubleRegionSimd.fixLowerBound} computes {@code dlo * 10^e / 10^f} in
 * floating point). Two decimals sharing a double image are therefore indistinguishable to it, and
 * the row is counted on the wrong side. The record path compares the stored BigDecimal exactly and
 * gets it right, which is what this test pins: the two paths must agree.
 */
public final class DecimalDoubleCollisionTest {

  private static final String DB = "decimal-collision-db";
  private static final String RES = "records.jn";

  /** Strictly above the threshold in decimal space; identical to it as a double. */
  private static final String COLLIDING = "1000.25000000000001";
  private static final String THRESHOLD = "1000.25";

  private Path dbDir;

  @BeforeEach
  void setUp() throws Exception {
    dbDir = Files.createTempDirectory("sirix-decimal-collision-");
    final StringBuilder sb = new StringBuilder();
    sb.append('[');
    for (int i = 0; i < 2000; i++) {
      if (i > 0) {
        sb.append(',');
      }
      // Every record carries the colliding value, so the miscount is the whole corpus rather than
      // one row lost in rounding noise.
      sb.append("{\"id\":").append(i).append(",\"price\":").append(COLLIDING).append('}');
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
  @DisplayName("a decimal sharing the threshold's double is not miscounted by the column path")
  void collidingDecimalIsNotMiscounted() throws Exception {
    final String predicate = "$u.price gt " + THRESHOLD;
    // Ground truth is decimal semantics: 1000.25000000000001 > 1000.25 for all 2000 records.
    assertEquals(2000L, count(predicate, false), "record path must compare decimals exactly");
    assertEquals(2000L, count(predicate, true),
                 "column path disagrees: a decimal above the threshold was answered through its "
                     + "double image, which equals the threshold");
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
