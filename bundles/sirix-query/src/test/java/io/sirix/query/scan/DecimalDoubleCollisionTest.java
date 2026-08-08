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
 *
 * <h2>Homogeneous is not enough</h2>
 * A corpus in which every value is inexact is served by the exact-decimal column and agrees for the
 * wrong reason: it never exercises the CHOICE of column. The mixed corpus below is the one that
 * does. {@code 1000.25} is dyadic, so its double image round-trips; if the collector asks "is this
 * value exact as a double?" instead of "what TYPE is this value?", the two prices land in different
 * camps, the tag stops being all-decimal, and it is encoded as ordinary ALP over double images —
 * where both prices are the same number.
 */
public final class DecimalDoubleCollisionTest {

  private static final String DB = "decimal-collision-db";
  private static final String MIXED_DB = "decimal-collision-mixed-db";
  private static final String RES = "records.jn";

  /** Strictly above the threshold in decimal space; identical to it as a double. */
  private static final String COLLIDING = "1000.25000000000001";
  private static final String THRESHOLD = "1000.25";

  private static final int N = 2000;

  private Path dbDir;

  @BeforeEach
  void setUp() throws Exception {
    dbDir = Files.createTempDirectory("sirix-decimal-collision-");
    final StringBuilder homogeneous = new StringBuilder();
    homogeneous.append('[');
    final StringBuilder mixed = new StringBuilder();
    mixed.append('[');
    for (int i = 0; i < N; i++) {
      if (i > 0) {
        homogeneous.append(',');
        mixed.append(',');
      }
      // Every record carries the colliding value, so the miscount is the whole corpus rather than
      // one row lost in rounding noise.
      homogeneous.append("{\"id\":").append(i).append(",\"price\":").append(COLLIDING).append('}');
      // Alternating: half the tag is exact as a double, half is not. Both are decimals.
      mixed.append("{\"id\":").append(i)
           .append(",\"price\":").append(i % 2 == 0 ? THRESHOLD : COLLIDING).append('}');
    }
    homogeneous.append(']');
    mixed.append(']');
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).buildPathSummary(true).build();
         var ctx = SirixQueryContext.createWithJsonStore(store);
         var chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain, "jn:store('" + DB + "','" + RES + "','" + homogeneous + "')").evaluate(ctx);
      new Query(chain, "jn:store('" + MIXED_DB + "','" + RES + "','" + mixed + "')").evaluate(ctx);
    }
  }

  @AfterEach
  void tearDown() {
    SequentialPipelineStrategy.setVectorizedExecutor(null);
    if (dbDir != null) {
      Databases.removeDatabase(dbDir.resolve(DB));
      Databases.removeDatabase(dbDir.resolve(MIXED_DB));
    }
  }

  @Test
  @DisplayName("a decimal sharing the threshold's double is not miscounted by the column path")
  void collidingDecimalIsNotMiscounted() throws Exception {
    final String predicate = "$u.price gt " + THRESHOLD;
    // Ground truth is decimal semantics: 1000.25000000000001 > 1000.25 for all 2000 records.
    assertEquals(N, count(DB, predicate, false), "record path must compare decimals exactly");
    assertEquals(N, count(DB, predicate, true),
                 "column path disagrees: a decimal above the threshold was answered through its "
                     + "double image, which equals the threshold");
  }

  @Test
  @DisplayName("a tag mixing an exact-as-double decimal with an inexact one is still exact")
  void mixedExactAndInexactDecimalsAgree() throws Exception {
    final String predicate = "$u.price gt " + THRESHOLD;
    // Only the odd records are strictly above the threshold in decimal space.
    assertEquals(N / 2, count(MIXED_DB, predicate, false),
                 "record path must compare decimals exactly");
    assertEquals(N / 2, count(MIXED_DB, predicate, true),
                 "column path disagrees: the tag mixes an exact-as-double decimal with an inexact "
                     + "one, so it was encoded over DOUBLE IMAGES — where both prices are the same "
                     + "number. The stored TYPE, not one value's exactness, must pick the column");
  }

  @Test
  @DisplayName("equality over the mixed tag separates the two prices as well")
  void mixedTagEqualityAgrees() throws Exception {
    final String predicate = "$u.price eq " + COLLIDING;
    assertEquals(N / 2, count(MIXED_DB, predicate, false),
                 "record path must compare decimals exactly");
    assertEquals(N / 2, count(MIXED_DB, predicate, true),
                 "column path disagrees: equality over a double image cannot separate two decimals "
                     + "that share it");
  }

  private long count(final String database, final String predicate, final boolean regionOnly)
      throws Exception {
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).buildPathSummary(true).build();
         var ctx = SirixQueryContext.createWithJsonStore(store);
         var chain = SirixCompileChain.createWithJsonStore(store)) {
      final var coll = store.lookup(database);
      final var resourceSession = coll.getDatabase().beginResourceSession(RES);
      try {
        final var exec =
            new SirixVectorizedExecutor(resourceSession, resourceSession.getMostRecentRevisionNumber());
        exec.setRegionOnlyCountEnabled(regionOnly);
        SequentialPipelineStrategy.setVectorizedExecutor(exec);
        try {
          return ((Int64) new Query(chain,
                                    "count(for $u in jn:doc('" + database + "','" + RES + "')[] where "
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
