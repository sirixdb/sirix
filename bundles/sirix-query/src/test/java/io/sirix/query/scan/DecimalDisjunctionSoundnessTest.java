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
 * A DISJUNCTION branch whose decimal threshold has no faithful double image must not be answered
 * from double bounds either.
 *
 * <p>The single-interval fold refuses that substitution deliberately: a decimal literal whose own
 * double image is inexact carries {@code doublesServable = false}, the plan's double bounds become
 * {@code NaN}, and the page keeps its record path unless an exact-decimal column can decide it. The
 * branch fold computed the same flag and then discarded it — every branch was published as servable
 * — so the disjunction was served from bounds rounded to the nearest double, which is exactly the
 * substitution the single-interval path exists to prevent.
 *
 * <h2>The corpus is the test</h2>
 * Two prices, and both are load-bearing. {@code 19.75} is a decimal (a JSON number with a decimal
 * point and no exponent is a {@code BigDecimal}) and it is dyadic, so its double image IS the
 * decimal; {@code 2.55e1} carries an exponent and round-trips, so it is stored as a genuine
 * {@code double}. A tag holding both is not all-decimal, so it cannot use the exact-decimal column,
 * and every one of its decimals round-trips, so it is admitted to ALP over DOUBLE IMAGES. That is
 * the one encoding where a rounded threshold decides a stored decimal.
 *
 * <p>The threshold is then chosen to sit inside {@code 19.75}'s ulp: {@code 19.750000000000000001}
 * is strictly greater than {@code 19.75} in decimal space, and its nearest double is {@code 19.75}
 * itself. The record path compares the stored {@code BigDecimal} against the exact literal and
 * counts the row; a column path folding {@code lt} to {@code nextDown(19.75)} does not. Its scale
 * (18) is past {@link io.sirix.page.pax.DoubleRegion#MAX_DECIMAL_SCALE}, so no exact-decimal
 * interval can rescue the branch either — the only sound answer is to decline the page.
 *
 * <p>A corpus of genuine doubles alone cannot see this: the record path promotes a decimal literal
 * to its double image for a double-typed row, so both paths round identically and agree for the
 * wrong reason. The stored value has to be a decimal, and the tag has to be encoded over images.
 */
public final class DecimalDisjunctionSoundnessTest {

  private static final int N = 2000;
  private static final String DB = "decimal-disjunction-db";
  private static final String RES = "records.jn";

  /** A decimal, dyadic, so its double image is exactly itself and the tag stays ALP-over-images. */
  private static final String EXACT_DECIMAL = "19.75";

  /** A genuine double: the exponent is what makes the shredder keep it as one. */
  private static final String GENUINE_DOUBLE = "2.55e1";

  /** Above {@link #EXACT_DECIMAL} in decimal space; rounds to it as a double. Scale 18. */
  private static final String INEXACT_THRESHOLD = "19.750000000000000001";

  private Path dbDir;

  @BeforeEach
  void setUp() throws Exception {
    dbDir = Files.createTempDirectory("sirix-decimal-disjunction-");
    final StringBuilder sb = new StringBuilder(N * 48);
    sb.append('[');
    for (int i = 0; i < N; i++) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append("{\"id\":").append(i).append(",\"price\":")
        .append(i % 2 == 0 ? GENUINE_DOUBLE : EXACT_DECIMAL).append('}');
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
  @DisplayName("a disjunction branch with an inexact decimal literal is not served from its image")
  void inexactBranchIsNotServedFromItsDoubleImage() throws Exception {
    final String predicate =
        "$u.price lt " + INEXACT_THRESHOLD + " or $u.price gt 500.05";
    // Decimal semantics: 19.75 < 19.750000000000000001 for every odd record, and no price is
    // above 500.05.
    assertEquals(N / 2, count(predicate, false), "record path must compare decimals exactly");
    assertEquals(N / 2, count(predicate, true),
                 "column path disagrees: the branch's double bounds were folded from the "
                     + "THRESHOLD'S ROUNDED IMAGE and then trusted, so a stored decimal strictly "
                     + "below the threshold fell outside them");
  }

  @Test
  @DisplayName("the same threshold as a single interval already refuses — the branch must match")
  void theSingleIntervalFormAgreesToo() throws Exception {
    final String predicate = "$u.price lt " + INEXACT_THRESHOLD;
    assertEquals(N / 2, count(predicate, false), "record path must compare decimals exactly");
    assertEquals(N / 2, count(predicate, true),
                 "the single-interval fold's guard against answering an inexact decimal threshold "
                     + "in double space no longer holds");
  }

  @Test
  @DisplayName("an exactly representable threshold is still served, so the guard is not a blanket")
  void anExactThresholdIsStillAnswered() throws Exception {
    // 19.875 is dyadic: its double image is the decimal, so both domains agree and the branch
    // stays servable. Without this the fix could be "refuse every decimal" and pass above.
    final String predicate = "$u.price lt 19.875 or $u.price gt 500.5";
    assertEquals(N / 2, count(predicate, false), "record path must compare decimals exactly");
    assertEquals(N / 2, count(predicate, true), "column path disagrees for an exact threshold");
  }

  private long count(final String predicate, final boolean regionOnly) throws Exception {
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).buildPathSummary(true).build();
         var ctx = SirixQueryContext.createWithJsonStore(store);
         var chain = SirixCompileChain.createWithJsonStore(store)) {
      final var coll = store.lookup(DB);
      final var resourceSession = coll.getDatabase().beginResourceSession(RES);
      try {
        final var exec = new SirixVectorizedExecutor(resourceSession,
                                                     resourceSession.getMostRecentRevisionNumber());
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
