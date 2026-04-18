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
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises the generic predicate-tree path end-to-end. Writes a deterministic
 * dataset (seeded RNG), then runs filter-count queries through the Brackit
 * optimizer → {@code SirixVectorizedExecutor.executePredicateCount} path.
 * Counts must match ground truth computed from the in-memory array.
 */
public final class PredicateTreeCountIntegrationTest {

  private static final int N = 20_000;
  private static final String DB = "pred-tree-ct-db";
  private static final String RES = "records.jn";
  private static final String[] DEPTS = { "Eng", "Sales", "Mkt", "Ops", "HR" };
  private static final String[] CITIES = { "NYC", "LA", "SF", "ATL", "BOS" };

  private Path dbDir;
  private int[] ages;
  private boolean[] active;
  private String[] dept;
  private String[] city;

  @BeforeEach
  void setUp() throws Exception {
    dbDir = Files.createTempDirectory("sirix-predtree-");
    Random rng = new Random(42);
    ages = new int[N];
    active = new boolean[N];
    dept = new String[N];
    city = new String[N];

    StringBuilder sb = new StringBuilder(N * 64);
    sb.append('[');
    for (int i = 0; i < N; i++) {
      if (i > 0) sb.append(',');
      ages[i] = 18 + rng.nextInt(48);
      dept[i] = DEPTS[rng.nextInt(DEPTS.length)];
      city[i] = CITIES[rng.nextInt(CITIES.length)];
      active[i] = rng.nextBoolean();
      sb.append("{\"id\":").append(i)
        .append(",\"age\":").append(ages[i])
        .append(",\"dept\":\"").append(dept[i])
        .append("\",\"city\":\"").append(city[i])
        .append("\",\"active\":").append(active[i] ? "true" : "false")
        .append('}');
    }
    sb.append(']');

    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).build();
         var ctx = SirixQueryContext.createWithJsonStore(store);
         var chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain, "jn:store('" + DB + "','" + RES + "','" + sb.toString().replace("'", "''") + "')")
          .evaluate(ctx);
    }
  }

  @AfterEach
  void tearDown() {
    SequentialPipelineStrategy.setVectorizedExecutor(null);
    if (dbDir != null) Databases.removeDatabase(dbDir);
  }

  @Test
  void singleNumCmp_age_gt_40() throws Exception {
    long expected = countWhere(i -> ages[i] > 40);
    long actual = runFilterCount("count(for $u in jn:doc('" + DB + "','" + RES + "')[] where $u.age gt 40 return $u)");
    assertEquals(expected, actual, "single NumCmp count mismatch");
  }

  @Test
  void andBoolConjunct_age_gt_40_and_active() throws Exception {
    long expected = countWhere(i -> ages[i] > 40 && active[i]);
    long actual = runFilterCount("count(for $u in jn:doc('" + DB + "','" + RES + "')[] "
        + "where $u.age gt 40 and $u.active return $u)");
    assertEquals(expected, actual, "NumCmp AND BoolRef count mismatch");
  }

  @Test
  void rangeAndBool_age_between_and_active() throws Exception {
    long expected = countWhere(i -> ages[i] > 30 && ages[i] < 50 && active[i]);
    long actual = runFilterCount("count(for $u in jn:doc('" + DB + "','" + RES + "')[] "
        + "where $u.age gt 30 and $u.age lt 50 and $u.active return $u)");
    assertEquals(expected, actual, "range AND BoolRef count mismatch");
  }

  @Test
  void stringEquality_city_eq_NYC() throws Exception {
    long expected = countWhere(i -> "NYC".equals(city[i]));
    long actual = runFilterCount("count(for $u in jn:doc('" + DB + "','" + RES + "')[] "
        + "where $u.city eq \"NYC\" return $u)");
    assertEquals(expected, actual, "string equality count mismatch");
  }

  @Test
  void crossFieldAnd_numeric_and_string() throws Exception {
    long expected = countWhere(i -> ages[i] > 35 && "Eng".equals(dept[i]));
    long actual = runFilterCount("count(for $u in jn:doc('" + DB + "','" + RES + "')[] "
        + "where $u.age gt 35 and $u.dept eq \"Eng\" return $u)");
    assertEquals(expected, actual, "NumCmp AND StrEq count mismatch");
  }

  @Test
  void orPredicate_city_NYC_or_SF() throws Exception {
    long expected = countWhere(i -> "NYC".equals(city[i]) || "SF".equals(city[i]));
    long actual = runFilterCount("count(for $u in jn:doc('" + DB + "','" + RES + "')[] "
        + "where $u.city eq \"NYC\" or $u.city eq \"SF\" return $u)");
    assertEquals(expected, actual, "OR count mismatch");
  }

  private long runFilterCount(final String query) throws Exception {
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).build();
         var ctx = SirixQueryContext.createWithJsonStore(store);
         var chain = SirixCompileChain.createWithJsonStore(store)) {
      var coll = store.lookup(DB);
      var resourceSession = coll.getDatabase().beginResourceSession(RES);
      final int rev = resourceSession.getMostRecentRevisionNumber();
      try {
        var exec = new SirixVectorizedExecutor(resourceSession, rev);
        SequentialPipelineStrategy.setVectorizedExecutor(exec);
        try {
          var res = new Query(chain, query).evaluate(ctx);
          assertTrue(res instanceof Int64, "expected Int64 count, got " + res);
          return ((Int64) res).longValue();
        } finally {
          exec.close();
          SequentialPipelineStrategy.setVectorizedExecutor(null);
        }
      } finally {
        resourceSession.close();
      }
    }
  }

  @FunctionalInterface
  private interface IntPredicate {
    boolean test(int i);
  }

  private long countWhere(final IntPredicate p) {
    long c = 0;
    for (int i = 0; i < N; i++) if (p.test(i)) c++;
    return c;
  }
}
