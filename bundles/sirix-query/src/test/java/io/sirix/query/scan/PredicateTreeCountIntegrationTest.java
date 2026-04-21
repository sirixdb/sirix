package io.sirix.query.scan;

import io.brackit.query.Query;
import io.brackit.query.atomic.Int64;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Iter;
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
  void filteredGroupBy_age_gt_40_by_dept() throws Exception {
    java.util.Map<String, Long> expected = new java.util.HashMap<>();
    for (int i = 0; i < N; i++) {
      if (ages[i] > 40) expected.merge(dept[i], 1L, Long::sum);
    }
    java.util.Map<String, Long> actual = runFilteredGroupByCount(
        "for $u in jn:doc('" + DB + "','" + RES + "')[] where $u.age gt 40 let $d := $u.dept group by $d return { \"dept\": $d, \"count\": count($u) }",
        "dept");
    assertEquals(expected, actual, "filtered group-by count mismatch");
  }

  private java.util.Map<String, Long> runFilteredGroupByCount(final String query, final String keyField)
      throws Exception {
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
          Sequence res = new Query(chain, query).evaluate(ctx);
          java.util.Map<String, Long> out = new java.util.HashMap<>();
          Iter it = res.iterate();
          try {
            Item item;
            while ((item = it.next()) != null) {
              if (item instanceof io.brackit.query.jsonitem.object.ArrayObject ao) {
                String k = ao.get(new io.brackit.query.atomic.QNm(keyField)).toString();
                long c = ((Int64) ao.get(new io.brackit.query.atomic.QNm("count"))).longValue();
                out.put(k, c);
              }
            }
          } finally { it.close(); }
          return out;
        } finally {
          exec.close();
          SequentialPipelineStrategy.setVectorizedExecutor(null);
        }
      } finally {
        resourceSession.close();
      }
    }
  }

  @Test
  void orPredicate_city_NYC_or_SF() throws Exception {
    long expected = countWhere(i -> "NYC".equals(city[i]) || "SF".equals(city[i]));
    long actual = runFilterCount("count(for $u in jn:doc('" + DB + "','" + RES + "')[] "
        + "where $u.city eq \"NYC\" or $u.city eq \"SF\" return $u)");
    assertEquals(expected, actual, "OR count mismatch");
  }

  /**
   * Column-batched evaluator correctness sweep. Runs each supported predicate
   * shape against the default ({@code -Dsirix.vec.batchGenericEval=true}) path
   * and asserts counts match in-memory ground truth, exercising
   * {@code EvalBatch}, {@code collectColumns}, and {@code evalCompiledBatch}
   * end-to-end for every opcode (NUM_CMP, STR_EQ, BOOL_REF, AND, OR).
   * The path toggle is read at class-init time (static final) so we cannot
   * flip it per-test without a forked JVM; running with
   * {@code -Dsirix.vec.batchGenericEval=false} exercises the legacy path via
   * the same test body.
   */
  @Test
  void batchEvaluator_allShapesCorrect() throws Exception {
    final long[][] expected = new long[][] {
        { countWhere(i -> ages[i] > 40) },
        { countWhere(i -> ages[i] > 40 && active[i]) },
        { countWhere(i -> ages[i] > 30 && ages[i] < 50 && active[i]) },
        { countWhere(i -> "NYC".equals(city[i])) },
        { countWhere(i -> ages[i] > 35 && "Eng".equals(dept[i])) },
        { countWhere(i -> "NYC".equals(city[i]) || "SF".equals(city[i])) },
    };
    final String[] queries = new String[] {
        "count(for $u in jn:doc('" + DB + "','" + RES + "')[] where $u.age gt 40 return $u)",
        "count(for $u in jn:doc('" + DB + "','" + RES + "')[] where $u.age gt 40 and $u.active return $u)",
        "count(for $u in jn:doc('" + DB + "','" + RES + "')[] "
            + "where $u.age gt 30 and $u.age lt 50 and $u.active return $u)",
        "count(for $u in jn:doc('" + DB + "','" + RES + "')[] where $u.city eq \"NYC\" return $u)",
        "count(for $u in jn:doc('" + DB + "','" + RES + "')[] "
            + "where $u.age gt 35 and $u.dept eq \"Eng\" return $u)",
        "count(for $u in jn:doc('" + DB + "','" + RES + "')[] "
            + "where $u.city eq \"NYC\" or $u.city eq \"SF\" return $u)",
    };
    for (int q = 0; q < queries.length; q++) {
      final long actual = runFilterCount(queries[q]);
      assertEquals(expected[q][0], actual, "shape[" + q + "] count mismatch");
    }
  }

  /**
   * Randomized fuzz sweep: generate 10 predicate shapes mixing NumCmp, StrEq,
   * BoolRef, AND, and OR, and assert the default pipeline (which exercises
   * the generated {@link SirixVectorizedExecutor.BatchPredicate} class) returns
   * the same count as in-memory ground truth. This effectively compares the
   * compiled path to the interpreter because the shapes' correctness is
   * independent of path — any divergence between them surfaces here first.
   */
  @Test
  void compiledPredicateMatchesInterpreter() throws Exception {
    final Random rng = new Random(12345);
    final String[] ops = { "gt", "lt", "ge", "le", "eq" };
    for (int t = 0; t < 10; t++) {
      // Build a random 2-or-3-term AND/OR predicate.
      final int kind = rng.nextInt(3);
      final int ageLit = 18 + rng.nextInt(48);
      final String op = ops[rng.nextInt(ops.length)];
      final String city1 = CITIES[rng.nextInt(CITIES.length)];
      final String city2 = CITIES[rng.nextInt(CITIES.length)];
      final String dept1 = DEPTS[rng.nextInt(DEPTS.length)];

      final String predStr;
      final IntPredicate expectedPred;
      if (kind == 0) {
        predStr = "$u.age " + op + " " + ageLit + " and $u.active";
        expectedPred = i -> cmp(ages[i], op, ageLit) && active[i];
      } else if (kind == 1) {
        predStr = "$u.city eq \"" + city1 + "\" or $u.city eq \"" + city2 + "\"";
        expectedPred = i -> city1.equals(city[i]) || city2.equals(city[i]);
      } else {
        predStr = "$u.age " + op + " " + ageLit + " and $u.dept eq \"" + dept1 + "\"";
        expectedPred = i -> cmp(ages[i], op, ageLit) && dept1.equals(dept[i]);
      }
      final long expected = countWhere(expectedPred);
      final long actual = runFilterCount("count(for $u in jn:doc('" + DB + "','" + RES + "')[] where "
          + predStr + " return $u)");
      assertEquals(expected, actual, "randomized predicate shape " + t + " (" + predStr + ") mismatch");
    }
  }

  private static boolean cmp(final int v, final String op, final int lit) {
    return switch (op) {
      case "gt" -> v > lit;
      case "lt" -> v < lit;
      case "ge" -> v >= lit;
      case "le" -> v <= lit;
      case "eq" -> v == lit;
      default -> false;
    };
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
