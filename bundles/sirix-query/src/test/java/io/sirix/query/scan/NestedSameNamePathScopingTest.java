package io.sirix.query.scan;

import io.brackit.query.Query;
import io.brackit.query.atomic.Int64;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.brackit.query.jdm.Sequence;
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

/**
 * Regression test for the nested-same-name correctness bug. Each record has a
 * top-level {@code age} and a nested {@code pet.age}. Queries over
 * {@code $u.age} must count only top-level ages — nested {@code pet.age}
 * values at a different tree depth must not contaminate the result.
 *
 * <p>Before the pathNodeKey-scoping fix, the shape-specific kernels and the
 * path-summary short-circuit both indexed by local name only and produced
 * incorrect totals on this dataset. The refactor in
 * {@link SirixVectorizedExecutor#resolveTargetPathNodeKey(String[], String)}
 * plus path-filtered slot scans gives correct answers here.
 */
public final class NestedSameNamePathScopingTest {

  private static final int N = 20_000;
  private static final String DB = "nested-same-name-db";
  private static final String RES = "records.jn";
  private static final String[] DEPTS = { "Eng", "Sales", "Mkt", "Ops", "HR" };

  private Path dbDir;
  private int[] topAge;
  private int[] nestedAge;
  private boolean[] active;
  private String[] dept;

  @BeforeEach
  void setUp() throws Exception {
    dbDir = Files.createTempDirectory("sirix-nested-same-name-");
    final Random rng = new Random(0xABCDEF);
    topAge = new int[N];
    nestedAge = new int[N];
    active = new boolean[N];
    dept = new String[N];

    final StringBuilder sb = new StringBuilder(N * 96);
    sb.append('[');
    for (int i = 0; i < N; i++) {
      if (i > 0) sb.append(',');
      topAge[i] = 20 + rng.nextInt(50);         // 20..69
      nestedAge[i] = 1 + rng.nextInt(15);       // 1..15 (pet age)
      active[i] = rng.nextBoolean();
      dept[i] = DEPTS[rng.nextInt(DEPTS.length)];
      sb.append("{\"id\":").append(i)
        .append(",\"age\":").append(topAge[i])
        .append(",\"active\":").append(active[i] ? "true" : "false")
        .append(",\"dept\":\"").append(dept[i])
        .append("\",\"pet\":{\"age\":").append(nestedAge[i])
        .append(",\"dept\":\"").append(DEPTS[rng.nextInt(DEPTS.length)])
        .append("\"}}");
    }
    sb.append(']');

    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).buildPathSummary(true).build();
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
  void filterCount_topLevel_age_gt_40_not_contaminated_by_pet_age() throws Exception {
    long expected = 0L;
    for (int i = 0; i < N; i++) if (topAge[i] > 40) expected++;
    final long actual = runFilterCount("count(for $u in jn:doc('" + DB + "','" + RES + "')[] "
        + "where $u.age gt 40 return $u)");
    assertEquals(expected, actual, "top-level $u.age filter must not count nested pet.age");
  }

  @Test
  void filterCount_topLevel_age_gt_40_AND_active() throws Exception {
    long expected = 0L;
    for (int i = 0; i < N; i++) if (topAge[i] > 40 && active[i]) expected++;
    final long actual = runFilterCount("count(for $u in jn:doc('" + DB + "','" + RES + "')[] "
        + "where $u.age gt 40 and $u.active return $u)");
    assertEquals(expected, actual, "nested pet.age must not leak into top-level AND predicate");
  }

  @Test
  void filterCount_topLevel_dept_eq_Eng_not_contaminated_by_pet_dept() throws Exception {
    long expected = 0L;
    for (int i = 0; i < N; i++) if ("Eng".equals(dept[i])) expected++;
    final long actual = runFilterCount("count(for $u in jn:doc('" + DB + "','" + RES + "')[] "
        + "where $u.dept eq \"Eng\" return $u)");
    assertEquals(expected, actual, "top-level $u.dept filter must not count nested pet.dept");
  }

  private long runFilterCount(final String query) throws Exception {
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).buildPathSummary(true).build();
         var ctx = SirixQueryContext.createWithJsonStore(store);
         var chain = SirixCompileChain.createWithJsonStore(store)) {
      final var coll = store.lookup(DB);
      final var resourceSession = coll.getDatabase().beginResourceSession(RES);
      final int rev = resourceSession.getMostRecentRevisionNumber();
      try {
        final var exec = new SirixVectorizedExecutor(resourceSession, rev);
        SequentialPipelineStrategy.setVectorizedExecutor(exec);
        try {
          final Sequence res = new Query(chain, query).evaluate(ctx);
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
}
