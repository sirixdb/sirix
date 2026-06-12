package io.sirix.query.scan;

import io.brackit.query.Query;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.util.serialize.StringSerializer;
import io.sirix.access.Databases;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Differential gate for the vectorized group-by paths: every query runs through
 * the interpreted pipeline AND the vectorized executor; the serialized results
 * (row-order normalized) must be IDENTICAL. Covers what the scale bench cannot:
 * numeric / boolean / double-typed group keys (historically returned EMPTY —
 * the kernels required string values), multi-key grouping, query-renamed output
 * fields, and predicated variants.
 */
public final class TypedGroupByDifferentialTest {

  // Prime record count: every integer-avg division is non-terminating, so any
  // double-vs-xs:decimal divergence in aggregate emission surfaces immediately.
  private static final int N = 1_999;
  private static final String DB = "typed-gb-db";
  private static final String RES = "records.jn";
  private static final String SRC = "jn:doc('" + DB + "','" + RES + "')[]";
  private static final String[] DEPTS = { "Eng", "Sales", "Mkt", "Ops" };
  private static final String[] CITIES = { "NYC", "LA", "SF" };

  private Path dbDir;

  @BeforeEach
  void setUp() throws Exception {
    dbDir = Files.createTempDirectory("sirix-typed-gb-");
    final Random rng = new Random(7);
    final StringBuilder sb = new StringBuilder(N * 96);
    sb.append('[');
    for (int i = 0; i < N; i++) {
      if (i > 0)
        sb.append(',');
      final String dept = DEPTS[rng.nextInt(DEPTS.length)];
      final String city = CITIES[rng.nextInt(CITIES.length)];
      final int age = 18 + rng.nextInt(8);
      final double score = (rng.nextInt(7) + 1) / 2.0; // 0.5 .. 3.5 — non-integral doubles
      final boolean active = rng.nextBoolean();
      sb.append("{\"id\":").append(i)
        .append(",\"dept\":\"").append(dept)
        .append("\",\"city\":\"").append(city)
        .append("\",\"age\":").append(age)
        .append(",\"score\":").append(score)
        .append(",\"active\":").append(active)
        // "amount" hashes NEGATIVE (like "active") — regression coverage for the
        // nameKey-sentinel family ('< 0' treated legitimate negative hashes as missing).
        .append(",\"amount\":").append(rng.nextInt(1000));
      sb.append('}');
    }
    sb.append(']');

    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).build();
         var ctx = SirixQueryContext.createWithJsonStore(store);
         var chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain, "jn:store('" + DB + "','" + RES + "','" + sb + "')").evaluate(ctx);
    }
  }

  @AfterEach
  void tearDown() {
    SequentialPipelineStrategy.setVectorizedExecutor(null);
    if (dbDir != null)
      Databases.removeDatabase(dbDir);
  }

  // ==================== single-key, typed values ====================

  @Test
  void stringKeyCanonical() throws Exception {
    assertDifferential("for $u in " + SRC + " let $d := $u.dept group by $d "
        + "return {\"dept\": $d, \"count\": count($u)}");
  }

  @Test
  void intKeyCanonical() throws Exception {
    // Historically the vectorized kernel required STRING group values and
    // silently returned an EMPTY sequence for numeric keys.
    assertDifferential("for $u in " + SRC + " let $a := $u.age group by $a "
        + "return {\"age\": $a, \"count\": count($u)}");
  }

  @Test
  void booleanKeyCanonical() throws Exception {
    assertDifferential("for $u in " + SRC + " let $b := $u.active group by $b "
        + "return {\"active\": $b, \"count\": count($u)}");
  }

  @Test
  void doubleKeyCanonical() throws Exception {
    assertDifferential("for $u in " + SRC + " let $s := $u.score group by $s "
        + "return {\"score\": $s, \"count\": count($u)}");
  }

  @Test
  void renamedStringKey() throws Exception {
    assertDifferential("for $u in " + SRC + " let $d := $u.dept group by $d "
        + "return {\"d\": $d, \"n\": count($u)}");
  }

  // ==================== multi-key ====================

  @Test
  void twoStringKeys() throws Exception {
    assertDifferential("for $u in " + SRC + " let $d := $u.dept, $c := $u.city group by $d, $c "
        + "return {\"d\": $d, \"c\": $c, \"n\": count($u)}");
  }

  @Test
  void stringAndIntKeys() throws Exception {
    assertDifferential("for $u in " + SRC + " let $d := $u.dept, $a := $u.age group by $d, $a "
        + "return {\"dept\": $d, \"age\": $a, \"count\": count($u)}");
  }

  @Test
  void threeKeysMixedTypes() throws Exception {
    assertDifferential("for $u in " + SRC + " let $d := $u.dept, $b := $u.active, $a := $u.age "
        + "group by $d, $b, $a return {\"d\": $d, \"b\": $b, \"a\": $a, \"n\": count($u)}");
  }

  @Test
  void returnOrderDiffersFromSpecOrder() throws Exception {
    assertDifferential("for $u in " + SRC + " let $d := $u.dept, $c := $u.city group by $d, $c "
        + "return {\"city\": $c, \"dept\": $d, \"count\": count($u)}");
  }

  // ==================== predicated ====================

  @Test
  void predicatedTwoKeys() throws Exception {
    assertDifferential("for $u in " + SRC + " where $u.active let $d := $u.dept, $c := $u.city "
        + "group by $d, $c return {\"d\": $d, \"c\": $c, \"n\": count($u)}");
  }

  @Test
  void predicatedIntKey() throws Exception {
    assertDifferential("for $u in " + SRC + " where $u.age gt 20 let $a := $u.age group by $a "
        + "return {\"age\": $a, \"count\": count($u)}");
  }

  @Test
  void numCmpPredicateMultiKey() throws Exception {
    assertDifferential("for $u in " + SRC + " where $u.age gt 19 and $u.active "
        + "let $d := $u.dept, $a := $u.age group by $d, $a "
        + "return {\"d\": $d, \"a\": $a, \"n\": count($u)}");
  }

  // ==================== negative-hash nameKey regressions ====================
  // nameKeys are String hashes; 'active' and 'amount' hash NEGATIVE. The scan
  // kernels treated `nameKey < 0` as the missing-field sentinel (-1), silently
  // emptying group-by / count-distinct / aggregates over such fields.

  @Test
  void countDistinctOverNegativeHashField() throws Exception {
    assertDifferential("count(for $u in " + SRC + " let $b := $u.active group by $b return $b)");
  }

  @Test
  void sumOverNegativeHashField() throws Exception {
    assertDifferential("sum(for $u in " + SRC + " return $u.amount)");
  }

  @Test
  void sumOverDoubleField() throws Exception {
    // Probes the aggregate path's number typing: score holds non-integral doubles.
    assertDifferential("sum(for $u in " + SRC + " return $u.score)");
  }

  @Test
  void predicatedSumOverDoubleField() throws Exception {
    assertDifferential("sum(for $u in " + SRC + " where $u.active return $u.score)");
  }

  @Test
  void predicatedAvgOverIntField() throws Exception {
    assertDifferential("avg(for $u in " + SRC + " where $u.age gt 20 return $u.age)");
  }

  @Test
  void avgOverIntField() throws Exception {
    // Integer avg is xs:decimal — must match the generic pipeline digit-for-digit.
    assertDifferential("avg(for $u in " + SRC + " return $u.age)");
  }

  @Test
  void groupByNegativeHashIntField() throws Exception {
    assertDifferential("for $u in " + SRC + " let $a := $u.amount group by $a "
        + "return {\"amount\": $a, \"count\": count($u)}");
  }

  // ==================== projection-backed paths ====================
  // Installing the wildcard projection on this resource routes the vectorized
  // runs through the columnar fast paths (incl. the multi-key composite
  // kernel); interpreted results remain the oracle. The registry is keyed by
  // the per-test temp resource, so no cross-test leakage.

  @Test
  void projectionTwoStringKeys() throws Exception {
    assertDifferentialWithProjection("for $u in " + SRC + " let $d := $u.dept, $c := $u.city group by $d, $c "
        + "return {\"d\": $d, \"c\": $c, \"n\": count($u)}");
  }

  @Test
  void projectionPredicatedTwoStringKeys() throws Exception {
    assertDifferentialWithProjection("for $u in " + SRC + " where $u.age gt 20 and $u.active "
        + "let $d := $u.dept, $c := $u.city group by $d, $c "
        + "return {\"d\": $d, \"c\": $c, \"n\": count($u)}");
  }

  @Test
  void projectionRenamedSingleKeyViaMultiPath() throws Exception {
    assertDifferentialWithProjection("for $u in " + SRC + " let $d := $u.dept group by $d "
        + "return {\"d\": $d, \"n\": count($u)}");
  }

  @Test
  void projectionMixedTypeKeysFallBack() throws Exception {
    // age is NUMERIC_LONG in the projection — the composite kernel requires
    // STRING_DICT, so this must fall back to the typed kernel and stay correct.
    assertDifferentialWithProjection("for $u in " + SRC + " let $d := $u.dept, $a := $u.age group by $d, $a "
        + "return {\"dept\": $d, \"age\": $a, \"count\": count($u)}");
  }

  @Test
  void projectionSumOverIntField() throws Exception {
    assertDifferentialWithProjection("sum(for $u in " + SRC + " return $u.amount)");
  }

  @Test
  void projectionAvgOverIntField() throws Exception {
    assertDifferentialWithProjection("avg(for $u in " + SRC + " return $u.age)");
  }

  @Test
  void projectionSumOverDoubleField() throws Exception {
    // score is non-integral: the builder truncates doubles into NUMERIC_LONG, so
    // the integrality gate MUST decline and fall back to the typed double path.
    assertDifferentialWithProjection("sum(for $u in " + SRC + " return $u.score)");
  }

  @Test
  void projectionPredicatedSumOverIntField() throws Exception {
    assertDifferentialWithProjection("sum(for $u in " + SRC + " where $u.active return $u.amount)");
  }

  private void assertDifferentialWithProjection(final String query) throws Exception {
    final String interpreted = normalize(run(query, false));
    final String vectorized = normalize(runWithProjection(query));
    assertEquals(interpreted, vectorized, "projection-backed vectorized result differs for: " + query);
  }

  private String runWithProjection(final String query) throws Exception {
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).build();
         var ctx = SirixQueryContext.createWithJsonStore(store);
         var chain = SirixCompileChain.createWithJsonStore(store)) {
      final var db = Databases.openJsonDatabase(dbDir.resolve(DB));
      final var session = db.beginResourceSession(RES);
      SirixVectorizedExecutor exec = null;
      try {
        io.sirix.query.bench.ScaleBenchProjectionSetupAccess.installWildcard(session);
        exec = new SirixVectorizedExecutor(session, session.getMostRecentRevisionNumber());
        SequentialPipelineStrategy.setVectorizedExecutor(exec);
        final Sequence result = new Query(chain, query).execute(ctx);
        final StringWriter out = new StringWriter();
        try (PrintWriter pw = new PrintWriter(out)) {
          new StringSerializer(pw).serialize(result);
        }
        return out.toString();
      } finally {
        SequentialPipelineStrategy.setVectorizedExecutor(null);
        if (exec != null) exec.close();
      }
    }
  }

  // ==================== harness ====================

  private void assertDifferential(final String query) throws Exception {
    final String interpreted = normalize(run(query, false));
    final String vectorized = normalize(run(query, true));
    assertEquals(interpreted, vectorized, "vectorized result differs from interpreted for: " + query);
  }

  private String run(final String query, final boolean vectorized) throws Exception {
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).build();
         var ctx = SirixQueryContext.createWithJsonStore(store);
         var chain = SirixCompileChain.createWithJsonStore(store)) {
      SirixVectorizedExecutor exec = null;
      try {
        if (vectorized) {
          final var db = Databases.openJsonDatabase(dbDir.resolve(DB));
          final var session = db.beginResourceSession(RES);
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

  /** Group emission order is engine-specific — compare as sorted record lines. */
  private static String normalize(final String s) {
    return s.replace("} {", "}\n{").lines().map(String::strip).filter(l -> !l.isEmpty()).sorted()
            .reduce("", (a, b) -> a + "\n" + b);
  }
}
