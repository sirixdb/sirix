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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Differential test harness for the predicate-tree path. Runs a pool of
 * randomized predicates through {@code executePredicateCount} and compares
 * every count against an in-memory ground truth. Any future fast-path
 * kernels (PAX-column SIMD, bytecode-compiled evaluators) must pass this
 * harness before being enabled — it catches the kind of off-by-epsilon
 * architectural bugs that the legacy shape-specific SIMD kernels had
 * (NumberRegion counts values, not records — they differ at page
 * boundaries).
 */
public final class PredicateTreeDifferentialTest {

  private static final int N = 200_000;
  private static final String DB = "pred-tree-diff-db";
  private static final String RES = "records.jn";
  private static final String[] DEPTS = { "Eng", "Sales", "Mkt", "Ops", "HR", "Finance", "Legal", "Supp" };
  private static final String[] CITIES = { "NYC", "LA", "SF", "ATL", "BOS", "CHI", "DEN", "DAL" };

  private Path dbDir;
  private int[] ages;
  private boolean[] active;
  private String[] dept;
  private String[] city;
  private int[] score;

  @BeforeEach
  void setUp() throws Exception {
    dbDir = Files.createTempDirectory("sirix-predtree-diff-");
    Random rng = new Random(0xC0FFEE);
    ages = new int[N];
    active = new boolean[N];
    dept = new String[N];
    city = new String[N];
    score = new int[N];

    StringBuilder sb = new StringBuilder(N * 80);
    sb.append('[');
    for (int i = 0; i < N; i++) {
      if (i > 0) sb.append(',');
      ages[i] = 18 + rng.nextInt(48);
      dept[i] = DEPTS[rng.nextInt(DEPTS.length)];
      city[i] = CITIES[rng.nextInt(CITIES.length)];
      active[i] = rng.nextBoolean();
      score[i] = rng.nextInt(1000);
      sb.append("{\"id\":").append(i)
        .append(",\"age\":").append(ages[i])
        .append(",\"score\":").append(score[i])
        .append(",\"dept\":\"").append(dept[i])
        .append("\",\"city\":\"").append(city[i])
        .append("\",\"active\":").append(active[i] ? "true" : "false")
        .append('}');
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

  @Disabled("heavy differential harness — shreds 200K records then runs 20 "
      + "randomised predicates. ~15 min wall time per run, not suitable for "
      + "the default :sirix-query:test suite. Enable manually via "
      + "--tests io.sirix.query.scan.PredicateTreeDifferentialTest when "
      + "validating kernel changes.")
  @Test
  void randomizedPredicates_matchGroundTruth() throws Exception {
    // 20 randomized predicate shapes covering: single NumCmp (every op),
    // NumCmp AND BoolRef, NumCmp AND StrEq, NumCmp AND NumCmp (same-field
    // range), NumCmp AND NumCmp (cross-field), triple conjunct, OR between
    // StrEqs, OR with NumCmp, NOT(NumCmp), all-false, all-true.
    final String[] cases = {
        "$u.age gt 40",
        "$u.age lt 25",
        "$u.age ge 50",
        "$u.age le 30",
        "$u.age eq 33",
        "$u.active",
        "$u.dept eq \"Eng\"",
        "$u.age gt 40 and $u.active",
        "$u.age gt 30 and $u.age lt 50",
        "$u.age gt 30 and $u.age lt 50 and $u.active",
        "$u.age gt 35 and $u.dept eq \"Eng\"",
        "$u.age gt 35 and $u.city eq \"NYC\"",
        "$u.age gt 40 and $u.score lt 500",
        "$u.city eq \"NYC\" or $u.city eq \"SF\"",
        "$u.age gt 60 or $u.age lt 20",
        "$u.dept eq \"Eng\" or $u.dept eq \"Sales\"",
        "$u.age gt 40 and ($u.city eq \"NYC\" or $u.city eq \"LA\")",
        "$u.age gt 50 and $u.active and $u.dept eq \"Eng\"",
        "$u.score gt 500 and $u.age lt 30",
        "$u.score gt 900 and $u.dept eq \"HR\" and $u.active"
    };

    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).buildPathSummary(true).build();
         var ctx = SirixQueryContext.createWithJsonStore(store);
         var chain = SirixCompileChain.createWithJsonStore(store)) {
      var coll = store.lookup(DB);
      var resourceSession = coll.getDatabase().beginResourceSession(RES);
      final int rev = resourceSession.getMostRecentRevisionNumber();
      try {
        var exec = new SirixVectorizedExecutor(resourceSession, rev);
        SequentialPipelineStrategy.setVectorizedExecutor(exec);
        try {
          for (final String predicate : cases) {
            final long expected = groundTruth(predicate);
            final String q = "count(for $u in jn:doc('" + DB + "','" + RES + "')[] where " + predicate + " return $u)";
            final Object result = new Query(chain, q).evaluate(ctx);
            final long actual = ((Int64) result).longValue();
            assertEquals(expected, actual, "count mismatch for predicate: " + predicate);
          }
        } finally {
          exec.close();
          SequentialPipelineStrategy.setVectorizedExecutor(null);
        }
      } finally {
        resourceSession.close();
      }
    }
  }

  /**
   * In-memory ground truth. Parses a narrow dialect that covers every shape
   * used in {@link #randomizedPredicates_matchGroundTruth}'s cases —
   * intentionally not a general XQuery interpreter, just enough to verify
   * executor output. Adding a new case to the test list requires extending
   * this.
   */
  private long groundTruth(final String predicate) {
    long c = 0;
    for (int i = 0; i < N; i++) {
      if (evalGt(predicate, i)) c++;
    }
    return c;
  }

  private boolean evalGt(final String p, final int i) {
    // Handle parenthesized disjunction inside an outer AND: split on ` and ` but
    // honor parens. Simple depth-aware split.
    final int and = topLevelAndIndex(p);
    if (and >= 0) {
      final String left = p.substring(0, and).trim();
      final String right = p.substring(and + 5).trim();
      return evalGt(left, i) && evalGt(right, i);
    }
    final int or = topLevelOrIndex(p);
    if (or >= 0) {
      final String left = p.substring(0, or).trim();
      final String right = p.substring(or + 4).trim();
      return evalGt(left, i) || evalGt(right, i);
    }
    // Strip parens.
    String q = p.trim();
    while (q.startsWith("(") && q.endsWith(")")) {
      q = q.substring(1, q.length() - 1).trim();
      // Re-check for top-level op inside the now-unwrapped expression.
      if (topLevelAndIndex(q) >= 0 || topLevelOrIndex(q) >= 0) return evalGt(q, i);
    }
    // Bare deref: `$u.active` → active[i] true
    if (q.equals("$u.active")) return active[i];
    // Comparison: `$u.FIELD OP VALUE`
    // Split on first space after $u.field.
    if (!q.startsWith("$u.")) throw new IllegalStateException("unknown leaf: " + q);
    final int firstSpace = q.indexOf(' ');
    final String field = q.substring(3, firstSpace);
    final int secondSpace = q.indexOf(' ', firstSpace + 1);
    final String op = q.substring(firstSpace + 1, secondSpace);
    final String rhs = q.substring(secondSpace + 1).trim();
    return switch (field) {
      case "age" -> numericOp(ages[i], op, Long.parseLong(rhs));
      case "score" -> numericOp(score[i], op, Long.parseLong(rhs));
      case "dept" -> "eq".equals(op) && dept[i].equals(unquote(rhs));
      case "city" -> "eq".equals(op) && city[i].equals(unquote(rhs));
      default -> throw new IllegalStateException("unknown field " + field);
    };
  }

  private int topLevelAndIndex(final String p) {
    return topLevelKeywordIndex(p, " and ");
  }

  private int topLevelOrIndex(final String p) {
    return topLevelKeywordIndex(p, " or ");
  }

  private int topLevelKeywordIndex(final String p, final String kw) {
    int depth = 0;
    for (int i = 0; i < p.length(); i++) {
      final char c = p.charAt(i);
      if (c == '(') depth++;
      else if (c == ')') depth--;
      else if (depth == 0 && p.regionMatches(i, kw, 0, kw.length())) return i;
    }
    return -1;
  }

  private boolean numericOp(final long v, final String op, final long t) {
    return switch (op) {
      case "gt" -> v > t;
      case "lt" -> v < t;
      case "ge" -> v >= t;
      case "le" -> v <= t;
      case "eq" -> v == t;
      default -> throw new IllegalStateException("unknown op " + op);
    };
  }

  private String unquote(final String s) {
    return s.startsWith("\"") && s.endsWith("\"") ? s.substring(1, s.length() - 1) : s;
  }
}
