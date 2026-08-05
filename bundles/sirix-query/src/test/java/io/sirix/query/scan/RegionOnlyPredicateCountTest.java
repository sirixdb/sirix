package io.sirix.query.scan;

import io.brackit.query.Query;
import io.brackit.query.atomic.Int64;
import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.optimizer.VectorizedScanAnnotation;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.sirix.access.Databases;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Differential test for the column-only predicate count: every predicate is answered twice — once
 * from the PAX regions without materializing a record, once through the record path — and the two
 * answers must agree with each other <em>and</em> with an in-memory ground truth.
 *
 * <p>The corpus is built to break the fast path where it is breakable, not only where it is fast:
 *
 * <ul>
 *   <li>{@code year} is an integer on most records but a <em>double</em> on a few. Doubles never
 *       enter the number region, so on any page holding one the region's tag count falls short of
 *       the page's slot count and the page must fall back. If the oracle were missing, those
 *       records would silently vanish from the count.</li>
 *   <li>{@code note} is present on only a fraction of records, so the anchor slot count and the
 *       record count differ.</li>
 *   <li>{@code title} repeats, so the string dictionary has real duplicates to count, and one
 *       queried literal occurs nowhere at all.</li>
 * </ul>
 */
public final class RegionOnlyPredicateCountTest {

  private static final int N = 20_000;
  private static final String DB = "region-only-count-db";
  private static final String RES = "records.jn";
  private static final String[] TITLES = { "Alpha", "Beta", "Gamma", "Delta", "Epsilon" };

  private Path dbDir;
  private long[] year;
  private boolean[] yearIsDouble;
  private String[] title;
  private int[] note;       // -1 == field absent on that record
  private boolean[] active;
  private boolean[] yearAbsent;  // year removed by a later revision

  @BeforeEach
  void setUp() throws Exception {
    dbDir = Files.createTempDirectory("sirix-region-only-count-");
    final Random rng = new Random(0x5EEDL);
    year = new long[N];
    yearIsDouble = new boolean[N];
    title = new String[N];
    note = new int[N];
    active = new boolean[N];
    yearAbsent = new boolean[N];

    final StringBuilder sb = new StringBuilder(N * 64);
    sb.append('[');
    for (int i = 0; i < N; i++) {
      if (i > 0) sb.append(',');
      year[i] = 1900 + rng.nextInt(124);
      title[i] = TITLES[rng.nextInt(TITLES.length)];
      note[i] = rng.nextInt(4) == 0 ? rng.nextInt(100) : -1;
      // Every 997th record stores year as a double — enough to land on many, but not all, pages.
      yearIsDouble[i] = i % 997 == 0;
      sb.append("{\"id\":").append(i).append(",\"year\":");
      if (yearIsDouble[i]) {
        sb.append(year[i]).append(".5");
      } else {
        sb.append(year[i]);
      }
      sb.append(",\"title\":\"").append(title[i]).append('"');
      active[i] = rng.nextInt(3) != 0;
      sb.append(",\"active\":").append(active[i]);
      if (note[i] >= 0) {
        sb.append(",\"note\":").append(note[i]);
      }
      sb.append('}');
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

  /**
   * A fractional threshold in a FUSED conjunction folds into the long interval and serves
   * double-free pages; double-bearing pages fail the completeness oracle and keep the record path.
   *
   * <p>{@code $u.year gt 1990.5 and $u.active} used to serve zero pages — the planner rejected the
   * fractional leaf outright. Folding {@code gt 1990.5} to longs {@code >= 1991} is sound
   * precisely because the fused kernel requires the long column to cover EVERY row: no double is
   * ever judged by the folded interval, it falls back instead. Both counters are pinned — served
   * for the clean pages, fallbacks for the double-bearing ones — and the count stays exact against
   * the absolute oracle either way.
   */
  @Test
  void fractionalFusedConjunctionServesDoubleFreePages() throws Exception {
    final String predicate = "$u.year gt 1990.5 and $u.active";
    final long expected = groundTruth(predicate);
    assertEquals(expected, count(predicate, false), "record path: " + predicate);
    SirixVectorizedExecutor.resetRegionOnlyCounters();
    assertEquals(expected, count(predicate, true), "column path: " + predicate);
    assertTrue(SirixVectorizedExecutor.regionOnlyPagesServed() > 0,
               "double-free pages must serve the folded fractional conjunction");
    assertTrue(SirixVectorizedExecutor.regionOnlyPageFallbacks() > 0,
               "double-bearing pages must keep the record path — the folded interval must never "
                   + "judge a double");
    assertTrue(expected > 0, "predicate matches nothing, so it proves nothing");
  }

  /**
   * A single-field numeric disjunction is answered as disjoint intervals summed per page.
   *
   * <p>{@code year lt 1950 or year gt 1990} has a sound anchor — both branches exclude records
   * missing {@code year} — so it reaches the executor, where the old planner folded conjunctions
   * only and sent every OR to per-record evaluation. The IN-list shape ({@code eq or eq}) and a
   * branch that is itself a conjunction ride the same fold; overlapping branches must collapse to
   * ONE interval and still be right, since double-counting the overlap is the natural bug.
   */
  @Test
  void numericDisjunctionsAreAnsweredAsDisjointIntervals() throws Exception {
    final String[] predicates = {
        "$u.year lt 1950 or $u.year gt 1990",
        "$u.year eq 1950 or $u.year eq 1990 or $u.year eq 2010",
        // Branches overlap: [1961..] u [..1980] covers everything — one merged interval.
        "$u.year gt 1960 or $u.year lt 1980",
    };
    for (final String predicate : predicates) {
      final long expected = groundTruth(predicate);
      assertEquals(expected, count(predicate, false), "record path: " + predicate);
      SirixVectorizedExecutor.resetRegionOnlyCounters();
      assertEquals(expected, count(predicate, true), "column path: " + predicate);
      assertTrue(SirixVectorizedExecutor.regionOnlyPagesServed() > 0,
                 "no page served from the number column for " + predicate + " (served="
                     + SirixVectorizedExecutor.regionOnlyPagesServed() + ", fellBack="
                     + SirixVectorizedExecutor.regionOnlyPageFallbacks() + ')');
      assertTrue(expected > 0, "predicate matches nothing, so it proves nothing: " + predicate);
    }
  }

  /**
   * The fused kernel must decline a page whose columns it cannot prove aligned.
   *
   * <p>{@code note} is present on only a quarter of the records, so on every page the note column
   * enumerates a subset of the records while {@code active} enumerates all of them — position
   * {@code i} of one is not position {@code i} of the other. The alignment certificate fails, the
   * page goes through the records, and the answer must still be right. Without the certificate this
   * predicate would pair each note with whichever record's flag happened to sit at that offset.
   */
  @Test
  void sparseFieldBreaksTheAlignmentAndFallsBack() throws Exception {
    final String predicate = "$u.note gt 50 and $u.active";
    final long expected = groundTruth(predicate);
    assertEquals(expected, count(predicate, false), "record path: " + predicate);
    SirixVectorizedExecutor.resetRegionOnlyCounters();
    assertEquals(expected, count(predicate, true), "column path: " + predicate);
    assertEquals(0, SirixVectorizedExecutor.regionOnlyPagesServed(),
                 "a sparse field cannot be positionally aligned with a dense one and must not be "
                     + "served from the fused columns");
    assertTrue(expected > 0, "predicate matches nothing, so it proves nothing");
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
        // Per executor, so the A/B is between two queries rather than between two moments in a
        // JVM other tests share.
        exec.setRegionOnlyCountEnabled(regionOnly);
        SequentialPipelineStrategy.setVectorizedExecutor(exec);
        try {
          final String q =
              "count(for $u in jn:doc('" + DB + "','" + RES + "')[] where " + predicate + " return $u)";
          return ((Int64) new Query(chain, q).evaluate(ctx)).longValue();
        } finally {
          exec.close();
          SequentialPipelineStrategy.setVectorizedExecutor(null);
        }
      } finally {
        resourceSession.close();
      }
    }
  }

  /** In-memory truth for the narrow predicate dialect used above. */
  private long groundTruth(final String predicate) {
    long c = 0;
    for (int i = 0; i < N; i++) {
      if (eval(predicate, i)) c++;
    }
    return c;
  }

  private boolean eval(final String predicate, final int i) {
    final String trimmed = predicate.trim();
    // A whole-string not(...) wrapper, before the or/and splits so a connective INSIDE the call
    // cannot be split on. Only when the matching close paren is the last character.
    if (trimmed.startsWith("not(")) {
      int depth = 0;
      for (int p = 3; p < trimmed.length(); p++) {
        if (trimmed.charAt(p) == '(') depth++;
        if (trimmed.charAt(p) == ')' && --depth == 0) {
          if (p == trimmed.length() - 1) {
            return !eval(trimmed.substring(4, p), i);
          }
          break;
        }
      }
    }
    // A fully parenthesized group: strip and recurse. Only balanced-outer parens, which is all the
    // predicates above use.
    if (trimmed.startsWith("(")) {
      int depth = 0;
      for (int p = 0; p < trimmed.length(); p++) {
        if (trimmed.charAt(p) == '(') depth++;
        if (trimmed.charAt(p) == ')' && --depth == 0) {
          if (p == trimmed.length() - 1) {
            return eval(trimmed.substring(1, p), i);
          }
          // "(...) or rest" / "(...) and rest"
          final String rest = trimmed.substring(p + 1).trim();
          if (rest.startsWith("or ")) {
            return eval(trimmed.substring(1, p), i) || eval(rest.substring(3), i);
          }
          if (rest.startsWith("and ")) {
            return eval(trimmed.substring(1, p), i) && eval(rest.substring(4), i);
          }
          break;
        }
      }
    }
    final int or = predicate.indexOf(" or ");
    if (or >= 0) {
      return eval(predicate.substring(0, or), i) || eval(predicate.substring(or + 4), i);
    }
    final int and = predicate.indexOf(" and ");
    if (and >= 0) {
      return eval(predicate.substring(0, and), i) && eval(predicate.substring(and + 5), i);
    }
    final String p = predicate.trim();
    // A boolean field reference carries no operator, so it is matched whole rather than split.
    if ("$u.active".equals(p)) {
      return active[i];
    }
    if ("not($u.active)".equals(p)) {
      return !active[i];
    }
    final int dot = p.indexOf('.');
    final int sp = p.indexOf(' ', dot);
    final String field = p.substring(dot + 1, sp);
    final int sp2 = p.indexOf(' ', sp + 1);
    final String op = p.substring(sp + 1, sp2);
    final String rhs = p.substring(sp2 + 1).trim();

    if ("id".equals(field)) {
      return compare(i, op, Long.parseLong(rhs));  // id == record index by construction
    }
    if ("title".equals(field)) {
      final String literal = rhs.substring(1, rhs.length() - 1);
      return "eq".equals(op) && literal.equals(title[i]);
    }
    if ("note".equals(field)) {
      if (note[i] < 0) return false;   // absent field never satisfies a comparison
      return compare(note[i], op, Long.parseLong(rhs));
    }
    if (yearAbsent[i]) return false;   // field removed — a comparison over it is never true
    // A quoted literal against the numeric field: the compiled string-equality leaf is false on a
    // number-typed value, which is what both engine paths compute.
    if (rhs.startsWith("\"")) return false;
    // year: the double-valued records compare as year + 0.5; a fractional threshold compares in
    // doubles, which is exact at these magnitudes and gives the fractional predicates an absolute
    // oracle instead of a self-comparison.
    if (rhs.indexOf('.') >= 0) {
      final double v = yearIsDouble[i] ? year[i] + 0.5d : (double) year[i];
      final double t = Double.parseDouble(rhs);
      return switch (op) {
        case "gt" -> v > t;
        case "ge" -> v >= t;
        case "lt" -> v < t;
        case "le" -> v <= t;
        case "eq" -> v == t;
        default -> throw new IllegalArgumentException(op);
      };
    }
    final long threshold = Long.parseLong(rhs);
    if (yearIsDouble[i]) {
      final double v = year[i] + 0.5d;
      return switch (op) {
        case "gt" -> v > threshold;
        case "ge" -> v >= threshold;
        case "lt" -> v < threshold;
        case "le" -> v <= threshold;
        case "eq" -> v == threshold;
        default -> throw new IllegalArgumentException(op);
      };
    }
    return compare(year[i], op, threshold);
  }

  private static boolean compare(final long v, final String op, final long threshold) {
    return switch (op) {
      case "gt" -> v > threshold;
      case "ge" -> v >= threshold;
      case "lt" -> v < threshold;
      case "le" -> v <= threshold;
      case "eq" -> v == threshold;
      default -> throw new IllegalArgumentException(op);
    };
  }
}
