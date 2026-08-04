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
  private boolean[] yearAbsent;  // year removed by a later revision

  @BeforeEach
  void setUp() throws Exception {
    dbDir = Files.createTempDirectory("sirix-region-only-count-");
    final Random rng = new Random(0x5EEDL);
    year = new long[N];
    yearIsDouble = new boolean[N];
    title = new String[N];
    note = new int[N];
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
   * A page whose records span several commits is reconstructed by the versioning layer, and the
   * column path answers it by merging the fragments' columns instead — on the rule that the newest
   * fragment DEFINING a slot owns it. Updating and deleting records in later revisions is what
   * exercises that rule: an updated slot must be counted once with its new value, and a deleted one
   * must not resurrect its old value from an older fragment.
   */
  @Test
  void multiFragmentPagesMergeColumnsInsteadOfReconstructing() throws Exception {
    // A second revision touching a slice of the records leaves their pages spanning two commits:
    // the newer fragment holds the changed slots, the older one everything else.
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).buildPathSummary(true).build();
         var ctx = SirixQueryContext.createWithJsonStoreAndCommitStrategy(
             store, SirixQueryContext.CommitStrategy.EXPLICIT);
         var chain = SirixCompileChain.createWithJsonStore(store)) {
      // Updated: the merge must take the NEW value once, not both values.
      new Query(chain, "for $r in jn:doc('" + DB + "','" + RES + "')[] where $r.id lt 400 "
          + "return replace json value of $r.year with 2100").evaluate(ctx);
      // Removed: the field is gone in the newer fragment, and the merge must NOT resurrect the old
      // value from the older one. This is the case the per-fragment slot bitmap exists for.
      new Query(chain, "for $r in jn:doc('" + DB + "','" + RES + "')[] "
          + "where $r.id ge 400 and $r.id lt 800 return delete json $r.year").evaluate(ctx);
      ctx.applyUpdates();
    }
    for (int i = 0; i < 400; i++) {
      year[i] = 2100;
      yearIsDouble[i] = false;
    }
    for (int i = 400; i < 800; i++) {
      yearAbsent[i] = true;
    }

    for (final String predicate : new String[] { "$u.year gt 1990", "$u.year eq 2100",
                                                 "$u.year lt 1950", "$u.year gt 1899" }) {
      final long expected = groundTruth(predicate);
      assertEquals(expected, count(predicate, false), "record path: " + predicate);
      assertEquals(expected, count(predicate, true), "merged column path: " + predicate);
    }
  }

  /**
   * A page that went through versioning reconstruction must come out of it still servable from its
   * columns. It is assembled slot by slot and starts with none of its own, so if the reconstruction
   * does not rebuild them the page falls back to its records on every later query — permanently,
   * because the reconstructed page is what the cache now holds.
   */
  @Test
  void reconstructedPagesKeepTheirColumns() throws Exception {
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).buildPathSummary(true).build();
         var ctx = SirixQueryContext.createWithJsonStoreAndCommitStrategy(
             store, SirixQueryContext.CommitStrategy.EXPLICIT);
         var chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain, "for $r in jn:doc('" + DB + "','" + RES + "')[] where $r.id lt 400 "
          + "return replace json value of $r.year with 2100").evaluate(ctx);
      ctx.applyUpdates();
    }
    for (int i = 0; i < 400; i++) {
      year[i] = 2100;
      yearIsDouble[i] = false;
    }

    // First query goes through the RECORD path, which reconstructs the multi-fragment pages and
    // leaves them in the cache. The second must still be served from columns.
    final String predicate = "$u.year gt 1990";
    assertEquals(groundTruth(predicate), count(predicate, false), "record path");

    SirixVectorizedExecutor.resetRegionOnlyCounters();
    assertEquals(groundTruth(predicate), count(predicate, true), "column path after reconstruction");
    assertTrue(SirixVectorizedExecutor.regionOnlyPagesServed() > 0,
               "no page was served from columns after reconstruction (served="
                   + SirixVectorizedExecutor.regionOnlyPagesServed() + ", fellBack="
                   + SirixVectorizedExecutor.regionOnlyPageFallbacks() + ", unavailable="
                   + SirixVectorizedExecutor.regionOnlyPagesUnavailable() + ')');
  }

  @Test
  void columnOnlyCountsMatchRecordPathAndGroundTruth() throws Exception {
    final String[] predicates = {
        "$u.year gt 1990",
        "$u.year ge 1990",
        "$u.year lt 1950",
        "$u.year le 1950",
        "$u.year eq 2000",
        "$u.year gt 1899",                       // every record
        "$u.year gt 3000",                       // no record
        "$u.year gt 1950 and $u.year lt 1960",   // interval
        "$u.year ge 1950 and $u.year le 1950",   // degenerate interval
        "$u.year gt 1960 and $u.year lt 1950",   // unsatisfiable
        "$u.title eq \"Gamma\"",
        "$u.title eq \"Nowhere\"",               // literal absent from every dictionary
        "$u.note gt 50",                         // sparse field
        "$u.note eq 7"
    };

    for (final String predicate : predicates) {
      final long expected = groundTruth(predicate);
      final long columnar = count(predicate, true);
      final long records = count(predicate, false);
      assertEquals(expected, records, "record path disagrees with ground truth: " + predicate);
      assertEquals(expected, columnar, "column-only path disagrees with ground truth: " + predicate);
    }
  }

  /**
   * The double-valued records are the reason the oracle exists: they are invisible to the number
   * region, so a page holding one can only be answered from its records. The test asserts both
   * that such pages are detected (the fallback counter moves) and that the answer stays exact.
   */
  @Test
  void pagesHoldingNonIntegerValuesFallBackToRecords() throws Exception {
    SirixVectorizedExecutor.resetRegionOnlyCounters();
    final String predicate = "$u.year gt 1990";
    final long actual = count(predicate, true);
    final long served = SirixVectorizedExecutor.regionOnlyPagesServed();
    final long fellBack = SirixVectorizedExecutor.regionOnlyPageFallbacks();
    final String seen = " (served=" + served + ", fellBack=" + fellBack
        + ", unavailable=" + SirixVectorizedExecutor.regionOnlyPagesUnavailable() + ')';
    assertEquals(groundTruth(predicate), actual, "count must stay exact across the fallback");
    assertTrue(fellBack > 0, "pages carrying a double-valued year must fall back to the record path" + seen);
    assertTrue(served > 0, "pages carrying only integer years must be served from the region" + seen);
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
    final int and = predicate.indexOf(" and ");
    if (and >= 0) {
      return eval(predicate.substring(0, and), i) && eval(predicate.substring(and + 5), i);
    }
    final String p = predicate.trim();
    final int dot = p.indexOf('.');
    final int sp = p.indexOf(' ', dot);
    final String field = p.substring(dot + 1, sp);
    final int sp2 = p.indexOf(' ', sp + 1);
    final String op = p.substring(sp + 1, sp2);
    final String rhs = p.substring(sp2 + 1).trim();

    if ("title".equals(field)) {
      final String literal = rhs.substring(1, rhs.length() - 1);
      return "eq".equals(op) && literal.equals(title[i]);
    }
    if ("note".equals(field)) {
      if (note[i] < 0) return false;   // absent field never satisfies a comparison
      return compare(note[i], op, Long.parseLong(rhs));
    }
    if (yearAbsent[i]) return false;   // field removed — a comparison over it is never true
    // year: the double-valued records compare as year + 0.5
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
