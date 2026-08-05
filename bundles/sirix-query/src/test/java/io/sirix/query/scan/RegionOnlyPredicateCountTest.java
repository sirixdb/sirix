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
  void pagesHoldingNonIntegerValuesAreServedByBothColumns() throws Exception {
    // These pages used to be this test's FALLBACK case: the long column's tag count fell short of
    // the anchor slots and the whole page went back to the records over a handful of doubles. The
    // double column closes the gap — the oracle sums both columns' tag counts, and the count is
    // longKernel + doubleKernel. The count staying exact is the load-bearing assertion: the double
    // 1990.5 satisfies `gt 1990` even though it is outside the folded long interval [1991, MAX],
    // so a double side derived from the long interval (rather than the original threshold) fails
    // here.
    SirixVectorizedExecutor.resetRegionOnlyCounters();
    final String predicate = "$u.year gt 1990";
    final long actual = count(predicate, true);
    final long served = SirixVectorizedExecutor.regionOnlyPagesServed();
    final long fellBack = SirixVectorizedExecutor.regionOnlyPageFallbacks();
    final String seen = " (served=" + served + ", fellBack=" + fellBack
        + ", unavailable=" + SirixVectorizedExecutor.regionOnlyPagesUnavailable() + ')';
    assertEquals(groundTruth(predicate), actual, "count must stay exact across both columns");
    assertEquals(0, fellBack, "double-bearing pages must now be served, not fall back" + seen);
    assertTrue(served > 0, "no page served at all" + seen);
  }

  /**
   * Fractional thresholds are served from both typed columns.
   *
   * <p>{@code $u.year gt 1990.5} folds to long interval {@code [1991, MAX]} plus double interval
   * {@code (1990.5, +inf)}; {@code eq 1990.5} folds to an EMPTY long interval and the point double
   * interval — the shape that would return zero everywhere if plan emptiness ignored the double
   * side. Ground truth evaluates fractional thresholds in doubles, so the oracle is absolute.
   */
  @Test
  void fractionalThresholdsAreServedFromBothColumns() throws Exception {
    for (final String predicate : new String[] { "$u.year gt 1990.5", "$u.year le 1990.5",
                                                 "$u.year eq 1990.5",
                                                 "$u.year ge 1950.5 and $u.year lt 2000.5" }) {
      final long expected = groundTruth(predicate);
      assertEquals(expected, count(predicate, false), "record path: " + predicate);
      SirixVectorizedExecutor.resetRegionOnlyCounters();
      assertEquals(expected, count(predicate, true), "column path: " + predicate);
      assertTrue(SirixVectorizedExecutor.regionOnlyPagesServed() > 0,
                 "no page served from the typed columns for " + predicate);
    }
  }

  /**
   * A disjunction past three branches is served by the LINEAR disjoint decomposition:
   * {@code |A| + |B and not A| + |C and not A and not B| + ...} — one anchored scan per branch
   * instead of {@code 2^k - 1} inclusion-exclusion terms. Four and five branches cover both sides
   * of the switchover, and the mixed leaf types (numeric, boolean, string) pin that each branch
   * anchors independently.
   */
  @Test
  void wideDisjunctionsAreServedByDisjointDecomposition() throws Exception {
    Assumptions.assumeTrue(predicateTreeClaimed("$u.year gt 2015 or $u.note gt 90"),
                           "requires a Brackit build that annotates decomposable counts");
    for (final String predicate : new String[] {
        "$u.year gt 2015 or $u.note gt 90 or $u.active or $u.title eq \"Gamma\"",
        "$u.year lt 1910 or $u.note gt 95 or $u.title eq \"Delta\" or $u.year gt 2020 or $u.note lt 3",
    }) {
      final long expected = groundTruth(predicate);
      assertEquals(expected, count(predicate, false), "record path: " + predicate);
      assertEquals(expected, count(predicate, true), "column path: " + predicate);
      assertTrue(expected > 0, "predicate matches nothing, so it proves nothing: " + predicate);
    }
  }

  /**
   * A predicate that lies entirely outside a page's stored range must be answered from the
   * zone-map region alone, with the number column never decompressed.
   *
   * <p>Asserting the count is not enough here: the same answer comes back whether the bounds
   * settled the page or the column was decompressed and scanned. Only the counter distinguishes
   * the two, so without it this optimisation could stop firing entirely and every test would still
   * pass. That is exactly what happened to the zone maps before they were lifted out of the
   * compressed payload — the pruning worked, and saved nothing.
   */
  @Test
  void boundsAloneAnswerPagesWithoutDecompressingTheColumn() throws Exception {
    // The corpus stores year in [1900, 2023]; both predicates are disjoint from every page's range.
    for (final String predicate : new String[] { "$u.year lt 1800", "$u.year gt 3000" }) {
      SirixVectorizedExecutor.resetZoneMapDecidedPages();
      assertEquals(groundTruth(predicate), count(predicate, true),
                   "column path disagrees with ground truth: " + predicate);
      assertTrue(SirixVectorizedExecutor.zoneMapDecidedPages() > 0,
                 "no page was settled from its zone map for " + predicate
                     + " — the prune is not firing (decided="
                     + SirixVectorizedExecutor.zoneMapDecidedPages() + ')');
    }

    // The other direction: a predicate every stored value satisfies is equally decidable.
    SirixVectorizedExecutor.resetZoneMapDecidedPages();
    assertEquals(groundTruth("$u.year gt 1899"), count("$u.year gt 1899", true));
    assertTrue(SirixVectorizedExecutor.zoneMapDecidedPages() > 0,
               "an all-match predicate must also be settled from bounds");

    // And a predicate that genuinely straddles the range must still be exact, having fallen through
    // the bounds and decompressed the column.
    SirixVectorizedExecutor.resetZoneMapDecidedPages();
    assertEquals(groundTruth("$u.year gt 1990"), count("$u.year gt 1990", true),
                 "a straddling predicate must fall through to the column and stay exact");
  }

  /**
   * A disjunction of string equalities must be answered from the dictionary as one set-membership
   * scan, not sent to the records.
   *
   * <p>Before this shape was planned, {@code title eq "A" or title eq "B"} failed the region
   * planner outright — it accepted only a conjunction — so every page fell back to reconstructing
   * records for a predicate the dictionary can settle with two probes and one pass over the packed
   * id column.
   */
  @Test
  void stringDisjunctionsAreAnsweredFromTheDictionary() throws Exception {
    final String[] predicates = {
        "$u.title eq \"Gamma\" or $u.title eq \"Delta\"",
        "$u.title eq \"Alpha\" or $u.title eq \"Beta\" or $u.title eq \"Epsilon\"",
        // One literal present, one absent from every page — the set must ignore the miss.
        "$u.title eq \"Gamma\" or $u.title eq \"Nowhere\"",
        // Both absent: the sketch must rule the page out without reading the dictionary.
        "$u.title eq \"Nowhere\" or $u.title eq \"AlsoNowhere\"",
        // Duplicated literal must not be counted twice.
        "$u.title eq \"Gamma\" or $u.title eq \"Gamma\"",
    };
    for (final String predicate : predicates) {
      final long expected = groundTruth(predicate);
      assertEquals(expected, count(predicate, false), "record path: " + predicate);
      SirixVectorizedExecutor.resetRegionOnlyCounters();
      assertEquals(expected, count(predicate, true), "column path: " + predicate);
      assertTrue(SirixVectorizedExecutor.regionOnlyPagesServed() > 0,
                 "no page served from columns for " + predicate + " (served="
                     + SirixVectorizedExecutor.regionOnlyPagesServed() + ", fellBack="
                     + SirixVectorizedExecutor.regionOnlyPageFallbacks() + ')');
    }
  }

  /**
   * A boolean predicate must be answered from the packed-bit column.
   *
   * <p>The planner recognised only numeric and string leaves, so {@code where $u.active} — the
   * cheapest predicate the layout can serve, one masked population count — was the one that always
   * reconstructed records.
   */
  @Test
  void booleanPredicatesAreAnsweredFromTheBitColumn() throws Exception {
    final long expected = groundTruth("$u.active");
    assertEquals(expected, count("$u.active", false), "record path");
    SirixVectorizedExecutor.resetRegionOnlyCounters();
    assertEquals(expected, count("$u.active", true), "column path");
    assertTrue(SirixVectorizedExecutor.regionOnlyPagesServed() > 0,
               "no page served from the bit column (served="
                   + SirixVectorizedExecutor.regionOnlyPagesServed() + ", fellBack="
                   + SirixVectorizedExecutor.regionOnlyPageFallbacks() + ')');

    assertEquals(N - groundTruth("not($u.active)"), expected,
                 "the two halves must partition the corpus");
  }

  /**
   * A bare {@code not($u.active)} is served as a COMPLEMENT: {@code N - count($u.active)}.
   *
   * <p>The popcount complement over the bit column ALONE is wrong — a record with no {@code active}
   * field satisfies {@code not($u.active)}, and a scan anchored on the column never visits it. What
   * makes the complement sound is where {@code N} comes from: the record TOTAL (the top-level
   * array's child count, provable without touching a record), not the column's slot count. The
   * positive count is popcounts over the bit column, and every record the scan cannot see is
   * accounted for algebraically.
   *
   * <p>The same decomposition serves a negated comparison and a negated conjunction, since
   * {@code N - count(X)} needs nothing from X beyond its own countability. On a stock Brackit that
   * predates the decomposable-count annotation, the shape stays on the generic pipeline and this
   * test skips.
   */
  @Test
  void bareNegationsAreServedByComplement() throws Exception {
    Assumptions.assumeTrue(predicateTreeClaimed("not($u.active)"),
                           "requires a Brackit build that annotates decomposable counts");
    for (final String predicate : new String[] { "not($u.active)",
                                                 "not($u.year gt 1990)",
                                                 "not($u.active and $u.year gt 1990)" }) {
      final long expected = groundTruth(predicate);
      assertEquals(expected, count(predicate, false), "record path: " + predicate);
      SirixVectorizedExecutor.resetRegionOnlyCounters();
      assertEquals(expected, count(predicate, true), "column path: " + predicate);
      assertTrue(SirixVectorizedExecutor.regionOnlyPagesServed() > 0,
                 "the inner count of " + predicate + " was not served from columns (served="
                     + SirixVectorizedExecutor.regionOnlyPagesServed() + ')');
      assertTrue(expected > 0, "predicate matches nothing, so it proves nothing: " + predicate);
    }
  }

  /**
   * An OR across two fields is served by inclusion-exclusion: {@code |A| + |B| - |A and B|}.
   *
   * <p>No sound scan anchor exists for the union — a record missing {@code year} can still satisfy
   * the {@code note} branch — so it cannot be ITERATED. But each term of the identity is anchored
   * and exactly countable, including over records missing fields ({@code note} is absent on three
   * quarters of the corpus, which is what makes this the hard case). The identity ranges over
   * totals, so no record is ever visited under the wrong anchor.
   */
  @Test
  void crossFieldDisjunctionIsServedByInclusionExclusion() throws Exception {
    Assumptions.assumeTrue(predicateTreeClaimed("$u.year gt 2015 or $u.note gt 90"),
                           "requires a Brackit build that annotates decomposable counts");
    for (final String predicate : new String[] { "$u.year gt 2015 or $u.note gt 90",
                                                 "$u.note gt 90 or $u.active" }) {
      final long expected = groundTruth(predicate);
      assertEquals(expected, count(predicate, false), "record path: " + predicate);
      SirixVectorizedExecutor.resetRegionOnlyCounters();
      assertEquals(expected, count(predicate, true), "column path: " + predicate);
      assertTrue(SirixVectorizedExecutor.regionOnlyPagesServed() > 0,
                 "no inclusion-exclusion term of " + predicate + " was served from columns");
      assertTrue(expected > 0, "predicate matches nothing, so it proves nothing: " + predicate);
    }
  }

  /**
   * A negation conjoined with a leaf that DOES exclude records missing its field is representable,
   * and must be both claimed and correct.
   *
   * <p>This is what the negation support buys. {@code $u.year gt 1990} is false on any record
   * lacking {@code year}, so {@code year} is a sound anchor for the conjunction as a whole and the
   * scan may iterate it. Before Brackit emitted {@link io.brackit.query.compiler.optimizer.PredicateNode.Not},
   * {@code extractPredicate} returned null for the second conjunct and the whole annotation was
   * dropped — the first conjunct's anchor went unused because of a negation it could carry.
   */
  @Test
  void negationConjoinedWithAnAnchoringLeafIsRepresentable() throws Exception {
    Assumptions.assumeTrue(brackitEmitsNotAndParens(),
                           "requires a Brackit build with fn:not PredicateNode support");
    for (final String predicate : new String[] { "$u.year gt 1990 and not($u.active)",
                                                 "$u.year lt 1950 and not($u.active)",
                                                 "$u.active and $u.year gt 1990" }) {
      // Both paths agree on the answer whether or not the pipeline is claimed, so the agreement
      // alone would pass on a compiler that silently dropped the negation. Pin the claim too.
      assertTrue(predicateTreeClaimed(predicate), "predicate not claimed at all: " + predicate);
      final long expected = groundTruth(predicate);
      assertEquals(expected, count(predicate, false), "record path: " + predicate);
      assertEquals(expected, count(predicate, true), "column path: " + predicate);
      assertTrue(expected > 0, "predicate matches nothing, so it proves nothing: " + predicate);
    }
  }

  /**
   * Whether the resolved Brackit build emits {@link io.brackit.query.compiler.optimizer.PredicateNode.Not}
   * for {@code fn:not} and sees through grouping parens. Probed rather than assumed: this module
   * can be built against a stock Brackit snapshot that predates both, and the tests below that
   * depend on them must SKIP there — a red suite on a dependency mismatch says "sirix is broken",
   * which is the wrong message.
   */
  private boolean brackitEmitsNotAndParens() throws Exception {
    return predicateTreeClaimed("$u.year gt 1990 and not($u.active)")
        && predicateTreeClaimed("($u.year ge 1940 and $u.year le 1950) or $u.year gt 2000");
  }

  /**
   * Whether the optimizer annotated the pipeline with a predicate tree for {@code predicate}.
   *
   * <p>Read off the optimized AST rather than inferred from a counter: an unclaimed pipeline still
   * produces the right count through the generic evaluator, so nothing in the answer distinguishes
   * "the executor evaluated the negation" from "the compiler gave up and the interpreter did it".
   */
  private boolean predicateTreeClaimed(final String predicate) throws Exception {
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).buildPathSummary(true).build();
         var chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain, "count(for $u in jn:doc('" + DB + "','" + RES + "')[] where " + predicate
          + " return $u)");
      return hasPredicateTree(chain.getOptimizedAST());
    }
  }

  private static boolean hasPredicateTree(final AST node) {
    if (node == null) {
      return false;
    }
    if (node.getProperty(VectorizedScanAnnotation.PREDICATE_TREE) != null) {
      return true;
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      if (hasPredicateTree(node.getChild(i))) {
        return true;
      }
    }
    return false;
  }

  /**
   * A numeric interval AND a boolean flag must be answered from the two columns in one pass.
   *
   * <p>This is the shape that needed {@link io.sirix.page.pax.RecordOrdinalRegion}. Counting the
   * number column and the bit column separately cannot answer it — the counts say how many rows
   * satisfy each, not how many satisfy both — and intersecting them requires knowing that position
   * {@code i} of one column and position {@code i} of the other are the same record. Nothing in the
   * per-field layout said so, which is why {@code BooleanRegionSimd.andInto} sat with no caller.
   *
   * <p>Both field orders are covered because the compiler anchors on whichever field comes first,
   * so {@code $u.active and $u.year gt 1990} hands the kernel the columns the other way round. The
   * negated flag is covered too: the bit column inverts during the AND rather than in a pass of its
   * own.
   */
  @Test
  void numericAndBooleanFuseIntoOnePass() throws Exception {
    final String[] predicates = {
        "$u.year gt 1990 and $u.active",
        "$u.active and $u.year gt 1990",
        "$u.year ge 1900 and $u.active",      // zone map settles the numeric half: all survive
        "$u.year gt 3000 and $u.active",      // zone map settles it the other way: none survive
        "$u.year gt 1950 and $u.year lt 2000 and $u.active",  // two comparisons, one interval
    };
    for (final String predicate : predicates) {
      final long expected = groundTruth(predicate);
      assertEquals(expected, count(predicate, false), "record path: " + predicate);
      SirixVectorizedExecutor.resetRegionOnlyCounters();
      assertEquals(expected, count(predicate, true), "column path: " + predicate);
      assertTrue(SirixVectorizedExecutor.regionOnlyPagesServed() > 0,
                 "no page served from the fused columns for " + predicate + " (served="
                     + SirixVectorizedExecutor.regionOnlyPagesServed() + ", fellBack="
                     + SirixVectorizedExecutor.regionOnlyPageFallbacks() + ')');
    }
  }

  /** The negated-flag fusions, split out because they need the patched Brackit to reach a plan. */
  @Test
  void negatedFlagFusesWhenBrackitEmitsNot() throws Exception {
    Assumptions.assumeTrue(brackitEmitsNotAndParens(),
                           "requires a Brackit build with fn:not PredicateNode support");
    for (final String predicate : new String[] { "$u.year gt 1990 and not($u.active)",
                                                 "$u.year gt 1950 and $u.id lt 15000 and not($u.active)" }) {
      final long expected = groundTruth(predicate);
      assertEquals(expected, count(predicate, false), "record path: " + predicate);
      SirixVectorizedExecutor.resetRegionOnlyCounters();
      assertEquals(expected, count(predicate, true), "column path: " + predicate);
      assertTrue(SirixVectorizedExecutor.regionOnlyPagesServed() > 0,
                 "no page served from the fused columns for " + predicate);
    }
  }

  /**
   * A string leaf and a numeric interval on the SAME field must fall back, not drop a leaf.
   *
   * <p>Found by review: the numeric guard rejected only a prior BOOLEAN leaf, so
   * {@code $u.year eq "nineteen" and $u.year gt 1990} overwrote the string leaf with the interval
   * and counted the interval alone — answering a WEAKER predicate. The truth is zero (a string
   * equality on a long-typed value matches nothing), which makes the over-count visible.
   */
  @Test
  void stringAndNumericLeafOnOneFieldFallsBack() throws Exception {
    final String predicate = "$u.year eq \"nineteen\" and $u.year gt 1990 and $u.id lt 10000";
    assertEquals(0L, count(predicate, false), "record path: " + predicate);
    SirixVectorizedExecutor.resetRegionOnlyCounters();
    assertEquals(0L, count(predicate, true), "column path: " + predicate);
    assertEquals(0, SirixVectorizedExecutor.regionOnlyPagesServed(),
                 "a field with both a string and a numeric leaf is not one column's predicate");
  }

  /**
   * Conjunctions the generalized kernel must answer: two numeric fields, three fields, and mixes.
   *
   * <p>The kernel intersects one row bitmap per column, so the shapes it covers are whatever
   * combination of numeric intervals and boolean flags a conjunction happens to use — not a fixed
   * pair. Two numeric fields land on two tags of the SAME number region, which is the case most
   * likely to confuse a tag lookup, and a three-field conjunction exercises masking a third column
   * into an accumulator two columns already narrowed.
   */
  @Test
  void multiFieldConjunctionsAreAnsweredFromColumns() throws Exception {
    final String[] predicates = {
        // Two numeric fields, two tags of one number region.
        "$u.year gt 1990 and $u.id lt 10000",
        "$u.id ge 5000 and $u.year lt 1950",
        // Numeric, numeric, boolean — a third column masked into an already-narrowed accumulator.
        "$u.year gt 1950 and $u.id lt 15000 and $u.active",
        // One numeric bound restated across two comparisons, folded into one interval per field.
        "$u.year ge 1960 and $u.year le 1980 and $u.id gt 100 and $u.id lt 19000",
        // A string-equality leaf beside a numeric one: the dictionary column produces the row
        // bitmap for the literal and the number column is intersected in.
        "$u.year gt 1990 and $u.title eq \"Gamma\"",
        "$u.title eq \"Delta\" and $u.active and $u.year lt 2000",
    };
    for (final String predicate : predicates) {
      final long expected = groundTruth(predicate);
      assertEquals(expected, count(predicate, false), "record path: " + predicate);
      SirixVectorizedExecutor.resetRegionOnlyCounters();
      assertEquals(expected, count(predicate, true), "column path: " + predicate);
      assertTrue(SirixVectorizedExecutor.regionOnlyPagesServed() > 0,
                 "no page served from the fused columns for " + predicate + " (served="
                     + SirixVectorizedExecutor.regionOnlyPagesServed() + ", fellBack="
                     + SirixVectorizedExecutor.regionOnlyPageFallbacks() + ')');
      assertTrue(expected > 0, "predicate matches nothing, so it proves nothing: " + predicate);
    }
  }

  /** The parenthesized conjunctive branch, split out: it needs paren-transparent Brackit. */
  @Test
  void parenthesizedDisjunctionBranchIsServed() throws Exception {
    Assumptions.assumeTrue(brackitEmitsNotAndParens(),
                           "requires a Brackit build that sees through grouping parens");
    final String predicate = "($u.year ge 1940 and $u.year le 1950) or $u.year gt 2000";
    final long expected = groundTruth(predicate);
    assertEquals(expected, count(predicate, false), "record path: " + predicate);
    SirixVectorizedExecutor.resetRegionOnlyCounters();
    assertEquals(expected, count(predicate, true), "column path: " + predicate);
    assertTrue(SirixVectorizedExecutor.regionOnlyPagesServed() > 0,
               "no page served from the number column for " + predicate);
  }

  /**
   * Disjunctions over a page whose field is split across the long and double columns are served
   * by summing per-interval kernel passes on BOTH columns — with the double union folded from the
   * ORIGINAL branch thresholds, which is the whole correctness story here.
   *
   * <p>The two sharp pins: {@code le 1990 or ge 1991} must EXCLUDE the year-as-double records
   * (1990.5 satisfies neither branch, though the merged long union [MIN, MAX] would swallow
   * everything), and {@code lt 1950 or gt 1990} must INCLUDE them (1990.5 > 1990, though the
   * folded long interval starts at 1991). Both directions fail if the double union is derived
   * from the merged integer intervals instead of the branches.
   */
  @Test
  void disjunctionsCombineBothColumnsWithoutFallback() throws Exception {
    for (final String predicate : new String[] {
        "$u.year lt 1950 or $u.year gt 1990",
        "$u.year le 1990 or $u.year ge 1991",
        "$u.year eq 1950 or $u.year eq 1990 or $u.year eq 2020",
    }) {
      final long expected = groundTruth(predicate);
      assertEquals(expected, count(predicate, false), "record path: " + predicate);
      SirixVectorizedExecutor.resetRegionOnlyCounters();
      assertEquals(expected, count(predicate, true), "column path: " + predicate);
      assertTrue(SirixVectorizedExecutor.regionOnlyPagesServed() > 0,
                 "no page served for " + predicate);
      assertEquals(0, SirixVectorizedExecutor.regionOnlyPageFallbacks(),
                   "double-bearing pages must be served by the per-interval double union for "
                       + predicate);
      assertTrue(expected > 0, "predicate matches nothing, so it proves nothing: " + predicate);
    }
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
