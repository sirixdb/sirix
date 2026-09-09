package io.sirix.query.scan;

import io.brackit.query.Query;
import io.brackit.query.atomic.Int64;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.util.serialize.StringSerializer;
import io.sirix.access.Databases;
import io.sirix.cache.IndexLogKey;
import io.sirix.index.IndexType;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.PageConstants;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.settings.Constants;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The column-scan path against every versioning strategy.
 *
 * <p>
 * Versioning is where this path is easiest to get wrong, because each strategy assembles a page
 * from a different set of fragments and the column merge has to reach the same answer as the record
 * reconstruction without ever building a record. {@link VersioningType#FULL} writes complete pages
 * and exercises the single-page read; the other three leave pages spanning commits and exercise the
 * merge. Running one test body over all of them is deliberate — a strategy-specific mistake shows
 * up as one enum value failing rather than as a number nobody re-derives.
 *
 * <p>
 * Each case writes a second revision that both <em>updates</em> and <em>removes</em> values, the
 * two cases the merge rule turns on: an updated slot must be counted once with its new value, and a
 * removed one must not have its old value resurrected from an older fragment.
 *
 * <p>
 * The removals also cover a write-path defect this test originally exposed: under
 * {@link VersioningType#FULL}, the second field removal of a transaction threw "No current page -
 * cannot acquire guard", because {@code acquireGuardForNode} relied on a guard the previous removal
 * had released and the intervening cursor movements were answered from the transaction's record
 * cache without going through the page layer.
 */
final class VersioningColumnScanTest {

  private static final int N = 6_000;
  private static final String DB = "versioning-column-db";
  private static final String RES = "records.jn";

  /** Records whose year is replaced in revision 2. */
  private static final int UPDATED_BELOW = 300;
  /** Records whose year is removed in revision 2. */
  private static final int REMOVED_BELOW = 600;

  private Path dbDir;
  private long[] year;
  private boolean[] yearAbsent;
  /** Records whose year is stored fractionally; {@code null} for the long-only tests. */
  private boolean[] yearIsDouble;
  /** The EXACT value those records hold — a decimal, which is what jn:store writes for one. */
  private BigDecimal yearFractionalValue;

  private record AggregatePair(String first, String second) {
  }

  @AfterEach
  void tearDown() {
    SequentialPipelineStrategy.setVectorizedExecutor(null);
    if (dbDir != null) {
      Databases.removeDatabase(dbDir);
      dbDir = null;
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void columnPathAgreesWithRecordPathAndGroundTruth(final VersioningType versioning) throws Exception {
    dbDir = Files.createTempDirectory("sirix-versioning-column-");
    shred(versioning);
    updateAndRemoveInSecondRevision(versioning);

    final String[] predicates = {"$u.year gt 1990", // spans updated, removed and untouched records
        "$u.year eq 2100", // exactly the updated ones
        "$u.year lt 1950", // only untouched ones
        "$u.year gt 1899" // every record that still HAS a year
    };

    SirixVectorizedExecutor.resetRegionOnlyCounters();
    for (final String predicate : predicates) {
      final long expected = groundTruth(predicate);
      assertEquals(expected, count(versioning, predicate, false), versioning + " record path: " + predicate);
      // Cold for the column path. The record-path query above leaves every page it touched
      // resident, and getRecordPageRegionsOnly serves a resident page straight from its region
      // table -- that page was already version-merged by the page layer, so the fragment merge
      // never runs. Left warm, this test agreed with the record path on all four strategies while
      // countFragmentedPageFromRegions was never entered once (regionMergedPages() == 0), which is
      // the whole half of the versioned column path it exists to cover.
      Databases.getGlobalBufferManager().getRecordPageCache().clear();
      assertEquals(expected, count(versioning, predicate, true), versioning + " column path: " + predicate);
    }

    // Agreement is necessary but not sufficient: a column path that silently declined every page
    // would also agree. Assert it actually answered from columns.
    final long served = SirixVectorizedExecutor.regionOnlyPagesServed();
    assertTrue(served > 0,
        versioning + ": no page was served from columns (served=" + served + ", fellBack="
            + SirixVectorizedExecutor.regionOnlyPageFallbacks() + ", unavailable="
            + SirixVectorizedExecutor.regionOnlyPagesUnavailable() + ')');

    // FULL writes complete pages, so it has no fragments to merge and nothing below applies to it.
    if (versioning == VersioningType.FULL) {
      assertEquals(0L, SirixVectorizedExecutor.regionMergedPages(),
          "FULL has no page fragments, so nothing should reach the merge");
      return;
    }
    assertTrue(SirixVectorizedExecutor.regionMergedPages() > 0,
        versioning + ": no page reached the fragment merge, so the versioned column path "
            + "was never exercised -- agreement with the record path above proves only that "
            + "the single-fragment path is right");
    // And that the merge had something to SHADOW. A merge in which no fragment overlaps a newer
    // one answers identically through the unmasked kernel, so without this the masking -- the part
    // that decides whether a superseded or deleted value is resurrected -- can be dead code while
    // every assertion above still passes.
    //
    // DIFFERENTIAL is exempt deliberately: its window is {cumulative delta, last full dump}, and
    // the cumulative delta can claim every anchor slot the older fragment could have supplied.
    // It is asserted to reach the merge (above), while the separate multi-delta test below verifies
    // its exact answers without coupling correctness to whether this valid merge happens to need a
    // masked or an unmasked kernel.
    if (versioning != VersioningType.DIFFERENTIAL) {
      assertTrue(SirixVectorizedExecutor.regionMergeMaskedKernels() > 0,
          versioning + ": every merged fragment was fully live, so the liveness masking " + "never ran (unmasked="
              + SirixVectorizedExecutor.regionMergeUnmaskedKernels() + ')');
    }
  }

  /**
   * A missing object-name region is not an empty object-name region.
   *
   * <p>
   * The compact region deliberately declines a page containing more than 255 distinct field names.
   * Its numeric regions remain valid, but without the field-to-slot alignment oracle a fragment merge
   * cannot attribute those values. It must reconstruct the page instead of claiming the fragment's
   * slots and silently suppressing values from older fragments.
   */
  @ParameterizedTest
  @EnumSource(value = VersioningType.class, mode = EnumSource.Mode.EXCLUDE, names = {"FULL"})
  void wideObjectWithoutNameRegionFallsBackWithoutUndercount(final VersioningType versioning) throws Exception {
    dbDir = Files.createTempDirectory("sirix-versioning-wide-object-");
    final StringBuilder json = new StringBuilder(4_096).append("[{\"target\":7");
    for (int i = 0; i < 300; i++) {
      json.append(",\"k").append(i).append("\":").append(i);
    }
    json.append("}]");

    try (var store = newStore(versioning);
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain, "jn:store('" + DB + "','" + RES + "','" + json + "')").evaluate(ctx);
    }
    try (var store = newStore(versioning);
        var ctx =
            SirixQueryContext.createWithJsonStoreAndCommitStrategy(store, SirixQueryContext.CommitStrategy.EXPLICIT);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain,
          "for $r in jn:doc('" + DB + "','" + RES + "')[] return replace json value of $r.k0 with 10000").evaluate(ctx);
      ctx.applyUpdates();
    }
    try (var store = newStore(versioning);
        var ctx =
            SirixQueryContext.createWithJsonStoreAndCommitStrategy(store, SirixQueryContext.CommitStrategy.EXPLICIT);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain, "for $r in jn:doc('" + DB + "','" + RES + "')[] return delete json $r.k1").evaluate(ctx);
      ctx.applyUpdates();
    }

    final String predicate = "$u.target eq 7";
    Databases.getGlobalBufferManager().getRecordPageCache().clear();
    assertEquals(1L, count(versioning, predicate, false), versioning + " record path");
    Databases.getGlobalBufferManager().getRecordPageCache().clear();
    SirixVectorizedExecutor.resetRegionOnlyCounters();
    assertEquals(1L, count(versioning, predicate, true), versioning + " column path");
    assertTrue(SirixVectorizedExecutor.regionOnlyPageFallbacks() > 0,
        versioning + ": the fragment lacking a name-key alignment region did not fall back");
  }

  /**
   * Overflow values are not represented in PAX. Both a predicate matching the old inline value and
   * one matching the current overflow value must therefore reconstruct revision 2; revision 1 and a
   * later shrink must remain correct (it may still inherit the conservative chain fallback). Every
   * assertion is cold and names its revision explicitly.
   */
  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void overflowValueNeverResurrectsOrDisappearsFromColumnCounts(final VersioningType versioning) throws Exception {
    dbDir = Files.createTempDirectory("sirix-versioning-overflow-column-");
    // The inline limit is a hard storage invariant across generic and fused direct-write paths.
    final String huge = "x".repeat(PageConstants.MAX_RECORD_SIZE + 1_024);
    try (var store = newStoreWithoutPathStatistics(versioning);
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain,
          "jn:store('" + DB + "','" + RES
              + "','[{\"payload\":\"small\",\"marker\":7,\"value\":7,\"nested\":{\"value\":1000}},"
              + "{\"value\":-9223372036854775808},{\"value\":1.25}," + "{\"value\":9223372036854775808}]')").evaluate(
                  ctx);
    }
    try (var store = newStore(versioning);
        var ctx =
            SirixQueryContext.createWithJsonStoreAndCommitStrategy(store, SirixQueryContext.CommitStrategy.EXPLICIT);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain,
          "for $r in jn:doc('" + DB + "','" + RES
              + "')[] where $r.marker eq 7 return replace json value of $r.payload with \"" + huge + "\"").evaluate(
                  ctx);
      ctx.applyUpdates();
    }
    try (var store = newStore(versioning);
        var ctx =
            SirixQueryContext.createWithJsonStoreAndCommitStrategy(store, SirixQueryContext.CommitStrategy.EXPLICIT);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain, "for $r in jn:doc('" + DB + "','" + RES
          + "')[] where $r.marker eq 7 return replace json value of $r.payload with \"tiny\"").evaluate(ctx);
      ctx.applyUpdates();
    }

    assertColdRecordAndColumnCount(versioning, 1, "$u.payload eq \"small\"", 1L, false, true);
    // String predicates are not guaranteed to select the region-only executor. Prove the page
    // certificate on the column-eligible marker instead: although marker itself is inline, the
    // same page contains an overflow record and therefore must be reconstructed as a whole.
    assertColdRecordAndColumnCount(versioning, 2, "$u.payload eq \"small\"", 0L, false, false);
    assertColdRecordAndColumnCount(versioning, 2, "$u.payload eq \"" + huge + "\"", 1L, false, false);
    assertColdRecordAndColumnCount(versioning, 2, "$u.marker eq 7", 1L, true, false);
    // The unrelated overflow makes the page-level PAX certificate incomplete, so these aggregates
    // exercise parallelAggregateMixed's reconstructed-record fallback. One `sum(value)` sees the
    // ordinary direct fused-long case, Long.MIN_VALUE (the direct accessor's conservative sentinel),
    // a decimal, and an out-of-long BigInteger. The latter three must retain the authoritative cursor
    // path, and merging all four lanes must remain exact. The nested `value:1000` is a same-name,
    // different-path decoy: neither the presence count nor the sum may include it. Run the same query
    // through a chain with no executor as the semantic oracle for every versioning strategy.
    assertDirectFusedIntegralReadable(versioning, 2);
    assertColdAggregateFallbackDifferential(versioning, 2);
    // A fragmenting strategy may retain revision 2's overflow fragment in revision 3's chain. The
    // page-level certificate then conservatively reconstructs the chain even though the newest
    // fragment is inline again, so revision 3 asserts the answer but not a particular serve path.
    assertColdRecordAndColumnCount(versioning, 3, "$u.payload eq \"tiny\"", 1L, false, false);
  }

  /**
   * {@code count($row.field)} counts field occurrences, not merely numeric/string observations. The
   * named PathNode is the exact structural oracle even when the value is null, object or array; a
   * missing field contributes nothing. The nested same-name array is an adversarial scope decoy:
   * array-valued slots carry an anonymous child path key, but resolving that detail must never
   * degrade into a resource-wide name count. Revision 2 combines replace, delete and insert, then
   * revision 1 is reopened to prove every versioning strategy retains its historical references.
   */
  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void structuralFieldCountIsScopedAndRevisionedAcrossAllValueKinds(final VersioningType versioning) throws Exception {
    dbDir = Files.createTempDirectory("sirix-versioning-structural-count-");
    try (var store = newStoreWithoutPathStatistics(versioning);
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain,
          "jn:store('" + DB + "','" + RES + "','[" + "{\"id\":1,\"presence\":null}," + "{\"id\":2,\"presence\":\"s\"},"
              + "{\"id\":3,\"presence\":true}," + "{\"id\":4,\"presence\":{\"x\":1}},"
              + "{\"id\":5,\"presence\":[1,2]}," + "{\"id\":6},"
              + "{\"id\":7,\"nested\":{\"presence\":[\"decoy\"]}}]')").evaluate(ctx);
    }

    assertFieldCountMatchesInterpreterAndSummary(versioning, 1, "presence", 5L);

    try (var store = newStoreWithoutPathStatistics(versioning);
        var ctx =
            SirixQueryContext.createWithJsonStoreAndCommitStrategy(store, SirixQueryContext.CommitStrategy.EXPLICIT);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain, "for $r in jn:doc('" + DB + "','" + RES
          + "')[] where $r.id eq 1 return replace json value of $r.presence with {\"updated\":true}").evaluate(ctx);
      new Query(chain, "for $r in jn:doc('" + DB + "','" + RES
          + "')[] where $r.id eq 2 or $r.id eq 3 return delete json $r.presence").evaluate(ctx);
      new Query(chain, "for $r in jn:doc('" + DB + "','" + RES
          + "')[] where $r.id eq 6 return insert json {\"presence\":false} into $r").evaluate(ctx);
      ctx.applyUpdates();
    }

    assertFieldCountMatchesInterpreterAndSummary(versioning, 2, "presence", 4L);
    assertFieldCountMatchesInterpreterAndSummary(versioning, 1, "presence", 5L);
  }

  /** A presence-only result must never occupy the full typed-aggregate cache entry. */
  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void countOnlyScanCacheNeverMasqueradesAsFullAggregate(final VersioningType versioning) throws Exception {
    dbDir = Files.createTempDirectory("sirix-versioning-count-cache-");
    try (var store = newStoreWithoutPathSummary(versioning);
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain,
          "jn:store('" + DB + "','" + RES + "','[{\"value\":7},{\"value\":-9223372036854775808},{\"value\":1.25},"
              + "{\"value\":9223372036854775808}]')").evaluate(ctx);
    }

    final AggregatePair countThenSum =
        vectorizedAggregatePairWithoutPathSummary(versioning, 1, "count", "sum", "value");
    assertEquals("4\n", countThenSum.first(), versioning + " count-only scan");
    assertEquals("8.25\n", countThenSum.second(), versioning + " count cache poisoned sum");

    final AggregatePair sumThenCount =
        vectorizedAggregatePairWithoutPathSummary(versioning, 1, "sum", "count", "value");
    assertEquals("8.25\n", sumThenCount.first(), versioning + " full typed scan");
    assertEquals("4\n", sumThenCount.second(), versioning + " typed scan did not supply presence count");
  }

  /** A third, disjoint partial delta remains exact under DIFFERENTIAL's cumulative-delta merge. */
  @ParameterizedTest
  @EnumSource(value = VersioningType.class, names = {"DIFFERENTIAL"})
  void differentialThirdRevisionMergesDisjointDeltasCorrectly(final VersioningType versioning) throws Exception {
    dbDir = Files.createTempDirectory("sirix-versioning-differential-mask-");
    shred(versioning);
    updateAndRemoveInSecondRevision(versioning);

    try (var store = newStore(versioning);
        var ctx =
            SirixQueryContext.createWithJsonStoreAndCommitStrategy(store, SirixQueryContext.CommitStrategy.EXPLICIT);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain, "for $r in jn:doc('" + DB + "','" + RES + "')[] where $r.id ge " + REMOVED_BELOW
          + " and $r.title eq \"t42\" return replace json value of $r.year with 2200").evaluate(ctx);
      new Query(chain, "for $r in jn:doc('" + DB + "','" + RES + "')[] where $r.id ge " + REMOVED_BELOW
          + " and $r.title eq \"t43\" return delete json $r.year").evaluate(ctx);
      ctx.applyUpdates();
    }
    for (int i = REMOVED_BELOW; i < N; i++) {
      if (i % 97 == 42) {
        year[i] = 2200;
      } else if (i % 97 == 43) {
        yearAbsent[i] = true;
      }
    }

    SirixVectorizedExecutor.resetRegionOnlyCounters();
    for (final String predicate : new String[] {"$u.year eq 2200", "$u.year gt 1990", "$u.year gt 1899"}) {
      final long expected = groundTruth(predicate);
      assertEquals(expected, count(versioning, predicate, false), "record path: " + predicate);
      Databases.getGlobalBufferManager().getRecordPageCache().clear();
      assertEquals(expected, count(versioning, predicate, true), "column path: " + predicate);
    }
    assertTrue(SirixVectorizedExecutor.regionMergedPages() > 0,
        "DIFFERENTIAL never entered its third-revision fragment merge");
  }

  /**
   * Fragments whose numeric field spans BOTH types are merged from columns, split by the stored field
   * ordinals.
   *
   * <p>
   * Revision 2 writes {@code 2100.5} — a double — over the first {@value #UPDATED_BELOW} years, so
   * the newer fragment's year column is all doubles while the older keeps longs, some shadowed.
   * Counting such a page means projecting one anchor-slot liveness bitmap into a long-column mask and
   * a double-column mask — what the double region's field-ordinal list exists for. Before it, any
   * fragment whose long tag count fell short of its anchor slots went straight back to record
   * reconstruction. FULL is excluded the same way as above: it writes complete pages and has nothing
   * to merge.
   */
  @ParameterizedTest
  @EnumSource(value = VersioningType.class, mode = EnumSource.Mode.EXCLUDE, names = {"FULL"})
  void mixedTypeFragmentsMergeBothColumns(final VersioningType versioning) throws Exception {
    dbDir = Files.createTempDirectory("sirix-versioning-mixed-");
    shred(versioning);
    yearIsDouble = new boolean[N];
    try (var store = newStore(versioning);
        var ctx =
            SirixQueryContext.createWithJsonStoreAndCommitStrategy(store, SirixQueryContext.CommitStrategy.EXPLICIT);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain, "for $r in jn:doc('" + DB + "','" + RES + "')[] where $r.id lt " + UPDATED_BELOW
          + " return replace json value of $r.year with 2100.5").evaluate(ctx);
      ctx.applyUpdates();
    }

    yearFractionalValue = new BigDecimal("2100.5");
    for (int i = 0; i < UPDATED_BELOW; i++) {
      year[i] = 2100;
      yearIsDouble[i] = true;
    }

    SirixVectorizedExecutor.resetRegionOnlyCounters();
    final long mergedBefore = SirixVectorizedExecutor.regionMergedPages();
    for (final String predicate : new String[] {"$u.year gt 2100", "$u.year le 2100", "$u.year gt 1990",
        "$u.year ge 2100.5"}) {
      final long expected = groundTruth(predicate);
      assertEquals(expected, count(versioning, predicate, false), versioning + " record path: " + predicate);
      // Cold, for the same reason as above: a resident page was already version-merged by the
      // page layer, and the fragment merge would never run.
      Databases.getGlobalBufferManager().getRecordPageCache().clear();
      assertEquals(expected, count(versioning, predicate, true), versioning + " merged column path: " + predicate);
      assertTrue(expected > 0, "predicate matches nothing, so it proves nothing: " + predicate);
    }
    assertTrue(SirixVectorizedExecutor.regionMergedPages() > mergedBefore,
        versioning + ": the mixed-type fragments were never merged from columns (served="
            + SirixVectorizedExecutor.regionOnlyPagesServed() + ", fellBack="
            + SirixVectorizedExecutor.regionOnlyPageFallbacks() + ')');
  }

  /**
   * A threshold with NO faithful double image is served by the fragment merge, in decimal space.
   *
   * <p>
   * {@code 2100.5} is dyadic and every test above leans on that without saying so: its double image
   * is the decimal, so the merge can answer it from double bounds and never has to reach the
   * exact-decimal kernel at all. {@code 2100.55} is not, which is the ordinary case — 19.99, 100.10,
   * essentially every real price. The plan then marks its double bounds UNSERVABLE and carries the
   * threshold only as an exact decimal interval.
   *
   * <p>
   * What that used to cost: the merge's entry gate asked whether the DOUBLE bounds were servable and
   * refused before any tag's encoding was read, so the exact-decimal arm behind it could not be
   * reached by the very predicates it exists for, and every such page went back to the record heap.
   * Hence both assertions — agreement alone would have passed on the record path all along, and
   * {@code regionMergedPages()} is what says the columns answered it.
   *
   * <p>
   * The refusal for a NON-decimal tag under such a threshold is still absolute, and must stay that
   * way: {@code dlo}/{@code dhi} are NaN there, every comparison under NaN is false, and the kernel's
   * {@code !(dlo <= dhi)} guard would score an unanswerable tag as a clean ZERO.
   */
  @ParameterizedTest
  @EnumSource(value = VersioningType.class, mode = EnumSource.Mode.EXCLUDE, names = {"FULL"})
  void inexactThresholdsMergeThroughTheDecimalKernel(final VersioningType versioning) throws Exception {
    dbDir = Files.createTempDirectory("sirix-versioning-inexact-");
    shred(versioning);
    yearIsDouble = new boolean[N];
    try (var store = newStore(versioning);
        var ctx =
            SirixQueryContext.createWithJsonStoreAndCommitStrategy(store, SirixQueryContext.CommitStrategy.EXPLICIT);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      // By TITLE, not by id range: t0 recurs every 97 records, so EVERY page ends up with a
      // fragment carrying decimals. An id-prefixed update leaves most pages long-only, and those
      // pages merge through the long arm no matter what the double gate does — the counter below
      // would then stay positive whether or not the decimal arm is reachable at all.
      new Query(chain, "for $r in jn:doc('" + DB + "','" + RES + "')[] where $r.title eq 't0'"
          + " return replace json value of $r.year with 2100.55").evaluate(ctx);
      ctx.applyUpdates();
    }
    yearFractionalValue = new BigDecimal("2100.55");
    for (int i = 0; i < N; i++) {
      if (i % 97 == 0) {
        year[i] = 2100;
        yearIsDouble[i] = true;
      }
    }

    SirixVectorizedExecutor.resetRegionOnlyCounters();
    final long mergedBefore = SirixVectorizedExecutor.regionMergedPages();
    for (final String predicate : new String[] {"$u.year gt 2100.54", "$u.year ge 2100.55", "$u.year lt 2100.55",
        "$u.year gt 1990.55"}) {
      final long expected = groundTruth(predicate);
      assertEquals(expected, count(versioning, predicate, false), versioning + " record path: " + predicate);
      Databases.getGlobalBufferManager().getRecordPageCache().clear();
      assertEquals(expected, count(versioning, predicate, true), versioning + " merged column path: " + predicate);
      assertTrue(expected > 0, "predicate matches nothing, so it proves nothing: " + predicate);
    }
    assertTrue(SirixVectorizedExecutor.regionMergedPages() > mergedBefore,
        versioning + ": an inexact threshold never reached the fragment merge, so the "
            + "exact-decimal arm behind the entry gate is unreachable for exactly the "
            + "predicates it exists for (served=" + SirixVectorizedExecutor.regionOnlyPagesServed() + ", fellBack="
            + SirixVectorizedExecutor.regionOnlyPageFallbacks() + ')');
  }

  /**
   * Disjunctions over mixed-type FRAGMENTS: the shape the merge used to decline outright, sending
   * every OR/IN predicate on an updated resource back to record reconstruction.
   *
   * <p>
   * Each disjoint interval is now one masked kernel pass per fragment, on both columns — the long
   * union through the number region, the branch-folded double union through the double region. The
   * sharp case rides along from the single-fragment tests: {@code le 2099 or ge 2101} must EXCLUDE
   * the 2100.5 records even though the merged long union would swallow everything.
   */
  @ParameterizedTest
  @EnumSource(value = VersioningType.class, mode = EnumSource.Mode.EXCLUDE, names = {"FULL"})
  void disjunctionsMergeMixedTypeFragments(final VersioningType versioning) throws Exception {
    dbDir = Files.createTempDirectory("sirix-versioning-mixed-or-");
    shred(versioning);
    yearIsDouble = new boolean[N];
    try (var store = newStore(versioning);
        var ctx =
            SirixQueryContext.createWithJsonStoreAndCommitStrategy(store, SirixQueryContext.CommitStrategy.EXPLICIT);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain, "for $r in jn:doc('" + DB + "','" + RES + "')[] where $r.id lt " + UPDATED_BELOW
          + " return replace json value of $r.year with 2100.5").evaluate(ctx);
      ctx.applyUpdates();
    }
    yearFractionalValue = new BigDecimal("2100.5");
    for (int i = 0; i < UPDATED_BELOW; i++) {
      year[i] = 2100;
      yearIsDouble[i] = true;
    }

    SirixVectorizedExecutor.resetRegionOnlyCounters();
    final long mergedBefore = SirixVectorizedExecutor.regionMergedPages();
    for (final String predicate : new String[] {"$u.year lt 1950 or $u.year gt 2100",
        "$u.year le 2099 or $u.year ge 2101", "$u.year eq 1950 or $u.year eq 2020 or $u.year ge 2100.5",}) {
      long expected = 0;
      for (final String branch : predicate.split(" or ")) {
        // Branches are disjoint by construction here, so the union is the sum.
        expected += groundTruth(branch);
      }
      assertEquals(expected, count(versioning, predicate, false), versioning + " record path: " + predicate);
      Databases.getGlobalBufferManager().getRecordPageCache().clear();
      assertEquals(expected, count(versioning, predicate, true), versioning + " merged column path: " + predicate);
      assertTrue(expected > 0, "predicate matches nothing, so it proves nothing: " + predicate);
    }
    assertTrue(SirixVectorizedExecutor.regionMergedPages() > mergedBefore,
        versioning + ": disjunctions never reached the fragment merge");
  }

  /**
   * A FUSED (cross-column) predicate over a versioned resource is served from the reconstructed page
   * — the positional-engine answer to the one shape the fragment merge cannot take.
   *
   * <p>
   * Intersecting row sets across columns needs every column in one coordinate space, which per-commit
   * fragments do not share. The reconstruction IS that space: the versioning layer merges the
   * fragments into one slotted page, and the region-only read now derives whatever columns the
   * predicate needs from the resident merged page instead of returning "no columns". The choreography
   * this test pins: the FIRST columnar query on a cold cache declines per page (fragments, no
   * alignment) and reconstructs through the record path; the SECOND finds the merged pages resident,
   * derives their columns — field names bringing the record-ordinal linkage the fused kernel's
   * alignment certificate requires — and serves with ZERO fallbacks. This mirrors
   * DuckDB/ClickHouse/Umbra, where versioning is resolved by materializing the storage unit
   * positionally before predicates run, not by merging deltas per predicate.
   */
  @ParameterizedTest
  @EnumSource(value = VersioningType.class, mode = EnumSource.Mode.EXCLUDE, names = {"FULL"})
  void fusedPredicatesServeFromReconstructedPages(final VersioningType versioning) throws Exception {
    dbDir = Files.createTempDirectory("sirix-versioning-fused-");
    shred(versioning);
    // Update ONLY — no removals, so every record keeps both fields and every reconstructed page
    // passes the fused kernel's completeness and alignment checks.
    try (var store = newStore(versioning);
        var ctx =
            SirixQueryContext.createWithJsonStoreAndCommitStrategy(store, SirixQueryContext.CommitStrategy.EXPLICIT);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain, "for $r in jn:doc('" + DB + "','" + RES + "')[] where $r.id lt " + UPDATED_BELOW
          + " return replace json value of $r.year with 2100").evaluate(ctx);
      ctx.applyUpdates();
    }
    for (int i = 0; i < UPDATED_BELOW; i++) {
      year[i] = 2100;
    }

    final String predicate = "$u.year gt 1990 and $u.id lt 3000";
    long expected = 0;
    for (int i = 0; i < N; i++) {
      if (year[i] > 1990 && i < 3000) {
        expected++;
      }
    }
    assertTrue(expected > 0, "predicate matches nothing, so it proves nothing");

    // Run 1, cold: multi-fragment pages have no shared alignment, so the fused plan declines and
    // the record path answers — reconstructing and caching each page as it goes.
    Databases.getGlobalBufferManager().getRecordPageCache().clear();
    assertEquals(expected, count(versioning, predicate, true), versioning + " cold fused column path: " + predicate);

    // Run 2, warm: every touched page is resident as a reconstructed single page; the read derives
    // the fused mask's columns from it and the kernel serves — no fallback anywhere.
    SirixVectorizedExecutor.resetRegionOnlyCounters();
    assertEquals(expected, count(versioning, predicate, true),
        versioning + " reconstructed fused column path: " + predicate);
    assertTrue(SirixVectorizedExecutor.regionOnlyPagesServed() > 0,
        versioning + ": no page served the fused plan from reconstructed columns (served="
            + SirixVectorizedExecutor.regionOnlyPagesServed() + ", fellBack="
            + SirixVectorizedExecutor.regionOnlyPageFallbacks() + ", unavailable="
            + SirixVectorizedExecutor.regionOnlyPagesUnavailable() + ')');
    assertEquals(0, SirixVectorizedExecutor.regionOnlyPageFallbacks(),
        versioning + ": a reconstructed page fell back — a fused column failed to derive");
    assertEquals(0, SirixVectorizedExecutor.regionOnlyPagesUnavailable(),
        versioning + ": a page was still unavailable on the warm run");
    // And the record path agrees, closing the differential.
    assertEquals(expected, count(versioning, predicate, false), versioning + " record path: " + predicate);
  }

  /**
   * The same reconstructed-page contract with a STRING leaf in the fused plan.
   *
   * <p>
   * Regression coverage for a gap the numeric-only test could not see: the on-demand region
   * derivation used to reach the string column through a pure payload getter that never builds, so
   * every fused plan touching a string field found the reconstructed table incomplete and fell back —
   * the feature simply never engaged for this plan shape, and no test noticed.
   */
  @ParameterizedTest
  @EnumSource(value = VersioningType.class, names = {"DIFFERENTIAL", "INCREMENTAL", "SLIDING_SNAPSHOT"})
  void fusedStringPredicatesServeFromReconstructedPages(final VersioningType versioning) throws Exception {
    dbDir = Files.createTempDirectory("sirix-versioning-fused-str-");
    shred(versioning);
    // Update ONLY, and only the year field — the predicate's fields stay untouched, but the pages
    // holding them become multi-fragment all the same, which is the case under test.
    try (var store = newStore(versioning);
        var ctx =
            SirixQueryContext.createWithJsonStoreAndCommitStrategy(store, SirixQueryContext.CommitStrategy.EXPLICIT);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain, "for $r in jn:doc('" + DB + "','" + RES + "')[] where $r.id lt " + UPDATED_BELOW
          + " return replace json value of $r.year with 2100").evaluate(ctx);
      ctx.applyUpdates();
    }
    for (int i = 0; i < UPDATED_BELOW; i++) {
      year[i] = 2100;
    }

    final String predicate = "$u.title eq \"t42\" and $u.id lt 3000";
    long expected = 0;
    for (int i = 0; i < 3000; i++) {
      if (i % 97 == 42) {
        expected++;
      }
    }
    assertTrue(expected > 0, "predicate matches nothing, so it proves nothing");

    Databases.getGlobalBufferManager().getRecordPageCache().clear();
    assertEquals(expected, count(versioning, predicate, true),
        versioning + " cold fused string column path: " + predicate);

    SirixVectorizedExecutor.resetRegionOnlyCounters();
    assertEquals(expected, count(versioning, predicate, true),
        versioning + " reconstructed fused string column path: " + predicate);
    assertTrue(SirixVectorizedExecutor.regionOnlyPagesServed() > 0,
        versioning + ": no page served the fused string plan from reconstructed columns" + " (served="
            + SirixVectorizedExecutor.regionOnlyPagesServed() + ", fellBack="
            + SirixVectorizedExecutor.regionOnlyPageFallbacks() + ", unavailable="
            + SirixVectorizedExecutor.regionOnlyPagesUnavailable() + ')');
    assertEquals(0, SirixVectorizedExecutor.regionOnlyPageFallbacks(),
        versioning + ": a reconstructed page fell back — the string column failed to derive");
    assertEquals(0, SirixVectorizedExecutor.regionOnlyPagesUnavailable(),
        versioning + ": a page was still unavailable on the warm run");
    assertEquals(expected, count(versioning, predicate, false), versioning + " record path: " + predicate);
  }

  private void shred(final VersioningType versioning) throws Exception {
    final Random rng = new Random(0xC01DBEEFL);
    year = new long[N];
    yearAbsent = new boolean[N];
    final StringBuilder sb = new StringBuilder(N * 48);
    sb.append('[');
    for (int i = 0; i < N; i++) {
      if (i > 0)
        sb.append(',');
      year[i] = 1900 + rng.nextInt(124);
      sb.append("{\"id\":")
        .append(i)
        .append(",\"year\":")
        .append(year[i])
        .append(",\"title\":\"t")
        .append(i % 97)
        .append("\"}");
    }
    sb.append(']');

    try (var store = newStore(versioning);
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain, "jn:store('" + DB + "','" + RES + "','" + sb + "')").evaluate(ctx);
    }
  }

  private void updateAndRemoveInSecondRevision(final VersioningType versioning) throws Exception {
    try (var store = newStore(versioning);
        var ctx =
            SirixQueryContext.createWithJsonStoreAndCommitStrategy(store, SirixQueryContext.CommitStrategy.EXPLICIT);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain, "for $r in jn:doc('" + DB + "','" + RES + "')[] where $r.id lt " + UPDATED_BELOW
          + " return replace json value of $r.year with 2100").evaluate(ctx);
      new Query(chain, "for $r in jn:doc('" + DB + "','" + RES + "')[] where $r.id ge " + UPDATED_BELOW
          + " and $r.id lt " + REMOVED_BELOW + " return delete json $r.year").evaluate(ctx);
      ctx.applyUpdates();
    }
    for (int i = 0; i < UPDATED_BELOW; i++) {
      year[i] = 2100;
    }
    for (int i = UPDATED_BELOW; i < REMOVED_BELOW; i++) {
      yearAbsent[i] = true;
    }
  }

  private BasicJsonDBStore newStore(final VersioningType versioning) {
    return BasicJsonDBStore.newBuilder().location(dbDir).buildPathSummary(true).versioningType(versioning).build();
  }

  /** Disable optional value statistics; structural field counts may still use PathNode references. */
  private BasicJsonDBStore newStoreWithoutPathStatistics(final VersioningType versioning) {
    return BasicJsonDBStore.newBuilder()
                           .location(dbDir)
                           .buildPathSummary(true)
                           .buildPathStatistics(false)
                           .versioningType(versioning)
                           .build();
  }

  private BasicJsonDBStore newStoreWithoutPathSummary(final VersioningType versioning) {
    return BasicJsonDBStore.newBuilder()
                           .location(dbDir)
                           .buildPathSummary(false)
                           .buildPathStatistics(false)
                           .versioningType(versioning)
                           .build();
  }

  private long count(final VersioningType versioning, final String predicate, final boolean columns) throws Exception {
    return count(versioning, predicate, columns, -1);
  }

  private long count(final VersioningType versioning, final String predicate, final boolean columns,
      final int requestedRevision) throws Exception {
    try (var store = newStore(versioning);
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      final var coll = store.lookup(DB);
      final var resourceSession = coll.getDatabase().beginResourceSession(RES);
      try {
        final int revision = requestedRevision > 0
            ? requestedRevision
            : resourceSession.getMostRecentRevisionNumber();
        final var exec = new SirixVectorizedExecutor(resourceSession, revision);
        // Per executor, so the A/B is between two queries rather than between two moments in a
        // JVM other tests share.
        exec.setRegionOnlyCountEnabled(columns);
        SequentialPipelineStrategy.setVectorizedExecutor(exec);
        try {
          final String document = requestedRevision > 0
              ? "jn:doc('" + DB + "','" + RES + "'," + requestedRevision + ')'
              : "jn:doc('" + DB + "','" + RES + "')";
          final String q = "count(for $u in " + document + "[] where " + predicate + " return $u)";
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

  private void assertColdRecordAndColumnCount(final VersioningType versioning, final int revision,
      final String predicate, final long expected, final boolean expectFallback, final boolean expectServed)
      throws Exception {
    Databases.getGlobalBufferManager().getRecordPageCache().clear();
    assertEquals(expected, count(versioning, predicate, false, revision),
        versioning + " revision " + revision + " record path: " + predicate);
    Databases.getGlobalBufferManager().getRecordPageCache().clear();
    SirixVectorizedExecutor.resetRegionOnlyCounters();
    assertEquals(expected, count(versioning, predicate, true, revision),
        versioning + " revision " + revision + " column path: " + predicate);
    if (expectFallback) {
      assertTrue(
          SirixVectorizedExecutor.regionOnlyPagesUnavailable() > 0
              || SirixVectorizedExecutor.regionOnlyPageFallbacks() > 0,
          versioning + " revision " + revision + " served an overflow page from incomplete columns (served="
              + SirixVectorizedExecutor.regionOnlyPagesServed() + ", fellBack="
              + SirixVectorizedExecutor.regionOnlyPageFallbacks() + ", unavailable="
              + SirixVectorizedExecutor.regionOnlyPagesUnavailable() + ')');
    }
    if (expectServed) {
      assertTrue(SirixVectorizedExecutor.regionOnlyPagesServed() > 0,
          versioning + " revision " + revision + " did not serve its complete columns");
    }
  }

  private void assertColdAggregateFallbackDifferential(final VersioningType versioning, final int revision)
      throws Exception {
    Databases.getGlobalBufferManager().getRecordPageCache().clear();
    final String interpretedCount = interpretedAggregateSnapshot(versioning, revision, "count", "value");
    final String interpretedSum = interpretedAggregateSnapshot(versioning, revision, "sum", "value");
    assertEquals("4\n", interpretedCount, versioning + " revision " + revision + " interpreted count oracle");
    assertEquals("8.25\n", interpretedSum, versioning + " revision " + revision + " interpreted sum oracle");

    // Structural count and the full typed fold have independent serving/cache routes. Query order
    // must not change either answer; the no-path-summary test above exercises the count-only cache
    // itself rather than the stronger PathNode reference shortcut used here.
    Databases.getGlobalBufferManager().getRecordPageCache().clear();
    final AggregatePair countThenSum = vectorizedAggregatePair(versioning, revision, "count", "sum", "value");
    assertEquals(interpretedCount, countThenSum.first(),
        versioning + " revision " + revision + " count-first structural result");
    assertEquals(interpretedSum, countThenSum.second(),
        versioning + " revision " + revision + " count-first result changed the later sum");

    Databases.getGlobalBufferManager().getRecordPageCache().clear();
    final AggregatePair sumThenCount = vectorizedAggregatePair(versioning, revision, "sum", "count", "value");
    assertEquals(interpretedSum, sumThenCount.first(), versioning + " revision " + revision + " sum-first typed scan");
    assertEquals(interpretedCount, sumThenCount.second(),
        versioning + " revision " + revision + " sum-first result changed the structural count");
  }

  private void assertFieldCountMatchesInterpreterAndSummary(final VersioningType versioning, final int revision,
      final String field, final long expected) throws Exception {
    final String interpreted = interpretedAggregateSnapshot(versioning, revision, "count", field);
    assertEquals(expected + "\n", interpreted,
        versioning + " revision " + revision + " interpreted structural count oracle");

    Databases.getGlobalBufferManager().getRecordPageCache().clear();
    try (var store = newStoreWithoutPathStatistics(versioning);
        var resourceSession = store.lookup(DB).getDatabase().beginResourceSession(RES)) {
      final SirixVectorizedExecutor executor = new SirixVectorizedExecutor(resourceSession, revision);
      try {
        final long servedBefore = SirixVectorizedExecutor.pathSummaryStatsServed();
        final String actual = serialize(executor.executeAggregate(null, new String[] {"[]"}, "count", field));
        assertEquals(interpreted, actual,
            versioning + " revision " + revision + " structural count differed from interpreter");
        assertTrue(SirixVectorizedExecutor.pathSummaryStatsServed() > servedBefore, versioning + " revision " + revision
            + " did not use the exact PathNode reference count with value statistics disabled");
      } finally {
        executor.close();
      }
    }
  }

  /** Positive witness that version reconstruction retained the allocation-free fused-long image. */
  private void assertDirectFusedIntegralReadable(final VersioningType versioning, final int revision) throws Exception {
    try (var store = newStoreWithoutPathStatistics(versioning);
        var resourceSession = store.lookup(DB).getDatabase().beginResourceSession(RES);
        var rtx = resourceSession.beginNodeReadOnlyTrx(revision)) {
      final int fieldKey = rtx.keyForName("value");
      final var reader = rtx.getStorageEngineReader();
      final long lastPage = rtx.getMaxNodeKey() >>> Constants.INP_REFERENCE_COUNT_EXPONENT;
      boolean found = false;
      for (long pageKey = 0L; pageKey <= lastPage && !found; pageKey++) {
        final var result = reader.getRecordPage(new IndexLogKey(IndexType.DOCUMENT, pageKey, 0, revision));
        if (result == null || !(result.page() instanceof KeyValueLeafPage page)) {
          continue;
        }
        for (final int slot : page.getObjectKeySlotsForNameKey(fieldKey)) {
          if (page.getSlotNodeKindId(slot) == KeyValueLeafPage.FUSED_OBJECT_NAMED_NUMBER_KIND_ID
              && page.getFusedObjectNamedNumberValueLongFromSlot(slot) == 7L) {
            found = true;
            break;
          }
        }
      }
      assertTrue(found, versioning + " revision " + revision
          + " lost the directly readable fused-long slot during version reconstruction");
    }
  }

  private String interpretedAggregateSnapshot(final VersioningType versioning, final int revision, final String func,
      final String field) throws Exception {
    try (var store = newStoreWithoutPathStatistics(versioning);
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store)) {
      final String document = "jn:doc('" + DB + "','" + RES + "'," + revision + ')';
      return serialize(new Query(chain, func + "(for $r in " + document + "[] return $r." + field + ")").execute(ctx));
    }
  }

  private AggregatePair vectorizedAggregatePair(final VersioningType versioning, final int revision,
      final String firstFunc, final String secondFunc, final String field) throws Exception {
    try (var store = newStoreWithoutPathStatistics(versioning);
        var resourceSession = store.lookup(DB).getDatabase().beginResourceSession(RES)) {
      final SirixVectorizedExecutor executor = new SirixVectorizedExecutor(resourceSession, revision);
      try {
        final String[] sourcePath = {"[]"};
        final String first = serialize(executor.executeAggregate(null, sourcePath, firstFunc, field));
        final String second = serialize(executor.executeAggregate(null, sourcePath, secondFunc, field));
        return new AggregatePair(first, second);
      } finally {
        executor.close();
      }
    }
  }

  private AggregatePair vectorizedAggregatePairWithoutPathSummary(final VersioningType versioning, final int revision,
      final String firstFunc, final String secondFunc, final String field) throws Exception {
    try (var store = newStoreWithoutPathSummary(versioning);
        var resourceSession = store.lookup(DB).getDatabase().beginResourceSession(RES)) {
      final SirixVectorizedExecutor executor = new SirixVectorizedExecutor(resourceSession, revision);
      try {
        final String[] sourcePath = {"[]"};
        final String first = serialize(executor.executeAggregate(null, sourcePath, firstFunc, field));
        final String second = serialize(executor.executeAggregate(null, sourcePath, secondFunc, field));
        return new AggregatePair(first, second);
      } finally {
        executor.close();
      }
    }
  }

  private static String serialize(final Sequence result) throws Exception {
    final StringWriter out = new StringWriter();
    try (PrintWriter writer = new PrintWriter(out)) {
      new StringSerializer(writer).serialize(result);
      writer.print('\n');
    }
    return out.toString();
  }

  /**
   * In-memory truth for the {@code $u.year <op> <number>} dialect, fractional thresholds and
   * double-valued records included — ONE evaluator for every test in this class, so the mixed-type
   * tests cannot drift from the long-only ones.
   */
  private long groundTruth(final String predicate) {
    final String[] parts = predicate.trim().split("\\s+");
    final String op = parts[1];
    // DECIMAL, not double. jn:store ingests a fractional JSON number as an exact decimal and the
    // interpreter compares it exactly, so a threshold with no faithful double image — 2100.55 —
    // would give this oracle a different answer than the engine if it rounded first, and the test
    // would be measuring its own arithmetic.
    final BigDecimal threshold = new BigDecimal(parts[2]);
    long c = 0;
    for (int i = 0; i < N; i++) {
      if (yearAbsent[i]) {
        continue; // a comparison over a removed field is never true
      }
      final BigDecimal v = yearIsDouble != null && yearIsDouble[i]
          ? yearFractionalValue
          : BigDecimal.valueOf(year[i]);
      final int cmp = v.compareTo(threshold);
      final boolean hit = switch (op) {
        case "gt" -> cmp > 0;
        case "ge" -> cmp >= 0;
        case "lt" -> cmp < 0;
        case "le" -> cmp <= 0;
        case "eq" -> cmp == 0;
        default -> throw new IllegalArgumentException(op);
      };
      if (hit)
        c++;
    }
    return c;
  }
}
