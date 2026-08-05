package io.sirix.query.scan;

import io.brackit.query.Query;
import io.brackit.query.atomic.Int64;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.sirix.access.Databases;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The column-scan path against every versioning strategy.
 *
 * <p>Versioning is where this path is easiest to get wrong, because each strategy assembles a page
 * from a different set of fragments and the column merge has to reach the same answer as the record
 * reconstruction without ever building a record. {@link VersioningType#FULL} writes complete pages
 * and exercises the single-page read; the other three leave pages spanning commits and exercise the
 * merge. Running one test body over all of them is deliberate — a strategy-specific mistake shows up
 * as one enum value failing rather than as a number nobody re-derives.
 *
 * <p>Each case writes a second revision that both <em>updates</em> and <em>removes</em> values, the
 * two cases the merge rule turns on: an updated slot must be counted once with its new value, and a
 * removed one must not have its old value resurrected from an older fragment.
 *
 * <p>The removals also cover a write-path defect this test originally exposed: under
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

    final String[] predicates = {
        "$u.year gt 1990",     // spans updated, removed and untouched records
        "$u.year eq 2100",     // exactly the updated ones
        "$u.year lt 1950",     // only untouched ones
        "$u.year gt 1899"      // every record that still HAS a year
    };

    SirixVectorizedExecutor.resetRegionOnlyCounters();
    for (final String predicate : predicates) {
      final long expected = groundTruth(predicate);
      assertEquals(expected, count(versioning, predicate, false),
                   versioning + " record path: " + predicate);
      // Cold for the column path. The record-path query above leaves every page it touched
      // resident, and getRecordPageRegionsOnly serves a resident page straight from its region
      // table -- that page was already version-merged by the page layer, so the fragment merge
      // never runs. Left warm, this test agreed with the record path on all four strategies while
      // countFragmentedPageFromRegions was never entered once (regionMergedPages() == 0), which is
      // the whole half of the versioned column path it exists to cover.
      Databases.getGlobalBufferManager().getRecordPageCache().clear();
      assertEquals(expected, count(versioning, predicate, true),
                   versioning + " column path: " + predicate);
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
    // DIFFERENTIAL is exempt on this fixture, and deliberately rather than by oversight: its window
    // is {cumulative delta, last full dump}, and with only two revisions the delta claims every
    // anchor slot the older fragment could have supplied, so the older one contributes nothing to
    // shadow. It is asserted to reach the merge (above) but not to mask. A fixture with a third
    // revision would exercise it; that is a gap in this test, not a property of the strategy.
    if (versioning != VersioningType.DIFFERENTIAL) {
      assertTrue(SirixVectorizedExecutor.regionMergeMaskedKernels() > 0,
                 versioning + ": every merged fragment was fully live, so the liveness masking "
                     + "never ran (unmasked="
                     + SirixVectorizedExecutor.regionMergeUnmaskedKernels() + ')');
    }
  }

  /**
   * Fragments whose numeric field spans BOTH types are merged from columns, split by the stored
   * field ordinals.
   *
   * <p>Revision 2 writes {@code 2100.5} — a double — over the first {@value #UPDATED_BELOW} years,
   * so the newer fragment's year column is all doubles while the older keeps longs, some shadowed.
   * Counting such a page means projecting one anchor-slot liveness bitmap into a long-column mask
   * and a double-column mask — what the double region's field-ordinal list exists for. Before it,
   * any fragment whose long tag count fell short of its anchor slots went straight back to record
   * reconstruction. FULL is excluded the same way as above: it writes complete pages and has
   * nothing to merge.
   */
  @ParameterizedTest
  @EnumSource(value = VersioningType.class, mode = EnumSource.Mode.EXCLUDE, names = { "FULL" })
  void mixedTypeFragmentsMergeBothColumns(final VersioningType versioning) throws Exception {
    dbDir = Files.createTempDirectory("sirix-versioning-mixed-");
    shred(versioning);
    try (var store = newStore(versioning);
         var ctx = SirixQueryContext.createWithJsonStoreAndCommitStrategy(
             store, SirixQueryContext.CommitStrategy.EXPLICIT);
         var chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain, "for $r in jn:doc('" + DB + "','" + RES + "')[] where $r.id lt "
          + UPDATED_BELOW + " return replace json value of $r.year with 2100.5").evaluate(ctx);
      ctx.applyUpdates();
    }

    SirixVectorizedExecutor.resetRegionOnlyCounters();
    final long mergedBefore = SirixVectorizedExecutor.regionMergedPages();
    for (final String predicate : new String[] { "$u.year gt 2100", "$u.year le 2100",
                                                 "$u.year gt 1990", "$u.year ge 2100.5" }) {
      // Truth over the mixed values: updated records compare as 2100.5, the rest as their longs.
      final String[] parts = predicate.trim().split("\\s+");
      final double threshold = Double.parseDouble(parts[2]);
      long expected = 0;
      for (int i = 0; i < N; i++) {
        final double v = i < UPDATED_BELOW ? 2100.5d : (double) year[i];
        final boolean hit = switch (parts[1]) {
          case "gt" -> v > threshold;
          case "ge" -> v >= threshold;
          case "lt" -> v < threshold;
          case "le" -> v <= threshold;
          default -> throw new IllegalArgumentException(parts[1]);
        };
        if (hit) expected++;
      }
      assertEquals(expected, count(versioning, predicate, false),
                   versioning + " record path: " + predicate);
      // Cold, for the same reason as above: a resident page was already version-merged by the
      // page layer, and the fragment merge would never run.
      Databases.getGlobalBufferManager().getRecordPageCache().clear();
      assertEquals(expected, count(versioning, predicate, true),
                   versioning + " merged column path: " + predicate);
      assertTrue(expected > 0, "predicate matches nothing, so it proves nothing: " + predicate);
    }
    assertTrue(SirixVectorizedExecutor.regionMergedPages() > mergedBefore,
               versioning + ": the mixed-type fragments were never merged from columns (served="
                   + SirixVectorizedExecutor.regionOnlyPagesServed() + ", fellBack="
                   + SirixVectorizedExecutor.regionOnlyPageFallbacks() + ')');
  }

  private void shred(final VersioningType versioning) throws Exception {
    final Random rng = new Random(0xC01DBEEFL);
    year = new long[N];
    yearAbsent = new boolean[N];
    final StringBuilder sb = new StringBuilder(N * 48);
    sb.append('[');
    for (int i = 0; i < N; i++) {
      if (i > 0) sb.append(',');
      year[i] = 1900 + rng.nextInt(124);
      sb.append("{\"id\":").append(i).append(",\"year\":").append(year[i])
        .append(",\"title\":\"t").append(i % 97).append("\"}");
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
         var ctx = SirixQueryContext.createWithJsonStoreAndCommitStrategy(
             store, SirixQueryContext.CommitStrategy.EXPLICIT);
         var chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain, "for $r in jn:doc('" + DB + "','" + RES + "')[] where $r.id lt " + UPDATED_BELOW
          + " return replace json value of $r.year with 2100").evaluate(ctx);
      new Query(chain, "for $r in jn:doc('" + DB + "','" + RES + "')[] where $r.id ge "
          + UPDATED_BELOW + " and $r.id lt " + REMOVED_BELOW + " return delete json $r.year")
          .evaluate(ctx);
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
    return BasicJsonDBStore.newBuilder()
                           .location(dbDir)
                           .buildPathSummary(true)
                           .versioningType(versioning)
                           .build();
  }

  private long count(final VersioningType versioning, final String predicate, final boolean columns)
      throws Exception {
    try (var store = newStore(versioning);
         var ctx = SirixQueryContext.createWithJsonStore(store);
         var chain = SirixCompileChain.createWithJsonStore(store)) {
      final var coll = store.lookup(DB);
      final var resourceSession = coll.getDatabase().beginResourceSession(RES);
      try {
        final var exec =
            new SirixVectorizedExecutor(resourceSession, resourceSession.getMostRecentRevisionNumber());
        // Per executor, so the A/B is between two queries rather than between two moments in a
        // JVM other tests share.
        exec.setRegionOnlyCountEnabled(columns);
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

  /** In-memory truth for the narrow {@code $u.year <op> <int>} dialect used above. */
  private long groundTruth(final String predicate) {
    final String[] parts = predicate.trim().split("\\s+");
    final String op = parts[1];
    final long threshold = Long.parseLong(parts[2]);
    long c = 0;
    for (int i = 0; i < N; i++) {
      if (yearAbsent[i]) {
        continue;  // a comparison over a removed field is never true
      }
      final long v = year[i];
      final boolean hit = switch (op) {
        case "gt" -> v > threshold;
        case "ge" -> v >= threshold;
        case "lt" -> v < threshold;
        case "le" -> v <= threshold;
        case "eq" -> v == threshold;
        default -> throw new IllegalArgumentException(op);
      };
      if (hit) c++;
    }
    return c;
  }
}
