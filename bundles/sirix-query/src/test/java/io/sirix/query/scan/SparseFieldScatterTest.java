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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Multi-field predicates over OPTIONAL fields — the shape the columnar route used to refuse.
 *
 * <h2>What was wrong</h2>
 *
 * <p>
 * A fused column plan read every field positionally: past its share of the page's skip prefix, a
 * field's occurrences had to enumerate the window's records one-for-one, or the page was handed
 * back to record reconstruction. A field carried by only some records satisfies no such thing, and
 * optional fields are the normal JSON shape — measured on a movie corpus, one of them declined
 * 14,400 pages of a two-predicate conjunction, each page paying its column read AND a full record
 * rebuild, which put the columnar route 22x behind the row pipeline it exists to beat.
 *
 * <p>
 * The linkage column already stores each occurrence's record ordinal, so the values were never
 * unattributable — only unreadable positionally. The scatter arm evaluates such a field over its
 * own occurrences and routes each result to its record's row.
 *
 * <h2>What these tests hold it to</h2>
 *
 * <p>
 * Every shape is checked against the same query with the column path switched off, which is the
 * only ground truth that cannot drift with the kernels. Agreement alone would pass vacuously — a
 * page nobody serves columnar agrees for free — so the shapes that must be SERVED assert the
 * scatter counter moved, and the shape that must be REFUSED asserts it did not.
 *
 * <p>
 * The corpus is built so records straddle page seams in every position: slot counts per record vary
 * with the field mix, so the seam offset walks through the record rather than sitting at one place.
 * That matters because a record left open at a page's end is exactly where "this field is absent"
 * and "this field is on the next page" look alike from one page's columns — the case the arm hands
 * to the record path instead of guessing.
 */
final class SparseFieldScatterTest {

  private static final int N = 12_000;
  /** Flat records with three optional fields. */
  private static final String FLAT = "sparse-scatter-flat";
  /** The same fields, but every record wraps one in a nested object. */
  private static final String NESTED = "sparse-scatter-nested";
  /** One resource per database: {@code jn:store} recreates a collection it is handed twice. */
  private static final String RES = "records.jn";

  /**
   * Conjunctions that must now be served from columns. Each pairs a dense field with a field only
   * some records carry, which is what the scatter arm exists for; the second one makes the OPTIONAL
   * field the anchor, so the window's rows are no longer the record ordinals themselves.
   */
  private static final List<String> SCATTERED_SHAPES = List.of("$u.year gt 1990 and $u.width gt 200",
      "$u.year gt 1990 and $u.width lt 200", "$u.width gt 200 and $u.year lt 1995",
      "$u.year gt 1990 and $u.genre eq 'drama'", "$u.year gt 1990 and $u.width gt 200 and $u.genre eq 'drama'",
      // No record carries this genre, so the string arm decides the leaf from the page dictionary
      // alone and never reads a value column — the route on which "select nothing" is expressed by
      // leaving the bitmap untouched. It follows a leaf that DOES write one, which is the order
      // under which a reused scratch would be read as this leaf's answer.
      "$u.year gt 1990 and $u.width gt 200 and $u.genre eq 'noir'");

  /**
   * Shapes the arm must not get WRONG, whoever ends up answering them. A per-field disjunction inside
   * a conjunction is not a plan the region planner folds today, so these go through the records; they
   * are here because the day it does fold one, this is the assertion that catches a scattered leaf
   * composing badly under {@code Or}.
   */
  private static final List<String> AGREEMENT_ONLY_SHAPES =
      List.of("$u.year gt 1990 and ($u.width lt 120 or $u.width gt 500)",
          "$u.year gt 1990 and ($u.genre eq 'drama' or $u.width gt 500)");

  /** Controls: every field is on every record, so the dense arm answers and nothing scatters. */
  private static final List<String> DENSE_SHAPES =
      List.of("$u.year gt 1990 and $u.active", "$u.year gt 1990 and $u.year lt 1995");

  private Path dbDir;

  @BeforeEach
  void setUp() throws Exception {
    dbDir = Files.createTempDirectory("sirix-sparse-scatter-");
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain, "jn:store('" + FLAT + "','" + RES + "','" + corpus(false) + "')").evaluate(ctx);
      new Query(chain, "jn:store('" + NESTED + "','" + RES + "','" + corpus(true) + "')").evaluate(ctx);
    }
  }

  /**
   * {@code width} on 55 % of records, {@code genre} on 40 %, both independent of each other and of
   * the record's length, so the field mix — and with it the record's slot count — changes from record
   * to record and the page seam lands at a different point inside a record on every page.
   *
   * @param nested wrap a field in an object, which splits the record's own keys around the nested
   *        object's and takes the whole page out of the arm's reach
   */
  private static String corpus(final boolean nested) {
    final StringBuilder sb = new StringBuilder(N * 72);
    sb.append('[');
    for (int i = 0; i < N; i++) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append("{\"active\":").append(i % 3 != 0).append(",\"year\":").append(1980 + i % 40);
      if (i % 20 < 11) {
        sb.append(",\"width\":").append(80 + i % 640);
      }
      if (nested) {
        sb.append(",\"meta\":{\"seq\":").append(i).append('}');
      }
      if (i % 5 < 2) {
        sb.append(",\"genre\":\"")
          .append(i % 2 == 0
              ? "drama"
              : "comedy")
          .append('"');
      }
      // Always last, so a nested object splits the record's keys rather than merely preceding them.
      sb.append(",\"title\":\"t").append(i).append("\"}");
    }
    return sb.append(']').toString();
  }

  @AfterEach
  void tearDown() {
    SequentialPipelineStrategy.setVectorizedExecutor(null);
    if (dbDir != null) {
      Databases.removeDatabase(dbDir.resolve(FLAT));
      Databases.removeDatabase(dbDir.resolve(NESTED));
    }
  }

  @Test
  @DisplayName("a conjunction touching an optional field agrees with the record path")
  void scatteredShapesAgreeWithTheRecordPath() throws Exception {
    for (final String predicate : allShapes()) {
      assertEquals(count(FLAT, predicate, false), count(FLAT, predicate, true),
          "column path disagrees with the record path for: " + predicate
              + " — an optional field was either read positionally against records it "
              + "does not sit on, or a record left open at a page seam was read as " + "lacking the field");
    }
  }

  @Test
  @DisplayName("those pages are SERVED from columns, not quietly reconstructed")
  void scatteredShapesAreServedFromColumns() throws Exception {
    for (final String predicate : SCATTERED_SHAPES) {
      SirixVectorizedExecutor.resetRegionOnlyCounters();
      count(FLAT, predicate, true);
      assertTrue(SirixVectorizedExecutor.regionScatterPages() > 0,
          "no page was answered by scattering for: " + predicate
              + " — agreement is then vacuous, because the pages went through the records "
              + "exactly as they did before the arm existed");
    }
  }

  @Test
  @DisplayName("a conjunction over fields every record carries still takes the positional arm")
  void denseShapesDoNotScatter() throws Exception {
    for (final String predicate : DENSE_SHAPES) {
      SirixVectorizedExecutor.resetRegionOnlyCounters();
      final long viaColumns = count(FLAT, predicate, true);
      assertEquals(count(FLAT, predicate, false), viaColumns, "column path disagrees: " + predicate);
      assertEquals(0L, SirixVectorizedExecutor.regionScatterPages(),
          "a page scattered for: " + predicate
              + " — every field here is on every record, so the positional arm has the "
              + "geometry it wants and the slower arm must not be reached");
    }
  }

  /** Every shape this test holds to record-path agreement. */
  private static List<String> allShapes() {
    return Stream.concat(SCATTERED_SHAPES.stream(), AGREEMENT_ONLY_SHAPES.stream()).toList();
  }

  @Test
  @DisplayName("records whose keys are split by a nested object are refused, not misread")
  void interleavedRecordsAreRefused() throws Exception {
    for (final String predicate : allShapes()) {
      SirixVectorizedExecutor.resetRegionOnlyCounters();
      final long viaColumns = count(NESTED, predicate, true);
      assertEquals(count(NESTED, predicate, false), viaColumns,
          "column path disagrees with the record path for: " + predicate);
      // With a nested object between a record's own keys, the linkage reads 0,1,0 and a record
      // other than the page's last can continue past the seam — so absence stops being decidable
      // from one page and the arm has to keep its hands off the whole page.
      assertEquals(0L, SirixVectorizedExecutor.regionScatterPages(),
          "a page with interleaved records was scattered for: " + predicate
              + " — absence there can be a field the next page holds, and reading it as "
              + "an absent field undercounts");
    }
  }

  @Test
  @DisplayName("a repeat scan, scheduled from the published page-skip bitmap, still agrees")
  void repeatScanAgrees() throws Exception {
    for (final String predicate : SCATTERED_SHAPES) {
      final long viaRecords = count(FLAT, predicate, false);
      assertEquals(viaRecords, count(FLAT, predicate, true), "first column scan disagrees: " + predicate);
      assertEquals(viaRecords, count(FLAT, predicate, true),
          "the SECOND column scan disagrees for: " + predicate + " — a boundary handoff outlived the page that set it");
    }
  }

  private long count(final String database, final String predicate, final boolean regionOnly) throws Exception {
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      final var coll = store.lookup(database);
      final var resourceSession = coll.getDatabase().beginResourceSession(RES);
      try {
        final var exec = new SirixVectorizedExecutor(resourceSession, resourceSession.getMostRecentRevisionNumber());
        exec.setRegionOnlyCountEnabled(regionOnly);
        SequentialPipelineStrategy.setVectorizedExecutor(exec);
        try {
          return ((Int64) new Query(chain,
              "count(for $u in jn:doc('" + database + "','" + RES + "')[] where " + predicate + " return $u)").evaluate(
                  ctx)).longValue();
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
