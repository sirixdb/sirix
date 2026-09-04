package io.sirix.query.scan;

import io.brackit.query.Query;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.util.serialize.StringSerializer;
import io.sirix.access.Databases;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.projection.ProjectionIndexCatalog;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Differential gate for the STRING predicate leaves (ordering {@code lt/le/gt/ge} and
 * {@code fn:contains}) through the full served route — brackit detection, predicate compilation,
 * the dict-set kernels, and the group-aggregate serve. Every vectorized run asserts the served
 * counter moved, so a silent decline cannot pass as agreement.
 *
 * <p>
 * Selectivity is SWEPT (common / mid / rare / none / absent-from-dictionary) — a wrong-answer bug
 * once hid behind a common literal for a whole session, its error scaling with rarity. The corpus
 * also carries a SUPPLEMENTARY character (U+10400, a 4-byte UTF-8 sequence) alongside a BMP
 * character in U+E000..U+FFFF (U+FF01) — the exact pair where raw UTF-8 byte order and the
 * interpreter's UTF-16 {@code String.compareTo} order DISAGREE, so a kernel comparing bytes without
 * the 4-byte-lead fallback inverts their order.
 */
public final class StringPredicateDifferentialTest {

  private static final int N = 1_777;
  private static final String DB = "strpred-db";
  private static final String RES = "records.jn";
  private static final String SRC = "jn:doc('" + DB + "','" + RES + "')[]";

  private java.nio.file.Path dbDir;

  @BeforeEach
  void setUp() throws Exception {
    dbDir = Files.createTempDirectory("sirix-strpred-");
    final Random rng = new Random(17);
    final StringBuilder sb = new StringBuilder(N * 80);
    sb.append('[');
    for (int i = 0; i < N; i++) {
      if (i > 0) {
        sb.append(',');
      }
      // date: 60 distinct ISO days across two months — range predicates cut at month
      // boundaries (common), mid-month (mid), the last day (rare), and outside (none).
      final int day = i % 60;
      final String date = day < 30
          ? "2013-07-" + String.format("%02d", day + 1)
          : "2013-08-" + String.format("%02d", day - 29);
      sb.append("{\"id\":").append(i);
      sb.append(",\"date\":\"").append(date).append('"');
      // url: ~15% contain "google", 1 in 1777 contains "zanzibar", none contain "xyzzy".
      sb.append(",\"url\":\"http://");
      if (i % 7 == 0) {
        sb.append("www.google.com/p").append(i % 31);
      } else if (i == 911) {
        sb.append("zanzibar.example.org/only");
      } else {
        sb.append("site").append(i % 41).append(".example.com/p").append(i % 13);
      }
      sb.append('"');
      // sup: the collation adversary. U+FF01 (BMP, 3-byte UTF-8: EF BC 81) vs U+10400
      // (supplementary, 4-byte UTF-8: F0 90 90 80). UTF-8 byte order says FF01 < 10400;
      // UTF-16 code-unit order says U+10400 (surrogate D801) < U+FF01. The interpreter uses
      // the latter.
      sb.append(",\"sup\":\"")
        .append(i % 3 == 0
            ? "！mark"
            : "𐐀deseret")
        .append('"');
      if (i % 5 != 0) {
        sb.append(",\"tier\":\"t").append(i % 4).append('"'); // sparse: missing rows must not match
      }
      sb.append('}');
    }
    sb.append(']');
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain, "jn:store('" + DB + "','" + RES + "','" + sb + "')").evaluate(ctx);
      createProjectionIndex(chain, ctx);
    }
    ProjectionIndexCatalog.clearCache();
  }

  @AfterEach
  void tearDown() {
    ProjectionIndexCatalog.clearCache();
    if (dbDir != null) {
      Databases.removeDatabase(dbDir);
    }
  }

  // ---- ordering, selectivity swept -----------------------------------------------------------

  @Test
  void dateRangeSelectivitySweep() throws Exception {
    // common: half the corpus; mid: one month; rare: one day; none: outside the domain.
    assertGroupServedDifferential("$u.date ge \"2013-07-01\" and $u.date le \"2013-07-31\"");
    assertGroupServedDifferential("$u.date ge \"2013-07-10\" and $u.date le \"2013-07-20\"");
    assertGroupServedDifferential("$u.date ge \"2013-08-30\" and $u.date le \"2013-08-30\"");
    assertGroupServedDifferential("$u.date gt \"2014-01-01\"");
  }

  @Test
  void singleSidedBoundsAndLiteralOnTheLeft() throws Exception {
    assertGroupServedDifferential("$u.date lt \"2013-07-05\"");
    // Literal on the LEFT: the detection must REVERSE the operator ("2013-07-05" gt $u.date
    // is $u.date lt "2013-07-05") — asymmetric, unlike eq/ne.
    assertGroupServedDifferential("\"2013-07-05\" gt $u.date");
  }

  @Test
  void orderingLiteralAbsentFromEveryDictionary() throws Exception {
    // A bound BETWEEN two stored values: matches must come out exactly, not via any
    // dict-id shortcut keyed on literal presence.
    assertGroupServedDifferential("$u.date ge \"2013-07-15T12\"");
  }

  @Test
  void orderingOverSparseFieldExcludesMissingRows() throws Exception {
    // tier is missing on 20% of records: `ge ""` is TRUE for every present value, so a
    // kernel that read missing as empty-string would overcount by exactly the sparse rows.
    assertGroupServedDifferential("$u.tier ge \"\"");
    assertGroupServedDifferential("$u.tier le \"t9\"");
  }

  @Test
  void supplementaryCharacterOrderingMatchesTheInterpreter() throws Exception {
    // The UTF-8-vs-UTF-16 divergence pair: U+10400 orders BELOW U+FF01 in the interpreter's
    // collation but ABOVE it in raw byte order. Both directions swept.
    assertGroupServedDifferential("$u.sup lt \"！\"");
    assertGroupServedDifferential("$u.sup ge \"！\"");
    assertGroupServedDifferential("$u.sup le \"𐐀deseret\"");
  }

  // ---- contains, selectivity swept -----------------------------------------------------------

  @Test
  void containsSelectivitySweep() throws Exception {
    assertGroupServedDifferential("contains($u.url, \"example\")"); // common
    assertGroupServedDifferential("contains($u.url, \"google\")"); // ~15%
    assertGroupServedDifferential("contains($u.url, \"zanzibar\")"); // one row
    assertGroupServedDifferential("contains($u.url, \"xyzzy\")"); // none
  }

  @Test
  void containsIsSubstringNotWholeValue() throws Exception {
    // No stored value EQUALS "google" — only contains it. A fingerprint/bloom shortcut keyed
    // on whole-value hashes would prune every leaf and answer zero.
    assertGroupServedDifferential("contains($u.url, \"www.google.com\")");
  }

  @Test
  void containsOverSparseFieldIsFalseOnMissing() throws Exception {
    // contains((), "t") is false — missing rows must not match even an empty-ish needle.
    assertGroupServedDifferential("contains($u.tier, \"t\")");
  }

  @Test
  void containsAndOrderingConjunction() throws Exception {
    assertGroupServedDifferential(
        "$u.date ge \"2013-07-01\" and $u.date le \"2013-07-31\" and contains($u.url, \"google\")");
  }

  // ---- negation (OP_NOT in the mask algebra) --------------------------------------------------

  @Test
  void notContainsSelectivitySweep() throws Exception {
    assertGroupServedDifferential("not(contains($u.url, \"example\"))"); // excludes the common mass
    assertGroupServedDifferential("not(contains($u.url, \"google\"))");
    assertGroupServedDifferential("not(contains($u.url, \"zanzibar\"))"); // excludes one row
    assertGroupServedDifferential("not(contains($u.url, \"xyzzy\"))"); // excludes nothing
  }

  @Test
  void notContainsOverSparseFieldIsTrueOnMissing() throws Exception {
    // THE truth-table flip negation exists for: contains over a MISSING tier is false, so
    // its negation must ADMIT the ~20% of rows without the field. An implementation that
    // ANDs presence back in after the complement drops exactly those rows.
    assertGroupServedDifferential("not(contains($u.tier, \"t\"))");
    assertGroupServedDifferential("not(contains($u.tier, \"t1\"))");
  }

  @Test
  void notComposesWithConjunctionDisjunctionAndItself() throws Exception {
    // The Q22 shape: a positive contains AND a negated contains AND an inequality.
    assertGroupServedDifferential(
        "contains($u.url, \"google\") and not(contains($u.url, \"www.\")) and $u.tier != \"\"");
    // De Morgan probe: NOT over a disjunction.
    assertGroupServedDifferential("not(contains($u.url, \"google\") or contains($u.url, \"example\"))");
    // Double negation collapses to the original truth table, missing rows included.
    assertGroupServedDifferential("not(not(contains($u.tier, \"t1\")))");
    // NOT over an ordering leaf and over a numeric comparison.
    assertGroupServedDifferential("not($u.date lt \"2013-07-15\")");
    assertGroupServedDifferential("not($u.id ge 900) and contains($u.url, \"example\")");
  }

  // ---- harness --------------------------------------------------------------------------------

  /**
   * Runs the ClickBench shape — filtered group-by, ordered by count, subsequence-capped — through
   * both pipelines, requiring byte-identical output AND that the group-aggregate route actually
   * served (the predicate must flow through the projection kernels, or agreement proves nothing).
   */
  private void assertGroupServedDifferential(final String where) throws Exception {
    final String query = "subsequence(for $u in " + SRC + " where " + where + " let $k := $u.id group by $k "
        + "let $c := count($u) order by $c descending return {\"k\": $k, \"c\": $c}, 1, 7)";
    final String interpreted = run(query, false);
    final long before = SirixVectorizedExecutor.groupAggServedCount();
    final String vectorized = run(query, true);
    assertTrue(SirixVectorizedExecutor.groupAggServedCount() > before,
        "predicate did NOT flow through the served group-aggregate route: " + where);
    assertEquals(interpreted, vectorized, "served result differs for predicate: " + where);
  }

  private String run(final String query, final boolean vectorized) throws Exception {
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = vectorized
            ? SirixCompileChain.createWithJsonStore(store)
            : SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store)) {
      SirixVectorizedExecutor exec = null;
      try {
        if (vectorized) {
          final var db = Databases.openJsonDatabase(dbDir.resolve(DB));
          final JsonResourceSession session = db.beginResourceSession(RES);
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

  private static void createProjectionIndex(final SirixCompileChain chain, final SirixQueryContext context) {
    new Query(chain, """
        let $doc := jn:doc('%s','%s')
        let $stats := jn:create-projection-index($doc, '/[]',
          ('/[]/id', '/[]/date', '/[]/url', '/[]/sup', '/[]/tier'),
          ('long', 'string', 'string', 'string', 'string'))
        return sdb:commit($doc)
        """.formatted(DB, RES)).evaluate(context);
  }
}
