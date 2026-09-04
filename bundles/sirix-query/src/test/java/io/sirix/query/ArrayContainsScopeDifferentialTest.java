package io.sirix.query;

import io.brackit.query.Query;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.Int64;
import io.brackit.query.compiler.translator.SequentialPipelineStrategy;
import io.sirix.access.Databases;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.node.NodeKind;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.scan.SirixVectorizedExecutor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The array-membership COLUMN route against the generic pipeline on every shape where the columns
 * alone cannot prove which array a value belongs to.
 *
 * <p>
 * Anchors are selected by NAME KEY only, element strings are tagged with their array LAYER's path
 * node key, and an array spilled onto the next page leaves orphan elements with no identity at all.
 * A same-name array nested elsewhere in the record is therefore indistinguishable from the
 * top-level one at the anchor, and the route has to prove scope three ways: the layer key on the
 * values it counts, the ordinal certificate on the gap it claims, and the record path — WITH the
 * source matcher — on every seam with no element to prove it. Each fixture here breaks exactly one
 * of those proofs, and the mutation seams show that the guard closing it is load-bearing rather
 * than decorative: an arm with the guard bypassed must answer WRONG.
 * </p>
 *
 * <p>
 * {@code jn:store} shreds sequentially, so node keys are document order (root 0, the top-level
 * array 1, records from 2), which is what lets the seam fixture put a nested array's node on the
 * LAST slot of a page by arithmetic rather than by luck.
 * </p>
 */
final class ArrayContainsScopeDifferentialTest {
  private static final String DB = "array-contains-scope-db";
  private static final int SLOTS = 1024;

  private Path dbDir;

  @BeforeEach
  void setUp() throws Exception {
    resetSeams();
    dbDir = Files.createTempDirectory("sirix-array-contains-scope-");
  }

  @AfterEach
  void tearDown() {
    resetSeams();
    SequentialPipelineStrategy.setVectorizedExecutor(null);
    if (dbDir != null) {
      Databases.removeDatabase(dbDir.resolve(DB));
    }
  }

  private static void resetSeams() {
    KeyValueLeafPage.ARRAY_ELEMENT_STRINGS_IN_REGION = false;
    KeyValueLeafPage.ELEMENT_STAGING_PURITY = true;
    SirixVectorizedExecutor.ARRAY_CONTAINS_COLUMNAR_ENABLED = false;
    SirixVectorizedExecutor.SEAM_SETTLE_EMPTY_GAP_FROM_ORPHANS = false;
    SirixVectorizedExecutor.SEAM_UNSCOPED_SEAM_RECORDS = false;
    SirixVectorizedExecutor.SEAM_SKIP_ELEMENT_PROMISE = false;
  }

  // ---------------------------------------------------------------------------------------------
  // Corpus A: top-level arrays with a sparse same-name array nested AFTER them
  // ---------------------------------------------------------------------------------------------

  @Test
  @DisplayName("a same-name array nested after the top-level one never leaks into its membership")
  void nestedSameNameArraysDoNotLeak() throws Exception {
    final String res = "corpus-a.jn";
    final int n = 3_000;
    final StringBuilder sb = new StringBuilder(n * 64);
    sb.append('[');
    long topOne = 0;
    long both = 0;
    for (int i = 0; i < n; i++) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append("{\"id\":").append(i).append(",\"genres\":[\"Top").append(i % 3).append('"');
      if (i % 3 == 1) {
        topOne++;
      }
      if (i % 11 == 0) {
        sb.append(",\"Both\"");
        both++;
      }
      sb.append(']');
      // Mixed elements in an UNQUERIED array on every record: a value comparison across types is a
      // type error in the interpreter, so no valid query can ask about them.
      sb.append(",\"tags\":[3,true]");
      if (i % 331 == 0) {
        // The nested same-name array: disjoint literal, plus the shared one, AFTER the top-level array
        // so the nested array's slots follow the anchor's in every page.
        sb.append(",\"meta\":{\"genres\":[\"Nested").append(i % 2).append("\",\"Both\"]}");
      }
      sb.append('}');
    }
    sb.append(']');
    store(res, sb.toString());
    final String nested = "some $g in $m.genres[] satisfies $g eq 'Nested0'";
    final String top = "some $g in $m.genres[] satisfies $g eq 'Top1'";
    final String shared = "some $g in $m.genres[] satisfies $g eq 'Both'";
    assertEquals(0L, generic(res, nested),
        "the generic pipeline: a nested-only literal is no member of the top-level array");
    assertEquals(topOne, generic(res, top));
    assertEquals(both, generic(res, shared));
    long served = 0;
    for (final String predicate : new String[] {nested, top, shared}) {
      SirixVectorizedExecutor.resetRegionOnlyCounters();
      assertEquals(generic(res, predicate), columns(res, predicate), "column route vs generic for: " + predicate);
      served += SirixVectorizedExecutor.regionOnlyPagesServed();
    }
    assertTrue(served > 0, "the column route served no page: the agreement above is vacuous");
  }

  // ---------------------------------------------------------------------------------------------
  // Corpus B: the field exists ONLY nested
  // ---------------------------------------------------------------------------------------------

  @Test
  @DisplayName("a field that exists only nested has an empty top-level membership")
  void nestedOnlyFieldHasEmptyTopLevelMembership() throws Exception {
    final String res = "corpus-b.jn";
    final int n = 1_200;
    final StringBuilder sb = new StringBuilder(n * 48);
    sb.append('[');
    for (int i = 0; i < n; i++) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append("{\"id\":")
        .append(i)
        .append(",\"meta\":{\"genres\":[\"N")
        .append(i % 3)
        .append("\"]},\"z\":")
        .append(i)
        .append('}');
    }
    sb.append(']');
    store(res, sb.toString());
    final String predicate = "some $g in $m.genres[] satisfies $g eq 'N1'";
    assertEquals(0L, generic(res, predicate));
    assertEquals(0L, columns(res, predicate), "the column route counted a nested-only field as a top-level member");
  }

  // ---------------------------------------------------------------------------------------------
  // The seam fixture: a nested same-name array node on the LAST slot of a page
  // ---------------------------------------------------------------------------------------------

  /**
   * Seven-node records: record J's object is at key 2 + 7J, so the nested array of the one record
   * with a nested same-name array sits at key 7J + 6; J = 1023 puts it at slot 1023 of page 6, and
   * its element at slot 0 of page 7. The filler records are FLAT (every field a child of the record
   * object): a record spanning a page boundary then has one off-page parent, which is the only shape
   * the record-ordinal linkage admits — a nested object at the seam would decline the page and the
   * mutation arms could not bite.
   */
  private static String seamCorpus(final int n, final int nestedAt, final boolean nestedBareArrayAfter) {
    final StringBuilder sb = new StringBuilder(n * 56);
    sb.append('[');
    for (int i = 0; i < n; i++) {
      if (i > 0) {
        sb.append(',');
      }
      if (i == nestedAt) {
        sb.append("{\"genres\":[\"Top\"],\"meta\":{\"genres\":[\"Nested\"]},\"f\":1}");
      } else if (nestedBareArrayAfter && i == nestedAt + 1) {
        sb.append("{\"genres\":[\"Top\"],\"a\":1,\"b\":2,\"c\":3,\"d\":4,\"p\":[[\"x\"]]}");
      } else {
        sb.append("{\"genres\":[\"Top\"],\"a\":1,\"b\":2,\"c\":3,\"d\":4}");
      }
    }
    sb.append(']');
    return sb.toString();
  }

  private void assertSeamShape(final String res) {
    final long nestedArrayKey = 7L * 1023 + 6; // 7167 = 7 * 1024 - 1
    assertEquals(SLOTS - 1, nestedArrayKey % SLOTS, "the fixture's arithmetic");
    try (var db = Databases.openJsonDatabase(dbDir.resolve(DB));
        JsonResourceSession session = db.beginResourceSession(res);
        JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
      assertTrue(rtx.moveTo(nestedArrayKey));
      assertEquals(NodeKind.OBJECT_NAMED_ARRAY, rtx.getKind(), "the nested array node on the last slot of page 6");
      assertEquals("genres", rtx.getName().getLocalName());
      assertTrue(rtx.moveToParent());
      assertEquals(NodeKind.OBJECT_NAMED_OBJECT, rtx.getKind(), "its parent is the nested object, not the record");
      assertEquals("meta", rtx.getName().getLocalName());
      assertTrue(rtx.moveTo(nestedArrayKey + 1));
      assertEquals(NodeKind.STRING_VALUE, rtx.getKind(), "its element opens page 7 as an orphan");
      assertEquals("Nested", rtx.getValue());
    }
  }

  @Test
  @DisplayName("an anchor with an empty gap is settled by the records with the matcher, never by the next page's orphans")
  void emptyGapSeamIsNotSettledFromOrphans() throws Exception {
    final String res = "seam-u.jn";
    final int n = 1_400;
    store(res, seamCorpus(n, 1023, false));
    assertSeamShape(res);
    final String nested = "some $g in $m.genres[] satisfies $g eq 'Nested'";
    final String top = "some $g in $m.genres[] satisfies $g eq 'Top'";
    assertEquals(0L, generic(res, nested));
    assertEquals(n, generic(res, top));
    SirixVectorizedExecutor.resetRegionOnlyCounters();
    assertEquals(0L, columns(res, nested),
        "the column route credited the nested array's orphan to the top-level anchor");
    assertTrue(SirixVectorizedExecutor.regionOnlyPagesServed() > 0, "the column route served no page: vacuous");
    assertEquals(n, columns(res, top));
    // Mutation: settle the empty-gap trailing anchor from page 7's orphan tag, as HEAD did. The
    // anchor at slot 1023 is the NESTED array (same name key), its gap is empty, and "Nested" is
    // page 7's orphan — so the refuted route counts record 1023.
    SirixVectorizedExecutor.SEAM_SETTLE_EMPTY_GAP_FROM_ORPHANS = true;
    assertEquals(1L, columns(res, nested), "the R1 guard is not load-bearing: bypassing it did not change the answer");
  }

  @Test
  @DisplayName("the seam record path applies the source matcher")
  void seamRecordPathAppliesTheMatcher() throws Exception {
    final String res = "seam-p.jn";
    final int n = 1_400;
    store(res, seamCorpus(n, 1023, true));
    assertSeamShape(res);
    final String nested = "some $g in $m.genres[] satisfies $g eq 'Nested'";
    assertEquals(0L, generic(res, nested));
    assertEquals(0L, columns(res, nested));
    // Page 7 holds a bare ARRAY element of a fused array (record 1024's "p"), so its element staging
    // is refused and the orphan lookup is undecidable: the empty-gap anchor at slot 1023 reaches the
    // record path either way. Only the matcher rejects it there — the nested array DOES contain the
    // literal.
    SirixVectorizedExecutor.SEAM_UNSCOPED_SEAM_RECORDS = true;
    assertEquals(1L, columns(res, nested),
        "the seam matcher is not load-bearing: bypassing it did not change the answer");
  }

  // ---------------------------------------------------------------------------------------------
  // F1 / W2: arrays of objects
  // ---------------------------------------------------------------------------------------------

  @Test
  @DisplayName("a page whose element staging was refused is declined, not served as zero")
  void refusedStagingIsDeclinedNotServedAsZero() throws Exception {
    final String res = "f1.jn";
    final int n = 600;
    final StringBuilder sb = new StringBuilder(n * 32);
    sb.append('[');
    for (int i = 0; i < n; i++) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append("{\"t\":\"s\",\"genres\":[{\"a\":1},\"Drama\"]}");
    }
    sb.append(']');
    store(res, sb.toString());
    // Truth by construction: the interpreter cannot atomize the OBJECT element (FOTY0012), so the
    // generic pipeline is no oracle here; the column route treats a non-string element as no match.
    final String predicate = "some $g in $m.genres[] satisfies $g eq 'Drama'";
    SirixVectorizedExecutor.resetRegionOnlyCounters();
    assertEquals(n, columns(res, predicate));
    assertEquals(0L, SirixVectorizedExecutor.regionOnlyPagesServed(),
        "an OBJECT element of a fused array poisons the page's staging: every page must decline");
    // Mutation: without the promise check, the unstaged pages carry no tags at all, the certificate
    // balances at zero for every anchor, and the route SERVES zero for records that match.
    SirixVectorizedExecutor.SEAM_SKIP_ELEMENT_PROMISE = true;
    assertNotEquals(n, columns(res, predicate),
        "the promise check is not load-bearing: bypassing it kept the answer exact");
  }

  @Test
  @DisplayName("mixed arrays of scalars and objects under one name are exact")
  void mixedArraysUnderOneNameAreExact() throws Exception {
    final String res = "w2.jn";
    final StringBuilder sb = new StringBuilder(4096);
    sb.append('[');
    for (int i = 0; i < 90; i++) {
      if (i > 0) {
        sb.append(',');
      }
      switch (i % 3) {
        case 0 -> sb.append("{\"genres\":[\"Comedy\",3,4]}");
        default -> sb.append("{\"genres\":[{\"a\":1},\"Drama\"]}");
      }
    }
    sb.append(']');
    store(res, sb.toString());
    // Truth by construction (numbers and objects beside the strings are type errors for the
    // interpreter's comparison; the column route treats them as no match).
    final String predicate = "some $g in $m.genres[] satisfies $g eq 'Drama'";
    assertEquals(60L, columns(res, predicate));
  }

  // ---------------------------------------------------------------------------------------------
  // W3: an array spilled across pages whose orphan run is not all strings
  // ---------------------------------------------------------------------------------------------

  @Test
  @DisplayName("a spilled array whose orphan run holds numbers still finds its last string")
  void spilledArrayWithNumericOrphansFindsItsLastString() throws Exception {
    final String res = "w3.jn";
    store(res, w3Corpus());
    // Truth by construction: exactly one record holds "Zzz" (the interpreter cannot compare the
    // numeric elements with a string, so it is no oracle here). Page 0 is served, "Zzz" is not on
    // it, and the all-numeric page 1 has no string column at all — the orphan lookup must answer
    // undecidable there and hand the record to the record path, never "absent".
    final String predicate = "some $g in $m.genres[] satisfies $g eq 'Zzz'";
    SirixVectorizedExecutor.resetRegionOnlyCounters();
    assertEquals(1L, columns(res, predicate), "the spilled array's last string was lost behind its numeric orphans");
    assertTrue(SirixVectorizedExecutor.regionOnlyPagesServed() > 0,
        "page 0 was not served: the seam was never reached");
  }

  /**
   * Record 0's array fills page 0 with strings (keys 4..1023, so the anchor's own gap balances the
   * certificate and page 0 is SERVED), continues through page 1 with nothing but numbers (keys
   * 1024..2047, the orphan run the purity rule refuses), and ends with "Zzz" at key 2048 — slot 0 of
   * page 2. Numbers inside page 0's gap would have declined the page before the orphan rule mattered.
   */
  private static String w3Corpus() {
    final StringBuilder sb = new StringBuilder(1020 * 6 + 1024 * 5 + 300 * 24);
    sb.append("[{\"genres\":[");
    for (int i = 0; i < 1020; i++) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append("\"S").append(i % 16).append('"');
    }
    for (int i = 0; i < 1024; i++) {
      sb.append(',').append(i % 10);
    }
    sb.append(",\"Zzz\"]}");
    for (int i = 0; i < 300; i++) {
      sb.append(",{\"genres\":[\"Comedy\"]}");
    }
    sb.append(']');
    return sb.toString();
  }

  // ---------------------------------------------------------------------------------------------
  // R2(i): a nested object BEFORE the array in every record
  // ---------------------------------------------------------------------------------------------

  @Test
  @DisplayName("an object before the array in every record is served, not declined as unclaimed values")
  void nestedObjectBeforeTheArrayIsServed() throws Exception {
    final String res = "r2.jn";
    final int n = 1_500;
    final StringBuilder sb = new StringBuilder(n * 40);
    sb.append('[');
    for (int i = 0; i < n; i++) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append("{\"meta\":{\"x\":1},\"genres\":[\"Drama\"]}");
    }
    sb.append(']');
    store(res, sb.toString());
    final String predicate = "some $g in $m.genres[] satisfies $g eq 'Drama'";
    assertEquals(n, generic(res, predicate));
    SirixVectorizedExecutor.resetRegionOnlyCounters();
    assertEquals(n, columns(res, predicate));
    assertTrue(SirixVectorizedExecutor.regionOnlyPagesServed() > 0,
        "the certificate subtracted phantom record boundaries and declined every page");
  }

  // ---------------------------------------------------------------------------------------------
  // (e) and (f): shapes the route must not misread
  // ---------------------------------------------------------------------------------------------

  @Test
  @DisplayName("a string-valued field has no array members")
  void stringValuedFieldHasNoArrayMembers() throws Exception {
    final String res = "e.jn";
    final StringBuilder sb = new StringBuilder(400 * 20);
    sb.append('[');
    for (int i = 0; i < 400; i++) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append("{\"genres\":\"Drama\"}");
    }
    sb.append(']');
    store(res, sb.toString());
    final String predicate = "some $g in $m.genres[] satisfies $g eq 'Drama'";
    final String source = "jn:doc('" + DB + "','" + res + "')[]";
    // `[]` over a string is a type error (XPTY0004) in the interpreter; the auto-wired route must not
    // turn that into a silent zero.
    assertEquals(outcome(source, predicate, false, false), outcome(source, predicate, true, true),
        "the auto-wired route and the interpreter disagree on `[]` over a string-valued field");
  }

  @Test
  @DisplayName("a field that is an array in some records and a string in others raises in both routes")
  void mixedArrayAndStringFieldRaisesInBothRoutes() throws Exception {
    final String res = "e-mixed.jn";
    final StringBuilder sb = new StringBuilder(2_000 * 24);
    sb.append('[');
    for (int i = 0; i < 2_000; i++) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append(i % 5 == 0
          ? "{\"genres\":\"Drama\"}"
          : "{\"genres\":[\"Drama\"]}");
    }
    sb.append(']');
    store(res, sb.toString());
    final String predicate = "some $g in $m.genres[] satisfies $g eq 'Drama'";
    final String source = "jn:doc('" + DB + "','" + res + "')[]";
    final String viaInterpreter = outcome(source, predicate, false, false);
    assertTrue(viaInterpreter.startsWith("error="), "the interpreter raises on `[]` over a string: " + viaInterpreter);
    assertEquals(viaInterpreter, outcome(source, predicate, true, true),
        "the auto-wired route answered where the interpreter raises — the summary gate let a mixed field through");
  }

  @Test
  @DisplayName("a nested source with a foreign same-name top-level array is exact and declines")
  void nestedSourceWithForeignTopLevelArrayIsExact() throws Exception {
    final String res = "f.jn";
    final int n = 900;
    final StringBuilder sb = new StringBuilder(n * 56);
    sb.append('[');
    for (int i = 0; i < n; i++) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append("{\"genres\":[\"Top\"],\"items\":[{\"genres\":[\"Inner\"]}]}");
    }
    sb.append(']');
    store(res, sb.toString());
    final String source = "jn:doc('" + DB + "','" + res + "')[].items[]";
    final String inner = "some $g in $m.genres[] satisfies $g eq 'Inner'";
    final String top = "some $g in $m.genres[] satisfies $g eq 'Top'";
    assertEquals(n, count(source, inner, false, false));
    assertEquals(0L, count(source, top, false, false));
    SirixVectorizedExecutor.resetRegionOnlyCounters();
    assertEquals(n, count(source, inner, true, true));
    assertEquals(0L, count(source, top, true, true));
    assertEquals(0L, SirixVectorizedExecutor.regionOnlyPagesServed(),
        "item objects are OBJECT children of a fused array: every page's staging is refused");
  }

  // ---------------------------------------------------------------------------------------------
  // helpers
  // ---------------------------------------------------------------------------------------------

  private void store(final String resource, final String json) throws Exception {
    // Read at serialization AND at derivation, so it must be on before the store.
    KeyValueLeafPage.ARRAY_ELEMENT_STRINGS_IN_REGION = true;
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain, "jn:store('" + DB + "','" + resource + "','" + json + "')").evaluate(ctx);
    }
  }

  private long generic(final String resource, final String predicate) throws Exception {
    return count("jn:doc('" + DB + "','" + resource + "')[]", predicate, false, false);
  }

  private long columns(final String resource, final String predicate) throws Exception {
    return count("jn:doc('" + DB + "','" + resource + "')[]", predicate, true, true);
  }

  private String outcome(final String source, final String predicate, final boolean autoWire, final boolean columnar)
      throws Exception {
    try {
      return "count=" + count(source, predicate, autoWire, columnar);
    } catch (final QueryException e) {
      return "error=" + e.getCode();
    }
  }

  private long count(final String source, final String predicate, final boolean autoWire, final boolean columnar)
      throws Exception {
    SirixVectorizedExecutor.ARRAY_CONTAINS_COLUMNAR_ENABLED = columnar;
    try (var store = BasicJsonDBStore.newBuilder().location(dbDir).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = autoWire
            ? SirixCompileChain.createWithJsonStore(store)
            : SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store)) {
      return ((Int64) new Query(chain, "count(for $m in " + source + " where " + predicate + " return $m)").evaluate(
          ctx)).longValue();
    }
  }
}
