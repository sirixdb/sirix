/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index;

import io.brackit.query.atomic.Atomic;
import io.brackit.query.atomic.DateTime;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.Path;
import io.brackit.query.util.path.PathParser;
import io.sirix.JsonTestHelper;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.IndexController;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.index.path.json.JsonPCRCollector;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.service.InsertPosition;
import io.sirix.service.json.shredder.JsonShredder;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Bound semantics of the bounded CAS scans.
 *
 * <p>
 * Two regressions are pinned here.
 *
 * <p>
 * <b>The byte window is wider than the logical range.</b> Index keys are NOT prefix-free — a CAS
 * string value is written as raw UTF-8 with no terminator or length prefix — so the cursor's
 * composite ceiling {@code serialize(max) ‖ 0xFFFFFFFF} covers every key that byte-EXTENDS
 * {@code max} ({@code "carpet"} sorts below the ceiling built for {@code "car"}, because
 * {@code 'p'} &lt; {@code 0xFF}). A bounded scan that trusted the byte window, or that trimmed an
 * excluded boundary value positionally (skip the first group if it equals min, the last if it
 * equals max), returned values outside the requested range.
 *
 * <p>
 * <b>A one-sided bound only pins the PCR at one end.</b> CAS keys serialize PCR-major (the
 * sign-flipped pathNodeKey is the first 8 bytes), so a two-sided range over a single PCR needs no
 * per-entry path check — both bounds carry the same 8-byte prefix. That does NOT extend to a
 * one-sided range: a {@code >= min} cursor runs off the end of its PCR into every higher one, and a
 * {@code <= max} cursor starts at the first key in the index, below every lower one. On an index
 * defined over several paths those neighbours are other paths' values.
 */
final class CASBoundedScanSemanticsTest {

  /** {@code car} is a strict prefix of {@code carpet} — the shape that breaks a byte-window bound. */
  private static final String JSON =
      "[{\"title\":\"apple\"},{\"title\":\"car\"},{\"title\":\"carpet\"},{\"title\":\"zebra\"}]";

  private static final String TITLE_PATH = "/[]/title";

  /**
   * Two indexed paths, hence two PCRs in one CAS index. {@code title} is declared first, so it takes
   * the lower pathNodeKey and its entries sort BELOW every {@code alias} entry — which puts one path
   * on each side of the other's unbounded end.
   */
  private static final String TWO_PATH_JSON = "[{\"title\":\"apple\",\"alias\":\"bravo\"},"
      + "{\"title\":\"car\",\"alias\":\"delta\"},{\"title\":\"zebra\",\"alias\":\"yankee\"}]";

  private static final String ALIAS_PATH = "/[]/alias";

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  @Test
  void boundedScansExcludeValuesOutsideTheRequestedRange() {
    withCASIndex(JSON, Set.of(TITLE_PATH), (indexController, rtx, casDef) -> {
      // <= "car" is {apple, car}: "carpet" is greater, however its composite sorts below the
      // ceiling built from "car".
      assertEquals(List.of("apple", "car"),
          valuesOf(rtx, filterHits(indexController, rtx, casDef, TITLE_PATH, "car", SearchMode.LOWER_OR_EQUAL)),
          "LOWER_OR_EQUAL(car)");

      // < "car" is {apple}: the equal group must be excluded even though another group follows it.
      assertEquals(List.of("apple"),
          valuesOf(rtx, filterHits(indexController, rtx, casDef, TITLE_PATH, "car", SearchMode.LOWER)), "LOWER(car)");

      // >= "car" is {car, carpet, zebra}; > "car" drops only the equal group.
      assertEquals(List.of("car", "carpet", "zebra"),
          valuesOf(rtx, filterHits(indexController, rtx, casDef, TITLE_PATH, "car", SearchMode.GREATER_OR_EQUAL)),
          "GREATER_OR_EQUAL(car)");
      assertEquals(List.of("carpet", "zebra"),
          valuesOf(rtx, filterHits(indexController, rtx, casDef, TITLE_PATH, "car", SearchMode.GREATER)),
          "GREATER(car)");

      // Range filters, each inclusivity combination.
      assertEquals(List.of("apple", "car"),
          valuesOf(rtx, rangeHits(indexController, rtx, casDef, TITLE_PATH, "a", "car", true, true)), "[a,car]");
      assertEquals(List.of("apple"),
          valuesOf(rtx, rangeHits(indexController, rtx, casDef, TITLE_PATH, "a", "car", true, false)), "[a,car)");
      assertEquals(List.of("carpet"),
          valuesOf(rtx, rangeHits(indexController, rtx, casDef, TITLE_PATH, "car", "zebra", false, false)),
          "(car,zebra)");
      assertEquals(List.of("car", "carpet", "zebra"),
          valuesOf(rtx, rangeHits(indexController, rtx, casDef, TITLE_PATH, "car", "zebra", true, true)),
          "[car,zebra]");
      assertEquals(List.of("apple", "car", "carpet", "zebra"),
          valuesOf(rtx, rangeHits(indexController, rtx, casDef, TITLE_PATH, "a", "zz", true, true)), "[a,zz]");

      // A window that straddles no value at all: "car" < "d" < "e" < "zebra".
      assertEquals(List.of(), valuesOf(rtx, rangeHits(indexController, rtx, casDef, TITLE_PATH, "d", "e", true, true)),
          "[d,e]");
      // A window whose ends cross is empty, not inverted.
      assertEquals(List.of(),
          valuesOf(rtx, rangeHits(indexController, rtx, casDef, TITLE_PATH, "zebra", "apple", true, true)),
          "[zebra,apple]");

      // One-sided ranges, both directions and both inclusivities.
      assertEquals(List.of("car", "carpet", "zebra"),
          valuesOf(rtx, rangeHits(indexController, rtx, casDef, TITLE_PATH, "car", null, true, true)), "[car,+inf)");
      assertEquals(List.of("carpet", "zebra"),
          valuesOf(rtx, rangeHits(indexController, rtx, casDef, TITLE_PATH, "car", null, false, true)), "(car,+inf)");
      assertEquals(List.of("apple", "car"),
          valuesOf(rtx, rangeHits(indexController, rtx, casDef, TITLE_PATH, null, "car", true, true)), "(-inf,car]");
      assertEquals(List.of("apple"),
          valuesOf(rtx, rangeHits(indexController, rtx, casDef, TITLE_PATH, null, "car", true, false)), "(-inf,car)");
    });
  }

  /**
   * A one-sided range on ONE path of a multi-path CAS index must not return the other path's values
   * through its unbounded end. {@code title} sits below {@code alias} in the key order, so the
   * {@code title} scan leaks upward and the {@code alias} scan leaks downward — one assertion per
   * direction.
   */
  @Test
  void oneSidedRangesOnAMultiPathIndexStayOnTheRequestedPath() {
    final List<String> titles = List.of("apple", "car", "zebra");
    final List<String> aliases = List.of("bravo", "delta", "yankee");

    withCASIndex(TWO_PATH_JSON, Set.of(TITLE_PATH, ALIAS_PATH), (indexController, rtx, casDef) -> {
      // The two decisive cases: `title` holds the LOWER pathNodeKey, so its `>= a` scan is the one
      // whose open end runs up into `alias`; `alias` holds the higher one, so its `<= zz` scan is
      // the one that starts below `title`. Every value in the document is inside both bounds, so a
      // leak returns all six instead of the requested path's three.
      assertEquals(titles, valuesOf(rtx, rangeHits(indexController, rtx, casDef, TITLE_PATH, "a", null, true, true)),
          "title >= a");
      assertEquals(aliases, valuesOf(rtx, rangeHits(indexController, rtx, casDef, ALIAS_PATH, null, "zz", true, true)),
          "alias <= zz");
      // The mirror directions cannot leak (each scan's open end points away from the other path);
      // kept as controls that the bound itself still admits everything it should.
      assertEquals(aliases, valuesOf(rtx, rangeHits(indexController, rtx, casDef, ALIAS_PATH, "a", null, true, true)),
          "alias >= a");
      assertEquals(titles, valuesOf(rtx, rangeHits(indexController, rtx, casDef, TITLE_PATH, null, "zz", true, true)),
          "title <= zz");

      // Two-sided over one path: both bounds pin the PCR, so this was always correct — pinned so a
      // future "optimization" cannot quietly widen it.
      assertEquals(titles, valuesOf(rtx, rangeHits(indexController, rtx, casDef, TITLE_PATH, "a", "zz", true, true)),
          "title in [a,zz]");
      assertEquals(List.of("bravo", "delta"),
          valuesOf(rtx, rangeHits(indexController, rtx, casDef, ALIAS_PATH, "bravo", "delta", true, true)),
          "alias in [bravo,delta]");

      // Both paths at once: the requested PCR set has two members, so the fast path is not taken at
      // all and the full scan's filter decides.
      assertEquals(List.of("apple", "bravo", "car", "delta", "yankee", "zebra"),
          valuesOf(rtx, rangeHits(indexController, rtx, casDef, Set.of(TITLE_PATH, ALIAS_PATH), "a", "zz", true, true)),
          "both paths in [a,zz]");

      // The same one-sided bounds through a CASFilter (SearchMode). NOTE these do NOT reach the
      // bounded-cursor path — openHOTIndexWithFilter gates it on the index having at most one
      // available PCR, which a two-path index does not — so they pin the full-scan filter branch
      // instead. The bounded CASFilter path is covered on the single-path index above.
      assertEquals(titles,
          valuesOf(rtx, filterHits(indexController, rtx, casDef, TITLE_PATH, "a", SearchMode.GREATER_OR_EQUAL)),
          "title GREATER_OR_EQUAL(a)");
      assertEquals(aliases,
          valuesOf(rtx, filterHits(indexController, rtx, casDef, ALIAS_PATH, "zz", SearchMode.LOWER_OR_EQUAL)),
          "alias LOWER_OR_EQUAL(zz)");
    });
  }

  /**
   * The instant family is ordered CHRONOLOGICALLY, not lexically.
   *
   * <p>
   * {@code CASKeySerializer} canonicalizes {@code xs:dateTime} / {@code xs:date} / {@code xs:time} to
   * UTC and writes fixed-width components, so the bounded byte cursor — which decides ranges by
   * unsigned byte order — agrees with the type's own order. Storing the raw lexical form instead put
   * {@code 12:00:00.500001Z} BELOW {@code 12:00:00Z} ({@code '.'} &lt; {@code 'Z'}) and every
   * explicit offset below both, so a one-sided bound silently dropped matching records.
   * {@code jn:valid-at} issues exactly two one-sided DATI ranges, and its candidate re-verification
   * cannot recover a record that never became a candidate.
   */
  @Test
  void instantRangesAreChronologicalNotLexical() {
    // 12:00:00.500001Z is chronologically ABOVE the probe 12:00:00.500Z; 12:00:00Z is below it; and
    // 14:00:00+02:00 is the same instant as 12:00:00Z spelled with an offset.
    final String json = "[{\"t\":\"2020-06-15T12:00:00Z\"},{\"t\":\"2020-06-15T12:00:00.500001Z\"},"
        + "{\"t\":\"2020-06-15T14:00:00+02:00\"}]";

    withCASIndex(json, Set.of(T_PATH), Type.DATI, (indexController, rtx, casDef) -> {
      final Atomic probe = new DateTime("2020-06-15T12:00:00.500Z");

      // Only the later instant is >= the probe. A lexical bound returns the "+02:00" spelling instead
      // and drops this one.
      assertEquals(List.of("2020-06-15T12:00:00.500001Z"),
          valuesOf(rtx, dateRangeHits(indexController, rtx, casDef, probe, null)), "dateTime >= probe");

      // The two spellings of 12:00:00Z are both below the probe, and both must come back.
      assertEquals(List.of("2020-06-15T12:00:00Z", "2020-06-15T14:00:00+02:00"),
          valuesOf(rtx, dateRangeHits(indexController, rtx, casDef, null, probe)), "dateTime <= probe");

      // The same instant written two ways must fall on the same side of an exact bound.
      final Atomic noon = new DateTime("2020-06-15T12:00:00Z");
      assertEquals(List.of("2020-06-15T12:00:00Z", "2020-06-15T14:00:00+02:00"),
          valuesOf(rtx, dateRangeHits(indexController, rtx, casDef, noon, noon)), "dateTime == noon");
    });
  }

  private static final String T_PATH = "/[]/t";

  private static Iterator<NodeReferences> dateRangeHits(final IndexController<?, ?> indexController,
      final JsonNodeReadOnlyTrx rtx, final IndexDef casDef, final @Nullable Atomic min, final @Nullable Atomic max) {
    return indexController.openCASIndex(rtx.getStorageEngineReader(), casDef,
        indexController.createCASFilterRange(Set.of(T_PATH), min, max, true, true, new JsonPCRCollector(rtx)));
  }

  /** What a test body gets once the index is built and a read-only trx is open. */
  @FunctionalInterface
  private interface IndexAssertions {
    void check(IndexController<?, ?> indexController, JsonNodeReadOnlyTrx rtx, IndexDef casDef);
  }

  /**
   * Shred {@code json}, build one CAS index over {@code paths}, commit, then run {@code assertions}
   * against a fresh read-only transaction.
   */
  private static void withCASIndex(final String json, final Set<String> paths, final IndexAssertions assertions) {
    withCASIndex(json, paths, Type.STR, assertions);
  }

  private static void withCASIndex(final String json, final Set<String> paths, final Type contentType,
      final IndexAssertions assertions) {
    final var dbPath = JsonTestHelper.PATHS.PATH1.getFile();
    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

    try (final var database = Databases.openJsonDatabase(dbPath)) {
      database.createResource(ResourceConfiguration.newBuilder(JsonTestHelper.RESOURCE).build());
      final IndexDef casDef;
      try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
          final JsonNodeTrx trx = session.beginNodeTrx()) {
        final var indexController = session.getWtxIndexController(trx.getRevisionNumber());
        casDef = IndexDefs.createCASIdxDef(false, contentType, parsePaths(paths), 0, IndexDef.DbType.JSON);
        indexController.createIndexes(Set.of(casDef), trx);
        new JsonShredder.Builder(trx, JsonShredder.createStringReader(json), InsertPosition.AS_FIRST_CHILD).build()
                                                                                                           .call();
        trx.commit();
      }

      try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
          final var rtx = session.beginNodeReadOnlyTrx()) {
        assertions.check(session.getRtxIndexController(rtx.getRevisionNumber()), rtx, casDef);
      }
    }
  }

  /**
   * Parse the index definition's paths. Deliberately no ordering ceremony: the callers pass
   * {@code Set.of(...)}, whose iteration order the JDK randomizes per JVM, so declaration order
   * cannot be pinned here. The PCR order the multi-path test leans on comes from the SHREDDED
   * DOCUMENT — {@code title} appears before {@code alias} in {@link #TWO_PATH_JSON}, so the path
   * summary gives it the lower pathNodeKey.
   */
  private static Set<Path<QNm>> parsePaths(final Set<String> paths) {
    final Set<Path<QNm>> parsed = new HashSet<>(paths.size());
    for (final String path : paths) {
      parsed.add(Path.parse(path, PathParser.Type.JSON));
    }
    return parsed;
  }

  private static Iterator<NodeReferences> rangeHits(final IndexController<?, ?> indexController,
      final JsonNodeReadOnlyTrx rtx, final IndexDef casDef, final String path, final @Nullable String min,
      final @Nullable String max, final boolean incMin, final boolean incMax) {
    return rangeHits(indexController, rtx, casDef, Set.of(path), min, max, incMin, incMax);
  }

  private static Iterator<NodeReferences> rangeHits(final IndexController<?, ?> indexController,
      final JsonNodeReadOnlyTrx rtx, final IndexDef casDef, final Set<String> paths, final @Nullable String min,
      final @Nullable String max, final boolean incMin, final boolean incMax) {
    return indexController.openCASIndex(rtx.getStorageEngineReader(), casDef,
        indexController.createCASFilterRange(paths, min == null
            ? null
            : new Str(min),
            max == null
                ? null
                : new Str(max),
            incMin, incMax, new JsonPCRCollector(rtx)));
  }

  private static Iterator<NodeReferences> filterHits(final IndexController<?, ?> indexController,
      final JsonNodeReadOnlyTrx rtx, final IndexDef casDef, final String path, final String key,
      final SearchMode mode) {
    return indexController.openCASIndex(rtx.getStorageEngineReader(), casDef,
        indexController.createCASFilter(Set.of(path), new Str(key), mode, new JsonPCRCollector(rtx)));
  }

  /**
   * Resolve every posting the scan returned to the indexed value, sorted.
   *
   * <p>
   * Counting postings is not a strong enough oracle for THIS fixture: it is built around the
   * {@code car}/{@code carpet} prefix collision, so a scan that returned {@code carpet} where
   * {@code apple} belongs has the right cardinality and the wrong answer. A sorted list also keeps an
   * accidental duplicate visible, which a set would swallow.
   */
  private static List<String> valuesOf(final JsonNodeReadOnlyTrx rtx, final Iterator<NodeReferences> hits) {
    final List<String> values = new ArrayList<>();
    while (hits.hasNext()) {
      hits.next().forEachNodeKey(nodeKey -> {
        if (!rtx.moveTo(nodeKey)) {
          throw new IllegalStateException("index posting " + nodeKey + " does not resolve to a node");
        }
        values.add(rtx.getValue());
      });
    }
    Collections.sort(values);
    return values;
  }
}
