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
import io.sirix.access.trx.node.json.JsonIndexController;
import io.sirix.access.trx.node.json.objectvalue.StringValue;
import io.sirix.api.Axis;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.axis.DescendantAxis;
import io.sirix.index.path.json.JsonPCRCollector;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.node.NodeKind;
import io.sirix.service.InsertPosition;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Differential test: every CAS query shape must give the same answer through the canonical HOT
 * index as an independent in-memory or document-scan oracle.
 *
 * <h2>Why this exists</h2>
 * <p>
 * Three consecutive review rounds each found a wrong-results bug in the HOT CAS read path that the
 * whole ~11,000-test suite passed straight through, because every existing test asserts a
 * hand-written count or set for a handful of hand-picked queries. The failures were not exotic;
 * they were <em>shapes</em> nobody had enumerated:
 * <ul>
 * <li>an upper bound whose value is a strict PREFIX of a stored value — index keys are raw UTF-8
 * with no terminator, so the composite ceiling {@code serialize(max) ‖ 0xFFFFFFFF} admits every
 * byte-extension of {@code max} ({@code "carpet"} sits under the ceiling built for
 * {@code "car"});</li>
 * <li>a ONE-SIDED bound on a multi-path index — CAS keys are PCR-major, so the open end runs
 * straight out of the queried path's key range into a neighbouring path's;</li>
 * <li>an EXCLUDED boundary value that is neither the first nor the last group emitted;</li>
 * <li>an EMPTY string bound, whose value region is zero bytes;</li>
 * <li>a content type whose key bytes are its raw lexical form, where byte order is not value
 * order.</li>
 * </ul>
 *
 * <p>
 * So this test does not hand-write expectations. It enumerates the cross product of {bound shape} ×
 * {inclusivity} × {search mode} × {path set} over a fixture built out of exactly those shapes, and
 * checks each answer against an oracle that never reads index state. That separation catches wrong
 * results in optimized index cursors without coupling the oracle to the persisted representation.
 */
final class CASIndexDifferentialTest {

  /**
   * One indexed value in the fixture, as the oracle sees it: which path it hangs under and what
   * string it holds. Kept as data so the brute-force oracle and the JSON come from one source.
   */
  private record Entry(String field, String value) {
  }

  /**
   * The fixture. Every row is here for a reason:
   * <ul>
   * <li>{@code car} / {@code carpet} / {@code carpeting} — a prefix chain: each is a strict prefix of
   * the next, which is the shape a byte-window bound gets wrong.</li>
   * <li>{@code car} twice — a posting list with more than one node, so a group that must be excluded
   * cannot be excluded by dropping a single node key.</li>
   * <li>{@code ""} — an empty value region (a bound built from it used to throw).</li>
   * <li>{@code a}, {@code zebra} — ordinary endpoints, so exclusive bounds have something on each
   * side.</li>
   * <li>{@code alias} rows interleaving the {@code title} range — a second PCR whose values sort
   * INSIDE the first path's value range, so a PCR leak cannot hide behind a disjoint value
   * range.</li>
   * </ul>
   */
  private static final List<Entry> FIXTURE = List.of(new Entry("title", ""), new Entry("title", "a"),
      new Entry("title", "car"), new Entry("title", "car"), new Entry("title", "carpet"),
      new Entry("title", "carpeting"), new Entry("title", "zebra"), new Entry("alias", ""), new Entry("alias", "a"),
      new Entry("alias", "car"), new Entry("alias", "carpet"), new Entry("alias", "zebra"));

  private static final String TITLE_PATH = "/[]/title";
  private static final String ALIAS_PATH = "/[]/alias";
  private static final String V_PATH = "/[]/v";
  private static final String MUTATION_PATH = "/records/[]/v";

  /** Probes chosen to land ON a stored value, BETWEEN two, and OUTSIDE the range on each end. */
  private static final List<String> PROBES =
      List.of("", "a", "b", "car", "carpe", "carpet", "carpeting", "carpetings", "d", "zebra", "zz");

  /**
   * Comparisons actually performed, asserted at the end. A matrix test whose loops silently collapse
   * (a {@code continue} that always fires, an empty probe list) passes just as green as one that ran
   * — so the count is part of the contract.
   */
  private static int comparisons;

  private static final String RESOURCE = "resource";

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  @Test
  void everyBoundShapeAgreesWithBruteForce() {
    comparisons = 0;
    final var dbPath = JsonTestHelper.PATHS.PATH1.getFile();
    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

    try (final var database = Databases.openJsonDatabase(dbPath)) {
      // A single-path index cannot exhibit a cross-PCR leak, and a multi-path index cannot exhibit
      // the single-PCR fast path — the bugs lived in each. Use a fresh resource per path set because
      // building the index shreds the document; reusing one would quietly double every posting.
      final List<Set<String>> pathSets = List.of(Set.of(TITLE_PATH), Set.of(TITLE_PATH, ALIAS_PATH));
      for (int i = 0; i < pathSets.size(); i++) {
        final Set<String> indexedPaths = pathSets.get(i);
        final String resource = RESOURCE + i;
        database.createResource(ResourceConfiguration.newBuilder(resource).build());
        final IndexDef def = buildIndex(database, resource, indexedPaths);

        try (final var session = database.beginResourceSession(resource);
            final var rtx = session.beginNodeReadOnlyTrx()) {

          final var controller = session.getRtxIndexController(rtx.getRevisionNumber());

          // Query one path at a time AND both at once: a filter narrower than the index is exactly
          // where the PCR check has to do work.
          for (final Set<String> queried : List.of(Set.of(TITLE_PATH), Set.of(ALIAS_PATH),
              Set.of(TITLE_PATH, ALIAS_PATH))) {
            if (!indexedPaths.containsAll(queried)) {
              continue;
            }
            checkRanges(indexedPaths, queried, controller, rtx, def);
            checkSearchModes(indexedPaths, queried, controller, rtx, def);
          }
        }
      }
    }

    // 4 (index shape, queried path set) combinations, each running 11x11 probe pairs x 4
    // inclusivities + 4 one-sided shapes per probe + 11 probes x 5 search modes = 583 comparisons,
    // i.e. 2332 in total. Asserted as a floor so a loop that silently stops iterating fails loudly
    // instead of passing green.
    assertTrue(comparisons >= 2_332, "the query matrix collapsed — only " + comparisons + " comparisons ran");
    System.out.println("CASIndexDifferentialTest: " + comparisons + " HOT-vs-bruteforce comparisons");
  }

  /**
   * The CONTENT-TYPE axis, which is what review round three broke — now asserted exactly.
   *
   * <p>
   * The HOT bounded cursor decides a range by unsigned byte order over the serialized key, so a type
   * is only safe on that path if its encoding puts byte order and value order in agreement. The
   * instant family used to fall through to the raw-lexical string branch, where it emphatically does
   * not: {@code 12:00:00.500001Z} sorts below {@code 12:00:00Z} because {@code '.'} &lt; {@code 'Z'},
   * and every explicit UTC offset sorts below both. {@code CASKeySerializer} now canonicalizes to UTC
   * and writes fixed-width components instead, so the answer must be exactly the chronological one.
   *
   * <p>
   * The fixture is written so text order and chronological order disagree in both directions —
   * fractional seconds, positive and negative offsets, and the same instant spelled three ways — so
   * any regression to lexical ordering shows up as a wrong answer rather than a lucky pass.
   */
  @Test
  void rangesOnTheInstantFamilyAreChronological() {
    final List<String> instants = List.of("2020-06-15T12:00:00Z", "2020-06-15T12:00:00.500001Z",
        "2020-06-15T12:00:00.5Z", "2020-06-15T14:00:00+02:00", "2020-06-15T09:00:00-03:00", "2021-01-01T00:00:00Z");
    final List<String> probes = List.of("2020-06-15T12:00:00Z", "2020-06-15T12:00:00.500Z", "2020-06-15T13:00:00Z",
        "2020-06-15T12:00:00+00:00", "2019-01-01T00:00:00Z", "2022-01-01T00:00:00Z");

    final StringBuilder json = new StringBuilder("[");
    for (int i = 0; i < instants.size(); i++) {
      json.append(i > 0
          ? ","
          : "").append("{\"v\":\"").append(instants.get(i)).append("\"}");
    }
    json.append(']');

    final var dbPath = JsonTestHelper.PATHS.PATH2.getFile();
    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));
    try (final var database = Databases.openJsonDatabase(dbPath)) {
      database.createResource(ResourceConfiguration.newBuilder("datetime").build());
      final IndexDef def = buildTypedIndex(database, "datetime", json.toString());

      try (final var session = database.beginResourceSession("datetime");
          final var rtx = session.beginNodeReadOnlyTrx()) {
        final var controller = session.getRtxIndexController(rtx.getRevisionNumber());
        final Set<String> paths = Set.of(V_PATH);

        int checked = 0;
        for (final String probe : probes) {
          for (final boolean incMin : new boolean[] {true, false}) {
            for (final boolean incMax : new boolean[] {true, false}) {
              // The two one-sided shapes jn:valid-at issues...
              checked += assertChronological(controller, rtx, def, paths, probe, null, incMin, incMax, instants);
              checked += assertChronological(controller, rtx, def, paths, null, probe, incMin, incMax, instants);
              // ...and every two-sided window over the probe set.
              for (final String other : probes) {
                checked += assertChronological(controller, rtx, def, paths, probe, other, incMin, incMax, instants);
              }
            }
          }
        }
        assertTrue(checked >= 192, "the instant matrix collapsed — only " + checked + " comparisons ran");
        System.out.println("CASIndexDifferentialTest: " + checked + " chronological instant comparisons");
      }
    }
  }

  /**
   * The scan must return exactly the instants chronologically inside the bounds — no more, no less.
   */
  private static int assertChronological(final IndexController<?, ?> controller, final JsonNodeReadOnlyTrx rtx,
      final IndexDef def, final Set<String> paths, final String min, final String max, final boolean incMin,
      final boolean incMax, final List<String> instants) {
    final Iterator<NodeReferences> hits = instantHits(controller, rtx, def, paths, min, max, incMin, incMax);

    final List<String> expected = new ArrayList<>();
    for (final String instant : instants) {
      final Instant value = OffsetDateTime.parse(instant).toInstant();
      if (min != null) {
        final int cmp = value.compareTo(OffsetDateTime.parse(min).toInstant());
        if (cmp < 0 || (cmp == 0 && !incMin)) {
          continue;
        }
      }
      if (max != null) {
        final int cmp = value.compareTo(OffsetDateTime.parse(max).toInstant());
        if (cmp > 0 || (cmp == 0 && !incMax)) {
          continue;
        }
      }
      expected.add("v=\"" + instant + "\"");
    }
    Collections.sort(expected);

    final String what = "instant range " + (incMin
        ? "["
        : "(") + q(min) + "," + q(max)
        + (incMax
            ? "]"
            : ")");
    assertEquals(expected, values(rtx, hits), what);
    return 1;
  }

  private static Iterator<NodeReferences> instantHits(final IndexController<?, ?> controller,
      final JsonNodeReadOnlyTrx rtx, final IndexDef def, final Set<String> paths, final String min, final String max,
      final boolean incMin, final boolean incMax) {
    return controller.openCASIndex(rtx.getStorageEngineReader(), def, controller.createCASFilterRange(paths, min == null
        ? null
        : new DateTime(min),
        max == null
            ? null
            : new DateTime(max),
        incMin, incMax, new JsonPCRCollector(rtx)));
  }

  /**
   * Exercises the one persisted CAS implementation through incremental insert, value replacement and
   * delete maintenance, then reopens every historical revision. The expected values come from a
   * descendant scan of that revision, not from any index structure; explicit state assertions keep a
   * coincident document/index mutation bug from becoming a false positive.
   */
  @ParameterizedTest(name = "incremental CAS maintenance with {0}")
  @EnumSource(VersioningType.class)
  void incrementalMutationsMatchDocumentScanAcrossVersioningTypes(final VersioningType versioningType) {
    final var dbPath = JsonTestHelper.PATHS.PATH1.getFile();
    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                                   .versioningApproach(versioningType)
                                                   .maxNumberOfRevisionsToRestore(4)
                                                   .build());

      final IndexDef def;
      try (final JsonResourceSession session = database.beginResourceSession(RESOURCE);
          final JsonNodeTrx wtx = session.beginNodeTrx()) {
        final JsonIndexController controller = session.getWtxIndexController(wtx.getRevisionNumber());
        def = IndexDefs.createCASIdxDef(false, Type.STR, parsePaths(Set.of(MUTATION_PATH)), 0, IndexDef.DbType.JSON);
        controller.createIndexes(Set.of(def), wtx);
        new JsonShredder.Builder(wtx,
            JsonShredder.createStringReader("{\"records\":[{\"v\":\"alpha\"},{\"v\":\"beta\"}]}"),
            InsertPosition.AS_FIRST_CHILD).build().call();
        wtx.commit();
      }

      // Revision 2 replaces one posting and appends one new posting. Both operations must update
      // only their affected HOT keys while revision 1 remains readable.
      try (final JsonResourceSession session = database.beginResourceSession(RESOURCE);
          final JsonNodeTrx wtx = session.beginNodeTrx()) {
        assertTrue(wtx.moveTo(namedStringNodeKey(wtx, "v", "beta")));
        wtx.setStringValue("delta");

        assertTrue(wtx.moveTo(namedNodeKey(wtx, "records", NodeKind.OBJECT_NAMED_ARRAY)));
        wtx.insertObjectAsLastChild().insertObjectRecordAsFirstChild("v", new StringValue("charlie"));
        wtx.commit();
      }

      // Revision 3 removes exactly one posting.
      try (final JsonResourceSession session = database.beginResourceSession(RESOURCE);
          final JsonNodeTrx wtx = session.beginNodeTrx()) {
        assertTrue(wtx.moveTo(namedStringNodeKey(wtx, "v", "alpha")));
        wtx.remove();
        wtx.commit();
      }

      try (final JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
        assertRevisionMatchesDocumentScan(session, def, 1, List.of("alpha", "beta"));
        assertRevisionMatchesDocumentScan(session, def, 2, List.of("alpha", "charlie", "delta"));
        assertRevisionMatchesDocumentScan(session, def, 3, List.of("charlie", "delta"));
      }
    }
  }

  private static IndexDef buildTypedIndex(final Database<JsonResourceSession> database, final String resource,
      final String json) {
    try (final var session = database.beginResourceSession(resource); final JsonNodeTrx trx = session.beginNodeTrx()) {
      final var controller = session.getWtxIndexController(trx.getRevisionNumber());
      final IndexDef def =
          IndexDefs.createCASIdxDef(false, Type.DATI, parsePaths(Set.of(V_PATH)), 0, IndexDef.DbType.JSON);
      controller.createIndexes(Set.of(def), trx);
      new JsonShredder.Builder(trx, JsonShredder.createStringReader(json), InsertPosition.AS_FIRST_CHILD).build()
                                                                                                         .call();
      trx.commit();
      return def;
    }
  }

  /** Two-sided, min-only and max-only bounds, every inclusivity combination, every probe pair. */
  private static void checkRanges(final Set<String> indexedPaths, final Set<String> queried,
      final IndexController<?, ?> controller, final JsonNodeReadOnlyTrx rtx, final IndexDef def) {
    for (final String min : PROBES) {
      for (final String max : PROBES) {
        for (final boolean incMin : new boolean[] {true, false}) {
          for (final boolean incMax : new boolean[] {true, false}) {
            // Two-sided, plus each one-sided shape built from the same probe.
            compare(indexedPaths, queried, min, max, incMin, incMax, controller, rtx, def);
          }
        }
      }
      compare(indexedPaths, queried, min, null, true, true, controller, rtx, def);
      compare(indexedPaths, queried, min, null, false, true, controller, rtx, def);
      compare(indexedPaths, queried, null, min, true, true, controller, rtx, def);
      compare(indexedPaths, queried, null, min, true, false, controller, rtx, def);
    }
  }

  private static void compare(final Set<String> indexedPaths, final Set<String> queried, final String min,
      final String max, final boolean incMin, final boolean incMax, final IndexController<?, ?> controller,
      final JsonNodeReadOnlyTrx rtx, final IndexDef def) {
    final String what = "index=" + indexedPaths + " query=" + queried + " range=" + (incMin
        ? "["
        : "(") + q(min) + "," + q(max)
        + (incMax
            ? "]"
            : ")");

    comparisons++;
    final List<String> actual = values(rtx, rangeHits(controller, rtx, def, queried, min, max, incMin, incMax));
    final List<String> expected = bruteForceRange(queried, min, max, incMin, incMax);

    assertEquals(expected, actual, "HOT disagrees with brute force — " + what);
  }

  /** All five {@link SearchMode}s against every probe. */
  private static void checkSearchModes(final Set<String> indexedPaths, final Set<String> queried,
      final IndexController<?, ?> controller, final JsonNodeReadOnlyTrx rtx, final IndexDef def) {
    for (final String probe : PROBES) {
      for (final SearchMode mode : SearchMode.values()) {
        final String what = "index=" + indexedPaths + " query=" + queried + " " + mode + "(" + q(probe) + ")";

        comparisons++;
        final List<String> actual = values(rtx, filterHits(controller, rtx, def, queried, probe, mode));
        final List<String> expected = bruteForceMode(queried, probe, mode);

        assertEquals(expected, actual, "HOT disagrees with brute force — " + what);
      }
    }
  }

  // ==================== oracles ====================

  /**
   * The answer computed straight off {@link #FIXTURE}, with no index involved. Plain
   * {@link String#compareTo} is the right comparator here because every fixture value and probe is
   * ASCII, where UTF-16 order, UTF-8 byte order and {@code Str#compareTo} all coincide.
   */
  private static List<String> bruteForceRange(final Set<String> queried, final String min, final String max,
      final boolean incMin, final boolean incMax) {
    final List<String> out = new ArrayList<>();
    for (final Entry e : FIXTURE) {
      if (!queried.contains("/[]/" + e.field())) {
        continue;
      }
      if (min != null && (incMin
          ? e.value().compareTo(min) < 0
          : e.value().compareTo(min) <= 0)) {
        continue;
      }
      if (max != null && (incMax
          ? e.value().compareTo(max) > 0
          : e.value().compareTo(max) >= 0)) {
        continue;
      }
      out.add(label(e));
    }
    Collections.sort(out);
    return out;
  }

  private static List<String> bruteForceMode(final Set<String> queried, final String probe, final SearchMode mode) {
    final List<String> out = new ArrayList<>();
    for (final Entry e : FIXTURE) {
      if (!queried.contains("/[]/" + e.field())) {
        continue;
      }
      final int cmp = e.value().compareTo(probe);
      final boolean keep = switch (mode) {
        case EQUAL -> cmp == 0;
        case LOWER -> cmp < 0;
        case LOWER_OR_EQUAL -> cmp <= 0;
        case GREATER -> cmp > 0;
        case GREATER_OR_EQUAL -> cmp >= 0;
      };
      if (keep) {
        out.add(label(e));
      }
    }
    Collections.sort(out);
    return out;
  }

  private static void assertRevisionMatchesDocumentScan(final JsonResourceSession session, final IndexDef def,
      final int revision, final List<String> expectedDocumentValues) {
    try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision)) {
      final List<String> scanned = scanNamedStringValues(rtx, "v");
      final List<String> expected = new ArrayList<>(expectedDocumentValues);
      Collections.sort(expected);
      assertEquals(expected, scanned, "document state in revision " + revision);

      final IndexController<?, ?> controller = session.getRtxIndexController(revision);
      final Set<String> path = Set.of(MUTATION_PATH);
      assertRangeMatchesScan(controller, rtx, def, path, scanned, null, null, true, true);
      assertRangeMatchesScan(controller, rtx, def, path, scanned, "b", "delta", true, true);
      assertRangeMatchesScan(controller, rtx, def, path, scanned, "charlie", null, false, true);
      assertRangeMatchesScan(controller, rtx, def, path, scanned, null, "charlie", true, false);

      for (final String probe : List.of("alpha", "beta", "charlie", "delta")) {
        final List<String> expectedEquals = new ArrayList<>();
        for (final String value : scanned) {
          if (value.equals(probe)) {
            expectedEquals.add("v=\"" + value + "\"");
          }
        }
        assertEquals(expectedEquals, values(rtx, filterHits(controller, rtx, def, path, probe, SearchMode.EQUAL)),
            "equality query for " + q(probe) + " in revision " + revision);
      }
    }
  }

  private static void assertRangeMatchesScan(final IndexController<?, ?> controller, final JsonNodeReadOnlyTrx rtx,
      final IndexDef def, final Set<String> path, final List<String> scanned, final String min, final String max,
      final boolean incMin, final boolean incMax) {
    final List<String> expected = new ArrayList<>();
    for (final String value : scanned) {
      if (min != null && (incMin
          ? value.compareTo(min) < 0
          : value.compareTo(min) <= 0)) {
        continue;
      }
      if (max != null && (incMax
          ? value.compareTo(max) > 0
          : value.compareTo(max) >= 0)) {
        continue;
      }
      expected.add("v=\"" + value + "\"");
    }
    assertEquals(expected, values(rtx, rangeHits(controller, rtx, def, path, min, max, incMin, incMax)),
        "range " + (incMin
            ? "["
            : "(") + q(min) + "," + q(max)
            + (incMax
                ? "]"
                : ")")
            + " in revision " + rtx.getRevisionNumber());
  }

  private static List<String> scanNamedStringValues(final JsonNodeReadOnlyTrx rtx, final String fieldName) {
    final long restoreNodeKey = rtx.getNodeKey();
    final List<String> values = new ArrayList<>();
    try {
      rtx.moveToDocumentRoot();
      final Axis descendants = new DescendantAxis(rtx);
      while (descendants.hasNext()) {
        descendants.nextLong();
        if ((rtx.getKind() == NodeKind.OBJECT_NAMED_STRING || rtx.getKind() == NodeKind.STRING_VALUE)
            && fieldName.equals(currentFieldName(rtx))) {
          values.add(rtx.getValue());
        }
      }
      Collections.sort(values);
      return values;
    } finally {
      rtx.moveTo(restoreNodeKey);
    }
  }

  private static long namedStringNodeKey(final JsonNodeReadOnlyTrx rtx, final String fieldName, final String value) {
    final long restoreNodeKey = rtx.getNodeKey();
    try {
      rtx.moveToDocumentRoot();
      final Axis descendants = new DescendantAxis(rtx);
      while (descendants.hasNext()) {
        descendants.nextLong();
        if ((rtx.getKind() == NodeKind.OBJECT_NAMED_STRING || rtx.getKind() == NodeKind.STRING_VALUE)
            && fieldName.equals(currentFieldName(rtx)) && value.equals(rtx.getValue())) {
          return rtx.getNodeKey();
        }
      }
      throw new AssertionError("missing " + fieldName + "=" + q(value));
    } finally {
      rtx.moveTo(restoreNodeKey);
    }
  }

  private static long namedNodeKey(final JsonNodeReadOnlyTrx rtx, final String fieldName, final NodeKind kind) {
    final long restoreNodeKey = rtx.getNodeKey();
    try {
      rtx.moveToDocumentRoot();
      final Axis descendants = new DescendantAxis(rtx);
      while (descendants.hasNext()) {
        descendants.nextLong();
        if (rtx.getKind() == kind && rtx.getName() != null && fieldName.equals(rtx.getName().getLocalName())) {
          return rtx.getNodeKey();
        }
      }
      throw new AssertionError("missing " + kind + " named " + fieldName);
    } finally {
      rtx.moveTo(restoreNodeKey);
    }
  }

  private static String currentFieldName(final JsonNodeReadOnlyTrx rtx) {
    final QNm ownName = rtx.getName();
    if (ownName != null) {
      return ownName.getLocalName();
    }

    final long valueNodeKey = rtx.getNodeKey();
    try {
      return rtx.moveToParent() && rtx.getName() != null
          ? rtx.getName().getLocalName()
          : null;
    } finally {
      rtx.moveTo(valueNodeKey);
    }
  }

  /** The oracle's rendering of a fixture row, matching what {@link #values} produces. */
  private static String label(final Entry e) {
    return e.field() + "=\"" + e.value() + "\"";
  }

  // ==================== fixture + plumbing ====================

  private static IndexDef buildIndex(final Database<JsonResourceSession> database, final String resource,
      final Set<String> paths) {
    try (final var session = database.beginResourceSession(resource); final JsonNodeTrx trx = session.beginNodeTrx()) {
      final var controller = session.getWtxIndexController(trx.getRevisionNumber());
      final IndexDef def = IndexDefs.createCASIdxDef(false, Type.STR, parsePaths(paths), 0, IndexDef.DbType.JSON);
      controller.createIndexes(Set.of(def), trx);
      new JsonShredder.Builder(trx, JsonShredder.createStringReader(json()), InsertPosition.AS_FIRST_CHILD).build()
                                                                                                           .call();
      trx.commit();
      return def;
    }
  }

  /** The fixture as a JSON array — one object per row, so each value is its own indexed node. */
  private static String json() {
    final StringBuilder sb = new StringBuilder("[");
    for (int i = 0; i < FIXTURE.size(); i++) {
      final Entry e = FIXTURE.get(i);
      if (i > 0) {
        sb.append(',');
      }
      sb.append("{\"").append(e.field()).append("\":\"").append(e.value()).append("\"}");
    }
    return sb.append(']').toString();
  }

  private static Set<Path<QNm>> parsePaths(final Set<String> paths) {
    final Set<Path<QNm>> parsed = new HashSet<>(paths.size());
    for (final String path : paths) {
      parsed.add(Path.parse(path, PathParser.Type.JSON));
    }
    return parsed;
  }

  private static Iterator<NodeReferences> rangeHits(final IndexController<?, ?> controller,
      final JsonNodeReadOnlyTrx rtx, final IndexDef def, final Set<String> paths, final String min, final String max,
      final boolean incMin, final boolean incMax) {
    return controller.openCASIndex(rtx.getStorageEngineReader(), def,
        controller.createCASFilterRange(paths, atom(min), atom(max), incMin, incMax, new JsonPCRCollector(rtx)));
  }

  private static Iterator<NodeReferences> filterHits(final IndexController<?, ?> controller,
      final JsonNodeReadOnlyTrx rtx, final IndexDef def, final Set<String> paths, final String key,
      final SearchMode mode) {
    return controller.openCASIndex(rtx.getStorageEngineReader(), def,
        controller.createCASFilter(paths, new Str(key), mode, new JsonPCRCollector(rtx)));
  }

  private static Atomic atom(final String value) {
    return value == null
        ? null
        : new Str(value);
  }

  /**
   * Resolve every posting to {@code field="value"}, sorted. Resolving beats counting on both axes:
   * the fixture is built around prefix collisions, so a scan returning {@code carpet} where
   * {@code car} belongs has the right cardinality and the wrong answer; and it carries the FIELD, so
   * a posting that leaked in from a neighbouring path is visible even when the two paths hold equal
   * values (both hold {@code ""} and {@code "car"}, deliberately).
   */
  private static List<String> values(final JsonNodeReadOnlyTrx rtx, final Iterator<NodeReferences> hits) {
    final List<String> out = new ArrayList<>();
    while (hits.hasNext()) {
      hits.next().forEachNodeKey(nodeKey -> {
        if (!rtx.moveTo(nodeKey)) {
          throw new IllegalStateException("index posting " + nodeKey + " does not resolve to a node");
        }
        final String value = rtx.getValue();
        // The indexed node is either a plain STRING_VALUE under an OBJECT_KEY, or one of the fused
        // OBJECT_NAMED_* kinds that carries name and value together — so look on the node first and
        // only walk up when it has no name of its own.
        QNm name = rtx.getName();
        if (name == null && rtx.moveToParent()) {
          name = rtx.getName();
        }
        out.add((name == null
            ? "?"
            : name.getLocalName()) + "=\"" + value + "\"");
      });
    }
    Collections.sort(out);
    return out;
  }

  private static String q(final String value) {
    return value == null
        ? "-inf"
        : "\"" + value + "\"";
  }
}
