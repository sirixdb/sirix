/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.projection.ProjectionSortedGroupScan.Group;
import io.sirix.index.projection.ProjectionSortedGroupScan.Order;
import io.sirix.io.bytepipe.ByteHandlerPipeline;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A sorted view over an optional aggregate field: a three-field view (one equality prefix field, a
 * string group, an ordered long value) whose value is absent for a few rows.
 *
 * <p>
 * Such a row has no group summary and no provable extremum, so both the summaries route and the
 * full-key route give up on it — each after walking the queried range. The directory header counts
 * those rows view-wide, beside the reserved unencodable rows, and that count lets the summaries
 * walk decline for both routes at once — but only where it has proved the second walk futile. A
 * leaf lying entirely inside the queried range and holding no summary must hold one of the counted
 * rows, and hold it inside the range, so the full-key walk would only reach the same row: the range
 * declines without reading a data leaf.
 * </p>
 *
 * <p>
 * A leaf that meets the range at one of its two ends proves nothing — the row without a value may
 * be one of the leaf's rows outside the range — so such a range keeps both walks, and keeps the
 * answer the full-key walk can still find for it. A range whose leaves all have summaries is served
 * from the summaries alone, untouched. A zero or unknown count changes nothing at all.
 * </p>
 */
final class ProjectionSortedMissingAggregateTest {

  private static final ProjectionSortKeyCodec.Layout LAYOUT =
      new ProjectionSortKeyCodec.Layout(new byte[] {ProjectionSortKeyCodec.FIELD_STRING,
          ProjectionSortKeyCodec.FIELD_STRING, ProjectionSortKeyCodec.FIELD_LONG});

  /** Rows per data leaf, small enough that any walk of a range reads many leaves. */
  private static final int ROWS_PER_LEAF = 2;

  /** The prefix whose rows all carry a value in every fixture. */
  private static final String CLEAN_PREFIX = "k0";
  /** The prefix holding the rows without a value, when the fixture has any. */
  private static final String DIRTY_PREFIX = "k1";
  /**
   * The four ranges of {@link #shapesFixture()}. The leaf carrying {@code k0}'s last row also carries
   * {@code k1}'s first, and that {@code k1} row is the one without a value, so the leaf has no
   * summary and meets {@code k0} at its upper end and {@code k1} at its lower end. The offending row
   * is outside {@code k0}, so {@code k0}'s walk serves it; it is inside {@code k1}, so {@code k1}'s
   * walk declines. {@code k2} holds a row without a value well inside it, and {@code k3} holds none.
   */
  private static final String BOUNDARY_SERVED_PREFIX = "k0";
  private static final String BOUNDARY_DECLINED_PREFIX = "k1";
  private static final String INSIDE_PREFIX = "k2";
  private static final String SHAPES_CLEAN_PREFIX = "k3";

  /** Header field offsets, mirroring {@code ProjectionSortedDirectory.Header}'s wire form. */
  private static final int VERSION_OFFSET = 4;
  private static final byte VERSION_WITHOUT_MISSING_AGGREGATE = 3;
  private static final int FIXED_HEADER_BYTES = 39;
  private static final int FIXED_HEADER_BYTES_WITHOUT_MISSING_AGGREGATE = 31;

  @TempDir
  Path temporaryDirectory;

  /** A fixture row; a null {@code value} leaves the aggregated last key field absent. */
  private record Row(String kind, String group, @Nullable Long value, long record) {
  }

  /**
   * The three shapes a range can take in one view that holds rows without an aggregate value, each
   * checked against the answer the same query gives with the count absent — the behaviour before it
   * existed — and against an independent fold.
   *
   * <ul>
   * <li>{@code INSIDE}: the first leaf of the range without a summary lies entirely inside it, so
   * that leaf holds a row of the range with no value. The summaries walk proves the full-key walk
   * would only reach the same row, and declines without it: no data leaf is read at all.</li>
   * <li>{@code BOUNDARY}: the only leaf of the range without a summary meets it at one end, so the
   * offending row may be outside the range. The full-key walk still runs, exactly as before, and
   * here it serves the range — the very answer a view-wide decline would have thrown away.</li>
   * <li>{@code CLEAN}: every leaf of the range has a summary, so the summaries route serves it
   * untouched, again without reading a data leaf.</li>
   * </ul>
   */
  @Test
  void eachRangeShapeDeclinesOrServesExactlyAsItDidBeforeTheCount() {
    final List<Row> rows = shapesFixture();
    withView(rows, "range-shapes", (session, reader) -> {
      assertRangeShapes(reader, rows);

      for (final Order order : Order.values()) {
        // INSIDE: proven decline, and the full-key walk never runs.
        final Reads inside = measure(reader, () -> assertNull(topK(reader, INSIDE_PREFIX, order, false)));
        assertEquals(0, inside.leaves(), "a proven decline must not start the full-key walk");
        assertTrue(inside.summaries() > 0, "the proof comes from the summaries walk");

        // BOUNDARY, offending row outside the range: the walk still runs and serves it.
        final Reads served = measure(reader,
            () -> assertEquals(expected(rows, BOUNDARY_SERVED_PREFIX, 4, order, false),
                topK(reader, BOUNDARY_SERVED_PREFIX, order, false)));
        assertTrue(served.leaves() > 0, "a boundary leaf proves nothing, so the walk must still run");

        // BOUNDARY, offending row inside the range: the walk still runs and declines, as before.
        final Reads declined =
            measure(reader, () -> assertNull(topK(reader, BOUNDARY_DECLINED_PREFIX, order, false)));
        assertTrue(declined.leaves() > 0, "a boundary leaf proves nothing here either");

        // CLEAN: served from the summaries alone.
        final Reads clean = measure(reader,
            () -> assertEquals(expected(rows, SHAPES_CLEAN_PREFIX, 4, order, false),
                topK(reader, SHAPES_CLEAN_PREFIX, order, false)));
        assertEquals(0, clean.leaves(), "a range whose leaves all have summaries needs no data leaf");
      }

      // Every shape answers what it answered before the count existed, min-only route included.
      final List<@Nullable List<Group>> counted = everyRoute(reader);
      downgradeHeader(session);
      try (JsonNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx()) {
        final StorageEngineReader uncounted = trx.getStorageEngineReader();
        assertEquals(ProjectionSortedDirectory.MISSING_AGGREGATE_ROWS_UNKNOWN,
            missingAggregateRowsOf(uncounted));
        assertEquals(counted, everyRoute(uncounted), "the count may only save reads, never change an answer");
      }
    });
  }

  /** The fixture really does present the three shapes; otherwise the test above proves nothing. */
  private static void assertRangeShapes(final StorageEngineReader reader, final List<Row> rows) {
    final ProjectionSortedDirectory.Accessor directory = ProjectionSortedDirectory.open(reader, 0);
    assertNotNull(directory);
    assertEquals(rowsWithoutAValue(rows), directory.missingAggregateRows());
    assertTrue(directory.holdsRowsWithoutAggregateValues());

    final List<Boolean> inside = summarylessLeavesOf(reader, INSIDE_PREFIX);
    assertFalse(inside.isEmpty(), "the INSIDE range must hold a leaf without a summary");
    assertTrue(inside.get(0), "the first such leaf of the INSIDE range must lie entirely inside it");

    for (final String kind : new String[] {BOUNDARY_SERVED_PREFIX, BOUNDARY_DECLINED_PREFIX}) {
      final List<Boolean> boundary = summarylessLeavesOf(reader, kind);
      assertEquals(1, boundary.size(), kind + " must hold exactly one leaf without a summary");
      assertFalse(boundary.get(0), kind + "'s leaf without a summary must meet it at one end");
    }

    assertEquals(List.of(), summarylessLeavesOf(reader, SHAPES_CLEAN_PREFIX),
        "the CLEAN range must have every summary");
  }

  /**
   * For each leaf of {@code kind}'s range that has no group summary, in key order, whether it lies
   * entirely inside that range — the same fence test the summaries route makes, done independently.
   */
  private static List<Boolean> summarylessLeavesOf(final StorageEngineReader reader, final String kind) {
    final ProjectionSortedDirectory.Accessor directory = ProjectionSortedDirectory.open(reader, 0);
    assertNotNull(directory);
    final byte[] prefix = prefix(kind);
    final ProjectionSortedDirectory.Accessor.LeafCursor cursor =
        directory.leaves(prefix, ProjectionSortKeyCodec.prefixUpperExclusive(prefix));
    final List<Boolean> summaryless = new ArrayList<>();
    while (cursor.id() != 0) {
      final int id = cursor.id();
      final byte[] firstKey = new byte[cursor.firstKeyLength()];
      cursor.copyFirstKeyTo(firstKey);
      final boolean startsInside = ProjectionSortKeyCodec.startsWith(firstKey, firstKey.length, prefix);
      final boolean more = cursor.advance();
      if (ProjectionSortedGroupSummary.read(reader, 0, id) == null) {
        summaryless.add(startsInside && more);
      }
    }
    return summaryless;
  }

  @Test
  void aViewWrittenBeforeTheCountStillAnswersExactly() {
    final List<Row> rows = fixture(false);
    withView(rows, "uncounted-still-exact", (session, reader) -> {
      final List<@Nullable List<Group>> counted = new ArrayList<>();
      for (final String kind : new String[] {CLEAN_PREFIX, DIRTY_PREFIX, "k2"}) {
        counted.addAll(everyRoute(reader, kind));
      }
      downgradeHeader(session);
      try (JsonNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx()) {
        final StorageEngineReader uncounted = trx.getStorageEngineReader();
        final ProjectionSortedDirectory.Accessor directory = ProjectionSortedDirectory.open(uncounted, 0);
        assertNotNull(directory);
        assertEquals(ProjectionSortedDirectory.MISSING_AGGREGATE_ROWS_UNKNOWN, directory.missingAggregateRows());
        final List<@Nullable List<Group>> walked = new ArrayList<>();
        for (final String kind : new String[] {CLEAN_PREFIX, DIRTY_PREFIX, "k2"}) {
          walked.addAll(everyRoute(uncounted, kind));
        }
        assertEquals(counted, walked, "a view with every value present answers the same, counted or not");
        assertEquals(expected(rows, CLEAN_PREFIX, 4, Order.MIN_ASC, false),
            topK(uncounted, CLEAN_PREFIX, Order.MIN_ASC, false));
      }
    });
  }

  @Test
  void everyRouteServesAViewWhoseRowsAllCarryAValue() {
    final List<Row> rows = fixture(false);
    withView(rows, "all-values-present", (session, reader) -> {
      final ProjectionSortedDirectory.Accessor directory = ProjectionSortedDirectory.open(reader, 0);
      assertNotNull(directory);
      assertEquals(0, directory.missingAggregateRows());
      assertFalse(directory.holdsRowsWithoutAggregateValues());

      for (final String prefix : new String[] {CLEAN_PREFIX, DIRTY_PREFIX, "k2"}) {
        final Reads reads = measure(reader, () -> {
          assertEquals(expected(rows, prefix, 4, Order.MIN_ASC, false), topK(reader, prefix, Order.MIN_ASC, false));
          assertEquals(expected(rows, prefix, 4, Order.MAX_DESC, false), topK(reader, prefix, Order.MAX_DESC, false));
          assertEquals(expected(rows, prefix, 4, Order.MIN_ASC, true), topK(reader, prefix, Order.MIN_ASC, true));
        });
        assertEquals(0, reads.leaves(), "every leaf has a summary, so no data leaf is read");
        assertTrue(reads.summaries() > 0, "the summaries route serves the range");
      }
    });
  }

  @Test
  void maintenanceTracksTheCountAcrossInsertUpdateAndDelete() {
    final Path databasePath = temporaryDirectory.resolve("maintenance");
    final List<Row> rows = fixture(false);
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource").build()));
      try (JsonResourceSession session = database.beginResourceSession("resource")) {
        final int built;
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          build(writer, rows);
          built = writer.getRevisionNumber();
          writer.commit();
        }
        assertEquals(0, missingAggregateRowsOf(session));
        assertNotNull(topKOfDirtyRange(session));

        // Insert: one row with no value in the aggregated field, into the dirty prefix's range.
        final byte[] absent = key(new Row(DIRTY_PREFIX, "g3", null, 9_001));
        final int inserted = edit(session, editor -> editor.insert(absent, new byte[0]));
        assertEquals(1, missingAggregateRowsOf(session));
        assertNull(topKOfDirtyRange(session), "no route can prove that range's extrema now");

        // Update: the same record gains a value, as one removal plus one insertion in a single pass.
        final byte[] present = key(new Row(DIRTY_PREFIX, "g3", 4_242L, 9_001));
        edit(session, editor -> editor.apply(new byte[][] {absent}, 1, new byte[][] {present}, null, 1));
        assertEquals(0, missingAggregateRowsOf(session));
        assertNotNull(topKOfDirtyRange(session), "the range is servable again");

        // Update the other way: the value goes away again, and two more rows arrive without one.
        final byte[] first = key(new Row(CLEAN_PREFIX, "g1", null, 9_002));
        final byte[] second = key(new Row("k2", "g4", null, 9_003));
        edit(session, editor -> {
          editor.apply(new byte[][] {present}, 1, new byte[][] {absent}, null, 1);
          editor.insert(first, new byte[0]);
          editor.insert(second, new byte[0]);
        });
        assertEquals(3, missingAggregateRowsOf(session));
        assertNull(topKOfDirtyRange(session));

        // Delete: each removal takes the count back down, and the last one restores service.
        edit(session, editor -> editor.remove(first));
        assertEquals(2, missingAggregateRowsOf(session));
        edit(session, editor -> editor.apply(new byte[][] {absent, second}, 2, new byte[0][], null, 0));
        assertEquals(0, missingAggregateRowsOf(session));
        assertEquals(expected(rows, CLEAN_PREFIX, 4, Order.MIN_ASC, false), topKOfLatest(session));
        assertEquals(expected(rows, DIRTY_PREFIX, 4, Order.MIN_ASC, false), topKOfDirtyRange(session));

        // Every committed revision keeps the count it was published with.
        assertEquals(0, missingAggregateRowsOf(session, built));
        assertEquals(1, missingAggregateRowsOf(session, inserted));
      }
    }
  }

  /**
   * The per-leaf recount is an initial-build cost alone. A maintenance pass keeps the header count
   * from the keys it inserts and removes, so rewriting a leaf must never walk that leaf's rows again
   * — a walk whose result the pass could not use anyway, since it would double count what the edit
   * keys already accounted for.
   */
  @Test
  void onlyTheInitialBuildRecountsALeafsRowsWithoutAValue() {
    final Path databasePath = temporaryDirectory.resolve("no-maintenance-recount");
    final List<Row> rows = fixture(true);
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource").build()));
      try (JsonResourceSession session = database.beginResourceSession("resource")) {
        final int built = recounts(() -> {
          try (JsonNodeTrx writer = session.beginNodeTrx()) {
            build(writer, rows);
            writer.commit();
          }
        });
        assertTrue(built > 0, "the build must count the leaves that get no summary");
        final long held = rowsWithoutAValue(rows);
        assertEquals(held, missingAggregateRowsOf(session));

        // Each pass rewrites a leaf that ends up without a summary, which is exactly the leaf a
        // recount would walk: the insert adds a row with no value, the update takes it away again.
        final byte[] absent = key(new Row(DIRTY_PREFIX, "g3", null, 9_301));
        final byte[] present = key(new Row(DIRTY_PREFIX, "g3", 4_242L, 9_301));
        assertEquals(0, recounts(() -> {
          edit(session, editor -> editor.insert(absent, new byte[0]));
          assertEquals(held + 1, missingAggregateRowsOf(session));
          edit(session, editor -> editor.apply(new byte[][] {absent}, 1, new byte[][] {present}, null, 1));
          edit(session, editor -> editor.remove(present));
        }), "a maintenance write must not walk the rewritten leaf's rows again");
        assertEquals(held, missingAggregateRowsOf(session));
        assertNull(topKOfDirtyRange(session), "the range still holds the fixture's rows without a value");
        assertEquals(expected(rows, CLEAN_PREFIX, 4, Order.MIN_ASC, false), topKOfLatest(session));
      }
    }
  }

  @Test
  void removingMoreRowsWithoutAValueThanTheViewHoldsFailsTheTransaction() {
    final Path databasePath = temporaryDirectory.resolve("underflow");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource").build()));
      try (JsonResourceSession session = database.beginResourceSession("resource");
          JsonNodeTrx writer = session.beginNodeTrx()) {
        build(writer, fixture(false));
        final ProjectionSortedDirectory.Editor editor =
            new ProjectionSortedDirectory.Editor(new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0));
        final byte[] absent = key(new Row(DIRTY_PREFIX, "g3", null, 9_101));
        assertEquals("sorted projection removes more rows without an aggregate value than it holds",
            assertThrows(IllegalStateException.class, () -> editor.remove(absent)).getMessage());
      }
    }
  }

  @Test
  void anUncountedViewNeverInventsACountFromOneMaintenancePass() {
    final Path databasePath = temporaryDirectory.resolve("uncounted-maintenance");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource").build()));
      try (JsonResourceSession session = database.beginResourceSession("resource")) {
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          build(writer, fixture(true));
          writer.commit();
        }
        downgradeHeader(session);
        // One pass sees only its own edits, never the rows the rest of the view already holds, so
        // an uncounted view stays uncounted until it is rebuilt or its summaries are backfilled.
        edit(session, editor -> editor.insert(key(new Row("k2", "g4", null, 9_201)), new byte[0]));
        assertEquals(ProjectionSortedDirectory.MISSING_AGGREGATE_ROWS_UNKNOWN, missingAggregateRowsOf(session));
        edit(session, editor -> editor.remove(key(new Row("k2", "g4", null, 9_201))));
        assertEquals(ProjectionSortedDirectory.MISSING_AGGREGATE_ROWS_UNKNOWN, missingAggregateRowsOf(session));
      }
    }
  }

  @Test
  void aSpilledBuildCountsTheRowsWithoutAnAggregateValue() {
    final List<Row> rows = new ArrayList<>();
    long missing = 0;
    for (int kind = 0; kind < 6; kind++) {
      for (int group = 0; group < 8; group++) {
        for (int row = 0; row < 32; row++) {
          final boolean absent = kind % 5 == 2 && row % 11 == 3;
          if (absent) {
            missing++;
          }
          rows.add(new Row("kind-" + (100 + kind), "group-" + (100 + group), absent
              ? null
              : (long) (group * 1_000 + row), rows.size() + 1));
        }
      }
    }
    final ProjectionSortedRunAccumulator run = new ProjectionSortedRunAccumulator(LAYOUT, 8L << 10,
        new ProjectionSortedRunSpill(temporaryDirectory.resolve("spill-root"), new ByteHandlerPipeline()));
    for (final Row row : rows) {
      final byte[] key = key(row);
      run.append(key, key.length);
    }
    assertTrue(run.spilledRunCount() > 1, "the fixture must exceed the heap budget several times");

    final Path databasePath = temporaryDirectory.resolve("spilled-build");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource").build()));
      try (JsonResourceSession session = database.beginResourceSession("resource")) {
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          assertEquals(rows.size(), run.persist(new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0)));
          writer.commit();
        }
        assertEquals(missing, missingAggregateRowsOf(session));
        try (JsonNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx()) {
          assertNull(topK(trx.getStorageEngineReader(), "kind-102", Order.MIN_ASC, false));
        }
      }
    } finally {
      run.release();
    }
  }

  @Test
  void backfillingLeafSummariesEstablishesTheCountOfAnUncountedView() {
    for (final boolean withAbsentValues : new boolean[] {true, false}) {
      final List<Row> rows = fixture(withAbsentValues);
      final Path databasePath = temporaryDirectory.resolve("backfill-" + withAbsentValues);
      assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));
      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
        assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource").build()));
        try (JsonResourceSession session = database.beginResourceSession("resource")) {
          try (JsonNodeTrx writer = session.beginNodeTrx()) {
            build(writer, rows);
            writer.commit();
          }
          downgradeHeader(session);
          assertEquals(ProjectionSortedDirectory.MISSING_AGGREGATE_ROWS_UNKNOWN, missingAggregateRowsOf(session));
          try (JsonNodeTrx writer = session.beginNodeTrx()) {
            ProjectionSortedGroupScan.buildLeafSummaries(writer.getStorageEngineWriter(), 0);
            writer.commit();
          }
          assertEquals(rowsWithoutAValue(rows), missingAggregateRowsOf(session));
          assertEquals(expected(rows, CLEAN_PREFIX, 4, Order.MIN_ASC, false), topKOfLatest(session));
          if (withAbsentValues) {
            assertNull(topKOfDirtyRange(session));
          } else {
            assertEquals(expected(rows, DIRTY_PREFIX, 4, Order.MIN_ASC, false), topKOfDirtyRange(session));
          }
        }
      }
    }
  }

  /**
   * A view whose last key field is not an aggregated long never consults the count, so it is never
   * paid for: the header keeps no count at all rather than a zero a later release could misread.
   */
  @Test
  void aViewThatAggregatesNothingKeepsNoCount() {
    final ProjectionSortKeyCodec.Layout strings =
        new ProjectionSortKeyCodec.Layout(new byte[] {ProjectionSortKeyCodec.FIELD_STRING,
            ProjectionSortKeyCodec.FIELD_STRING});
    assertFalse(strings.groupsByLastLong());
    final Path databasePath = temporaryDirectory.resolve("aggregates-nothing");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource").build()));
      try (JsonResourceSession session = database.beginResourceSession("resource")) {
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          final ProjectionSortedDirectory.Builder builder = new ProjectionSortedDirectory.Builder(
              new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0), strings);
          final List<byte[]> keys = new ArrayList<>();
          for (int row = 0; row < 8; row++) {
            final ProjectionSortKeyCodec.Writer key = new ProjectionSortKeyCodec.Writer();
            appendString(key, "g" + row);
            if (row % 3 == 0) {
              key.appendMissing();
            } else {
              appendString(key, "v" + row);
            }
            key.appendRecordKey(row + 1);
            keys.add(key.copyKey());
          }
          keys.sort(Arrays::compareUnsigned);
          for (int from = 0; from < keys.size(); from += ROWS_PER_LEAF) {
            final byte[][] leafKeys =
                keys.subList(from, Math.min(keys.size(), from + ROWS_PER_LEAF)).toArray(byte[][]::new);
            final ProjectionSortedLeaf leaf = ProjectionSortedLeaf.encode(leafKeys, null, leafKeys.length);
            assertNotNull(leaf);
            builder.append(leaf);
          }
          builder.finish();
          writer.commit();
        }
        assertEquals(ProjectionSortedDirectory.MISSING_AGGREGATE_ROWS_UNKNOWN, missingAggregateRowsOf(session));
      }
    }
  }

  /**
   * The two header counts never overlap: a reserved unencodable key is not a well-formed row key, so
   * it is never also counted as a row without an aggregate value.
   */
  @Test
  void anUnencodableRowIsNotCountedAsARowWithoutAnAggregateValue() {
    final ProjectionSortKeyCodec.Writer writer = new ProjectionSortKeyCodec.Writer();
    writer.writeUnencodable(17);
    final byte[] unencodable = Arrays.copyOf(writer.bytesRef(), writer.length());
    assertTrue(ProjectionSortKeyCodec.isUnencodable(unencodable, unencodable.length));
    assertFalse(LAYOUT.lastFieldMissing(unencodable, unencodable.length));

    final byte[] absent = key(new Row(CLEAN_PREFIX, "g0", null, 17));
    assertTrue(LAYOUT.lastFieldMissing(absent, absent.length));
    final byte[] present = key(new Row(CLEAN_PREFIX, "g0", 5L, 17));
    assertFalse(LAYOUT.lastFieldMissing(present, present.length));

    // A present long whose encoded bytes hold zeros where an absent field's marker would sit.
    final byte[] zeroed = key(new Row(CLEAN_PREFIX, "g0", Long.MIN_VALUE, 17));
    assertFalse(LAYOUT.lastFieldMissing(zeroed, zeroed.length));
  }

  // --- fixtures and helpers -------------------------------------------------------------------

  /**
   * Three prefixes of five groups of seven rows. {@code withAbsentValues} leaves three rows of the
   * dirty prefix without a value, spread far enough apart to kill three separate leaf summaries.
   */
  private static List<Row> fixture(final boolean withAbsentValues) {
    final List<Row> rows = new ArrayList<>();
    final String[] kinds = {CLEAN_PREFIX, DIRTY_PREFIX, "k2"};
    for (final String kind : kinds) {
      for (int group = 0; group < 5; group++) {
        for (int row = 0; row < 7; row++) {
          final boolean absent = withAbsentValues && DIRTY_PREFIX.equals(kind)
              && (group == 0 && row == 4 || group == 2 && row == 0 || group == 4 && row == 6);
          rows.add(new Row(kind, "g" + group, absent
              ? null
              : (long) (group * 1_000L - row * 37L), rows.size() + 1));
        }
      }
    }
    return List.copyOf(rows);
  }

  /**
   * Four prefixes over one view, laid out so that the three range shapes all occur.
   *
   * <p>
   * {@code k0} holds an odd number of rows, so the leaf that carries its last row also carries the
   * first row of {@code k1} — and that {@code k1} row is the one without a value. The leaf therefore
   * has no summary, while meeting {@code k0}'s range at its upper end and {@code k1}'s at its lower
   * end. {@code k1} holds no other such row, so its walk still runs and, since the offending row is
   * a {@code k1} row, declines; {@code k0}'s walk runs and serves, because the offending row is not
   * in its range. {@code k2} has one row without a value well inside it, and {@code k3} has none.
   * </p>
   */
  private static List<Row> shapesFixture() {
    final List<Row> rows = new ArrayList<>();
    appendKind(rows, BOUNDARY_SERVED_PREFIX, 5, 7, -1);
    appendKind(rows, BOUNDARY_DECLINED_PREFIX, 5, 7, 0);
    appendKind(rows, INSIDE_PREFIX, 4, 9, 1);
    appendKind(rows, SHAPES_CLEAN_PREFIX, 4, 9, -1);
    return List.copyOf(rows);
  }

  /**
   * {@code groups} groups of {@code rowsPerGroup} rows. The first row of {@code absentGroup}, if any,
   * carries no value; an absent value sorts before every present one, so it is that group's first
   * row in key order. Minima, maxima and spans stay distinct across groups, so no fold ties.
   */
  private static void appendKind(final List<Row> rows, final String kind, final int groups,
      final int rowsPerGroup, final int absentGroup) {
    for (int group = 0; group < groups; group++) {
      for (int row = 0; row < rowsPerGroup; row++) {
        rows.add(new Row(kind, "g" + group, group == absentGroup && row == 0
            ? null
            : (long) (group * 1_000L + (long) row * (group + 1)), rows.size() + 1));
      }
    }
  }

  /** Sorted data leaves and group summaries one query's reader fetched. */
  private record Reads(int leaves, int summaries) {
  }

  private static Reads measure(final StorageEngineReader reader, final Runnable query) {
    final AtomicInteger leaves = new AtomicInteger();
    final AtomicInteger summaries = new AtomicInteger();
    ProjectionSortedLeafStore.setQueryLeafReadObserverForTesting(ignored -> leaves.incrementAndGet());
    ProjectionSortedGroupSummary.setReadObserverForTesting(ignored -> summaries.incrementAndGet());
    try {
      query.run();
    } finally {
      ProjectionSortedLeafStore.setQueryLeafReadObserverForTesting(null);
      ProjectionSortedGroupSummary.setReadObserverForTesting(null);
    }
    return new Reads(leaves.get(), summaries.get());
  }

  /** Per-leaf recounts of the rows without a value that {@code work} paid for. */
  private static int recounts(final Runnable work) {
    final AtomicInteger recounts = new AtomicInteger();
    ProjectionSortedGroupSummary.setMissingCountObserverForTesting(ignored -> recounts.incrementAndGet());
    try {
      work.run();
    } finally {
      ProjectionSortedGroupSummary.setMissingCountObserverForTesting(null);
    }
    return recounts.get();
  }

  private static long rowsWithoutAValue(final List<Row> rows) {
    return rows.stream().filter(row -> row.value() == null).count();
  }

  private static byte[] key(final Row row) {
    final ProjectionSortKeyCodec.Writer writer = new ProjectionSortKeyCodec.Writer();
    appendString(writer, row.kind());
    appendString(writer, row.group());
    final Long value = row.value();
    if (value == null) {
      writer.appendMissing();
    } else {
      writer.appendLong(value);
    }
    writer.appendRecordKey(row.record());
    return writer.copyKey();
  }

  private static byte[] prefix(final String kind) {
    final ProjectionSortKeyCodec.Writer writer = new ProjectionSortKeyCodec.Writer();
    appendString(writer, kind);
    return writer.copyKey();
  }

  private static void appendString(final ProjectionSortKeyCodec.Writer writer, final String value) {
    final byte[] utf8 = value.getBytes(StandardCharsets.UTF_8);
    writer.appendUtf8(utf8, 0, utf8.length);
  }

  private static void build(final JsonNodeTrx writer, final List<Row> rows) {
    final byte[][] keys = rows.stream().map(ProjectionSortedMissingAggregateTest::key).toArray(byte[][]::new);
    Arrays.sort(keys, Arrays::compareUnsigned);
    final ProjectionSortedDirectory.Builder builder = new ProjectionSortedDirectory.Builder(
        new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0), LAYOUT);
    for (int from = 0; from < keys.length; from += ROWS_PER_LEAF) {
      final byte[][] leafKeys = Arrays.copyOfRange(keys, from, Math.min(keys.length, from + ROWS_PER_LEAF));
      final ProjectionSortedLeaf leaf = ProjectionSortedLeaf.encode(leafKeys, null, leafKeys.length);
      assertNotNull(leaf);
      builder.append(leaf);
    }
    builder.finish();
  }

  /** Build the view, commit it, then hand a read-only reader of that revision to {@code assertions}. */
  private void withView(final List<Row> rows, final String name, final ViewAssertions assertions) {
    final Path databasePath = temporaryDirectory.resolve(name);
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource").build()));
      try (JsonResourceSession session = database.beginResourceSession("resource")) {
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          build(writer, rows);
          writer.commit();
        }
        try (JsonNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx()) {
          assertions.accept(session, trx.getStorageEngineReader());
        }
      }
    }
  }

  @FunctionalInterface
  private interface ViewAssertions {
    void accept(JsonResourceSession session, StorageEngineReader reader);
  }

  private static @Nullable List<Group> topK(final StorageEngineReader reader, final String kind, final Order order,
      final boolean minOnly) {
    return ProjectionSortedGroupScan.topK(reader, 0, prefix(kind), 4, order, 1, minOnly, null);
  }

  /** Every shape the sorted routes serve over one range, so any change of behaviour shows up. */
  private static List<@Nullable List<Group>> everyRoute(final StorageEngineReader reader, final String kind) {
    final List<@Nullable List<Group>> answers = new ArrayList<>();
    for (final Order order : Order.values()) {
      answers.add(topK(reader, kind, order, false));
    }
    answers.add(topK(reader, kind, Order.MIN_ASC, true));
    return answers;
  }

  /** Every route over every range of {@link #shapesFixture()}. */
  private static List<@Nullable List<Group>> everyRoute(final StorageEngineReader reader) {
    final List<@Nullable List<Group>> answers = new ArrayList<>();
    for (final String kind : new String[] {BOUNDARY_SERVED_PREFIX, BOUNDARY_DECLINED_PREFIX, INSIDE_PREFIX,
        SHAPES_CLEAN_PREFIX}) {
      answers.addAll(everyRoute(reader, kind));
    }
    return answers;
  }

  private static long missingAggregateRowsOf(final StorageEngineReader reader) {
    final ProjectionSortedDirectory.Accessor directory = ProjectionSortedDirectory.open(reader, 0);
    assertNotNull(directory);
    return directory.missingAggregateRows();
  }

  private static @Nullable List<Group> topKOfLatest(final JsonResourceSession session) {
    try (JsonNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx()) {
      return topK(trx.getStorageEngineReader(), CLEAN_PREFIX, Order.MIN_ASC, false);
    }
  }

  /** The range {@link #fixture(boolean)} puts its rows without a value in, and the edits above too. */
  private static @Nullable List<Group> topKOfDirtyRange(final JsonResourceSession session) {
    try (JsonNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx()) {
      return topK(trx.getStorageEngineReader(), DIRTY_PREFIX, Order.MIN_ASC, false);
    }
  }

  private static long missingAggregateRowsOf(final JsonResourceSession session) {
    try (JsonNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx()) {
      return missingAggregateRowsOf(trx);
    }
  }

  private static long missingAggregateRowsOf(final JsonResourceSession session, final int revision) {
    try (JsonNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx(revision)) {
      return missingAggregateRowsOf(trx);
    }
  }

  private static long missingAggregateRowsOf(final JsonNodeReadOnlyTrx trx) {
    final ProjectionSortedDirectory.Accessor directory =
        ProjectionSortedDirectory.open(trx.getStorageEngineReader(), 0);
    assertNotNull(directory);
    return directory.missingAggregateRows();
  }

  /** Apply {@code edits} in one transaction; returns the revision they were committed as. */
  private static int edit(final JsonResourceSession session,
      final Consumer<ProjectionSortedDirectory.Editor> edits) {
    try (JsonNodeTrx writer = session.beginNodeTrx()) {
      edits.accept(
          new ProjectionSortedDirectory.Editor(new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0)));
      final int revision = writer.getRevisionNumber();
      writer.commit();
      return revision;
    }
  }

  /**
   * Rewrite the published header in the version-3 form, which carried no missing-aggregate count.
   * That is what a view built before the count existed holds on disk.
   */
  private static void downgradeHeader(final JsonResourceSession session) {
    try (JsonNodeTrx writer = session.beginNodeTrx()) {
      final ProjectionIndexHOTStorage storage = new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0);
      final byte[] counted = storage.getBlob(ProjectionSortedDirectory.HEADER_SLOT);
      assertNotNull(counted);
      final int fields = counted[FIXED_HEADER_BYTES - 1] & 0xFF;
      assertEquals(FIXED_HEADER_BYTES + fields, counted.length);
      final byte[] uncounted = new byte[FIXED_HEADER_BYTES_WITHOUT_MISSING_AGGREGATE + fields];
      System.arraycopy(counted, 0, uncounted, 0, FIXED_HEADER_BYTES_WITHOUT_MISSING_AGGREGATE - 1);
      uncounted[VERSION_OFFSET] = VERSION_WITHOUT_MISSING_AGGREGATE;
      uncounted[FIXED_HEADER_BYTES_WITHOUT_MISSING_AGGREGATE - 1] = (byte) fields;
      System.arraycopy(counted, FIXED_HEADER_BYTES, uncounted, FIXED_HEADER_BYTES_WITHOUT_MISSING_AGGREGATE, fields);
      storage.putBlob(ProjectionSortedDirectory.HEADER_SLOT, uncounted);
      writer.commit();
    }
  }

  /**
   * Independent fold over {@code kind}'s rows: the {@code limit} groups an order ranks highest, or
   * null on a tie within or at the cut, as the routes report it.
   */
  private static @Nullable List<Group> expected(final List<Row> rows, final String kind, final int limit,
      final Order order, final boolean minOnly) {
    final List<Group> groups = new ArrayList<>();
    rows.stream().filter(row -> row.kind().equals(kind)).map(Row::group).distinct().forEach(group -> {
      long minimum = Long.MAX_VALUE;
      long maximum = Long.MIN_VALUE;
      for (final Row row : rows) {
        if (row.kind().equals(kind) && row.group().equals(group)) {
          final Long value = row.value();
          assertNotNull(value, "the fold covers only fixtures whose rows all carry a value");
          minimum = Math.min(minimum, value);
          maximum = Math.max(maximum, value);
        }
      }
      groups.add(new Group(group, minimum, minOnly
          ? minimum
          : maximum));
    });
    final Comparator<Group> comparator = switch (order) {
      case MIN_ASC -> Comparator.comparingLong(Group::min);
      case MAX_DESC -> Comparator.comparingLong(Group::max).reversed();
      case SPAN_DESC -> Comparator.<Group>comparingLong(group -> group.max() - group.min()).reversed();
    };
    groups.sort(comparator);
    for (int i = 1; i < Math.min(groups.size(), limit + 1); i++) {
      if (comparator.compare(groups.get(i - 1), groups.get(i)) == 0) {
        return null;
      }
    }
    return List.copyOf(groups.subList(0, Math.min(limit, groups.size())));
  }
}
