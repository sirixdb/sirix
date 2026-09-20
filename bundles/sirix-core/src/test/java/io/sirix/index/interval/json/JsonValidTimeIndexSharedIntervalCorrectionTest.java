/*
 * [New BSD License]
 * Copyright (c) 2026, SirixDB Contributors
 * All rights reserved.
 */
package io.sirix.index.interval.json;

import io.brackit.query.util.path.PathParser;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.IndexType;
import io.sirix.index.hot.AbstractHOTIndexWriter;
import io.sirix.index.hot.HOTIncrementalInsert;
import io.sirix.index.hot.HOTInvariantValidator;
import io.sirix.index.interval.IntervalDomain;
import io.sirix.index.interval.RelationalIntervalTree;
import io.sirix.index.interval.ValidTimeIntervalIndexFactory;
import io.sirix.node.NodeKind;
import io.sirix.service.json.shredder.JsonShredder;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end load of a JSON resource with a declared valid-time index in the shape that made the
 * index writer publish a node whose children were not ordered by first key.
 *
 * <p>
 * A first publication stores many records that are all valid over the same interval. The interval
 * index then holds one endpoint per store, and the records spread over the trailing chunk index of
 * that one key: a handful of keys, each with a posting of several KiB, so that a leaf holds only a
 * few of them. A second publication corrects records — it replaces or retracts a span of a record's
 * validity, which shortens the record and appends what remains as new records — and so registers
 * many new endpoints under a few fork nodes next to those value-heavy chunk keys. Before the fix
 * the load stopped inside the second publication with
 * {@code IllegalStateException: HOT published structural splice is malformed (first:
 * I8-children-sorted-by-firstkey …)}: the writer had folded the upper half of a split leaf into its
 * parent past a sibling, and its own validation refused the result.
 * </p>
 *
 * <p>
 * Nothing about the data is unusual, but which structural handlers a load reaches depends on the
 * record layout — it fixes the node keys, hence the chunk each posting falls into and the shape of
 * the trie — and on the order of the corrections: that placement is met about once in several
 * thousand of them. The publication therefore opens with the {@link #LEADING_CORRECTIONS} that meet
 * it at once, and continues with a generated stream for breadth. That the load still reaches the
 * placement that went wrong is asserted, through the counter incremented where that placement is
 * declined; an unrelated layout change can move the case but no longer silently remove it. The same
 * holds for the join a declined fold falls back to, which has to split a side a bit of its block
 * cuts through. {@code HOTStraddlingLeafSpliceTest} pins both with the shortest sequences that
 * reach them.
 * </p>
 *
 * <p>
 * Surviving the load is not enough — every answer must stay exact. The committed trie must satisfy
 * every structural invariant, including that each stored key still routes to the leaf that holds
 * it, and each stabbing query is compared with a scan of the records, at the latest revision and at
 * the one before the corrections.
 * </p>
 *
 * <p>
 * Runs in about 10 seconds here, so it belongs in the default lane; a suite that grows past 60
 * seconds belongs behind {@code @Tag("heavy")}, which the advisory cross-platform CI lanes exclude
 * ({@code bundles/sirix-core/build.gradle}).
 * </p>
 */
final class JsonValidTimeIndexSharedIntervalCorrectionTest {

  private static final String RESOURCE = "valid-time-shared-interval";
  private static final int INDEX_ID = 0;

  /**
   * Records of the first publication; all valid over {@code [FIRST_DAY, FIRST_DAY + HORIZON_DAYS]}.
   * Their postings have to spread over enough chunk keys of several KiB each for the trie below them
   * to come from byte-driven leaf splits; the load that failed had 100,000.
   */
  private static final int RECORDS = 60_000;
  private static final int HORIZON_DAYS = 366;

  /**
   * The corrections that open the second publication, as {@code {record, fromDay, toDay, replaces}}:
   * the span {@code [fromDay, toDay]} of the record is replaced ({@code 1}) or retracted ({@code 0}).
   * The last of them is the one during which the writer published the disordered node.
   */
  private static final int[][] LEADING_CORRECTIONS =
      {{1, 0, 86, 1}, {2, 0, 7, 1}, {11, 38, 114, 0}, {75, 37, 88, 1}, {78, 0, 13, 1}, {120, 0, 75, 0}, {148, 0, 21, 1},
          {155, 14, 65, 1}, {162, 0, 82, 1}, {182, 0, 31, 1}, {212, 0, 87, 1}, {254, 12, 29, 1}, {272, 0, 90, 0},
          {284, 0, 21, 1}, {289, 0, 42, 1}, {299, 41, 73, 0}, {311, 0, 88, 1}, {331, 0, 5, 0}, {366, 7, 43, 1},
          {411, 13, 99, 1}, {419, 0, 73, 1}, {458, 0, 4, 1}, {512, 0, 39, 1}, {515, 24, 93, 0}, {534, 0, 82, 1},
          {537, 1, 35, 1}, {554, 38, 64, 1}, {555, 8, 55, 1}, {558, 0, 39, 1}, {562, 23, 105, 0}, {570, 0, 22, 1},
          {676, 9, 28, 1}, {719, 41, 51, 1}, {755, 0, 48, 1}, {774, 0, 15, 1}};

  /**
   * Corrections cut validity short within the first {@code 45 + 1 + 89} days; past them every probe
   * sees the same records, so the probes are dense up to here and sparse beyond.
   */
  private static final int LAST_CORRECTED_DAY = 135;

  /** Generated corrections that follow, over records past the leading ones, in ascending order. */
  private static final int GENERATED_CORRECTIONS = 300;
  private static final int FIRST_GENERATED_RECORD = 1_000;
  private static final int GENERATED_RECORD_STRIDE = 20;

  private static final Instant FIRST_DAY = Instant.parse("2024-01-01T00:00:00Z");

  @TempDir
  Path temporaryDirectory;

  @AfterEach
  void clearCaches() {
    Databases.clearGlobalCaches();
  }

  @Test
  @DisplayName("correcting records that share one validity interval keeps every valid-time answer exact")
  void correctionsOfSharedIntervalStayExact() {
    final Path databasePath = temporaryDirectory.resolve("db");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));

    final int capacity = RECORDS + 2 * (LEADING_CORRECTIONS.length + GENERATED_CORRECTIONS);
    final long[] objectKeys = new long[capacity];
    final int[] fromDay = new int[capacity];
    final int[] toDay = new int[capacity];
    int records = RECORDS;
    final int firstRevision;
    final int latestRevision;
    final long foldsDeclinedBefore = HOTIncrementalInsert.EXISTING_BIT_FOLD_NOT_ADJACENT.get();
    final long joinSplitsBefore = AbstractHOTIndexWriter.FRONTIER_JOIN_STRADDLE_SPLIT.get();

    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      assertTrue(database.createResource(
          ResourceConfiguration.newBuilder(RESOURCE).validTimePaths("vf", "vt").buildPathSummary(true).build()));

      try (JsonResourceSession session = database.beginResourceSession(RESOURCE);
          JsonNodeTrx wtx = session.beginNodeTrx()) {
        final StringBuilder json = new StringBuilder(RECORDS * 100).append('[');
        for (int record = 0; record < RECORDS; record++) {
          if (record > 0) {
            json.append(',');
          }
          appendRecord(json, record, 0, HORIZON_DAYS);
        }
        json.append(']');
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
        wtx.moveToDocumentRoot();
        assertTrue(wtx.moveToFirstChild(), "record array");
        final long arrayKey = wtx.getNodeKey();
        session.getWtxIndexController(wtx.getRevisionNumber()).createIndexes(Set.of(validTimeDefinition()), wtx);

        assertTrue(wtx.moveTo(arrayKey) && wtx.moveToFirstChild(), "first record");
        for (int record = 0; record < RECORDS; record++) {
          objectKeys[record] = wtx.getNodeKey();
          toDay[record] = HORIZON_DAYS;
          assertTrue(record + 1 == RECORDS || wtx.moveToRightSibling(), "record " + (record + 1));
        }
        wtx.commit();
        firstRevision = session.getMostRecentRevisionNumber();

        final StringBuilder record = new StringBuilder(128);
        for (final int[] correction : LEADING_CORRECTIONS) {
          records = correct(wtx, arrayKey, record, records, correction[0], correction[1], correction[2],
              correction[3] == 1, objectKeys, fromDay, toDay);
        }
        for (int correction = 0; correction < GENERATED_CORRECTIONS; correction++) {
          final long hash = mix(correction);
          final int corrected = FIRST_GENERATED_RECORD + correction * GENERATED_RECORD_STRIDE
              + (int) Long.remainderUnsigned(hash, GENERATED_RECORD_STRIDE);
          final int spanFrom = Long.remainderUnsigned(hash >>> 20, 5) == 0
              ? 16 + (int) Long.remainderUnsigned(hash >>> 24, 30)
              : 0;
          final int spanTo = spanFrom + 1 + (int) Long.remainderUnsigned(hash >>> 32, 90);
          records = correct(wtx, arrayKey, record, records, corrected, spanFrom, spanTo,
              Long.remainderUnsigned(hash >>> 40, 5) != 0, objectKeys, fromDay, toDay);
        }
        wtx.commit();
        latestRevision = session.getMostRecentRevisionNumber();
      }

      assertTrue(firstRevision < latestRevision);
      assertStructurallySound(database, latestRevision);
      assertExactStabs(database, latestRevision, records, objectKeys, fromDay, toDay);
      // Before the corrections every record was valid from the first day over the whole horizon.
      final int[] toDayAtFirstRevision = new int[RECORDS];
      Arrays.fill(toDayAtFirstRevision, HORIZON_DAYS);
      assertExactStabs(database, firstRevision, RECORDS, objectKeys, new int[RECORDS], toDayAtFirstRevision);

      // Last, so that a broken writer is reported as the defect it is and not as a load that no longer
      // reaches it.
      assertTrue(HOTIncrementalInsert.EXISTING_BIT_FOLD_NOT_ADJACENT.get() > foldsDeclinedBefore,
          "the load must reach a fold whose upper half would not land beside its slot; without one it no "
              + "longer covers the placement that was published out of order and its record layout must be "
              + "re-tuned");
      assertTrue(AbstractHOTIndexWriter.FRONTIER_JOIN_STRADDLE_SPLIT.get() > joinSplitsBefore,
          "the load must reach a complete-frontier join that has to split a side a bit of its block cuts "
              + "through; without one it no longer covers the join a declined fold falls back to");
    }
  }

  private static void assertStructurallySound(final Database<JsonResourceSession> database, final int revision) {
    try (JsonResourceSession session = database.beginResourceSession(RESOURCE);
        JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision)) {
      HOTInvariantValidator.validateIndex(rtx.getStorageEngineReader(), IndexType.VALIDTIME, INDEX_ID).assertOk();
    }
  }

  /**
   * Stab the index on days spread over the horizon — on shared bounds, between them, and outside
   * every interval — and compare each answer with a scan of the first {@code records} records.
   * Intervals are closed on both ends.
   */
  private static void assertExactStabs(final Database<JsonResourceSession> database, final int revision,
      final int records, final long[] objectKeys, final int[] fromDay, final int[] toDay) {
    try (JsonResourceSession session = database.beginResourceSession(RESOURCE);
        JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision)) {
      final IntervalDomain domain = new IntervalDomain();
      final RelationalIntervalTree tree =
          ValidTimeIntervalIndexFactory.createReaderTree(rtx.getStorageEngineReader(), INDEX_ID, domain);
      final LongArrayList actual = new LongArrayList(records);
      final LongArrayList expected = new LongArrayList(records);
      final long[] from = new long[records];
      final long[] to = new long[records];
      for (int record = 0; record < records; record++) {
        from[record] = day(fromDay[record]).toEpochMilli();
        to[record] = day(toDay[record]).toEpochMilli();
      }

      for (final int probeDay : probeDays()) {
        // Whole days hit the shared bounds exactly; the half-day offset lands strictly between them.
        for (final long offsetHours : new long[] {0L, 12L}) {
          final Instant probe = day(probeDay).plus(offsetHours, ChronoUnit.HOURS);
          final long point = probe.toEpochMilli();

          expected.clear();
          for (int record = 0; record < records; record++) {
            if (from[record] <= point && point <= to[record]) {
              expected.add(objectKeys[record]);
            }
          }
          expected.sort(null);

          actual.clear();
          tree.stab(domain.point(probe), actual::add);
          actual.sort(null);

          assertEquals(expected, actual, "valid-time answer at " + probe + " in revision " + revision);
        }
      }
    }
  }

  /**
   * The day before the first one, every fifth day while corrections still change the answer, then
   * days deep in the unchanged tail, the horizon's last day and the day after it.
   */
  private static int[] probeDays() {
    final IntArrayList days = new IntArrayList();
    for (int day = -1; day <= LAST_CORRECTED_DAY; day += 5) {
      days.add(day);
    }
    days.add(200);
    days.add(300);
    days.add(HORIZON_DAYS);
    days.add(HORIZON_DAYS + 1);
    return days.toIntArray();
  }

  /**
   * Replace or retract the span {@code [spanFrom, spanTo]} of a record valid over the whole horizon.
   * The record keeps the validity before the span — or, when the span starts on the first day, the
   * span itself if it is replaced, and nothing at all if it is retracted, in which case the record is
   * removed. A replaced span that starts later and the unchanged tail become new records.
   *
   * @return the number of records after the correction
   */
  private static int correct(final JsonNodeTrx wtx, final long arrayKey, final StringBuilder json,
      final int recordsBefore, final int corrected, final int spanFrom, final int spanTo, final boolean replaces,
      final long[] objectKeys, final int[] fromDay, final int[] toDay) {
    assertEquals(HORIZON_DAYS, toDay[corrected], "record " + corrected + " must not have been corrected before");
    int records = recordsBefore;
    if (spanFrom == 0 && !replaces) {
      assertTrue(wtx.moveTo(objectKeys[corrected]));
      wtx.remove();
      fromDay[corrected] = 1; // an empty interval: the record no longer answers any query
      toDay[corrected] = 0;
    } else {
      final int keptUntil = spanFrom == 0
          ? spanTo
          : spanFrom;
      assertTrue(wtx.moveTo(namedStringChildKey(wtx, objectKeys[corrected], "vt")));
      wtx.setStringValue(day(keptUntil).toString());
      toDay[corrected] = keptUntil;
      if (spanFrom != 0 && replaces) {
        records = appendRecord(wtx, arrayKey, json, records, spanFrom, spanTo, objectKeys, fromDay, toDay);
      }
    }
    return appendRecord(wtx, arrayKey, json, records, spanTo, HORIZON_DAYS, objectKeys, fromDay, toDay);
  }

  private static int appendRecord(final JsonNodeTrx wtx, final long arrayKey, final StringBuilder json,
      final int records, final int from, final int to, final long[] objectKeys, final int[] fromDay,
      final int[] toDay) {
    json.setLength(0);
    appendRecord(json, records, from, to);
    assertTrue(wtx.moveTo(arrayKey));
    wtx.insertSubtreeAsLastChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
    objectKeys[records] = wtx.getNodeKey();
    fromDay[records] = from;
    toDay[records] = to;
    return records + 1;
  }

  /** Eight fields: with the object itself a record spans nine node keys. */
  private static void appendRecord(final StringBuilder json, final int id, final int from, final int to) {
    json.append("{\"id\":")
        .append(id)
        .append(",\"a\":")
        .append(id % 97)
        .append(",\"b\":")
        .append(id % 89)
        .append(",\"c\":")
        .append(id % 83)
        .append(",\"d\":")
        .append(id % 79)
        .append(",\"e\":")
        .append(id % 4)
        .append(",\"vf\":\"")
        .append(day(from))
        .append("\",\"vt\":\"")
        .append(day(to))
        .append("\"}");
  }

  private static Instant day(final int day) {
    return FIRST_DAY.plus(day, ChronoUnit.DAYS);
  }

  /** SplitMix64 finalizer: a fixed, well-spread stream so the load is identical on every run. */
  private static long mix(final long value) {
    long z = (value + 1) * 0x9E3779B97F4A7C15L;
    z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
    z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
    return z ^ (z >>> 31);
  }

  private static IndexDef validTimeDefinition() {
    return IndexDefs.createValidTimeIdxDef(
        new LinkedHashSet<>(List.of(parse("/[]/vf", PathParser.Type.JSON), parse("/[]/vt", PathParser.Type.JSON))),
        INDEX_ID, IndexDef.DbType.JSON);
  }

  private static long namedStringChildKey(final JsonNodeTrx wtx, final long objectKey, final String name) {
    assertTrue(wtx.moveTo(objectKey));
    if (wtx.moveToFirstChild()) {
      do {
        if (wtx.getKind() == NodeKind.OBJECT_NAMED_STRING && name.equals(wtx.getName().getLocalName())) {
          return wtx.getNodeKey();
        }
      } while (wtx.moveToRightSibling());
    }
    throw new AssertionError("missing string field " + name + " below " + objectKey);
  }
}
