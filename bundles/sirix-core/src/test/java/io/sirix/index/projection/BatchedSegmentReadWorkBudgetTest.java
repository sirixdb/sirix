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
import io.sirix.budget.EngineWorkCounters;
import io.sirix.budget.WorkCapture;
import io.sirix.budget.WorkReport;
import io.sirix.io.StorageType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;

import java.nio.file.Path;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Work budget for the batched page read: a column fill that asks for many near-adjacent segment
 * pages at once is read in a handful of coalesced runs, not page by page, and reads each region
 * once.
 *
 * <p>
 * {@code FileChannelReader.read(PageReference[], ...)} sorts a batch by file offset, cuts it into
 * runs of near-adjacent pages and reads each run with two positional reads. Every way of getting
 * this wrong returns the same bytes. A batch that stops coalescing costs two reads per page instead
 * of two per run. A run builder over <em>unsorted</em> offsets is worse: it restarts at every
 * backward step and reads the same region again, which once turned 9 MB of segments into 355 MB of
 * reads. Only the counters tell these apart from the healthy read, so this test reads through the
 * column-fill entry point and asserts on them.
 *
 * <p>
 * The batch is handed over <em>shuffled</em>, because callers hand references in logical order and
 * the file is in commit-walk order: the two differ in production, and a batch that happened to be
 * sorted already would hide a missing sort.
 */
@Isolated
final class BatchedSegmentReadWorkBudgetTest {

  private static final String RESOURCE = "resource";

  /** Row groups of the fixture; one segment page per row group in the column that is read. */
  private static final int ROW_GROUPS = 600;

  /** Segment ids of three columns written side by side, so the file interleaves them. */
  private static final int[] SEGMENTS = {7, 23, 51};

  private static final int READ_COLUMN = 1;

  private static final int PAYLOAD_BYTES = 2048;

  /**
   * Runs the fill may take. The column's pages sit in two contiguous stretches of the file, so two is
   * what it takes today; the bound leaves the engine room to lay a commit out differently, and is
   * still an order of magnitude below what an unsorted or uncoalesced batch costs.
   */
  private static final int MAX_RUNS = 8;

  /** Fixed, so the batch order is the same permutation on every run and every machine. */
  private static final long SHUFFLE_SEED = 0x5EED_BA7CL;

  @TempDir
  private Path directory;

  @AfterEach
  void clearCaches() {
    Databases.clearGlobalCaches();
  }

  @Test
  void aColumnFillIsReadInCoalescedRunsAndCoversItsRegionOnce() throws Exception {
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(directory)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(directory)) {
      // FILE_CHANNEL explicitly: it is the backend that coalesces, and a budget must not depend on
      // which backend a platform defaults to.
      assertTrue(database.createResource(
          ResourceConfiguration.newBuilder(RESOURCE).storageType(StorageType.FILE_CHANNEL).build()));
      try (JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
        final ProjectionSlotLayout layout = writeSegments(session);
        Databases.clearGlobalCaches();

        try (JsonNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx()) {
          final StorageEngineReader reader = trx.getStorageEngineReader();
          final long[] offsets = new long[ROW_GROUPS];
          long first = Long.MAX_VALUE;
          long last = Long.MIN_VALUE;
          for (int rowGroup = 0; rowGroup < ROW_GROUPS; rowGroup++) {
            offsets[rowGroup] = ProjectionIndexHOTStorage.segmentPageOffset(reader, 0,
                layout.segmentSlot(rowGroup + 1, SEGMENTS[READ_COLUMN]), 0);
            assertTrue(offsets[rowGroup] >= 0, "segment " + rowGroup + " has no durable offset");
            first = Math.min(first, offsets[rowGroup]);
            last = Math.max(last, offsets[rowGroup]);
          }
          final int[] rowGroupAt = shuffle(offsets);
          final long region = last - first + PAYLOAD_BYTES;

          final WorkCapture.Captured<byte[][]> fill =
              WorkCapture.of(EngineWorkCounters.BATCHED_READS)
                         .call(() -> ProjectionIndexHOTStorage.readSegmentBytesBatch(reader, offsets));

          final byte[][] segments = fill.result();
          assertNotNull(segments);
          for (int i = 0; i < ROW_GROUPS; i++) {
            assertArrayEquals(payload(rowGroupAt[i], READ_COLUMN), segments[i],
                "the batch must return segment " + rowGroupAt[i] + " at the position it was asked for");
          }

          // Measured on this fixture, by mutation: healthy 2 runs / 0 singletons / 1 234 280 span bytes;
          // without the file-order sort 23 runs / 552 singletons; with coalescing off 0 runs / 600.
          final WorkReport work = fill.work();
          work.assertBetween(EngineWorkCounters.READ_RUNS, 1, MAX_RUNS,
              "the column fill no longer coalesces: zero runs means every page is read on its own, and dozens mean "
                  + "the run builder restarts at every backward step because the batch is not sorted by file offset");
          work.assertAtMost(EngineWorkCounters.READ_SINGLETONS, MAX_RUNS,
              "pages of a contiguous column were read one at a time instead of in a run: two positional reads per "
                  + "page where two per run would do");
          work.assertZero(EngineWorkCounters.READ_RUN_FALLBACKS,
              "a page of an append-only file did not end before its successor, so the run re-read it exactly");
          // The floor is the column itself, less the last page of each run, whose body is read apart
          // from the span; the ceiling is twice the column, and re-covering a region blows through it.
          work.assertBetween(EngineWorkCounters.READ_RUN_SPAN_BYTES, (long) (ROW_GROUPS - MAX_RUNS) * PAYLOAD_BYTES,
              2L * ROW_GROUPS * PAYLOAD_BYTES,
              "the coalesced runs cover a multiple of the bytes the column occupies: the same file region is being "
                  + "read more than once");
          assertTrue(region >= (long) ROW_GROUPS * PAYLOAD_BYTES,
              "the column's pages must span at least their own bytes, or the offsets above are not what was written");
        }
      }
    }
  }

  /** Writes three interleaved segment columns in one commit and returns the layout their keys use. */
  private static ProjectionSlotLayout writeSegments(final JsonResourceSession session) {
    try (JsonNodeTrx writer = session.beginNodeTrx()) {
      writer.insertArrayAsFirstChild();
      final ProjectionIndexHOTStorage storage =
          ProjectionIndexHOTStorage.forBulkBuild(writer.getStorageEngineWriter(), 0);
      final ProjectionSlotLayout layout = storage.slotLayout();
      for (int rowGroup = 0; rowGroup < ROW_GROUPS; rowGroup++) {
        for (int column = 0; column < SEGMENTS.length; column++) {
          storage.putColumnSegmentSlot(layout.segmentSlot(rowGroup + 1, SEGMENTS[column]), payload(rowGroup, column));
        }
      }
      writer.commit();
      return layout;
    }
  }

  /** Fisher-Yates over {@code offsets}; returns which row group each position now asks for. */
  private static int[] shuffle(final long[] offsets) {
    final int[] rowGroupAt = new int[offsets.length];
    for (int i = 0; i < rowGroupAt.length; i++) {
      rowGroupAt[i] = i;
    }
    final Random random = new Random(SHUFFLE_SEED);
    for (int i = offsets.length - 1; i > 0; i--) {
      final int j = random.nextInt(i + 1);
      final long offset = offsets[i];
      offsets[i] = offsets[j];
      offsets[j] = offset;
      final int rowGroup = rowGroupAt[i];
      rowGroupAt[i] = rowGroupAt[j];
      rowGroupAt[j] = rowGroup;
    }
    return rowGroupAt;
  }

  /** Incompressible, so a page's size on disk does not depend on the codec a platform loads. */
  private static byte[] payload(final int rowGroup, final int column) {
    final byte[] bytes = new byte[PAYLOAD_BYTES];
    new Random(31L * rowGroup + 17L * column + 1).nextBytes(bytes);
    return bytes;
  }
}
