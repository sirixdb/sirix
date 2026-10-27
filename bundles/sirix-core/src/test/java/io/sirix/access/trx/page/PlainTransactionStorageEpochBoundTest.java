package io.sirix.access.trx.page;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.cache.TransactionIntentLog;
import io.sirix.io.StorageType;
import io.sirix.service.json.shredder.JsonShredder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A plain write transaction must not hold every record page it touched until commit.
 *
 * <p>
 * This is a work budget in the engine's own unit, the live intent-log entry count: one entry per
 * record page the current storage epoch modified, which is what the off-heap arena has to hold. The
 * SH1 bulk load publishes through plain {@code beginNodeTrx()} transactions, and the storage-epoch
 * bound used to arm only for {@code KEEP_OPEN_ASYNC_FLUSH}; a publication therefore kept every
 * touched page resident (about 38,000 frames of 64 KiB in the first t1M publication), the allocator
 * never gives a frame back, and the load ran out of memory in proportion to the data it had loaded.
 * The fixture plays several publications, each inserting new records and updating records spread
 * over the pages of all earlier ones, and samples the live entry count after every mutation.
 * </p>
 *
 * <p>
 * The production bound is sized from the off-heap budget (one sixteenth of it in 64 KiB frames),
 * far more pages than a fixture should touch, so the test scales the threshold down to the import
 * bound through the writer's test seam instead of scaling the data up.
 * </p>
 *
 * <p>
 * Measured (2026-10-27, FILE_CHANNEL, bound 16): with the bound armed for plain transactions the
 * peak stays at 17 live entries over 10 to 14 rotations per publication; without it the
 * publications peak at 25, 41, 57 and 73 entries and never rotate. No wall clock is involved.
 * </p>
 */
final class PlainTransactionStorageEpochBoundTest {

  private static final String RESOURCE = "plain-trx-storage-epoch-bound";

  private static final int PUBLICATIONS = 4;

  /** New fused records per publication: two nodes each, so eight or nine record pages. */
  private static final int INSERTS_PER_PUBLICATION = 4_096;

  /** Updates spread over all earlier records; successive updates land on distinct record pages. */
  private static final int UPDATES_PER_PUBLICATION = 96;

  /**
   * Floor over the whole run: the publications really touched many more pages than one epoch holds.
   */
  private static final int MIN_TOTAL_ROTATIONS = 12;

  private static final int UPDATE_STRIDE = 557;

  /**
   * The epoch check runs before a mutation, so the live log may exceed the bound by the pages one
   * mutation touches; a bulk insert accounts after the record. Twice the bound leaves room for that
   * and still fails a transaction that never rotates after a few dozen pages.
   */
  private static final int PEAK_LIVE_ENTRY_BUDGET = 2 * NodeStorageEngineWriter.MAX_ASYNC_FLUSH_LOG_ENTRY_COUNT;

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
    NodeStorageEngineWriter.setStorageEpochLogEntryCountForTesting(
        NodeStorageEngineWriter.MAX_ASYNC_FLUSH_LOG_ENTRY_COUNT);
  }

  @AfterEach
  void tearDown() {
    NodeStorageEngineWriter.setStorageEpochLogEntryCountForTesting(0);
    JsonTestHelper.deleteEverything();
  }

  @Test
  @Timeout(value = 3, unit = TimeUnit.MINUTES, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void liveIntentLogStaysBoundedAcrossPublications() {
    Databases.createJsonDatabase(new DatabaseConfiguration(PATHS.PATH1.getFile()));
    final List<Long> recordKeys = new ArrayList<>(PUBLICATIONS * INSERTS_PER_PUBLICATION);
    final List<Long> values = new ArrayList<>(PUBLICATIONS * INSERTS_PER_PUBLICATION);
    final int[] peaks = new int[PUBLICATIONS];
    final int[] rotations = new int[PUBLICATIONS];
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(PATHS.PATH1.getFile())) {
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE).storageType(StorageType.FILE_CHANNEL).build());
      try (final JsonResourceSession session = database.beginResourceSession(RESOURCE);
          final JsonNodeTrx wtx = session.beginNodeTrx()) {
        final long arrayKey = wtx.insertArrayAsFirstChild().getNodeKey();
        wtx.commit();

        for (int publication = 0; publication < PUBLICATIONS; publication++) {
          // Every commit re-instantiates the storage writer and its intent log.
          final TransactionIntentLog log = ((NodeStorageEngineWriter) wtx.getStorageEngineWriter()).getLog();
          int peak = 0;
          for (int insert = 0; insert < INSERTS_PER_PUBLICATION; insert++) {
            final long value = (long) publication * INSERTS_PER_PUBLICATION + insert;
            assertTrue(wtx.moveTo(arrayKey));
            wtx.insertSubtreeAsLastChild(JsonShredder.createStringReader("{\"v\":" + value + "}"),
                JsonNodeTrx.Commit.NO);
            assertTrue(wtx.moveTo(arrayKey) && wtx.moveToLastChild());
            recordKeys.add(wtx.getNodeKey());
            values.add(value);
            peak = Math.max(peak, log.liveEntryCount());
          }
          for (int update = 0; update < UPDATES_PER_PUBLICATION; update++) {
            final int ordinal = (update * UPDATE_STRIDE) % recordKeys.size();
            assertTrue(wtx.moveTo(recordKeys.get(ordinal)) && wtx.moveToFirstChild());
            final long value = 1_000_000L * (publication + 1) + update;
            wtx.setNumberValue(value);
            values.set(ordinal, value);
            peak = Math.max(peak, log.liveEntryCount());
          }
          peaks[publication] = peak;
          rotations[publication] = log.getCurrentGeneration();
          wtx.commit();
        }
      }
    }

    final String figures =
        "peak live entries per publication " + Arrays.toString(peaks) + ", rotations " + Arrays.toString(rotations);
    System.out.println(PlainTransactionStorageEpochBoundTest.class.getSimpleName() + ": " + figures);
    int totalRotations = 0;
    for (int publication = 0; publication < PUBLICATIONS; publication++) {
      assertTrue(rotations[publication] >= 1, "publication " + publication + " rotated a storage epoch: " + figures);
      assertTrue(peaks[publication] <= PEAK_LIVE_ENTRY_BUDGET,
          "publication " + publication + " kept its live intent log bounded: " + figures);
      totalRotations += rotations[publication];
    }
    assertTrue(totalRotations >= MIN_TOTAL_ROTATIONS, "the fixture touched many more pages than one epoch: " + figures);

    Databases.clearGlobalCaches();
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final JsonResourceSession session = database.beginResourceSession(RESOURCE);
        final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
      assertEquals(PUBLICATIONS + 1, session.getMostRecentRevisionNumber());
      assertTrue(rtx.moveToDocumentRoot() && rtx.moveToFirstChild());
      assertEquals(recordKeys.size(), rtx.getChildCount());
      assertTrue(rtx.moveToFirstChild());
      for (int record = 0; record < recordKeys.size(); record++) {
        assertEquals(recordKeys.get(record), rtx.getNodeKey(), "record " + record);
        final long object = rtx.getNodeKey();
        assertTrue(rtx.moveToFirstChild());
        assertEquals(values.get(record), rtx.getNumberValue().longValue(), "value of record " + record);
        assertTrue(rtx.moveTo(object));
        assertEquals(record + 1 < recordKeys.size(), rtx.moveToRightSibling(), "sibling chain at record " + record);
      }
    }
  }
}
