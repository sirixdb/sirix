package io.sirix.access.trx.page;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.AfterCommitState;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.cache.FrameSlotAllocator;
import io.sirix.cache.TransactionIntentLog;
import io.sirix.io.StorageType;
import io.sirix.node.NodeKind;
import io.sirix.service.json.shredder.JsonShredder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A mutation whose pre-mutation check rotates a storage epoch must still read its own node
 * correctly.
 *
 * <p>
 * The write cursor binds its singleton to the record page the intent log serves, including a page
 * the previous rotation froze. Every mutator runs the epoch check first and reads the current node
 * afterwards. When that check rotates, the previously frozen snapshot is retired: its pages are on
 * disk, closed, and their frames released. A cursor still bound to one of those frames then decoded
 * the node from released memory. In the SH1 publication loop this surfaced as
 * {@code validateRemoval} rejecting an array object with "An object record value can not be
 * removed", because the parent key read back from the recycled frame pointed at a later record of
 * the same page. Production leaves a released frame's bytes intact until memory pressure recycles
 * it, so the test zero-fills released frames to make the stale read deterministic.
 * </p>
 */
final class AsyncFlushCursorRebindTest {

  private static final String RESOURCE = "async-flush-cursor-rebind";

  /** Fused records {@code {"v":i}} are two nodes; a record page holds 1,024 nodes. */
  private static final int RECORDS = 24 * 1024;

  /** Successive updates land on distinct record pages (and distinct record-history pages). */
  private static final int STRIDE = 600;

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
    FrameSlotAllocator.setPoisonOnReleaseForTesting(true);
  }

  @AfterEach
  void tearDown() {
    FrameSlotAllocator.setPoisonOnReleaseForTesting(false);
    JsonTestHelper.deleteEverything();
  }

  @Test
  @Timeout(value = 3, unit = TimeUnit.MINUTES, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void removalThatRotatesTheEpochReadsItsNodeFromTheLiveBinding() {
    Databases.createJsonDatabase(new DatabaseConfiguration(PATHS.PATH1.getFile()));
    final long[] values = new long[RECORDS];
    final List<Long> recordKeys;
    final long victim;
    final int victimOrdinal;
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(PATHS.PATH1.getFile())) {
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                                   .storeDiffs(false)
                                                   .hashKind(HashType.NONE)
                                                   .buildPathSummary(false)
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .build());
      try (final JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
        recordKeys = load(session, values);

        try (final JsonNodeTrx wtx = session.beginNodeTrx(Integer.MAX_VALUE, AfterCommitState.KEEP_OPEN_ASYNC_FLUSH)) {
          final NodeStorageEngineWriter writer = (NodeStorageEngineWriter) wtx.getStorageEngineWriter();
          final TransactionIntentLog log = writer.getLog();

          // Epoch 0: update records on distinct pages until the page bound rotates the first epoch.
          // The update that triggers the rotation lands in the new generation; the first one does not.
          int ordinal = 0;
          final List<Integer> firstEpoch = new ArrayList<>();
          while (log.getCurrentGeneration() == 0) {
            update(wtx, recordKeys, values, ordinal);
            firstEpoch.add(ordinal);
            ordinal += STRIDE;
          }
          assertEquals(1, log.getCurrentGeneration());
          victimOrdinal = firstEpoch.get(0);
          victim = recordKeys.get(victimOrdinal);

          // Epoch 1: fill the live log again on other pages, stopping right at the bound so the next
          // mutation is the one that rotates.
          while (!writer.isAsyncFlushLogBoundaryReached()) {
            update(wtx, recordKeys, values, ordinal);
            ordinal += STRIDE;
          }
          assertEquals(1, log.getCurrentGeneration(), "updates only rotate before themselves");
          assertTrue(ordinal < RECORDS, "the fixture has enough distinct record pages");

          // Position on a record whose page the first rotation froze and nothing touched since, then
          // remove it: the removal's own pre-mutation check retires that snapshot.
          assertTrue(wtx.moveTo(victim));
          assertEquals(NodeKind.OBJECT, wtx.getKind());
          wtx.remove();
          assertEquals(2, log.getCurrentGeneration(), "the rotation ran inside the removal");
          wtx.commit();
        }
      }
    }

    Databases.clearGlobalCaches();
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final JsonResourceSession session = database.beginResourceSession(RESOURCE);
        final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
      assertEquals(2, session.getMostRecentRevisionNumber(), "the load and the edit each published one revision");
      assertTrue(rtx.moveToDocumentRoot() && rtx.moveToFirstChild());
      assertEquals(RECORDS - 1, rtx.getChildCount());
      assertTrue(rtx.moveToFirstChild());
      for (int record = 0; record < RECORDS; record++) {
        if (record == victimOrdinal) {
          continue;
        }
        assertEquals(recordKeys.get(record), rtx.getNodeKey(), "record " + record + " in the sibling chain");
        final long object = rtx.getNodeKey();
        assertTrue(rtx.moveToFirstChild());
        assertEquals(values[record], rtx.getNumberValue().longValue(), "value of record " + record);
        assertTrue(rtx.moveTo(object));
        if (record + 1 < RECORDS) {
          assertTrue(rtx.moveToRightSibling(), "sibling after record " + record);
        } else {
          assertFalse(rtx.moveToRightSibling(), "no sibling after the last record");
        }
      }
    }
  }

  private static List<Long> load(final JsonResourceSession session, final long[] values) {
    final StringBuilder json = new StringBuilder(RECORDS * 12).append('[');
    for (int record = 0; record < RECORDS; record++) {
      if (record > 0) {
        json.append(',');
      }
      values[record] = record;
      json.append("{\"v\":").append(record).append('}');
    }
    json.append(']');
    final List<Long> keys = new ArrayList<>(RECORDS);
    try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
      assertTrue(wtx.moveToDocumentRoot() && wtx.moveToFirstChild() && wtx.moveToFirstChild());
      do {
        keys.add(wtx.getNodeKey());
      } while (wtx.moveToRightSibling());
      assertEquals(RECORDS, keys.size());
      wtx.commit();
    }
    return keys;
  }

  private static void update(final JsonNodeTrx wtx, final List<Long> recordKeys, final long[] values,
      final int ordinal) {
    assertTrue(wtx.moveTo(recordKeys.get(ordinal)) && wtx.moveToFirstChild(), "record " + ordinal);
    values[ordinal] = 1_000_000L + ordinal;
    wtx.setNumberValue(values[ordinal]);
  }
}
