/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.budget;

import io.sirix.JsonTestHelper;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.page.StorageEpochTestSupport;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.io.StorageType;
import io.sirix.node.BytesOut;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Work budget for the transient buffers of commits and cold reads.
 *
 * <p>
 * The engine owns no spare buffers outside the allocator: a commit's flush buffer, the sink a page
 * is serialized into and the buffer a read lands in are all borrowed from the allocator's scratch
 * and returned to it. The regressions this guards are the ones that used to hide in private pools:
 * a path that stops returning what it borrowed (a leak the allocator's budget pays for), a path
 * that borrows per page what it should hold per commit, and a warm path that makes the allocator
 * back fresh buffers instead of reusing the resident ones.
 *
 * <p>
 * Measured on this fixture (FILE_CHANNEL, one small object appended per commit, sixteen commits in
 * one transaction): 17 flush-buffer borrows (the transaction's writer and the one each commit
 * re-instantiates), 243 read borrows, no buffer created, none left lent. A read of the cached
 * resource borrows nothing; a cold read of it borrows 4 read buffers. With the writer's close no
 * longer returning its flush buffer the transaction leaves 17 buffers lent. Asking a writer for a
 * new flush buffer borrows nothing (it clears the one it holds); with the buffer exchanged instead,
 * the request returns a different buffer. A publication of 16,384 objects under the import bound
 * rotates 6 storage epochs and borrows 3 flush buffers (the writer's, its one append buffer, the
 * successor writer's) and 91 serialization buffers; with an append buffer per epoch it borrows 8.
 */
@Isolated
final class ScratchBufferWorkBudgetTest {

  private static final String RESOURCE = "scratch";

  private static final int COMMITS = 16;

  private static final WorkCapture CAPTURE = WorkCapture.of(EngineWorkCounters.SCRATCH);

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.closeEverything();
    JsonTestHelper.deleteEverything();
  }

  @Test
  void commitsHoldOneFlushBufferEachAndReturnEverythingTheyBorrow() throws Exception {
    final Path databasePath = JsonTestHelper.PATHS.PATH1.getFile();
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE).storageType(StorageType.FILE_CHANNEL).build());
      try (final JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
        // Warm: the first transaction makes the allocator back the buffers this thread's commits use.
        appendAndCommit(session, 2, 0);

        final WorkReport transaction = CAPTURE.run(() -> appendAndCommit(session, COMMITS, 100));
        transaction.assertExactly(EngineWorkCounters.SCRATCH_COMMIT_FLUSH_BORROWS, COMMITS + 1,
            "a storage writer borrows one flush buffer for its lifetime and a commit clears it; borrowing more "
                + "means the buffer is exchanged per commit or per flush")
                   .assertZero(EngineWorkCounters.SCRATCH_BUFFERS_LENT,
                       "the transaction left scratch buffers lent: a writer, a serializer or a read did not "
                           + "return what it borrowed")
                   .assertZero(EngineWorkCounters.SCRATCH_BUFFERS_CREATED,
                       "warm commits made the allocator back fresh buffers instead of reusing the resident ones")
                   .assertAtLeast(EngineWorkCounters.SCRATCH_READ_BORROWS, 1,
                       "a commit reads through allocator scratch; a zero here means the counter is not connected")
                   .assertZero(EngineWorkCounters.SCRATCH_REFUSALS, "the budget refused a commit's buffer")
                   .assertZero(EngineWorkCounters.SCRATCH_STALE_RETURNS, "a buffer was returned twice");
      }
      try (final JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
        final WorkReport warmRead = CAPTURE.run(() -> assertEquals(2 + COMMITS, countObjects(session)));
        warmRead.assertZero(EngineWorkCounters.SCRATCH_READ_BORROWS,
            "a read of cached pages went to storage and borrowed a read buffer")
                .assertZero(EngineWorkCounters.SCRATCH_BUFFERS_LENT, "the read left scratch buffers lent");
      }
    }
    Databases.clearGlobalCaches();
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
      final WorkReport coldRead = CAPTURE.run(() -> assertEquals(2 + COMMITS, countObjects(session)));
      coldRead.assertBetween(EngineWorkCounters.SCRATCH_READ_BORROWS, 1, 8,
          "a cold read of a few pages borrows one buffer per storage read; more means reads stopped sharing "
              + "the request floor or borrow per record")
              .assertZero(EngineWorkCounters.SCRATCH_BUFFERS_LENT, "the cold read left scratch buffers lent")
              .assertZero(EngineWorkCounters.SCRATCH_BUFFERS_CREATED,
                  "the cold read made the allocator back a fresh buffer although the commits left resident ones")
              .assertZero(EngineWorkCounters.SCRATCH_STALE_RETURNS, "a buffer was returned twice");
    }
  }

  @Test
  void aWriterAskedForANewFlushBufferKeepsItsBorrowAndClearsIt() throws Exception {
    final Path databasePath = JsonTestHelper.PATHS.PATH1.getFile();
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE).storageType(StorageType.FILE_CHANNEL).build());
      try (final JsonResourceSession session = database.beginResourceSession(RESOURCE);
          final JsonNodeTrx trx = session.beginNodeTrx()) {
        final StorageEngineWriter writer = trx.getStorageEngineWriter();
        final BytesOut<?> first = writer.newBufferedBytesInstance();
        first.writeLong(42L);
        final WorkCapture.Captured<BytesOut<?>> renewed = CAPTURE.call(() -> {
          writer.newBufferedBytesInstance();
          writer.newBufferedBytesInstance();
          return writer.newBufferedBytesInstance();
        });
        assertSame(first, renewed.result(), "the writer continues in the buffer it holds");
        assertEquals(0, renewed.result().writePosition(), "cleared for the next use");
        renewed.work()
               .assertZero(EngineWorkCounters.SCRATCH_COMMIT_FLUSH_BORROWS,
                   "a writer exchanged its flush buffer although the one it holds is small enough to keep")
               .assertZero(EngineWorkCounters.SCRATCH_BUFFERS_LENT, "the exchange left a buffer lent");
      }
    }
  }

  @Test
  void storageEpochsOfOneWriterShareOneAppendBuffer() throws Exception {
    final Path databasePath = JsonTestHelper.PATHS.PATH1.getFile();
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    StorageEpochTestSupport.setPlainTransactionBound(StorageEpochTestSupport.IMPORT_BOUND);
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE).storageType(StorageType.FILE_CHANNEL).build());
      try (final JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
        // Warm: one publication that rotates an epoch, so the buffers exist.
        publish(session, 4_096);

        final int[] epochs = new int[1];
        final WorkReport publication = CAPTURE.run(() -> epochs[0] = publish(session, 16_384));
        assertTrue(epochs[0] >= 3, "the fixture must rotate several storage epochs in one writer, not " + epochs[0]);
        publication.assertExactly(EngineWorkCounters.SCRATCH_COMMIT_FLUSH_BORROWS, 3,
            "one writer borrows its flush buffer and ONE append buffer for all of its storage epochs, and the "
                + "commit's successor writer its flush buffer; more means an append buffer per epoch")
                   .assertAtLeast(EngineWorkCounters.SCRATCH_SERIALIZATION_BORROWS, epochs[0],
                       "every epoch serializes its snapshot pages into borrowed scratch")
                   .assertZero(EngineWorkCounters.SCRATCH_BUFFERS_LENT,
                       "the publication left scratch buffers lent: an epoch's append or serialization buffer was "
                           + "not returned")
                   .assertZero(EngineWorkCounters.SCRATCH_REFUSALS, "the budget refused an epoch's buffer")
                   .assertZero(EngineWorkCounters.SCRATCH_STALE_RETURNS, "a buffer was returned twice");
      }
    } finally {
      StorageEpochTestSupport.setPlainTransactionBound(0);
    }
  }

  /**
   * One write transaction that appends {@code inserts} small objects and commits once.
   *
   * @return the storage epochs its writer rotated before the commit
   */
  private static int publish(final JsonResourceSession session, final int inserts) {
    try (final JsonNodeTrx trx = session.beginNodeTrx()) {
      trx.moveToDocumentRoot();
      final long arrayKey = trx.hasFirstChild()
          ? trx.getFirstChildKey()
          : trx.insertArrayAsFirstChild().getNodeKey();
      for (int i = 0; i < inserts; i++) {
        assertTrue(trx.moveTo(arrayKey));
        trx.insertSubtreeAsLastChild(JsonShredder.createStringReader("{\"v\":" + i + "}"), JsonNodeTrx.Commit.NO);
      }
      final int epochs = StorageEpochTestSupport.rotatedEpochs(trx);
      trx.commit();
      return epochs;
    }
  }

  /** One write transaction: {@code commits} commits of one appended object each, then close. */
  private static void appendAndCommit(final JsonResourceSession session, final int commits, final int base) {
    try (final JsonNodeTrx trx = session.beginNodeTrx()) {
      for (int i = 0; i < commits; i++) {
        trx.moveToDocumentRoot();
        if (trx.hasFirstChild()) {
          assertTrue(trx.moveToFirstChild(), "the array root");
          trx.insertSubtreeAsLastChild(JsonShredder.createStringReader("{\"n\":" + (base + i) + "}"),
              JsonNodeTrx.Commit.NO);
        } else {
          trx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[{\"n\":" + (base + i) + "}]"),
              JsonNodeTrx.Commit.NO);
        }
        trx.commit();
      }
    }
  }

  private static int countObjects(final JsonResourceSession session) {
    int objects = 0;
    try (final JsonNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx()) {
      trx.moveToDocumentRoot();
      assertTrue(trx.moveToFirstChild(), "the array root");
      if (trx.moveToFirstChild()) {
        do {
          objects++;
        } while (trx.moveToRightSibling());
      }
    }
    return objects;
  }
}
