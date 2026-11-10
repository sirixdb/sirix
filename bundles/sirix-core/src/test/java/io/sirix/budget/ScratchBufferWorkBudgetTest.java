/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.budget;

import io.sirix.JsonTestHelper;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.access.trx.node.json.ParallelBulkJsonImporter;
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

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
 * re-instantiates), 243 read borrows, 48 borrows for the writers' own file I/O (per commit: the
 * window the frontier is validated through, the revision record, the revision-record ring for both
 * beacon slots; 64 with a buffer per slot), no buffer created, none left lent. A read of the cached
 * resource borrows nothing; a cold read of it borrows 4 read buffers. With the writer's close no
 * longer returning its flush buffer the transaction leaves 17 buffers lent. Asking a writer for a
 * new flush buffer borrows nothing (it clears the one it holds); with the buffer exchanged instead,
 * the request returns a different buffer. A publication of 16,384 objects under the import bound
 * rotates 6 storage epochs and borrows 3 flush buffers (the writer's, its one append buffer, the
 * successor writer's) and 91 serialization buffers; with an append buffer per epoch it borrows 8.
 * Writing 24 objects whose strings exceed the record limit and widening their numbers borrows 30
 * page workspaces and 22 codec stagings and leaves none lent; reading the resource cold borrows 3
 * codec stagings (one per record page decoded) and leaves none lent. With the decoder's workspace
 * no longer returning its buffers at the end of a page, the thread keeps them from its first page
 * on and the cold read borrows 0. A parallel import of 24,000 records (5.6 MiB, 300 KiB chunks)
 * borrows 37 slabs of 256 KiB, built inline or by three builders, and leaves none lent; so do three
 * loads that fail (a member no builder can parse, input that ends inside the array, a coordinator
 * that fails holding a chunk). Left lent when one owner stops returning: 37 by the builders, 10 by
 * the coordinator's drain of the handoff queue, 2 by its unwinding of builds that never ran, 1 by
 * the feeder for the chunk it was filling.
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
                   .assertExactly(EngineWorkCounters.SCRATCH_WRITE_IO_BORROWS, 3L * COMMITS,
                       "a commit's writer borrows one window to validate the frontier it appends at, one buffer "
                           + "for its revision record and one for the revision-record ring it writes to both "
                           + "beacon slots; more means a buffer per slot, per write or per hash refill")
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

  @Test
  void overflowCarriersAndFieldResizesBorrowTheirWorkspaces() throws Exception {
    final Path databasePath = JsonTestHelper.PATHS.PATH1.getFile();
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE).storageType(StorageType.FILE_CHANNEL).build());
      try (final JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
        final String large = "x".repeat(6_000);
        final WorkReport write = CAPTURE.run(() -> {
          try (final JsonNodeTrx trx = session.beginNodeTrx()) {
            final long arrayKey = trx.insertArrayAsFirstChild().getNodeKey();
            for (int i = 0; i < 24; i++) {
              assertTrue(trx.moveTo(arrayKey));
              trx.insertSubtreeAsLastChild(
                  JsonShredder.createStringReader("{\"s\":\"" + large + i + "\",\"n\":" + i + "}"),
                  JsonNodeTrx.Commit.NO);
            }
            trx.commit();
            // Widen every number so its field no longer fits where it was written.
            assertTrue(trx.moveTo(arrayKey) && trx.moveToFirstChild());
            do {
              final long object = trx.getNodeKey();
              assertTrue(trx.moveToLastChild(), "the number field");
              trx.setNumberValue(1_000_000_000_000L + object);
              assertTrue(trx.moveTo(object));
            } while (trx.moveToRightSibling());
            trx.commit();
          }
        });
        write.assertAtLeast(EngineWorkCounters.SCRATCH_PAGE_BORROWS, 24,
            "each of the 24 strings beyond the record limit is installed through a borrowed side-image "
                + "workspace; fewer means the fixture no longer reaches that path")
             .assertAtLeast(EngineWorkCounters.SCRATCH_CODEC_BORROWS, 1,
                 "a commit assembles its page bodies in borrowed staging")
             .assertZero(EngineWorkCounters.SCRATCH_BUFFERS_LENT,
                 "the transaction left scratch lent: a side image, a resize workspace, a placement buffer or a "
                     + "body staging was not returned")
             .assertZero(EngineWorkCounters.SCRATCH_REFUSALS, "the budget refused a workspace")
             .assertZero(EngineWorkCounters.SCRATCH_STALE_RETURNS, "a workspace was returned twice");
      }
    }
    Databases.clearGlobalCaches();
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
      final WorkReport coldRead = CAPTURE.run(() -> assertEquals(24, countObjects(session)));
      coldRead.assertBetween(EngineWorkCounters.SCRATCH_CODEC_BORROWS, 1, 8,
          "decoding a record page borrows its body staging once; more means staging is borrowed per record "
              + "or per section")
              .assertZero(EngineWorkCounters.SCRATCH_BUFFERS_LENT,
                  "a decoded page kept its staging: the thread that read it still holds allocator memory")
              .assertZero(EngineWorkCounters.SCRATCH_BUFFERS_CREATED,
                  "the cold read made the allocator back a fresh buffer although the commits left resident ones")
              .assertZero(EngineWorkCounters.SCRATCH_STALE_RETURNS, "a staging buffer was returned twice");
    }
  }

  @Test
  void aParallelImportBorrowsItsChunksInSlabsAndReturnsThemAlsoWhenItFails() throws Exception {
    final Path databasePath = JsonTestHelper.PATHS.PATH1.getFile();
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    final byte[] document = importDocument(-1);
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      // Inline (the coordinator builds each chunk itself) and through a pool of builders.
      for (final int builders : new int[] {1, 3}) {
        final String resource = RESOURCE + "-import-" + builders;
        createImportResource(database, resource);
        try (final JsonResourceSession session = database.beginResourceSession(resource)) {
          final WorkReport load = CAPTURE.run(() -> {
            try (final JsonNodeTrx trx = session.beginNodeTrx()) {
              ParallelBulkJsonImporter.assembleBytes(trx, new ByteArrayInputStream(document), IMPORT_CHUNK_BYTES,
                  builders);
              trx.commit();
            }
          });
          load.assertExactly(EngineWorkCounters.SCRATCH_IMPORT_BORROWS, IMPORT_SLABS,
              "a chunk is handed to its builder in 256 KiB slabs, borrowed as the scanner fills it: this "
                  + "document cuts into 18 chunks of two slabs and a last one of one; more means smaller slabs "
                  + "or a second copy of the chunk")
              .assertZero(EngineWorkCounters.SCRATCH_BUFFERS_LENT,
                  "the import left scratch lent: a builder, the coordinator or the scanner kept a chunk's slabs")
              .assertZero(EngineWorkCounters.SCRATCH_REFUSALS, "the budget refused a slab")
              .assertZero(EngineWorkCounters.SCRATCH_STALE_RETURNS, "a slab was returned twice");
          assertEquals(IMPORT_RECORDS, countObjects(session), builders + " builders");
        }
      }

      // A member the scanner slices but no builder can parse: the load fails with chunks queued for
      // the coordinator, chunks waiting for a builder and the scanner in the middle of the next one.
      final byte[] unparsable = importDocument(IMPORT_RECORDS / 3);
      // A document that ends inside its array: the scanner itself fails, holding the chunk it fills.
      final byte[] truncated = Arrays.copyOf(document, document.length * 2 / 3);
      // And a coordinator that fails on the chunk it has just taken, before any build has it.
      final int[] chunksTaken = new int[1];
      final Runnable failOnTheSixthChunk = () -> {
        if (++chunksTaken[0] == 6) {
          throw new IllegalStateException("the coordinator failed with a chunk in hand");
        }
      };
      final byte[][] inputs = {unparsable, truncated, document};
      final Runnable[] hooks = {null, null, failOnTheSixthChunk};
      for (int failure = 0; failure < inputs.length; failure++) {
        final byte[] input = inputs[failure];
        final String resource = RESOURCE + "-import-failed-" + failure;
        createImportResource(database, resource);
        final Runnable previousHook = ParallelBulkJsonImporter.setChunkTakenHookForTesting(hooks[failure]);
        try (final JsonResourceSession session = database.beginResourceSession(resource)) {
          final WorkReport failed = CAPTURE.run(() -> {
            try (final JsonNodeTrx trx = session.beginNodeTrx()) {
              assertThrows(RuntimeException.class, () -> ParallelBulkJsonImporter.assembleBytes(trx,
                  new ByteArrayInputStream(input), IMPORT_CHUNK_BYTES, 3));
              trx.rollback();
            }
          });
          failed.assertAtLeast(EngineWorkCounters.SCRATCH_IMPORT_BORROWS, 2,
              "the failed load had borrowed slabs; a zero here means the fixture failed before it scanned")
                .assertZero(EngineWorkCounters.SCRATCH_BUFFERS_LENT,
                    "the failed load left scratch lent: the slabs of a chunk nobody built were not returned")
                .assertZero(EngineWorkCounters.SCRATCH_STALE_RETURNS, "a slab was returned twice");
        } finally {
          ParallelBulkJsonImporter.setChunkTakenHookForTesting(previousHook);
        }
      }
      assertEquals(6, chunksTaken[0], "the coordinator's own failure must have been the one that ended the last load");
    }
  }

  private static final int IMPORT_RECORDS = 24_000;

  /** Small enough that the document cuts into many chunks, large enough that a chunk is two slabs. */
  private static final int IMPORT_CHUNK_BYTES = 300 << 10;

  /**
   * Measured: {@link #importDocument} is 5.6 MiB, which the scanner cuts at that budget into 18
   * chunks of 300 KiB and the rest of one member (two slabs each) and a last chunk of 180 KiB (one
   * slab).
   */
  private static final int IMPORT_SLABS = 18 * 2 + 1;

  private static void createImportResource(final Database<JsonResourceSession> database, final String resource) {
    database.createResource(ResourceConfiguration.newBuilder(resource)
                                                 .storageType(StorageType.FILE_CHANNEL)
                                                 .useDeweyIDs(false)
                                                 .hashKind(HashType.NONE)
                                                 .storeNodeHistory(false)
                                                 .build());
  }

  /**
   * A top-level array of {@link #IMPORT_RECORDS} objects of 250 bytes each.
   *
   * @param unparsableRecord the record whose number is not JSON ({@code -1}: none). The scanner
   *        slices by structure and passes it; the builder that tokenizes it refuses.
   */
  private static byte[] importDocument(final int unparsableRecord) {
    final StringBuilder json = new StringBuilder(IMPORT_RECORDS * 256);
    final String text = "t".repeat(200);
    json.append('[');
    for (int i = 0; i < IMPORT_RECORDS; i++) {
      if (i > 0) {
        json.append(',');
      }
      json.append("{\"id\":");
      if (i == unparsableRecord) {
        json.append("12x4567");
      } else {
        json.append(1_000_000 + i);
      }
      json.append(",\"text\":\"").append(text).append(i % 10).append("\",\"flag\":").append(i % 2 == 0).append('}');
    }
    json.append(']');
    return json.toString().getBytes(StandardCharsets.UTF_8);
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
