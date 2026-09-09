/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.JsonTestHelper;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.access.trx.page.HOTTrieReader;
import io.sirix.index.hot.BulkSpliceTestBridge;
import io.sirix.index.hot.HOTBulkSlotLoader;
import io.sirix.index.hot.PathKeySerializer;
import io.sirix.page.PageReference;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Path;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * BOTH-ARMS WITNESS for bulk slot accumulation (campaign #76 phase 2): the same operation sequence
 * runs once with accumulation engaged and once on the plain per-entry path; the two storages must
 * be READ-IDENTICAL (in-transaction and after a real commit) while their construction counters
 * prove the two arms genuinely took different routes.
 *
 * <p>
 * Covers the contract corners individually: read-through of accumulated point keys,
 * last-writer-wins re-puts, tombstones, inline blobs, a REFERENCED blob whose side-page attach is
 * DEFERRED (read back through the pending map mid-accumulation, attached after the splice), and the
 * malformed-subtree oracle over the spliced tree.
 */
final class ProjectionBulkSlotAccumulationTest {

  private static final String RESOURCE_NAME = "bulk-slot-accumulation";
  private static final Path DATABASE_PATH = JsonTestHelper.PATHS.PATH1.getFile();

  private static final long LABEL_BASE = 1L << 50;
  private static final int LABELS = 50_000;
  private static final long META_SLOT = 0L;
  private static final long BIG_BLOB_SLOT = (1L << 42) + 7L;
  private static final int BIG_BLOB_BYTES = 200_000;

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
    Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH)) {
      db.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME).build());
    }
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
    Databases.getGlobalBufferManager().clearAllCaches();
  }

  @Test
  void bothArmsProduceIdenticalState() {
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final StorageEngineWriter sew = wtx.getStorageEngineWriter();
        final ProjectionIndexHOTStorage bulkArm = new ProjectionIndexHOTStorage(sew, 0);
        final ProjectionIndexHOTStorage plainArm = new ProjectionIndexHOTStorage(sew, 1);

        bulkArm.beginBulkSlotAccumulation();
        assertTrue(bulkArm.isBulkAccumulating(), "accumulation must engage on a virgin tree");

        runSequence(bulkArm);
        assertTrue(bulkArm.isBulkAccumulating(),
            "no trip may fire in this sequence — the big blob's side page must be DEFERRED");
        runSequence(plainArm);

        // In-transaction equality BEFORE the splice: the bulk arm serves accumulated state
        // read-through, so the two arms must already agree.
        compareInTransaction(bulkArm, plainArm);

        bulkArm.finalizeBulkSlotAccumulation();
        assertFalse(bulkArm.isBulkAccumulating(), "finalize must leave accumulation mode");

        // Witness that the arms took DIFFERENT construction routes.
        // Distinct spliced entries: LABELS label slots (one of them tombstoned in place —
        // still an entry) + the metadata blob + the big blob's marker.
        assertEquals(LABELS + 2, bulkArm.bulkSplicedEntryCount(),
            "bulk arm must have materialized exactly the accumulated distinct entries");
        assertEquals(0, plainArm.bulkSplicedEntryCount(), "plain arm must never bulk-splice");

        // Post-splice equality + structural oracle on both trees.
        compareInTransaction(bulkArm, plainArm);
        assertEquals(0, BulkSpliceTestBridge.malformedSubtreeCount(bulkArm), "bulk tree malformed");
        assertEquals(0, BulkSpliceTestBridge.malformedSubtreeCount(plainArm), "plain tree malformed");

        wtx.commit();
      }

      // Committed-state equality through the reader-side path — proves the spliced tree
      // survives the real commit identically to the per-entry tree.
      try (JsonNodeTrx probe = session.beginNodeTrx()) {
        final StorageEngineReader reader = probe.getStorageEngineReader();
        final Random sample = new Random(7L);
        for (int i = 0; i < 500; i++) {
          final long key = LABEL_BASE + sample.nextInt(LABELS);
          final byte[] fromBulk = committedSlot(reader, 0, key);
          final byte[] fromPlain = committedSlot(reader, 1, key);
          if (fromPlain == null) {
            assertNull(fromBulk, "committed presence mismatch at " + key);
          } else {
            assertArrayEquals(fromPlain, fromBulk, "committed payload mismatch at " + key);
          }
        }
        final byte[] bigFromBulk = ProjectionIndexHOTStorage.readSegmentPageBytes(reader, 0, BIG_BLOB_SLOT, 0);
        final byte[] bigFromPlain = ProjectionIndexHOTStorage.readSegmentPageBytes(reader, 1, BIG_BLOB_SLOT, 0);
        assertArrayEquals(bigFromPlain, bigFromBulk, "committed big-blob side page mismatch");
        assertArrayEquals(bigBlobPayload(), bigFromBulk, "committed big-blob content mismatch");
      }
    }
  }

  @Test
  void accumulationRefusesANonVirginTreeWithoutChangingIt() {
    final byte[] payload = {1, 2, 3, 4};
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
        JsonNodeTrx wtx = session.beginNodeTrx()) {
      final ProjectionIndexHOTStorage storage = new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), 0);
      storage.putBlob(META_SLOT, payload);

      assertThrows(IllegalStateException.class, storage::beginBulkSlotAccumulation,
          "bulk initialization must never become an alternate writer for populated state");
      assertFalse(storage.isBulkAccumulating());
      assertArrayEquals(payload, storage.getBlob(META_SLOT));
    }
  }

  /** Committed raw-slot read via trie navigation ({@code null} when absent/tombstoned). */
  private static byte @Nullable [] committedSlot(final StorageEngineReader reader, final int indexNumber,
      final long slotKey) {
    final PageReference root = ProjectionIndexHOTStorage.rootReference(reader, indexNumber);
    if (root == null) {
      return null;
    }
    final byte[] keyBytes = new byte[8];
    PathKeySerializer.INSTANCE.serialize(slotKey, keyBytes, 0);
    try (HOTTrieReader trieReader = new HOTTrieReader(reader)) {
      final MemorySegment slice = trieReader.get(root, keyBytes);
      return slice == null || slice.byteSize() == 0
          ? null
          : slice.toArray(ValueLayout.JAVA_BYTE);
    }
  }

  /** The shared operation sequence — every storage-contract corner the accumulator touches. */
  private static void runSequence(final ProjectionIndexHOTStorage storage) {
    final byte[] payload = new byte[30];
    for (int k = 0; k < LABELS; k++) {
      fillLabel(payload, k);
      storage.writeSlotValue(LABEL_BASE + k, payload);
      if (k % 100 == 50) {
        // Interleaved read of an earlier write — the order-label walk pattern (read-through).
        final byte[] expected = new byte[30];
        fillLabel(expected, k - 50);
        assertArrayEquals(expected, storage.getRawSlot(LABEL_BASE + k - 50),
            "mid-build read-back mismatch at " + (k - 50));
      }
    }
    // Last-writer-wins re-put.
    fillLabel(payload, 123_456);
    storage.writeSlotValue(LABEL_BASE + 17, payload);
    // Tombstone an accumulated label (write of the zero-length tombstone value).
    storage.writeSlotValue(LABEL_BASE + 33, new byte[0]);
    // Inline metadata blob.
    storage.putBlob(META_SLOT, metaPayload());
    // REFERENCED blob: marker in the slot, payload as a side OverflowPage (deferred while
    // accumulating), read back immediately.
    storage.putBlob(BIG_BLOB_SLOT, bigBlobPayload());
    assertArrayEquals(bigBlobPayload(), storage.getBlob(BIG_BLOB_SLOT), "big blob read-back");
  }

  private static void compareInTransaction(final ProjectionIndexHOTStorage bulkArm,
      final ProjectionIndexHOTStorage plainArm) {
    for (int k = 0; k < LABELS; k += 7) {
      final byte[] fromBulk = bulkArm.getRawSlot(LABEL_BASE + k);
      final byte[] fromPlain = plainArm.getRawSlot(LABEL_BASE + k);
      if (fromPlain == null) {
        assertNull(fromBulk, "presence mismatch at label " + k);
      } else {
        assertArrayEquals(fromPlain, fromBulk, "payload mismatch at label " + k);
      }
    }
    assertNull(bulkArm.getRawSlot(LABEL_BASE + 33), "tombstoned label must read absent (bulk)");
    assertNull(plainArm.getRawSlot(LABEL_BASE + 33), "tombstoned label must read absent (plain)");
    assertArrayEquals(plainArm.getBlob(META_SLOT), bulkArm.getBlob(META_SLOT), "metadata blob mismatch");
    assertArrayEquals(plainArm.getBlob(BIG_BLOB_SLOT), bulkArm.getBlob(BIG_BLOB_SLOT), "big blob mismatch");
  }

  private static void fillLabel(final byte[] payload, final int k) {
    for (int i = 0; i < payload.length; i++) {
      payload[i] = (byte) (k + i * 17);
    }
  }

  private static byte[] metaPayload() {
    final byte[] meta = new byte[100];
    for (int i = 0; i < meta.length; i++) {
      meta[i] = (byte) (i * 3);
    }
    return meta;
  }

  private static byte[] bigBlobPayload() {
    final byte[] big = new byte[BIG_BLOB_BYTES];
    for (int i = 0; i < big.length; i++) {
      big[i] = (byte) (i * 7);
    }
    return big;
  }

  /** Loader-level capacity contract: refusal at the entry cap, replace and membership semantics. */
  @Test
  void loaderCapacityAndFoldSemantics() {
    final HOTBulkSlotLoader loader = new HOTBulkSlotLoader(4, 1L << 20);
    assertTrue(loader.tryAdd(1L, new byte[] {1}));
    assertTrue(loader.tryAdd(2L, new byte[] {2}));
    assertTrue(loader.tryAdd(1L, new byte[] {9}), "re-put of an accumulated key must accumulate");
    assertTrue(loader.tryAdd(3L, new byte[0]), "zero-length (tombstone) payloads accumulate");
    assertFalse(loader.tryAdd(4L, new byte[] {4}), "the 5th write must refuse at maxEntries=4");
    assertTrue(loader.containsKey(1L));
    assertFalse(loader.containsKey(4L));
    assertArrayEquals(new byte[] {9}, loader.lastPayload(1L), "fold must keep the LAST payload");
    assertArrayEquals(new byte[0], loader.lastPayload(3L));
    assertNull(loader.lastPayload(4L), "a refused write must not be visible");
    assertEquals(4, loader.size());
    loader.clear();
    assertTrue(loader.isEmpty());
  }

  /**
   * Regression: a payload landing EXACTLY on the 1 MiB block boundary followed by a zero-length
   * (tombstone) payload. The block-full guard only rolled over when the next payload did not fit, and
   * a zero-length one "fits" at offset 1048576 — so the entry was recorded at an offset that no
   * longer fits the packed position's 20 offset bits and carried into the block INDEX, sending every
   * later read one block past the end of the arena.
   */
  @Test
  void aZeroLengthPayloadOnTheBlockBoundaryStaysAddressable() {
    final int blockBytes = 1 << 20;
    final int maxPayload = 65_535;
    final HOTBulkSlotLoader loader = new HOTBulkSlotLoader(1024, 8L << 20);

    // Fill the first block to EXACTLY its last byte.
    int filled = 0;
    long key = 0L;
    while (blockBytes - filled > maxPayload) {
      final byte[] payload = new byte[maxPayload];
      payload[0] = (byte) key;
      assertTrue(loader.tryAdd(key++, payload));
      filled += maxPayload;
    }
    final byte[] remainder = new byte[blockBytes - filled];
    if (remainder.length > 0) {
      remainder[0] = 42;
      assertTrue(loader.tryAdd(key++, remainder));
    }

    final long tombstoneKey = key++;
    assertTrue(loader.tryAdd(tombstoneKey, new byte[0]), "a tombstone must accumulate on the boundary");
    assertArrayEquals(new byte[0], loader.lastPayload(tombstoneKey),
        "the boundary tombstone must read back as a zero-length payload");

    // And the arena keeps working: the next real payload is addressable too.
    final long afterKey = key;
    final byte[] after = new byte[] {7, 8, 9};
    assertTrue(loader.tryAdd(afterKey, after));
    assertArrayEquals(after, loader.lastPayload(afterKey));
    if (remainder.length > 0) {
      assertArrayEquals(remainder, loader.lastPayload(afterKey - 2),
          "the payload that exactly closed the block must still read back");
    }
  }
}
