/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.page.HOTTrieReader;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.hot.PathKeySerializer;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.PageReference;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Regression coverage for opaque projection values aging through a sliding snapshot window. */
final class ProjectionSlidingSnapshotOpaqueValueTest {

  private static final String RESOURCE = "resource";
  private static final int INDEX_NUMBER = 0;
  private static final long OPAQUE_SLOT = 100L;
  private static final long DELETED_SLOT = 101L;
  private static final long CHURN_SLOT = 102L;
  private static final byte[] POSTING_TOMBSTONE_MARKER = {(byte) 0xFE};
  private static final byte[] PROJECTION_TOMBSTONE = new byte[0];

  @TempDir
  Path temporaryDirectory;

  @Test
  void liveOneByteFeAndZeroLengthTombstoneSurviveAColdSlidingWindowRotation() throws IOException {
    final Path databasePath = temporaryDirectory.resolve("projection-sliding-opaque");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                                   .versioningApproach(VersioningType.SLIDING_SNAPSHOT)
                                                   .maxNumberOfRevisionsToRestore(3)
                                                   .build());

      try (JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
        try (JsonNodeTrx wtx = session.beginNodeTrx()) {
          final ProjectionIndexHOTStorage storage =
              new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
          storage.writeSlotValue(OPAQUE_SLOT, POSTING_TOMBSTONE_MARKER);
          storage.writeSlotValue(DELETED_SLOT, new byte[] {0x44});
          storage.writeSlotValue(CHURN_SLOT, new byte[] {1});
          wtx.commit();
        }

        try (JsonNodeTrx wtx = session.beginNodeTrx()) {
          final ProjectionIndexHOTStorage storage =
              new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
          storage.writeSlotValue(DELETED_SLOT, PROJECTION_TOMBSTONE);
          storage.writeSlotValue(CHURN_SLOT, new byte[] {2});
          wtx.commit();
        }

        // Revision four is the first write with a full two-fragment chain. Its commit evicts
        // revision one's full dump, so OPAQUE_SLOT must be carried into the new sparse fragment.
        for (int revision = 3; revision <= 4; revision++) {
          try (JsonNodeTrx wtx = session.beginNodeTrx()) {
            final ProjectionIndexHOTStorage storage =
                new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
            storage.writeSlotValue(CHURN_SLOT, new byte[] {(byte) revision});
            wtx.commit();
          }
        }
      }
    }

    // Force every assertion through durable fragment loading; no writer swizzle or global cache may
    // hide a missing carry-forward entry.
    Databases.getGlobalBufferManager().clearAllCaches();
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        JsonResourceSession session = database.beginResourceSession(RESOURCE);
        JsonNodeReadOnlyTrx revisionTwo = session.beginNodeReadOnlyTrx(2);
        JsonNodeReadOnlyTrx revisionFour = session.beginNodeReadOnlyTrx(4)) {
      assertArrayEquals(PROJECTION_TOMBSTONE, readStoredSlot(revisionTwo.getStorageEngineReader(), DELETED_SLOT),
          "a zero-length projection value must remain a physically present tombstone");
      assertArrayEquals(POSTING_TOMBSTONE_MARKER, readStoredSlot(revisionFour.getStorageEngineReader(), OPAQUE_SLOT),
          "opaque projection 0xFE must not be classified as a posting-index tombstone while aging");
      assertArrayEquals(PROJECTION_TOMBSTONE, readStoredSlot(revisionFour.getStorageEngineReader(), DELETED_SLOT),
          "the newer zero-length tombstone must continue to shadow the retired live value");
      assertArrayEquals(new byte[] {4}, readStoredSlot(revisionFour.getStorageEngineReader(), CHURN_SLOT));
    } finally {
      Databases.getGlobalBufferManager().clearAllCaches();
    }
  }

  /**
   * Read a physically present value exactly, preserving zero length instead of mapping it to null.
   */
  private static byte[] readStoredSlot(final StorageEngineReader reader, final long slotKey) {
    final PageReference root = ProjectionIndexHOTStorage.rootReference(reader, INDEX_NUMBER);
    assertNotNull(root);
    final byte[] key = new byte[Long.BYTES];
    PathKeySerializer.INSTANCE.serialize(slotKey, key, 0);
    try (HOTTrieReader trieReader = new HOTTrieReader(reader)) {
      final HOTLeafPage leaf = trieReader.navigateToLeaf(root, key);
      assertNotNull(leaf, "slot " + slotKey + " must route to a leaf");
      final int index = leaf.findEntry(key);
      assertTrue(index >= 0, "slot " + slotKey + " must remain physically present");
      final long valueRef = leaf.valueRef(index);
      final int valueLength = HOTLeafPage.refLength(valueRef);
      assertTrue(valueLength >= 0, "slot " + slotKey + " must have a readable packed value");
      final byte[] value = new byte[valueLength];
      if (valueLength > 0) {
        leaf.copyRefInto(valueRef, 0, value, 0, valueLength);
      }
      return value;
    }
  }
}
