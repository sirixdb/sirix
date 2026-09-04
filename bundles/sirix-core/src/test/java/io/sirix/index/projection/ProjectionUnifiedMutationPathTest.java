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
import io.sirix.index.hot.AbstractHOTIndexWriter;
import io.sirix.index.hot.HOTInvariantValidator;
import io.sirix.index.hot.PathKeySerializer;
import io.sirix.page.HOTIndirectPage;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.PageReference;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** End-to-end gate for projection's single incremental mutation driver. */
final class ProjectionUnifiedMutationPathTest {

  private static final String RESOURCE = "resource";
  private static final int INITIAL_SLOTS = 520;
  private static final long UPDATED_SLOT = 100;
  private static final long DELETED_SLOT = 200;
  private static final long INSERTED_SLOT = 10_000;
  private static final long TOMBSTONED_BLOOM_SLOT = 16;
  private static final byte[] INSERTED_VALUE = {(byte) 0xC1, 0x23, 0x45, (byte) 0xFE};
  private static final byte[] TOMBSTONE = new byte[0];

  @TempDir
  Path temporaryDirectory;

  @ParameterizedTest(name = "{0}")
  @EnumSource(VersioningType.class)
  void insertUpdateDeleteAndSplitStayIncrementalAcrossVersioningTypes(final VersioningType versioningType)
      throws IOException {
    final Path databasePath = temporaryDirectory.resolve(versioningType.name().toLowerCase());
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                                   .versioningApproach(versioningType)
                                                   .maxNumberOfRevisionsToRestore(4)
                                                   .build());
    }

    final long validationFailuresBefore = AbstractHOTIndexWriter.STRUCTURAL_VALIDATION_FAILURE.get();
    final byte[] updatedValue = new byte[40_000];
    Arrays.fill(updatedValue, (byte) 0xD3);
    updatedValue[0] = (byte) 0xC1; // deliberately not a NodeReferences wire value
    final byte[] initialBloomBlob = new byte[40_000];
    Arrays.fill(initialBloomBlob, (byte) 0xA7);

    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage = new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), 0);
        for (long slot = 0; slot < INITIAL_SLOTS; slot++) {
          if (slot != TOMBSTONED_BLOOM_SLOT) {
            storage.writeSlotValue(slot, initialValue(slot));
          }
        }
        // Slot 16 belongs to the canonical blob family. Initialize it as a valid referenced blob so
        // the later tombstone exercises the real marker+side-page route.
        storage.putBlob(TOMBSTONED_BLOOM_SLOT, initialBloomBlob);
        wtx.commit();
      }

      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage = new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), 0);
        storage.writeSlotValue(UPDATED_SLOT, updatedValue);
        storage.writeSlotValue(DELETED_SLOT, TOMBSTONE);
        storage.writeSlotValue(INSERTED_SLOT, INSERTED_VALUE);

        // Presence-sensitive tombstone: the referenced canonical blob must lose both its marker and
        // its side page through the same incremental mutation driver.
        assertNotNull(storage.getSegmentPageBytes(TOMBSTONED_BLOOM_SLOT, 0));
        storage.tombstoneBlob(TOMBSTONED_BLOOM_SLOT);
        assertNull(storage.getSegmentPageBytes(TOMBSTONED_BLOOM_SLOT, 0));
        wtx.commit();
      }
    }

    assertEquals(validationFailuresBefore, AbstractHOTIndexWriter.STRUCTURAL_VALIDATION_FAILURE.get(),
        "ordinary projection insert/update/delete must publish only invariant-clean frontiers");

    Databases.getGlobalBufferManager().clearAllCaches();
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        JsonResourceSession session = database.beginResourceSession(RESOURCE);
        JsonNodeReadOnlyTrx revisionOne = session.beginNodeReadOnlyTrx(1);
        JsonNodeReadOnlyTrx revisionTwo = session.beginNodeReadOnlyTrx(2)) {
      final StorageEngineReader readerOne = revisionOne.getStorageEngineReader();
      final StorageEngineReader readerTwo = revisionTwo.getStorageEngineReader();

      assertArrayEquals(initialValue(UPDATED_SLOT), readRawSlot(readerOne, UPDATED_SLOT));
      assertArrayEquals(initialValue(DELETED_SLOT), readRawSlot(readerOne, DELETED_SLOT));
      assertArrayEquals(initialBloomBlob, ProjectionIndexHOTStorage.readBlob(readerOne, 0, TOMBSTONED_BLOOM_SLOT));
      assertNull(readRawSlot(readerOne, INSERTED_SLOT));

      assertArrayEquals(updatedValue, readRawSlot(readerTwo, UPDATED_SLOT),
          "a grow-overwrite that forces another local split must remain byte-exact");
      assertArrayEquals(TOMBSTONE, readRawSlot(readerTwo, DELETED_SLOT),
          "zero bytes are a physically present projection tombstone, not an unreadable value");
      assertArrayEquals(TOMBSTONE, readRawSlot(readerTwo, TOMBSTONED_BLOOM_SLOT),
          "presence-sensitive cleanup must retain the tombstone while dropping its side page");
      assertArrayEquals(INSERTED_VALUE, readRawSlot(readerTwo, INSERTED_SLOT));

      final PageReference root = ProjectionIndexHOTStorage.rootReference(readerTwo, 0);
      assertInstanceOf(HOTIndirectPage.class, readerTwo.loadHOTPage(root),
          "the fixture must exercise the shared structural split path");
      final HOTInvariantValidator.Result invariants = HOTInvariantValidator.validate(root, readerTwo);
      assertTrue(invariants.hardViolations().isEmpty(),
          "unified projection mutations must leave a valid HOT: " + invariants.hardViolations());
    } finally {
      Databases.getGlobalBufferManager().clearAllCaches();
    }
  }

  private static byte[] initialValue(final long slot) {
    final byte[] value = new byte[128];
    Arrays.fill(value, (byte) slot);
    value[0] = (byte) 0xC1; // opaque bytes: bitmap decoding would reject/corrupt this payload
    value[1] = (byte) slot;
    value[2] = (byte) (slot >>> 8);
    return value;
  }

  /** Read a raw projection slot while preserving a physically present zero-length value. */
  private static byte[] readRawSlot(final StorageEngineReader reader, final long slotKey) {
    final byte[] key = new byte[Long.BYTES];
    PathKeySerializer.INSTANCE.serialize(slotKey, key, 0);
    final PageReference root = ProjectionIndexHOTStorage.rootReference(reader, 0);
    try (HOTTrieReader trieReader = new HOTTrieReader(reader)) {
      final HOTLeafPage leaf = trieReader.navigateToLeaf(root, key);
      if (leaf == null) {
        return null;
      }
      final int index = leaf.findEntry(key);
      if (index < 0) {
        return null;
      }
      final long valueRef = leaf.valueRef(index);
      final int valueLength = HOTLeafPage.refLength(valueRef);
      assertTrue(valueLength >= 0, "the routed projection slot must be physically readable");
      final byte[] value = new byte[valueLength];
      if (valueLength > 0) {
        leaf.copyRefInto(valueRef, 0, value, 0, valueLength);
      }
      return value;
    }
  }
}
