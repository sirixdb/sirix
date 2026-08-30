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
import io.sirix.index.hot.PathKeySerializer;
import io.sirix.page.HOTIndirectPage;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.PageReference;
import io.sirix.page.interfaces.Page;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end regression for a complete projection HOT split dump at a fresh-page version boundary.
 *
 * <p>
 * No page, page key, page reference, fragment list, or transaction-log entry is fabricated by this
 * test. All three revisions use the public
 * {@link ProjectionIndexHOTStorage#putColumnSegmentSlot(long, byte[])} production path: revision 1
 * fills a leaf and then branches an outlier into a second leaf, revision 2 inserts a missing slot
 * into the full non-root leaf so it splits, and revision 3 updates a retained slot. The shared
 * incremental split publishes fresh half pages: the resulting live left-page history is sparse r3
 * -&gt; complete r2, while r1 remains reachable only through revision 1's historical root. Keeping
 * the old source page out of the new half's fragment chain is intentional — it prevents pre-split
 * keys from being reconstructed into the fresh half.
 * </p>
 */
@DisplayName("Projection HOT complete-dump fragment boundary")
final class HOTCompleteDumpFragmentBoundaryTest {

  private static final String RESOURCE = "resource";
  private static final long RETAINED_LEFT_KEY = 0;
  private static final long SPLIT_INSERTED_KEY = 1;
  private static final long MOVED_RIGHT_KEY = 512;
  private static final long ROOT_SPLIT_OUTLIER_KEY = 1L << 40;
  private static final byte[] SMALL_SEGMENT = segment((byte) 1);
  private static final byte[] UPDATED_SEGMENT = segment((byte) 2);

  @TempDir
  Path temporaryDirectory;

  @ParameterizedTest(name = "{0}")
  @EnumSource(value = VersioningType.class, names = {"INCREMENTAL", "SLIDING_SNAPSHOT"})
  void ordinaryProjectionWritesReachTheCompleteDumpBoundary(final VersioningType versioningType) throws IOException {
    final Path databasePath = temporaryDirectory.resolve(versioningType.name().toLowerCase());
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));

    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                                   .versioningApproach(versioningType)
                                                   .maxNumberOfRevisionsToRestore(3)
                                                   .build());

      writeInitialIndirectTree(database);
      insertIntoFullNonRootLeaf(database);
      assertCompleteSplitFragmentWasPersisted(database);
      updateRetainedLeftLeaf(database);
    }

    Databases.getGlobalBufferManager().clearAllCaches();

    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        JsonResourceSession session = database.beginResourceSession(RESOURCE);
        JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
      final StorageEngineReader reader = rtx.getStorageEngineReader();
      final PageReference leftReference = leafReferenceForKey(reader, RETAINED_LEFT_KEY);
      assertEquals(1, leftReference.getPageFragments().size(),
          "r3 must retain the r2 complete split dump, but not chain in the old split source");

      final List<HOTLeafPage> fragments = reader.loadHOTLeafFragments(leftReference);
      try {
        assertEquals(2, fragments.size(), "the live durable chain must be sparse r3 and complete r2");
        final long leftPageKey = fragments.getFirst().getPageKey();
        for (final HOTLeafPage fragment : fragments) {
          assertEquals(leftPageKey, fragment.getPageKey(),
              "the normal writer must retain the fresh left-half page key across r2 and r3");
        }
        assertFalse(fragments.get(0).isCompleteDump(), "r3 must be a sparse update");
        assertTrue(fragments.get(1).isCompleteDump(), "r2 must be the complete non-root split dump");
        assertTrue(fragments.get(1).findEntry(key(MOVED_RIGHT_KEY)) < 0,
            "r2's retained left half must exclude the key moved to the new right page");
      } finally {
        reader.releaseHOTLeafFragments(fragments, null);
      }

      final HOTLeafPage reconstructed = assertInstanceOf(HOTLeafPage.class, reader.loadHOTPage(leftReference));
      assertTrue(reconstructed.findEntry(key(RETAINED_LEFT_KEY)) >= 0);
      assertTrue(reconstructed.findEntry(key(SPLIT_INSERTED_KEY)) >= 0);
      assertFalse(reconstructed.findEntry(key(MOVED_RIGHT_KEY)) >= 0,
          "the left page must not resurrect a key from before its complete split dump");

      // Historical isolation: r1 still sees the source leaf, r2 sees the fresh split halves, and
      // only r3 sees the replacement. This is the versioning contract the fresh page identities
      // preserve without carrying r1 into the new left half's fragment chain.
      try (JsonNodeReadOnlyTrx r1 = session.beginNodeReadOnlyTrx(1);
          JsonNodeReadOnlyTrx r2 = session.beginNodeReadOnlyTrx(2);
          JsonNodeReadOnlyTrx r3 = session.beginNodeReadOnlyTrx(3)) {
        final StorageEngineReader reader1 = r1.getStorageEngineReader();
        final StorageEngineReader reader2 = r2.getStorageEngineReader();
        final StorageEngineReader reader3 = r3.getStorageEngineReader();
        assertNotNull(ProjectionIndexHOTStorage.readColumnSegmentSlot(reader1, 0, MOVED_RIGHT_KEY));
        assertNull(ProjectionIndexHOTStorage.readColumnSegmentSlot(reader1, 0, SPLIT_INSERTED_KEY));
        assertNotNull(ProjectionIndexHOTStorage.readColumnSegmentSlot(reader2, 0, MOVED_RIGHT_KEY));
        assertNotNull(ProjectionIndexHOTStorage.readColumnSegmentSlot(reader2, 0, SPLIT_INSERTED_KEY));
        assertArrayEquals(SMALL_SEGMENT,
            ProjectionIndexHOTStorage.readColumnSegmentSlot(reader2, 0, RETAINED_LEFT_KEY));
        assertArrayEquals(UPDATED_SEGMENT,
            ProjectionIndexHOTStorage.readColumnSegmentSlot(reader3, 0, RETAINED_LEFT_KEY));
        assertTrue(ProjectionIndexHOTStorage.segmentPageOffset(reader2, 0, RETAINED_LEFT_KEY, 0) >= 0,
            "r2's complete split dump must carry the referenced segment side page");
        assertTrue(ProjectionIndexHOTStorage.segmentPageOffset(reader3, 0, RETAINED_LEFT_KEY, 0) >= 0,
            "r3's sparse update must carry its referenced segment side page");
      }
    }
  }

  private static void writeInitialIndirectTree(final Database<JsonResourceSession> database) {
    try (JsonResourceSession session = database.beginResourceSession(RESOURCE);
        JsonNodeTrx wtx = session.beginNodeTrx()) {
      final ProjectionIndexHOTStorage storage = new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), 0);
      for (long entry = 0; entry < HOTLeafPage.MAX_ENTRIES; entry++) {
        storage.putColumnSegmentSlot(entry * 2, SMALL_SEGMENT);
      }
      storage.putColumnSegmentSlot(ROOT_SPLIT_OUTLIER_KEY, SMALL_SEGMENT);
      wtx.commit();
    }
  }

  private static void insertIntoFullNonRootLeaf(final Database<JsonResourceSession> database) {
    try (JsonResourceSession session = database.beginResourceSession(RESOURCE);
        JsonNodeTrx wtx = session.beginNodeTrx()) {
      new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), 0).putColumnSegmentSlot(SPLIT_INSERTED_KEY,
          SMALL_SEGMENT);
      wtx.commit();
    }
  }

  private static void assertCompleteSplitFragmentWasPersisted(final Database<JsonResourceSession> database) {
    try (JsonResourceSession session = database.beginResourceSession(RESOURCE);
        JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
      final StorageEngineReader reader = rtx.getStorageEngineReader();
      final PageReference leftReference = leafReferenceForKey(reader, RETAINED_LEFT_KEY);
      assertEquals(0, leftReference.getPageFragments().size(),
          "a fresh incremental split half must not inherit the old source page's fragment chain");

      final List<HOTLeafPage> fragments = reader.loadHOTLeafFragments(leftReference);
      try {
        assertEquals(1, fragments.size());
        assertTrue(fragments.getFirst().isCompleteDump(), "the supported split must emit a complete fresh left half");
        assertTrue(fragments.get(0).findEntry(key(MOVED_RIGHT_KEY)) < 0);
      } finally {
        reader.releaseHOTLeafFragments(fragments, null);
      }
    }
  }

  private static void updateRetainedLeftLeaf(final Database<JsonResourceSession> database) {
    try (JsonResourceSession session = database.beginResourceSession(RESOURCE);
        JsonNodeTrx wtx = session.beginNodeTrx()) {
      new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), 0).putColumnSegmentSlot(RETAINED_LEFT_KEY,
          UPDATED_SEGMENT);
      wtx.commit();
    }
  }

  private static PageReference leafReferenceForKey(final StorageEngineReader reader, final long slotKey) {
    PageReference reference = ProjectionIndexHOTStorage.rootReference(reader, 0);
    assertNotNull(reference, "projection HOT root must exist");
    final byte[] serializedKey = key(slotKey);

    while (true) {
      final Page page = reader.loadHOTPage(reference);
      if (page instanceof HOTLeafPage leaf) {
        assertTrue(leaf.findEntry(serializedKey) >= 0, "routed leaf must contain slot key " + slotKey);
        return reference;
      }

      final HOTIndirectPage indirect = assertInstanceOf(HOTIndirectPage.class, page);
      final int childIndex = indirect.findChildIndex(serializedKey);
      assertTrue(childIndex >= 0, "indirect page must route slot key " + slotKey);
      reference = indirect.getChildReference(childIndex);
      assertNotNull(reference, "routed child reference must exist for slot key " + slotKey);
    }
  }

  private static byte[] key(final long slotKey) {
    final byte[] key = new byte[Long.BYTES];
    assertEquals(Long.BYTES, PathKeySerializer.INSTANCE.serialize(slotKey, key, 0));
    return key;
  }

  private static byte[] segment(final byte marker) {
    final byte[] segment = new byte[600];
    segment[0] = marker;
    return segment;
  }
}
