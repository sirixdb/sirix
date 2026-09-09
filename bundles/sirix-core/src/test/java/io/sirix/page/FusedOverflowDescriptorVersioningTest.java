/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.access.trx.node.json.objectvalue.StringValue;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.cache.IndexLogKey;
import io.sirix.index.IndexType;
import io.sirix.io.StorageType;
import io.sirix.node.NodeKind;
import io.sirix.settings.StringCompressionType;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end coverage for the two-part physical carrier used by an oversized fused
 * {@link NodeKind#OBJECT_NAMED_STRING}: a scan-visible inline descriptor and a same-record-key
 * {@link OverflowPage} reference. Both halves must be versioned and shadowed atomically.
 */
final class FusedOverflowDescriptorVersioningTest {

  private static final String RESOURCE = "fused-overflow-versioning";
  private static final String INLINE_A = "inline-a";
  private static final String OVERFLOW_B = "overflow-b-" + "b".repeat(PageConstants.MAX_RECORD_SIZE + 4_096);
  private static final String INLINE_C = "inline-c";
  private static final String TOUCH_0 = "touch-0";
  private static final String TOUCH_1 = "touch-1";
  private static final String TOUCH_2 = "touch-2";

  @TempDir
  Path temporaryDirectory;

  @AfterEach
  void clearGlobalCaches() {
    Databases.clearGlobalCaches();
  }

  @ParameterizedTest(name = "{0}, Dewey IDs={1}: fused carrier survives version reconstruction")
  @MethodSource("versioningConfigurations")
  @Timeout(value = 1, unit = TimeUnit.MINUTES, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void descriptorAndSameKeyReferenceAreVersionedAsOneCarrier(final VersioningType versioningType,
      final boolean useDeweyIDs) {
    final Path databasePath = temporaryDirectory.resolve(versioningType.name() + "-dewey-" + useDeweyIDs);
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));

    final long payloadNodeKey;
    final long touchNodeKey;
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      assertTrue(database.createResource(resourceConfiguration(versioningType, useDeweyIDs)));
      try (final JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
        final long[] nodeKeys = insertInitialRevision(session);
        payloadNodeKey = nodeKeys[0];
        touchNodeKey = nodeKeys[1];

        updateString(session, payloadNodeKey, OVERFLOW_B); // revision 2: inline -> descriptor + reference
        updateString(session, touchNodeKey, TOUCH_1); // revision 3: carry the untouched pair
        updateString(session, payloadNodeKey, INLINE_C); // revision 4: pair -> inline, shadow both halves
        updateString(session, touchNodeKey, TOUCH_2); // revision 5: carry inline without stale reference
        assertEquals(5, session.getMostRecentRevisionNumber());

        assertRevision(session, 1, payloadNodeKey, INLINE_A, false, touchNodeKey, TOUCH_0);
        assertRevision(session, 2, payloadNodeKey, OVERFLOW_B, true, touchNodeKey, TOUCH_0);
        assertRevision(session, 3, payloadNodeKey, OVERFLOW_B, true, touchNodeKey, TOUCH_1);
        assertRevision(session, 4, payloadNodeKey, INLINE_C, false, touchNodeKey, TOUCH_1);
        assertRevision(session, 5, payloadNodeKey, INLINE_C, false, touchNodeKey, TOUCH_2);
      }
    }

    // A restore window of two makes revisions 3-5 cross each non-FULL strategy's restore cadence.
    // Clearing process-wide caches ensures every historic assertion below reconstructs from disk.
    Databases.clearGlobalCaches();
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
      assertEquals(5, session.getMostRecentRevisionNumber());
      assertRevision(session, 1, payloadNodeKey, INLINE_A, false, touchNodeKey, TOUCH_0);
      assertRevision(session, 2, payloadNodeKey, OVERFLOW_B, true, touchNodeKey, TOUCH_0);
      assertRevision(session, 3, payloadNodeKey, OVERFLOW_B, true, touchNodeKey, TOUCH_1);
      assertRevision(session, 4, payloadNodeKey, INLINE_C, false, touchNodeKey, TOUCH_1);
      assertRevision(session, 5, payloadNodeKey, INLINE_C, false, touchNodeKey, TOUCH_2);
    }
  }

  private static Stream<Arguments> versioningConfigurations() {
    return Arrays.stream(VersioningType.values())
                 .flatMap(versioningType -> Stream.of(Arguments.of(versioningType, false),
                     Arguments.of(versioningType, true)));
  }

  private static ResourceConfiguration resourceConfiguration(final VersioningType versioningType,
      final boolean useDeweyIDs) {
    return ResourceConfiguration.newBuilder(RESOURCE)
                                .storeDiffs(false)
                                .hashKind(HashType.NONE)
                                .buildPathSummary(false)
                                .useDeweyIDs(useDeweyIDs)
                                .stringCompressionType(StringCompressionType.NONE)
                                .versioningApproach(versioningType)
                                .maxNumberOfRevisionsToRestore(2)
                                .storageType(StorageType.FILE_CHANNEL)
                                .build();
  }

  private static long[] insertInitialRevision(final JsonResourceSession session) {
    try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
      final long objectNodeKey = wtx.insertObjectAsFirstChild().getNodeKey();
      final long payloadNodeKey = wtx.insertObjectRecordAsFirstChild("payload", new StringValue(INLINE_A)).getNodeKey();
      assertEquals(NodeKind.OBJECT_NAMED_STRING, wtx.getKind());
      assertTrue(wtx.moveTo(objectNodeKey));
      final long touchNodeKey = wtx.insertObjectRecordAsFirstChild("touch", new StringValue(TOUCH_0)).getNodeKey();
      assertEquals(NodeKind.OBJECT_NAMED_STRING, wtx.getKind());
      wtx.commit();
      return new long[] {payloadNodeKey, touchNodeKey};
    }
  }

  private static void updateString(final JsonResourceSession session, final long nodeKey, final String value) {
    try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
      assertTrue(wtx.moveTo(nodeKey));
      assertEquals(NodeKind.OBJECT_NAMED_STRING, wtx.getKind());
      wtx.setStringValue(value);
      assertEquals(value, wtx.getValue());
      wtx.commit();
    }
  }

  private static void assertRevision(final JsonResourceSession session, final int revision, final long payloadNodeKey,
      final String expectedPayload, final boolean overflow, final long touchNodeKey, final String expectedTouch) {
    try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision)) {
      assertTrue(rtx.moveTo(payloadNodeKey));
      assertEquals(NodeKind.OBJECT_NAMED_STRING, rtx.getKind());
      assertEquals("payload", rtx.getName().getLocalName());
      assertEquals(expectedPayload, rtx.getValue());

      assertTrue(rtx.moveTo(touchNodeKey));
      assertEquals(NodeKind.OBJECT_NAMED_STRING, rtx.getKind());
      assertEquals("touch", rtx.getName().getLocalName());
      assertEquals(expectedTouch, rtx.getValue());

      final StorageEngineReader reader = rtx.getStorageEngineReader();
      final long payloadPageKey = reader.pageKey(payloadNodeKey, IndexType.DOCUMENT);
      assertEquals(payloadPageKey, reader.pageKey(touchNodeKey, IndexType.DOCUMENT),
          "the unrelated mutation must exercise the payload's record page");
      final var loaded = reader.getRecordPage(new IndexLogKey(IndexType.DOCUMENT, payloadPageKey, 0, revision));
      assertNotNull(loaded, "document record page must exist");
      final KeyValueLeafPage page = (KeyValueLeafPage) loaded.page();
      final int slot = StorageEngineReader.recordPageOffset(payloadNodeKey);
      assertNotNull(page.getSlot(slot), "fused records must retain scan-visible inline bytes");

      final long sameKeyReferenceCount =
          page.referenceEntrySet().stream().filter(entry -> entry.getKey() == payloadNodeKey).count();
      if (overflow) {
        assertTrue(page.isFusedObjectNamedStringOverflowDescriptor(slot),
            "oversized fused value must be represented by a metadata descriptor");
        assertFalse(page.hasSlottedPageSlot(payloadNodeKey),
            "a metadata descriptor must not masquerade as a complete inline record");
        assertNotNull(page.getPageReference(payloadNodeKey),
            "overflow descriptor must retain its same-record-key companion reference");
        assertEquals(1, sameKeyReferenceCount,
            "overflow descriptor must have exactly one same-record-key companion reference");
      } else {
        assertFalse(page.isFusedObjectNamedStringOverflowDescriptor(slot),
            "inline value must not retain overflow descriptor metadata");
        assertTrue(page.hasSlottedPageSlot(payloadNodeKey), "inline value must be a complete slot record");
        assertNull(page.getPageReference(payloadNodeKey),
            "inline value must shadow and remove any older same-record-key overflow reference");
        assertEquals(0, sameKeyReferenceCount, "inline value must not retain a same-record-key overflow reference");
      }
    }
  }
}
