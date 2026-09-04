/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.access.trx.node.json.objectvalue.BooleanValue;
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
import io.sirix.node.json.ObjectNamedBooleanNode;
import io.sirix.settings.Constants;
import io.sirix.settings.StringCompressionType;
import io.sirix.settings.VersioningType;
import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** End-to-end version reconstruction coverage for capacity-spilled logical slot sidecars. */
final class OverflowSlotSidecarVersioningTest {

  private static final String RESOURCE = "overflow-sidecar-versioning";
  private static final String INLINE_A = "inline-a";
  private static final String OVERFLOW_B = "overflow-b-" + "b".repeat(PageConstants.MAX_RECORD_SIZE + 4_096);
  private static final String INLINE_C = "inline-c";
  private static final int PADDING_SLOT = Constants.NDP_NODE_COUNT - 1;
  private static final int PADDING_BYTES = 500;
  private static final int LAST_REVISION = 6;

  @TempDir
  Path temporaryDirectory;

  @AfterEach
  void clearGlobalCaches() {
    Databases.clearGlobalCaches();
  }

  @ParameterizedTest(name = "{0}, Dewey IDs={1}: sidecar carrier lifecycle")
  @MethodSource("versioningConfigurations")
  @Timeout(value = 2, unit = TimeUnit.MINUTES, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void capacitySidecarsDoNotResurrectAcrossFragments(final VersioningType versioningType, final boolean useDeweyIDs) {
    final Path databasePath = temporaryDirectory.resolve(versioningType.name() + "-dewey-" + useDeweyIDs);
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));

    final Fixture fixture;
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      assertTrue(database.createResource(resourceConfiguration(versioningType, useDeweyIDs)));
      try (final JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
        fixture = insertInlineRevision(session, useDeweyIDs);
        spillAtCapacity(session, fixture);
        replaceWithInlineValues(session, fixture);
        deletePayloads(session, fixture);
        updateTouch(session, fixture.touchNodeKey(), true);
        updateTouch(session, fixture.touchNodeKey(), false);
        assertEquals(LAST_REVISION, session.getMostRecentRevisionNumber());

        assertAllRevisions(session, fixture);
        assertCurrentRevision(session, fixture);
      }
    }

    Databases.clearGlobalCaches();
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
      assertEquals(LAST_REVISION, session.getMostRecentRevisionNumber());
      assertAllRevisions(session, fixture);
      assertCurrentRevision(session, fixture);
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
                                .storeNodeHistory(false)
                                .hashKind(HashType.NONE)
                                .buildPathSummary(false)
                                .useDeweyIDs(useDeweyIDs)
                                .stringCompressionType(StringCompressionType.NONE)
                                .versioningApproach(versioningType)
                                .maxNumberOfRevisionsToRestore(2)
                                .storageType(StorageType.FILE_CHANNEL)
                                .build();
  }

  private static Fixture insertInlineRevision(final JsonResourceSession session, final boolean useDeweyIDs) {
    try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
      final long objectNodeKey = wtx.insertObjectAsFirstChild().getNodeKey();

      final long completeNodeKey = wtx.insertObjectRecordAsFirstChild("complete", BooleanValue.FALSE).getNodeKey();
      final int completeNameKey = wtx.getNameKey();
      final long completePathNodeKey = wtx.getPathNodeKey();
      final byte[] completeDeweyId = deweyBytes(wtx, useDeweyIDs);

      assertTrue(wtx.moveTo(objectNodeKey));
      final long descriptorNodeKey =
          wtx.insertObjectRecordAsFirstChild("descriptor", new StringValue(INLINE_A)).getNodeKey();
      final int descriptorNameKey = wtx.getNameKey();
      final long descriptorPathNodeKey = wtx.getPathNodeKey();
      final byte[] descriptorDeweyId = deweyBytes(wtx, useDeweyIDs);

      assertTrue(wtx.moveTo(objectNodeKey));
      final long touchNodeKey = wtx.insertObjectRecordAsFirstChild("touch", BooleanValue.FALSE).getNodeKey();

      assertEquals(completeNodeKey >> Constants.NDP_NODE_COUNT_EXPONENT,
          descriptorNodeKey >> Constants.NDP_NODE_COUNT_EXPONENT);
      assertEquals(completeNodeKey >> Constants.NDP_NODE_COUNT_EXPONENT,
          touchNodeKey >> Constants.NDP_NODE_COUNT_EXPONENT);
      assertFalse(StorageEngineReader.recordPageOffset(completeNodeKey) == PADDING_SLOT);
      assertFalse(StorageEngineReader.recordPageOffset(descriptorNodeKey) == PADDING_SLOT);
      assertFalse(StorageEngineReader.recordPageOffset(touchNodeKey) == PADDING_SLOT);

      wtx.commit();
      return new Fixture(objectNodeKey, completeNodeKey, descriptorNodeKey, touchNodeKey, completeNameKey,
          descriptorNameKey, completePathNodeKey, descriptorPathNodeKey, completeDeweyId, descriptorDeweyId);
    }
  }

  private static byte[] deweyBytes(final JsonNodeTrx wtx, final boolean useDeweyIDs) {
    if (!useDeweyIDs) {
      assertNull(wtx.getDeweyID());
      return null;
    }
    assertNotNull(wtx.getDeweyID());
    return wtx.getDeweyID().toBytes();
  }

  private static void spillAtCapacity(final JsonResourceSession session, final Fixture fixture) {
    try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
      final ObjectNamedBooleanNode complete =
          (ObjectNamedBooleanNode) wtx.getStorageEngineWriter()
                                      .prepareRecordForModificationDocument(fixture.completeNodeKey());
      final KeyValueLeafPage page = complete.getOwnerPage();
      assertNotNull(page);
      final ObjectNamedBooleanNode completeSnapshot = complete.toSnapshot();

      saturateBumpTail(page);
      page.resizeRecord(completeSnapshot, fixture.completeNodeKey(), slot(fixture.completeNodeKey()));

      assertTrue(wtx.moveTo(fixture.descriptorNodeKey()));
      assertEquals(NodeKind.OBJECT_NAMED_STRING, wtx.getKind());
      wtx.setStringValue(OVERFLOW_B);
      assertEquals(OVERFLOW_B, wtx.getValue());
      wtx.commit();
    }
  }

  /**
   * Fill only the bump-allocation tail by repeatedly replacing one unreachable test slot. The live
   * footprint remains one tiny slot, so the next revision's normal version combine compacts the page
   * and can exercise the sidecar-to-inline transition without deleting hundreds of fixtures.
   */
  private static void saturateBumpTail(final KeyValueLeafPage page) {
    final long paddingNodeKey = (page.getPageKey() << Constants.NDP_NODE_COUNT_EXPONENT) + PADDING_SLOT;
    while (true) {
      final long offset = page.prepareHeapForDirectWriteOrOverflow(PADDING_BYTES, 0);
      if (offset == KeyValueLeafPage.DIRECT_WRITE_OVERFLOW) {
        break;
      }
      zero(page.getSlottedPage(), offset, PADDING_BYTES);
      page.completeDirectWrite(0, paddingNodeKey, PADDING_SLOT, PADDING_BYTES, null);
    }

    final int remaining =
        (int) (page.getSlottedPage().byteSize() - PageLayout.HEAP_START - PageLayout.getHeapEnd(page.getSlottedPage()));
    final int trailerBytes = page.areDeweyIDsStored()
        ? PageLayout.DEWEY_ID_TRAILER_SIZE
        : 0;
    final int finalRecordBytes = remaining - trailerBytes;
    if (finalRecordBytes >= 0 && remaining > 0) {
      final long offset = page.prepareHeapForDirectWriteOrOverflow(finalRecordBytes, 0);
      assertFalse(offset == KeyValueLeafPage.DIRECT_WRITE_OVERFLOW);
      zero(page.getSlottedPage(), offset, finalRecordBytes);
      page.completeDirectWrite(0, paddingNodeKey, PADDING_SLOT, finalRecordBytes, null);
    }

    assertEquals(KeyValueLeafPage.MAX_SLOTTED_PAGE_CAPACITY, page.getSlottedPage().byteSize());
    assertEquals(KeyValueLeafPage.DIRECT_WRITE_OVERFLOW, page.prepareHeapForDirectWriteOrOverflow(1, 0));
  }

  private static void zero(final MemorySegment page, final long offset, final int length) {
    if (length > 0) {
      page.asSlice(offset, length).fill((byte) 0);
    }
  }

  private static void replaceWithInlineValues(final JsonResourceSession session, final Fixture fixture) {
    try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
      assertTrue(wtx.moveTo(fixture.completeNodeKey()));
      assertEquals(NodeKind.OBJECT_NAMED_BOOLEAN, wtx.getKind());
      wtx.setBooleanValue(true);

      assertTrue(wtx.moveTo(fixture.descriptorNodeKey()));
      assertEquals(NodeKind.OBJECT_NAMED_STRING, wtx.getKind());
      wtx.setStringValue(INLINE_C);
      wtx.commit();
    }
  }

  private static void deletePayloads(final JsonResourceSession session, final Fixture fixture) {
    try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
      assertTrue(wtx.moveTo(fixture.completeNodeKey()));
      wtx.remove();
      assertTrue(wtx.moveTo(fixture.descriptorNodeKey()));
      wtx.remove();
      wtx.commit();
    }
  }

  private static void updateTouch(final JsonResourceSession session, final long touchNodeKey, final boolean value) {
    try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
      assertTrue(wtx.moveTo(touchNodeKey));
      assertEquals(NodeKind.OBJECT_NAMED_BOOLEAN, wtx.getKind());
      wtx.setBooleanValue(value);
      wtx.commit();
    }
  }

  private static void assertAllRevisions(final JsonResourceSession session, final Fixture fixture) {
    assertRevision(session, 1, fixture, Shape.INLINE, false, INLINE_A, false);
    assertRevision(session, 2, fixture, Shape.SIDE, false, OVERFLOW_B, false);
    assertRevision(session, 3, fixture, Shape.INLINE, true, INLINE_C, false);
    assertRevision(session, 4, fixture, Shape.DELETED, false, null, false);
    assertRevision(session, 5, fixture, Shape.DELETED, false, null, true);
    assertRevision(session, 6, fixture, Shape.DELETED, false, null, false);
  }

  private static void assertCurrentRevision(final JsonResourceSession session, final Fixture fixture) {
    try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
      assertEquals(LAST_REVISION, rtx.getRevisionNumber());
      assertTransaction(session, rtx, fixture, Shape.DELETED, false, null, false);
    }
  }

  private static void assertRevision(final JsonResourceSession session, final int revision, final Fixture fixture,
      final Shape shape, final boolean expectedComplete, final String expectedDescriptor, final boolean expectedTouch) {
    try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision)) {
      assertTransaction(session, rtx, fixture, shape, expectedComplete, expectedDescriptor, expectedTouch);
    }
  }

  private static void assertTransaction(final JsonResourceSession session, final JsonNodeReadOnlyTrx rtx,
      final Fixture fixture, final Shape shape, final boolean expectedComplete, final String expectedDescriptor,
      final boolean expectedTouch) {
    if (shape == Shape.DELETED) {
      assertFalse(rtx.moveTo(fixture.completeNodeKey()), "deleted complete-image record resurrected");
      assertFalse(rtx.moveTo(fixture.descriptorNodeKey()), "deleted descriptor record resurrected");
    } else {
      assertTrue(rtx.moveTo(fixture.completeNodeKey()));
      assertEquals(NodeKind.OBJECT_NAMED_BOOLEAN, rtx.getKind());
      assertEquals("complete", rtx.getName().getLocalName());
      assertEquals(expectedComplete, rtx.getBooleanValue());
      assertDewey(rtx, fixture.completeDeweyId());

      assertTrue(rtx.moveTo(fixture.descriptorNodeKey()));
      assertEquals(NodeKind.OBJECT_NAMED_STRING, rtx.getKind());
      assertEquals("descriptor", rtx.getName().getLocalName());
      assertEquals(expectedDescriptor, rtx.getValue());
      assertDewey(rtx, fixture.descriptorDeweyId());
    }

    assertTrue(rtx.moveTo(fixture.touchNodeKey()));
    assertEquals(expectedTouch, rtx.getBooleanValue());
    assertPhysicalCarriers(session, rtx, fixture, shape, expectedComplete);
  }

  private static void assertDewey(final JsonNodeReadOnlyTrx rtx, final byte[] expected) {
    if (expected == null) {
      assertNull(rtx.getDeweyID());
    } else {
      assertNotNull(rtx.getDeweyID());
      assertArrayEquals(expected, rtx.getDeweyID().toBytes());
    }
  }

  private static void assertPhysicalCarriers(final JsonResourceSession session, final JsonNodeReadOnlyTrx rtx,
      final Fixture fixture, final Shape shape, final boolean expectedComplete) {
    final StorageEngineReader reader = rtx.getStorageEngineReader();
    final long recordPageKey = reader.pageKey(fixture.completeNodeKey(), IndexType.DOCUMENT);
    assertEquals(recordPageKey, reader.pageKey(fixture.descriptorNodeKey(), IndexType.DOCUMENT));
    final var loaded =
        reader.getRecordPage(new IndexLogKey(IndexType.DOCUMENT, recordPageKey, 0, rtx.getRevisionNumber()));
    assertNotNull(loaded);
    final KeyValueLeafPage page = (KeyValueLeafPage) loaded.page();
    final int completeSlot = slot(fixture.completeNodeKey());
    final int descriptorSlot = slot(fixture.descriptorNodeKey());

    if (shape == Shape.SIDE) {
      assertCompleteSideCarrier(page, fixture, completeSlot, expectedComplete);
      assertDescriptorSideCarrier(page, fixture, descriptorSlot);
      // DIFFERENTIAL/INCREMENTAL periodic full publication preserves other records after this
      // fixture has exhausted the row heap, so those records may legitimately acquire additional
      // capacity sidecars. Validate every such carrier rather than assuming only our two targets.
      assertTrue(page.getSideSlotCount() >= 2);
      assertEverySideSlotIsPairedAndDisjoint(page);
    } else if (shape == Shape.INLINE) {
      assertInlineCarrier(page, fixture.completeNodeKey(), completeSlot, NodeKind.OBJECT_NAMED_BOOLEAN,
          fixture.completeDeweyId());
      assertInlineCarrier(page, fixture.descriptorNodeKey(), descriptorSlot, NodeKind.OBJECT_NAMED_STRING,
          fixture.descriptorDeweyId());
      assertEverySideSlotIsPairedAndDisjoint(page);
    } else {
      assertRetiredCarrier(page, fixture.completeNodeKey(), completeSlot);
      assertRetiredCarrier(page, fixture.descriptorNodeKey(), descriptorSlot);
      assertEverySideSlotIsPairedAndDisjoint(page);
    }
  }

  private static void assertCompleteSideCarrier(final KeyValueLeafPage page, final Fixture fixture, final int slot,
      final boolean expectedValue) {
    assertSideCarrier(page, fixture.completeNodeKey(), slot, NodeKind.OBJECT_NAMED_BOOLEAN, fixture.completeDeweyId());
    assertEquals(fixture.objectNodeKey(), page.getSlotParentKey(slot));
    assertEquals(fixture.completeNameKey(), page.getObjectKeyNameKeyFromSlot(slot));
    assertEquals(fixture.completePathNodeKey(), page.getObjectKeyPathNodeKeyFromSlot(slot, fixture.completeNodeKey()));
    assertEquals(expectedValue, page.getFusedObjectNamedBooleanValueFromSlot(slot),
        "the complete side image must retain its value, not only projection metadata");
    assertContains(page.getObjectKeySlotsForNameKey(fixture.completeNameKey()), slot);
  }

  private static void assertDescriptorSideCarrier(final KeyValueLeafPage page, final Fixture fixture, final int slot) {
    assertSideCarrier(page, fixture.descriptorNodeKey(), slot, NodeKind.OBJECT_NAMED_STRING,
        fixture.descriptorDeweyId());
    assertTrue(page.isFusedObjectNamedStringOverflowDescriptor(slot));
    assertEquals(fixture.objectNodeKey(), page.getSlotParentKey(slot));
    assertEquals(fixture.descriptorNameKey(), page.getObjectKeyNameKeyFromSlot(slot));
    assertEquals(fixture.descriptorPathNodeKey(),
        page.getObjectKeyPathNodeKeyFromSlot(slot, fixture.descriptorNodeKey()));
    assertContains(page.getObjectKeySlotsForNameKey(fixture.descriptorNameKey()), slot);
  }

  private static void assertSideCarrier(final KeyValueLeafPage page, final long nodeKey, final int slot,
      final NodeKind kind, final byte[] expectedDeweyId) {
    assertTrue(page.hasSideSlot(slot));
    assertFalse(PageLayout.isSlotPopulated(page.getSlottedPage(), slot),
        "one logical record must never occupy both the inline heap and sidecar");
    assertNull(page.getSlot(slot), "sidecar bytes must not masquerade as inline row-heap bytes");
    assertNotNull(page.getSideSlotImage(slot));
    assertEquals(kind.getId(), page.getSideSlotNodeKindId(slot));
    assertEquals(kind.getId(), page.getSlotNodeKindId(slot));
    assertArrayEquals(expectedDeweyId, page.getDeweyIdAsByteArray(slot));

    final PageReference companion = page.getPageReference(nodeKey);
    assertNotNull(companion, "side image must retain its same-record-key overflow authority");
    assertEquals(1, sameKeyReferenceCount(page, nodeKey));
  }

  private static void assertInlineCarrier(final KeyValueLeafPage page, final long nodeKey, final int slot,
      final NodeKind kind, final byte[] expectedDeweyId) {
    assertFalse(page.hasSideSlot(slot));
    assertTrue(PageLayout.isSlotPopulated(page.getSlottedPage(), slot));
    assertNotNull(page.getSlot(slot));
    assertEquals(kind.getId(), page.getInlineSlotNodeKindId(slot));
    assertEquals(kind.getId(), page.getSlotNodeKindId(slot));
    assertTrue(page.hasSlottedPageSlot(nodeKey));
    assertNull(page.getPageReference(nodeKey), "inline replacement must retire the older overflow half");
    assertEquals(0, sameKeyReferenceCount(page, nodeKey));
    assertArrayEquals(expectedDeweyId, page.getDeweyIdAsByteArray(slot));
  }

  private static void assertRetiredCarrier(final KeyValueLeafPage page, final long nodeKey, final int slot) {
    assertFalse(page.hasSideSlot(slot), "delete must shadow the older side image");
    assertNull(page.getPageReference(nodeKey), "delete must shadow the older overflow reference");
    assertEquals(0, sameKeyReferenceCount(page, nodeKey));
  }

  private static void assertEverySideSlotIsPairedAndDisjoint(final KeyValueLeafPage page) {
    int visited = 0;
    for (int slot = 0; slot < Constants.NDP_NODE_COUNT; slot++) {
      if (!page.hasSideSlot(slot)) {
        continue;
      }
      visited++;
      assertFalse(PageLayout.isSlotPopulated(page.getSlottedPage(), slot),
          "side slot " + slot + " overlaps the inline heap");
      final long nodeKey = (page.getPageKey() << Constants.NDP_NODE_COUNT_EXPONENT) + slot;
      assertNotNull(page.getPageReference(nodeKey), "side slot " + slot + " has no same-key reference");
      assertEquals(1, sameKeyReferenceCount(page, nodeKey));
    }
    assertEquals(page.getSideSlotCount(), visited);
  }

  private static long sameKeyReferenceCount(final KeyValueLeafPage page, final long nodeKey) {
    return page.referenceEntrySet().stream().filter(entry -> entry.getKey() == nodeKey).count();
  }

  private static void assertContains(final int[] slots, final int expectedSlot) {
    assertTrue(Arrays.stream(slots).anyMatch(slot -> slot == expectedSlot));
  }

  private static int slot(final long nodeKey) {
    return StorageEngineReader.recordPageOffset(nodeKey);
  }

  private enum Shape {
    INLINE, SIDE, DELETED
  }

  private record Fixture(long objectNodeKey, long completeNodeKey, long descriptorNodeKey, long touchNodeKey,
      int completeNameKey, int descriptorNameKey, long completePathNodeKey, long descriptorPathNodeKey,
      byte[] completeDeweyId, byte[] descriptorDeweyId) {
  }
}
