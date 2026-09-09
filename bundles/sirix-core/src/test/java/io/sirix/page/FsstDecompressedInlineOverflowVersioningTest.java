/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.access.trx.node.json.objectvalue.BooleanValue;
import io.sirix.access.trx.node.json.objectvalue.NullValue;
import io.sirix.access.trx.node.json.objectvalue.StringValue;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.cache.IndexLogKey;
import io.sirix.index.IndexType;
import io.sirix.io.StorageType;
import io.sirix.node.ByteArrayBytesIn;
import io.sirix.node.MemorySegmentBytesOut;
import io.sirix.node.NodeKind;
import io.sirix.node.interfaces.DataRecord;
import io.sirix.node.json.ObjectNamedStringNode;
import io.sirix.page.interfaces.Page;
import io.sirix.settings.Constants;
import io.sirix.settings.StringCompressionType;
import io.sirix.settings.VersioningType;
import io.sirix.utils.FSSTCompressor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Covers the threshold transition that is unique to a multi-fragment FSST combine: a raw fused
 * named string is too large for an inline slot, revision-local FSST makes it inline-eligible, and
 * decoding that slot while assembling a later revision expands it back beyond the inline ceiling.
 * The expanded record must use the canonical generic OverflowPage carrier, including its exact
 * Dewey ID, rather than placing flyweight bytes in a generic-record side page.
 */
final class FsstDecompressedInlineOverflowVersioningTest {

  private static final String RESOURCE = "fsst-inline-merge-overflow";
  private static final String FSST_TOKEN = "sirixdb-versioned-fsst-inline-overflow-";
  private static final String TARGET_VALUE = FSST_TOKEN.repeat(72);
  private static final byte[] TARGET_BYTES = TARGET_VALUE.getBytes(StandardCharsets.UTF_8);
  private static final int TARGET_RECORD_PAGE_KEY = 1;

  @TempDir
  Path temporaryDirectory;

  @AfterEach
  void clearGlobalCaches() {
    Databases.clearGlobalCaches();
  }

  @ParameterizedTest(name = "{0}: decoded inline FSST slot becomes canonical overflow")
  @EnumSource(VersioningType.class)
  @Timeout(value = 2, unit = TimeUnit.MINUTES, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void decodedInlineSlotCrossesOverflowThreshold(final VersioningType versioningType) {
    final Path databasePath = temporaryDirectory.resolve(versioningType.name());
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));

    final Fixture fixture;
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      assertTrue(database.createResource(resourceConfiguration(versioningType)));
      try (final JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
        fixture = createCompressedInlineRevision(session);
        assertEquals(1, session.getMostRecentRevisionNumber());
        assertRevisionCarrier(session, 1, fixture, false, false, false);

        updateDisjointRecord(session, fixture.touchNodeKey());
        assertEquals(2, session.getMostRecentRevisionNumber());

        // Revision 1 must remain bit-for-bit readable after the later combine has expanded its slot.
        assertRevisionCarrier(session, 1, fixture, false, false, false);
        final boolean expectOverflow = versioningType != VersioningType.FULL;
        assertRevisionCarrier(session, 2, fixture, true, expectOverflow, expectOverflow);
      }
    }

    Databases.clearGlobalCaches();
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
      assertEquals(2, session.getMostRecentRevisionNumber());
      assertRevisionCarrier(session, 1, fixture, false, false, false);
      final boolean expectOverflow = versioningType != VersioningType.FULL;
      assertRevisionCarrier(session, 2, fixture, true, expectOverflow, expectOverflow);
    }
  }

  private static ResourceConfiguration resourceConfiguration(final VersioningType versioningType) {
    return ResourceConfiguration.newBuilder(RESOURCE)
                                .storeDiffs(false)
                                .storeNodeHistory(false)
                                .hashKind(HashType.NONE)
                                .buildPathSummary(false)
                                .useDeweyIDs(true)
                                .stringCompressionType(StringCompressionType.FSST)
                                .versioningApproach(versioningType)
                                .maxNumberOfRevisionsToRestore(3)
                                .storageType(StorageType.FILE_CHANNEL)
                                .build();
  }

  private static Fixture createCompressedInlineRevision(final JsonResourceSession session) {
    try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
      final long objectNodeKey = wtx.insertObjectAsFirstChild().getNodeKey();
      long lastNodeKey = objectNodeKey;

      for (int i = 0; i < FSSTCompressor.MIN_SAMPLES_FOR_TABLE; i++) {
        assertTrue(wtx.moveTo(objectNodeKey));
        lastNodeKey =
            wtx.insertObjectRecordAsLastChild("training-" + i, new StringValue(trainingValue(i))).getNodeKey();
      }

      // Record page zero already contains the document root in revision zero. DIFFERENTIAL and
      // INCREMENTAL therefore reconstruct it from revisions 1 + 0 even on the first user commit,
      // which would legitimately decode the target before this test's disjoint update. Advance to
      // a page with no older fragment so revision 1 proves the encoded-inline source shape and
      // revision 2 is the first multi-fragment reconstruction of the target's page.
      int padding = 0;
      while (lastNodeKey < Constants.NDP_NODE_COUNT - 1L) {
        assertTrue(wtx.moveTo(objectNodeKey));
        lastNodeKey = wtx.insertObjectRecordAsLastChild("padding-" + padding++, NullValue.INSTANCE).getNodeKey();
      }
      assertEquals(Constants.NDP_NODE_COUNT - 1L, lastNodeKey);

      assertTrue(wtx.moveTo(objectNodeKey));
      final long touchNodeKey = wtx.insertObjectRecordAsLastChild("touch", BooleanValue.FALSE).getNodeKey();

      assertTrue(wtx.moveTo(objectNodeKey));
      final long targetNodeKey =
          wtx.insertObjectRecordAsLastChild("target", new StringValue(TARGET_VALUE)).getNodeKey();
      assertEquals(NodeKind.OBJECT_NAMED_STRING, wtx.getKind());
      assertEquals(TARGET_RECORD_PAGE_KEY, targetNodeKey >> Constants.NDP_NODE_COUNT_EXPONENT);
      assertEquals(targetNodeKey >> Constants.NDP_NODE_COUNT_EXPONENT,
          touchNodeKey >> Constants.NDP_NODE_COUNT_EXPONENT,
          "the later mutation must be disjoint but reside on the target's record page");

      assertNotNull(wtx.getDeweyID());
      final byte[] deweyId = wtx.getDeweyID().toBytes();
      final ObjectNamedStringNode rawTarget = assertInstanceOf(ObjectNamedStringNode.class,
          wtx.getStorageEngineWriter().prepareRecordForModificationDocument(targetNodeKey)).toSnapshot();
      assertFalse(rawTarget.isCompressed(), "revision 1 must bootstrap FSST from a genuinely raw target");
      assertArrayEquals(TARGET_BYTES, rawTarget.getRawValueWithoutDecompression());
      assertArrayEquals(deweyId, rawTarget.getDeweyIDAsBytes());

      final int rawGenericBytes = serializeRecord(session.getResourceConfig(), rawTarget).length;
      final int rawFlyweightBytes = rawTarget.estimateSerializedSize();
      assertTrue(rawGenericBytes > PageConstants.MAX_RECORD_SIZE,
          "raw generic record must exceed the inline ceiling: " + rawGenericBytes);
      assertTrue(rawFlyweightBytes > PageConstants.MAX_RECORD_SIZE,
          "raw flyweight record must exceed the inline ceiling: " + rawFlyweightBytes);

      wtx.commit();
      return new Fixture(targetNodeKey, touchNodeKey, deweyId, rawGenericBytes, rawFlyweightBytes);
    }
  }

  private static String trainingValue(final int sample) {
    return FSST_TOKEN.repeat(6) + "sample-" + sample;
  }

  private static byte[] serializeRecord(final ResourceConfiguration resourceConfig, final DataRecord record) {
    try (final MemorySegmentBytesOut output = new MemorySegmentBytesOut(256)) {
      resourceConfig.recordPersister.serialize(output, record, resourceConfig);
      return output.getDestination().toArray(ValueLayout.JAVA_BYTE);
    }
  }

  private static void updateDisjointRecord(final JsonResourceSession session, final long touchNodeKey) {
    try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
      assertTrue(wtx.moveTo(touchNodeKey));
      assertEquals(NodeKind.OBJECT_NAMED_BOOLEAN, wtx.getKind());
      wtx.setBooleanValue(true);
      wtx.commit();
    }
  }

  private static void assertRevisionCarrier(final JsonResourceSession session, final int revision,
      final Fixture fixture, final boolean expectedTouch, final boolean expectOverflow,
      final boolean expectFragmentChain) {
    try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision)) {
      assertTrue(rtx.moveTo(fixture.targetNodeKey()));
      assertEquals(NodeKind.OBJECT_NAMED_STRING, rtx.getKind());
      assertEquals("target", rtx.getName().getLocalName());
      assertEquals(TARGET_VALUE, rtx.getValue());
      assertNotNull(rtx.getDeweyID());
      assertArrayEquals(fixture.deweyId(), rtx.getDeweyID().toBytes());

      assertTrue(rtx.moveTo(fixture.touchNodeKey()));
      assertEquals(NodeKind.OBJECT_NAMED_BOOLEAN, rtx.getKind());
      assertEquals(expectedTouch, rtx.getBooleanValue());

      final StorageEngineReader reader = rtx.getStorageEngineReader();
      final long recordPageKey = reader.pageKey(fixture.targetNodeKey(), IndexType.DOCUMENT);
      final var loaded = reader.getRecordPage(new IndexLogKey(IndexType.DOCUMENT, recordPageKey, 0, revision));
      assertNotNull(loaded);
      if (expectFragmentChain) {
        assertFalse(loaded.reference().getPageFragments().isEmpty(),
            "the disjoint update must force a multi-fragment page reconstruction");
      }

      final KeyValueLeafPage page = (KeyValueLeafPage) loaded.page();
      final int slot = StorageEngineReader.recordPageOffset(fixture.targetNodeKey());
      assertArrayEquals(fixture.deweyId(), page.getDeweyIdAsByteArray(slot),
          "inline and overflow carriers must preserve the exact Dewey bytes");

      if (expectOverflow) {
        assertCanonicalOverflow(session, reader, page, slot, fixture);
      } else {
        assertCompressedInline(page, slot, fixture);
      }
    }
  }

  private static void assertCompressedInline(final KeyValueLeafPage page, final int slot, final Fixture fixture) {
    assertNotEquals(KeyValueLeafPage.NO_FSST_SYMBOL_TABLE_ID, page.getFsstSymbolTableId(),
        "the inline record must name the revision-local FSST table it uses");
    assertNotNull(page.getFsstSymbolTable());
    assertNotNull(page.getSlot(slot));
    assertTrue(page.hasSlottedPageSlot(fixture.targetNodeKey()));
    assertFalse(page.isFusedObjectNamedStringOverflowDescriptor(slot));
    assertTrue(page.isFusedObjectNamedStringValueCompressed(slot),
        "the raw >512-byte record must be genuinely FSST-compressed inline");
    assertNull(page.getPageReference(fixture.targetNodeKey()));

    final int recordBytes = PageLayout.getRecordOnlyLength(page.getSlottedPage(), slot);
    final int carrierBytes = PageLayout.getDirDataLength(page.getSlottedPage(), slot);
    assertTrue(recordBytes <= PageConstants.MAX_RECORD_SIZE,
        "compressed serialized record exceeds the inline ceiling: " + recordBytes);
    assertTrue(carrierBytes <= PageConstants.MAX_RECORD_SIZE,
        "compressed record plus Dewey metadata exceeds the inline ceiling: " + carrierBytes);
    assertTrue(page.readFusedObjectNamedStringStoredBytes(slot).length < TARGET_BYTES.length,
        "the persisted payload must be encoded, not merely associated with an FSST table");
  }

  private static void assertCanonicalOverflow(final JsonResourceSession session, final StorageEngineReader reader,
      final KeyValueLeafPage page, final int slot, final Fixture fixture) {
    assertNotNull(page.getSlot(slot), "overflow carrier must retain scan-visible fused metadata");
    assertTrue(page.isFusedObjectNamedStringOverflowDescriptor(slot));
    assertFalse(page.hasSlottedPageSlot(fixture.targetNodeKey()));

    final PageReference companion = page.getPageReference(fixture.targetNodeKey());
    assertNotNull(companion, "descriptor must retain its same-record-key OverflowPage reference");
    assertEquals(1,
        page.referenceEntrySet().stream().filter(entry -> entry.getKey() == fixture.targetNodeKey()).count());

    Page overflow = companion.getPage();
    if (overflow == null) {
      overflow = reader.getReader().read(companion, session.getResourceConfig());
    }
    final OverflowPage overflowPage = assertInstanceOf(OverflowPage.class, overflow);
    assertTrue(overflowPage.dataLength() > PageConstants.MAX_RECORD_SIZE,
        "decoded raw generic carrier must exceed 512 bytes (bootstrap generic=" + fixture.rawGenericBytes()
            + ", flyweight=" + fixture.rawFlyweightBytes() + ')');

    final DataRecord decoded =
        session.getResourceConfig().recordPersister.deserialize(new ByteArrayBytesIn(overflowPage.getDataBytes()),
            fixture.targetNodeKey(), page.getDeweyIdAsByteArray(slot), session.getResourceConfig());
    final ObjectNamedStringNode overflowNode = assertInstanceOf(ObjectNamedStringNode.class, decoded);
    assertFalse(overflowNode.isCompressed(),
        "generic OverflowPage data must be raw and independent of any record-page FSST table");
    assertArrayEquals(TARGET_BYTES, overflowNode.getRawValueWithoutDecompression());
    assertArrayEquals(fixture.deweyId(), overflowNode.getDeweyIDAsBytes());
  }

  private record Fixture(long targetNodeKey, long touchNodeKey, byte[] deweyId, int rawGenericBytes,
      int rawFlyweightBytes) {
  }
}
