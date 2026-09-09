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
import org.junit.jupiter.api.Test;
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
 * Regression coverage for an FSST-compressed fused string which is small enough for an inline slot,
 * but is forced out of line solely by a full record-page heap. The overflow payload must be
 * canonical raw data because a side page cannot depend on a revision-local FSST table.
 */
final class FsstDensePageCanonicalOverflowVersioningTest {

  private static final String RESOURCE = "fsst-dense-overflow";
  private static final String FSST_TOKEN = "sirixdb-canonical-overflow-dense-page-";
  private static final String TARGET_VALUE = FSST_TOKEN.repeat(72);
  private static final byte[] TARGET_BYTES = TARGET_VALUE.getBytes(StandardCharsets.UTF_8);
  // The bootstrap table is trained exclusively on ASCII. These UTF-8 bytes cannot match any
  // multi-byte table symbol, so each filler remains raw through both insert-time and commit-time
  // FSST attempts while still consuming a predictable ~400-byte inline record.
  private static final String DENSE_FILLER_VALUE = "界".repeat(110);
  private static final int TARGET_RECORD_PAGE_KEY = 1;
  private static final int MAX_DENSE_FILLERS = 900;
  private static final int MERGE_FILLER_COUNT = 700;
  private static final String MERGE_FILLER_VALUE = FSST_TOKEN.repeat(10);
  private static final String MERGE_TAIL_VALUE = "x".repeat(210);
  private static final String OLDER_TARGET_VALUE = FSST_TOKEN.repeat(72) + "older";
  private static final String NEWER_TARGET_VALUE = FSST_TOKEN.repeat(72) + "newer";

  @TempDir
  Path temporaryDirectory;

  @AfterEach
  void clearGlobalCaches() {
    Databases.clearGlobalCaches();
  }

  @ParameterizedTest(name = "{0}: dense-page FSST spill stays canonical across fragments")
  @EnumSource(VersioningType.class)
  @Timeout(value = 2, unit = TimeUnit.MINUTES, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void compressedInlineCandidateSpillsAsCanonicalRawOverflow(final VersioningType versioningType) {
    final Path databasePath = temporaryDirectory.resolve(versioningType.name());
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));

    final DenseSpillFixture fixture;
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      assertTrue(database.createResource(resourceConfiguration(versioningType)));
      try (final JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
        final BootstrapFixture bootstrap = bootstrapFsstTableAndAdvanceToNextRecordPage(session);
        assertRevisionFsstEngaged(session, bootstrap.trainingNodeKey(), bootstrap.trainingValue());

        fixture = createDensePageSpill(session, bootstrap.objectNodeKey());
        assertEquals(2, session.getMostRecentRevisionNumber());
        assertCanonicalCarrier(session, 2, fixture, false, false);

        updateDisjointTouchField(session, fixture.touchNodeKey());
        assertEquals(3, session.getMostRecentRevisionNumber());
        assertCanonicalCarrier(session, 2, fixture, false, false);
        assertCanonicalCarrier(session, 3, fixture, true, versioningType != VersioningType.FULL);
      }
    }

    Databases.clearGlobalCaches();
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        final JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
      assertEquals(3, session.getMostRecentRevisionNumber());
      assertCanonicalCarrier(session, 2, fixture, false, false);
      assertCanonicalCarrier(session, 3, fixture, true, versioningType != VersioningType.FULL);
    }
  }

  @Test
  @Timeout(value = 2, unit = TimeUnit.MINUTES, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void differentialLatestTargetOnlySpillShadowsOlderInlineInReadAndModificationCombines() {
    final Path databasePath = temporaryDirectory.resolve("differential-target-only-spill");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));

    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      assertTrue(database.createResource(differentialResourceConfiguration()));
      try (final JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
        final BootstrapFixture bootstrap = bootstrapFsstTableAndAdvanceToNextRecordPage(session);
        final LatestShadowFixture fixture = createCompressedInlineFullDump(session, bootstrap.objectNodeKey());
        assertEquals(2, session.getMostRecentRevisionNumber());
        assertLatestPhysicalFragmentIsInlineAndReferenceFree(session, 2, fixture.targetNodeKey());

        updateTarget(session, fixture.targetNodeKey(), NEWER_TARGET_VALUE);
        assertEquals(3, session.getMostRecentRevisionNumber());
        assertLatestPhysicalFragmentIsInlineAndReferenceFree(session, 3, fixture.targetNodeKey());

        // Read reconstruction copies the newest fragment first. Decompression exhausts the target
        // frame before this high-numbered slot, so the newest value acquires a reference which did
        // not exist on the source fragment. The older full-dump inline value must not overwrite it.
        assertNewestTargetSurvives(session, 3, fixture.targetNodeKey());

        // Preparing a disjoint record on the same page takes the independent modification-combine
        // path. Read-your-writes must still resolve the newest target-only side/reference carrier.
        updateTouchAndAssertTarget(session, fixture.touchNodeKey(), fixture.targetNodeKey());
        assertEquals(4, session.getMostRecentRevisionNumber());
        assertNewestTargetSurvives(session, 4, fixture.targetNodeKey());
      }
    }
  }

  private static ResourceConfiguration resourceConfiguration(final VersioningType versioningType) {
    return ResourceConfiguration.newBuilder(RESOURCE)
                                .storeDiffs(false)
                                .storeNodeHistory(false)
                                .hashKind(HashType.NONE)
                                .buildPathSummary(false)
                                .useDeweyIDs(false)
                                .stringCompressionType(StringCompressionType.FSST)
                                .versioningApproach(versioningType)
                                .maxNumberOfRevisionsToRestore(3)
                                .storageType(StorageType.FILE_CHANNEL)
                                .build();
  }

  private static ResourceConfiguration differentialResourceConfiguration() {
    return ResourceConfiguration.newBuilder(RESOURCE)
                                .storeDiffs(false)
                                .storeNodeHistory(false)
                                .hashKind(HashType.NONE)
                                .buildPathSummary(false)
                                .useDeweyIDs(false)
                                .stringCompressionType(StringCompressionType.FSST)
                                .versioningApproach(VersioningType.DIFFERENTIAL)
                                .maxNumberOfRevisionsToRestore(2)
                                .storageType(StorageType.FILE_CHANNEL)
                                .build();
  }

  private static BootstrapFixture bootstrapFsstTableAndAdvanceToNextRecordPage(final JsonResourceSession session) {
    try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
      final long objectNodeKey = wtx.insertObjectAsFirstChild().getNodeKey();
      long trainingNodeKey = Constants.NULL_ID_LONG;
      String firstTrainingValue = null;
      long lastNodeKey = objectNodeKey;

      for (int i = 0; i < FSSTCompressor.MIN_SAMPLES_FOR_TABLE; i++) {
        final String value = trainingValue(i);
        assertTrue(wtx.moveTo(objectNodeKey));
        lastNodeKey = wtx.insertObjectRecordAsFirstChild("training-" + i, new StringValue(value)).getNodeKey();
        if (i == 0) {
          trainingNodeKey = lastNodeKey;
          firstTrainingValue = value;
        }
      }

      int padding = 0;
      while (lastNodeKey < Constants.NDP_NODE_COUNT - 1L) {
        assertTrue(wtx.moveTo(objectNodeKey));
        lastNodeKey =
            wtx.insertObjectRecordAsFirstChild("page-zero-padding-" + padding++, NullValue.INSTANCE).getNodeKey();
      }
      assertEquals(Constants.NDP_NODE_COUNT - 1L, lastNodeKey,
          "the next document record must start a fresh record page");
      wtx.commit();
      return new BootstrapFixture(objectNodeKey, trainingNodeKey, firstTrainingValue);
    }
  }

  private static String trainingValue(final int sample) {
    return FSST_TOKEN.repeat(6) + "sample-" + sample;
  }

  private static LatestShadowFixture createCompressedInlineFullDump(final JsonResourceSession session,
      final long objectNodeKey) {
    try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
      assertTrue(wtx.moveTo(objectNodeKey));
      final long touchNodeKey = wtx.insertObjectRecordAsFirstChild("merge-touch", BooleanValue.FALSE).getNodeKey();
      assertEquals(TARGET_RECORD_PAGE_KEY, touchNodeKey >> Constants.NDP_NODE_COUNT_EXPONENT);

      for (int i = 0; i < MERGE_FILLER_COUNT; i++) {
        assertTrue(wtx.moveTo(objectNodeKey));
        final long fillerNodeKey =
            wtx.insertObjectRecordAsFirstChild("merge-filler-" + i, new StringValue(MERGE_FILLER_VALUE)).getNodeKey();
        assertEquals(TARGET_RECORD_PAGE_KEY, fillerNodeKey >> Constants.NDP_NODE_COUNT_EXPONENT,
            "merge fixture exhausted its target record page");
      }

      // The large reconstructed fillers leave a small tail which still fits the target's 33-byte
      // metadata descriptor. Consume that tail immediately before the target so the target itself,
      // not merely earlier fillers, must publish a side image plus a newly created reference.
      assertTrue(wtx.moveTo(objectNodeKey));
      final long tailNodeKey =
          wtx.insertObjectRecordAsFirstChild("merge-tail", new StringValue(MERGE_TAIL_VALUE)).getNodeKey();
      assertEquals(TARGET_RECORD_PAGE_KEY, tailNodeKey >> Constants.NDP_NODE_COUNT_EXPONENT);

      assertTrue(wtx.moveTo(objectNodeKey));
      final long targetNodeKey =
          wtx.insertObjectRecordAsFirstChild("merge-target", new StringValue(OLDER_TARGET_VALUE)).getNodeKey();
      assertEquals(TARGET_RECORD_PAGE_KEY, targetNodeKey >> Constants.NDP_NODE_COUNT_EXPONENT);

      final KeyValueLeafPage page = wtx.getStorageEngineWriter().getAllocKvl();
      final int targetSlot = StorageEngineReader.recordPageOffset(targetNodeKey);
      assertTrue(page.isFusedObjectNamedStringValueCompressed(targetSlot),
          "the inherited FSST table must keep the newest source slot inline");
      assertTrue(page.referenceEntrySet().isEmpty(), "the source fragment must not already own an overflow reference");
      wtx.commit();
      return new LatestShadowFixture(targetNodeKey, touchNodeKey);
    }
  }

  private static void updateTarget(final JsonResourceSession session, final long targetNodeKey, final String value) {
    try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
      assertTrue(wtx.moveTo(targetNodeKey));
      assertEquals(NodeKind.OBJECT_NAMED_STRING, wtx.getKind());
      wtx.setStringValue(value);
      wtx.commit();
    }
  }

  private static void updateTouchAndAssertTarget(final JsonResourceSession session, final long touchNodeKey,
      final long targetNodeKey) {
    try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
      assertTrue(wtx.moveTo(touchNodeKey));
      assertEquals(NodeKind.OBJECT_NAMED_BOOLEAN, wtx.getKind());
      wtx.setBooleanValue(true);

      assertTrue(wtx.moveTo(targetNodeKey));
      assertEquals(NodeKind.OBJECT_NAMED_STRING, wtx.getKind());
      assertEquals(NEWER_TARGET_VALUE, wtx.getValue(),
          "modification combine resurrected the older same-key inline value");
      wtx.commit();
    }
  }

  private static void assertLatestPhysicalFragmentIsInlineAndReferenceFree(final JsonResourceSession session,
      final int revision, final long targetNodeKey) {
    try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision)) {
      final StorageEngineReader reader = rtx.getStorageEngineReader();
      final long recordPageKey = reader.pageKey(targetNodeKey, IndexType.DOCUMENT);
      final var loaded = reader.getRecordPage(new IndexLogKey(IndexType.DOCUMENT, recordPageKey, 0, revision));
      assertNotNull(loaded);

      final PageReference physicalReference = new PageReference().setKey(loaded.reference().getKey());
      final KeyValueLeafPage physicalPage =
          (KeyValueLeafPage) reader.getReader().read(physicalReference, session.getResourceConfig());
      try {
        final int targetSlot = StorageEngineReader.recordPageOffset(targetNodeKey);
        assertTrue(physicalPage.hasSlottedPageSlot(targetNodeKey));
        assertTrue(physicalPage.isFusedObjectNamedStringValueCompressed(targetSlot));
        assertTrue(physicalPage.referenceEntrySet().isEmpty(),
            "the regression requires the newest physical fragment to have no source reference");
      } finally {
        physicalPage.close();
      }
    }
  }

  private static void assertNewestTargetSurvives(final JsonResourceSession session, final int revision,
      final long targetNodeKey) {
    try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision)) {
      assertTrue(rtx.moveTo(targetNodeKey));
      assertEquals(NEWER_TARGET_VALUE, rtx.getValue(), "read combine resurrected the older same-key inline value");

      final StorageEngineReader reader = rtx.getStorageEngineReader();
      final long recordPageKey = reader.pageKey(targetNodeKey, IndexType.DOCUMENT);
      final var loaded = reader.getRecordPage(new IndexLogKey(IndexType.DOCUMENT, recordPageKey, 0, revision));
      assertNotNull(loaded);
      final KeyValueLeafPage page = (KeyValueLeafPage) loaded.page();
      final int targetSlot = StorageEngineReader.recordPageOffset(targetNodeKey);
      assertTrue(page.hasSideSlot(targetSlot),
          "fixture did not force a target-only side/reference spill while copying the newest fragment: remaining="
              + remainingTailBytes(page) + ", inline=" + PageLayout.isSlotPopulated(page.getSlottedPage(), targetSlot)
              + ", inlineBytes=" + (PageLayout.isSlotPopulated(page.getSlottedPage(), targetSlot)
                  ? PageLayout.getDirDataLength(page.getSlottedPage(), targetSlot)
                  : 0)
              + ", sideCount=" + page.getSideSlotCount());
      assertFalse(PageLayout.isSlotPopulated(page.getSlottedPage(), targetSlot));
      assertNotNull(page.getPageReference(targetNodeKey));
    }
  }

  private static void assertRevisionFsstEngaged(final JsonResourceSession session, final long trainingNodeKey,
      final String expectedValue) {
    try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(1)) {
      assertTrue(rtx.moveTo(trainingNodeKey));
      assertEquals(expectedValue, rtx.getValue());

      final StorageEngineReader reader = rtx.getStorageEngineReader();
      final long recordPageKey = reader.pageKey(trainingNodeKey, IndexType.DOCUMENT);
      final var loaded =
          reader.getRecordPage(new IndexLogKey(IndexType.DOCUMENT, recordPageKey, 0, rtx.getRevisionNumber()));
      assertNotNull(loaded);
      final KeyValueLeafPage page = (KeyValueLeafPage) loaded.page();
      final int slot = StorageEngineReader.recordPageOffset(trainingNodeKey);
      assertNotEquals(KeyValueLeafPage.NO_FSST_SYMBOL_TABLE_ID, page.getFsstSymbolTableId(),
          "the bootstrap revision must persist a real FSST symbol table reference");
      assertNotNull(page.getFsstSymbolTable(), "the revision-local FSST table must be resolved");
      assertTrue(page.isFusedObjectNamedStringValueCompressed(slot),
          "the bootstrap sample must be stored in FSST form, not merely build an unused table");
      assertTrue(page.readFusedObjectNamedStringStoredBytes(slot).length < expectedValue.getBytes(
          StandardCharsets.UTF_8).length, "the stored bootstrap sample must be smaller than its raw UTF-8 value");
    }
  }

  private static DenseSpillFixture createDensePageSpill(final JsonResourceSession session, final long objectNodeKey) {
    try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
      assertTrue(wtx.moveTo(objectNodeKey));
      final long targetNodeKey =
          wtx.insertObjectRecordAsFirstChild("target", new StringValue(TARGET_VALUE)).getNodeKey();
      assertEquals(TARGET_RECORD_PAGE_KEY, targetNodeKey >> Constants.NDP_NODE_COUNT_EXPONENT);
      assertEquals(NodeKind.OBJECT_NAMED_STRING, wtx.getKind());

      assertTrue(wtx.moveTo(objectNodeKey));
      final long touchNodeKey = wtx.insertObjectRecordAsFirstChild("touch", BooleanValue.FALSE).getNodeKey();
      assertEquals(TARGET_RECORD_PAGE_KEY, touchNodeKey >> Constants.NDP_NODE_COUNT_EXPONENT);

      final var writer = wtx.getStorageEngineWriter();
      final KeyValueLeafPage page = writer.getAllocKvl();
      assertEquals(TARGET_RECORD_PAGE_KEY, page.getPageKey());
      final int targetSlot = StorageEngineReader.recordPageOffset(targetNodeKey);
      assertTrue(page.isFusedObjectNamedStringValueCompressed(targetSlot),
          "the existing revision FSST table must encode the target at insert time");
      assertNotEquals(KeyValueLeafPage.NO_FSST_SYMBOL_TABLE_ID, page.getFsstSymbolTableId());
      assertNotNull(page.getFsstSymbolTable());

      final ObjectNamedStringNode target = writer.prepareRecordForModificationDocument(targetNodeKey);
      final ObjectNamedStringNode compressedSnapshot = target.toSnapshot();
      assertTrue(compressedSnapshot.isCompressed());
      assertArrayEquals(TARGET_BYTES, compressedSnapshot.getRawValue());
      assertTrue(compressedSnapshot.getRawValueWithoutDecompression().length < TARGET_BYTES.length);

      final byte[] compressedWire = serializeRecord(session.getResourceConfig(), compressedSnapshot);
      final int compressedHeapEstimate = compressedSnapshot.estimateSerializedSize();
      final int compressedInlineBytes = PageLayout.getRecordOnlyLength(page.getSlottedPage(), targetSlot);
      assertTrue(compressedWire.length <= PageConstants.MAX_RECORD_SIZE,
          "generic compressed record must be inline-eligible: " + compressedWire.length);
      assertTrue(compressedHeapEstimate <= PageConstants.MAX_RECORD_SIZE,
          "flyweight compressed record must be inline-eligible: " + compressedHeapEstimate);
      assertTrue(compressedInlineBytes <= PageConstants.MAX_RECORD_SIZE,
          "actual compressed slot must be inline-eligible: " + compressedInlineBytes);

      final int descriptorBytes = compressedSnapshot.estimateOverflowDescriptorSize();
      final int spillThreshold = Math.min(compressedWire.length, compressedHeapEstimate);
      assertTrue(spillThreshold - descriptorBytes > 128,
          "fixture needs a tail window which fits the descriptor but not the compressed record");

      int denseFillers = 0;
      while (page.getSlottedPage().byteSize() < KeyValueLeafPage.MAX_SLOTTED_PAGE_CAPACITY
          || remainingTailBytes(page) >= spillThreshold + 2 * PageConstants.MAX_RECORD_SIZE) {
        assertTrue(wtx.moveTo(objectNodeKey));
        final long fillerNodeKey =
            wtx.insertObjectRecordAsFirstChild("dense-string-" + denseFillers, new StringValue(DENSE_FILLER_VALUE))
               .getNodeKey();
        assertEquals(TARGET_RECORD_PAGE_KEY, fillerNodeKey >> Constants.NDP_NODE_COUNT_EXPONENT,
            "dense fixture exhausted its target record page");
        assertTrue(++denseFillers <= MAX_DENSE_FILLERS, "dense fixture failed to reach the maximum page size");
      }
      assertEquals(KeyValueLeafPage.MAX_SLOTTED_PAGE_CAPACITY, page.getSlottedPage().byteSize());

      int tailFillers = 0;
      while (remainingTailBytes(page) >= spillThreshold) {
        final int before = remainingTailBytes(page);
        assertTrue(wtx.moveTo(objectNodeKey));
        final long fillerNodeKey =
            wtx.insertObjectRecordAsFirstChild("dense-tail-" + tailFillers++, NullValue.INSTANCE).getNodeKey();
        assertEquals(TARGET_RECORD_PAGE_KEY, fillerNodeKey >> Constants.NDP_NODE_COUNT_EXPONENT,
            "tail fixture exhausted its target record page");
        assertTrue(remainingTailBytes(page) < before, "tail filler must consume page heap bytes");
      }

      final int remaining = remainingTailBytes(page);
      assertTrue(remaining < compressedWire.length,
          "compressed generic record still fits the dense page tail: remaining=" + remaining);
      assertTrue(remaining < compressedHeapEstimate,
          "compressed flyweight record still fits the dense page tail: remaining=" + remaining);
      assertTrue(remaining >= descriptorBytes,
          "dense fixture left no room for the scan-visible overflow descriptor: remaining=" + remaining);

      // Re-run the ordinary resize path with the unchanged compressed snapshot. It cannot append the
      // otherwise-inline record at this tail position, so addReferences() must canonicalize the
      // compressed snapshot before installing its descriptor + same-key OverflowPage carrier.
      page.resizeRecord(compressedSnapshot, targetNodeKey, targetSlot);
      assertNull(page.getSlot(targetSlot), "the stale inline carrier must be cleared before the spill is sealed");
      assertNotNull(page.getRecord(targetSlot), "the compressed snapshot must remain queued for page sealing");
      assertNull(page.getPageReference(targetNodeKey), "the OverflowPage is installed only while sealing the page");

      wtx.commit();
      return new DenseSpillFixture(targetNodeKey, touchNodeKey, compressedWire.length, compressedHeapEstimate,
          compressedInlineBytes);
    }
  }

  private static int remainingTailBytes(final KeyValueLeafPage page) {
    return (int) page.getSlottedPage().byteSize() - PageLayout.HEAP_START - page.getCachedHeapEnd();
  }

  private static byte[] serializeRecord(final ResourceConfiguration resourceConfig, final DataRecord record) {
    try (final MemorySegmentBytesOut output = new MemorySegmentBytesOut(PageConstants.MAX_RECORD_SIZE)) {
      resourceConfig.recordPersister.serialize(output, record, resourceConfig);
      return output.getDestination().toArray(ValueLayout.JAVA_BYTE);
    }
  }

  private static void updateDisjointTouchField(final JsonResourceSession session, final long touchNodeKey) {
    try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
      assertTrue(wtx.moveTo(touchNodeKey));
      assertEquals(NodeKind.OBJECT_NAMED_BOOLEAN, wtx.getKind());
      wtx.setBooleanValue(true);
      wtx.commit();
    }
  }

  private static void assertCanonicalCarrier(final JsonResourceSession session, final int revision,
      final DenseSpillFixture fixture, final boolean expectedTouch, final boolean expectFragmentChain) {
    try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision)) {
      assertTrue(rtx.moveTo(fixture.targetNodeKey()));
      assertEquals(NodeKind.OBJECT_NAMED_STRING, rtx.getKind());
      assertEquals("target", rtx.getName().getLocalName());
      assertEquals(TARGET_VALUE, rtx.getValue());

      assertTrue(rtx.moveTo(fixture.touchNodeKey()));
      assertEquals(NodeKind.OBJECT_NAMED_BOOLEAN, rtx.getKind());
      assertEquals(expectedTouch, rtx.getBooleanValue());

      final StorageEngineReader reader = rtx.getStorageEngineReader();
      final long recordPageKey = reader.pageKey(fixture.targetNodeKey(), IndexType.DOCUMENT);
      final var loaded = reader.getRecordPage(new IndexLogKey(IndexType.DOCUMENT, recordPageKey, 0, revision));
      assertNotNull(loaded);
      if (expectFragmentChain) {
        assertFalse(loaded.reference().getPageFragments().isEmpty(),
            "the disjoint update must reconstruct the page from multiple version fragments");
      }

      final KeyValueLeafPage page = (KeyValueLeafPage) loaded.page();
      final int slot = StorageEngineReader.recordPageOffset(fixture.targetNodeKey());
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
          "canonical raw record should be larger than each measured compressed inline form: wire="
              + fixture.compressedWireBytes() + ", heapEstimate=" + fixture.compressedHeapEstimate() + ", actualSlot="
              + fixture.compressedInlineBytes());

      final DataRecord decoded =
          session.getResourceConfig().recordPersister.deserialize(new ByteArrayBytesIn(overflowPage.getDataBytes()),
              fixture.targetNodeKey(), page.getDeweyIdAsByteArray(slot), session.getResourceConfig());
      final ObjectNamedStringNode overflowNode = assertInstanceOf(ObjectNamedStringNode.class, decoded);
      assertFalse(overflowNode.isCompressed(),
          "OverflowPage payload must be canonical raw and independent of the record-page FSST table");
      assertArrayEquals(TARGET_BYTES, overflowNode.getRawValueWithoutDecompression());
    }
  }

  private record BootstrapFixture(long objectNodeKey, long trainingNodeKey, String trainingValue) {
  }

  private record LatestShadowFixture(long targetNodeKey, long touchNodeKey) {
  }

  private record DenseSpillFixture(long targetNodeKey, long touchNodeKey, int compressedWireBytes,
      int compressedHeapEstimate, int compressedInlineBytes) {
  }
}
