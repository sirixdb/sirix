/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.settings;

import io.sirix.access.ResourceConfiguration;
import io.sirix.api.ResourceSession;
import io.sirix.api.StorageEngineReader;
import io.sirix.cache.PageContainer;
import io.sirix.cache.TransactionIntentLog;
import io.sirix.index.IndexType;
import io.sirix.node.NodeKind;
import io.sirix.node.json.BooleanNode;
import io.sirix.node.json.ObjectNamedStringNode;
import io.sirix.node.json.StringNode;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.NodeFieldLayout;
import io.sirix.page.OverflowPage;
import io.sirix.page.PageLayout;
import io.sirix.page.PageReference;
import io.sirix.page.UberPage;
import io.sirix.utils.FSSTCompressor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Focused FSST metadata-copy regressions at the VersioningType/KVL boundary. */
final class VersioningTypeFsstMetadataCopyTest {

  private static final long RECORD_PAGE_KEY = 1L;
  private static final long NEWEST_TABLE_ID = 101L;
  private static final long OLDER_TABLE_ID = 202L;
  private static final int TARGET_SLOT = 900;
  private static final int NEWER_NAME_KEY = 73;
  private static final long NEWER_PATH_NODE_KEY = 19L;
  private static final byte[] NEWER_DEWEY_ID = new byte[] {1, 7, 4};
  private static final byte[] OLDER_DEWEY_ID = new byte[] {9, 2};
  private static final String DENSE_TOKEN = "versioning-target-only-carrier-";

  @Test
  void slidingBulkSeedAcceptsOlderTableIndependentSlotsAfterDictionaryRotation() {
    final ResourceConfiguration config = new ResourceConfiguration.Builder("sliding-fsst-raw-copy").build();
    final byte[] newestTable = symbolTable("newest-alpha-beta-gamma-");
    final byte[] olderTable = symbolTable("older-uno-dos-tres-");
    assertFalse(Arrays.equals(newestTable, olderTable), "fixture requires genuinely distinct FSST tables");

    final KeyValueLeafPage newest =
        new KeyValueLeafPage(RECORD_PAGE_KEY, IndexType.DOCUMENT, config, 3, null, null, false);
    final KeyValueLeafPage older =
        new KeyValueLeafPage(RECORD_PAGE_KEY, IndexType.DOCUMENT, config, 2, null, null, false);
    KeyValueLeafPage combined = null;
    try {
      final byte[] newestBoolean = booleanSlot(0, true);
      final byte[] olderBoolean = booleanSlot(1, false);
      final byte[] olderRawString = rawStringSlot(2, "raw value independent of either FSST dictionary");
      newest.setSlotWithNodeKind(MemorySegment.ofArray(newestBoolean), 0, NodeKind.BOOLEAN_VALUE.getId());
      older.setSlotWithNodeKind(MemorySegment.ofArray(olderBoolean), 1, NodeKind.BOOLEAN_VALUE.getId());
      older.setSlotWithNodeKind(MemorySegment.ofArray(olderRawString), 2, NodeKind.STRING_VALUE.getId());
      newest.setFsstSymbolTable(newestTable);
      newest.setFsstSymbolTableId(NEWEST_TABLE_ID);
      older.setFsstSymbolTable(olderTable);
      older.setFsstSymbolTableId(OLDER_TABLE_ID);

      @SuppressWarnings("rawtypes")
      final ResourceSession resourceSession = mock(ResourceSession.class);
      when(resourceSession.getResourceConfig()).thenReturn(config);
      final StorageEngineReader reader = mock(StorageEngineReader.class);
      when(reader.getResourceSession()).thenReturn(resourceSession);
      when(reader.getRevisionNumber()).thenReturn(3);

      combined = VersioningType.SLIDING_SNAPSHOT.combineRecordPages(List.of(newest, older), 3, reader);

      assertEquals(NEWEST_TABLE_ID, combined.getFsstSymbolTableId());
      assertNotNull(combined.getSlot(0));
      assertArrayEquals(olderBoolean, combined.getSlot(1).toArray(ValueLayout.JAVA_BYTE));
      assertArrayEquals(olderRawString, combined.getSlot(2).toArray(ValueLayout.JAVA_BYTE));
    } finally {
      if (combined != null) {
        combined.close();
      }
      older.close();
      newest.close();
    }
  }

  @ParameterizedTest(name = "{0}: periodic full publishes the newer target-only side winner")
  @EnumSource(value = VersioningType.class, names = {"DIFFERENTIAL", "INCREMENTAL"})
  void periodicFullDoesNotPublishOlderReferenceOverNewerCompressedInline(final VersioningType versioningType) {
    final ResourceConfiguration config =
        new ResourceConfiguration.Builder("periodic-full-side-winner").useDeweyIDs(true).build();
    final byte[] symbolTable = symbolTable(DENSE_TOKEN);
    final byte[] rawValue = (DENSE_TOKEN.repeat(12) + "newer").getBytes(StandardCharsets.UTF_8);
    final byte[] compressedValue = FSSTCompressor.encode(rawValue, symbolTable);
    assertTrue(compressedValue.length < rawValue.length);

    final KeyValueLeafPage newer =
        new KeyValueLeafPage(RECORD_PAGE_KEY, IndexType.DOCUMENT, config, 3, null, null, false);
    final KeyValueLeafPage older =
        new KeyValueLeafPage(RECORD_PAGE_KEY, IndexType.DOCUMENT, config, 2, null, null, false);
    PageContainer container = null;
    try {
      // The source fragment stays compact under FSST. Its decompressed image exceeds the bounded
      // target frame, so the high target slot is forced into a side image plus a fresh reference
      // only while the fragments are combined.
      for (int slot = 0; slot < 760; slot++) {
        final byte[] filler = objectNamedStringSlot(slot, compressedValue, true, 11, 12L);
        newer.setSlotWithNodeKind(MemorySegment.ofArray(filler), slot, NodeKind.OBJECT_NAMED_STRING.getId());
      }
      final byte[] newerTarget =
          objectNamedStringSlot(TARGET_SLOT, compressedValue, true, NEWER_NAME_KEY, NEWER_PATH_NODE_KEY);
      newer.setSlotWithNodeKind(MemorySegment.ofArray(newerTarget), TARGET_SLOT, NodeKind.OBJECT_NAMED_STRING.getId());
      newer.setDeweyId(NEWER_DEWEY_ID, TARGET_SLOT);
      newer.setFsstSymbolTable(symbolTable);
      newer.setFsstSymbolTableId(NEWEST_TABLE_ID);

      final long targetRecordKey = recordKey(TARGET_SLOT);
      final byte[] olderRecord =
          objectNamedStringSlot(TARGET_SLOT, "older".getBytes(StandardCharsets.UTF_8), false, 5, 6L);
      final byte[] olderSideImage = appendDeweyId(olderRecord, OLDER_DEWEY_ID);
      final long olderSideToken = older.prepareSideSlot(NodeKind.OBJECT_NAMED_STRING.getId(),
          MemorySegment.ofArray(olderSideImage), olderSideImage.length);
      older.publishSideSlot(TARGET_SLOT, olderSideToken);
      final PageReference olderReference = new PageReference();
      olderReference.setPage(new OverflowPage(new byte[] {42}));
      older.setPageReference(targetRecordKey, olderReference);

      @SuppressWarnings("rawtypes")
      final ResourceSession resourceSession = mock(ResourceSession.class);
      when(resourceSession.getResourceConfig()).thenReturn(config);
      final StorageEngineReader reader = mock(StorageEngineReader.class);
      when(reader.getResourceSession()).thenReturn(resourceSession);
      when(reader.getRevisionNumber()).thenReturn(3);
      when(reader.getUberPage()).thenReturn(new UberPage(3)); // current revision two: 2 % 2 == 0
      when(reader.getDatabaseId()).thenReturn(1L);
      when(reader.getResourceId()).thenReturn(2L);

      final PageReference owningReference = new PageReference().setKey(99L);
      final TransactionIntentLog log = mock(TransactionIntentLog.class);
      container =
          versioningType.combineRecordPagesForModification(List.of(newer, older), 2, reader, owningReference, log);

      final KeyValueLeafPage complete = (KeyValueLeafPage) container.getComplete();
      final KeyValueLeafPage modified = (KeyValueLeafPage) container.getModified();
      assertNewerTargetOnlyWinner(complete, targetRecordKey, olderReference);

      assertNull(modified.getPageReference(targetRecordKey),
          "an older reference must not be mirrored into the periodic full fragment");
      modified.addReferences(config);
      assertNewerTargetOnlyWinner(modified, targetRecordKey, olderReference);
    } finally {
      if (container != null) {
        container.getModified().close();
        container.getComplete().close();
      }
      older.close();
      newer.close();
    }
  }

  private static byte[] symbolTable(final String token) {
    final List<byte[]> samples = new ArrayList<>(FSSTCompressor.MIN_SAMPLES_FOR_TABLE * 2);
    for (int i = 0; i < FSSTCompressor.MIN_SAMPLES_FOR_TABLE * 2; i++) {
      samples.add((token.repeat(8) + i).getBytes(StandardCharsets.UTF_8));
    }
    final byte[] table = FSSTCompressor.buildSymbolTable(samples);
    assertNotNull(table);
    return table;
  }

  private static byte[] booleanSlot(final int slot, final boolean value) {
    final long nodeKey = (RECORD_PAGE_KEY << Constants.NDP_NODE_COUNT_EXPONENT) + slot;
    final MemorySegment scratch = MemorySegment.ofArray(new byte[128]);
    final int length = BooleanNode.writeNewRecord(scratch, 0L, new int[6], nodeKey, 0L, 0L, 0L, 0, 0, value);
    return scratch.asSlice(0L, length).toArray(ValueLayout.JAVA_BYTE);
  }

  private static byte[] rawStringSlot(final int slot, final String value) {
    final long nodeKey = (RECORD_PAGE_KEY << Constants.NDP_NODE_COUNT_EXPONENT) + slot;
    final byte[] raw = value.getBytes(StandardCharsets.UTF_8);
    final MemorySegment scratch = MemorySegment.ofArray(new byte[512]);
    final int length = StringNode.writeNewRecord(scratch, 0L, new int[6], nodeKey, 0L, 0L, 0L, 0, 0, raw, false);
    return scratch.asSlice(0L, length).toArray(ValueLayout.JAVA_BYTE);
  }

  private static byte[] objectNamedStringSlot(final int slot, final byte[] value, final boolean compressed,
      final int nameKey, final long pathNodeKey) {
    final long nodeKey = recordKey(slot);
    final MemorySegment scratch = MemorySegment.ofArray(new byte[1024]);
    final int length =
        ObjectNamedStringNode.writeNewRecord(scratch, 0L, new int[NodeFieldLayout.OBJECT_NAMED_STRING_FIELD_COUNT],
            nodeKey, 0L, 0L, 0L, nameKey, pathNodeKey, 1, 2, 0L, value, compressed);
    return scratch.asSlice(0L, length).toArray(ValueLayout.JAVA_BYTE);
  }

  private static byte[] appendDeweyId(final byte[] record, final byte[] deweyId) {
    final byte[] image = Arrays.copyOf(record, record.length + deweyId.length + PageLayout.DEWEY_ID_TRAILER_SIZE);
    System.arraycopy(deweyId, 0, image, record.length, deweyId.length);
    PageLayout.writeDeweyIdTrailer(MemorySegment.ofArray(image), image.length, deweyId.length);
    return image;
  }

  private static long recordKey(final int slot) {
    return (RECORD_PAGE_KEY << Constants.NDP_NODE_COUNT_EXPONENT) + slot;
  }

  private static void assertNewerTargetOnlyWinner(final KeyValueLeafPage page, final long targetRecordKey,
      final PageReference olderReference) {
    assertTrue(page.hasSideSlot(TARGET_SLOT),
        "decompressing the dense latest fragment must force the target to a side slot");
    assertFalse(PageLayout.isSlotPopulated(page.getSlottedPage(), TARGET_SLOT));
    final PageReference winner = page.getPageReference(targetRecordKey);
    assertNotNull(winner);
    assertNotSame(olderReference, winner, "the stale older reference replaced the newer winner");
    assertEquals(NEWER_NAME_KEY, page.getObjectKeyNameKeyFromSlot(TARGET_SLOT));
    assertEquals(NEWER_PATH_NODE_KEY, page.getObjectKeyPathNodeKeyFromSlot(TARGET_SLOT, targetRecordKey));
    assertArrayEquals(NEWER_DEWEY_ID, page.getDeweyIdAsByteArray(TARGET_SLOT));
  }
}
