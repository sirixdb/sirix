/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import io.sirix.access.DatabaseType;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.StorageEngineWriter;
import io.sirix.index.IndexType;
import io.sirix.node.HashCountEntryNode;
import io.sirix.node.HashEntryNode;
import io.sirix.node.NodeKind;
import io.sirix.page.delegates.ReferencesPage4;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import org.junit.jupiter.api.Test;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/** Write-side isolation guarantees for the lazily copied NamePage container. */
final class NamePageCopyForWriteTest {

  private static final int DICTIONARY_OFFSET = NamePage.ELEMENTS_REFERENCE_OFFSET;
  private static final int SECONDARY_INDEX_OFFSET = PageConstants.JSON_NAME_INDEX_OFFSET;

  @Test
  void copyDetachesReferencesAllocatorMapsAndLiveKeyBitmaps() {
    final NamePage historical = historicalPage(2L);
    historical.incrementAndGetMaxHotPageKey(SECONDARY_INDEX_OFFSET);
    historical.incrementAndGetCurrentMaxLevelOfIndirectPages(DICTIONARY_OFFSET);
    historical.setOrCreateReference(SECONDARY_INDEX_OFFSET, new PageReference().setKey(41L));
    final Roaring64Bitmap historicalLiveKeys = new Roaring64Bitmap();
    historicalLiveKeys.add(1L);
    historical.putLiveEntryNodeKeys(DICTIONARY_OFFSET, historicalLiveKeys);

    final NamePage copy = NamePage.copyForWrite(historical);

    final PageReference historicalReference = historical.getIndexReference(DatabaseType.JSON, SECONDARY_INDEX_OFFSET);
    final PageReference copiedReference = copy.getIndexReference(DatabaseType.JSON, SECONDARY_INDEX_OFFSET);
    assertNotSame(historicalReference, copiedReference);
    copiedReference.setKey(99L);
    assertEquals(41L, historicalReference.getKey());

    assertEquals(2L, copy.incrementAndGetMaxHotPageKey(SECONDARY_INDEX_OFFSET));
    assertEquals(1L, historical.getMaxHotPageKey(SECONDARY_INDEX_OFFSET));
    assertEquals(2, copy.incrementAndGetCurrentMaxLevelOfIndirectPages(DICTIONARY_OFFSET));
    assertEquals(1, historical.getCurrentMaxLevelOfIndirectPages(DICTIONARY_OFFSET));
    assertEquals(3L, copy.incrementAndGetMaxNodeKey(DICTIONARY_OFFSET));
    assertEquals(2L, historical.getMaxNodeKey(DICTIONARY_OFFSET));

    final Roaring64Bitmap copiedLiveKeys = copy.getLiveEntryNodeKeysToSerialize(DICTIONARY_OFFSET);
    copiedLiveKeys.add(3L);
    assertTrue(copiedLiveKeys.contains(3L));
    assertFalse(historical.getLiveEntryNodeKeysToSerialize(DICTIONARY_OFFSET).contains(3L));
  }

  @Test
  void copyNeverLoadsNamesOrTouchesItsSource() {
    final NamePage historical = historicalPage(2L);
    final Roaring64Bitmap historicalLiveKeys = new Roaring64Bitmap();
    historicalLiveKeys.add(1L);
    historical.putLiveEntryNodeKeys(DICTIONARY_OFFSET, historicalLiveKeys);
    final StorageEngineReader reader = mock(StorageEngineReader.class);

    @SuppressWarnings("deprecation")
    final NamePage copy = new NamePage(historical, reader);

    verifyNoInteractions(reader);
    assertEquals(2L, historical.getMaxNodeKey(DICTIONARY_OFFSET));
    assertTrue(historical.getLiveEntryNodeKeysToSerialize(DICTIONARY_OFFSET).contains(1L));
    assertEquals(2L, copy.getMaxNodeKey(DICTIONARY_OFFSET));
  }

  @Test
  void loadedHistoricalDictionaryAndWriteCopyRemainIndependent() {
    final String persistedName = "persisted-name";
    final int persistedKey = persistedName.hashCode();
    final NamePage historical = historicalPage(2L);
    final Roaring64Bitmap historicalLiveKeys = new Roaring64Bitmap();
    historicalLiveKeys.add(1L);
    historical.putLiveEntryNodeKeys(DICTIONARY_OFFSET, historicalLiveKeys);
    final StorageEngineWriter writer = writerWithPersistedName(persistedName, persistedKey);

    // Materialize the source first. copyForWrite must neither share nor mutate this Names object.
    assertEquals(persistedName, historical.getName(persistedKey, NodeKind.ELEMENT, writer));
    final NamePage copy = NamePage.copyForWrite(historical);
    // A write copy stays private even if its first read is made through a read-only delegate. It
    // must reconstruct directly instead of adopting the mutable shared NamesCache value.
    when(writer.hasTrxIntentLog()).thenReturn(false);
    assertEquals(persistedName, copy.getName(persistedKey, NodeKind.ELEMENT, writer));
    copy.setName(persistedName, NodeKind.ELEMENT, writer);
    assertEquals(1, historical.getCount(persistedKey, NodeKind.ELEMENT, writer));
    assertEquals(2, copy.getCount(persistedKey, NodeKind.ELEMENT, writer));

    final String sourceOnly = "source-only-name";
    final String copyOnly = "copy-only-name";
    final int sourceOnlyKey = historical.setName(sourceOnly, NodeKind.ELEMENT, writer);
    final int copyOnlyKey = copy.setName(copyOnly, NodeKind.ELEMENT, writer);
    assertNotEquals(sourceOnlyKey, copyOnlyKey);

    assertEquals(sourceOnly, historical.getName(sourceOnlyKey, NodeKind.ELEMENT, writer));
    assertNull(historical.getName(copyOnlyKey, NodeKind.ELEMENT, writer));
    assertEquals(copyOnly, copy.getName(copyOnlyKey, NodeKind.ELEMENT, writer));
    assertNull(copy.getName(sourceOnlyKey, NodeKind.ELEMENT, writer));
    assertEquals(1, historical.getCount(sourceOnlyKey, NodeKind.ELEMENT, writer));
    assertEquals(1, copy.getCount(copyOnlyKey, NodeKind.ELEMENT, writer));
  }

  @Test
  void nullHistoricalPageFailsBeforeAnyCopyCanBePublished() {
    assertThrows(NullPointerException.class, () -> NamePage.copyForWrite(null));
  }

  private static NamePage historicalPage(final long maxNodeKey) {
    final Int2LongOpenHashMap maxNodeKeys = new Int2LongOpenHashMap();
    maxNodeKeys.put(DICTIONARY_OFFSET, maxNodeKey);
    final Int2IntOpenHashMap levels = new Int2IntOpenHashMap();
    levels.put(DICTIONARY_OFFSET, 0);
    return new NamePage(new ReferencesPage4(), maxNodeKeys, levels, 0);
  }

  private static StorageEngineWriter writerWithPersistedName(final String name, final int key) {
    final StorageEngineWriter writer = mock(StorageEngineWriter.class);
    final HashEntryNode entry = new HashEntryNode(1L, key, name);
    final HashCountEntryNode count = new HashCountEntryNode(2L, 1);
    when(writer.hasTrxIntentLog()).thenReturn(true);
    when(writer.<HashEntryNode>getRecord(1L, IndexType.NAME, DICTIONARY_OFFSET)).thenReturn(entry);
    when(writer.<HashCountEntryNode>getRecord(2L, IndexType.NAME, DICTIONARY_OFFSET)).thenReturn(count);
    when(writer.<HashCountEntryNode>prepareRecordForModification(2L, IndexType.NAME, DICTIONARY_OFFSET)).thenReturn(
        new HashCountEntryNode(2L, 1));
    return writer;
  }
}
