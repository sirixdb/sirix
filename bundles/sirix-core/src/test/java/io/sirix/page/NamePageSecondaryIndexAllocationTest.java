/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import io.sirix.access.DatabaseType;
import io.sirix.settings.Constants;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Allocation rules for never-reused secondary NAME physical slots. */
final class NamePageSecondaryIndexAllocationTest {

  @Test
  void dictionarySlotValidatorOwnsBothReservedPrefixes() {
    for (int index = 0; index < PageConstants.JSON_NAME_INDEX_OFFSET; index++) {
      final int dictionaryIndex = index;
      assertTrue(NamePage.isNameDictionarySlot(DatabaseType.JSON, dictionaryIndex));
    }
    for (int index = 0; index < PageConstants.XML_NAME_INDEX_OFFSET; index++) {
      final int dictionaryIndex = index;
      assertTrue(NamePage.isNameDictionarySlot(DatabaseType.XML, dictionaryIndex));
    }

    assertThrows(IllegalArgumentException.class, () -> NamePage.isNameDictionarySlot(DatabaseType.JSON, -1));
    assertFalse(NamePage.isNameDictionarySlot(DatabaseType.JSON, PageConstants.JSON_NAME_INDEX_OFFSET));
    assertFalse(NamePage.isNameDictionarySlot(DatabaseType.XML, PageConstants.XML_NAME_INDEX_OFFSET));
    assertThrows(IllegalArgumentException.class,
        () -> NamePage.isNameDictionarySlot(DatabaseType.XML, Constants.INP_REFERENCE_COUNT));
  }

  @Test
  void reservedDictionaryPrefixesAreNeverReturned() {
    final NamePage json = new NamePage();
    for (int offset = 0; offset < PageConstants.JSON_NAME_INDEX_OFFSET; offset++) {
      json.setOrCreateReference(offset, durableReference(1_000L + offset));
    }
    assertEquals(PageConstants.JSON_NAME_INDEX_OFFSET, json.nextUnallocatedSecondaryNameIndex(DatabaseType.JSON));

    final NamePage xml = new NamePage();
    for (int offset = 0; offset < PageConstants.XML_NAME_INDEX_OFFSET; offset++) {
      xml.setOrCreateReference(offset, durableReference(2_000L + offset));
    }
    assertEquals(PageConstants.XML_NAME_INDEX_OFFSET, xml.nextUnallocatedSecondaryNameIndex(DatabaseType.XML));
  }

  @Test
  void droppedIndexRootRemainsReserved() {
    final NamePage page = new NamePage();
    final int first = PageConstants.JSON_NAME_INDEX_OFFSET;
    page.setOrCreateReference(first, durableReference(42L));

    assertTrue(page.isSecondaryNameIndexInitialized(DatabaseType.JSON, first));
    assertEquals(first + 1, page.nextUnallocatedSecondaryNameIndex(DatabaseType.JSON),
        "a physical tree surviving catalog removal must never be assigned to a new definition");
  }

  @Test
  void sparseInitializedSlotsLeaveTheFirstPhysicalGapAvailable() {
    final NamePage page = new NamePage();
    final int first = PageConstants.JSON_NAME_INDEX_OFFSET;
    page.setOrCreateReference(first, durableReference(100L));
    page.setOrCreateReference(first + 2, durableReference(102L));

    final int referencesBefore = page.getReferencesCount();
    assertFalse(page.isSecondaryNameIndexInitialized(DatabaseType.JSON, first + 1));
    assertEquals(first + 1, page.nextUnallocatedSecondaryNameIndex(DatabaseType.JSON));
    assertEquals(referencesBefore, page.getReferencesCount(), "allocation probing must not materialize the gap");
  }

  @Test
  void virginReadPlaceholderDoesNotBurnANameIndexSlot() {
    final NamePage page = new NamePage();
    final int first = PageConstants.JSON_NAME_INDEX_OFFSET;
    page.getOrCreateReference(first);
    final int referencesBefore = page.getReferencesCount();

    assertFalse(page.isSecondaryNameIndexInitialized(DatabaseType.JSON, first));
    assertEquals(first, page.nextUnallocatedSecondaryNameIndex(DatabaseType.JSON));
    assertEquals(referencesBefore, page.getReferencesCount(), "placeholder probing must remain non-mutating");
  }

  @Test
  void sparseHotAllocatorMetadataReservesSlotEvenWithoutLoadedRoot() {
    final NamePage page = new NamePage();
    final int first = PageConstants.JSON_NAME_INDEX_OFFSET;
    page.incrementAndGetMaxHotPageKey(first);

    assertTrue(page.isSecondaryNameIndexInitialized(DatabaseType.JSON, first));
    assertEquals(first + 1, page.nextUnallocatedSecondaryNameIndex(DatabaseType.JSON));
  }

  @Test
  void initializedProbeRejectsDictionaryAndOutOfRangeSlotsWithoutGrowingThePage() {
    final NamePage page = new NamePage();
    final int referencesBefore = page.getReferencesCount();

    assertThrows(IllegalArgumentException.class,
        () -> page.isSecondaryNameIndexInitialized(DatabaseType.JSON, PageConstants.JSON_NAME_INDEX_OFFSET - 1));
    assertThrows(IllegalArgumentException.class,
        () -> page.isSecondaryNameIndexInitialized(DatabaseType.XML, PageConstants.XML_NAME_INDEX_OFFSET - 1));
    assertThrows(IllegalArgumentException.class,
        () -> page.isSecondaryNameIndexInitialized(DatabaseType.JSON, Constants.INP_REFERENCE_COUNT));
    assertEquals(referencesBefore, page.getReferencesCount());
  }

  @Test
  void readPredicatesAndExistingRootLookupNeverCreateReferences() {
    final NamePage page = new NamePage();
    final int referencesBefore = page.getReferencesCount();

    assertFalse(page.hasProjectionValueDictionary(DatabaseType.JSON));
    assertNull(
        page.getNameDictionaryReference(DatabaseType.JSON, NamePage.JSON_PROJECTION_VALUE_DICTIONARY_REFERENCE_OFFSET));
    assertNull(page.getIndexReference(DatabaseType.JSON, PageConstants.JSON_NAME_INDEX_OFFSET));
    assertThrows(IllegalArgumentException.class,
        () -> page.getNameDictionaryReference(DatabaseType.JSON, PageConstants.JSON_NAME_INDEX_OFFSET));
    assertEquals(referencesBefore, page.getReferencesCount());
  }

  private static PageReference durableReference(final long key) {
    return new PageReference().setKey(key);
  }
}
