/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import io.sirix.page.delegates.BitmapReferencesPage;
import io.sirix.page.delegates.FullReferencesPage;
import io.sirix.page.delegates.ReferencesPage4;
import io.sirix.settings.Constants;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Structural tests for {@link ProjectionIndexPage}. */
final class ProjectionIndexPageTest {

  @Test
  void defaultCtorHasNoAllocatorEntriesOrInitializedIndexes() {
    final ProjectionIndexPage page = new ProjectionIndexPage();
    assertEquals(0, page.getMaxHotPageKeySize());
    assertFalse(page.isIndexInitialized(0));
    assertEquals(0, page.nextUnallocatedIndex());
  }

  @Test
  void maxHotPageKeyIsIndependentPerIndexAndReservesThePhysicalId() {
    final ProjectionIndexPage page = new ProjectionIndexPage();
    assertEquals(1L, page.incrementAndGetMaxHotPageKey(0));
    assertEquals(2L, page.incrementAndGetMaxHotPageKey(0));
    assertEquals(1L, page.incrementAndGetMaxHotPageKey(7));
    assertEquals(2L, page.getMaxHotPageKey(0));
    assertEquals(1L, page.getMaxHotPageKey(7));
    assertTrue(page.isIndexInitialized(0));
    assertTrue(page.isIndexInitialized(7));
    assertEquals(1, page.nextUnallocatedIndex());
  }

  @Test
  void deserializationCtorPreservesOnlySparseHotState() {
    final Int2LongOpenHashMap maxHotPageKeys = new Int2LongOpenHashMap();
    maxHotPageKeys.put(0, 7L);

    final ProjectionIndexPage page = new ProjectionIndexPage(new ReferencesPage4(), maxHotPageKeys);

    assertEquals(7L, page.getMaxHotPageKey(0));
    assertEquals(1, page.getMaxHotPageKeySize());
    assertTrue(page.isIndexInitialized(0));
  }

  @Test
  void pageKind_registeredAtByte16() {
    assertEquals((byte) 16, PageKind.PROJECTIONPAGE.getID());
    assertNotNull(PageKind.valueOf("PROJECTIONPAGE"));
  }

  @Test
  void getIndirectPageReference_returnsNonNull() {
    final ProjectionIndexPage page = new ProjectionIndexPage();
    assertNotNull(page.getIndirectPageReference(0));
  }

  @Test
  void nextUnallocatedIndexReadsSparseSlotsWithoutCreatingOne() {
    final ProjectionIndexPage page = new ProjectionIndexPage();
    markInitialized(page, 0);
    markInitialized(page, 2);

    assertEquals(1, page.nextUnallocatedIndex());
    assertEquals(2, page.getReferencesCount(), "the allocation probe must not materialize the hole");
  }

  @Test
  void readSidePlaceholderDoesNotConsumeAProjectionTreeId() {
    final ProjectionIndexPage page = new ProjectionIndexPage();
    page.getIndirectPageReference(0);

    assertEquals(0, page.nextUnallocatedIndex());
    assertEquals(1, page.getReferencesCount());
  }

  @Test
  void initializedProjectionRootConsumesAProjectionTreeId() {
    final ProjectionIndexPage page = new ProjectionIndexPage();
    page.getIndirectPageReference(0).setKey(42L);

    assertTrue(page.isIndexInitialized(0));
    assertEquals(1, page.nextUnallocatedIndex());
  }

  @Test
  void fromInclusiveScanSupportsBatchAllocationWithoutCreatingReferences() {
    final ProjectionIndexPage page = new ProjectionIndexPage();
    markInitialized(page, 3);
    markInitialized(page, 5);

    assertEquals(4, page.nextUnallocatedIndex(3));
    assertEquals(6, page.nextUnallocatedIndex(5));
    assertEquals(2, page.getReferencesCount());
  }

  @Test
  void indexOperationsRejectIdsOutsideThePhysicalReferenceSpace() {
    final ProjectionIndexPage page = new ProjectionIndexPage();

    assertThrows(IndexOutOfBoundsException.class, () -> page.isIndexInitialized(-1));
    assertThrows(IndexOutOfBoundsException.class, () -> page.nextUnallocatedIndex(Constants.INP_REFERENCE_COUNT));
    assertThrows(IndexOutOfBoundsException.class,
        () -> page.incrementAndGetMaxHotPageKey(Constants.INP_REFERENCE_COUNT));
  }

  @Test
  void persistedZeroHighWaterMarkConsumesAProjectionTreeId() {
    final Int2LongOpenHashMap maxHotPageKeys = new Int2LongOpenHashMap();
    maxHotPageKeys.put(0, 0L);
    final ProjectionIndexPage page = new ProjectionIndexPage(new ReferencesPage4(), maxHotPageKeys);

    assertEquals(1, page.nextUnallocatedIndex(),
        "a persisted zero is an initialization witness, not an absent map value");
  }

  @Test
  void nonMutatingProjectionLookupDoesNotGrowSparseDelegate() {
    final ProjectionIndexPage page = new ProjectionIndexPage();
    for (int index = 0; index < 4; index++) {
      page.getIndirectPageReference(index);
    }

    assertTrue(page.delegate() instanceof ReferencesPage4);
    assertNull(page.getIndexReference(4));
    assertTrue(page.delegate() instanceof ReferencesPage4);
    assertEquals(4, page.getReferencesCount());
  }

  @Test
  void nextUnallocatedIndexReadsBitmapSlotsWithoutMaterializingTheHole() {
    final ProjectionIndexPage page = new ProjectionIndexPage();
    for (int index = 0; index < 4; index++) {
      markInitialized(page, index);
    }
    markInitialized(page, 5);

    assertTrue(page.delegate() instanceof BitmapReferencesPage);
    assertEquals(4, page.nextUnallocatedIndex());
    assertEquals(5, page.getReferencesCount(), "the allocation probe must not materialize the hole");
  }

  @Test
  void nextUnallocatedIndexReadsFullDelegatePhysicalHoles() {
    final ProjectionIndexPage page = new ProjectionIndexPage();
    final int hole = 37;
    for (int index = 0; index <= BitmapReferencesPage.THRESHOLD; index++) {
      if (index != hole) {
        markInitialized(page, index);
      }
    }

    assertTrue(page.delegate() instanceof FullReferencesPage);
    assertEquals(hole, page.nextUnallocatedIndex());
    assertEquals(BitmapReferencesPage.THRESHOLD,
        page.getReferences().stream().filter(reference -> reference != null).count(),
        "the allocation probe must not materialize the hole");
  }

  @Test
  void nextUnallocatedIndexSurvivesFullDelegateCopyOnWrite() {
    final ProjectionIndexPage source = new ProjectionIndexPage();
    final int hole = 37;
    for (int index = 0; index <= BitmapReferencesPage.THRESHOLD; index++) {
      if (index != hole) {
        markInitialized(source, index);
      }
    }

    final ProjectionIndexPage copy = new ProjectionIndexPage(source);

    assertTrue(copy.delegate() instanceof FullReferencesPage);
    assertNull(copy.getIndexReference(hole),
        "copy-on-write must retain a null slot instead of manufacturing a placeholder");
    assertEquals(hole, copy.nextUnallocatedIndex(),
        "copy-on-write must preserve physical holes instead of materializing placeholders");
  }

  @Test
  void nextUnallocatedIndexFailsClearlyWhenPhysicalSlotsAreExhausted() {
    final ProjectionIndexPage page = new ProjectionIndexPage();
    for (int index = 0; index < Constants.INP_REFERENCE_COUNT; index++) {
      markInitialized(page, index);
    }

    final IllegalStateException exception = assertThrows(IllegalStateException.class, page::nextUnallocatedIndex);
    assertTrue(exception.getMessage().contains(Integer.toString(Constants.INP_REFERENCE_COUNT)));
  }

  private static void markInitialized(final ProjectionIndexPage page, final int index) {
    page.getIndirectPageReference(index).setKey(index + 1L);
  }

  @Test
  void referenceCreationGrowsAcrossBothDelegateThresholds() {
    final ProjectionIndexPage page = new ProjectionIndexPage();
    for (int index = 0; index <= BitmapReferencesPage.THRESHOLD; index++) {
      assertNotNull(page.getIndirectPageReference(index), "projection index " + index);
    }
    assertEquals(Constants.INP_REFERENCE_COUNT, page.getReferencesCount(),
        "crossing the bitmap threshold must promote to the fixed-capacity full delegate");
  }

  @Test
  void revisionRoot_exposesProjectionPage() {
    final RevisionRootPage root = new RevisionRootPage();
    final PageReference ref = root.getProjectionIndexPageReference();
    assertNotNull(ref);
    assertNotNull(ref.getPage(), "fresh revision must seed a ProjectionIndexPage");
    assertEquals(ProjectionIndexPage.class, ref.getPage().getClass(),
        "projection reference must hold a ProjectionIndexPage, not some other container");
  }
}
