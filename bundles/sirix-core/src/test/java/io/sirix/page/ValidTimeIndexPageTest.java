/*
 * [New BSD License]
 * Copyright (c) 2026, SirixDB Contributors
 * All rights reserved.
 */
package io.sirix.page;

import io.sirix.node.Bytes;
import io.sirix.node.BytesIn;
import io.sirix.node.BytesOut;
import io.sirix.page.delegates.BitmapReferencesPage;
import io.sirix.page.delegates.ReferencesPage4;
import io.sirix.settings.Constants;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Structural tests for {@link ValidTimeIndexPage} and the current {@link RevisionRootPage}
 * reference layout, including dense and sparse bitmap serialization.
 *
 * @author Johannes Lichtenberger
 */
final class ValidTimeIndexPageTest {

  @Test
  void defaultCtorHasNoAllocatorEntriesOrInitializedIndexes() {
    final ValidTimeIndexPage page = new ValidTimeIndexPage();
    assertEquals(0, page.getMaxHotPageKeySize());
    assertFalse(page.isIndexInitialized(0));
    assertEquals(0, page.nextUnallocatedIndex());
  }

  @Test
  void hotPageKeyIsIndependentPerIndexAndReservesThePhysicalId() {
    final ValidTimeIndexPage page = new ValidTimeIndexPage();
    assertEquals(1L, page.incrementAndGetMaxHotPageKey(0));
    assertEquals(2L, page.incrementAndGetMaxHotPageKey(0));
    assertEquals(1L, page.incrementAndGetMaxHotPageKey(9));
    assertEquals(2L, page.getMaxHotPageKey(0));
    assertEquals(1L, page.getMaxHotPageKey(9));
    assertTrue(page.isIndexInitialized(0));
    assertTrue(page.isIndexInitialized(9));
    assertEquals(1, page.nextUnallocatedIndex());
  }

  @Test
  void deserializationCtorPreservesOnlySparseHotState() {
    final Int2LongOpenHashMap maxHotPageKeys = new Int2LongOpenHashMap();
    maxHotPageKeys.put(0, 7L);

    final ValidTimeIndexPage page = new ValidTimeIndexPage(new ReferencesPage4(), maxHotPageKeys);

    assertEquals(7L, page.getMaxHotPageKey(0));
    assertTrue(page.isIndexInitialized(0));
  }

  @Test
  void pageKind_registeredAtByte17() {
    assertEquals((byte) 17, PageKind.VALIDTIMEPAGE.getID());
    assertNotNull(PageKind.valueOf("VALIDTIMEPAGE"));
    // The kind must resolve back from its id and its page class (the dispatcher relies on both).
    assertEquals(PageKind.VALIDTIMEPAGE, PageKind.getKind((byte) 17));
    assertEquals(PageKind.VALIDTIMEPAGE, PageKind.getKind(ValidTimeIndexPage.class));
  }

  @Test
  void getIndirectPageReference_returnsNonNull() {
    final ValidTimeIndexPage page = new ValidTimeIndexPage();
    assertNotNull(page.getIndirectPageReference(0));
    assertFalse(page.isIndexInitialized(0), "a virgin structural placeholder must not reserve an id");
  }

  @Test
  void allocatorUsesRootAndSparseMetadataWitnessesWithoutMaterializingHoles() {
    final ValidTimeIndexPage page = new ValidTimeIndexPage();
    page.getIndirectPageReference(3).setKey(4L);
    page.incrementAndGetMaxHotPageKey(5);

    assertTrue(page.isIndexInitialized(3));
    assertTrue(page.isIndexInitialized(5));
    assertEquals(4, page.nextUnallocatedIndex(3));
    assertEquals(6, page.nextUnallocatedIndex(5));
    assertEquals(1, page.getReferencesCount(), "allocation probes must not create root references");
  }

  @Test
  void indexOperationsRejectIdsOutsideThePhysicalReferenceSpace() {
    final ValidTimeIndexPage page = new ValidTimeIndexPage();

    assertThrows(IndexOutOfBoundsException.class, () -> page.isIndexInitialized(-1));
    assertThrows(IndexOutOfBoundsException.class, () -> page.nextUnallocatedIndex(Constants.INP_REFERENCE_COUNT));
  }

  @Test
  void freshRevisionRoot_seedsValidTimeIndexPage() {
    final RevisionRootPage root = new RevisionRootPage();
    final PageReference ref = root.getValidTimeIndexPageReference();
    assertNotNull(ref);
    assertNotNull(ref.getPage(), "fresh revision must seed a ValidTimeIndexPage at offset 10");
    assertEquals(ValidTimeIndexPage.class, ref.getPage().getClass(),
        "valid-time reference must hold a ValidTimeIndexPage, not some other container");
  }

  @Test
  void revisionRoot_referenceCountIncludesEveryIndexContainer() {
    assertEquals(13, RevisionRootPage.REVISION_ROOT_PAGE_REFERENCE_COUNT);
    final RevisionRootPage root = new RevisionRootPage();
    // Typed getters must preserve each container's assigned slot as new containers are appended.
    assertNotNull(root.getNamePageReference());
    assertNotNull(root.getCASPageReference());
    assertNotNull(root.getPathPageReference());
    assertNotNull(root.getDeweyIdPageReference());
    assertNotNull(root.getVectorPageReference());
    assertNotNull(root.getProjectionIndexPageReference());
    assertSame(root.getOrCreateReference(10), root.getValidTimeIndexPageReference());
    assertSame(root.getOrCreateReference(11), root.getTemporalAggregateIndexPageReference());
    assertSame(root.getOrCreateReference(12), root.getChangeLogIndexPageReference());
  }

  /**
   * The revision-root wire contract stores a presence bitmap followed by only the populated
   * references. All current slots must round-trip, while absent containers must remain absent until
   * requested without changing already persisted references.
   */
  @Test
  void denseAndSparseRevisionRootReferencesRoundTrip() {
    final int referenceCount = RevisionRootPage.REVISION_ROOT_PAGE_REFERENCE_COUNT;
    final BitmapReferencesPage dense = new BitmapReferencesPage(referenceCount);
    for (int offset = 0; offset < referenceCount; offset++) {
      dense.getOrCreateReference(offset).setKey(1000L + offset);
    }
    final BitmapReferencesPage denseRoundTripped = roundTrip(dense, referenceCount);
    assertEquals(referenceCount, denseRoundTripped.getBitmap().cardinality());
    for (int offset = 0; offset < referenceCount; offset++) {
      assertTrue(denseRoundTripped.getBitmap().get(offset), "offset " + offset + " must be present");
      assertEquals(1000L + offset, denseRoundTripped.getOrCreateReference(offset).getKey());
    }

    final int populatedCount = 10;
    final BitmapReferencesPage sparse = new BitmapReferencesPage(referenceCount);
    for (int offset = 0; offset < populatedCount; offset++) {
      sparse.getOrCreateReference(offset).setKey(2000L + offset);
    }
    final BitmapReferencesPage sparseRoundTripped = roundTrip(sparse, referenceCount);
    assertEquals(populatedCount, sparseRoundTripped.getBitmap().cardinality());
    for (int offset = 0; offset < populatedCount; offset++) {
      assertEquals(2000L + offset, sparseRoundTripped.getOrCreateReference(offset).getKey());
    }
    for (int offset = populatedCount; offset < referenceCount; offset++) {
      assertFalse(sparseRoundTripped.getBitmap().get(offset), "unset slot must remain absent on read");
      assertNotNull(sparseRoundTripped.getOrCreateReference(offset));
      assertTrue(sparseRoundTripped.getBitmap().get(offset));
      assertEquals(offset + 1, sparseRoundTripped.getBitmap().cardinality());
    }
    for (int offset = 0; offset < populatedCount; offset++) {
      assertEquals(2000L + offset, sparseRoundTripped.getOrCreateReference(offset).getKey());
    }
  }

  /** Serialize a {@link BitmapReferencesPage} delegate and read it back via the production ctor. */
  private static BitmapReferencesPage roundTrip(final BitmapReferencesPage page, final int referenceCountForRead) {
    final BytesOut<?> out = Bytes.elasticOffHeapByteBuffer();
    SerializationType.DATA.serializeBitmapReferencesPage(out, page.getReferences(), page.getBitmap());
    final BytesIn<?> in = Bytes.wrapForRead(out.toByteArray());
    return new BitmapReferencesPage(referenceCountForRead, in, SerializationType.DATA);
  }
}
