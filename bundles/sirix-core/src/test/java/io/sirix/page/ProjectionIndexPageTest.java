/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import io.sirix.page.delegates.ReferencesPage4;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/** Structural tests for {@link ProjectionIndexPage}. */
final class ProjectionIndexPageTest {

  @Test
  void defaultCtor_emptyCounters() {
    final ProjectionIndexPage page = new ProjectionIndexPage();
    assertEquals(0, page.getMaxNodeKeySize());
    assertEquals(0, page.getMaxHotPageKeySize());
    assertEquals(0, page.getCurrentMaxLevelOfIndirectPagesSize());
  }

  @Test
  void maxNodeKey_incrementPerIndex() {
    final ProjectionIndexPage page = new ProjectionIndexPage();
    assertEquals(0L, page.getMaxNodeKey(0));
    assertEquals(1L, page.incrementAndGetMaxNodeKey(0));
    assertEquals(2L, page.incrementAndGetMaxNodeKey(0));
    assertEquals(1L, page.incrementAndGetMaxNodeKey(1));
    assertEquals(2L, page.getMaxNodeKey(0));
    assertEquals(1L, page.getMaxNodeKey(1));
    assertEquals(2, page.getMaxNodeKeySize());
  }

  @Test
  void maxHotPageKey_independentFromNodeKey() {
    final ProjectionIndexPage page = new ProjectionIndexPage();
    assertEquals(1L, page.incrementAndGetMaxHotPageKey(0));
    assertEquals(2L, page.incrementAndGetMaxHotPageKey(0));
    assertEquals(2L, page.getMaxHotPageKey(0));
    assertEquals(0L, page.getMaxNodeKey(0), "node- and hot-page counters must not alias");
  }

  @Test
  void currentMaxLevelsOfIndirectPages_incrementAndRead() {
    final ProjectionIndexPage page = new ProjectionIndexPage();
    assertEquals(0, page.getCurrentMaxLevelOfIndirectPages(0));
    assertEquals(1, page.incrementAndGetCurrentMaxLevelOfIndirectPages(0));
    assertEquals(2, page.incrementAndGetCurrentMaxLevelOfIndirectPages(0));
    assertEquals(1, page.incrementAndGetCurrentMaxLevelOfIndirectPages(1));
    assertEquals(2, page.getCurrentMaxLevelOfIndirectPages(0));
    assertEquals(1, page.getCurrentMaxLevelOfIndirectPages(1));
    assertEquals(2, page.getCurrentMaxLevelOfIndirectPagesSize());
  }

  @Test
  void deserializationCtor_preservesState() {
    final Int2LongOpenHashMap maxNodeKeys = new Int2LongOpenHashMap();
    maxNodeKeys.put(0, 42L);
    maxNodeKeys.put(1, 100L);
    final Int2LongOpenHashMap maxHotPageKeys = new Int2LongOpenHashMap();
    maxHotPageKeys.put(0, 7L);
    final Int2IntOpenHashMap maxLevels = new Int2IntOpenHashMap();
    maxLevels.put(0, 3);

    final ProjectionIndexPage page = new ProjectionIndexPage(
        new ReferencesPage4(), maxNodeKeys, maxHotPageKeys, maxLevels);

    assertEquals(42L, page.getMaxNodeKey(0));
    assertEquals(100L, page.getMaxNodeKey(1));
    assertEquals(2, page.getMaxNodeKeySize());
    assertEquals(7L, page.getMaxHotPageKey(0));
    assertEquals(1, page.getMaxHotPageKeySize());
    assertEquals(3, page.getCurrentMaxLevelOfIndirectPages(0));
    assertEquals(1, page.getCurrentMaxLevelOfIndirectPagesSize());
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
  void revisionRoot_exposesProjectionPage() {
    final RevisionRootPage root = new RevisionRootPage();
    final PageReference ref = root.getProjectionIndexPageReference();
    assertNotNull(ref);
    assertNotNull(ref.getPage(), "fresh revision must seed a ProjectionIndexPage");
    assertEquals(ProjectionIndexPage.class, ref.getPage().getClass(),
        "projection reference must hold a ProjectionIndexPage, not some other container");
  }
}
