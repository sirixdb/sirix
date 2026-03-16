package io.sirix.page;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Unit tests for {@link VectorPage}.
 */
class VectorPageTest {

  @Test
  void testDefaultConstruction() {
    final VectorPage vectorPage = new VectorPage();
    assertNotNull(vectorPage);
    assertEquals(0, vectorPage.getMaxNodeKeySize());
    assertEquals(0, vectorPage.getCurrentMaxLevelOfIndirectPagesSize());
  }

  @Test
  void testMaxNodeKeyGetAndIncrement() {
    final VectorPage vectorPage = new VectorPage();

    // Initially should return 0 (default for missing key in Int2LongOpenHashMap)
    assertEquals(0L, vectorPage.getMaxNodeKey(0));

    // Increment from default (0) -> 1
    final long newKey = vectorPage.incrementAndGetMaxNodeKey(0);
    assertEquals(1L, newKey);
    assertEquals(1L, vectorPage.getMaxNodeKey(0));

    // Increment again -> 2
    assertEquals(2L, vectorPage.incrementAndGetMaxNodeKey(0));
    assertEquals(2L, vectorPage.getMaxNodeKey(0));
  }

  @Test
  void testMaxNodeKeyMultipleIndexes() {
    final VectorPage vectorPage = new VectorPage();

    vectorPage.incrementAndGetMaxNodeKey(0);
    vectorPage.incrementAndGetMaxNodeKey(0);
    vectorPage.incrementAndGetMaxNodeKey(1);

    assertEquals(2L, vectorPage.getMaxNodeKey(0));
    assertEquals(1L, vectorPage.getMaxNodeKey(1));
    assertEquals(2, vectorPage.getMaxNodeKeySize());
  }

  @Test
  void testCurrentMaxLevelOfIndirectPagesGetAndIncrement() {
    final VectorPage vectorPage = new VectorPage();

    // Initially 0
    assertEquals(0, vectorPage.getCurrentMaxLevelOfIndirectPages(0));

    // Increment
    final int newLevel = vectorPage.incrementAndGetCurrentMaxLevelOfIndirectPages(0);
    assertEquals(1, newLevel);
    assertEquals(1, vectorPage.getCurrentMaxLevelOfIndirectPages(0));

    // Increment again
    assertEquals(2, vectorPage.incrementAndGetCurrentMaxLevelOfIndirectPages(0));
  }

  @Test
  void testCurrentMaxLevelOfIndirectPagesSize() {
    final VectorPage vectorPage = new VectorPage();
    assertEquals(0, vectorPage.getCurrentMaxLevelOfIndirectPagesSize());

    vectorPage.incrementAndGetCurrentMaxLevelOfIndirectPages(0);
    assertEquals(1, vectorPage.getCurrentMaxLevelOfIndirectPagesSize());

    vectorPage.incrementAndGetCurrentMaxLevelOfIndirectPages(1);
    assertEquals(2, vectorPage.getCurrentMaxLevelOfIndirectPagesSize());
  }

  @Test
  void testDeserializationConstructor() {
    final var delegate = new io.sirix.page.delegates.ReferencesPage4();
    final var maxNodeKeys = new Int2LongOpenHashMap();
    maxNodeKeys.put(0, 42L);
    maxNodeKeys.put(1, 100L);
    final var maxLevels = new Int2IntOpenHashMap();
    maxLevels.put(0, 3);

    final VectorPage vectorPage = new VectorPage(delegate, maxNodeKeys, maxLevels);

    assertEquals(42L, vectorPage.getMaxNodeKey(0));
    assertEquals(100L, vectorPage.getMaxNodeKey(1));
    assertEquals(2, vectorPage.getMaxNodeKeySize());
    assertEquals(3, vectorPage.getCurrentMaxLevelOfIndirectPages(0));
    assertEquals(1, vectorPage.getCurrentMaxLevelOfIndirectPagesSize());
  }

  @Test
  void testPageKindVectorPageMapping() {
    // Verify VECTORPAGE has the expected byte ID
    assertEquals((byte) 15, PageKind.VECTORPAGE.getID());
    // Verify VECTORPAGE is a valid enum constant
    assertNotNull(PageKind.valueOf("VECTORPAGE"));
  }

  @Test
  void testGetIndirectPageReference() {
    final VectorPage vectorPage = new VectorPage();
    final PageReference ref = vectorPage.getIndirectPageReference(0);
    assertNotNull(ref);
  }

  @Test
  void testToString() {
    final VectorPage vectorPage = new VectorPage();
    final String str = vectorPage.toString();
    assertNotNull(str);
  }
}
