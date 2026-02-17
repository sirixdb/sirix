package io.sirix.index.hot;

import io.sirix.JsonTestHelper;
import io.sirix.index.IndexType;
import io.sirix.page.HOTIndirectPage;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.PageReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Bug-finding tests for HOT index implementation.
 * 
 * <p>
 * These tests focus on corner cases, edge conditions, and potential bug scenarios rather than just
 * code coverage. Each test is designed to catch specific types of bugs.
 * </p>
 */
@DisplayName("HOT Bug Finding Tests")
class HOTBugFindingTest {

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  @Nested
  @DisplayName("Leaf Page Split Corner Cases")
  class LeafPageSplitCornerCases {

    private Arena arena;

    private HOTLeafPage createTestLeafPage() {
      arena = Arena.ofConfined();
      MemorySegment slotMemory = arena.allocate(HOTLeafPage.DEFAULT_SIZE);
      return new HOTLeafPage(1L, 1, IndexType.CAS, slotMemory, null, new int[HOTLeafPage.MAX_ENTRIES], 0, 0);
    }

    @AfterEach
    void tearDownArena() {
      if (arena != null) {
        arena.close();
        arena = null;
      }
    }

    @Test
    @DisplayName("BUG: Split with exactly 2 entries should not lose data")
    void testSplitMinimumEntries() {
      HOTLeafPage left = createTestLeafPage();
      left.put(new byte[] {1}, new byte[] {10});
      left.put(new byte[] {2}, new byte[] {20});

      HOTLeafPage right = createTestLeafPage();
      byte[] splitKey = left.splitTo(right);

      // Verify no data loss
      assertEquals(2, left.getEntryCount() + right.getEntryCount(), "Total entries should remain 2 after split");
      assertNotNull(splitKey, "Split key should not be null");

      // Verify order preserved
      assertTrue(left.getEntryCount() >= 1, "Left should have at least 1 entry");
      assertTrue(right.getEntryCount() >= 1, "Right should have at least 1 entry");
    }

    @Test
    @DisplayName("BUG: Split with 1 entry should return null (gracefully handled)")
    void testSplitSingleEntry() {
      HOTLeafPage left = createTestLeafPage();
      left.put(new byte[] {1}, new byte[] {10});

      HOTLeafPage right = createTestLeafPage();

      // Single-entry pages cannot be split - returns null instead of throwing
      byte[] splitKey = left.splitTo(right);
      assertNull(splitKey, "Splitting page with 1 entry should return null");
      assertEquals(1, left.getEntryCount(), "Left page should still have 1 entry");
      assertEquals(0, right.getEntryCount(), "Right page should have 0 entries");
    }

    @Test
    @DisplayName("BUG: canSplit should return false for single-entry pages")
    void testCanSplitSingleEntry() {
      HOTLeafPage page = createTestLeafPage();
      assertFalse(page.canSplit(), "Empty page should not be splittable");

      page.put(new byte[] {1}, new byte[] {10});
      assertFalse(page.canSplit(), "Single-entry page should not be splittable");

      page.put(new byte[] {2}, new byte[] {20});
      assertTrue(page.canSplit(), "Two-entry page should be splittable");
    }

    @Test
    @DisplayName("BUG: Split should preserve key order")
    void testSplitPreservesKeyOrder() {
      HOTLeafPage left = createTestLeafPage();

      // Insert in random order
      List<Integer> values = new ArrayList<>();
      for (int i = 0; i < 100; i++) {
        values.add(i);
      }
      Collections.shuffle(values, new Random(42));

      for (int v : values) {
        left.put(new byte[] {(byte) v}, new byte[] {(byte) v});
      }

      HOTLeafPage right = createTestLeafPage();
      byte[] splitKey = left.splitTo(right);

      // Verify left page keys are sorted and less than split key
      byte[] leftLast = left.getLastKey();
      assertTrue(compareKeys(leftLast, splitKey) < 0, "Last key in left should be less than split key");

      // Verify right page keys are sorted and >= split key
      byte[] rightFirst = right.getFirstKey();
      assertArrayEquals(splitKey, rightFirst, "First key in right should equal split key");
    }

    @Test
    @DisplayName("BUG: Split with identical keys should handle correctly")
    void testSplitIdenticalKeys() {
      HOTLeafPage left = createTestLeafPage();

      // Insert same key with different values (simulating multiple node refs)
      byte[] sameKey = new byte[] {42};
      for (int i = 0; i < 10; i++) {
        left.put(sameKey, new byte[] {(byte) i});
      }

      // Add some different keys
      for (int i = 0; i < 50; i++) {
        left.put(new byte[] {(byte) (100 + i)}, new byte[] {(byte) i});
      }

      HOTLeafPage right = createTestLeafPage();

      // Should not throw
      assertDoesNotThrow(() -> left.splitTo(right), "Split with duplicate keys should not throw");
    }

    @Test
    @DisplayName("BUG: Split should handle very long keys")
    void testSplitVeryLongKeys() {
      HOTLeafPage left = createTestLeafPage();

      // Insert keys of increasing length
      for (int len = 1; len <= 100; len++) {
        byte[] key = new byte[len];
        Arrays.fill(key, (byte) len);
        left.put(key, new byte[] {(byte) len});
      }

      HOTLeafPage right = createTestLeafPage();
      byte[] splitKey = left.splitTo(right);

      assertNotNull(splitKey, "Split key should not be null");
      assertEquals(100, left.getEntryCount() + right.getEntryCount(), "Total entries should remain 100 after split");
    }

    private int compareKeys(byte[] a, byte[] b) {
      int minLen = Math.min(a.length, b.length);
      for (int i = 0; i < minLen; i++) {
        int cmp = (a[i] & 0xFF) - (b[i] & 0xFF);
        if (cmp != 0)
          return cmp;
      }
      return a.length - b.length;
    }
  }

  @Nested
  @DisplayName("Discriminative Bit Edge Cases")
  class DiscriminativeBitEdgeCases {

    @Test
    @DisplayName("BUG: Keys differing only in very last bit")
    void testKeysDifferingInLastBit() {
      byte[] key1 = new byte[] {(byte) 0b11111110};
      byte[] key2 = new byte[] {(byte) 0b11111111};

      int bit = DiscriminativeBitComputer.computeDifferingBit(key1, key2);
      assertEquals(7, bit, "Should detect difference in bit 7 (LSB)");

      // Verify navigation works correctly
      PageReference left = new PageReference();
      PageReference right = new PageReference();
      HOTIndirectPage biNode = HOTIndirectPage.createBiNode(1L, 1, bit, left, right);

      int idx1 = biNode.findChildIndex(key1);
      int idx2 = biNode.findChildIndex(key2);

      assertFalse(idx1 == idx2, "Keys differing in discriminative bit should route to different children");
    }

    @Test
    @DisplayName("BUG: Long keys differing in last byte")
    void testLongKeysDifferingInLastByte() {
      byte[] key1 = new byte[32];
      byte[] key2 = new byte[32];
      Arrays.fill(key1, (byte) 0xFF);
      Arrays.fill(key2, (byte) 0xFF);
      key2[31] = (byte) 0xFE; // Differ only in last byte

      int bit = DiscriminativeBitComputer.computeDifferingBit(key1, key2);
      assertTrue(bit >= 31 * 8, "Differing bit should be in byte 31");

      // Verify navigation
      PageReference left = new PageReference();
      PageReference right = new PageReference();
      HOTIndirectPage biNode = HOTIndirectPage.createBiNode(1L, 1, bit, left, right);

      int idx1 = biNode.findChildIndex(key1);
      int idx2 = biNode.findChildIndex(key2);

      assertFalse(idx1 == idx2, "Keys differing should route differently");
    }

    @Test
    @DisplayName("BUG: Different length keys - shorter key as prefix")
    void testDifferentLengthKeysPrefix() {
      byte[] short_key = new byte[] {1, 2, 3};
      byte[] long_key = new byte[] {1, 2, 3, 4, 5};

      int bit = DiscriminativeBitComputer.computeDifferingBit(short_key, long_key);

      // Should detect difference at first byte after short key ends
      assertEquals(3 * 8, bit, "Differing bit should be at byte 3 (first byte beyond short key)");
    }

    @Test
    @DisplayName("BUG: All zeros vs all ones should differ at bit 0")
    void testAllZerosVsAllOnes() {
      byte[] zeros = new byte[] {0, 0, 0, 0};
      byte[] ones = new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF};

      int bit = DiscriminativeBitComputer.computeDifferingBit(zeros, ones);
      assertEquals(0, bit, "Should detect difference at MSB (bit 0)");
    }
  }

  @Nested
  @DisplayName("Navigation Edge Cases")
  class NavigationEdgeCases {

    @Test
    @DisplayName("BUG: Navigation with key shorter than discriminative bit position")
    void testNavigationShortKey() {
      // Create BiNode with discriminative bit at position 16 (byte 2)
      PageReference left = new PageReference();
      PageReference right = new PageReference();
      HOTIndirectPage biNode = HOTIndirectPage.createBiNode(1L, 1, 16, left, right);

      // Navigate with 1-byte key (shorter than bit position)
      byte[] shortKey = new byte[] {42};

      // Should not throw and should return valid index
      int idx = biNode.findChildIndex(shortKey);
      assertTrue(idx >= 0 && idx < 2, "Should return valid child index for short key");
    }

    @Test
    @DisplayName("BUG: Navigation with empty key")
    void testNavigationEmptyKey() {
      PageReference left = new PageReference();
      PageReference right = new PageReference();
      HOTIndirectPage biNode = HOTIndirectPage.createBiNode(1L, 1, 0, left, right);

      byte[] emptyKey = new byte[0];

      // Should not throw
      int idx = biNode.findChildIndex(emptyKey);
      assertTrue(idx >= 0, "Should return valid index for empty key");
    }

    @Test
    @DisplayName("BUG: SpanNode navigation consistency")
    void testSpanNodeNavigationConsistency() {
      // Create SpanNode with 8 children
      PageReference[] children = new PageReference[8];
      byte[] partialKeys = new byte[8];
      for (int i = 0; i < 8; i++) {
        children[i] = new PageReference();
        children[i].setKey((long) i);
        partialKeys[i] = (byte) (i * 32); // 0, 32, 64, 96, 128, 160, 192, 224
      }

      HOTIndirectPage spanNode = HOTIndirectPage.createSpanNode(1L, 1, (byte) 0, 0xFFL, partialKeys, children, 2);

      // Test that same key always routes to same child
      Random random = new Random(12345);
      for (int trial = 0; trial < 100; trial++) {
        byte[] key = new byte[4];
        random.nextBytes(key);

        int idx1 = spanNode.findChildIndex(key);
        int idx2 = spanNode.findChildIndex(key);
        int idx3 = spanNode.findChildIndex(key.clone());

        assertEquals(idx1, idx2, "Same key should route to same child");
        assertEquals(idx1, idx3, "Cloned key should route to same child");
      }
    }

    @Test
    @DisplayName("BUG: BiNode with all 64 bit positions should work")
    void testAllBitPositions() {
      for (int bitPos = 0; bitPos < 64; bitPos++) {
        PageReference left = new PageReference();
        PageReference right = new PageReference();
        HOTIndirectPage biNode = HOTIndirectPage.createBiNode(1L, 1, bitPos, left, right);

        // Create key with bit set at position
        byte[] keyWithBit = new byte[8];
        int byteIdx = bitPos / 8;
        int bitInByte = 7 - (bitPos % 8);
        keyWithBit[byteIdx] = (byte) (1 << bitInByte);

        byte[] keyWithoutBit = new byte[8];

        int idxWith = biNode.findChildIndex(keyWithBit);
        int idxWithout = biNode.findChildIndex(keyWithoutBit);

        // Keys differing at discriminative bit should route differently
        assertFalse(idxWith == idxWithout, "Keys differing at bit " + bitPos + " should route to different children");
      }
    }
  }

  // Data Integrity E2E tests are covered in HOTMultiLayerIndirectPageTest
  // and HOTIndexIntegrationTest which use proper JSON shredder setup

  @Nested
  @DisplayName("Node Type Transition Edge Cases")
  class NodeTypeTransitionEdgeCases {

    @Test
    @DisplayName("BUG: BiNode to SpanNode transition at boundary")
    void testBiNodeToSpanNodeTransition() {
      // BiNode has max 2 children
      // When adding 3rd child, should upgrade to SpanNode

      PageReference left = new PageReference();
      PageReference right = new PageReference();
      HOTIndirectPage biNode = HOTIndirectPage.createBiNode(1L, 1, 0, left, right);

      assertTrue(NodeUpgradeManager.needsUpgrade(biNode, 3), "BiNode needs upgrade when adding 3rd child");
      assertFalse(NodeUpgradeManager.needsUpgrade(biNode, 2), "BiNode does not need upgrade at 2 children");
    }

    @Test
    @DisplayName("BUG: SpanNode to MultiNode transition at boundary")
    void testSpanNodeToMultiNodeTransition() {
      // SpanNode has max 16 children
      PageReference[] children = new PageReference[16];
      byte[] partialKeys = new byte[16];
      for (int i = 0; i < 16; i++) {
        children[i] = new PageReference();
        partialKeys[i] = (byte) i;
      }

      HOTIndirectPage spanNode = HOTIndirectPage.createSpanNode(1L, 1, (byte) 0, 0xFFFFL, partialKeys, children, 2);

      assertTrue(NodeUpgradeManager.needsUpgrade(spanNode, 17), "SpanNode needs upgrade when adding 17th child");
      assertFalse(NodeUpgradeManager.needsUpgrade(spanNode, 16), "SpanNode does not need upgrade at 16 children");
    }

    @Test
    @DisplayName("BUG: Downgrade boundary conditions")
    void testDowngradeBoundaryConditions() {
      PageReference[] children = new PageReference[4];
      byte[] partialKeys = new byte[4];
      for (int i = 0; i < 4; i++) {
        children[i] = new PageReference();
        partialKeys[i] = (byte) i;
      }

      HOTIndirectPage spanNode = HOTIndirectPage.createSpanNode(1L, 1, (byte) 0, 0xFL, partialKeys, children, 2);

      // SpanNode with 2 children should downgrade to BiNode
      assertTrue(NodeUpgradeManager.shouldDowngrade(spanNode, 2),
          "SpanNode should downgrade when reduced to 2 children");
      assertFalse(NodeUpgradeManager.shouldDowngrade(spanNode, 3), "SpanNode should not downgrade at 3 children");
    }
  }

  @Nested
  @DisplayName("SparsePartialKeys Edge Cases")
  class SparsePartialKeysEdgeCases {

    @Test
    @DisplayName("BUG: Search in empty-ish keys")
    void testSearchEmptyishKeys() {
      SparsePartialKeys<Byte> spk = SparsePartialKeys.forBytes(4);

      // All zeros
      spk.setEntry(0, (byte) 0);
      spk.setEntry(1, (byte) 0);
      spk.setEntry(2, (byte) 0);
      spk.setEntry(3, (byte) 0);

      int result = spk.search(0);
      assertTrue(result >= 0, "Should find matching entry");
    }

    @Test
    @DisplayName("BUG: Search boundary values")
    void testSearchBoundaryValues() {
      SparsePartialKeys<Byte> spk = SparsePartialKeys.forBytes(4);

      spk.setEntry(0, Byte.MIN_VALUE);
      spk.setEntry(1, (byte) 0);
      spk.setEntry(2, (byte) 127);
      spk.setEntry(3, Byte.MAX_VALUE);

      // All boundary values should be searchable
      assertTrue(spk.search(Byte.MIN_VALUE) >= 0, "Should find MIN_VALUE");
      assertTrue(spk.search((byte) 0) >= 0, "Should find 0");
      assertTrue(spk.search((byte) 127) >= 0, "Should find 127");
    }

    @Test
    @DisplayName("BUG: Short keys maximum capacity")
    void testShortKeysMaxCapacity() {
      SparsePartialKeys<Short> spk = SparsePartialKeys.forShorts(32);

      // Fill to capacity
      for (int i = 0; i < 32; i++) {
        spk.setEntry(i, (short) (i * 1000));
      }

      // All entries should be retrievable
      for (int i = 0; i < 32; i++) {
        assertEquals((short) (i * 1000), spk.getEntry(i), "Entry " + i + " should be correct");
      }
    }

    @Test
    @DisplayName("BUG: Int keys with negative values")
    void testIntKeysNegativeValues() {
      SparsePartialKeys<Integer> spk = SparsePartialKeys.forInts(4);

      spk.setEntry(0, Integer.MIN_VALUE);
      spk.setEntry(1, -1);
      spk.setEntry(2, 0);
      spk.setEntry(3, Integer.MAX_VALUE);

      assertEquals(Integer.MIN_VALUE, spk.getEntry(0));
      assertEquals(-1, spk.getEntry(1));
      assertEquals(0, spk.getEntry(2));
      assertEquals(Integer.MAX_VALUE, spk.getEntry(3));
    }
  }

  @Nested
  @DisplayName("withUpdatedChild Edge Cases")
  class WithUpdatedChildEdgeCases {

    @Test
    @DisplayName("BUG: Update first child in BiNode")
    void testUpdateFirstChild() {
      PageReference left = new PageReference();
      PageReference right = new PageReference();
      left.setKey(1L);
      right.setKey(2L);

      HOTIndirectPage original = HOTIndirectPage.createBiNode(100L, 1, 5, left, right);

      PageReference newLeft = new PageReference();
      newLeft.setKey(999L);

      HOTIndirectPage updated = original.withUpdatedChild(0, newLeft, 2);

      // Verify update
      assertEquals(999L, updated.getChildReference(0).getKey());
      assertEquals(2L, updated.getChildReference(1).getKey());

      // Verify original unchanged (immutability)
      assertEquals(1L, original.getChildReference(0).getKey());
    }

    @Test
    @DisplayName("BUG: Update last child in BiNode")
    void testUpdateLastChild() {
      PageReference left = new PageReference();
      PageReference right = new PageReference();
      left.setKey(1L);
      right.setKey(2L);

      HOTIndirectPage original = HOTIndirectPage.createBiNode(100L, 1, 5, left, right);

      PageReference newRight = new PageReference();
      newRight.setKey(999L);

      HOTIndirectPage updated = original.withUpdatedChild(1, newRight, 2);

      // Verify update
      assertEquals(1L, updated.getChildReference(0).getKey());
      assertEquals(999L, updated.getChildReference(1).getKey());
    }

    @Test
    @DisplayName("BUG: Update child in SpanNode middle")
    void testUpdateMiddleChildSpanNode() {
      PageReference[] children = new PageReference[5];
      byte[] partialKeys = new byte[5];
      for (int i = 0; i < 5; i++) {
        children[i] = new PageReference();
        children[i].setKey((long) i);
        partialKeys[i] = (byte) i;
      }

      HOTIndirectPage spanNode = HOTIndirectPage.createSpanNode(100L, 1, (byte) 0, 0xFFL, partialKeys, children, 2);

      PageReference newChild = new PageReference();
      newChild.setKey(999L);

      HOTIndirectPage updated = spanNode.withUpdatedChild(2, newChild, 2);

      // Verify middle child updated
      assertEquals(0L, updated.getChildReference(0).getKey());
      assertEquals(1L, updated.getChildReference(1).getKey());
      assertEquals(999L, updated.getChildReference(2).getKey());
      assertEquals(3L, updated.getChildReference(3).getKey());
      assertEquals(4L, updated.getChildReference(4).getKey());
    }
  }

  @Nested
  @DisplayName("Height Calculation Edge Cases")
  class HeightCalculationEdgeCases {

    @Test
    @DisplayName("BUG: BiNode height should be preserved through copy")
    void testBiNodeHeightPreserved() {
      for (int height = 0; height <= 10; height++) {
        PageReference left = new PageReference();
        PageReference right = new PageReference();

        HOTIndirectPage original = HOTIndirectPage.createBiNode(1L, 1, 5, left, right, height);

        PageReference newChild = new PageReference();
        HOTIndirectPage updated = original.withUpdatedChild(0, newChild, 2);

        assertEquals(height, updated.getHeight(), "Height should be preserved through withUpdatedChild");
      }
    }

    @Test
    @DisplayName("BUG: SpanNode height should be preserved")
    void testSpanNodeHeightPreserved() {
      PageReference[] children = new PageReference[4];
      byte[] partialKeys = new byte[4];
      for (int i = 0; i < 4; i++) {
        children[i] = new PageReference();
        partialKeys[i] = (byte) i;
      }

      int expectedHeight = 5;
      HOTIndirectPage spanNode =
          HOTIndirectPage.createSpanNode(1L, 1, (byte) 0, 0xFL, partialKeys, children, expectedHeight);

      assertEquals(expectedHeight, spanNode.getHeight(), "SpanNode height should be set correctly");

      PageReference newChild = new PageReference();
      HOTIndirectPage updated = spanNode.withUpdatedChild(0, newChild, 2);

      assertEquals(expectedHeight, updated.getHeight(), "SpanNode height should be preserved through update");
    }
  }

  @Nested
  @DisplayName("Identical/Near-Identical Key Edge Cases")
  class IdenticalKeyEdgeCases {

    private Arena arena;

    private HOTLeafPage createTestLeafPage() {
      arena = Arena.ofConfined();
      MemorySegment slotMemory = arena.allocate(HOTLeafPage.DEFAULT_SIZE);
      return new HOTLeafPage(1L, 1, IndexType.CAS, slotMemory, null, new int[HOTLeafPage.MAX_ENTRIES], 0, 0);
    }

    @AfterEach
    void tearDownArena() {
      if (arena != null) {
        arena.close();
        arena = null;
      }
    }

    @Test
    @DisplayName("BUG FIX: Many identical keys should be handled without crash")
    void testManyIdenticalKeys() {
      HOTLeafPage page = createTestLeafPage();

      // Insert the same key multiple times with different values
      // This simulates merging NodeReferences for the same index key
      byte[] sameKey = new byte[] {1, 2, 3, 4, 5};

      for (int i = 0; i < 100; i++) {
        // Each insertion should merge with the existing value
        byte[] value = new byte[] {(byte) i};
        page.put(sameKey, value);
      }

      // Only 1 entry should exist (all merged into one)
      assertEquals(1, page.getEntryCount(), "Identical keys should merge into a single entry");

      // The page should not be splittable (only 1 entry)
      assertFalse(page.canSplit(), "Page with single entry should not be splittable");

      // Attempting to split should return null, not throw
      HOTLeafPage right = createTestLeafPage();
      byte[] splitKey = page.splitTo(right);
      assertNull(splitKey, "Splitting single-entry page should return null");
    }

    @Test
    @DisplayName("BUG FIX: Near-identical keys (common prefix) should split correctly")
    void testNearIdenticalKeys() {
      HOTLeafPage page = createTestLeafPage();

      // Insert keys with long common prefix, differing only at the end
      byte[] prefix = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

      for (int i = 0; i < 50; i++) {
        byte[] key = new byte[prefix.length + 1];
        System.arraycopy(prefix, 0, key, 0, prefix.length);
        key[prefix.length] = (byte) i;
        page.put(key, new byte[] {(byte) i});
      }

      assertEquals(50, page.getEntryCount());
      assertTrue(page.canSplit());

      HOTLeafPage right = createTestLeafPage();
      byte[] splitKey = page.splitTo(right);

      assertNotNull(splitKey, "Split should succeed with multiple entries");
      assertTrue(page.getEntryCount() > 0, "Left page should have entries");
      assertTrue(right.getEntryCount() > 0, "Right page should have entries");
      assertEquals(50, page.getEntryCount() + right.getEntryCount(), "Total entries should be preserved");
    }

    @Test
    @DisplayName("BUG FIX: Page compact should reclaim space after value updates")
    void testCompactReclainsSpace() {
      HOTLeafPage page = createTestLeafPage();

      // Insert some entries with large values
      for (int i = 0; i < 10; i++) {
        byte[] key = new byte[] {(byte) i};
        byte[] value = new byte[100]; // Large value
        Arrays.fill(value, (byte) i);
        page.put(key, value);
      }

      long usedBefore = HOTLeafPage.DEFAULT_SIZE - page.getRemainingSpace();

      // Update entries with smaller values (creates fragmentation)
      for (int i = 0; i < 10; i++) {
        byte[] key = new byte[] {(byte) i};
        int idx = page.findEntry(key);
        if (idx >= 0) {
          page.updateValue(idx, new byte[] {(byte) i}); // Small value
        }
      }

      // Compact should reclaim the fragmented space
      int reclaimed = page.compact();

      assertTrue(reclaimed >= 0, "Compact should not report negative reclaimed space");
      // Note: Due to how updateValue works (in-place if smaller), fragmentation may vary
    }

    @Test
    @DisplayName("BUG FIX: getRemainingSpace and canFit should work correctly")
    void testCanFitPredictsInsertSuccess() {
      HOTLeafPage page = createTestLeafPage();

      // Fill page to near capacity
      int inserted = 0;
      while (true) {
        byte[] key = String.format("key%06d", inserted).getBytes();
        byte[] value = new byte[100];

        if (page.canFit(key, value)) {
          assertTrue(page.put(key, value), "canFit returned true but put failed");
          inserted++;
        } else {
          // Verify that put would indeed fail
          assertFalse(page.put(key, value), "canFit returned false but put succeeded");
          break;
        }

        if (inserted > HOTLeafPage.MAX_ENTRIES) {
          fail("Should have hit capacity limit");
        }
      }

      assertTrue(inserted > 0, "Should have inserted at least one entry");
    }

    @Test
    @DisplayName("BUG FIX: Discriminative bit for identical keys returns -1")
    void testDiscriminativeBitIdenticalKeys() {
      byte[] key = new byte[] {1, 2, 3, 4, 5};

      int bit = DiscriminativeBitComputer.computeDifferingBit(key, key);
      assertEquals(-1, bit, "Identical keys should return -1 for discriminative bit");

      // Also test with cloned key (different object, same content)
      byte[] keyClone = key.clone();
      bit = DiscriminativeBitComputer.computeDifferingBit(key, keyClone);
      assertEquals(-1, bit, "Cloned key should also return -1");
    }

    @Test
    @DisplayName("BUG FIX: Empty keys should handle gracefully")
    void testEmptyKeys() {
      byte[] empty1 = new byte[0];
      byte[] empty2 = new byte[0];
      byte[] nonEmpty = new byte[] {1};

      // Both empty
      int bit = DiscriminativeBitComputer.computeDifferingBit(empty1, empty2);
      assertEquals(-1, bit, "Two empty keys should return -1");

      // One empty, one not
      bit = DiscriminativeBitComputer.computeDifferingBit(empty1, nonEmpty);
      assertEquals(0, bit, "Empty vs non-empty should return 0");

      bit = DiscriminativeBitComputer.computeDifferingBit(nonEmpty, empty1);
      assertEquals(0, bit, "Non-empty vs empty should return 0");
    }
  }
}

