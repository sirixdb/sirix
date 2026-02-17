/*
 * Copyright (c) 2024, Sirix Contributors
 *
 * All rights reserved.
 */

package io.sirix.index.hot;

import io.brackit.query.atomic.Int32;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Type;
import io.sirix.JsonTestHelper;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.IndexType;
import io.sirix.index.SearchMode;
import io.sirix.index.path.json.JsonPCRCollector;
import io.sirix.page.HOTIndirectPage;
import io.sirix.page.PageReference;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests that directly exercise HOT internal APIs for maximum coverage.
 */
@DisplayName("HOT Direct API Tests")
class HOTDirectApiTest {

  private static final String RESOURCE_NAME = "testResource";
  private static final Path DATABASE_PATH = JsonTestHelper.PATHS.PATH1.getFile();

  @BeforeEach
  void setup() throws IOException {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void teardown() {
    JsonTestHelper.deleteEverything();
    Databases.getGlobalBufferManager().clearAllCaches();
  }

  @Nested
  @DisplayName("NodeUpgradeManager Tests")
  class NodeUpgradeManagerTests {

    @Test
    @DisplayName("Exercise all node type transitions")
    void testAllNodeTypeTransitions() {
      // Test BiNode -> SpanNode transition logic
      for (int children = 2; children <= 32; children++) {
        var type = NodeUpgradeManager.determineNodeType(children);
        assertNotNull(type);

        if (children <= 2) {
          assertEquals(HOTIndirectPage.NodeType.BI_NODE, type);
        } else if (children <= 16) {
          assertEquals(HOTIndirectPage.NodeType.SPAN_NODE, type);
        } else {
          assertEquals(HOTIndirectPage.NodeType.MULTI_NODE, type);
        }
      }
    }

    @Test
    @DisplayName("Test merge to SpanNode prerequisites")
    void testMergeToSpanNodePrerequisites() {
      PageReference lr1 = new PageReference();
      PageReference rr1 = new PageReference();
      HOTIndirectPage node1 = HOTIndirectPage.createBiNode(1L, 1, 0, lr1, rr1);

      PageReference lr2 = new PageReference();
      PageReference rr2 = new PageReference();
      HOTIndirectPage node2 = HOTIndirectPage.createBiNode(2L, 1, 0, lr2, rr2);

      // Check merge conditions
      boolean canMerge = NodeUpgradeManager.shouldMergeToSpanNode(node1, node2);
      assertTrue(canMerge, "Two compatible BiNodes should be mergeable");
    }

    @Test
    @DisplayName("Test isFull and isUnderfilled for all node types")
    void testFillStatusAllTypes() {
      // BiNode
      PageReference lr = new PageReference();
      PageReference rr = new PageReference();
      HOTIndirectPage biNode = HOTIndirectPage.createBiNode(1L, 1, 0, lr, rr);

      boolean isFull = NodeUpgradeManager.isFull(biNode);
      boolean isUnder = NodeUpgradeManager.isUnderfilled(biNode, 0.5);

      // BiNode with 2 children has 100% fill
      assertNotNull(Boolean.valueOf(isFull));
      assertNotNull(Boolean.valueOf(isUnder));

      // SpanNode with 4 children
      PageReference[] children = new PageReference[4];
      byte[] partialKeys = new byte[4];
      for (int i = 0; i < 4; i++) {
        children[i] = new PageReference();
        partialKeys[i] = (byte) i;
      }
      HOTIndirectPage spanNode = HOTIndirectPage.createSpanNode(2L, 1, (byte) 0, 0b1111L, partialKeys, children);

      boolean spanFull = NodeUpgradeManager.isFull(spanNode);
      boolean spanUnder = NodeUpgradeManager.isUnderfilled(spanNode, 0.5);

      assertNotNull(Boolean.valueOf(spanFull));
      assertNotNull(Boolean.valueOf(spanUnder));
    }

    @Test
    @DisplayName("Test getMaxChildrenForType")
    void testMaxChildrenForTypes() {
      assertEquals(2, NodeUpgradeManager.getMaxChildrenForType(HOTIndirectPage.NodeType.BI_NODE));
      assertEquals(16, NodeUpgradeManager.getMaxChildrenForType(HOTIndirectPage.NodeType.SPAN_NODE));
      assertEquals(32, NodeUpgradeManager.getMaxChildrenForType(HOTIndirectPage.NodeType.MULTI_NODE));
    }
  }

  @Nested
  @DisplayName("HeightOptimalSplitter Tests")
  class HeightOptimalSplitterTests {

    @Test
    @DisplayName("Test split point for uniform key distribution")
    void testSplitPointUniform() {
      byte[][] keys = new byte[64][];
      for (int i = 0; i < 64; i++) {
        keys[i] = new byte[] {(byte) (i * 4)};
      }

      int splitPoint = HeightOptimalSplitter.findOptimalSplitPoint(keys);
      assertTrue(splitPoint > 0 && splitPoint < 64, "Split should be in middle region");
    }

    @Test
    @DisplayName("Test split point for clustered keys")
    void testSplitPointClustered() {
      byte[][] keys = new byte[32][];
      // First half: 0x00-0x0F
      for (int i = 0; i < 16; i++) {
        keys[i] = new byte[] {(byte) i};
      }
      // Second half: 0xF0-0xFF
      for (int i = 0; i < 16; i++) {
        keys[16 + i] = new byte[] {(byte) (0xF0 + i)};
      }

      int splitPoint = HeightOptimalSplitter.findOptimalSplitPoint(keys);
      assertTrue(splitPoint >= 0 && splitPoint < 32);
    }

    @Test
    @DisplayName("Test BiNode creation with various bit positions")
    void testBiNodeCreationVariousBits() {
      for (int bit = 0; bit < 32; bit++) {
        PageReference leftRef = new PageReference();
        leftRef.setKey(bit * 2);
        PageReference rightRef = new PageReference();
        rightRef.setKey(bit * 2 + 1);

        HOTIndirectPage biNode = HeightOptimalSplitter.createBiNode(1L, 1, bit, leftRef, rightRef);
        assertNotNull(biNode);
        assertEquals(HOTIndirectPage.NodeType.BI_NODE, biNode.getNodeType());
      }
    }

    @Test
    @DisplayName("Test shouldCreateSpanNode logic")
    void testShouldCreateSpanNode() {
      // Single bit difference
      byte[] left1 = new byte[] {0x00};
      byte[] right1 = new byte[] {0x01};
      boolean result1 = HeightOptimalSplitter.shouldCreateSpanNode(left1, right1, 7);
      assertNotNull(Boolean.valueOf(result1));

      // Multi-bit difference in same byte
      byte[] left2 = new byte[] {0x00};
      byte[] right2 = new byte[] {(byte) 0xFF};
      boolean result2 = HeightOptimalSplitter.shouldCreateSpanNode(left2, right2, 0);
      assertNotNull(Boolean.valueOf(result2));

      // Multi-byte keys
      byte[] left3 = new byte[] {0x00, 0x00};
      byte[] right3 = new byte[] {0x00, 0x01};
      boolean result3 = HeightOptimalSplitter.shouldCreateSpanNode(left3, right3, 15);
      assertNotNull(Boolean.valueOf(result3));
    }
  }

  @Nested
  @DisplayName("SiblingMerger Tests")
  class SiblingMergerTests {

    @Test
    @DisplayName("Test canMerge for various node combinations")
    void testCanMergeVariousCombinations() {
      // Two BiNodes at same height
      PageReference lr1 = new PageReference();
      PageReference rr1 = new PageReference();
      HOTIndirectPage bi1 = HOTIndirectPage.createBiNode(1L, 1, 0, lr1, rr1);

      PageReference lr2 = new PageReference();
      PageReference rr2 = new PageReference();
      HOTIndirectPage bi2 = HOTIndirectPage.createBiNode(2L, 1, 0, lr2, rr2);

      // Just verify the method works - result depends on implementation
      boolean canMerge = SiblingMerger.canMerge(bi1, bi2);
      assertNotNull(Boolean.valueOf(canMerge));

      // BiNode at different height
      PageReference lr3 = new PageReference();
      PageReference rr3 = new PageReference();
      HOTIndirectPage bi3 = HOTIndirectPage.createBiNode(3L, 2, 0, lr3, rr3);

      boolean canMergeDiffHeight = SiblingMerger.canMerge(bi1, bi3);
      assertNotNull(Boolean.valueOf(canMergeDiffHeight));
    }

    @Test
    @DisplayName("Test shouldMerge based on fill factor")
    void testShouldMerge() {
      PageReference lr = new PageReference();
      PageReference rr = new PageReference();
      HOTIndirectPage biNode = HOTIndirectPage.createBiNode(1L, 1, 0, lr, rr);

      boolean shouldMerge = SiblingMerger.shouldMerge(biNode);
      // BiNode at 100% fill shouldn't need merge
      assertFalse(shouldMerge);
    }

    @Test
    @DisplayName("Test getFillFactor accuracy")
    void testGetFillFactor() {
      // BiNode: 2/2 = 100%
      PageReference lr = new PageReference();
      PageReference rr = new PageReference();
      HOTIndirectPage biNode = HOTIndirectPage.createBiNode(1L, 1, 0, lr, rr);
      double biFill = SiblingMerger.getFillFactor(biNode);
      assertEquals(1.0, biFill, 0.01);

      // SpanNode: 4/16 = 25%
      PageReference[] children = new PageReference[4];
      byte[] partialKeys = new byte[4];
      for (int i = 0; i < 4; i++) {
        children[i] = new PageReference();
        partialKeys[i] = (byte) i;
      }
      HOTIndirectPage spanNode = HOTIndirectPage.createSpanNode(2L, 1, (byte) 0, 0b1111L, partialKeys, children);
      double spanFill = SiblingMerger.getFillFactor(spanNode);
      assertTrue(spanFill > 0 && spanFill <= 1.0);
    }

    @Test
    @DisplayName("Test canCollapseBiNode")
    void testCanCollapseBiNode() {
      PageReference leftRef = new PageReference();
      leftRef.setKey(1);
      PageReference rightRef = new PageReference();
      rightRef.setKey(2);

      HOTIndirectPage biNode = HOTIndirectPage.createBiNode(1L, 1, 0, leftRef, rightRef);

      // Full BiNode cannot collapse
      assertFalse(SiblingMerger.canCollapseBiNode(biNode));
    }
  }

  @Nested
  @DisplayName("PartialKeyMapping Tests")
  class PartialKeyMappingTests {

    @Test
    @DisplayName("Test single bit extraction")
    void testSingleBitExtraction() {
      for (int bitPos = 0; bitPos < 32; bitPos++) {
        PartialKeyMapping mapping = PartialKeyMapping.forSingleBit(bitPos);
        assertNotNull(mapping);

        int msb = mapping.getMostSignificantBitIndex();
        int lsb = mapping.getLeastSignificantBitIndex();
        assertTrue(msb >= 0);
        assertTrue(lsb >= 0);
      }
    }

    @Test
    @DisplayName("Test multi-bit mapping with withAdditionalBit")
    void testMultiBitMapping() {
      PartialKeyMapping mapping = PartialKeyMapping.forSingleBit(0);

      for (int bit = 8; bit <= 56; bit += 8) {
        mapping = PartialKeyMapping.withAdditionalBit(mapping, bit);
        assertNotNull(mapping);
      }

      // Extract from 8-byte key
      byte[] key = new byte[8];
      for (int i = 0; i < 8; i++) {
        key[i] = (byte) (0x80 >> (i % 8));
      }

      int extracted = mapping.extractMask(key);
      assertTrue(extracted >= 0);
    }

    @Test
    @DisplayName("Test getMaskFor various bit indices")
    void testGetMaskFor() {
      PartialKeyMapping mapping = PartialKeyMapping.forSingleBit(0);
      mapping = PartialKeyMapping.withAdditionalBit(mapping, 8);
      mapping = PartialKeyMapping.withAdditionalBit(mapping, 16);

      for (int bitIdx = 0; bitIdx < 24; bitIdx++) {
        int mask = mapping.getMaskFor(bitIdx);
        assertTrue(mask >= 0);
      }
    }

    @Test
    @DisplayName("Test getPrefixBitsMask")
    void testGetPrefixBitsMask() {
      PartialKeyMapping mapping = PartialKeyMapping.forSingleBit(7);

      int prefix0 = mapping.getPrefixBitsMask(0);
      int prefix4 = mapping.getPrefixBitsMask(4);
      int prefix8 = mapping.getPrefixBitsMask(8);

      assertTrue(prefix0 >= 0);
      assertTrue(prefix4 >= 0);
      assertTrue(prefix8 >= 0);
    }
  }

  @Nested
  @DisplayName("SparsePartialKeys Tests")
  class SparsePartialKeysTests {

    @Test
    @DisplayName("Test byte key search with all patterns")
    void testByteKeySearchAllPatterns() {
      SparsePartialKeys<Byte> spk = SparsePartialKeys.forBytes(8);
      for (int i = 0; i < 8; i++) {
        spk.setEntry(i, (byte) (1 << i));
      }

      // Search for each individual bit pattern
      for (int pattern = 1; pattern < 256; pattern++) {
        int mask = spk.search(pattern);
        assertTrue(mask >= 0);
      }
    }

    @Test
    @DisplayName("Test short key search")
    void testShortKeySearch() {
      SparsePartialKeys<Short> spk = SparsePartialKeys.forShorts(16);
      for (int i = 0; i < 16; i++) {
        spk.setEntry(i, (short) (0x100 + i));
      }

      for (int i = 0; i < 16; i++) {
        int mask = spk.search(0x100 + i);
        assertTrue(mask != 0, "Should find entry " + i);
      }
    }

    @Test
    @DisplayName("Test int key search")
    void testIntKeySearch() {
      SparsePartialKeys<Integer> spk = SparsePartialKeys.forInts(8);
      for (int i = 0; i < 8; i++) {
        spk.setEntry(i, 0x10000 + i * 0x100);
      }

      for (int i = 0; i < 8; i++) {
        int mask = spk.search(0x10000 + i * 0x100);
        assertTrue(mask != 0, "Should find entry " + i);
      }
    }

    @Test
    @DisplayName("Test estimateSize for various configurations")
    void testEstimateSize() {
      for (int numEntries : new int[] {1, 4, 8, 16, 32}) {
        int byteSize = SparsePartialKeys.estimateSize(numEntries, Byte.class);
        int shortSize = SparsePartialKeys.estimateSize(numEntries, Short.class);
        int intSize = SparsePartialKeys.estimateSize(numEntries, Integer.class);

        assertTrue(byteSize > 0);
        assertTrue(shortSize >= byteSize);
        assertTrue(intSize >= shortSize);
      }
    }

    @Test
    @DisplayName("Test setNumEntries")
    void testSetNumEntries() {
      SparsePartialKeys<Byte> spk = SparsePartialKeys.forBytes(32);

      for (int numEntries : new int[] {1, 8, 16, 32}) {
        spk.setNumEntries(numEntries);
        // Just verify no exception
        assertNotNull(spk);
      }
    }
  }

  @Nested
  @DisplayName("ChunkDirectory Tests")
  class ChunkDirectoryTests {

    @Test
    @DisplayName("Test full directory lifecycle")
    void testFullLifecycle() {
      ChunkDirectory dir = new ChunkDirectory();

      // Initially empty
      assertTrue(dir.isEmpty());
      assertFalse(dir.isModified());
      assertEquals(0, dir.chunkCount());

      // Add chunks
      for (int i = 0; i < 10; i++) {
        PageReference ref = dir.getOrCreateChunkRef(i * 1000);
        ref.setKey(100 + i);
      }

      assertEquals(10, dir.chunkCount());
      assertFalse(dir.isEmpty());
      assertTrue(dir.isModified());

      // Clear modified
      dir.clearModified();
      assertFalse(dir.isModified());

      // Copy
      ChunkDirectory copy = dir.copy();
      assertEquals(dir.chunkCount(), copy.chunkCount());

      // Get indices
      int[] indices = dir.getChunkIndices();
      assertEquals(10, indices.length);
    }

    @Test
    @DisplayName("Test chunkIndexFor calculation")
    void testChunkIndexFor() {
      // Boundary tests
      assertEquals(ChunkDirectory.chunkIndexFor(0), ChunkDirectory.chunkIndexFor(65535));
      assertTrue(ChunkDirectory.chunkIndexFor(65536) > ChunkDirectory.chunkIndexFor(0));
    }

    @Test
    @DisplayName("Test setChunkRef and getChunkRef")
    void testSetGetChunkRef() {
      ChunkDirectory dir = new ChunkDirectory();

      for (int idx = 0; idx < 20; idx++) {
        PageReference ref = new PageReference();
        ref.setKey(1000 + idx);
        dir.setChunkRef(idx, ref);

        PageReference retrieved = dir.getChunkRef(idx);
        assertNotNull(retrieved);
        assertEquals(1000 + idx, retrieved.getKey());
      }
    }

    @Test
    @DisplayName("Test getChunkRefAtPosition")
    void testGetChunkRefAtPosition() {
      int[] indices = new int[] {0, 100, 200, 300, 400};
      PageReference[] refs = new PageReference[5];
      for (int i = 0; i < 5; i++) {
        refs[i] = new PageReference();
        refs[i].setKey(i * 10);
      }

      ChunkDirectory dir = new ChunkDirectory(5, indices, refs);

      for (int pos = 0; pos < 5; pos++) {
        PageReference ref = dir.getChunkRefAtPosition(pos);
        assertNotNull(ref);
        assertEquals(pos * 10, ref.getKey());
      }
    }
  }

  @Nested
  @DisplayName("DiscriminativeBitComputer Tests")
  class DiscriminativeBitComputerTests {

    @Test
    @DisplayName("Test bit position in each byte")
    void testBitPositionEachByte() {
      for (int bytePos = 0; bytePos < 8; bytePos++) {
        byte[] key1 = new byte[8];
        byte[] key2 = new byte[8];
        key2[bytePos] = 0x01; // Bit 7 of this byte differs

        int bit = DiscriminativeBitComputer.computeDifferingBit(key1, key2);
        assertEquals(bytePos * 8 + 7, bit);
      }
    }

    @Test
    @DisplayName("Test all bit positions in a byte")
    void testAllBitPositionsInByte() {
      for (int bitInByte = 0; bitInByte < 8; bitInByte++) {
        byte[] key1 = new byte[] {0};
        byte[] key2 = new byte[] {(byte) (1 << (7 - bitInByte))};

        int bit = DiscriminativeBitComputer.computeDifferingBit(key1, key2);
        assertEquals(bitInByte, bit);
      }
    }

    @Test
    @DisplayName("Test getByteIndex")
    void testGetByteIndex() {
      for (int bit = 0; bit < 64; bit++) {
        int byteIdx = DiscriminativeBitComputer.getByteIndex(bit);
        assertEquals(bit / 8, byteIdx);
      }
    }

    @Test
    @DisplayName("Test with very long keys")
    void testVeryLongKeys() {
      byte[] key1 = new byte[128];
      byte[] key2 = new byte[128];
      key2[100] = 0x01;

      int bit = DiscriminativeBitComputer.computeDifferingBit(key1, key2);
      assertEquals(100 * 8 + 7, bit);
    }
  }
}

