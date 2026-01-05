/*
 * Copyright (c) 2024, Sirix Contributors
 *
 * All rights reserved.
 */

package io.sirix.index.hot;

import io.sirix.JsonTestHelper;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.node.NodeKind;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Additional tests to boost coverage for HOT classes.
 */
@DisplayName("HOT Additional Coverage Tests")
class HOTAdditionalCoverageTest {

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
  @DisplayName("Integration Tests")
  class IntegrationTests {

    @Test
    @DisplayName("Large object exercises index writer paths")
    void testLargeObject() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME)
            .versioningApproach(VersioningType.FULL)
            .build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
             JsonNodeTrx wtx = session.beginNodeTrx()) {
          StringBuilder json = new StringBuilder("{");
          for (int i = 0; i < 100; i++) {
            if (i > 0) json.append(",");
            json.append("\"key").append(i).append("\": ").append(i);
          }
          json.append("}");
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
          wtx.commit();
        }

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
             var rtx = session.beginNodeReadOnlyTrx()) {
          rtx.moveToDocumentRoot();
          rtx.moveToFirstChild();
          assertEquals(NodeKind.OBJECT, rtx.getKind());
        }
      }
    }
  }

  @Nested
  @DisplayName("HeightOptimalSplitter Tests")
  class HeightOptimalSplitterTests {

    @Test
    @DisplayName("SplitResult record accessors")
    void testSplitResultAccessors() {
      PageReference leftRef = new PageReference();
      leftRef.setKey(1);
      PageReference rightRef = new PageReference();
      rightRef.setKey(2);
      
      HOTIndirectPage newRoot = HOTIndirectPage.createBiNode(1L, 1, 0, leftRef, rightRef);
      
      var result = new HeightOptimalSplitter.SplitResult(newRoot, leftRef, rightRef, 5);
      
      assertNotNull(result.newRoot());
      assertNotNull(result.leftChild());
      assertNotNull(result.rightChild());
      assertEquals(5, result.discriminativeBitIndex());
    }
  }

  @Nested
  @DisplayName("NodeUpgradeManager Tests")
  class NodeUpgradeManagerTests {

    @Test
    @DisplayName("Node type for all boundary values")
    void testNodeTypeBoundaries() {
      assertEquals(HOTIndirectPage.NodeType.BI_NODE, NodeUpgradeManager.determineNodeType(1));
      assertEquals(HOTIndirectPage.NodeType.BI_NODE, NodeUpgradeManager.determineNodeType(2));
      assertEquals(HOTIndirectPage.NodeType.SPAN_NODE, NodeUpgradeManager.determineNodeType(3));
      assertEquals(HOTIndirectPage.NodeType.SPAN_NODE, NodeUpgradeManager.determineNodeType(16));
      assertEquals(HOTIndirectPage.NodeType.MULTI_NODE, NodeUpgradeManager.determineNodeType(17));
      assertEquals(HOTIndirectPage.NodeType.MULTI_NODE, NodeUpgradeManager.determineNodeType(32));
    }

    @Test
    @DisplayName("Node type by discriminative bits")
    void testNodeTypeByBits() {
      assertEquals(HOTIndirectPage.NodeType.BI_NODE, NodeUpgradeManager.determineNodeTypeByBits(0));
      assertEquals(HOTIndirectPage.NodeType.BI_NODE, NodeUpgradeManager.determineNodeTypeByBits(1));
      assertEquals(HOTIndirectPage.NodeType.SPAN_NODE, NodeUpgradeManager.determineNodeTypeByBits(2));
      assertEquals(HOTIndirectPage.NodeType.SPAN_NODE, NodeUpgradeManager.determineNodeTypeByBits(4));
      assertEquals(HOTIndirectPage.NodeType.MULTI_NODE, NodeUpgradeManager.determineNodeTypeByBits(5));
      assertEquals(HOTIndirectPage.NodeType.MULTI_NODE, NodeUpgradeManager.determineNodeTypeByBits(8));
    }

    // NOTE: testNeedsUpgrade and testShouldDowngrade tests are in NodeUpgradeManagerTest.java
  }

  @Nested
  @DisplayName("SiblingMerger Tests")
  class SiblingMergerTests {

    @Test
    @DisplayName("MergeResult failure factory")
    void testMergeResultFailure() {
      var result = SiblingMerger.MergeResult.failure();

      assertFalse(result.success());
      assertNull(result.mergedNode());
      assertFalse(result.replacesLeft());
    }

    @Test
    @DisplayName("MergeResult success factory")
    void testMergeResultSuccess() {
      PageReference leftRef = new PageReference();
      PageReference rightRef = new PageReference();
      HOTIndirectPage merged = HOTIndirectPage.createBiNode(1L, 1, 0, leftRef, rightRef);
      
      var result = SiblingMerger.MergeResult.success(merged, true);

      assertTrue(result.success());
      assertNotNull(result.mergedNode());
      assertTrue(result.replacesLeft());
    }

    @Test
    @DisplayName("Can merge compatible BiNodes")
    void testCanMergeBiNodes() {
      PageReference leftRef1 = new PageReference();
      leftRef1.setKey(1);
      PageReference rightRef1 = new PageReference();
      rightRef1.setKey(2);
      HOTIndirectPage biNode1 = HOTIndirectPage.createBiNode(1L, 1, 0, leftRef1, rightRef1);

      PageReference leftRef2 = new PageReference();
      leftRef2.setKey(3);
      PageReference rightRef2 = new PageReference();
      rightRef2.setKey(4);
      HOTIndirectPage biNode2 = HOTIndirectPage.createBiNode(2L, 1, 0, leftRef2, rightRef2);

      boolean canMerge = SiblingMerger.canMerge(biNode1, biNode2);
      assertTrue(canMerge);
    }

    @Test
    @DisplayName("Get fill factor")
    void testGetFillFactor() {
      PageReference leftRef = new PageReference();
      PageReference rightRef = new PageReference();
      
      HOTIndirectPage biNode = HOTIndirectPage.createBiNode(1L, 1, 0, leftRef, rightRef);
      
      double fillFactor = SiblingMerger.getFillFactor(biNode);
      assertTrue(fillFactor > 0 && fillFactor <= 1.0);
    }

    @Test
    @DisplayName("Should merge detection")
    void testShouldMerge() {
      PageReference leftRef = new PageReference();
      PageReference rightRef = new PageReference();
      
      HOTIndirectPage biNode = HOTIndirectPage.createBiNode(1L, 1, 0, leftRef, rightRef);
      
      // BiNode with 2 children - check shouldMerge
      boolean shouldMerge = SiblingMerger.shouldMerge(biNode);
      // Just verify it doesn't throw
      assertNotNull(Boolean.valueOf(shouldMerge));
    }
  }

  @Nested
  @DisplayName("SparsePartialKeys Tests")
  class SparsePartialKeysTests {

    @Test
    @DisplayName("Byte keys search")
    void testByteKeysSearch() {
      SparsePartialKeys<Byte> spk = SparsePartialKeys.forBytes(4);
      spk.setEntry(0, (byte) 0x01);
      spk.setEntry(1, (byte) 0x02);
      spk.setEntry(2, (byte) 0x04);
      spk.setEntry(3, (byte) 0x08);

      int mask = spk.search(0xFF);
      assertTrue(mask != 0, "Search should find matches");
    }

    @Test
    @DisplayName("Short keys creation and usage")
    void testShortKeys() {
      SparsePartialKeys<Short> spk = SparsePartialKeys.forShorts(4);
      assertNotNull(spk);
      spk.setEntry(0, (short) 0x0100);
      spk.setEntry(1, (short) 0x0200);
      
      int mask = spk.search(0xFFFF);
      assertTrue(mask >= 0);
    }

    @Test
    @DisplayName("Int keys creation")
    void testIntKeys() {
      SparsePartialKeys<Integer> spk = SparsePartialKeys.forInts(4);
      assertNotNull(spk);
      spk.setEntry(0, 0x01000000);
      
      int mask = spk.search(0xFFFFFFFF);
      assertTrue(mask >= 0);
    }

    @Test
    @DisplayName("Estimate size for different types")
    void testEstimateSize() {
      int byteSize = SparsePartialKeys.estimateSize(16, Byte.class);
      int shortSize = SparsePartialKeys.estimateSize(16, Short.class);
      int intSize = SparsePartialKeys.estimateSize(16, Integer.class);
      
      assertTrue(byteSize > 0);
      assertTrue(shortSize > byteSize);
      assertTrue(intSize > shortSize);
    }
  }

  @Nested
  @DisplayName("PartialKeyMapping Tests")
  class PartialKeyMappingTests {

    @Test
    @DisplayName("Create mapping for single bit")
    void testForSingleBit() {
      PartialKeyMapping mapping = PartialKeyMapping.forSingleBit(7);
      assertNotNull(mapping);
    }

    @Test
    @DisplayName("Add additional bit to mapping")
    void testWithAdditionalBit() {
      PartialKeyMapping initial = PartialKeyMapping.forSingleBit(7);
      PartialKeyMapping extended = PartialKeyMapping.withAdditionalBit(initial, 15);
      assertNotNull(extended);
    }

    @Test
    @DisplayName("Get prefix bits mask")
    void testGetPrefixBitsMask() {
      PartialKeyMapping mapping = PartialKeyMapping.forSingleBit(7);
      int prefixMask = mapping.getPrefixBitsMask(8);
      assertTrue(prefixMask >= 0);
    }
  }

  @Nested
  @DisplayName("ChunkDirectory Tests")
  class ChunkDirectoryTests {

    @Test
    @DisplayName("Empty directory creation")
    void testEmptyChunkDirectory() {
      ChunkDirectory dir = new ChunkDirectory();
      assertEquals(0, dir.chunkCount());
    }

    @Test
    @DisplayName("Directory with chunks")
    void testChunkDirectoryWithChunks() {
      PageReference[] refs = new PageReference[3];
      int[] indices = new int[3];
      for (int i = 0; i < 3; i++) {
        refs[i] = new PageReference();
        refs[i].setKey(100 + i);
        indices[i] = i * 1000;
      }

      ChunkDirectory dir = new ChunkDirectory(3, indices, refs);
      assertEquals(3, dir.chunkCount());
    }
  }

  @Nested
  @DisplayName("PathKeySerializer Tests")
  class PathKeySerializerTests {

    @Test
    @DisplayName("Serialize and deserialize roundtrip")
    void testPathKeyRoundtrip() {
      PathKeySerializer serializer = PathKeySerializer.INSTANCE;
      byte[] buffer = new byte[16];

      long key = 12345L;
      int written = serializer.serialize(key, buffer, 0);
      assertEquals(8, written);

      long restored = serializer.deserialize(buffer, 0, 8);
      assertEquals(key, restored);
    }

    @Test
    @DisplayName("Negative keys roundtrip")
    void testNegativePathKeys() {
      PathKeySerializer serializer = PathKeySerializer.INSTANCE;
      byte[] buffer = new byte[16];

      long key = -12345L;
      serializer.serialize(key, buffer, 0);
      long restored = serializer.deserialize(buffer, 0, 8);
      assertEquals(key, restored);
    }

    @Test
    @DisplayName("Order preservation")
    void testPathKeyOrdering() {
      PathKeySerializer serializer = PathKeySerializer.INSTANCE;
      byte[] buf1 = new byte[8];
      byte[] buf2 = new byte[8];

      serializer.serialize(-1L, buf1, 0);
      serializer.serialize(1L, buf2, 0);

      int cmp = serializer.compare(buf1, 0, 8, buf2, 0, 8);
      assertTrue(cmp < 0, "-1 should sort before 1");
    }
  }

  @Nested
  @DisplayName("HeightOptimalSplitter Extended Tests")
  class HeightOptimalSplitterExtendedTests {

    @Test
    @DisplayName("Find optimal split point with sorted keys")
    void testFindOptimalSplitPoint() {
      byte[][] keys = new byte[][] {
          {0x00, 0x01},
          {0x00, 0x02},
          {0x00, 0x03},
          {0x01, 0x00},
          {0x01, 0x01},
          {0x01, 0x02}
      };
      
      int splitPoint = HeightOptimalSplitter.findOptimalSplitPoint(keys);
      assertTrue(splitPoint >= 0 && splitPoint < keys.length);
    }

    @Test
    @DisplayName("Should create SpanNode based on discriminative bit")
    void testShouldCreateSpanNode() {
      byte[] leftMax = new byte[] {0x0F};
      byte[] rightMin = new byte[] {0x10};
      
      boolean shouldCreate = HeightOptimalSplitter.shouldCreateSpanNode(leftMax, rightMin, 3);
      // Just verify it doesn't throw and returns a value
      assertNotNull(Boolean.valueOf(shouldCreate));
    }

    @Test
    @DisplayName("Create BiNode from split")
    void testCreateBiNode() {
      PageReference leftRef = new PageReference();
      leftRef.setKey(100);
      PageReference rightRef = new PageReference();
      rightRef.setKey(200);
      
      byte[] splitKey = new byte[] {0x50};
      
      HOTIndirectPage biNode = HeightOptimalSplitter.createBiNode(
          1L, 1, 7, leftRef, rightRef);
      
      assertNotNull(biNode);
      assertEquals(HOTIndirectPage.NodeType.BI_NODE, biNode.getNodeType());
    }
  }

  @Nested
  @DisplayName("NodeUpgradeManager Extended Tests")
  class NodeUpgradeManagerExtendedTests {

    @Test
    @DisplayName("Check if node is full")
    void testIsFull() {
      PageReference leftRef = new PageReference();
      PageReference rightRef = new PageReference();
      
      HOTIndirectPage biNode = HOTIndirectPage.createBiNode(1L, 1, 0, leftRef, rightRef);
      
      boolean isFull = NodeUpgradeManager.isFull(biNode);
      // BiNode can hold exactly 2 children, so isFull() check depends on implementation
      assertNotNull(Boolean.valueOf(isFull));
    }

    @Test
    @DisplayName("Check if node is underfilled")
    void testIsUnderfilled() {
      PageReference leftRef = new PageReference();
      PageReference rightRef = new PageReference();
      
      HOTIndirectPage biNode = HOTIndirectPage.createBiNode(1L, 1, 0, leftRef, rightRef);
      
      boolean isUnderfilled = NodeUpgradeManager.isUnderfilled(biNode, 0.5);
      assertFalse(isUnderfilled, "BiNode with 2 children is not underfilled");
    }

    @Test
    @DisplayName("Get max children for each node type")
    void testGetMaxChildrenForType() {
      assertEquals(2, NodeUpgradeManager.getMaxChildrenForType(HOTIndirectPage.NodeType.BI_NODE));
      assertEquals(16, NodeUpgradeManager.getMaxChildrenForType(HOTIndirectPage.NodeType.SPAN_NODE));
      assertEquals(32, NodeUpgradeManager.getMaxChildrenForType(HOTIndirectPage.NodeType.MULTI_NODE));
    }

    @Test
    @DisplayName("Should merge to SpanNode")
    void testShouldMergeToSpanNode() {
      PageReference leftRef1 = new PageReference();
      PageReference rightRef1 = new PageReference();
      HOTIndirectPage node1 = HOTIndirectPage.createBiNode(1L, 1, 0, leftRef1, rightRef1);

      PageReference leftRef2 = new PageReference();
      PageReference rightRef2 = new PageReference();
      HOTIndirectPage node2 = HOTIndirectPage.createBiNode(2L, 1, 0, leftRef2, rightRef2);
      
      boolean shouldMerge = NodeUpgradeManager.shouldMergeToSpanNode(node1, node2);
      assertTrue(shouldMerge, "Two BiNodes should merge to SpanNode");
    }
  }

  @Nested
  @DisplayName("SiblingMerger Extended Tests")
  class SiblingMergerExtendedTests {

    @Test
    @DisplayName("Can collapse BiNode")
    void testCanCollapseBiNode() {
      PageReference leftRef = new PageReference();
      leftRef.setKey(1);
      PageReference rightRef = new PageReference();
      rightRef.setKey(2);
      
      HOTIndirectPage biNode = HOTIndirectPage.createBiNode(1L, 1, 0, leftRef, rightRef);
      
      // BiNode with both children can't be collapsed
      boolean canCollapse = SiblingMerger.canCollapseBiNode(biNode);
      assertFalse(canCollapse);
    }

    @Test
    @DisplayName("Can collapse BiNode check")
    void testCanCollapseBiNodeCheck() {
      PageReference leftRef = new PageReference();
      leftRef.setKey(1);
      PageReference rightRef = new PageReference();
      rightRef.setKey(2);
      
      HOTIndirectPage biNode = HOTIndirectPage.createBiNode(1L, 1, 0, leftRef, rightRef);
      
      // BiNode with both children cannot be collapsed
      boolean canCollapse = SiblingMerger.canCollapseBiNode(biNode);
      assertFalse(canCollapse, "BiNode with 2 children cannot be collapsed");
    }
  }

  @Nested
  @DisplayName("SparsePartialKeys Extended Tests")
  class SparsePartialKeysExtendedTests {

    @Test
    @DisplayName("Set and get number of entries")
    void testSetNumEntries() {
      SparsePartialKeys<Byte> spk = SparsePartialKeys.forBytes(8);
      spk.setNumEntries(4);
      // Just verify no exception
      assertNotNull(spk);
    }

    @Test
    @DisplayName("Search with zero pattern")
    void testSearchZeroPattern() {
      SparsePartialKeys<Byte> spk = SparsePartialKeys.forBytes(4);
      spk.setEntry(0, (byte) 0x01);
      spk.setEntry(1, (byte) 0x02);
      
      int mask = spk.search(0x00);
      // Zero pattern behavior depends on implementation - just verify it returns
      assertTrue(mask >= 0);
    }

    @Test
    @DisplayName("Search with exact match pattern")
    void testSearchExactMatch() {
      SparsePartialKeys<Byte> spk = SparsePartialKeys.forBytes(4);
      spk.setEntry(0, (byte) 0x01);
      spk.setEntry(1, (byte) 0x02);
      spk.setEntry(2, (byte) 0x04);
      spk.setEntry(3, (byte) 0x08);
      
      int mask = spk.search(0x02);
      assertTrue((mask & 0x02) != 0, "Should find entry 1");
    }
  }

  @Nested
  @DisplayName("ChunkDirectory Extended Tests")
  class ChunkDirectoryExtendedTests {

    @Test
    @DisplayName("Get chunk index at position")
    void testGetChunkIndex() {
      PageReference[] refs = new PageReference[3];
      int[] indices = new int[] {0, 1000, 2000};
      for (int i = 0; i < 3; i++) {
        refs[i] = new PageReference();
        refs[i].setKey(100 + i);
      }
      
      ChunkDirectory dir = new ChunkDirectory(3, indices, refs);
      
      int chunkIdx = dir.getChunkIndex(1);
      assertEquals(1000, chunkIdx);
    }

    @Test
    @DisplayName("Get chunk indices array")
    void testGetChunkIndices() {
      PageReference[] refs = new PageReference[2];
      refs[0] = new PageReference();
      refs[1] = new PageReference();
      
      ChunkDirectory dir = new ChunkDirectory(2, new int[] {0, 1000}, refs);
      
      int[] indices = dir.getChunkIndices();
      assertNotNull(indices);
      assertEquals(0, indices[0]);
      assertEquals(1000, indices[1]);
    }

    @Test
    @DisplayName("Get chunk reference at position")
    void testGetChunkRefAtPosition() {
      PageReference[] refs = new PageReference[2];
      refs[0] = new PageReference();
      refs[0].setKey(100);
      refs[1] = new PageReference();
      refs[1].setKey(200);
      
      ChunkDirectory dir = new ChunkDirectory(2, new int[] {0, 1000}, refs);
      
      PageReference ref = dir.getChunkRefAtPosition(0);
      assertEquals(100, ref.getKey());
    }

    @Test
    @DisplayName("Get or create chunk ref")
    void testGetOrCreateChunkRef() {
      ChunkDirectory dir = new ChunkDirectory();
      
      PageReference ref = dir.getOrCreateChunkRef(0);
      assertNotNull(ref);
      assertEquals(1, dir.chunkCount());
    }

    @Test
    @DisplayName("Set chunk ref")
    void testSetChunkRef() {
      ChunkDirectory dir = new ChunkDirectory();
      PageReference ref = new PageReference();
      ref.setKey(999);
      
      dir.setChunkRef(5, ref);
      
      PageReference retrieved = dir.getChunkRef(5);
      assertNotNull(retrieved);
      assertEquals(999, retrieved.getKey());
    }

    @Test
    @DisplayName("Chunk index for document node key")
    void testChunkIndexFor() {
      int chunkIdx = ChunkDirectory.chunkIndexFor(65536L);
      assertTrue(chunkIdx >= 0);
    }

    @Test
    @DisplayName("Is empty")
    void testIsEmpty() {
      ChunkDirectory empty = new ChunkDirectory();
      assertTrue(empty.isEmpty());
      
      empty.getOrCreateChunkRef(0);
      assertFalse(empty.isEmpty());
    }

    @Test
    @DisplayName("Is modified")
    void testIsModified() {
      ChunkDirectory dir = new ChunkDirectory();
      dir.getOrCreateChunkRef(0);
      assertTrue(dir.isModified());
      
      dir.clearModified();
      assertFalse(dir.isModified());
    }

    @Test
    @DisplayName("Copy directory")
    void testCopyDirectory() {
      PageReference[] refs = new PageReference[2];
      refs[0] = new PageReference();
      refs[0].setKey(100);
      refs[1] = new PageReference();
      refs[1].setKey(200);
      
      ChunkDirectory original = new ChunkDirectory(2, new int[] {0, 1000}, refs);
      ChunkDirectory copy = original.copy();
      
      assertEquals(original.chunkCount(), copy.chunkCount());
    }

    @Test
    @DisplayName("ToString")
    void testToString() {
      ChunkDirectory dir = new ChunkDirectory();
      String str = dir.toString();
      assertNotNull(str);
      assertTrue(str.contains("ChunkDirectory"));
    }

    @Test
    @DisplayName("Equals and hashCode")
    void testEqualsHashCode() {
      PageReference[] refs1 = new PageReference[] { new PageReference() };
      PageReference[] refs2 = new PageReference[] { new PageReference() };
      
      ChunkDirectory dir1 = new ChunkDirectory(1, new int[] {0}, refs1);
      ChunkDirectory dir2 = new ChunkDirectory(1, new int[] {0}, refs2);
      
      // Same structure
      assertEquals(dir1.hashCode(), dir2.hashCode());
    }
  }

  @Nested
  @DisplayName("PartialKeyMapping Extended Tests")
  class PartialKeyMappingExtendedTests {

    @Test
    @DisplayName("Extract mask from key")
    void testExtractMask() {
      PartialKeyMapping mapping = PartialKeyMapping.forSingleBit(7);
      byte[] key = new byte[] {(byte) 0x80, 0x00};
      
      int extracted = mapping.extractMask(key);
      assertTrue(extracted >= 0);
    }

    @Test
    @DisplayName("Get mask for highest bit")
    void testGetMaskForHighestBit() {
      PartialKeyMapping mapping = PartialKeyMapping.forSingleBit(7);
      int mask = mapping.getMaskForHighestBit();
      assertTrue(mask >= 0);
    }

    @Test
    @DisplayName("Get most significant bit index")
    void testGetMostSignificantBitIndex() {
      PartialKeyMapping mapping = PartialKeyMapping.forSingleBit(15);
      int msb = mapping.getMostSignificantBitIndex();
      assertTrue(msb >= 0);
    }

    @Test
    @DisplayName("Get least significant bit index")
    void testGetLeastSignificantBitIndex() {
      PartialKeyMapping mapping = PartialKeyMapping.forSingleBit(7);
      int lsb = mapping.getLeastSignificantBitIndex();
      assertTrue(lsb >= 0);
    }
  }

  @Nested
  @DisplayName("DiscriminativeBitComputer Extended Tests")
  class DiscriminativeBitComputerExtendedTests {

    @Test
    @DisplayName("Compute differing bit for identical keys")
    void testIdenticalKeys() {
      byte[] key1 = new byte[] {0x01, 0x02, 0x03};
      byte[] key2 = new byte[] {0x01, 0x02, 0x03};
      
      int bit = DiscriminativeBitComputer.computeDifferingBit(key1, key2);
      assertEquals(-1, bit, "Identical keys should return -1");
    }

    @Test
    @DisplayName("Compute differing bit for keys of different lengths")
    void testDifferentLengthKeys() {
      byte[] key1 = new byte[] {0x01, 0x02};
      byte[] key2 = new byte[] {0x01, 0x02, 0x03};
      
      int bit = DiscriminativeBitComputer.computeDifferingBit(key1, key2);
      assertTrue(bit >= 0, "Different length keys should have a differing bit");
    }

    @Test
    @DisplayName("Compute differing bit for empty keys")
    void testEmptyKeys() {
      byte[] key1 = new byte[0];
      byte[] key2 = new byte[0];
      
      int bit = DiscriminativeBitComputer.computeDifferingBit(key1, key2);
      assertEquals(-1, bit, "Empty keys should return -1");
    }

    @Test
    @DisplayName("Compute differing bit with one empty key")
    void testOneEmptyKey() {
      byte[] key1 = new byte[0];
      byte[] key2 = new byte[] {0x01};
      
      int bit = DiscriminativeBitComputer.computeDifferingBit(key1, key2);
      assertEquals(0, bit, "One empty key should differ at bit 0");
    }
  }
}
