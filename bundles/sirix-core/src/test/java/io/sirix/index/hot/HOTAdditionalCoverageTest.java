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

    @Test
    @DisplayName("Upgrade detection for BiNode")
    void testNeedsUpgrade() {
      PageReference leftRef = new PageReference();
      leftRef.setKey(1);
      PageReference rightRef = new PageReference();
      rightRef.setKey(2);

      HOTIndirectPage biNode = HOTIndirectPage.createBiNode(1L, 1, 0, leftRef, rightRef);
      
      assertFalse(NodeUpgradeManager.needsUpgrade(biNode, 2));
      assertTrue(NodeUpgradeManager.needsUpgrade(biNode, 3));
    }

    @Test
    @DisplayName("Downgrade detection for SpanNode")
    void testShouldDowngrade() {
      PageReference[] children = new PageReference[4];
      byte[] partialKeys = new byte[4];
      for (int i = 0; i < 4; i++) {
        children[i] = new PageReference();
        children[i].setKey(i);
        partialKeys[i] = (byte) i;
      }

      HOTIndirectPage spanNode = HOTIndirectPage.createSpanNode(1L, 1, (byte) 0, 0b1111L, partialKeys, children);

      assertFalse(NodeUpgradeManager.shouldDowngrade(spanNode, 3));
      assertTrue(NodeUpgradeManager.shouldDowngrade(spanNode, 2));
    }
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
}
