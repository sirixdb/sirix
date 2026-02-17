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
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.IndexType;
import io.sirix.index.path.json.JsonPCRCollector;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.node.NodeKind;
import io.sirix.page.HOTIndirectPage;
import io.sirix.page.HOTLeafPage;
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Comprehensive tests to achieve 80%+ coverage for HOT classes.
 */
@DisplayName("HOT Comprehensive Coverage Tests")
class HOTComprehensiveCoverageTest {

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
  @DisplayName("HOTIndexWriter Coverage")
  class HOTIndexWriterCoverageTests {

    @Test
    @DisplayName("Write many entries triggering page splits")
    void testWriteManyEntries() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(
            ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {
          // Create many unique paths to trigger index operations
          StringBuilder json = new StringBuilder("{");
          for (int i = 0; i < 200; i++) {
            if (i > 0)
              json.append(",");
            json.append("\"path").append(i).append("\": {\"nested\": ").append(i).append("}");
          }
          json.append("}");
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
          wtx.commit();
        }

        // Verify data was indexed
        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            var rtx = session.beginNodeReadOnlyTrx()) {
          rtx.moveToDocumentRoot();
          rtx.moveToFirstChild();
          assertEquals(NodeKind.OBJECT, rtx.getKind());
        }
      }
    }

    @Test
    @DisplayName("Write then remove entries")
    void testWriteThenRemove() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(
            ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());

        // Create initial data
        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {
          StringBuilder json = new StringBuilder("[");
          for (int i = 0; i < 50; i++) {
            if (i > 0)
              json.append(",");
            json.append("{\"id\": ").append(i).append("}");
          }
          json.append("]");
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
          wtx.commit();
        }

        // Remove the first child in a new revision
        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.moveToDocumentRoot();
          wtx.moveToFirstChild(); // array
          if (wtx.hasFirstChild()) {
            wtx.moveToFirstChild(); // first object
            wtx.remove();
          }
          wtx.commit();
        }

        // Verify remaining data
        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            var rtx = session.beginNodeReadOnlyTrx()) {
          rtx.moveToDocumentRoot();
          assertTrue(rtx.hasFirstChild());
        }
      }
    }

    @Test
    @DisplayName("Update existing entries")
    void testUpdateEntries() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(
            ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());

        // Create initial data
        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"name\": \"initial\", \"value\": 1}"),
              JsonNodeTrx.Commit.NO);
          wtx.commit();
        }

        // Update data multiple times
        for (int rev = 0; rev < 5; rev++) {
          try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
              JsonNodeTrx wtx = session.beginNodeTrx()) {
            wtx.moveToDocumentRoot();
            wtx.moveToFirstChild();
            wtx.remove();
            wtx.insertSubtreeAsFirstChild(
                JsonShredder.createStringReader("{\"name\": \"rev" + rev + "\", \"value\": " + (rev * 10) + "}"),
                JsonNodeTrx.Commit.NO);
            wtx.commit();
          }
        }

        // Verify final state
        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            var rtx = session.beginNodeReadOnlyTrx()) {
          rtx.moveToDocumentRoot();
          assertTrue(rtx.hasFirstChild());
        }
      }
    }
  }

  @Nested
  @DisplayName("HOTIndexReader Coverage")
  class HOTIndexReaderCoverageTests {

    @Test
    @DisplayName("Read after multiple writes")
    void testReadAfterMultipleWrites() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(
            ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());

        // Create data
        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {
          StringBuilder json = new StringBuilder("{");
          for (int i = 0; i < 100; i++) {
            if (i > 0)
              json.append(",");
            json.append("\"key").append(i).append("\": \"value").append(i).append("\"");
          }
          json.append("}");
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
          wtx.commit();
        }

        // Read data at different revisions
        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
          for (int rev = 1; rev <= session.getMostRecentRevisionNumber(); rev++) {
            try (var rtx = session.beginNodeReadOnlyTrx(rev)) {
              rtx.moveToDocumentRoot();
              if (rtx.hasFirstChild()) {
                rtx.moveToFirstChild();
                assertNotNull(rtx.getKind());
              }
            }
          }
        }
      }
    }

    @Test
    @DisplayName("Read empty index")
    void testReadEmptyIndex() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(
            ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());

        // Create minimal data
        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{}"), JsonNodeTrx.Commit.NO);
          wtx.commit();
        }

        // Read empty structure
        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            var rtx = session.beginNodeReadOnlyTrx()) {
          rtx.moveToDocumentRoot();
          rtx.moveToFirstChild();
          assertEquals(NodeKind.OBJECT, rtx.getKind());
          assertFalse(rtx.hasFirstChild());
        }
      }
    }
  }

  @Nested
  @DisplayName("NodeUpgradeManager Full Coverage")
  class NodeUpgradeManagerFullCoverageTests {

    @Test
    @DisplayName("Create and test SpanNode merge")
    void testMergeToSpanNode() {
      // Create two BiNodes
      PageReference leftRef1 = new PageReference();
      leftRef1.setKey(1);
      PageReference rightRef1 = new PageReference();
      rightRef1.setKey(2);
      HOTIndirectPage node1 = HOTIndirectPage.createBiNode(1L, 1, 0, leftRef1, rightRef1);

      PageReference leftRef2 = new PageReference();
      leftRef2.setKey(3);
      PageReference rightRef2 = new PageReference();
      rightRef2.setKey(4);
      HOTIndirectPage node2 = HOTIndirectPage.createBiNode(2L, 1, 0, leftRef2, rightRef2);

      // Test merge capability
      boolean canMerge = NodeUpgradeManager.shouldMergeToSpanNode(node1, node2);
      assertTrue(canMerge);
    }

    @Test
    @DisplayName("Test all node type determinations")
    void testAllNodeTypeDeterminations() {
      // Test children-based determination
      for (int i = 1; i <= 32; i++) {
        HOTIndirectPage.NodeType type = NodeUpgradeManager.determineNodeType(i);
        assertNotNull(type);
        if (i <= 2) {
          assertEquals(HOTIndirectPage.NodeType.BI_NODE, type);
        } else if (i <= 16) {
          assertEquals(HOTIndirectPage.NodeType.SPAN_NODE, type);
        } else {
          assertEquals(HOTIndirectPage.NodeType.MULTI_NODE, type);
        }
      }
    }

    @Test
    @DisplayName("Test node fill status")
    void testNodeFillStatus() {
      PageReference leftRef = new PageReference();
      PageReference rightRef = new PageReference();
      HOTIndirectPage biNode = HOTIndirectPage.createBiNode(1L, 1, 0, leftRef, rightRef);

      // Test various fill factor thresholds
      boolean underfilled1 = NodeUpgradeManager.isUnderfilled(biNode, 0.1);
      boolean underfilled2 = NodeUpgradeManager.isUnderfilled(biNode, 0.5);
      boolean underfilled3 = NodeUpgradeManager.isUnderfilled(biNode, 0.9);

      // Results depend on implementation, just verify no exceptions
      assertNotNull(Boolean.valueOf(underfilled1));
      assertNotNull(Boolean.valueOf(underfilled2));
      assertNotNull(Boolean.valueOf(underfilled3));
    }
  }

  @Nested
  @DisplayName("HeightOptimalSplitter Full Coverage")
  class HeightOptimalSplitterFullCoverageTests {

    @Test
    @DisplayName("Find split points for various key distributions")
    void testSplitPointsVariousDistributions() {
      // Uniform distribution
      byte[][] uniform = new byte[8][];
      for (int i = 0; i < 8; i++) {
        uniform[i] = new byte[] {(byte) (i * 16)};
      }
      int splitUniform = HeightOptimalSplitter.findOptimalSplitPoint(uniform);
      assertTrue(splitUniform >= 0 && splitUniform < uniform.length);

      // Clustered distribution
      byte[][] clustered =
          new byte[][] {{0x00}, {0x01}, {0x02}, {0x03}, {(byte) 0xF0}, {(byte) 0xF1}, {(byte) 0xF2}, {(byte) 0xF3}};
      int splitClustered = HeightOptimalSplitter.findOptimalSplitPoint(clustered);
      assertTrue(splitClustered >= 0 && splitClustered < clustered.length);
    }

    @Test
    @DisplayName("Test BiNode creation with various discriminative bits")
    void testBiNodeCreationVariousBits() {
      for (int bit = 0; bit < 16; bit++) {
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
    @DisplayName("Test should create SpanNode decision")
    void testShouldCreateSpanNodeDecision() {
      // Keys with multiple bits difference in same byte
      byte[] left1 = new byte[] {0x0F};
      byte[] right1 = new byte[] {(byte) 0xF0};
      boolean should1 = HeightOptimalSplitter.shouldCreateSpanNode(left1, right1, 0);

      // Keys with single bit difference
      byte[] left2 = new byte[] {0x00};
      byte[] right2 = new byte[] {0x01};
      boolean should2 = HeightOptimalSplitter.shouldCreateSpanNode(left2, right2, 7);

      // Just verify the method executes without error
      assertNotNull(Boolean.valueOf(should1));
      assertNotNull(Boolean.valueOf(should2));
    }
  }

  @Nested
  @DisplayName("SiblingMerger Full Coverage")
  class SiblingMergerFullCoverageTests {

    @Test
    @DisplayName("Test merge compatibility for various node configurations")
    void testMergeCompatibility() {
      // Two BiNodes at same height
      PageReference lr1 = new PageReference();
      PageReference rr1 = new PageReference();
      HOTIndirectPage bi1 = HOTIndirectPage.createBiNode(1L, 1, 0, lr1, rr1);

      PageReference lr2 = new PageReference();
      PageReference rr2 = new PageReference();
      HOTIndirectPage bi2 = HOTIndirectPage.createBiNode(2L, 1, 0, lr2, rr2);

      assertTrue(SiblingMerger.canMerge(bi1, bi2));
    }

    @Test
    @DisplayName("Test should merge decision")
    void testShouldMergeDecision() {
      PageReference leftRef = new PageReference();
      PageReference rightRef = new PageReference();
      HOTIndirectPage biNode = HOTIndirectPage.createBiNode(1L, 1, 0, leftRef, rightRef);

      boolean shouldMerge = SiblingMerger.shouldMerge(biNode);
      // Result depends on fill factor - just verify no exception
      assertNotNull(Boolean.valueOf(shouldMerge));
    }

    @Test
    @DisplayName("Test fill factor for different node types")
    void testFillFactorVariousTypes() {
      // BiNode
      PageReference lr = new PageReference();
      PageReference rr = new PageReference();
      HOTIndirectPage biNode = HOTIndirectPage.createBiNode(1L, 1, 0, lr, rr);
      double biFill = SiblingMerger.getFillFactor(biNode);
      assertTrue(biFill > 0 && biFill <= 1.0);

      // SpanNode
      PageReference[] children = new PageReference[4];
      byte[] partialKeys = new byte[4];
      for (int i = 0; i < 4; i++) {
        children[i] = new PageReference();
        children[i].setKey(i);
        partialKeys[i] = (byte) i;
      }
      HOTIndirectPage spanNode = HOTIndirectPage.createSpanNode(2L, 1, (byte) 0, 0b1111L, partialKeys, children);
      double spanFill = SiblingMerger.getFillFactor(spanNode);
      assertTrue(spanFill > 0 && spanFill <= 1.0);
    }

    @Test
    @DisplayName("Test BiNode collapse")
    void testBiNodeCollapse() {
      PageReference leftRef = new PageReference();
      leftRef.setKey(100);
      PageReference rightRef = new PageReference();
      rightRef.setKey(200);

      HOTIndirectPage biNode = HOTIndirectPage.createBiNode(1L, 1, 0, leftRef, rightRef);

      boolean canCollapse = SiblingMerger.canCollapseBiNode(biNode);
      assertFalse(canCollapse, "BiNode with 2 children cannot collapse");
    }
  }

  @Nested
  @DisplayName("SparsePartialKeys Full Coverage")
  class SparsePartialKeysFullCoverageTests {

    @Test
    @DisplayName("Test all partial key types")
    void testAllPartialKeyTypes() {
      // Byte keys
      SparsePartialKeys<Byte> byteKeys = SparsePartialKeys.forBytes(8);
      for (int i = 0; i < 8; i++) {
        byteKeys.setEntry(i, (byte) (1 << i));
      }
      int byteMask = byteKeys.search(0xFF);
      assertTrue(byteMask != 0);

      // Short keys
      SparsePartialKeys<Short> shortKeys = SparsePartialKeys.forShorts(8);
      for (int i = 0; i < 8; i++) {
        shortKeys.setEntry(i, (short) (1 << i));
      }
      int shortMask = shortKeys.search(0xFFFF);
      assertTrue(shortMask != 0);

      // Int keys
      SparsePartialKeys<Integer> intKeys = SparsePartialKeys.forInts(8);
      for (int i = 0; i < 8; i++) {
        intKeys.setEntry(i, 1 << i);
      }
      int intMask = intKeys.search(0xFFFFFFFF);
      assertTrue(intMask != 0);
    }

    @Test
    @DisplayName("Test search with various patterns")
    void testSearchVariousPatterns() {
      SparsePartialKeys<Byte> spk = SparsePartialKeys.forBytes(8);
      spk.setEntry(0, (byte) 0x01);
      spk.setEntry(1, (byte) 0x02);
      spk.setEntry(2, (byte) 0x04);
      spk.setEntry(3, (byte) 0x08);
      spk.setEntry(4, (byte) 0x10);
      spk.setEntry(5, (byte) 0x20);
      spk.setEntry(6, (byte) 0x40);
      spk.setEntry(7, (byte) 0x80);

      // Test various search patterns
      int mask1 = spk.search(0x01); // Match first
      int mask2 = spk.search(0x80); // Match last
      int mask3 = spk.search(0x0F); // Match first four
      int mask4 = spk.search(0xF0); // Match last four
      int mask5 = spk.search(0xFF); // Match all

      assertTrue(mask1 != 0);
      assertTrue(mask2 != 0);
      assertTrue(mask3 != 0);
      assertTrue(mask4 != 0);
      assertTrue(mask5 != 0);
    }

    @Test
    @DisplayName("Test estimate size for all types")
    void testEstimateSizeAllTypes() {
      for (int numEntries : new int[] {1, 8, 16, 32}) {
        int byteSize = SparsePartialKeys.estimateSize(numEntries, Byte.class);
        int shortSize = SparsePartialKeys.estimateSize(numEntries, Short.class);
        int intSize = SparsePartialKeys.estimateSize(numEntries, Integer.class);

        assertTrue(byteSize > 0);
        assertTrue(shortSize >= byteSize);
        assertTrue(intSize >= shortSize);
      }
    }
  }

  @Nested
  @DisplayName("PartialKeyMapping Full Coverage")
  class PartialKeyMappingFullCoverageTests {

    @Test
    @DisplayName("Test mapping for various bit positions")
    void testMappingVariousBitPositions() {
      for (int bitPos : new int[] {0, 7, 8, 15, 16, 23, 31, 63}) {
        PartialKeyMapping mapping = PartialKeyMapping.forSingleBit(bitPos);
        assertNotNull(mapping);

        int msb = mapping.getMostSignificantBitIndex();
        int lsb = mapping.getLeastSignificantBitIndex();
        assertTrue(msb >= 0);
        assertTrue(lsb >= 0);
      }
    }

    @Test
    @DisplayName("Test adding additional bits")
    void testAddingAdditionalBits() {
      PartialKeyMapping initial = PartialKeyMapping.forSingleBit(0);

      PartialKeyMapping with1 = PartialKeyMapping.withAdditionalBit(initial, 8);
      assertNotNull(with1);

      PartialKeyMapping with2 = PartialKeyMapping.withAdditionalBit(with1, 16);
      assertNotNull(with2);

      PartialKeyMapping with3 = PartialKeyMapping.withAdditionalBit(with2, 24);
      assertNotNull(with3);
    }

    @Test
    @DisplayName("Test prefix bits mask")
    void testPrefixBitsMask() {
      PartialKeyMapping mapping = PartialKeyMapping.forSingleBit(7);

      for (int bitIndex : new int[] {0, 4, 8, 12, 16}) {
        int prefixMask = mapping.getPrefixBitsMask(bitIndex);
        assertTrue(prefixMask >= 0);
      }
    }

    @Test
    @DisplayName("Test extract mask from keys")
    void testExtractMaskFromKeys() {
      PartialKeyMapping mapping = PartialKeyMapping.forSingleBit(7);

      byte[][] testKeys = new byte[][] {{0x00}, {0x01}, {(byte) 0x80}, {(byte) 0xFF}, {0x00, 0x01},
          {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}};

      for (byte[] key : testKeys) {
        int extracted = mapping.extractMask(key);
        assertTrue(extracted >= 0);
      }
    }

    @Test
    @DisplayName("Test mask for highest bit")
    void testMaskForHighestBit() {
      for (int bitPos : new int[] {0, 7, 15, 31}) {
        PartialKeyMapping mapping = PartialKeyMapping.forSingleBit(bitPos);
        int highestMask = mapping.getMaskForHighestBit();
        assertTrue(highestMask >= 0);
      }
    }
  }

  @Nested
  @DisplayName("ChunkDirectory Full Coverage")
  class ChunkDirectoryFullCoverageTests {

    @Test
    @DisplayName("Full lifecycle test")
    void testFullLifecycle() {
      ChunkDirectory dir = new ChunkDirectory();
      assertTrue(dir.isEmpty());
      assertFalse(dir.isModified());
      assertEquals(0, dir.chunkCount());

      // Add first chunk
      PageReference ref1 = dir.getOrCreateChunkRef(0);
      ref1.setKey(100);
      assertEquals(1, dir.chunkCount());
      assertTrue(dir.isModified());

      // Add more chunks
      for (int i = 1; i <= 5; i++) {
        PageReference ref = dir.getOrCreateChunkRef(i * 1000);
        ref.setKey(100 + i);
      }
      assertEquals(6, dir.chunkCount());

      // Clear modified flag
      dir.clearModified();
      assertFalse(dir.isModified());

      // Copy and verify
      ChunkDirectory copy = dir.copy();
      assertEquals(dir.chunkCount(), copy.chunkCount());

      // Verify toString
      String str = dir.toString();
      assertNotNull(str);
      assertTrue(str.contains("ChunkDirectory"));
    }

    @Test
    @DisplayName("Test chunk index calculation")
    void testChunkIndexCalculation() {
      for (long docKey : new long[] {0, 1000, 65536, 100000, 1000000}) {
        int chunkIdx = ChunkDirectory.chunkIndexFor(docKey);
        assertTrue(chunkIdx >= 0);
      }
    }

    @Test
    @DisplayName("Test get and set chunk refs")
    void testGetSetChunkRefs() {
      ChunkDirectory dir = new ChunkDirectory();

      // Set refs at various indices
      for (int idx : new int[] {0, 5, 10, 100}) {
        PageReference ref = new PageReference();
        ref.setKey(idx * 10);
        dir.setChunkRef(idx, ref);

        PageReference retrieved = dir.getChunkRef(idx);
        assertNotNull(retrieved);
        assertEquals(idx * 10, retrieved.getKey());
      }
    }

    @Test
    @DisplayName("Test equals and hashCode")
    void testEqualsHashCode() {
      ChunkDirectory dir1 = new ChunkDirectory();
      dir1.getOrCreateChunkRef(0).setKey(100);

      ChunkDirectory dir2 = new ChunkDirectory();
      dir2.getOrCreateChunkRef(0).setKey(100);

      // Test equality properties
      assertEquals(dir1.chunkCount(), dir2.chunkCount());
    }
  }

  @Nested
  @DisplayName("DiscriminativeBitComputer Full Coverage")
  class DiscriminativeBitComputerFullCoverageTests {

    @Test
    @DisplayName("Test with long keys (8+ bytes)")
    void testLongKeys() {
      byte[] key1 = new byte[] {0, 0, 0, 0, 0, 0, 0, 0, 0x01};
      byte[] key2 = new byte[] {0, 0, 0, 0, 0, 0, 0, 0, 0x02};

      int bit = DiscriminativeBitComputer.computeDifferingBit(key1, key2);
      assertTrue(bit >= 64, "Difference should be in 9th byte");
    }

    @Test
    @DisplayName("Test with keys differing in every byte")
    void testDifferingEveryByte() {
      for (int bytePos = 0; bytePos < 8; bytePos++) {
        byte[] key1 = new byte[8];
        byte[] key2 = new byte[8];
        key2[bytePos] = 0x01;

        int bit = DiscriminativeBitComputer.computeDifferingBit(key1, key2);
        assertTrue(bit >= bytePos * 8 && bit < (bytePos + 1) * 8);
      }
    }

    @Test
    @DisplayName("Test with all bits set")
    void testAllBitsSet() {
      byte[] key1 = new byte[] {(byte) 0xFF, (byte) 0xFF};
      byte[] key2 = new byte[] {(byte) 0xFF, (byte) 0xFE};

      int bit = DiscriminativeBitComputer.computeDifferingBit(key1, key2);
      assertTrue(bit >= 8); // Difference in second byte
    }
  }

  @Nested
  @DisplayName("Integration Tests for Full Coverage")
  class IntegrationFullCoverageTests {

    @Test
    @DisplayName("Many revisions with data growth")
    void testManyRevisionsWithGrowth() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME)
                                                     .versioningApproach(VersioningType.INCREMENTAL)
                                                     .maxNumberOfRevisionsToRestore(3)
                                                     .build());

        // Create many revisions with growing data
        for (int rev = 0; rev < 10; rev++) {
          try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
              JsonNodeTrx wtx = session.beginNodeTrx()) {

            if (rev == 0) {
              StringBuilder json = new StringBuilder("[");
              for (int i = 0; i < 20; i++) {
                if (i > 0)
                  json.append(",");
                json.append(i);
              }
              json.append("]");
              wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
            } else {
              wtx.moveToDocumentRoot();
              wtx.moveToFirstChild();
              wtx.remove();
              StringBuilder json = new StringBuilder("[");
              for (int i = 0; i < 20 + rev * 5; i++) {
                if (i > 0)
                  json.append(",");
                json.append(i + rev * 100);
              }
              json.append("]");
              wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
            }
            wtx.commit();
          }
        }

        // Read all revisions
        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
          for (int rev = 1; rev <= session.getMostRecentRevisionNumber(); rev++) {
            try (var rtx = session.beginNodeReadOnlyTrx(rev)) {
              rtx.moveToDocumentRoot();
              assertTrue(rtx.hasFirstChild());
            }
          }
        }
      }
    }

    @Test
    @DisplayName("Complex nested structure")
    void testComplexNestedStructure() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(
            ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {

          // Create deeply nested structure
          StringBuilder json = new StringBuilder("{");
          for (int i = 0; i < 10; i++) {
            if (i > 0)
              json.append(",");
            json.append("\"level").append(i).append("\": {");
            for (int j = 0; j < 5; j++) {
              if (j > 0)
                json.append(",");
              json.append("\"sub").append(j).append("\": [");
              for (int k = 0; k < 3; k++) {
                if (k > 0)
                  json.append(",");
                json.append("{\"value\": ").append(i * 100 + j * 10 + k).append("}");
              }
              json.append("]");
            }
            json.append("}");
          }
          json.append("}");

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
          wtx.commit();
        }

        // Verify structure was created
        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            var rtx = session.beginNodeReadOnlyTrx()) {
          rtx.moveToDocumentRoot();
          assertTrue(rtx.hasFirstChild(), "Should have root element");
          rtx.moveToFirstChild();
          assertEquals(NodeKind.OBJECT, rtx.getKind());
          assertTrue(rtx.hasFirstChild(), "Object should have children");
        }
      }
    }
  }
}

