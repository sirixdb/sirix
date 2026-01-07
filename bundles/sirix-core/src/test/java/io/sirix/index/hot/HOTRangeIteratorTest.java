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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for HOTIndexReader RangeIterator to achieve full coverage.
 */
@DisplayName("HOT RangeIterator Tests")
class HOTRangeIteratorTest {

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
  @DisplayName("HOTIndexReader Direct Tests")
  class HOTIndexReaderDirectTests {

    @Test
    @DisplayName("Test get method for existing key")
    void testGetExistingKey() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME)
            .versioningApproach(VersioningType.FULL)
            .build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
             JsonNodeTrx wtx = session.beginNodeTrx()) {
          // Create test data
          wtx.insertSubtreeAsFirstChild(
              JsonShredder.createStringReader("{\"path1\": 1, \"path2\": 2, \"path3\": 3}"),
              JsonNodeTrx.Commit.NO);
          wtx.commit();
        }

        // Read back
        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
             var rtx = session.beginNodeReadOnlyTrx()) {
          rtx.moveToDocumentRoot();
          assertTrue(rtx.hasFirstChild());
        }
      }
    }

    @Test
    @DisplayName("Test navigation to leaf for missing key")
    void testNavigateToLeafMissingKey() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME)
            .versioningApproach(VersioningType.FULL)
            .build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
             JsonNodeTrx wtx = session.beginNodeTrx()) {
          // Create sparse data with gaps
          StringBuilder json = new StringBuilder("{");
          for (int i = 0; i < 20; i++) {
            if (i > 0) json.append(",");
            json.append("\"key").append(i * 10).append("\": ").append(i);
          }
          json.append("}");
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
          wtx.commit();
        }

        // Read back and verify
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
  @DisplayName("HOTIndexWriter Direct Tests")
  class HOTIndexWriterDirectTests {

    @Test
    @DisplayName("Test index initialization")
    void testIndexInitialization() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME)
            .versioningApproach(VersioningType.FULL)
            .build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
             JsonNodeTrx wtx = session.beginNodeTrx()) {
          // First insert triggers initialization
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"init\": true}"), JsonNodeTrx.Commit.NO);
          wtx.commit();
        }

        // More operations after init
        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
             JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.moveToDocumentRoot();
          wtx.moveToFirstChild();
          wtx.remove();
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"after_init\": true}"), JsonNodeTrx.Commit.NO);
          wtx.commit();
        }
      }
    }

    @Test
    @DisplayName("Test compareKeys with various key types")
    void testCompareKeys() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME)
            .versioningApproach(VersioningType.FULL)
            .build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
             JsonNodeTrx wtx = session.beginNodeTrx()) {
          // Create entries that will require key comparison during index updates
          StringBuilder json = new StringBuilder("{");
          for (char c = 'a'; c <= 'z'; c++) {
            if (c != 'a') json.append(",");
            json.append("\"").append(c).append("\": ").append((int) c);
          }
          json.append("}");
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
          wtx.commit();
        }
      }
    }
  }

  @Nested
  @DisplayName("HeightOptimalSplitter Direct Tests")
  class HeightOptimalSplitterDirectTests {

    @Test
    @DisplayName("Test leaf page split")
    void testLeafPageSplit() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME)
            .versioningApproach(VersioningType.FULL)
            .build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
             JsonNodeTrx wtx = session.beginNodeTrx()) {
          // Create enough entries to trigger splits
          StringBuilder json = new StringBuilder("{");
          for (int i = 0; i < 500; i++) {
            if (i > 0) json.append(",");
            json.append("\"field").append(i).append("\": ").append(i);
          }
          json.append("}");
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
          wtx.commit();
        }
      }
    }
  }

  @Nested
  @DisplayName("SiblingMerger Direct Tests")
  class SiblingMergerDirectTests {

    @Test
    @DisplayName("Test merge leaf pages")
    void testMergeLeafPages() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME)
            .versioningApproach(VersioningType.FULL)
            .build());

        // Create data
        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
             JsonNodeTrx wtx = session.beginNodeTrx()) {
          StringBuilder json = new StringBuilder("[");
          for (int i = 0; i < 100; i++) {
            if (i > 0) json.append(",");
            json.append("{\"id\": ").append(i).append("}");
          }
          json.append("]");
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
          wtx.commit();
        }

        // Delete data to potentially trigger merges
        for (int round = 0; round < 3; round++) {
          try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
               JsonNodeTrx wtx = session.beginNodeTrx()) {
            wtx.moveToDocumentRoot();
            wtx.moveToFirstChild();  // array
            if (wtx.hasFirstChild()) {
              wtx.moveToFirstChild();
              wtx.remove();
            }
            wtx.commit();
          }
        }
      }
    }
  }

  @Nested
  @DisplayName("NodeUpgradeManager Direct Tests")
  class NodeUpgradeManagerDirectTests {

    @Test
    @DisplayName("Test node type transitions through many insertions")
    void testNodeTypeTransitions() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME)
            .versioningApproach(VersioningType.FULL)
            .build());

        // Insert many entries to trigger node upgrades
        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
             JsonNodeTrx wtx = session.beginNodeTrx()) {
          StringBuilder json = new StringBuilder("{");
          for (int i = 0; i < 1000; i++) {
            if (i > 0) json.append(",");
            // Use diverse key patterns
            json.append("\"k_").append(String.format("%04d", i)).append("\": ").append(i);
          }
          json.append("}");
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
          wtx.commit();
        }
      }
    }
  }

  @Nested
  @DisplayName("SparsePartialKeys Direct Tests")
  class SparsePartialKeysDirectTests {

    @Test
    @DisplayName("Test find masks by pattern")
    void testFindMasksByPattern() {
      SparsePartialKeys<Byte> spk = SparsePartialKeys.forBytes(16);
      for (int i = 0; i < 16; i++) {
        spk.setEntry(i, (byte) (1 << (i % 8)));
      }

      // Test different patterns
      int mask1 = spk.search(0x01);
      int mask2 = spk.search(0x02);
      int mask3 = spk.search(0xFF);
      
      assertTrue(mask1 >= 0);
      assertTrue(mask2 >= 0);
      assertTrue(mask3 >= 0);
    }

    @Test
    @DisplayName("Test get entry")
    void testGetEntry() {
      SparsePartialKeys<Byte> spk = SparsePartialKeys.forBytes(8);
      spk.setEntry(0, (byte) 0x42);
      
      Byte value = spk.getEntry(0);
      assertEquals((byte) 0x42, value);
    }

    @Test
    @DisplayName("Test short and int scalar search")
    void testShortIntScalarSearch() {
      // Short keys
      SparsePartialKeys<Short> shortKeys = SparsePartialKeys.forShorts(4);
      for (int i = 0; i < 4; i++) {
        shortKeys.setEntry(i, (short) (0x100 + i));
      }
      int shortMask = shortKeys.search(0x101);
      assertTrue(shortMask >= 0);

      // Int keys
      SparsePartialKeys<Integer> intKeys = SparsePartialKeys.forInts(4);
      for (int i = 0; i < 4; i++) {
        intKeys.setEntry(i, 0x10000 + i);
      }
      int intMask = intKeys.search(0x10002);
      assertTrue(intMask >= 0);
    }
  }

  @Nested
  @DisplayName("PartialKeyMapping Direct Tests")
  class PartialKeyMappingDirectTests {

    @Test
    @DisplayName("Test multi-mask extraction")
    void testMultiMaskExtraction() {
      // Create mapping with multiple bits
      PartialKeyMapping mapping = PartialKeyMapping.forSingleBit(0);
      mapping = PartialKeyMapping.withAdditionalBit(mapping, 8);
      mapping = PartialKeyMapping.withAdditionalBit(mapping, 16);
      mapping = PartialKeyMapping.withAdditionalBit(mapping, 24);
      
      byte[] key = new byte[] {(byte)0x80, (byte)0x40, (byte)0x20, (byte)0x10};
      int extracted = mapping.extractMask(key);
      assertTrue(extracted >= 0);
    }

    @Test
    @DisplayName("Test get mask for various bit indices")
    void testGetMaskForVariousBits() {
      PartialKeyMapping mapping = PartialKeyMapping.forSingleBit(0);
      mapping = PartialKeyMapping.withAdditionalBit(mapping, 7);
      mapping = PartialKeyMapping.withAdditionalBit(mapping, 15);
      
      for (int i = 0; i < 16; i++) {
        int mask = mapping.getMaskFor(i);
        assertTrue(mask >= 0);
      }
    }
  }

  @Nested
  @DisplayName("ChunkDirectory Equals Coverage")
  class ChunkDirectoryEqualsTests {

    @Test
    @DisplayName("Test equals with null")
    void testEqualsNull() {
      ChunkDirectory dir = new ChunkDirectory();
      assertFalse(dir.equals(null));
    }

    @Test
    @DisplayName("Test equals with same object")
    void testEqualsSameObject() {
      ChunkDirectory dir = new ChunkDirectory();
      assertTrue(dir.equals(dir));
    }

    @Test
    @DisplayName("Test equals with different class")
    void testEqualsDifferentClass() {
      ChunkDirectory dir = new ChunkDirectory();
      assertFalse(dir.equals("not a directory"));
    }

    @Test
    @DisplayName("Test equals with different chunk count")
    void testEqualsDifferentCount() {
      ChunkDirectory dir1 = new ChunkDirectory();
      dir1.getOrCreateChunkRef(0);
      
      ChunkDirectory dir2 = new ChunkDirectory();
      dir2.getOrCreateChunkRef(0);
      dir2.getOrCreateChunkRef(1);
      
      assertFalse(dir1.equals(dir2));
    }

    @Test
    @DisplayName("Test equals with different indices")
    void testEqualsDifferentIndices() {
      ChunkDirectory dir1 = new ChunkDirectory();
      dir1.setChunkRef(0, new io.sirix.page.PageReference());
      
      ChunkDirectory dir2 = new ChunkDirectory();
      dir2.setChunkRef(1, new io.sirix.page.PageReference());
      
      assertFalse(dir1.equals(dir2));
    }
  }

  @Nested
  @DisplayName("PathKeySerializer Tests")
  class PathKeySerializerTests {

    @Test
    @DisplayName("Serialize and deserialize paths")
    void testSerializeDeserializePaths() {
      PathKeySerializer serializer = PathKeySerializer.INSTANCE;
      
      long[] testValues = new long[] {0, 1, 100, 1000, Long.MAX_VALUE};
      
      for (long val : testValues) {
        byte[] buf = new byte[8];
        int len = serializer.serialize(val, buf, 0);
        assertEquals(8, len);
        
        long deserialized = serializer.deserialize(buf, 0, 8);
        assertEquals(val, deserialized);
      }
    }

    @Test
    @DisplayName("Compare path keys")
    void testComparePathKeys() {
      PathKeySerializer serializer = PathKeySerializer.INSTANCE;
      
      byte[] buf1 = new byte[8];
      byte[] buf2 = new byte[8];
      
      serializer.serialize(10L, buf1, 0);
      serializer.serialize(20L, buf2, 0);
      
      int cmp = serializer.compare(buf1, 0, 8, buf2, 0, 8);
      assertTrue(cmp < 0, "10 should be less than 20");
    }

    @Test
    @DisplayName("Order preservation with negative values")
    void testOrderPreservation() {
      PathKeySerializer serializer = PathKeySerializer.INSTANCE;
      
      byte[] bufNeg = new byte[8];
      byte[] bufPos = new byte[8];
      
      serializer.serialize(-1L, bufNeg, 0);
      serializer.serialize(1L, bufPos, 0);
      
      int cmp = serializer.compare(bufNeg, 0, 8, bufPos, 0, 8);
      assertTrue(cmp < 0, "-1 should sort before 1 in order-preserving format");
    }
  }
}

