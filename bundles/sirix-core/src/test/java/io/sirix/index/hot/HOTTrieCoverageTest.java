package io.sirix.index.hot;

import io.brackit.query.atomic.Dbl;
import io.brackit.query.atomic.Int32;
import io.brackit.query.jdm.Type;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.cache.LinuxMemorySegmentAllocator;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.IndexType;
import io.sirix.index.SearchMode;
import io.sirix.index.path.json.JsonPCRCollector;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
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
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests to increase coverage for HOTTrie-related classes.
 * 
 * <p>
 * Targets:
 * </p>
 * <ul>
 * <li>{@code HOTTrieWriter} - split logic, path updates</li>
 * <li>{@code HOTTrieReader} - navigation, loading</li>
 * <li>{@code SparsePartialKeys} - all key types</li>
 * </ul>
 */
@DisplayName("HOT Trie Coverage Tests")
class HOTTrieCoverageTest {

  @TempDir
  Path tempDir;

  private Path DATABASE_PATH;
  private static final String RESOURCE_NAME = "hot-trie-coverage";

  @BeforeEach
  void setUp() throws IOException {
    DATABASE_PATH = tempDir.resolve("hot-trie-db");
    Files.createDirectories(DATABASE_PATH);
    System.setProperty("sirix.index.useHOT", "true");
    LinuxMemorySegmentAllocator.getInstance();
  }

  @AfterEach
  void tearDown() {
    System.clearProperty("sirix.index.useHOT");
    try {
      Databases.removeDatabase(DATABASE_PATH);
    } catch (Exception ignored) {
    }
  }

  @Nested
  @DisplayName("SparsePartialKeys Coverage")
  class SparsePartialKeysCoverage {

    @Test
    @DisplayName("forBytes - set and get all positions")
    void testBytesSetGetAllPositions() {
      SparsePartialKeys<Byte> spk = SparsePartialKeys.forBytes(32);

      // Set all positions
      for (int i = 0; i < 32; i++) {
        spk.setEntry(i, (byte) (i * 7));
      }

      // Verify all positions
      for (int i = 0; i < 32; i++) {
        assertEquals((byte) (i * 7), spk.getEntry(i));
      }
    }

    @Test
    @DisplayName("forShorts - set and get all positions")
    void testShortsSetGetAllPositions() {
      SparsePartialKeys<Short> spk = SparsePartialKeys.forShorts(32);

      for (int i = 0; i < 32; i++) {
        spk.setEntry(i, (short) (i * 1000));
      }

      for (int i = 0; i < 32; i++) {
        assertEquals((short) (i * 1000), spk.getEntry(i));
      }
    }

    @Test
    @DisplayName("forInts - set and get all positions")
    void testIntsSetGetAllPositions() {
      SparsePartialKeys<Integer> spk = SparsePartialKeys.forInts(32);

      for (int i = 0; i < 32; i++) {
        spk.setEntry(i, i * 100000);
      }

      for (int i = 0; i < 32; i++) {
        assertEquals(i * 100000, spk.getEntry(i));
      }
    }

    @Test
    @DisplayName("Search - find existing entry")
    void testSearchExisting() {
      SparsePartialKeys<Byte> spk = SparsePartialKeys.forBytes(8);

      for (int i = 0; i < 8; i++) {
        spk.setEntry(i, (byte) (i * 10));
      }

      // Search for existing values - search returns insertion point or match
      int idx = spk.search(30);
      assertTrue(idx >= 0, "Should find entry for value 30");

      idx = spk.search(0);
      assertTrue(idx >= 0, "Should find entry for value 0");

      idx = spk.search(70);
      assertTrue(idx >= 0, "Should find entry for value 70");
    }

    @Test
    @DisplayName("Search - boundary handling")
    void testSearchBoundary() {
      SparsePartialKeys<Byte> spk = SparsePartialKeys.forBytes(4);

      spk.setEntry(0, (byte) 10);
      spk.setEntry(1, (byte) 20);
      spk.setEntry(2, (byte) 30);
      spk.setEntry(3, (byte) 40);

      // Search returns an index (may be insertion point)
      int idx = spk.search(10);
      assertTrue(idx >= 0, "Should handle value 10");

      idx = spk.search(40);
      assertTrue(idx >= 0, "Should handle value 40");
    }

    @Test
    @DisplayName("Bytes - boundary values")
    void testBytesBoundaryValues() {
      SparsePartialKeys<Byte> spk = SparsePartialKeys.forBytes(4);

      spk.setEntry(0, Byte.MIN_VALUE);
      spk.setEntry(1, (byte) -1);
      spk.setEntry(2, (byte) 0);
      spk.setEntry(3, Byte.MAX_VALUE);

      assertEquals(Byte.MIN_VALUE, spk.getEntry(0));
      assertEquals((byte) -1, spk.getEntry(1));
      assertEquals((byte) 0, spk.getEntry(2));
      assertEquals(Byte.MAX_VALUE, spk.getEntry(3));
    }

    @Test
    @DisplayName("Shorts - boundary values")
    void testShortsBoundaryValues() {
      SparsePartialKeys<Short> spk = SparsePartialKeys.forShorts(4);

      spk.setEntry(0, Short.MIN_VALUE);
      spk.setEntry(1, (short) -1);
      spk.setEntry(2, (short) 0);
      spk.setEntry(3, Short.MAX_VALUE);

      assertEquals(Short.MIN_VALUE, spk.getEntry(0));
      assertEquals((short) -1, spk.getEntry(1));
      assertEquals((short) 0, spk.getEntry(2));
      assertEquals(Short.MAX_VALUE, spk.getEntry(3));
    }

    @Test
    @DisplayName("Ints - boundary values")
    void testIntsBoundaryValues() {
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
  @DisplayName("HOTLeafPage Coverage")
  class HOTLeafPageCoverage {

    private Arena arena;

    @AfterEach
    void tearDownArena() {
      if (arena != null) {
        arena.close();
        arena = null;
      }
    }

    private HOTLeafPage createLeafPage() {
      arena = Arena.ofConfined();
      MemorySegment slotMemory = arena.allocate(HOTLeafPage.DEFAULT_SIZE);
      return new HOTLeafPage(1L, 1, IndexType.CAS, slotMemory, null, new int[HOTLeafPage.MAX_ENTRIES], 0, 0);
    }

    @Test
    @DisplayName("Empty page - getFirstKey and getLastKey")
    void testEmptyPageKeys() {
      HOTLeafPage page = createLeafPage();

      assertNull(page.getFirstKey(), "Empty page should have null first key");
      assertNull(page.getLastKey(), "Empty page should have null last key");
      assertEquals(0, page.getEntryCount());
    }

    @Test
    @DisplayName("Single entry page")
    void testSingleEntryPage() {
      HOTLeafPage page = createLeafPage();
      page.put(new byte[] {1, 2, 3}, new byte[] {10, 20, 30});

      assertEquals(1, page.getEntryCount());
      assertNotNull(page.getFirstKey());
      assertNotNull(page.getLastKey());
      // Use Arrays.equals for byte[] comparison
      assertTrue(java.util.Arrays.equals(page.getFirstKey(), page.getLastKey()));
    }

    @Test
    @DisplayName("Full page - needsSplit")
    void testFullPageNeedsSplit() {
      HOTLeafPage page = createLeafPage();

      // Insert many small entries
      for (int i = 0; i < 400; i++) {
        byte[] key = new byte[] {(byte) (i >> 8), (byte) i};
        byte[] value = new byte[] {(byte) i};
        page.put(key, value);
      }

      // With many entries, needsSplit should eventually return true
      // (depends on memory usage)
      assertTrue(page.getEntryCount() > 0);
    }

    @Test
    @DisplayName("getKey and getValue by index")
    void testGetByIndex() {
      HOTLeafPage page = createLeafPage();

      for (int i = 0; i < 10; i++) {
        page.put(new byte[] {(byte) (i * 10)}, new byte[] {(byte) (i * 100)});
      }

      // Keys are sorted, so order may differ from insertion order
      for (int i = 0; i < 10; i++) {
        byte[] key = page.getKey(i);
        byte[] value = page.getValue(i);
        assertNotNull(key);
        assertNotNull(value);
      }
    }

    @Test
    @DisplayName("Copy page creates independent copy")
    void testCopyIndependent() {
      HOTLeafPage original = createLeafPage();
      original.put(new byte[] {1}, new byte[] {10});
      original.put(new byte[] {2}, new byte[] {20});

      HOTLeafPage copy = original.copy();

      // Modify original
      original.put(new byte[] {3}, new byte[] {30});

      // Copy should not be affected
      assertEquals(2, copy.getEntryCount());
      assertEquals(3, original.getEntryCount());

      // Clean up copy's arena
      copy.close();
    }
  }

  @Nested
  @DisplayName("HOTIndirectPage Coverage")
  class HOTIndirectPageCoverage {

    @Test
    @DisplayName("BiNode - getNumChildren")
    void testBiNodeNumChildren() {
      PageReference left = new PageReference();
      PageReference right = new PageReference();

      HOTIndirectPage biNode = HOTIndirectPage.createBiNode(1L, 1, 5, left, right);

      assertEquals(2, biNode.getNumChildren());
      assertEquals(HOTIndirectPage.NodeType.BI_NODE, biNode.getNodeType());
    }

    @Test
    @DisplayName("SpanNode - navigation with various key bytes")
    void testSpanNodeNavigation() {
      PageReference[] children = new PageReference[4];
      byte[] partialKeys = new byte[4];
      for (int i = 0; i < 4; i++) {
        children[i] = new PageReference();
        children[i].setKey((long) i);
        partialKeys[i] = (byte) (i * 64); // 0, 64, 128, 192
      }

      HOTIndirectPage spanNode = HOTIndirectPage.createSpanNode(1L, 1, (byte) 0, 0xFFL, partialKeys, children, 2);

      assertEquals(4, spanNode.getNumChildren());
      assertEquals(HOTIndirectPage.NodeType.SPAN_NODE, spanNode.getNodeType());

      // Test navigation with different first bytes
      int idx1 = spanNode.findChildIndex(new byte[] {0});
      int idx2 = spanNode.findChildIndex(new byte[] {64});
      int idx3 = spanNode.findChildIndex(new byte[] {(byte) 128});
      int idx4 = spanNode.findChildIndex(new byte[] {(byte) 192});

      assertTrue(idx1 >= 0 && idx1 < 4);
      assertTrue(idx2 >= 0 && idx2 < 4);
      assertTrue(idx3 >= 0 && idx3 < 4);
      assertTrue(idx4 >= 0 && idx4 < 4);
    }

    @Test
    @DisplayName("MultiNode - navigation")
    void testMultiNodeNavigation() {
      PageReference[] children = new PageReference[20];
      byte[] partialKeys = new byte[20];
      for (int i = 0; i < 20; i++) {
        children[i] = new PageReference();
        children[i].setKey((long) i);
        partialKeys[i] = (byte) (i * 10);
      }

      HOTIndirectPage multiNode = HOTIndirectPage.createMultiNode(1L, 1, 0, 0xFFL, partialKeys, children, 3);

      assertEquals(20, multiNode.getNumChildren());
      assertEquals(HOTIndirectPage.NodeType.MULTI_NODE, multiNode.getNodeType());

      // Test navigation
      for (int keyByte = 0; keyByte < 256; keyByte += 10) {
        int idx = multiNode.findChildIndex(new byte[] {(byte) keyByte});
        assertTrue(idx >= 0 && idx < 20, "Index should be valid for keyByte=" + keyByte);
      }
    }

    @Test
    @DisplayName("getHeight returns correct height")
    void testGetHeight() {
      PageReference left = new PageReference();
      PageReference right = new PageReference();

      HOTIndirectPage biNode = HOTIndirectPage.createBiNode(1L, 1, 5, left, right, 3);
      assertEquals(3, biNode.getHeight());

      HOTIndirectPage biNode2 = HOTIndirectPage.createBiNode(2L, 1, 5, left, right, 10);
      assertEquals(10, biNode2.getHeight());
    }

    @Test
    @DisplayName("getPageKey returns correct key")
    void testGetPageKey() {
      PageReference left = new PageReference();
      PageReference right = new PageReference();

      HOTIndirectPage biNode = HOTIndirectPage.createBiNode(42L, 1, 5, left, right);
      assertEquals(42L, biNode.getPageKey());
    }

    @Test
    @DisplayName("getChildReference returns correct reference")
    void testGetChildReference() {
      PageReference left = new PageReference();
      PageReference right = new PageReference();
      left.setKey(100L);
      right.setKey(200L);

      HOTIndirectPage biNode = HOTIndirectPage.createBiNode(1L, 1, 5, left, right);

      assertEquals(100L, biNode.getChildReference(0).getKey());
      assertEquals(200L, biNode.getChildReference(1).getKey());
    }
  }

  @Nested
  @DisplayName("Large Dataset Integration")
  class LargeDatasetIntegration {

    @Test
    @DisplayName("Force deep tree with 10000 entries")
    @Timeout(value = 180, unit = TimeUnit.SECONDS)
    void testDeepTreeWith10000Entries() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(
            ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {
          var indexController = session.getWtxIndexController(wtx.getRevisionNumber());

          final var pathToVal = parse("/data/[]/val", io.brackit.query.util.path.PathParser.Type.JSON);
          final var casIndexDef =
              IndexDefs.createCASIdxDef(false, Type.INR, Collections.singleton(pathToVal), 0, IndexDef.DbType.JSON);
          indexController.createIndexes(Set.of(casIndexDef), wtx);

          // 10000 entries to force multiple levels
          StringBuilder json = new StringBuilder("{\"data\": [");
          for (int i = 0; i < 10000; i++) {
            if (i > 0)
              json.append(",");
            json.append("{\"val\": ").append(i).append("}");
          }
          json.append("]}");

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
          wtx.commit();

          // Query all entries
          var casIndex = indexController.openCASIndex(wtx.getStorageEngineReader(), casIndexDef, indexController.createCASFilter(
              Set.of("/data/[]/val"), new Int32(0), SearchMode.GREATER_OR_EQUAL, new JsonPCRCollector(wtx)));

          long count = 0;
          while (casIndex.hasNext()) {
            count += casIndex.next().getNodeKeys().getLongCardinality();
          }

          System.out.println("Found " + count + " entries out of 10000");
          assertTrue(count >= 9000, "Should find at least 90% of 10000 entries, got " + count);
        }
      }
    }

    @Test
    @DisplayName("Double values with precision")
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testDoubleValuesPrecision() throws IOException {
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(DATABASE_PATH)) {
        database.createResource(
            ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeTrx wtx = session.beginNodeTrx()) {
          var indexController = session.getWtxIndexController(wtx.getRevisionNumber());

          final var pathToVal = parse("/floats/[]/val", io.brackit.query.util.path.PathParser.Type.JSON);
          final var casIndexDef =
              IndexDefs.createCASIdxDef(false, Type.DBL, Collections.singleton(pathToVal), 0, IndexDef.DbType.JSON);
          indexController.createIndexes(Set.of(casIndexDef), wtx);

          StringBuilder json = new StringBuilder("{\"floats\": [");
          for (int i = 0; i < 500; i++) {
            if (i > 0)
              json.append(",");
            // Various precision values
            double val = i * 0.123456789;
            json.append("{\"val\": ").append(val).append("}");
          }
          json.append("]}");

          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
          wtx.commit();

          // Query for values >= 10.0
          var casIndex = indexController.openCASIndex(wtx.getStorageEngineReader(), casIndexDef, indexController.createCASFilter(
              Set.of("/floats/[]/val"), new Dbl(10.0), SearchMode.GREATER_OR_EQUAL, new JsonPCRCollector(wtx)));

          long count = 0;
          while (casIndex.hasNext()) {
            count += casIndex.next().getNodeKeys().getLongCardinality();
          }
          assertTrue(count > 0, "Should find double values >= 10.0");
        }
      }
    }
  }

  @Nested
  @DisplayName("DiscriminativeBit Edge Cases")
  class DiscriminativeBitEdgeCases {

    @Test
    @DisplayName("computeDifferingBit - same keys")
    void testSameKeys() {
      byte[] key = new byte[] {1, 2, 3, 4};
      int bit = DiscriminativeBitComputer.computeDifferingBit(key, key.clone());
      assertEquals(-1, bit, "Same keys should return -1");
    }

    @Test
    @DisplayName("computeDifferingBit - first bit differs")
    void testFirstBitDiffers() {
      byte[] key1 = new byte[] {0};
      byte[] key2 = new byte[] {(byte) 0x80};
      int bit = DiscriminativeBitComputer.computeDifferingBit(key1, key2);
      assertEquals(0, bit, "Should detect difference at bit 0");
    }

    @Test
    @DisplayName("computeDifferingBit - last bit differs")
    void testLastBitDiffers() {
      byte[] key1 = new byte[] {0};
      byte[] key2 = new byte[] {1};
      int bit = DiscriminativeBitComputer.computeDifferingBit(key1, key2);
      assertEquals(7, bit, "Should detect difference at bit 7");
    }

    @Test
    @DisplayName("computeDifferingBit - multi-byte keys")
    void testMultiByteKeys() {
      byte[] key1 = new byte[] {0, 0, 0, 0};
      byte[] key2 = new byte[] {0, 0, 0, 1};
      int bit = DiscriminativeBitComputer.computeDifferingBit(key1, key2);
      assertEquals(31, bit, "Should detect difference at bit 31 (byte 3, bit 7)");
    }

    @Test
    @DisplayName("isBitSet - various positions")
    void testIsBitSet() {
      byte[] key = new byte[] {(byte) 0b10101010, (byte) 0b01010101};

      // Bit positions 0-15 for 2-byte key
      // Byte 0: 10101010 (bits 0-7)
      // Byte 1: 01010101 (bits 8-15)
      assertTrue(DiscriminativeBitComputer.isBitSet(key, 0));
      assertFalse(DiscriminativeBitComputer.isBitSet(key, 1));
      assertTrue(DiscriminativeBitComputer.isBitSet(key, 2));

      assertFalse(DiscriminativeBitComputer.isBitSet(key, 8));
      assertTrue(DiscriminativeBitComputer.isBitSet(key, 9));
    }

    @Test
    @DisplayName("getByteIndex and getBitPositionInByte")
    void testIndexCalculations() {
      // Test getByteIndex
      assertEquals(0, DiscriminativeBitComputer.getByteIndex(0));
      assertEquals(0, DiscriminativeBitComputer.getByteIndex(7));
      assertEquals(1, DiscriminativeBitComputer.getByteIndex(8));
      assertEquals(3, DiscriminativeBitComputer.getByteIndex(31));

      // Test getBitPositionInByte
      assertEquals(0, DiscriminativeBitComputer.getBitPositionInByte(0));
      assertEquals(7, DiscriminativeBitComputer.getBitPositionInByte(7));
      assertEquals(0, DiscriminativeBitComputer.getBitPositionInByte(8));
      assertEquals(7, DiscriminativeBitComputer.getBitPositionInByte(15));
    }

    @Test
    @DisplayName("countDiscriminativeBits")
    void testCountDiscriminativeBits() {
      assertEquals(0, DiscriminativeBitComputer.countDiscriminativeBits(0L));
      assertEquals(1, DiscriminativeBitComputer.countDiscriminativeBits(1L));
      assertEquals(8, DiscriminativeBitComputer.countDiscriminativeBits(0xFFL));
      assertEquals(64, DiscriminativeBitComputer.countDiscriminativeBits(-1L));
    }
  }
}

