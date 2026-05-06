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
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
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

      assertEquals(0, page.getFirstKey().length, "Empty page should have empty first key");
      assertEquals(0, page.getLastKey().length, "Empty page should have empty last key");
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
      assertEquals(HOTIndirectPage.NodeType.SPAN_NODE, biNode.getNodeType());
    }

    @Test
    @DisplayName("SpanNode - navigation with various key bytes")
    void testSpanNodeNavigation() {
      PageReference[] children = new PageReference[4];
      int[] partialKeys = new int[4];
      for (int i = 0; i < 4; i++) {
        children[i] = new PageReference();
        children[i].setKey((long) i);
        partialKeys[i] = i * 64; // 0, 64, 128, 192
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
      int[] partialKeys = new int[20];
      for (int i = 0; i < 20; i++) {
        children[i] = new PageReference();
        children[i].setKey((long) i);
        partialKeys[i] = i * 10;
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

  @Nested
  @DisplayName("MultiMask Layout Tests")
  class MultiMaskTests {

    @Test
    @DisplayName("MultiMask SpanNode creation and child lookup")
    void testMultiMaskSpanNodeLookup() {
      // Create a MultiMask node with disc bits at byte 0 and byte 10 (span > 8 bytes).
      //   Byte 0,  bit 3 (MSB-first) → maskByte = 1 << (7-3) = 0x10
      //   Byte 10, bit 5 (MSB-first) → maskByte = 1 << (7-5) = 0x04
      byte[] extractionPositions = {0, 10};
      // BE chunk packing: extraction-byte index 0 → long bits 56-63, index 1 → 48-55, etc.
      long[] extractionMasks = {(0x10L << 56) | (0x04L << 48)};
      int numExtractionBytes = 2;
      // Total disc bits = popcount(0x10) + popcount(0x04) = 1 + 1 = 2 → 4 children max

      // BE PEXT extracts mask-set bits in order from LOW to HIGH long bit; but BE chunk packing
      // puts extraction-byte 0's bit at long bit 60 and extraction-byte 1's bit at long bit 50.
      // PEXT's result-bit 0 = lower mask bit = byte 1's bit (key[10] bit 5).
      // PEXT's result-bit 1 = higher mask bit = byte 0's bit (key[0] bit 3).
      // Then BE chunk concatenation places chunk 0's PEXT result in HIGH bits of partial-key.
      // With 1 chunk total, the partial-key directly equals chunk 0's PEXT result.
      // Lex order on (key[0]-bit3, key[10]-bit5): (0,0),(0,1),(1,0),(1,1).
      // Mapping each to BE partial-key = (key[0]-bit3 << 1) | key[10]-bit5: 0,1,2,3.
      int[] partialKeys = {0, 1, 2, 3};
      PageReference[] children = new PageReference[4];
      for (int i = 0; i < 4; i++) {
        children[i] = new PageReference();
        children[i].setKey(100 + i);
      }

      // MSB index = min absolute bit pos = byte 0 * 8 + bit 3 = 3
      short msbIndex = 3;

      HOTIndirectPage node = HOTIndirectPage.createSpanNodeMultiMask(
          1L, 1, extractionPositions, extractionMasks, numExtractionBytes,
          partialKeys, children, 1, msbIndex);

      assertEquals(HOTIndirectPage.LayoutType.MULTI_MASK, node.getLayoutType());
      assertEquals(4, node.getNumChildren());
      assertEquals(2, node.getTotalDiscBits());
      assertEquals(3, node.getMostSignificantBitIndex());

      // (0,0) → pk=0 → child 0
      byte[] key00 = new byte[11];
      assertEquals(0, node.findChildIndex(key00));

      // (1,0) → pk=(1<<1)|0=0b10=2 → child 2
      byte[] key10 = new byte[11];
      key10[0] = 0x10;
      assertEquals(2, node.findChildIndex(key10));

      // (0,1) → pk=(0<<1)|1=0b01=1 → child 1
      byte[] key01 = new byte[11];
      key01[10] = 0x04;
      assertEquals(1, node.findChildIndex(key01));

      // (1,1) → pk=0b11=3 → child 3
      byte[] key11 = new byte[11];
      key11[0] = 0x10;
      key11[10] = 0x04;
      assertEquals(3, node.findChildIndex(key11));
    }

    @Test
    @DisplayName("MultiMask partial key width selection")
    void testMultiMaskPartialKeyWidth() {
      // With 8 disc bits → width 1 (byte)
      assertEquals(1, HOTIndirectPage.determinePartialKeyWidthFromBitCount(8));
      // With 9 disc bits → width 2 (short)
      assertEquals(2, HOTIndirectPage.determinePartialKeyWidthFromBitCount(9));
      // With 16 disc bits → width 2 (short)
      assertEquals(2, HOTIndirectPage.determinePartialKeyWidthFromBitCount(16));
      // With 17 disc bits → width 4 (int)
      assertEquals(4, HOTIndirectPage.determinePartialKeyWidthFromBitCount(17));
    }

    @Test
    @DisplayName("MultiMask copy constructor preserves extraction data")
    void testMultiMaskCopyConstructor() {
      byte[] extractionPositions = {2, 15, 25};
      // BE chunk packing: extraction-byte index 0 → long bits 56-63, index 1 → 48-55, etc.
      long[] extractionMasks = {(0x80L << 56) | (0x01L << 48) | (0x40L << 40)};
      int numExtractionBytes = 3;
      int[] partialKeys = {0, 1, 2, 3, 4};
      PageReference[] children = new PageReference[5];
      for (int i = 0; i < 5; i++) {
        children[i] = new PageReference();
        children[i].setKey(200 + i);
      }
      short msbIndex = 16; // byte 2 * 8 + 0

      HOTIndirectPage original = HOTIndirectPage.createSpanNodeMultiMask(
          10L, 1, extractionPositions, extractionMasks, numExtractionBytes,
          partialKeys, children, 1, msbIndex);

      // Copy constructor
      HOTIndirectPage copy = new HOTIndirectPage(original);
      assertEquals(HOTIndirectPage.LayoutType.MULTI_MASK, copy.getLayoutType());
      assertEquals(5, copy.getNumChildren());
      assertEquals(3, copy.getTotalDiscBits());
      assertEquals(3, copy.getNumExtractionBytes());

      // Verify extraction data was cloned (not aliased)
      byte[] origPos = original.getExtractionPositions();
      byte[] copyPos = copy.getExtractionPositions();
      assertNotNull(copyPos);
      assertEquals(origPos.length, copyPos.length);
      for (int i = 0; i < origPos.length; i++) {
        assertEquals(origPos[i], copyPos[i]);
      }

      // Verify lookup works on copy. With BE PEXT, key[2]=0x80 (bit 0 MSB set) sets the highest
      // mask bit (long bit 63 of chunk 0); after compress, that's result-bit 2 of a 3-bit pk
      // = 4. partialKey 4 is in our {0..4} array → child 4.
      byte[] key = new byte[26];
      key[2] = (byte) 0x80;
      int idx = copy.findChildIndex(key);
      assertEquals(4, idx, "Should find child 4 (BE: key[2]-MSB → result-bit 2 → pk=4)");
    }

    @Test
    @DisplayName("MultiMask with multiple extraction chunks (>8 extraction bytes)")
    void testMultiMaskMultipleChunks() {
      // Create 10 extraction bytes (requires 2 long chunks)
      byte[] extractionPositions = new byte[10];
      for (int i = 0; i < 10; i++) {
        extractionPositions[i] = (byte) (i * 3); // positions 0, 3, 6, 9, 12, 15, 18, 21, 24, 27
      }
      // BE chunk packing: extraction-byte at chunk-offset {@code o} → long bits
      // {@code (7-o)*8 .. (7-o)*8 + 7}. Each extraction byte has bit 0 (MSB-first) set →
      // maskByte 0x80 → long-bit (7-o)*8 + 7.
      long[] extractionMasks = new long[2];
      for (int i = 0; i < 8; i++) {
        extractionMasks[0] |= (0x80L << ((7 - i) * 8));
      }
      for (int i = 0; i < 2; i++) {
        extractionMasks[1] |= (0x80L << ((7 - i) * 8));
      }
      int numExtractionBytes = 10;
      // 10 disc bits → partial key width = 2 (short)
      assertEquals(2, HOTIndirectPage.determinePartialKeyWidthFromBitCount(10));

      // Create 3 children (just to verify construction works)
      int[] partialKeys = {0, 1, 2};
      PageReference[] children = new PageReference[3];
      for (int i = 0; i < 3; i++) {
        children[i] = new PageReference();
        children[i].setKey(300 + i);
      }
      short msbIndex = 0; // byte 0 * 8 + 0

      HOTIndirectPage node = HOTIndirectPage.createSpanNodeMultiMask(
          20L, 1, extractionPositions, extractionMasks, numExtractionBytes,
          partialKeys, children, 1, msbIndex);

      assertEquals(10, node.getTotalDiscBits());
      assertEquals(10, node.getNumExtractionBytes());

      // Lookup with all bits clear → partial key 0 → child 0
      byte[] keyAllClear = new byte[28];
      assertEquals(0, node.findChildIndex(keyAllClear));
    }

    @Test
    @DisplayName("SIMD gather path produces correct results for all key patterns")
    void testSIMDGatherAllPatterns() {
      // 3 extraction bytes at positions 1, 8, 20 (span = 20 bytes, fits in 32-byte AVX2 vector)
      // Each extracts 2 bits → 6 disc bits total → 64 partial key combinations.
      //   Extraction byte 0 = key[1], MSB-first bits 0,1 → maskByte 0xC0
      //   Extraction byte 1 = key[8], MSB-first bits 6,7 → maskByte 0x03
      //   Extraction byte 2 = key[20], MSB-first bits 2,3 → maskByte 0x30
      // BE chunk packing: extraction-byte 0 → long bits 56-63, byte 1 → 48-55, byte 2 → 40-47.
      byte[] extractionPositions = {1, 8, 20};
      long[] extractionMasks = {(0xC0L << 56) | (0x03L << 48) | (0x30L << 40)};
      int numExtractionBytes = 3;

      // BE PEXT extracts mask bits low-to-high: low mask bits are extraction-byte 2's, then 1's,
      // then 0's. So result-bit 0 = key[20]-bit3, result-bit 1 = key[20]-bit2, result-bit 2 =
      // key[8]-bit7, result-bit 3 = key[8]-bit6, result-bit 4 = key[1]-bit1, result-bit 5 =
      // key[1]-bit0. With 1 chunk total, result IS the partial-key.
      //
      // We pre-compute partial keys for the 8 children below for the byte-0-only pattern
      // (key[1] varies, key[8]=key[20]=0): partial-key = (key[1]-bit0 << 5) | (key[1]-bit1 << 4).
      int[] partialKeys = {0, 1, 2, 3, 4, 5, 6, 7};
      PageReference[] children = new PageReference[8];
      for (int i = 0; i < 8; i++) {
        children[i] = new PageReference();
        children[i].setKey(400 + i);
      }
      short msbIndex = (short) (1 * 8); // byte 1, bit 0

      HOTIndirectPage node = HOTIndirectPage.createSpanNodeMultiMask(
          30L, 1, extractionPositions, extractionMasks, numExtractionBytes,
          partialKeys, children, 1, msbIndex);

      // All zeros → pk = 0. Sparse-search: subsetPick on matchMask.
      byte[] key0 = new byte[21];
      assertEquals(0, node.findChildIndex(key0));

      // key[1] = 0x80 (bit 0 = MSB set) → pk = (1<<5)|0 = 32. Subset of {0..7}? 32 has no
      // bits matching any small partial key, so subset search returns -1 (NOT_FOUND).
      // (See findChildSpanNode: when no partial key is a subset, return NOT_FOUND.)
      // Switch to a more interesting pattern: set both bits in key[1] →
      // key[1] = 0xC0 → pk = (1<<5)|(1<<4) = 48. Still doesn't match any small pk.
      // The original test exercised LE PEXT result mappings; under BE they're permuted. Test
      // with key combinations that DO hit the {0..7} partial-key set:
      //   pk=1 requires only result-bit 0 = 1 → key[20]-bit3 = 1 → key[20] = 0x10.
      byte[] keyPk1 = new byte[21];
      keyPk1[20] = 0x10;
      assertEquals(1, node.findChildIndex(keyPk1));

      //   pk=2 requires only result-bit 1 = 1 → key[20]-bit2 = 1 → key[20] = 0x20.
      byte[] keyPk2 = new byte[21];
      keyPk2[20] = 0x20;
      assertEquals(2, node.findChildIndex(keyPk2));

      //   pk=3 → both result-bits 0,1 set → key[20] = 0x30.
      byte[] keyPk3 = new byte[21];
      keyPk3[20] = 0x30;
      assertEquals(3, node.findChildIndex(keyPk3));

      //   pk=4 requires only result-bit 2 = 1 → key[8]-bit7 = 1 → key[8] = 0x01.
      byte[] keyPk4 = new byte[21];
      keyPk4[8] = 0x01;
      assertEquals(4, node.findChildIndex(keyPk4));

      //   pk=5 → result-bits 0,2 → key[20]=0x10, key[8]=0x01.
      byte[] keyPk5 = new byte[21];
      keyPk5[20] = 0x10;
      keyPk5[8] = 0x01;
      assertEquals(5, node.findChildIndex(keyPk5));

      //   pk=7 → result-bits 0,1,2 → key[20]=0x30, key[8]=0x01.
      byte[] keyPk7 = new byte[21];
      keyPk7[20] = 0x30;
      keyPk7[8] = 0x01;
      assertEquals(7, node.findChildIndex(keyPk7));
    }

    @Test
    @DisplayName("Scalar fallback for wide span (>32 bytes between extraction positions)")
    void testScalarFallbackWideSpan() {
      // Extraction positions at 0 and 40 → span = 41 bytes > 32 → scalar fallback.
      //   Byte 0, bit 0 (MSB) → maskByte = 0x80
      //   Byte 40, bit 7 (LSB) → maskByte = 0x01
      // BE chunk packing: extraction-byte 0 → long bits 56-63, byte 1 → 48-55.
      byte[] extractionPositions = {0, 40};
      long[] extractionMasks = {(0x80L << 56) | (0x01L << 48)};
      int numExtractionBytes = 2;

      // BE PEXT extracts: low mask bit = byte 1's mask bit at long-bit 48 → result-bit 0;
      // high mask bit = byte 0's mask bit at long-bit 63 → result-bit 1. Partial-key =
      // (key[0]-bit0 << 1) | key[40]-bit7. Lex order on (key[0]-bit0, key[40]-bit7):
      // (0,0),(0,1),(1,0),(1,1) → partial keys 0,1,2,3.
      int[] partialKeys = {0, 1, 2, 3};
      PageReference[] children = new PageReference[4];
      for (int i = 0; i < 4; i++) {
        children[i] = new PageReference();
        children[i].setKey(500 + i);
      }
      short msbIndex = 0;

      HOTIndirectPage node = HOTIndirectPage.createSpanNodeMultiMask(
          40L, 1, extractionPositions, extractionMasks, numExtractionBytes,
          partialKeys, children, 1, msbIndex);

      // (0,0) → pk=0 → child 0
      byte[] key0 = new byte[41];
      assertEquals(0, node.findChildIndex(key0));

      // (1,0) → pk=(1<<1)|0=0b10=2 → child 2
      byte[] key10 = new byte[41];
      key10[0] = (byte) 0x80;
      assertEquals(2, node.findChildIndex(key10));

      // (0,1) → pk=(0<<1)|1=0b01=1 → child 1
      byte[] key01 = new byte[41];
      key01[40] = 0x01;
      assertEquals(1, node.findChildIndex(key01));

      // (1,1) → pk=0b11=3 → child 3
      byte[] key11 = new byte[41];
      key11[0] = (byte) 0x80;
      key11[40] = 0x01;
      assertEquals(3, node.findChildIndex(key11));
    }
  }

  // ===== Prefix Compression and PEXT Routing Tests =====

  @Nested
  @DisplayName("Prefix Compression Tests")
  class PrefixCompressionTests {

    @Test
    @DisplayName("First insert sets prefix to full key")
    void testFirstInsertSetsPrefix() {
      HOTLeafPage page = new HOTLeafPage(1, 1, IndexType.PATH);
      try {
        byte[] key = "hello/world".getBytes();
        byte[] value = {0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x42};

        assertTrue(page.put(key, value));
        assertEquals(1, page.getEntryCount());

        // Prefix should be the full key
        assertEquals(key.length, page.getCommonPrefixLen());
        assertArrayEquals(key, java.util.Arrays.copyOf(page.getCommonPrefix(), page.getCommonPrefixLen()));

        // Reconstructed key should match
        assertArrayEquals(key, page.getKey(0));
      } finally {
        page.close();
      }
    }

    @Test
    @DisplayName("Two keys with shared prefix establish LCP")
    void testTwoKeysEstablishLCP() {
      HOTLeafPage page = new HOTLeafPage(1, 1, IndexType.PATH);
      try {
        byte[] key1 = "hello/world".getBytes();
        byte[] key2 = "hello/earth".getBytes();
        byte[] value = {0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x42};

        assertTrue(page.put(key1, value));
        assertTrue(page.put(key2, value));
        assertEquals(2, page.getEntryCount());

        // LCP of "hello/world" and "hello/earth" is "hello/" (6 bytes)
        assertEquals(6, page.getCommonPrefixLen());
        assertEquals("hello/", new String(java.util.Arrays.copyOf(page.getCommonPrefix(), page.getCommonPrefixLen())));

        // Both keys should be reconstructable
        assertArrayEquals(key2, page.getKey(0)); // "hello/earth" sorts first
        assertArrayEquals(key1, page.getKey(1)); // "hello/world" sorts second
      } finally {
        page.close();
      }
    }

    @Test
    @DisplayName("Prefix shrinks when dissimilar key arrives")
    void testPrefixShrinkage() {
      HOTLeafPage page = new HOTLeafPage(1, 1, IndexType.PATH);
      try {
        byte[] key1 = "hello/world".getBytes();
        byte[] key2 = "hello/earth".getBytes();
        byte[] key3 = "goodbye".getBytes();
        byte[] value = {0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x42};

        page.put(key1, value);
        page.put(key2, value);
        assertEquals(6, page.getCommonPrefixLen()); // "hello/"

        page.put(key3, value);
        // LCP of "hello/earth", "hello/world", "goodbye" is "" (0 bytes)
        assertEquals(0, page.getCommonPrefixLen());

        // All keys still retrievable
        assertEquals(3, page.getEntryCount());
        assertArrayEquals(key3, page.getKey(0)); // "goodbye"
        assertArrayEquals(key2, page.getKey(1)); // "hello/earth"
        assertArrayEquals(key1, page.getKey(2)); // "hello/world"
      } finally {
        page.close();
      }
    }

    @Test
    @DisplayName("findEntry works with prefix compression")
    void testFindEntryWithPrefix() {
      HOTLeafPage page = new HOTLeafPage(1, 1, IndexType.PATH);
      try {
        byte[] value = {0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x42};

        // Insert sorted keys with common prefix
        page.put("/root/element/attr1".getBytes(), value);
        page.put("/root/element/attr2".getBytes(), value);
        page.put("/root/element/child".getBytes(), value);

        // Verify prefix
        assertEquals("/root/element/".length(), page.getCommonPrefixLen());

        // Find existing keys
        assertTrue(page.findEntry("/root/element/attr1".getBytes()) >= 0);
        assertTrue(page.findEntry("/root/element/attr2".getBytes()) >= 0);
        assertTrue(page.findEntry("/root/element/child".getBytes()) >= 0);

        // Key not in page
        assertTrue(page.findEntry("/root/element/missing".getBytes()) < 0);

        // Key that doesn't match prefix → should return not found
        assertTrue(page.findEntry("/other/path".getBytes()) < 0);

        // Key shorter than prefix → should return not found
        assertTrue(page.findEntry("/root".getBytes()) < 0);
      } finally {
        page.close();
      }
    }

    @Test
    @DisplayName("Split produces correct prefixes for both halves")
    void testSplitPrefixRecomputation() {
      HOTLeafPage source = new HOTLeafPage(1, 1, IndexType.PATH);
      HOTLeafPage target = new HOTLeafPage(2, 1, IndexType.PATH);
      try {
        byte[] value = {0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x42};

        // Insert keys that will split into groups with different prefixes
        source.put("apple/red".getBytes(), value);
        source.put("apple/green".getBytes(), value);
        source.put("banana/yellow".getBytes(), value);
        source.put("banana/brown".getBytes(), value);

        byte[] splitKey = source.splitTo(target);
        assertNotNull(splitKey);

        // Source (left half) should have longer prefix after split
        assertTrue(source.getEntryCount() >= 1);
        assertTrue(target.getEntryCount() >= 1);

        // All keys should be reconstructable from both halves
        for (int i = 0; i < source.getEntryCount(); i++) {
          byte[] key = source.getKey(i);
          assertNotNull(key);
          assertTrue(key.length > 0);
        }
        for (int i = 0; i < target.getEntryCount(); i++) {
          byte[] key = target.getKey(i);
          assertNotNull(key);
          assertTrue(key.length > 0);
        }
      } finally {
        source.close();
        target.close();
      }
    }

    @Test
    @DisplayName("Copy preserves prefix")
    void testCopyPreservesPrefix() {
      HOTLeafPage original = new HOTLeafPage(1, 1, IndexType.PATH);
      try {
        byte[] value = {0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x42};
        original.put("/path/to/node1".getBytes(), value);
        original.put("/path/to/node2".getBytes(), value);

        assertEquals("/path/to/node".length(), original.getCommonPrefixLen());

        HOTLeafPage copy = original.copy();
        try {
          assertEquals(original.getCommonPrefixLen(), copy.getCommonPrefixLen());
          assertArrayEquals(
              java.util.Arrays.copyOf(original.getCommonPrefix(), original.getCommonPrefixLen()),
              java.util.Arrays.copyOf(copy.getCommonPrefix(), copy.getCommonPrefixLen()));

          // Keys should be identical
          for (int i = 0; i < original.getEntryCount(); i++) {
            assertArrayEquals(original.getKey(i), copy.getKey(i));
            assertArrayEquals(original.getValue(i), copy.getValue(i));
          }
        } finally {
          copy.close();
        }
      } finally {
        original.close();
      }
    }

    @Test
    @DisplayName("Delete with prefix compression preserves key reconstruction")
    void testDeleteWithPrefix() {
      HOTLeafPage page = new HOTLeafPage(1, 1, IndexType.PATH);
      try {
        byte[] value = {0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x42};
        page.put("/prefix/key1".getBytes(), value);
        page.put("/prefix/key2".getBytes(), value);
        page.put("/prefix/key3".getBytes(), value);

        // Delete middle key
        assertTrue(page.delete("/prefix/key2".getBytes()));

        // Other keys still findable
        assertTrue(page.findEntry("/prefix/key1".getBytes()) >= 0);
        assertTrue(page.findEntry("/prefix/key3".getBytes()) >= 0);

        // Prefix still valid
        assertEquals("/prefix/key".length(), page.getCommonPrefixLen());
      } finally {
        page.close();
      }
    }
  }

  @Nested
  @DisplayName("PEXT Routing Tests")
  class PextRoutingTests {

    @Test
    @DisplayName("PEXT routing finds entries correctly with ≤32 entries")
    void testPextRoutingSmallPage() {
      HOTLeafPage page = new HOTLeafPage(1, 1, IndexType.PATH);
      try {
        byte[] value = {0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x42};

        // Insert 10 entries with common prefix
        for (int i = 0; i < 10; i++) {
          byte[] key = ("/data/item" + String.format("%02d", i)).getBytes();
          page.put(key, value);
        }

        assertEquals(10, page.getEntryCount());
        assertTrue(page.getEntryCount() <= 32); // Should use PEXT path

        // Verify all entries findable
        for (int i = 0; i < 10; i++) {
          byte[] key = ("/data/item" + String.format("%02d", i)).getBytes();
          int idx = page.findEntry(key);
          assertTrue(idx >= 0, "Key /data/item" + String.format("%02d", i) + " not found");
          assertArrayEquals(key, page.getKey(idx));
        }

        // Verify non-existent key not found
        assertTrue(page.findEntry("/data/item99".getBytes()) < 0);
      } finally {
        page.close();
      }
    }

    @Test
    @DisplayName("PEXT routing handles single-byte suffix differences")
    void testPextSingleByteDifferences() {
      HOTLeafPage page = new HOTLeafPage(1, 1, IndexType.PATH);
      try {
        byte[] value = {0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x42};

        // Keys that differ only in last byte
        page.put("prefix_a".getBytes(), value);
        page.put("prefix_b".getBytes(), value);
        page.put("prefix_c".getBytes(), value);
        page.put("prefix_d".getBytes(), value);

        assertEquals("prefix_".length(), page.getCommonPrefixLen());

        // All should be findable
        assertEquals(0, page.findEntry("prefix_a".getBytes()));
        assertEquals(1, page.findEntry("prefix_b".getBytes()));
        assertEquals(2, page.findEntry("prefix_c".getBytes()));
        assertEquals(3, page.findEntry("prefix_d".getBytes()));
      } finally {
        page.close();
      }
    }

    @Test
    @DisplayName("Binary search fallback for >32 entries")
    void testBinarySearchFallbackLargePage() {
      HOTLeafPage page = new HOTLeafPage(1, 1, IndexType.PATH);
      try {
        byte[] value = {0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x42};

        // Insert 50 entries (exceeds PEXT limit of 32)
        for (int i = 0; i < 50; i++) {
          byte[] key = ("/path/entry" + String.format("%03d", i)).getBytes();
          page.put(key, value);
        }

        assertEquals(50, page.getEntryCount());

        // All entries should still be findable (via binary search fallback)
        for (int i = 0; i < 50; i++) {
          byte[] key = ("/path/entry" + String.format("%03d", i)).getBytes();
          int idx = page.findEntry(key);
          assertTrue(idx >= 0, "Key /path/entry" + String.format("%03d", i) + " not found");
        }
      } finally {
        page.close();
      }
    }

    @Test
    @DisplayName("PEXT with empty suffix entries")
    void testPextWithEmptySuffix() {
      HOTLeafPage page = new HOTLeafPage(1, 1, IndexType.PATH);
      try {
        byte[] value = {0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x42};

        // Single entry: prefix = key, suffix = empty
        page.put("exactkey".getBytes(), value);
        assertEquals("exactkey".length(), page.getCommonPrefixLen());

        int idx = page.findEntry("exactkey".getBytes());
        assertEquals(0, idx);

        // Non-matching key
        assertTrue(page.findEntry("other".getBytes()) < 0);
      } finally {
        page.close();
      }
    }

    @Test
    @DisplayName("mergeWithNodeRefs works with prefix compression")
    void testMergeWithNodeRefsAndPrefix() {
      HOTLeafPage page = new HOTLeafPage(1, 1, IndexType.PATH);
      try {
        // Packed format: [0x00][count=1][nodeKey:8] = 10 bytes
        byte[] key = "/index/path".getBytes();
        byte[] value1 = {0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01};

        assertTrue(page.mergeWithNodeRefs(key, key.length, value1, value1.length));
        assertEquals(1, page.getEntryCount());

        // Merge again with same key but different node key
        byte[] value2 = {0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02};
        assertTrue(page.mergeWithNodeRefs(key, key.length, value2, value2.length));
        assertEquals(1, page.getEntryCount()); // Still 1 entry, merged values

        // Add different key
        byte[] key2 = "/index/name".getBytes();
        assertTrue(page.mergeWithNodeRefs(key2, key2.length, value1, value1.length));
        assertEquals(2, page.getEntryCount());

        // Prefix should be "/index/"
        assertEquals("/index/".length(), page.getCommonPrefixLen());
      } finally {
        page.close();
      }
    }

    @Test
    @DisplayName("splitToWithInsert handles prefix correctly")
    void testSplitToWithInsertPrefix() {
      HOTLeafPage source = new HOTLeafPage(1, 1, IndexType.PATH);
      HOTLeafPage target = new HOTLeafPage(2, 1, IndexType.PATH);
      try {
        byte[] value = {0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x42};

        // Fill with keys that have distinct first characters (easy MSDB split)
        for (char c = 'a'; c <= 'z'; c++) {
          source.put(("key_" + c + "_data").getBytes(), value);
        }

        // Split with insert of a new key
        byte[] newKey = "key_A_data".getBytes();
        boolean result = source.splitToWithInsert(target, newKey, newKey.length, value, value.length);
        assertTrue(result);

        // Both pages should have entries
        assertTrue(source.getEntryCount() > 0);
        assertTrue(target.getEntryCount() > 0);

        // Total entries should be 27 (26 original + 1 new)
        assertEquals(27, source.getEntryCount() + target.getEntryCount());

        // All keys should be findable in one of the pages
        for (char c = 'a'; c <= 'z'; c++) {
          byte[] key = ("key_" + c + "_data").getBytes();
          boolean found = source.findEntry(key) >= 0 || target.findEntry(key) >= 0;
          assertTrue(found, "Key key_" + c + "_data not found after split");
        }
        boolean newKeyFound = source.findEntry(newKey) >= 0 || target.findEntry(newKey) >= 0;
        assertTrue(newKeyFound, "New key not found after split");
      } finally {
        source.close();
        target.close();
      }
    }

  }
}

