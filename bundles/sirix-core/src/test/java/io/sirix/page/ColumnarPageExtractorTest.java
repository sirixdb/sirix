package io.sirix.page;

import io.sirix.access.ResourceConfiguration;
import io.sirix.index.IndexType;
import io.sirix.node.DeltaVarIntCodec;
import io.sirix.node.json.ObjectStringNode;
import io.sirix.node.json.StringNode;
import io.sirix.settings.Constants;
import io.sirix.settings.Fixed;
import net.openhft.hashing.LongHashFunction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;

import static io.sirix.cache.MemorySegmentAllocator.SIXTYFOUR_KB;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link ColumnarPageExtractor} — the core engine that extracts
 * string-type columns from slotted page MemorySegments.
 */
final class ColumnarPageExtractorTest {

  private static final int CAPACITY = 1024;
  private static final LongHashFunction HASH_FN = LongHashFunction.xx3();

  private Arena arena;
  private ColumnarPageExtractor extractor;

  // Output arrays (reused across tests)
  private final long[] nodeKeys = new long[CAPACITY];
  private final long[] parentKeys = new long[CAPACITY];
  private final int[] payloadOffs = new int[CAPACITY];
  private final int[] valueLens = new int[CAPACITY];
  private final boolean[] compressed = new boolean[CAPACITY];
  private final int[] deweyOffs = new int[CAPACITY];
  private final int[] deweyLens = new int[CAPACITY];

  @BeforeEach
  void setUp() {
    arena = Arena.ofConfined();
    extractor = new ColumnarPageExtractor();
  }

  @AfterEach
  void tearDown() {
    if (arena != null) {
      arena.close();
    }
  }

  // --- Helper: create a page at a given pageKey ---

  private KeyValueLeafPage createPage(final long recordPageKey) {
    return new KeyValueLeafPage(recordPageKey, IndexType.DOCUMENT,
        new ResourceConfiguration.Builder("testResource").build(), 1,
        arena.allocate(SIXTYFOUR_KB), null);
  }

  private KeyValueLeafPage createPageWithDewey(final long recordPageKey) {
    return new KeyValueLeafPage(recordPageKey, IndexType.DOCUMENT,
        new ResourceConfiguration.Builder("testResource").useDeweyIDs(true).build(), 1,
        arena.allocate(SIXTYFOUR_KB), null);
  }

  // --- Helper: create and serialize a StringNode ---

  private void serializeStringNode(final KeyValueLeafPage page, final long nodeKey,
      final long parentKey, final String value) {
    final int slot = (int) (nodeKey & (Constants.NDP_NODE_COUNT - 1));
    final byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    final StringNode node = new StringNode(nodeKey, parentKey, 0, 0,
        Fixed.NULL_NODE_KEY.getStandardProperty(),
        Fixed.NULL_NODE_KEY.getStandardProperty(),
        0, bytes, HASH_FN, (byte[]) null);
    node.setWriteSingleton(true);
    page.serializeNewRecord(node, nodeKey, slot);
  }

  // --- Helper: create and serialize an ObjectStringNode ---

  private void serializeObjectStringNode(final KeyValueLeafPage page, final long nodeKey,
      final long parentKey, final String value) {
    final int slot = (int) (nodeKey & (Constants.NDP_NODE_COUNT - 1));
    final byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    final ObjectStringNode node = new ObjectStringNode(nodeKey, parentKey, 0, 0,
        0, bytes, HASH_FN, (byte[]) null);
    node.setWriteSingleton(true);
    page.serializeNewRecord(node, nodeKey, slot);
  }

  // ==================== Tests ====================

  @Test
  void extractFromPageWithOnlyStrings() {
    final KeyValueLeafPage page = createPage(0);

    // Insert 3 StringNodes at slots 0, 1, 2
    serializeStringNode(page, 0, 100, "hello");
    serializeStringNode(page, 1, 101, "world");
    serializeStringNode(page, 2, 102, "test");

    final int writePos = extractor.extractStringsFromPage(
        page, nodeKeys, parentKeys, payloadOffs, valueLens,
        compressed, deweyOffs, deweyLens, 0);

    assertEquals(3, writePos, "Should extract 3 string nodes");

    // Verify node keys (page 0, slots 0-2)
    assertEquals(0, nodeKeys[0]);
    assertEquals(1, nodeKeys[1]);
    assertEquals(2, nodeKeys[2]);

    // Verify parent keys were delta-decoded correctly
    assertEquals(100, parentKeys[0]);
    assertEquals(101, parentKeys[1]);
    assertEquals(102, parentKeys[2]);

    // Verify payload lengths match UTF-8 byte lengths
    assertEquals(5, valueLens[0]); // "hello"
    assertEquals(5, valueLens[1]); // "world"
    assertEquals(4, valueLens[2]); // "test"

    // Verify payloads can be read back
    final MemorySegment seg = page.getSlottedPage();
    assertStringAt(seg, payloadOffs[0], valueLens[0], "hello");
    assertStringAt(seg, payloadOffs[1], valueLens[1], "world");
    assertStringAt(seg, payloadOffs[2], valueLens[2], "test");

    // No compression for plain strings
    assertFalse(compressed[0]);
    assertFalse(compressed[1]);
    assertFalse(compressed[2]);

    page.close();
  }

  @Test
  void extractFromMixedNodeTypePage() {
    final KeyValueLeafPage page = createPage(0);

    // Insert a StringNode at slot 5 and an ObjectStringNode at slot 10
    serializeStringNode(page, 5, 200, "stringVal");
    serializeObjectStringNode(page, 10, 300, "objStringVal");

    // Also insert a non-string node type by directly writing raw bytes
    // (simulates an ObjectNode at slot 0 — kindId != 30 and != 40)
    insertDummyNonStringNode(page, 0, (byte) 24); // ObjectNode kindId=24

    final int writePos = extractor.extractStringsFromPage(
        page, nodeKeys, parentKeys, payloadOffs, valueLens,
        compressed, deweyOffs, deweyLens, 0);

    assertEquals(2, writePos, "Should extract only string-type nodes");

    // Verify the two extracted nodes
    assertEquals(5, nodeKeys[0]);
    assertEquals(200, parentKeys[0]);
    assertEquals(9, valueLens[0]); // "stringVal" = 9 bytes

    assertEquals(10, nodeKeys[1]);
    assertEquals(300, parentKeys[1]);
    assertEquals(12, valueLens[1]); // "objStringVal" = 12 bytes

    page.close();
  }

  @Test
  void extractFromEmptyPage() {
    final KeyValueLeafPage page = createPage(0);

    final int writePos = extractor.extractStringsFromPage(
        page, nodeKeys, parentKeys, payloadOffs, valueLens,
        compressed, deweyOffs, deweyLens, 0);

    assertEquals(0, writePos, "Empty page should yield writePos=0");
    page.close();
  }

  @Test
  void extractFromSparsePageWithFewStrings() {
    final KeyValueLeafPage page = createPage(0);

    // Insert just 2 strings at widely spaced slots
    serializeStringNode(page, 100, 500, "sparse1");
    serializeStringNode(page, 900, 600, "sparse2");

    final int writePos = extractor.extractStringsFromPage(
        page, nodeKeys, parentKeys, payloadOffs, valueLens,
        compressed, deweyOffs, deweyLens, 0);

    assertEquals(2, writePos);
    assertEquals(100, nodeKeys[0]);
    assertEquals(900, nodeKeys[1]);
    assertEquals(500, parentKeys[0]);
    assertEquals(600, parentKeys[1]);

    page.close();
  }

  @Test
  void extractObjectStringNodes() {
    final KeyValueLeafPage page = createPage(0);

    serializeObjectStringNode(page, 0, 50, "obj1");
    serializeObjectStringNode(page, 1, 51, "obj2");

    final int writePos = extractor.extractStringsFromPage(
        page, nodeKeys, parentKeys, payloadOffs, valueLens,
        compressed, deweyOffs, deweyLens, 0);

    assertEquals(2, writePos);
    assertEquals(0, nodeKeys[0]);
    assertEquals(1, nodeKeys[1]);
    assertEquals(50, parentKeys[0]);
    assertEquals(51, parentKeys[1]);
    assertEquals(4, valueLens[0]); // "obj1"
    assertEquals(4, valueLens[1]); // "obj2"

    page.close();
  }

  @Test
  void extractWithDeweyIds() {
    final KeyValueLeafPage page = createPageWithDewey(0);

    // Create a string node with DeweyID
    final byte[] deweyBytes = new byte[]{1, 2, 3, 4};
    final int slot = 5;
    final long nodeKey = 5;
    final StringNode node = new StringNode(nodeKey, 100, 0, 0,
        Fixed.NULL_NODE_KEY.getStandardProperty(),
        Fixed.NULL_NODE_KEY.getStandardProperty(),
        0, "hello".getBytes(StandardCharsets.UTF_8), HASH_FN, deweyBytes);
    node.setWriteSingleton(true);
    page.serializeNewRecord(node, nodeKey, slot);

    final int writePos = extractor.extractStringsFromPage(
        page, nodeKeys, parentKeys, payloadOffs, valueLens,
        compressed, deweyOffs, deweyLens, 0);

    assertEquals(1, writePos);
    assertEquals(5, nodeKeys[0]);
    assertTrue(deweyLens[0] > 0, "DeweyID length should be > 0");
    assertTrue(deweyOffs[0] > 0, "DeweyID offset should be > 0");

    page.close();
  }

  @Test
  void extractWithoutDeweyIds() {
    final KeyValueLeafPage page = createPage(0); // no DeweyIDs

    serializeStringNode(page, 0, 100, "noDewey");

    final int writePos = extractor.extractStringsFromPage(
        page, nodeKeys, parentKeys, payloadOffs, valueLens,
        compressed, deweyOffs, deweyLens, 0);

    assertEquals(1, writePos);
    assertEquals(-1, deweyOffs[0], "DeweyID offset should be -1 when not stored");
    assertEquals(0, deweyLens[0], "DeweyID length should be 0 when not stored");

    page.close();
  }

  @Test
  void extractBatchFullMidPage() {
    final KeyValueLeafPage page = createPage(0);

    // Insert more strings than output array capacity (use small arrays)
    final long[] smallNodeKeys = new long[3];
    final long[] smallParentKeys = new long[3];
    final int[] smallPayloadOffs = new int[3];
    final int[] smallValueLens = new int[3];
    final boolean[] smallCompressed = new boolean[3];
    final int[] smallDeweyOffs = new int[3];
    final int[] smallDeweyLens = new int[3];

    // Insert 5 string nodes
    for (int i = 0; i < 5; i++) {
      serializeStringNode(page, i, 100 + i, "val" + i);
    }

    final int writePos = extractor.extractStringsFromPage(
        page, smallNodeKeys, smallParentKeys, smallPayloadOffs, smallValueLens,
        smallCompressed, smallDeweyOffs, smallDeweyLens, 0);

    assertEquals(3, writePos, "Should stop at batch capacity (3)");
    assertEquals(0, smallNodeKeys[0]);
    assertEquals(1, smallNodeKeys[1]);
    assertEquals(2, smallNodeKeys[2]);

    page.close();
  }

  @Test
  void extractAccumulatesFromWritePos() {
    final KeyValueLeafPage page1 = createPage(0);
    final KeyValueLeafPage page2 = createPage(1);

    serializeStringNode(page1, 0, 100, "page1_val");
    serializeStringNode(page2, 1024, 200, "page2_val");

    // Extract from page1 starting at writePos=0
    int writePos = extractor.extractStringsFromPage(
        page1, nodeKeys, parentKeys, payloadOffs, valueLens,
        compressed, deweyOffs, deweyLens, 0);
    assertEquals(1, writePos);

    // Extract from page2 continuing at writePos=1
    writePos = extractor.extractStringsFromPage(
        page2, nodeKeys, parentKeys, payloadOffs, valueLens,
        compressed, deweyOffs, deweyLens, writePos);
    assertEquals(2, writePos);

    // Verify both pages' data
    assertEquals(0, nodeKeys[0]);
    assertEquals(100, parentKeys[0]);
    assertEquals(1024, nodeKeys[1]);
    assertEquals(200, parentKeys[1]);

    page1.close();
    page2.close();
  }

  @Test
  void extractMixedStringAndObjectStringNodes() {
    final KeyValueLeafPage page = createPage(0);

    serializeStringNode(page, 0, 10, "string");
    serializeObjectStringNode(page, 1, 20, "objString");
    serializeStringNode(page, 2, 30, "anotherString");
    serializeObjectStringNode(page, 3, 40, "anotherObjStr");

    final int writePos = extractor.extractStringsFromPage(
        page, nodeKeys, parentKeys, payloadOffs, valueLens,
        compressed, deweyOffs, deweyLens, 0);

    assertEquals(4, writePos);
    assertEquals(6, valueLens[0]);  // "string"
    assertEquals(9, valueLens[1]);  // "objString"
    assertEquals(13, valueLens[2]); // "anotherString"
    assertEquals(13, valueLens[3]); // "anotherObjStr"

    page.close();
  }

  @Test
  void extractFromNullSlottedPage() {
    // Create a page but don't write anything — getSlottedPage() should still return
    // the allocated segment. Test with a completely null page scenario.
    // This is simulated by using a page whose slotted page we can't null directly,
    // but we verify the extractor handles populatedCount=0.
    final KeyValueLeafPage page = createPage(0);

    final int writePos = extractor.extractStringsFromPage(
        page, nodeKeys, parentKeys, payloadOffs, valueLens,
        compressed, deweyOffs, deweyLens, 0);

    assertEquals(0, writePos, "Page with no populated slots should return 0");
    page.close();
  }

  @Test
  void extractPreservesCorrectPageBaseNodeKey() {
    // Page key 5 → base node key = 5 << 10 = 5120
    final KeyValueLeafPage page = createPage(5);

    // Node at slot 7 → nodeKey = 5120 | 7 = 5127
    serializeStringNode(page, 5127, 5000, "pageKey5");

    final int writePos = extractor.extractStringsFromPage(
        page, nodeKeys, parentKeys, payloadOffs, valueLens,
        compressed, deweyOffs, deweyLens, 0);

    assertEquals(1, writePos);
    assertEquals(5127, nodeKeys[0]);
    assertEquals(5000, parentKeys[0]);

    page.close();
  }

  // ==================== Helpers ====================

  private void assertStringAt(final MemorySegment seg, final int offset,
      final int length, final String expected) {
    final byte[] bytes = new byte[length];
    MemorySegment.copy(seg, ValueLayout.JAVA_BYTE, offset, bytes, 0, length);
    assertEquals(expected, new String(bytes, StandardCharsets.UTF_8));
  }

  /**
   * Insert a dummy non-string record at the given slot by writing raw bytes.
   * This simulates a node type that the extractor should skip.
   */
  private void insertDummyNonStringNode(final KeyValueLeafPage page,
      final int slot, final byte kindId) {
    final MemorySegment seg = page.getSlottedPage();

    // Manually write a minimal record to the heap
    final int heapEnd = PageLayout.getHeapEnd(seg);
    final long absOffset = PageLayout.HEAP_START + (long) heapEnd;

    // Write a minimal record: [kindId] + 2 field offset bytes + some data
    seg.set(ValueLayout.JAVA_BYTE, absOffset, kindId);
    seg.set(ValueLayout.JAVA_BYTE, absOffset + 1, (byte) 0); // field0 offset
    seg.set(ValueLayout.JAVA_BYTE, absOffset + 2, (byte) 2); // field1 offset
    // Write 4 bytes of dummy data
    seg.set(ValueLayout.JAVA_INT_UNALIGNED, absOffset + 3, 0);
    final int recordLen = 7;

    // Update heap end
    PageLayout.setHeapEnd(seg, heapEnd + recordLen);
    PageLayout.setHeapUsed(seg, PageLayout.getHeapUsed(seg) + recordLen);

    // Set directory entry
    PageLayout.setDirEntry(seg, slot, heapEnd, recordLen, kindId & 0xFF);

    // Mark slot populated
    if (!PageLayout.isSlotPopulated(seg, slot)) {
      PageLayout.markSlotPopulated(seg, slot);
      PageLayout.setPopulatedCount(seg, PageLayout.getPopulatedCount(seg) + 1);
    }
  }
}
