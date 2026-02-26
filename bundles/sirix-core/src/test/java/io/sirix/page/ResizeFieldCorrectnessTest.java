package io.sirix.page;

import io.sirix.access.ResourceConfiguration;
import io.sirix.index.IndexType;
import io.sirix.node.DeltaVarIntCodec;
import io.sirix.node.json.ObjectNode;
import io.sirix.settings.Fixed;
import net.openhft.hashing.LongHashFunction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.stream.Stream;

import static io.sirix.cache.LinuxMemorySegmentAllocator.SIXTYFOUR_KB;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link DeltaVarIntCodec#resizeField} and {@link KeyValueLeafPage#resizeRecordField}.
 *
 * <p>Verifies INV-4 (offset table consistency) and INV-5 (raw-copy resize correctness)
 * from the LeanStore write path plan.
 */
class ResizeFieldCorrectnessTest {

  private static final long NODE_KEY = 1000L;
  private static final long PARENT_KEY = 999L;
  private static final long RIGHT_SIB_KEY = 1001L;
  private static final long LEFT_SIB_KEY = Fixed.NULL_NODE_KEY.getStandardProperty();
  private static final long FIRST_CHILD_KEY = 1002L;
  private static final long LAST_CHILD_KEY = 1005L;
  private static final int PREV_REVISION = 0;
  private static final int LAST_MOD_REVISION = 1;
  private static final long HASH = 0xDEADBEEFCAFEL;
  private static final long CHILD_COUNT = 4L;
  private static final long DESCENDANT_COUNT = 42L;

  private KeyValueLeafPage page;
  private Arena arena;
  private ObjectNode objectNode;

  @BeforeEach
  void setUp() {
    arena = Arena.ofConfined();
    page = new KeyValueLeafPage(0L, IndexType.DOCUMENT,
        new ResourceConfiguration.Builder("testResource").build(), 1,
        arena.allocate(SIXTYFOUR_KB), null);

    objectNode = new ObjectNode(NODE_KEY, PARENT_KEY, PREV_REVISION, LAST_MOD_REVISION,
        RIGHT_SIB_KEY, LEFT_SIB_KEY, FIRST_CHILD_KEY, LAST_CHILD_KEY,
        CHILD_COUNT, DESCENDANT_COUNT, HASH,
        LongHashFunction.xx3(), (byte[]) null);
    objectNode.setWriteSingleton(true);
  }

  @AfterEach
  void tearDown() {
    if (page != null) {
      page.close();
    }
    if (arena != null) {
      arena.close();
    }
  }

  /**
   * Helper: serialize ObjectNode to the page heap at slot 0, then bind it.
   */
  private void serializeAndBind() {
    page.serializeNewRecord(objectNode, NODE_KEY, 0);
    // serializeNewRecord calls clearBinding, so re-bind manually
    final MemorySegment sp = page.getSlottedPage();
    final int heapOff = PageLayout.getDirHeapOffset(sp, 0);
    final long recordBase = PageLayout.heapAbsoluteOffset(heapOff);
    objectNode.bind(sp, recordBase, NODE_KEY, 0);
    objectNode.setOwnerPage(page);
  }

  /**
   * Verify all 10 fields of the bound ObjectNode match expected values.
   */
  private void assertAllFields(final long expectedParent, final long expectedRightSib,
      final long expectedLeftSib, final long expectedFirstChild, final long expectedLastChild,
      final int expectedPrevRev, final int expectedLastModRev, final long expectedHash,
      final long expectedChildCount, final long expectedDescCount) {
    assertEquals(expectedParent, objectNode.getParentKey(), "parentKey");
    assertEquals(expectedRightSib, objectNode.getRightSiblingKey(), "rightSiblingKey");
    assertEquals(expectedLeftSib, objectNode.getLeftSiblingKey(), "leftSiblingKey");
    assertEquals(expectedFirstChild, objectNode.getFirstChildKey(), "firstChildKey");
    assertEquals(expectedLastChild, objectNode.getLastChildKey(), "lastChildKey");
    assertEquals(expectedPrevRev, objectNode.getPreviousRevisionNumber(), "previousRevision");
    assertEquals(expectedLastModRev, objectNode.getLastModifiedRevisionNumber(), "lastModifiedRevision");
    assertEquals(expectedHash, objectNode.getHash(), "hash");
    assertEquals(expectedChildCount, objectNode.getChildCount(), "childCount");
    assertEquals(expectedDescCount, objectNode.getDescendantCount(), "descendantCount");
  }

  // ==================== INV-5: Raw-Copy Resize Correctness ====================

  /**
   * Provides (fieldIndex, newValue) pairs for each of the 10 ObjectNode fields.
   * Each new value is chosen to require a different varint width than the original.
   */
  static Stream<Arguments> fieldResizeParameters() {
    // Original values produce small deltas (1-2 byte varints).
    // New values produce large deltas (3+ byte varints) to force width change.
    final long farKey = NODE_KEY + 100_000L; // large delta → 3-byte varint
    return Stream.of(
        // fieldIndex, newValue (key or int/long), description
        Arguments.of(NodeFieldLayout.OBJECT_PARENT_KEY, farKey, "parentKey"),
        Arguments.of(NodeFieldLayout.OBJECT_RIGHT_SIB_KEY, farKey, "rightSiblingKey"),
        Arguments.of(NodeFieldLayout.OBJECT_LEFT_SIB_KEY, farKey, "leftSiblingKey"),
        Arguments.of(NodeFieldLayout.OBJECT_FIRST_CHILD_KEY, farKey, "firstChildKey"),
        Arguments.of(NodeFieldLayout.OBJECT_LAST_CHILD_KEY, farKey, "lastChildKey"),
        // Revision fields: small → large signed varint
        Arguments.of(NodeFieldLayout.OBJECT_PREV_REVISION, 100_000L, "previousRevision"),
        Arguments.of(NodeFieldLayout.OBJECT_LAST_MOD_REVISION, 100_000L, "lastModifiedRevision"),
        // Hash: fixed 8 bytes, no width change — test that it survives resize of OTHER field
        // childCount/descendantCount: signed long varint
        Arguments.of(NodeFieldLayout.OBJECT_CHILD_COUNT, 100_000L, "childCount"),
        Arguments.of(NodeFieldLayout.OBJECT_DESCENDANT_COUNT, 100_000L, "descendantCount")
    );
  }

  @ParameterizedTest(name = "resize field {2} (index={0})")
  @MethodSource("fieldResizeParameters")
  void resizeRecordField_preservesUnchangedFields(final int fieldIndex, final long newValue,
      final String fieldName) {
    serializeAndBind();

    // Record heap state before resize
    final MemorySegment sp = page.getSlottedPage();
    final int heapEndBefore = PageLayout.getHeapEnd(sp);

    // Perform raw-copy resize via KVL
    final DeltaVarIntCodec.FieldEncoder encoder = createEncoder(fieldIndex, newValue);
    page.resizeRecordField(objectNode, NODE_KEY, 0, fieldIndex,
        NodeFieldLayout.OBJECT_FIELD_COUNT, encoder);

    // INV-2: Heap monotonicity
    final int heapEndAfter = PageLayout.getHeapEnd(page.getSlottedPage());
    assertTrue(heapEndAfter > heapEndBefore, "heapEnd must advance after resize");

    // INV-5: All unchanged fields preserved, changed field updated
    final long expectedParent = fieldIndex == NodeFieldLayout.OBJECT_PARENT_KEY ? newValue : PARENT_KEY;
    final long expectedRightSib = fieldIndex == NodeFieldLayout.OBJECT_RIGHT_SIB_KEY ? newValue : RIGHT_SIB_KEY;
    final long expectedLeftSib = fieldIndex == NodeFieldLayout.OBJECT_LEFT_SIB_KEY ? newValue : LEFT_SIB_KEY;
    final long expectedFirstChild = fieldIndex == NodeFieldLayout.OBJECT_FIRST_CHILD_KEY ? newValue : FIRST_CHILD_KEY;
    final long expectedLastChild = fieldIndex == NodeFieldLayout.OBJECT_LAST_CHILD_KEY ? newValue : LAST_CHILD_KEY;
    final int expectedPrevRev = fieldIndex == NodeFieldLayout.OBJECT_PREV_REVISION ? (int) newValue : PREV_REVISION;
    final int expectedLastModRev = fieldIndex == NodeFieldLayout.OBJECT_LAST_MOD_REVISION ? (int) newValue : LAST_MOD_REVISION;
    final long expectedChildCount = fieldIndex == NodeFieldLayout.OBJECT_CHILD_COUNT ? newValue : CHILD_COUNT;
    final long expectedDescCount = fieldIndex == NodeFieldLayout.OBJECT_DESCENDANT_COUNT ? newValue : DESCENDANT_COUNT;

    assertAllFields(expectedParent, expectedRightSib, expectedLeftSib,
        expectedFirstChild, expectedLastChild,
        expectedPrevRev, expectedLastModRev,
        HASH, // hash is always fixed 8 bytes, never changed in this test
        expectedChildCount, expectedDescCount);
  }

  /**
   * Test resizing a field that shrinks (large varint → small varint).
   * This exercises the negative widthDelta path.
   */
  @Test
  void resizeRecordField_shrinkField() {
    // Create node with a far-away parent (large delta → multi-byte varint)
    final long farParent = NODE_KEY + 100_000L;
    objectNode = new ObjectNode(NODE_KEY, farParent, PREV_REVISION, LAST_MOD_REVISION,
        RIGHT_SIB_KEY, LEFT_SIB_KEY, FIRST_CHILD_KEY, LAST_CHILD_KEY,
        CHILD_COUNT, DESCENDANT_COUNT, HASH,
        LongHashFunction.xx3(), (byte[]) null);
    objectNode.setWriteSingleton(true);
    serializeAndBind();

    // Now resize parentKey back to a nearby value (small delta → 1-byte varint)
    final long nearParent = NODE_KEY - 1;
    final DeltaVarIntCodec.FieldEncoder encoder = (target, offset) ->
        DeltaVarIntCodec.writeDeltaToSegment(target, offset, nearParent, NODE_KEY);
    page.resizeRecordField(objectNode, NODE_KEY, 0, NodeFieldLayout.OBJECT_PARENT_KEY,
        NodeFieldLayout.OBJECT_FIELD_COUNT, encoder);

    assertAllFields(nearParent, RIGHT_SIB_KEY, LEFT_SIB_KEY,
        FIRST_CHILD_KEY, LAST_CHILD_KEY,
        PREV_REVISION, LAST_MOD_REVISION, HASH,
        CHILD_COUNT, DESCENDANT_COUNT);
  }

  /**
   * Test resizing NULL_NODE_KEY field to a real value and back.
   */
  @Test
  void resizeRecordField_nullToReal_and_realToNull() {
    serializeAndBind();

    // leftSiblingKey starts as NULL (-1). Resize to a real value.
    final long realSib = NODE_KEY + 50_000L;
    DeltaVarIntCodec.FieldEncoder encoder = (target, offset) ->
        DeltaVarIntCodec.writeDeltaToSegment(target, offset, realSib, NODE_KEY);
    page.resizeRecordField(objectNode, NODE_KEY, 0, NodeFieldLayout.OBJECT_LEFT_SIB_KEY,
        NodeFieldLayout.OBJECT_FIELD_COUNT, encoder);

    assertEquals(realSib, objectNode.getLeftSiblingKey());
    // Other fields preserved
    assertEquals(PARENT_KEY, objectNode.getParentKey());
    assertEquals(HASH, objectNode.getHash());

    // Now resize back to NULL
    final long nullKey = Fixed.NULL_NODE_KEY.getStandardProperty();
    encoder = (target, offset) ->
        DeltaVarIntCodec.writeDeltaToSegment(target, offset, nullKey, NODE_KEY);
    page.resizeRecordField(objectNode, NODE_KEY, 0, NodeFieldLayout.OBJECT_LEFT_SIB_KEY,
        NodeFieldLayout.OBJECT_FIELD_COUNT, encoder);

    assertEquals(nullKey, objectNode.getLeftSiblingKey());
    assertEquals(PARENT_KEY, objectNode.getParentKey());
    assertEquals(HASH, objectNode.getHash());
  }

  /**
   * Test multiple consecutive resizes on the same node (fragmentation test).
   * INV-2: heap must grow monotonically. INV-8: fragmentation tracked.
   */
  @Test
  void resizeRecordField_multipleResizes_heapMonotonic() {
    serializeAndBind();

    final MemorySegment sp = page.getSlottedPage();
    int prevHeapEnd = PageLayout.getHeapEnd(sp);

    // Resize 5 different fields consecutively
    final long farKey = NODE_KEY + 200_000L;
    final int[] fields = {
        NodeFieldLayout.OBJECT_PARENT_KEY,
        NodeFieldLayout.OBJECT_RIGHT_SIB_KEY,
        NodeFieldLayout.OBJECT_FIRST_CHILD_KEY,
        NodeFieldLayout.OBJECT_CHILD_COUNT,
        NodeFieldLayout.OBJECT_DESCENDANT_COUNT
    };

    for (final int field : fields) {
      final DeltaVarIntCodec.FieldEncoder encoder = createEncoder(field, farKey);
      page.resizeRecordField(objectNode, NODE_KEY, 0, field,
          NodeFieldLayout.OBJECT_FIELD_COUNT, encoder);

      final int currentHeapEnd = PageLayout.getHeapEnd(page.getSlottedPage());
      assertTrue(currentHeapEnd > prevHeapEnd,
          "heapEnd must advance after resize of field " + field);
      prevHeapEnd = currentHeapEnd;
    }

    // Verify all resized fields have the new value
    assertEquals(farKey, objectNode.getParentKey());
    assertEquals(farKey, objectNode.getRightSiblingKey());
    assertEquals(farKey, objectNode.getFirstChildKey());
    assertEquals(farKey, objectNode.getChildCount());
    assertEquals(farKey, objectNode.getDescendantCount());

    // Unchanged fields preserved
    assertEquals(LEFT_SIB_KEY, objectNode.getLeftSiblingKey());
    assertEquals(LAST_CHILD_KEY, objectNode.getLastChildKey());
    assertEquals(HASH, objectNode.getHash());
  }

  // ==================== INV-4: Offset Table Consistency ====================

  @Test
  void resizeRecordField_offsetTableMonotonic() {
    serializeAndBind();

    // Resize a middle field to force offset shifts
    final long farKey = NODE_KEY + 100_000L;
    final DeltaVarIntCodec.FieldEncoder encoder = (target, offset) ->
        DeltaVarIntCodec.writeDeltaToSegment(target, offset, farKey, NODE_KEY);
    page.resizeRecordField(objectNode, NODE_KEY, 0,
        NodeFieldLayout.OBJECT_FIRST_CHILD_KEY,
        NodeFieldLayout.OBJECT_FIELD_COUNT, encoder);

    // Read offset table and verify monotonicity
    final MemorySegment sp = page.getSlottedPage();
    final int heapOffset = PageLayout.getDirHeapOffset(sp, 0);
    final long recordBase = PageLayout.heapAbsoluteOffset(heapOffset);

    int prevOffset = -1;
    for (int i = 0; i < NodeFieldLayout.OBJECT_FIELD_COUNT; i++) {
      final int fieldOff = sp.get(ValueLayout.JAVA_BYTE, recordBase + 1 + i) & 0xFF;
      assertTrue(fieldOff > prevOffset || (i == 0 && fieldOff == 0),
          "offset table must be monotonically increasing: field " + i
              + " offset=" + fieldOff + " prev=" + prevOffset);
      prevOffset = fieldOff;
    }
  }

  // ==================== INV-3: Directory-Heap Consistency ====================

  @Test
  void resizeRecordField_directoryPointsWithinHeap() {
    serializeAndBind();

    // Resize
    final long farKey = NODE_KEY + 100_000L;
    final DeltaVarIntCodec.FieldEncoder encoder = (target, offset) ->
        DeltaVarIntCodec.writeDeltaToSegment(target, offset, farKey, NODE_KEY);
    page.resizeRecordField(objectNode, NODE_KEY, 0,
        NodeFieldLayout.OBJECT_PARENT_KEY,
        NodeFieldLayout.OBJECT_FIELD_COUNT, encoder);

    final MemorySegment sp = page.getSlottedPage();
    final int dirHeapOff = PageLayout.getDirHeapOffset(sp, 0);
    final int dirDataLen = PageLayout.getDirDataLength(sp, 0);
    final int heapEnd = PageLayout.getHeapEnd(sp);

    assertTrue(dirHeapOff + dirDataLen <= heapEnd,
        "directory entry must point within heap bounds");
    assertTrue(PageLayout.isSlotPopulated(sp, 0), "slot must remain populated");
    assertTrue(PageLayout.getDirNodeKindId(sp, 0) > 0, "nodeKindId must be > 0");
  }

  // ==================== DeltaVarIntCodec.resizeField unit test ====================

  @Test
  void resizeField_directCodecCall() {
    // Create a small MemorySegment with a mock record:
    // [kindByte:1][offsets:3][field0:1byte][field1:1byte][field2:1byte]
    final int fieldCount = 3;
    final byte[] src = new byte[64];
    final MemorySegment srcSeg = MemorySegment.ofArray(src);

    // Write mock record
    srcSeg.set(ValueLayout.JAVA_BYTE, 0, (byte) 42); // nodeKind
    // Offset table: field0 at 0, field1 at 2, field2 at 4
    srcSeg.set(ValueLayout.JAVA_BYTE, 1, (byte) 0);
    srcSeg.set(ValueLayout.JAVA_BYTE, 2, (byte) 2);
    srcSeg.set(ValueLayout.JAVA_BYTE, 3, (byte) 4);
    // Data region starts at offset 4 (1 + 3)
    // field0: 2 bytes [0xAA, 0xBB]
    srcSeg.set(ValueLayout.JAVA_BYTE, 4, (byte) 0xAA);
    srcSeg.set(ValueLayout.JAVA_BYTE, 5, (byte) 0xBB);
    // field1: 2 bytes [0xCC, 0xDD]
    srcSeg.set(ValueLayout.JAVA_BYTE, 6, (byte) 0xCC);
    srcSeg.set(ValueLayout.JAVA_BYTE, 7, (byte) 0xDD);
    // field2: 2 bytes [0xEE, 0xFF]
    srcSeg.set(ValueLayout.JAVA_BYTE, 8, (byte) 0xEE);
    srcSeg.set(ValueLayout.JAVA_BYTE, 9, (byte) 0xFF);

    final int srcRecordLen = 10; // 1 + 3 + 6

    // Resize field1 from 2 bytes to 3 bytes [0x11, 0x22, 0x33]
    final byte[] dst = new byte[64];
    final MemorySegment dstSeg = MemorySegment.ofArray(dst);

    final int newRecordLen = DeltaVarIntCodec.resizeField(
        srcSeg, 0, srcRecordLen,
        fieldCount, 1,
        dstSeg, 0,
        (target, offset) -> {
          target.set(ValueLayout.JAVA_BYTE, offset, (byte) 0x11);
          target.set(ValueLayout.JAVA_BYTE, offset + 1, (byte) 0x22);
          target.set(ValueLayout.JAVA_BYTE, offset + 2, (byte) 0x33);
          return 3;
        });

    // Expected: 1 + 3 + (2 + 3 + 2) = 11 bytes
    assertEquals(11, newRecordLen);

    // NodeKind preserved
    assertEquals(42, dstSeg.get(ValueLayout.JAVA_BYTE, 0));

    // Offset table: field0=0, field1=2 (unchanged), field2=5 (shifted by +1)
    assertEquals(0, dstSeg.get(ValueLayout.JAVA_BYTE, 1) & 0xFF);
    assertEquals(2, dstSeg.get(ValueLayout.JAVA_BYTE, 2) & 0xFF);
    assertEquals(5, dstSeg.get(ValueLayout.JAVA_BYTE, 3) & 0xFF);

    // Data region at offset 4:
    // field0: [0xAA, 0xBB] (unchanged)
    assertEquals((byte) 0xAA, dstSeg.get(ValueLayout.JAVA_BYTE, 4));
    assertEquals((byte) 0xBB, dstSeg.get(ValueLayout.JAVA_BYTE, 5));
    // field1: [0x11, 0x22, 0x33] (new value)
    assertEquals((byte) 0x11, dstSeg.get(ValueLayout.JAVA_BYTE, 6));
    assertEquals((byte) 0x22, dstSeg.get(ValueLayout.JAVA_BYTE, 7));
    assertEquals((byte) 0x33, dstSeg.get(ValueLayout.JAVA_BYTE, 8));
    // field2: [0xEE, 0xFF] (unchanged, shifted)
    assertEquals((byte) 0xEE, dstSeg.get(ValueLayout.JAVA_BYTE, 9));
    assertEquals((byte) 0xFF, dstSeg.get(ValueLayout.JAVA_BYTE, 10));
  }

  // ==================== Helpers ====================

  /**
   * Create a FieldEncoder for the given ObjectNode field index and new value.
   */
  private DeltaVarIntCodec.FieldEncoder createEncoder(final int fieldIndex, final long newValue) {
    return switch (fieldIndex) {
      case NodeFieldLayout.OBJECT_PARENT_KEY,
           NodeFieldLayout.OBJECT_RIGHT_SIB_KEY,
           NodeFieldLayout.OBJECT_LEFT_SIB_KEY,
           NodeFieldLayout.OBJECT_FIRST_CHILD_KEY,
           NodeFieldLayout.OBJECT_LAST_CHILD_KEY ->
          (target, offset) -> DeltaVarIntCodec.writeDeltaToSegment(target, offset, newValue, NODE_KEY);
      case NodeFieldLayout.OBJECT_PREV_REVISION,
           NodeFieldLayout.OBJECT_LAST_MOD_REVISION ->
          (target, offset) -> DeltaVarIntCodec.writeSignedToSegment(target, offset, (int) newValue);
      case NodeFieldLayout.OBJECT_HASH ->
          (target, offset) -> {
            DeltaVarIntCodec.writeLongToSegment(target, offset, newValue);
            return Long.BYTES;
          };
      case NodeFieldLayout.OBJECT_CHILD_COUNT,
           NodeFieldLayout.OBJECT_DESCENDANT_COUNT ->
          (target, offset) -> DeltaVarIntCodec.writeSignedLongToSegment(target, offset, newValue);
      default -> throw new IllegalArgumentException("Unknown field index: " + fieldIndex);
    };
  }
}
