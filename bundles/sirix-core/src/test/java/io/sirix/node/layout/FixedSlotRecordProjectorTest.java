package io.sirix.node.layout;

import io.brackit.query.atomic.QNm;
import io.sirix.node.NodeKind;
import io.sirix.node.MemorySegmentBytesIn;
import io.sirix.node.json.BooleanNode;
import io.sirix.node.json.NumberNode;
import io.sirix.node.json.ObjectKeyNode;
import io.sirix.node.json.ObjectNumberNode;
import io.sirix.node.json.ObjectStringNode;
import io.sirix.node.json.StringNode;
import io.sirix.node.xml.AttributeNode;
import io.sirix.node.xml.CommentNode;
import io.sirix.node.xml.ElementNode;
import io.sirix.node.xml.PINode;
import io.sirix.node.xml.TextNode;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import net.openhft.hashing.LongHashFunction;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FixedSlotRecordProjectorTest {

  @Test
  void projectBooleanNodeIntoFixedSlot() {
    final NodeKindLayout layout = NodeKindLayouts.layoutFor(NodeKind.BOOLEAN_VALUE);
    final BooleanNode node =
        new BooleanNode(42L, 7L, 11, 13, 17L, 19L, 23L, true, LongHashFunction.xx3(), (byte[]) null);

    try (final Arena arena = Arena.ofConfined()) {
      final MemorySegment slot = arena.allocate(layout.fixedSlotSizeInBytes());
      final boolean projected = FixedSlotRecordProjector.project(node, layout, slot, 0L);

      assertTrue(projected);
      assertEquals(7L, SlotLayoutAccessors.readLongField(slot, layout, StructuralField.PARENT_KEY));
      assertEquals(17L, SlotLayoutAccessors.readLongField(slot, layout, StructuralField.RIGHT_SIBLING_KEY));
      assertEquals(19L, SlotLayoutAccessors.readLongField(slot, layout, StructuralField.LEFT_SIBLING_KEY));
      assertEquals(11, SlotLayoutAccessors.readIntField(slot, layout, StructuralField.PREVIOUS_REVISION));
      assertEquals(13, SlotLayoutAccessors.readIntField(slot, layout, StructuralField.LAST_MODIFIED_REVISION));
      assertEquals(23L, SlotLayoutAccessors.readLongField(slot, layout, StructuralField.HASH));
      assertTrue(SlotLayoutAccessors.readBooleanField(slot, layout, StructuralField.BOOLEAN_VALUE));
    }
  }

  @Test
  void projectObjectKeyNodeIntoFixedSlot() {
    final NodeKindLayout layout = NodeKindLayouts.layoutFor(NodeKind.OBJECT_KEY);
    final ObjectKeyNode node =
        new ObjectKeyNode(128L, 3L, 5L, 8, 9, 11L, 13L, 17L, 19, 23L, 29L, LongHashFunction.xx3(), (byte[]) null);

    try (final Arena arena = Arena.ofConfined()) {
      final MemorySegment slot = arena.allocate(layout.fixedSlotSizeInBytes());
      final boolean projected = FixedSlotRecordProjector.project(node, layout, slot, 0L);

      assertTrue(projected);
      assertEquals(3L, SlotLayoutAccessors.readLongField(slot, layout, StructuralField.PARENT_KEY));
      assertEquals(11L, SlotLayoutAccessors.readLongField(slot, layout, StructuralField.RIGHT_SIBLING_KEY));
      assertEquals(13L, SlotLayoutAccessors.readLongField(slot, layout, StructuralField.LEFT_SIBLING_KEY));
      assertEquals(17L, SlotLayoutAccessors.readLongField(slot, layout, StructuralField.FIRST_CHILD_KEY));
      assertEquals(17L, SlotLayoutAccessors.readLongField(slot, layout, StructuralField.LAST_CHILD_KEY));
      assertEquals(5L, SlotLayoutAccessors.readLongField(slot, layout, StructuralField.PATH_NODE_KEY));
      assertEquals(19, SlotLayoutAccessors.readIntField(slot, layout, StructuralField.NAME_KEY));
      assertEquals(8, SlotLayoutAccessors.readIntField(slot, layout, StructuralField.PREVIOUS_REVISION));
      assertEquals(9, SlotLayoutAccessors.readIntField(slot, layout, StructuralField.LAST_MODIFIED_REVISION));
      assertEquals(29L, SlotLayoutAccessors.readLongField(slot, layout, StructuralField.HASH));
      assertEquals(23L, SlotLayoutAccessors.readLongField(slot, layout, StructuralField.DESCENDANT_COUNT));
    }
  }

  @Test
  void projectStringNodeRoundTrip() {
    final NodeKindLayout layout = NodeKindLayouts.layoutFor(NodeKind.STRING_VALUE);
    final byte[] value = "hello world".getBytes(StandardCharsets.UTF_8);
    final StringNode node =
        new StringNode(42L, 7L, 11, 13, 17L, 19L, 23L, value, LongHashFunction.xx3(), (byte[]) null);

    final int inlinePayload = FixedSlotRecordProjector.computeInlinePayloadLength(node, layout);
    assertEquals(value.length, inlinePayload);

    final int totalSize = layout.fixedSlotSizeInBytes() + inlinePayload;

    try (final Arena arena = Arena.ofConfined()) {
      final MemorySegment slot = arena.allocate(totalSize);
      final boolean projected = FixedSlotRecordProjector.project(node, layout, slot, 0L);

      assertTrue(projected);

      // Verify structural fields
      assertEquals(7L, SlotLayoutAccessors.readLongField(slot, layout, StructuralField.PARENT_KEY));
      assertEquals(17L, SlotLayoutAccessors.readLongField(slot, layout, StructuralField.RIGHT_SIBLING_KEY));
      assertEquals(19L, SlotLayoutAccessors.readLongField(slot, layout, StructuralField.LEFT_SIBLING_KEY));
      assertEquals(11, SlotLayoutAccessors.readIntField(slot, layout, StructuralField.PREVIOUS_REVISION));
      assertEquals(13, SlotLayoutAccessors.readIntField(slot, layout, StructuralField.LAST_MODIFIED_REVISION));
      assertEquals(23L, SlotLayoutAccessors.readLongField(slot, layout, StructuralField.HASH));

      // Verify PayloadRef metadata
      final long pointer = SlotLayoutAccessors.readPayloadPointer(slot, layout, 0);
      final int length = SlotLayoutAccessors.readPayloadLength(slot, layout, 0);
      final int flags = SlotLayoutAccessors.readPayloadFlags(slot, layout, 0);

      assertEquals(layout.fixedSlotSizeInBytes(), (int) pointer);
      assertEquals(value.length, length);
      assertEquals(0, flags); // not compressed

      // Verify inline payload bytes
      final byte[] readValue = new byte[length];
      MemorySegment.copy(slot, pointer, MemorySegment.ofArray(readValue), 0, length);
      assertArrayEquals(value, readValue);
    }
  }

  @Test
  void projectObjectStringNodeRoundTrip() {
    final NodeKindLayout layout = NodeKindLayouts.layoutFor(NodeKind.OBJECT_STRING_VALUE);
    final byte[] value = "test value".getBytes(StandardCharsets.UTF_8);
    final ObjectStringNode node =
        new ObjectStringNode(42L, 7L, 11, 13, 23L, value, LongHashFunction.xx3(), (byte[]) null);

    final int inlinePayload = FixedSlotRecordProjector.computeInlinePayloadLength(node, layout);
    assertEquals(value.length, inlinePayload);

    final int totalSize = layout.fixedSlotSizeInBytes() + inlinePayload;

    try (final Arena arena = Arena.ofConfined()) {
      final MemorySegment slot = arena.allocate(totalSize);
      final boolean projected = FixedSlotRecordProjector.project(node, layout, slot, 0L);

      assertTrue(projected);

      // Verify structural fields
      assertEquals(7L, SlotLayoutAccessors.readLongField(slot, layout, StructuralField.PARENT_KEY));
      assertEquals(11, SlotLayoutAccessors.readIntField(slot, layout, StructuralField.PREVIOUS_REVISION));
      assertEquals(13, SlotLayoutAccessors.readIntField(slot, layout, StructuralField.LAST_MODIFIED_REVISION));
      assertEquals(23L, SlotLayoutAccessors.readLongField(slot, layout, StructuralField.HASH));

      // Verify inline payload bytes
      final int length = SlotLayoutAccessors.readPayloadLength(slot, layout, 0);
      final long pointer = SlotLayoutAccessors.readPayloadPointer(slot, layout, 0);
      final byte[] readValue = new byte[length];
      MemorySegment.copy(slot, pointer, MemorySegment.ofArray(readValue), 0, length);
      assertArrayEquals(value, readValue);
    }
  }

  @Test
  void projectNumberNodeRoundTrip() {
    final NodeKindLayout layout = NodeKindLayouts.layoutFor(NodeKind.NUMBER_VALUE);
    final NumberNode node = new NumberNode(42L, 7L, 11, 13, 17L, 19L, 23L, 42.5, LongHashFunction.xx3(), (byte[]) null);

    final int inlinePayload = FixedSlotRecordProjector.computeInlinePayloadLength(node, layout);
    assertTrue(inlinePayload > 0);

    final int totalSize = layout.fixedSlotSizeInBytes() + inlinePayload;

    try (final Arena arena = Arena.ofConfined()) {
      final MemorySegment slot = arena.allocate(totalSize);
      final boolean projected = FixedSlotRecordProjector.project(node, layout, slot, 0L);

      assertTrue(projected);

      // Verify structural fields
      assertEquals(7L, SlotLayoutAccessors.readLongField(slot, layout, StructuralField.PARENT_KEY));
      assertEquals(17L, SlotLayoutAccessors.readLongField(slot, layout, StructuralField.RIGHT_SIBLING_KEY));
      assertEquals(19L, SlotLayoutAccessors.readLongField(slot, layout, StructuralField.LEFT_SIBLING_KEY));
      assertEquals(23L, SlotLayoutAccessors.readLongField(slot, layout, StructuralField.HASH));

      // Verify PayloadRef metadata
      final long pointer = SlotLayoutAccessors.readPayloadPointer(slot, layout, 0);
      final int length = SlotLayoutAccessors.readPayloadLength(slot, layout, 0);
      assertEquals(layout.fixedSlotSizeInBytes(), (int) pointer);
      assertTrue(length > 0);

      // Verify inline payload can be deserialized back to number
      final MemorySegment payloadSlice = slot.asSlice(pointer, length);
      final Number deserialized = NodeKind.deserializeNumber(new MemorySegmentBytesIn(payloadSlice));
      assertEquals(42.5, deserialized.doubleValue(), 0.0001);
    }
  }

  @Test
  void projectObjectNumberNodeRoundTrip() {
    final NodeKindLayout layout = NodeKindLayouts.layoutFor(NodeKind.OBJECT_NUMBER_VALUE);
    final ObjectNumberNode node =
        new ObjectNumberNode(42L, 7L, 11, 13, 23L, 123, LongHashFunction.xx3(), (byte[]) null);

    final int inlinePayload = FixedSlotRecordProjector.computeInlinePayloadLength(node, layout);
    assertTrue(inlinePayload > 0);

    final int totalSize = layout.fixedSlotSizeInBytes() + inlinePayload;

    try (final Arena arena = Arena.ofConfined()) {
      final MemorySegment slot = arena.allocate(totalSize);
      final boolean projected = FixedSlotRecordProjector.project(node, layout, slot, 0L);

      assertTrue(projected);

      // Verify inline payload can be deserialized back to number
      final long pointer = SlotLayoutAccessors.readPayloadPointer(slot, layout, 0);
      final int length = SlotLayoutAccessors.readPayloadLength(slot, layout, 0);
      final MemorySegment payloadSlice = slot.asSlice(pointer, length);
      final Number deserialized = NodeKind.deserializeNumber(new MemorySegmentBytesIn(payloadSlice));
      assertEquals(123, deserialized.intValue());
    }
  }

  @Test
  void projectNumberNodeBigDecimalRoundTrip() {
    final NodeKindLayout layout = NodeKindLayouts.layoutFor(NodeKind.NUMBER_VALUE);
    final BigDecimal bigDecimalValue = new BigDecimal("12345678901234567890.12345");
    final NumberNode node =
        new NumberNode(42L, 7L, 11, 13, 17L, 19L, 23L, bigDecimalValue, LongHashFunction.xx3(), (byte[]) null);

    final int inlinePayload = FixedSlotRecordProjector.computeInlinePayloadLength(node, layout);
    assertTrue(inlinePayload > 0);

    final int totalSize = layout.fixedSlotSizeInBytes() + inlinePayload;

    try (final Arena arena = Arena.ofConfined()) {
      final MemorySegment slot = arena.allocate(totalSize);
      assertTrue(FixedSlotRecordProjector.project(node, layout, slot, 0L));

      final long pointer = SlotLayoutAccessors.readPayloadPointer(slot, layout, 0);
      final int length = SlotLayoutAccessors.readPayloadLength(slot, layout, 0);
      final MemorySegment payloadSlice = slot.asSlice(pointer, length);
      final Number deserialized = NodeKind.deserializeNumber(new MemorySegmentBytesIn(payloadSlice));
      assertEquals(bigDecimalValue, deserialized);
    }
  }

  @Test
  void projectCompressedStringPreservesFlags() {
    final NodeKindLayout layout = NodeKindLayouts.layoutFor(NodeKind.STRING_VALUE);
    final byte[] rawCompressed = {0x01, 0x02, 0x03, 0x04};
    final StringNode node = new StringNode(42L, 7L, 11, 13, 17L, 19L, 23L, rawCompressed, LongHashFunction.xx3(),
        (byte[]) null, true, null);

    final int inlinePayload = FixedSlotRecordProjector.computeInlinePayloadLength(node, layout);
    final int totalSize = layout.fixedSlotSizeInBytes() + inlinePayload;

    try (final Arena arena = Arena.ofConfined()) {
      final MemorySegment slot = arena.allocate(totalSize);
      assertTrue(FixedSlotRecordProjector.project(node, layout, slot, 0L));

      final int flags = SlotLayoutAccessors.readPayloadFlags(slot, layout, 0);
      assertEquals(1, flags); // compressed flag set
    }
  }

  @Test
  void hasSupportedPayloadsReturnsTrueForValueBlob() {
    final NodeKindLayout stringLayout = NodeKindLayouts.layoutFor(NodeKind.STRING_VALUE);
    assertTrue(FixedSlotRecordProjector.hasSupportedPayloads(stringLayout));

    final NodeKindLayout booleanLayout = NodeKindLayouts.layoutFor(NodeKind.BOOLEAN_VALUE);
    assertTrue(FixedSlotRecordProjector.hasSupportedPayloads(booleanLayout));
  }

  @Test
  void computeInlinePayloadLengthReturnsZeroForNonPayloadNodes() {
    final NodeKindLayout layout = NodeKindLayouts.layoutFor(NodeKind.BOOLEAN_VALUE);
    final BooleanNode node = new BooleanNode(1L, 2L, 3, 4, 5L, 6L, 7L, false, LongHashFunction.xx3(), (byte[]) null);
    assertEquals(0, FixedSlotRecordProjector.computeInlinePayloadLength(node, layout));
  }

  @Test
  void projectTextNodeRoundTrip() {
    final NodeKindLayout layout = NodeKindLayouts.layoutFor(NodeKind.TEXT);
    final byte[] value = "hello xml text".getBytes(StandardCharsets.UTF_8);
    final TextNode node =
        new TextNode(42L, 7L, 11, 13, 17L, 19L, 23L, value, false, LongHashFunction.xx3(), (byte[]) null);

    final int inlinePayload = FixedSlotRecordProjector.computeInlinePayloadLength(node, layout);
    assertEquals(value.length, inlinePayload);

    final int totalSize = layout.fixedSlotSizeInBytes() + inlinePayload;

    try (final Arena arena = Arena.ofConfined()) {
      final MemorySegment slot = arena.allocate(totalSize);
      final boolean projected = FixedSlotRecordProjector.project(node, layout, slot, 0L);

      assertTrue(projected);

      // Verify structural fields
      assertEquals(7L, SlotLayoutAccessors.readLongField(slot, layout, StructuralField.PARENT_KEY));
      assertEquals(17L, SlotLayoutAccessors.readLongField(slot, layout, StructuralField.RIGHT_SIBLING_KEY));
      assertEquals(19L, SlotLayoutAccessors.readLongField(slot, layout, StructuralField.LEFT_SIBLING_KEY));
      assertEquals(11, SlotLayoutAccessors.readIntField(slot, layout, StructuralField.PREVIOUS_REVISION));
      assertEquals(13, SlotLayoutAccessors.readIntField(slot, layout, StructuralField.LAST_MODIFIED_REVISION));

      // Verify PayloadRef metadata
      final long pointer = SlotLayoutAccessors.readPayloadPointer(slot, layout, 0);
      final int length = SlotLayoutAccessors.readPayloadLength(slot, layout, 0);
      final int flags = SlotLayoutAccessors.readPayloadFlags(slot, layout, 0);

      assertEquals(layout.fixedSlotSizeInBytes(), (int) pointer);
      assertEquals(value.length, length);
      assertEquals(0, flags); // not compressed

      // Verify inline payload bytes
      final byte[] readValue = new byte[length];
      MemorySegment.copy(slot, pointer, MemorySegment.ofArray(readValue), 0, length);
      assertArrayEquals(value, readValue);
    }
  }

  @Test
  void projectTextNodeCompressedRoundTrip() {
    final NodeKindLayout layout = NodeKindLayouts.layoutFor(NodeKind.TEXT);
    final byte[] rawCompressed = {0x01, 0x02, 0x03, 0x04};
    final TextNode node =
        new TextNode(42L, 7L, 11, 13, 17L, 19L, 23L, rawCompressed, true, LongHashFunction.xx3(), (byte[]) null);

    final int inlinePayload = FixedSlotRecordProjector.computeInlinePayloadLength(node, layout);
    final int totalSize = layout.fixedSlotSizeInBytes() + inlinePayload;

    try (final Arena arena = Arena.ofConfined()) {
      final MemorySegment slot = arena.allocate(totalSize);
      assertTrue(FixedSlotRecordProjector.project(node, layout, slot, 0L));

      final int flags = SlotLayoutAccessors.readPayloadFlags(slot, layout, 0);
      assertEquals(1, flags); // compressed flag set
    }
  }

  @Test
  void projectCommentNodeRoundTrip() {
    final NodeKindLayout layout = NodeKindLayouts.layoutFor(NodeKind.COMMENT);
    final byte[] value = "a comment".getBytes(StandardCharsets.UTF_8);
    final CommentNode node =
        new CommentNode(42L, 7L, 11, 13, 17L, 19L, 23L, value, false, LongHashFunction.xx3(), (byte[]) null);

    final int inlinePayload = FixedSlotRecordProjector.computeInlinePayloadLength(node, layout);
    assertEquals(value.length, inlinePayload);

    final int totalSize = layout.fixedSlotSizeInBytes() + inlinePayload;

    try (final Arena arena = Arena.ofConfined()) {
      final MemorySegment slot = arena.allocate(totalSize);
      assertTrue(FixedSlotRecordProjector.project(node, layout, slot, 0L));

      assertEquals(7L, SlotLayoutAccessors.readLongField(slot, layout, StructuralField.PARENT_KEY));
      assertEquals(17L, SlotLayoutAccessors.readLongField(slot, layout, StructuralField.RIGHT_SIBLING_KEY));
      assertEquals(19L, SlotLayoutAccessors.readLongField(slot, layout, StructuralField.LEFT_SIBLING_KEY));
      assertEquals(11, SlotLayoutAccessors.readIntField(slot, layout, StructuralField.PREVIOUS_REVISION));
      assertEquals(13, SlotLayoutAccessors.readIntField(slot, layout, StructuralField.LAST_MODIFIED_REVISION));

      final int length = SlotLayoutAccessors.readPayloadLength(slot, layout, 0);
      final long pointer = SlotLayoutAccessors.readPayloadPointer(slot, layout, 0);
      final byte[] readValue = new byte[length];
      MemorySegment.copy(slot, pointer, MemorySegment.ofArray(readValue), 0, length);
      assertArrayEquals(value, readValue);
    }
  }

  @Test
  void projectAttributeNodeRoundTrip() {
    final NodeKindLayout layout = NodeKindLayouts.layoutFor(NodeKind.ATTRIBUTE);
    final byte[] value = "attr-value".getBytes(StandardCharsets.UTF_8);
    final QNm qNm = new QNm("ns", "prefix", "local");
    final AttributeNode node =
        new AttributeNode(42L, 7L, 11, 13, 5L, 100, 200, 300, 23L, value, LongHashFunction.xx3(), (byte[]) null, qNm);

    final int inlinePayload = FixedSlotRecordProjector.computeInlinePayloadLength(node, layout);
    assertEquals(value.length, inlinePayload);

    final int totalSize = layout.fixedSlotSizeInBytes() + inlinePayload;

    try (final Arena arena = Arena.ofConfined()) {
      final MemorySegment slot = arena.allocate(totalSize);
      assertTrue(FixedSlotRecordProjector.project(node, layout, slot, 0L));

      assertEquals(7L, SlotLayoutAccessors.readLongField(slot, layout, StructuralField.PARENT_KEY));
      assertEquals(5L, SlotLayoutAccessors.readLongField(slot, layout, StructuralField.PATH_NODE_KEY));
      assertEquals(100, SlotLayoutAccessors.readIntField(slot, layout, StructuralField.PREFIX_KEY));
      assertEquals(200, SlotLayoutAccessors.readIntField(slot, layout, StructuralField.LOCAL_NAME_KEY));
      assertEquals(300, SlotLayoutAccessors.readIntField(slot, layout, StructuralField.URI_KEY));
      assertEquals(11, SlotLayoutAccessors.readIntField(slot, layout, StructuralField.PREVIOUS_REVISION));
      assertEquals(13, SlotLayoutAccessors.readIntField(slot, layout, StructuralField.LAST_MODIFIED_REVISION));

      final int flags = SlotLayoutAccessors.readPayloadFlags(slot, layout, 0);
      assertEquals(0, flags); // attributes never compressed

      final int length = SlotLayoutAccessors.readPayloadLength(slot, layout, 0);
      final long pointer = SlotLayoutAccessors.readPayloadPointer(slot, layout, 0);
      final byte[] readValue = new byte[length];
      MemorySegment.copy(slot, pointer, MemorySegment.ofArray(readValue), 0, length);
      assertArrayEquals(value, readValue);
    }
  }

  @Test
  void projectPINodeRoundTrip() {
    final NodeKindLayout layout = NodeKindLayouts.layoutFor(NodeKind.PROCESSING_INSTRUCTION);
    final byte[] value = "pi-target data".getBytes(StandardCharsets.UTF_8);
    final QNm qNm = new QNm("", "", "xml-stylesheet");
    final PINode node = new PINode(42L, 7L, 11, 13, 17L, 19L, 31L, 37L, 3, 5, 23L, 41L, 100, 200, 300, value, false,
        LongHashFunction.xx3(), (byte[]) null, qNm);

    final int inlinePayload = FixedSlotRecordProjector.computeInlinePayloadLength(node, layout);
    assertEquals(value.length, inlinePayload);

    final int totalSize = layout.fixedSlotSizeInBytes() + inlinePayload;

    try (final Arena arena = Arena.ofConfined()) {
      final MemorySegment slot = arena.allocate(totalSize);
      assertTrue(FixedSlotRecordProjector.project(node, layout, slot, 0L));

      // Verify structural fields
      assertEquals(7L, SlotLayoutAccessors.readLongField(slot, layout, StructuralField.PARENT_KEY));
      assertEquals(17L, SlotLayoutAccessors.readLongField(slot, layout, StructuralField.RIGHT_SIBLING_KEY));
      assertEquals(19L, SlotLayoutAccessors.readLongField(slot, layout, StructuralField.LEFT_SIBLING_KEY));
      assertEquals(31L, SlotLayoutAccessors.readLongField(slot, layout, StructuralField.FIRST_CHILD_KEY));
      assertEquals(37L, SlotLayoutAccessors.readLongField(slot, layout, StructuralField.LAST_CHILD_KEY));
      assertEquals(3L, SlotLayoutAccessors.readLongField(slot, layout, StructuralField.CHILD_COUNT));
      assertEquals(5L, SlotLayoutAccessors.readLongField(slot, layout, StructuralField.DESCENDANT_COUNT));

      // Verify name fields
      assertEquals(41L, SlotLayoutAccessors.readLongField(slot, layout, StructuralField.PATH_NODE_KEY));
      assertEquals(100, SlotLayoutAccessors.readIntField(slot, layout, StructuralField.PREFIX_KEY));
      assertEquals(200, SlotLayoutAccessors.readIntField(slot, layout, StructuralField.LOCAL_NAME_KEY));
      assertEquals(300, SlotLayoutAccessors.readIntField(slot, layout, StructuralField.URI_KEY));

      assertEquals(11, SlotLayoutAccessors.readIntField(slot, layout, StructuralField.PREVIOUS_REVISION));
      assertEquals(13, SlotLayoutAccessors.readIntField(slot, layout, StructuralField.LAST_MODIFIED_REVISION));

      // Verify inline payload bytes
      final int length = SlotLayoutAccessors.readPayloadLength(slot, layout, 0);
      final long pointer = SlotLayoutAccessors.readPayloadPointer(slot, layout, 0);
      final byte[] readValue = new byte[length];
      MemorySegment.copy(slot, pointer, MemorySegment.ofArray(readValue), 0, length);
      assertArrayEquals(value, readValue);
    }
  }

  @Test
  void hasSupportedPayloadsAcceptsVectorPayloadRefs() {
    final NodeKindLayout layoutWithAttrs = NodeKindLayout.builder(NodeKind.ELEMENT)
                                                         .addField(StructuralField.PARENT_KEY)
                                                         .addField(StructuralField.HASH)
                                                         .addPayloadRef("attrs", PayloadRefKind.ATTRIBUTE_VECTOR)
                                                         .build();

    assertTrue(FixedSlotRecordProjector.hasSupportedPayloads(layoutWithAttrs));

    // project() should reject a type mismatch (BooleanNode for ELEMENT layout)
    final BooleanNode node = new BooleanNode(1L, 2L, 3, 4, 5L, 6L, 7L, false, LongHashFunction.xx3(), (byte[]) null);

    try (final Arena arena = Arena.ofConfined()) {
      final MemorySegment slot = arena.allocate(Math.max(layoutWithAttrs.fixedSlotSizeInBytes(), 128));
      assertFalse(FixedSlotRecordProjector.project(node, layoutWithAttrs, slot, 0L));
    }
  }

  @Test
  void elementNodeRoundTripWithAttributesAndNamespaces() {
    final NodeKindLayout layout = NodeKindLayouts.layoutFor(NodeKind.ELEMENT);
    final LongHashFunction hashFunction = LongHashFunction.xx3();
    final QNm qNm = new QNm("ns", "pfx", "local");

    final LongArrayList attrKeys = new LongArrayList(new long[] {100L, 200L, 300L});
    final LongArrayList nsKeys = new LongArrayList(new long[] {400L, 500L});

    final ElementNode node = new ElementNode(42L, 10L, 3, 7, 20L, 30L, 40L, 50L, 5L, 100L, 999L, 60L, 11, 22, 33,
        hashFunction, (byte[]) null, attrKeys, nsKeys, qNm);

    try (final Arena arena = Arena.ofConfined()) {
      final int totalSize =
          layout.fixedSlotSizeInBytes() + node.getAttributeCount() * Long.BYTES + node.getNamespaceCount() * Long.BYTES;
      final MemorySegment slot = arena.allocate(totalSize);
      assertTrue(FixedSlotRecordProjector.project(node, layout, slot, 0L));

      // Verify structural fields
      assertEquals(10L, SlotLayoutAccessors.readLongField(slot, layout, StructuralField.PARENT_KEY));
      assertEquals(20L, SlotLayoutAccessors.readLongField(slot, layout, StructuralField.RIGHT_SIBLING_KEY));
      assertEquals(30L, SlotLayoutAccessors.readLongField(slot, layout, StructuralField.LEFT_SIBLING_KEY));
      assertEquals(40L, SlotLayoutAccessors.readLongField(slot, layout, StructuralField.FIRST_CHILD_KEY));
      assertEquals(50L, SlotLayoutAccessors.readLongField(slot, layout, StructuralField.LAST_CHILD_KEY));
      assertEquals(60L, SlotLayoutAccessors.readLongField(slot, layout, StructuralField.PATH_NODE_KEY));
      assertEquals(11, SlotLayoutAccessors.readIntField(slot, layout, StructuralField.PREFIX_KEY));
      assertEquals(22, SlotLayoutAccessors.readIntField(slot, layout, StructuralField.LOCAL_NAME_KEY));
      assertEquals(33, SlotLayoutAccessors.readIntField(slot, layout, StructuralField.URI_KEY));
      assertEquals(3, SlotLayoutAccessors.readIntField(slot, layout, StructuralField.PREVIOUS_REVISION));
      assertEquals(7, SlotLayoutAccessors.readIntField(slot, layout, StructuralField.LAST_MODIFIED_REVISION));
      assertEquals(999L, SlotLayoutAccessors.readLongField(slot, layout, StructuralField.HASH));
      assertEquals(5L, SlotLayoutAccessors.readLongField(slot, layout, StructuralField.CHILD_COUNT));
      assertEquals(100L, SlotLayoutAccessors.readLongField(slot, layout, StructuralField.DESCENDANT_COUNT));

      // Verify attribute vector payload
      assertEquals(3 * Long.BYTES, SlotLayoutAccessors.readPayloadLength(slot, layout, 0));
      // Verify namespace vector payload
      assertEquals(2 * Long.BYTES, SlotLayoutAccessors.readPayloadLength(slot, layout, 1));

      // Populate existing singleton from projected slot
      final ElementNode populated = new ElementNode(0L, 0L, 0, 0, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0, 0, 0, hashFunction,
          (byte[]) null, new LongArrayList(), new LongArrayList(), new QNm(""));
      assertTrue(FixedSlotRecordMaterializer.populateExisting(populated, NodeKind.ELEMENT, 42L, slot, 0L,
          (int) slot.byteSize(), null, null));
      assertEquals(42L, populated.getNodeKey());
      assertEquals(10L, populated.getParentKey());
      assertEquals(20L, populated.getRightSiblingKey());
      assertEquals(30L, populated.getLeftSiblingKey());
      assertEquals(40L, populated.getFirstChildKey());
      assertEquals(50L, populated.getLastChildKey());
      assertEquals(60L, populated.getPathNodeKey());
      assertEquals(11, populated.getPrefixKey());
      assertEquals(22, populated.getLocalNameKey());
      assertEquals(33, populated.getURIKey());
      assertEquals(3, populated.getPreviousRevisionNumber());
      assertEquals(7, populated.getLastModifiedRevisionNumber());
      assertEquals(999L, populated.getHash());
      assertEquals(5L, populated.getChildCount());
      assertEquals(100L, populated.getDescendantCount());
      assertEquals(3, populated.getAttributeCount());
      assertEquals(100L, populated.getAttributeKey(0));
      assertEquals(200L, populated.getAttributeKey(1));
      assertEquals(300L, populated.getAttributeKey(2));
      assertEquals(2, populated.getNamespaceCount());
      assertEquals(400L, populated.getNamespaceKey(0));
      assertEquals(500L, populated.getNamespaceKey(1));
    }
  }

  @Test
  void elementNodeRoundTripWithEmptyVectors() {
    final NodeKindLayout layout = NodeKindLayouts.layoutFor(NodeKind.ELEMENT);
    final LongHashFunction hashFunction = LongHashFunction.xx3();

    final ElementNode node = new ElementNode(1L, 2L, 1, 1, 3L, 4L, 5L, 6L, 0L, 0L, 0L, 7L, 0, 0, 0, hashFunction,
        (byte[]) null, new LongArrayList(), new LongArrayList(), new QNm(""));

    try (final Arena arena = Arena.ofConfined()) {
      final MemorySegment slot = arena.allocate(layout.fixedSlotSizeInBytes());
      assertTrue(FixedSlotRecordProjector.project(node, layout, slot, 0L));

      assertEquals(0, SlotLayoutAccessors.readPayloadLength(slot, layout, 0));
      assertEquals(0, SlotLayoutAccessors.readPayloadLength(slot, layout, 1));

      final ElementNode populated = new ElementNode(0L, 0L, 0, 0, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0, 0, 0, hashFunction,
          (byte[]) null, new LongArrayList(), new LongArrayList(), new QNm(""));
      assertTrue(FixedSlotRecordMaterializer.populateExisting(populated, NodeKind.ELEMENT, 1L, slot, 0L,
          (int) slot.byteSize(), null, null));
      assertEquals(0, populated.getAttributeCount());
      assertEquals(0, populated.getNamespaceCount());
      assertEquals(2L, populated.getParentKey());
    }
  }

  @Test
  void elementNodePopulateExistingRoundTrip() {
    final NodeKindLayout layout = NodeKindLayouts.layoutFor(NodeKind.ELEMENT);
    final LongHashFunction hashFunction = LongHashFunction.xx3();

    final LongArrayList attrKeys = new LongArrayList(new long[] {10L, 20L});
    final LongArrayList nsKeys = new LongArrayList(new long[] {30L});

    final ElementNode original = new ElementNode(5L, 100L, 2, 4, 101L, 102L, 103L, 104L, 3L, 50L, 777L, 105L, 8, 9, 10,
        hashFunction, (byte[]) null, attrKeys, nsKeys, new QNm(""));

    try (final Arena arena = Arena.ofConfined()) {
      final int totalSize = layout.fixedSlotSizeInBytes() + original.getAttributeCount() * Long.BYTES
          + original.getNamespaceCount() * Long.BYTES;
      final MemorySegment slot = arena.allocate(totalSize);
      assertTrue(FixedSlotRecordProjector.project(original, layout, slot, 0L));

      // Populate into an existing singleton
      final ElementNode singleton = new ElementNode(0L, 0L, 0, 0, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0, 0, 0, hashFunction,
          (byte[]) null, new LongArrayList(), new LongArrayList(), new QNm(""));

      assertTrue(FixedSlotRecordMaterializer.populateExisting(singleton, NodeKind.ELEMENT, 5L, slot, 0L,
          (int) slot.byteSize(), null, null));

      assertEquals(5L, singleton.getNodeKey());
      assertEquals(100L, singleton.getParentKey());
      assertEquals(101L, singleton.getRightSiblingKey());
      assertEquals(102L, singleton.getLeftSiblingKey());
      assertEquals(103L, singleton.getFirstChildKey());
      assertEquals(104L, singleton.getLastChildKey());
      assertEquals(777L, singleton.getHash());
      assertEquals(2, singleton.getAttributeCount());
      assertEquals(10L, singleton.getAttributeKey(0));
      assertEquals(20L, singleton.getAttributeKey(1));
      assertEquals(1, singleton.getNamespaceCount());
      assertEquals(30L, singleton.getNamespaceKey(0));
    }
  }
}
