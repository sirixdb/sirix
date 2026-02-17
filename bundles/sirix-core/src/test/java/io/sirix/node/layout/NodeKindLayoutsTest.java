package io.sirix.node.layout;

import io.sirix.node.NodeKind;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class NodeKindLayoutsTest {

  @Test
  void xmlAndJsonCoreKindsExposeFixedLayouts() {
    final NodeKind[] fixedKinds = {NodeKind.XML_DOCUMENT, NodeKind.ELEMENT, NodeKind.ATTRIBUTE, NodeKind.NAMESPACE,
        NodeKind.TEXT, NodeKind.COMMENT, NodeKind.PROCESSING_INSTRUCTION, NodeKind.JSON_DOCUMENT, NodeKind.OBJECT,
        NodeKind.ARRAY, NodeKind.OBJECT_KEY, NodeKind.STRING_VALUE, NodeKind.NUMBER_VALUE, NodeKind.BOOLEAN_VALUE,
        NodeKind.NULL_VALUE, NodeKind.OBJECT_STRING_VALUE, NodeKind.OBJECT_NUMBER_VALUE, NodeKind.OBJECT_BOOLEAN_VALUE,
        NodeKind.OBJECT_NULL_VALUE};

    for (final NodeKind kind : fixedKinds) {
      final NodeKindLayout layout = NodeKindLayouts.layoutFor(kind);
      assertTrue(layout.isFixedSlotSupported(), "Expected fixed-slot support for: " + kind);
      assertTrue(layout.fixedSlotSizeInBytes() > 0, "Expected non-zero slot size for: " + kind);
      assertSame(layout, kind.layoutDescriptor(), "NodeKind should delegate to registry for: " + kind);
      assertTrue(kind.hasFixedSlotLayout(), "NodeKind should expose fixed-slot support for: " + kind);
      assertEquals(layout.fixedSlotSizeInBytes(), kind.fixedSlotSizeInBytes(),
          "NodeKind should expose fixed-slot size for: " + kind);
    }
  }

  @Test
  void unsupportedKindsRemainMarkedUnsupported() {
    final NodeKindLayout pathLayout = NodeKindLayouts.layoutFor(NodeKind.PATH);
    assertFalse(pathLayout.isFixedSlotSupported());
    assertTrue(pathLayout.fixedSlotSizeInBytes() == 0);
    assertTrue(pathLayout.payloadRefCount() == 0);
    assertFalse(NodeKind.PATH.hasFixedSlotLayout());
    assertEquals(0, NodeKind.PATH.fixedSlotSizeInBytes());
  }

  @Test
  void fixedSlotContractsHaveNoOverlappingRanges() {
    for (final NodeKind kind : NodeKind.values()) {
      final NodeKindLayout layout = NodeKindLayouts.layoutFor(kind);
      if (!layout.isFixedSlotSupported()) {
        continue;
      }

      final int slotSize = layout.fixedSlotSizeInBytes();
      final boolean[] occupied = new boolean[slotSize];

      for (final StructuralField field : StructuralField.values()) {
        final int offset = layout.offsetOfOrMinusOne(field);
        if (offset < 0) {
          continue;
        }
        assertTrue(offset + field.widthInBytes() <= slotSize, "Field exceeds slot boundary: " + kind + "/" + field);
        markRangeFreeThenOccupy(occupied, offset, field.widthInBytes(), kind + "/" + field);
      }

      for (int i = 0; i < layout.payloadRefCount(); i++) {
        final PayloadRef payloadRef = layout.payloadRef(i);
        assertNotNull(payloadRef);
        markRangeFreeThenOccupy(occupied, payloadRef.pointerOffset(), PayloadRef.POINTER_WIDTH_BYTES,
            kind + "/" + payloadRef.name() + "#ptr");
        markRangeFreeThenOccupy(occupied, payloadRef.lengthOffset(), PayloadRef.LENGTH_WIDTH_BYTES,
            kind + "/" + payloadRef.name() + "#len");
        markRangeFreeThenOccupy(occupied, payloadRef.flagsOffset(), PayloadRef.FLAGS_WIDTH_BYTES,
            kind + "/" + payloadRef.name() + "#flags");
      }
    }
  }

  private static void markRangeFreeThenOccupy(final boolean[] occupied, final int offset, final int width,
      final String label) {
    assertTrue(offset >= 0, "Negative offset for: " + label);
    assertTrue(offset + width <= occupied.length, "Out-of-bounds range for: " + label);
    for (int i = offset; i < offset + width; i++) {
      assertFalse(occupied[i], "Overlapping byte in slot layout for: " + label);
      occupied[i] = true;
    }
  }
}
