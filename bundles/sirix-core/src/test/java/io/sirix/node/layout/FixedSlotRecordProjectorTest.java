package io.sirix.node.layout;

import io.sirix.node.NodeKind;
import io.sirix.node.json.BooleanNode;
import io.sirix.node.json.ObjectKeyNode;
import net.openhft.hashing.LongHashFunction;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FixedSlotRecordProjectorTest {

  @Test
  void projectBooleanNodeIntoFixedSlot() {
    final NodeKindLayout layout = NodeKindLayouts.layoutFor(NodeKind.BOOLEAN_VALUE);
    final BooleanNode node = new BooleanNode(42L, 7L, 11, 13, 17L, 19L, 23L, true, LongHashFunction.xx3(), (byte[]) null);

    try (final Arena arena = Arena.ofConfined()) {
      final MemorySegment slot = arena.allocate(layout.fixedSlotSizeInBytes());
      final boolean projected = FixedSlotRecordProjector.project(node, layout, slot);

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
    final ObjectKeyNode node = new ObjectKeyNode(128L,
                                                 3L,
                                                 5L,
                                                 8,
                                                 9,
                                                 11L,
                                                 13L,
                                                 17L,
                                                 19,
                                                 23L,
                                                 29L,
                                                 LongHashFunction.xx3(),
                                                 (byte[]) null);

    try (final Arena arena = Arena.ofConfined()) {
      final MemorySegment slot = arena.allocate(layout.fixedSlotSizeInBytes());
      final boolean projected = FixedSlotRecordProjector.project(node, layout, slot);

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
  void rejectPayloadLayoutsInThisPhase() {
    final NodeKindLayout layout = NodeKindLayouts.layoutFor(NodeKind.STRING_VALUE);
    final BooleanNode node = new BooleanNode(1L, 2L, 3, 4, 5L, 6L, 7L, false, LongHashFunction.xx3(), (byte[]) null);

    try (final Arena arena = Arena.ofConfined()) {
      final MemorySegment slot = arena.allocate(Math.max(layout.fixedSlotSizeInBytes(), 64));
      assertFalse(FixedSlotRecordProjector.project(node, layout, slot));
    }
  }
}
