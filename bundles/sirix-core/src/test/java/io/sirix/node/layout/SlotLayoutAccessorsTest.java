package io.sirix.node.layout;

import io.sirix.node.NodeKind;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class SlotLayoutAccessorsTest {

  @Test
  void longAndIntFieldRoundTrip() {
    final NodeKindLayout layout = NodeKindLayouts.layoutFor(NodeKind.OBJECT);
    try (Arena arena = Arena.ofConfined()) {
      final MemorySegment slot = arena.allocate(layout.fixedSlotSizeInBytes());

      SlotLayoutAccessors.writeLongField(slot, layout, StructuralField.PARENT_KEY, 100L);
      SlotLayoutAccessors.writeLongField(slot, layout, StructuralField.FIRST_CHILD_KEY, 101L);
      SlotLayoutAccessors.writeIntField(slot, layout, StructuralField.PREVIOUS_REVISION, 7);

      assertEquals(100L, SlotLayoutAccessors.readLongField(slot, layout, StructuralField.PARENT_KEY));
      assertEquals(101L, SlotLayoutAccessors.readLongField(slot, layout, StructuralField.FIRST_CHILD_KEY));
      assertEquals(7, SlotLayoutAccessors.readIntField(slot, layout, StructuralField.PREVIOUS_REVISION));
    }
  }

  @Test
  void booleanFieldRoundTrip() {
    final NodeKindLayout layout = NodeKindLayouts.layoutFor(NodeKind.BOOLEAN_VALUE);
    try (Arena arena = Arena.ofConfined()) {
      final MemorySegment slot = arena.allocate(layout.fixedSlotSizeInBytes());

      SlotLayoutAccessors.writeBooleanField(slot, layout, StructuralField.BOOLEAN_VALUE, true);
      assertTrue(SlotLayoutAccessors.readBooleanField(slot, layout, StructuralField.BOOLEAN_VALUE));

      SlotLayoutAccessors.writeBooleanField(slot, layout, StructuralField.BOOLEAN_VALUE, false);
      assertFalse(SlotLayoutAccessors.readBooleanField(slot, layout, StructuralField.BOOLEAN_VALUE));
    }
  }

  @Test
  void payloadRefRoundTrip() {
    final NodeKindLayout layout = NodeKindLayouts.layoutFor(NodeKind.STRING_VALUE);
    try (Arena arena = Arena.ofConfined()) {
      final MemorySegment slot = arena.allocate(layout.fixedSlotSizeInBytes());
      SlotLayoutAccessors.writePayloadRef(slot, layout, 0, 4096L, 128, 3);

      assertEquals(4096L, SlotLayoutAccessors.readPayloadPointer(slot, layout, 0));
      assertEquals(128, SlotLayoutAccessors.readPayloadLength(slot, layout, 0));
      assertEquals(3, SlotLayoutAccessors.readPayloadFlags(slot, layout, 0));
    }
  }

  @Test
  void missingFieldAccessFailsFast() {
    final NodeKindLayout layout = NodeKindLayouts.layoutFor(NodeKind.NULL_VALUE);
    try (Arena arena = Arena.ofConfined()) {
      final MemorySegment slot = arena.allocate(layout.fixedSlotSizeInBytes());
      assertThrows(IllegalArgumentException.class,
          () -> SlotLayoutAccessors.readLongField(slot, layout, StructuralField.FIRST_CHILD_KEY));
    }
  }

  @Test
  void invalidPayloadIndexFailsFast() {
    final NodeKindLayout layout = NodeKindLayouts.layoutFor(NodeKind.STRING_VALUE);
    try (Arena arena = Arena.ofConfined()) {
      final MemorySegment slot = arena.allocate(layout.fixedSlotSizeInBytes());
      assertThrows(IllegalArgumentException.class, () -> SlotLayoutAccessors.readPayloadPointer(slot, layout, 1));
    }
  }

  @Test
  void payloadRefRejectsNegativeLengthOrFlags() {
    final NodeKindLayout layout = NodeKindLayouts.layoutFor(NodeKind.STRING_VALUE);
    try (Arena arena = Arena.ofConfined()) {
      final MemorySegment slot = arena.allocate(layout.fixedSlotSizeInBytes());
      assertThrows(IllegalArgumentException.class,
          () -> SlotLayoutAccessors.writePayloadRef(slot, layout, 0, 1L, -1, 0));
      assertThrows(IllegalArgumentException.class,
          () -> SlotLayoutAccessors.writePayloadRef(slot, layout, 0, 1L, 1, -1));
    }
  }
}
