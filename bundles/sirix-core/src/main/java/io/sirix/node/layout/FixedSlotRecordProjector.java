package io.sirix.node.layout;

import io.sirix.node.interfaces.BooleanValueNode;
import io.sirix.node.interfaces.DataRecord;
import io.sirix.node.interfaces.NameNode;
import io.sirix.node.interfaces.StructNode;
import io.sirix.node.interfaces.immutable.ImmutableNode;
import io.sirix.node.json.ObjectKeyNode;

import java.lang.foreign.MemorySegment;
import java.util.Objects;

/**
 * Projects record structural metadata into a fixed-slot memory layout.
 *
 * <p>This projector is intentionally limited to payload-free layouts. It mirrors fields used on
 * hot structural paths while leaving payload-bearing node kinds on the existing serializer path.
 */
public final class FixedSlotRecordProjector {
  private FixedSlotRecordProjector() {
  }

  /**
   * Project a record into the given fixed-slot target.
   *
   * @param record record to project
   * @param layout fixed-slot layout descriptor
   * @param targetSlot target slot memory
   * @return {@code true} if projection succeeded, {@code false} if the record does not provide all required fields
   */
  public static boolean project(final DataRecord record, final NodeKindLayout layout, final MemorySegment targetSlot) {
    Objects.requireNonNull(record, "record must not be null");
    Objects.requireNonNull(layout, "layout must not be null");
    Objects.requireNonNull(targetSlot, "targetSlot must not be null");

    if (!layout.isFixedSlotSupported() || layout.payloadRefCount() != 0) {
      return false;
    }

    final int requiredSize = layout.fixedSlotSizeInBytes();
    if (targetSlot.byteSize() < requiredSize) {
      throw new IllegalArgumentException(
          "target slot too small: " + targetSlot.byteSize() + " < " + requiredSize);
    }

    // Clear stale values from previous projection in this slot.
    targetSlot.asSlice(0, requiredSize).fill((byte) 0);

    final ImmutableNode immutableNode = record instanceof ImmutableNode node ? node : null;
    final StructNode structNode = record instanceof StructNode node ? node : null;
    final NameNode nameNode = record instanceof NameNode node ? node : null;
    final BooleanValueNode booleanValueNode = record instanceof BooleanValueNode node ? node : null;
    final ObjectKeyNode objectKeyNode = record instanceof ObjectKeyNode node ? node : null;

    if (!writeCommonFields(record, immutableNode, targetSlot, layout)) {
      return false;
    }
    if (!writeStructuralFields(structNode, targetSlot, layout)) {
      return false;
    }
    if (!writeNameFields(nameNode, objectKeyNode, targetSlot, layout)) {
      return false;
    }
    if (!writeBooleanField(booleanValueNode, targetSlot, layout)) {
      return false;
    }

    return true;
  }

  private static boolean writeCommonFields(final DataRecord record, final ImmutableNode immutableNode,
      final MemorySegment targetSlot, final NodeKindLayout layout) {
    if (layout.hasField(StructuralField.PREVIOUS_REVISION)) {
      SlotLayoutAccessors.writeIntField(targetSlot,
                                        layout,
                                        StructuralField.PREVIOUS_REVISION,
                                        record.getPreviousRevisionNumber());
    }
    if (layout.hasField(StructuralField.LAST_MODIFIED_REVISION)) {
      SlotLayoutAccessors.writeIntField(targetSlot,
                                        layout,
                                        StructuralField.LAST_MODIFIED_REVISION,
                                        record.getLastModifiedRevisionNumber());
    }
    if (layout.hasField(StructuralField.PARENT_KEY)) {
      if (immutableNode == null) {
        return false;
      }
      SlotLayoutAccessors.writeLongField(targetSlot, layout, StructuralField.PARENT_KEY, immutableNode.getParentKey());
    }
    if (layout.hasField(StructuralField.HASH)) {
      if (immutableNode == null) {
        return false;
      }
      SlotLayoutAccessors.writeLongField(targetSlot, layout, StructuralField.HASH, immutableNode.getHash());
    }
    return true;
  }

  private static boolean writeStructuralFields(final StructNode structNode, final MemorySegment targetSlot,
      final NodeKindLayout layout) {
    if (layout.hasField(StructuralField.RIGHT_SIBLING_KEY)) {
      if (structNode == null) {
        return false;
      }
      SlotLayoutAccessors.writeLongField(targetSlot,
                                         layout,
                                         StructuralField.RIGHT_SIBLING_KEY,
                                         structNode.getRightSiblingKey());
    }
    if (layout.hasField(StructuralField.LEFT_SIBLING_KEY)) {
      if (structNode == null) {
        return false;
      }
      SlotLayoutAccessors.writeLongField(targetSlot, layout, StructuralField.LEFT_SIBLING_KEY, structNode.getLeftSiblingKey());
    }
    if (layout.hasField(StructuralField.FIRST_CHILD_KEY)) {
      if (structNode == null) {
        return false;
      }
      SlotLayoutAccessors.writeLongField(targetSlot, layout, StructuralField.FIRST_CHILD_KEY, structNode.getFirstChildKey());
    }
    if (layout.hasField(StructuralField.LAST_CHILD_KEY)) {
      if (structNode == null) {
        return false;
      }
      SlotLayoutAccessors.writeLongField(targetSlot, layout, StructuralField.LAST_CHILD_KEY, structNode.getLastChildKey());
    }
    if (layout.hasField(StructuralField.CHILD_COUNT)) {
      if (structNode == null) {
        return false;
      }
      SlotLayoutAccessors.writeLongField(targetSlot, layout, StructuralField.CHILD_COUNT, structNode.getChildCount());
    }
    if (layout.hasField(StructuralField.DESCENDANT_COUNT)) {
      if (structNode == null) {
        return false;
      }
      SlotLayoutAccessors.writeLongField(targetSlot,
                                         layout,
                                         StructuralField.DESCENDANT_COUNT,
                                         structNode.getDescendantCount());
    }
    return true;
  }

  private static boolean writeNameFields(final NameNode nameNode, final ObjectKeyNode objectKeyNode,
      final MemorySegment targetSlot, final NodeKindLayout layout) {
    if (layout.hasField(StructuralField.PATH_NODE_KEY)) {
      if (nameNode == null) {
        return false;
      }
      SlotLayoutAccessors.writeLongField(targetSlot, layout, StructuralField.PATH_NODE_KEY, nameNode.getPathNodeKey());
    }
    if (layout.hasField(StructuralField.PREFIX_KEY)) {
      if (nameNode == null) {
        return false;
      }
      SlotLayoutAccessors.writeIntField(targetSlot, layout, StructuralField.PREFIX_KEY, nameNode.getPrefixKey());
    }
    if (layout.hasField(StructuralField.LOCAL_NAME_KEY)) {
      if (nameNode == null) {
        return false;
      }
      SlotLayoutAccessors.writeIntField(targetSlot, layout, StructuralField.LOCAL_NAME_KEY, nameNode.getLocalNameKey());
    }
    if (layout.hasField(StructuralField.URI_KEY)) {
      if (nameNode == null) {
        return false;
      }
      SlotLayoutAccessors.writeIntField(targetSlot, layout, StructuralField.URI_KEY, nameNode.getURIKey());
    }
    if (layout.hasField(StructuralField.NAME_KEY)) {
      if (objectKeyNode == null) {
        return false;
      }
      SlotLayoutAccessors.writeIntField(targetSlot, layout, StructuralField.NAME_KEY, objectKeyNode.getNameKey());
    }
    return true;
  }

  private static boolean writeBooleanField(final BooleanValueNode booleanValueNode, final MemorySegment targetSlot,
      final NodeKindLayout layout) {
    if (layout.hasField(StructuralField.BOOLEAN_VALUE)) {
      if (booleanValueNode == null) {
        return false;
      }
      SlotLayoutAccessors.writeBooleanField(targetSlot,
                                            layout,
                                            StructuralField.BOOLEAN_VALUE,
                                            booleanValueNode.getValue());
    }
    return true;
  }
}
