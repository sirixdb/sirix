package io.sirix.node.layout;

import io.sirix.node.NodeKind;
import io.sirix.node.MemorySegmentBytesOut;
import io.sirix.node.interfaces.BooleanValueNode;
import io.sirix.node.interfaces.DataRecord;
import io.sirix.node.interfaces.NameNode;
import io.sirix.node.interfaces.NumericValueNode;
import io.sirix.node.interfaces.StructNode;
import io.sirix.node.interfaces.immutable.ImmutableNode;
import io.sirix.node.json.ObjectKeyNode;
import io.sirix.node.json.ObjectStringNode;
import io.sirix.node.json.StringNode;
import io.sirix.node.xml.AttributeNode;
import io.sirix.node.xml.CommentNode;
import io.sirix.node.xml.ElementNode;
import io.sirix.node.xml.PINode;
import io.sirix.node.xml.TextNode;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Objects;

/**
 * Projects record structural metadata into a fixed-slot memory layout.
 *
 * <p>
 * The projector writes fixed-width structural fields directly, fills payload reference metadata,
 * and writes inline payload bytes for VALUE_BLOB payloads (strings, numbers).
 */
public final class FixedSlotRecordProjector {

  /** Unaligned long layout for reading/writing vector entries in inline payload. */
  private static final ValueLayout.OfLong LONG_UNALIGNED = ValueLayout.JAVA_LONG_UNALIGNED;

  /** Pre-zeroed buffer used to clear header bytes without allocating an asSlice(). */
  private static final MemorySegment ZERO_BUFFER = MemorySegment.ofArray(new byte[512]);

  /** Thread-local buffer for serializing number values inline. */
  private static final ThreadLocal<MemorySegmentBytesOut> NUMBER_BUFFER =
      ThreadLocal.withInitial(() -> new MemorySegmentBytesOut(32));

  private FixedSlotRecordProjector() {}

  /**
   * Check whether all payload refs in the layout are supported for inline projection. Supported
   * kinds: VALUE_BLOB, ATTRIBUTE_VECTOR, NAMESPACE_VECTOR.
   *
   * @param layout fixed-slot layout descriptor
   * @return {@code true} if all payload refs are supported or there are no payload refs
   */
  public static boolean hasSupportedPayloads(final NodeKindLayout layout) {
    for (int i = 0, refs = layout.payloadRefCount(); i < refs; i++) {
      final PayloadRefKind kind = layout.payloadRef(i).kind();
      if (kind != PayloadRefKind.VALUE_BLOB && kind != PayloadRefKind.ATTRIBUTE_VECTOR
          && kind != PayloadRefKind.NAMESPACE_VECTOR) {
        return false;
      }
    }
    return true;
  }

  /**
   * Compute the inline payload byte length for a record projected into the given layout.
   *
   * @param record the data record
   * @param layout the layout descriptor
   * @return total inline payload bytes, or {@code -1} if the record cannot be projected
   */
  public static int computeInlinePayloadLength(final DataRecord record, final NodeKindLayout layout) {
    int total = 0;
    for (int i = 0, refs = layout.payloadRefCount(); i < refs; i++) {
      final PayloadRef payloadRef = layout.payloadRef(i);
      final PayloadRefKind kind = payloadRef.kind();
      if (kind == PayloadRefKind.VALUE_BLOB) {
        final int length = getValueBlobLength(record);
        if (length < 0) {
          return -1;
        }
        total += length;
      } else if (kind == PayloadRefKind.ATTRIBUTE_VECTOR || kind == PayloadRefKind.NAMESPACE_VECTOR) {
        final int length = getVectorPayloadLength(record, kind);
        if (length < 0) {
          return -1;
        }
        total += length;
      } else {
        return -1;
      }
    }
    return total;
  }

  /**
   * Project a record into the given fixed-slot target at the specified base offset.
   *
   * <p>
   * For payload-bearing nodes (strings, numbers), the target area starting at {@code baseOffset} must
   * be sized to include both the fixed-slot header and inline payload bytes. The payload bytes are
   * written immediately after the header, and the PayloadRef metadata in the header stores the offset
   * (relative to slot data start) and length.
   *
   * @param record record to project
   * @param layout fixed-slot layout descriptor
   * @param targetSlot target memory (may be the full slotMemory)
   * @param baseOffset absolute byte offset where the slot data begins
   * @return {@code true} if projection succeeded, {@code false} if the record cannot be projected
   */
  public static boolean project(final DataRecord record, final NodeKindLayout layout, final MemorySegment targetSlot,
      final long baseOffset) {
    Objects.requireNonNull(record, "record must not be null");
    Objects.requireNonNull(layout, "layout must not be null");
    Objects.requireNonNull(targetSlot, "targetSlot must not be null");

    if (!layout.isFixedSlotSupported()) {
      return false;
    }

    // Only supported payload kinds are allowed for inline projection.
    if (!hasSupportedPayloads(layout)) {
      return false;
    }

    final int headerSize = layout.fixedSlotSizeInBytes();
    if (targetSlot.byteSize() - baseOffset < headerSize) {
      throw new IllegalArgumentException(
          "target slot too small: available=" + (targetSlot.byteSize() - baseOffset) + " < " + headerSize);
    }

    // Clear the header portion using bulk copy from pre-zeroed buffer (avoids asSlice allocation).
    MemorySegment.copy(ZERO_BUFFER, 0, targetSlot, baseOffset, headerSize);

    final ImmutableNode immutableNode = record instanceof ImmutableNode node
        ? node
        : null;
    final StructNode structNode = record instanceof StructNode node
        ? node
        : null;
    final NameNode nameNode = record instanceof NameNode node
        ? node
        : null;
    final BooleanValueNode booleanValueNode = record instanceof BooleanValueNode node
        ? node
        : null;
    final ObjectKeyNode objectKeyNode = record instanceof ObjectKeyNode node
        ? node
        : null;

    if (!writeCommonFields(record, immutableNode, targetSlot, baseOffset, layout)) {
      return false;
    }
    if (!writeStructuralFields(structNode, targetSlot, baseOffset, layout)) {
      return false;
    }
    if (!writeNameFields(nameNode, objectKeyNode, targetSlot, baseOffset, layout)) {
      return false;
    }
    if (!writeBooleanField(booleanValueNode, targetSlot, baseOffset, layout)) {
      return false;
    }
    if (!writeInlinePayloadRefs(record, targetSlot, baseOffset, layout)) {
      return false;
    }

    return true;
  }

  private static boolean writeCommonFields(final DataRecord record, final ImmutableNode immutableNode,
      final MemorySegment targetSlot, final long baseOffset, final NodeKindLayout layout) {
    if (layout.hasField(StructuralField.PREVIOUS_REVISION)) {
      SlotLayoutAccessors.writeIntField(targetSlot, baseOffset, layout, StructuralField.PREVIOUS_REVISION,
          record.getPreviousRevisionNumber());
    }
    if (layout.hasField(StructuralField.LAST_MODIFIED_REVISION)) {
      SlotLayoutAccessors.writeIntField(targetSlot, baseOffset, layout, StructuralField.LAST_MODIFIED_REVISION,
          record.getLastModifiedRevisionNumber());
    }
    if (layout.hasField(StructuralField.PARENT_KEY)) {
      if (immutableNode == null) {
        return false;
      }
      SlotLayoutAccessors.writeLongField(targetSlot, baseOffset, layout, StructuralField.PARENT_KEY,
          immutableNode.getParentKey());
    }
    if (layout.hasField(StructuralField.HASH)) {
      if (immutableNode == null) {
        return false;
      }
      SlotLayoutAccessors.writeLongField(targetSlot, baseOffset, layout, StructuralField.HASH, immutableNode.getHash());
    }
    return true;
  }

  private static boolean writeStructuralFields(final StructNode structNode, final MemorySegment targetSlot,
      final long baseOffset, final NodeKindLayout layout) {
    if (layout.hasField(StructuralField.RIGHT_SIBLING_KEY)) {
      if (structNode == null) {
        return false;
      }
      SlotLayoutAccessors.writeLongField(targetSlot, baseOffset, layout, StructuralField.RIGHT_SIBLING_KEY,
          structNode.getRightSiblingKey());
    }
    if (layout.hasField(StructuralField.LEFT_SIBLING_KEY)) {
      if (structNode == null) {
        return false;
      }
      SlotLayoutAccessors.writeLongField(targetSlot, baseOffset, layout, StructuralField.LEFT_SIBLING_KEY,
          structNode.getLeftSiblingKey());
    }
    if (layout.hasField(StructuralField.FIRST_CHILD_KEY)) {
      if (structNode == null) {
        return false;
      }
      SlotLayoutAccessors.writeLongField(targetSlot, baseOffset, layout, StructuralField.FIRST_CHILD_KEY,
          structNode.getFirstChildKey());
    }
    if (layout.hasField(StructuralField.LAST_CHILD_KEY)) {
      if (structNode == null) {
        return false;
      }
      SlotLayoutAccessors.writeLongField(targetSlot, baseOffset, layout, StructuralField.LAST_CHILD_KEY,
          structNode.getLastChildKey());
    }
    if (layout.hasField(StructuralField.CHILD_COUNT)) {
      if (structNode == null) {
        return false;
      }
      SlotLayoutAccessors.writeLongField(targetSlot, baseOffset, layout, StructuralField.CHILD_COUNT,
          structNode.getChildCount());
    }
    if (layout.hasField(StructuralField.DESCENDANT_COUNT)) {
      if (structNode == null) {
        return false;
      }
      SlotLayoutAccessors.writeLongField(targetSlot, baseOffset, layout, StructuralField.DESCENDANT_COUNT,
          structNode.getDescendantCount());
    }
    return true;
  }

  private static boolean writeNameFields(final NameNode nameNode, final ObjectKeyNode objectKeyNode,
      final MemorySegment targetSlot, final long baseOffset, final NodeKindLayout layout) {
    if (layout.hasField(StructuralField.PATH_NODE_KEY)) {
      if (nameNode == null) {
        return false;
      }
      SlotLayoutAccessors.writeLongField(targetSlot, baseOffset, layout, StructuralField.PATH_NODE_KEY,
          nameNode.getPathNodeKey());
    }
    if (layout.hasField(StructuralField.PREFIX_KEY)) {
      if (nameNode == null) {
        return false;
      }
      SlotLayoutAccessors.writeIntField(targetSlot, baseOffset, layout, StructuralField.PREFIX_KEY,
          nameNode.getPrefixKey());
    }
    if (layout.hasField(StructuralField.LOCAL_NAME_KEY)) {
      if (nameNode == null) {
        return false;
      }
      SlotLayoutAccessors.writeIntField(targetSlot, baseOffset, layout, StructuralField.LOCAL_NAME_KEY,
          nameNode.getLocalNameKey());
    }
    if (layout.hasField(StructuralField.URI_KEY)) {
      if (nameNode == null) {
        return false;
      }
      SlotLayoutAccessors.writeIntField(targetSlot, baseOffset, layout, StructuralField.URI_KEY, nameNode.getURIKey());
    }
    if (layout.hasField(StructuralField.NAME_KEY)) {
      if (objectKeyNode == null) {
        return false;
      }
      SlotLayoutAccessors.writeIntField(targetSlot, baseOffset, layout, StructuralField.NAME_KEY,
          objectKeyNode.getNameKey());
    }
    return true;
  }

  private static boolean writeBooleanField(final BooleanValueNode booleanValueNode, final MemorySegment targetSlot,
      final long baseOffset, final NodeKindLayout layout) {
    if (layout.hasField(StructuralField.BOOLEAN_VALUE)) {
      if (booleanValueNode == null) {
        return false;
      }
      SlotLayoutAccessors.writeBooleanField(targetSlot, baseOffset, layout, StructuralField.BOOLEAN_VALUE,
          booleanValueNode.getValue());
    }
    return true;
  }

  /**
   * Write PayloadRef metadata and inline payload bytes. Supports VALUE_BLOB (strings, numbers),
   * ATTRIBUTE_VECTOR, and NAMESPACE_VECTOR. The inline payload is written immediately after the
   * fixed-slot header.
   */
  private static boolean writeInlinePayloadRefs(final DataRecord record, final MemorySegment targetSlot,
      final long baseOffset, final NodeKindLayout layout) {
    final int refs = layout.payloadRefCount();
    if (refs == 0) {
      return true;
    }

    // payloadOffset is relative to slot data start (i.e. starts at headerSize)
    int payloadOffset = layout.fixedSlotSizeInBytes();

    for (int i = 0; i < refs; i++) {
      final PayloadRef payloadRef = layout.payloadRef(i);
      final PayloadRefKind refKind = payloadRef.kind();

      if (refKind == PayloadRefKind.ATTRIBUTE_VECTOR) {
        if (!(record instanceof ElementNode element)) {
          return false;
        }
        final int count = element.getAttributeCount();
        final int length = count * Long.BYTES;
        SlotLayoutAccessors.writePayloadRef(targetSlot, baseOffset, layout, i, payloadOffset, length, 0);
        for (int j = 0; j < count; j++) {
          targetSlot.set(LONG_UNALIGNED, baseOffset + payloadOffset, element.getAttributeKey(j));
          payloadOffset += Long.BYTES;
        }
        continue;
      } else if (refKind == PayloadRefKind.NAMESPACE_VECTOR) {
        if (!(record instanceof ElementNode element)) {
          return false;
        }
        final int count = element.getNamespaceCount();
        final int length = count * Long.BYTES;
        SlotLayoutAccessors.writePayloadRef(targetSlot, baseOffset, layout, i, payloadOffset, length, 0);
        for (int j = 0; j < count; j++) {
          targetSlot.set(LONG_UNALIGNED, baseOffset + payloadOffset, element.getNamespaceKey(j));
          payloadOffset += Long.BYTES;
        }
        continue;
      }

      // VALUE_BLOB handling
      if (refKind != PayloadRefKind.VALUE_BLOB) {
        return false;
      }

      final byte[] payloadBytes;
      final int flags;

      if (record instanceof StringNode sn) {
        payloadBytes = sn.getRawValueWithoutDecompression();
        flags = sn.isCompressed()
            ? 1
            : 0;
      } else if (record instanceof ObjectStringNode osn) {
        payloadBytes = osn.getRawValueWithoutDecompression();
        flags = osn.isCompressed()
            ? 1
            : 0;
      } else if (record instanceof TextNode tn) {
        payloadBytes = tn.getRawValueWithoutDecompression();
        flags = tn.isCompressed()
            ? 1
            : 0;
      } else if (record instanceof CommentNode cn) {
        payloadBytes = cn.getRawValueWithoutDecompression();
        flags = cn.isCompressed()
            ? 1
            : 0;
      } else if (record instanceof PINode pi) {
        payloadBytes = pi.getRawValueWithoutDecompression();
        flags = pi.isCompressed()
            ? 1
            : 0;
      } else if (record instanceof AttributeNode an) {
        payloadBytes = an.getRawValueWithoutDecompression();
        flags = 0; // attributes never compressed
      } else if (record instanceof NumericValueNode nn) {
        final MemorySegmentBytesOut buf = NUMBER_BUFFER.get();
        buf.clear();
        NodeKind.serializeNumber(nn.getValue(), buf);
        final MemorySegment serialized = buf.getDestination();
        final int numLen = (int) serialized.byteSize();
        SlotLayoutAccessors.writePayloadRef(targetSlot, baseOffset, layout, i, payloadOffset, numLen, 0);
        if (numLen > 0) {
          MemorySegment.copy(serialized, 0, targetSlot, baseOffset + payloadOffset, numLen);
        }
        payloadOffset += numLen;
        continue;
      } else {
        return false;
      }

      final int length = payloadBytes != null
          ? payloadBytes.length
          : 0;
      SlotLayoutAccessors.writePayloadRef(targetSlot, baseOffset, layout, i, payloadOffset, length, flags);
      if (length > 0) {
        MemorySegment.copy(MemorySegment.ofArray(payloadBytes), 0, targetSlot, baseOffset + payloadOffset, length);
      }
      payloadOffset += length;
    }

    return true;
  }

  /**
   * Get the inline payload byte length for a VALUE_BLOB from the given record.
   *
   * @return byte length, or {@code -1} if the record type is unsupported
   */
  private static int getValueBlobLength(final DataRecord record) {
    if (record instanceof StringNode sn) {
      final byte[] raw = sn.getRawValueWithoutDecompression();
      return raw != null
          ? raw.length
          : 0;
    }
    if (record instanceof ObjectStringNode osn) {
      final byte[] raw = osn.getRawValueWithoutDecompression();
      return raw != null
          ? raw.length
          : 0;
    }
    if (record instanceof TextNode tn) {
      final byte[] raw = tn.getRawValueWithoutDecompression();
      return raw != null
          ? raw.length
          : 0;
    }
    if (record instanceof CommentNode cn) {
      final byte[] raw = cn.getRawValueWithoutDecompression();
      return raw != null
          ? raw.length
          : 0;
    }
    if (record instanceof PINode pi) {
      final byte[] raw = pi.getRawValueWithoutDecompression();
      return raw != null
          ? raw.length
          : 0;
    }
    if (record instanceof AttributeNode an) {
      final byte[] raw = an.getRawValueWithoutDecompression();
      return raw != null
          ? raw.length
          : 0;
    }
    if (record instanceof NumericValueNode nn) {
      final MemorySegmentBytesOut buf = NUMBER_BUFFER.get();
      buf.clear();
      NodeKind.serializeNumber(nn.getValue(), buf);
      return (int) buf.getDestination().byteSize();
    }
    return -1;
  }

  /**
   * Get the inline payload byte length for an ATTRIBUTE_VECTOR or NAMESPACE_VECTOR.
   *
   * @return byte length (count * 8), or {@code -1} if the record type is unsupported
   */
  private static int getVectorPayloadLength(final DataRecord record, final PayloadRefKind kind) {
    if (!(record instanceof ElementNode element)) {
      return -1;
    }
    if (kind == PayloadRefKind.ATTRIBUTE_VECTOR) {
      return element.getAttributeCount() * Long.BYTES;
    } else if (kind == PayloadRefKind.NAMESPACE_VECTOR) {
      return element.getNamespaceCount() * Long.BYTES;
    }
    return -1;
  }
}
