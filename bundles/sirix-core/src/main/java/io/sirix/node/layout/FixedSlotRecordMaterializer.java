package io.sirix.node.layout;

import io.brackit.query.atomic.QNm;
import io.sirix.access.ResourceConfiguration;
import io.sirix.node.MemorySegmentBytesIn;
import io.sirix.node.NodeKind;
import io.sirix.node.interfaces.DataRecord;
import io.sirix.node.interfaces.StructNode;
import io.sirix.node.json.ArrayNode;
import io.sirix.node.json.BooleanNode;
import io.sirix.node.json.JsonDocumentRootNode;
import io.sirix.node.json.NullNode;
import io.sirix.node.json.NumberNode;
import io.sirix.node.json.ObjectBooleanNode;
import io.sirix.node.json.ObjectKeyNode;
import io.sirix.node.json.ObjectNode;
import io.sirix.node.json.ObjectNullNode;
import io.sirix.node.json.ObjectNumberNode;
import io.sirix.node.json.ObjectStringNode;
import io.sirix.node.json.StringNode;
import io.sirix.node.interfaces.ValueNode;
import io.sirix.node.xml.AttributeNode;
import io.sirix.node.xml.CommentNode;
import io.sirix.node.xml.ElementNode;
import io.sirix.node.xml.NamespaceNode;
import io.sirix.node.xml.PINode;
import io.sirix.node.xml.TextNode;
import io.sirix.node.xml.XmlDocumentRootNode;
import io.sirix.settings.Fixed;
import net.openhft.hashing.LongHashFunction;

import java.lang.foreign.ValueLayout;

import java.lang.foreign.MemorySegment;

/**
 * Materializes {@link DataRecord} instances from fixed-slot in-memory layouts.
 *
 * <p>
 * Supports both payload-free node kinds and inline-payload node kinds (strings, numbers). Each call
 * allocates a fresh Java object. This is required because the write path (e.g.
 * {@code adaptForInsert}, {@code rollingAdd}) holds live references to multiple records of the same
 * kind simultaneously — singletons would cause aliasing corruption.
 * </p>
 */
public final class FixedSlotRecordMaterializer {
  private static final QNm EMPTY_QNM = new QNm("");

  private FixedSlotRecordMaterializer() {}

  /**
   * Populate an existing DataRecord singleton from fixed-slot bytes at the given offset within
   * {@code data} (zero allocation — avoids {@code asSlice()}).
   *
   * <p>
   * This method reads structural fields, metadata, and inline payloads from the fixed-slot
   * MemorySegment into the existing object's setters. For payload-bearing nodes (strings, numbers),
   * uses lazy value loading via {@code setLazyRawValue}/{@code setLazyNumberValue} to defer byte[]
   * allocation until the value is actually accessed.
   * </p>
   *
   * <p>
   * <b>CRITICAL:</b> For payload-bearing nodes, {@code setLazyRawValue}/{@code setLazyNumberValue}
   * resets hash to 0. Therefore hash must be set AFTER the lazy value call.
   * </p>
   *
   * @param existing the singleton to populate (must be the correct type for nodeKind)
   * @param nodeKind the node kind
   * @param nodeKey the record key
   * @param data the backing MemorySegment (may be the full slotMemory)
   * @param dataOffset absolute byte offset within {@code data} where this slot's data begins
   * @param dataLength length of the slot data in bytes
   * @param deweyIdBytes optional DeweyID bytes
   * @param resourceConfig resource configuration (unused but kept for API consistency)
   * @return true if the singleton was populated, false if the node kind is unsupported
   */
  public static boolean populateExisting(final DataRecord existing, final NodeKind nodeKind, final long nodeKey,
      final MemorySegment data, final long dataOffset, final int dataLength, final byte[] deweyIdBytes,
      final ResourceConfiguration resourceConfig) {
    if (existing == null || nodeKind == null || data == null) {
      return false;
    }

    final NodeKindLayout layout = nodeKind.layoutDescriptor();
    if (!layout.isFixedSlotSupported() || dataLength < layout.fixedSlotSizeInBytes()) {
      return false;
    }

    if (!FixedSlotRecordProjector.hasSupportedPayloads(layout)) {
      return false;
    }

    return switch (nodeKind) {
      case JSON_DOCUMENT -> {
        if (!(existing instanceof JsonDocumentRootNode node)) {
          yield false;
        }
        node.setNodeKey(nodeKey);
        populateJsonDocumentFields(node, data, dataOffset, layout);
        node.setDeweyIDBytes(deweyIdBytes);
        yield true;
      }
      case XML_DOCUMENT -> {
        if (!(existing instanceof XmlDocumentRootNode node)) {
          yield false;
        }
        node.setNodeKey(nodeKey);
        node.setFirstChildKey(
            SlotLayoutAccessors.readLongField(data, dataOffset, layout, StructuralField.FIRST_CHILD_KEY));
        node.setLastChildKey(
            SlotLayoutAccessors.readLongField(data, dataOffset, layout, StructuralField.LAST_CHILD_KEY));
        node.setChildCount(SlotLayoutAccessors.readLongField(data, dataOffset, layout, StructuralField.CHILD_COUNT));
        node.setDescendantCount(
            SlotLayoutAccessors.readLongField(data, dataOffset, layout, StructuralField.DESCENDANT_COUNT));
        node.setHash(SlotLayoutAccessors.readLongField(data, dataOffset, layout, StructuralField.HASH));
        node.setDeweyIDBytes(deweyIdBytes);
        yield true;
      }
      case OBJECT -> {
        if (!(existing instanceof ObjectNode node)) {
          yield false;
        }
        node.setNodeKey(nodeKey);
        populateObjectFields(node, data, dataOffset, layout);
        node.setDeweyIDBytes(deweyIdBytes);
        yield true;
      }
      case ARRAY -> {
        if (!(existing instanceof ArrayNode node)) {
          yield false;
        }
        node.setNodeKey(nodeKey);
        populateArrayFields(node, data, dataOffset, layout);
        node.setPathNodeKey(SlotLayoutAccessors.readLongField(data, dataOffset, layout, StructuralField.PATH_NODE_KEY));
        node.setDeweyIDBytes(deweyIdBytes);
        yield true;
      }
      case OBJECT_KEY -> {
        if (!(existing instanceof ObjectKeyNode node)) {
          yield false;
        }
        node.setNodeKey(nodeKey);
        node.setParentKey(SlotLayoutAccessors.readLongField(data, dataOffset, layout, StructuralField.PARENT_KEY));
        node.setPathNodeKey(SlotLayoutAccessors.readLongField(data, dataOffset, layout, StructuralField.PATH_NODE_KEY));
        node.setPreviousRevision(
            SlotLayoutAccessors.readIntField(data, dataOffset, layout, StructuralField.PREVIOUS_REVISION));
        node.setLastModifiedRevision(
            SlotLayoutAccessors.readIntField(data, dataOffset, layout, StructuralField.LAST_MODIFIED_REVISION));
        node.setRightSiblingKey(
            SlotLayoutAccessors.readLongField(data, dataOffset, layout, StructuralField.RIGHT_SIBLING_KEY));
        node.setLeftSiblingKey(
            SlotLayoutAccessors.readLongField(data, dataOffset, layout, StructuralField.LEFT_SIBLING_KEY));
        node.setFirstChildKey(
            SlotLayoutAccessors.readLongField(data, dataOffset, layout, StructuralField.FIRST_CHILD_KEY));
        node.setNameKey(SlotLayoutAccessors.readIntField(data, dataOffset, layout, StructuralField.NAME_KEY));
        node.clearCachedName();
        node.setDescendantCount(
            SlotLayoutAccessors.readLongField(data, dataOffset, layout, StructuralField.DESCENDANT_COUNT));
        node.setHash(SlotLayoutAccessors.readLongField(data, dataOffset, layout, StructuralField.HASH));
        node.setDeweyIDBytes(deweyIdBytes);
        yield true;
      }
      case BOOLEAN_VALUE -> {
        if (!(existing instanceof BooleanNode node)) {
          yield false;
        }
        node.setNodeKey(nodeKey);
        populateBooleanNodeFields(node, data, dataOffset, layout);
        node.setDeweyIDBytes(deweyIdBytes);
        yield true;
      }
      case NULL_VALUE -> {
        if (!(existing instanceof NullNode node)) {
          yield false;
        }
        node.setNodeKey(nodeKey);
        populateLeafNodeFields(node, data, dataOffset, layout);
        node.setDeweyIDBytes(deweyIdBytes);
        yield true;
      }
      case OBJECT_BOOLEAN_VALUE -> {
        if (!(existing instanceof ObjectBooleanNode node)) {
          yield false;
        }
        node.setNodeKey(nodeKey);
        node.setParentKey(SlotLayoutAccessors.readLongField(data, dataOffset, layout, StructuralField.PARENT_KEY));
        node.setPreviousRevision(
            SlotLayoutAccessors.readIntField(data, dataOffset, layout, StructuralField.PREVIOUS_REVISION));
        node.setLastModifiedRevision(
            SlotLayoutAccessors.readIntField(data, dataOffset, layout, StructuralField.LAST_MODIFIED_REVISION));
        node.setHash(SlotLayoutAccessors.readLongField(data, dataOffset, layout, StructuralField.HASH));
        node.setValue(SlotLayoutAccessors.readBooleanField(data, dataOffset, layout, StructuralField.BOOLEAN_VALUE));
        node.setDeweyIDBytes(deweyIdBytes);
        yield true;
      }
      case OBJECT_NULL_VALUE -> {
        if (!(existing instanceof ObjectNullNode node)) {
          yield false;
        }
        node.setNodeKey(nodeKey);
        node.setParentKey(SlotLayoutAccessors.readLongField(data, dataOffset, layout, StructuralField.PARENT_KEY));
        node.setPreviousRevision(
            SlotLayoutAccessors.readIntField(data, dataOffset, layout, StructuralField.PREVIOUS_REVISION));
        node.setLastModifiedRevision(
            SlotLayoutAccessors.readIntField(data, dataOffset, layout, StructuralField.LAST_MODIFIED_REVISION));
        node.setHash(SlotLayoutAccessors.readLongField(data, dataOffset, layout, StructuralField.HASH));
        node.setDeweyIDBytes(deweyIdBytes);
        yield true;
      }
      case NAMESPACE -> {
        if (!(existing instanceof NamespaceNode node)) {
          yield false;
        }
        node.setNodeKey(nodeKey);
        node.setParentKey(SlotLayoutAccessors.readLongField(data, dataOffset, layout, StructuralField.PARENT_KEY));
        node.setPreviousRevision(
            SlotLayoutAccessors.readIntField(data, dataOffset, layout, StructuralField.PREVIOUS_REVISION));
        node.setLastModifiedRevision(
            SlotLayoutAccessors.readIntField(data, dataOffset, layout, StructuralField.LAST_MODIFIED_REVISION));
        node.setPathNodeKey(SlotLayoutAccessors.readLongField(data, dataOffset, layout, StructuralField.PATH_NODE_KEY));
        node.setPrefixKey(SlotLayoutAccessors.readIntField(data, dataOffset, layout, StructuralField.PREFIX_KEY));
        node.setLocalNameKey(
            SlotLayoutAccessors.readIntField(data, dataOffset, layout, StructuralField.LOCAL_NAME_KEY));
        node.setURIKey(SlotLayoutAccessors.readIntField(data, dataOffset, layout, StructuralField.URI_KEY));
        node.setHash(SlotLayoutAccessors.readLongField(data, dataOffset, layout, StructuralField.HASH));
        node.setName(EMPTY_QNM);
        node.setDeweyIDBytes(deweyIdBytes);
        yield true;
      }
      case ELEMENT -> {
        if (!(existing instanceof ElementNode node)) {
          yield false;
        }
        node.setNodeKey(nodeKey);
        node.setParentKey(SlotLayoutAccessors.readLongField(data, dataOffset, layout, StructuralField.PARENT_KEY));
        node.setRightSiblingKey(
            SlotLayoutAccessors.readLongField(data, dataOffset, layout, StructuralField.RIGHT_SIBLING_KEY));
        node.setLeftSiblingKey(
            SlotLayoutAccessors.readLongField(data, dataOffset, layout, StructuralField.LEFT_SIBLING_KEY));
        node.setFirstChildKey(
            SlotLayoutAccessors.readLongField(data, dataOffset, layout, StructuralField.FIRST_CHILD_KEY));
        node.setLastChildKey(
            SlotLayoutAccessors.readLongField(data, dataOffset, layout, StructuralField.LAST_CHILD_KEY));
        node.setPathNodeKey(SlotLayoutAccessors.readLongField(data, dataOffset, layout, StructuralField.PATH_NODE_KEY));
        node.setPrefixKey(SlotLayoutAccessors.readIntField(data, dataOffset, layout, StructuralField.PREFIX_KEY));
        node.setLocalNameKey(
            SlotLayoutAccessors.readIntField(data, dataOffset, layout, StructuralField.LOCAL_NAME_KEY));
        node.setURIKey(SlotLayoutAccessors.readIntField(data, dataOffset, layout, StructuralField.URI_KEY));
        node.setPreviousRevision(
            SlotLayoutAccessors.readIntField(data, dataOffset, layout, StructuralField.PREVIOUS_REVISION));
        node.setLastModifiedRevision(
            SlotLayoutAccessors.readIntField(data, dataOffset, layout, StructuralField.LAST_MODIFIED_REVISION));
        node.setChildCount(SlotLayoutAccessors.readLongField(data, dataOffset, layout, StructuralField.CHILD_COUNT));
        node.setDescendantCount(
            SlotLayoutAccessors.readLongField(data, dataOffset, layout, StructuralField.DESCENDANT_COUNT));
        node.setHash(SlotLayoutAccessors.readLongField(data, dataOffset, layout, StructuralField.HASH));
        readInlineVectorPayload(node, data, dataOffset, layout, 0, true);
        readInlineVectorPayload(node, data, dataOffset, layout, 1, false);
        node.setName(EMPTY_QNM);
        node.setDeweyIDBytes(deweyIdBytes);
        yield true;
      }
      case STRING_VALUE -> {
        if (!(existing instanceof StringNode node)) {
          yield false;
        }
        node.setNodeKey(nodeKey);
        populateLeafNodeFields(node, data, dataOffset, layout);
        // setLazyRawValue BEFORE setHash — setLazyRawValue resets hash to 0
        final long strPointer = SlotLayoutAccessors.readPayloadPointer(data, dataOffset, layout, 0);
        final int strLength = SlotLayoutAccessors.readPayloadLength(data, dataOffset, layout, 0);
        final int strFlags = SlotLayoutAccessors.readPayloadFlags(data, dataOffset, layout, 0);
        node.setLazyRawValue(data, dataOffset + strPointer, strLength, (strFlags & 1) != 0);
        node.setHash(SlotLayoutAccessors.readLongField(data, dataOffset, layout, StructuralField.HASH));
        node.setDeweyIDBytes(deweyIdBytes);
        yield true;
      }
      case NUMBER_VALUE -> {
        if (!(existing instanceof NumberNode node)) {
          yield false;
        }
        node.setNodeKey(nodeKey);
        populateLeafNodeFields(node, data, dataOffset, layout);
        // setLazyNumberValue BEFORE setHash — setLazyNumberValue resets hash to 0
        final long numPointer = SlotLayoutAccessors.readPayloadPointer(data, dataOffset, layout, 0);
        final int numLength = SlotLayoutAccessors.readPayloadLength(data, dataOffset, layout, 0);
        node.setLazyNumberValue(data, dataOffset + numPointer, numLength);
        node.setHash(SlotLayoutAccessors.readLongField(data, dataOffset, layout, StructuralField.HASH));
        node.setDeweyIDBytes(deweyIdBytes);
        yield true;
      }
      case OBJECT_STRING_VALUE -> {
        if (!(existing instanceof ObjectStringNode node)) {
          yield false;
        }
        node.setNodeKey(nodeKey);
        node.setParentKey(SlotLayoutAccessors.readLongField(data, dataOffset, layout, StructuralField.PARENT_KEY));
        node.setPreviousRevision(
            SlotLayoutAccessors.readIntField(data, dataOffset, layout, StructuralField.PREVIOUS_REVISION));
        node.setLastModifiedRevision(
            SlotLayoutAccessors.readIntField(data, dataOffset, layout, StructuralField.LAST_MODIFIED_REVISION));
        // setLazyRawValue BEFORE setHash — setLazyRawValue resets hash to 0
        final long objStrPointer = SlotLayoutAccessors.readPayloadPointer(data, dataOffset, layout, 0);
        final int objStrLength = SlotLayoutAccessors.readPayloadLength(data, dataOffset, layout, 0);
        final int objStrFlags = SlotLayoutAccessors.readPayloadFlags(data, dataOffset, layout, 0);
        node.setLazyRawValue(data, dataOffset + objStrPointer, objStrLength, (objStrFlags & 1) != 0);
        node.setHash(SlotLayoutAccessors.readLongField(data, dataOffset, layout, StructuralField.HASH));
        node.setDeweyIDBytes(deweyIdBytes);
        yield true;
      }
      case OBJECT_NUMBER_VALUE -> {
        if (!(existing instanceof ObjectNumberNode node)) {
          yield false;
        }
        node.setNodeKey(nodeKey);
        node.setParentKey(SlotLayoutAccessors.readLongField(data, dataOffset, layout, StructuralField.PARENT_KEY));
        node.setPreviousRevision(
            SlotLayoutAccessors.readIntField(data, dataOffset, layout, StructuralField.PREVIOUS_REVISION));
        node.setLastModifiedRevision(
            SlotLayoutAccessors.readIntField(data, dataOffset, layout, StructuralField.LAST_MODIFIED_REVISION));
        // setLazyNumberValue BEFORE setHash — setLazyNumberValue resets hash to 0
        final long objNumPointer = SlotLayoutAccessors.readPayloadPointer(data, dataOffset, layout, 0);
        final int objNumLength = SlotLayoutAccessors.readPayloadLength(data, dataOffset, layout, 0);
        node.setLazyNumberValue(data, dataOffset + objNumPointer, objNumLength);
        node.setHash(SlotLayoutAccessors.readLongField(data, dataOffset, layout, StructuralField.HASH));
        node.setDeweyIDBytes(deweyIdBytes);
        yield true;
      }
      case TEXT -> {
        if (!(existing instanceof TextNode node)) {
          yield false;
        }
        node.setNodeKey(nodeKey);
        populateLeafNodeFields(node, data, dataOffset, layout);
        // setLazyRawValue BEFORE setHash — setLazyRawValue resets hash to 0
        final long textPointer = SlotLayoutAccessors.readPayloadPointer(data, dataOffset, layout, 0);
        final int textLength = SlotLayoutAccessors.readPayloadLength(data, dataOffset, layout, 0);
        final int textFlags = SlotLayoutAccessors.readPayloadFlags(data, dataOffset, layout, 0);
        node.setLazyRawValue(data, dataOffset + textPointer, textLength, (textFlags & 1) != 0);
        node.setHash(SlotLayoutAccessors.readLongField(data, dataOffset, layout, StructuralField.HASH));
        node.setDeweyIDBytes(deweyIdBytes);
        yield true;
      }
      case COMMENT -> {
        if (!(existing instanceof CommentNode node)) {
          yield false;
        }
        node.setNodeKey(nodeKey);
        populateLeafNodeFields(node, data, dataOffset, layout);
        // setLazyRawValue BEFORE setHash — setLazyRawValue resets hash to 0
        final long commentPointer = SlotLayoutAccessors.readPayloadPointer(data, dataOffset, layout, 0);
        final int commentLength = SlotLayoutAccessors.readPayloadLength(data, dataOffset, layout, 0);
        final int commentFlags = SlotLayoutAccessors.readPayloadFlags(data, dataOffset, layout, 0);
        node.setLazyRawValue(data, dataOffset + commentPointer, commentLength, (commentFlags & 1) != 0);
        node.setHash(SlotLayoutAccessors.readLongField(data, dataOffset, layout, StructuralField.HASH));
        node.setDeweyIDBytes(deweyIdBytes);
        yield true;
      }
      case ATTRIBUTE -> {
        if (!(existing instanceof AttributeNode node)) {
          yield false;
        }
        node.setNodeKey(nodeKey);
        node.setParentKey(SlotLayoutAccessors.readLongField(data, dataOffset, layout, StructuralField.PARENT_KEY));
        node.setPathNodeKey(SlotLayoutAccessors.readLongField(data, dataOffset, layout, StructuralField.PATH_NODE_KEY));
        node.setPrefixKey(SlotLayoutAccessors.readIntField(data, dataOffset, layout, StructuralField.PREFIX_KEY));
        node.setLocalNameKey(
            SlotLayoutAccessors.readIntField(data, dataOffset, layout, StructuralField.LOCAL_NAME_KEY));
        node.setURIKey(SlotLayoutAccessors.readIntField(data, dataOffset, layout, StructuralField.URI_KEY));
        node.setPreviousRevision(
            SlotLayoutAccessors.readIntField(data, dataOffset, layout, StructuralField.PREVIOUS_REVISION));
        node.setLastModifiedRevision(
            SlotLayoutAccessors.readIntField(data, dataOffset, layout, StructuralField.LAST_MODIFIED_REVISION));
        // setLazyRawValue BEFORE setHash — setLazyRawValue resets hash to 0
        final long attrPointer = SlotLayoutAccessors.readPayloadPointer(data, dataOffset, layout, 0);
        final int attrLength = SlotLayoutAccessors.readPayloadLength(data, dataOffset, layout, 0);
        node.setLazyRawValue(data, dataOffset + attrPointer, attrLength);
        node.setHash(SlotLayoutAccessors.readLongField(data, dataOffset, layout, StructuralField.HASH));
        node.setName(EMPTY_QNM);
        node.setDeweyIDBytes(deweyIdBytes);
        yield true;
      }
      case PROCESSING_INSTRUCTION -> {
        if (!(existing instanceof PINode node)) {
          yield false;
        }
        node.setNodeKey(nodeKey);
        node.setParentKey(SlotLayoutAccessors.readLongField(data, dataOffset, layout, StructuralField.PARENT_KEY));
        node.setRightSiblingKey(
            SlotLayoutAccessors.readLongField(data, dataOffset, layout, StructuralField.RIGHT_SIBLING_KEY));
        node.setLeftSiblingKey(
            SlotLayoutAccessors.readLongField(data, dataOffset, layout, StructuralField.LEFT_SIBLING_KEY));
        node.setFirstChildKey(
            SlotLayoutAccessors.readLongField(data, dataOffset, layout, StructuralField.FIRST_CHILD_KEY));
        node.setLastChildKey(
            SlotLayoutAccessors.readLongField(data, dataOffset, layout, StructuralField.LAST_CHILD_KEY));
        node.setPathNodeKey(SlotLayoutAccessors.readLongField(data, dataOffset, layout, StructuralField.PATH_NODE_KEY));
        node.setPrefixKey(SlotLayoutAccessors.readIntField(data, dataOffset, layout, StructuralField.PREFIX_KEY));
        node.setLocalNameKey(
            SlotLayoutAccessors.readIntField(data, dataOffset, layout, StructuralField.LOCAL_NAME_KEY));
        node.setURIKey(SlotLayoutAccessors.readIntField(data, dataOffset, layout, StructuralField.URI_KEY));
        node.setPreviousRevision(
            SlotLayoutAccessors.readIntField(data, dataOffset, layout, StructuralField.PREVIOUS_REVISION));
        node.setLastModifiedRevision(
            SlotLayoutAccessors.readIntField(data, dataOffset, layout, StructuralField.LAST_MODIFIED_REVISION));
        node.setChildCount(SlotLayoutAccessors.readLongField(data, dataOffset, layout, StructuralField.CHILD_COUNT));
        node.setDescendantCount(
            SlotLayoutAccessors.readLongField(data, dataOffset, layout, StructuralField.DESCENDANT_COUNT));
        // setLazyRawValue BEFORE setHash — setLazyRawValue resets hash to 0
        final long piPointer = SlotLayoutAccessors.readPayloadPointer(data, dataOffset, layout, 0);
        final int piLength = SlotLayoutAccessors.readPayloadLength(data, dataOffset, layout, 0);
        final int piFlags = SlotLayoutAccessors.readPayloadFlags(data, dataOffset, layout, 0);
        node.setLazyRawValue(data, dataOffset + piPointer, piLength, (piFlags & 1) != 0);
        node.setHash(SlotLayoutAccessors.readLongField(data, dataOffset, layout, StructuralField.HASH));
        node.setName(EMPTY_QNM);
        node.setDeweyIDBytes(deweyIdBytes);
        yield true;
      }
      default -> false;
    };
  }

  /**
   * Populate common structural fields for an ObjectNode.
   */
  private static void populateObjectFields(final ObjectNode node, final MemorySegment data, final long dataOffset,
      final NodeKindLayout layout) {
    node.setParentKey(SlotLayoutAccessors.readLongField(data, dataOffset, layout, StructuralField.PARENT_KEY));
    node.setPreviousRevision(
        SlotLayoutAccessors.readIntField(data, dataOffset, layout, StructuralField.PREVIOUS_REVISION));
    node.setLastModifiedRevision(
        SlotLayoutAccessors.readIntField(data, dataOffset, layout, StructuralField.LAST_MODIFIED_REVISION));
    node.setRightSiblingKey(
        SlotLayoutAccessors.readLongField(data, dataOffset, layout, StructuralField.RIGHT_SIBLING_KEY));
    node.setLeftSiblingKey(
        SlotLayoutAccessors.readLongField(data, dataOffset, layout, StructuralField.LEFT_SIBLING_KEY));
    node.setFirstChildKey(SlotLayoutAccessors.readLongField(data, dataOffset, layout, StructuralField.FIRST_CHILD_KEY));
    node.setLastChildKey(SlotLayoutAccessors.readLongField(data, dataOffset, layout, StructuralField.LAST_CHILD_KEY));
    node.setChildCount(SlotLayoutAccessors.readLongField(data, dataOffset, layout, StructuralField.CHILD_COUNT));
    node.setDescendantCount(
        SlotLayoutAccessors.readLongField(data, dataOffset, layout, StructuralField.DESCENDANT_COUNT));
    node.setHash(SlotLayoutAccessors.readLongField(data, dataOffset, layout, StructuralField.HASH));
  }

  /**
   * Populate common structural fields for an ArrayNode.
   */
  private static void populateArrayFields(final ArrayNode node, final MemorySegment data, final long dataOffset,
      final NodeKindLayout layout) {
    node.setParentKey(SlotLayoutAccessors.readLongField(data, dataOffset, layout, StructuralField.PARENT_KEY));
    node.setPreviousRevision(
        SlotLayoutAccessors.readIntField(data, dataOffset, layout, StructuralField.PREVIOUS_REVISION));
    node.setLastModifiedRevision(
        SlotLayoutAccessors.readIntField(data, dataOffset, layout, StructuralField.LAST_MODIFIED_REVISION));
    node.setRightSiblingKey(
        SlotLayoutAccessors.readLongField(data, dataOffset, layout, StructuralField.RIGHT_SIBLING_KEY));
    node.setLeftSiblingKey(
        SlotLayoutAccessors.readLongField(data, dataOffset, layout, StructuralField.LEFT_SIBLING_KEY));
    node.setFirstChildKey(SlotLayoutAccessors.readLongField(data, dataOffset, layout, StructuralField.FIRST_CHILD_KEY));
    node.setLastChildKey(SlotLayoutAccessors.readLongField(data, dataOffset, layout, StructuralField.LAST_CHILD_KEY));
    node.setChildCount(SlotLayoutAccessors.readLongField(data, dataOffset, layout, StructuralField.CHILD_COUNT));
    node.setDescendantCount(
        SlotLayoutAccessors.readLongField(data, dataOffset, layout, StructuralField.DESCENDANT_COUNT));
    node.setHash(SlotLayoutAccessors.readLongField(data, dataOffset, layout, StructuralField.HASH));
  }

  /**
   * Populate fields for a JsonDocumentRootNode.
   */
  private static void populateJsonDocumentFields(final JsonDocumentRootNode node, final MemorySegment data,
      final long dataOffset, final NodeKindLayout layout) {
    node.setParentKey(Fixed.NULL_NODE_KEY.getStandardProperty());
    node.setRightSiblingKey(Fixed.NULL_NODE_KEY.getStandardProperty());
    node.setLeftSiblingKey(Fixed.NULL_NODE_KEY.getStandardProperty());
    node.setFirstChildKey(SlotLayoutAccessors.readLongField(data, dataOffset, layout, StructuralField.FIRST_CHILD_KEY));
    node.setLastChildKey(SlotLayoutAccessors.readLongField(data, dataOffset, layout, StructuralField.LAST_CHILD_KEY));
    node.setChildCount(SlotLayoutAccessors.readLongField(data, dataOffset, layout, StructuralField.CHILD_COUNT));
    node.setDescendantCount(
        SlotLayoutAccessors.readLongField(data, dataOffset, layout, StructuralField.DESCENDANT_COUNT));
    node.setHash(SlotLayoutAccessors.readLongField(data, dataOffset, layout, StructuralField.HASH));
    node.setPreviousRevision(0);
    node.setLastModifiedRevision(0);
  }

  /**
   * Populate common fields for leaf nodes with siblings.
   */
  private static void populateLeafNodeFields(final StructNode node, final MemorySegment data, final long dataOffset,
      final NodeKindLayout layout) {
    node.setParentKey(SlotLayoutAccessors.readLongField(data, dataOffset, layout, StructuralField.PARENT_KEY));
    node.setPreviousRevision(
        SlotLayoutAccessors.readIntField(data, dataOffset, layout, StructuralField.PREVIOUS_REVISION));
    node.setLastModifiedRevision(
        SlotLayoutAccessors.readIntField(data, dataOffset, layout, StructuralField.LAST_MODIFIED_REVISION));
    node.setRightSiblingKey(
        SlotLayoutAccessors.readLongField(data, dataOffset, layout, StructuralField.RIGHT_SIBLING_KEY));
    node.setLeftSiblingKey(
        SlotLayoutAccessors.readLongField(data, dataOffset, layout, StructuralField.LEFT_SIBLING_KEY));
    node.setHash(SlotLayoutAccessors.readLongField(data, dataOffset, layout, StructuralField.HASH));
  }

  /**
   * Populate fields for a BooleanNode (has siblings + boolean value).
   */
  private static void populateBooleanNodeFields(final BooleanNode node, final MemorySegment data, final long dataOffset,
      final NodeKindLayout layout) {
    node.setParentKey(SlotLayoutAccessors.readLongField(data, dataOffset, layout, StructuralField.PARENT_KEY));
    node.setPreviousRevision(
        SlotLayoutAccessors.readIntField(data, dataOffset, layout, StructuralField.PREVIOUS_REVISION));
    node.setLastModifiedRevision(
        SlotLayoutAccessors.readIntField(data, dataOffset, layout, StructuralField.LAST_MODIFIED_REVISION));
    node.setRightSiblingKey(
        SlotLayoutAccessors.readLongField(data, dataOffset, layout, StructuralField.RIGHT_SIBLING_KEY));
    node.setLeftSiblingKey(
        SlotLayoutAccessors.readLongField(data, dataOffset, layout, StructuralField.LEFT_SIBLING_KEY));
    node.setHash(SlotLayoutAccessors.readLongField(data, dataOffset, layout, StructuralField.HASH));
    node.setValue(SlotLayoutAccessors.readBooleanField(data, dataOffset, layout, StructuralField.BOOLEAN_VALUE));
  }

  /**
   * Materialize a fixed-slot record.
   *
   * @param nodeKind node kind metadata for this slot
   * @param nodeKey record key
   * @param data fixed-slot bytes (header + optional inline payload)
   * @param deweyIdBytes optional DeweyID bytes
   * @param resourceConfig resource configuration
   * @return materialized record, or {@code null} if this slot cannot be materialized as fixed format
   */
  public static DataRecord materialize(final NodeKind nodeKind, final long nodeKey, final MemorySegment data,
      final byte[] deweyIdBytes, final ResourceConfiguration resourceConfig) {
    if (nodeKind == null || data == null || resourceConfig == null) {
      return null;
    }

    final NodeKindLayout layout = nodeKind.layoutDescriptor();
    if (!layout.isFixedSlotSupported() || data.byteSize() < layout.fixedSlotSizeInBytes()) {
      return null;
    }

    if (!FixedSlotRecordProjector.hasSupportedPayloads(layout)) {
      return null;
    }

    final LongHashFunction hashFunction = resourceConfig.nodeHashFunction;
    final long nullNodeKey = Fixed.NULL_NODE_KEY.getStandardProperty();

    return switch (nodeKind) {
      case JSON_DOCUMENT -> {
        final JsonDocumentRootNode node = new JsonDocumentRootNode(nodeKey, 0, 0, 0, 0, hashFunction);
        node.setParentKey(nullNodeKey);
        node.setRightSiblingKey(nullNodeKey);
        node.setLeftSiblingKey(nullNodeKey);
        node.setFirstChildKey(SlotLayoutAccessors.readLongField(data, layout, StructuralField.FIRST_CHILD_KEY));
        node.setLastChildKey(SlotLayoutAccessors.readLongField(data, layout, StructuralField.LAST_CHILD_KEY));
        node.setChildCount(SlotLayoutAccessors.readLongField(data, layout, StructuralField.CHILD_COUNT));
        node.setDescendantCount(SlotLayoutAccessors.readLongField(data, layout, StructuralField.DESCENDANT_COUNT));
        node.setHash(SlotLayoutAccessors.readLongField(data, layout, StructuralField.HASH));
        node.setPreviousRevision(0);
        node.setLastModifiedRevision(0);
        node.setDeweyIDBytes(deweyIdBytes);
        yield node;
      }
      case XML_DOCUMENT -> {
        final XmlDocumentRootNode node = new XmlDocumentRootNode(nodeKey, 0, 0, 0, 0, hashFunction);
        node.setFirstChildKey(SlotLayoutAccessors.readLongField(data, layout, StructuralField.FIRST_CHILD_KEY));
        node.setLastChildKey(SlotLayoutAccessors.readLongField(data, layout, StructuralField.LAST_CHILD_KEY));
        node.setChildCount(SlotLayoutAccessors.readLongField(data, layout, StructuralField.CHILD_COUNT));
        node.setDescendantCount(SlotLayoutAccessors.readLongField(data, layout, StructuralField.DESCENDANT_COUNT));
        node.setHash(SlotLayoutAccessors.readLongField(data, layout, StructuralField.HASH));
        node.setDeweyIDBytes(deweyIdBytes);
        yield node;
      }
      case OBJECT -> {
        final ObjectNode node = new ObjectNode(nodeKey, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, hashFunction, deweyIdBytes);
        node.setParentKey(SlotLayoutAccessors.readLongField(data, layout, StructuralField.PARENT_KEY));
        node.setPreviousRevision(SlotLayoutAccessors.readIntField(data, layout, StructuralField.PREVIOUS_REVISION));
        node.setLastModifiedRevision(
            SlotLayoutAccessors.readIntField(data, layout, StructuralField.LAST_MODIFIED_REVISION));
        node.setRightSiblingKey(SlotLayoutAccessors.readLongField(data, layout, StructuralField.RIGHT_SIBLING_KEY));
        node.setLeftSiblingKey(SlotLayoutAccessors.readLongField(data, layout, StructuralField.LEFT_SIBLING_KEY));
        node.setFirstChildKey(SlotLayoutAccessors.readLongField(data, layout, StructuralField.FIRST_CHILD_KEY));
        node.setLastChildKey(SlotLayoutAccessors.readLongField(data, layout, StructuralField.LAST_CHILD_KEY));
        node.setChildCount(SlotLayoutAccessors.readLongField(data, layout, StructuralField.CHILD_COUNT));
        node.setDescendantCount(SlotLayoutAccessors.readLongField(data, layout, StructuralField.DESCENDANT_COUNT));
        node.setHash(SlotLayoutAccessors.readLongField(data, layout, StructuralField.HASH));
        yield node;
      }
      case ARRAY -> {
        final ArrayNode node = new ArrayNode(nodeKey, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, hashFunction, deweyIdBytes);
        node.setParentKey(SlotLayoutAccessors.readLongField(data, layout, StructuralField.PARENT_KEY));
        node.setPathNodeKey(SlotLayoutAccessors.readLongField(data, layout, StructuralField.PATH_NODE_KEY));
        node.setPreviousRevision(SlotLayoutAccessors.readIntField(data, layout, StructuralField.PREVIOUS_REVISION));
        node.setLastModifiedRevision(
            SlotLayoutAccessors.readIntField(data, layout, StructuralField.LAST_MODIFIED_REVISION));
        node.setRightSiblingKey(SlotLayoutAccessors.readLongField(data, layout, StructuralField.RIGHT_SIBLING_KEY));
        node.setLeftSiblingKey(SlotLayoutAccessors.readLongField(data, layout, StructuralField.LEFT_SIBLING_KEY));
        node.setFirstChildKey(SlotLayoutAccessors.readLongField(data, layout, StructuralField.FIRST_CHILD_KEY));
        node.setLastChildKey(SlotLayoutAccessors.readLongField(data, layout, StructuralField.LAST_CHILD_KEY));
        node.setChildCount(SlotLayoutAccessors.readLongField(data, layout, StructuralField.CHILD_COUNT));
        node.setDescendantCount(SlotLayoutAccessors.readLongField(data, layout, StructuralField.DESCENDANT_COUNT));
        node.setHash(SlotLayoutAccessors.readLongField(data, layout, StructuralField.HASH));
        yield node;
      }
      case OBJECT_KEY -> {
        final ObjectKeyNode node = new ObjectKeyNode(nodeKey, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, hashFunction, deweyIdBytes);
        node.setParentKey(SlotLayoutAccessors.readLongField(data, layout, StructuralField.PARENT_KEY));
        node.setPathNodeKey(SlotLayoutAccessors.readLongField(data, layout, StructuralField.PATH_NODE_KEY));
        node.setPreviousRevision(SlotLayoutAccessors.readIntField(data, layout, StructuralField.PREVIOUS_REVISION));
        node.setLastModifiedRevision(
            SlotLayoutAccessors.readIntField(data, layout, StructuralField.LAST_MODIFIED_REVISION));
        node.setRightSiblingKey(SlotLayoutAccessors.readLongField(data, layout, StructuralField.RIGHT_SIBLING_KEY));
        node.setLeftSiblingKey(SlotLayoutAccessors.readLongField(data, layout, StructuralField.LEFT_SIBLING_KEY));
        node.setFirstChildKey(SlotLayoutAccessors.readLongField(data, layout, StructuralField.FIRST_CHILD_KEY));
        node.setNameKey(SlotLayoutAccessors.readIntField(data, layout, StructuralField.NAME_KEY));
        node.clearCachedName();
        node.setDescendantCount(SlotLayoutAccessors.readLongField(data, layout, StructuralField.DESCENDANT_COUNT));
        node.setHash(SlotLayoutAccessors.readLongField(data, layout, StructuralField.HASH));
        yield node;
      }
      case BOOLEAN_VALUE -> {
        final BooleanNode node = new BooleanNode(nodeKey, 0, 0, 0, 0, 0, 0, false, hashFunction, deweyIdBytes);
        node.setParentKey(SlotLayoutAccessors.readLongField(data, layout, StructuralField.PARENT_KEY));
        node.setPreviousRevision(SlotLayoutAccessors.readIntField(data, layout, StructuralField.PREVIOUS_REVISION));
        node.setLastModifiedRevision(
            SlotLayoutAccessors.readIntField(data, layout, StructuralField.LAST_MODIFIED_REVISION));
        node.setRightSiblingKey(SlotLayoutAccessors.readLongField(data, layout, StructuralField.RIGHT_SIBLING_KEY));
        node.setLeftSiblingKey(SlotLayoutAccessors.readLongField(data, layout, StructuralField.LEFT_SIBLING_KEY));
        node.setHash(SlotLayoutAccessors.readLongField(data, layout, StructuralField.HASH));
        node.setValue(SlotLayoutAccessors.readBooleanField(data, layout, StructuralField.BOOLEAN_VALUE));
        yield node;
      }
      case NULL_VALUE -> {
        final NullNode node = new NullNode(nodeKey, 0, 0, 0, 0, 0, 0, hashFunction, deweyIdBytes);
        node.setParentKey(SlotLayoutAccessors.readLongField(data, layout, StructuralField.PARENT_KEY));
        node.setPreviousRevision(SlotLayoutAccessors.readIntField(data, layout, StructuralField.PREVIOUS_REVISION));
        node.setLastModifiedRevision(
            SlotLayoutAccessors.readIntField(data, layout, StructuralField.LAST_MODIFIED_REVISION));
        node.setRightSiblingKey(SlotLayoutAccessors.readLongField(data, layout, StructuralField.RIGHT_SIBLING_KEY));
        node.setLeftSiblingKey(SlotLayoutAccessors.readLongField(data, layout, StructuralField.LEFT_SIBLING_KEY));
        node.setHash(SlotLayoutAccessors.readLongField(data, layout, StructuralField.HASH));
        yield node;
      }
      case OBJECT_BOOLEAN_VALUE -> {
        final ObjectBooleanNode node = new ObjectBooleanNode(nodeKey, 0, 0, 0, 0, false, hashFunction, deweyIdBytes);
        node.setParentKey(SlotLayoutAccessors.readLongField(data, layout, StructuralField.PARENT_KEY));
        node.setPreviousRevision(SlotLayoutAccessors.readIntField(data, layout, StructuralField.PREVIOUS_REVISION));
        node.setLastModifiedRevision(
            SlotLayoutAccessors.readIntField(data, layout, StructuralField.LAST_MODIFIED_REVISION));
        node.setHash(SlotLayoutAccessors.readLongField(data, layout, StructuralField.HASH));
        node.setValue(SlotLayoutAccessors.readBooleanField(data, layout, StructuralField.BOOLEAN_VALUE));
        yield node;
      }
      case OBJECT_NULL_VALUE -> {
        final ObjectNullNode node = new ObjectNullNode(nodeKey, 0, 0, 0, 0, hashFunction, deweyIdBytes);
        node.setParentKey(SlotLayoutAccessors.readLongField(data, layout, StructuralField.PARENT_KEY));
        node.setPreviousRevision(SlotLayoutAccessors.readIntField(data, layout, StructuralField.PREVIOUS_REVISION));
        node.setLastModifiedRevision(
            SlotLayoutAccessors.readIntField(data, layout, StructuralField.LAST_MODIFIED_REVISION));
        node.setHash(SlotLayoutAccessors.readLongField(data, layout, StructuralField.HASH));
        yield node;
      }
      case NAMESPACE -> {
        final NamespaceNode node =
            new NamespaceNode(nodeKey, 0, 0, 0, 0, 0, 0, 0, 0, hashFunction, deweyIdBytes, EMPTY_QNM);
        node.setParentKey(SlotLayoutAccessors.readLongField(data, layout, StructuralField.PARENT_KEY));
        node.setPreviousRevision(SlotLayoutAccessors.readIntField(data, layout, StructuralField.PREVIOUS_REVISION));
        node.setLastModifiedRevision(
            SlotLayoutAccessors.readIntField(data, layout, StructuralField.LAST_MODIFIED_REVISION));
        node.setPathNodeKey(SlotLayoutAccessors.readLongField(data, layout, StructuralField.PATH_NODE_KEY));
        node.setPrefixKey(SlotLayoutAccessors.readIntField(data, layout, StructuralField.PREFIX_KEY));
        node.setLocalNameKey(SlotLayoutAccessors.readIntField(data, layout, StructuralField.LOCAL_NAME_KEY));
        node.setURIKey(SlotLayoutAccessors.readIntField(data, layout, StructuralField.URI_KEY));
        node.setHash(SlotLayoutAccessors.readLongField(data, layout, StructuralField.HASH));
        node.setName(EMPTY_QNM);
        yield node;
      }
      case ELEMENT -> {
        final ElementNode node = new ElementNode(nodeKey, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, hashFunction,
            deweyIdBytes, null, null, EMPTY_QNM);
        node.setParentKey(SlotLayoutAccessors.readLongField(data, layout, StructuralField.PARENT_KEY));
        node.setRightSiblingKey(SlotLayoutAccessors.readLongField(data, layout, StructuralField.RIGHT_SIBLING_KEY));
        node.setLeftSiblingKey(SlotLayoutAccessors.readLongField(data, layout, StructuralField.LEFT_SIBLING_KEY));
        node.setFirstChildKey(SlotLayoutAccessors.readLongField(data, layout, StructuralField.FIRST_CHILD_KEY));
        node.setLastChildKey(SlotLayoutAccessors.readLongField(data, layout, StructuralField.LAST_CHILD_KEY));
        node.setPathNodeKey(SlotLayoutAccessors.readLongField(data, layout, StructuralField.PATH_NODE_KEY));
        node.setPrefixKey(SlotLayoutAccessors.readIntField(data, layout, StructuralField.PREFIX_KEY));
        node.setLocalNameKey(SlotLayoutAccessors.readIntField(data, layout, StructuralField.LOCAL_NAME_KEY));
        node.setURIKey(SlotLayoutAccessors.readIntField(data, layout, StructuralField.URI_KEY));
        node.setPreviousRevision(SlotLayoutAccessors.readIntField(data, layout, StructuralField.PREVIOUS_REVISION));
        node.setLastModifiedRevision(
            SlotLayoutAccessors.readIntField(data, layout, StructuralField.LAST_MODIFIED_REVISION));
        node.setChildCount(SlotLayoutAccessors.readLongField(data, layout, StructuralField.CHILD_COUNT));
        node.setDescendantCount(SlotLayoutAccessors.readLongField(data, layout, StructuralField.DESCENDANT_COUNT));
        node.setHash(SlotLayoutAccessors.readLongField(data, layout, StructuralField.HASH));
        readInlineVectorPayload(node, data, layout, 0, true);
        readInlineVectorPayload(node, data, layout, 1, false);
        node.setName(EMPTY_QNM);
        yield node;
      }
      case STRING_VALUE -> {
        final StringNode node = new StringNode(nodeKey, 0, 0, 0, 0, 0, 0, new byte[0], hashFunction, deweyIdBytes);
        node.setParentKey(SlotLayoutAccessors.readLongField(data, layout, StructuralField.PARENT_KEY));
        node.setRightSiblingKey(SlotLayoutAccessors.readLongField(data, layout, StructuralField.RIGHT_SIBLING_KEY));
        node.setLeftSiblingKey(SlotLayoutAccessors.readLongField(data, layout, StructuralField.LEFT_SIBLING_KEY));
        node.setPreviousRevision(SlotLayoutAccessors.readIntField(data, layout, StructuralField.PREVIOUS_REVISION));
        node.setLastModifiedRevision(
            SlotLayoutAccessors.readIntField(data, layout, StructuralField.LAST_MODIFIED_REVISION));
        node.setHash(SlotLayoutAccessors.readLongField(data, layout, StructuralField.HASH));
        readInlineStringPayload(node, data, layout, 0);
        yield node;
      }
      case NUMBER_VALUE -> {
        final NumberNode node = new NumberNode(nodeKey, 0, 0, 0, 0, 0, 0, 0, hashFunction, deweyIdBytes);
        node.setParentKey(SlotLayoutAccessors.readLongField(data, layout, StructuralField.PARENT_KEY));
        node.setRightSiblingKey(SlotLayoutAccessors.readLongField(data, layout, StructuralField.RIGHT_SIBLING_KEY));
        node.setLeftSiblingKey(SlotLayoutAccessors.readLongField(data, layout, StructuralField.LEFT_SIBLING_KEY));
        node.setPreviousRevision(SlotLayoutAccessors.readIntField(data, layout, StructuralField.PREVIOUS_REVISION));
        node.setLastModifiedRevision(
            SlotLayoutAccessors.readIntField(data, layout, StructuralField.LAST_MODIFIED_REVISION));
        node.setHash(SlotLayoutAccessors.readLongField(data, layout, StructuralField.HASH));
        readInlineNumberPayload(node, data, layout, 0);
        yield node;
      }
      case OBJECT_STRING_VALUE -> {
        final ObjectStringNode node =
            new ObjectStringNode(nodeKey, 0, 0, 0, 0, new byte[0], hashFunction, deweyIdBytes);
        node.setParentKey(SlotLayoutAccessors.readLongField(data, layout, StructuralField.PARENT_KEY));
        node.setPreviousRevision(SlotLayoutAccessors.readIntField(data, layout, StructuralField.PREVIOUS_REVISION));
        node.setLastModifiedRevision(
            SlotLayoutAccessors.readIntField(data, layout, StructuralField.LAST_MODIFIED_REVISION));
        node.setHash(SlotLayoutAccessors.readLongField(data, layout, StructuralField.HASH));
        readInlineObjectStringPayload(node, data, layout, 0);
        yield node;
      }
      case OBJECT_NUMBER_VALUE -> {
        final ObjectNumberNode node = new ObjectNumberNode(nodeKey, 0, 0, 0, 0, 0, hashFunction, deweyIdBytes);
        node.setParentKey(SlotLayoutAccessors.readLongField(data, layout, StructuralField.PARENT_KEY));
        node.setPreviousRevision(SlotLayoutAccessors.readIntField(data, layout, StructuralField.PREVIOUS_REVISION));
        node.setLastModifiedRevision(
            SlotLayoutAccessors.readIntField(data, layout, StructuralField.LAST_MODIFIED_REVISION));
        node.setHash(SlotLayoutAccessors.readLongField(data, layout, StructuralField.HASH));
        readInlineNumberPayloadForObject(node, data, layout, 0);
        yield node;
      }
      case TEXT -> {
        final TextNode node = new TextNode(nodeKey, 0, 0, 0, 0, 0, 0, new byte[0], false, hashFunction, deweyIdBytes);
        node.setParentKey(SlotLayoutAccessors.readLongField(data, layout, StructuralField.PARENT_KEY));
        node.setRightSiblingKey(SlotLayoutAccessors.readLongField(data, layout, StructuralField.RIGHT_SIBLING_KEY));
        node.setLeftSiblingKey(SlotLayoutAccessors.readLongField(data, layout, StructuralField.LEFT_SIBLING_KEY));
        node.setPreviousRevision(SlotLayoutAccessors.readIntField(data, layout, StructuralField.PREVIOUS_REVISION));
        node.setLastModifiedRevision(
            SlotLayoutAccessors.readIntField(data, layout, StructuralField.LAST_MODIFIED_REVISION));
        // setRawValue BEFORE setHash — setRawValue resets hash to 0
        readInlineXmlValuePayload(node, data, layout, 0);
        node.setHash(SlotLayoutAccessors.readLongField(data, layout, StructuralField.HASH));
        yield node;
      }
      case COMMENT -> {
        final CommentNode node =
            new CommentNode(nodeKey, 0, 0, 0, 0, 0, 0, new byte[0], false, hashFunction, deweyIdBytes);
        node.setParentKey(SlotLayoutAccessors.readLongField(data, layout, StructuralField.PARENT_KEY));
        node.setRightSiblingKey(SlotLayoutAccessors.readLongField(data, layout, StructuralField.RIGHT_SIBLING_KEY));
        node.setLeftSiblingKey(SlotLayoutAccessors.readLongField(data, layout, StructuralField.LEFT_SIBLING_KEY));
        node.setPreviousRevision(SlotLayoutAccessors.readIntField(data, layout, StructuralField.PREVIOUS_REVISION));
        node.setLastModifiedRevision(
            SlotLayoutAccessors.readIntField(data, layout, StructuralField.LAST_MODIFIED_REVISION));
        // setRawValue BEFORE setHash — setRawValue resets hash to 0
        readInlineXmlValuePayload(node, data, layout, 0);
        node.setHash(SlotLayoutAccessors.readLongField(data, layout, StructuralField.HASH));
        yield node;
      }
      case ATTRIBUTE -> {
        final AttributeNode node =
            new AttributeNode(nodeKey, 0, 0, 0, 0, 0, 0, 0, 0, new byte[0], hashFunction, deweyIdBytes, EMPTY_QNM);
        node.setParentKey(SlotLayoutAccessors.readLongField(data, layout, StructuralField.PARENT_KEY));
        node.setPathNodeKey(SlotLayoutAccessors.readLongField(data, layout, StructuralField.PATH_NODE_KEY));
        node.setPrefixKey(SlotLayoutAccessors.readIntField(data, layout, StructuralField.PREFIX_KEY));
        node.setLocalNameKey(SlotLayoutAccessors.readIntField(data, layout, StructuralField.LOCAL_NAME_KEY));
        node.setURIKey(SlotLayoutAccessors.readIntField(data, layout, StructuralField.URI_KEY));
        node.setPreviousRevision(SlotLayoutAccessors.readIntField(data, layout, StructuralField.PREVIOUS_REVISION));
        node.setLastModifiedRevision(
            SlotLayoutAccessors.readIntField(data, layout, StructuralField.LAST_MODIFIED_REVISION));
        // setRawValue BEFORE setHash — setRawValue resets hash to 0
        readInlineXmlValuePayload(node, data, layout, 0);
        node.setHash(SlotLayoutAccessors.readLongField(data, layout, StructuralField.HASH));
        node.setName(EMPTY_QNM);
        yield node;
      }
      case PROCESSING_INSTRUCTION -> {
        final PINode node = new PINode(nodeKey, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, new byte[0], false,
            hashFunction, deweyIdBytes, EMPTY_QNM);
        node.setParentKey(SlotLayoutAccessors.readLongField(data, layout, StructuralField.PARENT_KEY));
        node.setRightSiblingKey(SlotLayoutAccessors.readLongField(data, layout, StructuralField.RIGHT_SIBLING_KEY));
        node.setLeftSiblingKey(SlotLayoutAccessors.readLongField(data, layout, StructuralField.LEFT_SIBLING_KEY));
        node.setFirstChildKey(SlotLayoutAccessors.readLongField(data, layout, StructuralField.FIRST_CHILD_KEY));
        node.setLastChildKey(SlotLayoutAccessors.readLongField(data, layout, StructuralField.LAST_CHILD_KEY));
        node.setPathNodeKey(SlotLayoutAccessors.readLongField(data, layout, StructuralField.PATH_NODE_KEY));
        node.setPrefixKey(SlotLayoutAccessors.readIntField(data, layout, StructuralField.PREFIX_KEY));
        node.setLocalNameKey(SlotLayoutAccessors.readIntField(data, layout, StructuralField.LOCAL_NAME_KEY));
        node.setURIKey(SlotLayoutAccessors.readIntField(data, layout, StructuralField.URI_KEY));
        node.setPreviousRevision(SlotLayoutAccessors.readIntField(data, layout, StructuralField.PREVIOUS_REVISION));
        node.setLastModifiedRevision(
            SlotLayoutAccessors.readIntField(data, layout, StructuralField.LAST_MODIFIED_REVISION));
        node.setChildCount(SlotLayoutAccessors.readLongField(data, layout, StructuralField.CHILD_COUNT));
        node.setDescendantCount(SlotLayoutAccessors.readLongField(data, layout, StructuralField.DESCENDANT_COUNT));
        // setRawValue BEFORE setHash — setRawValue resets hash to 0
        readInlineXmlValuePayload(node, data, layout, 0);
        node.setHash(SlotLayoutAccessors.readLongField(data, layout, StructuralField.HASH));
        node.setName(EMPTY_QNM);
        yield node;
      }
      default -> null;
    };
  }

  /**
   * Read inline string payload from the fixed-slot data and set it on the StringNode.
   */
  private static void readInlineStringPayload(final StringNode node, final MemorySegment data,
      final NodeKindLayout layout, final int payloadRefIndex) {
    final long pointer = SlotLayoutAccessors.readPayloadPointer(data, layout, payloadRefIndex);
    final int length = SlotLayoutAccessors.readPayloadLength(data, layout, payloadRefIndex);
    final int flags = SlotLayoutAccessors.readPayloadFlags(data, layout, payloadRefIndex);
    final boolean isCompressed = (flags & 1) != 0;

    final byte[] payloadBytes;
    if (length > 0) {
      payloadBytes = new byte[length];
      MemorySegment.copy(data, pointer, MemorySegment.ofArray(payloadBytes), 0, length);
    } else {
      payloadBytes = new byte[0];
    }
    node.setRawValue(payloadBytes, isCompressed, null);
  }

  /**
   * Read inline string payload from the fixed-slot data and set it on the ObjectStringNode.
   */
  private static void readInlineObjectStringPayload(final ObjectStringNode node, final MemorySegment data,
      final NodeKindLayout layout, final int payloadRefIndex) {
    final long pointer = SlotLayoutAccessors.readPayloadPointer(data, layout, payloadRefIndex);
    final int length = SlotLayoutAccessors.readPayloadLength(data, layout, payloadRefIndex);
    final int flags = SlotLayoutAccessors.readPayloadFlags(data, layout, payloadRefIndex);
    final boolean isCompressed = (flags & 1) != 0;

    final byte[] payloadBytes;
    if (length > 0) {
      payloadBytes = new byte[length];
      MemorySegment.copy(data, pointer, MemorySegment.ofArray(payloadBytes), 0, length);
    } else {
      payloadBytes = new byte[0];
    }
    node.setRawValue(payloadBytes, isCompressed, null);
  }

  /**
   * Read inline number payload from the fixed-slot data and set it on the NumberNode.
   */
  private static void readInlineNumberPayload(final NumberNode node, final MemorySegment data,
      final NodeKindLayout layout, final int payloadRefIndex) {
    final long pointer = SlotLayoutAccessors.readPayloadPointer(data, layout, payloadRefIndex);
    final int length = SlotLayoutAccessors.readPayloadLength(data, layout, payloadRefIndex);

    if (length > 0) {
      final MemorySegment payloadSlice = data.asSlice(pointer, length);
      final MemorySegmentBytesIn bytesIn = new MemorySegmentBytesIn(payloadSlice);
      final Number value = NodeKind.deserializeNumber(bytesIn);
      node.setValue(value);
    } else {
      node.setValue(0);
    }
  }

  /**
   * Read inline number payload from the fixed-slot data and set it on the ObjectNumberNode.
   */
  private static void readInlineNumberPayloadForObject(final ObjectNumberNode node, final MemorySegment data,
      final NodeKindLayout layout, final int payloadRefIndex) {
    final long pointer = SlotLayoutAccessors.readPayloadPointer(data, layout, payloadRefIndex);
    final int length = SlotLayoutAccessors.readPayloadLength(data, layout, payloadRefIndex);

    if (length > 0) {
      final MemorySegment payloadSlice = data.asSlice(pointer, length);
      final MemorySegmentBytesIn bytesIn = new MemorySegmentBytesIn(payloadSlice);
      final Number value = NodeKind.deserializeNumber(bytesIn);
      node.setValue(value);
    } else {
      node.setValue(0);
    }
  }

  /**
   * Read inline vector payload (attribute keys or namespace keys) from fixed-slot data and populate
   * the ElementNode's lists.
   *
   * <p>
   * Each vector entry is stored as an uncompressed 8-byte long in the inline payload area. The count
   * is derived from {@code length / Long.BYTES}.
   * </p>
   *
   * <p>
   * This overload uses zero-based addressing (for slices or materialize cold path).
   * </p>
   *
   * @param node the ElementNode to populate
   * @param data fixed-slot bytes
   * @param layout the layout descriptor
   * @param payloadRefIndex 0 for attributes, 1 for namespaces
   * @param isAttributes true to populate attribute keys, false for namespace keys
   */
  public static void readInlineVectorPayload(final ElementNode node, final MemorySegment data,
      final NodeKindLayout layout, final int payloadRefIndex, final boolean isAttributes) {
    readInlineVectorPayload(node, data, 0L, layout, payloadRefIndex, isAttributes);
  }

  /**
   * Read inline vector payload with explicit base offset (avoids asSlice allocation).
   *
   * @param node the ElementNode to populate
   * @param data the backing MemorySegment
   * @param dataOffset absolute byte offset where slot data begins
   * @param layout the layout descriptor
   * @param payloadRefIndex 0 for attributes, 1 for namespaces
   * @param isAttributes true to populate attribute keys, false for namespace keys
   */
  public static void readInlineVectorPayload(final ElementNode node, final MemorySegment data, final long dataOffset,
      final NodeKindLayout layout, final int payloadRefIndex, final boolean isAttributes) {
    final long pointer = SlotLayoutAccessors.readPayloadPointer(data, dataOffset, layout, payloadRefIndex);
    final int length = SlotLayoutAccessors.readPayloadLength(data, dataOffset, layout, payloadRefIndex);
    final int count = length / Long.BYTES;
    if (isAttributes) {
      node.clearAttributeKeys();
      for (int i = 0; i < count; i++) {
        node.insertAttribute(data.get(ValueLayout.JAVA_LONG_UNALIGNED, dataOffset + pointer + (long) i * Long.BYTES));
      }
    } else {
      node.clearNamespaceKeys();
      for (int i = 0; i < count; i++) {
        node.insertNamespace(data.get(ValueLayout.JAVA_LONG_UNALIGNED, dataOffset + pointer + (long) i * Long.BYTES));
      }
    }
  }

  /**
   * Read inline value payload from the fixed-slot data and set it on an XML ValueNode. Handles
   * TextNode, CommentNode, PINode, and AttributeNode. If the compressed flag is set, the node's
   * compressed state is updated accordingly.
   */
  private static void readInlineXmlValuePayload(final ValueNode node, final MemorySegment data,
      final NodeKindLayout layout, final int payloadRefIndex) {
    final long pointer = SlotLayoutAccessors.readPayloadPointer(data, layout, payloadRefIndex);
    final int length = SlotLayoutAccessors.readPayloadLength(data, layout, payloadRefIndex);
    final int flags = SlotLayoutAccessors.readPayloadFlags(data, layout, payloadRefIndex);
    final boolean isCompressed = (flags & 1) != 0;

    final byte[] payloadBytes;
    if (length > 0) {
      payloadBytes = new byte[length];
      MemorySegment.copy(data, pointer, MemorySegment.ofArray(payloadBytes), 0, length);
    } else {
      payloadBytes = new byte[0];
    }
    node.setRawValue(payloadBytes);
    if (isCompressed) {
      if (node instanceof TextNode tn) {
        tn.setCompressed(true);
      } else if (node instanceof CommentNode cn) {
        cn.setCompressed(true);
      } else if (node instanceof PINode pi) {
        pi.setCompressed(true);
      }
    }
  }

}
