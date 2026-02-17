package io.sirix.node.layout;

import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.node.BytesOut;
import io.sirix.node.DeltaVarIntCodec;
import io.sirix.node.NodeKind;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * Zero-allocation byte-level transformer from fixed-slot format to compact (delta-varint) format.
 *
 * <p>
 * Instead of materializing a DataRecord Java object and re-serializing it, this class reads
 * fixed-width fields at known offsets and writes the compact encoding directly. The output is
 * byte-identical to
 * {@code NodeKind.serialize(sink, FixedSlotRecordMaterializer.materialize(...), config)}.
 *
 * <p>
 * All field offsets are pre-computed as {@code static final} constants from {@link NodeKindLayouts}
 * at class-load time, so the JIT treats them as compile-time constants.
 */
public final class FixedToCompactTransformer {

  private static final ValueLayout.OfLong JAVA_LONG_UNALIGNED = ValueLayout.JAVA_LONG.withByteAlignment(1);
  private static final ValueLayout.OfInt JAVA_INT_UNALIGNED = ValueLayout.JAVA_INT.withByteAlignment(1);

  // ──────────────────────── JSON OBJECT ────────────────────────
  private static final NodeKindLayout L_OBJ = NodeKindLayouts.layoutFor(NodeKind.OBJECT);
  private static final int OBJ_PARENT = L_OBJ.offsetOfOrMinusOne(StructuralField.PARENT_KEY);
  private static final int OBJ_RSIB = L_OBJ.offsetOfOrMinusOne(StructuralField.RIGHT_SIBLING_KEY);
  private static final int OBJ_LSIB = L_OBJ.offsetOfOrMinusOne(StructuralField.LEFT_SIBLING_KEY);
  private static final int OBJ_FCHILD = L_OBJ.offsetOfOrMinusOne(StructuralField.FIRST_CHILD_KEY);
  private static final int OBJ_LCHILD = L_OBJ.offsetOfOrMinusOne(StructuralField.LAST_CHILD_KEY);
  private static final int OBJ_PREV_REV = L_OBJ.offsetOfOrMinusOne(StructuralField.PREVIOUS_REVISION);
  private static final int OBJ_LAST_MOD = L_OBJ.offsetOfOrMinusOne(StructuralField.LAST_MODIFIED_REVISION);
  private static final int OBJ_HASH = L_OBJ.offsetOfOrMinusOne(StructuralField.HASH);
  private static final int OBJ_CHILD_CNT = L_OBJ.offsetOfOrMinusOne(StructuralField.CHILD_COUNT);
  private static final int OBJ_DESC_CNT = L_OBJ.offsetOfOrMinusOne(StructuralField.DESCENDANT_COUNT);

  // ──────────────────────── JSON ARRAY ────────────────────────
  private static final NodeKindLayout L_ARR = NodeKindLayouts.layoutFor(NodeKind.ARRAY);
  private static final int ARR_PARENT = L_ARR.offsetOfOrMinusOne(StructuralField.PARENT_KEY);
  private static final int ARR_RSIB = L_ARR.offsetOfOrMinusOne(StructuralField.RIGHT_SIBLING_KEY);
  private static final int ARR_LSIB = L_ARR.offsetOfOrMinusOne(StructuralField.LEFT_SIBLING_KEY);
  private static final int ARR_FCHILD = L_ARR.offsetOfOrMinusOne(StructuralField.FIRST_CHILD_KEY);
  private static final int ARR_LCHILD = L_ARR.offsetOfOrMinusOne(StructuralField.LAST_CHILD_KEY);
  private static final int ARR_PATH = L_ARR.offsetOfOrMinusOne(StructuralField.PATH_NODE_KEY);
  private static final int ARR_PREV_REV = L_ARR.offsetOfOrMinusOne(StructuralField.PREVIOUS_REVISION);
  private static final int ARR_LAST_MOD = L_ARR.offsetOfOrMinusOne(StructuralField.LAST_MODIFIED_REVISION);
  private static final int ARR_HASH = L_ARR.offsetOfOrMinusOne(StructuralField.HASH);
  private static final int ARR_CHILD_CNT = L_ARR.offsetOfOrMinusOne(StructuralField.CHILD_COUNT);
  private static final int ARR_DESC_CNT = L_ARR.offsetOfOrMinusOne(StructuralField.DESCENDANT_COUNT);

  // ──────────────────────── JSON OBJECT_KEY ────────────────────────
  private static final NodeKindLayout L_OKEY = NodeKindLayouts.layoutFor(NodeKind.OBJECT_KEY);
  private static final int OKEY_PARENT = L_OKEY.offsetOfOrMinusOne(StructuralField.PARENT_KEY);
  private static final int OKEY_RSIB = L_OKEY.offsetOfOrMinusOne(StructuralField.RIGHT_SIBLING_KEY);
  private static final int OKEY_LSIB = L_OKEY.offsetOfOrMinusOne(StructuralField.LEFT_SIBLING_KEY);
  private static final int OKEY_FCHILD = L_OKEY.offsetOfOrMinusOne(StructuralField.FIRST_CHILD_KEY);
  private static final int OKEY_PATH = L_OKEY.offsetOfOrMinusOne(StructuralField.PATH_NODE_KEY);
  private static final int OKEY_NAME = L_OKEY.offsetOfOrMinusOne(StructuralField.NAME_KEY);
  private static final int OKEY_PREV_REV = L_OKEY.offsetOfOrMinusOne(StructuralField.PREVIOUS_REVISION);
  private static final int OKEY_LAST_MOD = L_OKEY.offsetOfOrMinusOne(StructuralField.LAST_MODIFIED_REVISION);
  private static final int OKEY_HASH = L_OKEY.offsetOfOrMinusOne(StructuralField.HASH);
  private static final int OKEY_DESC_CNT = L_OKEY.offsetOfOrMinusOne(StructuralField.DESCENDANT_COUNT);

  // ──────────────────────── JSON STRING_VALUE ────────────────────────
  private static final NodeKindLayout L_STR = NodeKindLayouts.layoutFor(NodeKind.STRING_VALUE);
  private static final int STR_PARENT = L_STR.offsetOfOrMinusOne(StructuralField.PARENT_KEY);
  private static final int STR_RSIB = L_STR.offsetOfOrMinusOne(StructuralField.RIGHT_SIBLING_KEY);
  private static final int STR_LSIB = L_STR.offsetOfOrMinusOne(StructuralField.LEFT_SIBLING_KEY);
  private static final int STR_PREV_REV = L_STR.offsetOfOrMinusOne(StructuralField.PREVIOUS_REVISION);
  private static final int STR_LAST_MOD = L_STR.offsetOfOrMinusOne(StructuralField.LAST_MODIFIED_REVISION);
  private static final int STR_HASH = L_STR.offsetOfOrMinusOne(StructuralField.HASH);
  private static final int STR_PR0_PTR = L_STR.payloadRef(0).pointerOffset();
  private static final int STR_PR0_LEN = L_STR.payloadRef(0).lengthOffset();
  private static final int STR_PR0_FLAGS = L_STR.payloadRef(0).flagsOffset();

  // ──────────────────────── JSON NUMBER_VALUE ────────────────────────
  private static final NodeKindLayout L_NUM = NodeKindLayouts.layoutFor(NodeKind.NUMBER_VALUE);
  private static final int NUM_PARENT = L_NUM.offsetOfOrMinusOne(StructuralField.PARENT_KEY);
  private static final int NUM_RSIB = L_NUM.offsetOfOrMinusOne(StructuralField.RIGHT_SIBLING_KEY);
  private static final int NUM_LSIB = L_NUM.offsetOfOrMinusOne(StructuralField.LEFT_SIBLING_KEY);
  private static final int NUM_PREV_REV = L_NUM.offsetOfOrMinusOne(StructuralField.PREVIOUS_REVISION);
  private static final int NUM_LAST_MOD = L_NUM.offsetOfOrMinusOne(StructuralField.LAST_MODIFIED_REVISION);
  private static final int NUM_HASH = L_NUM.offsetOfOrMinusOne(StructuralField.HASH);
  private static final int NUM_PR0_PTR = L_NUM.payloadRef(0).pointerOffset();
  private static final int NUM_PR0_LEN = L_NUM.payloadRef(0).lengthOffset();

  // ──────────────────────── JSON BOOLEAN_VALUE ────────────────────────
  private static final NodeKindLayout L_BOOL = NodeKindLayouts.layoutFor(NodeKind.BOOLEAN_VALUE);
  private static final int BOOL_PARENT = L_BOOL.offsetOfOrMinusOne(StructuralField.PARENT_KEY);
  private static final int BOOL_RSIB = L_BOOL.offsetOfOrMinusOne(StructuralField.RIGHT_SIBLING_KEY);
  private static final int BOOL_LSIB = L_BOOL.offsetOfOrMinusOne(StructuralField.LEFT_SIBLING_KEY);
  private static final int BOOL_PREV_REV = L_BOOL.offsetOfOrMinusOne(StructuralField.PREVIOUS_REVISION);
  private static final int BOOL_LAST_MOD = L_BOOL.offsetOfOrMinusOne(StructuralField.LAST_MODIFIED_REVISION);
  private static final int BOOL_HASH = L_BOOL.offsetOfOrMinusOne(StructuralField.HASH);
  private static final int BOOL_VAL = L_BOOL.offsetOfOrMinusOne(StructuralField.BOOLEAN_VALUE);

  // ──────────────────────── JSON NULL_VALUE ────────────────────────
  private static final NodeKindLayout L_NULL = NodeKindLayouts.layoutFor(NodeKind.NULL_VALUE);
  private static final int NULL_PARENT = L_NULL.offsetOfOrMinusOne(StructuralField.PARENT_KEY);
  private static final int NULL_RSIB = L_NULL.offsetOfOrMinusOne(StructuralField.RIGHT_SIBLING_KEY);
  private static final int NULL_LSIB = L_NULL.offsetOfOrMinusOne(StructuralField.LEFT_SIBLING_KEY);
  private static final int NULL_PREV_REV = L_NULL.offsetOfOrMinusOne(StructuralField.PREVIOUS_REVISION);
  private static final int NULL_LAST_MOD = L_NULL.offsetOfOrMinusOne(StructuralField.LAST_MODIFIED_REVISION);
  private static final int NULL_HASH = L_NULL.offsetOfOrMinusOne(StructuralField.HASH);

  // ──────────────────────── JSON OBJECT_STRING_VALUE ────────────────────────
  private static final NodeKindLayout L_OSTR = NodeKindLayouts.layoutFor(NodeKind.OBJECT_STRING_VALUE);
  private static final int OSTR_PARENT = L_OSTR.offsetOfOrMinusOne(StructuralField.PARENT_KEY);
  private static final int OSTR_PREV_REV = L_OSTR.offsetOfOrMinusOne(StructuralField.PREVIOUS_REVISION);
  private static final int OSTR_LAST_MOD = L_OSTR.offsetOfOrMinusOne(StructuralField.LAST_MODIFIED_REVISION);
  private static final int OSTR_HASH = L_OSTR.offsetOfOrMinusOne(StructuralField.HASH);
  private static final int OSTR_PR0_PTR = L_OSTR.payloadRef(0).pointerOffset();
  private static final int OSTR_PR0_LEN = L_OSTR.payloadRef(0).lengthOffset();
  private static final int OSTR_PR0_FLAGS = L_OSTR.payloadRef(0).flagsOffset();

  // ──────────────────────── JSON OBJECT_NUMBER_VALUE ────────────────────────
  private static final NodeKindLayout L_ONUM = NodeKindLayouts.layoutFor(NodeKind.OBJECT_NUMBER_VALUE);
  private static final int ONUM_PARENT = L_ONUM.offsetOfOrMinusOne(StructuralField.PARENT_KEY);
  private static final int ONUM_PREV_REV = L_ONUM.offsetOfOrMinusOne(StructuralField.PREVIOUS_REVISION);
  private static final int ONUM_LAST_MOD = L_ONUM.offsetOfOrMinusOne(StructuralField.LAST_MODIFIED_REVISION);
  private static final int ONUM_HASH = L_ONUM.offsetOfOrMinusOne(StructuralField.HASH);
  private static final int ONUM_PR0_PTR = L_ONUM.payloadRef(0).pointerOffset();
  private static final int ONUM_PR0_LEN = L_ONUM.payloadRef(0).lengthOffset();

  // ──────────────────────── JSON OBJECT_BOOLEAN_VALUE ────────────────────────
  private static final NodeKindLayout L_OBOOL = NodeKindLayouts.layoutFor(NodeKind.OBJECT_BOOLEAN_VALUE);
  private static final int OBOOL_PARENT = L_OBOOL.offsetOfOrMinusOne(StructuralField.PARENT_KEY);
  private static final int OBOOL_PREV_REV = L_OBOOL.offsetOfOrMinusOne(StructuralField.PREVIOUS_REVISION);
  private static final int OBOOL_LAST_MOD = L_OBOOL.offsetOfOrMinusOne(StructuralField.LAST_MODIFIED_REVISION);
  private static final int OBOOL_HASH = L_OBOOL.offsetOfOrMinusOne(StructuralField.HASH);
  private static final int OBOOL_VAL = L_OBOOL.offsetOfOrMinusOne(StructuralField.BOOLEAN_VALUE);

  // ──────────────────────── JSON OBJECT_NULL_VALUE ────────────────────────
  private static final NodeKindLayout L_ONULL = NodeKindLayouts.layoutFor(NodeKind.OBJECT_NULL_VALUE);
  private static final int ONULL_PARENT = L_ONULL.offsetOfOrMinusOne(StructuralField.PARENT_KEY);
  private static final int ONULL_PREV_REV = L_ONULL.offsetOfOrMinusOne(StructuralField.PREVIOUS_REVISION);
  private static final int ONULL_LAST_MOD = L_ONULL.offsetOfOrMinusOne(StructuralField.LAST_MODIFIED_REVISION);
  private static final int ONULL_HASH = L_ONULL.offsetOfOrMinusOne(StructuralField.HASH);

  // ──────────────────────── JSON_DOCUMENT ────────────────────────
  private static final NodeKindLayout L_JDOC = NodeKindLayouts.layoutFor(NodeKind.JSON_DOCUMENT);
  private static final int JDOC_FCHILD = L_JDOC.offsetOfOrMinusOne(StructuralField.FIRST_CHILD_KEY);
  private static final int JDOC_DESC_CNT = L_JDOC.offsetOfOrMinusOne(StructuralField.DESCENDANT_COUNT);

  // ──────────────────────── XML_DOCUMENT ────────────────────────
  private static final NodeKindLayout L_XDOC = NodeKindLayouts.layoutFor(NodeKind.XML_DOCUMENT);
  private static final int XDOC_FCHILD = L_XDOC.offsetOfOrMinusOne(StructuralField.FIRST_CHILD_KEY);
  private static final int XDOC_HASH = L_XDOC.offsetOfOrMinusOne(StructuralField.HASH);
  private static final int XDOC_DESC_CNT = L_XDOC.offsetOfOrMinusOne(StructuralField.DESCENDANT_COUNT);

  // ──────────────────────── XML ELEMENT ────────────────────────
  private static final NodeKindLayout L_ELEM = NodeKindLayouts.layoutFor(NodeKind.ELEMENT);
  private static final int ELEM_PARENT = L_ELEM.offsetOfOrMinusOne(StructuralField.PARENT_KEY);
  private static final int ELEM_RSIB = L_ELEM.offsetOfOrMinusOne(StructuralField.RIGHT_SIBLING_KEY);
  private static final int ELEM_LSIB = L_ELEM.offsetOfOrMinusOne(StructuralField.LEFT_SIBLING_KEY);
  private static final int ELEM_FCHILD = L_ELEM.offsetOfOrMinusOne(StructuralField.FIRST_CHILD_KEY);
  private static final int ELEM_LCHILD = L_ELEM.offsetOfOrMinusOne(StructuralField.LAST_CHILD_KEY);
  private static final int ELEM_PATH = L_ELEM.offsetOfOrMinusOne(StructuralField.PATH_NODE_KEY);
  private static final int ELEM_PREFIX = L_ELEM.offsetOfOrMinusOne(StructuralField.PREFIX_KEY);
  private static final int ELEM_LNAME = L_ELEM.offsetOfOrMinusOne(StructuralField.LOCAL_NAME_KEY);
  private static final int ELEM_URI = L_ELEM.offsetOfOrMinusOne(StructuralField.URI_KEY);
  private static final int ELEM_PREV_REV = L_ELEM.offsetOfOrMinusOne(StructuralField.PREVIOUS_REVISION);
  private static final int ELEM_LAST_MOD = L_ELEM.offsetOfOrMinusOne(StructuralField.LAST_MODIFIED_REVISION);
  private static final int ELEM_HASH = L_ELEM.offsetOfOrMinusOne(StructuralField.HASH);
  private static final int ELEM_CHILD_CNT = L_ELEM.offsetOfOrMinusOne(StructuralField.CHILD_COUNT);
  private static final int ELEM_DESC_CNT = L_ELEM.offsetOfOrMinusOne(StructuralField.DESCENDANT_COUNT);
  private static final int ELEM_ATTR_PR_PTR = L_ELEM.payloadRef(0).pointerOffset();
  private static final int ELEM_ATTR_PR_LEN = L_ELEM.payloadRef(0).lengthOffset();
  private static final int ELEM_NS_PR_PTR = L_ELEM.payloadRef(1).pointerOffset();
  private static final int ELEM_NS_PR_LEN = L_ELEM.payloadRef(1).lengthOffset();

  // ──────────────────────── XML ATTRIBUTE ────────────────────────
  private static final NodeKindLayout L_ATTR = NodeKindLayouts.layoutFor(NodeKind.ATTRIBUTE);
  private static final int ATTR_PARENT = L_ATTR.offsetOfOrMinusOne(StructuralField.PARENT_KEY);
  private static final int ATTR_PATH = L_ATTR.offsetOfOrMinusOne(StructuralField.PATH_NODE_KEY);
  private static final int ATTR_PREFIX = L_ATTR.offsetOfOrMinusOne(StructuralField.PREFIX_KEY);
  private static final int ATTR_LNAME = L_ATTR.offsetOfOrMinusOne(StructuralField.LOCAL_NAME_KEY);
  private static final int ATTR_URI = L_ATTR.offsetOfOrMinusOne(StructuralField.URI_KEY);
  private static final int ATTR_PREV_REV = L_ATTR.offsetOfOrMinusOne(StructuralField.PREVIOUS_REVISION);
  private static final int ATTR_LAST_MOD = L_ATTR.offsetOfOrMinusOne(StructuralField.LAST_MODIFIED_REVISION);
  private static final int ATTR_PR0_PTR = L_ATTR.payloadRef(0).pointerOffset();
  private static final int ATTR_PR0_LEN = L_ATTR.payloadRef(0).lengthOffset();

  // ──────────────────────── XML NAMESPACE ────────────────────────
  private static final NodeKindLayout L_NS = NodeKindLayouts.layoutFor(NodeKind.NAMESPACE);
  private static final int NS_PARENT = L_NS.offsetOfOrMinusOne(StructuralField.PARENT_KEY);
  private static final int NS_PATH = L_NS.offsetOfOrMinusOne(StructuralField.PATH_NODE_KEY);
  private static final int NS_PREFIX = L_NS.offsetOfOrMinusOne(StructuralField.PREFIX_KEY);
  private static final int NS_LNAME = L_NS.offsetOfOrMinusOne(StructuralField.LOCAL_NAME_KEY);
  private static final int NS_URI = L_NS.offsetOfOrMinusOne(StructuralField.URI_KEY);
  private static final int NS_PREV_REV = L_NS.offsetOfOrMinusOne(StructuralField.PREVIOUS_REVISION);
  private static final int NS_LAST_MOD = L_NS.offsetOfOrMinusOne(StructuralField.LAST_MODIFIED_REVISION);

  // ──────────────────────── XML TEXT ────────────────────────
  private static final NodeKindLayout L_TEXT = NodeKindLayouts.layoutFor(NodeKind.TEXT);
  private static final int TEXT_PARENT = L_TEXT.offsetOfOrMinusOne(StructuralField.PARENT_KEY);
  private static final int TEXT_RSIB = L_TEXT.offsetOfOrMinusOne(StructuralField.RIGHT_SIBLING_KEY);
  private static final int TEXT_LSIB = L_TEXT.offsetOfOrMinusOne(StructuralField.LEFT_SIBLING_KEY);
  private static final int TEXT_PREV_REV = L_TEXT.offsetOfOrMinusOne(StructuralField.PREVIOUS_REVISION);
  private static final int TEXT_LAST_MOD = L_TEXT.offsetOfOrMinusOne(StructuralField.LAST_MODIFIED_REVISION);
  private static final int TEXT_PR0_PTR = L_TEXT.payloadRef(0).pointerOffset();
  private static final int TEXT_PR0_LEN = L_TEXT.payloadRef(0).lengthOffset();
  private static final int TEXT_PR0_FLAGS = L_TEXT.payloadRef(0).flagsOffset();

  // ──────────────────────── XML COMMENT ────────────────────────
  private static final NodeKindLayout L_CMT = NodeKindLayouts.layoutFor(NodeKind.COMMENT);
  private static final int CMT_PARENT = L_CMT.offsetOfOrMinusOne(StructuralField.PARENT_KEY);
  private static final int CMT_RSIB = L_CMT.offsetOfOrMinusOne(StructuralField.RIGHT_SIBLING_KEY);
  private static final int CMT_LSIB = L_CMT.offsetOfOrMinusOne(StructuralField.LEFT_SIBLING_KEY);
  private static final int CMT_PREV_REV = L_CMT.offsetOfOrMinusOne(StructuralField.PREVIOUS_REVISION);
  private static final int CMT_LAST_MOD = L_CMT.offsetOfOrMinusOne(StructuralField.LAST_MODIFIED_REVISION);
  private static final int CMT_PR0_PTR = L_CMT.payloadRef(0).pointerOffset();
  private static final int CMT_PR0_LEN = L_CMT.payloadRef(0).lengthOffset();
  private static final int CMT_PR0_FLAGS = L_CMT.payloadRef(0).flagsOffset();

  // ──────────────────────── XML PROCESSING_INSTRUCTION ────────────────────────
  private static final NodeKindLayout L_PI = NodeKindLayouts.layoutFor(NodeKind.PROCESSING_INSTRUCTION);
  private static final int PI_PARENT = L_PI.offsetOfOrMinusOne(StructuralField.PARENT_KEY);
  private static final int PI_RSIB = L_PI.offsetOfOrMinusOne(StructuralField.RIGHT_SIBLING_KEY);
  private static final int PI_LSIB = L_PI.offsetOfOrMinusOne(StructuralField.LEFT_SIBLING_KEY);
  private static final int PI_FCHILD = L_PI.offsetOfOrMinusOne(StructuralField.FIRST_CHILD_KEY);
  private static final int PI_LCHILD = L_PI.offsetOfOrMinusOne(StructuralField.LAST_CHILD_KEY);
  private static final int PI_PATH = L_PI.offsetOfOrMinusOne(StructuralField.PATH_NODE_KEY);
  private static final int PI_PREFIX = L_PI.offsetOfOrMinusOne(StructuralField.PREFIX_KEY);
  private static final int PI_LNAME = L_PI.offsetOfOrMinusOne(StructuralField.LOCAL_NAME_KEY);
  private static final int PI_URI = L_PI.offsetOfOrMinusOne(StructuralField.URI_KEY);
  private static final int PI_PREV_REV = L_PI.offsetOfOrMinusOne(StructuralField.PREVIOUS_REVISION);
  private static final int PI_LAST_MOD = L_PI.offsetOfOrMinusOne(StructuralField.LAST_MODIFIED_REVISION);
  private static final int PI_HASH = L_PI.offsetOfOrMinusOne(StructuralField.HASH);
  private static final int PI_CHILD_CNT = L_PI.offsetOfOrMinusOne(StructuralField.CHILD_COUNT);
  private static final int PI_DESC_CNT = L_PI.offsetOfOrMinusOne(StructuralField.DESCENDANT_COUNT);
  private static final int PI_PR0_PTR = L_PI.payloadRef(0).pointerOffset();
  private static final int PI_PR0_LEN = L_PI.payloadRef(0).lengthOffset();
  private static final int PI_PR0_FLAGS = L_PI.payloadRef(0).flagsOffset();

  private FixedToCompactTransformer() {}

  /**
   * Transform a fixed-slot record to compact format, writing the result (including the leading
   * NodeKind id byte) into {@code sink}.
   *
   * <p>
   * The output is byte-identical to
   * {@code NodeSerializerImpl.serialize(sink, FixedSlotRecordMaterializer.materialize(...), config)}.
   *
   * @param nodeKind the kind of node stored in this slot
   * @param nodeKey the absolute node key
   * @param slot the fixed-slot bytes (zero-based, length = fixed slot size)
   * @param config the resource configuration
   * @param sink the output sink to write compact bytes into
   */
  public static void transform(final NodeKind nodeKind, final long nodeKey, final MemorySegment slot,
      final ResourceConfiguration config, final BytesOut<?> sink) {
    // Write the NodeKind id byte (same as NodeSerializerImpl.serialize)
    sink.writeByte(nodeKind.getId());

    switch (nodeKind) {
      // ── JSON structural ──
      case OBJECT -> transformObject(nodeKey, slot, config, sink);
      case ARRAY -> transformArray(nodeKey, slot, config, sink);
      case OBJECT_KEY -> transformObjectKey(nodeKey, slot, config, sink);
      // ── JSON value (with siblings) ──
      case STRING_VALUE -> transformStringValue(nodeKey, slot, config, sink);
      case NUMBER_VALUE -> transformNumberValue(nodeKey, slot, config, sink);
      case BOOLEAN_VALUE -> transformBooleanValue(nodeKey, slot, config, sink);
      case NULL_VALUE -> transformNullValue(nodeKey, slot, config, sink);
      // ── JSON value (object-property, no siblings) ──
      case OBJECT_STRING_VALUE -> transformObjectStringValue(nodeKey, slot, config, sink);
      case OBJECT_NUMBER_VALUE -> transformObjectNumberValue(nodeKey, slot, config, sink);
      case OBJECT_BOOLEAN_VALUE -> transformObjectBooleanValue(nodeKey, slot, config, sink);
      case OBJECT_NULL_VALUE -> transformObjectNullValue(nodeKey, slot, config, sink);
      // ── JSON document ──
      case JSON_DOCUMENT -> transformJsonDocument(nodeKey, slot, sink);
      // ── XML ──
      case XML_DOCUMENT -> transformXmlDocument(nodeKey, slot, config, sink);
      case ELEMENT -> transformElement(nodeKey, slot, config, sink);
      case ATTRIBUTE -> transformAttribute(nodeKey, slot, sink);
      case NAMESPACE -> transformNamespace(nodeKey, slot, sink);
      case TEXT -> transformText(nodeKey, slot, sink);
      case COMMENT -> transformComment(nodeKey, slot, sink);
      case PROCESSING_INSTRUCTION -> transformProcessingInstruction(nodeKey, slot, config, sink);
      default ->
        throw new UnsupportedOperationException("FixedToCompactTransformer does not support node kind: " + nodeKind);
    }
  }

  // ════════════════════════════ JSON STRUCTURAL ════════════════════════════

  private static void transformObject(final long nodeKey, final MemorySegment slot, final ResourceConfiguration config,
      final BytesOut<?> sink) {
    DeltaVarIntCodec.encodeDelta(sink, slot.get(JAVA_LONG_UNALIGNED, OBJ_PARENT), nodeKey);
    DeltaVarIntCodec.encodeDelta(sink, slot.get(JAVA_LONG_UNALIGNED, OBJ_RSIB), nodeKey);
    DeltaVarIntCodec.encodeDelta(sink, slot.get(JAVA_LONG_UNALIGNED, OBJ_LSIB), nodeKey);
    DeltaVarIntCodec.encodeDelta(sink, slot.get(JAVA_LONG_UNALIGNED, OBJ_FCHILD), nodeKey);
    DeltaVarIntCodec.encodeDelta(sink, slot.get(JAVA_LONG_UNALIGNED, OBJ_LCHILD), nodeKey);
    DeltaVarIntCodec.encodeSigned(sink, slot.get(JAVA_INT_UNALIGNED, OBJ_PREV_REV));
    DeltaVarIntCodec.encodeSigned(sink, slot.get(JAVA_INT_UNALIGNED, OBJ_LAST_MOD));
    if (config.storeChildCount()) {
      DeltaVarIntCodec.encodeSigned(sink, (int) slot.get(JAVA_LONG_UNALIGNED, OBJ_CHILD_CNT));
    }
    if (config.hashType != HashType.NONE) {
      sink.writeLong(slot.get(JAVA_LONG_UNALIGNED, OBJ_HASH));
      DeltaVarIntCodec.encodeSigned(sink, (int) slot.get(JAVA_LONG_UNALIGNED, OBJ_DESC_CNT));
    }
  }

  private static void transformArray(final long nodeKey, final MemorySegment slot, final ResourceConfiguration config,
      final BytesOut<?> sink) {
    DeltaVarIntCodec.encodeDelta(sink, slot.get(JAVA_LONG_UNALIGNED, ARR_PARENT), nodeKey);
    DeltaVarIntCodec.encodeDelta(sink, slot.get(JAVA_LONG_UNALIGNED, ARR_RSIB), nodeKey);
    DeltaVarIntCodec.encodeDelta(sink, slot.get(JAVA_LONG_UNALIGNED, ARR_LSIB), nodeKey);
    DeltaVarIntCodec.encodeDelta(sink, slot.get(JAVA_LONG_UNALIGNED, ARR_FCHILD), nodeKey);
    DeltaVarIntCodec.encodeDelta(sink, slot.get(JAVA_LONG_UNALIGNED, ARR_LCHILD), nodeKey);
    DeltaVarIntCodec.encodeDelta(sink, slot.get(JAVA_LONG_UNALIGNED, ARR_PATH), nodeKey);
    DeltaVarIntCodec.encodeSigned(sink, slot.get(JAVA_INT_UNALIGNED, ARR_PREV_REV));
    DeltaVarIntCodec.encodeSigned(sink, slot.get(JAVA_INT_UNALIGNED, ARR_LAST_MOD));
    if (config.storeChildCount()) {
      DeltaVarIntCodec.encodeSigned(sink, (int) slot.get(JAVA_LONG_UNALIGNED, ARR_CHILD_CNT));
    }
    if (config.hashType != HashType.NONE) {
      sink.writeLong(slot.get(JAVA_LONG_UNALIGNED, ARR_HASH));
      DeltaVarIntCodec.encodeSigned(sink, (int) slot.get(JAVA_LONG_UNALIGNED, ARR_DESC_CNT));
    }
  }

  private static void transformObjectKey(final long nodeKey, final MemorySegment slot,
      final ResourceConfiguration config, final BytesOut<?> sink) {
    DeltaVarIntCodec.encodeDelta(sink, slot.get(JAVA_LONG_UNALIGNED, OKEY_PARENT), nodeKey);
    DeltaVarIntCodec.encodeDelta(sink, slot.get(JAVA_LONG_UNALIGNED, OKEY_RSIB), nodeKey);
    DeltaVarIntCodec.encodeDelta(sink, slot.get(JAVA_LONG_UNALIGNED, OKEY_LSIB), nodeKey);
    DeltaVarIntCodec.encodeDelta(sink, slot.get(JAVA_LONG_UNALIGNED, OKEY_FCHILD), nodeKey);
    // LAST_CHILD_KEY is in the fixed layout but NOT written in compact format
    DeltaVarIntCodec.encodeSigned(sink, slot.get(JAVA_INT_UNALIGNED, OKEY_NAME));
    DeltaVarIntCodec.encodeDelta(sink, slot.get(JAVA_LONG_UNALIGNED, OKEY_PATH), nodeKey);
    DeltaVarIntCodec.encodeSigned(sink, slot.get(JAVA_INT_UNALIGNED, OKEY_PREV_REV));
    DeltaVarIntCodec.encodeSigned(sink, slot.get(JAVA_INT_UNALIGNED, OKEY_LAST_MOD));
    if (config.hashType != HashType.NONE) {
      sink.writeLong(slot.get(JAVA_LONG_UNALIGNED, OKEY_HASH));
      DeltaVarIntCodec.encodeSigned(sink, (int) slot.get(JAVA_LONG_UNALIGNED, OKEY_DESC_CNT));
    }
  }

  // ════════════════════════════ JSON VALUE (WITH SIBLINGS) ════════════════════════════

  private static void transformStringValue(final long nodeKey, final MemorySegment slot,
      final ResourceConfiguration config, final BytesOut<?> sink) {
    DeltaVarIntCodec.encodeDelta(sink, slot.get(JAVA_LONG_UNALIGNED, STR_PARENT), nodeKey);
    DeltaVarIntCodec.encodeDelta(sink, slot.get(JAVA_LONG_UNALIGNED, STR_RSIB), nodeKey);
    DeltaVarIntCodec.encodeDelta(sink, slot.get(JAVA_LONG_UNALIGNED, STR_LSIB), nodeKey);
    DeltaVarIntCodec.encodeSigned(sink, slot.get(JAVA_INT_UNALIGNED, STR_PREV_REV));
    DeltaVarIntCodec.encodeSigned(sink, slot.get(JAVA_INT_UNALIGNED, STR_LAST_MOD));
    if (config.hashType != HashType.NONE) {
      sink.writeLong(slot.get(JAVA_LONG_UNALIGNED, STR_HASH));
    }
    // Compression flag + raw value bytes
    final int flags = slot.get(JAVA_INT_UNALIGNED, STR_PR0_FLAGS);
    sink.writeByte((byte) (flags & 1));
    final long pointer = slot.get(JAVA_LONG_UNALIGNED, STR_PR0_PTR);
    final int length = slot.get(JAVA_INT_UNALIGNED, STR_PR0_LEN);
    DeltaVarIntCodec.encodeSigned(sink, length);
    if (length > 0) {
      sink.writeSegment(slot, pointer, length);
    }
  }

  private static void transformNumberValue(final long nodeKey, final MemorySegment slot,
      final ResourceConfiguration config, final BytesOut<?> sink) {
    DeltaVarIntCodec.encodeDelta(sink, slot.get(JAVA_LONG_UNALIGNED, NUM_PARENT), nodeKey);
    DeltaVarIntCodec.encodeDelta(sink, slot.get(JAVA_LONG_UNALIGNED, NUM_RSIB), nodeKey);
    DeltaVarIntCodec.encodeDelta(sink, slot.get(JAVA_LONG_UNALIGNED, NUM_LSIB), nodeKey);
    DeltaVarIntCodec.encodeSigned(sink, slot.get(JAVA_INT_UNALIGNED, NUM_PREV_REV));
    DeltaVarIntCodec.encodeSigned(sink, slot.get(JAVA_INT_UNALIGNED, NUM_LAST_MOD));
    if (config.hashType != HashType.NONE) {
      sink.writeLong(slot.get(JAVA_LONG_UNALIGNED, NUM_HASH));
    }
    // Number payload: already in serialized format (type byte + encoded value)
    final long pointer = slot.get(JAVA_LONG_UNALIGNED, NUM_PR0_PTR);
    final int length = slot.get(JAVA_INT_UNALIGNED, NUM_PR0_LEN);
    if (length > 0) {
      sink.writeSegment(slot, pointer, length);
    }
  }

  private static void transformBooleanValue(final long nodeKey, final MemorySegment slot,
      final ResourceConfiguration config, final BytesOut<?> sink) {
    DeltaVarIntCodec.encodeDelta(sink, slot.get(JAVA_LONG_UNALIGNED, BOOL_PARENT), nodeKey);
    DeltaVarIntCodec.encodeDelta(sink, slot.get(JAVA_LONG_UNALIGNED, BOOL_RSIB), nodeKey);
    DeltaVarIntCodec.encodeDelta(sink, slot.get(JAVA_LONG_UNALIGNED, BOOL_LSIB), nodeKey);
    DeltaVarIntCodec.encodeSigned(sink, slot.get(JAVA_INT_UNALIGNED, BOOL_PREV_REV));
    DeltaVarIntCodec.encodeSigned(sink, slot.get(JAVA_INT_UNALIGNED, BOOL_LAST_MOD));
    // Boolean value written BEFORE hash
    sink.writeBoolean(slot.get(ValueLayout.JAVA_BYTE, BOOL_VAL) != 0);
    if (config.hashType != HashType.NONE) {
      sink.writeLong(slot.get(JAVA_LONG_UNALIGNED, BOOL_HASH));
    }
  }

  private static void transformNullValue(final long nodeKey, final MemorySegment slot,
      final ResourceConfiguration config, final BytesOut<?> sink) {
    DeltaVarIntCodec.encodeDelta(sink, slot.get(JAVA_LONG_UNALIGNED, NULL_PARENT), nodeKey);
    DeltaVarIntCodec.encodeDelta(sink, slot.get(JAVA_LONG_UNALIGNED, NULL_RSIB), nodeKey);
    DeltaVarIntCodec.encodeDelta(sink, slot.get(JAVA_LONG_UNALIGNED, NULL_LSIB), nodeKey);
    DeltaVarIntCodec.encodeSigned(sink, slot.get(JAVA_INT_UNALIGNED, NULL_PREV_REV));
    DeltaVarIntCodec.encodeSigned(sink, slot.get(JAVA_INT_UNALIGNED, NULL_LAST_MOD));
    if (config.hashType != HashType.NONE) {
      sink.writeLong(slot.get(JAVA_LONG_UNALIGNED, NULL_HASH));
    }
  }

  // ════════════════════════════ JSON VALUE (OBJECT-PROPERTY, NO SIBLINGS)
  // ════════════════════════════

  private static void transformObjectStringValue(final long nodeKey, final MemorySegment slot,
      final ResourceConfiguration config, final BytesOut<?> sink) {
    DeltaVarIntCodec.encodeDelta(sink, slot.get(JAVA_LONG_UNALIGNED, OSTR_PARENT), nodeKey);
    DeltaVarIntCodec.encodeSigned(sink, slot.get(JAVA_INT_UNALIGNED, OSTR_PREV_REV));
    DeltaVarIntCodec.encodeSigned(sink, slot.get(JAVA_INT_UNALIGNED, OSTR_LAST_MOD));
    if (config.hashType != HashType.NONE) {
      sink.writeLong(slot.get(JAVA_LONG_UNALIGNED, OSTR_HASH));
    }
    // Compression flag + raw value bytes
    final int flags = slot.get(JAVA_INT_UNALIGNED, OSTR_PR0_FLAGS);
    sink.writeByte((byte) (flags & 1));
    final long pointer = slot.get(JAVA_LONG_UNALIGNED, OSTR_PR0_PTR);
    final int length = slot.get(JAVA_INT_UNALIGNED, OSTR_PR0_LEN);
    DeltaVarIntCodec.encodeSigned(sink, length);
    if (length > 0) {
      sink.writeSegment(slot, pointer, length);
    }
  }

  private static void transformObjectNumberValue(final long nodeKey, final MemorySegment slot,
      final ResourceConfiguration config, final BytesOut<?> sink) {
    DeltaVarIntCodec.encodeDelta(sink, slot.get(JAVA_LONG_UNALIGNED, ONUM_PARENT), nodeKey);
    DeltaVarIntCodec.encodeSigned(sink, slot.get(JAVA_INT_UNALIGNED, ONUM_PREV_REV));
    DeltaVarIntCodec.encodeSigned(sink, slot.get(JAVA_INT_UNALIGNED, ONUM_LAST_MOD));
    if (config.hashType != HashType.NONE) {
      sink.writeLong(slot.get(JAVA_LONG_UNALIGNED, ONUM_HASH));
    }
    // Number payload: already in serialized format
    final long pointer = slot.get(JAVA_LONG_UNALIGNED, ONUM_PR0_PTR);
    final int length = slot.get(JAVA_INT_UNALIGNED, ONUM_PR0_LEN);
    if (length > 0) {
      sink.writeSegment(slot, pointer, length);
    }
  }

  private static void transformObjectBooleanValue(final long nodeKey, final MemorySegment slot,
      final ResourceConfiguration config, final BytesOut<?> sink) {
    DeltaVarIntCodec.encodeDelta(sink, slot.get(JAVA_LONG_UNALIGNED, OBOOL_PARENT), nodeKey);
    DeltaVarIntCodec.encodeSigned(sink, slot.get(JAVA_INT_UNALIGNED, OBOOL_PREV_REV));
    DeltaVarIntCodec.encodeSigned(sink, slot.get(JAVA_INT_UNALIGNED, OBOOL_LAST_MOD));
    // Boolean value written BEFORE hash
    sink.writeBoolean(slot.get(ValueLayout.JAVA_BYTE, OBOOL_VAL) != 0);
    if (config.hashType != HashType.NONE) {
      sink.writeLong(slot.get(JAVA_LONG_UNALIGNED, OBOOL_HASH));
    }
  }

  private static void transformObjectNullValue(final long nodeKey, final MemorySegment slot,
      final ResourceConfiguration config, final BytesOut<?> sink) {
    DeltaVarIntCodec.encodeDelta(sink, slot.get(JAVA_LONG_UNALIGNED, ONULL_PARENT), nodeKey);
    DeltaVarIntCodec.encodeSigned(sink, slot.get(JAVA_INT_UNALIGNED, ONULL_PREV_REV));
    DeltaVarIntCodec.encodeSigned(sink, slot.get(JAVA_INT_UNALIGNED, ONULL_LAST_MOD));
    if (config.hashType != HashType.NONE) {
      sink.writeLong(slot.get(JAVA_LONG_UNALIGNED, ONULL_HASH));
    }
  }

  // ════════════════════════════ JSON DOCUMENT ════════════════════════════

  private static void transformJsonDocument(final long nodeKey, final MemorySegment slot, final BytesOut<?> sink) {
    DeltaVarIntCodec.encodeDelta(sink, slot.get(JAVA_LONG_UNALIGNED, JDOC_FCHILD), nodeKey);
    DeltaVarIntCodec.encodeSignedLong(sink, slot.get(JAVA_LONG_UNALIGNED, JDOC_DESC_CNT));
  }

  // ════════════════════════════ XML DOCUMENT ════════════════════════════

  private static void transformXmlDocument(final long nodeKey, final MemorySegment slot,
      final ResourceConfiguration config, final BytesOut<?> sink) {
    DeltaVarIntCodec.encodeDelta(sink, slot.get(JAVA_LONG_UNALIGNED, XDOC_FCHILD), nodeKey);
    if (config.hashType != HashType.NONE) {
      sink.writeLong(slot.get(JAVA_LONG_UNALIGNED, XDOC_HASH));
      DeltaVarIntCodec.encodeSignedLong(sink, slot.get(JAVA_LONG_UNALIGNED, XDOC_DESC_CNT));
    }
  }

  // ════════════════════════════ XML ELEMENT ════════════════════════════

  private static void transformElement(final long nodeKey, final MemorySegment slot, final ResourceConfiguration config,
      final BytesOut<?> sink) {
    DeltaVarIntCodec.encodeDelta(sink, slot.get(JAVA_LONG_UNALIGNED, ELEM_PARENT), nodeKey);
    DeltaVarIntCodec.encodeDelta(sink, slot.get(JAVA_LONG_UNALIGNED, ELEM_RSIB), nodeKey);
    DeltaVarIntCodec.encodeDelta(sink, slot.get(JAVA_LONG_UNALIGNED, ELEM_LSIB), nodeKey);
    DeltaVarIntCodec.encodeDelta(sink, slot.get(JAVA_LONG_UNALIGNED, ELEM_FCHILD), nodeKey);
    DeltaVarIntCodec.encodeDelta(sink, slot.get(JAVA_LONG_UNALIGNED, ELEM_LCHILD), nodeKey);
    // Name node fields
    DeltaVarIntCodec.encodeDelta(sink, slot.get(JAVA_LONG_UNALIGNED, ELEM_PATH), nodeKey);
    DeltaVarIntCodec.encodeSigned(sink, slot.get(JAVA_INT_UNALIGNED, ELEM_PREFIX));
    DeltaVarIntCodec.encodeSigned(sink, slot.get(JAVA_INT_UNALIGNED, ELEM_LNAME));
    DeltaVarIntCodec.encodeSigned(sink, slot.get(JAVA_INT_UNALIGNED, ELEM_URI));
    // Metadata
    DeltaVarIntCodec.encodeSigned(sink, slot.get(JAVA_INT_UNALIGNED, ELEM_PREV_REV));
    DeltaVarIntCodec.encodeSigned(sink, slot.get(JAVA_INT_UNALIGNED, ELEM_LAST_MOD));
    if (config.storeChildCount()) {
      DeltaVarIntCodec.encodeSigned(sink, (int) slot.get(JAVA_LONG_UNALIGNED, ELEM_CHILD_CNT));
    }
    if (config.hashType != HashType.NONE) {
      sink.writeLong(slot.get(JAVA_LONG_UNALIGNED, ELEM_HASH));
      DeltaVarIntCodec.encodeSigned(sink, (int) slot.get(JAVA_LONG_UNALIGNED, ELEM_DESC_CNT));
    }
    // Attribute keys vector
    final long attrPointer = slot.get(JAVA_LONG_UNALIGNED, ELEM_ATTR_PR_PTR);
    final int attrLength = slot.get(JAVA_INT_UNALIGNED, ELEM_ATTR_PR_LEN);
    final int attrCount = attrLength / Long.BYTES;
    DeltaVarIntCodec.encodeSigned(sink, attrCount);
    for (int i = 0; i < attrCount; i++) {
      DeltaVarIntCodec.encodeDelta(sink, slot.get(JAVA_LONG_UNALIGNED, attrPointer + (long) i * Long.BYTES), nodeKey);
    }
    // Namespace keys vector
    final long nsPointer = slot.get(JAVA_LONG_UNALIGNED, ELEM_NS_PR_PTR);
    final int nsLength = slot.get(JAVA_INT_UNALIGNED, ELEM_NS_PR_LEN);
    final int nsCount = nsLength / Long.BYTES;
    DeltaVarIntCodec.encodeSigned(sink, nsCount);
    for (int i = 0; i < nsCount; i++) {
      DeltaVarIntCodec.encodeDelta(sink, slot.get(JAVA_LONG_UNALIGNED, nsPointer + (long) i * Long.BYTES), nodeKey);
    }
  }

  // ════════════════════════════ XML ATTRIBUTE ════════════════════════════

  private static void transformAttribute(final long nodeKey, final MemorySegment slot, final BytesOut<?> sink) {
    DeltaVarIntCodec.encodeDelta(sink, slot.get(JAVA_LONG_UNALIGNED, ATTR_PARENT), nodeKey);
    DeltaVarIntCodec.encodeDelta(sink, slot.get(JAVA_LONG_UNALIGNED, ATTR_PATH), nodeKey);
    DeltaVarIntCodec.encodeSigned(sink, slot.get(JAVA_INT_UNALIGNED, ATTR_PREFIX));
    DeltaVarIntCodec.encodeSigned(sink, slot.get(JAVA_INT_UNALIGNED, ATTR_LNAME));
    DeltaVarIntCodec.encodeSigned(sink, slot.get(JAVA_INT_UNALIGNED, ATTR_URI));
    DeltaVarIntCodec.encodeSigned(sink, slot.get(JAVA_INT_UNALIGNED, ATTR_PREV_REV));
    DeltaVarIntCodec.encodeSigned(sink, slot.get(JAVA_INT_UNALIGNED, ATTR_LAST_MOD));
    // Attribute values are always written uncompressed (flag = 0x00)
    sink.writeByte((byte) 0);
    final long pointer = slot.get(JAVA_LONG_UNALIGNED, ATTR_PR0_PTR);
    final int length = slot.get(JAVA_INT_UNALIGNED, ATTR_PR0_LEN);
    DeltaVarIntCodec.encodeSigned(sink, length);
    if (length > 0) {
      sink.writeSegment(slot, pointer, length);
    }
  }

  // ════════════════════════════ XML NAMESPACE ════════════════════════════

  private static void transformNamespace(final long nodeKey, final MemorySegment slot, final BytesOut<?> sink) {
    DeltaVarIntCodec.encodeDelta(sink, slot.get(JAVA_LONG_UNALIGNED, NS_PARENT), nodeKey);
    DeltaVarIntCodec.encodeDelta(sink, slot.get(JAVA_LONG_UNALIGNED, NS_PATH), nodeKey);
    DeltaVarIntCodec.encodeSigned(sink, slot.get(JAVA_INT_UNALIGNED, NS_PREFIX));
    DeltaVarIntCodec.encodeSigned(sink, slot.get(JAVA_INT_UNALIGNED, NS_LNAME));
    DeltaVarIntCodec.encodeSigned(sink, slot.get(JAVA_INT_UNALIGNED, NS_URI));
    DeltaVarIntCodec.encodeSigned(sink, slot.get(JAVA_INT_UNALIGNED, NS_PREV_REV));
    DeltaVarIntCodec.encodeSigned(sink, slot.get(JAVA_INT_UNALIGNED, NS_LAST_MOD));
  }

  // ════════════════════════════ XML TEXT ════════════════════════════

  private static void transformText(final long nodeKey, final MemorySegment slot, final BytesOut<?> sink) {
    DeltaVarIntCodec.encodeDelta(sink, slot.get(JAVA_LONG_UNALIGNED, TEXT_PARENT), nodeKey);
    DeltaVarIntCodec.encodeDelta(sink, slot.get(JAVA_LONG_UNALIGNED, TEXT_RSIB), nodeKey);
    DeltaVarIntCodec.encodeDelta(sink, slot.get(JAVA_LONG_UNALIGNED, TEXT_LSIB), nodeKey);
    DeltaVarIntCodec.encodeSigned(sink, slot.get(JAVA_INT_UNALIGNED, TEXT_PREV_REV));
    DeltaVarIntCodec.encodeSigned(sink, slot.get(JAVA_INT_UNALIGNED, TEXT_LAST_MOD));
    // Compression flag + value bytes (hash is NOT serialized for TEXT)
    final int flags = slot.get(JAVA_INT_UNALIGNED, TEXT_PR0_FLAGS);
    sink.writeByte((byte) (flags & 1));
    final long pointer = slot.get(JAVA_LONG_UNALIGNED, TEXT_PR0_PTR);
    final int length = slot.get(JAVA_INT_UNALIGNED, TEXT_PR0_LEN);
    DeltaVarIntCodec.encodeSigned(sink, length);
    if (length > 0) {
      sink.writeSegment(slot, pointer, length);
    }
  }

  // ════════════════════════════ XML COMMENT ════════════════════════════

  private static void transformComment(final long nodeKey, final MemorySegment slot, final BytesOut<?> sink) {
    DeltaVarIntCodec.encodeDelta(sink, slot.get(JAVA_LONG_UNALIGNED, CMT_PARENT), nodeKey);
    DeltaVarIntCodec.encodeDelta(sink, slot.get(JAVA_LONG_UNALIGNED, CMT_RSIB), nodeKey);
    DeltaVarIntCodec.encodeDelta(sink, slot.get(JAVA_LONG_UNALIGNED, CMT_LSIB), nodeKey);
    DeltaVarIntCodec.encodeSigned(sink, slot.get(JAVA_INT_UNALIGNED, CMT_PREV_REV));
    DeltaVarIntCodec.encodeSigned(sink, slot.get(JAVA_INT_UNALIGNED, CMT_LAST_MOD));
    // Compression flag + value bytes (hash is NOT serialized for COMMENT)
    final int flags = slot.get(JAVA_INT_UNALIGNED, CMT_PR0_FLAGS);
    sink.writeByte((byte) (flags & 1));
    final long pointer = slot.get(JAVA_LONG_UNALIGNED, CMT_PR0_PTR);
    final int length = slot.get(JAVA_INT_UNALIGNED, CMT_PR0_LEN);
    DeltaVarIntCodec.encodeSigned(sink, length);
    if (length > 0) {
      sink.writeSegment(slot, pointer, length);
    }
  }

  // ════════════════════════════ XML PROCESSING_INSTRUCTION ════════════════════════════

  private static void transformProcessingInstruction(final long nodeKey, final MemorySegment slot,
      final ResourceConfiguration config, final BytesOut<?> sink) {
    DeltaVarIntCodec.encodeDelta(sink, slot.get(JAVA_LONG_UNALIGNED, PI_PARENT), nodeKey);
    DeltaVarIntCodec.encodeDelta(sink, slot.get(JAVA_LONG_UNALIGNED, PI_RSIB), nodeKey);
    DeltaVarIntCodec.encodeDelta(sink, slot.get(JAVA_LONG_UNALIGNED, PI_LSIB), nodeKey);
    DeltaVarIntCodec.encodeDelta(sink, slot.get(JAVA_LONG_UNALIGNED, PI_FCHILD), nodeKey);
    DeltaVarIntCodec.encodeDelta(sink, slot.get(JAVA_LONG_UNALIGNED, PI_LCHILD), nodeKey);
    DeltaVarIntCodec.encodeDelta(sink, slot.get(JAVA_LONG_UNALIGNED, PI_PATH), nodeKey);
    DeltaVarIntCodec.encodeSigned(sink, slot.get(JAVA_INT_UNALIGNED, PI_PREFIX));
    DeltaVarIntCodec.encodeSigned(sink, slot.get(JAVA_INT_UNALIGNED, PI_LNAME));
    DeltaVarIntCodec.encodeSigned(sink, slot.get(JAVA_INT_UNALIGNED, PI_URI));
    DeltaVarIntCodec.encodeSigned(sink, slot.get(JAVA_INT_UNALIGNED, PI_PREV_REV));
    DeltaVarIntCodec.encodeSigned(sink, slot.get(JAVA_INT_UNALIGNED, PI_LAST_MOD));
    if (config.storeChildCount()) {
      DeltaVarIntCodec.encodeSigned(sink, (int) slot.get(JAVA_LONG_UNALIGNED, PI_CHILD_CNT));
    }
    if (config.hashType != HashType.NONE) {
      sink.writeLong(slot.get(JAVA_LONG_UNALIGNED, PI_HASH));
      DeltaVarIntCodec.encodeSigned(sink, (int) slot.get(JAVA_LONG_UNALIGNED, PI_DESC_CNT));
    }
    // Compression flag + value bytes
    final int flags = slot.get(JAVA_INT_UNALIGNED, PI_PR0_FLAGS);
    sink.writeByte((byte) (flags & 1));
    final long pointer = slot.get(JAVA_LONG_UNALIGNED, PI_PR0_PTR);
    final int length = slot.get(JAVA_INT_UNALIGNED, PI_PR0_LEN);
    DeltaVarIntCodec.encodeSigned(sink, length);
    if (length > 0) {
      sink.writeSegment(slot, pointer, length);
    }
  }
}
