package io.sirix.page;

import io.sirix.node.NodeKind;

/**
 * Defines the per-record offset table layout for each {@link NodeKind}.
 *
 * <p>Each record in the heap has an offset table with one byte per field,
 * enabling O(1) access to any field without parsing preceding varints.
 * This class defines the field count and field index constants for each NodeKind.
 *
 * <h2>Offset Table Format</h2>
 * <pre>
 * [nodeKind: 1 byte]
 * [fieldOffsetTable: fieldCount × 1 byte]
 * [data region: varint fields + hash + optional payload]
 * </pre>
 *
 * <p>ALL fields are always present in the slotted page format, including hash (0 when unused)
 * and childCount/descendantCount (0 when unused). This eliminates conditional field logic
 * and makes the offset table size fixed per NodeKind.
 */
public final class NodeFieldLayout {

  private NodeFieldLayout() {
    throw new AssertionError("Utility class");
  }

  /** Fixed width of hash field in bytes (always 8). */
  public static final int HASH_WIDTH = Long.BYTES;

  /** NAME_KEY field index for primitive-fused {@code OBJECT_NAMED_*} (kindIds 48-51). */
  public static final int FUSED_PRIMITIVE_NAME_KEY_FIELD = 3;

  /** NAME_KEY field index for structural-fused {@code OBJECT_NAMED_OBJECT/ARRAY} (kindIds 52-53). */
  public static final int FUSED_STRUCTURAL_NAME_KEY_FIELD = 5;

  // ==================== OBJECT NODE (10 fields) ====================

  /** Total field count for OBJECT nodes. */
  public static final int OBJECT_FIELD_COUNT = 10;

  public static final int OBJECT_PARENT_KEY = 0;
  public static final int OBJECT_RIGHT_SIB_KEY = 1;
  public static final int OBJECT_LEFT_SIB_KEY = 2;
  public static final int OBJECT_FIRST_CHILD_KEY = 3;
  public static final int OBJECT_LAST_CHILD_KEY = 4;
  public static final int OBJECT_PREV_REVISION = 5;
  public static final int OBJECT_LAST_MOD_REVISION = 6;
  public static final int OBJECT_HASH = 7;
  public static final int OBJECT_CHILD_COUNT = 8;
  public static final int OBJECT_DESCENDANT_COUNT = 9;

  // ==================== ARRAY NODE (11 fields) ====================

  /** Total field count for ARRAY nodes. */
  public static final int ARRAY_FIELD_COUNT = 11;

  public static final int ARRAY_PARENT_KEY = 0;
  public static final int ARRAY_RIGHT_SIB_KEY = 1;
  public static final int ARRAY_LEFT_SIB_KEY = 2;
  public static final int ARRAY_FIRST_CHILD_KEY = 3;
  public static final int ARRAY_LAST_CHILD_KEY = 4;
  public static final int ARRAY_PATH_NODE_KEY = 5;
  public static final int ARRAY_PREV_REVISION = 6;
  public static final int ARRAY_LAST_MOD_REVISION = 7;
  public static final int ARRAY_HASH = 8;
  public static final int ARRAY_CHILD_COUNT = 9;
  public static final int ARRAY_DESCENDANT_COUNT = 10;

  // (Phase 4: legacy OBJECT_KEY (kindId 26) and OBJECT_KEY_PAX (kindId 126)
  //  field-layout constants deleted — fully replaced by the 6 fused OBJECT_NAMED_*
  //  kinds 48-53.)

  // ==================== STRING_VALUE NODE (7 fields + payload) ====================

  /** Total field count for STRING_VALUE nodes (excluding payload). */
  public static final int STRING_VALUE_FIELD_COUNT = 6;

  public static final int STRVAL_PARENT_KEY = 0;
  public static final int STRVAL_RIGHT_SIB_KEY = 1;
  public static final int STRVAL_LEFT_SIB_KEY = 2;
  public static final int STRVAL_PREV_REVISION = 3;
  public static final int STRVAL_LAST_MOD_REVISION = 4;
  /** Points to the start of [isCompressed:1][valueLength:varint][value:bytes]. */
  public static final int STRVAL_PAYLOAD = 5;

  // ==================== NUMBER_VALUE NODE (7 fields + payload) ====================

  /** Total field count for NUMBER_VALUE nodes (excluding payload). */
  public static final int NUMBER_VALUE_FIELD_COUNT = 6;

  public static final int NUMVAL_PARENT_KEY = 0;
  public static final int NUMVAL_RIGHT_SIB_KEY = 1;
  public static final int NUMVAL_LEFT_SIB_KEY = 2;
  public static final int NUMVAL_PREV_REVISION = 3;
  public static final int NUMVAL_LAST_MOD_REVISION = 4;
  /** Points to the start of [numberType:1][numberData:variable]. */
  public static final int NUMVAL_PAYLOAD = 5;

  // ==================== BOOLEAN_VALUE NODE (7 fields) ====================

  /** Total field count for BOOLEAN_VALUE nodes. */
  public static final int BOOLEAN_VALUE_FIELD_COUNT = 6;

  public static final int BOOLVAL_PARENT_KEY = 0;
  public static final int BOOLVAL_RIGHT_SIB_KEY = 1;
  public static final int BOOLVAL_LEFT_SIB_KEY = 2;
  public static final int BOOLVAL_PREV_REVISION = 3;
  public static final int BOOLVAL_LAST_MOD_REVISION = 4;
  public static final int BOOLVAL_VALUE = 5;

  // ==================== NULL_VALUE NODE (6 fields) ====================

  /** Total field count for NULL_VALUE nodes. */
  public static final int NULL_VALUE_FIELD_COUNT = 5;

  public static final int NULLVAL_PARENT_KEY = 0;
  public static final int NULLVAL_RIGHT_SIB_KEY = 1;
  public static final int NULLVAL_LEFT_SIB_KEY = 2;
  public static final int NULLVAL_PREV_REVISION = 3;
  public static final int NULLVAL_LAST_MOD_REVISION = 4;

  // ==================== OBJECT_NAMED_BOOLEAN NODE (fused OBJECT_KEY + boolean leaf) ====================
  //
  // Layout rationale: OBJECT_NAMED_* fuses the parent OBJECT_KEY and its primitive-typed
  // child into a single slotted record. Structural navigation fields match OBJECT_KEY
  // (parent + siblings + path) so the node sits in the parent OBJECT's child list, and
  // also carries the primitive value payload. No firstChild/lastChild: fused nodes are leaves.
  //
  // Wire layout per slot:
  //   [kindByte][offsetTable: FIELD_COUNT x 1 byte][data region]
  //   data region order matches writeNewRecord:
  //     0 parentKey (delta-varint)
  //     1 rightSiblingKey (delta-varint)
  //     2 leftSiblingKey (delta-varint)
  //     3 nameKey (signed varint)
  //     4 pathNodeKey (delta-varint)
  //     5 previousRevision (signed varint)
  //     6 lastModifiedRevision (signed varint)
  //     7 hash (fixed 8 bytes)
  //     8 value (boolean: 1 byte)

  /** Total field count for OBJECT_NAMED_BOOLEAN nodes. */
  public static final int OBJECT_NAMED_BOOLEAN_FIELD_COUNT = 9;

  public static final int OBJNAMEDBOOL_PARENT_KEY = 0;
  public static final int OBJNAMEDBOOL_RIGHT_SIB_KEY = 1;
  public static final int OBJNAMEDBOOL_LEFT_SIB_KEY = 2;
  public static final int OBJNAMEDBOOL_NAME_KEY = 3;
  public static final int OBJNAMEDBOOL_PATH_NODE_KEY = 4;
  public static final int OBJNAMEDBOOL_PREV_REVISION = 5;
  public static final int OBJNAMEDBOOL_LAST_MOD_REVISION = 6;
  public static final int OBJNAMEDBOOL_HASH = 7;
  public static final int OBJNAMEDBOOL_VALUE = 8;

  // ==================== OBJECT_NAMED_NUMBER NODE (fused OBJECT_KEY + number leaf) ====================
  //
  // Wire layout per slot (9 fields + variable payload):
  //     0 parentKey (delta-varint)
  //     1 rightSiblingKey (delta-varint)
  //     2 leftSiblingKey (delta-varint)
  //     3 nameKey (signed varint)
  //     4 pathNodeKey (delta-varint)
  //     5 previousRevision (signed varint)
  //     6 lastModifiedRevision (signed varint)
  //     7 hash (fixed 8 bytes)
  //     8 payload [numberType:1][numberData:variable]

  /** Total field count for OBJECT_NAMED_NUMBER nodes (excluding variable-length payload). */
  public static final int OBJECT_NAMED_NUMBER_FIELD_COUNT = 9;

  public static final int OBJNAMEDNUM_PARENT_KEY = 0;
  public static final int OBJNAMEDNUM_RIGHT_SIB_KEY = 1;
  public static final int OBJNAMEDNUM_LEFT_SIB_KEY = 2;
  public static final int OBJNAMEDNUM_NAME_KEY = 3;
  public static final int OBJNAMEDNUM_PATH_NODE_KEY = 4;
  public static final int OBJNAMEDNUM_PREV_REVISION = 5;
  public static final int OBJNAMEDNUM_LAST_MOD_REVISION = 6;
  public static final int OBJNAMEDNUM_HASH = 7;
  /** Points to the start of [numberType:1][numberData:variable]. */
  public static final int OBJNAMEDNUM_PAYLOAD = 8;

  // ==================== OBJECT_NAMED_STRING NODE (fused OBJECT_KEY + string leaf) ====================
  //
  // Wire layout per slot (9 fields + variable payload):
  //     0 parentKey (delta-varint)
  //     1 rightSiblingKey (delta-varint)
  //     2 leftSiblingKey (delta-varint)
  //     3 nameKey (signed varint)
  //     4 pathNodeKey (delta-varint)
  //     5 previousRevision (signed varint)
  //     6 lastModifiedRevision (signed varint)
  //     7 hash (fixed 8 bytes)
  //     8 payload [isCompressed:1][valueLength:varint][value:bytes]

  /** Total field count for OBJECT_NAMED_STRING nodes (excluding variable-length payload). */
  public static final int OBJECT_NAMED_STRING_FIELD_COUNT = 9;

  public static final int OBJNAMEDSTR_PARENT_KEY = 0;
  public static final int OBJNAMEDSTR_RIGHT_SIB_KEY = 1;
  public static final int OBJNAMEDSTR_LEFT_SIB_KEY = 2;
  public static final int OBJNAMEDSTR_NAME_KEY = 3;
  public static final int OBJNAMEDSTR_PATH_NODE_KEY = 4;
  public static final int OBJNAMEDSTR_PREV_REVISION = 5;
  public static final int OBJNAMEDSTR_LAST_MOD_REVISION = 6;
  public static final int OBJNAMEDSTR_HASH = 7;
  /** Points to the start of [isCompressed:1][valueLength:varint][value:bytes]. */
  public static final int OBJNAMEDSTR_PAYLOAD = 8;

  // ==================== OBJECT_NAMED_NULL NODE (fused OBJECT_KEY + null leaf) ====================
  //
  // Wire layout per slot (8 fields, no value payload):
  //     0 parentKey (delta-varint)
  //     1 rightSiblingKey (delta-varint)
  //     2 leftSiblingKey (delta-varint)
  //     3 nameKey (signed varint)
  //     4 pathNodeKey (delta-varint)
  //     5 previousRevision (signed varint)
  //     6 lastModifiedRevision (signed varint)
  //     7 hash (fixed 8 bytes)

  /** Total field count for OBJECT_NAMED_NULL nodes. */
  public static final int OBJECT_NAMED_NULL_FIELD_COUNT = 8;

  public static final int OBJNAMEDNULL_PARENT_KEY = 0;
  public static final int OBJNAMEDNULL_RIGHT_SIB_KEY = 1;
  public static final int OBJNAMEDNULL_LEFT_SIB_KEY = 2;
  public static final int OBJNAMEDNULL_NAME_KEY = 3;
  public static final int OBJNAMEDNULL_PATH_NODE_KEY = 4;
  public static final int OBJNAMEDNULL_PREV_REVISION = 5;
  public static final int OBJNAMEDNULL_LAST_MOD_REVISION = 6;
  public static final int OBJNAMEDNULL_HASH = 7;

  // ==================== OBJECT_NAMED_OBJECT NODE (Phase 1 — fused OBJECT_KEY + nested OBJECT) ====================
  //
  // Internal node carrying both the field-name role (nameKey, pathNodeKey) and the
  // structural OBJECT shape (firstChild, lastChild, childCount, descendantCount). On-wire
  // layout (12 fields) — structural keys first, name/path next, hash + counts last:
  //     0 parentKey         (delta-varint)
  //     1 rightSiblingKey   (delta-varint)
  //     2 leftSiblingKey    (delta-varint)
  //     3 firstChildKey     (delta-varint)
  //     4 lastChildKey      (delta-varint)
  //     5 nameKey           (signed varint)
  //     6 pathNodeKey       (delta-varint)
  //     7 previousRevision  (signed varint)
  //     8 lastModifiedRev   (signed varint)
  //     9 hash              (fixed 8 bytes)
  //    10 childCount        (signed long varint)
  //    11 descendantCount   (signed long varint)

  /** Total field count for OBJECT_NAMED_OBJECT nodes. */
  public static final int OBJECT_NAMED_OBJECT_FIELD_COUNT = 12;

  public static final int OBJNAMEDOBJ_PARENT_KEY = 0;
  public static final int OBJNAMEDOBJ_RIGHT_SIB_KEY = 1;
  public static final int OBJNAMEDOBJ_LEFT_SIB_KEY = 2;
  public static final int OBJNAMEDOBJ_FIRST_CHILD_KEY = 3;
  public static final int OBJNAMEDOBJ_LAST_CHILD_KEY = 4;
  public static final int OBJNAMEDOBJ_NAME_KEY = 5;
  public static final int OBJNAMEDOBJ_PATH_NODE_KEY = 6;
  public static final int OBJNAMEDOBJ_PREV_REVISION = 7;
  public static final int OBJNAMEDOBJ_LAST_MOD_REVISION = 8;
  public static final int OBJNAMEDOBJ_HASH = 9;
  public static final int OBJNAMEDOBJ_CHILD_COUNT = 10;
  public static final int OBJNAMEDOBJ_DESCENDANT_COUNT = 11;

  // ==================== OBJECT_NAMED_ARRAY NODE (Phase 1 — fused OBJECT_KEY + nested ARRAY) ====================
  //
  // Same field shape and indices as OBJECT_NAMED_OBJECT — name+structural+counts. The kind
  // distinction lives at the kindId byte (52 vs 53) and in serialization-time semantics
  // (array vs object descent / output emit).

  /** Total field count for OBJECT_NAMED_ARRAY nodes. */
  public static final int OBJECT_NAMED_ARRAY_FIELD_COUNT = 12;

  public static final int OBJNAMEDARR_PARENT_KEY = 0;
  public static final int OBJNAMEDARR_RIGHT_SIB_KEY = 1;
  public static final int OBJNAMEDARR_LEFT_SIB_KEY = 2;
  public static final int OBJNAMEDARR_FIRST_CHILD_KEY = 3;
  public static final int OBJNAMEDARR_LAST_CHILD_KEY = 4;
  public static final int OBJNAMEDARR_NAME_KEY = 5;
  public static final int OBJNAMEDARR_PATH_NODE_KEY = 6;
  public static final int OBJNAMEDARR_PREV_REVISION = 7;
  public static final int OBJNAMEDARR_LAST_MOD_REVISION = 8;
  public static final int OBJNAMEDARR_HASH = 9;
  public static final int OBJNAMEDARR_CHILD_COUNT = 10;
  public static final int OBJNAMEDARR_DESCENDANT_COUNT = 11;

  // ==================== JSON_DOCUMENT_ROOT (7 fields) ====================

  /** Total field count for JSON_DOCUMENT_ROOT nodes. */
  public static final int JSON_DOCUMENT_ROOT_FIELD_COUNT = 7;

  public static final int JDOCROOT_FIRST_CHILD_KEY = 0;
  public static final int JDOCROOT_LAST_CHILD_KEY = 1;
  public static final int JDOCROOT_CHILD_COUNT = 2;
  public static final int JDOCROOT_DESCENDANT_COUNT = 3;
  public static final int JDOCROOT_PREV_REVISION = 4;
  public static final int JDOCROOT_LAST_MOD_REVISION = 5;
  public static final int JDOCROOT_HASH = 6;

  // ==================== XML ELEMENT NODE (15 fields + payload) ====================

  /** Total field count for ELEMENT nodes (excluding attr/ns payload). */
  public static final int ELEMENT_FIELD_COUNT = 15;

  public static final int ELEM_PARENT_KEY = 0;
  public static final int ELEM_RIGHT_SIB_KEY = 1;
  public static final int ELEM_LEFT_SIB_KEY = 2;
  public static final int ELEM_FIRST_CHILD_KEY = 3;
  public static final int ELEM_LAST_CHILD_KEY = 4;
  public static final int ELEM_PATH_NODE_KEY = 5;
  public static final int ELEM_PREFIX_KEY = 6;
  public static final int ELEM_LOCAL_NAME_KEY = 7;
  public static final int ELEM_URI_KEY = 8;
  public static final int ELEM_PREV_REVISION = 9;
  public static final int ELEM_LAST_MOD_REVISION = 10;
  public static final int ELEM_HASH = 11;
  public static final int ELEM_CHILD_COUNT = 12;
  public static final int ELEM_DESCENDANT_COUNT = 13;
  /** Points to [attrCount:varint][attrKeys:delta...][nsCount:varint][nsKeys:delta...]. */
  public static final int ELEM_PAYLOAD = 14;

  // ==================== XML ATTRIBUTE NODE (9 fields + payload) ====================

  /** Total field count for ATTRIBUTE nodes (excluding value payload). */
  public static final int ATTRIBUTE_FIELD_COUNT = 8;

  public static final int ATTR_PARENT_KEY = 0;
  public static final int ATTR_PATH_NODE_KEY = 1;
  public static final int ATTR_PREFIX_KEY = 2;
  public static final int ATTR_LOCAL_NAME_KEY = 3;
  public static final int ATTR_URI_KEY = 4;
  public static final int ATTR_PREV_REVISION = 5;
  public static final int ATTR_LAST_MOD_REVISION = 6;
  /** Points to [isCompressed:1][valueLength:varint][value:bytes]. */
  public static final int ATTR_PAYLOAD = 7;

  // ==================== XML TEXT NODE (7 fields + payload) ====================

  /** Total field count for TEXT nodes (excluding value payload). */
  public static final int TEXT_FIELD_COUNT = 6;

  public static final int TEXT_PARENT_KEY = 0;
  public static final int TEXT_RIGHT_SIB_KEY = 1;
  public static final int TEXT_LEFT_SIB_KEY = 2;
  public static final int TEXT_PREV_REVISION = 3;
  public static final int TEXT_LAST_MOD_REVISION = 4;
  /** Points to [isCompressed:1][valueLength:varint][value:bytes]. */
  public static final int TEXT_PAYLOAD = 5;

  // ==================== XML COMMENT NODE (7 fields + payload) ====================

  /** Total field count for COMMENT nodes (excluding value payload). */
  public static final int COMMENT_FIELD_COUNT = 6;

  public static final int COMMENT_PARENT_KEY = 0;
  public static final int COMMENT_RIGHT_SIB_KEY = 1;
  public static final int COMMENT_LEFT_SIB_KEY = 2;
  public static final int COMMENT_PREV_REVISION = 3;
  public static final int COMMENT_LAST_MOD_REVISION = 4;
  /** Points to [isCompressed:1][valueLength:varint][value:bytes]. */
  public static final int COMMENT_PAYLOAD = 5;

  // ==================== XML PI NODE (15 fields + payload) ====================

  /** Total field count for PROCESSING_INSTRUCTION nodes (excluding value payload). */
  public static final int PI_FIELD_COUNT = 14;

  public static final int PI_PARENT_KEY = 0;
  public static final int PI_RIGHT_SIB_KEY = 1;
  public static final int PI_LEFT_SIB_KEY = 2;
  public static final int PI_FIRST_CHILD_KEY = 3;
  public static final int PI_LAST_CHILD_KEY = 4;
  public static final int PI_PATH_NODE_KEY = 5;
  public static final int PI_PREFIX_KEY = 6;
  public static final int PI_LOCAL_NAME_KEY = 7;
  public static final int PI_URI_KEY = 8;
  public static final int PI_PREV_REVISION = 9;
  public static final int PI_LAST_MOD_REVISION = 10;
  public static final int PI_CHILD_COUNT = 11;
  public static final int PI_DESCENDANT_COUNT = 12;
  /** Points to [isCompressed:1][valueLength:varint][value:bytes]. */
  public static final int PI_PAYLOAD = 13;

  // ==================== XML NAMESPACE NODE (8 fields) ====================

  /** Total field count for NAMESPACE nodes. */
  public static final int NAMESPACE_FIELD_COUNT = 8;

  public static final int NS_PARENT_KEY = 0;
  public static final int NS_PATH_NODE_KEY = 1;
  public static final int NS_PREFIX_KEY = 2;
  public static final int NS_LOCAL_NAME_KEY = 3;
  public static final int NS_URI_KEY = 4;
  public static final int NS_PREV_REVISION = 5;
  public static final int NS_LAST_MOD_REVISION = 6;
  public static final int NS_HASH = 7;

  // ==================== XML DOCUMENT ROOT NODE (7 fields) ====================

  /** Total field count for XML_DOCUMENT_ROOT nodes. */
  public static final int XML_DOCUMENT_ROOT_FIELD_COUNT = 7;

  public static final int XDOCROOT_FIRST_CHILD_KEY = 0;
  public static final int XDOCROOT_LAST_CHILD_KEY = 1;
  public static final int XDOCROOT_CHILD_COUNT = 2;
  public static final int XDOCROOT_DESCENDANT_COUNT = 3;
  public static final int XDOCROOT_PREV_REVISION = 4;
  public static final int XDOCROOT_LAST_MOD_REVISION = 5;
  public static final int XDOCROOT_HASH = 6;

  // ==================== FIELD COUNT LOOKUP ====================

  /**
   * Get the field count for a given NodeKind.
   *
   * @param kindId the NodeKind byte ID
   * @return the field count, or -1 if unknown/unsupported
   */
  public static int fieldCountForKind(final int kindId) {
    return switch (kindId) {
      case 1 -> ELEMENT_FIELD_COUNT;            // ELEMENT
      case 2 -> ATTRIBUTE_FIELD_COUNT;           // ATTRIBUTE
      case 3 -> TEXT_FIELD_COUNT;                // TEXT
      case 7 -> PI_FIELD_COUNT;                  // PROCESSING_INSTRUCTION
      case 8 -> COMMENT_FIELD_COUNT;             // COMMENT
      case 9 -> XML_DOCUMENT_ROOT_FIELD_COUNT;   // XML_DOCUMENT
      case 13 -> NAMESPACE_FIELD_COUNT;          // NAMESPACE
      case 24 -> OBJECT_FIELD_COUNT;             // OBJECT
      case 25 -> ARRAY_FIELD_COUNT;              // ARRAY
      case 27 -> BOOLEAN_VALUE_FIELD_COUNT;      // BOOLEAN_VALUE
      case 28 -> NUMBER_VALUE_FIELD_COUNT;       // NUMBER_VALUE
      case 29 -> NULL_VALUE_FIELD_COUNT;         // NULL_VALUE
      case 30 -> STRING_VALUE_FIELD_COUNT;       // STRING_VALUE
      case 31 -> JSON_DOCUMENT_ROOT_FIELD_COUNT; // JSON_DOCUMENT_ROOT
      case 48 -> OBJECT_NAMED_BOOLEAN_FIELD_COUNT;  // OBJECT_NAMED_BOOLEAN
      case 49 -> OBJECT_NAMED_NUMBER_FIELD_COUNT;   // OBJECT_NAMED_NUMBER
      case 50 -> OBJECT_NAMED_STRING_FIELD_COUNT;   // OBJECT_NAMED_STRING
      case 51 -> OBJECT_NAMED_NULL_FIELD_COUNT;     // OBJECT_NAMED_NULL
      case 52 -> OBJECT_NAMED_OBJECT_FIELD_COUNT;   // OBJECT_NAMED_OBJECT (Phase 1 reserved)
      case 53 -> OBJECT_NAMED_ARRAY_FIELD_COUNT;    // OBJECT_NAMED_ARRAY  (Phase 1 reserved)
      default -> -1;
    };
  }

  /**
   * Get the field count for a given NodeKind enum.
   *
   * @param kind the NodeKind
   * @return the field count, or -1 if unsupported
   */
  public static int fieldCountForKind(final NodeKind kind) {
    return fieldCountForKind(kind.getId());
  }

  /**
   * Returns the field-table index of the PARENT_KEY field for a given NodeKind,
   * or {@code -1} if the kind has no parent key (JSON_DOCUMENT_ROOT, XML_DOCUMENT_ROOT).
   *
   * <p>Used by the columnar structural-key extractor to locate the per-record
   * parentKey varint on the slotted-page heap without a full record parse.
   */
  public static int parentKeyFieldIndexForKind(final int kindId) {
    return switch (kindId) {
      case 1 -> ELEM_PARENT_KEY;                 // ELEMENT
      case 2 -> ATTR_PARENT_KEY;                 // ATTRIBUTE
      case 3 -> TEXT_PARENT_KEY;                 // TEXT
      case 7 -> PI_PARENT_KEY;                   // PROCESSING_INSTRUCTION
      case 8 -> COMMENT_PARENT_KEY;              // COMMENT
      case 9 -> -1;                              // XML_DOCUMENT_ROOT (no parent)
      case 13 -> NS_PARENT_KEY;                  // NAMESPACE
      case 24 -> OBJECT_PARENT_KEY;              // OBJECT
      case 25 -> ARRAY_PARENT_KEY;               // ARRAY
      case 27 -> BOOLVAL_PARENT_KEY;             // BOOLEAN_VALUE
      case 28 -> NUMVAL_PARENT_KEY;              // NUMBER_VALUE
      case 29 -> NULLVAL_PARENT_KEY;             // NULL_VALUE
      case 30 -> STRVAL_PARENT_KEY;              // STRING_VALUE
      case 31 -> -1;                             // JSON_DOCUMENT_ROOT (no parent)
      case 48 -> OBJNAMEDBOOL_PARENT_KEY;        // OBJECT_NAMED_BOOLEAN (iter#30 fusion)
      case 49 -> OBJNAMEDNUM_PARENT_KEY;         // OBJECT_NAMED_NUMBER  (iter#30 fusion)
      case 50 -> OBJNAMEDSTR_PARENT_KEY;         // OBJECT_NAMED_STRING  (iter#30 fusion)
      case 51 -> OBJNAMEDNULL_PARENT_KEY;        // OBJECT_NAMED_NULL    (iter#30 fusion)
      case 52 -> OBJNAMEDOBJ_PARENT_KEY;         // OBJECT_NAMED_OBJECT  (Phase 1 reserved)
      case 53 -> OBJNAMEDARR_PARENT_KEY;         // OBJECT_NAMED_ARRAY   (Phase 1 reserved)
      default -> -1;
    };
  }

  /**
   * Returns the field-table index of the PATH_NODE_KEY field for a given NodeKind,
   * or {@code -1} if the kind has no pathNodeKey (primitive VALUE records, OBJECT,
   * ARRAY w/o pathSummary, document roots where it's absent).
   *
   * <p>Used by the pathNodeKey dict region encoder to locate the per-record varint on
   * the slotted-page heap. In record-shaped workloads each page typically carries
   * only 3-10 distinct pathNodeKey values across its 1000+ slots, so dict encoding
   * beats per-record varints by 2-3× on disk.
   */
  public static int pathNodeKeyFieldIndexForKind(final int kindId) {
    return switch (kindId) {
      case 1 -> ELEM_PATH_NODE_KEY;              // ELEMENT
      case 2 -> ATTR_PATH_NODE_KEY;              // ATTRIBUTE
      case 3 -> -1;                              // TEXT (no pathNodeKey)
      case 7 -> PI_PATH_NODE_KEY;                // PROCESSING_INSTRUCTION
      case 8 -> -1;                              // COMMENT
      case 9 -> -1;                              // XML_DOCUMENT_ROOT
      case 13 -> NS_PATH_NODE_KEY;               // NAMESPACE
      case 24 -> -1;                             // OBJECT (no pathNodeKey in layout)
      case 25 -> ARRAY_PATH_NODE_KEY;            // ARRAY
      case 27 -> -1;                             // BOOLEAN_VALUE
      case 28 -> -1;                             // NUMBER_VALUE
      case 29 -> -1;                             // NULL_VALUE
      case 30 -> -1;                             // STRING_VALUE
      case 31 -> -1;                             // JSON_DOCUMENT_ROOT
      case 48 -> OBJNAMEDBOOL_PATH_NODE_KEY;     // OBJECT_NAMED_BOOLEAN (iter#30 fusion)
      case 49 -> OBJNAMEDNUM_PATH_NODE_KEY;      // OBJECT_NAMED_NUMBER
      case 50 -> OBJNAMEDSTR_PATH_NODE_KEY;      // OBJECT_NAMED_STRING
      case 51 -> OBJNAMEDNULL_PATH_NODE_KEY;     // OBJECT_NAMED_NULL
      case 52 -> OBJNAMEDOBJ_PATH_NODE_KEY;      // OBJECT_NAMED_OBJECT  (Phase 1 reserved)
      case 53 -> OBJNAMEDARR_PATH_NODE_KEY;      // OBJECT_NAMED_ARRAY   (Phase 1 reserved)
      default -> -1;
    };
  }

  /**
   * Returns the field-table index of the NAME_KEY field for a fused
   * {@code OBJECT_NAMED_*} record (kindIds 48-53), or {@code -1} for any other
   * kind (which has no fused-style nameKey field).
   *
   * <p>Used by the Lever 4 nameKey-elision pre-scan to locate each fused slot's
   * nameKey varint within the data region. Primitive-fused leaves (kindIds 48-51)
   * place the field at index 3; structural-fused (kindIds 52-53) place it at
   * index 5 (after firstChild/lastChild keys). Other XML/JSON kinds use distinct
   * naming fields (LOCAL_NAME / PREFIX / URI) which Lever 4 does not target —
   * those kinds return {@code -1}.
   */
  public static int nameKeyFieldIndexForKind(final int kindId) {
    return switch (kindId) {
      case 48 -> OBJNAMEDBOOL_NAME_KEY;          // OBJECT_NAMED_BOOLEAN  (idx 3)
      case 49 -> OBJNAMEDNUM_NAME_KEY;           // OBJECT_NAMED_NUMBER   (idx 3)
      case 50 -> OBJNAMEDSTR_NAME_KEY;           // OBJECT_NAMED_STRING   (idx 3)
      case 51 -> OBJNAMEDNULL_NAME_KEY;          // OBJECT_NAMED_NULL     (idx 3)
      case 52 -> OBJNAMEDOBJ_NAME_KEY;           // OBJECT_NAMED_OBJECT   (idx 5, Phase 1 reserved)
      case 53 -> OBJNAMEDARR_NAME_KEY;           // OBJECT_NAMED_ARRAY    (idx 5, Phase 1 reserved)
      default -> -1;
    };
  }

  /**
   * Returns the field-table index of the HASH field for a given NodeKind, or
   * {@code -1} if the kind stores no hash (records that aren't part of the
   * rolling hash tree — e.g. child values of an object key).
   *
   * <p>Used by the columnar structural-key extractor to locate the per-record
   * fixed-width hash bytes on the slotted-page heap without a full record parse.
   */
  public static int hashFieldIndexForKind(final int kindId) {
    return switch (kindId) {
      case 1 -> ELEM_HASH;                       // ELEMENT
      case 2 -> -1;                              // ATTRIBUTE (no hash field)
      case 3 -> -1;                              // TEXT (no hash field in offset table)
      case 7 -> -1;                              // PROCESSING_INSTRUCTION (no hash field)
      case 8 -> -1;                              // COMMENT (no hash field)
      case 9 -> XDOCROOT_HASH;                   // XML_DOCUMENT_ROOT
      case 13 -> NS_HASH;                        // NAMESPACE
      case 24 -> OBJECT_HASH;                    // OBJECT
      case 25 -> ARRAY_HASH;                     // ARRAY
      case 27 -> -1;                             // BOOLEAN_VALUE (no hash field)
      case 28 -> -1;                             // NUMBER_VALUE (no hash field)
      case 29 -> -1;                             // NULL_VALUE (no hash field)
      case 30 -> -1;                             // STRING_VALUE (no hash field)
      case 31 -> JDOCROOT_HASH;                  // JSON_DOCUMENT_ROOT
      // Note: kindIds 48-51 (iter#30 fused primitives) are intentionally NOT enumerated
      // here even though their on-disk record DOES carry a hash field — their on-page hash
      // elision is currently disabled by design. Adding them here would silently enable
      // elision for iter#30 records and is out of scope for Phase 1.
      case 52 -> OBJNAMEDOBJ_HASH;               // OBJECT_NAMED_OBJECT (Phase 1 reserved)
      case 53 -> OBJNAMEDARR_HASH;               // OBJECT_NAMED_ARRAY  (Phase 1 reserved)
      default -> -1;
    };
  }
}
