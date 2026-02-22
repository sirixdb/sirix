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
 * <p>ALL fields are always present in the new unified format, including hash (0 when unused)
 * and childCount/descendantCount (0 when unused). This eliminates conditional field logic
 * and makes the offset table size fixed per NodeKind.
 */
public final class NodeFieldLayout {

  private NodeFieldLayout() {
    throw new AssertionError("Utility class");
  }

  /** Fixed width of hash field in bytes (always 8). */
  public static final int HASH_WIDTH = Long.BYTES;

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

  // ==================== OBJECT_KEY NODE (10 fields) ====================

  /** Total field count for OBJECT_KEY nodes. */
  public static final int OBJECT_KEY_FIELD_COUNT = 10;

  public static final int OBJKEY_PARENT_KEY = 0;
  public static final int OBJKEY_RIGHT_SIB_KEY = 1;
  public static final int OBJKEY_LEFT_SIB_KEY = 2;
  public static final int OBJKEY_FIRST_CHILD_KEY = 3;
  public static final int OBJKEY_NAME_KEY = 4;
  public static final int OBJKEY_PATH_NODE_KEY = 5;
  public static final int OBJKEY_PREV_REVISION = 6;
  public static final int OBJKEY_LAST_MOD_REVISION = 7;
  public static final int OBJKEY_HASH = 8;
  public static final int OBJKEY_DESCENDANT_COUNT = 9;

  // ==================== STRING_VALUE NODE (7 fields + payload) ====================

  /** Total field count for STRING_VALUE nodes (excluding payload). */
  public static final int STRING_VALUE_FIELD_COUNT = 7;

  public static final int STRVAL_PARENT_KEY = 0;
  public static final int STRVAL_RIGHT_SIB_KEY = 1;
  public static final int STRVAL_LEFT_SIB_KEY = 2;
  public static final int STRVAL_PREV_REVISION = 3;
  public static final int STRVAL_LAST_MOD_REVISION = 4;
  public static final int STRVAL_HASH = 5;
  /** Points to the start of [isCompressed:1][valueLength:varint][value:bytes]. */
  public static final int STRVAL_PAYLOAD = 6;

  // ==================== NUMBER_VALUE NODE (7 fields + payload) ====================

  /** Total field count for NUMBER_VALUE nodes (excluding payload). */
  public static final int NUMBER_VALUE_FIELD_COUNT = 7;

  public static final int NUMVAL_PARENT_KEY = 0;
  public static final int NUMVAL_RIGHT_SIB_KEY = 1;
  public static final int NUMVAL_LEFT_SIB_KEY = 2;
  public static final int NUMVAL_PREV_REVISION = 3;
  public static final int NUMVAL_LAST_MOD_REVISION = 4;
  public static final int NUMVAL_HASH = 5;
  /** Points to the start of [numberType:1][numberData:variable]. */
  public static final int NUMVAL_PAYLOAD = 6;

  // ==================== BOOLEAN_VALUE NODE (7 fields) ====================

  /** Total field count for BOOLEAN_VALUE nodes. */
  public static final int BOOLEAN_VALUE_FIELD_COUNT = 7;

  public static final int BOOLVAL_PARENT_KEY = 0;
  public static final int BOOLVAL_RIGHT_SIB_KEY = 1;
  public static final int BOOLVAL_LEFT_SIB_KEY = 2;
  public static final int BOOLVAL_PREV_REVISION = 3;
  public static final int BOOLVAL_LAST_MOD_REVISION = 4;
  public static final int BOOLVAL_VALUE = 5;
  public static final int BOOLVAL_HASH = 6;

  // ==================== NULL_VALUE NODE (6 fields) ====================

  /** Total field count for NULL_VALUE nodes. */
  public static final int NULL_VALUE_FIELD_COUNT = 6;

  public static final int NULLVAL_PARENT_KEY = 0;
  public static final int NULLVAL_RIGHT_SIB_KEY = 1;
  public static final int NULLVAL_LEFT_SIB_KEY = 2;
  public static final int NULLVAL_PREV_REVISION = 3;
  public static final int NULLVAL_LAST_MOD_REVISION = 4;
  public static final int NULLVAL_HASH = 5;

  // ==================== OBJECT_STRING_VALUE NODE (5 fields + payload) ====================

  /** Total field count for OBJECT_STRING_VALUE nodes (excluding payload). */
  public static final int OBJECT_STRING_VALUE_FIELD_COUNT = 5;

  public static final int OBJSTRVAL_PARENT_KEY = 0;
  public static final int OBJSTRVAL_PREV_REVISION = 1;
  public static final int OBJSTRVAL_LAST_MOD_REVISION = 2;
  public static final int OBJSTRVAL_HASH = 3;
  /** Points to the start of [isCompressed:1][valueLength:varint][value:bytes]. */
  public static final int OBJSTRVAL_PAYLOAD = 4;

  // ==================== OBJECT_NUMBER_VALUE NODE (5 fields + payload) ====================

  /** Total field count for OBJECT_NUMBER_VALUE nodes (excluding payload). */
  public static final int OBJECT_NUMBER_VALUE_FIELD_COUNT = 5;

  public static final int OBJNUMVAL_PARENT_KEY = 0;
  public static final int OBJNUMVAL_PREV_REVISION = 1;
  public static final int OBJNUMVAL_LAST_MOD_REVISION = 2;
  public static final int OBJNUMVAL_HASH = 3;
  /** Points to the start of [numberType:1][numberData:variable]. */
  public static final int OBJNUMVAL_PAYLOAD = 4;

  // ==================== OBJECT_BOOLEAN_VALUE NODE (5 fields) ====================

  /** Total field count for OBJECT_BOOLEAN_VALUE nodes. */
  public static final int OBJECT_BOOLEAN_VALUE_FIELD_COUNT = 5;

  public static final int OBJBOOLVAL_PARENT_KEY = 0;
  public static final int OBJBOOLVAL_PREV_REVISION = 1;
  public static final int OBJBOOLVAL_LAST_MOD_REVISION = 2;
  public static final int OBJBOOLVAL_VALUE = 3;
  public static final int OBJBOOLVAL_HASH = 4;

  // ==================== OBJECT_NULL_VALUE NODE (4 fields) ====================

  /** Total field count for OBJECT_NULL_VALUE nodes. */
  public static final int OBJECT_NULL_VALUE_FIELD_COUNT = 4;

  public static final int OBJNULLVAL_PARENT_KEY = 0;
  public static final int OBJNULLVAL_PREV_REVISION = 1;
  public static final int OBJNULLVAL_LAST_MOD_REVISION = 2;
  public static final int OBJNULLVAL_HASH = 3;

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

  // ==================== FIELD COUNT LOOKUP ====================

  /**
   * Get the field count for a given NodeKind.
   *
   * @param kindId the NodeKind byte ID
   * @return the field count, or -1 if unknown/unsupported
   */
  public static int fieldCountForKind(final int kindId) {
    return switch (kindId) {
      case 24 -> OBJECT_FIELD_COUNT;           // OBJECT
      case 25 -> ARRAY_FIELD_COUNT;            // ARRAY
      case 26 -> OBJECT_KEY_FIELD_COUNT;       // OBJECT_KEY
      case 27 -> BOOLEAN_VALUE_FIELD_COUNT;    // BOOLEAN_VALUE
      case 28 -> NUMBER_VALUE_FIELD_COUNT;     // NUMBER_VALUE
      case 29 -> NULL_VALUE_FIELD_COUNT;       // NULL_VALUE
      case 30 -> STRING_VALUE_FIELD_COUNT;     // STRING_VALUE
      case 40 -> OBJECT_STRING_VALUE_FIELD_COUNT;   // OBJECT_STRING_VALUE
      case 41 -> OBJECT_BOOLEAN_VALUE_FIELD_COUNT;  // OBJECT_BOOLEAN_VALUE
      case 42 -> OBJECT_NUMBER_VALUE_FIELD_COUNT;   // OBJECT_NUMBER_VALUE
      case 43 -> OBJECT_NULL_VALUE_FIELD_COUNT;     // OBJECT_NULL_VALUE
      case 31 -> JSON_DOCUMENT_ROOT_FIELD_COUNT;    // JSON_DOCUMENT_ROOT
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
}
