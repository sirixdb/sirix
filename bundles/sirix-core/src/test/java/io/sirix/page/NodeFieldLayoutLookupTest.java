/*
 * Copyright (c) 2026, Sirix Contributors
 *
 * All rights reserved.
 */

package io.sirix.page;

import io.sirix.node.NodeKind;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The per-kind lookups answer for every kind id, not just the ones a page happens to contain.
 *
 * <p>They are read from flat byte tables rather than evaluated as switches, because the page
 * encoder calls them several times per record and an indirect jump per call is dead weight on a
 * page that mixes kinds. The tables are built from the switches at class-initialisation time, so
 * the risk is not that a value was mistyped — it is that the tabulation itself is wrong at the
 * edges: a kind id past the end of the table, a negative id, a value that does not survive being
 * narrowed to a byte, or a sentinel that loses its sign on the way back out.
 *
 * <p>So this sweeps the whole {@code int} boundary region rather than sampling the kinds in use.
 * Each lookup must agree with the layout constants for every kind that has the field, and must
 * return the {@code -1} sentinel everywhere else — including for ids no {@link NodeKind} claims.
 */
@DisplayName("Per-kind field lookups")
final class NodeFieldLayoutLookupTest {

  /** Field counts, one per kind that has a layout. */
  @Test
  @DisplayName("field counts match the layout constants for every kind that has one")
  void fieldCounts() {
    assertEquals(NodeFieldLayout.ELEMENT_FIELD_COUNT, NodeFieldLayout.fieldCountForKind(1));
    assertEquals(NodeFieldLayout.ATTRIBUTE_FIELD_COUNT, NodeFieldLayout.fieldCountForKind(2));
    assertEquals(NodeFieldLayout.TEXT_FIELD_COUNT, NodeFieldLayout.fieldCountForKind(3));
    assertEquals(NodeFieldLayout.PI_FIELD_COUNT, NodeFieldLayout.fieldCountForKind(7));
    assertEquals(NodeFieldLayout.COMMENT_FIELD_COUNT, NodeFieldLayout.fieldCountForKind(8));
    assertEquals(NodeFieldLayout.XML_DOCUMENT_ROOT_FIELD_COUNT, NodeFieldLayout.fieldCountForKind(9));
    assertEquals(NodeFieldLayout.NAMESPACE_FIELD_COUNT, NodeFieldLayout.fieldCountForKind(13));
    assertEquals(NodeFieldLayout.OBJECT_FIELD_COUNT, NodeFieldLayout.fieldCountForKind(24));
    assertEquals(NodeFieldLayout.ARRAY_FIELD_COUNT, NodeFieldLayout.fieldCountForKind(25));
    assertEquals(NodeFieldLayout.BOOLEAN_VALUE_FIELD_COUNT, NodeFieldLayout.fieldCountForKind(27));
    assertEquals(NodeFieldLayout.NUMBER_VALUE_FIELD_COUNT, NodeFieldLayout.fieldCountForKind(28));
    assertEquals(NodeFieldLayout.NULL_VALUE_FIELD_COUNT, NodeFieldLayout.fieldCountForKind(29));
    assertEquals(NodeFieldLayout.STRING_VALUE_FIELD_COUNT, NodeFieldLayout.fieldCountForKind(30));
    assertEquals(NodeFieldLayout.JSON_DOCUMENT_ROOT_FIELD_COUNT, NodeFieldLayout.fieldCountForKind(31));
    assertEquals(NodeFieldLayout.OBJECT_NAMED_BOOLEAN_FIELD_COUNT, NodeFieldLayout.fieldCountForKind(48));
    assertEquals(NodeFieldLayout.OBJECT_NAMED_NUMBER_FIELD_COUNT, NodeFieldLayout.fieldCountForKind(49));
    assertEquals(NodeFieldLayout.OBJECT_NAMED_STRING_FIELD_COUNT, NodeFieldLayout.fieldCountForKind(50));
    assertEquals(NodeFieldLayout.OBJECT_NAMED_NULL_FIELD_COUNT, NodeFieldLayout.fieldCountForKind(51));
    assertEquals(NodeFieldLayout.OBJECT_NAMED_OBJECT_FIELD_COUNT, NodeFieldLayout.fieldCountForKind(52));
    assertEquals(NodeFieldLayout.OBJECT_NAMED_ARRAY_FIELD_COUNT, NodeFieldLayout.fieldCountForKind(53));
  }

  /** The overload taking a {@link NodeKind} must agree with the one taking its id. */
  @Test
  @DisplayName("the NodeKind overload agrees with the id overload for every kind")
  void nodeKindOverloadAgrees() {
    for (final NodeKind kind : NodeKind.values()) {
      assertEquals(NodeFieldLayout.fieldCountForKind(kind.getId()),
          NodeFieldLayout.fieldCountForKind(kind),
          () -> "field count disagrees between overloads for " + kind);
    }
  }

  /** Field indices, spot-checked against the layout constants for kinds that carry the field. */
  @Test
  @DisplayName("field indices match the layout constants")
  void fieldIndices() {
    assertEquals(NodeFieldLayout.ARRAY_PARENT_KEY, NodeFieldLayout.parentKeyFieldIndexForKind(25));
    assertEquals(NodeFieldLayout.OBJECT_PARENT_KEY, NodeFieldLayout.parentKeyFieldIndexForKind(24));
    assertEquals(NodeFieldLayout.ARRAY_FIRST_CHILD_KEY,
        NodeFieldLayout.firstChildKeyFieldIndexForKind(25));
    assertEquals(NodeFieldLayout.ARRAY_LEFT_SIB_KEY,
        NodeFieldLayout.leftSiblingKeyFieldIndexForKind(25));
    assertEquals(NodeFieldLayout.ARRAY_RIGHT_SIB_KEY,
        NodeFieldLayout.rightSiblingKeyFieldIndexForKind(25));
    assertEquals(NodeFieldLayout.ARRAY_PATH_NODE_KEY,
        NodeFieldLayout.pathNodeKeyFieldIndexForKind(25));
    assertEquals(NodeFieldLayout.ARRAY_HASH, NodeFieldLayout.hashFieldIndexForKind(25));

    // All four primitive-fused kinds put the nameKey at the same index; the encoder's
    // cheap-reject pre-pass hardcodes it, so a disagreement here would corrupt pages.
    for (int kindId = 48; kindId <= 51; kindId++) {
      assertEquals(NodeFieldLayout.FUSED_PRIMITIVE_NAME_KEY_FIELD,
          NodeFieldLayout.nameKeyFieldIndexForKind(kindId),
          "primitive-fused kind " + kindId + " must place its nameKey where the encoder expects");
    }
    assertEquals(NodeFieldLayout.FUSED_STRUCTURAL_NAME_KEY_FIELD,
        NodeFieldLayout.nameKeyFieldIndexForKind(52));
    assertEquals(NodeFieldLayout.FUSED_STRUCTURAL_NAME_KEY_FIELD,
        NodeFieldLayout.nameKeyFieldIndexForKind(53));
  }

  /**
   * Kinds without a given field, and ids no kind claims, both report the {@code -1} sentinel.
   *
   * <p>Callers branch on {@code < 0} to mean "this record has no such field and nothing should be
   * stripped from it". A table that returned 0 instead — the natural value for an unwritten byte —
   * would read as "field 0", and the encoder would strip bytes out of a record that never had the
   * field.
   */
  @Test
  @DisplayName("absent fields and unknown kinds report -1")
  void absentFieldsReportSentinel() {
    // TEXT has no pathNodeKey; ATTRIBUTE, TEXT and the standalone primitives have no hash.
    assertEquals(-1, NodeFieldLayout.pathNodeKeyFieldIndexForKind(3));
    assertEquals(-1, NodeFieldLayout.hashFieldIndexForKind(2));
    assertEquals(-1, NodeFieldLayout.hashFieldIndexForKind(30));
    // Only the fused OBJECT_NAMED_* kinds have a fused nameKey.
    assertEquals(-1, NodeFieldLayout.nameKeyFieldIndexForKind(25));
    // Document roots have no parent.
    assertEquals(-1, NodeFieldLayout.parentKeyFieldIndexForKind(31));

    for (final int unknown : new int[] { 0, 4, 5, 6, 10, 26, 32, 47, 54, 100, 255 }) {
      assertEquals(-1, NodeFieldLayout.fieldCountForKind(unknown),
          "unassigned kind id " + unknown + " must not claim a layout");
      assertEquals(-1, NodeFieldLayout.parentKeyFieldIndexForKind(unknown));
      assertEquals(-1, NodeFieldLayout.firstChildKeyFieldIndexForKind(unknown));
      assertEquals(-1, NodeFieldLayout.leftSiblingKeyFieldIndexForKind(unknown));
      assertEquals(-1, NodeFieldLayout.rightSiblingKeyFieldIndexForKind(unknown));
      assertEquals(-1, NodeFieldLayout.pathNodeKeyFieldIndexForKind(unknown));
      assertEquals(-1, NodeFieldLayout.nameKeyFieldIndexForKind(unknown));
      assertEquals(-1, NodeFieldLayout.hashFieldIndexForKind(unknown));
    }
  }

  /**
   * Ids outside the table's range answer {@code -1} rather than aliasing back into it.
   *
   * <p>The tables are indexed directly by kind id. Masking the index instead of range-checking it
   * would be a byte cheaper and would fold 256 onto 0 and 281 onto ELEMENT — handing a caller a
   * real field index for an id that has no layout at all.
   */
  @Test
  @DisplayName("out-of-range ids do not alias into the table")
  void outOfRangeIdsDoNotAlias() {
    for (final int outOfRange : new int[] { -1, -256, 256, 257, 281, 512, Integer.MIN_VALUE,
        Integer.MAX_VALUE }) {
      assertEquals(-1, NodeFieldLayout.fieldCountForKind(outOfRange),
          "kind id " + outOfRange + " is outside the table and must not resolve to a layout");
      assertEquals(-1, NodeFieldLayout.parentKeyFieldIndexForKind(outOfRange));
      assertEquals(-1, NodeFieldLayout.firstChildKeyFieldIndexForKind(outOfRange));
      assertEquals(-1, NodeFieldLayout.leftSiblingKeyFieldIndexForKind(outOfRange));
      assertEquals(-1, NodeFieldLayout.rightSiblingKeyFieldIndexForKind(outOfRange));
      assertEquals(-1, NodeFieldLayout.pathNodeKeyFieldIndexForKind(outOfRange));
      assertEquals(-1, NodeFieldLayout.nameKeyFieldIndexForKind(outOfRange));
      assertEquals(-1, NodeFieldLayout.hashFieldIndexForKind(outOfRange));
    }
  }
}
