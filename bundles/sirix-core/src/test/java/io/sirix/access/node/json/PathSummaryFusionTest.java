package io.sirix.access.node.json;

import io.brackit.query.atomic.QNm;
import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.api.Axis;
import io.sirix.axis.DescendantAxis;
import io.sirix.index.path.summary.PathSummaryReader;
import io.sirix.node.NodeKind;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * PathSummary correctness under default-on iter#30 + iter#32 fusion.
 *
 * <p>Verifies the design contracts captured in {@code PathSummaryWriter}:
 * <ul>
 *   <li>Fused {@code OBJECT_NAMED_*} primitive leaves register exactly one path-summary entry
 *       under the canonical {@code OBJECT_NAMED_OBJECT} pathKind, reusing the slot that an
 *       unfused {@code OBJECT_KEY} would have populated.</li>
 *   <li>Fused {@code OBJECT_NAMED_ARRAY} structural records bump the path-summary entry for
 *       <em>both</em> the field-name level and the underlying array level (Phase 2 mirror).</li>
 *   <li>Reference counts increment on duplicate-field insert and decrement on remove; when the
 *       last reference goes away the entry is gone too.</li>
 *   <li>{@code setObjectKeyName} on a fused record correctly migrates references between the
 *       old and new path-summary entries.</li>
 * </ul>
 */
public final class PathSummaryFusionTest {

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  /**
   * Walk the path summary and snapshot {@code (qname → references)} for level-1 entries.
   * Asserts each is registered with the canonical fused pathKind {@link NodeKind#OBJECT_NAMED_OBJECT}.
   */
  private static Map<String, Integer> level1Refs(final PathSummaryReader summary) {
    final Map<String, Integer> out = new HashMap<>();
    final Axis axis = new DescendantAxis(summary);
    while (axis.hasNext()) {
      axis.nextLong();
      if (summary.getLevel() != 1) {
        continue;
      }
      final NodeKind pathKind = summary.getPathKind();
      assertTrue(pathKind == NodeKind.OBJECT_NAMED_OBJECT || pathKind == NodeKind.ARRAY,
          "level-1 path-summary entries must use the canonical fused pathKind "
              + "OBJECT_NAMED_OBJECT (or ARRAY for array-typed children); got: " + pathKind);
      final QNm name = summary.getName();
      assertNotNull(name, "level-1 path-summary entry has null name at nodeKey="
          + summary.getNodeKey());
      out.put(name.getLocalName(), summary.getReferences());
    }
    return out;
  }

  @Test
  void fusedPrimitiveLeaves_registerCanonicalOBJECT_NAMED_OBJECT_pathKind() {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      try (final var wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(
            JsonShredder.createStringReader("{\"s\":\"v\",\"n\":42,\"b\":true,\"z\":null}"));
        wtx.commit();
      }
      try (final var summary = session.openPathSummary()) {
        final Map<String, Integer> refs = level1Refs(summary);
        assertEquals(4, refs.size(),
            "expected exactly 4 level-1 path entries for the 4 fused primitive fields, got: " + refs);
        assertEquals(1, refs.get("s"), "string-valued field 's' refcount");
        assertEquals(1, refs.get("n"), "number-valued field 'n' refcount");
        assertEquals(1, refs.get("b"), "boolean-valued field 'b' refcount");
        assertEquals(1, refs.get("z"), "null-valued field 'z' refcount");
      }
    }
  }

  @Test
  void duplicateFieldName_acrossSiblingObjects_increments_references() {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      try (final var wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(
            JsonShredder.createStringReader("[{\"name\":\"a\"},{\"name\":\"b\"}]"));
        wtx.commit();
      }
      try (final var summary = session.openPathSummary()) {
        final Axis axis = new DescendantAxis(summary);
        int nameRefs = -1;
        while (axis.hasNext()) {
          axis.nextLong();
          final QNm name = summary.getName();
          if (name != null && "name".equals(name.getLocalName())) {
            assertEquals(NodeKind.OBJECT_NAMED_OBJECT, summary.getPathKind(),
                "fused string field's path entry must use OBJECT_NAMED_OBJECT");
            nameRefs = summary.getReferences();
          }
        }
        assertEquals(2, nameRefs,
            "two fused OBJECT_NAMED_STRING records sharing the same field path must register references=2");
      }
    }
  }

  @Test
  void removingFusedField_decrements_pathSummaryReferences() {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      try (final var wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(
            JsonShredder.createStringReader("[{\"k\":1},{\"k\":2}]"));
        wtx.commit();
        // doc(0) → array(1) → first object(2) → fused "k=1"(3).
        wtx.moveTo(2);
        wtx.moveToFirstChild();
        assertEquals(NodeKind.OBJECT_NAMED_NUMBER, wtx.getKind());
        wtx.remove();
        wtx.commit();
      }
      try (final var summary = session.openPathSummary()) {
        int kRefs = -1;
        final Axis axis = new DescendantAxis(summary);
        while (axis.hasNext()) {
          axis.nextLong();
          final QNm name = summary.getName();
          if (name != null && "k".equals(name.getLocalName())) {
            kRefs = summary.getReferences();
          }
        }
        assertEquals(1, kRefs,
            "after removing one of two fused OBJECT_NAMED_NUMBER records, references must drop to 1");
      }
    }
  }

  @Test
  void removingLastFusedField_collapses_pathSummaryEntry() {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      try (final var wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(
            JsonShredder.createStringReader("{\"only\":\"once\"}"));
        wtx.commit();
        wtx.moveTo(2);
        assertEquals(NodeKind.OBJECT_NAMED_STRING, wtx.getKind());
        wtx.remove();
        wtx.commit();
      }
      try (final var summary = session.openPathSummary()) {
        final Axis axis = new DescendantAxis(summary);
        while (axis.hasNext()) {
          axis.nextLong();
          final QNm name = summary.getName();
          if (name != null && "only".equals(name.getLocalName())) {
            // PathSummaryWriter contract: when references drops to 0 the entry should be
            // removed. If it ever resurfaces with refs=0, the path-summary garbage-collection
            // path is broken — fail loudly.
            throw new AssertionError(
                "path-summary entry for 'only' should have been removed when its last reference "
                    + "went away; instead found refs=" + summary.getReferences());
          }
        }
      }
    }
  }

  @Test
  void fusedStructural_OBJECT_NAMED_ARRAY_registers_pathSummaryEntries_onBothLevels() {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      try (final var wtx = session.beginNodeTrx()) {
        // Phase 2 structural fusion produces an OBJECT_NAMED_ARRAY for {"items": [...]}.
        // The path summary must mirror the entry at BOTH the field-name level (OBJECT_NAMED_OBJECT
        // pathKind under "items") AND the underlying array level (ARRAY pathKind for elements).
        wtx.insertSubtreeAsFirstChild(
            JsonShredder.createStringReader("{\"items\":[1,2,3]}"));
        wtx.commit();
      }
      try (final var summary = session.openPathSummary()) {
        final Axis axis = new DescendantAxis(summary);
        boolean sawItemsField = false;
        boolean sawArrayLevel = false;
        while (axis.hasNext()) {
          axis.nextLong();
          final QNm name = summary.getName();
          if (name != null && "items".equals(name.getLocalName())) {
            assertEquals(NodeKind.OBJECT_NAMED_OBJECT, summary.getPathKind(),
                "field 'items' must be registered under canonical OBJECT_NAMED_OBJECT pathKind");
            assertTrue(summary.getReferences() >= 1,
                "field 'items' must have at least one reference, got: " + summary.getReferences());
            sawItemsField = true;
          }
          if (summary.getPathKind() == NodeKind.ARRAY && summary.getLevel() == 2) {
            // Level-2 ARRAY entry below "items" field — the structural-fusion mirror.
            sawArrayLevel = true;
          }
        }
        assertTrue(sawItemsField, "path-summary lacks field-level entry for fused 'items'");
        assertTrue(sawArrayLevel,
            "path-summary lacks the array-level mirror entry that Phase 2 structural fusion must "
                + "register under OBJECT_NAMED_ARRAY");
      }
    }
  }

  @Test
  void renamingFusedField_migrates_pathSummaryReferences() {
    try (final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
         final var session = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      try (final var wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(
            JsonShredder.createStringReader("{\"oldName\":\"v\"}"));
        wtx.commit();
        wtx.moveTo(2);
        assertEquals(NodeKind.OBJECT_NAMED_STRING, wtx.getKind());
        wtx.setObjectKeyName("newName");
        wtx.commit();
      }
      try (final var summary = session.openPathSummary()) {
        final Axis axis = new DescendantAxis(summary);
        Integer oldRefs = null;
        Integer newRefs = null;
        while (axis.hasNext()) {
          axis.nextLong();
          final QNm name = summary.getName();
          if (name == null) {
            continue;
          }
          if ("oldName".equals(name.getLocalName())) {
            oldRefs = summary.getReferences();
          } else if ("newName".equals(name.getLocalName())) {
            newRefs = summary.getReferences();
            assertEquals(NodeKind.OBJECT_NAMED_OBJECT, summary.getPathKind(),
                "renamed field 'newName' must register under canonical fused pathKind");
          }
        }
        assertNull(oldRefs,
            "after setObjectKeyName, the old 'oldName' path entry must be gone, but found refs="
                + oldRefs);
        assertEquals(Integer.valueOf(1), newRefs,
            "after setObjectKeyName, the new 'newName' path entry must hold the migrated reference");
      }
    }
  }
}
