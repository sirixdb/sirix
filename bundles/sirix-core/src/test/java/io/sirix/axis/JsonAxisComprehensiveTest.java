package io.sirix.axis;

import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.api.visitor.JsonNodeVisitor;
import io.sirix.api.visitor.VisitResult;
import io.sirix.api.visitor.VisitResultType;
import io.sirix.axis.filter.FilterAxis;
import io.sirix.axis.filter.json.ObjectFilter;
import io.sirix.axis.filter.json.StringValueFilter;
import io.sirix.axis.visitor.VisitorDescendantAxis;
import io.sirix.node.NodeKind;
import io.sirix.node.immutable.json.ImmutableArrayNode;
import io.sirix.node.immutable.json.ImmutableBooleanNode;
import io.sirix.node.immutable.json.ImmutableJsonDocumentRootNode;
import io.sirix.node.immutable.json.ImmutableNullNode;
import io.sirix.node.immutable.json.ImmutableNumberNode;
import io.sirix.node.immutable.json.ImmutableObjectNode;
import io.sirix.node.immutable.json.ImmutableStringNode;
import io.sirix.node.json.ObjectNamedBooleanNode;
import io.sirix.node.json.ObjectNamedNullNode;
import io.sirix.node.json.ObjectNamedNumberNode;
import io.sirix.node.json.ObjectNamedStringNode;
import io.sirix.utils.JsonDocumentCreator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Comprehensive tests for JSON navigation axes.
 *
 * <p>Test document structure (node keys 0-20) — iter#32 fusion collapses each
 * {@code (key, primitive)} pair onto a single OBJECT_NAMED_* record, eliminating
 * the legacy OBJECT_KEY + primitive_VALUE child pair.
 * <pre>
 * 0: JSON_DOCUMENT
 * 1: OBJECT (root)
 *   2: OBJECT_KEY "foo"
 *     3: ARRAY
 *       4: STRING_VALUE "bar"
 *       5: NULL_VALUE
 *       6: NUMBER_VALUE 2.33
 *   7: OBJECT_KEY "bar"
 *     8: OBJECT
 *        9: OBJECT_NAMED_STRING "hello":"world"
 *       10: OBJECT_NAMED_BOOLEAN "helloo":true
 *   11: OBJECT_NAMED_STRING "baz":"hello"
 *   12: OBJECT_KEY "tada"
 *     13: ARRAY
 *       14: OBJECT {"foo":"bar"}
 *         15: OBJECT_NAMED_STRING "foo":"bar"
 *       16: OBJECT {"baz":false}
 *         17: OBJECT_NAMED_BOOLEAN "baz":false
 *       18: STRING_VALUE "boo"
 *       19: OBJECT (empty)
 *       20: ARRAY (empty)
 * </pre>
 */
final class JsonAxisComprehensiveTest {

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    if (openSession != null && !openSession.isClosed()) {
      openSession.close();
      openSession = null;
    }
    JsonTestHelper.deleteEverything();
  }

  private JsonResourceSession openSession;

  /**
   * Creates the standard test document and returns a read-only transaction positioned at the
   * document root. The session is tracked and closed in tearDown.
   */
  private JsonNodeReadOnlyTrx openReadTrx() {
    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      JsonDocumentCreator.create(wtx);
      wtx.commit();
    }

    openSession = database.beginResourceSession(JsonTestHelper.RESOURCE);
    final JsonNodeReadOnlyTrx rtx = openSession.beginNodeReadOnlyTrx();
    rtx.moveToDocumentRoot();
    return rtx;
  }

  // ---------- 1. DescendantAxis from document root ----------

  @Test
  void testDescendantAxisFromRoot() {
    try (final var rtx = openReadTrx()) {
      rtx.moveToDocumentRoot();

      int count = 0;
      final var axis = new DescendantAxis(rtx);
      while (axis.hasNext()) {
        axis.nextLong();
        count++;
      }

      // 17 descendants: nodes 1-17 after iter#32 structural fusion (excludes document root).
      assertEquals(17, count);
    }
  }

  // ---------- 2. DescendantAxis from subtree ----------

  @Test
  void testDescendantAxisFromSubtree() {
    try (final var rtx = openReadTrx()) {
      // iter#32 fusion: "foo" OBJECT_KEY+ARRAY collapsed into a single OBJECT_NAMED_ARRAY at
      // key 2; the array body is the fused record itself, so we descend from key 2.
      rtx.moveTo(2);
      assertEquals(NodeKind.OBJECT_NAMED_ARRAY, rtx.getKind());

      int count = 0;
      final var axis = new DescendantAxis(rtx);
      while (axis.hasNext()) {
        axis.nextLong();
        count++;
      }

      // Descendants of fused-array node 2: STRING_VALUE(3), NULL_VALUE(4), NUMBER_VALUE(5).
      assertEquals(3, count);
    }
  }

  // ---------- 3. DescendantAxis from leaf ----------

  @Test
  void testDescendantAxisFromLeaf() {
    try (final var rtx = openReadTrx()) {
      // iter#32 fusion: STRING_VALUE("bar") moved from key 4 -> 3 because foo's
      // OBJECT_KEY+ARRAY collapsed into one OBJECT_NAMED_ARRAY at key 2.
      rtx.moveTo(3);
      assertEquals(NodeKind.STRING_VALUE, rtx.getKind());

      int count = 0;
      final var axis = new DescendantAxis(rtx);
      while (axis.hasNext()) {
        axis.nextLong();
        count++;
      }

      assertEquals(0, count);
    }
  }

  // ---------- 4. DescendantAxis with IncludeSelf ----------

  @Test
  void testDescendantAxisIncludeSelf() {
    try (final var rtx = openReadTrx()) {
      rtx.moveToDocumentRoot();

      int count = 0;
      final var axis = new DescendantAxis(rtx, IncludeSelf.YES);
      while (axis.hasNext()) {
        axis.nextLong();
        count++;
      }

      // All 18 nodes (0-17) including document root after iter#32 fusion (structural fusion
      // collapses each OBJECT_KEY+OBJECT/ARRAY pair, dropping the standalone container nodes).
      assertEquals(18, count);
    }
  }

  // ---------- 5. PostOrderAxis traversal ----------

  @Test
  void testPostOrderAxisTraversal() {
    try (final var rtx = openReadTrx()) {
      // iter#32 structural fusion: "bar" key+OBJECT collapsed into OBJECT_NAMED_OBJECT at key 6.
      rtx.moveTo(6);
      assertEquals(NodeKind.OBJECT_NAMED_OBJECT, rtx.getKind());

      final List<Long> visited = new ArrayList<>(8);
      final var axis = new PostOrderAxis(rtx, IncludeSelf.YES);
      while (axis.hasNext()) {
        visited.add(axis.nextLong());
      }

      // Post-order (iter#32 structural fusion): leaves before parents.
      // Subtree of fused node 6:
      //   7: OBJECT_NAMED_STRING "hello":"world"
      //   8: OBJECT_NAMED_BOOLEAN "helloo":true
      // Post-order: 7, 8, 6
      assertEquals(List.of(7L, 8L, 6L), visited);
    }
  }

  // ---------- 6. ChildAxis of object ----------

  @Test
  void testChildAxisOfObject() {
    try (final var rtx = openReadTrx()) {
      // Move to root object (node 1)
      rtx.moveTo(1);
      assertEquals(NodeKind.OBJECT, rtx.getKind());

      final List<Long> children = new ArrayList<>(4);
      final var axis = new ChildAxis(rtx);
      while (axis.hasNext()) {
        children.add(axis.nextLong());
      }

      // Children of root object after iter#32 structural fusion:
      //   2: OBJECT_NAMED_ARRAY "foo"
      //   6: OBJECT_NAMED_OBJECT "bar"
      //   9: OBJECT_NAMED_STRING "baz"
      //  10: OBJECT_NAMED_ARRAY "tada"
      assertEquals(List.of(2L, 6L, 9L, 10L), children);
    }
  }

  // ---------- 7. ChildAxis of array ----------

  @Test
  void testChildAxisOfArray() {
    try (final var rtx = openReadTrx()) {
      // iter#32 fusion: "foo" OBJECT_KEY+ARRAY collapsed into OBJECT_NAMED_ARRAY at key 2.
      rtx.moveTo(2);
      assertEquals(NodeKind.OBJECT_NAMED_ARRAY, rtx.getKind());

      final List<Long> children = new ArrayList<>(4);
      final var axis = new ChildAxis(rtx);
      while (axis.hasNext()) {
        children.add(axis.nextLong());
      }

      // Children of fused array: STRING_VALUE(3), NULL_VALUE(4), NUMBER_VALUE(5).
      assertEquals(List.of(3L, 4L, 5L), children);
    }
  }

  // ---------- 8. ChildAxis of leaf ----------

  @Test
  void testChildAxisOfLeaf() {
    try (final var rtx = openReadTrx()) {
      // iter#32 fusion: STRING_VALUE("bar") moved from key 4 -> 3.
      rtx.moveTo(3);
      assertEquals(NodeKind.STRING_VALUE, rtx.getKind());

      int count = 0;
      final var axis = new ChildAxis(rtx);
      while (axis.hasNext()) {
        axis.nextLong();
        count++;
      }

      assertEquals(0, count);
    }
  }

  // ---------- 9. FollowingSiblingAxis ----------

  @Test
  void testFollowingSiblingAxis() {
    try (final var rtx = openReadTrx()) {
      // iter#32 structural fusion: "foo" is now OBJECT_NAMED_ARRAY at key 2 (was OBJECT_KEY).
      rtx.moveTo(2);
      assertEquals(NodeKind.OBJECT_NAMED_ARRAY, rtx.getKind());

      final List<Long> siblings = new ArrayList<>(4);
      final var axis = new FollowingSiblingAxis(rtx);
      while (axis.hasNext()) {
        siblings.add(axis.nextLong());
      }

      // Following siblings of "foo" after iter#32 fusion: "bar"(6), "baz"(9), "tada"(10).
      assertEquals(List.of(6L, 9L, 10L), siblings);
    }
  }

  // ---------- 10. PrecedingSiblingAxis ----------

  @Test
  void testPrecedingSiblingAxis() {
    try (final var rtx = openReadTrx()) {
      // iter#32 structural fusion: "tada" is OBJECT_NAMED_ARRAY at key 10 (was OBJECT_KEY 15).
      rtx.moveTo(10);
      assertEquals(NodeKind.OBJECT_NAMED_ARRAY, rtx.getKind());

      final List<Long> siblings = new ArrayList<>(4);
      final var axis = new PrecedingSiblingAxis(rtx);
      while (axis.hasNext()) {
        siblings.add(axis.nextLong());
      }

      // Preceding siblings returned in document order: "foo"(2), "bar"(6), "baz"(9).
      assertEquals(List.of(2L, 6L, 9L), siblings);
    }
  }

  // ---------- 11. AncestorAxis ----------

  @Test
  void testAncestorAxis() {
    try (final var rtx = openReadTrx()) {
      // iter#32 structural fusion: "hello":"world" is OBJECT_NAMED_STRING at key 7.
      rtx.moveTo(7);
      assertEquals(NodeKind.OBJECT_NAMED_STRING, rtx.getKind());

      final List<Long> ancestors = new ArrayList<>(4);
      final var axis = new AncestorAxis(rtx);
      while (axis.hasNext()) {
        ancestors.add(axis.nextLong());
      }

      // Ancestors from node 7 up: fused OBJECT_NAMED_OBJECT "bar"(6), root OBJECT(1).
      assertEquals(List.of(6L, 1L), ancestors);
    }
  }

  // ---------- 12. ParentAxis ----------

  @Test
  void testParentAxis() {
    try (final var rtx = openReadTrx()) {
      // iter#32 fusion: STRING_VALUE("bar") moved from key 4 -> 3, and its parent (the
      // legacy OBJECT_KEY+ARRAY pair at keys 2/3) collapsed into a single OBJECT_NAMED_ARRAY
      // at key 2.
      rtx.moveTo(3);
      assertEquals(NodeKind.STRING_VALUE, rtx.getKind());

      final List<Long> parents = new ArrayList<>(1);
      final var axis = new ParentAxis(rtx);
      while (axis.hasNext()) {
        parents.add(axis.nextLong());
      }

      // Parent of node 3 (STRING_VALUE "bar") is the fused OBJECT_NAMED_ARRAY "foo" at key 2.
      assertEquals(List.of(2L), parents);
    }
  }

  // ---------- 13. FilterAxis + ObjectFilter ----------

  @Test
  void testFilterAxisObjectFilter() {
    try (final var rtx = openReadTrx()) {
      rtx.moveToDocumentRoot();

      int objectCount = 0;
      final var axis = new FilterAxis<>(new DescendantAxis(rtx), new ObjectFilter(rtx));
      while (axis.hasNext()) {
        axis.nextLong();
        assertEquals(NodeKind.OBJECT, rtx.getKind());
        objectCount++;
      }

      // OBJECTs in document after iter#32 structural fusion (excludes fused OBJECT_NAMED_OBJECT):
      //   root(1), {"foo":"bar"}(11), {"baz":false}(13), empty(16) — 4 OBJECTs.
      assertEquals(4, objectCount);
    }
  }

  // ---------- 14. FilterAxis + StringValueFilter ----------

  @Test
  void testFilterAxisStringFilter() {
    try (final var rtx = openReadTrx()) {
      rtx.moveToDocumentRoot();

      final List<String> stringValues = new ArrayList<>(4);
      final var axis = new FilterAxis<>(new DescendantAxis(rtx), new StringValueFilter(rtx));
      while (axis.hasNext()) {
        axis.nextLong();
        assertEquals(NodeKind.STRING_VALUE, rtx.getKind());
        stringValues.add(rtx.getValue());
      }

      // STRING_VALUE nodes only (excludes fused OBJECT_NAMED_STRING): "bar"(3), "boo"(15).
      assertEquals(List.of("bar", "boo"), stringValues);
    }
  }

  // ---------- 15. LevelOrderAxis ----------

  @Test
  void testLevelOrderAxis() {
    try (final var rtx = openReadTrx()) {
      // Position at root object (node 1) for BFS traversal
      rtx.moveTo(1);

      final List<Long> visited = new ArrayList<>(32);
      final var axis = LevelOrderAxis.newBuilder(rtx).includeSelf().build();
      while (axis.hasNext()) {
        visited.add(axis.nextLong());
      }

      // BFS must visit all 17 nodes in the subtree rooted at node 1 (including self)
      // after iter#32 structural fusion (18 total nodes minus the JSON_DOCUMENT root).
      assertEquals(17, visited.size());

      // First node visited must be the root object itself
      assertEquals(1L, visited.getFirst().longValue());

      // Level 1 children (object keys / fused records) must appear before any level 2 node.
      final int idxFoo = visited.indexOf(2L);    // OBJECT_NAMED_ARRAY "foo"
      final int idxBar = visited.indexOf(6L);    // OBJECT_NAMED_OBJECT "bar"
      final int idxBaz = visited.indexOf(9L);    // OBJECT_NAMED_STRING "baz":"hello"
      final int idxTada = visited.indexOf(10L);  // OBJECT_NAMED_ARRAY "tada"

      // All object key children must be present
      assertTrue(idxFoo >= 0);
      assertTrue(idxBar >= 0);
      assertTrue(idxBaz >= 0);
      assertTrue(idxTada >= 0);

      // Level 2 nodes (array elements + bar's inner fields) must come after all level 1 keys.
      final int maxLevel1 = Math.max(Math.max(idxFoo, idxBar), Math.max(idxBaz, idxTada));
      final int idxStringBar = visited.indexOf(3L);  // STRING_VALUE "bar" (foo array elem 0)
      final int idxNamedHello = visited.indexOf(7L); // OBJECT_NAMED_STRING "hello":"world"
      assertTrue(idxStringBar > maxLevel1, "Level 2 node should come after all level 1 nodes");
      assertTrue(idxNamedHello > maxLevel1, "Level 2 node should come after all level 1 nodes");
    }
  }

  // ---------- 16. VisitorDescendantAxis ----------

  @Test
  void testVisitorDescendantAxis() {
    try (final var rtx = openReadTrx()) {
      // Position at root object (node 1) to avoid double-visit of document root.
      // VisitorDescendantAxis invokes the visitor once before the first iteration and once per
      // yielded node, so the start node's visitor callback fires one extra time.
      // By starting at node 1 without includeSelf we get a clean 1:1 mapping between yielded
      // nodes and visitor callbacks for all descendants.
      rtx.moveTo(1);

      final Map<NodeKind, Integer> kindCounts = new EnumMap<>(NodeKind.class);

      final JsonNodeVisitor countingVisitor = new JsonNodeVisitor() {
        private void increment(final NodeKind kind) {
          kindCounts.merge(kind, 1, Integer::sum);
        }

        @Override
        public VisitResult visit(final ImmutableObjectNode node) {
          increment(NodeKind.OBJECT);
          return VisitResultType.CONTINUE;
        }

        @Override
        public VisitResult visit(final ImmutableArrayNode node) {
          increment(NodeKind.ARRAY);
          return VisitResultType.CONTINUE;
        }

        @Override
        public VisitResult visit(final ImmutableStringNode node) {
          increment(NodeKind.STRING_VALUE);
          return VisitResultType.CONTINUE;
        }

        @Override
        public VisitResult visit(final ImmutableNumberNode node) {
          increment(NodeKind.NUMBER_VALUE);
          return VisitResultType.CONTINUE;
        }

        @Override
        public VisitResult visit(final ImmutableNullNode node) {
          increment(NodeKind.NULL_VALUE);
          return VisitResultType.CONTINUE;
        }

        @Override
        public VisitResult visit(final ImmutableBooleanNode node) {
          increment(NodeKind.BOOLEAN_VALUE);
          return VisitResultType.CONTINUE;
        }

        @Override
        public VisitResult visit(final ObjectNamedStringNode node) {
          increment(NodeKind.OBJECT_NAMED_STRING);
          return VisitResultType.CONTINUE;
        }

        @Override
        public VisitResult visit(final ObjectNamedBooleanNode node) {
          increment(NodeKind.OBJECT_NAMED_BOOLEAN);
          return VisitResultType.CONTINUE;
        }

        @Override
        public VisitResult visit(final ObjectNamedNumberNode node) {
          increment(NodeKind.OBJECT_NAMED_NUMBER);
          return VisitResultType.CONTINUE;
        }

        @Override
        public VisitResult visit(final ObjectNamedNullNode node) {
          increment(NodeKind.OBJECT_NAMED_NULL);
          return VisitResultType.CONTINUE;
        }

        @Override
        public VisitResult visit(final ImmutableJsonDocumentRootNode node) {
          increment(NodeKind.JSON_DOCUMENT);
          return VisitResultType.CONTINUE;
        }

        @Override
        public VisitResult visit(final io.sirix.node.json.ObjectNamedObjectNode node) {
          increment(NodeKind.OBJECT_NAMED_OBJECT);
          return VisitResultType.CONTINUE;
        }

        @Override
        public VisitResult visit(final io.sirix.node.json.ObjectNamedArrayNode node) {
          increment(NodeKind.OBJECT_NAMED_ARRAY);
          return VisitResultType.CONTINUE;
        }
      };

      final var axis = VisitorDescendantAxis.newBuilder(rtx)
          .visitor(countingVisitor)
          .build();

      int totalCount = 0;
      while (axis.hasNext()) {
        axis.nextLong();
        totalCount++;
      }

      // 16 descendants of root object (nodes 2-17) after iter#32 structural fusion.
      assertEquals(16, totalCount);

      // The visitor is invoked once extra for the start node (node 1, OBJECT) before the first
      // yielded node, plus once per yielded node. So the OBJECT count includes the start node.
      // Descendant objects after iter#32 structural fusion: tada-elem-0(11), tada-elem-1(13),
      // empty(16) = 3, plus the start node visit = 4.
      assertEquals(4, kindCounts.getOrDefault(NodeKind.OBJECT, 0));

      // OBJECT_KEY was deleted from NodeKind enum after iter#32 structural fusion;
      // the residual OBJECT_NAMED_OBJECT count is asserted below at line 570.
      // Bare ARRAY: only the empty trailing array (17) — foo and tada arrays are now fused.
      assertEquals(1, kindCounts.getOrDefault(NodeKind.ARRAY, 0));
      assertEquals(2, kindCounts.getOrDefault(NodeKind.STRING_VALUE, 0));
      assertEquals(1, kindCounts.getOrDefault(NodeKind.NUMBER_VALUE, 0));
      assertEquals(1, kindCounts.getOrDefault(NodeKind.NULL_VALUE, 0));
      // Fused string fields: hello, baz (top-level), foo (in tada) = 3.
      assertEquals(3, kindCounts.getOrDefault(NodeKind.OBJECT_NAMED_STRING, 0));
      // Fused boolean fields: helloo, baz (in tada) = 2.
      assertEquals(2, kindCounts.getOrDefault(NodeKind.OBJECT_NAMED_BOOLEAN, 0));
      // Fused structural records: bar(6) = 1 OBJECT_NAMED_OBJECT, foo(2)+tada(10) = 2 OBJECT_NAMED_ARRAY.
      assertEquals(1, kindCounts.getOrDefault(NodeKind.OBJECT_NAMED_OBJECT, 0));
      assertEquals(2, kindCounts.getOrDefault(NodeKind.OBJECT_NAMED_ARRAY, 0));

      // No BOOLEAN_VALUE, OBJECT_NAMED_NUMBER, or OBJECT_NAMED_NULL in the test document
      assertFalse(kindCounts.containsKey(NodeKind.BOOLEAN_VALUE));
      assertFalse(kindCounts.containsKey(NodeKind.OBJECT_NAMED_NUMBER));
      assertFalse(kindCounts.containsKey(NodeKind.OBJECT_NAMED_NULL));
    }
  }
}
