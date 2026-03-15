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
import io.sirix.node.immutable.json.ImmutableObjectBooleanNode;
import io.sirix.node.immutable.json.ImmutableObjectKeyNode;
import io.sirix.node.immutable.json.ImmutableObjectNode;
import io.sirix.node.immutable.json.ImmutableObjectNullNode;
import io.sirix.node.immutable.json.ImmutableObjectNumberNode;
import io.sirix.node.immutable.json.ImmutableObjectStringNode;
import io.sirix.node.immutable.json.ImmutableStringNode;
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
 * <p>Test document structure (node keys 0-25):
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
 *       9: OBJECT_KEY "hello"
 *         10: OBJECT_STRING_VALUE "world"
 *       11: OBJECT_KEY "helloo"
 *         12: OBJECT_BOOLEAN_VALUE true
 *   13: OBJECT_KEY "baz"
 *     14: OBJECT_STRING_VALUE "hello"
 *   15: OBJECT_KEY "tada"
 *     16: ARRAY
 *       17: OBJECT {"foo":"bar"}
 *         18: OBJECT_KEY "foo"
 *           19: OBJECT_STRING_VALUE "bar"
 *       20: OBJECT {"baz":false}
 *         21: OBJECT_KEY "baz"
 *           22: OBJECT_BOOLEAN_VALUE false
 *       23: STRING_VALUE "boo"
 *       24: OBJECT (empty)
 *       25: ARRAY (empty)
 * </pre>
 */
final class JsonAxisComprehensiveTest {

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  /**
   * Creates the standard test document and returns a read-only transaction positioned at the
   * document root.
   */
  private JsonNodeReadOnlyTrx openReadTrx() {
    final var database = JsonTestHelper.getDatabase(PATHS.PATH1.getFile());
    try (final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      JsonDocumentCreator.create(wtx);
      wtx.commit();
    }

    final JsonResourceSession session = database.beginResourceSession(JsonTestHelper.RESOURCE);
    final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx();
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

      // 25 descendants: nodes 1-25 (excludes document root itself)
      assertEquals(25, count);
    }
  }

  // ---------- 2. DescendantAxis from subtree ----------

  @Test
  void testDescendantAxisFromSubtree() {
    try (final var rtx = openReadTrx()) {
      // Move to the "foo" array (node 3)
      rtx.moveTo(3);
      assertEquals(NodeKind.ARRAY, rtx.getKind());

      int count = 0;
      final var axis = new DescendantAxis(rtx);
      while (axis.hasNext()) {
        axis.nextLong();
        count++;
      }

      // Descendants of array node 3: STRING_VALUE(4), NULL_VALUE(5), NUMBER_VALUE(6)
      assertEquals(3, count);
    }
  }

  // ---------- 3. DescendantAxis from leaf ----------

  @Test
  void testDescendantAxisFromLeaf() {
    try (final var rtx = openReadTrx()) {
      // Move to STRING_VALUE "bar" (node 4) -- a leaf node
      rtx.moveTo(4);
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

      // All 26 nodes including document root
      assertEquals(26, count);
    }
  }

  // ---------- 5. PostOrderAxis traversal ----------

  @Test
  void testPostOrderAxisTraversal() {
    try (final var rtx = openReadTrx()) {
      // Position at the "bar" object (node 8): {"hello":"world","helloo":true}
      rtx.moveTo(8);
      assertEquals(NodeKind.OBJECT, rtx.getKind());

      final List<Long> visited = new ArrayList<>(8);
      final var axis = new PostOrderAxis(rtx, IncludeSelf.YES);
      while (axis.hasNext()) {
        visited.add(axis.nextLong());
      }

      // Post-order: leaves before parents
      // Subtree of node 8:
      //   9: OBJECT_KEY "hello"
      //     10: OBJECT_STRING_VALUE "world"
      //   11: OBJECT_KEY "helloo"
      //     12: OBJECT_BOOLEAN_VALUE true
      // Post-order: 10, 9, 12, 11, 8
      assertEquals(List.of(10L, 9L, 12L, 11L, 8L), visited);
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

      // Children of root object are the 4 OBJECT_KEY nodes: "foo"(2), "bar"(7), "baz"(13), "tada"(15)
      assertEquals(List.of(2L, 7L, 13L, 15L), children);
    }
  }

  // ---------- 7. ChildAxis of array ----------

  @Test
  void testChildAxisOfArray() {
    try (final var rtx = openReadTrx()) {
      // Move to "foo" array (node 3)
      rtx.moveTo(3);
      assertEquals(NodeKind.ARRAY, rtx.getKind());

      final List<Long> children = new ArrayList<>(4);
      final var axis = new ChildAxis(rtx);
      while (axis.hasNext()) {
        children.add(axis.nextLong());
      }

      // Children of array: STRING_VALUE(4), NULL_VALUE(5), NUMBER_VALUE(6)
      assertEquals(List.of(4L, 5L, 6L), children);
    }
  }

  // ---------- 8. ChildAxis of leaf ----------

  @Test
  void testChildAxisOfLeaf() {
    try (final var rtx = openReadTrx()) {
      // Move to STRING_VALUE "bar" (node 4) -- a leaf node
      rtx.moveTo(4);
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
      // Move to first object key "foo" (node 2)
      rtx.moveTo(2);
      assertEquals(NodeKind.OBJECT_KEY, rtx.getKind());

      final List<Long> siblings = new ArrayList<>(4);
      final var axis = new FollowingSiblingAxis(rtx);
      while (axis.hasNext()) {
        siblings.add(axis.nextLong());
      }

      // Following siblings of "foo": "bar"(7), "baz"(13), "tada"(15)
      assertEquals(List.of(7L, 13L, 15L), siblings);
    }
  }

  // ---------- 10. PrecedingSiblingAxis ----------

  @Test
  void testPrecedingSiblingAxis() {
    try (final var rtx = openReadTrx()) {
      // Move to last object key "tada" (node 15)
      rtx.moveTo(15);
      assertEquals(NodeKind.OBJECT_KEY, rtx.getKind());

      final List<Long> siblings = new ArrayList<>(4);
      final var axis = new PrecedingSiblingAxis(rtx);
      while (axis.hasNext()) {
        siblings.add(axis.nextLong());
      }

      // Preceding siblings returned in document order: "foo"(2), "bar"(7), "baz"(13)
      assertEquals(List.of(2L, 7L, 13L), siblings);
    }
  }

  // ---------- 11. AncestorAxis ----------

  @Test
  void testAncestorAxis() {
    try (final var rtx = openReadTrx()) {
      // Move to OBJECT_STRING_VALUE "world" (node 10) -- deep leaf
      rtx.moveTo(10);
      assertEquals(NodeKind.OBJECT_STRING_VALUE, rtx.getKind());

      final List<Long> ancestors = new ArrayList<>(4);
      final var axis = new AncestorAxis(rtx);
      while (axis.hasNext()) {
        ancestors.add(axis.nextLong());
      }

      // Ancestors from node 10 up to (but excluding) document root:
      // parent 9 (OBJECT_KEY "hello"), 8 (OBJECT), 7 (OBJECT_KEY "bar"), 1 (OBJECT root)
      assertEquals(List.of(9L, 8L, 7L, 1L), ancestors);
    }
  }

  // ---------- 12. ParentAxis ----------

  @Test
  void testParentAxis() {
    try (final var rtx = openReadTrx()) {
      // Move to STRING_VALUE "bar" (node 4)
      rtx.moveTo(4);
      assertEquals(NodeKind.STRING_VALUE, rtx.getKind());

      final List<Long> parents = new ArrayList<>(1);
      final var axis = new ParentAxis(rtx);
      while (axis.hasNext()) {
        parents.add(axis.nextLong());
      }

      // Parent of node 4 is the "foo" array (node 3)
      assertEquals(List.of(3L), parents);
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

      // Objects in document: root(1), inner(8), {"foo":"bar"}(17), {"baz":false}(20), empty(24)
      assertEquals(5, objectCount);
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

      // STRING_VALUE nodes only (not OBJECT_STRING_VALUE): "bar"(4), "boo"(23)
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

      // BFS must visit all 25 nodes in the subtree rooted at node 1 (including self)
      assertEquals(25, visited.size());

      // First node visited must be the root object itself
      assertEquals(1L, visited.getFirst().longValue());

      // Level 1 children (object keys) must appear before any level 2 node.
      // Verify the 4 object keys appear before any deeper node.
      final int idxFoo = visited.indexOf(2L);
      final int idxBar = visited.indexOf(7L);
      final int idxBaz = visited.indexOf(13L);
      final int idxTada = visited.indexOf(15L);

      // All object key children must be present
      assertTrue(idxFoo >= 0);
      assertTrue(idxBar >= 0);
      assertTrue(idxBaz >= 0);
      assertTrue(idxTada >= 0);

      // Level 2 nodes (e.g., array node 3, object node 8) must come after all level 1 keys
      final int maxLevel1 = Math.max(Math.max(idxFoo, idxBar), Math.max(idxBaz, idxTada));
      final int idxArray3 = visited.indexOf(3L);
      final int idxObject8 = visited.indexOf(8L);
      assertTrue(idxArray3 > maxLevel1, "Level 2 node should come after all level 1 nodes");
      assertTrue(idxObject8 > maxLevel1, "Level 2 node should come after all level 1 nodes");
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
        public VisitResult visit(final ImmutableObjectKeyNode node) {
          increment(NodeKind.OBJECT_KEY);
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
        public VisitResult visit(final ImmutableObjectStringNode node) {
          increment(NodeKind.OBJECT_STRING_VALUE);
          return VisitResultType.CONTINUE;
        }

        @Override
        public VisitResult visit(final ImmutableObjectBooleanNode node) {
          increment(NodeKind.OBJECT_BOOLEAN_VALUE);
          return VisitResultType.CONTINUE;
        }

        @Override
        public VisitResult visit(final ImmutableObjectNumberNode node) {
          increment(NodeKind.OBJECT_NUMBER_VALUE);
          return VisitResultType.CONTINUE;
        }

        @Override
        public VisitResult visit(final ImmutableObjectNullNode node) {
          increment(NodeKind.OBJECT_NULL_VALUE);
          return VisitResultType.CONTINUE;
        }

        @Override
        public VisitResult visit(final ImmutableJsonDocumentRootNode node) {
          increment(NodeKind.JSON_DOCUMENT);
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

      // 24 descendant nodes of root object (nodes 2-25)
      assertEquals(24, totalCount);

      // The visitor is invoked once extra for the start node (node 1, OBJECT) before the first
      // yielded node, plus once per yielded node. So the OBJECT count includes the start node.
      // Descendant objects: 8, 17, 20, 24 = 4, plus start node visit = 5
      assertEquals(5, kindCounts.getOrDefault(NodeKind.OBJECT, 0));

      // Other kind counts reflect the visitor callbacks for yielded descendants
      assertEquals(8, kindCounts.getOrDefault(NodeKind.OBJECT_KEY, 0));
      assertEquals(3, kindCounts.getOrDefault(NodeKind.ARRAY, 0));
      assertEquals(2, kindCounts.getOrDefault(NodeKind.STRING_VALUE, 0));
      assertEquals(1, kindCounts.getOrDefault(NodeKind.NUMBER_VALUE, 0));
      assertEquals(1, kindCounts.getOrDefault(NodeKind.NULL_VALUE, 0));
      assertEquals(3, kindCounts.getOrDefault(NodeKind.OBJECT_STRING_VALUE, 0));
      assertEquals(2, kindCounts.getOrDefault(NodeKind.OBJECT_BOOLEAN_VALUE, 0));

      // No BOOLEAN_VALUE, OBJECT_NUMBER_VALUE, or OBJECT_NULL_VALUE in the test document
      assertFalse(kindCounts.containsKey(NodeKind.BOOLEAN_VALUE));
      assertFalse(kindCounts.containsKey(NodeKind.OBJECT_NUMBER_VALUE));
      assertFalse(kindCounts.containsKey(NodeKind.OBJECT_NULL_VALUE));
    }
  }
}
