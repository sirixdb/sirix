package io.sirix.access.node.json;

import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.api.visitor.JsonNodeVisitor;
import io.sirix.api.visitor.VisitResult;
import io.sirix.api.visitor.VisitResultType;
import io.sirix.axis.visitor.VisitorDescendantAxis;
import io.sirix.node.immutable.json.ImmutableArrayNode;
import io.sirix.node.immutable.json.ImmutableBooleanNode;
import io.sirix.node.immutable.json.ImmutableJsonDocumentRootNode;
import io.sirix.node.immutable.json.ImmutableNullNode;
import io.sirix.node.immutable.json.ImmutableNumberNode;
import io.sirix.node.immutable.json.ImmutableObjectNode;
import io.sirix.node.immutable.json.ImmutableStringNode;
import io.sirix.node.json.ObjectNamedArrayNode;
import io.sirix.node.json.ObjectNamedBooleanNode;
import io.sirix.node.json.ObjectNamedNullNode;
import io.sirix.node.json.ObjectNamedNumberNode;
import io.sirix.node.json.ObjectNamedObjectNode;
import io.sirix.node.json.ObjectNamedStringNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for the JSON visitor pattern using {@link JsonNodeVisitor} and {@link VisitorDescendantAxis}.
 *
 * <p>Uses the standard test document:
 * <pre>
 * {"foo":["bar",null,2.33],"bar":{"hello":"world","helloo":true},"baz":"hello",
 *  "tada":[{"foo":"bar"},{"baz":false},"boo",{},[]]}
 * </pre>
 */
public final class JsonVisitorTest {

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
    JsonTestHelper.createTestDocument();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  /**
   * Verify that traversal visits every node kind present in the test document.
   *
   * <p>The test document has 27 nodes total. The null values from
   * {@code insertNullValueAsRightSibling()} in the first array and the empty
   * objects/arrays produce the counts below.
   */
  @Test
  void testVisitAllNodeKinds() {
    try (final JsonResourceSession session =
             JsonTestHelper.getDatabase(PATHS.PATH1.getFile()).beginResourceSession(JsonTestHelper.RESOURCE);
         final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {

      final NodeKindCounter counter = new NodeKindCounter();
      final VisitorDescendantAxis axis =
          VisitorDescendantAxis.newBuilder(rtx).includeSelf().visitor(counter).build();
      while (axis.hasNext()) {
        axis.nextLong();
      }

      // The VisitorDescendantAxis with includeSelf visits the document root node twice:
      // once for the self-inclusion pass and once for the descent from self to first child.
      assertEquals(2, counter.documentRootCount);
      // iter#32 structural fusion: foo→OBJECT_NAMED_ARRAY, bar→OBJECT_NAMED_OBJECT, tada→OBJECT_NAMED_ARRAY
      // — those three former (OBJECT_KEY+OBJECT/ARRAY) pairs collapse into single fused records.
      // The remaining ImmutableObjectNode count is: root (1), 1st-tada {foo:bar} (1),
      // 2nd-tada {baz:false} (1), tada empty {} (1) = 4. Likewise for ARRAY: only the empty []
      // in tada remains (foo's array and tada's array fuse).
      assertEquals(4, counter.objectNodeCount);
      assertEquals(1, counter.arrayNodeCount);
      // Only the inner-tada object keys remain as plain OBJECT_KEY: foo (in tada-obj), baz (in tada-obj).
      assertEquals(0, counter.objectKeyNodeCount);
      assertEquals(2, counter.stringNodeCount);
      // Fused (key, primitive) records: 3 strings (hello, baz, foo-in-tada), 2 booleans (helloo, baz-in-tada).
      assertEquals(3, counter.objectStringNodeCount);
      assertEquals(1, counter.nullNodeCount);
      assertEquals(1, counter.numberNodeCount);
      assertEquals(2, counter.objectBooleanNodeCount);
      assertEquals(0, counter.booleanNodeCount);
      assertEquals(0, counter.objectNumberNodeCount);
      assertEquals(0, counter.objectNullNodeCount);
      // Structural fused: bar→OBJECT_NAMED_OBJECT, foo+tada→OBJECT_NAMED_ARRAY.
      assertEquals(1, counter.objectNamedObjectNodeCount);
      assertEquals(2, counter.objectNamedArrayNodeCount);
    }
  }

  @Test
  void testCountObjectNodes() {
    try (final JsonResourceSession session =
             JsonTestHelper.getDatabase(PATHS.PATH1.getFile()).beginResourceSession(JsonTestHelper.RESOURCE);
         final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {

      final int[] count = {0};
      final JsonNodeVisitor visitor = new JsonNodeVisitor() {
        @Override
        public VisitResult visit(final ImmutableObjectNode node) {
          count[0]++;
          return VisitResultType.CONTINUE;
        }

        @Override
        public VisitResult visit(final ObjectNamedObjectNode node) {
          // iter#32 structural fusion: OBJECT_NAMED_OBJECT plays the OBJECT role.
          count[0]++;
          return VisitResultType.CONTINUE;
        }
      };

      final VisitorDescendantAxis axis =
          VisitorDescendantAxis.newBuilder(rtx).includeSelf().visitor(visitor).build();
      while (axis.hasNext()) {
        axis.nextLong();
      }

      assertEquals(5, count[0]);
    }
  }

  @Test
  void testCountArrayNodes() {
    try (final JsonResourceSession session =
             JsonTestHelper.getDatabase(PATHS.PATH1.getFile()).beginResourceSession(JsonTestHelper.RESOURCE);
         final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {

      final int[] count = {0};
      final JsonNodeVisitor visitor = new JsonNodeVisitor() {
        @Override
        public VisitResult visit(final ImmutableArrayNode node) {
          count[0]++;
          return VisitResultType.CONTINUE;
        }

        @Override
        public VisitResult visit(final ObjectNamedArrayNode node) {
          // iter#32 structural fusion: OBJECT_NAMED_ARRAY plays the ARRAY role.
          count[0]++;
          return VisitResultType.CONTINUE;
        }
      };

      final VisitorDescendantAxis axis =
          VisitorDescendantAxis.newBuilder(rtx).includeSelf().visitor(visitor).build();
      while (axis.hasNext()) {
        axis.nextLong();
      }

      assertEquals(3, count[0]);
    }
  }

  /**
   * Collect string values from both {@link ImmutableStringNode} and
   * {@link ObjectNamedStringNode} nodes.
   * Expected: "bar" (array child), "world", "hello", "bar" (tada child), "boo".
   */
  @Test
  void testCollectStringValues() {
    try (final JsonResourceSession session =
             JsonTestHelper.getDatabase(PATHS.PATH1.getFile()).beginResourceSession(JsonTestHelper.RESOURCE);
         final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {

      final List<String> values = new ArrayList<>(8);
      final JsonNodeVisitor visitor = new JsonNodeVisitor() {
        @Override
        public VisitResult visit(final ImmutableStringNode node) {
          values.add(node.getValue());
          return VisitResultType.CONTINUE;
        }

        @Override
        public VisitResult visit(final ObjectNamedStringNode node) {
          values.add(node.getValue());
          return VisitResultType.CONTINUE;
        }
      };

      final VisitorDescendantAxis axis =
          VisitorDescendantAxis.newBuilder(rtx).includeSelf().visitor(visitor).build();
      while (axis.hasNext()) {
        axis.nextLong();
      }

      assertEquals(5, values.size());
      assertTrue(values.contains("bar"));
      assertTrue(values.contains("world"));
      assertTrue(values.contains("hello"));
      assertTrue(values.contains("boo"));
    }
  }

  /**
   * Collect object key names from both {@link ImmutableObjectKeyNode} and the iter#32 fused
   * OBJECT_NAMED_* records — both play the OBJECT_KEY role.
   * Expected keys (8): foo, bar, hello, helloo, baz, tada, foo (inner), baz (inner).
   */
  @Test
  void testCollectObjectKeyNames() {
    try (final JsonResourceSession session =
             JsonTestHelper.getDatabase(PATHS.PATH1.getFile()).beginResourceSession(JsonTestHelper.RESOURCE);
         final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {

      final List<String> keyNames = new ArrayList<>(8);
      final JsonNodeVisitor visitor = new JsonNodeVisitor() {
        private void recordName(final long nodeKey) {
          final long savedKey = rtx.getNodeKey();
          rtx.moveTo(nodeKey);
          keyNames.add(rtx.getName().getLocalName());
          rtx.moveTo(savedKey);
        }

        @Override
        public VisitResult visit(final ObjectNamedStringNode node) {
          recordName(node.getNodeKey());
          return VisitResultType.CONTINUE;
        }

        @Override
        public VisitResult visit(final ObjectNamedNumberNode node) {
          recordName(node.getNodeKey());
          return VisitResultType.CONTINUE;
        }

        @Override
        public VisitResult visit(final ObjectNamedBooleanNode node) {
          recordName(node.getNodeKey());
          return VisitResultType.CONTINUE;
        }

        @Override
        public VisitResult visit(final ObjectNamedNullNode node) {
          recordName(node.getNodeKey());
          return VisitResultType.CONTINUE;
        }

        @Override
        public VisitResult visit(final ObjectNamedObjectNode node) {
          // Phase 4: OBJECT_NAMED_OBJECT plays the field-name role for object-valued fields.
          recordName(node.getNodeKey());
          return VisitResultType.CONTINUE;
        }

        @Override
        public VisitResult visit(final ObjectNamedArrayNode node) {
          // iter#32 structural fusion: OBJECT_NAMED_ARRAY plays the OBJECT_KEY role.
          recordName(node.getNodeKey());
          return VisitResultType.CONTINUE;
        }
      };

      final VisitorDescendantAxis axis =
          VisitorDescendantAxis.newBuilder(rtx).includeSelf().visitor(visitor).build();
      while (axis.hasNext()) {
        axis.nextLong();
      }

      assertEquals(8, keyNames.size());
      assertTrue(keyNames.contains("foo"));
      assertTrue(keyNames.contains("bar"));
      assertTrue(keyNames.contains("hello"));
      assertTrue(keyNames.contains("helloo"));
      assertTrue(keyNames.contains("baz"));
      assertTrue(keyNames.contains("tada"));
    }
  }

  /**
   * Collect number values. The test document has a single number: 2.33.
   */
  @Test
  void testCollectNumbers() {
    try (final JsonResourceSession session =
             JsonTestHelper.getDatabase(PATHS.PATH1.getFile()).beginResourceSession(JsonTestHelper.RESOURCE);
         final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {

      final List<Number> numbers = new ArrayList<>(4);
      final JsonNodeVisitor visitor = new JsonNodeVisitor() {
        @Override
        public VisitResult visit(final ImmutableNumberNode node) {
          numbers.add(node.getValue());
          return VisitResultType.CONTINUE;
        }

        @Override
        public VisitResult visit(final ObjectNamedNumberNode node) {
          numbers.add(node.getValue());
          return VisitResultType.CONTINUE;
        }
      };

      final VisitorDescendantAxis axis =
          VisitorDescendantAxis.newBuilder(rtx).includeSelf().visitor(visitor).build();
      while (axis.hasNext()) {
        axis.nextLong();
      }

      assertEquals(1, numbers.size());
      assertEquals(2.33, numbers.get(0).doubleValue(), 0.001);
    }
  }

  /**
   * Collect boolean values from both {@link ImmutableBooleanNode} and
   * {@link ObjectNamedBooleanNode}. Expected: true ("helloo"), false ("baz" in tada).
   */
  @Test
  void testCollectBooleans() {
    try (final JsonResourceSession session =
             JsonTestHelper.getDatabase(PATHS.PATH1.getFile()).beginResourceSession(JsonTestHelper.RESOURCE);
         final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {

      final List<Boolean> booleans = new ArrayList<>(4);
      final JsonNodeVisitor visitor = new JsonNodeVisitor() {
        @Override
        public VisitResult visit(final ImmutableBooleanNode node) {
          booleans.add(node.getValue());
          return VisitResultType.CONTINUE;
        }

        @Override
        public VisitResult visit(final ObjectNamedBooleanNode node) {
          booleans.add(node.getValue());
          return VisitResultType.CONTINUE;
        }
      };

      final VisitorDescendantAxis axis =
          VisitorDescendantAxis.newBuilder(rtx).includeSelf().visitor(visitor).build();
      while (axis.hasNext()) {
        axis.nextLong();
      }

      assertEquals(2, booleans.size());
      assertTrue(booleans.contains(true));
      assertTrue(booleans.contains(false));
    }
  }

  /**
   * Count null node visits. The test document has exactly one null value (in the first array).
   */
  @Test
  void testCountNullNodes() {
    try (final JsonResourceSession session =
             JsonTestHelper.getDatabase(PATHS.PATH1.getFile()).beginResourceSession(JsonTestHelper.RESOURCE);
         final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {

      final int[] count = {0};
      final JsonNodeVisitor visitor = new JsonNodeVisitor() {
        @Override
        public VisitResult visit(final ImmutableNullNode node) {
          count[0]++;
          return VisitResultType.CONTINUE;
        }

        @Override
        public VisitResult visit(final ObjectNamedNullNode node) {
          count[0]++;
          return VisitResultType.CONTINUE;
        }
      };

      final VisitorDescendantAxis axis =
          VisitorDescendantAxis.newBuilder(rtx).includeSelf().visitor(visitor).build();
      while (axis.hasNext()) {
        axis.nextLong();
      }

      assertEquals(1, count[0]);
    }
  }

  /**
   * Test SKIPSIBLINGS: when the visitor returns SKIPSIBLINGS on the first child of the
   * first array (the "bar" StringNode), the remaining array children (null, 2.33) should
   * be skipped.
   */
  @Test
  void testSkipSiblings() {
    try (final JsonResourceSession session =
             JsonTestHelper.getDatabase(PATHS.PATH1.getFile()).beginResourceSession(JsonTestHelper.RESOURCE);
         final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {

      final List<String> visitedStrings = new ArrayList<>(4);
      final int[] nullCount = {0};
      final int[] numberCount = {0};

      final JsonNodeVisitor visitor = new JsonNodeVisitor() {
        private boolean firstArrayChildSeen;

        @Override
        public VisitResult visit(final ImmutableStringNode node) {
          visitedStrings.add(node.getValue());
          if (!firstArrayChildSeen) {
            firstArrayChildSeen = true;
            return VisitResultType.SKIPSIBLINGS;
          }
          return VisitResultType.CONTINUE;
        }

        @Override
        public VisitResult visit(final ImmutableNullNode node) {
          nullCount[0]++;
          return VisitResultType.CONTINUE;
        }

        @Override
        public VisitResult visit(final ImmutableNumberNode node) {
          numberCount[0]++;
          return VisitResultType.CONTINUE;
        }
      };

      final VisitorDescendantAxis axis =
          VisitorDescendantAxis.newBuilder(rtx).includeSelf().visitor(visitor).build();
      while (axis.hasNext()) {
        axis.nextLong();
      }

      // "bar" in the first array was visited, but its siblings (null, 2.33) were skipped
      assertTrue(visitedStrings.contains("bar"));
      assertEquals(0, nullCount[0]);
      assertEquals(0, numberCount[0]);
    }
  }

  /**
   * Test SKIPSUBTREE: under iter#32 structural fusion, the inner object
   * {"hello":"world","helloo":true} is the fused OBJECT_NAMED_OBJECT "bar".
   * Returning SKIPSUBTREE there should skip its children (hello/world/helloo/true).
   */
  @Test
  void testSkipSubtree() {
    try (final JsonResourceSession session =
             JsonTestHelper.getDatabase(PATHS.PATH1.getFile()).beginResourceSession(JsonTestHelper.RESOURCE);
         final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {

      final List<String> visitedObjectKeys = new ArrayList<>(8);
      final List<String> visitedObjectStrings = new ArrayList<>(4);
      final int[] objectBooleanCount = {0};

      final JsonNodeVisitor visitor = new JsonNodeVisitor() {
        private void recordKeyName(final long nodeKey) {
          // Resolve the name via the transaction (cachedName may be null on the immutable wrapper)
          final long savedKey = rtx.getNodeKey();
          rtx.moveTo(nodeKey);
          visitedObjectKeys.add(rtx.getName().getLocalName());
          rtx.moveTo(savedKey);
        }

        @Override
        public VisitResult visit(final ObjectNamedStringNode node) {
          // Phase 4 fusion: OBJECT_NAMED_STRING plays the field-name role AND carries the value.
          recordKeyName(node.getNodeKey());
          visitedObjectStrings.add(node.getValue());
          return VisitResultType.CONTINUE;
        }

        @Override
        public VisitResult visit(final ObjectNamedBooleanNode node) {
          recordKeyName(node.getNodeKey());
          objectBooleanCount[0]++;
          return VisitResultType.CONTINUE;
        }

        @Override
        public VisitResult visit(final ObjectNamedObjectNode node) {
          // iter#32 P2: structural fusion — "bar" is the fused OBJECT_NAMED_OBJECT.
          // Skip its subtree to suppress hello/world/helloo/true.
          recordKeyName(node.getNodeKey());
          return VisitResultType.SKIPSUBTREE;
        }

        @Override
        public VisitResult visit(final ObjectNamedArrayNode node) {
          recordKeyName(node.getNodeKey());
          return VisitResultType.CONTINUE;
        }
      };

      final VisitorDescendantAxis axis =
          VisitorDescendantAxis.newBuilder(rtx).includeSelf().visitor(visitor).build();
      while (axis.hasNext()) {
        axis.nextLong();
      }

      // "hello" and "helloo" should NOT be in visited keys (they are children of the skipped object)
      assertFalse(visitedObjectKeys.contains("hello"));
      assertFalse(visitedObjectKeys.contains("helloo"));
      // "world" should NOT be in visited object strings
      assertFalse(visitedObjectStrings.contains("world"));
      // Only 1 ObjectBooleanNode should be visited (the "false" one in tada; the "true" was skipped)
      assertEquals(1, objectBooleanCount[0]);
      // Other keys should still be visited
      assertTrue(visitedObjectKeys.contains("foo"));
      assertTrue(visitedObjectKeys.contains("bar"));
      assertTrue(visitedObjectKeys.contains("baz"));
      assertTrue(visitedObjectKeys.contains("tada"));
    }
  }

  /**
   * Test using {@link VisitorDescendantAxis} with a counting visitor and verify
   * the axis iterates over all descendant nodes.
   */
  @Test
  void testVisitorDescendantAxisTraversal() {
    try (final JsonResourceSession session =
             JsonTestHelper.getDatabase(PATHS.PATH1.getFile()).beginResourceSession(JsonTestHelper.RESOURCE);
         final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {

      final NodeKindCounter counter = new NodeKindCounter();

      // Start from document root, include self
      rtx.moveToDocumentRoot();
      final VisitorDescendantAxis axis =
          VisitorDescendantAxis.newBuilder(rtx).includeSelf().visitor(counter).build();

      int totalNodesTraversed = 0;
      while (axis.hasNext()) {
        axis.nextLong();
        totalNodesTraversed++;
      }

      // After iter#32 fusion (primitive + structural) the test fixture has 18 records:
      // 5 primitive (key, primitive) pairs collapsed (iter#30) + 3 structural (foo+ARRAY,
      // bar+OBJECT, tada+ARRAY) collapsed (iter#32 P2). Pre-fusion was 26 nodes.
      assertEquals(18, totalNodesTraversed);

      final int totalVisited = counter.documentRootCount + counter.objectNodeCount
          + counter.arrayNodeCount + counter.objectKeyNodeCount + counter.stringNodeCount
          + counter.objectStringNodeCount + counter.nullNodeCount + counter.numberNodeCount
          + counter.objectBooleanNodeCount + counter.booleanNodeCount
          + counter.objectNumberNodeCount + counter.objectNullNodeCount
          + counter.objectNamedObjectNodeCount + counter.objectNamedArrayNodeCount;
      assertEquals(19, totalVisited);
    }
  }

  /**
   * Test calling {@code acceptVisitor} directly on a document root node.
   */
  @Test
  void testAcceptVisitorOnDocumentRoot() {
    try (final JsonResourceSession session =
             JsonTestHelper.getDatabase(PATHS.PATH1.getFile()).beginResourceSession(JsonTestHelper.RESOURCE);
         final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {

      rtx.moveToDocumentRoot();

      final boolean[] visited = {false};
      final JsonNodeVisitor visitor = new JsonNodeVisitor() {
        @Override
        public VisitResult visit(final ImmutableJsonDocumentRootNode node) {
          visited[0] = true;
          return VisitResultType.CONTINUE;
        }
      };

      final VisitResult result = rtx.acceptVisitor(visitor);

      assertTrue(visited[0]);
      assertEquals(VisitResultType.CONTINUE, result);
    }
  }

  /**
   * Test calling {@code acceptVisitor} directly on an object node verifies
   * that the correct visit method is invoked.
   */
  @Test
  void testAcceptVisitorOnObjectNode() {
    try (final JsonResourceSession session =
             JsonTestHelper.getDatabase(PATHS.PATH1.getFile()).beginResourceSession(JsonTestHelper.RESOURCE);
         final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {

      // Move to document root, then to the first child which is the top-level object
      rtx.moveToDocumentRoot();
      rtx.moveToFirstChild();
      assertTrue(rtx.isObject());

      final boolean[] visited = {false};
      final JsonNodeVisitor visitor = new JsonNodeVisitor() {
        @Override
        public VisitResult visit(final ImmutableObjectNode node) {
          visited[0] = true;
          return VisitResultType.CONTINUE;
        }
      };

      final VisitResult result = rtx.acceptVisitor(visitor);

      assertTrue(visited[0]);
      assertEquals(VisitResultType.CONTINUE, result);
    }
  }

  /**
   * Test calling {@code acceptVisitor} directly on an array node verifies
   * that the correct visit method is invoked.
   */
  @Test
  void testAcceptVisitorOnArrayNode() {
    try (final JsonResourceSession session =
             JsonTestHelper.getDatabase(PATHS.PATH1.getFile()).beginResourceSession(JsonTestHelper.RESOURCE);
         final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {

      // iter#32 fusion: legacy "ObjectKeyNode foo + ArrayNode" pair is now the single
      // fused OBJECT_NAMED_ARRAY at key 2. Navigate: docRoot -> object -> first child = fused.
      rtx.moveToDocumentRoot();
      rtx.moveToFirstChild();  // top-level object
      rtx.moveToFirstChild();  // OBJECT_NAMED_ARRAY "foo" (fused — IS-an-array)
      assertTrue(rtx.isArray());

      final boolean[] visited = {false};
      final JsonNodeVisitor visitor = new JsonNodeVisitor() {
        @Override
        public VisitResult visit(final ImmutableArrayNode node) {
          visited[0] = true;
          return VisitResultType.CONTINUE;
        }

        @Override
        public VisitResult visit(final ObjectNamedArrayNode node) {
          // Fused OBJECT_NAMED_ARRAY plays the ARRAY role.
          visited[0] = true;
          return VisitResultType.CONTINUE;
        }
      };

      final VisitResult result = rtx.acceptVisitor(visitor);

      assertTrue(visited[0]);
      assertEquals(VisitResultType.CONTINUE, result);
    }
  }

  /**
   * Private inner class that counts visits per node kind.
   */
  private static final class NodeKindCounter implements JsonNodeVisitor {
    int documentRootCount;
    int objectNodeCount;
    int arrayNodeCount;
    int objectKeyNodeCount;
    int stringNodeCount;
    int objectStringNodeCount;
    int nullNodeCount;
    int numberNodeCount;
    int objectBooleanNodeCount;
    int booleanNodeCount;
    int objectNumberNodeCount;
    int objectNullNodeCount;
    int objectNamedObjectNodeCount;
    int objectNamedArrayNodeCount;

    @Override
    public VisitResult visit(final ImmutableJsonDocumentRootNode node) {
      documentRootCount++;
      return VisitResultType.CONTINUE;
    }

    @Override
    public VisitResult visit(final ImmutableObjectNode node) {
      objectNodeCount++;
      return VisitResultType.CONTINUE;
    }

    @Override
    public VisitResult visit(final ImmutableArrayNode node) {
      arrayNodeCount++;
      return VisitResultType.CONTINUE;
    }

    @Override
    public VisitResult visit(final ImmutableStringNode node) {
      stringNodeCount++;
      return VisitResultType.CONTINUE;
    }

    @Override
    public VisitResult visit(final ObjectNamedStringNode node) {
      objectStringNodeCount++;
      return VisitResultType.CONTINUE;
    }

    @Override
    public VisitResult visit(final ImmutableNullNode node) {
      nullNodeCount++;
      return VisitResultType.CONTINUE;
    }

    @Override
    public VisitResult visit(final ImmutableNumberNode node) {
      numberNodeCount++;
      return VisitResultType.CONTINUE;
    }

    @Override
    public VisitResult visit(final ObjectNamedBooleanNode node) {
      objectBooleanNodeCount++;
      return VisitResultType.CONTINUE;
    }

    @Override
    public VisitResult visit(final ImmutableBooleanNode node) {
      booleanNodeCount++;
      return VisitResultType.CONTINUE;
    }

    @Override
    public VisitResult visit(final ObjectNamedNumberNode node) {
      objectNumberNodeCount++;
      return VisitResultType.CONTINUE;
    }

    @Override
    public VisitResult visit(final ObjectNamedNullNode node) {
      objectNullNodeCount++;
      return VisitResultType.CONTINUE;
    }

    @Override
    public VisitResult visit(final ObjectNamedObjectNode node) {
      objectNamedObjectNodeCount++;
      return VisitResultType.CONTINUE;
    }

    @Override
    public VisitResult visit(final ObjectNamedArrayNode node) {
      objectNamedArrayNodeCount++;
      return VisitResultType.CONTINUE;
    }
  }
}
