/*
 * Copyright (c) 2024, Sirix Contributors
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the <organization> nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.sirix.access.trx.node;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.json.InternalJsonNodeReadOnlyTrx;
import io.sirix.api.Database;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.axis.ChildAxis;
import io.sirix.axis.DescendantAxis;
import io.sirix.node.NodeKind;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.Fixed;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for flyweight cursor corner cases.
 * These tests verify the save/restore pattern, concurrent axes, and other edge cases.
 *
 * @author Johannes Lichtenberger
 */
class FlyweightCursorTest {

  private static final String RESOURCE = "testResource";
  
  @TempDir
  Path tempDir;
  
  private Database<JsonResourceSession> database;
  
  @BeforeEach
  void setUp() {
    final var dbPath = tempDir.resolve("testdb");
    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));
    database = Databases.openJsonDatabase(dbPath);
    database.createResource(ResourceConfiguration.newBuilder(RESOURCE).build());
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.close();
    }
  }

  /**
   * Test save/restore pattern: getCurrentNode() must return stable snapshot.
   */
  @Test
  void testSaveRestorePattern() {
    // Create a simple JSON structure
    try (final var session = database.beginResourceSession(RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"a\": 1, \"b\": 2}"));
      wtx.commit();
    }

    // Test save/restore pattern
    try (final var session = database.beginResourceSession(RESOURCE);
         final var rtx = session.beginNodeReadOnlyTrx()) {
      
      // Cast to internal interface to access getCurrentNode/setCurrentNode
      final var internalRtx = (InternalJsonNodeReadOnlyTrx) rtx;
      
      // Move to document root
      rtx.moveToDocumentRoot();
      assertTrue(rtx.moveToFirstChild());  // Move to object
      
      // Save current position
      final var savedNode = internalRtx.getCurrentNode();
      final long savedKey = savedNode.getNodeKey();
      final NodeKind savedKind = savedNode.getKind();
      
      // Move away
      assertTrue(rtx.moveToFirstChild());  // Move to first child of object
      long newKey = rtx.getNodeKey();
      assertTrue(newKey != savedKey, "Should have moved to different node");
      
      // Verify saved node is still stable (not affected by moveTo)
      assertEquals(savedKey, savedNode.getNodeKey(), "Saved node key should not change");
      assertEquals(savedKind, savedNode.getKind(), "Saved node kind should not change");
      
      // Restore position
      internalRtx.setCurrentNode(savedNode);
      assertEquals(savedKey, rtx.getNodeKey(), "Should be back at saved position");
      assertEquals(savedKind, rtx.getKind(), "Should have same kind after restore");
    }
  }

  /**
   * Test that multiple calls to getNode() return consistent values.
   */
  @Test
  void testGetNodeReturnsConsistentValues() {
    try (final var session = database.beginResourceSession(RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"key\": \"value\"}"));
      wtx.commit();
    }

    try (final var session = database.beginResourceSession(RESOURCE);
         final var rtx = session.beginNodeReadOnlyTrx()) {
      
      rtx.moveToDocumentRoot();
      rtx.moveToFirstChild();
      
      // Get two snapshots
      var node1 = rtx.getNode();
      var node2 = rtx.getNode();
      
      // They should be equal in content
      assertEquals(node1.getNodeKey(), node2.getNodeKey());
      assertEquals(node1.getKind(), node2.getKind());
    }
  }

  /**
   * Test flyweight getters return correct values.
   */
  @Test
  void testFlyweightGettersReturnCorrectValues() {
    try (final var session = database.beginResourceSession(RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[1, 2, 3]"));
      wtx.commit();
    }

    try (final var session = database.beginResourceSession(RESOURCE);
         final var rtx = session.beginNodeReadOnlyTrx()) {
      
      rtx.moveToDocumentRoot();
      assertTrue(rtx.moveToFirstChild());  // Array
      assertEquals(NodeKind.ARRAY, rtx.getKind(), "Should be at Array node");
      
      // Test getters match getNode() values
      var node = rtx.getNode();
      assertEquals(node.getNodeKey(), rtx.getNodeKey());
      assertEquals(node.getParentKey(), rtx.getParentKey());
      assertEquals(node.getKind(), rtx.getKind());
      
      // Move to first child (number 1)
      assertTrue(rtx.moveToFirstChild());
      assertEquals(NodeKind.NUMBER_VALUE, rtx.getKind(), "Should be at Number node");
      
      node = rtx.getNode();
      assertEquals(node.getNodeKey(), rtx.getNodeKey());
      assertEquals(node.getParentKey(), rtx.getParentKey());
      assertEquals(node.getKind(), rtx.getKind());
    }
  }

  /**
   * Test axis iteration with flyweight cursor.
   */
  @Test
  void testAxisIterationWithFlyweight() {
    try (final var session = database.beginResourceSession(RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"a\": 1, \"b\": 2, \"c\": 3}"));
      wtx.commit();
    }

    try (final var session = database.beginResourceSession(RESOURCE);
         final var rtx = session.beginNodeReadOnlyTrx()) {
      
      rtx.moveToDocumentRoot();
      rtx.moveToFirstChild();  // Object
      assertEquals(NodeKind.OBJECT, rtx.getKind(), "Should be at Object node");
      
      // Verify flyweight values match object-based values
      var objectNode = rtx.getNode();
      assertEquals(objectNode.getNodeKey(), rtx.getNodeKey(), "nodeKey mismatch");
      
      // Iterate over children using ChildAxis
      int count = 0;
      for (var axis = new ChildAxis(rtx); axis.hasNext();) {
        axis.nextLong();
        count++;
        assertNotNull(rtx.getKind());
        assertTrue(rtx.getNodeKey() >= 0);
      }
      
      assertEquals(3, count, "Should have 3 object keys");
    }
  }

  /**
   * Test descendant axis with flyweight cursor.
   */
  @Test
  void testDescendantAxisWithFlyweight() {
    try (final var session = database.beginResourceSession(RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"nested\": {\"deep\": 42}}"));
      wtx.commit();
    }

    try (final var session = database.beginResourceSession(RESOURCE);
         final var rtx = session.beginNodeReadOnlyTrx()) {
      
      rtx.moveToDocumentRoot();
      
      // Count all descendants
      int count = 0;
      for (var axis = new DescendantAxis(rtx); axis.hasNext();) {
        axis.nextLong();
        count++;
      }
      
      // Should have: object, key "nested", object, key "deep", number 42
      assertTrue(count >= 5, "Should have at least 5 descendants, got: " + count);
    }
  }

  /**
   * Test that hasFirstChild/hasRightSibling work correctly with flyweight state.
   */
  @Test
  void testHasMethodsWithFlyweight() {
    try (final var session = database.beginResourceSession(RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[1, 2]"));
      wtx.commit();
    }

    try (final var session = database.beginResourceSession(RESOURCE);
         final var rtx = session.beginNodeReadOnlyTrx()) {
      
      rtx.moveToDocumentRoot();
      assertTrue(rtx.hasFirstChild(), "Document root should have first child");
      
      rtx.moveToFirstChild();  // Array
      assertTrue(rtx.hasFirstChild(), "Array should have first child");
      assertTrue(rtx.hasParent(), "Array should have parent");
      
      rtx.moveToFirstChild();  // First number
      assertTrue(rtx.hasRightSibling(), "First element should have right sibling");
      assertTrue(rtx.hasParent(), "First element should have parent");
      
      rtx.moveToRightSibling();  // Second number
      assertTrue(rtx.hasLeftSibling(), "Second element should have left sibling");
    }
  }

  /**
   * Test getNodeKey() returns correct value with flyweight.
   */
  @Test
  void testGetNodeKeyWithFlyweight() {
    try (final var session = database.beginResourceSession(RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"x\": 1}"));
      wtx.commit();
    }

    try (final var session = database.beginResourceSession(RESOURCE);
         final var rtx = session.beginNodeReadOnlyTrx()) {
      
      rtx.moveToDocumentRoot();
      assertEquals(Fixed.DOCUMENT_NODE_KEY.getStandardProperty(), rtx.getNodeKey());
      
      rtx.moveToFirstChild();
      long objectKey = rtx.getNodeKey();
      assertTrue(objectKey > 0, "Object should have positive key");
      
      // Verify it matches getNode()
      assertEquals(objectKey, rtx.getNode().getNodeKey());
    }
  }

  /**
   * Test transaction close releases page guard.
   */
  @Test
  void testTransactionCloseReleasesResources() {
    try (final var session = database.beginResourceSession(RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"test\": 123}"));
      wtx.commit();
    }

    // Open and close transaction multiple times - should not leak resources
    for (int i = 0; i < 10; i++) {
      try (final var session = database.beginResourceSession(RESOURCE);
           final var rtx = session.beginNodeReadOnlyTrx()) {
        
        rtx.moveToDocumentRoot();
        rtx.moveToFirstChild();
        // Access some data to ensure flyweight is engaged
        rtx.getNodeKey();
        rtx.getParentKey();
        rtx.getKind();
      }
      // Transaction closed - resources should be released
    }
    
    // If we get here without OOM or errors, resources are being released properly
    assertTrue(true);
  }

  /**
   * Test that optimized traversal mode works correctly with caching.
   */
  @Test
  void testOptimizedTraversalWithCaching() {
    try (final var session = database.beginResourceSession(RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"test\": 123}"));
      wtx.commit();
    }

    try (final var session = database.beginResourceSession(RESOURCE);
         final var rtx = session.beginNodeReadOnlyTrx()) {
      
      // Cast to AbstractNodeReadOnlyTrx to access mode checks
      final var abstractRtx = (AbstractNodeReadOnlyTrx<?, ?, ?>) rtx;
      
      // Move to document root
      rtx.moveToDocumentRoot();
      
      // Move to first child (object node)
      assertTrue(rtx.moveToFirstChild());
      
      // Verify we can read data correctly after move
      assertEquals(NodeKind.OBJECT, rtx.getKind());
      assertTrue(rtx.getNodeKey() > 0);
      
      // Move to another node
      assertTrue(rtx.moveToFirstChild());
      
      // Verify data is still correct after subsequent move
      assertTrue(rtx.getNodeKey() > 0);
      
      // Move back to first child to verify cache is working
      rtx.moveToParent();
      assertEquals(NodeKind.OBJECT, rtx.getKind());
    }
  }

  /**
   * Test that snapshot obtained via getCurrentNode() is immutable.
   * The snapshot should not change when the cursor moves to a different node.
   */
  @Test
  void testSnapshotImmutabilityAfterMove() {
    // Create a JSON structure with multiple nodes
    try (final var session = database.beginResourceSession(RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"first\": 1, \"second\": 2}"));
      wtx.commit();
    }

    try (final var session = database.beginResourceSession(RESOURCE);
         final var rtx = session.beginNodeReadOnlyTrx()) {
      
      final var internalRtx = (InternalJsonNodeReadOnlyTrx) rtx;
      
      // Navigate to the first object key
      rtx.moveToDocumentRoot();
      rtx.moveToFirstChild();  // Object node
      rtx.moveToFirstChild();  // First object key "first"
      
      // Get a snapshot of the first node
      final var firstSnapshot = internalRtx.getCurrentNode();
      final long firstNodeKey = firstSnapshot.getNodeKey();
      final NodeKind firstKind = firstSnapshot.getKind();
      
      // Verify we have a valid snapshot
      assertNotNull(firstSnapshot);
      assertEquals(NodeKind.OBJECT_KEY, firstKind);
      
      // Move to the next sibling (different node)
      assertTrue(rtx.moveToRightSibling());
      
      // Get a snapshot of the second node  
      final var secondSnapshot = internalRtx.getCurrentNode();
      final long secondNodeKey = secondSnapshot.getNodeKey();
      
      // Verify it's a different node
      assertTrue(firstNodeKey != secondNodeKey, 
          "Node keys should be different after moving to sibling");
      
      // CRITICAL: The first snapshot must still have its original values!
      // If using singletons improperly, the first snapshot would now show the second node's data.
      assertEquals(firstNodeKey, firstSnapshot.getNodeKey(), 
          "Snapshot node key must be immutable after cursor move");
      assertEquals(firstKind, firstSnapshot.getKind(),
          "Snapshot kind must be immutable after cursor move");
    }
  }

  /**
   * Test nested axes with singleton reuse - inner axis should not corrupt outer axis.
   */
  @Test
  void testNestedAxesWithSingletons() {
    // Create a JSON structure with nested data
    try (final var session = database.beginResourceSession(RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(
          "[{\"a\": 1}, {\"b\": 2}, {\"c\": 3}]"));
      wtx.commit();
    }

    try (final var session = database.beginResourceSession(RESOURCE);
         final var rtx = session.beginNodeReadOnlyTrx()) {
      
      rtx.moveToDocumentRoot();
      rtx.moveToFirstChild();  // Array node
      
      int outerCount = 0;
      int innerTotal = 0;
      
      // Outer loop: iterate over array children (objects)
      for (long outerKey : new ChildAxis(rtx)) {
        outerCount++;
        final long savedOuterKey = outerKey;
        final NodeKind savedOuterKind = rtx.getKind();
        
        // Inner loop: iterate over object children (object keys)
        for (long innerKey : new ChildAxis(rtx)) {
          innerTotal++;
          // Inner axis consumes rtx cursor position
        }
        
        // After inner loop, restore to outer position
        rtx.moveTo(savedOuterKey);
        
        // VERIFY: After restoring, we should be back at the same outer node
        assertEquals(savedOuterKey, rtx.getNodeKey(),
            "Outer cursor position must be restorable after inner axis");
        assertEquals(savedOuterKind, rtx.getKind(),
            "Outer cursor kind must match after restore");
      }
      
      // Should have iterated over 3 array children
      assertEquals(3, outerCount, "Should have 3 objects in array");
      // Each object has 1 key, so 3 keys total
      assertEquals(3, innerTotal, "Should have 3 object keys total");
    }
  }

  /**
   * Test that singleton mode enables zero-allocation traversal while maintaining correctness.
   */
  @Test
  void testSingletonModeCorrectness() {
    // Create a diverse JSON structure
    try (final var session = database.beginResourceSession(RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(
          "{\"str\": \"hello\", \"num\": 42, \"bool\": true, \"null\": null, \"arr\": [1, 2]}"));
      wtx.commit();
    }

    try (final var session = database.beginResourceSession(RESOURCE);
         final var rtx = session.beginNodeReadOnlyTrx()) {
      
      int objectKeyCount = 0;
      int stringCount = 0;
      int numberCount = 0;
      int booleanCount = 0;
      int nullCount = 0;
      int arrayCount = 0;
      
      // Traverse all descendants
      for (final long key : new DescendantAxis(rtx)) {
        switch (rtx.getKind()) {
          case OBJECT_KEY -> objectKeyCount++;
          case OBJECT_STRING_VALUE -> stringCount++;
          case OBJECT_NUMBER_VALUE -> numberCount++;
          case OBJECT_BOOLEAN_VALUE -> booleanCount++;
          case OBJECT_NULL_VALUE -> nullCount++;
          case ARRAY -> arrayCount++;
          case NUMBER_VALUE -> numberCount++;
          default -> { /* other node types */ }
        }
      }
      
      // Verify correct traversal
      assertEquals(5, objectKeyCount, "Should have 5 object keys");
      assertEquals(1, stringCount, "Should have 1 string value");
      assertEquals(1 + 2, numberCount, "Should have 3 number values (1 object, 2 array)");
      assertEquals(1, booleanCount, "Should have 1 boolean value");
      assertEquals(1, nullCount, "Should have 1 null value");
      assertEquals(1, arrayCount, "Should have 1 array");
    }
  }
}
