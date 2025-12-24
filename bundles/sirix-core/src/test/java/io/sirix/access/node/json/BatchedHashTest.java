package io.sirix.access.node.json;

import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.axis.DescendantAxis;
import io.sirix.axis.IncludeSelf;
import io.sirix.service.json.shredder.JsonShredder;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests that hash computation works correctly during bulk insert.
 * Verifies the batched hash optimization doesn't break hash integrity.
 */
public final class BatchedHashTest {

  private static final String RESOURCE = "testResource";

  @BeforeEach
  public void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  public void tearDown() {
    JsonTestHelper.closeEverything();
  }

  /**
   * Test that hashes are computed correctly after bulk insert via shredder.
   * Verifies that all nodes have non-zero hashes.
   */
  @Test
  void testHashesComputedAfterBulkInsert() {
    final Path dbPath = PATHS.PATH1.getFile();
    final String json = """
        {"users": [
          {"name": "Alice", "age": 30},
          {"name": "Bob", "age": 25}
        ]}
        """;

    if (!Files.exists(dbPath)) {
      Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));
    }

    try (final var database = Databases.openJsonDatabase(dbPath)) {
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE)
          .hashKind(HashType.ROLLING)
          .build());

      try (final var session = database.beginResourceSession(RESOURCE);
           final var wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json));
        wtx.commit();

        try (final var rtx = session.beginNodeReadOnlyTrx()) {
          // All non-document-root nodes should have non-zero hashes
          Long2LongOpenHashMap hashes = collectAllNodeHashes(rtx);
          for (var entry : hashes.long2LongEntrySet()) {
            // Skip document root (nodeKey=0) which doesn't have a hash
            if (entry.getLongKey() != 0) {
              assertNotEquals(0L, entry.getLongValue(), 
                  "Node " + entry.getLongKey() + " should have computed hash");
            }
          }

          // Root object hash should be non-zero
          rtx.moveToDocumentRoot();
          rtx.moveToFirstChild();
          assertNotEquals(0L, rtx.getHash(), "Root object should have hash");
        }
      }
    }
  }

  /**
   * Test that adding children updates parent hash.
   */
  @Test
  void testHashUpdateWhenAddingChildren() {
    final Path dbPath = PATHS.PATH1.getFile();

    if (!Files.exists(dbPath)) {
      Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));
    }

    try (final var database = Databases.openJsonDatabase(dbPath)) {
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE)
          .hashKind(HashType.ROLLING)
          .build());

      long hashAfterFirstChild;
      try (final var session = database.beginResourceSession(RESOURCE);
           final var wtx = session.beginNodeTrx()) {
        // Insert array with one element
        wtx.insertArrayAsFirstChild();
        wtx.insertNumberValueAsFirstChild(1);
        wtx.commit();

        try (final var rtx = session.beginNodeReadOnlyTrx()) {
          rtx.moveToDocumentRoot();
          rtx.moveToFirstChild();  // array
          hashAfterFirstChild = rtx.getHash();
        }
      }

      long hashAfterSecondChild;
      try (final var session = database.beginResourceSession(RESOURCE);
           final var wtx = session.beginNodeTrx()) {
        // Add another element
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild();  // array
        wtx.moveToFirstChild();  // first number
        wtx.insertNumberValueAsRightSibling(2);
        wtx.commit();

        try (final var rtx = session.beginNodeReadOnlyTrx()) {
          rtx.moveToDocumentRoot();
          rtx.moveToFirstChild();  // array
          hashAfterSecondChild = rtx.getHash();
        }
      }

      // Parent hash should change when adding a child
      assertNotEquals(hashAfterFirstChild, hashAfterSecondChild,
          "Array hash should change when adding child");
    }
  }

  /**
   * Test that bulk insert produces valid hierarchical hashes.
   * Parent hashes should be different from child hashes.
   */
  @Test
  void testHierarchicalHashIntegrity() {
    final Path dbPath = PATHS.PATH1.getFile();
    final String json = """
        {"parent": {"child": "value"}}
        """;

    if (!Files.exists(dbPath)) {
      Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));
    }

    try (final var database = Databases.openJsonDatabase(dbPath)) {
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE)
          .hashKind(HashType.ROLLING)
          .build());

      try (final var session = database.beginResourceSession(RESOURCE);
           final var wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json));
        wtx.commit();

        try (final var rtx = session.beginNodeReadOnlyTrx()) {
          rtx.moveToDocumentRoot();
          rtx.moveToFirstChild();  // outer object
          long outerHash = rtx.getHash();
          
          rtx.moveToFirstChild();  // "parent" key
          rtx.moveToFirstChild();  // inner object
          long innerHash = rtx.getHash();
          
          rtx.moveToFirstChild();  // "child" key
          rtx.moveToFirstChild();  // "value" string
          long leafHash = rtx.getHash();
          
          // All hashes should be non-zero
          assertNotEquals(0L, outerHash);
          assertNotEquals(0L, innerHash);
          assertNotEquals(0L, leafHash);
          
          // Parent and child hashes should be different
          assertNotEquals(outerHash, innerHash, 
              "Parent and child hashes should be different");
          assertNotEquals(innerHash, leafHash,
              "Parent and leaf hashes should be different");
        }
      }
    }
  }

  /**
   * Test that large bulk inserts produce valid hashes.
   * Tests the batched hash optimization with many nodes.
   */
  @Test
  void testLargeBulkInsertHashes() {
    final Path dbPath = PATHS.PATH1.getFile();
    
    // Generate JSON with 100 array elements
    StringBuilder jsonBuilder = new StringBuilder("[");
    for (int i = 0; i < 100; i++) {
      if (i > 0) jsonBuilder.append(",");
      jsonBuilder.append(i);
    }
    jsonBuilder.append("]");
    final String json = jsonBuilder.toString();

    if (!Files.exists(dbPath)) {
      Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));
    }

    try (final var database = Databases.openJsonDatabase(dbPath)) {
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE)
          .hashKind(HashType.ROLLING)
          .build());

      try (final var session = database.beginResourceSession(RESOURCE);
           final var wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json));
        wtx.commit();

        try (final var rtx = session.beginNodeReadOnlyTrx()) {
          // Count nodes and verify all have hashes
          Long2LongOpenHashMap hashes = collectAllNodeHashes(rtx);
          
          // Should have: 1 document root + 1 array + 100 number values = 102 nodes
          assertTrue(hashes.size() >= 101, 
              "Should have at least 101 nodes (array + 100 children)");
          
          // All non-document-root nodes should have non-zero hashes
          int nodesWithHash = 0;
          for (var entry : hashes.long2LongEntrySet()) {
            if (entry.getLongKey() != 0 && entry.getLongValue() != 0) {
              nodesWithHash++;
            }
          }
          assertTrue(nodesWithHash >= 100, 
              "At least 100 nodes should have non-zero hashes");
        }
      }
    }
  }

  /**
   * Test that hashes are stable across revisions for diffing.
   * Same node should have same hash if unchanged between revisions.
   */
  @Test
  void testHashStabilityForDiffing() {
    final Path dbPath = PATHS.PATH1.getFile();

    if (!Files.exists(dbPath)) {
      Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));
    }

    try (final var database = Databases.openJsonDatabase(dbPath)) {
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE)
          .hashKind(HashType.ROLLING)
          .build());

      // Insert initial structure and get hash
      long initialArrayHash;
      long initialFirstChildHash;
      try (final var session = database.beginResourceSession(RESOURCE);
           final var wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[1, 2, 3]"));
        wtx.commit();

        try (final var rtx = session.beginNodeReadOnlyTrx()) {
          rtx.moveToDocumentRoot();
          rtx.moveToFirstChild();  // array
          initialArrayHash = rtx.getHash();
          rtx.moveToFirstChild();  // first number (1)
          initialFirstChildHash = rtx.getHash();
        }
      }

      // Add a new element - array hash should change, but first child hash should stay same
      long afterAddArrayHash;
      long afterAddFirstChildHash;
      try (final var session = database.beginResourceSession(RESOURCE);
           final var wtx = session.beginNodeTrx()) {
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild();  // array
        wtx.moveToLastChild();   // last number (3)
        wtx.insertNumberValueAsRightSibling(4);  // add 4
        wtx.commit();

        try (final var rtx = session.beginNodeReadOnlyTrx()) {
          rtx.moveToDocumentRoot();
          rtx.moveToFirstChild();  // array
          afterAddArrayHash = rtx.getHash();
          rtx.moveToFirstChild();  // first number (1)
          afterAddFirstChildHash = rtx.getHash();
        }
      }

      // Array hash should change (new child added)
      assertNotEquals(initialArrayHash, afterAddArrayHash,
          "Parent hash should change when child added");
      
      // First child hash should remain the same (unchanged node)
      assertEquals(initialFirstChildHash, afterAddFirstChildHash,
          "Unchanged node should have same hash across revisions");
    }
  }

  private Long2LongOpenHashMap collectAllNodeHashes(JsonNodeReadOnlyTrx rtx) {
    Long2LongOpenHashMap hashes = new Long2LongOpenHashMap();
    rtx.moveToDocumentRoot();
    new DescendantAxis(rtx, IncludeSelf.YES).forEach(unused ->
        hashes.put(rtx.getNodeKey(), rtx.getHash()));
    return hashes;
  }
}
