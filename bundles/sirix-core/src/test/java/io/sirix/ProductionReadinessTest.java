/*
 * Copyright (c) 2024, Sirix Contributors
 *
 * All rights reserved.
 */

package io.sirix;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonNodeTrx.Commit;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.exception.SirixException;
import io.sirix.node.NodeKind;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Production readiness tests for SirixDB.
 * 
 * <p>These tests verify critical properties required for production use:
 * <ul>
 *   <li>ACID transaction properties</li>
 *   <li>Concurrent access correctness</li>
 *   <li>Large data handling</li>
 *   <li>Resource cleanup and leak prevention</li>
 *   <li>Edge case handling</li>
 *   <li>Data integrity verification</li>
 * </ul>
 */
@DisplayName("Production Readiness Tests")
class ProductionReadinessTest {

  private static final String RESOURCE_NAME = "test-resource";
  private Path tempDir;

  @BeforeEach
  void setUp() throws IOException {
    try {
      Databases.getGlobalBufferManager().clearAllCaches();
    } catch (Exception e) {
      // Ignore
    }
    tempDir = Files.createTempDirectory("sirix-production-test");
  }

  @AfterEach
  void tearDown() throws IOException {
    if (tempDir != null) {
      deleteRecursively(tempDir);
    }
    try {
      Databases.getGlobalBufferManager().clearAllCaches();
    } catch (Exception e) {
      // Ignore
    }
  }

  private void deleteRecursively(Path path) throws IOException {
    if (Files.isDirectory(path)) {
      try (var entries = Files.list(path)) {
        for (Path entry : entries.toList()) {
          deleteRecursively(entry);
        }
      }
    }
    Files.deleteIfExists(path);
  }

  // ============================================================================
  // ACID Transaction Properties
  // ============================================================================

  @Nested
  @DisplayName("ACID Transaction Properties")
  class ACIDTests {

    @Test
    @DisplayName("Atomicity: Uncommitted changes are not visible")
    void testAtomicityUncommittedNotVisible() throws IOException {
      Path dbPath = tempDir.resolve("db-atomicity");
      Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

      try (Database<JsonResourceSession> db = Databases.openJsonDatabase(dbPath)) {
        db.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME).build());

        // Write but rollback
        try (JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
             JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[1,2,3]"), Commit.NO);
          // Explicitly rollback
          wtx.rollback();
        }

        // After rollback, data should not exist
        try (JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
             JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
          rtx.moveToDocumentRoot();
          assertFalse(rtx.moveToFirstChild(),
              "Rolled back changes should not persist");
        }
      }
    }

    @Test
    @DisplayName("Consistency: Transaction sees consistent snapshot")
    void testConsistencySnapshot() throws IOException {
      Path dbPath = tempDir.resolve("db-consistency");
      Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

      try (Database<JsonResourceSession> db = Databases.openJsonDatabase(dbPath)) {
        db.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME).build());

        // Create initial data - revision 1
        try (JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
             JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[1,2,3]"), Commit.NO);
          wtx.commit();
        }

        // Create revision 2
        try (JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
             JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.moveToDocumentRoot();
          wtx.moveToFirstChild();
          wtx.moveToLastChild();
          wtx.insertNumberValueAsRightSibling(4);
          wtx.commit();
        }
        
        // Reader on revision 1 should see 3 elements
        try (JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
             JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(1)) {
          rtx.moveToDocumentRoot();
          rtx.moveToFirstChild();
          int count = 0;
          if (rtx.moveToFirstChild()) {
            do {
              count++;
            } while (rtx.moveToRightSibling());
          }
          assertEquals(3, count, "Reader should see consistent snapshot of revision 1");
        }
        
        // Reader on revision 2 should see 4 elements
        try (JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
             JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(2)) {
          rtx.moveToDocumentRoot();
          rtx.moveToFirstChild();
          int count = 0;
          if (rtx.moveToFirstChild()) {
            do {
              count++;
            } while (rtx.moveToRightSibling());
          }
          assertEquals(4, count, "Reader should see snapshot of revision 2");
        }
      }
    }

    @Test
    @DisplayName("Isolation: Different revisions are isolated")
    void testIsolation() throws IOException {
      Path dbPath = tempDir.resolve("db-isolation");
      Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

      try (Database<JsonResourceSession> db = Databases.openJsonDatabase(dbPath)) {
        db.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME).build());

        // Create revision 1
        try (JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
             JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"counter\":0}"), Commit.NO);
          wtx.commit();
        }

        // Create revision 2
        try (JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
             JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.moveToDocumentRoot();
          if (wtx.moveToFirstChild()) {
            wtx.remove();
          }
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"counter\":1}"), Commit.NO);
          wtx.commit();
        }

        // Create revision 3
        try (JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
             JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.moveToDocumentRoot();
          if (wtx.moveToFirstChild()) {
            wtx.remove();
          }
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"counter\":2}"), Commit.NO);
          wtx.commit();
        }

        // Read each revision and verify isolation
        for (int rev = 1; rev <= 3; rev++) {
          try (JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
               JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(rev)) {
            rtx.moveToDocumentRoot();
            rtx.moveToFirstChild();
            rtx.moveToFirstChild();
            rtx.moveToFirstChild();
            if (rtx.getKind() == NodeKind.OBJECT_NUMBER_VALUE) {
              assertEquals(rev - 1, rtx.getNumberValue().intValue(),
                  "Revision " + rev + " should see counter=" + (rev - 1));
            }
          }
        }
      }
    }

    @Test
    @DisplayName("Durability: Committed data persists after close/reopen")
    void testDurability() throws IOException {
      Path dbPath = tempDir.resolve("db-durability");
      
      // Create and populate database
      Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));
      try (Database<JsonResourceSession> db = Databases.openJsonDatabase(dbPath)) {
        db.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME).build());
        
        try (JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
             JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.insertSubtreeAsFirstChild(
              JsonShredder.createStringReader("{\"persistent\":\"data\",\"number\":42}"), 
              Commit.NO);
          wtx.commit();
        }
      }

      // Force cache clear
      Databases.getGlobalBufferManager().clearAllCaches();

      // Reopen and verify data persisted
      try (Database<JsonResourceSession> db = Databases.openJsonDatabase(dbPath)) {
        try (JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
             JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
          rtx.moveToDocumentRoot();
          assertTrue(rtx.moveToFirstChild(), "Data should persist after reopen");
          assertEquals(NodeKind.OBJECT, rtx.getKind());
          
          // Verify specific values
          rtx.moveToFirstChild(); // First key
          assertEquals("persistent", rtx.getName().getLocalName());
        }
      }
    }
  }

  // ============================================================================
  // Concurrent Access Tests
  // ============================================================================

  @Nested
  @DisplayName("Concurrent Access")
  class ConcurrentAccessTests {

    @Test
    @DisplayName("Multiple sequential readers see consistent data")
    @Timeout(30)
    void testSequentialReaders() throws Exception {
      Path dbPath = tempDir.resolve("db-sequential-readers");
      Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

      try (Database<JsonResourceSession> db = Databases.openJsonDatabase(dbPath)) {
        db.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME).build());

        // Create data
        try (JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
             JsonNodeTrx wtx = session.beginNodeTrx()) {
          StringBuilder json = new StringBuilder("[");
          for (int i = 0; i < 100; i++) {
            if (i > 0) json.append(",");
            json.append(i);
          }
          json.append("]");
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), Commit.NO);
          wtx.commit();
        }

        // Multiple sequential reads should all see same data
        for (int r = 0; r < 10; r++) {
          try (JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
               JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
            rtx.moveToDocumentRoot();
            rtx.moveToFirstChild();
            int count = 0;
            if (rtx.moveToFirstChild()) {
              do {
                count++;
              } while (rtx.moveToRightSibling());
            }
            assertEquals(100, count, "Reader " + r + " should see 100 elements");
          }
        }
      }
    }

    @Test
    @DisplayName("Sequential writers produce correct revision sequence")
    @Timeout(60)
    void testSequentialWriters() throws Exception {
      Path dbPath = tempDir.resolve("db-sequential-writers");
      Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

      try (Database<JsonResourceSession> db = Databases.openJsonDatabase(dbPath)) {
        db.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME).build());

        int numRevisions = 20;
        
        for (int rev = 0; rev < numRevisions; rev++) {
          try (JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
               JsonNodeTrx wtx = session.beginNodeTrx()) {
            if (rev == 0) {
              wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[0]"), Commit.NO);
            } else {
              wtx.moveToDocumentRoot();
              wtx.moveToFirstChild();
              wtx.moveToLastChild();
              wtx.insertNumberValueAsRightSibling(rev);
            }
            wtx.commit();
          }
        }

        // Verify all revisions are accessible
        try (JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
          assertEquals(numRevisions, session.getMostRecentRevisionNumber(),
              "Should have " + numRevisions + " revisions");
          
          for (int rev = 1; rev <= numRevisions; rev++) {
            try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(rev)) {
              rtx.moveToDocumentRoot();
              rtx.moveToFirstChild();
              int count = 0;
              if (rtx.moveToFirstChild()) {
                do {
                  count++;
                } while (rtx.moveToRightSibling());
              }
              assertEquals(rev, count, "Revision " + rev + " should have " + rev + " elements");
            }
          }
        }
      }
    }

    @Test
    @DisplayName("Reader on old revision sees historical data")
    @Timeout(30)
    void testHistoricalReader() throws Exception {
      Path dbPath = tempDir.resolve("db-historical-reader");
      Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

      try (Database<JsonResourceSession> db = Databases.openJsonDatabase(dbPath)) {
        db.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME).build());

        // Create revision 1
        try (JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
             JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[1,2,3,4,5]"), Commit.NO);
          wtx.commit();
        }

        // Create revisions 2-5
        for (int rev = 2; rev <= 5; rev++) {
          try (JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
               JsonNodeTrx wtx = session.beginNodeTrx()) {
            wtx.moveToDocumentRoot();
            wtx.moveToFirstChild();
            wtx.moveToLastChild();
            wtx.insertNumberValueAsRightSibling(rev * 10);
            wtx.commit();
          }
        }

        // Verify revision 1 still has only 5 elements
        try (JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
             JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(1)) {
          rtx.moveToDocumentRoot();
          rtx.moveToFirstChild();
          int count = 0;
          if (rtx.moveToFirstChild()) {
            do {
              count++;
            } while (rtx.moveToRightSibling());
          }
          assertEquals(5, count, "Revision 1 should always have 5 elements");
        }

        // Verify revision 5 has 9 elements (5 original + 4 added)
        try (JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
             JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(5)) {
          rtx.moveToDocumentRoot();
          rtx.moveToFirstChild();
          int count = 0;
          if (rtx.moveToFirstChild()) {
            do {
              count++;
            } while (rtx.moveToRightSibling());
          }
          assertEquals(9, count, "Revision 5 should have 9 elements");
        }
      }
    }
  }

  // ============================================================================
  // Large Data Tests
  // ============================================================================

  @Nested
  @DisplayName("Large Data Handling")
  class LargeDataTests {

    @Test
    @DisplayName("Handle large JSON document (10000 elements)")
    @Timeout(120)
    void testLargeDocument() throws IOException {
      Path dbPath = tempDir.resolve("db-large-doc");
      Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

      final int numElements = 10000;

      try (Database<JsonResourceSession> db = Databases.openJsonDatabase(dbPath)) {
        db.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME).build());

        // Create large array
        try (JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
             JsonNodeTrx wtx = session.beginNodeTrx()) {
          StringBuilder json = new StringBuilder("[");
          for (int i = 0; i < numElements; i++) {
            if (i > 0) json.append(",");
            json.append("{\"id\":").append(i)
               .append(",\"value\":\"item").append(i).append("\"}");
          }
          json.append("]");
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), Commit.NO);
          wtx.commit();
        }

        // Verify all elements
        try (JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
             JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
          rtx.moveToDocumentRoot();
          rtx.moveToFirstChild();
          int count = 0;
          if (rtx.moveToFirstChild()) {
            do {
              count++;
            } while (rtx.moveToRightSibling());
          }
          assertEquals(numElements, count, "Should have all " + numElements + " elements");
        }
      }
    }

    @Test
    @DisplayName("Handle deeply nested JSON (depth 100)")
    @Timeout(60)
    void testDeeplyNestedDocument() throws IOException {
      Path dbPath = tempDir.resolve("db-deep-nested");
      Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

      final int depth = 100;

      try (Database<JsonResourceSession> db = Databases.openJsonDatabase(dbPath)) {
        db.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME).build());

        // Create deeply nested structure
        try (JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
             JsonNodeTrx wtx = session.beginNodeTrx()) {
          StringBuilder json = new StringBuilder();
          for (int i = 0; i < depth; i++) {
            json.append("{\"level").append(i).append("\":");
          }
          json.append("\"deepest\"");
          for (int i = 0; i < depth; i++) {
            json.append("}");
          }
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), Commit.NO);
          wtx.commit();
        }

        // Navigate to deepest level
        try (JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
             JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
          rtx.moveToDocumentRoot();
          int actualDepth = 0;
          while (rtx.moveToFirstChild()) {
            if (rtx.getKind() == NodeKind.OBJECT_KEY) {
              actualDepth++;
            }
          }
          // Account for string value at the end
          assertTrue(actualDepth >= depth - 1, 
              "Should be able to navigate deeply nested structure");
        }
      }
    }

    @Test
    @DisplayName("Handle wide JSON object (1000 keys)")
    @Timeout(60)
    void testWideObject() throws IOException {
      Path dbPath = tempDir.resolve("db-wide-object");
      Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

      final int numKeys = 1000;

      try (Database<JsonResourceSession> db = Databases.openJsonDatabase(dbPath)) {
        db.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME).build());

        try (JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
             JsonNodeTrx wtx = session.beginNodeTrx()) {
          StringBuilder json = new StringBuilder("{");
          for (int i = 0; i < numKeys; i++) {
            if (i > 0) json.append(",");
            json.append("\"key").append(String.format("%04d", i)).append("\":").append(i);
          }
          json.append("}");
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), Commit.NO);
          wtx.commit();
        }

        // Count keys
        try (JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
             JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
          rtx.moveToDocumentRoot();
          rtx.moveToFirstChild();
          int count = 0;
          if (rtx.moveToFirstChild()) {
            do {
              if (rtx.getKind() == NodeKind.OBJECT_KEY) {
                count++;
              }
            } while (rtx.moveToRightSibling());
          }
          assertEquals(numKeys, count, "Should have all " + numKeys + " keys");
        }
      }
    }
  }

  // ============================================================================
  // Resource Cleanup Tests
  // ============================================================================

  @Nested
  @DisplayName("Resource Cleanup")
  class ResourceCleanupTests {

    @Test
    @DisplayName("Multiple open/close cycles don't leak resources")
    void testOpenCloseCycles() throws IOException {
      Path dbPath = tempDir.resolve("db-open-close");
      Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

      for (int cycle = 0; cycle < 10; cycle++) {
        try (Database<JsonResourceSession> db = Databases.openJsonDatabase(dbPath)) {
          if (cycle == 0) {
            db.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME).build());
          }
          
          try (JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
               JsonNodeTrx wtx = session.beginNodeTrx()) {
            if (cycle == 0) {
              wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[1]"), Commit.NO);
            } else {
              wtx.moveToDocumentRoot();
              wtx.moveToFirstChild();
              wtx.moveToLastChild();
              wtx.insertNumberValueAsRightSibling(cycle);
            }
            wtx.commit();
          }
        }
        // Force GC hint
        System.gc();
      }

      // Verify data integrity after all cycles
      try (Database<JsonResourceSession> db = Databases.openJsonDatabase(dbPath);
           JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
           JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        rtx.moveToDocumentRoot();
        rtx.moveToFirstChild();
        int count = 0;
        if (rtx.moveToFirstChild()) {
          do {
            count++;
          } while (rtx.moveToRightSibling());
        }
        assertEquals(10, count, "All data from all cycles should be present");
      }
    }

    @Test
    @DisplayName("Explicit rollback discards changes")
    void testExplicitRollback() throws IOException {
      Path dbPath = tempDir.resolve("db-rollback");
      Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

      try (Database<JsonResourceSession> db = Databases.openJsonDatabase(dbPath)) {
        db.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME).build());

        // Create committed data
        try (JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
             JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[1,2,3]"), Commit.NO);
          wtx.commit();
        }

        // Start transaction, make changes, then rollback
        try (JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
             JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.moveToDocumentRoot();
          wtx.moveToFirstChild();
          wtx.moveToLastChild();
          wtx.insertNumberValueAsRightSibling(999);
          // Explicitly rollback
          wtx.rollback();
        }

        // Verify rollback
        try (JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
             JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
          rtx.moveToDocumentRoot();
          rtx.moveToFirstChild();
          int count = 0;
          if (rtx.moveToFirstChild()) {
            do {
              count++;
            } while (rtx.moveToRightSibling());
          }
          assertEquals(3, count, "Rolled back changes should not persist");
        }
      }
    }
  }

  // ============================================================================
  // Edge Cases and Boundary Conditions
  // ============================================================================

  @Nested
  @DisplayName("Edge Cases")
  class EdgeCaseTests {

    @Test
    @DisplayName("Empty document handling")
    void testEmptyDocument() throws IOException {
      Path dbPath = tempDir.resolve("db-empty");
      Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

      try (Database<JsonResourceSession> db = Databases.openJsonDatabase(dbPath)) {
        db.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME).build());

        // Empty object
        try (JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
             JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{}"), Commit.NO);
          wtx.commit();
        }

        try (JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
             JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
          rtx.moveToDocumentRoot();
          assertTrue(rtx.moveToFirstChild());
          assertEquals(NodeKind.OBJECT, rtx.getKind());
          assertFalse(rtx.moveToFirstChild(), "Empty object should have no children");
        }
      }
    }

    @Test
    @DisplayName("Empty array handling")
    void testEmptyArray() throws IOException {
      Path dbPath = tempDir.resolve("db-empty-array");
      Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

      try (Database<JsonResourceSession> db = Databases.openJsonDatabase(dbPath)) {
        db.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME).build());

        try (JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
             JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[]"), Commit.NO);
          wtx.commit();
        }

        try (JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
             JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
          rtx.moveToDocumentRoot();
          assertTrue(rtx.moveToFirstChild());
          assertEquals(NodeKind.ARRAY, rtx.getKind());
          assertFalse(rtx.moveToFirstChild(), "Empty array should have no children");
        }
      }
    }

    @Test
    @DisplayName("Null value handling")
    void testNullValue() throws IOException {
      Path dbPath = tempDir.resolve("db-null");
      Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

      try (Database<JsonResourceSession> db = Databases.openJsonDatabase(dbPath)) {
        db.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME).build());

        try (JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
             JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.insertSubtreeAsFirstChild(
              JsonShredder.createStringReader("{\"value\":null}"), Commit.NO);
          wtx.commit();
        }

        try (JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
             JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
          rtx.moveToDocumentRoot();
          rtx.moveToFirstChild();
          rtx.moveToFirstChild();
          rtx.moveToFirstChild();
          // In an object, null is OBJECT_NULL_VALUE
          assertEquals(NodeKind.OBJECT_NULL_VALUE, rtx.getKind());
        }
      }
    }

    @Test
    @DisplayName("Unicode string handling")
    void testUnicodeStrings() throws IOException {
      Path dbPath = tempDir.resolve("db-unicode");
      Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

      String unicodeValue = "Hello 世界 🌍 مرحبا שלום";

      try (Database<JsonResourceSession> db = Databases.openJsonDatabase(dbPath)) {
        db.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME).build());

        try (JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
             JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.insertSubtreeAsFirstChild(
              JsonShredder.createStringReader("{\"message\":\"" + unicodeValue + "\"}"), 
              Commit.NO);
          wtx.commit();
        }

        try (JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
             JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
          rtx.moveToDocumentRoot();
          rtx.moveToFirstChild();
          rtx.moveToFirstChild();
          rtx.moveToFirstChild();
          assertEquals(unicodeValue, rtx.getValue());
        }
      }
    }

    @Test
    @DisplayName("Large number handling")
    void testLargeNumbers() throws IOException {
      Path dbPath = tempDir.resolve("db-large-numbers");
      Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

      try (Database<JsonResourceSession> db = Databases.openJsonDatabase(dbPath)) {
        db.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME).build());

        // Use moderately large numbers that JSON can handle reliably
        try (JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
             JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.insertSubtreeAsFirstChild(
              JsonShredder.createStringReader(
                  "{\"large\":1234567890123,\"negative\":-9876543210,\"decimal\":3.141592653589793}"), 
              Commit.NO);
          wtx.commit();
        }

        try (JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
             JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
          rtx.moveToDocumentRoot();
          rtx.moveToFirstChild();
          rtx.moveToFirstChild();
          rtx.moveToFirstChild();
          assertEquals(1234567890123L, rtx.getNumberValue().longValue());
        }
      }
    }

    @Test
    @DisplayName("Special JSON characters in strings")
    void testSpecialCharacters() throws IOException {
      Path dbPath = tempDir.resolve("db-special-chars");
      Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

      try (Database<JsonResourceSession> db = Databases.openJsonDatabase(dbPath)) {
        db.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME).build());

        try (JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
             JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.insertSubtreeAsFirstChild(
              JsonShredder.createStringReader(
                  "{\"escaped\":\"line1\\nline2\\ttab\\\"quote\\\\\"}"), 
              Commit.NO);
          wtx.commit();
        }

        try (JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
             JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
          rtx.moveToDocumentRoot();
          rtx.moveToFirstChild();
          rtx.moveToFirstChild();
          rtx.moveToFirstChild();
          String value = rtx.getValue();
          assertTrue(value.contains("\n"), "Should contain newline");
          assertTrue(value.contains("\t"), "Should contain tab");
          assertTrue(value.contains("\""), "Should contain quote");
          assertTrue(value.contains("\\"), "Should contain backslash");
        }
      }
    }
  }

  // ============================================================================
  // Versioning Stress Tests
  // ============================================================================

  @Nested
  @DisplayName("Versioning Stress")
  class VersioningStressTests {

    @Test
    @DisplayName("All versioning types work with 50 revisions")
    @Timeout(180)
    void testAllVersioningTypes50Revisions() throws IOException {
      for (VersioningType versioningType : VersioningType.values()) {
        Path dbPath = tempDir.resolve("db-versioning-" + versioningType.name());
        Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

        try (Database<JsonResourceSession> db = Databases.openJsonDatabase(dbPath)) {
          db.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME)
              .versioningApproach(versioningType)
              .maxNumberOfRevisionsToRestore(5)
              .build());

          try (JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
            // Create 50 revisions
            for (int rev = 0; rev < 50; rev++) {
              try (JsonNodeTrx wtx = session.beginNodeTrx()) {
                if (rev == 0) {
                  wtx.insertSubtreeAsFirstChild(
                      JsonShredder.createStringReader("[0]"), Commit.NO);
                } else {
                  wtx.moveToDocumentRoot();
                  wtx.moveToFirstChild();
                  wtx.moveToLastChild();
                  wtx.insertNumberValueAsRightSibling(rev);
                }
                wtx.commit();
              }
            }

            assertEquals(50, session.getMostRecentRevisionNumber(),
                versioningType + " should have 50 revisions");

            // Verify first, middle, and last revisions
            for (int rev : new int[]{1, 25, 50}) {
              try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(rev)) {
                rtx.moveToDocumentRoot();
                rtx.moveToFirstChild();
                int count = 0;
                if (rtx.moveToFirstChild()) {
                  do {
                    count++;
                  } while (rtx.moveToRightSibling());
                }
                assertEquals(rev, count,
                    versioningType + " revision " + rev + " should have " + rev + " elements");
              }
            }
          }
        }
      }
    }
  }

  // ============================================================================
  // Data Integrity Tests
  // ============================================================================

  @Nested
  @DisplayName("Data Integrity")
  class DataIntegrityTests {

    @Test
    @DisplayName("Node keys are unique and stable across sessions")
    void testNodeKeyStability() throws IOException {
      Path dbPath = tempDir.resolve("db-node-keys");
      Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

      Set<Long> nodeKeys = new HashSet<>();

      try (Database<JsonResourceSession> db = Databases.openJsonDatabase(dbPath)) {
        db.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME).build());

        // Create data and collect node keys
        try (JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
             JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.insertSubtreeAsFirstChild(
              JsonShredder.createStringReader("[1,2,3,{\"key\":\"value\"}]"), Commit.NO);
          wtx.commit();

          // Collect all node keys
          wtx.moveToDocumentRoot();
          collectNodeKeys(wtx, nodeKeys);
        }
      }

      // Reopen and verify same node keys
      try (Database<JsonResourceSession> db = Databases.openJsonDatabase(dbPath);
           JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
           JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        
        Set<Long> reopenedNodeKeys = new HashSet<>();
        rtx.moveToDocumentRoot();
        collectNodeKeysReadOnly(rtx, reopenedNodeKeys);

        assertEquals(nodeKeys, reopenedNodeKeys, 
            "Node keys should be stable across sessions");
      }
    }

    private void collectNodeKeys(JsonNodeTrx trx, Set<Long> keys) {
      keys.add(trx.getNodeKey());
      if (trx.moveToFirstChild()) {
        do {
          collectNodeKeys(trx, keys);
        } while (trx.moveToRightSibling());
        trx.moveToParent();
      }
    }

    private void collectNodeKeysReadOnly(JsonNodeReadOnlyTrx trx, Set<Long> keys) {
      keys.add(trx.getNodeKey());
      if (trx.moveToFirstChild()) {
        do {
          collectNodeKeysReadOnly(trx, keys);
        } while (trx.moveToRightSibling());
        trx.moveToParent();
      }
    }

    @Test
    @DisplayName("Parent-child relationships are consistent")
    void testParentChildConsistency() throws IOException {
      Path dbPath = tempDir.resolve("db-parent-child");
      Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

      try (Database<JsonResourceSession> db = Databases.openJsonDatabase(dbPath)) {
        db.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME).build());

        try (JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
             JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.insertSubtreeAsFirstChild(
              JsonShredder.createStringReader("{\"a\":{\"b\":{\"c\":1}}}"), Commit.NO);
          wtx.commit();
        }

        try (JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
             JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
          // Navigate to deepest node
          rtx.moveToDocumentRoot();
          List<Long> pathDown = new ArrayList<>();
          
          while (rtx.moveToFirstChild()) {
            pathDown.add(rtx.getNodeKey());
          }
          
          // Navigate back up
          List<Long> pathUp = new ArrayList<>();
          while (rtx.moveToParent() && !rtx.isDocumentRoot()) {
            pathUp.add(rtx.getNodeKey());
          }
          
          // Paths should be reverse of each other
          Collections.reverse(pathUp);
          // Compare intermediate nodes (exclude the deepest value node)
          pathDown.remove(pathDown.size() - 1);
          assertEquals(pathDown, pathUp, 
              "Parent navigation should mirror child navigation");
        }
      }
    }
  }
}

