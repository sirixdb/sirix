/*
 * Copyright (c) 2024, Sirix Contributors
 *
 * All rights reserved.
 */

package io.sirix.index.hot;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonNodeTrx.Commit;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.IndexType;
import io.sirix.index.path.summary.PathSummaryReader;
import io.sirix.node.NodeKind;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Comprehensive tests for HOT indexes with many revisions (10+) and changing index content.
 * 
 * <p>
 * These tests specifically verify that:
 * <ul>
 * <li>Index content changes correctly across many revisions</li>
 * <li>Historical revisions show correct index state at that point in time</li>
 * <li>RevsToRestore threshold works correctly with index changes</li>
 * <li>All versioning strategies correctly handle index evolution</li>
 * </ul>
 */
@DisplayName("HOT Index Many Revisions Tests")
class HOTIndexManyRevisionsTest {

  private static final String RESOURCE_NAME = "test-resource";
  private Path tempDir;

  @BeforeEach
  void setUp() throws IOException {
    try {
      Databases.getGlobalBufferManager().clearAllCaches();
    } catch (Exception e) {
      // Ignore
    }
    tempDir = Files.createTempDirectory("sirix-hot-many-revisions-test");
    System.setProperty("sirix.index.useHOT", "true");
  }

  @AfterEach
  void tearDown() throws IOException {
    System.clearProperty("sirix.index.useHOT");
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

  private int countElements(JsonNodeReadOnlyTrx rtx) {
    int count = 0;
    rtx.moveToDocumentRoot();
    if (rtx.moveToFirstChild()) {
      if (rtx.moveToFirstChild()) {
        do {
          count++;
        } while (rtx.moveToRightSibling());
      }
    }
    return count;
  }

  // ============================================================================
  // Many Revisions with Additive Pattern
  // ============================================================================

  @Nested
  @DisplayName("Many Revisions with Growing Data")
  class ManyRevisionsGrowingData {

    @Test
    @DisplayName("FULL versioning handles 15 revisions correctly")
    void testFullVersioningManyRevisions() throws IOException {
      int totalRevisions = 15;
      Path dbPath = tempDir.resolve("db-full-many-" + totalRevisions);
      DatabaseConfiguration dbConfig = new DatabaseConfiguration(dbPath);
      Databases.createJsonDatabase(dbConfig);

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
        database.createResource(
            ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
          // Create many revisions with growing arrays
          for (int rev = 0; rev < totalRevisions; rev++) {
            try (JsonNodeTrx wtx = session.beginNodeTrx()) {
              if (rev == 0) {
                // Initial array
                StringBuilder json = new StringBuilder("[");
                for (int i = 0; i < 10; i++) {
                  if (i > 0)
                    json.append(",");
                  json.append(i);
                }
                json.append("]");
                wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), Commit.NO);
              } else {
                // Add more elements
                wtx.moveToDocumentRoot();
                wtx.moveToFirstChild();
                wtx.moveToLastChild();
                for (int i = 0; i < 5; i++) {
                  wtx.insertNumberValueAsRightSibling(rev * 100 + i);
                }
              }
              wtx.commit();
            }
          }

          assertEquals(totalRevisions, session.getMostRecentRevisionNumber(),
              "Should have " + totalRevisions + " revisions");

          // Verify each revision has expected element count
          for (int rev = 1; rev <= totalRevisions; rev++) {
            try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(rev)) {
              int expectedCount = 10 + (rev - 1) * 5;
              int actualCount = countElements(rtx);
              assertEquals(expectedCount, actualCount,
                  "Revision " + rev + " should have " + expectedCount + " elements");
            }
          }
        }
      }
    }

    @Test
    @DisplayName("INCREMENTAL versioning with threshold handles 12 revisions")
    void testIncrementalVersioningManyRevisions() throws IOException {
      int totalRevisions = 12;
      Path dbPath = tempDir.resolve("db-incremental-many-" + totalRevisions);
      DatabaseConfiguration dbConfig = new DatabaseConfiguration(dbPath);
      Databases.createJsonDatabase(dbConfig);

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
        database.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME)
                                                     .versioningApproach(VersioningType.INCREMENTAL)
                                                     .maxNumberOfRevisionsToRestore(3)
                                                     .build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
          for (int rev = 0; rev < totalRevisions; rev++) {
            try (JsonNodeTrx wtx = session.beginNodeTrx()) {
              if (rev == 0) {
                StringBuilder json = new StringBuilder("[");
                for (int i = 0; i < 20; i++) {
                  if (i > 0)
                    json.append(",");
                  json.append(i);
                }
                json.append("]");
                wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), Commit.NO);
              } else {
                wtx.moveToDocumentRoot();
                wtx.moveToFirstChild();
                wtx.moveToLastChild();
                for (int i = 0; i < 3; i++) {
                  wtx.insertNumberValueAsRightSibling(rev * 1000 + i);
                }
              }
              wtx.commit();
            }
          }

          assertEquals(totalRevisions, session.getMostRecentRevisionNumber());

          // Verify all revisions including those beyond revsToRestore threshold
          for (int rev = 1; rev <= totalRevisions; rev++) {
            try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(rev)) {
              int expectedCount = 20 + (rev - 1) * 3;
              int actualCount = countElements(rtx);
              assertEquals(expectedCount, actualCount,
                  "INCREMENTAL: Revision " + rev + " (beyond threshold) should have " + expectedCount + " elements");
            }
          }
        }
      }
    }

    @Test
    @DisplayName("DIFFERENTIAL versioning with threshold handles 16 revisions")
    void testDifferentialVersioningManyRevisions() throws IOException {
      int totalRevisions = 16;
      Path dbPath = tempDir.resolve("db-differential-many-" + totalRevisions);
      DatabaseConfiguration dbConfig = new DatabaseConfiguration(dbPath);
      Databases.createJsonDatabase(dbConfig);

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
        database.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME)
                                                     .versioningApproach(VersioningType.DIFFERENTIAL)
                                                     .maxNumberOfRevisionsToRestore(4)
                                                     .build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
          for (int rev = 0; rev < totalRevisions; rev++) {
            try (JsonNodeTrx wtx = session.beginNodeTrx()) {
              if (rev == 0) {
                StringBuilder json = new StringBuilder("[");
                for (int i = 0; i < 15; i++) {
                  if (i > 0)
                    json.append(",");
                  json.append(i);
                }
                json.append("]");
                wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), Commit.NO);
              } else {
                wtx.moveToDocumentRoot();
                wtx.moveToFirstChild();
                wtx.moveToLastChild();
                for (int i = 0; i < 4; i++) {
                  wtx.insertNumberValueAsRightSibling(rev * 100 + i);
                }
              }
              wtx.commit();
            }
          }

          assertEquals(totalRevisions, session.getMostRecentRevisionNumber());

          // Verify revisions at and around snapshot boundaries
          for (int rev = 1; rev <= totalRevisions; rev++) {
            try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(rev)) {
              int expectedCount = 15 + (rev - 1) * 4;
              int actualCount = countElements(rtx);
              assertEquals(expectedCount, actualCount,
                  "DIFFERENTIAL: Revision " + rev + " should have " + expectedCount + " elements");
            }
          }
        }
      }
    }

    @Test
    @DisplayName("SLIDING_SNAPSHOT versioning handles 20 revisions")
    void testSlidingSnapshotVersioningManyRevisions() throws IOException {
      int totalRevisions = 20;
      Path dbPath = tempDir.resolve("db-sliding-many-" + totalRevisions);
      DatabaseConfiguration dbConfig = new DatabaseConfiguration(dbPath);
      Databases.createJsonDatabase(dbConfig);

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
        database.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME)
                                                     .versioningApproach(VersioningType.SLIDING_SNAPSHOT)
                                                     .maxNumberOfRevisionsToRestore(5)
                                                     .build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
          for (int rev = 0; rev < totalRevisions; rev++) {
            try (JsonNodeTrx wtx = session.beginNodeTrx()) {
              if (rev == 0) {
                StringBuilder json = new StringBuilder("[");
                for (int i = 0; i < 12; i++) {
                  if (i > 0)
                    json.append(",");
                  json.append(i);
                }
                json.append("]");
                wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), Commit.NO);
              } else {
                wtx.moveToDocumentRoot();
                wtx.moveToFirstChild();
                wtx.moveToLastChild();
                for (int i = 0; i < 2; i++) {
                  wtx.insertNumberValueAsRightSibling(rev * 10 + i);
                }
              }
              wtx.commit();
            }
          }

          assertEquals(totalRevisions, session.getMostRecentRevisionNumber());

          // Verify all revisions
          for (int rev = 1; rev <= totalRevisions; rev++) {
            try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(rev)) {
              int expectedCount = 12 + (rev - 1) * 2;
              int actualCount = countElements(rtx);
              assertEquals(expectedCount, actualCount,
                  "SLIDING_SNAPSHOT: Revision " + rev + " should have " + expectedCount + " elements");
            }
          }
        }
      }
    }
  }

  // ============================================================================
  // Mixed Add/Remove Patterns
  // ============================================================================

  @Nested
  @DisplayName("Mixed Add/Remove Operations")
  class MixedOperations {

    @Test
    @DisplayName("Varying array sizes across 15 revisions with FULL versioning")
    void testAlternatingAddRemoveFull() throws IOException {
      Path dbPath = tempDir.resolve("db-alternating-full");
      DatabaseConfiguration dbConfig = new DatabaseConfiguration(dbPath);
      Databases.createJsonDatabase(dbConfig);

      final int TOTAL_REVISIONS = 15;
      // Varying sizes pattern: 20, 25, 30, 22, 27, 32, 24, 29, 34...
      int[] expectedSizes = new int[TOTAL_REVISIONS];

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
        database.createResource(
            ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
          for (int rev = 0; rev < TOTAL_REVISIONS; rev++) {
            try (JsonNodeTrx wtx = session.beginNodeTrx()) {
              // Each revision creates a fresh array with varying size
              if (rev > 0) {
                wtx.moveToDocumentRoot();
                if (wtx.moveToFirstChild()) {
                  wtx.remove();
                }
              }
              // Pattern: base size increases, but every 3rd revision is smaller
              int baseSize = 20 + rev * 2;
              int size = (rev % 3 == 0 && rev > 0)
                  ? baseSize - 8
                  : baseSize;
              expectedSizes[rev] = size;

              StringBuilder json = new StringBuilder("[");
              for (int i = 0; i < size; i++) {
                if (i > 0)
                  json.append(",");
                json.append(rev * 100 + i);
              }
              json.append("]");
              wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), Commit.NO);
              wtx.commit();
            }
          }

          // Verify all revisions
          for (int rev = 1; rev <= TOTAL_REVISIONS; rev++) {
            try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(rev)) {
              int actualCount = countElements(rtx);
              assertEquals(expectedSizes[rev - 1], actualCount,
                  "Revision " + rev + " should have " + expectedSizes[rev - 1] + " elements");
            }
          }
        }
      }
    }

    @Test
    @DisplayName("Varying array sizes across 12 revisions with DIFFERENTIAL versioning")
    void testAlternatingAddRemoveDifferential() throws IOException {
      Path dbPath = tempDir.resolve("db-alternating-differential");
      DatabaseConfiguration dbConfig = new DatabaseConfiguration(dbPath);
      Databases.createJsonDatabase(dbConfig);

      final int TOTAL_REVISIONS = 12;
      int[] expectedSizes = new int[TOTAL_REVISIONS];

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
        database.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME)
                                                     .versioningApproach(VersioningType.DIFFERENTIAL)
                                                     .maxNumberOfRevisionsToRestore(3)
                                                     .build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
          for (int rev = 0; rev < TOTAL_REVISIONS; rev++) {
            try (JsonNodeTrx wtx = session.beginNodeTrx()) {
              // Each revision creates a fresh array
              if (rev > 0) {
                wtx.moveToDocumentRoot();
                if (wtx.moveToFirstChild()) {
                  wtx.remove();
                }
              }
              // Alternating pattern: odd=grow, even=shrink
              int size = (rev % 2 == 0)
                  ? 25 + rev
                  : 30 + rev * 2;
              expectedSizes[rev] = size;

              StringBuilder json = new StringBuilder("[");
              for (int i = 0; i < size; i++) {
                if (i > 0)
                  json.append(",");
                json.append(rev * 100 + i);
              }
              json.append("]");
              wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), Commit.NO);
              wtx.commit();
            }
          }

          // Verify all revisions including snapshot boundaries
          for (int rev = 1; rev <= TOTAL_REVISIONS; rev++) {
            try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(rev)) {
              int actualCount = countElements(rtx);
              assertEquals(expectedSizes[rev - 1], actualCount,
                  "DIFFERENTIAL: Revision " + rev + " should have " + expectedSizes[rev - 1] + " elements");
            }
          }
        }
      }
    }
  }

  // ============================================================================
  // Object Key Changes (More Index-Relevant)
  // ============================================================================

  @Nested
  @DisplayName("Object Key Changes Across Revisions")
  class ObjectKeyChanges {

    private int countObjectKeys(JsonNodeReadOnlyTrx rtx) {
      int count = 0;
      rtx.moveToDocumentRoot();
      if (rtx.moveToFirstChild()) {
        if (rtx.getKind() == NodeKind.OBJECT) {
          if (rtx.moveToFirstChild()) {
            do {
              if (rtx.getKind() == NodeKind.OBJECT_KEY) {
                count++;
              }
            } while (rtx.moveToRightSibling());
          }
        }
      }
      return count;
    }

    @Test
    @DisplayName("Growing object with many keys across 10 revisions")
    void testGrowingObjectManyRevisions() throws IOException {
      Path dbPath = tempDir.resolve("db-growing-object");
      DatabaseConfiguration dbConfig = new DatabaseConfiguration(dbPath);
      Databases.createJsonDatabase(dbConfig);

      final int TOTAL_REVISIONS = 10;

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
        database.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME)
                                                     .versioningApproach(VersioningType.DIFFERENTIAL)
                                                     .maxNumberOfRevisionsToRestore(3)
                                                     .build());

        int[] expectedKeys = new int[TOTAL_REVISIONS];

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
          for (int rev = 0; rev < TOTAL_REVISIONS; rev++) {
            try (JsonNodeTrx wtx = session.beginNodeTrx()) {
              if (rev == 0) {
                // Initial object with 10 keys
                StringBuilder json = new StringBuilder("{");
                for (int i = 0; i < 10; i++) {
                  if (i > 0)
                    json.append(",");
                  json.append("\"key").append(String.format("%03d", i)).append("\":").append(i);
                }
                json.append("}");
                wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), Commit.NO);
                expectedKeys[rev] = 10;
              } else {
                // Replace entire object with larger one
                wtx.moveToDocumentRoot();
                if (wtx.moveToFirstChild()) {
                  wtx.remove();
                }
                int keyCount = 10 + rev * 5;
                StringBuilder json = new StringBuilder("{");
                for (int i = 0; i < keyCount; i++) {
                  if (i > 0)
                    json.append(",");
                  json.append("\"key").append(String.format("%03d", i)).append("\":").append(i);
                }
                json.append("}");
                wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), Commit.NO);
                expectedKeys[rev] = keyCount;
              }
              wtx.commit();
            }
          }

          // Verify object key counts across all revisions
          for (int rev = 1; rev <= TOTAL_REVISIONS; rev++) {
            try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(rev)) {
              int actualKeys = countObjectKeys(rtx);
              assertEquals(expectedKeys[rev - 1], actualKeys,
                  "Revision " + rev + " should have " + expectedKeys[rev - 1] + " object keys");
            }
          }
        }
      }
    }

    @Test
    @DisplayName("Object with key replacements across 12 revisions")
    void testObjectKeyReplacements() throws IOException {
      Path dbPath = tempDir.resolve("db-key-replacements");
      DatabaseConfiguration dbConfig = new DatabaseConfiguration(dbPath);
      Databases.createJsonDatabase(dbConfig);

      final int TOTAL_REVISIONS = 12;
      final int KEYS_PER_REVISION = 20;

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
        database.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME)
                                                     .versioningApproach(VersioningType.SLIDING_SNAPSHOT)
                                                     .maxNumberOfRevisionsToRestore(4)
                                                     .build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
          for (int rev = 0; rev < TOTAL_REVISIONS; rev++) {
            try (JsonNodeTrx wtx = session.beginNodeTrx()) {
              // Each revision creates a fresh object with KEYS_PER_REVISION keys
              // Keys are labeled with the revision number for verification
              if (rev > 0) {
                wtx.moveToDocumentRoot();
                if (wtx.moveToFirstChild()) {
                  wtx.remove();
                }
              }
              StringBuilder json = new StringBuilder("{");
              for (int i = 0; i < KEYS_PER_REVISION; i++) {
                if (i > 0)
                  json.append(",");
                json.append("\"key")
                    .append(String.format("%03d", i))
                    .append("\":\"rev")
                    .append(rev)
                    .append("_val")
                    .append(i)
                    .append("\"");
              }
              json.append("}");
              wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), Commit.NO);
              wtx.commit();
            }
          }

          // Verify each revision has correct key count and values are correct
          for (int rev = 1; rev <= TOTAL_REVISIONS; rev++) {
            try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(rev)) {
              int keyCount = countObjectKeys(rtx);
              assertEquals(KEYS_PER_REVISION, keyCount,
                  "SLIDING_SNAPSHOT: Revision " + rev + " should have " + KEYS_PER_REVISION + " keys");

              // Verify first key has correct value for this revision
              rtx.moveToDocumentRoot();
              rtx.moveToFirstChild();
              rtx.moveToFirstChild(); // First key
              rtx.moveToFirstChild(); // Value of first key
              if (rtx.getKind() == NodeKind.STRING_VALUE) {
                String value = rtx.getValue();
                assertTrue(value.startsWith("rev" + (rev - 1)), "Revision " + rev
                    + " first value should start with 'rev" + (rev - 1) + "' but was '" + value + "'");
              }
            }
          }
        }
      }
    }
  }

  // ============================================================================
  // Stress Tests
  // ============================================================================

  @Nested
  @DisplayName("Stress Tests")
  class StressTests {

    @Test
    @DisplayName("30 revisions with FULL versioning")
    void test30RevisionsFullVersioning() throws IOException {
      Path dbPath = tempDir.resolve("db-stress-full-30");
      DatabaseConfiguration dbConfig = new DatabaseConfiguration(dbPath);
      Databases.createJsonDatabase(dbConfig);

      final int TOTAL_REVISIONS = 30;

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
        database.createResource(
            ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
          for (int rev = 0; rev < TOTAL_REVISIONS; rev++) {
            try (JsonNodeTrx wtx = session.beginNodeTrx()) {
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

          assertEquals(TOTAL_REVISIONS, session.getMostRecentRevisionNumber());

          // Verify latest has all elements
          try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
            assertEquals(TOTAL_REVISIONS, countElements(rtx),
                "Latest revision should have " + TOTAL_REVISIONS + " elements");
          }

          // Verify first revision
          try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(1)) {
            assertEquals(1, countElements(rtx), "First revision should have 1 element");
          }

          // Verify middle revision
          int midRev = TOTAL_REVISIONS / 2;
          try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(midRev)) {
            assertEquals(midRev, countElements(rtx),
                "Middle revision " + midRev + " should have " + midRev + " elements");
          }
        }
      }
    }

    @Test
    @DisplayName("20 revisions with INCREMENTAL versioning and small threshold")
    void test20RevisionsIncrementalSmallThreshold() throws IOException {
      Path dbPath = tempDir.resolve("db-stress-incremental-20");
      DatabaseConfiguration dbConfig = new DatabaseConfiguration(dbPath);
      Databases.createJsonDatabase(dbConfig);

      final int TOTAL_REVISIONS = 20;
      final int REVS_TO_RESTORE = 2; // Very small threshold

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
        database.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME)
                                                     .versioningApproach(VersioningType.INCREMENTAL)
                                                     .maxNumberOfRevisionsToRestore(REVS_TO_RESTORE)
                                                     .build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
          for (int rev = 0; rev < TOTAL_REVISIONS; rev++) {
            try (JsonNodeTrx wtx = session.beginNodeTrx()) {
              if (rev == 0) {
                wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[0,1,2]"), Commit.NO);
              } else {
                wtx.moveToDocumentRoot();
                wtx.moveToFirstChild();
                wtx.moveToLastChild();
                wtx.insertNumberValueAsRightSibling(rev + 2);
              }
              wtx.commit();
            }
          }

          assertEquals(TOTAL_REVISIONS, session.getMostRecentRevisionNumber());

          // Verify every 5th revision
          for (int rev = 1; rev <= TOTAL_REVISIONS; rev += 5) {
            try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(rev)) {
              int expected = 3 + (rev - 1);
              assertEquals(expected, countElements(rtx), "Revision " + rev + " should have " + expected + " elements");
            }
          }
        }
      }
    }
  }
}

