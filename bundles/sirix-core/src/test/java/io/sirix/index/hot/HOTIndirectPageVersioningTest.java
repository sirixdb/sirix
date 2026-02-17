/*
 * Copyright (c) 2024, SirixDB
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

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Integration tests for HOTIndirectPage navigation combined with versioning.
 * 
 * <p>
 * These tests verify that:
 * <ul>
 * <li>HOTIndirectPages (created by leaf page splits) work correctly across multiple revisions</li>
 * <li>Historical revisions can be read correctly when HOTIndirectPages are involved</li>
 * <li>Different versioning strategies (FULL, INCREMENTAL, DIFFERENTIAL, SLIDING_SNAPSHOT) work with
 * HOT splits</li>
 * <li>Data integrity is maintained when pages split AND versions accumulate</li>
 * </ul>
 */
@DisplayName("HOT IndirectPage Versioning Integration Tests")
class HOTIndirectPageVersioningTest {

  private static final String RESOURCE_NAME = "test-resource";
  private Path tempDir;

  @BeforeEach
  void setUp() throws IOException {
    // Clear global caches to ensure clean state
    try {
      Databases.getGlobalBufferManager().clearAllCaches();
    } catch (Exception e) {
      // Ignore - buffer manager might not exist yet
    }

    tempDir = Files.createTempDirectory("sirix-hot-indirect-versioning-test");
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

  // ============================================================================
  // Helper methods
  // ============================================================================

  /**
   * Creates a JSON object with the specified number of keys. Keys are formatted as "keyNNN" for
   * consistent ordering.
   */
  private String createLargeJsonObject(int keyCount) {
    StringBuilder json = new StringBuilder("{");
    for (int i = 0; i < keyCount; i++) {
      if (i > 0)
        json.append(",");
      json.append(String.format("\"key%03d\": \"value%d\"", i, i));
    }
    json.append("}");
    return json.toString();
  }

  /**
   * Creates a JSON array with the specified number of elements.
   */
  private String createLargeJsonArray(int elementCount) {
    StringBuilder json = new StringBuilder("[");
    for (int i = 0; i < elementCount; i++) {
      if (i > 0)
        json.append(",");
      json.append(i);
    }
    json.append("]");
    return json.toString();
  }

  /**
   * Counts object record keys (string keys in JSON objects) in the tree.
   */
  private int countObjectRecordKeys(JsonNodeReadOnlyTrx rtx) {
    int count = 0;
    rtx.moveToDocumentRoot();
    if (rtx.moveToFirstChild()) {
      count += countKeysRecursive(rtx);
    }
    return count;
  }

  private int countKeysRecursive(JsonNodeReadOnlyTrx rtx) {
    int count = 0;
    do {
      if (rtx.getKind() == NodeKind.OBJECT_KEY) {
        count++;
      }
      if (rtx.hasFirstChild()) {
        rtx.moveToFirstChild();
        count += countKeysRecursive(rtx);
        rtx.moveToParent();
      }
    } while (rtx.moveToRightSibling());
    return count;
  }

  /**
   * Counts array elements in the tree.
   */
  private int countArrayElements(JsonNodeReadOnlyTrx rtx) {
    int count = 0;
    rtx.moveToDocumentRoot();
    if (rtx.moveToFirstChild()) { // Array
      if (rtx.moveToFirstChild()) { // First element
        do {
          if (rtx.getKind() == NodeKind.NUMBER_VALUE) {
            count++;
          }
        } while (rtx.moveToRightSibling());
      }
    }
    return count;
  }

  // ============================================================================
  // Tests for HOTIndirectPage with different versioning strategies
  // ============================================================================

  @Nested
  @DisplayName("FULL versioning with HOTIndirectPages")
  class FullVersioningWithIndirectPages {

    @Test
    @DisplayName("Large dataset triggers splits, multiple revisions are readable")
    void testLargeDatasetWithMultipleRevisions() throws IOException {
      Path dbPath = tempDir.resolve("db-full-splits");
      DatabaseConfiguration dbConfig = new DatabaseConfiguration(dbPath);
      Databases.createJsonDatabase(dbConfig);

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
        database.createResource(
            ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
          // Revision 1: Large object with 200 keys (likely triggers HOT page splits)
          try (JsonNodeTrx wtx = session.beginNodeTrx()) {
            wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(createLargeJsonObject(200)), Commit.NO);
            wtx.commit();
          }

          // Revision 2: Modify by removing and inserting different size
          try (JsonNodeTrx wtx = session.beginNodeTrx()) {
            wtx.moveToDocumentRoot();
            if (wtx.moveToFirstChild()) {
              wtx.remove();
            }
            wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(createLargeJsonObject(300)), Commit.NO);
            wtx.commit();
          }

          // Revision 3: Even larger dataset
          try (JsonNodeTrx wtx = session.beginNodeTrx()) {
            wtx.moveToDocumentRoot();
            if (wtx.moveToFirstChild()) {
              wtx.remove();
            }
            wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(createLargeJsonObject(400)), Commit.NO);
            wtx.commit();
          }

          // Verify revision counts
          assertEquals(3, session.getMostRecentRevisionNumber(), "Should have 3 revisions");

          // Verify revision 1 has 200 keys
          try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(1)) {
            int keyCount = countObjectRecordKeys(rtx);
            assertEquals(200, keyCount, "Revision 1 should have 200 keys");
          }

          // Verify revision 2 has 300 keys
          try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(2)) {
            int keyCount = countObjectRecordKeys(rtx);
            assertEquals(300, keyCount, "Revision 2 should have 300 keys");
          }

          // Verify revision 3 has 400 keys
          try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(3)) {
            int keyCount = countObjectRecordKeys(rtx);
            assertEquals(400, keyCount, "Revision 3 should have 400 keys");
          }
        }
      }

      // Reopen and verify data persists
      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
            JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
          int keyCount = countObjectRecordKeys(rtx);
          assertEquals(400, keyCount, "Should have 400 keys after reopening");
        }
      }
    }
  }

  @Nested
  @DisplayName("INCREMENTAL versioning with HOTIndirectPages")
  class IncrementalVersioningWithIndirectPages {

    @Test
    @DisplayName("Incremental versioning correctly reconstructs pages after splits")
    void testIncrementalVersioningWithSplits() throws IOException {
      Path dbPath = tempDir.resolve("db-incremental-splits");
      DatabaseConfiguration dbConfig = new DatabaseConfiguration(dbPath);
      Databases.createJsonDatabase(dbConfig);

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
        database.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME)
                                                     .versioningApproach(VersioningType.INCREMENTAL)
                                                     .maxNumberOfRevisionsToRestore(3) // Trigger fragment combining
                                                     .build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
          // Create 6 revisions with increasing data sizes - exceeds revsToRestore=3 to test combining
          int[] sizes = {100, 150, 200, 250, 300, 350};

          for (int rev = 0; rev < sizes.length; rev++) {
            try (JsonNodeTrx wtx = session.beginNodeTrx()) {
              if (rev > 0) {
                wtx.moveToDocumentRoot();
                if (wtx.moveToFirstChild()) {
                  wtx.remove();
                }
              }
              wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(createLargeJsonObject(sizes[rev])),
                  Commit.NO);
              wtx.commit();
            }
          }

          assertEquals(6, session.getMostRecentRevisionNumber(), "Should have 6 revisions");

          // Verify each historical revision has correct data
          for (int rev = 1; rev <= sizes.length; rev++) {
            try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(rev)) {
              int keyCount = countObjectRecordKeys(rtx);
              assertEquals(sizes[rev - 1], keyCount, "Revision " + rev + " should have " + sizes[rev - 1] + " keys");
            }
          }
        }
      }
    }

    @Test
    @DisplayName("Incremental versioning with modifications to same keys")
    void testIncrementalVersioningModifySameKeys() throws IOException {
      Path dbPath = tempDir.resolve("db-incremental-modify");
      DatabaseConfiguration dbConfig = new DatabaseConfiguration(dbPath);
      Databases.createJsonDatabase(dbConfig);

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
        database.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME)
                                                     .versioningApproach(VersioningType.INCREMENTAL)
                                                     .maxNumberOfRevisionsToRestore(5)
                                                     .build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
          // Revision 1: Create initial large object
          try (JsonNodeTrx wtx = session.beginNodeTrx()) {
            wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(createLargeJsonObject(150)), Commit.NO);
            wtx.commit();
          }

          // Revisions 2-6: Incrementally add more keys (building on existing structure)
          for (int rev = 2; rev <= 6; rev++) {
            try (JsonNodeTrx wtx = session.beginNodeTrx()) {
              wtx.moveToDocumentRoot();
              if (wtx.moveToFirstChild()) {
                wtx.remove();
              }
              // Each revision adds 50 more keys
              int keyCount = 150 + (rev - 1) * 50;
              wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(createLargeJsonObject(keyCount)),
                  Commit.NO);
              wtx.commit();
            }
          }

          // Verify all revisions
          for (int rev = 1; rev <= 6; rev++) {
            try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(rev)) {
              int expectedKeys = 150 + (rev - 1) * 50;
              int actualKeys = countObjectRecordKeys(rtx);
              assertEquals(expectedKeys, actualKeys, "Revision " + rev + " should have " + expectedKeys + " keys");
            }
          }
        }
      }
    }
  }

  @Nested
  @DisplayName("DIFFERENTIAL versioning with HOTIndirectPages")
  class DifferentialVersioningWithIndirectPages {

    @Test
    @DisplayName("Differential versioning handles page splits correctly")
    void testDifferentialVersioningWithSplits() throws IOException {
      Path dbPath = tempDir.resolve("db-differential-splits");
      DatabaseConfiguration dbConfig = new DatabaseConfiguration(dbPath);
      Databases.createJsonDatabase(dbConfig);

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
        database.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME)
                                                     .versioningApproach(VersioningType.DIFFERENTIAL)
                                                     .maxNumberOfRevisionsToRestore(3)
                                                     .build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
          // Create 4 revisions with varying sizes to trigger splits
          int[] sizes = {180, 220, 260, 320};

          for (int rev = 0; rev < sizes.length; rev++) {
            try (JsonNodeTrx wtx = session.beginNodeTrx()) {
              if (rev > 0) {
                wtx.moveToDocumentRoot();
                if (wtx.moveToFirstChild()) {
                  wtx.remove();
                }
              }
              wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(createLargeJsonObject(sizes[rev])),
                  Commit.NO);
              wtx.commit();
            }
          }

          // Verify all historical revisions are accessible
          for (int rev = 1; rev <= sizes.length; rev++) {
            try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(rev)) {
              int keyCount = countObjectRecordKeys(rtx);
              assertEquals(sizes[rev - 1], keyCount,
                  "Revision " + rev + " should have " + sizes[rev - 1] + " keys with DIFFERENTIAL versioning");
            }
          }
        }
      }
    }
  }

  @Nested
  @DisplayName("SLIDING_SNAPSHOT versioning with HOTIndirectPages")
  class SlidingSnapshotVersioningWithIndirectPages {

    @Test
    @DisplayName("Sliding snapshot handles page splits with window management")
    void testSlidingSnapshotWithSplits() throws IOException {
      Path dbPath = tempDir.resolve("db-sliding-splits");
      DatabaseConfiguration dbConfig = new DatabaseConfiguration(dbPath);
      Databases.createJsonDatabase(dbConfig);

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
        database.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME)
                                                     .versioningApproach(VersioningType.SLIDING_SNAPSHOT)
                                                     .maxNumberOfRevisionsToRestore(4)
                                                     .build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
          // Create 6 revisions with varying sizes
          int[] sizes = {120, 180, 240, 300, 350, 400};

          for (int rev = 0; rev < sizes.length; rev++) {
            try (JsonNodeTrx wtx = session.beginNodeTrx()) {
              if (rev > 0) {
                wtx.moveToDocumentRoot();
                if (wtx.moveToFirstChild()) {
                  wtx.remove();
                }
              }
              wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(createLargeJsonObject(sizes[rev])),
                  Commit.NO);
              wtx.commit();
            }
          }

          // Verify latest revision
          try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
            int keyCount = countObjectRecordKeys(rtx);
            assertEquals(400, keyCount, "Latest revision should have 400 keys");
          }

          // Verify we can read all historical revisions (even with sliding window)
          for (int rev = 1; rev <= sizes.length; rev++) {
            try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(rev)) {
              int keyCount = countObjectRecordKeys(rtx);
              assertEquals(sizes[rev - 1], keyCount, "Revision " + rev + " should have " + sizes[rev - 1] + " keys");
            }
          }
        }
      }
    }
  }

  // ============================================================================
  // Cross-versioning comparison tests
  // ============================================================================

  @Nested
  @DisplayName("Cross-versioning type comparison")
  class CrossVersioningComparison {

    @Test
    @DisplayName("All versioning types produce consistent results with large datasets")
    void testAllVersioningTypesConsistent() throws IOException {
      VersioningType[] versioningTypes = {VersioningType.FULL, VersioningType.INCREMENTAL, VersioningType.DIFFERENTIAL,
          VersioningType.SLIDING_SNAPSHOT};

      int[] sizes = {100, 200, 300};

      for (VersioningType versioningType : versioningTypes) {
        Path dbPath = tempDir.resolve("db-compare-" + versioningType.name().toLowerCase());
        DatabaseConfiguration dbConfig = new DatabaseConfiguration(dbPath);
        Databases.createJsonDatabase(dbConfig);

        try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
          database.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME)
                                                       .versioningApproach(versioningType)
                                                       .maxNumberOfRevisionsToRestore(3)
                                                       .build());

          try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
            // Create 3 revisions
            for (int rev = 0; rev < sizes.length; rev++) {
              try (JsonNodeTrx wtx = session.beginNodeTrx()) {
                if (rev > 0) {
                  wtx.moveToDocumentRoot();
                  if (wtx.moveToFirstChild()) {
                    wtx.remove();
                  }
                }
                wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(createLargeJsonObject(sizes[rev])),
                    Commit.NO);
                wtx.commit();
              }
            }

            // Verify each revision
            for (int rev = 1; rev <= sizes.length; rev++) {
              try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(rev)) {
                int keyCount = countObjectRecordKeys(rtx);
                assertEquals(sizes[rev - 1], keyCount,
                    versioningType + " Revision " + rev + " should have " + sizes[rev - 1] + " keys");
              }
            }
          }
        }
      }
    }
  }

  // ============================================================================
  // Edge cases and stress tests
  // ============================================================================

  @Nested
  @DisplayName("Edge cases with splits and versioning")
  class EdgeCasesWithSplitsAndVersioning {

    @Test
    @DisplayName("Empty then large dataset across revisions")
    void testEmptyToLargeDataset() throws IOException {
      Path dbPath = tempDir.resolve("db-empty-to-large");
      DatabaseConfiguration dbConfig = new DatabaseConfiguration(dbPath);
      Databases.createJsonDatabase(dbConfig);

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
        database.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME)
                                                     .versioningApproach(VersioningType.INCREMENTAL)
                                                     .maxNumberOfRevisionsToRestore(5)
                                                     .build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
          // Revision 1: Empty object
          try (JsonNodeTrx wtx = session.beginNodeTrx()) {
            wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{}"), Commit.NO);
            wtx.commit();
          }

          // Revision 2: Small object
          try (JsonNodeTrx wtx = session.beginNodeTrx()) {
            wtx.moveToDocumentRoot();
            if (wtx.moveToFirstChild()) {
              wtx.remove();
            }
            wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(createLargeJsonObject(10)), Commit.NO);
            wtx.commit();
          }

          // Revision 3: Large object (triggers splits)
          try (JsonNodeTrx wtx = session.beginNodeTrx()) {
            wtx.moveToDocumentRoot();
            if (wtx.moveToFirstChild()) {
              wtx.remove();
            }
            wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(createLargeJsonObject(300)), Commit.NO);
            wtx.commit();
          }

          // Verify revision 1 (empty object has 0 keys)
          try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(1)) {
            int keyCount = countObjectRecordKeys(rtx);
            assertEquals(0, keyCount, "Revision 1 should have 0 keys (empty object)");
          }

          // Verify revision 2 (10 keys)
          try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(2)) {
            int keyCount = countObjectRecordKeys(rtx);
            assertEquals(10, keyCount, "Revision 2 should have 10 keys");
          }

          // Verify revision 3 (300 keys)
          try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(3)) {
            int keyCount = countObjectRecordKeys(rtx);
            assertEquals(300, keyCount, "Revision 3 should have 300 keys");
          }
        }
      }
    }

    @Test
    @DisplayName("Large to empty dataset across revisions")
    void testLargeToEmptyDataset() throws IOException {
      Path dbPath = tempDir.resolve("db-large-to-empty");
      DatabaseConfiguration dbConfig = new DatabaseConfiguration(dbPath);
      Databases.createJsonDatabase(dbConfig);

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
        database.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME)
                                                     .versioningApproach(VersioningType.DIFFERENTIAL)
                                                     .maxNumberOfRevisionsToRestore(3)
                                                     .build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
          // Revision 1: Large object
          try (JsonNodeTrx wtx = session.beginNodeTrx()) {
            wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(createLargeJsonObject(250)), Commit.NO);
            wtx.commit();
          }

          // Revision 2: Medium object
          try (JsonNodeTrx wtx = session.beginNodeTrx()) {
            wtx.moveToDocumentRoot();
            if (wtx.moveToFirstChild()) {
              wtx.remove();
            }
            wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(createLargeJsonObject(50)), Commit.NO);
            wtx.commit();
          }

          // Revision 3: Empty object
          try (JsonNodeTrx wtx = session.beginNodeTrx()) {
            wtx.moveToDocumentRoot();
            if (wtx.moveToFirstChild()) {
              wtx.remove();
            }
            wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{}"), Commit.NO);
            wtx.commit();
          }

          // Verify all revisions
          try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(1)) {
            assertEquals(250, countObjectRecordKeys(rtx), "Revision 1 should have 250 keys");
          }
          try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(2)) {
            assertEquals(50, countObjectRecordKeys(rtx), "Revision 2 should have 50 keys");
          }
          try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(3)) {
            assertEquals(0, countObjectRecordKeys(rtx), "Revision 3 should have 0 keys");
          }
        }
      }
    }

    @Test
    @DisplayName("Array data with versioning and potential splits")
    void testArrayDataWithVersioning() throws IOException {
      Path dbPath = tempDir.resolve("db-array-versioning");
      DatabaseConfiguration dbConfig = new DatabaseConfiguration(dbPath);
      Databases.createJsonDatabase(dbConfig);

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
        database.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME)
                                                     .versioningApproach(VersioningType.INCREMENTAL)
                                                     .maxNumberOfRevisionsToRestore(4)
                                                     .build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
          int[] arraySizes = {50, 100, 200, 350};

          for (int rev = 0; rev < arraySizes.length; rev++) {
            try (JsonNodeTrx wtx = session.beginNodeTrx()) {
              if (rev > 0) {
                wtx.moveToDocumentRoot();
                if (wtx.moveToFirstChild()) {
                  wtx.remove();
                }
              }
              wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(createLargeJsonArray(arraySizes[rev])),
                  Commit.NO);
              wtx.commit();
            }
          }

          // Verify array element counts for each revision
          for (int rev = 1; rev <= arraySizes.length; rev++) {
            try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(rev)) {
              int elementCount = countArrayElements(rtx);
              assertEquals(arraySizes[rev - 1], elementCount,
                  "Revision " + rev + " should have " + arraySizes[rev - 1] + " array elements");
            }
          }
        }
      }
    }

    @Test
    @DisplayName("Many small revisions with gradual growth")
    void testManySmallRevisions() throws IOException {
      Path dbPath = tempDir.resolve("db-many-revisions");
      DatabaseConfiguration dbConfig = new DatabaseConfiguration(dbPath);
      Databases.createJsonDatabase(dbConfig);

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
        database.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME)
                                                     .versioningApproach(VersioningType.SLIDING_SNAPSHOT)
                                                     .maxNumberOfRevisionsToRestore(4) // Trigger snapshot behavior
                                                     .build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
          // Now we can use more revisions since RECORD_TO_REVISIONS bug is fixed
          int numRevisions = 12;

          for (int rev = 0; rev < numRevisions; rev++) {
            try (JsonNodeTrx wtx = session.beginNodeTrx()) {
              if (rev > 0) {
                wtx.moveToDocumentRoot();
                if (wtx.moveToFirstChild()) {
                  wtx.remove();
                }
              }
              // Each revision adds 15 more keys
              int keyCount = 50 + rev * 15;
              wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(createLargeJsonObject(keyCount)),
                  Commit.NO);
              wtx.commit();
            }
          }

          assertEquals(numRevisions, session.getMostRecentRevisionNumber(),
              "Should have " + numRevisions + " revisions");

          // Verify each revision
          for (int rev = 1; rev <= numRevisions; rev++) {
            try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(rev)) {
              int expectedKeys = 50 + (rev - 1) * 15;
              int actualKeys = countObjectRecordKeys(rtx);
              assertEquals(expectedKeys, actualKeys, "Revision " + rev + " should have " + expectedKeys + " keys");
            }
          }
        }
      }
    }
  }

  // ============================================================================
  // RevsToRestore threshold tests
  // ============================================================================

  @Nested
  @DisplayName("RevsToRestore threshold behavior")
  class RevsToRestoreThresholdTests {

    @Test
    @DisplayName("INCREMENTAL: revisions beyond threshold are reconstructed correctly")
    void testIncrementalBeyondThreshold() throws IOException {
      Path dbPath = tempDir.resolve("db-incremental-threshold");
      DatabaseConfiguration dbConfig = new DatabaseConfiguration(dbPath);
      Databases.createJsonDatabase(dbConfig);

      final int REVS_TO_RESTORE = 3;
      final int TOTAL_REVISIONS = 8; // Well beyond threshold

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
        database.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME)
                                                     .versioningApproach(VersioningType.INCREMENTAL)
                                                     .maxNumberOfRevisionsToRestore(REVS_TO_RESTORE)
                                                     .build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
          // Create many revisions - use additive pattern to avoid RECORD_TO_REVISIONS limits
          // Each revision inserts a new nested object instead of removing
          for (int rev = 0; rev < TOTAL_REVISIONS; rev++) {
            try (JsonNodeTrx wtx = session.beginNodeTrx()) {
              if (rev == 0) {
                // First revision: create initial structure
                wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(createLargeJsonObject(50)), Commit.NO);
              } else {
                // Subsequent revisions: remove and recreate with more data
                wtx.moveToDocumentRoot();
                if (wtx.moveToFirstChild()) {
                  wtx.remove();
                }
                wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(createLargeJsonObject(50 + rev * 5)),
                    Commit.NO);
              }
              wtx.commit();
            }
          }

          assertEquals(TOTAL_REVISIONS, session.getMostRecentRevisionNumber(),
              "Should have " + TOTAL_REVISIONS + " revisions");

          // Verify ALL revisions are readable - even those beyond the threshold
          // This tests that INCREMENTAL correctly reconstructs pages
          for (int rev = 1; rev <= TOTAL_REVISIONS; rev++) {
            try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(rev)) {
              int expectedKeys = 50 + (rev - 1) * 5;
              int actualKeys = countObjectRecordKeys(rtx);
              assertEquals(expectedKeys, actualKeys, "INCREMENTAL Revision " + rev + " (revsToRestore="
                  + REVS_TO_RESTORE + ") should have " + expectedKeys + " keys");
            }
          }
        }
      }
    }

    @Test
    @DisplayName("DIFFERENTIAL: revisions at snapshot boundaries are correct")
    void testDifferentialAtSnapshotBoundaries() throws IOException {
      Path dbPath = tempDir.resolve("db-differential-threshold");
      DatabaseConfiguration dbConfig = new DatabaseConfiguration(dbPath);
      Databases.createJsonDatabase(dbConfig);

      final int REVS_TO_RESTORE = 3;
      final int TOTAL_REVISIONS = 9; // 3x the threshold to hit multiple snapshot points

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
        database.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME)
                                                     .versioningApproach(VersioningType.DIFFERENTIAL)
                                                     .maxNumberOfRevisionsToRestore(REVS_TO_RESTORE)
                                                     .build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
          for (int rev = 0; rev < TOTAL_REVISIONS; rev++) {
            try (JsonNodeTrx wtx = session.beginNodeTrx()) {
              if (rev > 0) {
                wtx.moveToDocumentRoot();
                if (wtx.moveToFirstChild()) {
                  wtx.remove();
                }
              }
              // Different key counts make each revision distinguishable
              int keyCount = 40 + rev * 4;
              wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(createLargeJsonObject(keyCount)),
                  Commit.NO);
              wtx.commit();
            }
          }

          assertEquals(TOTAL_REVISIONS, session.getMostRecentRevisionNumber(),
              "Should have " + TOTAL_REVISIONS + " revisions");

          // Verify all revisions - especially at snapshot boundaries (rev 3, 6, 9)
          for (int rev = 1; rev <= TOTAL_REVISIONS; rev++) {
            try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(rev)) {
              int expectedKeys = 40 + (rev - 1) * 4;
              int actualKeys = countObjectRecordKeys(rtx);
              boolean isSnapshotBoundary = (rev % REVS_TO_RESTORE == 0);
              assertEquals(expectedKeys, actualKeys, "DIFFERENTIAL Revision " + rev + (isSnapshotBoundary
                  ? " (SNAPSHOT)"
                  : " (delta)") + " should have " + expectedKeys + " keys");
            }
          }
        }
      }
    }

    @Test
    @DisplayName("SLIDING_SNAPSHOT: window compaction doesn't lose data")
    void testSlidingSnapshotWindowCompaction() throws IOException {
      Path dbPath = tempDir.resolve("db-sliding-threshold");
      DatabaseConfiguration dbConfig = new DatabaseConfiguration(dbPath);
      Databases.createJsonDatabase(dbConfig);

      final int REVS_TO_RESTORE = 4;
      final int TOTAL_REVISIONS = 12; // 3x the window size

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
        database.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME)
                                                     .versioningApproach(VersioningType.SLIDING_SNAPSHOT)
                                                     .maxNumberOfRevisionsToRestore(REVS_TO_RESTORE)
                                                     .build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
          for (int rev = 0; rev < TOTAL_REVISIONS; rev++) {
            try (JsonNodeTrx wtx = session.beginNodeTrx()) {
              if (rev > 0) {
                wtx.moveToDocumentRoot();
                if (wtx.moveToFirstChild()) {
                  wtx.remove();
                }
              }
              // Use prime-based key counts for easy identification
              int keyCount = 31 + rev * 3;
              wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(createLargeJsonObject(keyCount)),
                  Commit.NO);
              wtx.commit();
            }
          }

          assertEquals(TOTAL_REVISIONS, session.getMostRecentRevisionNumber(),
              "Should have " + TOTAL_REVISIONS + " revisions");

          // Verify all historical revisions remain accessible
          for (int rev = 1; rev <= TOTAL_REVISIONS; rev++) {
            try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(rev)) {
              int expectedKeys = 31 + (rev - 1) * 3;
              int actualKeys = countObjectRecordKeys(rtx);
              assertEquals(expectedKeys, actualKeys, "SLIDING_SNAPSHOT Revision " + rev + " (window=" + REVS_TO_RESTORE
                  + ") should have " + expectedKeys + " keys");
            }
          }
        }
      }
    }

    @Test
    @DisplayName("Small threshold (2) creates frequent snapshots")
    void testSmallThreshold() throws IOException {
      Path dbPath = tempDir.resolve("db-threshold-small");
      DatabaseConfiguration dbConfig = new DatabaseConfiguration(dbPath);
      Databases.createJsonDatabase(dbConfig);

      // Note: threshold=1 is not supported for INCREMENTAL/DIFFERENTIAL
      // Use threshold=2 which creates snapshots every 2 revisions
      final int REVS_TO_RESTORE = 2;
      final int TOTAL_REVISIONS = 6;

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
        database.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME)
                                                     .versioningApproach(VersioningType.INCREMENTAL)
                                                     .maxNumberOfRevisionsToRestore(REVS_TO_RESTORE)
                                                     .build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
          for (int rev = 0; rev < TOTAL_REVISIONS; rev++) {
            try (JsonNodeTrx wtx = session.beginNodeTrx()) {
              if (rev > 0) {
                wtx.moveToDocumentRoot();
                if (wtx.moveToFirstChild()) {
                  wtx.remove();
                }
              }
              int keyCount = 25 + rev * 5;
              wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(createLargeJsonObject(keyCount)),
                  Commit.NO);
              wtx.commit();
            }
          }

          // Verify all revisions with threshold=2 (snapshots at rev 2, 4, 6)
          for (int rev = 1; rev <= TOTAL_REVISIONS; rev++) {
            try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(rev)) {
              int expectedKeys = 25 + (rev - 1) * 5;
              int actualKeys = countObjectRecordKeys(rtx);
              assertEquals(expectedKeys, actualKeys,
                  "With threshold=2, Revision " + rev + " should have " + expectedKeys + " keys");
            }
          }
        }
      }
    }

    @Test
    @DisplayName("Large threshold behaves like pure delta storage")
    void testLargeThreshold() throws IOException {
      Path dbPath = tempDir.resolve("db-threshold-large");
      DatabaseConfiguration dbConfig = new DatabaseConfiguration(dbPath);
      Databases.createJsonDatabase(dbConfig);

      final int REVS_TO_RESTORE = 100; // Very large - effectively pure delta
      final int TOTAL_REVISIONS = 5;

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
        database.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME)
                                                     .versioningApproach(VersioningType.DIFFERENTIAL)
                                                     .maxNumberOfRevisionsToRestore(REVS_TO_RESTORE)
                                                     .build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
          for (int rev = 0; rev < TOTAL_REVISIONS; rev++) {
            try (JsonNodeTrx wtx = session.beginNodeTrx()) {
              if (rev > 0) {
                wtx.moveToDocumentRoot();
                if (wtx.moveToFirstChild()) {
                  wtx.remove();
                }
              }
              int keyCount = 35 + rev * 7;
              wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(createLargeJsonObject(keyCount)),
                  Commit.NO);
              wtx.commit();
            }
          }

          // Verify all revisions with large threshold (all deltas from rev 0)
          for (int rev = 1; rev <= TOTAL_REVISIONS; rev++) {
            try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(rev)) {
              int expectedKeys = 35 + (rev - 1) * 7;
              int actualKeys = countObjectRecordKeys(rtx);
              assertEquals(expectedKeys, actualKeys,
                  "With large threshold, Revision " + rev + " should have " + expectedKeys + " keys");
            }
          }
        }
      }
    }

    @Test
    @DisplayName("Mixed operations across threshold boundaries")
    void testMixedOperationsAcrossThreshold() throws IOException {
      Path dbPath = tempDir.resolve("db-mixed-threshold");
      DatabaseConfiguration dbConfig = new DatabaseConfiguration(dbPath);
      Databases.createJsonDatabase(dbConfig);

      final int REVS_TO_RESTORE = 2;
      final int TOTAL_REVISIONS = 7;

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
        database.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME)
                                                     .versioningApproach(VersioningType.INCREMENTAL)
                                                     .maxNumberOfRevisionsToRestore(REVS_TO_RESTORE)
                                                     .build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
          // Pattern: grow, shrink, grow, shrink... across threshold boundaries
          int[] keyCounts = {30, 50, 25, 60, 35, 70, 40};

          for (int rev = 0; rev < TOTAL_REVISIONS; rev++) {
            try (JsonNodeTrx wtx = session.beginNodeTrx()) {
              if (rev > 0) {
                wtx.moveToDocumentRoot();
                if (wtx.moveToFirstChild()) {
                  wtx.remove();
                }
              }
              wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(createLargeJsonObject(keyCounts[rev])),
                  Commit.NO);
              wtx.commit();
            }
          }

          // Verify all revisions with mixed grow/shrink pattern
          for (int rev = 1; rev <= TOTAL_REVISIONS; rev++) {
            try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(rev)) {
              int expectedKeys = keyCounts[rev - 1];
              int actualKeys = countObjectRecordKeys(rtx);
              assertEquals(expectedKeys, actualKeys,
                  "Mixed pattern Revision " + rev + " should have " + expectedKeys + " keys");
            }
          }
        }
      }
    }
  }

  // ============================================================================
  // Persistence tests
  // ============================================================================

  @Nested
  @DisplayName("Persistence with splits and versioning")
  class PersistenceWithSplitsAndVersioning {

    @Test
    @DisplayName("Data persists correctly after close/reopen with splits")
    void testPersistenceAfterReopenWithSplits() throws IOException {
      Path dbPath = tempDir.resolve("db-persistence-splits");
      DatabaseConfiguration dbConfig = new DatabaseConfiguration(dbPath);
      Databases.createJsonDatabase(dbConfig);

      int[] sizes = {150, 250, 350};

      // Create database with multiple revisions
      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
        database.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME)
                                                     .versioningApproach(VersioningType.INCREMENTAL)
                                                     .maxNumberOfRevisionsToRestore(4)
                                                     .build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
          for (int rev = 0; rev < sizes.length; rev++) {
            try (JsonNodeTrx wtx = session.beginNodeTrx()) {
              if (rev > 0) {
                wtx.moveToDocumentRoot();
                if (wtx.moveToFirstChild()) {
                  wtx.remove();
                }
              }
              wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(createLargeJsonObject(sizes[rev])),
                  Commit.NO);
              wtx.commit();
            }
          }
        }
      }

      // Clear caches to simulate fresh start
      try {
        Databases.getGlobalBufferManager().clearAllCaches();
      } catch (Exception e) {
        // Ignore
      }

      // Reopen and verify all revisions
      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
          assertEquals(3, session.getMostRecentRevisionNumber(), "Should have 3 revisions after reopen");

          for (int rev = 1; rev <= sizes.length; rev++) {
            try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(rev)) {
              int keyCount = countObjectRecordKeys(rtx);
              assertEquals(sizes[rev - 1], keyCount,
                  "Revision " + rev + " should have " + sizes[rev - 1] + " keys after reopen");
            }
          }
        }
      }
    }
  }
}

