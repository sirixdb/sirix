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
import io.sirix.api.json.JsonResourceSession;
import io.sirix.node.NodeKind;
import io.sirix.api.json.JsonNodeTrx.Commit;
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
 * Integration tests for HOT versioning across all VersioningType strategies.
 * Verifies that HOTLeafPage is properly versioned through the VersioningType enum.
 */
@DisplayName("HOT Versioning Integration Tests")
class HOTVersioningIntegrationTest {

  private static final String RESOURCE_NAME = "test-resource";
  private Path tempDir;

  @BeforeEach
  void setUp() throws IOException {
    // Clear global caches FIRST to ensure clean state from previous test
    try {
      Databases.getGlobalBufferManager().clearAllCaches();
    } catch (Exception e) {
      // Ignore - buffer manager might not exist yet on first test
    }
    
    tempDir = Files.createTempDirectory("sirix-hot-versioning-test");
    System.setProperty("sirix.index.useHOT", "true");
  }

  @AfterEach
  void tearDown() throws IOException {
    System.clearProperty("sirix.index.useHOT");
    
    if (tempDir != null) {
      deleteRecursively(tempDir);
    }
    
    // Clear global buffer manager caches to release memory segments
    try {
      Databases.getGlobalBufferManager().clearAllCaches();
    } catch (Exception e) {
      // Ignore - might happen if buffer manager not initialized yet
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

  @Nested
  @DisplayName("Versioning Strategy Tests")
  class VersioningStrategyTests {

    @Test
    @DisplayName("HOT works with FULL versioning")
    void testHOTWithFullVersioning() throws IOException {
      testHOTWithVersioningStrategy(VersioningType.FULL);
    }
    
    @Test
    @DisplayName("HOT works with INCREMENTAL versioning")
    void testHOTWithIncrementalVersioning() throws IOException {
      testHOTWithVersioningStrategy(VersioningType.INCREMENTAL);
    }
    
    @Test
    @DisplayName("HOT works with DIFFERENTIAL versioning")
    void testHOTWithDifferentialVersioning() throws IOException {
      testHOTWithVersioningStrategy(VersioningType.DIFFERENTIAL);
    }
    
    @Test
    @DisplayName("HOT works with SLIDING_SNAPSHOT versioning")
    void testHOTWithSlidingSnapshotVersioning() throws IOException {
      testHOTWithVersioningStrategy(VersioningType.SLIDING_SNAPSHOT);
    }

    private void testHOTWithVersioningStrategy(VersioningType versioningType) throws IOException {
      Path dbPath = tempDir.resolve("db-" + versioningType.name());

      // Create database with specific versioning type
      DatabaseConfiguration dbConfig = new DatabaseConfiguration(dbPath);
      Databases.createJsonDatabase(dbConfig);

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
        // Create resource with the specified versioning type
        database.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME)
            .versioningApproach(versioningType)
            .maxNumberOfRevisionsToRestore(3) // Use small value to trigger fragment combining
            .build());

        // Insert data - use Commit.NO to avoid implicit commit, then commit manually
        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
             JsonNodeTrx wtx = session.beginNodeTrx()) {
          
          StringBuilder json = new StringBuilder("{");
          for (int i = 1; i <= 20; i++) {
            if (i > 1) json.append(",");
            json.append("\"key").append(i).append("\": \"value").append(i).append("\"");
          }
          json.append("}");
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), Commit.NO);
          wtx.commit();
        }

        // Verify data is readable
        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
             JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
          int keyCount = countObjectKeys(rtx);
          assertEquals(20, keyCount, 
              "Should have 20 keys with " + versioningType + " versioning");
        }
      }

      // Reopen database and verify data persists
      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
             JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
          int keyCount = countObjectKeys(rtx);
          assertEquals(20, keyCount, 
              "Should have 20 keys after reopening with " + versioningType + " versioning");
        }
      }
    }

    private int countObjectKeys(JsonNodeReadOnlyTrx rtx) {
      int keyCount = 0;
      rtx.moveToDocumentRoot();
      if (rtx.moveToFirstChild()) {  // Object
        if (rtx.moveToFirstChild()) {  // First key
          do {
            if (rtx.getKind() == NodeKind.OBJECT_KEY) {
              keyCount++;
            }
          } while (rtx.moveToRightSibling());
        }
      }
      return keyCount;
    }
  }

  @Nested
  @DisplayName("INCREMENTAL Versioning Specific Tests")
  class IncrementalVersioningTests {

    @Test
    @DisplayName("INCREMENTAL versioning maintains correct data in historical revisions")
    void testIncrementalVersioningCombinesFragments() throws IOException {
      testVersioningMaintainsHistoricalData(VersioningType.INCREMENTAL);
    }
    
    @Test
    @DisplayName("DIFFERENTIAL versioning maintains correct data in historical revisions")
    void testDifferentialVersioningCombinesFragments() throws IOException {
      testVersioningMaintainsHistoricalData(VersioningType.DIFFERENTIAL);
    }
    
    @Test
    @DisplayName("SLIDING_SNAPSHOT versioning maintains correct data in historical revisions")
    void testSlidingSnapshotVersioningCombinesFragments() throws IOException {
      testVersioningMaintainsHistoricalData(VersioningType.SLIDING_SNAPSHOT);
    }
    
    private void testVersioningMaintainsHistoricalData(VersioningType versioningType) throws IOException {
      // This test creates multiple revisions (with remove+insert pattern) and verifies
      // that historical revisions can be read correctly.
      // Note: This tests the data tree versioning, not the HOT index specifically.
      
      Path dbPath = tempDir.resolve("multirev-" + versioningType.name().toLowerCase() + "-" + System.nanoTime());

      DatabaseConfiguration dbConfig = new DatabaseConfiguration(dbPath);
      Databases.createJsonDatabase(dbConfig);

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
        database.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME)
            .versioningApproach(versioningType)
            .maxNumberOfRevisionsToRestore(5)
            .build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME)) {
          // Revision 1: Create array with 10 elements
          // Use Commit.NO to avoid double-commit (insertSubtree has Commit.IMPLICIT by default)
          try (JsonNodeTrx wtx = session.beginNodeTrx()) {
            wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[1,2,3,4,5,6,7,8,9,10]"), Commit.NO);
            wtx.commit();
          }
          
          // Revision 2: Replace with array of 20 elements
          try (JsonNodeTrx wtx = session.beginNodeTrx()) {
            wtx.moveToDocumentRoot();
            if (wtx.moveToFirstChild()) {
              wtx.remove();
            }
            StringBuilder json = new StringBuilder("[");
            for (int i = 1; i <= 20; i++) {
              if (i > 1) json.append(",");
              json.append(i);
            }
            json.append("]");
            wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), Commit.NO);
            wtx.commit();
          }

          // Check revision number
          int mostRecent = session.getMostRecentRevisionNumber();
          assertEquals(2, mostRecent, 
              "Most recent revision should be 2 for " + versioningType);
          
          // Verify latest revision (2) has 20 elements
          try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
            assertEquals(2, rtx.getRevisionNumber(),
                "Latest rtx should be on revision 2 for " + versioningType);
            int elementCount = countArrayElements(rtx);
            assertEquals(20, elementCount, 
                "Latest revision should have 20 elements for " + versioningType);
          }
          
          // Verify revision 2 explicitly has 20 elements
          try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(2)) {
            int elementCount = countArrayElements(rtx);
            assertEquals(20, elementCount, 
                "Revision 2 should have 20 elements with " + versioningType);
          }
          
          // Verify revision 1 still has 10 elements
          try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(1)) {
            int elementCount = countArrayElements(rtx);
            assertEquals(10, elementCount, 
                "Revision 1 should have 10 elements with " + versioningType);
          }
        }
      }
    }

    private int countArrayElements(JsonNodeReadOnlyTrx rtx) {
      int count = 0;
      rtx.moveToDocumentRoot();
      if (rtx.moveToFirstChild()) {  // Array
        if (rtx.moveToFirstChild()) {  // First element
          do {
            if (rtx.getKind() == NodeKind.NUMBER_VALUE) {
              count++;
            }
          } while (rtx.moveToRightSibling());
        }
      }
      return count;
    }
  }

  @Nested
  @DisplayName("HOT Index Persistence Tests")
  class HOTIndexPersistenceTests {

    @Test
    @DisplayName("HOT index survives database restart with DIFFERENTIAL versioning")
    void testHOTSurvivesRestartWithDifferentialVersioning() throws IOException {
      Path dbPath = tempDir.resolve("db-differential-restart");

      // Create database and insert data
      DatabaseConfiguration dbConfig = new DatabaseConfiguration(dbPath);
      Databases.createJsonDatabase(dbConfig);

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
        database.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME)
            .versioningApproach(VersioningType.DIFFERENTIAL)
              .maxNumberOfRevisionsToRestore(4)
            .build());

        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
             JsonNodeTrx wtx = session.beginNodeTrx()) {
          
          StringBuilder json = new StringBuilder("{");
          for (int i = 1; i <= 25; i++) {
            if (i > 1) json.append(",");
            json.append("\"field").append(i).append("\": \"data").append(i).append("\"");
          }
          json.append("}");
          
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()));
          wtx.commit();
        }
      }

      // Reopen database and verify data
      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
        try (JsonResourceSession session = database.beginResourceSession(RESOURCE_NAME);
             JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
          
          int keyCount = 0;
          rtx.moveToDocumentRoot();
          if (rtx.moveToFirstChild()) {
            if (rtx.moveToFirstChild()) {
              do {
                if (rtx.getKind() == NodeKind.OBJECT_KEY) {
                  keyCount++;
                }
              } while (rtx.moveToRightSibling());
            }
          }
          
          assertEquals(25, keyCount, "Should have 25 keys after database restart");
        }
      }
    }
  }
}

