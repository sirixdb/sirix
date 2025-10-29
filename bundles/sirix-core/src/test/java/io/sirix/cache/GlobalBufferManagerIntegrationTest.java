package io.sirix.cache;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonResourceSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for global BufferManager with multiple databases and resources.
 * Validates that:
 * 1. Multiple databases can coexist with a single global BufferManager
 * 2. No cache collisions occur between different databases
 * 3. No cache collisions occur between different resources
 * 4. Database IDs are properly assigned and persisted
 *
 * @author Johannes Lichtenberger
 */
public class GlobalBufferManagerIntegrationTest {

  @TempDir
  Path tempDir;

  private Path database1Path;
  private Path database2Path;

  @BeforeEach
  public void setUp() {
    database1Path = tempDir.resolve("database1");
    database2Path = tempDir.resolve("database2");
  }

  @AfterEach
  public void tearDown() {
    Databases.removeDatabase(database1Path);
    Databases.removeDatabase(database2Path);
  }

  @Test
  public void testMultipleDatabasesHaveUniqueDatabaseIds() {
    // Create two databases
    final var db1Config = new DatabaseConfiguration(database1Path);
    Databases.createJsonDatabase(db1Config);

    final var db2Config = new DatabaseConfiguration(database2Path);
    Databases.createJsonDatabase(db2Config);

    // Open databases
    try (final Database<JsonResourceSession> db1 = Databases.openJsonDatabase(database1Path);
         final Database<JsonResourceSession> db2 = Databases.openJsonDatabase(database2Path)) {

      // Verify databases have different IDs
      final var db1ConfigLoaded = DatabaseConfiguration.deserialize(database1Path);
      final var db2ConfigLoaded = DatabaseConfiguration.deserialize(database2Path);

      assertNotEquals(db1ConfigLoaded.getDatabaseId(), db2ConfigLoaded.getDatabaseId(),
          "Different databases must have unique database IDs");

      assertTrue(db1ConfigLoaded.getDatabaseId() >= 0, "Database 1 ID should be valid");
      assertTrue(db2ConfigLoaded.getDatabaseId() >= 0, "Database 2 ID should be valid");
    }
  }

  @Test
  public void testMultipleResourcesInSameDatabaseHaveUniqueResourceIds() {
    // Create database
    Databases.createJsonDatabase(new DatabaseConfiguration(database1Path));

    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(database1Path)) {
      // Create two resources
      db.createResource(ResourceConfiguration.newBuilder("resource1").build());
      db.createResource(ResourceConfiguration.newBuilder("resource2").build());

      // Open resources
      try (final JsonResourceSession res1 = db.beginResourceSession("resource1");
           final JsonResourceSession res2 = db.beginResourceSession("resource2")) {

        final long res1Id = res1.getResourceConfig().getID();
        final long res2Id = res2.getResourceConfig().getID();

        assertNotEquals(res1Id, res2Id,
            "Different resources in same database must have unique resource IDs");

        // Verify both have same database ID
        final long res1DbId = res1.getResourceConfig().getDatabaseId();
        final long res2DbId = res2.getResourceConfig().getDatabaseId();

        assertEquals(res1DbId, res2DbId,
            "Resources in same database should have same database ID");
      }
    }
  }

  @Test
  public void testGlobalBufferManagerIsShared() {
    // Create two databases
    Databases.createJsonDatabase(new DatabaseConfiguration(database1Path));
    Databases.createJsonDatabase(new DatabaseConfiguration(database2Path));

    try (final Database<JsonResourceSession> db1 = Databases.openJsonDatabase(database1Path);
         final Database<JsonResourceSession> db2 = Databases.openJsonDatabase(database2Path)) {

      // Create resources in both databases
      db1.createResource(ResourceConfiguration.newBuilder("resource1").build());
      db2.createResource(ResourceConfiguration.newBuilder("resource1").build());

      try (final JsonResourceSession res1 = db1.beginResourceSession("resource1");
           final JsonResourceSession res2 = db2.beginResourceSession("resource1")) {

        // Both should use the same global BufferManager instance
        final var bufferManager1 = Databases.getGlobalBufferManager();
        final var bufferManager2 = Databases.getBufferManager(database2Path);

        assertSame(bufferManager1, bufferManager2,
            "All databases should share the same global BufferManager instance");
      }
    }
  }

  @Test
  public void testDatabaseIdPersistence() {
    // Create database
    Databases.createJsonDatabase(new DatabaseConfiguration(database1Path));

    // Get database ID
    final long originalDatabaseId;
    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(database1Path)) {
      originalDatabaseId = DatabaseConfiguration.deserialize(database1Path).getDatabaseId();
      assertTrue(originalDatabaseId >= 0, "Database ID should be assigned");
    }

    // Reopen database and verify ID persisted
    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(database1Path)) {
      final long reloadedDatabaseId = DatabaseConfiguration.deserialize(database1Path).getDatabaseId();

      assertEquals(originalDatabaseId, reloadedDatabaseId,
          "Database ID should persist across database close/reopen");
    }
  }

  @Test
  public void testCacheKeysIncludeResourceAndDatabaseIds() {
    // Create database with resource
    Databases.createJsonDatabase(new DatabaseConfiguration(database1Path));

    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(database1Path)) {
      db.createResource(ResourceConfiguration.newBuilder("testResource").build());

      try (final JsonResourceSession session = db.beginResourceSession("testResource");
           final var rtx = session.beginNodeReadOnlyTrx()) {

        // Access the page transaction to verify IDs are set
        final var pageRtx = rtx.getPageTrx();

        // Verify transaction has access to both IDs
        final long databaseId = pageRtx.getDatabaseId();
        final long resourceId = pageRtx.getResourceId();

        assertTrue(databaseId >= 0, "Page transaction should have valid database ID");
        assertTrue(resourceId >= 0, "Page transaction should have valid resource ID");

        // Verify IDs match configuration
        assertEquals(session.getResourceConfig().getDatabaseId(), databaseId,
            "Transaction database ID should match resource config");
        assertEquals(session.getResourceConfig().getID(), resourceId,
            "Transaction resource ID should match resource config");
      }
    }
  }
}

