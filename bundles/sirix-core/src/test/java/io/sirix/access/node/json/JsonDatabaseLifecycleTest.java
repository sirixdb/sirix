package io.sirix.access.node.json;

import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.io.StorageType;
import io.sirix.node.SirixDeweyID;
import io.sirix.settings.VersioningType;
import io.sirix.utils.JsonDocumentCreator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for database and resource lifecycle operations including creation,
 * opening, closing, persistence, and resource configuration.
 */
public final class JsonDatabaseLifecycleTest {

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  @Test
  void testCreateAndOpenDatabase() {
    final Path dbPath = PATHS.PATH1.getFile();
    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
      assertNotNull(database);
      assertTrue(database.isOpen());
    }
  }

  @Test
  void testMultipleResources() {
    final Path dbPath = PATHS.PATH1.getFile();
    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
      database.createResource(ResourceConfiguration.newBuilder("resource1").build());
      database.createResource(ResourceConfiguration.newBuilder("resource2").build());
      database.createResource(ResourceConfiguration.newBuilder("resource3").build());

      assertTrue(database.existsResource("resource1"));
      assertTrue(database.existsResource("resource2"));
      assertTrue(database.existsResource("resource3"));

      final List<Path> resources = database.listResources();
      assertEquals(3, resources.size());
    }
  }

  @Test
  void testExistsResource() {
    final Path dbPath = PATHS.PATH1.getFile();
    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
      database.createResource(ResourceConfiguration.newBuilder("existing").build());

      assertTrue(database.existsResource("existing"));
      assertFalse(database.existsResource("nonexistent"));
    }
  }

  @Test
  void testOpenCloseReopenPersistence() {
    final Path dbPath = PATHS.PATH1.getFile();
    final String resourceName = "persistenceTest";
    final String expectedJson = JsonDocumentCreator.JSON;

    // Create database, insert data, close.
    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
      database.createResource(ResourceConfiguration.newBuilder(resourceName).build());
      try (final JsonResourceSession session = database.beginResourceSession(resourceName);
           final JsonNodeTrx wtx = session.beginNodeTrx()) {
        JsonDocumentCreator.create(wtx);
        wtx.commit();
      }
    }

    // Reopen and verify data persists.
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
      assertTrue(database.existsResource(resourceName));
      try (final JsonResourceSession session = database.beginResourceSession(resourceName);
           final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertTrue(rtx.moveToDocumentRoot());
        assertTrue(rtx.moveToFirstChild());
        assertTrue(rtx.isObject());
      }
    }
  }

  @Test
  void testRemoveDatabase() {
    final Path dbPath = PATHS.PATH1.getFile();
    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
      assertNotNull(database);
    }

    Databases.removeDatabase(dbPath);
    assertFalse(Files.exists(dbPath));
  }

  @Test
  void testCustomResourceConfigHashKind() {
    final Path dbPath = PATHS.PATH1.getFile();
    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
      final ResourceConfiguration config =
          ResourceConfiguration.newBuilder("rollingHash")
                               .hashKind(HashType.ROLLING)
                               .build();
      database.createResource(config);
      assertTrue(database.existsResource("rollingHash"));
    }
  }

  @Test
  void testCustomResourceConfigVersioning() {
    final Path dbPath = PATHS.PATH1.getFile();
    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
      final ResourceConfiguration config =
          ResourceConfiguration.newBuilder("fullVersioning")
                               .versioningApproach(VersioningType.FULL)
                               .build();
      database.createResource(config);
      assertTrue(database.existsResource("fullVersioning"));
    }
  }

  @Test
  void testCustomResourceConfigStorageType() {
    final Path dbPath = PATHS.PATH1.getFile();
    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
      final ResourceConfiguration config =
          ResourceConfiguration.newBuilder("memMapped")
                               .storageType(StorageType.MEMORY_MAPPED)
                               .build();
      database.createResource(config);
      assertTrue(database.existsResource("memMapped"));
    }
  }

  @Test
  void testMultipleDatabases() {
    final Path dbPath1 = PATHS.PATH1.getFile();
    final Path dbPath2 = PATHS.PATH2.getFile();

    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath1));
    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath2));

    try (final Database<JsonResourceSession> db1 = Databases.openJsonDatabase(dbPath1);
         final Database<JsonResourceSession> db2 = Databases.openJsonDatabase(dbPath2)) {
      assertNotNull(db1);
      assertNotNull(db2);
      assertTrue(db1.isOpen());
      assertTrue(db2.isOpen());

      db1.createResource(ResourceConfiguration.newBuilder("res1").build());
      db2.createResource(ResourceConfiguration.newBuilder("res2").build());

      assertTrue(db1.existsResource("res1"));
      assertFalse(db1.existsResource("res2"));
      assertTrue(db2.existsResource("res2"));
      assertFalse(db2.existsResource("res1"));
    }
  }

  @Test
  void testSessionCloseAndReopen() {
    final Path dbPath = PATHS.PATH1.getFile();
    final String resourceName = "sessionTest";

    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
      database.createResource(ResourceConfiguration.newBuilder(resourceName).build());

      // First session: insert data and close.
      try (final JsonResourceSession session = database.beginResourceSession(resourceName);
           final JsonNodeTrx wtx = session.beginNodeTrx()) {
        JsonDocumentCreator.create(wtx);
        wtx.commit();
      }

      // Second session: verify data is still accessible.
      try (final JsonResourceSession session = database.beginResourceSession(resourceName);
           final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertTrue(rtx.moveToDocumentRoot());
        assertTrue(rtx.moveToFirstChild());
        assertTrue(rtx.isObject());
      }
    }
  }

  @Test
  void testDatabaseName() {
    final Path dbPath = PATHS.PATH1.getFile();
    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
      final String name = database.getName();
      assertNotNull(name);
      assertEquals(dbPath.getFileName().toString(), name);
    }
  }

  @Test
  void testDeweyIDsEnabled() {
    final Path dbPath = PATHS.PATH1.getFile();
    final String resourceName = "deweyTest";

    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
      final ResourceConfiguration config =
          ResourceConfiguration.newBuilder(resourceName)
                               .useDeweyIDs(true)
                               .build();
      database.createResource(config);

      try (final JsonResourceSession session = database.beginResourceSession(resourceName);
           final JsonNodeTrx wtx = session.beginNodeTrx()) {
        JsonDocumentCreator.create(wtx);
        wtx.commit();
      }

      try (final JsonResourceSession session = database.beginResourceSession(resourceName);
           final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertTrue(rtx.moveToDocumentRoot());
        assertTrue(rtx.moveToFirstChild());
        final SirixDeweyID deweyID = rtx.getDeweyID();
        assertNotNull(deweyID);
      }
    }
  }

  @Test
  void testHashesEnabled() {
    final Path dbPath = PATHS.PATH1.getFile();
    final String resourceName = "hashTest";

    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
      final ResourceConfiguration config =
          ResourceConfiguration.newBuilder(resourceName)
                               .hashKind(HashType.ROLLING)
                               .build();
      database.createResource(config);

      try (final JsonResourceSession session = database.beginResourceSession(resourceName);
           final JsonNodeTrx wtx = session.beginNodeTrx()) {
        JsonDocumentCreator.create(wtx);
        wtx.commit();
      }

      try (final JsonResourceSession session = database.beginResourceSession(resourceName);
           final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertTrue(rtx.moveToDocumentRoot());
        assertTrue(rtx.moveToFirstChild());
        final long hash = rtx.getHash();
        assertNotEquals(0L, hash);
      }
    }
  }

  @Test
  void testEmptyResourceList() {
    final Path dbPath = PATHS.PATH1.getFile();
    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));

    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(dbPath)) {
      final List<Path> resources = database.listResources();
      assertNotNull(resources);
      assertTrue(resources.isEmpty());
    }
  }
}
