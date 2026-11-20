/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access;

import io.sirix.api.Database;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.io.ram.RAMStorage;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Removing a database releases the committed state of its in-memory resources, which lives in the
 * process rather than in the deleted files.
 */
final class DatabasesInMemoryRemovalTest {

  @TempDir
  Path directory;

  @Test
  void removeDatabaseReleasesItsInMemoryStores() {
    final Path path = directory.resolve("database");
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    final ResourceConfiguration configuration;
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      configuration = LocalDatabaseInMemoryRemovalTest.InMemoryResources.createCommitted(database);
    }
    assertTrue(new RAMStorage(configuration).exists(), "precondition: the store outlives the database handle");

    Databases.removeDatabase(path);

    try {
      assertFalse(new RAMStorage(configuration).exists(), "a removed database's stores must be released");
    } finally {
      // The probe above registered an empty store at the removed path.
      RAMStorage.discardUnder(path);
    }
  }
}
