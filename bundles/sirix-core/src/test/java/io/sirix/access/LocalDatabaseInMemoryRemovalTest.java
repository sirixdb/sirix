/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access;

import io.sirix.access.trx.node.HashType;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.io.StorageType;
import io.sirix.io.ram.RAMStorage;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Removing an {@link StorageType#IN_MEMORY} resource releases its committed state, which lives in
 * the process rather than in the deleted files.
 */
final class LocalDatabaseInMemoryRemovalTest {

  @TempDir
  Path directory;

  @AfterEach
  void removeDatabase() {
    Databases.removeDatabase(directory.resolve("database"));
  }

  @Test
  void removeResourceReleasesTheInMemoryStore() {
    final Path path = directory.resolve("database");
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      final ResourceConfiguration configuration = InMemoryResources.createCommitted(database);
      assertTrue(new RAMStorage(configuration).exists(), "precondition: the committed state is retained");

      database.removeResource(InMemoryResources.NAME);

      // Same path AND same identity: only the removal itself can have dropped the store.
      assertFalse(new RAMStorage(configuration).exists(), "a removed resource's store must be released");
    }
  }

  /** Shared fixture for the removal tests of both removal owners. */
  static final class InMemoryResources {
    static final String NAME = "resource";

    private InMemoryResources() {}

    /** Create an in-memory resource with one committed value and return its configuration. */
    static ResourceConfiguration createCommitted(final Database<JsonResourceSession> database) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder(NAME)
                                                              .storageType(StorageType.IN_MEMORY)
                                                              .hashKind(HashType.NONE)
                                                              .buildPathSummary(false)
                                                              .build()));
      try (JsonResourceSession session = database.beginResourceSession(NAME);
          JsonNodeTrx writer = session.beginNodeTrx()) {
        writer.insertNumberValueAsFirstChild(0);
        writer.commit();
        return session.getResourceConfig();
      }
    }
  }
}
