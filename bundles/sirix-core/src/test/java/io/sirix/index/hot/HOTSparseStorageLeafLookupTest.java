/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexType;
import io.sirix.page.PathPage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Storage-engine HOT root lookup must address sparse physical slots without materializing gaps. */
final class HOTSparseStorageLeafLookupTest {

  private static final String RESOURCE = "sparse-storage-hot-leaf";
  private static final int SPARSE_INDEX = 911;

  @TempDir
  Path temporaryDirectory;

  @Test
  void writerAndReaderResolveSparseHighIdWithoutCreatingReferences() {
    final Path databasePath = temporaryDirectory.resolve("db");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));

    try {
      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
        database.createResource(ResourceConfiguration.newBuilder(RESOURCE).build());
      }

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
          JsonResourceSession session = database.beginResourceSession(RESOURCE);
          JsonNodeTrx wtx = session.beginNodeTrx()) {
        final StorageEngineWriter storageEngineWriter = wtx.getStorageEngineWriter();
        final HOTLongIndexWriter indexWriter =
            HOTLongIndexWriter.create(storageEngineWriter, IndexType.PATH, SPARSE_INDEX);
        indexWriter.indexNodeKey(42L, 7L);

        final PathPage pathPage = storageEngineWriter.getPathPage(storageEngineWriter.getActualRevisionRootPage());
        final int referencesBeforeLookup = pathPage.getReferencesCount();
        assertTrue(referencesBeforeLookup < SPARSE_INDEX,
            "fixture must distinguish populated-reference count from physical slot offset");
        assertNotNull(storageEngineWriter.getHOTLeafPage(IndexType.PATH, SPARSE_INDEX));
        assertNull(storageEngineWriter.getHOTLeafPage(IndexType.PATH, SPARSE_INDEX - 1));
        assertEquals(referencesBeforeLookup, pathPage.getReferencesCount(),
            "writer lookup must neither reject a sparse ID nor create the missing predecessor");

        wtx.commit();
      }

      Databases.getGlobalBufferManager().clearAllCaches();
      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
          JsonResourceSession session = database.beginResourceSession(RESOURCE);
          JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final StorageEngineReader storageEngineReader = rtx.getStorageEngineReader();
        final PathPage pathPage = storageEngineReader.getPathPage(storageEngineReader.getActualRevisionRootPage());
        final int referencesBeforeLookup = pathPage.getReferencesCount();
        assertTrue(referencesBeforeLookup < SPARSE_INDEX,
            "fixture must distinguish populated-reference count from physical slot offset");
        assertNotNull(storageEngineReader.getHOTLeafPage(IndexType.PATH, SPARSE_INDEX));
        assertNull(storageEngineReader.getHOTLeafPage(IndexType.PATH, SPARSE_INDEX - 1));
        assertEquals(referencesBeforeLookup, pathPage.getReferencesCount(),
            "reader lookup must neither reject a sparse ID nor create the missing predecessor");
      }
    } finally {
      Databases.getGlobalBufferManager().clearAllCaches();
      Databases.removeDatabase(databasePath);
    }
  }
}
