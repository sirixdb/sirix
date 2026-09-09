/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.DatabaseType;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.IndexType;
import io.sirix.index.projection.GlobalValueDictionary;
import io.sirix.index.projection.GlobalValueDictionaryWriter;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** End-to-end regression for all four JSON NamePage namespaces sharing one committed page. */
final class NamePageReservedSlotCoexistenceIntegrationTest {

  private static final String RESOURCE = "name-page-slot-coexistence";
  private static final int UNIQUE_NAME_COUNT = HOTLeafPage.MAX_ENTRIES + 64;
  private static final byte[] FSST_TABLE = "coexistence-fsst-table".getBytes(StandardCharsets.UTF_8);
  private static final byte[] GLOBAL_VALUE = "coexistence-global-value".getBytes(StandardCharsets.UTF_8);

  @TempDir
  Path temporaryDirectory;

  @Test
  void dictionariesAndSecondaryNameIndexSurviveCommitColdReopenAndAllocatorResume() throws IOException {
    final Path databasePath = temporaryDirectory.resolve("database");
    final IndexDef nameIndex = IndexDefs.createNameIdxDef(0, IndexDef.DbType.JSON);
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try {
      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
        database.createResource(ResourceConfiguration.newBuilder(RESOURCE).build());
      }

      final long fsstTableId;
      final long dictionaryHeaderKey;
      final long persistedHotPageKey;
      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
          JsonResourceSession session = database.beginResourceSession(RESOURCE);
          JsonNodeTrx wtx = session.beginNodeTrx()) {
        final var indexController = session.getWtxIndexController(wtx.getRevisionNumber());
        indexController.createIndexes(Set.of(nameIndex), wtx);
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(wideObject()), JsonNodeTrx.Commit.NO);

        final var storageEngineWriter = wtx.getStorageEngineWriter();
        final NamePage namePage = storageEngineWriter.getNamePage(storageEngineWriter.getActualRevisionRootPage());
        fsstTableId = namePage.setFsstSymbolTable(FSST_TABLE, DatabaseType.JSON, storageEngineWriter,
            storageEngineWriter.getLog());

        final GlobalValueDictionaryWriter dictionary = new GlobalValueDictionaryWriter();
        try {
          assertEquals(1, dictionary.intern(GLOBAL_VALUE, 0, GLOBAL_VALUE.length));
          dictionaryHeaderKey =
              dictionary.flush(namePage, DatabaseType.JSON, storageEngineWriter, storageEngineWriter.getLog());
        } finally {
          dictionary.release();
        }

        persistedHotPageKey = namePage.getMaxHotPageKey(nameIndex.getID());
        assertTrue(persistedHotPageKey > 0,
            "the fixture must split the NAME root so allocator persistence is exercised");
        assertEquals(3, namePage.getDictionaryOffsetCount(),
            "secondary NAME metadata leaked into the positional dictionary counters");
        assertEquals(1, namePage.getMaxHotPageKeySize());
        wtx.commit();
      }

      Databases.getGlobalBufferManager().clearAllCaches();
      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
          JsonResourceSession session = database.beginResourceSession(RESOURCE);
          JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final var storageEngineReader = rtx.getStorageEngineReader();
        final NamePage namePage = storageEngineReader.getNamePage(storageEngineReader.getActualRevisionRootPage());
        assertArrayEquals(FSST_TABLE, namePage.getFsstSymbolTable(fsstTableId, DatabaseType.JSON, storageEngineReader));
        assertEquals(new String(GLOBAL_VALUE, StandardCharsets.UTF_8),
            GlobalValueDictionary.value(dictionaryHeaderKey, 1, storageEngineReader));
        assertEquals(persistedHotPageKey, namePage.getMaxHotPageKey(nameIndex.getID()));
        assertEquals(3, namePage.getDictionaryOffsetCount());
        assertEquals(nameIndex.getID() + 1, namePage.nextUnallocatedSecondaryNameIndex(DatabaseType.JSON),
            "cold allocation probing must reserve the committed physical NAME tree");

        final var indexController = session.getRtxIndexController(rtx.getRevisionNumber());
        final IndexDef persistedNameIndex = indexController.getIndexes().getIndexDef(nameIndex.getID(), IndexType.NAME);
        assertNotNull(persistedNameIndex);
        final var references = indexController.openNameIndex(storageEngineReader, persistedNameIndex,
            indexController.createNameFilter(Set.of(name(UNIQUE_NAME_COUNT - 1))));
        assertTrue(references.hasNext(), "the cold-reopened NAME index lost its final key");
        assertTrue(references.next().getNodeKeys().getLongCardinality() > 0);
      }

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
          JsonResourceSession session = database.beginResourceSession(RESOURCE);
          JsonNodeTrx wtx = session.beginNodeTrx()) {
        final var storageEngineWriter = wtx.getStorageEngineWriter();
        final NamePage namePage = storageEngineWriter.getNamePage(storageEngineWriter.getActualRevisionRootPage());
        assertEquals(Math.addExact(persistedHotPageKey, 1L), namePage.incrementAndGetMaxHotPageKey(nameIndex.getID()),
            "the sparse physical NAME id resumed from the wrong page-key high-water mark");
        wtx.rollback();
      }
    } finally {
      Databases.getGlobalBufferManager().clearAllCaches();
      Databases.removeDatabase(databasePath);
    }
  }

  private static String wideObject() {
    final StringBuilder json = new StringBuilder(UNIQUE_NAME_COUNT * 20).append('{');
    for (int index = 0; index < UNIQUE_NAME_COUNT; index++) {
      if (index > 0) {
        json.append(',');
      }
      json.append('"').append(name(index)).append("\":").append(index);
    }
    return json.append('}').toString();
  }

  private static String name(final int index) {
    return "coexistence_name_" + index;
  }
}
