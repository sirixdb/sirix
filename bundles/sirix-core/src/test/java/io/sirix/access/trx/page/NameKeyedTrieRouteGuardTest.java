/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access.trx.page;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.api.xml.XmlResourceSession;
import io.sirix.cache.IndexLogKey;
import io.sirix.cache.TransactionIntentLog;
import io.sirix.index.IndexType;
import io.sirix.node.FsstSymbolTableNode;
import io.sirix.node.interfaces.DataRecord;
import io.sirix.page.NamePage;
import io.sirix.page.PageConstants;
import io.sirix.page.PageReference;
import io.sirix.page.RevisionRootPage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

/** The generic keyed-trie API must never address a secondary NAME HOT slot. */
final class NameKeyedTrieRouteGuardTest {

  private static final String RESOURCE = "name-keyed-trie-route-guard";

  @TempDir
  Path temporaryDirectory;

  @AfterEach
  void clearGlobalCaches() {
    Databases.clearGlobalCaches();
  }

  @Test
  void centralRouteGuardUsesTheDatabaseSpecificBoundary() {
    final StorageEngineReader jsonReader = mock(StorageEngineReader.class);
    doReturn(mock(JsonResourceSession.class)).when(jsonReader).getResourceSession();
    assertDoesNotThrow(() -> NodeStorageEngineReader.validateKeyedTrieRoute(jsonReader, IndexType.NAME,
        PageConstants.JSON_NAME_INDEX_OFFSET - 1));
    assertThrows(IllegalArgumentException.class, () -> NodeStorageEngineReader.validateKeyedTrieRoute(jsonReader,
        IndexType.NAME, PageConstants.JSON_NAME_INDEX_OFFSET));

    final StorageEngineReader xmlReader = mock(StorageEngineReader.class);
    doReturn(mock(XmlResourceSession.class)).when(xmlReader).getResourceSession();
    assertDoesNotThrow(() -> NodeStorageEngineReader.validateKeyedTrieRoute(xmlReader, IndexType.NAME,
        PageConstants.XML_NAME_INDEX_OFFSET - 1));
    assertThrows(IllegalArgumentException.class, () -> NodeStorageEngineReader.validateKeyedTrieRoute(xmlReader,
        IndexType.NAME, PageConstants.XML_NAME_INDEX_OFFSET));
  }

  @ParameterizedTest
  @EnumSource(value = IndexType.class, names = {"PATH", "CAS", "PROJECTION", "VALIDTIME"})
  void genericKeyedTrieRouteRejectsEveryHotOnlyFamily(final IndexType indexType) {
    assertThrows(IllegalArgumentException.class,
        () -> NodeStorageEngineReader.validateKeyedTrieRoute(mock(StorageEngineReader.class), indexType, 0));
  }

  @Test
  void keyedTrieWriterRejectsASecondaryNameSlotBeforeNavigation() {
    final StorageEngineWriter writer = mock(StorageEngineWriter.class);
    doReturn(mock(JsonResourceSession.class)).when(writer).getResourceSession();

    assertThrows(IllegalArgumentException.class,
        () -> new KeyedTrieWriter().prepareLeafOfTree(writer, mock(TransactionIntentLog.class), new int[] {10},
            new PageReference(), 0L, PageConstants.JSON_NAME_INDEX_OFFSET, IndexType.NAME, new RevisionRootPage()));
  }

  @Test
  void everyReaderAndWriterEntryPointRejectsASecondaryNameSlot() {
    final Path databasePath = temporaryDirectory.resolve("database");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));

    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE).build());

      final int secondaryNameIndex = PageConstants.JSON_NAME_INDEX_OFFSET;
      try (JsonResourceSession session = database.beginResourceSession(RESOURCE);
          JsonNodeTrx wtx = session.beginNodeTrx()) {
        final NodeStorageEngineWriter writer = (NodeStorageEngineWriter) wtx.getStorageEngineWriter();
        final DataRecord record = new FsstSymbolTableNode(1L, new byte[] {1});

        assertThrows(IllegalArgumentException.class, () -> writer.getRecord(1L, IndexType.NAME, secondaryNameIndex));
        assertThrows(IllegalArgumentException.class,
            () -> writer.createRecord(record, IndexType.NAME, secondaryNameIndex));
        assertThrows(IllegalArgumentException.class,
            () -> writer.prepareRecordForModification(1L, IndexType.NAME, secondaryNameIndex));
        assertThrows(IllegalArgumentException.class,
            () -> writer.persistRecord(record, IndexType.NAME, secondaryNameIndex));
        assertThrows(IllegalArgumentException.class, () -> writer.removeRecord(1L, IndexType.NAME, secondaryNameIndex));
        assertThrows(IllegalArgumentException.class,
            () -> writer.getModifiedPageForRead(0L, IndexType.NAME, secondaryNameIndex));
        assertThrows(IllegalArgumentException.class,
            () -> writer.getLeafPageReference(0L, secondaryNameIndex, IndexType.NAME));
        assertThrows(IllegalArgumentException.class, () -> writer.getCurrentMaxIndirectPageTreeLevel(IndexType.NAME,
            secondaryNameIndex, writer.getActualRevisionRootPage()));
        assertThrows(IllegalArgumentException.class, () -> writer.getReferenceToLeafOfSubtree(new PageReference(), 0L,
            secondaryNameIndex, IndexType.NAME, writer.getActualRevisionRootPage()));
      }

      try (JsonResourceSession session = database.beginResourceSession(RESOURCE);
          JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final NodeStorageEngineReader reader = (NodeStorageEngineReader) rtx.getStorageEngineReader();

        assertThrows(IllegalArgumentException.class, () -> reader.getRecord(1L, IndexType.NAME, secondaryNameIndex));
        assertThrows(IllegalArgumentException.class,
            () -> reader.lookupSlotOrCached(1L, IndexType.NAME, secondaryNameIndex));
        assertThrows(IllegalArgumentException.class,
            () -> reader.lookupSlotWithGuard(1L, IndexType.NAME, secondaryNameIndex));
        assertThrows(IllegalArgumentException.class, () -> reader.getRecordPage(
            new IndexLogKey(IndexType.NAME, 0L, secondaryNameIndex, reader.getRevisionNumber())));
        assertThrows(IllegalArgumentException.class,
            () -> reader.getLeafPageReference(0L, secondaryNameIndex, IndexType.NAME));
        assertThrows(IllegalArgumentException.class,
            () -> reader.getPageReference(reader.getActualRevisionRootPage(), IndexType.NAME, secondaryNameIndex));
        assertThrows(IllegalArgumentException.class, () -> reader.getCurrentMaxIndirectPageTreeLevel(IndexType.NAME,
            secondaryNameIndex, reader.getActualRevisionRootPage()));
        assertThrows(IllegalArgumentException.class, () -> reader.getReferenceToLeafOfSubtree(new PageReference(), 0L,
            secondaryNameIndex, IndexType.NAME, reader.getActualRevisionRootPage()));
      }
    }
  }
}
