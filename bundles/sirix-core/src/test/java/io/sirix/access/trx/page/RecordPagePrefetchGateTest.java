/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access.trx.page;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexType;
import io.sirix.io.StorageType;
import io.sirix.page.PageReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The standard FILE_CHANNEL backend now advertises an advisory prefetch batch, which switches on the
 * span hints the scan paths already issue. Read transactions see the batch, write transactions never
 * do, and hinting never changes what is read.
 */
final class RecordPagePrefetchGateTest {
  private static final int VALUES = 3_000;

  @TempDir
  Path directory;

  @Test
  void readTransactionsAdvertiseTheBackendBatchAndWriteTransactionsNever() {
    final Path path = directory.resolve("prefetch-gate");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(path)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      assertTrue(database.createResource(
          ResourceConfiguration.newBuilder("resource").storageType(StorageType.FILE_CHANNEL).build()));
      try (JsonResourceSession session = database.beginResourceSession("resource")) {
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          writer.insertArrayAsFirstChild();
          for (int i = 0; i < VALUES; i++) {
            writer.insertStringValueAsFirstChild("value-" + i);
            writer.moveToParent();
          }
          assertEquals(0, writer.getStorageEngineWriter().recordPagePrefetchBatch(),
              "a write transaction reads through its intent log and must never advertise a batch");
          writer.commit();
        }
      }
    }
    Databases.clearGlobalCaches();
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path);
        JsonResourceSession session = database.beginResourceSession("resource");
        JsonNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx()) {
      final StorageEngineReader reader = trx.getStorageEngineReader();
      final int batch = reader.recordPagePrefetchBatch();
      if (System.getProperty("os.name", "").toLowerCase(Locale.ROOT).contains("linux")) {
        assertTrue(batch > 0, "on Linux the file-channel backend must advertise its advisory batch");
      }
      assertTrue(batch >= 0);
      // Resolve every record page the array's values live on and hint them all, both through the
      // record-page route and through the raw span route, before reading a single value.
      assertTrue(trx.moveToFirstChild());
      assertTrue(trx.moveToFirstChild());
      final long[] recordPageKeys = new long[VALUES];
      int distinct = 0;
      long previousPageKey = -1;
      do {
        final long pageKey = reader.pageKey(trx.getNodeKey(), IndexType.DOCUMENT);
        if (pageKey != previousPageKey) {
          recordPageKeys[distinct++] = pageKey;
          previousPageKey = pageKey;
        }
      } while (trx.moveToRightSibling());
      assertTrue(distinct > 1, "the fixture must span several record pages");
      final int hinted = distinct;
      assertDoesNotThrow(() -> reader.prefetchRecordPages(recordPageKeys, hinted, IndexType.DOCUMENT));
      final PageReference[] spans = new PageReference[distinct];
      int resolved = 0;
      for (int i = 0; i < distinct; i++) {
        final PageReference leaf = reader.getLeafPageReference(recordPageKeys[i], 0, IndexType.DOCUMENT);
        if (leaf != null) {
          spans[resolved++] = leaf;
        }
      }
      final int spanCount = resolved;
      assertDoesNotThrow(() -> reader.prefetchPageSpans(spans, spanCount));
      assertDoesNotThrow(() -> reader.prefetchRecordPages(recordPageKeys, 0, IndexType.DOCUMENT));
      // insertStringValueAsFirstChild reverses insertion order.
      assertTrue(trx.moveToParent());
      assertTrue(trx.moveToFirstChild());
      for (int expected = VALUES - 1; expected >= 0; expected--) {
        assertEquals("value-" + expected, trx.getValue());
        if (expected > 0) {
          assertTrue(trx.moveToRightSibling());
        }
      }
      assertFalse(trx.moveToRightSibling());
    }
  }
}
