package io.sirix.index.interval;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexType;
import io.sirix.index.hot.HOTIndexReader;
import io.sirix.io.StorageType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ValidTimeReadCacheTest {
  @TempDir
  Path directory;

  @AfterEach
  void clearCaches() {
    Databases.clearGlobalCaches();
  }

  @Test
  void masksAreMemoizedPerRevisionAcrossTransactionsUntilCleared() {
    final Path databasePath = directory.resolve("masks");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (var database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder("rows").storageType(StorageType.FILE_CHANNEL).build());
      try (var session = database.beginResourceSession("rows")) {
        assertTrue(Databases.getGlobalBufferManager().getHOTLookupCache().isEnabled());
        try (var writer = session.beginNodeTrx()) {
          writer.insertObjectAsFirstChild();
          ValidTimeIntervalIndexFactory.createWriterTree(writer.getStorageEngineWriter(), 0, new IntervalDomain())
                                       .insert(1, 10, 20);
          writer.commit();
        }
        final int first = session.getMostRecentRevisionNumber();
        final PostingMask lower = lowerMask(session, first);
        assertEquals(0b10L, lower.word(0), "the interval is registered under reference 1");
        assertSame(lower, lowerMask(session, first), "a later transaction of the same revision reuses the mask");
        try (var writer = session.beginNodeTrx()) {
          ValidTimeIntervalIndexFactory.createWriterTree(writer.getStorageEngineWriter(), 0, new IntervalDomain())
                                       .insert(2, 10, 20);
          writer.commit();
        }
        final int second = session.getMostRecentRevisionNumber();
        final PostingMask updated = lowerMask(session, second);
        assertNotSame(lower, updated, "a new revision reads its own mask");
        assertEquals(0b110L, updated.word(0), "the new revision registers both references");
        assertSame(lower, lowerMask(session, first), "the earlier revision keeps its own mask");
        Objects.requireNonNull(session.getValidTimeReadCache()).clear();
        final PostingMask reloaded = lowerMask(session, first);
        assertNotSame(lower, reloaded, "clearing the session memo reloads the mask");
        assertEquals(lower.word(0), reloaded.word(0));
      }
    }
  }

  private static PostingMask lowerMask(final JsonResourceSession session, final int revision) {
    try (var transaction = session.beginNodeReadOnlyTrx(revision)) {
      final StorageEngineReader storage = transaction.getStorageEngineReader();
      final var reader = HOTIndexReader.create(storage, ValidTimeKeySerializer.INSTANCE, IndexType.VALIDTIME, 0);
      return new HotOrderedStore(ValidTimeKey.STORE_LOWER, null, reader).chunkMask(16, 10, 0);
    }
  }
}
