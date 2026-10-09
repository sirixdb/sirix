package io.sirix.index.interval;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.index.IndexType;
import io.sirix.index.hot.HOTIndexReader;
import io.sirix.io.StorageType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

final class PostingMaskCacheTest {
  @TempDir
  Path directory;

  @AfterEach
  void clearCaches() {
    Databases.clearGlobalCaches();
  }

  @Test
  void masksSurviveTransactionClosureButRemainRevisionAndGenerationSpecific() {
    Databases.createJsonDatabase(new DatabaseConfiguration(directory.resolve("masks")));
    try (var database = Databases.openJsonDatabase(directory.resolve("masks"))) {
      database.createResource(ResourceConfiguration.newBuilder("rows").storageType(StorageType.FILE_CHANNEL).build());
      try (var session = database.beginResourceSession("rows")) {
        try (var writer = session.beginNodeTrx()) {
          writer.insertObjectAsFirstChild();
          ValidTimeIntervalIndexFactory.createWriterTree(writer.getStorageEngineWriter(), 0, new IntervalDomain())
                                       .insert(1, 10, 20);
          writer.commit();
        }
        final int before = session.getMostRecentRevisionNumber();
        final PostingMask original;
        try (var transaction = session.beginNodeReadOnlyTrx(before)) {
          final var reader = HOTIndexReader.create(transaction.getStorageEngineReader(),
              ValidTimeKeySerializer.INSTANCE, IndexType.VALIDTIME, 0);
          original = new HotOrderedStore(ValidTimeKey.STORE_LOWER, null, reader).chunkMask(16, 10, 1);
          assertEquals(2, original.word(0));
        }
        try (var transaction = session.beginNodeReadOnlyTrx(before)) {
          final var reader = HOTIndexReader.create(transaction.getStorageEngineReader(),
              ValidTimeKeySerializer.INSTANCE, IndexType.VALIDTIME, 0);
          assertSame(original, new HotOrderedStore(ValidTimeKey.STORE_LOWER, null, reader).chunkMask(16, 10, 0));
        }
        try (var writer = session.beginNodeTrx()) {
          final var tree =
              ValidTimeIntervalIndexFactory.createWriterTree(writer.getStorageEngineWriter(), 0, new IntervalDomain());
          tree.insert(2, 10, 20);
          final var reader = HOTIndexReader.create(writer.getStorageEngineReader(), ValidTimeKeySerializer.INSTANCE,
              IndexType.VALIDTIME, 0);
          final var store = new HotOrderedStore(ValidTimeKey.STORE_LOWER, null, reader);
          assertEquals(6, store.chunkMask(16, 10, 0).word(0));
          tree.insert(3, 10, 20);
          assertEquals(14, store.chunkMask(16, 10, 0).word(0), "writer masks must never be memoized");
          writer.commit();
        }
        try (var transaction = session.beginNodeReadOnlyTrx()) {
          final var reader = HOTIndexReader.create(transaction.getStorageEngineReader(),
              ValidTimeKeySerializer.INSTANCE, IndexType.VALIDTIME, 0);
          final PostingMask updated = new HotOrderedStore(ValidTimeKey.STORE_LOWER, null, reader).chunkMask(16, 10, 0);
          assertNotSame(original, updated);
          assertEquals(14, updated.word(0));
          assertEquals(2, original.word(0));
        }
        Databases.clearGlobalCaches();
        try (var transaction = session.beginNodeReadOnlyTrx(before)) {
          final var reader = HOTIndexReader.create(transaction.getStorageEngineReader(),
              ValidTimeKeySerializer.INSTANCE, IndexType.VALIDTIME, 0);
          final PostingMask reloaded = new HotOrderedStore(ValidTimeKey.STORE_LOWER, null, reader).chunkMask(16, 10, 0);
          assertNotSame(original, reloaded);
          assertEquals(2, reloaded.word(0));
        }
      }
    }
  }
}
