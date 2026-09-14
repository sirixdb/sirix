package io.sirix.access.trx.page;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.DatabaseType;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.AfterCommitState;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexType;
import io.sirix.io.StorageType;
import io.sirix.node.ValueDictionaryEntryNode;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.NamePage;
import io.sirix.page.PageFragmentKeyImpl;
import io.sirix.page.PageReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import io.sirix.settings.VersioningType;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

final class WriterRecordPageCacheTest {
  @TempDir
  Path directory;

  @AfterEach
  void clearCaches() {
    Databases.clearGlobalCaches();
  }

  @Test
  void boundedCacheReusesOnlyTheExactOffsetHashAndFragmentHistoryAndDrainsAfterFailure() {
    final NodeStorageEngineReader reader = mock(NodeStorageEngineReader.class);
    when(reader.readRecordPageFromExactReference(any(PageReference.class)))
        .thenAnswer(invocation -> mock(KeyValueLeafPage.class));
    final WriterRecordPageCache cache = new WriterRecordPageCache(reader, WriterRecordPageCache.MIN_CAPACITY);
    final PageReference reference = new PageReference().setKey(100);
    final KeyValueLeafPage first = cache.get(reference);
    assertSame(first, cache.get(new PageReference().setKey(100)));
    verify(reader, times(1)).readRecordPageFromExactReference(any(PageReference.class));

    reference.setHash(42L);
    final KeyValueLeafPage changedHash = cache.get(reference);
    assertNotSame(first, changedHash);
    reference.setPageFragments(List.of(new PageFragmentKeyImpl(1, 50, 0, 0)));
    final KeyValueLeafPage changedHistory = cache.get(reference);
    assertNotSame(changedHash, changedHistory);
    assertSame(changedHistory, cache.get(reference));
    reference.setPageFragments(List.of(new PageFragmentKeyImpl(2, 50, 0, 0)));
    assertNotSame(changedHistory, cache.get(reference));

    for (int i = 0; i < WriterRecordPageCache.MIN_CAPACITY; i++) {
      cache.get(new PageReference().setKey(1000 + i));
    }
    verify(first).retire();
    verify(changedHash).retire();
    verify(changedHistory).retire();

    final KeyValueLeafPage broken = cache.get(new PageReference().setKey(1000));
    final KeyValueLeafPage survivor = cache.get(new PageReference().setKey(1007));
    doThrow(new IllegalStateException("retire failed")).when(broken).retire();
    assertThrows(IllegalStateException.class, cache::close);
    verify(survivor).retire();
    cache.close(); // References were severed even when a retirement failed.
    verify(survivor, times(1)).retire();
  }

  @Test
  void absentAndPresentZeroChecksumsRequireSeparateReads() {
    final NodeStorageEngineReader reader = mock(NodeStorageEngineReader.class);
    when(reader.readRecordPageFromExactReference(any(PageReference.class)))
        .thenAnswer(invocation -> mock(KeyValueLeafPage.class));
    final PageReference reference = new PageReference().setKey(100);
    try (WriterRecordPageCache cache = new WriterRecordPageCache(reader, WriterRecordPageCache.MIN_CAPACITY)) {
      final KeyValueLeafPage unchecked = cache.get(reference);
      reference.setHash(0L);
      final KeyValueLeafPage checked = cache.get(reference);
      assertNotSame(unchecked, checked, "a checksum requirement must not reuse an unchecked page");
      assertSame(checked, cache.get(reference));
      verify(unchecked).retire();

      reference.clearHash();
      final KeyValueLeafPage uncheckedAgain = cache.get(reference);
      assertNotSame(checked, uncheckedAgain);
      assertSame(uncheckedAgain, cache.get(reference));
      verify(checked).retire();
      verify(reader, times(3)).readRecordPageFromExactReference(any(PageReference.class));
    }
  }

  @Test
  void capacityFollowsTheArenaBudgetAndHasAFixedCeiling() {
    assertEquals(8, WriterRecordPageCache.capacityForBudget(0));
    assertEquals(8, WriterRecordPageCache.capacityForBudget(64L << 20));
    assertEquals(8, WriterRecordPageCache.capacityForBudget(96L << 20));
    assertEquals(16, WriterRecordPageCache.capacityForBudget(128L << 20));
    assertEquals(64, WriterRecordPageCache.capacityForBudget(512L << 20));
    assertEquals(256, WriterRecordPageCache.capacityForBudget(2L << 30));
    assertEquals(256, WriterRecordPageCache.capacityForBudget(Long.MAX_VALUE));
    assertThrows(IllegalArgumentException.class, () -> WriterRecordPageCache.capacityForBudget(-1));
    final NodeStorageEngineReader reader = mock(NodeStorageEngineReader.class);
    for (final int invalid : new int[] {0, 4, 9, 255, 512}) {
      assertThrows(IllegalArgumentException.class, () -> new WriterRecordPageCache(reader, invalid));
    }
  }

  @Test
  void primitiveIndexRetainsTheWorkingSetAndRemovesFailedReplacements() {
    final NodeStorageEngineReader reader = mock(NodeStorageEngineReader.class);
    when(reader.readRecordPageFromExactReference(any(PageReference.class)))
        .thenAnswer(invocation -> mock(KeyValueLeafPage.class));
    final int capacity = WriterRecordPageCache.MAX_CAPACITY;
    try (WriterRecordPageCache cache = new WriterRecordPageCache(reader, capacity)) {
      final KeyValueLeafPage[] pages = new KeyValueLeafPage[capacity];
      for (int i = 0; i < capacity; i++) {
        pages[i] = cache.get(new PageReference().setKey((long) i << 32 | i));
      }
      for (int i = capacity - 1; i >= 0; i--) {
        assertSame(pages[i], cache.get(new PageReference().setKey((long) i << 32 | i)));
      }
      verify(reader, times(capacity)).readRecordPageFromExactReference(any(PageReference.class));
      final PageReference replacement = new PageReference().setKey(123456789);
      when(reader.readRecordPageFromExactReference(replacement)).thenThrow(new IllegalStateException("read failed"));
      assertThrows(IllegalStateException.class, () -> cache.get(replacement));
      verify(pages[0]).retire();
      assertNotSame(pages[0], cache.get(new PageReference().setKey(0)), "failed replacement left no stale slot");
      assertSame(pages[capacity - 1], cache.get(new PageReference().setKey((long) (capacity - 1) << 32 | capacity - 1)));
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void asyncReadbackSeesCurrentWritesAndPreservesCommittedRevisions(final VersioningType versioning) {
    final Path path = directory.resolve("dictionary");
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      database.createResource(ResourceConfiguration.newBuilder("resource").versioningApproach(versioning)
          .maxNumberOfRevisionsToRestore(3).storageType(StorageType.FILE_CHANNEL).storeDiffs(false).build());
      try (JsonResourceSession session = database.beginResourceSession("resource");
          JsonNodeTrx trx = session.beginNodeTrx(Integer.MAX_VALUE, AfterCommitState.KEEP_OPEN_ASYNC_FLUSH)) {
        trx.insertObjectAsFirstChild();
        final NodeStorageEngineWriter writer = (NodeStorageEngineWriter) trx.getStorageEngineWriter();
        put(writer, 1, "original");
        put(writer, 2, "neighbor");
        writer.asyncFlush();
        writer.awaitPendingAsyncFlush();
        assertEquals("original", read(writer, 1));
        assertEquals("neighbor", read(writer, 2));
        assertNotNull(writer.cachedRecordReadPagesForTesting(), "exercise the durable readback route");

        put(writer, 1, "updated");
        assertEquals("updated", read(writer, 1), "the mutable transaction must win over cached durable content");
        assertEquals("neighbor", read(writer, 2));
        writer.asyncFlush();
        writer.awaitPendingAsyncFlush();
        assertEquals("updated", read(writer, 1));
        assertEquals("neighbor", read(writer, 2));
        trx.commit();
        assertNull(writer.cachedRecordReadPagesForTesting(), "commit releases writer-owned native pages");
      }
      try (JsonResourceSession session = database.beginResourceSession("resource");
          JsonNodeTrx trx = session.beginNodeTrx()) {
        final NodeStorageEngineWriter writer = (NodeStorageEngineWriter) trx.getStorageEngineWriter();
        put(writer, 2, "revision-two");
        writer.asyncFlush();
        writer.awaitPendingAsyncFlush();
        assertEquals("updated", read(writer, 1));
        assertEquals("revision-two", read(writer, 2));
        trx.commit();
      }
      try (JsonResourceSession session = database.beginResourceSession("resource");
          JsonNodeTrx trx = session.beginNodeTrx()) {
        final NodeStorageEngineWriter writer = (NodeStorageEngineWriter) trx.getStorageEngineWriter();
        put(writer, 1, "rolled-back");
        writer.asyncFlush();
        writer.awaitPendingAsyncFlush();
        assertEquals("rolled-back", read(writer, 1));
        assertNotNull(writer.cachedRecordReadPagesForTesting());
        trx.rollback();
        assertNull(writer.cachedRecordReadPagesForTesting(), "rollback releases pages before offsets can be reused");
      }
    }
    Databases.clearGlobalCaches();
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path);
        JsonResourceSession session = database.beginResourceSession("resource")) {
      for (int revision = 1; revision <= 2; revision++) {
        try (JsonNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx(revision)) {
          assertEquals("updated", read(trx.getStorageEngineReader(), 1));
          assertEquals(revision == 1 ? "neighbor" : "revision-two", read(trx.getStorageEngineReader(), 2));
        }
      }
    }
  }

  private static void put(final NodeStorageEngineWriter writer, final long key, final String value) {
    final NamePage names = writer.getNamePage(writer.getActualRevisionRootPage());
    names.putProjectionValueDictionaryRecord(new ValueDictionaryEntryNode(key, value.getBytes(StandardCharsets.UTF_8)),
        DatabaseType.JSON, writer, writer.getLog());
  }

  private static String read(final StorageEngineReader reader, final long key) {
    final ValueDictionaryEntryNode node =
        reader.getRecord(key, IndexType.NAME, NamePage.projectionValueDictionaryOffset(DatabaseType.JSON));
    assertNotNull(node);
    return new String(node.getValue(), StandardCharsets.UTF_8);
  }
}
