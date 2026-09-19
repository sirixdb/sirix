/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.io.StorageType;
import io.sirix.page.OverflowPage;
import io.sirix.page.PageReference;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

final class ProjectionBlobBatchReadTest {
  private static final int COUNT = 257;
  private static final long FIRST_SLOT = 5_000_000L;

  @TempDir
  Path directory;

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void mixedBlobWindowsPreserveCallerOrderWriterStateAndHistoricalRevisions(final VersioningType versioning) {
    final Path path = directory.resolve("blob-batch");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(path)));
    final long[] slots = slots();
    final byte[][] first = new byte[COUNT][];
    final byte[][] second = new byte[COUNT][];
    for (int i = 0; i < COUNT; i++) {
      first[i] = payload(i, 1);
      second[i] = i % 7 == 0
          ? payload(i, 2)
          : first[i];
    }
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource")
                                                              .storageType(StorageType.FILE_CHANNEL)
                                                              .versioningApproach(versioning)
                                                              .maxNumberOfRevisionsToRestore(2)
                                                              .build()));
      try (JsonResourceSession session = database.beginResourceSession("resource")) {
        for (int revision = 1; revision <= 2; revision++) {
          final byte[][] expected = revision == 1
              ? first
              : second;
          try (JsonNodeTrx writer = session.beginNodeTrx()) {
            final ProjectionIndexHOTStorage storage = new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0);
            for (int i = 0; i < COUNT; i++) {
              if (revision == 1 || i % 7 == 0) {
                storage.putBlob(FIRST_SLOT + 3L * i, expected[i]);
              }
            }
            assertTrue(writer.getStorageEngineWriter().hasTrxIntentLog());
            assertWindow(writer.getStorageEngineWriter(), slots, expected);
            writer.commit();
          }
        }
      }
    }
    Databases.clearGlobalCaches();
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path);
        JsonResourceSession session = database.beginResourceSession("resource")) {
      for (int revision = 1; revision <= 2; revision++) {
        try (JsonNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx(revision)) {
          final StorageEngineReader actual = trx.getStorageEngineReader();
          assertWindow(actual, slots, revision == 1
              ? first
              : second);
        }
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void eachBatchedPayloadStillRequiresItsOwnLengthAndHash(final boolean coalesce) {
    final Path path = directory.resolve("blob-integrity");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(path)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      assertTrue(database.createResource(
          ResourceConfiguration.newBuilder("resource").storageType(StorageType.FILE_CHANNEL).build()));
      try (JsonResourceSession session = database.beginResourceSession("resource")) {
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          final ProjectionIndexHOTStorage storage = new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0);
          storage.putBlob(FIRST_SLOT, payload(1, 1));
          storage.putBlob(FIRST_SLOT + 3, payload(2, 1));
          writer.commit();
        }
        try (JsonNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx()) {
          final StorageEngineReader actual = trx.getStorageEngineReader();
          for (int corrupt = 0; corrupt < 2; corrupt++) {
            final int target = corrupt;
            for (int kind = 0; kind < 3; kind++) {
              final int corruption = kind;
              final StorageEngineReader reader = mock(StorageEngineReader.class, delegatesTo(actual));
              final AtomicInteger reads = new AtomicInteger();
              final AtomicInteger batches = new AtomicInteger();
              doAnswer(invocation -> {
                final OverflowPage page = actual.readSideOverflowPage(invocation.getArgument(0));
                if (reads.getAndIncrement() != target) {
                  return page;
                }
                if (corruption == 0) {
                  final byte[] bytes = page.getDataBytes().clone();
                  bytes[bytes.length - 1] ^= 1;
                  return new OverflowPage(bytes);
                }
                return corruption == 1
                    ? new OverflowPage(new byte[1])
                    : null;
              }).when(reader).readSideOverflowPage(any(PageReference.class));
              doAnswer(invocation -> {
                final OverflowPage[] pages = actual.readSideOverflowPageBatch(invocation.getArgument(0));
                batches.incrementAndGet();
                if (corruption == 0) {
                  final byte[] bytes = pages[target].getDataBytes().clone();
                  bytes[bytes.length - 1] ^= 1;
                  pages[target] = new OverflowPage(bytes);
                } else {
                  pages[target] = corruption == 1
                      ? new OverflowPage(new byte[1])
                      : null;
                }
                return pages;
              }).when(reader).readSideOverflowPageBatch(any(long[].class));
              assertThrows(IllegalStateException.class, () -> ProjectionIndexHOTStorage.readBlobBatch(reader, 0,
                  new long[] {FIRST_SLOT, FIRST_SLOT + 3}, coalesce));
              assertEquals(coalesce
                  ? 1
                  : 0, batches.get());
            }
          }
          assertArrayEquals(payload(1, 1), ProjectionIndexHOTStorage.readBlob(actual, 0, FIRST_SLOT));
          assertArrayEquals(payload(2, 1), ProjectionIndexHOTStorage.readBlob(actual, 0, FIRST_SLOT + 3));
        }
      }
    }
  }

  @Test
  void emptyAndInvalidBatchesDoNotNavigateOrReadPages() {
    final StorageEngineReader reader = mock(StorageEngineReader.class);
    assertEquals(0, ProjectionIndexHOTStorage.readBlobBatch(reader, 0, new long[0]).length);
    assertThrows(NullPointerException.class, () -> ProjectionIndexHOTStorage.readBlobBatch(null, 0, new long[0]));
    assertThrows(NullPointerException.class, () -> ProjectionIndexHOTStorage.readBlobBatch(reader, 0, null));
    assertThrows(IllegalArgumentException.class,
        () -> ProjectionIndexHOTStorage.readBlobBatch(reader, -1, new long[0]));
    assertThrows(IllegalArgumentException.class,
        () -> ProjectionIndexHOTStorage.readBlobBatch(reader, 0, new long[1025]));
    assertThrows(IllegalArgumentException.class,
        () -> ProjectionIndexHOTStorage.readBlobBatch(reader, 0, new long[] {Long.MAX_VALUE}));
    verifyNoInteractions(reader);
  }

  private static long[] slots() {
    final long[] keys = new long[COUNT + 3];
    for (int i = 0; i < COUNT; i++) {
      keys[i] = FIRST_SLOT + 3L * (COUNT - i - 1);
    }
    keys[COUNT] = FIRST_SLOT + 1;
    keys[COUNT + 1] = keys[0];
    keys[COUNT + 2] = FIRST_SLOT + 10_000_000;
    return keys;
  }

  private static void assertWindow(final StorageEngineReader reader, final long[] slots, final byte[][] expected) {
    final byte[][] values = ProjectionIndexHOTStorage.readBlobBatch(reader, 0, slots);
    assertEquals(slots.length, values.length);
    for (int i = 0; i < slots.length; i++) {
      final long relative = slots[i] - FIRST_SLOT;
      if (relative >= 0 && relative < 3L * COUNT && relative % 3 == 0) {
        assertArrayEquals(expected[(int) (relative / 3)], values[i]);
      } else {
        assertNull(values[i]);
      }
      assertArrayEquals(ProjectionIndexHOTStorage.readBlob(reader, 0, slots[i]), values[i]);
    }
  }

  private static byte[] payload(final int index, final int revision) {
    final int length = revision == 2
        ? (index % 2 == 0
            ? 31
            : 3073)
        : index == 0
            ? 0
            : index % 3 == 0
                ? 32
                : 2048 + index;
    final byte[] value = new byte[length];
    new Random(17L * revision + index).nextBytes(value);
    return value;
  }
}
