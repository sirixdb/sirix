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
import io.sirix.cache.ProjectionBlobCache;
import io.sirix.io.StorageType;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Retained committed payloads must never shadow writer state or a reused revision after truncation.
 */
@Isolated
final class ProjectionSortedCacheLifecycleTest {
  @TempDir
  Path temporary;

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void rollbackTruncationRevisionReuseAndCloseFenceRetainedPayloads(final VersioningType versioning) {
    Databases.clearGlobalCaches();
    final ProjectionBlobCache cache = Databases.getGlobalBufferManager().getProjectionBlobCache();
    final long empty = cache.getCurrentWeightBytes();
    final Path path = temporary.resolve(versioning.name());
    final byte[][] keys = new byte[256][];
    for (int row = 0; row < keys.length; row++) {
      keys[row] = SortedScanFixtures.key(new SortedScanFixtures.Row("group", row, row + 1));
    }
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(path)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource")
                                                              .storageType(StorageType.FILE_CHANNEL)
                                                              .versioningApproach(versioning)
                                                              .maxNumberOfRevisionsToRestore(32)
                                                              .build()));
      try (JsonResourceSession session = database.beginResourceSession("resource")) {
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          final byte[][] values = new byte[keys.length][];
          Arrays.fill(values, payload(1));
          final ProjectionSortedDirectory.Builder builder = new ProjectionSortedDirectory.Builder(
              new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0), SortedScanFixtures.GROUP_VALUE);
          builder.append(Objects.requireNonNull(ProjectionSortedLeaf.encode(keys, values, keys.length)));
          builder.finish();
          writer.commit();
        }
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          replace(writer, keys[0], 2);
          writer.commit();
        }
        try (JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx(2)) {
          for (int warm = 0; warm < 12; warm++) {
            assertPayload(reader.getStorageEngineReader(), keys[0], 2);
          }
          assertTrue(cache.getCurrentWeightBytes() > empty, "the real read path must retain its verified payload");
        }
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          replace(writer, keys[0], 9);
          assertPayload(writer.getStorageEngineReader(), keys[0], 9);
          writer.rollback();
        }
        try (JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx(2)) {
          assertPayload(reader.getStorageEngineReader(), keys[0], 2);
        }
        final long generation = cache.generation();
        try (final var writer = session.createStorageEngineWriter()) {
          writer.truncateTo(1);
        }
        assertTrue(cache.generation() > generation, "truncation must fence in-flight admissions");
        assertEquals(empty, cache.getCurrentWeightBytes(), "truncation retires the old routing identities");
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          replace(writer, keys[0], 3);
          writer.commit();
        }
        assertEquals(2, session.getMostRecentRevisionNumber());
        try (JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx(2)) {
          assertPayload(reader.getStorageEngineReader(), keys[0], 3);
        }
        try (JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx(1)) {
          assertPayload(reader.getStorageEngineReader(), keys[0], 1);
        }
        assertTrue(cache.getCurrentWeightBytes() > empty);
      }
    }
    // The buffer manager is global: closing one session must not evict another session's entries.
    assertTrue(cache.getCurrentWeightBytes() <= cache.getMaxWeightBytes());
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path);
        JsonResourceSession session = database.beginResourceSession("resource");
        JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx()) {
      assertPayload(reader.getStorageEngineReader(), keys[0], 3);
    }
    Databases.clearGlobalCaches();
    assertEquals(empty, cache.getCurrentWeightBytes(), "the owning buffer manager releases retained payloads");
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path);
        JsonResourceSession session = database.beginResourceSession("resource");
        JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx()) {
      assertPayload(reader.getStorageEngineReader(), keys[0], 3);
    }
  }

  private static void replace(final JsonNodeTrx writer, final byte[] key, final int value) {
    final ProjectionSortedDirectory.Editor editor =
        new ProjectionSortedDirectory.Editor(new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0));
    editor.apply(new byte[][] {key}, 1, new byte[][] {key}, new byte[][] {payload(value)}, 1);
  }

  private static byte[] payload(final int value) {
    final byte[] bytes = new byte[64];
    Arrays.fill(bytes, (byte) value);
    return bytes;
  }

  private static void assertPayload(final StorageEngineReader reader, final byte[] key, final int value) {
    final ProjectionSortedDirectory.Accessor sorted = ProjectionSortedDirectory.open(reader, 0);
    assertNotNull(sorted);
    final ProjectionSortedDirectory.Accessor.Cursor cursor = sorted.seek(key);
    assertTrue(cursor.isValid());
    assertArrayEquals(key, cursor.copyKey());
    final byte[] bytes = new byte[cursor.payloadLength()];
    cursor.copyPayloadTo(bytes, 0);
    assertArrayEquals(payload(value), bytes);
  }
}
