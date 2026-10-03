/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.Database;
import io.sirix.api.HOTReadIntent;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.io.StorageType;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Point reads agree with full reconstruction through rotations, tombstones and inline transitions.
 */
final class ProjectionBlobHistoryReadTest {
  private static final int SLOTS = 48;
  private static final long FIRST = 5_000_000L;

  @TempDir
  Path directory;

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void everyCommittedAndUncommittedValueSurvivesTwoWindows(final VersioningType versioning) {
    final Path path = directory.resolve(versioning.name());
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(path)));
    final List<byte[][]> history = new ArrayList<>();
    byte[][] expected = new byte[SLOTS][];
    for (int i = 0; i < SLOTS; i++)
      expected[i] = payload(i, 0);
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource")
                                                              .storageType(StorageType.FILE_CHANNEL)
                                                              .versioningApproach(versioning)
                                                              .maxNumberOfRevisionsToRestore(32)
                                                              .storeDiffs(false)
                                                              .storeNodeHistory(false)
                                                              .buildPathSummary(false)
                                                              .hashKind(HashType.NONE)
                                                              .build()));
      try (JsonResourceSession session = database.beginResourceSession("resource")) {
        for (int revision = 1; revision <= 70; revision++) {
          expected = expected.clone();
          try (JsonNodeTrx writer = session.beginNodeTrx()) {
            final ProjectionIndexHOTStorage storage = new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0);
            if (revision == 1) {
              for (int i = 0; i < SLOTS; i++)
                storage.putBlob(FIRST + i, expected[i]);
            } else {
              final int edited = revision % 12;
              expected[edited] = revision % 5 == 0
                  ? null
                  : payload(edited, revision);
              if (expected[edited] == null)
                storage.tombstoneBlob(FIRST + edited);
              else
                storage.putBlob(FIRST + edited, expected[edited]);
              assertArrayEquals(expected[edited],
                  ProjectionIndexHOTStorage.readBlob(writer.getStorageEngineWriter(), 0, FIRST + edited));
            }
            writer.commit();
          }
          history.add(expected);
        }
      }
    }
    for (int revision = 1; revision <= history.size(); revision++) {
      Databases.clearGlobalCaches();
      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path);
          JsonResourceSession session = database.beginResourceSession("resource");
          JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx(revision)) {
        final byte[][] snapshot = history.get(revision - 1);
        final long[] slots = new long[SLOTS];
        for (int i = 0; i < SLOTS; i++) {
          slots[i] = FIRST + i;
          assertArrayEquals(snapshot[i],
              ProjectionIndexHOTStorage.readBlob(reader.getStorageEngineReader(), 0, slots[i], HOTReadIntent.POINT),
              "revision " + revision + ", slot " + i);
        }
        // Batched marker capture reconstructs full leaves; a preceding point lookup must not
        // have cached a partial image that makes unrelated slots appear absent.
        final byte[][] complete =
            ProjectionIndexHOTStorage.readBlobBatch(reader.getStorageEngineReader(), 0, slots, true);
        for (int i = 0; i < SLOTS; i++)
          assertArrayEquals(snapshot[i], complete[i]);
        assertNull(
            ProjectionIndexHOTStorage.readBlob(reader.getStorageEngineReader(), 0, FIRST + SLOTS, HOTReadIntent.POINT));
      }
    }
  }

  private static byte[] payload(final int slot, final int revision) {
    final byte[] value = new byte[(slot + revision) % 3 == 0
        ? 5_000
        : 24];
    new Random(31L * revision + slot).nextBytes(value);
    return value;
  }
}
