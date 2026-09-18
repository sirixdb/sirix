/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ProjectionFlagSummaryChunksTest {
  private static final String RESOURCE = "resource";
  private static final int INDEX_NUMBER = 0;

  @TempDir
  Path temporaryDirectory;

  @Test
  void changedLeafRewritesOnlyItsChunkAndOldRevisionKeepsItsEvidence() {
    final Path databasePath = temporaryDirectory.resolve("flag-summary-versioning");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder(RESOURCE).build()));
      try (JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
        final int firstRevision;
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          final ProjectionIndexHOTStorage storage =
              new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), INDEX_NUMBER);
          final ProjectionIndexFences.BuildWriter fences = new ProjectionIndexFences.BuildWriter();
          final ProjectionFlagSummaryChunks.BuildWriter summaries = new ProjectionFlagSummaryChunks.BuildWriter();
          for (int slot = 1; slot <= ProjectionFlagSummaryChunks.CHUNK_LEAVES + 1; slot++) {
            final long key = 1000L + slot;
            final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded = encoded(key, false);
            storage.putRowGroupAsColumnSegmentSlots(slot, encoded);
            fences.append(storage, key, key);
            summaries.append(storage, encoded.descriptor());
          }
          firstRevision = writer.getRevisionNumber();
          fences.finish(storage);
          summaries.finish(storage, ProjectionFlagSummaryChunks.CHUNK_LEAVES + 1, 1, firstRevision);
          writer.commit();
        }

        final byte[] firstChunk;
        try (JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx(firstRevision)) {
          assertArrayEquals(new byte[] {0}, ProjectionFlagSummaryChunks.readAll(reader.getStorageEngineReader(),
              INDEX_NUMBER, ProjectionFlagSummaryChunks.CHUNK_LEAVES + 1, 1, firstRevision));
          firstChunk = ProjectionIndexHOTStorage.readBlob(reader.getStorageEngineReader(), INDEX_NUMBER,
              ProjectionFlagSummaryChunks.CHUNK_SLOT_BASE);
        }

        final int secondRevision;
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          final ProjectionIndexHOTStorage storage =
              new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), INDEX_NUMBER);
          final int slot = ProjectionFlagSummaryChunks.CHUNK_LEAVES + 1;
          final ProjectionIndexFences.Accessor fences = ProjectionIndexFences.open(storage, slot);
          assertTrue(storage.putRowGroupAsColumnSegmentSlots(slot, encoded(1000L + slot, true)));
          secondRevision = writer.getRevisionNumber();
          final LongOpenHashSet changed = new LongOpenHashSet();
          changed.add(slot);
          ProjectionFlagSummaryChunks.rewriteTouched(storage, fences, changed, 1, slot, firstRevision,
              secondRevision);
          assertArrayEquals(firstChunk, storage.getBlob(ProjectionFlagSummaryChunks.CHUNK_SLOT_BASE));
          writer.commit();
        }

        try (JsonNodeReadOnlyTrx oldReader = session.beginNodeReadOnlyTrx(firstRevision);
            JsonNodeReadOnlyTrx newReader = session.beginNodeReadOnlyTrx(secondRevision)) {
          assertArrayEquals(new byte[] {0}, ProjectionFlagSummaryChunks.readAll(oldReader.getStorageEngineReader(),
              INDEX_NUMBER, ProjectionFlagSummaryChunks.CHUNK_LEAVES + 1, 1, firstRevision));
          assertArrayEquals(new byte[] {2}, ProjectionFlagSummaryChunks.readAll(newReader.getStorageEngineReader(),
              INDEX_NUMBER, ProjectionFlagSummaryChunks.CHUNK_LEAVES + 1, 1, secondRevision));
          assertArrayEquals(firstChunk, ProjectionIndexHOTStorage.readBlob(newReader.getStorageEngineReader(),
              INDEX_NUMBER, ProjectionFlagSummaryChunks.CHUNK_SLOT_BASE));
          assertNull(ProjectionFlagSummaryChunks.readAll(newReader.getStorageEngineReader(), INDEX_NUMBER,
              ProjectionFlagSummaryChunks.CHUNK_LEAVES + 1, 1, firstRevision),
              "a summary for a different revision cannot certify this snapshot");
        }
      }
    }
  }

  @Test
  void malformedChunkAndMissingHeaderDeclineEvidence() {
    final Path databasePath = temporaryDirectory.resolve("flag-summary-corruption");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder(RESOURCE).build()));
      try (JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
        try (JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx()) {
          assertNull(ProjectionFlagSummaryChunks.readAll(reader.getStorageEngineReader(), INDEX_NUMBER, 1, 1,
              reader.getRevisionNumber()));
        }
        final int revision;
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          final ProjectionIndexHOTStorage storage =
              new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), INDEX_NUMBER);
          final ProjectionFlagSummaryChunks.BuildWriter summaries = new ProjectionFlagSummaryChunks.BuildWriter();
          summaries.append(storage, encoded(1001L, false).descriptor());
          revision = writer.getRevisionNumber();
          summaries.finish(storage, 1, 1, revision);
          storage.putBlob(ProjectionFlagSummaryChunks.CHUNK_SLOT_BASE, new byte[] {1, 2, 3});
          writer.commit();
        }
        try (JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx(revision)) {
          assertNull(ProjectionFlagSummaryChunks.readAll(reader.getStorageEngineReader(), INDEX_NUMBER, 1, 1,
              revision));
        }
      }
    }
  }

  private static ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded(final long key,
      final boolean nonIntegral) {
    final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(
        new byte[] {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG});
    assertTrue(page.appendRow(key, new long[] {17L}, new boolean[] {false}, new String[] {null},
        new boolean[] {true}, new boolean[] {false}, new boolean[] {nonIntegral}));
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded =
        ProjectionIndexColumnSegmentCodec.encode(page.serialize());
    assertFalse(encoded.descriptor().length == 0);
    return encoded;
  }
}
