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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ProjectionSortedLeafStoreTest {

  @TempDir
  Path temporaryDirectory;

  @Test
  void replacingOneLeafPreservesOldRevisionAndUntouchedNeighbour() {
    final Path databasePath = temporaryDirectory.resolve("sorted-projection-leaves");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource").build()));
      try (JsonResourceSession session = database.beginResourceSession("resource")) {
        final ProjectionSortedLeaf first = leaf((byte) 1);
        final ProjectionSortedLeaf second = leaf((byte) 2);
        final int oldRevision;
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          final ProjectionIndexHOTStorage storage = new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0);
          ProjectionSortedLeafStore.write(storage, 1, first);
          ProjectionSortedLeafStore.write(storage, 2, second);
          oldRevision = writer.getRevisionNumber();
          writer.commit();
        }
        final int newRevision;
        final ProjectionSortedLeaf inserted = first.withInserted(new byte[] {3}, new byte[] {9});
        assertNotNull(inserted);
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          final ProjectionIndexHOTStorage storage = new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0);
          ProjectionSortedLeafStore.write(storage, 1, inserted);
          newRevision = writer.getRevisionNumber();
          writer.commit();
        }
        try (JsonNodeReadOnlyTrx oldReader = session.beginNodeReadOnlyTrx(oldRevision);
            JsonNodeReadOnlyTrx newReader = session.beginNodeReadOnlyTrx(newRevision)) {
          final ProjectionSortedLeaf oldFirst = ProjectionSortedLeafStore.read(oldReader.getStorageEngineReader(), 0, 1);
          final ProjectionSortedLeaf newFirst = ProjectionSortedLeafStore.read(newReader.getStorageEngineReader(), 0, 1);
          final ProjectionSortedLeaf oldSecond = ProjectionSortedLeafStore.read(oldReader.getStorageEngineReader(), 0, 2);
          final ProjectionSortedLeaf newSecond = ProjectionSortedLeafStore.read(newReader.getStorageEngineReader(), 0, 2);
          assertNotNull(oldFirst);
          assertNotNull(newFirst);
          assertNotNull(oldSecond);
          assertNotNull(newSecond);
          assertArrayEquals(first.encodedBytes(), oldFirst.encodedBytes());
          assertArrayEquals(inserted.encodedBytes(), newFirst.encodedBytes());
          assertArrayEquals(second.encodedBytes(), oldSecond.encodedBytes());
          assertArrayEquals(second.encodedBytes(), newSecond.encodedBytes());
        }
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          final ProjectionIndexHOTStorage storage = new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0);
          ProjectionSortedLeafStore.remove(storage, 1);
          writer.commit();
        }
        try (JsonNodeReadOnlyTrx oldReader = session.beginNodeReadOnlyTrx(newRevision);
            JsonNodeReadOnlyTrx latest = session.beginNodeReadOnlyTrx()) {
          assertNotNull(ProjectionSortedLeafStore.read(oldReader.getStorageEngineReader(), 0, 1));
          assertNull(ProjectionSortedLeafStore.read(latest.getStorageEngineReader(), 0, 1));
          assertNotNull(ProjectionSortedLeafStore.read(latest.getStorageEngineReader(), 0, 2));
        }
      }
    }
    assertThrows(NullPointerException.class, () -> ProjectionSortedLeafStore.read(
        (ProjectionIndexHOTStorage) null, 0));
  }

  private static ProjectionSortedLeaf leaf(final byte value) {
    final ProjectionSortedLeaf leaf = ProjectionSortedLeaf.encode(new byte[][] {{value}}, null, 1);
    assertNotNull(leaf);
    return leaf;
  }
}
