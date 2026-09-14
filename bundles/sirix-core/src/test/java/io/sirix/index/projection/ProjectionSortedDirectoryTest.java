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
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ProjectionSortedDirectoryTest {

  @TempDir
  Path temporaryDirectory;

  @Test
  void persistedTwoLevelDirectoryRoutesExactAndBetweenFenceKeys() {
    final Path databasePath = temporaryDirectory.resolve("sorted-projection-directory");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource").build()));
      try (JsonResourceSession session = database.beginResourceSession("resource")) {
        final int revision;
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          final ProjectionIndexHOTStorage storage = new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0);
          final ProjectionSortedDirectory.Builder directory = new ProjectionSortedDirectory.Builder(storage);
          for (int i = 0; i < 300; i++) {
            final ProjectionSortedLeaf leaf = ProjectionSortedLeaf.encode(
                new byte[][] {key(i * 2)}, new byte[][] {key(i)}, 1);
            assertNotNull(leaf);
            assertEquals(i + 1, directory.append(leaf));
          }
          assertNull(storage.getBlob(ProjectionSortedDirectory.HEADER_SLOT));
          directory.finish();
          assertNotNull(storage.getBlob(ProjectionSortedDirectory.HEADER_SLOT));
          revision = writer.getRevisionNumber();
          writer.commit();
        }
        try (JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx(revision)) {
          final ProjectionSortedDirectory.Accessor directory =
              ProjectionSortedDirectory.open(reader.getStorageEngineReader(), 0);
          assertNotNull(directory);
          assertEquals(300, directory.dataLeafCount());
          assertEquals(1, directory.findLeafId(new byte[0]));
          assertEquals(300, directory.findLeafId(new byte[] {(byte) 0xFF}));
          for (int i = 0; i < 300; i++) {
            assertEquals(i + 1, directory.findLeafId(key(i * 2)));
            assertEquals(i + 1, directory.findLeafId(key(i * 2 + 1)));
          }
          final ProjectionSortedLeaf middle = ProjectionSortedLeafStore.read(reader.getStorageEngineReader(), 0, 151);
          assertNotNull(middle);
          assertArrayEquals(key(300), middle.copyKey(0));
          final ProjectionSortedDirectory.Accessor.Cursor cursor = directory.first();
          final byte[] payload = new byte[Integer.BYTES];
          for (int i = 0; i < 300; i++) {
            assertTrue(cursor.isValid());
            assertEquals(i + 1, cursor.leafId());
            assertEquals(0, cursor.row());
            assertArrayEquals(key(i * 2), cursor.copyKey());
            assertEquals(Integer.BYTES, cursor.payloadLength());
            cursor.copyPayloadTo(payload, 0);
            assertArrayEquals(key(i), payload);
            assertEquals(i < 299, cursor.advance());
          }
          assertFalse(cursor.isValid());
          assertFalse(cursor.advance());
          assertThrows(IllegalStateException.class, cursor::copyKey);

          final ProjectionSortedDirectory.Accessor.Cursor skipped = directory.first();
          assertTrue(skipped.skipPrefix(key(0), Integer.BYTES));
          assertArrayEquals(key(2), skipped.copyKey());
          assertFalse(skipped.skipPrefix(new byte[] {0}, 1));
          assertFalse(skipped.isValid());

          final ProjectionSortedDirectory.Accessor.Cursor captured = directory.first();
          assertTrue(captured.skipPrefixCapturingLast(key(0), Integer.BYTES));
          final byte[] lastSkipped = new byte[Integer.BYTES];
          assertEquals(Integer.BYTES, captured.lastSkippedKeyLength());
          captured.copyLastSkippedKeyTo(lastSkipped);
          assertArrayEquals(key(0), lastSkipped);
          assertArrayEquals(key(2), captured.copyKey());
          assertFalse(captured.skipPrefixCapturingLast(new byte[] {0}, 1));
          captured.copyLastSkippedKeyTo(lastSkipped);
          assertArrayEquals(key(598), lastSkipped);
          assertFalse(captured.isValid());

          final ProjectionSortedDirectory.Accessor.Cursor between = directory.seek(key(301));
          assertTrue(between.isValid());
          assertArrayEquals(key(302), between.copyKey());
          assertEquals(152, between.leafId());
          assertFalse(directory.seek(key(601)).isValid());
        }
      }
    }
  }

  @Test
  void prefixJumpsPreserveBoundariesInsideLeavesAndAcrossDirectoryLevels() {
    final Path databasePath = temporaryDirectory.resolve("sorted-prefix-boundaries");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource").build()));
      try (JsonResourceSession session = database.beginResourceSession("resource")) {
        final int before;
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          final ProjectionIndexHOTStorage storage = new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0);
          final ProjectionSortedDirectory.Builder builder = new ProjectionSortedDirectory.Builder(storage);
          for (int i = 0; i < 300; i++) {
            final ProjectionSortedLeaf leaf = ProjectionSortedLeaf.encode(
                new byte[][] {key(3 * i), key(3 * i + 1), key(3 * i + 2)}, null, 3);
            assertNotNull(leaf);
            builder.append(leaf);
          }
          builder.finish();
          before = writer.getRevisionNumber();
          writer.commit();
        }
        final int after;
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          final ProjectionSortedDirectory.Editor editor = new ProjectionSortedDirectory.Editor(
              new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0));
          editor.remove(key(511));
          editor.remove(key(512));
          after = writer.getRevisionNumber();
          writer.commit();
        }
        for (final int revision : new int[] {before, after}) {
          try (JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx(revision)) {
            final ProjectionSortedDirectory.Accessor directory =
                ProjectionSortedDirectory.open(reader.getStorageEngineReader(), 0);
            assertNotNull(directory);
            for (final int start : new int[] {0, 17, 255, 256, 257, 510, 513, 767, 768, 899}) {
              for (int length = 0; length <= Integer.BYTES; length++) {
                final byte[] prefix = key(start);
                int last = start;
                int next = start + 1;
                while (next < 900) {
                  if (revision == after && (next == 511 || next == 512)) {
                    next++;
                    continue;
                  }
                  if (!Arrays.equals(prefix, 0, length, key(next), 0, length)) {
                    break;
                  }
                  last = next++;
                }
                final ProjectionSortedDirectory.Accessor.Cursor captured = directory.seek(prefix);
                assertEquals(next < 900, captured.skipPrefixCapturingLast(prefix, length));
                final byte[] maximum = new byte[captured.lastSkippedKeyLength()];
                captured.copyLastSkippedKeyTo(maximum);
                assertArrayEquals(key(last), maximum);
                final ProjectionSortedDirectory.Accessor.Cursor skipped = directory.seek(prefix);
                assertEquals(next < 900, skipped.skipPrefix(prefix, length));
                if (next < 900) {
                  assertArrayEquals(key(next), captured.copyKey());
                  assertArrayEquals(key(next), skipped.copyKey());
                }
              }
            }
          }
        }
      }
    }
  }

  private static byte[] key(final int value) {
    return new byte[] {(byte) (value >>> 24), (byte) (value >>> 16), (byte) (value >>> 8), (byte) value};
  }
}
