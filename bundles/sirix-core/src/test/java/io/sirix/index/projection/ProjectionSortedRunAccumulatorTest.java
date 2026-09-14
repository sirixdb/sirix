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
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ProjectionSortedRunAccumulatorTest {

  @TempDir
  Path temporaryDirectory;

  @Test
  void packedBlocksSortAndPersistWithoutPerRowKeyObjects() {
    final int rows = 2_048;
    final int[] order = new int[rows];
    for (int i = 0; i < rows; i++) {
      order[i] = i;
    }
    final Random random = new Random(0x5A71L);
    for (int i = rows - 1; i > 0; i--) {
      final int other = random.nextInt(i + 1);
      final int value = order[i];
      order[i] = order[other];
      order[other] = value;
    }
    final ProjectionSortedRunAccumulator run = new ProjectionSortedRunAccumulator();
    final byte[] scratch = new byte[96];
    for (final int value : order) {
      encode(value, scratch);
      run.append(scratch, scratch.length);
    }
    assertEquals(rows, run.rowCount());
    assertThrows(IllegalArgumentException.class, () -> run.append(new byte[0], 0x1_0000));
    final Path databasePath = temporaryDirectory.resolve("packed-sort-run");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource").build()));
      try (JsonResourceSession session = database.beginResourceSession("resource")) {
        final int revision;
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          assertEquals(rows, run.persist(new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0)));
          revision = writer.getRevisionNumber();
          writer.commit();
        }
        try (JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx(revision)) {
          final ProjectionSortedDirectory.Accessor directory =
              ProjectionSortedDirectory.open(reader.getStorageEngineReader(), 0);
          assertNotNull(directory);
          final ProjectionSortedDirectory.Accessor.Cursor cursor = directory.first();
          final byte[] expected = new byte[scratch.length];
          for (int i = 0; i < rows; i++) {
            assertTrue(cursor.isValid());
            encode(i, expected);
            assertArrayEquals(expected, cursor.copyKey());
            assertEquals(i < rows - 1, cursor.advance());
          }
          assertFalse(cursor.isValid());
        }
      }
    }
    run.release();
    assertEquals(0, run.rowCount());
  }

  @Test
  void groupedScanDecodesEscapedStringsAndDeclinesUnstableTies() {
    final ProjectionSortedRunAccumulator run = new ProjectionSortedRunAccumulator();
    addGroupRow(run, "a\u0000b", 1_000, 1);
    addGroupRow(run, "a\u0000b", 5_000, 2);
    addGroupRow(run, "b", 2_000, 3);
    addGroupRow(run, "b", 9_000, 4);
    addGroupRow(run, "c", 3_000, 5);
    addGroupRow(run, "c", 4_000, 6);
    addGroupRow(run, "d", 2_000, 7);
    addGroupRow(run, "d", 9_000, 8);
    final Path databasePath = temporaryDirectory.resolve("sorted-groups");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource").build()));
      try (JsonResourceSession session = database.beginResourceSession("resource")) {
        final int revision;
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          assertEquals(8, run.persist(new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0)));
          revision = writer.getRevisionNumber();
          writer.commit();
        }
        try (JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx(revision)) {
          assertEquals(List.of(new ProjectionSortedGroupScan.Group("a\u0000b", 1_000, 5_000)),
              ProjectionSortedGroupScan.topK(reader.getStorageEngineReader(), 0, 1,
                  ProjectionSortedGroupScan.Order.MIN_ASC, 1));
          assertEquals(List.of(new ProjectionSortedGroupScan.Group("a\u0000b", 1_000, 1_000)),
              ProjectionSortedGroupScan.topK(reader.getStorageEngineReader(), 0, 1,
                  ProjectionSortedGroupScan.Order.MIN_ASC, 1, true));
          assertNull(ProjectionSortedGroupScan.topK(reader.getStorageEngineReader(), 0, 1,
              ProjectionSortedGroupScan.Order.SPAN_DESC, 1_000));
        }
      }
    }
  }

  private static void addGroupRow(final ProjectionSortedRunAccumulator run, final String group,
      final long value, final long recordKey) {
    final ProjectionSortKeyCodec.Writer writer = new ProjectionSortKeyCodec.Writer();
    final byte[] utf8 = group.getBytes(StandardCharsets.UTF_8);
    writer.appendUtf8(utf8, 0, utf8.length);
    writer.appendLong(value);
    writer.appendRecordKey(recordKey);
    run.append(writer.bytesRef(), writer.length());
  }

  private static void encode(final int value, final byte[] target) {
    target[0] = (byte) (value >>> 24);
    target[1] = (byte) (value >>> 16);
    target[2] = (byte) (value >>> 8);
    target[3] = (byte) value;
    for (int i = 4; i < target.length; i++) {
      target[i] = (byte) (i * 17);
    }
  }
}
