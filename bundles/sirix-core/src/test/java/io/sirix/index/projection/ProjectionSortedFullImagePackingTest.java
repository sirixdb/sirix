/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.projection.ProjectionSortedGroupScan.Order;
import io.sirix.io.PageHasher;
import io.sirix.io.StorageType;
import io.sirix.io.bytepipe.ByteHandlerPipeline;
import io.sirix.page.PageReference;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Complete streaming images and every historical revision have independently checked contents. */
final class ProjectionSortedFullImagePackingTest {
  private static final int GROUPS = 64;
  private static final int ROWS_PER_GROUP = 512;
  private static final ProjectionSortKeyCodec.Layout LAYOUT = new ProjectionSortKeyCodec.Layout(
      new byte[] {ProjectionSortKeyCodec.FIELD_STRING, ProjectionSortKeyCodec.FIELD_LONG});

  @TempDir
  Path directory;

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void streamingImagesArePackedAndEveryHistoricalRevisionIsExact(final VersioningType type) throws IOException {
    for (final int window : new int[] {3, 32}) {
      final Path path = directory.resolve(type.name() + window);
      final TreeMap<byte[], byte[]> expected = new TreeMap<>(Arrays::compareUnsigned);
      for (int group = 0; group < GROUPS; group++) {
        for (int row = 0; row < ROWS_PER_GROUP; row++) {
          expected.put(key(group, row, false), payload(1L + group * ROWS_PER_GROUP + row));
        }
      }
      assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(path)));
      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
        assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource")
                                                                .storageType(StorageType.FILE_CHANNEL)
                                                                .versioningApproach(type)
                                                                .maxNumberOfRevisionsToRestore(window)
                                                                .byteHandlerPipeline(new ByteHandlerPipeline())
                                                                .useTextCompression(false)
                                                                .useDeweyIDs(false)
                                                                .storeDiffs(false)
                                                                .storeNodeHistory(false)
                                                                .buildPathSummary(false)
                                                                .hashKind(HashType.NONE)
                                                                .build()));
        try (JsonResourceSession session = database.beginResourceSession("resource")) {
          try (JsonNodeTrx writer = session.beginNodeTrx()) {
            final ProjectionSortedDirectory.Builder builder = new ProjectionSortedDirectory.Builder(
                new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0), LAYOUT);
            final byte[][] keys = expected.keySet().toArray(byte[][]::new);
            final byte[][] values = expected.values().toArray(byte[][]::new);
            for (int from = 0; from < keys.length; from += 256) {
              final ProjectionSortedLeaf leaf = ProjectionSortedLeaf.encode(Arrays.copyOfRange(keys, from, from + 256),
                  Arrays.copyOfRange(values, from, from + 256), 256);
              assertNotNull(leaf);
              builder.append(leaf);
            }
            builder.finish();
            writer.commit();
          }
          try (JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx(1)) {
            assertPackedBody(reader.getStorageEngineReader(), 20_190);
          }
          for (int operation = 0; operation < 129; operation++) {
            try (JsonNodeTrx writer = session.beginNodeTrx()) {
              edit(operation, new ProjectionSortedDirectory.Editor(
                  new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0)), null);
              writer.commit();
            }
          }
          if (type == VersioningType.FULL) {
            try (JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx(130)) {
              assertPackedBody(reader.getStorageEngineReader(), 27_933);
            }
          }
        }
      }
      Databases.clearGlobalCaches();
      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path);
          JsonResourceSession session = database.beginResourceSession("resource")) {
        for (int revision = 1; revision <= 130; revision++) {
          if (revision > 1) {
            edit(revision - 2, null, expected);
          }
          try (JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx(revision)) {
            final ProjectionSortedDirectory.Accessor sorted =
                ProjectionSortedDirectory.open(reader.getStorageEngineReader(), 0);
            assertNotNull(sorted);
            final ProjectionSortedDirectory.Accessor.Cursor cursor = sorted.first();
            final byte[] value = new byte[Long.BYTES];
            for (final var row : expected.entrySet()) {
              assertTrue(cursor.isValid());
              assertArrayEquals(row.getKey(), cursor.copyKey());
              assertEquals(value.length, cursor.payloadLength());
              cursor.copyPayloadTo(value, 0);
              assertArrayEquals(row.getValue(), value);
              cursor.advance();
            }
            assertTrue(!cursor.isValid());
            final var groups =
                ProjectionSortedGroupScan.topK(reader.getStorageEngineReader(), 0, 32, Order.SPAN_DESC, 1);
            assertNotNull(groups);
            assertEquals(32, groups.size());
            for (final var group : groups) {
              final int id = Integer.parseInt(group.key().substring(1)) - 1000;
              assertEquals(id * 1_000_000L, group.min());
              assertEquals(id * 1_000_000L + 1022L * (id + 1), group.max());
            }
          }
        }
      }
    }
  }

  private static byte[] key(final int group, final int row, final boolean inserted) {
    final byte[] name = ("g" + (1000 + group)).getBytes(StandardCharsets.UTF_8);
    final ProjectionSortKeyCodec.Writer writer = new ProjectionSortKeyCodec.Writer();
    writer.appendUtf8(name, 0, name.length);
    writer.appendLong(group * 1_000_000L + (2L * row + (inserted
        ? 1
        : 0)) * (group + 1));
    writer.appendRecordKey(inserted
        ? 1L + GROUPS * ROWS_PER_GROUP + group
        : 1L + group * ROWS_PER_GROUP + row);
    return writer.copyKey();
  }

  private static byte[] payload(final long value) {
    final byte[] bytes = new byte[Long.BYTES];
    ProjectionIndexRowGroupCodec.putLongLEAt(bytes, 0, value);
    return bytes;
  }

  private static void edit(final int operation, final ProjectionSortedDirectory.Editor editor,
      final TreeMap<byte[], byte[]> expected) {
    final byte[] key = key(operation / 3 % GROUPS, 8, operation % 3 != 0);
    final byte[] value = payload(1_000_000L + operation);
    if (editor != null) {
      if (operation % 3 != 1) {
        editor.remove(key);
      }
      if (operation % 3 != 2) {
        editor.insert(key, value);
      }
    }
    if (expected != null) {
      if (operation % 3 == 2) {
        assertNotNull(expected.remove(key));
      } else {
        expected.put(key, value);
      }
    }
  }

  private static void assertPackedBody(final StorageEngineReader reader, final int expectedSize) throws IOException {
    final PageReference root = ProjectionIndexHOTStorage.rootReference(reader, 0);
    assertNotNull(root);
    assertTrue(root.hasHash());
    final Path data = reader.getResourceSession().getResourcePath().resolve("data").resolve("sirix.data");
    try (FileChannel channel = FileChannel.open(data, StandardOpenOption.READ)) {
      final ByteBuffer header = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN);
      read(channel, header, root.getKey());
      assertEquals(expectedSize, header.flip().getInt());
      final byte[] body = new byte[expectedSize];
      read(channel, ByteBuffer.wrap(body), root.getKey() + Integer.BYTES);
      assertEquals(root.getHashAsLong(), PageHasher.computeLong(body));
    }
  }

  private static void read(final FileChannel channel, final ByteBuffer target, final long offset) throws IOException {
    while (target.hasRemaining()) {
      assertTrue(channel.read(target, offset + target.position()) > 0);
    }
  }
}
