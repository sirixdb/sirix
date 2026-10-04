/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.io.StorageType;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Logical mutation locality and native reconstruction are independent of the fragment strategy. */
final class ProjectionSortedChunkLocalityTest {
  private static final ProjectionSortKeyCodec.Layout LONG_LAYOUT =
      new ProjectionSortKeyCodec.Layout(new byte[] {ProjectionSortKeyCodec.FIELD_LONG});
  private static final byte[] PAYLOAD = new byte[24];

  @TempDir
  Path temporaryDirectory;

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void oneTupleEditsNeverRewriteLaterRanges(final VersioningType versioning) {
    for (final int chunks : new int[] {4, 16, 64}) {
      final Path path = temporaryDirectory.resolve(versioning + "-" + chunks);
      create(path, versioning);
      final TreeMap<byte[], byte[]> expected = new TreeMap<>(Arrays::compareUnsigned);
      final List<TreeMap<byte[], byte[]>> history = new ArrayList<>();
      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path);
          JsonResourceSession session = database.beginResourceSession("resource")) {
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          final ProjectionSortedDirectory.Builder builder = new ProjectionSortedDirectory.Builder(
              new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0), LONG_LAYOUT);
          for (int chunk = 0; chunk < chunks; chunk++) {
            final byte[][] keys = new byte[8][];
            final byte[][] payloads = new byte[8][];
            for (int row = 0; row < keys.length; row++) {
              keys[row] = key(2L * (chunk * keys.length + row));
              payloads[row] = PAYLOAD.clone();
              expected.put(keys[row], payloads[row]);
            }
            builder.append(leaf(keys, payloads));
          }
          builder.finish();
          writer.commit();
        }
        history.add(new TreeMap<>(expected));
        for (int operation = 0; operation < 18; operation++) {
          final Map<Integer, byte[]> before = chunks(session, session.getMostRecentRevisionNumber());
          final byte[] editedKey = key(operation % 3 == 0
              ? 4
              : 5);
          try (JsonNodeTrx writer = session.beginNodeTrx()) {
            final ProjectionSortedDirectory.Editor editor =
                new ProjectionSortedDirectory.Editor(new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0));
            if (operation % 3 == 0) {
              final byte[] replacement = PAYLOAD.clone();
              replacement[0] = (byte) (operation + 1);
              editor.apply(new byte[][] {editedKey}, 1, new byte[][] {editedKey}, new byte[][] {replacement}, 1);
              expected.put(editedKey, replacement);
            } else if (operation % 3 == 1) {
              editor.insert(editedKey, PAYLOAD.clone());
              expected.put(editedKey, PAYLOAD.clone());
            } else {
              editor.remove(editedKey);
              expected.remove(editedKey);
            }
            writer.commit();
          }
          final Map<Integer, byte[]> after = chunks(session, session.getMostRecentRevisionNumber());
          final List<Integer> changed = changedIds(before, after);
          assertEquals(List.of(1), changed, "one fitting edit may change only its stable range");
          assertEquals(before.keySet(), after.keySet(), "later ranges must never be renumbered");
          for (final byte[] bytes : after.values()) {
            assertTrue(bytes.length <= ProjectionSortedChunks.MAX_BYTES);
          }

          history.add(new TreeMap<>(expected));
        }
      }
      Databases.clearGlobalCaches();
      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path);
          JsonResourceSession session = database.beginResourceSession("resource")) {
        for (int revision = 1; revision <= history.size(); revision++) {
          assertRows(session, revision, history.get(revision - 1));
        }
      }
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void oversizedTuplesAndSeparatorsRemainQueryableAcrossHistoryAndRollback(final VersioningType versioning) {
    final Path path = temporaryDirectory.resolve("wide-" + versioning);
    create(path, versioning);
    final TreeMap<byte[], byte[]> expected = new TreeMap<>(Arrays::compareUnsigned);
    final List<TreeMap<byte[], byte[]>> history = new ArrayList<>();
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path);
        JsonResourceSession session = database.beginResourceSession("resource")) {
      try (JsonNodeTrx writer = session.beginNodeTrx()) {
        final ProjectionSortedDirectory.Builder builder = new ProjectionSortedDirectory.Builder(
            new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0), SortedScanFixtures.GROUP_VALUE);
        for (int group = 0; group < 28; group++) {
          final byte[] row = wideKey(group);
          final byte[] payload = new byte[] {(byte) group};
          expected.put(row, payload);
          builder.append(leaf(new byte[][] {row}, new byte[][] {payload}));
        }
        builder.finish();
        writer.commit();
      }
      history.add(new TreeMap<>(expected));
      for (int operation = 0; operation < 10; operation++) {
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          final ProjectionSortedDirectory.Editor editor =
              new ProjectionSortedDirectory.Editor(new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0));
          editor.remove(wideKey(operation));
          expected.remove(wideKey(operation));
          final byte[] inserted = wideKey(40 + operation);
          editor.insert(inserted, PAYLOAD);
          expected.put(inserted, PAYLOAD);
          writer.commit();
        }
        history.add(new TreeMap<>(expected));
      }
      try (JsonNodeTrx writer = session.beginNodeTrx()) {
        final ProjectionSortedDirectory.Editor editor =
            new ProjectionSortedDirectory.Editor(new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0));
        editor.remove(wideKey(11));
        editor.insert(wideKey(100), PAYLOAD);
        writer.rollback();
      }
      assertRows(session, session.getMostRecentRevisionNumber(), expected);
    }
    Databases.clearGlobalCaches();
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path);
        JsonResourceSession session = database.beginResourceSession("resource")) {
      for (int revision = 1; revision <= history.size(); revision++) {
        assertRows(session, revision, history.get(revision - 1));
      }
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void splitMergeAndTombstoneTouchOnlyTheLocalRanges(final VersioningType versioning) {
    final Path path = temporaryDirectory.resolve("split-" + versioning);
    create(path, versioning);
    final TreeMap<byte[], byte[]> expected = new TreeMap<>(Arrays::compareUnsigned);
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path);
        JsonResourceSession session = database.beginResourceSession("resource")) {
      try (JsonNodeTrx writer = session.beginNodeTrx()) {
        new ProjectionSortedDirectory.Builder(new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0),
            LONG_LAYOUT).finish();
        writer.commit();
      }
      for (int row = 0; row < 90; row++) {
        final Map<Integer, byte[]> before = chunks(session, session.getMostRecentRevisionNumber());
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          final ProjectionSortedDirectory.Editor editor =
              new ProjectionSortedDirectory.Editor(new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0));
          editor.insert(key(row), PAYLOAD);
          writer.commit();
        }
        expected.put(key(row), PAYLOAD);
        final Map<Integer, byte[]> after = chunks(session, session.getMostRecentRevisionNumber());
        assertTrue(changedIds(before, after).size() <= 2, "a fitting tuple splits only one local range");
      }
      for (int row = 0; row < 90; row++) {
        final Map<Integer, byte[]> before = chunks(session, session.getMostRecentRevisionNumber());
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          new ProjectionSortedDirectory.Editor(
              new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0)).remove(key(row));
          writer.commit();
        }
        expected.remove(key(row));
        assertTrue(changedIds(before, chunks(session, session.getMostRecentRevisionNumber())).size() <= 2,
            "underflow may merge only the immediate neighbor");
        assertRows(session, session.getMostRecentRevisionNumber(), expected);
      }
      assertRows(session, 1, new TreeMap<>(Arrays::compareUnsigned));
    }
  }

  private static void create(final Path path, final VersioningType type) {
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(path)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource")
                                                              .storageType(StorageType.FILE_CHANNEL)
                                                              .versioningApproach(type)
                                                              .maxNumberOfRevisionsToRestore(3)
                                                              .storeDiffs(false)
                                                              .buildPathSummary(false)
                                                              .hashKind(HashType.NONE)
                                                              .build()));
    }
  }

  private static ProjectionSortedLeaf leaf(final byte[][] keys, final byte[][] payloads) {
    final ProjectionSortedLeaf leaf = ProjectionSortedLeaf.encode(keys, payloads, keys.length);
    assertNotNull(leaf);
    return leaf;
  }

  private static byte[] key(final long value) {
    final ProjectionSortKeyCodec.Writer writer = new ProjectionSortKeyCodec.Writer();
    writer.appendLong(value);
    writer.appendRecordKey(value + 1);
    return writer.copyKey();
  }

  private static byte[] wideKey(final int ordinal) {
    final byte[] text = ("x".repeat(900) + String.format("%04d", ordinal)).getBytes(StandardCharsets.UTF_8);
    final ProjectionSortKeyCodec.Writer writer = new ProjectionSortKeyCodec.Writer();
    writer.appendUtf8(text, 0, text.length);
    writer.appendLong(ordinal);
    writer.appendRecordKey(ordinal + 1L);
    return writer.copyKey();
  }

  private static Map<Integer, byte[]> chunks(final JsonResourceSession session, final int revision) {
    final Map<Integer, byte[]> result = new TreeMap<>();
    try (JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx(revision)) {
      final ProjectionSortedDirectory.Accessor directory =
          ProjectionSortedDirectory.open(reader.getStorageEngineReader(), 0);
      assertNotNull(directory);
      final ProjectionSortedDirectory.Accessor.LeafCursor cursor = directory.leaves();
      while (cursor.id() != 0) {
        final ProjectionSortedLeaf leaf =
            ProjectionSortedLeafStore.read(reader.getStorageEngineReader(), 0, cursor.id());
        assertNotNull(leaf);
        result.put(cursor.id(), leaf.encodedBytes());
        cursor.advance();
      }
    }
    return result;
  }

  private static List<Integer> changedIds(final Map<Integer, byte[]> before, final Map<Integer, byte[]> after) {
    final List<Integer> changed = new ArrayList<>();
    final Map<Integer, byte[]> union = new TreeMap<>(before);
    union.putAll(after);
    for (final int id : union.keySet()) {
      if (!Arrays.equals(before.get(id), after.get(id))) {
        changed.add(id);
      }
    }
    return changed;
  }

  private static void assertRows(final JsonResourceSession session, final int revision,
      final TreeMap<byte[], byte[]> expected) {
    try (JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx(revision)) {
      final ProjectionSortedDirectory.Accessor directory =
          ProjectionSortedDirectory.open(reader.getStorageEngineReader(), 0);
      assertNotNull(directory);
      final ProjectionSortedDirectory.Accessor.Cursor cursor = directory.first();
      for (final Map.Entry<byte[], byte[]> row : expected.entrySet()) {
        assertTrue(cursor.isValid(), "revision " + revision + " has too few rows");
        assertArrayEquals(row.getKey(), cursor.copyKey());
        final byte[] actualPayload = new byte[cursor.payloadLength()];
        cursor.copyPayloadTo(actualPayload, 0);
        assertArrayEquals(row.getValue(), actualPayload);
        final ProjectionSortedDirectory.Accessor.Cursor point = directory.seek(row.getKey());
        assertTrue(point.isValid());
        assertArrayEquals(row.getKey(), point.copyKey());
        cursor.advance();
      }
      assertFalse(cursor.isValid(), "revision " + revision + " has extra rows");
    }
  }
}
