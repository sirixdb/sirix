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
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ProjectionSortedEditorTest {

  @TempDir
  Path temporaryDirectory;

  @Test
  void randomizedWritesKeepEveryFenceConsistentUntilTheDirectoryIsEmpty() {
    final Path databasePath = temporaryDirectory.resolve("sorted-randomized");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource").build()));
      try (JsonResourceSession session = database.beginResourceSession("resource")) {
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          final ProjectionSortedDirectory.Builder builder = new ProjectionSortedDirectory.Builder(
              new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0), SortedScanFixtures.GROUP_VALUE);
          builder.finish();
          writer.commit();
        }
        final int[] order = new int[384];
        for (int i = 0; i < order.length; i++) {
          order[i] = i;
        }
        final Random random = new Random(0x51A17L);
        for (int i = order.length - 1; i > 0; i--) {
          final int j = random.nextInt(i + 1);
          final int value = order[i];
          order[i] = order[j];
          order[j] = value;
        }
        final int populated;
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          final ProjectionSortedDirectory.Editor editor =
              new ProjectionSortedDirectory.Editor(new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0));
          for (final int value : order) {
            editor.insert(key(value), key(value ^ 0x55));
          }
          populated = writer.getRevisionNumber();
          writer.commit();
        }
        try (JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx(populated)) {
          final ProjectionSortedDirectory.Accessor directory =
              ProjectionSortedDirectory.open(reader.getStorageEngineReader(), 0);
          assertNotNull(directory);
          final ProjectionSortedDirectory.Accessor.Cursor cursor = directory.first();
          final byte[] payload = new byte[Integer.BYTES];
          for (int i = 0; i < order.length; i++) {
            assertTrue(cursor.isValid());
            assertArrayEquals(key(i), cursor.copyKey());
            cursor.copyPayloadTo(payload, 0);
            assertArrayEquals(key(i ^ 0x55), payload);
            assertEquals(i < order.length - 1, cursor.advance());
          }
          assertFalse(cursor.isValid());
        }
        final int emptied;
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          final ProjectionSortedDirectory.Editor editor =
              new ProjectionSortedDirectory.Editor(new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0));
          for (final int value : order) {
            editor.remove(key(value));
          }
          emptied = writer.getRevisionNumber();
          writer.commit();
        }
        try (JsonNodeReadOnlyTrx oldReader = session.beginNodeReadOnlyTrx(populated);
            JsonNodeReadOnlyTrx newReader = session.beginNodeReadOnlyTrx(emptied)) {
          final ProjectionSortedDirectory.Accessor oldDirectory =
              ProjectionSortedDirectory.open(oldReader.getStorageEngineReader(), 0);
          final ProjectionSortedDirectory.Accessor newDirectory =
              ProjectionSortedDirectory.open(newReader.getStorageEngineReader(), 0);
          assertNotNull(oldDirectory);
          assertNotNull(newDirectory);
          assertTrue(oldDirectory.first().isValid());
          assertEquals(0, newDirectory.dataLeafCount());
          assertFalse(newDirectory.first().isValid());
        }
      }
    }
  }

  @Test
  void splittingAFullLeafAndRootKeepsIterationOrderedAcrossRevisions() {
    final Path databasePath = temporaryDirectory.resolve("sorted-split");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource").build()));
      try (JsonResourceSession session = database.beginResourceSession("resource")) {
        final int before;
        int firstSingletonId = 0;
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          final ProjectionIndexHOTStorage storage = new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0);
          final ProjectionSortedDirectory.Builder builder =
              new ProjectionSortedDirectory.Builder(storage, SortedScanFixtures.GROUP_VALUE);
          final byte[][] keys = new byte[ProjectionSortedLeaf.MAX_ROWS][];
          for (int i = 0; i < keys.length; i++) {
            keys[i] = key(2 * i + 2);
          }
          final ProjectionSortedLeaf full = ProjectionSortedLeaf.encode(keys, null, keys.length);
          assertNotNull(full);
          assertEquals(1, builder.append(full));
          for (int i = 0; i < 255; i++) {
            final int id = builder.append(single(1000 + i));
            if (i == 0) {
              firstSingletonId = id;
              assertEquals(2, firstSingletonId, "256 encoded rows fit one coarse chunk before the row-cap split");
            }
            assertEquals(firstSingletonId + i, id);
          }
          builder.finish();
          before = writer.getRevisionNumber();
          writer.commit();
        }
        final int afterSplit;
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          final ProjectionIndexHOTStorage storage = new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0);
          final ProjectionSortedDirectory.Editor editor = new ProjectionSortedDirectory.Editor(storage);
          // The larger covering value deliberately fills the bounded chunk and forces a split.
          editor.insert(key(1), new byte[256]);
          afterSplit = writer.getRevisionNumber();
          writer.commit();
        }
        try (JsonNodeReadOnlyTrx oldReader = session.beginNodeReadOnlyTrx(before);
            JsonNodeReadOnlyTrx newReader = session.beginNodeReadOnlyTrx(afterSplit)) {
          final ProjectionSortedDirectory.Accessor oldDirectory =
              ProjectionSortedDirectory.open(oldReader.getStorageEngineReader(), 0);
          final ProjectionSortedDirectory.Accessor newDirectory =
              ProjectionSortedDirectory.open(newReader.getStorageEngineReader(), 0);
          assertNotNull(oldDirectory);
          assertNotNull(newDirectory);
          assertEquals(firstSingletonId + 254, oldDirectory.dataLeafCount());
          assertEquals(oldDirectory.dataLeafCount() + 1, newDirectory.dataLeafCount());
          assertArrayEquals(key(2), oldDirectory.first().copyKey());
          assertArrayEquals(key(1), newDirectory.first().copyKey());
          assertEquals(1, newDirectory.findLeafId(key(1)));
          assertEquals(oldDirectory.findLeafId(key(2)), newDirectory.findLeafId(key(2)));
          assertEquals(firstSingletonId, newDirectory.findLeafId(key(1000)));
          assertScan(newDirectory, true);
          assertScan(oldDirectory, false);
        }
        final int afterRemoval;
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          final ProjectionIndexHOTStorage storage = new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0);
          final ProjectionSortedDirectory.Editor editor = new ProjectionSortedDirectory.Editor(storage);
          editor.remove(key(1000));
          afterRemoval = writer.getRevisionNumber();
          writer.commit();
        }
        try (JsonNodeReadOnlyTrx previous = session.beginNodeReadOnlyTrx(afterSplit);
            JsonNodeReadOnlyTrx current = session.beginNodeReadOnlyTrx(afterRemoval)) {
          final ProjectionSortedDirectory.Accessor oldDirectory =
              ProjectionSortedDirectory.open(previous.getStorageEngineReader(), 0);
          final ProjectionSortedDirectory.Accessor newDirectory =
              ProjectionSortedDirectory.open(current.getStorageEngineReader(), 0);
          assertNotNull(oldDirectory);
          assertNotNull(newDirectory);
          assertEquals(firstSingletonId + 255, oldDirectory.dataLeafCount());
          assertEquals(oldDirectory.dataLeafCount() - 1, newDirectory.dataLeafCount());
          assertArrayEquals(key(1000), oldDirectory.seek(key(1000)).copyKey());
          assertArrayEquals(key(1001), newDirectory.seek(key(1000)).copyKey());
          assertEquals(firstSingletonId + 1, newDirectory.findLeafId(key(1001)));
        }
      }
    }
  }

  @Test
  void deletingTheLastRowAndReinsertingUsesNewIdsWithoutChangingHistory() {
    final Path databasePath = temporaryDirectory.resolve("sorted-empty");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource").build()));
      try (JsonResourceSession session = database.beginResourceSession("resource")) {
        final int before;
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          final ProjectionIndexHOTStorage storage = new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0);
          final ProjectionSortedDirectory.Builder builder =
              new ProjectionSortedDirectory.Builder(storage, SortedScanFixtures.GROUP_VALUE);
          builder.append(single(10));
          builder.finish();
          before = writer.getRevisionNumber();
          writer.commit();
        }
        final int empty;
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          final ProjectionSortedDirectory.Editor editor =
              new ProjectionSortedDirectory.Editor(new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0));
          editor.remove(key(10));
          empty = writer.getRevisionNumber();
          writer.commit();
        }
        final int after;
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          final ProjectionSortedDirectory.Editor editor =
              new ProjectionSortedDirectory.Editor(new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0));
          editor.insert(key(5), new byte[0]);
          after = writer.getRevisionNumber();
          writer.commit();
        }
        try (JsonNodeReadOnlyTrx oldReader = session.beginNodeReadOnlyTrx(before);
            JsonNodeReadOnlyTrx emptyReader = session.beginNodeReadOnlyTrx(empty);
            JsonNodeReadOnlyTrx newReader = session.beginNodeReadOnlyTrx(after)) {
          final ProjectionSortedDirectory.Accessor oldDirectory =
              ProjectionSortedDirectory.open(oldReader.getStorageEngineReader(), 0);
          final ProjectionSortedDirectory.Accessor emptyDirectory =
              ProjectionSortedDirectory.open(emptyReader.getStorageEngineReader(), 0);
          final ProjectionSortedDirectory.Accessor newDirectory =
              ProjectionSortedDirectory.open(newReader.getStorageEngineReader(), 0);
          assertNotNull(oldDirectory);
          assertNotNull(emptyDirectory);
          assertNotNull(newDirectory);
          assertEquals(1, oldDirectory.first().leafId());
          assertArrayEquals(key(10), oldDirectory.first().copyKey());
          assertEquals(0, emptyDirectory.dataLeafCount());
          assertFalse(emptyDirectory.first().isValid());
          assertEquals(2, newDirectory.first().leafId());
          assertArrayEquals(key(5), newDirectory.first().copyKey());
        }
      }
    }
  }

  @Test
  void batchedEditsRewriteEachTouchedLeafOnceAndSplitOnlyWhenFull() {
    final Path databasePath = temporaryDirectory.resolve("sorted-batch");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource").build()));
      try (JsonResourceSession session = database.beginResourceSession("resource")) {
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          final ProjectionSortedDirectory.Builder builder = new ProjectionSortedDirectory.Builder(
              new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0), SortedScanFixtures.GROUP_VALUE);
          for (int leaf = 0; leaf < 3; leaf++) {
            final byte[][] keys = new byte[100][];
            for (int i = 0; i < keys.length; i++) {
              keys[i] = key(leaf * 10_000 + 10 * i);
            }
            builder.append(ProjectionSortedLeaf.encode(keys, null, keys.length));
          }
          builder.finish();
          writer.commit();
        }
        final Int2IntOpenHashMap writesByLeaf = new Int2IntOpenHashMap();
        final TreeSet<Integer> touched = new TreeSet<>();
        final int initialLeaves;
        final int revision;
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          final ProjectionSortedDirectory.Editor editor =
              new ProjectionSortedDirectory.Editor(new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0));
          final byte[][] removals = {key(0), key(20), key(990), key(20_000)};
          final byte[][] insertions = new byte[200][];
          for (int i = 0; i < 199; i++) {
            insertions[i] = key(10_000 + 10 * i + 5);
          }
          insertions[199] = key(30_000);
          final ProjectionSortedDirectory.Accessor before =
              ProjectionSortedDirectory.open(writer.getStorageEngineWriter(), 0);
          assertNotNull(before);
          initialLeaves = before.dataLeafCount();
          for (final byte[] removal : removals) {
            touched.add(before.findLeafId(removal));
          }
          for (final byte[] insertion : insertions) {
            touched.add(before.findLeafId(insertion));
          }
          ProjectionSortedLeafStore.setWriteObserverForTesting(leafId -> writesByLeaf.addTo(leafId, 1));
          try {
            editor.apply(removals, removals.length, insertions, null, insertions.length);
          } finally {
            ProjectionSortedLeafStore.setWriteObserverForTesting(null);
          }
          revision = writer.getRevisionNumber();
          writer.commit();
        }
        for (int id = 1; id <= initialLeaves; id++) {
          assertEquals(touched.contains(id)
              ? 1
              : 0, writesByLeaf.get(id),
              "each touched original chunk is written once; untouched chunks are not written");
        }
        assertTrue(writesByLeaf.keySet().intStream().anyMatch(id -> id > initialLeaves),
            "the large insertion must allocate local split chunks");
        assertTrue(writesByLeaf.values().intStream().allMatch(count -> count == 1));
        try (JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx(revision)) {
          final ProjectionSortedDirectory.Accessor directory =
              ProjectionSortedDirectory.open(reader.getStorageEngineReader(), 0);
          assertNotNull(directory);
          final TreeSet<Integer> expected = new TreeSet<>();
          for (int leaf = 0; leaf < 3; leaf++) {
            for (int i = 0; i < 100; i++) {
              expected.add(leaf * 10_000 + 10 * i);
            }
          }
          expected.removeAll(List.of(0, 20, 990, 20_000));
          for (int i = 0; i < 199; i++) {
            expected.add(10_000 + 10 * i + 5);
          }
          expected.add(30_000);
          final ProjectionSortedDirectory.Accessor.Cursor cursor = directory.first();
          for (final int value : expected) {
            assertTrue(cursor.isValid());
            assertArrayEquals(key(value), cursor.copyKey());
            cursor.advance();
          }
          assertFalse(cursor.isValid());
          assertTrue(directory.dataLeafCount() > 3);
          for (final int value : expected) {
            final ProjectionSortedDirectory.Accessor.Cursor seek = directory.seek(key(value));
            assertArrayEquals(key(value), seek.copyKey());
          }
        }
      }
    }
  }

  private static void assertScan(final ProjectionSortedDirectory.Accessor directory, final boolean withOne) {
    final ProjectionSortedDirectory.Accessor.Cursor cursor = directory.first();
    if (withOne) {
      assertRoutedTuple(directory, cursor, key(1), new byte[256]);
      assertTrue(cursor.advance());
    }
    for (int i = 0; i < 256; i++) {
      assertTrue(cursor.isValid());
      // The bounded split allocates chunk 263 for part of the original key range.
      // Those exact keys and empty covering payloads are unchanged; verify both access routes
      // instead of assuming the old 127-row split boundary and physical IDs 1/257.
      assertRoutedTuple(directory, cursor, key(2 * i + 2), new byte[0]);
      if (withOne && cursor.leafId() == 263) {
        System.out.printf("CHUNK_ROUTE key=%d payloadBytes=%d actualLeafId=%d exactScanAndSeek=PASS%n", 2 * i + 2,
            cursor.payloadLength(), cursor.leafId());
      }
      assertTrue(cursor.advance());
    }
    for (int i = 0; i < 255; i++) {
      assertTrue(cursor.isValid());
      assertRoutedTuple(directory, cursor, key(1000 + i), new byte[0]);
      assertEquals(i < 254, cursor.advance());
    }
    assertFalse(cursor.isValid());
  }

  private static void assertRoutedTuple(final ProjectionSortedDirectory.Accessor directory,
      final ProjectionSortedDirectory.Accessor.Cursor scan, final byte[] expectedKey, final byte[] expectedPayload) {
    assertArrayEquals(expectedKey, scan.copyKey());
    final byte[] scanPayload = new byte[scan.payloadLength()];
    scan.copyPayloadTo(scanPayload, 0);
    assertArrayEquals(expectedPayload, scanPayload);
    final ProjectionSortedDirectory.Accessor.Cursor seek = directory.seek(expectedKey);
    assertTrue(seek.isValid());
    assertArrayEquals(expectedKey, seek.copyKey());
    final byte[] seekPayload = new byte[seek.payloadLength()];
    seek.copyPayloadTo(seekPayload, 0);
    assertArrayEquals(expectedPayload, seekPayload);
    assertEquals(scan.leafId(), seek.leafId());
    assertEquals(scan.leafId(), directory.findLeafId(expectedKey));
  }

  private static ProjectionSortedLeaf single(final int value) {
    final ProjectionSortedLeaf leaf = ProjectionSortedLeaf.encode(new byte[][] {key(value)}, null, 1);
    assertNotNull(leaf);
    return leaf;
  }

  private static byte[] key(final int value) {
    return new byte[] {(byte) (value >>> 24), (byte) (value >>> 16), (byte) (value >>> 8), (byte) value};
  }
}
