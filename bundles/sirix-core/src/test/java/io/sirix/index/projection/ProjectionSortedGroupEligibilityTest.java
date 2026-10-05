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
import io.sirix.index.projection.ProjectionSortedGroupScan.Order;
import io.sirix.io.StorageType;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Isolated
final class ProjectionSortedGroupEligibilityTest {
  @TempDir
  Path temporary;

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void wideAndMissingValuesDeclineRetireAndRebuildWithoutChangingHistory(final VersioningType versioning) {
    for (final boolean missing : new boolean[] {false, true}) {
      final Path path = temporary.resolve(versioning + "-" + missing);
      assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(path)));
      final List<SortedScanFixtures.Row> rows = new ArrayList<>();
      for (int group = 0; group < 40; group++) {
        for (int i = 0; i < 4; i++) {
          rows.add(new SortedScanFixtures.Row("g" + (1000 + group), group * 1000L + i, rows.size() + 1));
        }
      }
      final SortedScanFixtures.Row wide = new SortedScanFixtures.Row("z".repeat(256), -100, 900_001);
      final byte[] bad = missing
          ? missingValueKey()
          : SortedScanFixtures.key(wide);
      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
        assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource")
                                                                .storageType(StorageType.FILE_CHANNEL)
                                                                .versioningApproach(versioning)
                                                                .maxNumberOfRevisionsToRestore(3)
                                                                .build()));
        try (JsonResourceSession session = database.beginResourceSession("resource")) {
          try (JsonNodeTrx writer = session.beginNodeTrx()) {
            SortedScanFixtures.build(writer, rows, new int[] {2});
            writer.commit();
          }
          final int oldNodes;
          try (JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx()) {
            final byte[] header = ProjectionIndexHOTStorage.readBlob(reader.getStorageEngineReader(), 0,
                ProjectionSortedGroupSma.HEADER_SLOT);
            assertNotNull(header);
            oldNodes = ProjectionIndexRowGroupCodec.getIntLE(header, 12);
            assertTrue(oldNodes > 0);
          }
          try (JsonNodeTrx writer = session.beginNodeTrx()) {
            new ProjectionSortedDirectory.Editor(
                new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0)).insert(bad, new byte[0]);
            writer.commit();
          }
          Databases.clearGlobalCaches();
          try (JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx()) {
            final StorageEngineReader storage = reader.getStorageEngineReader();
            assertEquals(-1, ProjectionSortedGroupSma.publishedGroupCount(storage, 0));
            for (int id = 1; id <= oldNodes; id++) {
              assertNull(ProjectionIndexHOTStorage.readBlob(storage, 0, ProjectionSortedGroupSma.HEADER_SLOT + id),
                  "declining a published tree must retire every prior node");
            }
          }
          try (JsonNodeTrx writer = session.beginNodeTrx()) {
            final ProjectionIndexHOTStorage storage = new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0);
            new ProjectionSortedDirectory.Editor(storage).remove(bad);
            // Explicit rebuild from current chunks, using the same production streaming builder.
            final ProjectionSortedDirectory.Accessor directory =
                ProjectionSortedDirectory.open(writer.getStorageEngineWriter(), 0);
            final ProjectionSortedGroupSma.Builder rebuild =
                new ProjectionSortedGroupSma.Builder(storage, directory.layout());
            final ProjectionSortedDirectory.Accessor.LeafCursor cursor = directory.leaves();
            int chunks = 0;
            while (cursor.id() != 0) {
              final ProjectionSortedLeaf leaf = ProjectionSortedLeafStore.read(storage, cursor.id());
              rebuild.appendSummary(ProjectionSortedGroupSummary.encode(leaf, directory.layout()));
              chunks++;
              cursor.advance();
            }
            rebuild.finish(chunks);
            writer.commit();
          }
          for (int revision = 1; revision <= 3; revision++) {
            Databases.clearGlobalCaches();
            try (JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx(revision)) {
              final StorageEngineReader storage = reader.getStorageEngineReader();
              final List<byte[]> expected = new ArrayList<>();
              rows.forEach(row -> expected.add(SortedScanFixtures.key(row)));
              if (revision == 2) {
                expected.add(bad);
              }
              expected.sort(Arrays::compareUnsigned);
              for (int pass = 0; pass < 2; pass++) {
                final ProjectionSortedDirectory.Accessor.Cursor cursor =
                    ProjectionSortedDirectory.open(storage, 0).first();
                for (final byte[] key : expected) {
                  assertTrue(cursor.isValid());
                  assertArrayEquals(key, cursor.copyKey());
                  cursor.advance();
                }
                assertTrue(!cursor.isValid());
                assertEquals(revision == 2
                    ? -1
                    : 40, ProjectionSortedGroupSma.publishedGroupCount(storage, 0));
                if (revision == 2 && missing) {
                  assertNull(ProjectionSortedGroupScan.topK(storage, 0, 5, Order.MIN_ASC, 1, true));
                } else {
                  final List<SortedScanFixtures.Row> expectedRows = new ArrayList<>(rows);
                  if (revision == 2) {
                    expectedRows.add(wide);
                  }
                  assertEquals(SortedScanFixtures.expectedMin(expectedRows, 5),
                      ProjectionSortedGroupScan.topK(storage, 0, 5, Order.MIN_ASC, 1, true));
                }
              }
            }
          }
        }
      }
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void initialBuildWithWideOrMissingValuesPublishesNoPartialTree(final VersioningType versioning) {
    for (final boolean missing : new boolean[] {false, true}) {
      final Path path = temporary.resolve("initial-" + versioning + "-" + missing);
      assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(path)));
      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
        assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource")
                                                                .storageType(StorageType.FILE_CHANNEL)
                                                                .versioningApproach(versioning)
                                                                .build()));
        try (JsonResourceSession session = database.beginResourceSession("resource")) {
          final List<byte[]> keys = new ArrayList<>();
          for (int i = 0; i < 40; i++) {
            keys.add(SortedScanFixtures.key(new SortedScanFixtures.Row("g" + (1000 + i), i, i + 1)));
          }
          keys.add(missing
              ? missingValueKey()
              : SortedScanFixtures.key(new SortedScanFixtures.Row("z".repeat(256), 1000, 1001)));
          keys.sort(Arrays::compareUnsigned);
          try (JsonNodeTrx writer = session.beginNodeTrx()) {
            final ProjectionSortedDirectory.Builder builder = new ProjectionSortedDirectory.Builder(
                new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0), SortedScanFixtures.GROUP_VALUE);
            for (final byte[] key : keys) {
              builder.append(ProjectionSortedLeaf.encode(new byte[][] {key}, null, 1));
            }
            builder.finish();
            writer.commit();
          }
          Databases.clearGlobalCaches();
          try (JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx()) {
            final StorageEngineReader storage = reader.getStorageEngineReader();
            assertEquals(-1, ProjectionSortedGroupSma.publishedGroupCount(storage, 0));
            for (int i = 1; i <= 40; i++) {
              assertNull(ProjectionIndexHOTStorage.readBlob(storage, 0, ProjectionSortedGroupSma.HEADER_SLOT + i));
            }
            final ProjectionSortedDirectory.Accessor.Cursor cursor = ProjectionSortedDirectory.open(storage, 0).first();
            for (final byte[] key : keys) {
              assertArrayEquals(key, cursor.copyKey());
              cursor.advance();
            }
            assertTrue(!cursor.isValid());
          }
        }
      }
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void directSummaryInvalidationRetiresTheUnownedTree(final VersioningType versioning) {
    final Path path = temporary.resolve("direct-" + versioning);
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(path)));
    final List<SortedScanFixtures.Row> rows = new ArrayList<>();
    for (int i = 0; i < 64; i++) {
      rows.add(new SortedScanFixtures.Row("g" + (1000 + i), i * 10L, i + 1));
    }
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource")
                                                              .storageType(StorageType.FILE_CHANNEL)
                                                              .versioningApproach(versioning)
                                                              .build()));
      try (JsonResourceSession session = database.beginResourceSession("resource")) {
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          SortedScanFixtures.build(writer, rows, new int[] {1});
          writer.commit();
        }
        final int nodes;
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          final ProjectionIndexHOTStorage storage = new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0);
          final byte[] header = storage.getBlob(ProjectionSortedGroupSma.HEADER_SLOT);
          assertNotNull(header);
          nodes = ProjectionIndexRowGroupCodec.getIntLE(header, 12);
          assertTrue(nodes > 0);
          storage.tombstoneBlob(ProjectionSortedGroupSummary.slot(1));
          writer.commit();
        }
        Databases.clearGlobalCaches();
        try (JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx()) {
          final StorageEngineReader storage = reader.getStorageEngineReader();
          assertEquals(-1, ProjectionSortedGroupSma.publishedGroupCount(storage, 0));
          for (int i = 1; i <= nodes; i++) {
            assertNull(ProjectionIndexHOTStorage.readBlob(storage, 0, ProjectionSortedGroupSma.HEADER_SLOT + i));
          }
          assertEquals(SortedScanFixtures.expectedMin(rows, 10),
              ProjectionSortedGroupScan.topK(storage, 0, 10, Order.MIN_ASC, 1, true));
        }
        try (JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx(1)) {
          assertEquals(64, ProjectionSortedGroupSma.publishedGroupCount(reader.getStorageEngineReader(), 0));
        }
      }
    }
  }

  private static byte[] missingValueKey() {
    final ProjectionSortKeyCodec.Writer key = new ProjectionSortKeyCodec.Writer();
    final byte[] group = "zz".getBytes(StandardCharsets.UTF_8);
    key.appendUtf8(group, 0, group.length);
    key.appendMissing();
    key.appendRecordKey(900_001);
    return key.copyKey();
  }


}
