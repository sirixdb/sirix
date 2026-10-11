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
import io.sirix.index.projection.ProjectionSortedGroupScan.Group;
import io.sirix.index.projection.ProjectionSortedGroupScan.Order;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Path;
import java.util.List;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ProjectionSortedSingleGroupProofTest {
  @TempDir
  Path directory;

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void unprovenBoundsRecoverOnlyTouchedEntriesAndProofsFollowSummaryHistory(final VersioningType type) {
    final Path path = directory.resolve(type.name());
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(path)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource")
                                                              .versioningApproach(type)
                                                              .maxNumberOfRevisionsToRestore(3)
                                                              .build()));
      try (JsonResourceSession session = database.beginResourceSession("resource")) {
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          SortedScanFixtures.build(writer,
              List.of(new SortedScanFixtures.Row("a", 1, 1), new SortedScanFixtures.Row("a", 50, 2),
                  new SortedScanFixtures.Row("b", 2, 3), new SortedScanFixtures.Row("c", 3, 4)),
              new int[] {2});
          writer.commit();
        }
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          final ProjectionIndexHOTStorage storage = new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0);
          final byte[] unproven = Objects.requireNonNull(storage.getBlob(ProjectionSortedLeafBounds.slot(1))).clone();
          ProjectionIndexRowGroupCodec.putLongLEAt(unproven, 8,
              ProjectionIndexRowGroupCodec.getLongLE(unproven, 8) & 0xFFFFL);
          storage.putBlob(ProjectionSortedLeafBounds.slot(1), unproven);
          writer.commit();
        }
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          new ProjectionSortedDirectory.Editor(
              new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0)).insert(
                  SortedScanFixtures.key(new SortedScanFixtures.Row("a", 100, 5)), new byte[0]);
          writer.commit();
        }
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0).tombstoneBlob(
              ProjectionSortedGroupSummary.slot(1));
          writer.commit();
        }
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          assertEquals(2, ProjectionSortedGroupScan.buildLeafSummaries(writer.getStorageEngineWriter(), 0));
          writer.commit();
        }
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0).tombstoneBlob(
              ProjectionSortedGroupSummary.slot(1));
          writer.rollback();
        }
      }
    }
    Databases.clearGlobalCaches();
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path);
        JsonResourceSession session = database.beginResourceSession("resource")) {
      for (int revision = 1; revision <= 5; revision++) {
        try (JsonNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx(revision)) {
          final StorageEngineReader reader = trx.getStorageEngineReader();
          final ProjectionSortedDirectory.Accessor sorted = ProjectionSortedDirectory.open(reader, 0);
          assertNotNull(sorted);
          final ProjectionSortedLeafBounds.Candidates candidates = ProjectionSortedLeafBounds.read(reader, 0, sorted);
          assertNotNull(candidates);
          assertArrayEquals(new boolean[] {revision != 2 && revision != 4, false}, candidates.singleGroups());
          final List<Group> expected = List.of(new Group("a", 1, revision < 3
              ? 50
              : 100));
          final List<Group> summarized =
              ProjectionSortedGroupScan.topKFromSummaries(reader, 0, sorted, 1, Order.SPAN_DESC, 1, false);
          if (revision == 4) {
            assertNull(summarized, "removal must invalidate a previously published proof");
          } else {
            assertEquals(expected, summarized);
          }
          assertEquals(expected, ProjectionSortedGroupScan.topK(reader, 0, 1, Order.SPAN_DESC, 1));
        }
      }
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void proofWithoutValidExtremaIsRejected(final VersioningType type) {
    final Path path = directory.resolve("invalid-" + type);
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(path)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      assertTrue(
          database.createResource(ResourceConfiguration.newBuilder("resource").versioningApproach(type).build()));
      try (JsonResourceSession session = database.beginResourceSession("resource");
          JsonNodeTrx writer = session.beginNodeTrx()) {
        SortedScanFixtures.build(writer, List.of(new SortedScanFixtures.Row("a", 1, 1)), new int[] {1});
        final ProjectionIndexHOTStorage storage = new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0);
        final byte[] invalid = Objects.requireNonNull(storage.getBlob(ProjectionSortedLeafBounds.slot(1))).clone();
        invalid[4] = 1;
        storage.putBlob(ProjectionSortedLeafBounds.slot(1), invalid);
        assertThrows(IllegalStateException.class, () -> ProjectionSortedLeafBounds.read(writer.getStorageEngineWriter(),
            0, Objects.requireNonNull(ProjectionSortedDirectory.open(writer.getStorageEngineWriter(), 0))));
        invalid[4] = 2;
        ProjectionIndexRowGroupCodec.putLongLEAt(invalid, 8, 1L << 16);
        storage.putBlob(ProjectionSortedLeafBounds.slot(1), invalid);
        assertThrows(IllegalStateException.class, () -> ProjectionSortedLeafBounds.read(writer.getStorageEngineWriter(),
            0, Objects.requireNonNull(ProjectionSortedDirectory.open(writer.getStorageEngineWriter(), 0))));
      }
    }
  }
}
