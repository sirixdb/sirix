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
import io.sirix.index.projection.ProjectionSortedGroupScan.Order;
import io.sirix.io.StorageType;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/** Group extrema must read selected slots without reconstructing unrelated native leaf contents. */
@Isolated
final class ProjectionSortedGroupRoutingWorkBudgetTest {
  @TempDir
  Path temporary;

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void coldExtremaUseBoundedSelectiveWorkAndWarmExtremaReuseVerifiedNodes(final VersioningType versioning) {
    assertTrue(VersioningType.hotMergeDiagEnabled(), "fragment-work instruments must be enabled");
    final List<SortedScanFixtures.Row> rows = new ArrayList<>();
    for (int group = 0; group < 64; group++) {
      for (int row = 0; row < 64; row++) {
        rows.add(
            new SortedScanFixtures.Row("g" + (1000 + group), group * 1_000_000L + row * (group + 1L), rows.size() + 1));
      }
    }
    final Path path = temporary.resolve(versioning.name());
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(path)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource")
                                                              .storageType(StorageType.FILE_CHANNEL)
                                                              .versioningApproach(versioning)
                                                              .maxNumberOfRevisionsToRestore(32)
                                                              .build()));
      try (JsonResourceSession session = database.beginResourceSession("resource")) {
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          // Four initial chunks per group keep the tree eligible after local underflow merges.
          SortedScanFixtures.build(writer, rows, new int[] {16});
          writer.commit();
        }
        for (int revision = 2; revision <= 31; revision++) {
          final SortedScanFixtures.Row prior = rows.getLast();
          final SortedScanFixtures.Row next =
              new SortedScanFixtures.Row(prior.group(), prior.value() + 100, prior.record());
          try (JsonNodeTrx writer = session.beginNodeTrx()) {
            new ProjectionSortedDirectory.Editor(
                new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0)).apply(
                    new byte[][] {SortedScanFixtures.key(prior)}, 1, new byte[][] {SortedScanFixtures.key(next)}, null,
                    1);
            writer.commit();
          }
          rows.set(rows.size() - 1, next);
        }
        try (JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx()) {
          assertEquals(64, ProjectionSortedGroupSma.publishedGroupCount(reader.getStorageEngineReader(), 0));
        }
      }
    }
    for (final Order order : new Order[] {Order.MIN_ASC, Order.SPAN_DESC}) {
      Databases.clearGlobalCaches();
      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path);
          JsonResourceSession session = database.beginResourceSession("resource");
          JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx()) {
        final long merges = VersioningType.multiFragmentMerges();
        final long walks = VersioningType.pointLeafReads();
        final long fragments = VersioningType.pointFragmentsWalked();
        final var expected = order == Order.MIN_ASC
            ? SortedScanFixtures.expectedMin(rows, 5)
            : SortedScanFixtures.expectedSpan(rows, 5, 3);
        assertEquals(expected,
            ProjectionSortedGroupScan.topK(reader.getStorageEngineReader(), 0, 5, order, order == Order.MIN_ASC
                ? 1
                : 3, order == Order.MIN_ASC));
        assertEquals(merges, VersioningType.multiFragmentMerges(), "SMA queries must not merge full native leaves");
        final long coldWalks = VersioningType.pointLeafReads() - walks;
        assertTrue(coldWalks <= 12, "directory header and bounded group nodes require at most twelve slot walks");
        assertTrue(VersioningType.pointFragmentsWalked() - fragments <= 12 * 30,
            "selected slots cannot walk more than the fixture's history");
        if (versioning != VersioningType.FULL) {
          assertTrue(coldWalks > 0, "the cold fragmented fixture must exercise slot reconstruction");
        }
        final long warmWalks = VersioningType.pointLeafReads();
        assertEquals(expected,
            ProjectionSortedGroupScan.topK(reader.getStorageEngineReader(), 0, 5, order, order == Order.MIN_ASC
                ? 1
                : 3, order == Order.MIN_ASC));
        assertTrue(VersioningType.pointLeafReads() - warmWalks <= 1,
            "verified group nodes are retained; only the unadmitted directory header may walk again");
        assertEquals(merges, VersioningType.multiFragmentMerges());
      }
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void retirementWorkDependsOnReachableNodesRatherThanRetiredIdGaps(final VersioningType versioning) {
    final Path path = temporary.resolve("bounded-retirement-" + versioning);
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(path)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource")
                                                              .storageType(StorageType.FILE_CHANNEL)
                                                              .versioningApproach(versioning)
                                                              .build()));
      try (JsonResourceSession session = database.beginResourceSession("resource")) {
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          SortedScanFixtures.build(writer, List.of(new SortedScanFixtures.Row("group", 10, 1)), new int[] {1});
          writer.commit();
        }
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          final ProjectionIndexHOTStorage actual = new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0);
          final byte[] header = actual.getBlob(ProjectionSortedGroupSma.HEADER_SLOT);
          assertNotNull(header);
          assertEquals(1, ProjectionIndexRowGroupCodec.getIntLE(header, 12));
          // Valid allocation history with retired gaps, without depending on a particular split cadence.
          ProjectionIndexRowGroupCodec.putIntLEAt(header, 12, 256);
          actual.putBlob(ProjectionSortedGroupSma.HEADER_SLOT, header);
          final ProjectionIndexHOTStorage counted = mock(ProjectionIndexHOTStorage.class, delegatesTo(actual));
          assertTrue(ProjectionSortedGroupSma.invalidate(counted, false));
          verify(counted, times(2)).tombstoneBlob(anyLong());
          verify(counted).tombstoneBlob(ProjectionSortedGroupSma.HEADER_SLOT + 1);
          verify(counted).tombstoneBlob(ProjectionSortedGroupSma.HEADER_SLOT);
          writer.commit();
        }
        Databases.clearGlobalCaches();
        try (JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx()) {
          assertEquals(-1, ProjectionSortedGroupSma.publishedGroupCount(reader.getStorageEngineReader(), 0));
          assertNull(ProjectionIndexHOTStorage.readBlob(reader.getStorageEngineReader(), 0,
              ProjectionSortedGroupSma.HEADER_SLOT + 1));
        }
        try (JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx(1)) {
          assertEquals(1, ProjectionSortedGroupSma.publishedGroupCount(reader.getStorageEngineReader(), 0));
        }
      }
    }
  }
}
