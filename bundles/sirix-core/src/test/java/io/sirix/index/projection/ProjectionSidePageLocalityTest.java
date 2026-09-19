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
import io.sirix.io.StorageType;
import io.sirix.settings.VersioningType;
import io.sirix.page.OverflowPage;
import io.sirix.page.PageReference;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Path;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ResourceLock(Resources.SYSTEM_PROPERTIES)
final class ProjectionSidePageLocalityTest {
  private static final int ROWS = 257;
  private static final int[] SEGMENTS = {7, 23, 51};
  private static final int[] ARRIVAL = {2, 0, 1};

  @TempDir
  Path directory;

  @ParameterizedTest
  @EnumSource(ProjectionSlotLayout.class)
  void groupedAppendPreservesPayloadsHistoryAndUntouchedOffsets(final ProjectionSlotLayout layout) {
    final String property = "sirix.projection.columnMajorSlots";
    final String previous = System.getProperty(property);
    System.setProperty(property, Boolean.toString(layout == ProjectionSlotLayout.COLUMN_MAJOR));
    try {
      for (final VersioningType versioning : VersioningType.values()) {
        verifyLayout(layout, versioning);
      }
    } finally {
      if (previous == null) {
        System.clearProperty(property);
      } else {
        System.setProperty(property, previous);
      }
    }
  }

  private void verifyLayout(final ProjectionSlotLayout layout, final VersioningType versioning) {
    final Path path = directory.resolve(versioning.name());
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(path)));
    final long[][] offsets = new long[SEGMENTS.length][ROWS];
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource")
                                                              .storageType(StorageType.FILE_CHANNEL)
                                                              .versioningApproach(versioning)
                                                              .maxNumberOfRevisionsToRestore(2)
                                                              .build()));
      try (JsonResourceSession session = database.beginResourceSession("resource")) {
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          writer.insertArrayAsFirstChild();
          final long arrayKey = writer.getNodeKey();
          final ProjectionIndexHOTStorage storage =
              ProjectionIndexHOTStorage.forBulkBuild(writer.getStorageEngineWriter(), 0);
          for (int row = 0; row < ROWS; row++) {
            for (final int column : ARRIVAL) {
              storage.putColumnSegmentSlot(layout.segmentSlot(row + 1, SEGMENTS[column]), payload(row, column, 1));
            }
            if (row % 64 == 63) {
              writer.moveTo(arrayKey);
              writer.insertStringValueAsFirstChild("epoch-" + row);
              writer.moveToDocumentRoot();
              final PageReference immediate = new PageReference();
              immediate.setPage(new OverflowPage(new byte[128]));
              assertTrue(writer.getStorageEngineWriter().stageUncommittedOverflowPage(immediate));
              writer.getStorageEngineWriter().asyncFlush();
            }
          }
          assertArrayEquals(payload(128, 1, 1), ProjectionIndexHOTStorage.readColumnSegmentSlot(
              writer.getStorageEngineWriter(), 0, layout.segmentSlot(129, SEGMENTS[1])));
          writer.commit();
        }
        try (JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx(1)) {
          long previousOffset = -1;
          for (int column = 0; column < SEGMENTS.length; column++) {
            for (int row = 0; row < ROWS; row++) {
              final long key = layout.segmentSlot(row + 1, SEGMENTS[column]);
              final long offset =
                  ProjectionIndexHOTStorage.segmentPageOffset(reader.getStorageEngineReader(), 0, key, 0);
              assertTrue(offset > previousOffset, "columns must be adjacent and stable within the append batch");
              offsets[column][row] = offset;
              previousOffset = offset;
            }
          }
        }
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          final ProjectionIndexHOTStorage storage = new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0);
          storage.putColumnSegmentSlot(layout.segmentSlot(18, SEGMENTS[1]), payload(17, 1, 2));
          storage.putColumnSegmentSlot(layout.segmentSlot(22, SEGMENTS[2]), payload(21, 2, 2));
          writer.commit();
        }
      }
    }
    Databases.clearGlobalCaches();
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path);
        JsonResourceSession session = database.beginResourceSession("resource")) {
      for (int revision = 1; revision <= 2; revision++) {
        try (JsonNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx(revision)) {
          final StorageEngineReader reader = trx.getStorageEngineReader();
          for (int column = 0; column < SEGMENTS.length; column++) {
            for (int row = 0; row < ROWS; row++) {
              final boolean changed = (row == 17 && column == 1) || (row == 21 && column == 2);
              final long key = layout.segmentSlot(row + 1, SEGMENTS[column]);
              assertArrayEquals(payload(row, column, revision == 2 && changed
                  ? 2
                  : 1), ProjectionIndexHOTStorage.readColumnSegmentSlot(reader, 0, key));
              if (revision == 1 || !changed) {
                assertEquals(offsets[column][row], ProjectionIndexHOTStorage.segmentPageOffset(reader, 0, key, 0),
                    "untouched payloads must retain their durable offsets");
              }
            }
          }
        }
      }
    }
  }

  private static byte[] payload(final int row, final int column, final int revision) {
    final int length = revision == 2
        ? (column == 1
            ? 31
            : 4097)
        : 2048 + column;
    final byte[] bytes = new byte[length];
    new Random(31L * row + 17L * column + revision).nextBytes(bytes);
    return bytes;
  }
}
