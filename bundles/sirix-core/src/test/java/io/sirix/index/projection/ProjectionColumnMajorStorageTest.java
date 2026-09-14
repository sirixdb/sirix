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
import io.sirix.index.projection.ProjectionIndexHOTStorage.RowGroupDirectory;
import io.sirix.settings.Constants;
import io.sirix.settings.VersioningType;
import it.unimi.dsi.fastutil.longs.Long2ObjectRBTreeMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

@ResourceLock(Resources.SYSTEM_PROPERTIES)
final class ProjectionColumnMajorStorageTest {
  private static final String RESOURCE = "resource";
  private static final String LAYOUT_PROPERTY = "sirix.projection.columnMajorSlots";
  private static final int GROUPS = 80;
  private static final int MAX_ID = ProjectionIndexHOTStorage.MAX_ROW_GROUPS;
  private static final byte[] KINDS = {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG,
      ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN, ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT};

  @TempDir
  Path temporaryDirectory;

  @AfterEach
  void clearCaches() {
    Databases.getGlobalBufferManager().clearAllCaches();
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void bothLayoutsKeepTheirOwnersAcrossMutationRollbackAndColdReopen(final VersioningType versioning) {
    final Path path = temporaryDirectory.resolve("layouts");
    final byte[][] initial = new byte[GROUPS + 1][];
    for (int i = 0; i < initial.length; i++) {
      initial[i] = rowGroup(i == 0 || i == 6
          ? 1024
          : i == 3
              ? 0
              : 32,
          i, 0);
    }
    final byte[][] second = initial.clone();
    second[0] = rowGroup(2, 0, 1);
    second[1] = null;
    final byte[][] third = initial.clone();
    third[0] = rowGroup(1024, 0, 2);
    third[1] = rowGroup(9, 1, 3);
    final byte[][][] revisions = {initial, second, third};
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(path)));
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(path)) {
      assertTrue(db.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                                        .versioningApproach(versioning)
                                                        .maxNumberOfRevisionsToRestore(2)
                                                        .build()));
      try (JsonResourceSession session = db.beginResourceSession(RESOURCE)) {
        try (JsonNodeTrx wtx = session.beginNodeTrx()) {
          for (final ProjectionSlotLayout layout : ProjectionSlotLayout.values()) {
            final ProjectionIndexHOTStorage storage = freshStorage(wtx, layout);
            storage.requireVirginTreeForInitialBuild();
            storage.putBlob(0, metadata(initial.length, 1).serialize());
            for (int i = 0; i < initial.length; i++) {
              storage.putRowGroupAsColumnSegmentSlots(id(i), ProjectionIndexColumnSegmentCodec.encode(initial[i]));
            }
          }
          wtx.commit();
        }
        for (int revision = 2; revision <= 3; revision++) {
          try (JsonNodeTrx wtx = session.beginNodeTrx()) {
            for (final ProjectionSlotLayout layout : ProjectionSlotLayout.values()) {
              final ProjectionIndexHOTStorage storage =
                  new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), layout.ordinal());
              assertEquals(layout, storage.slotLayout());
              storage.putRowGroupAsColumnSegmentSlots(1,
                  ProjectionIndexColumnSegmentCodec.encode(revisions[revision - 1][0]));
              if (revision == 2) {
                storage.tombstoneRowGroupAsColumnSegmentSlots(2);
              } else {
                storage.putRowGroupAsColumnSegmentSlots(2, ProjectionIndexColumnSegmentCodec.encode(third[1]));
              }
              storage.putBlob(0, metadata(revision == 2
                  ? GROUPS
                  : GROUPS + 1, revision).serialize());
              assertArrayEquals(revisions[revision - 1][0], storage.getRowGroupFromColumnSegmentSlots(1));
            }
            wtx.commit();
          }
        }
        try (JsonNodeTrx wtx = session.beginNodeTrx()) {
          for (final ProjectionSlotLayout layout : ProjectionSlotLayout.values()) {
            final ProjectionIndexHOTStorage storage =
                new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), layout.ordinal());
            storage.tombstoneRowGroupAsColumnSegmentSlots(1);
            storage.putRowGroupAsColumnSegmentSlots(MAX_ID,
                ProjectionIndexColumnSegmentCodec.encode(rowGroup(2, 99, 9)));
          }
          wtx.rollback();
        }
        assertEquals(3, session.getMostRecentRevisionNumber());
      }
    }

    Databases.getGlobalBufferManager().clearAllCaches();
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(path);
        JsonResourceSession session = db.beginResourceSession(RESOURCE)) {
      final int body = ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(0);
      for (final ProjectionSlotLayout layout : ProjectionSlotLayout.values()) {
        final long[] changedOffsets = new long[3];
        final long[] untouchedOffsets = new long[3];
        for (int revision = 1; revision <= 3; revision++) {
          try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision)) {
            final StorageEngineReader reader = rtx.getStorageEngineReader();
            final int index = layout.ordinal();
            final ProjectionIndexMetadata metadata =
                ProjectionIndexMetadata.parse(ProjectionIndexHOTStorage.readBlob(reader, index, 0));
            assertNotNull(metadata);
            assertEquals(layout, metadata.slotLayout());
            assertRevision(reader, index, layout, revisions[revision - 1]);
            changedOffsets[revision - 1] =
                ProjectionIndexHOTStorage.segmentPageOffset(reader, index, layout.segmentSlot(1, body), 0);
            untouchedOffsets[revision - 1] =
                ProjectionIndexHOTStorage.segmentPageOffset(reader, index, layout.segmentSlot(7, body), 0);
            if (layout == ProjectionSlotLayout.COLUMN_MAJOR) {
              assertLogicalFetch(session, reader, revision, revisions[revision - 1]);
            }
          }
        }
        assertTrue(changedOffsets[0] >= 0);
        assertTrue(changedOffsets[1] < 0, "shrinking the body removes its overflow owner");
        assertTrue(changedOffsets[2] >= 0);
        assertNotEquals(changedOffsets[0], changedOffsets[2]);
        assertTrue(untouchedOffsets[0] >= 0);
        assertEquals(untouchedOffsets[0], untouchedOffsets[1], "an untouched segment keeps its exact owner");
        assertEquals(untouchedOffsets[1], untouchedOffsets[2]);
      }
    }
  }

  private static void assertRevision(final StorageEngineReader reader, final int index,
      final ProjectionSlotLayout layout, final byte[][] expected) {
    final int[] order = order(expected);
    for (int i = 0; i < expected.length; i++) {
      assertArrayEquals(expected[i],
          ProjectionIndexHOTStorage.readRowGroupFromColumnSegmentSlots(reader, index, id(i)));
      assertEquals(expected[i] == null
          ? -1
          : i == 3
              ? 0
              : RowGroupDescriptor.rowCount(ProjectionIndexColumnSegmentCodec.encode(expected[i]).descriptor()),
          ProjectionIndexHOTStorage.readRowCountFromColumnSegmentSlots(reader, index, id(i)));
    }
    final List<byte[]> rows =
        ProjectionIndexHOTStorage.readAllRowGroupsFromColumnSegmentSlots(reader, index, order.length, order);
    assertEquals(order.length, rows.size());
    for (int i = 0; i < order.length; i++) {
      assertArrayEquals(expected[position(order[i])], rows.get(i));
    }
    final List<RowGroupDirectory> physical =
        ProjectionIndexHOTStorage.readAllRowGroupDirectoriesFromColumnSegmentSlots(reader, index, order.length, order);
    assertNotNull(physical);
    assertEquals(order.length, physical.size());
    assertFalse(physical.getFirst().logicalSlots());
    final Long2ObjectRBTreeMap<ProjectionIndexHOTStorage.RawBlobSlot> descriptors = new Long2ObjectRBTreeMap<>();
    final ArrayList<ProjectionIndexHOTStorage.RawBlobSlot> segments = new ArrayList<>();
    ProjectionIndexHOTStorage.collectSlotsRange(reader, index, order.length, 2, 5, descriptors, segments);
    assertEquals(expected[1] == null
        ? 3
        : 4, descriptors.size());
    assertTrue(segments.stream().allMatch(slot -> slot.rowGroupId() >= 2 && slot.rowGroupId() <= 5));
    assertNull(ProjectionIndexHOTStorage.readBlob(reader, index, (layout == ProjectionSlotLayout.COLUMN_MAJOR
        ? ProjectionSlotLayout.ROW_GROUP_MAJOR
        : ProjectionSlotLayout.COLUMN_MAJOR).descriptorSlot(1)), "the other namespace must remain absent");
  }

  private static void assertLogicalFetch(final JsonResourceSession session, final StorageEngineReader reader,
      final int revision, final byte[][] expected) {
    final int[] order = order(expected);
    final List<RowGroupDirectory> directories =
        ProjectionIndexHOTStorage.readColumnMajorDirectories(reader, 1, order.length, order);
    final AtomicInteger opened = new AtomicInteger();
    final AtomicInteger closed = new AtomicInteger();
    final List<RowGroupDirectory> parallel = ProjectionIndexHOTStorage.readColumnMajorDirectories(reader, 1,
        order.length, order, worker -> {
          try (JsonNodeReadOnlyTrx lane = session.beginNodeReadOnlyTrx(revision)) {
            opened.incrementAndGet();
            worker.accept(lane.getStorageEngineReader());
          } finally {
            closed.incrementAndGet();
          }
        }, 3);
    assertEquals(3, opened.get());
    assertEquals(opened.get(), closed.get());
    assertEquals(directories.size(), parallel.size());
    for (int i = 0; i < directories.size(); i++) {
      final RowGroupDirectory serial = directories.get(i);
      final RowGroupDirectory concurrent = parallel.get(i);
      assertEquals(serial.rowGroupId(), concurrent.rowGroupId());
      assertArrayEquals(serial.descriptor(), concurrent.descriptor());
      assertArrayEquals(serial.columnSegmentIds(), concurrent.columnSegmentIds());
      assertArrayEquals(serial.columnSegmentOffsets(), concurrent.columnSegmentOffsets());
      assertNull(concurrent.inlineColumnSegmentBytes());
      assertTrue(concurrent.logicalSlots());
    }
    final ProjectionColumnStore store = new ProjectionColumnStore(directories, 1);
    assertTrue(store.hasLogicalSlotSources());
    final StorageEngineReader prefetchReader = mock(StorageEngineReader.class);
    store.prefetchAllSegments(prefetchReader);
    verifyNoInteractions(prefetchReader);
    assertThrows(IllegalArgumentException.class, () -> new ProjectionColumnStore(directories));
    final ProjectionColumnStore.ColumnSegmentFetcher fetcher =
        ProjectionIndexCatalog.columnSegmentFetcher(session, revision);
    final byte[][] actual = store.columnBytes(0, ProjectionIndexCatalog.columnSegmentFetcher(session, revision));
    final int body = ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(0);
    for (int i = 0; i < order.length; i++) {
      final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded =
          ProjectionIndexColumnSegmentCodec.encode(expected[position(order[i])]);
      assertArrayEquals(encoded.segments()[RowGroupDescriptor.entryIndexOf(encoded.descriptor(), body)], actual[i]);
    }
    final ProjectionColumnStore.ColumnSlice[] strings = store.column(2, fetcher);
    final ProjectionColumnStore.LeafColumnAccess window = store.windowedLeafAccess(fetcher, null, 2, 2);
    for (int i = 0; i < order.length; i++) {
      assertEquals(strings[i].rowCount(), window.slice(2, i).rowCount());
      assertArrayEquals(strings[i].presenceWords(), window.slice(2, i).presenceWords());
      assertArrayEquals(strings[i].stringDictIds(), window.slice(2, i).stringDictIds());
      assertArrayEquals(store.recordKeys(fetcher)[i], window.recordKeys(i));
    }
    final long[] keys = {Constants.NULL_ID_LONG, ProjectionSlotLayout.COLUMN_MAJOR.segmentSlot(MAX_ID, body),
        ProjectionSlotLayout.COLUMN_MAJOR.segmentSlot(MAX_ID - 1L, body),
        ProjectionSlotLayout.COLUMN_MAJOR.segmentSlot(1, body)};
    final byte[][] out = new byte[keys.length][];
    ProjectionIndexHOTStorage.readColumnSlotRange(reader, 1, keys, 0, keys.length, out);
    assertNull(out[0]);
    assertNotNull(out[1]);
    assertNull(out[2]);
    assertNotNull(out[3]);
    assertThrows(IllegalArgumentException.class, () -> ProjectionIndexHOTStorage.readColumnSlotRange(reader, 1,
        new long[] {keys[1], keys[1]}, 0, 2, new byte[2][]));
    assertThrows(IllegalArgumentException.class,
        () -> ProjectionIndexHOTStorage.readColumnSlotRange(reader, 0, keys, 0, keys.length, out));
  }

  @Test
  void partitionedDirectoriesRejectGapsOrphansAndCorruptionAndCloseTheirReaders() {
    final Path path = temporaryDirectory.resolve("partition-failures");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(path)));
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(path)) {
      assertTrue(db.createResource(ResourceConfiguration.newBuilder(RESOURCE).build()));
      try (JsonResourceSession session = db.beginResourceSession(RESOURCE)) {
        try (JsonNodeTrx wtx = session.beginNodeTrx()) {
          final ProjectionIndexHOTStorage storage = freshStorage(wtx, ProjectionSlotLayout.COLUMN_MAJOR);
          storage.putBlob(0, metadata(4, 1).serialize());
          for (final int id : new int[] {2, 4, 6, 8}) {
            storage.putRowGroupAsColumnSegmentSlots(id,
                ProjectionIndexColumnSegmentCodec.encode(rowGroup(16, id, 0)));
          }
          wtx.commit();
        }
        final int[][] wrongOrders = {
            {8, 4, 6}, // orphan before the first expected key
            {6, 2, 4}, // orphan after the last expected key
            {8, 2, 6}, // orphan in a gap
            {8, 2, 6, 9}, // one missing key and one unexpected key; same total count
            {8, 2, 4, 4}}; // duplicate order entry
        for (final int[] order : wrongOrders) {
          assertDirectoryReadFailsAndCloses(session, 1, order);
        }
        try (JsonNodeTrx wtx = session.beginNodeTrx()) {
          final ProjectionIndexHOTStorage storage = new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), 1);
          // Valid PIXB checksum, malformed descriptor: corruption must be caught after capture.
          storage.putBlob(ProjectionSlotLayout.COLUMN_MAJOR.descriptorSlot(4), new byte[] {1, 2, 3});
          wtx.commit();
        }
        assertDirectoryReadFailsAndCloses(session, 2, new int[] {8, 2, 6, 4});
        try (JsonNodeReadOnlyTrx historical = session.beginNodeReadOnlyTrx(1)) {
          final List<RowGroupDirectory> intact = ProjectionIndexHOTStorage.readColumnMajorDirectories(
              historical.getStorageEngineReader(), 1, 4, new int[] {8, 2, 6, 4}, worker -> {
                try (JsonNodeReadOnlyTrx lane = session.beginNodeReadOnlyTrx(1)) {
                  worker.accept(lane.getStorageEngineReader());
                }
              }, 3);
          assertEquals(8, intact.getFirst().rowGroupId(), "the historical logical order remains intact");
        }
      }
    }
  }

  private static void assertDirectoryReadFailsAndCloses(final JsonResourceSession session, final int revision,
      final int[] order) {
    final AtomicInteger opened = new AtomicInteger();
    final AtomicInteger closed = new AtomicInteger();
    try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision)) {
      assertThrows(IllegalStateException.class, () -> ProjectionIndexHOTStorage.readColumnMajorDirectories(
          rtx.getStorageEngineReader(), 1, order.length, order, worker -> {
            try (JsonNodeReadOnlyTrx lane = session.beginNodeReadOnlyTrx(revision)) {
              opened.incrementAndGet();
              worker.accept(lane.getStorageEngineReader());
            } finally {
              closed.incrementAndGet();
            }
          }, 3));
      assertEquals(opened.get(), closed.get(), "all readers must be closed before failure returns");
    }
  }

  private static ProjectionIndexHOTStorage freshStorage(final JsonNodeTrx wtx, final ProjectionSlotLayout layout) {
    final String previous = System.getProperty(LAYOUT_PROPERTY);
    try {
      System.setProperty(LAYOUT_PROPERTY, Boolean.toString(layout == ProjectionSlotLayout.COLUMN_MAJOR));
      return ProjectionIndexHOTStorage.forBulkBuild(wtx.getStorageEngineWriter(), layout.ordinal());
    } finally {
      if (previous == null) {
        System.clearProperty(LAYOUT_PROPERTY);
      } else {
        System.setProperty(LAYOUT_PROPERTY, previous);
      }
    }
  }

  private static ProjectionIndexMetadata metadata(final int count, final int revision) {
    return new ProjectionIndexMetadata("/[]", new String[] {"/[]/n", "/[]/b", "/[]/s"}, new String[] {"n", "b", "s"},
        KINDS, count, revision);
  }

  private static int id(final int position) {
    return position == GROUPS
        ? MAX_ID
        : position + 1;
  }

  private static int position(final int id) {
    return id == MAX_ID
        ? GROUPS
        : id - 1;
  }

  private static int[] order(final byte[][] rows) {
    final int[] result = new int[(int) Arrays.stream(rows).filter(row -> row != null).count()];
    int count = 0;
    // Reversed physical order proves that array positions do not come from numeric ids or trie order.
    for (int i = rows.length - 1; i >= 0; i--) {
      if (rows[i] != null) {
        result[count++] = id(i);
      }
    }
    return result;
  }

  private static byte[] rowGroup(final int rows, final int seed, final int bump) {
    final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(KINDS);
    final Random random = new Random(seed);
    final long[] numbers = new long[3];
    final boolean[] booleans = new boolean[3];
    final String[] strings = new String[3];
    final boolean[] present = {true, true, seed % 5 != 0};
    final boolean[] clean = new boolean[3];
    long key = (seed + 1L) * 10_000_000_000L;
    for (int row = 0; row < rows; row++) {
      key += 1 + random.nextInt(1 << 20);
      numbers[0] = random.nextInt(1 << 20) + bump;
      booleans[1] = (row & 1) == 0;
      strings[2] = "Department-" + row % 8;
      assertTrue(page.appendRow(key, numbers, booleans, strings, present, clean, clean));
    }
    return page.serialize();
  }
}
