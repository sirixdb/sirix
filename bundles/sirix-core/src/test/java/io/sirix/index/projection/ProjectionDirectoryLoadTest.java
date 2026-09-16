package io.sirix.index.projection;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.projection.ProjectionDirectoryLoad.Result;
import io.sirix.index.projection.ProjectionIndexHOTStorage.ParallelWalkReaders;
import io.sirix.index.projection.ProjectionIndexHOTStorage.RowGroupDirectory;
import io.sirix.io.StorageType;
import io.sirix.io.bytepipe.ByteHandlerPipeline;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.never;

@ResourceLock(Resources.SYSTEM_PROPERTIES)
final class ProjectionDirectoryLoadTest {
  private static final int GROUPS = 2048;
  private static final int ENTRY_BYTES = 244;
  private static final byte[] KINDS = {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG};
  private static final String LAYOUT_PROPERTY = "sirix.projection.columnMajorSlots";

  @TempDir
  Path temporary;

  @AfterEach
  void clearCaches() {
    Databases.clearGlobalCaches();
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void coldHistoricalRevisionsRetainReorderedDescriptorsAndSparseFallback(final VersioningType versioning) {
    final Path path = create(versioning);
    final int[] original = order(GROUPS, 0);
    final int[] rotated = order(GROUPS, GROUPS / 2);
    final int[] sparse = Arrays.copyOf(rotated, rotated.length - 1);
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path);
        JsonResourceSession session = database.beginResourceSession("resource")) {
      populate(session, GROUPS);
      try (JsonNodeTrx writer = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage = new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0);
        setOrder(storage, rotated);
        storage.putRowGroupAsColumnSegmentSlots(1, ProjectionIndexColumnSegmentCodec.encode(rowGroup(1, 99)));
        storage.putBlob(0, metadata(GROUPS, 2).serialize());
        writer.commit();
      }
      try (JsonNodeTrx writer = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage = new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0);
        setOrder(storage, sparse);
        storage.tombstoneRowGroupAsColumnSegmentSlots(rotated[rotated.length - 1]);
        storage.putBlob(0, metadata(sparse.length, 3).serialize());
        assertFalse(ProjectionIndexFences.hasBoundedDenseOrder(writer.getStorageEngineReader(), 0, sparse.length));
        writer.commit();
      }
    }
    Databases.clearGlobalCaches();
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path);
        JsonResourceSession session = database.beginResourceSession("resource")) {
      for (int revision = 1; revision <= 3; revision++) {
        final int readRevision = revision;
        final int[] expected = revision == 1
            ? original
            : revision == 2
                ? rotated
                : sparse;
        final AtomicInteger opened = new AtomicInteger();
        final AtomicInteger closed = new AtomicInteger();
        try (JsonNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx(revision)) {
          final StorageEngineReader reader = trx.getStorageEngineReader();
          final ParallelWalkReaders leases = worker -> {
            try (JsonNodeReadOnlyTrx lane = session.beginNodeReadOnlyTrx(readRevision)) {
              opened.incrementAndGet();
              assertNotSame(reader, lane.getStorageEngineReader());
              worker.accept(lane.getStorageEngineReader());
            } finally {
              closed.incrementAndGet();
            }
          };
          final Result overlapped = ProjectionDirectoryLoad.read(reader, 0, expected.length, leases, true);
          assertArrayEquals(expected, overlapped.physicalOrder());
          assertEquals(opened.get(), closed.get());
          if (revision == 3) {
            assertEquals(0, opened.get(), "a sparse revision does not start a dense-load worker");
          }
          final Result serial = ProjectionDirectoryLoad.read(reader, 0, expected.length, null, false);
          assertEquivalent(serial, overlapped);
          for (int i = 0; i < expected.length; i++) {
            final int id = expected[i];
            final byte[] encoded = ProjectionIndexColumnSegmentCodec.encode(rowGroup(id, revision >= 2 && id == 1
                ? 99
                : 0)).descriptor();
            assertArrayEquals(encoded, overlapped.directories().get(i).descriptor());
          }
        }
      }
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2, 3, 4, 5})
  void corruptLinksDescriptorsHolesAndOrphansNeverLeakWorkers(final int corruption) {
    final Path path = create(VersioningType.FULL);
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path);
        JsonResourceSession session = database.beginResourceSession("resource")) {
      populate(session, GROUPS);
      try (JsonNodeTrx writer = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage = new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0);
        if (corruption == 0 || corruption == 1 || corruption == 5) {
          final long slot = ProjectionIndexFences.CHUNK_SLOT_BASE + 16;
          byte[] bytes = storage.getBlob(slot).clone();
          if (corruption == 0)
            bytes = Arrays.copyOf(bytes, bytes.length - 1);
          else
            put(bytes, 16, GROUPS + 1);
          storage.putBlob(slot, bytes);
        }
        if (corruption == 2 || corruption == 5) {
          storage.putBlob(ProjectionSlotLayout.COLUMN_MAJOR.descriptorSlot(1000), new byte[] {1, 2, 3});
        } else if (corruption == 3) {
          storage.putRowGroupAsColumnSegmentSlots(GROUPS + 1,
              ProjectionIndexColumnSegmentCodec.encode(rowGroup(GROUPS + 1, 0)));
        } else if (corruption == 4) {
          storage.tombstoneRowGroupAsColumnSegmentSlots(1000);
        }
        writer.commit();
      }
      for (final boolean overlap : new boolean[] {false, true}) {
        final AtomicInteger opened = new AtomicInteger();
        final AtomicInteger closed = new AtomicInteger();
        try (JsonNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx(2)) {
          assertThrows(IllegalStateException.class,
              () -> ProjectionDirectoryLoad.read(trx.getStorageEngineReader(), 0, GROUPS, worker -> {
                try (JsonNodeReadOnlyTrx lane = session.beginNodeReadOnlyTrx(2)) {
                  opened.incrementAndGet();
                  worker.accept(lane.getStorageEngineReader());
                } finally {
                  closed.incrementAndGet();
                }
              }, overlap));
          assertEquals(opened.get(), closed.get(), "failure must join and close every started reader");
        }
      }
      try (JsonNodeReadOnlyTrx historical = session.beginNodeReadOnlyTrx(1)) {
        final Result intact = ProjectionDirectoryLoad.read(historical.getStorageEngineReader(), 0, GROUPS, worker -> {
          try (JsonNodeReadOnlyTrx lane = session.beginNodeReadOnlyTrx(1)) {
            worker.accept(lane.getStorageEngineReader());
          }
        }, true);
        assertArrayEquals(order(GROUPS, 0), intact.physicalOrder());
      }
    }
  }

  @Test
  void denseReadOverlapsFenceInputWithAnIndependentReader() {
    if (Runtime.getRuntime().availableProcessors() < 2)
      return;
    final Path path = create(VersioningType.FULL);
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path);
        JsonResourceSession session = database.beginResourceSession("resource")) {
      populate(session, GROUPS);
      try (JsonNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx(1)) {
        final StorageEngineReader actual = trx.getStorageEngineReader();
        final StorageEngineReader reader = mock(StorageEngineReader.class, delegatesTo(actual));
        final CountDownLatch directoryStarted = new CountDownLatch(1);
        final CountDownLatch fenceStarted = new CountDownLatch(1);
        final AtomicInteger closed = new AtomicInteger();
        final AtomicInteger opened = new AtomicInteger();
        doAnswer(invocation -> {
          await(directoryStarted);
          fenceStarted.countDown();
          return actual.readSideOverflowPageBatch(invocation.getArgument(0));
        }).when(reader).readSideOverflowPageBatch(any(long[].class));
        final String priorBatch = System.getProperty("sirix.projection.batchPhysicalOrder");
        try {
          System.setProperty("sirix.projection.batchPhysicalOrder", "true");
          final Result result = ProjectionDirectoryLoad.read(reader, 0, GROUPS, worker -> {
            try (JsonNodeReadOnlyTrx lane = session.beginNodeReadOnlyTrx(1)) {
              opened.incrementAndGet();
              assertNotSame(actual, lane.getStorageEngineReader());
              directoryStarted.countDown();
              await(fenceStarted);
              worker.accept(lane.getStorageEngineReader());
            } finally {
              closed.incrementAndGet();
            }
          }, true);
          assertArrayEquals(order(GROUPS, 0), result.physicalOrder());
          assertTrue(opened.get() > 0);
          assertEquals(opened.get(), closed.get());
          assertEquals(0, fenceStarted.getCount());
        } finally {
          if (priorBatch == null)
            System.clearProperty("sirix.projection.batchPhysicalOrder");
          else
            System.setProperty("sirix.projection.batchPhysicalOrder", priorBatch);
        }
      }
    }
  }

  @Test
  void writerSmallAndOversizedViewsCannotStartDenseLoading() {
    final StorageEngineReader reader = mock(StorageEngineReader.class);
    assertFalse(ProjectionIndexFences.hasBoundedDenseOrder(reader, 0, 1023));
    assertFalse(ProjectionIndexFences.hasBoundedDenseOrder(reader, 0, (1 << 20) + 1));
    when(reader.hasTrxIntentLog()).thenReturn(true);
    assertFalse(ProjectionIndexFences.hasBoundedDenseOrder(reader, 0, GROUPS));
    verify(reader, never()).readSideOverflowPageBatch(any(long[].class));
    assertThrows(NullPointerException.class, () -> ProjectionIndexFences.hasBoundedDenseOrder(null, 0, GROUPS));
    assertThrows(IllegalArgumentException.class, () -> ProjectionIndexFences.hasBoundedDenseOrder(reader, 0, -1));
  }

  private static void await(final CountDownLatch latch) {
    try {
      assertTrue(latch.await(10, TimeUnit.SECONDS), "independent input lane did not start");
    } catch (final InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new AssertionError(interrupted);
    }
  }

  private static void assertEquivalent(final Result expected, final Result actual) {
    assertArrayEquals(expected.physicalOrder(), actual.physicalOrder());
    assertEquals(expected.directories().size(), actual.directories().size());
    for (int i = 0; i < expected.directories().size(); i++) {
      final RowGroupDirectory a = expected.directories().get(i);
      final RowGroupDirectory b = actual.directories().get(i);
      assertEquals(a.rowGroupId(), b.rowGroupId());
      assertArrayEquals(a.descriptor(), b.descriptor());
      assertArrayEquals(a.columnSegmentIds(), b.columnSegmentIds());
      assertArrayEquals(a.columnSegmentOffsets(), b.columnSegmentOffsets());
    }
  }

  private Path create(final VersioningType versioning) {
    final Path path = temporary.resolve("database");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(path)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource")
                                                              .storageType(StorageType.FILE_CHANNEL)
                                                              .byteHandlerPipeline(new ByteHandlerPipeline())
                                                              .versioningApproach(versioning)
                                                              .maxNumberOfRevisionsToRestore(2)
                                                              .build()));
    }
    return path;
  }

  private static void populate(final JsonResourceSession session, final int groups) {
    final String previous = System.getProperty(LAYOUT_PROPERTY);
    try (JsonNodeTrx writer = session.beginNodeTrx()) {
      System.setProperty(LAYOUT_PROPERTY, "true");
      final ProjectionIndexHOTStorage storage =
          ProjectionIndexHOTStorage.forBulkBuild(writer.getStorageEngineWriter(), 0);
      storage.putBlob(0, metadata(groups, 1).serialize());
      final long[] first = new long[groups];
      for (int i = 0; i < groups; i++) {
        first[i] = 10L * (i + 1);
        storage.putRowGroupAsColumnSegmentSlots(i + 1, ProjectionIndexColumnSegmentCodec.encode(rowGroup(i + 1, 0)));
      }
      ProjectionIndexFences.write(storage, groups, first, first);
      writer.commit();
    } finally {
      if (previous == null)
        System.clearProperty(LAYOUT_PROPERTY);
      else
        System.setProperty(LAYOUT_PROPERTY, previous);
    }
  }

  private static ProjectionIndexMetadata metadata(final int groups, final int revision) {
    return new ProjectionIndexMetadata("/[]", new String[] {"/[]/n"}, new String[] {"n"}, KINDS, groups,
        revision).withSlotLayout(ProjectionSlotLayout.COLUMN_MAJOR);
  }

  private static byte[] rowGroup(final int id, final int bump) {
    final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(KINDS);
    assertTrue(page.appendRow(10L * id, new long[] {id + bump}, new boolean[1], new String[1], new boolean[] {true},
        new boolean[1], new boolean[1]));
    return page.serialize();
  }

  private static int[] order(final int groups, final int pivot) {
    final int[] order = new int[groups];
    for (int i = 0; i < groups; i++)
      order[i] = (i + pivot) % groups + 1;
    return order;
  }

  private static void setOrder(final ProjectionIndexHOTStorage storage, final int[] order) {
    final int[] previous = new int[GROUPS + 1];
    final int[] next = new int[GROUPS + 1];
    for (int i = 0; i < order.length; i++) {
      previous[order[i]] = i == 0
          ? 0
          : order[i - 1];
      next[order[i]] = i + 1 == order.length
          ? 0
          : order[i + 1];
    }
    for (int chunkId = 0; chunkId < ProjectionIndexFences.chunkCount(GROUPS); chunkId++) {
      final long slot = ProjectionIndexFences.CHUNK_SLOT_BASE + chunkId;
      final byte[] bytes = storage.getBlob(slot).clone();
      for (int local = 0; local < ProjectionIndexFences.CHUNK_LEAVES; local++) {
        final int id = chunkId * ProjectionIndexFences.CHUNK_LEAVES + local + 1;
        put(bytes, local * ENTRY_BYTES + 16, next[id]);
        put(bytes, local * ENTRY_BYTES + 20, previous[id]);
      }
      storage.putBlob(slot, bytes);
    }
    final byte[] header = storage.getBlob(ProjectionIndexFences.ORDER_HEADER_SLOT).clone();
    put(header, 16, order.length);
    put(header, 24, order[0]);
    put(header, 28, order[order.length - 1]);
    put(header, 32, order[order.length - 1]);
    storage.putBlob(ProjectionIndexFences.ORDER_HEADER_SLOT, header);
  }

  private static void put(final byte[] bytes, final int offset, final int value) {
    ProjectionIndexRowGroupCodec.putIntLEAt(bytes, offset, value);
  }
}
