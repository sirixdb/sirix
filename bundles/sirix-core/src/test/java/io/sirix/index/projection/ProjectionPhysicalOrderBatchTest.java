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
import io.sirix.io.bytepipe.ByteHandlerPipeline;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

final class ProjectionPhysicalOrderBatchTest {
  private static final int GROUPS = 65 * ProjectionIndexFences.CHUNK_LEAVES;
  private static final int ENTRY_BYTES = 244;
  private static final int NEXT = 16;
  private static final int PREVIOUS = 20;
  private static final int OWNER = 24;

  @TempDir
  Path directory;

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void committedPermutationsAndHistoryUseBoundedBatches(final VersioningType versioning) {
    final Path path = create(versioning);
    final int[] original = order(GROUPS, 0);
    final int[] rotated = order(GROUPS, GROUPS / 2);
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path);
        JsonResourceSession session = database.beginResourceSession("resource")) {
      populate(session, GROUPS);
      try (JsonNodeTrx writer = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage = new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0);
        // The document-link reader must follow links, independent of physical chunk order.
        final int[] predecessor = new int[GROUPS + 1];
        final int[] successor = new int[GROUPS + 1];
        for (int i = 0; i < GROUPS; i++) {
          predecessor[rotated[i]] = i == 0
              ? 0
              : rotated[i - 1];
          successor[rotated[i]] = i + 1 == GROUPS
              ? 0
              : rotated[i + 1];
        }
        for (int chunkId = 0; chunkId < ProjectionIndexFences.chunkCount(GROUPS); chunkId++) {
          final long slot = ProjectionIndexFences.CHUNK_SLOT_BASE + chunkId;
          final byte[] chunk = storage.getBlob(slot).clone();
          for (int local = 0; local < ProjectionIndexFences.CHUNK_LEAVES; local++) {
            final int physical = chunkId * ProjectionIndexFences.CHUNK_LEAVES + local + 1;
            put(chunk, local * ENTRY_BYTES + PREVIOUS, predecessor[physical]);
            put(chunk, local * ENTRY_BYTES + NEXT, successor[physical]);
          }
          storage.putBlob(slot, chunk);
        }
        final byte[] header = storage.getBlob(ProjectionIndexFences.ORDER_HEADER_SLOT).clone();
        put(header, 24, rotated[0]);
        put(header, 28, rotated[GROUPS - 1]);
        put(header, 32, rotated[GROUPS - 1]);
        storage.putBlob(ProjectionIndexFences.ORDER_HEADER_SLOT, header);
        assertArrayEquals(rotated, ProjectionIndexFences.readPhysicalOrder(storage, GROUPS));
        writer.commit();
      }
    }
    Databases.clearGlobalCaches();
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path);
        JsonResourceSession session = database.beginResourceSession("resource")) {
      for (int revision = 1; revision <= 2; revision++) {
        try (JsonNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx(revision)) {
          final StorageEngineReader actual = trx.getStorageEngineReader();
          final AtomicInteger batches = new AtomicInteger();
          final StorageEngineReader counted = counted(actual, batches);
          final int[] expected = revision == 1
              ? original
              : rotated;
          assertArrayEquals(expected, ProjectionIndexFences.readPhysicalOrder(counted, 0, GROUPS, true));
          assertEquals(2, batches.get(), "65 full chunks must use bounded windows of 64 and 1");
          assertArrayEquals(expected, ProjectionIndexFences.readPhysicalOrder(actual, 0, GROUPS, false));
        }
      }
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2, 3, 4, 5})
  void batchingRetainsLinkOwnerLengthAndMissingChunkChecks(final int corruption) {
    final Path path = create(VersioningType.FULL);
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path);
        JsonResourceSession session = database.beginResourceSession("resource")) {
      populate(session, GROUPS);
      try (JsonNodeTrx writer = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage = new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0);
        final int chunkId = 32;
        final long slot = ProjectionIndexFences.CHUNK_SLOT_BASE + chunkId;
        byte[] chunk = storage.getBlob(slot).clone();
        switch (corruption) {
          case 0 -> put(chunk, OWNER, 0);
          case 1 -> put(chunk, PREVIOUS, 0);
          case 2 -> put(chunk, NEXT, GROUPS + 1);
          case 3 -> put(chunk, NEXT, chunkId * ProjectionIndexFences.CHUNK_LEAVES + 1);
          case 4 -> chunk = Arrays.copyOf(chunk, chunk.length - 1);
          case 5 -> chunk = new byte[0];
          default -> throw new AssertionError(corruption);
        }
        // putBlob recomputes the outer payload hash; these are semantic corruption checks.
        storage.putBlob(slot, chunk);
        writer.commit();
      }
      try (JsonNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx()) {
        assertThrows(IllegalStateException.class,
            () -> ProjectionIndexFences.readPhysicalOrder(trx.getStorageEngineReader(), 0, GROUPS, false));
        assertThrows(IllegalStateException.class,
            () -> ProjectionIndexFences.readPhysicalOrder(trx.getStorageEngineReader(), 0, GROUPS, true));
      }
    }
  }

  @Test
  void sparseOrderDoesNotReadAnUnusedMissingPhysicalChunk() {
    final Path path = create(VersioningType.FULL);
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path);
        JsonResourceSession session = database.beginResourceSession("resource")) {
      populate(session, GROUPS + 1);
      try (JsonNodeTrx writer = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage = new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0);
        final long liveChunkSlot = ProjectionIndexFences.CHUNK_SLOT_BASE + 64;
        final byte[] liveChunk = storage.getBlob(liveChunkSlot).clone();
        put(liveChunk, (ProjectionIndexFences.CHUNK_LEAVES - 1) * ENTRY_BYTES + NEXT, 0);
        storage.putBlob(liveChunkSlot, liveChunk);
        final byte[] header = storage.getBlob(ProjectionIndexFences.ORDER_HEADER_SLOT).clone();
        put(header, 16, GROUPS);
        put(header, 20, GROUPS + 1);
        put(header, 28, GROUPS);
        put(header, 32, GROUPS);
        storage.putBlob(ProjectionIndexFences.ORDER_HEADER_SLOT, header);
        storage.putBlob(ProjectionIndexFences.CHUNK_SLOT_BASE + 65, new byte[0]);
        writer.commit();
      }
      try (JsonNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx()) {
        final AtomicInteger batches = new AtomicInteger();
        assertArrayEquals(order(GROUPS, 0),
            ProjectionIndexFences.readPhysicalOrder(counted(trx.getStorageEngineReader(), batches), 0, GROUPS, true));
        assertEquals(0, batches.get());
      }
    }
  }

  private Path create(final VersioningType versioning) {
    final Path path = directory.resolve("database");
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
    final long[] first = new long[groups];
    final long[] last = new long[groups];
    for (int i = 0; i < groups; i++) {
      first[i] = 10L * i;
      last[i] = first[i] + 5;
    }
    try (JsonNodeTrx writer = session.beginNodeTrx()) {
      ProjectionIndexFences.write(new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0), groups, first,
          last);
      writer.commit();
    }
  }

  private static StorageEngineReader counted(final StorageEngineReader actual, final AtomicInteger batches) {
    final StorageEngineReader counted = mock(StorageEngineReader.class, delegatesTo(actual));
    doAnswer(invocation -> {
      final long[] offsets = invocation.getArgument(0);
      assertTrue(offsets.length <= 64);
      batches.incrementAndGet();
      return actual.readSideOverflowPageBatch(offsets);
    }).when(counted).readSideOverflowPageBatch(any(long[].class));
    return counted;
  }

  private static int[] order(final int groups, final int pivot) {
    final int[] order = new int[groups];
    for (int i = 0; i < groups; i++) {
      order[i] = (i + pivot) % groups + 1;
    }
    return order;
  }

  private static void put(final byte[] bytes, final int offset, final int value) {
    ProjectionIndexRowGroupCodec.putIntLEAt(bytes, offset, value);
  }
}
