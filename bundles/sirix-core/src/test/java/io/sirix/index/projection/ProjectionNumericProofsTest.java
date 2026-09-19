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
import io.sirix.index.projection.ProjectionColumnStore.ColumnSegmentFetcher;
import io.sirix.index.projection.ProjectionColumnStore.ColumnSlice;
import io.sirix.index.projection.ProjectionIndexColumnSegmentCodec.EncodedRowGroup;
import io.sirix.index.projection.ProjectionIndexColumnSegmentCodec.NumericBucketDecoder;
import io.sirix.index.projection.ProjectionIndexHOTStorage.RowGroupDirectory;
import io.sirix.io.StorageType;
import io.sirix.settings.Constants;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

@ResourceLock(Resources.SYSTEM_PROPERTIES)
final class ProjectionNumericProofsTest {
  private static final byte LONG = ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG;
  @TempDir
  Path directory;

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void bucketViewReadsOnlyResidualBodiesAndNeverLeaksRepresentatives(final boolean missingProof) {
    final List<EncodedRowGroup> groups = new ArrayList<>();
    for (int group = 1; group <= 128; group++) {
      groups.add(encoded(group, 1024, (group % 6 - 3) * 1000L, group == 65));
    }
    final Map<Long, byte[]> proofs = buildProofs(groups);
    if (missingProof) {
      proofs.remove(ProjectionNumericProofs.slot(0, 0));
    }
    // A changed BODY invalidates only its own proof entry. Physical directory order is unrelated
    // to numeric slot order, as after a mid-document split.
    groups.set(1, encoded(2, 1024, 7000, false));
    final AtomicInteger fetched = new AtomicInteger();
    final List<RowGroupDirectory> directories = new ArrayList<>();
    final Map<Long, byte[]> sources = new HashMap<>();
    for (int ordinal = 0; ordinal < groups.size(); ordinal++) {
      final int physical = (ordinal + 64) % 128;
      final EncodedRowGroup encoded = groups.get(physical);
      final long[] offsets = new long[encoded.segments().length];
      for (int entry = 0; entry < offsets.length; entry++) {
        offsets[entry] = (physical + 1L) * 100 + entry;
        sources.put(offsets[entry], encoded.segments()[entry]);
      }
      directories.add(new RowGroupDirectory(physical + 1L, encoded.descriptor(), encoded.columnSegmentIds(), offsets,
          new byte[offsets.length][]));
    }
    final ColumnSegmentFetcher fetcher = new ColumnSegmentFetcher() {
      @Override
      public byte[][] fetchAll(final long[] offsets) {
        final byte[][] result = new byte[offsets.length][];
        for (int i = 0; i < offsets.length; i++) {
          if (offsets[i] != Constants.NULL_ID_LONG) {
            result[i] = sources.get(offsets[i]);
            assertNotNull(result[i]);
            fetched.incrementAndGet();
          }
        }
        return result;
      }

      @Override
      public byte[][] fetchNumericProofs(final int index, final int column, final long[] slots) {
        assertEquals(0, index);
        assertEquals(0, column);
        final byte[][] result = new byte[slots.length][];
        for (int i = 0; i < slots.length; i++)
          result[i] = proofs.get(slots[i]);
        return result;
      }
    };
    final ProjectionColumnStore store = new ProjectionColumnStore(directories, 0);
    final ColumnSlice[] actual = store.numericBucketKeyColumn(0, fetcher, 7, 1000, 24);
    assertEquals(missingProof
        ? 65
        : 2, fetched.get(), "only missing, stale and sparse leaves may read a BODY");
    for (int ordinal = 0; ordinal < actual.length; ordinal++) {
      final int physical = (ordinal + 64) % 128;
      final EncodedRowGroup encoded = groups.get(physical);
      final ColumnSlice truth = ProjectionIndexColumnSegmentCodec.decodeBodySlice(encoded.descriptor(),
          encoded.segments()[Arrays.binarySearch(encoded.columnSegmentIds(),
              ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(0))],
          0);
      assertArrayEquals(truth.presenceWords(), actual[ordinal].presenceWords());
      for (int row = 0; row < truth.rowCount(); row++) {
        if ((truth.presenceWords()[row >>> 6] & 1L << (row & 63)) != 0) {
          assertEquals(((truth.numericValues()[row] + 7) / 1000) % 24,
              ((actual[ordinal].numericValues()[row] + 7) / 1000) % 24);
        }
      }
    }
    assertFalse(store.columnFilled(0));
    final ColumnSlice[] ordinary = store.column(0, fetcher);
    assertEquals((missingProof
        ? 65
        : 2) + 128, fetched.get());
    assertEquals(2000, ordinary[0].numericValues()[0]);
    assertEquals(2001, ordinary[0].numericValues()[1]);
    assertSame(ordinary, store.numericBucketKeyColumn(0, fetcher, 0, 2, 0));
    final int sparseBody = Arrays.binarySearch(groups.get(64).columnSegmentIds(),
        ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(0));
    final long sparseOffset = 6500L + sparseBody;
    final byte[] corruptBody = sources.get(sparseOffset).clone();
    corruptBody[corruptBody.length - 1] ^= 1;
    sources.put(sparseOffset, corruptBody);
    final ProjectionColumnStore damaged = new ProjectionColumnStore(directories, 0);
    assertThrows(IllegalStateException.class, () -> damaged.numericBucketKeyColumn(0, fetcher, 7, 1000, 24));
    assertTrue(damaged.columnKnownCorrupt(0), "residual BODY failures retain the ordinary corruption memo");
  }

  @Test
  void proofsRequireSourceIntegrityAndRejectMalformedEvidence() {
    final EncodedRowGroup encoded = encoded(1, 1024, 1000, false);
    final Map<Long, byte[]> blobs = buildProofs(List.of(encoded));
    final byte[] source = blobs.get(ProjectionNumericProofs.slot(0, 0));
    final ProjectionNumericProofs.Chunk chunk = new ProjectionNumericProofs.Chunk(source, 0, 0);
    final NumericBucketDecoder decoder = new NumericBucketDecoder(0, 1000, 24);
    assertNotNull(chunk.decode(encoded.descriptor(), 1, decoder));
    assertNull(chunk.decode(encoded(1, 1023, 1000, false).descriptor(), 1, decoder));
    assertNull(chunk.decode(encoded(1, 1024, 2000, false).descriptor(), 1, decoder));
    final byte[] broken = source.clone();
    broken[0] ^= 1;
    assertThrows(IllegalStateException.class, () -> new ProjectionNumericProofs.Chunk(broken, 0, 0));
    final byte[] mismatched = source.clone();
    ProjectionIndexRowGroupCodec.putLongLEAt(mismatched, 24 + 8, 999);
    final ProjectionNumericProofs.Chunk bad = new ProjectionNumericProofs.Chunk(mismatched, 0, 0);
    assertThrows(IllegalStateException.class, () -> bad.decode(encoded.descriptor(), 1, decoder));
    assertThrows(IllegalStateException.class, () -> new ProjectionNumericProofs.Chunk(source, 0, 1));
    assertThrows(IllegalStateException.class, () -> chunk.decode(encoded.descriptor(), 65, decoder));
    final int body =
        Arrays.binarySearch(encoded.columnSegmentIds(), ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(0));
    encoded.segments()[body][encoded.segments()[body].length - 1] ^= 1;
    assertThrows(IllegalStateException.class, () -> buildProofs(List.of(encoded)));
    assertFalse(ProjectionNumericProofs.available(null, 0));
    assertThrows(IllegalStateException.class, () -> ProjectionNumericProofs.available(new byte[12], 0));
    assertTrue(ProjectionNumericProofs.slot(RowGroupDescriptor.MAX_COLUMNS - 1,
        ProjectionNumericProofs.CHUNKS_PER_COLUMN - 1) < ProjectionNumericProofs.HEADER_SLOT);
    assertThrows(IllegalArgumentException.class, () -> ProjectionNumericProofs.chunk(0));
    assertThrows(IllegalArgumentException.class, () -> ProjectionNumericProofs.slot(RowGroupDescriptor.MAX_COLUMNS, 0));
  }

  @Test
  void signedQuotientsOverflowAndRepresentativeCapacityRetainTheFallback() {
    final NumericBucketDecoder decoder = new NumericBucketDecoder(0, 1000, 0);
    for (int group = 0; group < 256; group++) {
      assertNotNull(decoder.decodeFullPresenceProof(1024, group * 1000L, group * 1000L + 99));
    }
    assertNull(decoder.decodeFullPresenceProof(1024, 256000, 256099));
    assertNull(new NumericBucketDecoder(1, 10, 24).decodeFullPresenceProof(3, Long.MAX_VALUE - 1, Long.MAX_VALUE));
    assertNull(new NumericBucketDecoder(-1, 10, 24).decodeFullPresenceProof(3, Long.MIN_VALUE, Long.MIN_VALUE + 1));
    assertNull(new NumericBucketDecoder(0, 1, 24).decodeFullPresenceProof(3, 1, 25));
    final ColumnSlice signed = new NumericBucketDecoder(0, 1000, 24).decodeFullPresenceProof(65, -999, 999);
    assertNotNull(signed);
    assertArrayEquals(new long[] {-1L, 1}, signed.presenceWords());
    final ColumnSlice first = decoder.decodeFullPresenceProof(1024, 1000, 1009);
    final ColumnSlice second = decoder.decodeFullPresenceProof(1024, 2000, 2099);
    assertSame(first.presenceWords(), second.presenceWords(), "full masks are immutable and shared per worker");
  }

  @ParameterizedTest
  @EnumSource(ProjectionSlotLayout.class)
  void committedProofsPreserveHistoryAcrossAllVersioningAndStorageEpochs(final ProjectionSlotLayout layout) {
    final String property = "sirix.projection.columnMajorSlots";
    final String previous = System.getProperty(property);
    System.setProperty(property, Boolean.toString(layout == ProjectionSlotLayout.COLUMN_MAJOR));
    try {
      for (final VersioningType versioning : VersioningType.values()) {
        verifyHistory(layout, versioning);
      }
    } finally {
      if (previous == null)
        System.clearProperty(property);
      else
        System.setProperty(property, previous);
    }
  }

  private void verifyHistory(final ProjectionSlotLayout layout, final VersioningType versioning) {
    final Path path = directory.resolve(versioning.name());
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(path)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("r")
                                                              .storageType(StorageType.FILE_CHANNEL)
                                                              .versioningApproach(versioning)
                                                              .maxNumberOfRevisionsToRestore(2)
                                                              .build()));
      try (JsonResourceSession session = database.beginResourceSession("r")) {
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          writer.insertArrayAsFirstChild();
          ProjectionIndexHOTStorage storage =
              ProjectionIndexHOTStorage.forBulkBuild(writer.getStorageEngineWriter(), 0);
          final ProjectionNumericProofs.Builder proofs = new ProjectionNumericProofs.Builder();
          for (int group = 1; group <= 130; group++) {
            final EncodedRowGroup encoded = encoded(group, 65, group * 1000L, false);
            storage.putRowGroupAsColumnSegmentSlots(group, encoded);
            proofs.append(storage, group, encoded);
            if (group % 31 == 0) {
              writer.getStorageEngineWriter().asyncFlush();
              storage = ProjectionIndexHOTStorage.forBulkBuild(writer.getStorageEngineWriter(), 0);
            }
          }
          proofs.finish(storage);
          storage.putBlob(0, new ProjectionIndexMetadata("/[]", new String[] {"/[]/n"}, new String[] {"n"},
              new byte[] {LONG}, 130, 1).withSlotLayout(layout).serialize());
          writer.commit();
        }
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          final ProjectionIndexHOTStorage storage = new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0);
          storage.putRowGroupAsColumnSegmentSlots(1, encoded(1, 65, 9000, false));
          storage.tombstoneRowGroupAsColumnSegmentSlots(65);
          storage.putRowGroupAsColumnSegmentSlots(131, encoded(131, 65, 11000, false));
          writer.commit();
        }
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          final ProjectionIndexHOTStorage storage = new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0);
          storage.putRowGroupAsColumnSegmentSlots(2, encoded(2, 65, 17000, false));
          writer.rollback();
        }
      }
    }
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path);
        JsonResourceSession session = database.beginResourceSession("r")) {
      byte[] firstChunk = null;
      for (int revision = 1; revision <= 2; revision++) {
        try (JsonNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx(revision)) {
          final StorageEngineReader reader = trx.getStorageEngineReader();
          assertTrue(ProjectionNumericProofs.available(
              ProjectionIndexHOTStorage.readBlob(reader, 0, ProjectionNumericProofs.HEADER_SLOT), 0));
          final byte[] bytes = ProjectionIndexHOTStorage.readBlob(reader, 0, ProjectionNumericProofs.slot(0, 0));
          if (firstChunk == null)
            firstChunk = bytes;
          else
            assertArrayEquals(firstChunk, bytes);
          final ProjectionNumericProofs.Chunk chunk = new ProjectionNumericProofs.Chunk(bytes, 0, 0);
          final byte[] descriptor = ProjectionIndexHOTStorage.readBlob(reader, 0, layout.descriptorSlot(1));
          final NumericBucketDecoder decoder = new NumericBucketDecoder(0, 1000, 0);
          if (revision == 1)
            assertNotNull(chunk.decode(descriptor, 1, decoder));
          else
            assertNull(chunk.decode(descriptor, 1, decoder));
          final byte[] untouched = ProjectionIndexHOTStorage.readBlob(reader, 0, layout.descriptorSlot(2));
          assertEquals(2, chunk.decode(untouched, 2, decoder).numericValues()[0] / 1000);
          final byte[] deleted = ProjectionIndexHOTStorage.readBlob(reader, 0, layout.descriptorSlot(65));
          if (revision == 1)
            assertNotNull(deleted);
          else
            assertNull(deleted);
          final byte[] newChunk = ProjectionIndexHOTStorage.readBlob(reader, 0, ProjectionNumericProofs.slot(0, 2));
          final ProjectionNumericProofs.Chunk tail = new ProjectionNumericProofs.Chunk(newChunk, 0, 2);
          assertNotNull(
              tail.decode(ProjectionIndexHOTStorage.readBlob(reader, 0, layout.descriptorSlot(130)), 130, decoder));
          if (revision == 2)
            assertNull(
                tail.decode(ProjectionIndexHOTStorage.readBlob(reader, 0, layout.descriptorSlot(131)), 131, decoder));
        }
      }
    }
  }

  private static Map<Long, byte[]> buildProofs(final List<EncodedRowGroup> groups) {
    final Map<Long, byte[]> blobs = new HashMap<>();
    final Map<Long, byte[]> originals = new HashMap<>();
    final ProjectionIndexHOTStorage storage = mock(ProjectionIndexHOTStorage.class);
    doAnswer(call -> {
      final long slot = call.getArgument(0);
      final byte[] bytes = call.getArgument(1);
      blobs.put(slot, bytes.clone());
      originals.put(slot, bytes);
      return null;
    }).when(storage).putBlob(anyLong(), any(byte[].class));
    final ProjectionNumericProofs.Builder builder = new ProjectionNumericProofs.Builder();
    for (int group = 0; group < groups.size(); group++)
      builder.append(storage, group + 1, groups.get(group));
    builder.finish(storage);
    for (final Map.Entry<Long, byte[]> entry : blobs.entrySet())
      assertArrayEquals(entry.getValue(), originals.get(entry.getKey()));
    assertThrows(IllegalStateException.class, () -> builder.finish(storage));
    return blobs;
  }

  private static EncodedRowGroup encoded(final int group, final int rows, final long base, final boolean sparse) {
    final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(new byte[] {LONG});
    for (int row = 0; row < rows; row++) {
      assertTrue(page.appendRow(group * 10000L + row, new long[] {base + row % 90}, new boolean[1], new String[1],
          new boolean[] {!sparse || row != 3}, new boolean[1], new boolean[1]));
    }
    return ProjectionIndexColumnSegmentCodec.encode(page.serialize());
  }
}
