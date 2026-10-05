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
import io.sirix.cache.ProjectionBlobCache;
import io.sirix.io.StorageType;
import io.sirix.page.PageReference;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Path;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

/** Counts actual reader work, including referenced payloads, rather than checking elapsed time. */
@Isolated
final class ProjectionSortedRetentionWorkBudgetTest {
  @TempDir
  Path temporary;

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void warmRoutingAndPayloadsNeedNoSideReadsAndSameChunkRowsNeverPromote(final VersioningType versioning) {
    assertTrue(VersioningType.hotMergeDiagEnabled(), "fragment-work instruments must be enabled");
    final Path path = temporary.resolve(versioning.name());
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(path)));
    final byte[] payload = new byte[64];
    Arrays.fill(payload, (byte) 7);
    final byte[][] keys = new byte[1024][];
    for (int i = 0; i < keys.length; i++) {
      keys[i] = SortedScanFixtures.key(new SortedScanFixtures.Row("group", i, i + 1));
    }
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource")
                                                              .storageType(StorageType.FILE_CHANNEL)
                                                              .versioningApproach(versioning)
                                                              .maxNumberOfRevisionsToRestore(32)
                                                              .build()));
      try (JsonResourceSession session = database.beginResourceSession("resource")) {
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          final ProjectionSortedDirectory.Builder builder = new ProjectionSortedDirectory.Builder(
              new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0), SortedScanFixtures.GROUP_VALUE);
          for (int from = 0; from < keys.length; from += 256) {
            final byte[][] values = new byte[256][];
            Arrays.fill(values, payload);
            builder.append(ProjectionSortedLeaf.encode(Arrays.copyOfRange(keys, from, from + 256), values, 256));
          }
          builder.finish();
          writer.commit();
        }
        for (int revision = 2; revision <= 35; revision++) {
          try (JsonNodeTrx writer = session.beginNodeTrx()) {
            new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0).putBlob(
                ProjectionSortedLeafStore.LEAF_SLOT_BASE + (7L << 32), new byte[] {(byte) revision});
            writer.commit();
          }
        }
        Databases.clearGlobalCaches();
        try (JsonNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx()) {
          final StorageEngineReader actual = trx.getStorageEngineReader();
          final StorageEngineReader reader = mock(StorageEngineReader.class, delegatesTo(actual));
          final long[] sideReads = {0};
          final long[] nativeLoads = {0};
          doAnswer(call -> {
            sideReads[0]++;
            return actual.readSideOverflowPage(call.getArgument(0));
          }).when(reader).readSideOverflowPage(any(PageReference.class));
          doAnswer(call -> {
            nativeLoads[0]++;
            return actual.loadHOTPageAndGuard(call.getArgument(0));
          }).when(reader).loadHOTPageAndGuard(any(PageReference.class));
          doAnswer(call -> {
            nativeLoads[0]++;
            return actual.loadHOTPage(call.getArgument(0));
          }).when(reader).loadHOTPage(any(PageReference.class));
          final ProjectionBlobCache cache = reader.getBufferManager().getProjectionBlobCache();
          final long empty = cache.getCurrentWeightBytes();
          final ProjectionSortedDirectory.Accessor sorted = ProjectionSortedDirectory.open(reader, 0);
          assertNotNull(sorted);
          assertEquals(empty, cache.getCurrentWeightBytes(), "an ambiguous open must not admit routing");
          final ProjectionSortedDirectory.Accessor.Cursor cursor = sorted.seek(keys[0]);
          assertTrue(sideReads[0] > 0, "the fixture must resolve a referenced encoded chunk");
          final int chunk = cursor.leafId();
          final long initialLoads = nativeLoads[0];
          final byte[] result = new byte[payload.length];
          for (int row = 0; row < 32; row++) {
            assertEquals(chunk, cursor.leafId());
            assertArrayEquals(keys[row], cursor.copyKey());
            cursor.copyPayloadTo(result, 0);
            assertArrayEquals(payload, result);
            assertTrue(cursor.advance());
          }
          assertEquals(initialLoads, nativeLoads[0], "same-chunk consumption performs no native promotion");
          final long merges = VersioningType.multiFragmentMerges();
          final long olderFragments = VersioningType.fragmentsWalked();
          final long pointFragments = VersioningType.pointFragmentsWalked();
          int crossingRow = 32;
          while (cursor.leafId() == chunk) {
            assertTrue(cursor.advance());
            crossingRow++;
          }
          assertArrayEquals(keys[crossingRow], cursor.copyKey());
          cursor.copyPayloadTo(result, 0);
          assertArrayEquals(payload, result);
          if (versioning == VersioningType.FULL) {
            assertEquals(initialLoads, nativeLoads[0], "FULL reuses its complete resident native page");
            assertEquals(merges, VersioningType.multiFragmentMerges(), "FULL performs no extra fragment merge");
            assertEquals(olderFragments, VersioningType.fragmentsWalked(), "FULL visits no older native fragments");
            assertEquals(pointFragments, VersioningType.pointFragmentsWalked(), "FULL performs no older slot walk");
          } else {
            assertTrue(nativeLoads[0] > initialLoads, "crossing chunks exercises guarded scan promotion");
          }
          for (int query = 0; query < 12; query++) {
            final long before = sideReads[0];
            final long walks = VersioningType.pointLeafReads();
            final ProjectionSortedDirectory.Accessor fresh = ProjectionSortedDirectory.open(reader, 0);
            final ProjectionSortedDirectory.Accessor.Cursor point = fresh.seek(keys[0]);
            point.copyPayloadTo(result, 0);
            assertArrayEquals(payload, result);
            if (query >= 8) {
              assertEquals(before, sideReads[0], "warm routing and payloads perform zero side reads");
              assertEquals(walks, VersioningType.pointLeafReads(), "warm seeks perform zero fragment walks");
            }
          }
          assertTrue(cache.getCurrentWeightBytes() > empty);
          assertTrue(cache.getCurrentWeightBytes() <= cache.getMaxWeightBytes());
          // The public mutable blob result cannot alter a retained payload or another query.
          final long slot = ProjectionSortedLeafStore.LEAF_SLOT_BASE + chunk;
          final byte[] pristine = ProjectionIndexHOTStorage.readBlob(reader, 0, slot);
          assertNotNull(pristine);
          final byte[] changed = ProjectionIndexHOTStorage.readBlob(reader, 0, slot);
          Arrays.fill(changed, (byte) 0);
          assertArrayEquals(pristine, ProjectionIndexHOTStorage.readBlob(reader, 0, slot));
          reader.getBufferManager().clearCachesForResource(reader.getDatabaseId(), reader.getResourceId());
          assertEquals(empty, cache.getCurrentWeightBytes());
        }
      }
    }
  }
}
