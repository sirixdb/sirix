/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.HOTReadIntent;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.cache.HOTMiniPageCache;
import io.sirix.cache.BufferManager;
import io.sirix.cache.ProjectionBlobCache;
import io.sirix.cache.HOTMiniPageCache.ReadScope;
import io.sirix.io.StorageType;
import io.sirix.page.HOTMiniPage;
import io.sirix.page.PageReference;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/** Regression: a logical scan must not repeatedly copy and grow a native point mini page. */
@Isolated
final class ProjectionSortedReadIntentTest {
  @TempDir
  Path temporary;

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void onlyCrossingLogicalChunksPromotesWhileKnownScansNeverAdmit(final VersioningType versioning) {
    final Path path = temporary.resolve("intent-" + versioning);
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(path)));
    final List<SortedScanFixtures.Row> rows = new ArrayList<>();
    for (int row = 0; row < 256; row++) {
      rows.add(new SortedScanFixtures.Row("one", row, row + 1));
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
          for (int from = 0; from < rows.size(); from += 8) {
            final byte[][] keys = new byte[8][];
            final byte[][] payloads = new byte[8][];
            for (int row = 0; row < 8; row++) {
              keys[row] = SortedScanFixtures.key(rows.get(from + row));
              payloads[row] = payload(from + row);
            }
            final ProjectionSortedLeaf leaf = ProjectionSortedLeaf.encode(keys, payloads, 8);
            assertNotNull(leaf);
            builder.append(leaf);
          }
          builder.finish();
          writer.commit();
        }
        // Revisions change a different opaque slot, leaving the sorted values historical in the
        // same native leaf chain. These are separate ordinary commits, never a fabricated chain.
        for (int revision = 2; revision <= 13; revision++) {
          try (JsonNodeTrx writer = session.beginNodeTrx()) {
            new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0).putBlob(
                ProjectionSortedLeafStore.LEAF_SLOT_BASE + (7L << 32), new byte[] {(byte) revision});
            writer.commit();
          }
        }
        for (int mode = 0; mode < 15; mode++) {
          Databases.clearGlobalCaches();
          try (JsonNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx()) {
            // Delegate to the real reader without copying its mutable guard/lifetime state.
            final StorageEngineReader actualReader = withoutBlobCache(trx.getStorageEngineReader());
            final StorageEngineReader reader = mock(StorageEngineReader.class, delegatesTo(actualReader));
            final long[] dataPointWalks = {0};
            final List<PageReference> pointReferences = new ArrayList<>();
            doAnswer(invocation -> {
              final PageReference reference = invocation.getArgument(0);
              pointReferences.add(new PageReference().setKey(reference.getKey())
                                                     .setDatabaseId(actualReader.getDatabaseId())
                                                     .setResourceId(actualReader.getResourceId()));
              final long before = VersioningType.pointLeafReads();
              try {
                return actualReader.readHOTProjectionEntry(invocation.getArgument(0), invocation.getArgument(1),
                    invocation.getArgument(2), HOTReadIntent.POINT, invocation.getArgument(4));
              } finally {
                dataPointWalks[0] += VersioningType.pointLeafReads() - before;
              }
            }).when(reader)
              .readHOTProjectionEntry(any(PageReference.class), any(byte[].class), anyLong(), eq(HOTReadIntent.POINT),
                  any(ReadScope.class));
            final HOTMiniPageCache mini = reader.getBufferManager().getHOTMiniPageCache();
            assertEquals(0, mini.getCurrentWeightBytes());
            if (mode == 2) {
              for (int leaf = 1; leaf <= 32; leaf++) {
                assertNotNull(
                    ProjectionIndexHOTStorage.readBlob(reader, 0, ProjectionSortedLeafStore.LEAF_SLOT_BASE + leaf));
                assertEquals(0, mini.getCurrentWeightBytes(), "a general blob sweep retains complete leaves");
              }
            } else {
              final ProjectionSortedDirectory.Accessor sorted = ProjectionSortedDirectory.open(reader, 0);
              assertNotNull(sorted);
              assertEquals(0, mini.getCurrentWeightBytes(), "opening an ambiguous directory cannot admit entries");
              final int first = mode == 0
                  ? 0
                  : mode == 12
                      ? 23
                      : mode == 13
                          ? 255
                          : 17;
              final int end = mode == 0
                  ? rows.size()
                  : first + 32;
              final byte[] firstKey = SortedScanFixtures.key(rows.get(first));
              final ProjectionSortedDirectory.Accessor.Cursor cursor = mode == 0
                  ? sorted.first()
                  : mode == 3
                      ? sorted.scanFrom(firstKey)
                      : sorted.seek(firstKey);
              if (mode == 1 || mode >= 4) {
                // The first data miss consumes the whole admission budget. Metadata can only
                // use a later point's otherwise unused budget; scans cannot consume it.
                verify(reader, times(versioning == VersioningType.FULL
                    ? 0
                    : 1)).readHOTProjectionEntry(any(PageReference.class), any(byte[].class), anyLong(),
                        eq(HOTReadIntent.POINT), any(ReadScope.class));
                if (versioning != VersioningType.FULL) {
                  assertTrue(mini.getCurrentWeightBytes() > 0,
                      "optimistic seek must actually admit on a fragmented view");
                  assertTrue(dataPointWalks[0] > 0,
                      "the first data-entry walk makes the cache-hit assertion non-vacuous");
                }
                final long weight = mini.getCurrentWeightBytes();
                final long walks = dataPointWalks[0];
                assertArrayEquals(firstKey, sorted.seek(firstKey).copyKey());
                assertTrue(mini.getCurrentWeightBytes() >= weight,
                    "one previously non-admitted metadata record may use the data cache hit's spare admission");
                assertEquals(walks, dataPointWalks[0],
                    "the repeated data POINT hits its mini entry even while metadata is warming");
                clearInvocations(reader);
                if (mode == 4) {
                  // The next key is in the SAME logical leaf: promotion cannot wait for a leaf
                  // crossing, and capturing the old key must survive the representation change.
                  assertTrue(cursor.skipPrefixCapturingLast(firstKey, firstKey.length));
                  assertNoDataMini(mini, pointReferences);
                  final long retainedMetadata = mini.getCurrentWeightBytes();
                  assertArrayEquals(SortedScanFixtures.key(rows.get(first + 1)), cursor.copyKey());
                  final byte[] captured = new byte[cursor.lastSkippedKeyLength()];
                  cursor.copyLastSkippedKeyTo(captured);
                  assertArrayEquals(firstKey, captured);
                  assertEquals(retainedMetadata, mini.getCurrentWeightBytes(), "prefix reads cannot grow metadata");
                  verify(reader, never()).readHOTProjectionEntry(any(PageReference.class), any(byte[].class), anyLong(),
                      eq(HOTReadIntent.POINT), any(ReadScope.class));
                  continue;
                }
                if (mode >= 5) {
                  checkLazyMovement(mode, first, rows, sorted, cursor, reader, mini, dataPointWalks, pointReferences,
                      versioning);
                  continue;
                }
              }
              final int initialLeaf = cursor.leafId();
              long previousScanWeight = mini.getCurrentWeightBytes();
              for (int row = first; row < end; row++) {
                assertTrue(cursor.isValid());
                assertArrayEquals(SortedScanFixtures.key(rows.get(row)), cursor.copyKey());
                assertEquals(8, cursor.payloadLength());
                final byte[] bytes = new byte[8];
                cursor.copyPayloadTo(bytes, 0);
                assertArrayEquals(payload(row), bytes);
                if (mode != 1 || cursor.leafId() != initialLeaf) {
                  assertNoDataMini(mini, pointReferences);
                  assertTrue(mini.getCurrentWeightBytes() <= previousScanWeight,
                      "a scan cannot grow the cache, including metadata admitted by an earlier point");
                  if (mode == 0 || mode == 3) {
                    assertEquals(0, mini.getCurrentWeightBytes(), "known scans retain zero mini bytes");
                  }
                }
                final long beforeAdvance = mini.getCurrentWeightBytes();
                cursor.advance();
                if (mode == 1 && row == first) {
                  assertEquals(beforeAdvance, mini.getCurrentWeightBytes(),
                      "within-chunk advance keeps the owned encoded bytes without promotion");
                } else {
                  assertTrue(mini.getCurrentWeightBytes() <= beforeAdvance, "advancing a scan never admits metadata");
                }
                previousScanWeight = mini.getCurrentWeightBytes();
              }
              verify(reader, never()).readHOTProjectionEntry(any(PageReference.class), any(byte[].class), anyLong(),
                  eq(HOTReadIntent.POINT), any(ReadScope.class));
              if (mode == 0) {
                assertFalse(cursor.isValid());
              }
            }
          }
        }
        // Open a fresh accessor for every point, as real queries do. Once a bounded series of
        // spare admissions resolves the directory path, neither metadata nor data may re-walk it.
        Databases.clearGlobalCaches();
        try (JsonNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx()) {
          final StorageEngineReader reader = withoutBlobCache(trx.getStorageEngineReader());
          final byte[] key = SortedScanFixtures.key(rows.get(17));
          final byte[] value = new byte[8];
          for (int query = 0; query < 16; query++) {
            final long before = VersioningType.pointLeafReads();
            final ProjectionSortedDirectory.Accessor fresh = ProjectionSortedDirectory.open(reader, 0);
            assertNotNull(fresh);
            final ProjectionSortedDirectory.Accessor.Cursor point = fresh.seek(key);
            point.copyPayloadTo(value, 0);
            assertArrayEquals(payload(17), value);
            assertTrue(point.advance()); // Abandoned within-leaf advance cannot discard the hits.
            if (query == 0 && versioning != VersioningType.FULL) {
              assertTrue(VersioningType.pointLeafReads() > before, "make the warm-work bound non-vacuous");
            }
            if (query >= 8) {
              assertEquals(before, VersioningType.pointLeafReads(),
                  "repeated metadata and data point reads must do zero fragment walks");
            }
          }
        }

        // Non-vacuity: the same persisted fragmented view still supports the explicit point API.
        Databases.clearGlobalCaches();
        try (JsonNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx()) {
          final StorageEngineReader reader = withoutBlobCache(trx.getStorageEngineReader());
          assertNotNull(ProjectionIndexHOTStorage.readBlob(reader, 0, ProjectionSortedLeafStore.LEAF_SLOT_BASE + 1,
              HOTReadIntent.POINT));
          if (versioning != VersioningType.FULL) {
            assertTrue(reader.getBufferManager().getHOTMiniPageCache().getCurrentWeightBytes() > 0);
          }
        }
      }
    }
  }

  private static void checkLazyMovement(final int mode, final int first, final List<SortedScanFixtures.Row> rows,
      final ProjectionSortedDirectory.Accessor sorted, final ProjectionSortedDirectory.Accessor.Cursor cursor,
      final StorageEngineReader reader, final HOTMiniPageCache mini, final long[] dataPointWalks,
      final List<PageReference> pointReferences, final VersioningType versioning) {
    final long weight = mini.getCurrentWeightBytes();
    final long walks = dataPointWalks[0];
    final byte[] bytes = new byte[8];
    cursor.copyPayloadTo(bytes, 0);
    assertArrayEquals(payload(first), bytes);
    final int previousLeaf = cursor.leafId();
    final int previousRow = cursor.row();
    assertEquals(mode != 13, cursor.advance());
    if (mode == 12 || mode == 13) {
      assertNoDataMini(mini, pointReferences);
      assertTrue(mini.getCurrentWeightBytes() <= weight, "a leaf crossing cannot add metadata");
      if (mode == 12) {
        assertArrayEquals(SortedScanFixtures.key(rows.get(first + 1)), cursor.copyKey());
      } else {
        assertFalse(cursor.isValid());
        assertFalse(cursor.advance());
        assertThrows(IllegalStateException.class, cursor::copyKey);
      }
      assertNoPointGuards(reader, mini, pointReferences);
      return;
    }
    assertTrue(cursor.isValid());
    assertEquals(previousLeaf, cursor.leafId());
    assertEquals(previousRow + 1, cursor.row());
    assertEquals(weight, mini.getCurrentWeightBytes(), "position checks do not read another key or payload");
    assertEquals(walks, dataPointWalks[0]);
    if (mode == 5) {
      // Exactly the frozen point loop: consume once, advance once, abandon this cursor. Another
      // cursor remains POINT and reuses the mini entry even while the abandoned one is SCAN.
      final List<HOTMiniPage> admitted = assertNoPointGuards(reader, mini, pointReferences);
      final byte[] key = SortedScanFixtures.key(rows.get(first));
      final ProjectionSortedDirectory.Accessor.Cursor nextPoint = sorted.seek(key);
      nextPoint.copyPayloadTo(bytes, 0);
      assertArrayEquals(payload(first), bytes);
      final long afterPointWeight = mini.getCurrentWeightBytes();
      assertTrue(nextPoint.advance());
      assertEquals(afterPointWeight, mini.getCurrentWeightBytes(), "abandoned advances never admit metadata");
      assertEquals(walks, dataPointWalks[0], "a new point must not inherit the abandoned cursor's scan intent");
      assertNoPointGuards(reader, mini, pointReferences);
      mini.clear();
      for (final HOTMiniPage page : admitted) {
        assertTrue(page.isClosed(), "abandoned cursors cannot keep a retired mini page alive");
      }
      assertArrayEquals(key, sorted.seek(key).copyKey());
      if (versioning != VersioningType.FULL) {
        assertTrue(mini.getCurrentWeightBytes() > 0, "a subsequent point can admit after eviction");
      }
      assertNoPointGuards(reader, mini, pointReferences);
      return;
    }
    int expectedRow = first + 1;
    final byte[] nextKey = SortedScanFixtures.key(rows.get(expectedRow));
    switch (mode) {
      case 6 -> assertArrayEquals(nextKey, cursor.copyKey());
      case 7 -> assertEquals(nextKey.length, cursor.keyLength());
      case 8 -> {
        final byte[] copied = new byte[nextKey.length];
        cursor.copyKeyTo(copied);
        assertArrayEquals(nextKey, copied);
      }
      case 9 -> assertEquals(bytes.length, cursor.payloadLength());
      case 10 -> cursor.copyPayloadTo(bytes, 0);
      case 11 -> {
        assertTrue(cursor.advance());
        assertEquals(weight, mini.getCurrentWeightBytes(), "multiple unconsumed advances hold no native image");
        expectedRow++;
        cursor.copyPayloadTo(bytes, 0);
      }
      case 14 -> {
        assertTrue(cursor.skipPrefixCapturingLast(nextKey, nextKey.length));
        final byte[] captured = new byte[cursor.lastSkippedKeyLength()];
        cursor.copyLastSkippedKeyTo(captured);
        assertArrayEquals(nextKey, captured);
        expectedRow++;
      }
      default -> throw new AssertionError("unknown movement case " + mode);
    }
    if (mode == 14) {
      assertNoDataMini(mini, pointReferences);
    } else {
      assertEquals(weight, mini.getCurrentWeightBytes(), "consuming another row in the same chunk never promotes");
    }
    assertTrue(mini.getCurrentWeightBytes() <= weight, "consuming a scan cannot add metadata");
    assertArrayEquals(SortedScanFixtures.key(rows.get(expectedRow)), cursor.copyKey());
    cursor.copyPayloadTo(bytes, 0);
    assertArrayEquals(payload(expectedRow), bytes);
    verify(reader, never()).readHOTProjectionEntry(any(PageReference.class), any(byte[].class), anyLong(),
        eq(HOTReadIntent.POINT), any(ReadScope.class));
    assertNoPointGuards(reader, mini, pointReferences);
  }

  private static void assertNoDataMini(final HOTMiniPageCache mini, final List<PageReference> references) {
    for (final PageReference reference : references) {
      final HOTMiniPage page = mini.getAndGuard(reference);
      if (page != null) {
        page.releaseGuard();
      }
      assertEquals(null, page, "the ordinary complete loader must discard the data leaf's mini image");
    }
  }

  private static List<HOTMiniPage> assertNoPointGuards(final StorageEngineReader reader, final HOTMiniPageCache mini,
      final List<PageReference> references) {
    final List<HOTMiniPage> pages = new ArrayList<>();
    for (final PageReference reference : references) {
      final HOTMiniPage page = mini.getAndGuard(reference);
      if (page != null) {
        try {
          assertEquals(1, page.getGuardCount(), "only this test's inspection owns a mini guard");
          pages.add(page);
        } finally {
          page.releaseGuard();
        }
        assertEquals(0, page.getGuardCount());
      }
    }
    reader.getBufferManager()
          .getHOTLeafFragmentCache()
          .asMap()
          .values()
          .forEach(page -> assertEquals(0, page.getGuardCount(),
              "detached sorted cursors never retain a raw-fragment guard"));
    return pages;
  }

  /** Exercise the mini-page path independently of verified payload retention. */
  private static StorageEngineReader withoutBlobCache(final StorageEngineReader actual) {
    final BufferManager manager = mock(BufferManager.class, delegatesTo(actual.getBufferManager()));
    doReturn(ProjectionBlobCache.disabled()).when(manager).getProjectionBlobCache();
    final StorageEngineReader reader = mock(StorageEngineReader.class, delegatesTo(actual));
    doReturn(manager).when(reader).getBufferManager();
    return reader;
  }

  private static byte[] payload(final int row) {
    return new byte[] {(byte) row, (byte) (row >>> 8), (byte) ~row, 42, 7, -11, 93, 12};
  }
}
