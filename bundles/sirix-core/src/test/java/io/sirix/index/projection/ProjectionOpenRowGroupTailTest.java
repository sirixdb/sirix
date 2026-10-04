/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.page.PageReference;
import io.sirix.settings.Constants;
import io.sirix.settings.VersioningType;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

/**
 * Open-row-group row tail: appended rows are stored row-major and merged on read; every reader path
 * sees the row group the writer published, at every revision, across every versioning type; the
 * fold happens at completion, on any non-append write and on demand, and a rollback around it
 * leaves the tail intact.
 */
@Isolated
final class ProjectionOpenRowGroupTailTest {
  private static final String RESOURCE = "resource";
  private static final int INDEX = 0;
  private static final byte[] KINDS = {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG,
      ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN, ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT};
  private static final String[] DEPTS = {"Eng", "Sales", "Mkt", "Ops", "Legal"};
  private static final int BODY_0 = ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(0);
  private static final int BODY_2 = ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(2);

  @TempDir
  Path temporaryDirectory;

  private final AtomicLong mergeCount = new AtomicLong();
  private @Nullable Runnable previousMergeObserver;

  @BeforeEach
  void observeMerges() {
    previousMergeObserver = ProjectionOpenRowGroupTail.observeMergesForTesting(mergeCount::incrementAndGet);
  }

  @AfterEach
  void restoreMergeObserver() {
    ProjectionOpenRowGroupTail.observeMergesForTesting(previousMergeObserver);
  }

  private Path create(final VersioningType versioningType, final String name) {
    final Path databasePath = temporaryDirectory.resolve(name + "-" + versioningType.name().toLowerCase(Locale.ROOT));
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                                   .versioningApproach(versioningType)
                                                   .maxNumberOfRevisionsToRestore(3)
                                                   .build());
    }
    return databasePath;
  }

  private static long keyAt(final long base, final int row) {
    return base + 3L * row;
  }

  private static long ageAt(final int row) {
    return 18 + (row * 7L) % 48;
  }

  private static boolean flagAt(final int row) {
    return (row & 1) == 0;
  }

  private static String deptAt(final int row) {
    return DEPTS[(row * 3) % DEPTS.length];
  }

  /** The row group with rows {@code [0, rows)} built the ordinary way (one append per row). */
  private static ProjectionIndexRowGroupPage pageWithRows(final long keyBase, final int rows) {
    return pageWithRows(keyBase, rows, -1);
  }

  private static ProjectionIndexRowGroupPage pageWithRows(final long keyBase, final int rows, final int skipRow) {
    final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(KINDS);
    final long[] longs = new long[3];
    final boolean[] bools = new boolean[3];
    final String[] strings = new String[3];
    final boolean[] present = {true, true, true};
    final boolean[] unrepresentable = new boolean[3];
    final boolean[] nonIntegral = new boolean[3];
    for (int row = 0; row < rows; row++) {
      if (row == skipRow) {
        continue;
      }
      longs[0] = ageAt(row);
      bools[1] = flagAt(row);
      strings[2] = deptAt(row);
      assertTrue(page.appendRow(keyAt(keyBase, row), longs, bools, strings, present, unrepresentable, nonIntegral));
    }
    return page;
  }

  private static byte[] syntheticLabel(final int row) {
    return new byte[] {(byte) (row >>> 24), (byte) (row >>> 16), (byte) (row >>> 8), (byte) row};
  }

  /**
   * The row with values of {@code row}, appended at page position {@code position} (its synthetic
   * label).
   */
  private static ProjectionOpenRowGroupTail.Row tailRow(final long keyBase, final int row, final int position) {
    final long[] longs = {ageAt(row), 0L, 0L};
    final boolean[] bools = {false, flagAt(row), false};
    final byte[][] strings = {null, null, deptAt(row).getBytes(StandardCharsets.UTF_8)};
    final String[][] sets = new String[3][];
    return new ProjectionOpenRowGroupTail.Row(keyAt(keyBase, row), false, syntheticLabel(position), longs, bools,
        strings, sets, new boolean[] {true, true, true}, new boolean[3], new boolean[3], new boolean[3]);
  }

  /** What the maintenance writer does for a pure append of {@code count} rows to row group 1. */
  private static void tailAppend(final ProjectionIndexHOTStorage storage, final long keyBase, final int fromRow,
      final int count) {
    final byte[] raw = storage.getRowGroupFromColumnSegmentSlots(1);
    assertNotNull(raw);
    final ProjectionIndexRowGroupPage merged = ProjectionIndexRowGroupPage.deserialize(raw);
    final List<ProjectionOpenRowGroupTail.Row> rows = new ArrayList<>(count);
    for (int row = fromRow; row < fromRow + count; row++) {
      rows.add(tailRow(keyBase, row, merged.getRowCount() + row - fromRow));
    }
    ProjectionOpenRowGroupTail.appendRows(merged, rows, 1);
    storage.putOpenRowGroupTailAppend(1, ProjectionIndexColumnSegmentCodec.encodePooled(merged),
        ProjectionOpenRowGroupTail.encodeRows(KINDS, rows), count, merged.serialize());
  }

  private static long bodyOffset(final StorageEngineReader reader, final int columnSegmentId) {
    return ProjectionIndexHOTStorage.segmentPageOffset(reader, INDEX,
        ProjectionIndexHOTStorage.columnSegmentSlotKey(1, columnSegmentId), 0);
  }

  private static void assertEveryReaderSees(final StorageEngineReader reader,
      final ProjectionIndexRowGroupPage expected, final byte[] otherRaw, final boolean tailed) {
    final byte[] expectedRaw = expected.serialize();
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup expectedEncoded =
        requireNonNull(ProjectionIndexColumnSegmentCodec.encode(expectedRaw));
    assertNotNull(expectedEncoded);
    assertArrayEquals(expectedRaw, ProjectionIndexHOTStorage.readRowGroupFromColumnSegmentSlots(reader, INDEX, 1),
        "single row-group read");
    assertEquals(expected.getRowCount(), ProjectionIndexHOTStorage.readRowCountFromColumnSegmentSlots(reader, INDEX, 1),
        "descriptor-only row count");
    final List<byte[]> all = ProjectionIndexHOTStorage.readAllRowGroupsFromColumnSegmentSlots(reader, INDEX, 2);
    assertEquals(2, all.size());
    assertArrayEquals(expectedRaw, all.get(0), "batch assembly of the tailed row group");
    assertArrayEquals(otherRaw, all.get(1), "batch assembly of the untouched row group");
    final List<ProjectionIndexHOTStorage.RowGroupDirectory> directories =
        ProjectionIndexHOTStorage.readAllRowGroupDirectoriesFromColumnSegmentSlots(reader, INDEX, 2);
    assertNotNull(directories);
    assertDirectory(directories.get(0), expectedEncoded, tailed);
    assertFalse(RowGroupDescriptor.isTailed(directories.get(1).descriptor()));
    final ProjectionIndexHOTStorage.RowGroupDirectory[] window =
        ProjectionIndexHOTStorage.readDirectoryWindow(reader, INDEX, new int[] {1, 2}, 0, 2);
    assertDirectory(window[0], expectedEncoded, tailed);
    final byte[] descriptor =
        ProjectionIndexHOTStorage.readBlob(reader, INDEX, ProjectionIndexHOTStorage.rowGroupDescriptorSlotKey(1));
    assertNotNull(descriptor);
    assertEquals(tailed, RowGroupDescriptor.isTailed(descriptor), "published descriptor version");
    assertTrue(RowGroupDescriptor.sameContentIgnoringVersion(descriptor, expectedEncoded.descriptor()),
        "published descriptor describes the merged row group");
  }

  private static void assertDirectory(final ProjectionIndexHOTStorage.RowGroupDirectory directory,
      final ProjectionIndexColumnSegmentCodec.EncodedRowGroup expected, final boolean tailed) {
    assertEquals(1L, directory.rowGroupId());
    assertEquals(tailed, RowGroupDescriptor.isTailed(directory.descriptor()));
    assertTrue(RowGroupDescriptor.sameContentIgnoringVersion(directory.descriptor(), expected.descriptor()));
    assertArrayEquals(expected.columnSegmentIds(), directory.columnSegmentIds());
    for (int i = 0; i < expected.segments().length; i++) {
      final byte[] inline = directory.inlineColumnSegmentBytes() == null
          ? null
          : directory.inlineColumnSegmentBytes()[i];
      if (tailed) {
        assertEquals(Constants.NULL_ID_LONG, directory.columnSegmentOffsets()[i], "merged segments are served inline");
        assertArrayEquals(expected.segments()[i], inline, "merged segment " + expected.columnSegmentIds()[i]);
      } else if (inline != null) {
        assertArrayEquals(expected.segments()[i], inline);
      }
    }
  }

  @ParameterizedTest(name = "{0}")
  @EnumSource(VersioningType.class)
  void persistedOrderExceptionsSurviveNormalAndExceptionalTailAppends(final VersioningType versioningType) {
    final byte[] otherRaw = pageWithRows(2_000_000L, 5).serialize();
    for (final boolean exceptional : new boolean[] {false, true}) {
      final Path databasePath = create(versioningType, "bitmap-" + exceptional);
      final ProjectionIndexRowGroupPage base = pageWithOrderExceptions(64, exceptional);
      final ProjectionIndexRowGroupPage appended = pageWithOrderExceptions(65, exceptional);
      final ProjectionOpenRowGroupTail.Row values = tailRow(10L, 64, 64);
      final ProjectionOpenRowGroupTail.Row row = new ProjectionOpenRowGroupTail.Row(appended.recordKeys()[64],
          exceptional, values.orderLabel(), values.longs(), values.bools(), values.strings(), values.sets(),
          values.present(), values.unrepresentable(), values.nonIntegral(), values.nonDoubleSource());
      ProjectionOpenRowGroupTail.clearCacheForTesting();
      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
          JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          final ProjectionIndexHOTStorage storage =
              new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), INDEX);
          storage.putRowGroupAsColumnSegmentSlots(1,
              requireNonNull(ProjectionIndexColumnSegmentCodec.encode(base.serialize())));
          storage.putRowGroupAsColumnSegmentSlots(2,
              requireNonNull(ProjectionIndexColumnSegmentCodec.encode(otherRaw)));
          writer.commit();
        }
        for (int attempt = 0; attempt < 2; attempt++) {
          try (JsonNodeTrx writer = session.beginNodeTrx()) {
            final ProjectionIndexHOTStorage storage =
                new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), INDEX);
            final byte[] raw = storage.getRowGroupFromColumnSegmentSlots(1);
            assertNotNull(raw);
            final ProjectionIndexRowGroupPage merged = ProjectionIndexRowGroupPage.deserialize(raw);
            assertEquals(1, merged.orderExceptionBits().length);
            ProjectionOpenRowGroupTail.appendRows(merged, List.of(row), 1);
            assertTrue(merged.orderExceptionAt(31));
            assertEquals(exceptional, merged.orderExceptionAt(64));
            assertEquals(ageAt(64), merged.numericColumn(0)[64]);
            assertArrayEquals(appended.serialize(), merged.serialize());
            storage.putOpenRowGroupTailAppend(1, ProjectionIndexColumnSegmentCodec.encodePooled(merged),
                ProjectionOpenRowGroupTail.encodeRows(KINDS, List.of(row)), 1, merged.serialize());
            ProjectionOpenRowGroupTail.clearCacheForTesting();
            assertArrayEquals(appended.serialize(), storage.getRowGroupFromColumnSegmentSlots(1),
                "cold writer replay retains the persisted exception bits");
            if (attempt == 0) {
              writer.rollback();
            } else {
              writer.commit();
            }
          }
          ProjectionOpenRowGroupTail.clearCacheForTesting();
          try (JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx()) {
            assertEveryReaderSees(reader.getStorageEngineReader(), attempt == 0
                ? base
                : appended, otherRaw, attempt != 0);
          }
        }
      }
      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
          JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
        for (int revision = 1; revision <= 2; revision++) {
          ProjectionOpenRowGroupTail.clearCacheForTesting();
          try (JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx(revision)) {
            assertEveryReaderSees(reader.getStorageEngineReader(), revision == 1
                ? base
                : appended, otherRaw, revision == 2);
          }
        }
      }
    }
  }

  private static ProjectionIndexRowGroupPage pageWithOrderExceptions(final int rows, final boolean tailException) {
    final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(KINDS);
    for (int row = 0; row < rows; row++) {
      final ProjectionOpenRowGroupTail.Row values = tailRow(10L, row, row);
      final boolean exceptional = row == 31 || (row == 64 && tailException);
      final long key = exceptional
          ? 1_000_000L + row
          : values.recordKey();
      assertTrue(page.appendExtractedUtf8Row(key, values.longs(), values.bools(), values.strings(),
          new int[] {0, 0, values.strings()[2].length}, values.sets(), values.present(), values.unrepresentable(),
          values.nonIntegral(), values.nonDoubleSource(), exceptional, values.orderLabel()));
    }
    return page;
  }

  @ParameterizedTest(name = "{0}")
  @EnumSource(VersioningType.class)
  void memoHitsSkipBasePayloadsAndColdRoutesSkipDictionaryHashes(final VersioningType versioningType) {
    final Path databasePath = create(versioningType, "memo-read-work");
    final int groups = 512;
    final byte[] otherRaw = pageWithRows(2_000_000L, 5).serialize();
    final ProjectionIndexRowGroupPage expected = pageWithUniqueStrings(601);
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup expectedEncoded =
        requireNonNull(ProjectionIndexColumnSegmentCodec.encode(expected.serialize()));
    final int[] physicalOrder = new int[groups];
    for (int group = 0; group < groups; group++) {
      physicalOrder[group] = group + 1;
    }
    ProjectionOpenRowGroupTail.clearCacheForTesting();
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
      try (JsonNodeTrx writer = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage = new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), INDEX);
        storage.putRowGroupAsColumnSegmentSlots(1,
            requireNonNull(ProjectionIndexColumnSegmentCodec.encode(pageWithUniqueStrings(600).serialize())));
        for (int group = 2; group <= groups; group++) {
          storage.putRowGroupAsColumnSegmentSlots(group,
              requireNonNull(ProjectionIndexColumnSegmentCodec.encode(otherRaw)));
        }
        writer.commit();
        final ProjectionIndexRowGroupPage merged = pageWithUniqueStrings(601);
        final ProjectionOpenRowGroupTail.Row values = tailRow(10L, 600, 600);
        final ProjectionOpenRowGroupTail.Row row = new ProjectionOpenRowGroupTail.Row(values.recordKey(), false,
            values.orderLabel(), values.longs(), values.bools(),
            new byte[][] {null, null, uniqueString(600).getBytes(StandardCharsets.UTF_8)}, values.sets(),
            values.present(), values.unrepresentable(), values.nonIntegral(), values.nonDoubleSource());
        new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), INDEX).putOpenRowGroupTailAppend(1,
            ProjectionIndexColumnSegmentCodec.encodePooled(merged),
            ProjectionOpenRowGroupTail.encodeRows(KINDS, List.of(row)), 1, merged.serialize());
        writer.commit();
      }
      try (JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx()) {
        final StorageEngineReader storage = reader.getStorageEngineReader();
        final int revision = reader.getRevisionNumber();
        final ProjectionOpenRowGroupTail.Header header = ProjectionOpenRowGroupTail.Header.decode(
            ProjectionIndexHOTStorage.readBlob(storage, INDEX, ProjectionOpenRowGroupTail.headerSlot(1)), 1);
        final byte[] baseDescriptor = header.baseDescriptor();
        final LongOpenHashSet baseOffsets = new LongOpenHashSet(RowGroupDescriptor.columnSegmentCount(baseDescriptor));
        for (int entry = 0; entry < RowGroupDescriptor.columnSegmentCount(baseDescriptor); entry++) {
          final int id = RowGroupDescriptor.entryColumnSegmentId(baseDescriptor, entry);
          final long offset = bodyOffset(storage, id);
          if (offset >= 0) {
            baseOffsets.add(offset);
          }
        }
        final long hashesOffset = bodyOffset(storage, ProjectionIndexColumnSegmentCodec.dictHashColumnSegmentId(2));
        assertTrue(hashesOffset >= 0, "the fixture must persist dictionary hashes by reference");
        assertTrue(bodyOffset(storage, ProjectionIndexColumnSegmentCodec.bloomColumnSegmentId(2)) >= 0,
            "the fixture must persist unused raw-assembly Bloom bytes by reference");
        final LongOpenHashSet requiredOffsets = new LongOpenHashSet(KINDS.length + 2);
        final int[] requiredIds = {ProjectionIndexColumnSegmentCodec.keysColumnSegmentId(), BODY_0,
            ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(1), BODY_2,
            ProjectionIndexColumnSegmentCodec.dictColumnSegmentId(2)};
        for (final int id : requiredIds) {
          final long offset = bodyOffset(storage, id);
          if (offset >= 0) {
            requiredOffsets.add(offset);
          }
        }
        assertFalse(requiredOffsets.isEmpty(), "required base payloads must also be referenced");
        final int routes = Runtime.getRuntime().availableProcessors() >= 2
            ? 5
            : 4;
        for (final boolean cold : new boolean[] {false, true}) {
          for (int route = 0; route < routes; route++) {
            if (cold) {
              ProjectionOpenRowGroupTail.clearCacheForTesting();
            }
            final AtomicInteger baseReads = new AtomicInteger();
            final AtomicInteger hashReads = new AtomicInteger();
            final StorageEngineReader counted =
                countPayloadReads(storage, baseOffsets, hashesOffset, baseReads, hashReads);
            List<ProjectionIndexHOTStorage.RowGroupDirectory> directories = null;
            switch (route) {
              case 0 -> assertArrayEquals(expected.serialize(),
                  ProjectionIndexHOTStorage.readRowGroupFromColumnSegmentSlots(counted, INDEX, 1));
              case 1 -> {
                final List<byte[]> all = ProjectionIndexHOTStorage.readAllRowGroupsFromColumnSegmentSlots(counted,
                    INDEX, groups, physicalOrder);
                assertEquals(groups, all.size());
                assertArrayEquals(expected.serialize(), all.get(0));
                for (int group = 1; group < groups; group++) {
                  assertArrayEquals(otherRaw, all.get(group));
                }
              }
              case 2 ->
                directories = ProjectionIndexHOTStorage.readAllRowGroupDirectoriesFromColumnSegmentSlots(counted, INDEX,
                    groups, physicalOrder);
              case 3 -> directories = Arrays.asList(
                  ProjectionIndexHOTStorage.readDirectoryWindow(counted, INDEX, new int[] {1, groups}, 0, 2));
              case 4 -> {
                final AtomicInteger workers = new AtomicInteger();
                directories = ProjectionIndexHOTStorage.readAllRowGroupDirectoriesFromColumnSegmentSlots(counted, INDEX,
                    groups, physicalOrder, worker -> {
                      try (JsonNodeReadOnlyTrx workerTrx = session.beginNodeReadOnlyTrx(revision)) {
                        worker.accept(countPayloadReads(workerTrx.getStorageEngineReader(), baseOffsets, hashesOffset,
                            baseReads, hashReads));
                        workers.incrementAndGet();
                      }
                    }, true);
                assertTrue(workers.get() > 0, "the directory route must engage worker readers");
              }
              default -> throw new AssertionError(route);
            }
            if (route >= 2) {
              assertNotNull(directories);
              assertEquals(route == 3
                  ? 2
                  : groups, directories.size());
              assertDirectory(directories.get(0), expectedEncoded, true);
              for (int group = 1; group < directories.size(); group++) {
                assertFalse(RowGroupDescriptor.isTailed(directories.get(group).descriptor()));
                assertEquals(route == 3
                    ? groups
                    : group + 1, directories.get(group).rowGroupId());
              }
            }
            assertEquals(cold
                ? requiredOffsets.size()
                : 0, baseReads.get(), "base payload reads for route " + route + ", cold=" + cold);
            assertEquals(0, hashReads.get(), "raw assembly never consumes base dictionary hashes");
          }
        }
      }
    }
  }

  private static String uniqueString(final int row) {
    return "department-with-a-distinct-dictionary-entry-" + row;
  }

  private static ProjectionIndexRowGroupPage pageWithUniqueStrings(final int rows) {
    final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(KINDS);
    for (int row = 0; row < rows; row++) {
      assertTrue(page.appendRow(keyAt(10L, row), new long[] {ageAt(row), 0L, 0L},
          new boolean[] {false, flagAt(row), false}, new String[] {null, null, uniqueString(row)}));
    }
    return page;
  }

  private static StorageEngineReader countPayloadReads(final StorageEngineReader delegate,
      final LongOpenHashSet baseOffsets, final long hashesOffset, final AtomicInteger baseReads,
      final AtomicInteger hashReads) {
    return (StorageEngineReader) Proxy.newProxyInstance(StorageEngineReader.class.getClassLoader(),
        new Class<?>[] {StorageEngineReader.class}, (proxy, method, arguments) -> {
          if ("readSideOverflowPageBatch".equals(method.getName())) {
            for (final long offset : (long[]) arguments[0]) {
              countPayloadOffset(offset, baseOffsets, hashesOffset, baseReads, hashReads);
            }
          } else if ("readSideOverflowPage".equals(method.getName())) {
            countPayloadOffset(((PageReference) arguments[0]).getKey(), baseOffsets, hashesOffset, baseReads,
                hashReads);
          }
          try {
            return method.invoke(delegate, arguments);
          } catch (final InvocationTargetException failure) {
            throw failure.getCause();
          }
        });
  }

  private static void countPayloadOffset(final long offset, final LongOpenHashSet baseOffsets, final long hashesOffset,
      final AtomicInteger baseReads, final AtomicInteger hashReads) {
    if (baseOffsets.contains(offset)) {
      baseReads.incrementAndGet();
    }
    if (offset == hashesOffset) {
      hashReads.incrementAndGet();
    }
  }

  @ParameterizedTest(name = "{0}")
  @EnumSource(VersioningType.class)
  void aColdMergeRejectsADescriptorThatDisagreesWithTheTail(final VersioningType versioningType) {
    final Path databasePath = create(versioningType, "corrupt");
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX).putRowGroupAsColumnSegmentSlots(1,
            requireNonNull(ProjectionIndexColumnSegmentCodec.encode(pageWithRows(10L, 600).serialize())));
        wtx.commit();
        tailAppend(new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX), 10L, 600, 2);
        wtx.commit();
        final byte[] wrong = RowGroupDescriptor.withVersion(
            requireNonNull(ProjectionIndexColumnSegmentCodec.encode(pageWithRows(10L, 601).serialize())).descriptor(),
            RowGroupDescriptor.VERSION_TAILED);
        new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX).putBlob(
            ProjectionIndexHOTStorage.rowGroupDescriptorSlotKey(1), wrong);
        wtx.commit();
      }
      ProjectionOpenRowGroupTail.clearCacheForTesting();
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final IllegalStateException failure = assertThrows(IllegalStateException.class,
            () -> ProjectionIndexHOTStorage.readRowGroupFromColumnSegmentSlots(rtx.getStorageEngineReader(), INDEX, 1));
        assertTrue(requireNonNull(failure.getMessage()).contains("disagrees with the published descriptor"));
      }
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(2)) {
        assertArrayEquals(pageWithRows(10L, 602).serialize(),
            ProjectionIndexHOTStorage.readRowGroupFromColumnSegmentSlots(rtx.getStorageEngineReader(), INDEX, 1));
      }
    }
  }

  @ParameterizedTest(name = "{0}")
  @EnumSource(VersioningType.class)
  void aColdParallelDirectoryWalkIncludesTheTail(final VersioningType versioningType) {
    assumeTrue(Runtime.getRuntime().availableProcessors() >= 2,
        "parallel directory routing requires at least two processors");
    final Path databasePath = create(versioningType, "parallel");
    final int groups = 512;
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage = new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX);
        for (int group = 1; group <= groups; group++) {
          final long base = group == 1
              ? 10L
              : 1_000_000L + 10_000L * group;
          storage.putRowGroupAsColumnSegmentSlots(group,
              requireNonNull(ProjectionIndexColumnSegmentCodec.encode(pageWithRows(base, 600).serialize())));
        }
        wtx.commit();
        tailAppend(new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX), 10L, 600, 2);
        wtx.commit();
      }
    }
    ProjectionOpenRowGroupTail.clearCacheForTesting();
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        JsonResourceSession session = database.beginResourceSession(RESOURCE);
        JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
      final int revision = rtx.getRevisionNumber();
      final AtomicInteger workers = new AtomicInteger();
      final int[] physicalOrder = new int[groups];
      for (int group = 0; group < groups; group++) {
        physicalOrder[group] = group + 1;
      }
      final List<ProjectionIndexHOTStorage.RowGroupDirectory> parallel =
          ProjectionIndexHOTStorage.readAllRowGroupDirectoriesFromColumnSegmentSlots(rtx.getStorageEngineReader(),
              INDEX, groups, physicalOrder, worker -> {
                try (JsonNodeReadOnlyTrx workerTrx = session.beginNodeReadOnlyTrx(revision)) {
                  worker.accept(workerTrx.getStorageEngineReader());
                  workers.incrementAndGet();
                }
              }, true);
      assertTrue(workers.get() > 0, "the fixture must engage worker readers");
      assertNotNull(parallel);
      assertEquals(groups, parallel.size());
      assertDirectory(parallel.get(0),
          requireNonNull(ProjectionIndexColumnSegmentCodec.encode(pageWithRows(10L, 602).serialize())), true);
      final List<ProjectionIndexHOTStorage.RowGroupDirectory> serial =
          ProjectionIndexHOTStorage.readAllRowGroupDirectoriesFromColumnSegmentSlots(rtx.getStorageEngineReader(),
              INDEX, groups);
      assertNotNull(serial);
      for (int group = 0; group < groups; group++) {
        assertEquals(serial.get(group).rowGroupId(), parallel.get(group).rowGroupId());
        assertArrayEquals(serial.get(group).descriptor(), parallel.get(group).descriptor());
        assertArrayEquals(serial.get(group).columnSegmentOffsets(), parallel.get(group).columnSegmentOffsets());
      }
    }
  }

  @ParameterizedTest(name = "{0}")
  @EnumSource(VersioningType.class)
  void appendedRowsAreMergedIdenticallyByEveryReaderAtEveryRevision(final VersioningType versioningType)
      throws IOException {
    final Path databasePath = create(versioningType, "merge");
    final long keyBase = 10L;
    final byte[] otherRaw = pageWithRows(1_000_000L, 5).serialize();
    final int[] appends = {1, 2, 1, 3};
    ProjectionOpenRowGroupTail.clearCacheForTesting();
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage = new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX);
        assertTrue(storage.putRowGroupAsColumnSegmentSlots(1,
            requireNonNull(ProjectionIndexColumnSegmentCodec.encode(pageWithRows(keyBase, 600).serialize()))));
        assertTrue(storage.putRowGroupAsColumnSegmentSlots(2,
            requireNonNull(ProjectionIndexColumnSegmentCodec.encode(otherRaw))));
        wtx.commit();
      }
      final long body0Offset;
      final long body2Offset;
      final byte[] baseDescriptor;
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final StorageEngineReader reader = rtx.getStorageEngineReader();
        assertEveryReaderSees(reader, pageWithRows(keyBase, 600), otherRaw, false);
        body0Offset = bodyOffset(reader, BODY_0);
        body2Offset = bodyOffset(reader, BODY_2);
        baseDescriptor =
            ProjectionIndexHOTStorage.readBlob(reader, INDEX, ProjectionIndexHOTStorage.rowGroupDescriptorSlotKey(1));
      }
      int rows = 600;
      for (final int count : appends) {
        try (JsonNodeTrx wtx = session.beginNodeTrx()) {
          final ProjectionIndexHOTStorage storage = new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX);
          tailAppend(storage, keyBase, rows, count);
          final long merges = mergeCount.get();
          assertArrayEquals(pageWithRows(keyBase, rows + count).serialize(),
              storage.getRowGroupFromColumnSegmentSlots(1));
          assertEquals(merges, mergeCount.get(), "append seeds the writer memo");
          ProjectionOpenRowGroupTail.clearCacheForTesting();
          assertArrayEquals(pageWithRows(keyBase, rows + count).serialize(),
              storage.getRowGroupFromColumnSegmentSlots(1));
          assertEquals(merges + 1, mergeCount.get(),
              "a cold same-transaction read resolves referenced tail blobs from the intent log");
          wtx.commit();
        }
        rows += count;
        try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
          final StorageEngineReader reader = rtx.getStorageEngineReader();
          assertEveryReaderSees(reader, pageWithRows(keyBase, rows), otherRaw, true);
          assertEquals(body0Offset, bodyOffset(reader, BODY_0), "no column segment is rewritten by a tail append");
          assertEquals(body2Offset, bodyOffset(reader, BODY_2), "no column segment is rewritten by a tail append");
        }
      }
      // every earlier revision still reconstructs its own merged row group
      int revision = 2;
      int seen = 600;
      for (final int count : appends) {
        seen += count;
        try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision)) {
          assertEveryReaderSees(rtx.getStorageEngineReader(), pageWithRows(keyBase, seen), otherRaw, true);
        }
        revision++;
      }
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(1)) {
        assertEveryReaderSees(rtx.getStorageEngineReader(), pageWithRows(keyBase, 600), otherRaw, false);
      }
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final StorageEngineReader reader = rtx.getStorageEngineReader();
        final ProjectionOpenRowGroupTail.Header header = ProjectionOpenRowGroupTail.Header.decode(
            ProjectionIndexHOTStorage.readBlob(reader, INDEX, ProjectionOpenRowGroupTail.headerSlot(1)), 1);
        assertEquals(appends.length, header.blobCount());
        assertEquals(rows - 600, header.rowCount());
        assertArrayEquals(baseDescriptor, header.baseDescriptor(), "the tail header keeps the base descriptor");
        for (int seq = 1; seq <= appends.length; seq++) {
          assertNotNull(ProjectionIndexHOTStorage.readBlob(reader, INDEX, ProjectionOpenRowGroupTail.rowsSlot(1, seq)));
          assertTrue(ProjectionIndexHOTStorage.segmentPageOffset(reader, INDEX,
              ProjectionOpenRowGroupTail.rowsSlot(1, seq), 0) >= 0,
              "tail rows blobs are referenced side pages, not inline leaf entries (blob " + seq + ")");
        }
      }
      // the writer's own same-transaction reads merge too — from the memo the last append seeded
      // (no tail replay), and identically after the memo is dropped (one cold merge with a batch read)
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage = new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX);
        final ProjectionIndexRowGroupPage expected = pageWithRows(keyBase, rows);
        final long mergesBefore = mergeCount.get();
        assertArrayEquals(expected.serialize(), storage.getRowGroupFromColumnSegmentSlots(1));
        assertEquals(mergesBefore, mergeCount.get(),
            "the next writer hydrates the tailed row group from the seeded memo");
        ProjectionOpenRowGroupTail.clearCacheForTesting();
        assertArrayEquals(expected.serialize(), storage.getRowGroupFromColumnSegmentSlots(1));
        assertEquals(mergesBefore + 1, mergeCount.get(), "a dropped memo costs exactly one cold merge");
        final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded =
            requireNonNull(ProjectionIndexColumnSegmentCodec.encode(expected.serialize()));
        final byte[] descriptor = requireNonNull(storage.getVerifiedRowGroupDescriptor(1));
        assertTrue(RowGroupDescriptor.isTailed(descriptor));
        final int entry = RowGroupDescriptor.entryIndexOf(descriptor, BODY_2);
        assertArrayEquals(encoded.segments()[entry],
            storage.getVerifiedColumnSegment(1, descriptor, BODY_2, ProjectionIndexColumnSegmentCodec.SEG_KIND_BODY));
        wtx.rollback();
      }
    }
    // Drop the process memo as well as closing every transaction: each historical tail must be
    // reconstructed from persisted base segments and side pages after a cold reopen.
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
      int seen = 600;
      for (int revision = 1; revision <= appends.length + 1; revision++) {
        if (revision > 1) {
          seen += appends[revision - 2];
        }
        try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision)) {
          ProjectionOpenRowGroupTail.clearCacheForTesting();
          assertEveryReaderSees(rtx.getStorageEngineReader(), pageWithRows(keyBase, seen), otherRaw, revision > 1);
          ProjectionOpenRowGroupTail.clearCacheForTesting();
          final List<byte[]> all =
              ProjectionIndexHOTStorage.readAllRowGroupsFromColumnSegmentSlots(rtx.getStorageEngineReader(), INDEX, 2);
          assertArrayEquals(pageWithRows(keyBase, seen).serialize(), all.get(0));
          ProjectionOpenRowGroupTail.clearCacheForTesting();
          final List<ProjectionIndexHOTStorage.RowGroupDirectory> directories =
              ProjectionIndexHOTStorage.readAllRowGroupDirectoriesFromColumnSegmentSlots(rtx.getStorageEngineReader(),
                  INDEX, 2);
          assertNotNull(directories);
          assertDirectory(directories.get(0),
              requireNonNull(ProjectionIndexColumnSegmentCodec.encode(pageWithRows(keyBase, seen).serialize())),
              revision > 1);
        }
      }
    }
  }

  @ParameterizedTest(name = "{0}")
  @EnumSource(VersioningType.class)
  void theTailFoldsAtCompletionOnNonAppendWritesOnDemandAndOnTombstone(final VersioningType versioningType)
      throws IOException {
    final Path databasePath = create(versioningType, "fold");
    final long keyBase = 20L;
    final byte[] otherRaw = pageWithRows(2_000_000L, 3).serialize();
    ProjectionOpenRowGroupTail.clearCacheForTesting();
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
      final int base = ProjectionIndexRowGroupPage.MAX_ROWS - 3;
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage = new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX);
        assertTrue(storage.putRowGroupAsColumnSegmentSlots(1,
            requireNonNull(ProjectionIndexColumnSegmentCodec.encode(pageWithRows(keyBase, base).serialize()))));
        assertTrue(storage.putRowGroupAsColumnSegmentSlots(2,
            requireNonNull(ProjectionIndexColumnSegmentCodec.encode(otherRaw))));
        wtx.commit();
      }
      final long body0Before;
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        body0Before = bodyOffset(rtx.getStorageEngineReader(), BODY_0);
      }
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        tailAppend(new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX), keyBase, base, 2);
        wtx.commit();
      }
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertEveryReaderSees(rtx.getStorageEngineReader(), pageWithRows(keyBase, base + 2), otherRaw, true);
      }
      // completion: the row group becomes full — the maintenance path writes it whole, which folds
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage = new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX);
        assertTrue(storage.putRowGroupAsColumnSegmentSlots(1, requireNonNull(ProjectionIndexColumnSegmentCodec.encode(
            pageWithRows(keyBase, ProjectionIndexRowGroupPage.MAX_ROWS).serialize()))));
        wtx.commit();
      }
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final StorageEngineReader reader = rtx.getStorageEngineReader();
        assertEveryReaderSees(reader, pageWithRows(keyBase, ProjectionIndexRowGroupPage.MAX_ROWS), otherRaw, false);
        assertTailGone(reader, 2);
        assertNotEquals(body0Before, bodyOffset(reader, BODY_0), "the fold writes the merged segments");
      }
      // the revision before the fold still serves the tailed row group
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(2)) {
        assertEveryReaderSees(rtx.getStorageEngineReader(), pageWithRows(keyBase, base + 2), otherRaw, true);
      }
      // a non-append rewrite (one row removed) of a tailed row group folds it into the rebuilt content
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage = new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX);
        assertTrue(storage.putRowGroupAsColumnSegmentSlots(1,
            requireNonNull(ProjectionIndexColumnSegmentCodec.encode(pageWithRows(keyBase, 100).serialize()))));
        wtx.commit();
      }
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage = new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX);
        tailAppend(storage, keyBase, 100, 3);
        ProjectionOpenRowGroupTail.clearCacheForTesting();
        assertArrayEquals(pageWithRows(keyBase, 103).serialize(), storage.getRowGroupFromColumnSegmentSlots(1),
            "a reused tail namespace reads this transaction's new side-page payload");
        wtx.commit();
      }
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage = new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX);
        assertTrue(storage.putRowGroupAsColumnSegmentSlots(1,
            requireNonNull(ProjectionIndexColumnSegmentCodec.encode(pageWithRows(keyBase, 103, 50).serialize()))));
        wtx.commit();
      }
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final StorageEngineReader reader = rtx.getStorageEngineReader();
        assertEveryReaderSees(reader, pageWithRows(keyBase, 103, 50), otherRaw, false);
        assertTailGone(reader, 1);
      }
      // on demand (the column-patch pre-fold): content identical, tail gone, idempotent
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        tailAppend(new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX), keyBase, 103, 1);
        wtx.commit();
      }
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage = new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX);
        final byte[] tailedDescriptor = requireNonNull(storage.getVerifiedRowGroupDescriptor(1));
        assertTrue(RowGroupDescriptor.isTailed(tailedDescriptor));
        assertThrows(IllegalStateException.class,
            () -> storage.putColumnPatches(1, tailedDescriptor, tailedDescriptor,
                new ProjectionIndexColumnSegmentCodec.EncodedColumn[1], 1, new long[1]),
            "column patches refuse a tailed prior");
        assertTrue(storage.foldOpenRowGroupTail(1));
        assertFalse(storage.foldOpenRowGroupTail(1));
        assertFalse(RowGroupDescriptor.isTailed(requireNonNull(storage.getVerifiedRowGroupDescriptor(1))));
        wtx.commit();
      }
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final StorageEngineReader reader = rtx.getStorageEngineReader();
        final ProjectionIndexRowGroupPage expected = pageWithRows(keyBase, 103, 50);
        final long[] longs = {ageAt(103), 0L, 0L};
        final boolean[] bools = {false, flagAt(103), false};
        assertTrue(expected.appendRow(keyAt(keyBase, 103), longs, bools, new String[] {null, null, deptAt(103)},
            new boolean[] {true, true, true}, new boolean[3], new boolean[3]));
        assertEveryReaderSees(reader, expected, otherRaw, false);
        assertTailGone(reader, 1);
      }
      // tombstoning a tailed row group removes its tail slots too
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        tailAppend(new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX), keyBase, 104, 1);
        wtx.commit();
      }
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX).tombstoneRowGroupAsColumnSegmentSlots(1);
        wtx.commit();
      }
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final StorageEngineReader reader = rtx.getStorageEngineReader();
        assertNull(ProjectionIndexHOTStorage.readRowGroupFromColumnSegmentSlots(reader, INDEX, 1));
        assertTailGone(reader, 1);
      }
    }
  }

  private static void assertTailGone(final StorageEngineReader reader, final int blobs) {
    assertNull(ProjectionIndexHOTStorage.readBlob(reader, INDEX, ProjectionOpenRowGroupTail.headerSlot(1)),
        "tail header tombstoned");
    for (int seq = 1; seq <= blobs; seq++) {
      assertNull(ProjectionIndexHOTStorage.readBlob(reader, INDEX, ProjectionOpenRowGroupTail.rowsSlot(1, seq)),
          "tail rows blob " + seq + " tombstoned");
    }
  }

  @ParameterizedTest(name = "{0}")
  @EnumSource(VersioningType.class)
  void publicReadResultsCannotMutateTheMergeMemo(final VersioningType versioningType) {
    final Path databasePath = create(versioningType, "read-ownership");
    final long keyBase = 10L;
    final byte[] expected = pageWithRows(keyBase, 602).serialize();
    final byte[] otherRaw = pageWithRows(1_000_000L, 5).serialize();
    ProjectionOpenRowGroupTail.clearCacheForTesting();
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
      try (JsonNodeTrx writer = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage = new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), INDEX);
        storage.putRowGroupAsColumnSegmentSlots(1,
            requireNonNull(ProjectionIndexColumnSegmentCodec.encode(pageWithRows(keyBase, 600).serialize())));
        storage.putRowGroupAsColumnSegmentSlots(2, requireNonNull(ProjectionIndexColumnSegmentCodec.encode(otherRaw)));
        writer.commit();
      }
      try (JsonNodeTrx writer = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage = new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), INDEX);
        tailAppend(storage, keyBase, 600, 2);
        final byte[] exposed = storage.getRowGroupFromColumnSegmentSlots(1);
        assertNotNull(exposed);
        exposed[0] ^= 1;
        assertArrayEquals(expected, storage.getRowGroupFromColumnSegmentSlots(1), "writer read owns its result");
        writer.commit();
      }
      try (JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx()) {
        final StorageEngineReader storage = reader.getStorageEngineReader();
        final byte[] exposed = ProjectionIndexHOTStorage.readRowGroupFromColumnSegmentSlots(storage, INDEX, 1);
        assertNotNull(exposed);
        exposed[0] ^= 1;
        assertArrayEquals(expected, ProjectionIndexHOTStorage.readRowGroupFromColumnSegmentSlots(storage, INDEX, 1),
            "committed read owns its result");
        final List<byte[]> batch = ProjectionIndexHOTStorage.readAllRowGroupsFromColumnSegmentSlots(storage, INDEX, 2);
        batch.get(0)[0] ^= 1;
        assertArrayEquals(expected, ProjectionIndexHOTStorage.readRowGroupFromColumnSegmentSlots(storage, INDEX, 1),
            "batch read owns its result");
        final List<ProjectionIndexHOTStorage.RowGroupDirectory> directories =
            ProjectionIndexHOTStorage.readAllRowGroupDirectoriesFromColumnSegmentSlots(storage, INDEX, 2);
        assertNotNull(directories);
        final byte[][] segments = directories.get(0).inlineColumnSegmentBytes();
        assertNotNull(segments);
        for (final byte[] segment : segments) {
          if (segment != null && segment.length > 0) {
            segment[0] ^= 1;
            break;
          }
        }
        assertEveryReaderSees(storage, pageWithRows(keyBase, 602), otherRaw, true);
      }
    }
  }

  @ParameterizedTest(name = "{0}")
  @EnumSource(VersioningType.class)
  void referenceBoundFoldsAtomicallyAndReusedSlotsPreserveEveryRevision(final VersioningType versioningType) {
    final Path databasePath = create(versioningType, "reference-bound");
    final long keyBase = 30L;
    final int baseRows = 600;
    final int appends = 70;
    final byte[] otherRaw = pageWithRows(3_000_000L, 4).serialize();
    ProjectionOpenRowGroupTail.clearCacheForTesting();
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
      try (JsonNodeTrx writer = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage = new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), INDEX);
        storage.putRowGroupAsColumnSegmentSlots(1,
            requireNonNull(ProjectionIndexColumnSegmentCodec.encode(pageWithRows(keyBase, baseRows).serialize())));
        storage.putRowGroupAsColumnSegmentSlots(2, requireNonNull(ProjectionIndexColumnSegmentCodec.encode(otherRaw)));
        writer.commit();
      }
      for (int append = 1; append <= appends; append++) {
        if (append == 65) {
          // Roll back the reference-bound fold before committing it. The published 64-blob tail
          // must retain both its descriptor and every referenced row, despite the tombstones.
          try (JsonNodeTrx writer = session.beginNodeTrx()) {
            final ProjectionIndexHOTStorage storage =
                new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), INDEX);
            tailAppend(storage, keyBase, baseRows + append - 1, 1);
            assertFalse(RowGroupDescriptor.isTailed(requireNonNull(storage.getVerifiedRowGroupDescriptor(1))));
            assertArrayEquals(pageWithRows(keyBase, baseRows + append).serialize(),
                storage.getRowGroupFromColumnSegmentSlots(1));
            writer.rollback();
          }
          ProjectionOpenRowGroupTail.clearCacheForTesting();
          try (JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx()) {
            assertEveryReaderSees(reader.getStorageEngineReader(), pageWithRows(keyBase, baseRows + 64), otherRaw,
                true);
            final ProjectionOpenRowGroupTail.Header header =
                ProjectionOpenRowGroupTail.Header.decode(ProjectionIndexHOTStorage.readBlob(
                    reader.getStorageEngineReader(), INDEX, ProjectionOpenRowGroupTail.headerSlot(1)), 1);
            assertEquals(64, header.blobCount());
          }
        }
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          final ProjectionIndexHOTStorage storage =
              new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), INDEX);
          tailAppend(storage, keyBase, baseRows + append - 1, 1);
          assertArrayEquals(pageWithRows(keyBase, baseRows + append).serialize(),
              storage.getRowGroupFromColumnSegmentSlots(1));
          writer.commit();
        }
        ProjectionOpenRowGroupTail.clearCacheForTesting();
        try (JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx()) {
          final StorageEngineReader storage = reader.getStorageEngineReader();
          assertEveryReaderSees(storage, pageWithRows(keyBase, baseRows + append), otherRaw, append != 65);
          if (append == 65) {
            assertTailGone(storage, 64);
          } else {
            final ProjectionOpenRowGroupTail.Header header = ProjectionOpenRowGroupTail.Header.decode(
                ProjectionIndexHOTStorage.readBlob(storage, INDEX, ProjectionOpenRowGroupTail.headerSlot(1)), 1);
            assertEquals(append < 65
                ? append
                : append - 65, header.blobCount());
            assertEquals(append < 65
                ? baseRows
                : baseRows + 65, RowGroupDescriptor.rowCount(header.baseDescriptor()));
          }
        }
      }
    }
    // Reopen and force each historical revision to reconstruct from its own base and tail slots.
    // Slots 1..5 have been reused after the fold; their earlier payloads must still be readable.
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
      for (int append = 0; append <= appends; append++) {
        ProjectionOpenRowGroupTail.clearCacheForTesting();
        try (JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx(append + 1)) {
          assertEveryReaderSees(reader.getStorageEngineReader(), pageWithRows(keyBase, baseRows + append), otherRaw,
              append != 0 && append != 65);
        }
      }
    }
  }

  @ParameterizedTest(name = "{0}")
  @EnumSource(VersioningType.class)
  void aRollbackAroundTheFoldLeavesTheTailIntact(final VersioningType versioningType) throws IOException {
    final Path databasePath = create(versioningType, "rollback");
    final long keyBase = 30L;
    final byte[] otherRaw = pageWithRows(3_000_000L, 4).serialize();
    ProjectionOpenRowGroupTail.clearCacheForTesting();
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage = new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX);
        assertTrue(storage.putRowGroupAsColumnSegmentSlots(1,
            requireNonNull(ProjectionIndexColumnSegmentCodec.encode(pageWithRows(keyBase, 200).serialize()))));
        assertTrue(storage.putRowGroupAsColumnSegmentSlots(2,
            requireNonNull(ProjectionIndexColumnSegmentCodec.encode(otherRaw))));
        wtx.commit();
      }
      // a rolled-back tail append leaves the row group untailed
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        tailAppend(new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX), keyBase, 200, 2);
        wtx.rollback();
      }
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertEveryReaderSees(rtx.getStorageEngineReader(), pageWithRows(keyBase, 200), otherRaw, false);
        assertTailGone(rtx.getStorageEngineReader(), 1);
      }
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        tailAppend(new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX), keyBase, 200, 2);
        wtx.commit();
      }
      // a rolled-back fold leaves the tail exactly as published
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage = new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX);
        assertTrue(storage.foldOpenRowGroupTail(1));
        assertFalse(RowGroupDescriptor.isTailed(requireNonNull(storage.getVerifiedRowGroupDescriptor(1))));
        wtx.rollback();
      }
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final StorageEngineReader reader = rtx.getStorageEngineReader();
        assertEveryReaderSees(reader, pageWithRows(keyBase, 202), otherRaw, true);
        final ProjectionOpenRowGroupTail.Header header = ProjectionOpenRowGroupTail.Header.decode(
            ProjectionIndexHOTStorage.readBlob(reader, INDEX, ProjectionOpenRowGroupTail.headerSlot(1)), 1);
        assertEquals(1, header.blobCount());
        assertEquals(2, header.rowCount());
      }
      // the committed fold
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        assertTrue(new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX).foldOpenRowGroupTail(1));
        wtx.commit();
      }
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertEveryReaderSees(rtx.getStorageEngineReader(), pageWithRows(keyBase, 202), otherRaw, false);
        assertTailGone(rtx.getStorageEngineReader(), 1);
      }
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(2)) {
        assertEveryReaderSees(rtx.getStorageEngineReader(), pageWithRows(keyBase, 202), otherRaw, true);
      }
    }
  }

  @Test
  void tailNamespaceCoversTheSameRowGroupRangeAsStorage() {
    final long last = ProjectionOpenRowGroupTail.rowsSlot(ProjectionIndexHOTStorage.MAX_ROW_GROUPS,
        ProjectionOpenRowGroupTail.MAX_TAIL_BLOBS);
    assertTrue(last < (1L << 47), "side-page owners fit the HOT composite reference key");
    assertTrue(ProjectionOpenRowGroupTail.isTailSlot(last));
    assertThrows(IllegalArgumentException.class, () -> ProjectionOpenRowGroupTail.headerSlot(0));
    assertThrows(IllegalArgumentException.class,
        () -> ProjectionOpenRowGroupTail.headerSlot(ProjectionIndexHOTStorage.MAX_ROW_GROUPS + 1L));
    assertThrows(IllegalArgumentException.class, () -> ProjectionOpenRowGroupTail.rowsSlot(1, 0));
  }

  @Test
  void malformedRowBlobsAreRejectedBeforeAllocatingTheirDeclaredLanes() {
    final byte[] kinds = {ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT};
    final ProjectionOpenRowGroupTail.Row row = new ProjectionOpenRowGroupTail.Row(1, false, new byte[] {1}, new long[1],
        new boolean[1], new byte[][] {new byte[] {65}}, new String[1][], new boolean[] {true}, new boolean[1],
        new boolean[1], new boolean[1]);
    final byte[] blob = ProjectionOpenRowGroupTail.encodeRows(kinds, List.of(row));
    final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(kinds);
    assertTrue(page.appendRow(1, new long[1], new boolean[1], new String[] {"A"}, new boolean[] {true}, new boolean[1],
        new boolean[1]));
    final byte[] descriptor = requireNonNull(ProjectionIndexColumnSegmentCodec.encode(page.serialize())).descriptor();
    ProjectionOpenRowGroupTail.validateAppendHeader(blob, descriptor, 1);
    assertThrows(IllegalArgumentException.class,
        () -> ProjectionOpenRowGroupTail.validateAppendHeader(blob, descriptor, 2));
    final byte[] wrongKinds = blob.clone();
    wrongKinds[11] = ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN;
    assertThrows(IllegalArgumentException.class,
        () -> ProjectionOpenRowGroupTail.validateAppendHeader(wrongKinds, descriptor, 1));
    for (int length = 0; length < blob.length; length++) {
      final byte[] truncated = Arrays.copyOf(blob, length);
      assertThrows(IllegalStateException.class, () -> ProjectionOpenRowGroupTail.decodeRows(truncated, kinds, 1));
    }
    final byte[] hugeString = blob.clone();
    RowGroupDescriptor.putIntLE(hugeString, 25, Integer.MAX_VALUE);
    assertThrows(IllegalStateException.class, () -> ProjectionOpenRowGroupTail.decodeRows(hugeString, kinds, 1));
    final byte[] setKinds = {ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET};
    hugeString[11] = setKinds[0];
    assertThrows(IllegalStateException.class, () -> ProjectionOpenRowGroupTail.decodeRows(hugeString, setKinds, 1));
    final byte[] trailing = Arrays.copyOf(blob, blob.length + 1);
    assertThrows(IllegalStateException.class, () -> ProjectionOpenRowGroupTail.decodeRows(trailing, kinds, 1));
  }

  @Test
  void tailRowsBlobRoundTripsEveryColumnKindAndFlag() {
    final byte[] kinds = {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG,
        ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN, ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT,
        ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET, ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL,
        ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_DOUBLE, ProjectionIndexRowGroupPage.COLUMN_KIND_TIMESTAMP};
    final List<ProjectionOpenRowGroupTail.Row> rows = new ArrayList<>();
    rows.add(new ProjectionOpenRowGroupTail.Row(41L, false, new byte[] {1, 2, 3},
        new long[] {-7L, 0L, 0L, 0L, 123_456_789L, Double.doubleToRawLongBits(2.5), 1_700_000_000_000L},
        new boolean[] {false, true, false, false, false, false, false},
        new byte[][] {null, null, "Ärztin".getBytes(StandardCharsets.UTF_8), null, null, null, null},
        new String[][] {null, null, null, {"x", "yy", ""}, null, null, null},
        new boolean[] {true, true, true, true, true, true, true}, new boolean[7], new boolean[7],
        new boolean[] {false, false, false, false, false, true, false}));
    rows.add(new ProjectionOpenRowGroupTail.Row(42L, true, new byte[] {1, 2, 4}, new long[7], new boolean[7],
        new byte[][] {null, null, new byte[0], null, null, null, null},
        new String[][] {null, null, null, new String[0], null, null, null},
        new boolean[] {false, true, false, true, false, false, false},
        new boolean[] {false, false, true, false, true, false, false},
        new boolean[] {true, false, false, false, false, true, false}, new boolean[7]));
    final byte[] blob = ProjectionOpenRowGroupTail.encodeRows(kinds, rows);
    final List<ProjectionOpenRowGroupTail.Row> decoded = ProjectionOpenRowGroupTail.decodeRows(blob, kinds, 7L);
    assertEquals(rows.size(), decoded.size());
    for (int i = 0; i < rows.size(); i++) {
      final ProjectionOpenRowGroupTail.Row a = rows.get(i);
      final ProjectionOpenRowGroupTail.Row b = decoded.get(i);
      assertEquals(a.recordKey(), b.recordKey());
      assertEquals(a.orderException(), b.orderException());
      assertArrayEquals(a.orderLabel(), b.orderLabel());
      assertArrayEquals(a.longs(), b.longs());
      assertArrayEquals(a.bools(), b.bools());
      assertArrayEquals(a.present(), b.present());
      assertArrayEquals(a.unrepresentable(), b.unrepresentable());
      assertArrayEquals(a.nonIntegral(), b.nonIntegral());
      assertArrayEquals(a.nonDoubleSource(), b.nonDoubleSource());
      for (int c = 0; c < kinds.length; c++) {
        if (kinds[c] == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT) {
          assertArrayEquals(a.strings()[c] == null
              ? new byte[0]
              : a.strings()[c], b.strings()[c]);
        } else if (kinds[c] == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET) {
          assertArrayEquals(a.sets()[c] == null
              ? new String[0]
              : a.sets()[c], b.sets()[c]);
        }
      }
    }
    assertThrows(IllegalStateException.class, () -> ProjectionOpenRowGroupTail.decodeRows(blob, KINDS, 7L),
        "a blob for other column kinds is refused");
    final ProjectionOpenRowGroupTail.Header header = new ProjectionOpenRowGroupTail.Header(
        requireNonNull(ProjectionIndexColumnSegmentCodec.encode(pageWithRows(5L, 3).serialize())).descriptor(), 2, 5);
    final ProjectionOpenRowGroupTail.Header back = ProjectionOpenRowGroupTail.Header.decode(header.encode(), 9L);
    assertArrayEquals(header.baseDescriptor(), back.baseDescriptor());
    assertEquals(2, back.blobCount());
    assertEquals(5, back.rowCount());
  }
}
