package io.sirix.index.hot;

import com.google.gson.JsonPrimitive;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.Path;
import io.brackit.query.util.path.PathParser;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.json.JsonIndexController;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.IndexType;
import io.sirix.index.SearchMode;
import io.sirix.index.path.json.JsonPCRCollector;
import io.sirix.index.redblacktree.keyvalue.CASValue;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.roaringbitmap.longlong.LongIterator;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class PostingDeltaLongCasKeyTest {
  private static final int ROWS = 600;
  private static final int REMOVED = 200;
  private static final String CATEGORY_PATH = "/[]/category";
  private static final String PREFIX = "\0".repeat(130);
  private static final String BOUNDARY = "\0".repeat(117) + 'a';

  @TempDir
  File temporaryDirectory;

  @AfterEach
  void clearCaches() {
    Databases.clearGlobalCaches();
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void bulkPostingsStayExactAtEveryRevisionAndAfterReopen(final VersioningType versioningType) {
    exercisePostings(versioningType, true);
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void incrementalPostingsStayExactAtEveryRevisionAndAfterReopen(final VersioningType versioningType) {
    exercisePostings(versioningType, false);
  }

  private void exercisePostings(final VersioningType versioningType, final boolean bulk) {
    final File directory = new File(temporaryDirectory, "postings");
    final List<long[]> snapshots = new ArrayList<>(3);
    final TreeSet<Long> expected = new TreeSet<>();
    final CASValue[] keys = new CASValue[ROWS];
    for (int i = 0; i < ROWS; i++) {
      keys[i] = key(value(i));
      expected.add(i + 1L);
    }
    final CASValue control = key("z");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(directory.toPath())));
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(directory.toPath())) {
      database.createResource(ResourceConfiguration.newBuilder("resource")
                                                   .versioningApproach(versioningType)
                                                   .maxNumberOfRevisionsToRestore(3)
                                                   .storeDiffs(false)
                                                   .build());
      try (final JsonResourceSession session = database.beginResourceSession("resource")) {
        for (int revision = 1; revision <= 3; revision++) {
          try (final JsonNodeTrx trx = session.beginNodeTrx()) {
            final HOTIndexWriter<CASValue> writer =
                HOTIndexWriter.create(trx.getStorageEngineWriter(), CASKeySerializer.INSTANCE, IndexType.CAS, 0);
            if (revision == 1) {
              if (bulk) {
                final HOTBulkIndexLoader<CASValue> loader = writer.createBulkLoader();
                for (int i = 0; i < ROWS; i++) {
                  loader.add(keys[i], i + 1L);
                }
                loader.add(control, ROWS + 1L);
                loader.flush();
              } else {
                for (int i = 0; i < ROWS; i++) {
                  writer.indexNodeKey(keys[i], i + 1L);
                }
                writer.indexNodeKey(control, ROWS + 1L);
              }
            } else if (revision == 2) {
              assertTrue(writer.remove(keys[REMOVED], REMOVED + 1L));
              assertFalse(writer.remove(keys[REMOVED], REMOVED + 1L));
              assertTrue(expected.remove(REMOVED + 1L));
            } else {
              writer.indexNodeKey(keys[REMOVED], REMOVED + 1L);
              writer.indexNodeKey(keys[REMOVED], REMOVED + 1L);
              assertTrue(expected.add(REMOVED + 1L));
            }
            final long[] snapshot = expected.stream().mapToLong(Long::longValue).toArray();
            for (final CASValue key : keys) {
              assertArrayEquals(snapshot, requireNonNull(writer.get(key, SearchMode.EQUAL)).toSortedArray());
            }
            assertArrayEquals(new long[] {ROWS + 1L},
                requireNonNull(writer.get(control, SearchMode.EQUAL)).toSortedArray());
            snapshots.add(snapshot);
            trx.commit();
          }
          assertPostings(session, revision, keys, snapshots.get(revision - 1));
        }
        for (int revision = 1; revision <= snapshots.size(); revision++) {
          assertPostings(session, revision, keys, snapshots.get(revision - 1));
        }
      }
    }
    Databases.clearGlobalCaches();
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(directory.toPath());
        final JsonResourceSession session = database.beginResourceSession("resource")) {
      for (int revision = 1; revision <= snapshots.size(); revision++) {
        assertPostings(session, revision, keys, snapshots.get(revision - 1));
      }
    }
  }

  private static void assertPostings(final JsonResourceSession session, final int revision, final CASValue[] keys,
      final long[] expected) {
    try (final JsonNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx(revision)) {
      final HOTIndexReader<CASValue> reader =
          HOTIndexReader.create(trx.getStorageEngineReader(), CASKeySerializer.INSTANCE, IndexType.CAS, 0);
      for (final CASValue key : keys) {
        assertArrayEquals(expected, requireNonNull(reader.get(key, SearchMode.EQUAL)).toSortedArray());
      }
      assertArrayEquals(new long[] {ROWS + 1L}, requireNonNull(reader.get(key("z"), SearchMode.EQUAL)).toSortedArray());
      final Iterator<Map.Entry<CASValue, NodeReferences>> groups = reader.range(key(value(0)), key(value(ROWS - 1)));
      assertTrue(groups.hasNext());
      assertArrayEquals(expected, groups.next().getValue().toSortedArray());
      assertFalse(groups.hasNext());
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void bulkValueRechecksStayExactAcrossDeltasFoldsAndReopen(final VersioningType versioningType) {
    exerciseValueRechecks(versioningType, true);
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void incrementalValueRechecksStayExactAcrossDeltasFoldsAndReopen(final VersioningType versioningType) {
    exerciseValueRechecks(versioningType, false);
  }

  private void exerciseValueRechecks(final VersioningType versioningType, final boolean bulk) {
    final File directory = new File(temporaryDirectory, "values");
    final List<long[]> snapshots = new ArrayList<>(4);
    final long[] valueKeys = new long[ROWS + 3];
    final long[] objectKeys = new long[valueKeys.length];
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(directory.toPath())));
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(directory.toPath())) {
      database.createResource(ResourceConfiguration.newBuilder("resource")
                                                   .versioningApproach(versioningType)
                                                   .maxNumberOfRevisionsToRestore(3)
                                                   .storeDiffs(false)
                                                   .build());
      try (final JsonResourceSession session = database.beginResourceSession("resource")) {
        try (final JsonNodeTrx trx = session.beginNodeTrx()) {
          final IndexDef def = IndexDefs.createCASIdxDef(false, Type.STR,
              Set.of(Path.parse(CATEGORY_PATH, PathParser.Type.JSON)), 0, IndexDef.DbType.JSON);
          if (bulk) {
            final StringBuilder json = new StringBuilder(ROWS * 850).append('[');
            for (int i = 0; i < valueKeys.length; i++) {
              if (i != 0) {
                json.append(',');
              }
              json.append(objectJson(value(i)));
            }
            trx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.append(']').toString()),
                JsonNodeTrx.Commit.NO);
            session.getWtxIndexController(trx.getRevisionNumber()).createIndexes(Set.of(def), trx);
            trx.moveToDocumentRoot();
            assertTrue(trx.moveToFirstChild());
            assertTrue(trx.moveToFirstChild());
            for (int i = 0; i < valueKeys.length; i++) {
              objectKeys[i] = trx.getNodeKey();
              assertTrue(trx.moveToFirstChild());
              valueKeys[i] = trx.getNodeKey();
              assertTrue(trx.moveTo(objectKeys[i]));
              if (i + 1 < valueKeys.length) {
                assertTrue(trx.moveToRightSibling());
              }
            }
          } else {
            trx.insertArrayAsFirstChild();
            session.getWtxIndexController(trx.getRevisionNumber()).createIndexes(Set.of(def), trx);
            for (int i = 0; i < valueKeys.length; i++) {
              objectKeys[i] = insertObject(trx, value(i));
              assertTrue(trx.moveToFirstChild());
              valueKeys[i] = trx.getNodeKey();
            }
          }
          trx.commit();
        }
        snapshots.add(valueKeys.clone());
        assertValueRechecks(session, 1, valueKeys);
        final long writes = HOTIndexWriter.postingDeltaWrites();
        final long folds = HOTIndexWriter.postingDeltaFolds();
        final long referenced = AbstractHOTIndexWriter.REFERENCED_CHUNK_WRITES.get();
        for (int revision = 2; revision <= 4; revision++) {
          try (final JsonNodeTrx trx = session.beginNodeTrx()) {
            if (revision != 3) {
              assertTrue(trx.moveTo(objectKeys[REMOVED]));
              trx.remove();
              valueKeys[REMOVED] = -1;
            }
            if (revision != 2) {
              objectKeys[REMOVED] = insertObject(trx, value(REMOVED));
              assertTrue(trx.moveToFirstChild());
              valueKeys[REMOVED] = trx.getNodeKey();
            }
            if (revision == 4) {
              for (int i = 0; i < PostingDeltas.FOLD_BOUND; i++) {
                assertTrue(trx.moveTo(objectKeys[REMOVED]));
                trx.remove();
                objectKeys[REMOVED] = insertObject(trx, value(REMOVED));
                assertTrue(trx.moveToFirstChild());
                valueKeys[REMOVED] = trx.getNodeKey();
              }
            }
            trx.commit();
          }
          snapshots.add(valueKeys.clone());
          assertValueRechecks(session, revision, valueKeys);
        }
        assertTrue(HOTIndexWriter.postingDeltaWrites() > writes);
        assertTrue(HOTIndexWriter.postingDeltaFolds() > folds);
        assertTrue(AbstractHOTIndexWriter.REFERENCED_CHUNK_WRITES.get() > referenced);
        for (int revision = 1; revision <= snapshots.size(); revision++) {
          assertValueRechecks(session, revision, snapshots.get(revision - 1));
        }
      }
    }
    Databases.clearGlobalCaches();
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(directory.toPath());
        final JsonResourceSession session = database.beginResourceSession("resource")) {
      for (int revision = 1; revision <= snapshots.size(); revision++) {
        assertValueRechecks(session, revision, snapshots.get(revision - 1));
      }
    }
  }

  private static long insertObject(final JsonNodeTrx trx, final String value) {
    trx.moveToDocumentRoot();
    assertTrue(trx.moveToFirstChild());
    trx.insertSubtreeAsLastChild(JsonShredder.createStringReader(objectJson(value)), JsonNodeTrx.Commit.NO);
    return trx.getNodeKey();
  }

  private static void assertValueRechecks(final JsonResourceSession session, final int revision,
      final long[] valueKeys) {
    try (final JsonNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx(revision)) {
      final JsonIndexController controller = session.getRtxIndexController(revision);
      final IndexDef def = requireNonNull(controller.getIndexes().getIndexDef(0, IndexType.CAS));
      final HOTIndexReader<CASValue> reader =
          HOTIndexReader.create(trx.getStorageEngineReader(), CASKeySerializer.INSTANCE, IndexType.CAS, 0);
      final long[] expectedPostings =
          Arrays.stream(valueKeys, 0, ROWS).filter(nodeKey -> nodeKey >= 0).sorted().toArray();
      assertTrue(trx.moveTo(valueKeys[0]));
      final CASValue probe = new CASValue(new Str(value(REMOVED)), Type.STR, trx.getPathNodeKey());
      assertArrayEquals(expectedPostings, requireNonNull(reader.get(probe, SearchMode.EQUAL)).toSortedArray());
      for (final int index : new int[] {0, REMOVED, ROWS - 1, ROWS, ROWS + 1, ROWS + 2, ROWS + 3}) {
        final Iterator<NodeReferences> hits =
            controller.openCASIndex(trx.getStorageEngineReader(), def, controller.createCASFilter(Set.of(CATEGORY_PATH),
                new Str(value(index)), SearchMode.EQUAL, new JsonPCRCollector(trx)));
        final TreeSet<Long> actual = new TreeSet<>();
        while (hits.hasNext()) {
          final LongIterator nodeKeys = hits.next().nodeKeyIterator();
          while (nodeKeys.hasNext()) {
            actual.add(nodeKeys.next());
          }
        }
        final long expected = index < valueKeys.length
            ? valueKeys[index]
            : -1;
        assertEquals(expected < 0
            ? Set.of()
            : Set.of(expected), actual, "exact value at revision " + revision + " for row " + index);
      }
      assertPublicRanges(trx, controller, def, revision, valueKeys);
    }
  }

  private static void assertPublicRanges(final JsonNodeReadOnlyTrx trx, final JsonIndexController controller,
      final IndexDef def, final int revision, final long[] valueKeys) {
    // These short raw values share an escaped prefix that exhausts the stored-key budget.
    final CASValue first = key(value(0));
    final CASValue last = key(value(ROWS - 1));
    final byte[] firstKey = new byte[CASKeySerializer.INSTANCE.maxSerializedLength(first)];
    final byte[] lastKey = new byte[CASKeySerializer.INSTANCE.maxSerializedLength(last)];
    assertEquals(CASKeySerializer.INSTANCE.serialize(first, firstKey, 0),
        CASKeySerializer.INSTANCE.serialize(last, lastKey, 0));
    assertArrayEquals(firstKey, lastKey);
    final String[][] bounds = {{value(0), value(ROWS - 1)}, {value(REMOVED - 1), value(REMOVED + 1)},
        {value(REMOVED), value(REMOVED)}, {PREFIX + "1990", PREFIX + "2010"}, {"\0".repeat(118), PREFIX},
        {"\0".repeat(118), value(REMOVED)}, {PREFIX, value(REMOVED)}, {BOUNDARY, BOUNDARY + '\0'},
        {BOUNDARY, BOUNDARY + '\1'}, {PREFIX, "z"}, {null, value(REMOVED)}, {value(REMOVED), null}};
    for (int range = 0; range < bounds.length; range++) {
      final String min = bounds[range][0];
      final String max = bounds[range][1];
      for (final boolean includeMin : new boolean[] {false, true}) {
        for (final boolean includeMax : new boolean[] {false, true}) {
          final Set<Long> expected = expectedRange(valueKeys, min, max, includeMin, includeMax);
          final Iterator<NodeReferences> hits = controller.openCASIndex(trx.getStorageEngineReader(), def,
              controller.createCASFilterRange(Set.of(CATEGORY_PATH), min == null
                  ? null
                  : new Str(min),
                  max == null
                      ? null
                      : new Str(max),
                  includeMin, includeMax, new JsonPCRCollector(trx)));
          assertEquals(expected, rangePostings(hits), "public range " + range + " at revision " + revision
              + " inclusive [" + includeMin + ", " + includeMax + ']');
          if (min == null || max == null) {
            final SearchMode mode = min == null
                ? includeMax
                    ? SearchMode.LOWER_OR_EQUAL
                    : SearchMode.LOWER
                : includeMin
                    ? SearchMode.GREATER_OR_EQUAL
                    : SearchMode.GREATER;
            final Iterator<NodeReferences> comparisonHits = controller.openCASIndex(trx.getStorageEngineReader(), def,
                controller.createCASFilter(Set.of(CATEGORY_PATH), new Str(min == null
                    ? max
                    : min), mode, new JsonPCRCollector(trx)));
            assertEquals(expected, rangePostings(comparisonHits),
                "public comparison " + mode + " at revision " + revision);
          }
        }
      }
    }
  }

  private static Set<Long> expectedRange(final long[] valueKeys, final @Nullable String min, final @Nullable String max,
      final boolean includeMin, final boolean includeMax) {
    final Set<Long> expected = new TreeSet<>();
    for (int row = 0; row < valueKeys.length; row++) {
      if (valueKeys[row] < 0) {
        continue;
      }
      final String original = value(row);
      final int lower = min == null
          ? 1
          : original.compareTo(min);
      final int upper = max == null
          ? -1
          : original.compareTo(max);
      if ((lower > 0 || (includeMin && lower == 0)) && (upper < 0 || (includeMax && upper == 0))) {
        expected.add(valueKeys[row]);
      }
    }
    return expected;
  }

  private static Set<Long> rangePostings(final Iterator<NodeReferences> hits) {
    final TreeSet<Long> actual = new TreeSet<>();
    while (hits.hasNext()) {
      final LongIterator nodeKeys = hits.next().nodeKeyIterator();
      while (nodeKeys.hasNext()) {
        assertTrue(actual.add(nodeKeys.next()), "duplicate range posting");
      }
    }
    return actual;
  }

  private static CASValue key(final String value) {
    return new CASValue(new Str(value), Type.STR, 1);
  }

  private static String value(final int index) {
    return switch (index) {
      case ROWS -> "z";
      case ROWS + 1 -> BOUNDARY;
      case ROWS + 2 -> BOUNDARY + '\0';
      default -> PREFIX + Integer.toString(1000 + index).substring(1);
    };
  }

  private static String objectJson(final String value) {
    return "{\"category\":" + new JsonPrimitive(value) + '}';
  }
}
