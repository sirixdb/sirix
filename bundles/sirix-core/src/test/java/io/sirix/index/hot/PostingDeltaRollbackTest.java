package io.sirix.index.hot;

import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Type;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexType;
import io.sirix.index.SearchMode;
import io.sirix.index.interval.ValidTimeKey;
import io.sirix.index.interval.ValidTimeKeySerializer;
import io.sirix.index.redblacktree.keyvalue.CASValue;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.settings.VersioningType;
import io.sirix.page.HOTLeafPage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class PostingDeltaRollbackTest {
  @TempDir
  Path directory;

  @AfterEach
  void clearCaches() {
    Databases.clearGlobalCaches();
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void rollbackRestoresTheBaseAndLiveDeltasAcrossAFold(final VersioningType versioningType) {
    exercise(versioningType, IndexType.CAS, CASKeySerializer.INSTANCE, new CASValue(new Str("hot\0"), Type.STR, 1));
    exercise(versioningType, IndexType.VALIDTIME, ValidTimeKeySerializer.INSTANCE, new ValidTimeKey((byte) 0, 1, 2));
  }

  private <K extends Comparable<? super K>> void exercise(final VersioningType versioningType, final IndexType type,
      final HOTKeySerializer<K> serializer, final K key) {
    final Path path = directory.resolve(type.name());
    final TreeSet<Long> expected = new TreeSet<>();
    final List<long[]> snapshots = new ArrayList<>();
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(path)));
    final long foldsBefore = HOTIndexWriter.postingDeltaFolds();
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      database.createResource(ResourceConfiguration.newBuilder("resource")
                                                   .versioningApproach(versioningType)
                                                   .maxNumberOfRevisionsToRestore(3)
                                                   .storeDiffs(false)
                                                   .build());
      try (JsonResourceSession session = database.beginResourceSession("resource")) {
        try (JsonNodeTrx trx = session.beginNodeTrx()) {
          final HOTBulkIndexLoader<K> loader =
              HOTIndexWriter.create(trx.getStorageEngineWriter(), serializer, type, 0).createBulkLoader();
          for (int i = 0; i < 400; i++) {
            loader.add(key, i * 2L);
            expected.add(i * 2L);
          }
          loader.flush();
          trx.commit();
          snapshots.add(keys(expected));
        }
        try (JsonNodeTrx trx = session.beginNodeTrx()) {
          final HOTIndexWriter<K> writer = HOTIndexWriter.create(trx.getStorageEngineWriter(), serializer, type, 0);
          for (int i = 0; i < PostingDeltas.FOLD_BOUND - 1; i++) {
            writer.indexNodeKey(key, 1000L + i);
            expected.add(1000L + i);
          }
          trx.commit();
          snapshots.add(keys(expected));
        }
        try (JsonNodeTrx trx = session.beginNodeTrx()) {
          final HOTIndexWriter<K> writer = HOTIndexWriter.create(trx.getStorageEngineWriter(), serializer, type, 0);
          writer.indexNodeKey(key, 2000); // folds the committed live deltas
          assertTrue(writer.remove(key, 0));
          final TreeSet<Long> uncommitted = new TreeSet<>(expected);
          uncommitted.add(2000L);
          uncommitted.remove(0L);
          assertArrayEquals(keys(uncommitted), writer.get(key, SearchMode.EQUAL).toSortedArray());
          trx.rollback();
        }
        assertRevisions(session, serializer, type, key, snapshots);
        try (JsonNodeTrx trx = session.beginNodeTrx()) {
          final HOTIndexWriter<K> writer = HOTIndexWriter.create(trx.getStorageEngineWriter(), serializer, type, 0);
          assertArrayEquals(keys(expected), writer.get(key, SearchMode.EQUAL).toSortedArray());
          writer.indexNodeKey(key, 2001); // folds the restored live deltas again
          expected.add(2001L);
          trx.commit();
          snapshots.add(keys(expected));
        }
        assertRevisions(session, serializer, type, key, snapshots);
        // Shrink through the hot threshold, empty the chunk, then reuse its tombstoned slots.
        try (JsonNodeTrx trx = session.beginNodeTrx()) {
          final HOTIndexWriter<K> writer = HOTIndexWriter.create(trx.getStorageEngineWriter(), serializer, type, 0);
          for (final long nodeKey : keys(expected)) {
            assertTrue(writer.remove(key, nodeKey));
          }
          expected.clear();
          trx.commit();
          snapshots.add(keys(expected));
        }
        assertRevisions(session, serializer, type, key, snapshots);
        try (JsonNodeTrx trx = session.beginNodeTrx()) {
          final HOTIndexWriter<K> writer = HOTIndexWriter.create(trx.getStorageEngineWriter(), serializer, type, 0);
          for (int i = 0; i < 450; i++) {
            writer.indexNodeKey(key, 2L * i);
            expected.add(2L * i);
          }
          trx.commit();
          snapshots.add(keys(expected));
        }
        assertRevisions(session, serializer, type, key, snapshots);
      }
    }
    assertTrue(HOTIndexWriter.postingDeltaFolds() - foldsBefore >= 2, "both transactions must cross the fold");
    Databases.clearGlobalCaches();
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path);
        JsonResourceSession session = database.beginResourceSession("resource")) {
      assertRevisions(session, serializer, type, key, snapshots);
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void anotherWriterCannotLeaveTheDecodedBaseStale(final VersioningType versioningType) {
    final ValidTimeKey key = new ValidTimeKey((byte) 0, 1, 2);
    final ValidTimeKeySerializer serializer = ValidTimeKeySerializer.INSTANCE;
    final Path path = directory.resolve("aliases");
    final TreeSet<Long> expected = new TreeSet<>();
    final List<long[]> snapshots = new ArrayList<>();
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(path)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      database.createResource(ResourceConfiguration.newBuilder("resource")
                                                   .versioningApproach(versioningType)
                                                   .maxNumberOfRevisionsToRestore(3)
                                                   .storeDiffs(false)
                                                   .build());
      try (JsonResourceSession session = database.beginResourceSession("resource")) {
        try (JsonNodeTrx trx = session.beginNodeTrx()) {
          final HOTBulkIndexLoader<ValidTimeKey> loader =
              HOTIndexWriter.create(trx.getStorageEngineWriter(), serializer, IndexType.VALIDTIME, 0)
                            .createBulkLoader();
          for (int i = 0; i < 400; i++) {
            loader.add(key, i * 2L);
            expected.add(i * 2L);
          }
          loader.flush();
          trx.commit();
          snapshots.add(keys(expected));
        }
        try (JsonNodeTrx trx = session.beginNodeTrx()) {
          final HOTIndexWriter<ValidTimeKey> first =
              HOTIndexWriter.create(trx.getStorageEngineWriter(), serializer, IndexType.VALIDTIME, 0);
          final HOTIndexWriter<ValidTimeKey> second =
              HOTIndexWriter.create(trx.getStorageEngineWriter(), serializer, IndexType.VALIDTIME, 0);
          first.indexNodeKey(key, 1000); // decode the original base
          expected.add(1000L);
          second.indexNodeKey(key, 1001);
          expected.add(1001L);
          for (int i = 0; i < PostingDeltas.FOLD_BOUND - 2; i++) {
            second.indexNodeKey(key, 2000L + i); // replace the base through the other writer
            expected.add(2000L + i);
          }
          assertTrue(first.remove(key, 2000), "the first writer must observe the newly folded base");
          assertTrue(first.remove(key, 1001));
          expected.remove(2000L);
          expected.remove(1001L);
          final byte[] beforeMarker = baseMarker(first, key);
          // The next fold keeps both cardinality and serialized size unchanged. Its marker has the
          // same hash/length, but the immutable side payload differs from first's cached bitmap.
          for (int i = 0; i < PostingDeltas.FOLD_BOUND / 2 - 2; i++) {
            assertTrue(second.remove(key, 2L * i));
            expected.remove(2L * i);
          }
          for (int i = 0; i < PostingDeltas.FOLD_BOUND / 2; i++) {
            second.indexNodeKey(key, 3000L + i);
            expected.add(3000L + i);
          }
          assertArrayEquals(beforeMarker, baseMarker(second, key), "same marker, different side-page contents");
          assertTrue(first.remove(key, 3000), "decoded-base reuse must compare the resolved payload");
          expected.remove(3000L);
          assertArrayEquals(keys(expected), first.get(key, SearchMode.EQUAL).toSortedArray());
          assertArrayEquals(keys(expected), second.get(key, SearchMode.EQUAL).toSortedArray());
          trx.commit();
          snapshots.add(keys(expected));
        }
        assertRevisions(session, serializer, IndexType.VALIDTIME, key, snapshots);
      }
    }
    Databases.clearGlobalCaches();
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path);
        JsonResourceSession session = database.beginResourceSession("resource")) {
      assertRevisions(session, serializer, IndexType.VALIDTIME, key, snapshots);
    }
  }

  private static byte[] baseMarker(final HOTIndexWriter<ValidTimeKey> writer, final ValidTimeKey key) {
    final byte[] composite = new byte[ValidTimeKeySerializer.KEY_BYTES + Integer.BYTES];
    ValidTimeKeySerializer.INSTANCE.serialize(key, composite, 0);
    final HOTLeafPage leaf = writer.acquireLeafForRead(composite, composite.length);
    try {
      final byte[] marker = leaf.copyStoredValue(leaf.findEntry(composite));
      assertTrue(NodeReferencesSerializer.isReferenced(marker, 0, marker.length));
      return marker;
    } finally {
      writer.releaseLeafReadGuard(leaf, null);
    }
  }

  private static long[] keys(final TreeSet<Long> keys) {
    return keys.stream().mapToLong(Long::longValue).toArray();
  }

  private static <K extends Comparable<? super K>> void assertRevisions(final JsonResourceSession session,
      final HOTKeySerializer<K> serializer, final IndexType type, final K key, final List<long[]> snapshots) {
    for (int i = 0; i < snapshots.size(); i++) {
      try (JsonNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx(i + 1)) {
        final HOTIndexReader<K> reader = HOTIndexReader.create(trx.getStorageEngineReader(), serializer, type, 0);
        final NodeReferences references = reader.get(key, SearchMode.EQUAL);
        assertArrayEquals(snapshots.get(i), references == null
            ? new long[0]
            : references.toSortedArray());
      }
    }
  }
}
