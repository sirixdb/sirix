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
import io.sirix.page.HOTIndirectPage;
import io.sirix.page.HOTLeafPage;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class PostingDeltaViewReuseTest {
  @TempDir
  Path directory;

  @AfterEach
  void clearCaches() {
    Databases.clearGlobalCaches();
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void reusedMembershipSurvivesFoldSplitAndWriterSwitchInOneCommit(final VersioningType versioning) {
    exercise(versioning, IndexType.CAS, CASKeySerializer.INSTANCE, new CASValue(new Str("hot-a"), Type.STR, 1),
        new CASValue(new Str("hot-b"), Type.STR, 1));
    exercise(versioning, IndexType.VALIDTIME, ValidTimeKeySerializer.INSTANCE, new ValidTimeKey((byte) 0, 1, 2),
        new ValidTimeKey((byte) 1, 1, 2));
  }

  private <K extends Comparable<? super K>> void exercise(final VersioningType versioning, final IndexType type,
      final HOTKeySerializer<K> serializer, final K firstKey, final K secondKey) {
    final Path path = directory.resolve(type.name());
    final TreeSet<Long> first = new TreeSet<>();
    final TreeSet<Long> second = new TreeSet<>();
    final List<long[][]> revisions = new ArrayList<>();
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(path)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      database.createResource(ResourceConfiguration.newBuilder("postings")
                                                   .versioningApproach(versioning)
                                                   .maxNumberOfRevisionsToRestore(3)
                                                   .storeDiffs(false)
                                                   .verifyChecksumsOnRead(true)
                                                   .build());
      try (JsonResourceSession session = database.beginResourceSession("postings")) {
        try (JsonNodeTrx trx = session.beginNodeTrx()) {
          final HOTIndexWriter<K> writer = HOTIndexWriter.create(trx.getStorageEngineWriter(), serializer, type, 0);
          final HOTBulkIndexLoader<K> loader = writer.createBulkLoader();
          for (int i = 0; i < 400; i++) {
            loader.add(firstKey, 2L * i);
            loader.add(secondKey, 2L * i);
            first.add(2L * i);
            second.add(2L * i);
          }
          loader.flush();
          assertInstanceOf(HOTLeafPage.class, trx.getStorageEngineWriter().loadHOTPage(writer.rootReference));
          trx.commit();
          revisions.add(snapshot(first, second));
        }
        assertRevisions(session, serializer, type, firstKey, secondKey, revisions);
        try (JsonNodeTrx trx = session.beginNodeTrx()) {
          final HOTIndexWriter<K> writer =
              HOTIndexWriter.create(trx.getStorageEngineWriter(), serializer, type, 0, 256, 4);
          writer.indexNodeKey(firstKey, 1000);
          first.add(1000L);
          writer.indexNodeKey(secondKey, 2000);
          second.add(2000L);
          writer.indexNodeKey(firstKey, 1000); // duplicate in a cached pending delta
          writer.indexNodeKey(firstKey, 2); // duplicate in the cached base
          assertFalse(writer.remove(firstKey, 65000));
          writer.indexNodeKey(firstKey, 1001);
          first.add(1001L);
          assertTrue(writer.remove(firstKey, 0));
          first.remove(0L);
          writer.indexNodeKey(firstKey, 1002); // fourth change folds this cached view
          first.add(1002L);
          assertTrue(writer.remove(firstKey, 1000));
          first.remove(1000L);
          assertCurrent(writer, firstKey, secondKey, first, second);

          // New chunk keys force the original single leaf to split while chunk zero has live deltas.
          for (int i = 1; i <= 2 * HOTLeafPage.MAX_ENTRIES; i++) {
            final long nodeKey = ((long) i << 16) | 3L;
            writer.indexNodeKey(firstKey, nodeKey);
            first.add(nodeKey);
          }
          assertInstanceOf(HOTIndirectPage.class, trx.getStorageEngineWriter().loadHOTPage(writer.rootReference));
          assertTrue(writer.remove(firstKey, 1002));
          first.remove(1002L);
          writer.indexNodeKey(firstKey, 1002); // below the high-water mark: membership must be checked
          first.add(1002L);
          writer.indexNodeKey(firstKey, 1002);
          writer.indexNodeKey(firstKey, 1003); // folds after the split
          first.add(1003L);
          assertCurrent(writer, firstKey, secondKey, first, second);

          final HOTIndexWriter<K> alias =
              HOTIndexWriter.create(trx.getStorageEngineWriter(), serializer, type, 0, 256, 4);
          alias.indexNodeKey(firstKey, 3000);
          assertTrue(writer.remove(firstKey, 3000), "writer switch must invalidate the older membership");
          assertFalse(alias.remove(firstKey, 3000));
          assertTrue(writer.remove(secondKey, 2000));
          second.remove(2000L);
          assertCurrent(writer, firstKey, secondKey, first, second);
          trx.commit();
          revisions.add(snapshot(first, second));
        }
        assertRevisions(session, serializer, type, firstKey, secondKey, revisions);
      }
    }
    Databases.clearGlobalCaches();
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path);
        JsonResourceSession session = database.beginResourceSession("postings")) {
      assertRevisions(session, serializer, type, firstKey, secondKey, revisions);
    }
  }

  private static long[][] snapshot(final TreeSet<Long> first, final TreeSet<Long> second) {
    return new long[][] {first.stream().mapToLong(Long::longValue).toArray(),
        second.stream().mapToLong(Long::longValue).toArray()};
  }

  private static <K extends Comparable<? super K>> void assertCurrent(final HOTIndexWriter<K> writer, final K firstKey,
      final K secondKey, final TreeSet<Long> first, final TreeSet<Long> second) {
    final long[][] expected = snapshot(first, second);
    assertArrayEquals(expected[0], requireNonNull(writer.get(firstKey, SearchMode.EQUAL)).toSortedArray());
    assertArrayEquals(expected[1], requireNonNull(writer.get(secondKey, SearchMode.EQUAL)).toSortedArray());
  }

  private static <K extends Comparable<? super K>> void assertRevisions(final JsonResourceSession session,
      final HOTKeySerializer<K> serializer, final IndexType type, final K firstKey, final K secondKey,
      final List<long[][]> revisions) {
    for (int i = 0; i < revisions.size(); i++) {
      try (JsonNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx(i + 1)) {
        final HOTIndexReader<K> reader = HOTIndexReader.create(trx.getStorageEngineReader(), serializer, type, 0);
        assertArrayEquals(revisions.get(i)[0], requireNonNull(reader.get(firstKey, SearchMode.EQUAL)).toSortedArray());
        assertArrayEquals(revisions.get(i)[1], requireNonNull(reader.get(secondKey, SearchMode.EQUAL)).toSortedArray());
        HOTInvariantValidator.validateIndex(trx.getStorageEngineReader(), type, 0).assertOk();
      }
    }
  }
}
