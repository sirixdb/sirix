package io.sirix.budget;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexType;
import io.sirix.index.SearchMode;
import io.sirix.index.hot.HOTBulkIndexLoader;
import io.sirix.index.hot.HOTIndexWriter;
import io.sirix.index.hot.PostingDeltas;
import io.sirix.index.interval.ValidTimeKey;
import io.sirix.index.interval.ValidTimeKeySerializer;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Path;
import java.util.TreeSet;

import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Hot chunks rewrite the base once per bounded delta sequence, and duplicate operations write
 * nothing.
 */
@Isolated
final class PostingDeltaWorkBudgetTest {
  private static final WorkCapture CAPTURE = WorkCapture.of(EngineWorkCounters.HOT_POSTINGS);
  private static final WorkCounter WRITES = EngineWorkCounters.POSTING_DELTA_WRITES;
  private static final WorkCounter FOLDS = EngineWorkCounters.POSTING_DELTA_FOLDS;
  private static final ValidTimeKey KEY = new ValidTimeKey(ValidTimeKey.STORE_LOWER, 1234, 5678);

  @TempDir
  Path directory;

  @AfterEach
  void clearCaches() {
    Databases.clearGlobalCaches();
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void hotChangesUseDeltasAndFoldOnlyAtTheBound(final VersioningType versioningType) throws Exception {
    final Path path = directory.resolve("db");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(path)));
    final TreeSet<Long> expected = new TreeSet<>();
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      database.createResource(ResourceConfiguration.newBuilder("postings")
                                                   .versioningApproach(versioningType)
                                                   .maxNumberOfRevisionsToRestore(3)
                                                   .storeDiffs(false)
                                                   .build());
      try (JsonResourceSession session = database.beginResourceSession("postings")) {
        try (JsonNodeTrx trx = session.beginNodeTrx()) {
          final HOTBulkIndexLoader<ValidTimeKey> loader = writer(trx).createBulkLoader();
          for (int i = 0; i < 400; i++) {
            loader.add(KEY, i * 2L);
            expected.add(i * 2L);
          }
          loader.flush();
          trx.commit();
        }
        for (int round = 0; round < 2; round++) {
          try (JsonNodeTrx trx = session.beginNodeTrx()) {
            final HOTIndexWriter<ValidTimeKey> writer = writer(trx);
            final boolean remove = round == 1;
            final WorkReport changes = CAPTURE.run(() -> {
              for (int i = 0; i < PostingDeltas.FOLD_BOUND; i++) {
                final long nodeKey = 2000L + i;
                if (remove) {
                  assertTrue(writer.remove(KEY, nodeKey));
                  expected.remove(nodeKey);
                } else {
                  writer.indexNodeKey(KEY, nodeKey);
                  expected.add(nodeKey);
                }
              }
            });
            changes.assertExactly(WRITES, PostingDeltas.FOLD_BOUND - 1,
                "hot changes must use delta slots between folds")
                   .assertExactly(FOLDS, 1, "one bounded delta sequence must replace the base once");
            CAPTURE.run(() -> {
              writer.indexNodeKey(KEY, 0);
              assertFalse(writer.remove(KEY, 65000));
            })
                   .assertExactly(WRITES, 0, "duplicates must not write delta slots")
                   .assertExactly(FOLDS, 0, "duplicates must not trigger folds");
            assertArrayEquals(expected.stream().mapToLong(Long::longValue).toArray(),
                requireNonNull(writer.get(KEY, SearchMode.EQUAL)).toSortedArray());
            trx.commit();
          }
        }
      }
    }
  }

  private static HOTIndexWriter<ValidTimeKey> writer(final JsonNodeTrx trx) {
    return HOTIndexWriter.create(trx.getStorageEngineWriter(), ValidTimeKeySerializer.INSTANCE, IndexType.VALIDTIME, 0);
  }
}
