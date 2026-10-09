package io.sirix.index.hot;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.page.HOTRangeCursor;
import io.sirix.access.trx.page.HOTTrieReader;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexType;
import io.sirix.index.SearchMode;
import io.sirix.index.interval.ValidTimeKey;
import io.sirix.index.interval.ValidTimeKeySerializer;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Path;

import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Guards the physical posting slots a range scan must visit for a fixed hot-key workload. */
@Isolated
final class PostingDeltaReadFootprintTest {
  private static final int KEYS = 16;
  private static final int BASE_POSTINGS = 400;
  private static final int ADDED_POSTINGS = 63;
  private static final String RESOURCE = "posting-read-footprint";

  @TempDir
  Path directory;

  @AfterEach
  void clearCaches() {
    Databases.clearGlobalCaches();
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void residualDeltasKeepTheRangeReadFootprintBounded(final VersioningType versioning) {
    final Path path = directory.resolve("db");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(path)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                                              .versioningApproach(versioning)
                                                              .maxNumberOfRevisionsToRestore(3)
                                                              .storeDiffs(false)
                                                              .build()));
      try (JsonResourceSession session = database.beginResourceSession(RESOURCE);
          JsonNodeTrx trx = session.beginNodeTrx()) {
        final HOTIndexWriter<ValidTimeKey> writer = HOTIndexWriter.create(trx.getStorageEngineWriter(),
            ValidTimeKeySerializer.INSTANCE, IndexType.VALIDTIME, 0);
        final HOTBulkIndexLoader<ValidTimeKey> loader = writer.createBulkLoader();
        for (int key = 0; key < KEYS; key++) {
          for (int posting = 0; posting < BASE_POSTINGS; posting++) {
            loader.add(key(key), posting * 2L);
          }
        }
        loader.flush();
        assertPostings(trx.getStorageEngineWriter(), 0);
        trx.commit();
        assertRevision(session, 1, 0);
        final HOTIndexWriter<ValidTimeKey> updates = HOTIndexWriter.create(trx.getStorageEngineWriter(),
            ValidTimeKeySerializer.INSTANCE, IndexType.VALIDTIME, 0);
        for (int posting = 0; posting < ADDED_POSTINGS; posting++) {
          for (int key = 0; key < KEYS; key++) {
            updates.indexNodeKey(key(key), 2000L + posting);
          }
        }
        assertPostings(trx.getStorageEngineWriter(), ADDED_POSTINGS);
        trx.commit();
        assertRevision(session, 1, 0);
        assertRevision(session, 2, ADDED_POSTINGS);
      }
    }
    Databases.clearGlobalCaches();
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path);
        JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
      assertRevision(session, 1, 0);
      assertRevision(session, 2, ADDED_POSTINGS);
    }
  }

  private static ValidTimeKey key(final int key) {
    return new ValidTimeKey(ValidTimeKey.STORE_LOWER, 1234, 5678 + key);
  }

  private static void assertRevision(final JsonResourceSession session, final int revision, final int added) {
    try (JsonNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx(revision)) {
      assertPostings(trx.getStorageEngineReader(), added);
      final HOTIndexReader<ValidTimeKey> reader =
          HOTIndexReader.create(trx.getStorageEngineReader(), ValidTimeKeySerializer.INSTANCE, IndexType.VALIDTIME, 0);
      int slots = 0;
      try (HOTTrieReader trie = new HOTTrieReader(trx.getStorageEngineReader());
          HOTRangeCursor cursor = trie.range(requireNonNull(reader.getRootReference()), null, null)) {
        while (cursor.hasNext()) {
          slots++;
          cursor.advance();
        }
      }
      // This fixed workload needs at most one base plus fifteen residual changes per hot key.
      // Counting persisted slots measures work a range walk must do, independent of caching and
      // page-versioning geometry. Do not derive this budget from the configured fold bound.
      assertTrue(slots <= 256, "range scan visits " + slots + " posting slots, exceeding the 256-slot read budget");
    }
  }

  private static void assertPostings(final StorageEngineReader storage, final int added) {
    final long[] expected = new long[BASE_POSTINGS + added];
    for (int posting = 0; posting < BASE_POSTINGS; posting++) {
      expected[posting] = posting * 2L;
    }
    for (int posting = 0; posting < added; posting++) {
      expected[BASE_POSTINGS + posting] = 2000L + posting;
    }
    final HOTIndexReader<ValidTimeKey> reader =
        HOTIndexReader.create(storage, ValidTimeKeySerializer.INSTANCE, IndexType.VALIDTIME, 0);
    for (int key = 0; key < KEYS; key++) {
      final NodeReferences references = requireNonNull(reader.get(key(key), SearchMode.EQUAL));
      assertArrayEquals(expected, references.toSortedArray(), "postings of hot key " + key);
    }
  }
}
