package io.sirix.index.hot;

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
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Path;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A posting chunk keeps its hot/cold state when its leaf is stored packed (FMT-214). The default
 * writer stores a committed posting as a frame-of-reference far shorter than its ordinary form; the
 * posting-delta rules must still treat the chunk as hot, so postings added as deltas before the
 * commit stay visible to later adds and removes.
 */
final class HOTPackedPostingDeltaTest {

  private static final String RESOURCE = "packed-posting-delta";
  private static final int INDEX_NUMBER = 0;
  /** An ordinary chunk of 2 + 8n bytes is hot from 32 postings; its frame needs only 5 + (n - 1). */
  private static final int HOT_CHUNK_BYTES = 256;
  private static final int FOLD_BOUND = 64;
  private static final int POSTINGS = 40;
  private static final ValidTimeKey KEY = new ValidTimeKey((byte) 0, 1_704_067_200_001L, 1_709_396_983_808L);

  @TempDir
  Path temporaryDirectory;

  @AfterEach
  void clearCaches() {
    Databases.clearGlobalCaches();
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void postingsAddedAsDeltasStayRemovableAfterAPackedCommit(final VersioningType versioningType) {
    final Path databasePath = temporaryDirectory.resolve("db");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      assertTrue(database.createResource(
          ResourceConfiguration.newBuilder(RESOURCE).versioningApproach(versioningType).storeDiffs(false).build()));
      try (JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
        try (JsonNodeTrx wtx = session.beginNodeTrx()) {
          final HOTIndexWriter<ValidTimeKey> writer = writer(wtx);
          for (long nodeKey = 1; nodeKey <= POSTINGS; nodeKey++) {
            writer.indexNodeKey(KEY, nodeKey);
          }
          wtx.commit();
        }
        try (JsonNodeTrx wtx = session.beginNodeTrx()) {
          assertTrue(writer(wtx).remove(KEY, POSTINGS), "the last posting, added after the chunk turned hot");
          wtx.commit();
        }
        try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
          final NodeReferences postings = HOTIndexReader.create(rtx.getStorageEngineReader(),
              ValidTimeKeySerializer.INSTANCE, IndexType.VALIDTIME, INDEX_NUMBER).get(KEY, SearchMode.EQUAL);
          assertNotNull(postings);
          assertArrayEquals(LongStream.rangeClosed(1, POSTINGS - 1).toArray(), postings.toSortedArray());
        }
      }
    }
  }

  private static HOTIndexWriter<ValidTimeKey> writer(final JsonNodeTrx wtx) {
    return HOTIndexWriter.create(wtx.getStorageEngineWriter(), ValidTimeKeySerializer.INSTANCE, IndexType.VALIDTIME,
        INDEX_NUMBER, HOT_CHUNK_BYTES, FOLD_BOUND);
  }
}
