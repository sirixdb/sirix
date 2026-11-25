/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.budget;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexType;
import io.sirix.index.SearchMode;
import io.sirix.index.hot.HOTIndexReader;
import io.sirix.index.hot.HOTIndexWriter;
import io.sirix.index.hot.HOTKeySerializer;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A point lookup pays one routing step per indirect level on its path, so its counted steps measure
 * path depth — the figure a lost outer spine inflates (FMT-306, issue
 * <a href="https://github.com/sirixdb/sirix/issues/1457">1457</a>).
 *
 * <p>
 * Fixture: 4,096 sixteen-byte keys inserted into one CAS index, committed, then read after a cold
 * reopen. Ascending is the control for descending: both orders pack the identical eight full
 * leaves, so a route that costs more for one order than the other is this defect and not the data.
 * Measured: one routing step per lookup under FULL and DIFFERENTIAL alike.
 *
 * <p>
 * Mutation evidence: publishing the dirty spine unfolded, by returning early from
 * {@code HOTCommitNormalizer.normalize}, leaves the descending index at three levels. Both arms
 * then read 3 routing steps against this bound of 1, while the ascending control still reads 1 —
 * which is also the asymmetry the defect produced before the repair.
 *
 * <p>
 * Leaf resolutions are deliberately not bounded here: a point lookup in a single-revision resource
 * reconstructs no fragment, so {@code hot.leafLoads} stays at zero in both arms, and a bound on an
 * instrument that reads zero either way would guard nothing. These are counted routing steps, not
 * elapsed time or physical reads.
 */
@Isolated
final class HOTLookupShapeWorkBudgetTest {
  private static final HOTKeySerializer<Long> SERIALIZER = new HOTKeySerializer<>() {
    @Override
    public int serialize(final Long value, final byte[] destination, final int offset) {
      for (int i = 0; i < 8; i++) {
        destination[offset + i] = 0;
        destination[offset + 8 + i] = (byte) (value >>> (56 - i * 8));
      }
      return 16;
    }

    @Override
    public int maxSerializedLength(final Long value) {
      return 16;
    }

    @Override
    public Long deserialize(final byte[] bytes, final int offset, final int length) {
      long value = 0;
      for (int i = 8; i < 16; i++) {
        value = (value << 8) | (bytes[offset + i] & 0xFFL);
      }
      return value;
    }
  };

  private static final int KEYS = 4096;
  private static final long NODE_KEY = 7L;
  private static final WorkCounter ROUTING_STEPS = EngineWorkCounters.HOT_INDIRECT_ROUTING_STEPS;
  private static final WorkCapture CAPTURE = WorkCapture.of(ROUTING_STEPS);

  @TempDir
  Path directory;

  @AfterEach
  void clearCaches() {
    Databases.clearGlobalCaches();
  }

  @ParameterizedTest
  @EnumSource(value = VersioningType.class, names = {"FULL", "DIFFERENTIAL"})
  void aPointLookupRoutesThroughOneLevelWhateverTheInsertionOrder(final VersioningType versioning) throws Exception {
    assertTrue(VersioningType.hotMergeDiagEnabled(),
        "the HOT work counters must be collecting, or every bound below reads zero");
    for (final boolean descending : new boolean[] {false, true}) {
      final String order = descending
          ? "descending"
          : "ascending";
      final Path path = directory.resolve(versioning.name() + "-" + order);
      assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(path)));
      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
        assertTrue(database.createResource(
            ResourceConfiguration.newBuilder("index").versioningApproach(versioning).storeDiffs(false).build()));
        try (JsonResourceSession session = database.beginResourceSession("index");
            JsonNodeTrx transaction = session.beginNodeTrx()) {
          final HOTIndexWriter<Long> writer =
              HOTIndexWriter.create(transaction.getStorageEngineWriter(), SERIALIZER, IndexType.CAS, 0);
          for (final long key : keys(descending)) {
            writer.indexNodeKey(key, NODE_KEY);
          }
          transaction.commit();
        }
      }
      Databases.clearGlobalCaches();
      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path);
          JsonResourceSession session = database.beginResourceSession("index");
          JsonNodeReadOnlyTrx transaction = session.beginNodeReadOnlyTrx()) {
        final StorageEngineReader reader = transaction.getStorageEngineReader();
        final HOTIndexReader<Long> index = HOTIndexReader.create(reader, SERIALIZER, IndexType.CAS, 0);
        // One warm lookup first: the capture must price a route, not the index root's first resolution
        // or the reader's first-key memoization.
        assertNotNull(index.get(0L, SearchMode.EQUAL), order + ": the index must answer its first key");
        for (final long key : new long[] {1L, KEYS / 2L, KEYS - 1L}) {
          final WorkCapture.Captured<NodeReferences> lookup = CAPTURE.call(() -> index.get(key, SearchMode.EQUAL));
          assertNotNull(lookup.result(), order + ": key " + key + " must be found");
          assertArrayEquals(new long[] {NODE_KEY}, lookup.result().toSortedArray(),
              order + ": key " + key + " must answer its own posting");
          lookup.work()
                .assertExactly(ROUTING_STEPS, 1,
                    order + ": a committed trie over eight full leaves routes a key through exactly one indirect "
                        + "level; a lost outer spine or an unnormalized commit adds one step per excess level");
        }
      }
      Databases.clearGlobalCaches();
    }
  }

  private static List<Long> keys(final boolean descending) {
    final List<Long> keys = new ArrayList<>(KEYS);
    for (long key = 0; key < KEYS; key++) {
      keys.add(key);
    }
    if (descending) {
      Collections.reverse(keys);
    }
    return keys;
  }
}
