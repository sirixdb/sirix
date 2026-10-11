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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The valid-time posting stream of a batched SH1 load (t50k, 2,500 edits per publication, posting
 * deltas on) that stopped the load with
 * {@code HOT could not construct an invariant-clean incremental frontier}: the complete-frontier
 * splice persistently split a subtree before the new key, and the plain compression of the right
 * half dropped every column above a child which still discriminated on one of them (a Direction-1
 * child legitimately straddles a column its parent uses only for other branches). The half then
 * carried a less significant root bit than its child, broke the trie condition (I11), and every
 * candidate at every level of the spine was rightly declined — there was nowhere left to go.
 *
 * <p>
 * The stream is the load's captured valid-time registrations, reduced by delta debugging to the
 * transactions and operations the failure needs (46,395 of 236,235; a removal whose registration
 * the reduction dropped is replayed as the removal of an absent posting), replayed at index level
 * with one transaction per publication, so the consolidation cadence and the delta slots come out
 * as they did in the load. After every commit the trie must satisfy every invariant and hold
 * exactly the stream's postings, under each versioning type; the reach assertion comes last so that
 * a broken writer is reported as the defect it is.
 */
final class HOTFrontierSplitHalfCanonicalTest {

  private static final String RESOURCE = "frontier-split-half";
  private static final int INDEX_NUMBER = 0;
  private static final String STREAM = "/hot/frontier-split-half-t50k.ops.gz";

  @TempDir
  Path temporaryDirectory;

  @AfterEach
  void clearCaches() {
    Databases.clearGlobalCaches();
  }

  @ParameterizedTest(name = "{0}: the split half is built canonically and the stream stays exact")
  @EnumSource(VersioningType.class)
  void splitHalfWhoseChildStraddlesADroppedColumnIsBuiltCanonically(final VersioningType versioningType) {
    final long recanonicalizedBefore = AbstractHOTIndexWriter.FRONTIER_SPLIT_HALF_RECANONICALIZED.get();
    final long deltaWritesBefore = HOTIndexWriter.postingDeltaWrites();
    // Preserve the captured split geometry independently of the production fold policy.
    replay(versioningType, 256, 64);

    // Last, so that a broken writer is reported as the defect it is and not as a stream that no longer
    // reaches it.
    assertTrue(HOTIndexWriter.postingDeltaWrites() > deltaWritesBefore,
        "the stream must write posting deltas: it is the batched, delta-on load that failed");
    assertTrue(AbstractHOTIndexWriter.FRONTIER_SPLIT_HALF_RECANONICALIZED.get() > recanonicalizedBefore,
        "the stream must reach a persistent split whose half has to be built canonically; without one it no longer"
            + " covers the frontier failure");
  }

  @ParameterizedTest(name = "{0}: the production fold policy preserves the complete frontier stream")
  @EnumSource(VersioningType.class)
  void frontierStreamWithProductionGeometry(final VersioningType versioningType) {
    replay(versioningType, PostingDeltas.HOT_CHUNK_BYTES, PostingDeltas.FOLD_BOUND);
  }

  @Tag("heavy")
  @ParameterizedTest(name = "{0}: hot bytes {1}, fold bound {2}")
  @MethodSource("deltaGeometries")
  void frontierStreamAcrossDeltaGeometries(final VersioningType versioningType, final int hotChunkBytes,
      final int foldBound) {
    final long foldsBefore = HOTIndexWriter.postingDeltaFolds();
    replay(versioningType, hotChunkBytes, foldBound);
    assertTrue(HOTIndexWriter.postingDeltaFolds() > foldsBefore, "the stream must exercise folds at this geometry");
  }

  static Stream<Arguments> deltaGeometries() {
    return HOTValidTimeCorrectionStreamTest.deltaGeometries();
  }

  private void replay(final VersioningType versioningType, final int hotChunkBytes, final int foldBound) {
    final List<List<String>> transactions = transactions();
    final Map<ValidTimeKey, Set<Long>> expected = new HashMap<>();
    final List<Map<ValidTimeKey, Set<Long>>> snapshots = new ArrayList<>();

    final Path databasePath = temporaryDirectory.resolve("db");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      assertTrue(database.createResource(
          ResourceConfiguration.newBuilder(RESOURCE).versioningApproach(versioningType).storeDiffs(false).build()));
      try (JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
        int publication = 0;
        for (final List<String> ops : transactions) {
          try (JsonNodeTrx wtx = session.beginNodeTrx()) {
            final HOTIndexWriter<ValidTimeKey> writer = HOTIndexWriter.create(wtx.getStorageEngineWriter(),
                ValidTimeKeySerializer.INSTANCE, IndexType.VALIDTIME, INDEX_NUMBER, hotChunkBytes, foldBound);
            for (final String op : ops) {
              apply(writer, expected, op);
            }
            wtx.commit();
          }
          publication++;
          final Map<ValidTimeKey, Set<Long>> snapshot = new HashMap<>();
          expected.forEach((key, keys) -> snapshot.put(key, new TreeSet<>(keys)));
          snapshots.add(snapshot);
          try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
            HOTInvariantValidator.validateIndex(rtx.getStorageEngineReader(), IndexType.VALIDTIME, INDEX_NUMBER)
                                 .assertOk();
            assertExact(rtx, expected, publication);
          }
        }
        assertRevisions(session, snapshots);
      }
    }
    Databases.clearGlobalCaches();
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
      assertRevisions(session, snapshots);
    }
  }

  private static void assertRevisions(final JsonResourceSession session,
      final List<Map<ValidTimeKey, Set<Long>>> snapshots) {
    for (int i = 0; i < snapshots.size(); i++) {
      try (JsonNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx(i + 1)) {
        assertExact(trx, snapshots.get(i), i + 1);
      }
    }
  }

  private static void apply(final HOTIndexWriter<ValidTimeKey> writer, final Map<ValidTimeKey, Set<Long>> expected,
      final String op) {
    final String[] fields = op.split(" ", 0);
    assertEquals(5, fields.length, "op " + op);
    final ValidTimeKey key =
        new ValidTimeKey(Byte.parseByte(fields[1]), Long.parseLong(fields[2]), Long.parseLong(fields[3]));
    final long nodeKey = Long.parseLong(fields[4]);
    switch (fields[0]) {
      case "i" -> {
        writer.indexNodeKey(key, nodeKey);
        expected.computeIfAbsent(key, k -> new TreeSet<>()).add(nodeKey);
      }
      case "r" -> {
        final Set<Long> postings = expected.get(key);
        if (postings == null || !postings.remove(nodeKey)) {
          // The reduction dropped this posting's registration; the writer must see it as absent too.
          assertFalse(writer.remove(key, nodeKey), "removal of an unregistered posting must report absence: " + op);
          return;
        }
        if (postings.isEmpty()) {
          expected.remove(key);
        }
        assertTrue(writer.remove(key, nodeKey), "the posting to remove must be registered: " + op);
      }
      default -> throw new IllegalArgumentException("unknown op: " + op);
    }
  }

  private static void assertExact(final JsonNodeReadOnlyTrx rtx, final Map<ValidTimeKey, Set<Long>> expected,
      final int publication) {
    final HOTIndexReader<ValidTimeKey> reader = HOTIndexReader.create(rtx.getStorageEngineReader(),
        ValidTimeKeySerializer.INSTANCE, IndexType.VALIDTIME, INDEX_NUMBER);
    for (final Map.Entry<ValidTimeKey, Set<Long>> entry : expected.entrySet()) {
      final NodeReferences postings = reader.get(entry.getKey(), SearchMode.EQUAL);
      assertNotNull(postings, "postings of " + entry.getKey() + " after publication " + publication);
      final long[] nodeKeys = entry.getValue().stream().mapToLong(Long::longValue).toArray();
      Arrays.sort(nodeKeys);
      assertArrayEquals(nodeKeys, postings.toSortedArray(),
          "postings of " + entry.getKey() + " after publication " + publication);
    }
  }

  /** The stream, one list of ops per transaction ({@code T} lines open a transaction). */
  private static List<List<String>> transactions() {
    final List<List<String>> transactions = new ArrayList<>();
    try (InputStream in = HOTFrontierSplitHalfCanonicalTest.class.getResourceAsStream(STREAM)) {
      assertNotNull(in, "missing test resource " + STREAM);
      try (BufferedReader reader =
          new BufferedReader(new InputStreamReader(new GZIPInputStream(in), StandardCharsets.US_ASCII))) {
        List<String> current = null;
        String line;
        while ((line = reader.readLine()) != null) {
          if (line.isEmpty() || line.charAt(0) == '#') {
            continue;
          }
          if (line.charAt(0) == 'T') {
            current = new ArrayList<>();
            transactions.add(current);
          } else {
            assertNotNull(current, "op before the first transaction: " + line);
            current.add(line);
          }
        }
      }
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
    assertTrue(transactions.size() >= 2, "the stream has an initial load and at least one publication");
    return transactions;
  }
}
