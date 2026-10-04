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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.api.io.TempDir;

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
import java.util.zip.GZIPInputStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The valid-time posting stream of a one-operation-per-commit SH1 load (t100k, every event its own
 * revision, posting deltas on, folded chunks as referenced side pages) that stopped the load in its
 * second publication with {@code HOT published structural path is malformed}: a branch insert
 * folded the new key's leaf into the subtree subset routing had descended into — a node that was
 * well-formed by itself — while the key sorted above that subtree's right neighbour at the parent
 * (I12). Subset routing does not imply lexicographic position: a zero column of a sparse partial
 * claims nothing, so a sibling's range can lie between the subtree's keys and the new key. The
 * ancestors see a subtree only through its extremes, which is what the branch spine-order guard now
 * proves for every placement before anything is built.
 *
 * <p>
 * The stream is the load's captured valid-time registrations of the contracts resource, reduced by
 * delta debugging to the transactions and operations the failure needs, replayed at index level
 * with the load's own commit cadence — one transaction per event — because the shape needs it: the
 * same operations batched into one or a few hundred commits do not reach it, and neither do
 * prefixes of fewer than ≈63,000 of its one-event base inserts, although those touch another
 * subtree (a leaf reconstructed from sliding-snapshot fragments splits differently from one grown
 * in memory). Replaying tens of thousands of commits takes minutes, so the suite is tagged
 * {@code heavy} like the other soak-style HOT suites and validates the trie's invariants and exact
 * postings at a cadence and after the last commit, not after every one. The reach assertion comes
 * last so that a broken writer is reported as the defect it is.
 */
@Tag("heavy")
@DisplayName("HOT branch spine-order guard (per-operation t100k stream)")
final class HOTBranchSpineOrderTest {

  private static final String RESOURCE = "branch-spine-order";
  private static final int INDEX_NUMBER = 0;
  private static final String STREAM = "/hot/branch-spine-order-t100k.ops.gz";
  /**
   * Full validation (invariants + exact postings) after every this-many commits, and after the last.
   */
  private static final int VALIDATE_EVERY = 10_000;

  @TempDir
  Path temporaryDirectory;

  @AfterEach
  void clearCaches() {
    Databases.clearGlobalCaches();
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  @DisplayName("a branch insert whose key sorts past an ancestor's neighbour is delegated and the stream stays exact")
  void branchWhoseExtremeCrossesAnAncestorsNeighbourIsDelegated(final VersioningType versioningType) {
    final long delegatedBefore = AbstractHOTIndexWriter.BRANCH_SPINE_ORDER_DELEGATED.get();
    final long referencedBefore = AbstractHOTIndexWriter.REFERENCED_CHUNK_WRITES.get();
    final long deltaWritesBefore = HOTIndexWriter.postingDeltaWrites();

    replay(versioningType);

    // Last, so that a broken writer is reported as the defect it is and not as a stream that no longer
    // reaches it.
    assertTrue(HOTIndexWriter.postingDeltaWrites() > deltaWritesBefore,
        "the stream must write posting deltas: it is the delta-on load that failed");
    assertTrue(AbstractHOTIndexWriter.REFERENCED_CHUNK_WRITES.get() > referencedBefore,
        "the stream's folds must store referenced chunks: it is that leaf geometry the load failed under");
    if (versioningType == VersioningType.SLIDING_SNAPSHOT) {
      assertTrue(AbstractHOTIndexWriter.BRANCH_SPINE_ORDER_DELEGATED.get() > delegatedBefore,
          "the captured sliding-snapshot geometry must reach the branch spine-order delegation");
    }
  }

  private void replay(final VersioningType versioningType) {
    final List<List<String>> transactions = transactions();
    final Map<ValidTimeKey, Set<Long>> expected = new HashMap<>();
    final Map<Integer, Map<ValidTimeKey, Set<Long>>> checkpoints = new HashMap<>();

    final Path databasePath = temporaryDirectory.resolve("db");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      assertTrue(database.createResource(
          ResourceConfiguration.newBuilder(RESOURCE).versioningApproach(versioningType).storeDiffs(false).build()));
      try (JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
        int committed = 0;
        for (final List<String> ops : transactions) {
          try (JsonNodeTrx wtx = session.beginNodeTrx()) {
            final HOTIndexWriter<ValidTimeKey> writer = HOTIndexWriter.create(wtx.getStorageEngineWriter(),
                ValidTimeKeySerializer.INSTANCE, IndexType.VALIDTIME, INDEX_NUMBER);
            for (final String op : ops) {
              apply(writer, expected, op);
            }
            wtx.commit();
          }
          committed++;
          if (committed % VALIDATE_EVERY == 0 || committed == transactions.size()) {
            try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
              HOTInvariantValidator.validateIndex(rtx.getStorageEngineReader(), IndexType.VALIDTIME, INDEX_NUMBER)
                                   .assertOk();
              assertExact(rtx, expected, committed);
            }
            final Map<ValidTimeKey, Set<Long>> snapshot = new HashMap<>();
            expected.forEach((key, postings) -> snapshot.put(key, new TreeSet<>(postings)));
            checkpoints.put(committed, snapshot);
          }
        }
      }
    }
    Databases.clearGlobalCaches();
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
      for (final Map.Entry<Integer, Map<ValidTimeKey, Set<Long>>> checkpoint : checkpoints.entrySet()) {
        try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(checkpoint.getKey())) {
          HOTInvariantValidator.validateIndex(rtx.getStorageEngineReader(), IndexType.VALIDTIME, INDEX_NUMBER)
                               .assertOk();
          assertExact(rtx, checkpoint.getValue(), checkpoint.getKey());
        }
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
      final int committed) {
    final HOTIndexReader<ValidTimeKey> reader = HOTIndexReader.create(rtx.getStorageEngineReader(),
        ValidTimeKeySerializer.INSTANCE, IndexType.VALIDTIME, INDEX_NUMBER);
    for (final Map.Entry<ValidTimeKey, Set<Long>> entry : expected.entrySet()) {
      final NodeReferences postings = reader.get(entry.getKey(), SearchMode.EQUAL);
      assertNotNull(postings, "postings of " + entry.getKey() + " after commit " + committed);
      final long[] nodeKeys = entry.getValue().stream().mapToLong(Long::longValue).toArray();
      Arrays.sort(nodeKeys);
      assertArrayEquals(nodeKeys, postings.toSortedArray(),
          "postings of " + entry.getKey() + " after commit " + committed);
    }
  }

  /** The stream, one list of ops per transaction ({@code T} lines open a transaction). */
  private static List<List<String>> transactions() {
    final List<List<String>> transactions = new ArrayList<>();
    try (InputStream in = HOTBranchSpineOrderTest.class.getResourceAsStream(STREAM)) {
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
            assertNotNull(current, "an op before the first transaction marker: " + line);
            current.add(line);
          }
        }
      }
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
    assertFalse(transactions.isEmpty(), "the stream must hold at least one transaction");
    return transactions;
  }
}
