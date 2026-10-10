package io.sirix.query.function.jn.temporal;

import io.brackit.query.Query;
import io.brackit.query.jdm.Sequence;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.ValidTimeConfig;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.io.StorageType;
import io.sirix.index.interval.PostingMask;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.function.jn.temporal.ValidTimeIntervalIndex.ChunkCandidates;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBItem;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/** Differential boundary and historical-revision gates for the compressed single-stab path. */
final class ValidTimeRunStabTest {
  @TempDir
  Path directory;

  @AfterEach
  void clearCaches() {
    Databases.clearGlobalCaches();
  }

  @Test
  void chunkRunsMergeDuplicatesAndRemoveOnlyExactStarts() {
    final PostingMask membership = PostingMask.from(NodeReferences.ofSortedArray(new long[] {1, 63, 64, 1000, 65535}));
    final PostingMask verification = PostingMask.from(NodeReferences.ofSortedArray(new long[] {64, 1000}));
    for (final boolean bitmap : new boolean[] {false, true}) {
      final var candidates = new ChunkCandidates(base -> membership, base -> verification);
      final NodeReferences first = NodeReferences.ofSortedArray(new long[] {1, 64, 65535});
      final NodeReferences second = NodeReferences.ofSortedArray(new long[] {1, 63, 64, 1000});
      if (bitmap) {
        first.getNodeKeys();
        second.getNodeKeys();
      }
      candidates.accept(65536, first, false);
      candidates.accept(0, second, false);
      candidates.accept(65536, second, true);
      candidates.accept(0, first, false);
      candidates.removeExactStarts(65536, first, false);
      candidates.removeExactStarts(0, first, false);
      final var evidence = candidates.evidence();
      assertArrayEquals(new long[] {63, 64, 1000, 65536 + 64, 65536 + 1000}, evidence.keys());
      assertArrayEquals(new long[] {64, 1000, 65536 + 64, 65536 + 1000}, evidence.unverified().toArray());
    }
  }

  @Test
  void manyInterleavedRunsMergeAcrossChunksWithStrictStartTies() {
    final long[] memberKeys = new long[53];
    for (int key = 0; key <= 50; key++) {
      memberKeys[key] = key;
    }
    memberKeys[51] = 63;
    memberKeys[52] = 65535;
    final PostingMask membership = PostingMask.from(NodeReferences.ofSortedArray(memberKeys));
    final PostingMask verification = PostingMask.from(NodeReferences.ofSortedArray(new long[] {17, 63}));
    for (final boolean bitmap : new boolean[] {false, true}) {
      final var candidates = new ChunkCandidates(base -> membership, base -> verification);
      for (int chunk = 2; chunk >= 0; chunk--) {
        final long base = (long) chunk << 16;
        for (int run = 16; run >= 0; run--) {
          final NodeReferences refs =
              NodeReferences.ofSortedArray(new long[] {run, 17 + run, 34 + run, 63, 65535});
          if (bitmap) {
            refs.getNodeKeys();
          }
          candidates.accept(base, refs, false);
        }
        candidates.removeExactStarts(base, NodeReferences.ofSortedArray(new long[] {0, 17, 63, 65535}), false);
      }
      final long[] expected = new long[153];
      final long[] unverified = new long[6];
      for (int chunk = 0; chunk < 3; chunk++) {
        final long base = (long) chunk << 16;
        for (int key = 1; key <= 50; key++) {
          expected[chunk * 51 + key - 1] = base | key;
        }
        expected[chunk * 51 + 50] = base | 63;
        unverified[chunk * 2] = base | 17;
        unverified[chunk * 2 + 1] = base | 63;
      }
      final var evidence = candidates.evidence();
      assertArrayEquals(expected, evidence.keys());
      assertArrayEquals(unverified, evidence.unverified().toArray());
    }
  }

  @Test
  void adaptiveChunkPromotionPreservesTiesAndVerification() {
    final long[] members = new long[8192];
    for (int key = 0; key < members.length; key++) {
      members[key] = key;
    }
    final PostingMask membership = PostingMask.from(NodeReferences.ofSortedArray(members));
    final PostingMask verification = PostingMask.from(NodeReferences.ofSortedArray(new long[] {0, 4096, 8191}));
    for (final boolean bitmap : new boolean[] {false, true}) {
      final var candidates = new ChunkCandidates(base -> membership, base -> verification);
      for (int offset = 0; offset < members.length; offset += 512) {
        final NodeReferences references =
            NodeReferences.ofSortedArray(Arrays.copyOfRange(members, offset, offset + 512));
        if (bitmap) {
          references.getNodeKeys();
        }
        candidates.accept(65536, references, false);
      }
      candidates.accept(0, NodeReferences.ofSortedArray(new long[] {0, 1, 4096, 8191}), true);
      candidates.accept(65536, NodeReferences.ofSortedArray(members), false);
      candidates.removeExactStarts(65536, NodeReferences.ofSortedArray(new long[] {0, 1, 4096, 8191}), false);
      candidates.removeExactStarts(0, NodeReferences.ofSortedArray(new long[] {0, 4096, 8191}), false);
      final long[] expected = new long[8194];
      expected[0] = 0;
      expected[1] = 4096;
      expected[2] = 8191;
      int position = 3;
      for (int key = 0; key < members.length; key++) {
        if (key != 1) {
          expected[position++] = 65536L | key;
        }
      }
      final var evidence = candidates.evidence();
      assertArrayEquals(expected, evidence.keys());
      assertArrayEquals(new long[] {0, 4096, 8191, 65536, 69632, 73727}, evidence.unverified().toArray());
    }
  }

  @Test
  void fullyRemovedAndRejectedRunsEmitNoKeys() {
    final NodeReferences references = NodeReferences.ofSortedArray(new long[] {0, 65535});
    final PostingMask membership = PostingMask.from(references);
    final PostingMask empty = PostingMask.from(null);
    final var removed = new ChunkCandidates(base -> membership, base -> empty);
    removed.accept(0, references, false);
    removed.accept(0, references, false);
    removed.removeExactStarts(0, references, false);
    assertArrayEquals(new long[0], removed.evidence().keys());
    final var rejected = new ChunkCandidates(base -> membership, base -> empty);
    rejected.accept(0, references, true);
    rejected.removeExactStarts(0, references, false);
    assertArrayEquals(new long[0], rejected.evidence().keys());
    final var nonMember = new ChunkCandidates(base -> empty, base -> empty);
    nonMember.accept(0, references, false);
    assertArrayEquals(new long[0], nonMember.evidence().keys());
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void everyBoundaryAndInexactPointMatchesTheScanInOldAndNewRevisions(final VersioningType versioning) {
    final String json = """
        [{"vf":"2024-01-01T00:00:00Z","vt":"2024-01-02T00:00:00Z"},
         {"vf":"2024-01-01T00:00:00Z","vt":"2024-01-02T00:00:00.000500Z"},
         {"vf":"2024-01-01T00:00:00.000500Z","vt":"2024-01-02T00:00:00Z"},
         {"vf":"2024-01-02T00:00:00Z","vt":"2024-01-02T00:00:00Z"},
         {"vf":"1969-12-31T23:59:59Z","vt":"1970-01-01T00:00:00.000500Z"},
         {"vf":"2024-01-01T00:00:00Z"},
         {"vt":"2024-01-02T00:00:00.000500Z"},
         {"vf":"2024-01-01T00:00:00Z","vt":"2024-01-02T00:00:00Z",
          "nested":[{"vf":"2020-01-01T00:00:00Z","vt":"2030-01-01T00:00:00Z"}]}]
        """;
    final Path databasePath = directory.resolve("runs");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (var database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder("rows")
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .versioningApproach(versioning)
                                                   .validTimePaths("vf", "vt")
                                                   .build());
      try (var session = database.beginResourceSession("rows"); var writer = session.beginNodeTrx()) {
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json), JsonNodeTrx.Commit.NO);
        writer.commit();
      }
    }
    try (var store = BasicJsonDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain,
          "let $d := jn:doc('runs','rows') let $i := jn:create-valid-time-index($d) return sdb:commit($d)").evaluate(
              context);
      final var collection = store.lookup("runs");
      final JsonDBItem original = collection.getDocument("rows");
      final var session = original.getResourceSession();
      final int before = original.getTrx().getRevisionNumber();
      final var writer = session.getNodeTrx().orElseGet(session::beginNodeTrx);
      writer.moveTo(original.getNodeKey());
      writer.moveToFirstChild();
      writer.moveToLastChild();
      writer.setStringValue("2024-01-01T00:00:00Z");
      writer.commit();
      final int after = session.getMostRecentRevisionNumber();
      for (final int revision : new int[] {before, after, before}) {
        final JsonDBItem document = collection.getDocument("rows", revision);
        final ValidTimeConfig config = Objects.requireNonNull(session.getResourceConfig().getValidTimeConfig());
        for (final String timestamp : new String[] {"1969-12-31T23:59:59Z", "1970-01-01T00:00:00Z",
            "1970-01-01T00:00:00.000250Z", "2024-01-01T00:00:00Z", "2024-01-01T00:00:00.000250Z",
            "2024-01-01T00:00:00.000500Z", "2024-01-02T00:00:00Z", "2024-01-02T00:00:00.000250Z",
            "2024-01-02T00:00:00.000500Z", "2024-01-02T00:00:00.001Z", "+11000-01-01T00:00:00Z"}) {
          final Instant point = Instant.parse(timestamp);
          for (final boolean strictStart : new boolean[] {false, true}) {
            for (final boolean strictEnd : new boolean[] {false, true}) {
              final Sequence indexed = ValidTimeIntervalIndex.sequence(document, point, config, strictStart, strictEnd);
              assertNotNull(indexed);
              final Sequence scan = ValidTimeFilter.linearScanSequence(document, point, config, strictStart, strictEnd);
              assertArrayEquals(keys(scan), keys(indexed), revision + " " + timestamp + " " + strictStart + strictEnd);
            }
          }
        }
      }
    }
  }

  private static long[] keys(final Sequence sequence) {
    final LongArrayList keys = new LongArrayList();
    try (var iterator = sequence.iterate()) {
      for (var item = iterator.next(); item != null; item = iterator.next()) {
        keys.add(((JsonDBItem) item).getNodeKey());
      }
    }
    final long[] sorted = keys.toLongArray();
    Arrays.sort(sorted);
    return sorted;
  }
}
