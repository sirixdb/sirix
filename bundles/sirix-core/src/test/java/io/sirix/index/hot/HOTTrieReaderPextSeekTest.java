/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
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
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.page.HOTIndirectPage;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.PageReference;
import io.sirix.page.interfaces.Page;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;

/**
 * Direct wire-level oracles for the PEXT-routed {@link HOTTrieReader} seek primitives.
 *
 * <p>
 * The physical seek cases compare exact {@code logicalKey || chunkIdx_be4} keys with an independent
 * unsigned-lexicographic oracle after a cold database reopen. A dedicated logical-read regression
 * then proves that the generic reader enters that same lower-bound route when chunk zero is absent.
 * Together they catch both a route that is internally self-consistent but lands in the wrong leaf
 * and a logical wrapper that makes an independent positioning decision.
 * </p>
 */
final class HOTTrieReaderPextSeekTest {

  private static final String RESOURCE = "hot-pext-seek";
  private static final int INDEX_NUMBER = 0;
  private static final Comparator<byte[]> UNSIGNED_LEX = Arrays::compareUnsigned;
  private static final HexFormat HEX = HexFormat.of();

  @TempDir
  Path temporaryDirectory;

  @Test
  @Timeout(value = 120, unit = TimeUnit.SECONDS)
  void rootLeafSeeksMatchUnsignedLexOracleForVariableAndCompositeKeys() throws IOException {
    final List<Posting> postings = rootLeafPostings();
    assertAdversarialLogicalKeyCoverage(postings);
    assertVariableAndPrefixLogicalKeyCoverage(postings);

    withColdDurableTree(temporaryDirectory.resolve("root-leaf"), postings, (reader, trie, root, validation) -> {
      assertInstanceOf(HOTLeafPage.class, trie.resolvePage(root),
          "the small corpus must exercise the root-leaf reader path");
      assertEquals(0, validation.observedHeight());

      final List<byte[]> oracle = physicalOracle(postings);
      final List<LeafSnapshot> leaves = snapshotLeaves(trie, root);
      assertEquals(1, leaves.size());
      assertKeysEqual(oracle, flatten(leaves), "durable root-leaf contents");

      final byte[] chunkLogicalKey = compositeLogicalKey();
      final byte[] chunk0 = composite(chunkLogicalKey, 0);
      final byte[] chunk1 = composite(chunkLogicalKey, 1);
      final byte[] chunk2 = composite(chunkLogicalKey, 2);
      final byte[] chunk255 = composite(chunkLogicalKey, 255);
      final int chunk0Index = lowerIndex(oracle, chunk0, false);
      assertArrayEquals(chunk0, oracle.get(chunk0Index));
      assertArrayEquals(chunk1, oracle.get(chunk0Index + 1));
      assertArrayEquals(chunk255, oracle.get(chunk0Index + 2));
      assertFalse(contains(oracle, chunk2), "chunk 2 is the deliberate absent composite probe");

      final List<byte[]> absentProbes = new ArrayList<>();
      absentProbes.add(new byte[0]);
      absentProbes.add(appendZero(oracle.getLast()));
      absentProbes.add(chunk2);
      absentProbes.add(composite(byteTenProbeLogicalKey(), 7));

      assertAllSeeksMatchOracle(trie, root, oracle, absentProbes);
      assertAllRangesMatchOracle(trie, root, oracle, leaves, absentProbes);
    });
  }

  @Test
  @Timeout(value = 180, unit = TimeUnit.SECONDS)
  void indirectTreeSeeksMatchUnsignedLexOracleAtEveryLeafBoundary() throws IOException {
    final List<Posting> postings = indirectTreePostings();
    assertAdversarialLogicalKeyCoverage(postings);

    withColdDurableTree(temporaryDirectory.resolve("indirect"), postings, (reader, trie, root, validation) -> {
      assertInstanceOf(HOTIndirectPage.class, trie.resolvePage(root),
          "more than one leaf worth of keys must exercise indirect PEXT descent");
      assertTrue(validation.observedHeight() >= 1, "the validator must have visited a level below the root");

      final List<byte[]> oracle = physicalOracle(postings);
      final List<LeafSnapshot> leaves = snapshotLeaves(trie, root);
      assertTrue(leaves.size() > 1, "the corpus must cross at least one durable leaf boundary");
      assertKeysEqual(oracle, flatten(leaves), "durable indirect-tree contents");

      final List<byte[]> absentProbes = new ArrayList<>(leaves.size() + 2);
      absentProbes.add(new byte[0]);
      absentProbes.add(appendZero(oracle.getLast()));
      for (int i = 0; i + 1 < leaves.size(); i++) {
        final byte[] leftLast = leaves.get(i).lastKey();
        final byte[] rightFirst = leaves.get(i + 1).firstKey();
        final byte[] gapProbe = appendZero(leftLast);
        assertTrue(UNSIGNED_LEX.compare(leftLast, gapProbe) < 0,
            "boundary probe must follow the left leaf's final key");
        assertTrue(UNSIGNED_LEX.compare(gapProbe, rightFirst) < 0,
            "equal-width physical keys guarantee a strict absent key at every boundary");
        assertFalse(contains(oracle, gapProbe));
        absentProbes.add(gapProbe);
      }

      assertAllSeeksMatchOracle(trie, root, oracle, absentProbes);
      assertAllRangesMatchOracle(trie, root, oracle, leaves, absentProbes);
    });
  }

  @Test
  @Timeout(value = 180, unit = TimeUnit.SECONDS)
  void logicalChunkWalkFindsMissingChunkZeroAtDurableLeafBoundaries() throws IOException {
    final List<Posting> postings = nonZeroChunkPostings();
    assertAdversarialLogicalKeyCoverage(postings);
    assertVariableAndPrefixLogicalKeyCoverage(postings);

    withColdDurableTree(temporaryDirectory.resolve("logical-nonzero-chunks"), postings,
        (reader, trie, root, validation) -> {
          assertInstanceOf(HOTIndirectPage.class, trie.resolvePage(root));
          assertTrue(validation.observedHeight() >= 1);

          final List<byte[]> oracle = physicalOracle(postings);
          final List<LeafSnapshot> leaves = snapshotLeaves(trie, root);
          assertTrue(leaves.size() > 1, "the corpus must create durable leaf boundaries");
          assertKeysEqual(oracle, flatten(leaves), "non-zero-chunk durable contents");

          final HOTIndexReader<ByteKey> logicalReader =
              HOTIndexReader.create(reader, ByteKeySerializer.INSTANCE, IndexType.CAS, INDEX_NUMBER);

          int qualifyingBoundaries = 0;
          byte[] boundaryLogicalKey = null;
          for (int leafIndex = 1; leafIndex < leaves.size(); leafIndex++) {
            final byte[] firstStoredChunk = leaves.get(leafIndex).firstKey();
            final int logicalLength = firstStoredChunk.length - HOTKeySerializer.CHUNK_IDX_BYTES;
            final byte[] logicalKey = Arrays.copyOf(firstStoredChunk, logicalLength);
            final int firstChunkIndex = readIntBE(firstStoredChunk, logicalLength);
            assertTrue(firstChunkIndex > 0, "the corpus deliberately stores no chunk zero");

            final byte[] missingChunkZero = composite(logicalKey, 0);
            assertFalse(contains(oracle, missingChunkZero));
            final int expectedIndex = lowerIndex(oracle, missingChunkZero, false);
            if (expectedIndex >= oracle.size()
                || UNSIGNED_LEX.compare(oracle.get(expectedIndex), firstStoredChunk) != 0) {
              // A strict-prefix logical key can place another composite between prefix||0 and this
              // physical key. Fixed-width corpus keys have no such interleaving, and therefore
              // provide the boundary oracle below.
              continue;
            }

            qualifyingBoundaries++;
            boundaryLogicalKey = logicalKey;
            final byte[] oversizedSeek = Arrays.copyOf(missingChunkZero, missingChunkZero.length + 37);
            Arrays.fill(oversizedSeek, missingChunkZero.length, oversizedSeek.length, (byte) 0xA5);
            assertOptionalKeyEquals(firstStoredChunk,
                seekKey(trie, root, oversizedSeek, missingChunkZero.length, false),
                "valid-length lowerBound at leaf boundary " + leafIndex);
            assertPostings(logicalReader.get(new ByteKey(logicalKey), SearchMode.EQUAL),
                nodeKeysFor(postings, logicalKey));
          }
          assertTrue(qualifyingBoundaries > 0,
              "at least one missing chunk-zero seek must resolve to the next durable leaf's first slot");
          assertNotNull(boundaryLogicalKey);

          // Boundary lookups above ran first, against an empty memoization cache. Sweep the complete
          // corpus afterwards so every non-zero chunk and every serializer-admitted key shape also
          // gets an end-to-end logical-read oracle.
          for (final Posting posting : postings) {
            assertPostings(logicalReader.get(new ByteKey(posting.logicalKey()), SearchMode.EQUAL),
                new long[] {posting.nodeKey()});
          }

          // Explicit insertion-point-zero oracle through an oversized buffer: stale tail bytes must
          // not turn the one-byte key into a longer PEXT input.
          final byte[] beforeFirst = new byte[33];
          Arrays.fill(beforeFirst, (byte) 0xA5);
          beforeFirst[0] = 0;
          final HOTTrieReader.LowerBoundResult first = trie.lowerBound(root, beforeFirst, 1);
          assertNotNull(first.leaf);
          assertEquals(0, first.indexInLeaf);
          assertOptionalKeyEquals(oracle.getFirst(), seekKey(trie, root, beforeFirst, 1, false),
              "insertionPoint == 0 with stale tail");

          // The raw-byte serializer admits strict prefixes. Exact prefix, truncated, and extended
          // probes must remain distinct logical keys even though their physical composites overlap
          // in unsigned-lex order.
          final byte[] truncated = Arrays.copyOf(boundaryLogicalKey, boundaryLogicalKey.length - 1);
          final byte[] extended = Arrays.copyOf(boundaryLogicalKey, boundaryLogicalKey.length + 1);
          extended[extended.length - 1] = 0x10;
          assertNull(logicalReader.get(new ByteKey(truncated), SearchMode.EQUAL));
          assertNull(logicalReader.get(new ByteKey(extended), SearchMode.EQUAL));

          final List<Posting> strictPrefixFamily =
              postings.stream().filter(posting -> posting.logicalKey()[0] == 0x21).toList();
          assertTrue(strictPrefixFamily.size() >= 3);
          for (final Posting posting : strictPrefixFamily) {
            assertPostings(logicalReader.get(new ByteKey(posting.logicalKey()), SearchMode.EQUAL),
                new long[] {posting.nodeKey()});
          }
        });
  }

  @ParameterizedTest(name = "{0}")
  @EnumSource(VersioningType.class)
  @Timeout(value = 300, unit = TimeUnit.SECONDS)
  void incrementalHistoricalSeeksStayOnThePextRouteAcrossVersioningTypes(final VersioningType versioningType)
      throws IOException {
    final Path databasePath = temporaryDirectory.resolve("incremental-" + versioningType.name().toLowerCase());
    final List<Posting> corpus = indirectTreePostings();
    final List<Posting> initial = new ArrayList<>(corpus.subList(0, 560));
    final List<Posting> additions = new ArrayList<>(corpus.subList(560, 608));
    final Posting updated = initial.get(9);
    final Posting deleted = initial.get(19);
    final long addedChunkNodeKey = (3L << 16) | 42L;

    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try {
      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
        database.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                                     .versioningApproach(versioningType)
                                                     .maxNumberOfRevisionsToRestore(4)
                                                     .build());
      }

      final int firstRevision = commitIncremental(databasePath, writer -> {
        for (final Posting posting : initial) {
          writer.indexNodeKey(new ByteKey(posting.logicalKey()), posting.nodeKey());
        }
      });
      final int secondRevision = commitIncremental(databasePath, writer -> {
        for (final Posting posting : additions) {
          writer.indexNodeKey(new ByteKey(posting.logicalKey()), posting.nodeKey());
        }
        writer.indexNodeKey(new ByteKey(updated.logicalKey()), addedChunkNodeKey);
        assertTrue(writer.remove(new ByteKey(deleted.logicalKey()), deleted.nodeKey()));
      });
      final int thirdRevision = commitIncremental(databasePath,
          writer -> assertTrue(writer.remove(new ByteKey(updated.logicalKey()), updated.nodeKey())));

      final List<Posting> secondPhysicalState = new ArrayList<>(initial);
      secondPhysicalState.addAll(additions);
      secondPhysicalState.add(new Posting(updated.logicalKey(), addedChunkNodeKey));

      assertColdIncrementalRevision(databasePath, firstRevision, physicalOracle(initial), updated,
          new long[] {updated.nodeKey()}, deleted, new long[] {deleted.nodeKey()});
      assertColdIncrementalRevision(databasePath, secondRevision, physicalOracle(secondPhysicalState), updated,
          new long[] {updated.nodeKey(), addedChunkNodeKey}, deleted, new long[0]);
      assertColdIncrementalRevision(databasePath, thirdRevision, physicalOracle(secondPhysicalState), updated,
          new long[] {addedChunkNodeKey}, deleted, new long[0]);
    } finally {
      Databases.getGlobalBufferManager().clearAllCaches();
      Databases.removeDatabase(databasePath);
    }
  }

  @Test
  void materializedTrieWithMissingPextChildFailsClosed() {
    final PageReference root = new PageReference();
    root.setPage(HOTIndirectPage.createBiNode(1L, 1, 0, null, new PageReference()));

    try (HOTTrieReader trie = new HOTTrieReader(mock(StorageEngineReader.class))) {
      final IllegalStateException failure =
          assertThrows(IllegalStateException.class, () -> trie.containsKey(root, new byte[] {0x00}));
      assertTrue(failure.getMessage().contains("HOT structural corruption"));
      assertTrue(failure.getMessage().contains("has no reference"));
    }
  }

  /**
   * Builds through the production bulk writer, commits, closes every writer-side object, clears the
   * global page caches, and reopens read-only. Structural invariants are asserted before the first
   * seek so a malformed writer output cannot be mistaken for a reader-only mismatch.
   */
  private static void withColdDurableTree(final Path databasePath, final List<Posting> postings,
      final DurableTreeAssertion assertion) throws IOException {
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try {
      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
        database.createResource(
            ResourceConfiguration.newBuilder(RESOURCE).versioningApproach(VersioningType.FULL).build());
      }

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
          JsonResourceSession session = database.beginResourceSession(RESOURCE);
          JsonNodeTrx wtx = session.beginNodeTrx()) {
        final HOTIndexWriter<ByteKey> writer = HOTIndexWriter.create(wtx.getStorageEngineWriter(),
            ByteKeySerializer.INSTANCE, IndexType.CAS, INDEX_NUMBER);
        final HOTBulkIndexLoader<ByteKey> loader = writer.createBulkLoader();
        for (final Posting posting : postings) {
          loader.add(new ByteKey(posting.logicalKey()), posting.nodeKey());
        }
        loader.flush();
        wtx.commit();
      }

      Databases.getGlobalBufferManager().clearAllCaches();
      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
          JsonResourceSession session = database.beginResourceSession(RESOURCE);
          JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx();
          HOTTrieReader trie = new HOTTrieReader(rtx.getStorageEngineReader())) {
        final StorageEngineReader reader = rtx.getStorageEngineReader();
        final HOTInvariantValidator.Result validation =
            HOTInvariantValidator.validateIndex(reader, IndexType.CAS, INDEX_NUMBER);
        validation.assertOk();
        final PageReference root = HOTInvariantValidator.resolveRootRef(reader, IndexType.CAS, INDEX_NUMBER);
        assertNotNull(root, "the committed CAS HOT root must be addressable after a cold reopen");
        assertEquals(physicalOracle(postings).size(), validation.storedKeyCount());
        assertion.run(reader, trie, root, validation);
      }
    } finally {
      Databases.getGlobalBufferManager().clearAllCaches();
      Databases.removeDatabase(databasePath);
    }
  }

  private static int commitIncremental(final Path databasePath, final IncrementalMutation mutation) throws IOException {
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        JsonResourceSession session = database.beginResourceSession(RESOURCE);
        JsonNodeTrx wtx = session.beginNodeTrx()) {
      final HOTIndexWriter<ByteKey> writer =
          HOTIndexWriter.create(wtx.getStorageEngineWriter(), ByteKeySerializer.INSTANCE, IndexType.CAS, INDEX_NUMBER);
      mutation.apply(writer);
      wtx.commit();
      return session.getMostRecentRevisionNumber();
    }
  }

  private static void assertColdIncrementalRevision(final Path databasePath, final int revision,
      final List<byte[]> oracle, final Posting updated, final long[] expectedUpdatedPostings, final Posting deleted,
      final long[] expectedDeletedPostings) throws IOException {
    Databases.getGlobalBufferManager().clearAllCaches();
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        JsonResourceSession session = database.beginResourceSession(RESOURCE);
        JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision);
        HOTTrieReader trie = new HOTTrieReader(rtx.getStorageEngineReader())) {
      final StorageEngineReader reader = rtx.getStorageEngineReader();
      final HOTInvariantValidator.Result validation =
          HOTInvariantValidator.validateIndex(reader, IndexType.CAS, INDEX_NUMBER);
      validation.assertOk();

      final PageReference root = HOTInvariantValidator.resolveRootRef(reader, IndexType.CAS, INDEX_NUMBER);
      assertNotNull(root);
      assertTrue(validation.observedHeight() >= 1, "incremental corpus must retain an indirect root");
      final List<LeafSnapshot> leaves = snapshotLeaves(trie, root);
      assertKeysEqual(oracle, flatten(leaves), "incremental revision " + revision + " physical keys");

      final List<byte[]> absentProbes = boundaryAbsentProbes(oracle, leaves);
      assertAllSeeksMatchOracle(trie, root, oracle, absentProbes);
      assertAllRangesMatchOracle(trie, root, oracle, leaves, absentProbes);

      final HOTIndexReader<ByteKey> logicalReader =
          HOTIndexReader.create(reader, ByteKeySerializer.INSTANCE, IndexType.CAS, INDEX_NUMBER);
      assertPostings(logicalReader.get(new ByteKey(updated.logicalKey()), SearchMode.EQUAL), expectedUpdatedPostings);
      assertPostings(logicalReader.get(new ByteKey(deleted.logicalKey()), SearchMode.EQUAL), expectedDeletedPostings);
    }
  }

  private static List<byte[]> boundaryAbsentProbes(final List<byte[]> oracle, final List<LeafSnapshot> leaves) {
    final List<byte[]> probes = new ArrayList<>(leaves.size() + 2);
    probes.add(new byte[0]);
    probes.add(appendZero(oracle.getLast()));
    for (int i = 0; i + 1 < leaves.size(); i++) {
      final byte[] gap = appendZero(leaves.get(i).lastKey());
      if (UNSIGNED_LEX.compare(gap, leaves.get(i + 1).firstKey()) < 0) {
        probes.add(gap);
      }
    }
    return probes;
  }

  private static void assertPostings(final NodeReferences actual, final long[] expected) {
    if (expected.length == 0) {
      assertNull(actual);
      return;
    }
    assertNotNull(actual);
    assertArrayEquals(expected, actual.toSortedArray());
  }

  private static void assertAllSeeksMatchOracle(final HOTTrieReader trie, final PageReference root,
      final List<byte[]> oracle, final List<byte[]> absentProbes) {
    for (final byte[] storedKey : oracle) {
      assertSeekMatchesOracle(trie, root, oracle, storedKey, true);
    }

    final Set<byte[]> uniqueAbsent = new TreeSet<>(UNSIGNED_LEX);
    uniqueAbsent.addAll(absentProbes);
    for (final byte[] probe : uniqueAbsent) {
      assertFalse(contains(oracle, probe), "test probe unexpectedly exists: " + hex(probe));
      assertSeekMatchesOracle(trie, root, oracle, probe, false);
    }
  }

  private static void assertSeekMatchesOracle(final HOTTrieReader trie, final PageReference root,
      final List<byte[]> oracle, final byte[] probe, final boolean expectedPresent) {
    final String description = "probe=" + hex(probe);
    assertEquals(expectedPresent, trie.containsKey(root, probe), "containsKey " + description);
    if (expectedPresent) {
      assertNotNull(trie.get(root, probe), "get " + description);
    } else {
      assertNull(trie.get(root, probe), "get " + description);
    }

    final int expectedLowerIndex = lowerIndex(oracle, probe, false);
    final int expectedUpperIndex = lowerIndex(oracle, probe, true);
    assertOptionalKeyEquals(keyAt(oracle, expectedLowerIndex), seekKey(trie, root, probe, false),
        "lowerBound " + description);
    assertOptionalKeyEquals(keyAt(oracle, expectedUpperIndex), seekKey(trie, root, probe, true),
        "upperBound " + description);
  }

  private static byte[] seekKey(final HOTTrieReader trie, final PageReference root, final byte[] probe,
      final boolean upper) {
    return seekKey(trie, root, probe, probe.length, upper);
  }

  private static byte[] seekKey(final HOTTrieReader trie, final PageReference root, final byte[] probe,
      final int probeLength, final boolean upper) {
    for (int attempt = 0; attempt < HOTTrieReader.MAX_STAMP_RETRIES; attempt++) {
      final HOTTrieReader.LowerBoundResult result = upper
          ? trie.upperBound(root, probe, probeLength)
          : trie.lowerBound(root, probe, probeLength);
      if (result.leaf == null) {
        return null;
      }

      final byte[] key;
      try {
        key = result.leaf.getKey(result.indexInLeaf);
      } catch (final RuntimeException exception) {
        if (trie.validateCurrentLeaf()) {
          throw exception;
        }
        continue;
      }
      if (trie.validateCurrentLeaf()) {
        return key;
      }
    }
    throw new AssertionError((upper
        ? "upperBound"
        : "lowerBound") + " could not produce a stamp-stable key for " + hex(Arrays.copyOf(probe, probeLength)));
  }

  private static void assertAllRangesMatchOracle(final HOTTrieReader trie, final PageReference root,
      final List<byte[]> oracle, final List<LeafSnapshot> leaves, final List<byte[]> absentProbes) {
    assertKeysEqual(oracle, collectRange(trie, root, null, null), "unbounded range");

    final byte[] from = oracle.get(oracle.size() / 7);
    final byte[] to = oracle.get(oracle.size() - oracle.size() / 9 - 1);
    assertKeysEqual(expectedRange(oracle, from, to), collectRange(trie, root, from, to), "broad bounded range");

    for (int i = 0; i + 1 < leaves.size(); i++) {
      final byte[] leftLast = leaves.get(i).lastKey();
      final byte[] rightFirst = leaves.get(i + 1).firstKey();
      assertKeysEqual(expectedRange(oracle, leftLast, rightFirst), collectRange(trie, root, leftLast, rightFirst),
          "range across leaf boundary " + i);
    }

    for (final byte[] absent : absentProbes) {
      final int lower = lowerIndex(oracle, absent, false);
      if (lower < oracle.size()) {
        final byte[] next = oracle.get(lower);
        assertKeysEqual(expectedRange(oracle, absent, next), collectRange(trie, root, absent, next),
            "range beginning at absent key " + hex(absent));
      } else {
        assertKeysEqual(List.of(), collectRange(trie, root, absent, null),
            "range beginning after the final key " + hex(absent));
      }
    }
  }

  private static List<byte[]> collectRange(final HOTTrieReader trie, final PageReference root, final byte[] from,
      final byte[] to) {
    final List<byte[]> actual = new ArrayList<>();
    try (HOTRangeCursor cursor = trie.range(root, from, to)) {
      while (cursor.hasNext()) {
        final byte[] key = cursor.next().keyBytes();
        if (!actual.isEmpty()) {
          assertTrue(UNSIGNED_LEX.compare(actual.getLast(), key) < 0,
              "range must be strictly unsigned-lex monotonic: " + hex(actual.getLast()) + " then " + hex(key));
        }
        actual.add(key);
      }
    }
    return actual;
  }

  private static List<byte[]> expectedRange(final List<byte[]> oracle, final byte[] from, final byte[] to) {
    final int first = from == null
        ? 0
        : lowerIndex(oracle, from, false);
    final int afterLast = to == null
        ? oracle.size()
        : lowerIndex(oracle, to, true);
    return new ArrayList<>(oracle.subList(first, Math.max(first, afterLast)));
  }

  private static List<LeafSnapshot> snapshotLeaves(final HOTTrieReader trie, final PageReference root) {
    final List<LeafSnapshot> leaves = new ArrayList<>();
    snapshotLeaves(trie, root, leaves);
    return leaves;
  }

  private static void snapshotLeaves(final HOTTrieReader trie, final PageReference reference,
      final List<LeafSnapshot> leaves) {
    final Page page = trie.resolvePage(reference);
    assertNotNull(page, "every committed HOT child reference must resolve");
    if (page instanceof HOTIndirectPage indirect) {
      for (int i = 0; i < indirect.getNumChildren(); i++) {
        snapshotLeaves(trie, indirect.getChildReference(i), leaves);
      }
      return;
    }
    final HOTLeafPage initialLeaf = assertInstanceOf(HOTLeafPage.class, page);

    for (int attempt = 0; attempt < HOTTrieReader.MAX_STAMP_RETRIES; attempt++) {
      final HOTLeafPage leaf;
      if (attempt == 0) {
        leaf = initialLeaf;
      } else {
        leaf = assertInstanceOf(HOTLeafPage.class, trie.resolvePage(reference));
      }
      final List<byte[]> keys;
      try {
        final int entryCount = leaf.getEntryCount();
        keys = new ArrayList<>(entryCount);
        for (int i = 0; i < entryCount; i++) {
          keys.add(leaf.getKey(i));
        }
      } catch (final RuntimeException exception) {
        if (trie.validateCurrentLeaf()) {
          throw exception;
        }
        continue;
      }
      if (trie.validateCurrentLeaf()) {
        assertFalse(keys.isEmpty(), "the durable tree must not retain empty leaves");
        assertStrictlyAscending(keys, "leaf snapshot");
        leaves.add(new LeafSnapshot(List.copyOf(keys)));
        return;
      }
    }
    fail("leaf could not be snapshotted under a stable allocator stamp");
  }

  private static List<byte[]> flatten(final List<LeafSnapshot> leaves) {
    final List<byte[]> keys = new ArrayList<>();
    for (final LeafSnapshot leaf : leaves) {
      keys.addAll(leaf.keys());
    }
    assertStrictlyAscending(keys, "leaf traversal");
    return keys;
  }

  private static List<byte[]> physicalOracle(final List<Posting> postings) {
    final Set<byte[]> sorted = new TreeSet<>(UNSIGNED_LEX);
    for (final Posting posting : postings) {
      sorted.add(composite(posting.logicalKey(), (int) (posting.nodeKey() >>> 16)));
    }
    return new ArrayList<>(sorted);
  }

  private static byte[] composite(final byte[] logicalKey, final int chunkIndex) {
    final byte[] composite = Arrays.copyOf(logicalKey, logicalKey.length + HOTKeySerializer.CHUNK_IDX_BYTES);
    HOTKeySerializer.writeChunkIdxBE(composite, logicalKey.length, chunkIndex);
    return composite;
  }

  private static List<Posting> rootLeafPostings() {
    final List<Posting> postings = new ArrayList<>();
    long nodeKey = 1;
    postings.add(new Posting(new byte[] {0x00}, nodeKey++));
    postings.add(new Posting(new byte[] {0x00, (byte) 0xFF}, nodeKey++));
    postings.add(new Posting(new byte[] {(byte) 0xFF}, nodeKey++));
    postings.add(new Posting(new byte[] {(byte) 0xFF, (byte) 0xFF}, nodeKey++));

    final byte[] commonPrefix = new byte[9];
    Arrays.fill(commonPrefix, (byte) 0x4B);
    postings.add(new Posting(commonPrefix, nodeKey++));
    postings.add(new Posting(concat(commonPrefix, new byte[] {0x00, (byte) 0xFF}), nodeKey++));
    postings.add(new Posting(concat(commonPrefix, new byte[] {(byte) 0xFF, 0x00}), nodeKey++));
    postings.add(new Posting(concat(commonPrefix, new byte[] {(byte) 0xFF, (byte) 0xFF}), nodeKey++));

    for (int i = 1; i < 64; i++) {
      final byte[] logicalKey = new byte[11 + i % 9];
      Arrays.fill(logicalKey, 0, 9, (byte) 0x4B);
      logicalKey[9] = (byte) (i << 2);
      logicalKey[10] = (byte) i;
      for (int j = 11; j < logicalKey.length - 1; j++) {
        logicalKey[j] = (byte) (i * 31 + j);
      }
      logicalKey[logicalKey.length - 1] = (byte) ((i & 1) == 0
          ? 0x00
          : 0xFF);
      postings.add(new Posting(logicalKey, nodeKey++));
    }

    final byte[] chunkLogicalKey = compositeLogicalKey();
    postings.add(new Posting(chunkLogicalKey, 7));
    postings.add(new Posting(chunkLogicalKey, (1L << 16) | 8));
    postings.add(new Posting(chunkLogicalKey, (255L << 16) | 9));
    return postings;
  }

  private static List<Posting> indirectTreePostings() {
    final List<Posting> postings = new ArrayList<>(1_024);
    for (int i = 0; i < 1_024; i++) {
      final byte[] logicalKey = new byte[20];
      Arrays.fill(logicalKey, 0, 9, (byte) 0x5A);
      logicalKey[9] = (byte) i;
      logicalKey[10] = (byte) (i >>> 8);
      logicalKey[11] = (byte) (i >>> 4);
      logicalKey[12] = (byte) (i * 37);
      logicalKey[13] = (byte) (i * 73);
      logicalKey[14] = (byte) (i >>> 2);
      logicalKey[15] = (byte) (i * 11);
      logicalKey[16] = (byte) (i >>> 1);
      logicalKey[17] = (byte) (i * 19);
      logicalKey[18] = (byte) (i >>> 3);
      logicalKey[19] = (byte) ((i & 1) == 0
          ? 0x00
          : 0xFF);
      postings.add(new Posting(logicalKey, i + 1L));
    }
    return postings;
  }

  private static List<Posting> nonZeroChunkPostings() {
    final List<Posting> postings = new ArrayList<>(1_028);
    for (int i = 0; i < 1_024; i++) {
      final byte[] logicalKey = new byte[20];
      Arrays.fill(logicalKey, 0, 9, (byte) 0x6C);
      logicalKey[9] = (byte) i;
      logicalKey[10] = (byte) (i >>> 8);
      logicalKey[11] = (byte) (i >>> 4);
      logicalKey[12] = (byte) (i * 37);
      logicalKey[13] = (byte) (i * 73);
      logicalKey[14] = (byte) (i >>> 2);
      logicalKey[15] = (byte) (i * 11);
      logicalKey[16] = (byte) (i >>> 1);
      logicalKey[17] = (byte) (i * 19);
      logicalKey[18] = (byte) (i >>> 3);
      logicalKey[19] = (byte) ((i & 1) == 0
          ? 0x00
          : 0xFF);
      final long nodeKey = ((long) (1 + i % 251) << 16) | (i + 1L);
      postings.add(new Posting(logicalKey, nodeKey));
    }

    final byte[] strictPrefix = new byte[] {0x21, 0x32, 0x43, 0x54, 0x65, 0x76, 0x07, 0x18, 0x29};
    postings.add(new Posting(strictPrefix, (300L << 16) | 1));
    postings.add(new Posting(concat(strictPrefix, new byte[] {0x00}), (301L << 16) | 2));
    postings.add(new Posting(concat(strictPrefix, new byte[] {0x00, (byte) 0xFF}), (302L << 16) | 3));
    postings.add(new Posting(concat(strictPrefix, new byte[] {(byte) 0xFF, 0x00, 0x00}), (303L << 16) | 4));
    return postings;
  }

  private static void assertAdversarialLogicalKeyCoverage(final List<Posting> postings) {
    final List<byte[]> logicalKeys = postings.stream().map(Posting::logicalKey).toList();
    assertTrue(logicalKeys.stream().anyMatch(key -> key.length > 9 && key[9] == 0x00),
        "the corpus must branch on a zero at byte 10");
    assertTrue(logicalKeys.stream().anyMatch(key -> key.length > 9 && key[9] == (byte) 0xFF),
        "the corpus must branch on 0xFF at byte 10");
    assertTrue(logicalKeys.stream().anyMatch(key -> key[key.length - 1] == 0x00), "the corpus must contain zero tails");
    assertTrue(logicalKeys.stream().anyMatch(key -> key[key.length - 1] == (byte) 0xFF),
        "the corpus must contain 0xFF tails");
  }

  private static void assertVariableAndPrefixLogicalKeyCoverage(final List<Posting> postings) {
    final Set<Integer> lengths = new TreeSet<>();
    final Set<byte[]> logicalKeys = new TreeSet<>(UNSIGNED_LEX);
    for (final Posting posting : postings) {
      lengths.add(posting.logicalKey().length);
      logicalKeys.add(posting.logicalKey());
    }
    assertTrue(lengths.size() > 4, "the root-leaf corpus must contain several logical key lengths");

    byte[] previous = null;
    boolean foundStrictPrefix = false;
    for (final byte[] key : logicalKeys) {
      if (previous != null && key.length > previous.length
          && Arrays.equals(previous, 0, previous.length, key, 0, previous.length)) {
        foundStrictPrefix = true;
        break;
      }
      previous = key;
    }
    assertTrue(foundStrictPrefix, "the root-leaf corpus must contain a strict logical-key prefix pair");
  }

  private static byte[] compositeLogicalKey() {
    return new byte[] {(byte) 0x91, 0x23, 0x45, 0x67, (byte) 0x89, (byte) 0xAB, (byte) 0xCD, (byte) 0xEF, 0x12, 0x34,
        0x00, (byte) 0xFF};
  }

  private static byte[] byteTenProbeLogicalKey() {
    return new byte[] {0x4B, 0x4B, 0x4B, 0x4B, 0x4B, 0x4B, 0x4B, 0x4B, 0x4B, 0x7E, 0x55, (byte) 0xFF};
  }

  private static byte[] appendZero(final byte[] bytes) {
    return Arrays.copyOf(bytes, bytes.length + 1);
  }

  private static byte[] concat(final byte[] left, final byte[] right) {
    final byte[] result = Arrays.copyOf(left, left.length + right.length);
    System.arraycopy(right, 0, result, left.length, right.length);
    return result;
  }

  private static int readIntBE(final byte[] bytes, final int offset) {
    Objects.checkFromIndexSize(offset, Integer.BYTES, bytes.length);
    return (bytes[offset] & 0xFF) << 24 | (bytes[offset + 1] & 0xFF) << 16 | (bytes[offset + 2] & 0xFF) << 8
        | bytes[offset + 3] & 0xFF;
  }

  private static long[] nodeKeysFor(final List<Posting> postings, final byte[] logicalKey) {
    return postings.stream()
                   .filter(posting -> Arrays.equals(posting.logicalKey(), logicalKey))
                   .mapToLong(Posting::nodeKey)
                   .sorted()
                   .toArray();
  }

  private static int lowerIndex(final List<byte[]> sorted, final byte[] probe, final boolean strict) {
    int low = 0;
    int high = sorted.size();
    while (low < high) {
      final int middle = (low + high) >>> 1;
      final int comparison = UNSIGNED_LEX.compare(sorted.get(middle), probe);
      if (comparison < 0 || strict && comparison == 0) {
        low = middle + 1;
      } else {
        high = middle;
      }
    }
    return low;
  }

  private static boolean contains(final List<byte[]> sorted, final byte[] probe) {
    final int index = lowerIndex(sorted, probe, false);
    return index < sorted.size() && UNSIGNED_LEX.compare(sorted.get(index), probe) == 0;
  }

  private static byte[] keyAt(final List<byte[]> keys, final int index) {
    return index == keys.size()
        ? null
        : keys.get(index);
  }

  private static void assertOptionalKeyEquals(final byte[] expected, final byte[] actual, final String description) {
    if (expected == null) {
      assertNull(actual, description + " expected exhaustion but returned " + hex(actual));
    } else {
      assertNotNull(actual, description + " unexpectedly exhausted");
      assertArrayEquals(expected, actual, description);
    }
  }

  private static void assertKeysEqual(final List<byte[]> expected, final List<byte[]> actual,
      final String description) {
    assertEquals(expected.size(), actual.size(), description + " key count");
    for (int i = 0; i < expected.size(); i++) {
      assertArrayEquals(expected.get(i), actual.get(i), description + " key " + i);
    }
  }

  private static void assertStrictlyAscending(final List<byte[]> keys, final String description) {
    for (int i = 1; i < keys.size(); i++) {
      assertTrue(UNSIGNED_LEX.compare(keys.get(i - 1), keys.get(i)) < 0, description + " is not strictly ordered at "
          + (i - 1) + ": " + hex(keys.get(i - 1)) + " then " + hex(keys.get(i)));
    }
  }

  private static String hex(final byte[] bytes) {
    return bytes == null
        ? "<exhausted>"
        : HEX.formatHex(bytes);
  }

  private record Posting(byte[] logicalKey, long nodeKey) {
    private Posting {
      logicalKey = Objects.requireNonNull(logicalKey, "logicalKey").clone();
      if (logicalKey.length == 0) {
        throw new IllegalArgumentException("logicalKey must not be empty");
      }
    }
  }

  private record LeafSnapshot(List<byte[]> keys) {
    private byte[] firstKey() {
      return keys.getFirst();
    }

    private byte[] lastKey() {
      return keys.getLast();
    }
  }

  private static final class ByteKey implements Comparable<ByteKey> {
    private final byte[] bytes;

    private ByteKey(final byte[] bytes) {
      this.bytes = Objects.requireNonNull(bytes, "bytes").clone();
      if (bytes.length == 0) {
        throw new IllegalArgumentException("bytes must not be empty");
      }
    }

    @Override
    public int compareTo(final ByteKey other) {
      return UNSIGNED_LEX.compare(bytes, other.bytes);
    }
  }

  private enum ByteKeySerializer implements HOTKeySerializer<ByteKey> {
    INSTANCE;

    @Override
    public int serialize(final ByteKey key, final byte[] destination, final int offset) {
      Objects.requireNonNull(key, "key");
      Objects.checkFromIndexSize(offset, key.bytes.length, destination.length);
      System.arraycopy(key.bytes, 0, destination, offset, key.bytes.length);
      return key.bytes.length;
    }

    @Override
    public int maxSerializedLength(final ByteKey key) {
      return Objects.requireNonNull(key, "key").bytes.length;
    }

    @Override
    public ByteKey deserialize(final byte[] bytes, final int offset, final int length) {
      Objects.checkFromIndexSize(offset, length, bytes.length);
      return new ByteKey(Arrays.copyOfRange(bytes, offset, offset + length));
    }
  }

  @FunctionalInterface
  private interface IncrementalMutation {
    void apply(HOTIndexWriter<ByteKey> writer);
  }

  @FunctionalInterface
  private interface DurableTreeAssertion {
    void run(StorageEngineReader reader, HOTTrieReader trie, PageReference root,
        HOTInvariantValidator.Result validation) throws IOException;
  }
}
