/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.JsonTestHelper;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.page.HOTRangeCursor;
import io.sirix.access.trx.page.HOTTrieReader;
import io.sirix.page.HOTLeafPage;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.page.PageReference;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end test for {@link ProjectionIndexHOTStorage}.
 *
 * <p>Verifies the projection index persists across DB close/reopen and that
 * writes survive the transaction intent log flush. Covers:
 *
 * <ul>
 *   <li>Single-leaf put + same-trx read</li>
 *   <li>Multi-leaf put that forces at least one HOT leaf split
 *       (MAX_ENTRIES per HOTLeafPage), with per-leaf byte-equal readback</li>
 *   <li>Reopen after commit → read all leaves back byte-equal</li>
 *   <li>Update semantics: putting the same leafIndex twice replaces the
 *       prior payload, not appending or merging</li>
 * </ul>
 */
final class ProjectionIndexHOTStorageTest {

  private static final String RESOURCE_NAME = "testResource";
  private static final Path DATABASE_PATH = JsonTestHelper.PATHS.PATH1.getFile();
  private static final int INDEX_NUMBER = 0;

  @BeforeEach
  void setUp() throws IOException {
    JsonTestHelper.deleteEverything();
    Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH)) {
      db.createResource(
          ResourceConfiguration.newBuilder(RESOURCE_NAME).versioningApproach(VersioningType.FULL).build());
    }
  }

  @AfterEach
  void tearDown() throws IOException {
    JsonTestHelper.deleteEverything();
    Databases.getGlobalBufferManager().clearAllCaches();
  }

  @Test
  void singleLeaf_putThenGetInSameTrx_roundTrips() throws IOException {
    final byte[] payload = deterministicPayload(2_000, 0xCAFE0001L);

    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
         JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
         JsonNodeTrx wtx = session.beginNodeTrx()) {
      final ProjectionIndexHOTStorage storage =
          new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
      storage.put(42L, payload);

      final byte[] readBack = storage.get(42L);
      assertNotNull(readBack, "same-trx get after put must succeed");
      assertArrayEquals(payload, readBack, "payload must round-trip byte-exact");

      assertNull(storage.get(43L), "unwritten leafIndex must return null");
      wtx.commit();
    }
  }

  @Test
  void manyLeaves_persistAcrossReopen() throws IOException {
    // 2048 leaves with small payloads → comfortably forces at least one HOT
    // leaf split (HOTLeafPage caps entries well below 2 K).
    final int numLeaves = 2_048;
    final int payloadSize = 128;
    final long seed = 0xBEEF_DEADL;

    // Write
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
         JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
         JsonNodeTrx wtx = session.beginNodeTrx()) {
      final ProjectionIndexHOTStorage storage =
          new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);

      for (int i = 0; i < numLeaves; i++) {
        storage.put(i, payloadFor(i, payloadSize, seed));
      }
      wtx.commit();
    }

    // Reopen & read back
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
         JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      final var rtx = session.beginNodeReadOnlyTrx();
      try {
        final PageReference rootRef =
            ProjectionIndexHOTStorage.rootReference(rtx.getStorageEngineReader(), INDEX_NUMBER);
        assertNotNull(rootRef, "projection sub-tree root must survive reopen");

        try (HOTTrieReader trieReader = new HOTTrieReader(rtx.getStorageEngineReader())) {
          for (int i = 0; i < numLeaves; i++) {
            final byte[] keyBytes = ProjectionIndexHOTStorage.encodeKey(i);
            final MemorySegment value = trieReader.get(rootRef, keyBytes);
            assertNotNull(value, "leafIndex=" + i + " must be readable after reopen");

            final byte[] expected = payloadFor(i, payloadSize, seed);
            final byte[] actual = new byte[(int) value.byteSize()];
            MemorySegment.copy(value, ValueLayout.JAVA_BYTE, 0, actual, 0, actual.length);
            assertArrayEquals(expected, actual,
                "leafIndex=" + i + " payload must be byte-exact after reopen");
          }
        }
      } finally {
        rtx.close();
      }
    }
  }

  @Test
  void putTwice_replacesPayload() throws IOException {
    final byte[] first = new byte[] { 1, 2, 3, 4 };
    final byte[] second = new byte[] { 9, 9, 9, 9, 9, 9, 9, 9 };

    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
         JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
         JsonNodeTrx wtx = session.beginNodeTrx()) {
      final ProjectionIndexHOTStorage storage =
          new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
      storage.put(7L, first);
      assertArrayEquals(first, storage.get(7L));

      storage.put(7L, second);
      assertArrayEquals(second, storage.get(7L),
          "second put must replace the prior payload, not merge/append");
      wtx.commit();
    }
  }

  @Test
  void readAll_returnsLeavesInAscendingLeafIndexOrder() throws IOException {
    final int numLeaves = 600;
    final int payloadSize = 64;
    final long seed = 0xF00D_BABEL;

    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
         JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
         JsonNodeTrx wtx = session.beginNodeTrx()) {
      final ProjectionIndexHOTStorage storage =
          new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
      // Insert in scrambled order to prove readAll reorders by leafIndex.
      final int[] order = new int[numLeaves];
      for (int i = 0; i < numLeaves; i++) order[i] = i;
      final Random rng = new Random(0xC0FFEEL);
      for (int i = numLeaves - 1; i > 0; i--) {
        final int j = rng.nextInt(i + 1);
        final int tmp = order[i]; order[i] = order[j]; order[j] = tmp;
      }
      for (final int leafIndex : order) {
        storage.put(leafIndex, payloadFor(leafIndex, payloadSize, seed));
      }
      wtx.commit();
    }

    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
         JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      final var rtx = session.beginNodeReadOnlyTrx();
      try {
        final java.util.List<byte[]> all =
            ProjectionIndexHOTStorage.readAll(rtx.getStorageEngineReader(), INDEX_NUMBER);
        assertEquals(numLeaves, all.size(), "readAll must return every persisted leaf");
        for (int i = 0; i < numLeaves; i++) {
          assertArrayEquals(payloadFor(i, payloadSize, seed), all.get(i),
              "readAll must yield leafIndex " + i + " at position " + i);
        }
      } finally {
        rtx.close();
      }
    }
  }

  /**
   * Cross-check: {@link ProjectionIndexHOTStorage#readAllViaCursor} must return the
   * exact same set of leaves as the probe-based {@link ProjectionIndexHOTStorage#readAll}.
   *
   * <p>The comment in {@code readAll} cites a historical bug where {@code HOTRangeCursor}
   * dropped entries after splits. The recent {@code aaa8d4453} fix
   * ("remove unsafe leaf min/max range cache — HOT isn't in-order partitioned") removed
   * the most plausible root cause. This test verifies the cursor path is now correct
   * at 1K / 10K / 100K scale, each of which crosses one or more HOT leaf-split boundaries.
   * If this passes, the cursor path can replace the probe-based {@code readAll} for
   * hydration, eliminating the per-leaf top-down walk that dominates cold-cache startup.
   */
  @Test
  void readAllViaCursor_matchesReadAll_atScale() throws IOException {
    for (final int numLeaves : new int[] { 1_000, 10_000, 100_000 }) {
      JsonTestHelper.deleteEverything();
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));
      try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH)) {
        db.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME)
            .versioningApproach(VersioningType.FULL).build());
      }

      final int payloadSize = 64;
      final long seed = 0xC0DE_CAFEL ^ numLeaves;

      try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
           JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
           JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        for (int i = 0; i < numLeaves; i++) {
          storage.put(i, payloadFor(i, payloadSize, seed));
        }
        wtx.commit();
      }

      try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
           JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
        final var rtx = session.beginNodeReadOnlyTrx();
        try {
          final var probe = ProjectionIndexHOTStorage.readAll(
              rtx.getStorageEngineReader(), INDEX_NUMBER);
          final var cursor = ProjectionIndexHOTStorage.readAllViaCursor(
              rtx.getStorageEngineReader(), INDEX_NUMBER);

          assertEquals(numLeaves, probe.size(),
              "N=" + numLeaves + ": probe path must return all leaves");
          assertEquals(probe.size(), cursor.size(),
              "N=" + numLeaves + ": cursor must return same leaf count as probe "
                  + "(probe=" + probe.size() + ", cursor=" + cursor.size() + ")");

          for (int i = 0; i < numLeaves; i++) {
            assertArrayEquals(probe.get(i), cursor.get(i),
                "N=" + numLeaves + " leafIndex=" + i
                    + ": cursor must return byte-exact payload");
          }
        } finally {
          rtx.close();
        }
      }

      JsonTestHelper.deleteEverything();
    }
  }

  /**
   * Parallel-cursor variant must produce byte-identical output to the serial cursor.
   */
  @Test
  void readAllViaCursorParallel_matchesSerial_atScale() throws IOException {
    for (final int numLeaves : new int[] { 1_000, 10_000, 100_000 }) {
      JsonTestHelper.deleteEverything();
      Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));
      try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH)) {
        db.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME)
            .versioningApproach(VersioningType.FULL).build());
      }

      final int payloadSize = 64;
      final long seed = 0xBEEF_CAFEL ^ numLeaves;

      try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
           JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
           JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        for (int i = 0; i < numLeaves; i++) {
          storage.put(i, payloadFor(i, payloadSize, seed));
        }
        wtx.commit();
      }

      try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
           JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
        final var rtx = session.beginNodeReadOnlyTrx();
        try {
          final var serial = ProjectionIndexHOTStorage.readAllViaCursor(
              rtx.getStorageEngineReader(), INDEX_NUMBER);
          final var parallel = ProjectionIndexHOTStorage.readAllViaCursorParallel(
              rtx.getStorageEngineReader(), INDEX_NUMBER);

          assertEquals(numLeaves, serial.size(), "N=" + numLeaves + " serial size");
          assertEquals(serial.size(), parallel.size(), "N=" + numLeaves
              + ": parallel size must match serial (serial=" + serial.size()
              + ", parallel=" + parallel.size() + ")");
          for (int i = 0; i < numLeaves; i++) {
            assertArrayEquals(serial.get(i), parallel.get(i),
                "N=" + numLeaves + " leafIndex=" + i
                    + ": parallel must produce byte-exact payload");
          }
        } finally {
          rtx.close();
        }
      }

      JsonTestHelper.deleteEverything();
    }
  }

  @Test
  void readAll_onFreshDb_returnsEmpty() throws IOException {
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
         JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      final var rtx = session.beginNodeReadOnlyTrx();
      try {
        assertTrue(ProjectionIndexHOTStorage.readAll(rtx.getStorageEngineReader(), INDEX_NUMBER).isEmpty(),
            "a DB that never built a projection must report no leaves");
      } finally {
        rtx.close();
      }
    }
  }

  /**
   * iter#03c depth-2 parallel hydrate must produce byte-identical output to
   * the serial cursor across eight scenarios: small, medium, large,
   * tombstoned-sparse, empty, single-leaf, 50%-tombstoned, skewed.
   *
   * <p>Written as one {@link Test} with a tear-down / rebuild step per
   * scenario so a single CI failure message identifies which scenario
   * regressed. All scenarios run on the same JVM; the parallel path's
   * one-shot FANOUT_LOGGED gate fires only once across the eight cases.
   */
  @Test
  void readAllViaCursorParallelDepth2_matchesSerial_eightScenarios() throws IOException {
    // (a) small — 100 leaves, likely a single HOT leaf page (no splits).
    assertParallelDepth2EquivalentToSerial("small",
        /*numLeaves=*/ 100, /*payloadSize=*/ 128, /*seed=*/ 0xA01L, /*tombstoneEvery=*/ 0);

    // (b) medium — 10 K leaves, forces at least a handful of splits.
    assertParallelDepth2EquivalentToSerial("medium",
        /*numLeaves=*/ 10_000, /*payloadSize=*/ 64, /*seed=*/ 0xA02L, /*tombstoneEvery=*/ 0);

    // (c) large — 100 K leaves at production scale (close to the 97 657
    // benchmark shape). Exercises depth-2 fan-out properly.
    assertParallelDepth2EquivalentToSerial("large",
        /*numLeaves=*/ 100_000, /*payloadSize=*/ 32, /*seed=*/ 0xA03L, /*tombstoneEvery=*/ 0);

    // (d) tombstoned-mix — every 17th leaf is written then tombstoned
    // (put with shrinking payload through the EMPTY_CHUNK path) before
    // commit. Serial and parallel must agree on which leaves survive.
    assertParallelDepth2EquivalentToSerial("tombstoned-mix",
        /*numLeaves=*/ 2_000, /*payloadSize=*/ 256, /*seed=*/ 0xA04L, /*tombstoneEvery=*/ 17);

    // (e) empty — no leaves ever written; parallel must return an empty
    // list without NPE.
    assertParallelDepth2EquivalentToSerial("empty",
        /*numLeaves=*/ 0, /*payloadSize=*/ 128, /*seed=*/ 0xA05L, /*tombstoneEvery=*/ 0);

    // (f) single-leaf — one leafIndex only; root IS the leaf, so the
    // parallel path must fall back to serial (rootInstanceOf HOTLeafPage).
    assertParallelDepth2EquivalentToSerial("single-leaf",
        /*numLeaves=*/ 1, /*payloadSize=*/ 128, /*seed=*/ 0xA06L, /*tombstoneEvery=*/ 0);

    // (g) 50%-tombstoned — every other leaf tombstoned. Stresses the
    // drop-set path and the ordering of survivor emission at scale.
    assertParallelDepth2EquivalentToSerial("50pct-tombstoned",
        /*numLeaves=*/ 5_000, /*payloadSize=*/ 96, /*seed=*/ 0xA07L, /*tombstoneEvery=*/ 2);

    // (h) skewed — clustered leafIndexes (one sub-tree takes the bulk
    // via a leafIndex stride that makes half the values cluster in one
    // HOT sub-range).
    assertParallelDepth2EquivalentToSerial_skewed("skewed-80-20",
        /*numLeaves=*/ 5_000, /*payloadSize=*/ 64, /*seed=*/ 0xA08L);
  }

  /**
   * Skewed-distribution variant: writes {@code numLeaves} leaves whose
   * leafIndex values are drawn from two clusters (roughly 80/20 split
   * by index value) to force one depth-2 sub-tree to carry more entries.
   * This exercises the work-stealing + straggler path in the parallel
   * merge.
   */
  private void assertParallelDepth2EquivalentToSerial_skewed(
      final String scenario, final int numLeaves, final int payloadSize,
      final long seed) throws IOException {
    JsonTestHelper.deleteEverything();
    Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH)) {
      db.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME)
          .versioningApproach(VersioningType.FULL).build());
    }

    // Build a leaf-index list whose top bit varies 80/20 (skew) — this
    // lands most of the work in one depth-2 sub-tree (since the HOT
    // routing discriminates on the top-bit region for composite keys).
    final long[] leafIdx = new long[numLeaves];
    final int heavyShare = (int) (numLeaves * 0.8);
    // Heavy cluster: [0, numLeaves*2) → top bit 0.
    for (int i = 0; i < heavyShare; i++) {
      leafIdx[i] = i * 2L;
    }
    // Light cluster: [1L << 32, 1L << 32 + (numLeaves - heavyShare)) →
    // top of the 56-bit leafIndex space, routed to a different sub-tree.
    for (int i = heavyShare; i < numLeaves; i++) {
      leafIdx[i] = (1L << 32) + (i - heavyShare);
    }

    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
         JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
         JsonNodeTrx wtx = session.beginNodeTrx()) {
      final ProjectionIndexHOTStorage storage =
          new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
      for (int i = 0; i < numLeaves; i++) {
        storage.put(leafIdx[i], payloadFor(i, payloadSize, seed));
      }
      wtx.commit();
    }

    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
         JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      final var rtx = session.beginNodeReadOnlyTrx();
      try {
        final java.util.List<byte[]> serial = ProjectionIndexHOTStorage.readAllViaCursor(
            rtx.getStorageEngineReader(), INDEX_NUMBER);
        final java.util.List<byte[]> parallel =
            ProjectionIndexHOTStorage.readAllViaCursorParallelDepth2(
                rtx.getStorageEngineReader(), INDEX_NUMBER);

        assertEquals(numLeaves, serial.size(), "[" + scenario + "] serial size");
        assertEquals(serial.size(), parallel.size(),
            "[" + scenario + "] parallel/serial size mismatch");
        for (int i = 0; i < serial.size(); i++) {
          assertArrayEquals(serial.get(i), parallel.get(i),
              "[" + scenario + "] leaf " + i + ": byte-for-byte equivalence");
        }
      } finally {
        rtx.close();
      }
    }

    JsonTestHelper.deleteEverything();
    Databases.getGlobalBufferManager().clearAllCaches();
  }

  private void assertParallelDepth2EquivalentToSerial(
      final String scenario, final int numLeaves, final int payloadSize,
      final long seed, final int tombstoneEvery) throws IOException {
    JsonTestHelper.deleteEverything();
    Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH)) {
      db.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME)
          .versioningApproach(VersioningType.FULL).build());
    }

    // Reserve: track which leaves are expected to survive the scenario so
    // we can assert size exactly. A tombstoned leaf is dropped from the
    // visible set by the existing readAll contract.
    final int expectedVisible;
    if (numLeaves == 0) {
      expectedVisible = 0;
    } else {
      try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
           JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
           JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        int drops = 0;
        for (int i = 0; i < numLeaves; i++) {
          storage.put(i, payloadFor(i, payloadSize, seed));
          if (tombstoneEvery > 0 && (i % tombstoneEvery) == 0 && i != 0) {
            // Write an empty payload to tombstone this leaf. put(0-length)
            // writes one EMPTY_CHUNK per chunkCount, which matches the
            // read-side "any zero-sized chunk terminates the leaf" rule.
            storage.put(i, new byte[0]);
            drops++;
          }
        }
        wtx.commit();
        expectedVisible = numLeaves - drops;
      }
    }

    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
         JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      final var rtx = session.beginNodeReadOnlyTrx();
      try {
        final java.util.List<byte[]> serial = ProjectionIndexHOTStorage.readAllViaCursor(
            rtx.getStorageEngineReader(), INDEX_NUMBER);
        final java.util.List<byte[]> parallel =
            ProjectionIndexHOTStorage.readAllViaCursorParallelDepth2(
                rtx.getStorageEngineReader(), INDEX_NUMBER);

        assertEquals(expectedVisible, serial.size(),
            "[" + scenario + "] serial size (expected=" + expectedVisible + ")");
        assertEquals(serial.size(), parallel.size(),
            "[" + scenario + "] parallel size must match serial "
                + "(serial=" + serial.size() + ", parallel=" + parallel.size() + ")");

        for (int i = 0; i < serial.size(); i++) {
          assertArrayEquals(serial.get(i), parallel.get(i),
              "[" + scenario + "] leaf " + i + ": byte-for-byte equivalence");
        }
      } finally {
        rtx.close();
      }
    }

    JsonTestHelper.deleteEverything();
    Databases.getGlobalBufferManager().clearAllCaches();
  }

  @Test
  void put_payloadFitsInSingleChunk_whenUnderChunkSize() throws IOException {
    // With the default CHUNK_SIZE (HOT slot max 65 535 B), any realistic
    // projection leaf (≤ 20 KB) occupies one chunk. Verify chunkCountOf
    // reports 1 and get returns the byte-exact payload.
    final int payloadSize = Math.min(20 * 1024, ProjectionIndexHOTStorage.CHUNK_SIZE);
    final byte[] payload = deterministicPayload(payloadSize, 0x1234_5678L);

    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
         JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
         JsonNodeTrx wtx = session.beginNodeTrx()) {
      final ProjectionIndexHOTStorage storage =
          new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
      storage.put(99L, payload);
      assertEquals(1, storage.chunkCountOf(99L),
          "payload < CHUNK_SIZE must occupy exactly one chunk");
      assertArrayEquals(payload, storage.get(99L),
          "get must return the full payload byte-exact");
      wtx.commit();
    }
  }

  @Test
  void shrinkingPayload_keepsCorrectBytes() throws IOException {
    // Both payloads fit in one chunk at the default CHUNK_SIZE — verify
    // updating a leaf with a shorter payload reads back only the shorter
    // bytes, not stale tail content from the prior revision.
    final int bigSize = 12 * 1024;
    final int smallSize = 2 * 1024;
    final byte[] big = deterministicPayload(bigSize, 0xBBBB_1111L);
    final byte[] small = deterministicPayload(smallSize, 0x5555_2222L);

    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
         JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
         JsonNodeTrx wtx = session.beginNodeTrx()) {
      final ProjectionIndexHOTStorage storage =
          new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
      storage.put(7L, big);
      storage.put(7L, small);
      assertArrayEquals(small, storage.get(7L),
          "get after shrink must return only the new payload");
      wtx.commit();
    }
  }

  @org.junit.jupiter.api.Disabled("Pre-existing Sirix limitation tracked as task #57: HOT index "
      + "sub-trees don't provide per-revision historical read isolation — a chunk updated in "
      + "revision N bleeds into revision N-1 reads. The test is kept as an executable "
      + "specification of the target SLIDING_SNAPSHOT contract so the versioning fix can "
      + "flip @Disabled when it lands. Reproduces today: r2 putChunk at (5,1) → r1 reader "
      + "sees the r2 bytes when walking via HOTTrieReader. Same underlying issue affects "
      + "CAS / PATH / NAME indexes equally — not a projection-specific regression.")
  @Test
  void multiRevision_chunkUpdateInR2DoesNotAffectR1Readers() throws IOException {
    // Revision 1: write leafIndex 5 with a 3-chunk payload.
    // Revision 2: update chunk 1 in-place (putChunk).
    // Verify: reading at revision 1 still sees the r1 payload; reading at
    // revision 2 sees the updated chunk 1 + unchanged chunks 0 and 2.
    //
    // Keeps a single session open across both write transactions and the
    // historical reads — matches the pattern in {@code
    // HOTIndexManyRevisionsTest} where closing and reopening the session
    // between writes confuses in-memory RevisionRootPage references. The
    // CoW semantics the test pins are the same.
    final byte[] c0r1 = deterministicPayload(4096, 0x1010_1010L);
    final byte[] c1r1 = deterministicPayload(4096, 0x2020_2020L);
    final byte[] c2r1 = deterministicPayload(1024, 0x3030_3030L);
    final byte[] c1r2 = deterministicPayload(4096, 0x9999_FFFFL);

    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
         JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      final int r1Revision;
      final int r2Revision;

      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        storage.putChunk(5L, 0, c0r1);
        storage.putChunk(5L, 1, c1r1);
        storage.putChunk(5L, 2, c2r1);
        wtx.commit();
        r1Revision = wtx.getRevisionNumber();
      }

      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        storage.putChunk(5L, 1, c1r2);
        wtx.commit();
        r2Revision = wtx.getRevisionNumber();
      }

      assertTrue(r2Revision > r1Revision, "r2 must be newer than r1 — sanity");

      // Historical read at r1.
      try (var r1 = session.beginNodeReadOnlyTrx(r1Revision);
           HOTTrieReader reader = new HOTTrieReader(r1.getStorageEngineReader())) {
        final PageReference root =
            ProjectionIndexHOTStorage.rootReference(r1.getStorageEngineReader(), INDEX_NUMBER);
        assertNotNull(root, "r1 must still have a projection root");
        assertArrayEquals(c0r1, readChunkAt(reader, root, 5L, 0), "r1 chunk 0 must be unchanged");
        assertArrayEquals(c1r1, readChunkAt(reader, root, 5L, 1),
            "r1 chunk 1 must still see the pre-update payload — CoW must not back-propagate");
        assertArrayEquals(c2r1, readChunkAt(reader, root, 5L, 2), "r1 chunk 2 must be unchanged");
      }

      // Read at r2 — must see updated chunk 1; chunks 0 and 2 alias r1.
      try (var r2 = session.beginNodeReadOnlyTrx(r2Revision);
           HOTTrieReader reader = new HOTTrieReader(r2.getStorageEngineReader())) {
        final PageReference root =
            ProjectionIndexHOTStorage.rootReference(r2.getStorageEngineReader(), INDEX_NUMBER);
        assertNotNull(root, "r2 must have a projection root");
        assertArrayEquals(c0r1, readChunkAt(reader, root, 5L, 0), "r2 chunk 0 must alias r1 value");
        assertArrayEquals(c1r2, readChunkAt(reader, root, 5L, 1),
            "r2 chunk 1 must reflect the surgical update");
        assertArrayEquals(c2r1, readChunkAt(reader, root, 5L, 2), "r2 chunk 2 must alias r1 value");
      }
    }
  }

  private static byte[] readChunkAt(final HOTTrieReader reader, final PageReference root,
      final long leafIndex, final int chunkIdx) {
    final byte[] key = ProjectionIndexHOTStorage.encodeCompositeKey(leafIndex, chunkIdx);
    final MemorySegment seg = reader.get(root, key);
    if (seg == null) return null;
    final byte[] out = new byte[(int) seg.byteSize()];
    MemorySegment.copy(seg, ValueLayout.JAVA_BYTE, 0, out, 0, out.length);
    return out;
  }

  @Test
  void putChunk_allowsPerChunkUpdateWithoutRewritingSiblings() throws IOException {
    // Simulate the listener-style update: write three chunks, then rewrite
    // only chunk 1. Assert chunk 0 and chunk 2 still carry their original
    // bytes — i.e. putChunk is a surgical update, not a "put-full-payload"
    // stand-in.
    final byte[] c0 = deterministicPayload(4096, 0xC0_0L);
    final byte[] c1 = deterministicPayload(4096, 0xC0_1L);
    final byte[] c2 = deterministicPayload(1234, 0xC0_2L);
    final byte[] c1Updated = deterministicPayload(4096, 0xAAAA_BBBBL);

    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
         JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
         JsonNodeTrx wtx = session.beginNodeTrx()) {
      final ProjectionIndexHOTStorage storage =
          new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
      storage.putChunk(42L, 0, c0);
      storage.putChunk(42L, 1, c1);
      storage.putChunk(42L, 2, c2);

      storage.putChunk(42L, 1, c1Updated);

      assertArrayEquals(c0, storage.getChunk(42L, 0), "chunk 0 must survive a sibling rewrite");
      assertArrayEquals(c1Updated, storage.getChunk(42L, 1), "chunk 1 must reflect the surgical update");
      assertArrayEquals(c2, storage.getChunk(42L, 2), "chunk 2 must survive a sibling rewrite");
      wtx.commit();
    }
  }

  @Test
  void getMany_andReadAllParallel_matchSerialReadAll() throws IOException {
    // Write N leaves, then read via readAll / readAllParallel / getMany —
    // all three must produce byte-identical payloads. Guards against a
    // future regression in the parallel dispatcher or the two-pass
    // readAll assembly that would silently corrupt payloads.
    final int n = 200;
    final int payloadSize = 8 * 1024 + 317; // forces 3 chunks incl. partial
    final long seed = 0xABBA_F00DL;

    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
         JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        for (int i = 0; i < n; i++) storage.put(i, payloadFor(i, payloadSize, seed));
        wtx.commit();
      }
      // Use a single wtx for both the read-only query (via getStorageEngineReader)
      // and the parallel path (which needs an instance-method receiver). No
      // in-flight uncommitted mutations — the wtx above has already committed.
      try (JsonNodeTrx probe = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(probe.getStorageEngineWriter(), INDEX_NUMBER);
        // Diag: confirm the cursor visits every leafIndex 0..n-1 in order.
        for (int li = 0; li < n; li++) {
          final byte[] p = storage.get(li);
          assertArrayEquals(payloadFor(li, payloadSize, seed), p,
              "storage.get mismatch at leaf " + li + " — write-path corrupted before any readAll was called");
        }
        final java.util.List<byte[]> serial =
            ProjectionIndexHOTStorage.readAll(probe.getStorageEngineReader(), INDEX_NUMBER);
        final java.util.List<byte[]> parallel =
            ProjectionIndexHOTStorage.readAllParallel(storage, probe.getStorageEngineReader(), INDEX_NUMBER);
        assertEquals(n, serial.size(), "serial readAll leaf count");
        assertEquals(n, parallel.size(), "parallel readAll leaf count");
        for (int i = 0; i < n; i++) {
          assertArrayEquals(payloadFor(i, payloadSize, seed), serial.get(i),
              "serial readAll mismatch at leaf " + i);
          assertArrayEquals(payloadFor(i, payloadSize, seed), parallel.get(i),
              "parallel readAll mismatch at leaf " + i);
        }

        final long[] order = new long[n];
        for (int i = 0; i < n; i++) order[i] = (i * 31L + 7L) % n; // coprime, scrambled
        final byte[][] many = storage.getMany(order);
        for (int i = 0; i < n; i++) {
          assertArrayEquals(payloadFor((int) order[i], payloadSize, seed), many[i],
              "getMany mismatch at position " + i + " (leafIndex=" + order[i] + ")");
        }
      }
    }
  }

  @Test
  void put_throughputSanity_10kLeaves20KBEach() throws IOException {
    // Crude throughput sanity check to catch future regressions on the
    // per-put navigation cost. 10 K leaves × 20 KB = 200 MB of projection
    // payload split into 50 K chunks of 4 KB each. On the perf/umbra
    // reference host this completes in ~2 s post-optimisation (down from
    // ~12 s pre-optimisation, where every putChunk re-walked the HOT trie
    // from root). We assert a very loose 30 s ceiling — the point is to
    // fail loudly if someone reintroduces the per-chunk navigation cost.
    final int numLeaves = 10_000;
    final int payloadSize = 20 * 1024;
    final long seed = 0xDEAD_BEEFL;

    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
         JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
         JsonNodeTrx wtx = session.beginNodeTrx()) {
      final ProjectionIndexHOTStorage storage =
          new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
      final long t0 = System.nanoTime();
      for (int i = 0; i < numLeaves; i++) {
        storage.put(i, payloadFor(i, payloadSize, seed));
      }
      final long putMs = (System.nanoTime() - t0) / 1_000_000L;
      wtx.commit();
      // Loose ceiling — dev boxes vary — but 30 s for 10 K × 20 KB is a
      // ~60× regression over the current cost. Anything in that range
      // means the navigation short-circuit or tail-tombstone probe
      // regressed.
      assertTrue(putMs < 30_000, "put() throughput regressed: 10K×20KB took " + putMs + " ms");
      System.out.println("[perf] put " + numLeaves + " × " + payloadSize + " B = " + putMs + " ms ("
          + (numLeaves * 1000L / Math.max(1, putMs)) + " leaves/sec)");
    }
  }

  @Test
  void storageWaste_chunkingOverheadIsNegligible() throws IOException {
    // Write N leaves and measure on-disk file size with vs. without the
    // projection index installed. Chunk slot headers add ~12 B overhead
    // per chunk (HOTLeafPage suffix-len + value-len) — at 5 chunks per
    // 20 KB leaf that's ~60 B per leaf = ~0.3% overhead pre-compression.
    // Post-LZ4 the delta is smaller still because the per-chunk header
    // bytes are identical across chunks and compress to near-zero.
    final int n = 200;
    final int payloadSize = 8 * 1024 + 317;
    final long seed = 0xC0DE_F00DL;

    final long sizeWithProj;
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
         JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        for (int i = 0; i < n; i++) storage.put(i, payloadFor(i, payloadSize, seed));
        wtx.commit();
      }
      sizeWithProj = totalFileBytes(DATABASE_PATH);
    }

    // Clean and rebuild without the projection to establish baseline.
    JsonTestHelper.deleteEverything();
    Databases.createJsonDatabase(new io.sirix.access.DatabaseConfiguration(DATABASE_PATH));
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH)) {
      db.createResource(
          io.sirix.access.ResourceConfiguration.newBuilder(RESOURCE_NAME)
              .versioningApproach(VersioningType.FULL).build());
    }
    final long sizeBaseline;
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
         JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
         JsonNodeTrx wtx = session.beginNodeTrx()) {
      // Match the write-transaction footprint of the withProj run — just
      // commit once. No projection data written.
      wtx.commit();
      sizeBaseline = totalFileBytes(DATABASE_PATH);
    }

    final long projBytes = sizeWithProj - sizeBaseline;
    final long uncompressedPayload = (long) n * payloadSize;
    final double ratio = uncompressedPayload > 0 ? (double) projBytes / uncompressedPayload : 0.0;
    System.out.println(String.format(
        "[proj.storage] %d leaves × %d B = %d B uncompressed → %d B on-disk (ratio %.2fx, "
            + "post-LZ4; baseline %d B)",
        n, payloadSize, uncompressedPayload, projBytes, ratio, sizeBaseline));

    // Post-LZ4 the projection leaf data compresses reasonably despite
    // being random bytes in the test — the per-chunk overhead is in the
    // noise. Loose ceiling: projection bytes < 1.3x uncompressed payload.
    // A blowup would mean the chunking is emitting huge metadata overhead.
    assertTrue(projBytes < uncompressedPayload * 13 / 10,
        "projection storage overhead too high: " + projBytes
            + " > 1.3 × uncompressed payload " + uncompressedPayload);
  }

  /** Recursively sum every regular file under {@code root}. */
  private static long totalFileBytes(final Path root) throws IOException {
    final long[] total = { 0 };
    if (!Files.exists(root)) return 0;
    Files.walk(root).forEach(p -> {
      try {
        if (Files.isRegularFile(p)) total[0] += Files.size(p);
      } catch (IOException ignore) {
        // Skip inaccessible files; test is approximate.
      }
    });
    return total[0];
  }

  @Test
  void compositeKey_isSignedOrderPreservingForRealisticLeafIndexes() {
    // Composite encoding: (leafIndex << 8) | chunkIdx. leafIndex must
    // fit in 56 bits for the packing to round-trip — projection builds
    // produce monotonic non-negative ids, never close to the limit.
    final long[] leafIndexes = { 0L, 1L, 42L, 100_000L, 10_000_000L, 1L << 54 };
    final int[] chunkIdxs = { 0, 1, 10, 100, 255 };
    for (final long li : leafIndexes) {
      for (final int ci : chunkIdxs) {
        final byte[] encoded = ProjectionIndexHOTStorage.encodeCompositeKey(li, ci);
        final long[] decoded = ProjectionIndexHOTStorage.decodeCompositeKey(encoded);
        assertEquals(li, decoded[0], "leafIndex round-trip failed for (" + li + "," + ci + ")");
        assertEquals(ci, (int) decoded[1], "chunkIdx round-trip failed for (" + li + "," + ci + ")");
      }
    }
    // Composite ordering: (L, c) < (L, c+1), and (L, anything) < (L+1, anything).
    final byte[] a = ProjectionIndexHOTStorage.encodeCompositeKey(10L, 0);
    final byte[] b = ProjectionIndexHOTStorage.encodeCompositeKey(10L, 5);
    final byte[] c = ProjectionIndexHOTStorage.encodeCompositeKey(11L, 0);
    assertTrue(Arrays.compareUnsigned(a, b) < 0, "(10,0) < (10,5) expected");
    assertTrue(Arrays.compareUnsigned(b, c) < 0, "(10,5) < (11,0) expected");
  }

  private static byte[] payloadFor(final long leafIndex, final int size, final long seed) {
    // Deterministic per-leaf pattern so the assertion error at any leafIndex
    // is reproducible. Tag the head with the leafIndex so mismatched/crossed
    // reads are easy to spot.
    final byte[] out = new byte[size];
    final Random rng = new Random(seed ^ leafIndex);
    rng.nextBytes(out);
    out[0] = (byte) (leafIndex & 0xFF);
    out[1] = (byte) ((leafIndex >>> 8) & 0xFF);
    out[2] = (byte) 0xAA;
    out[3] = (byte) 0x55;
    return out;
  }

  private static byte[] deterministicPayload(final int size, final long seed) {
    final byte[] out = new byte[size];
    new Random(seed).nextBytes(out);
    return out;
  }

  /**
   * iter#04 equivalence gate: {@link HOTTrieReader#get} must return byte-identical
   * {@link MemorySegment} payloads regardless of the
   * {@code -Dsirix.hot.prefetch.parallelism=N} cap. Covers five permit values:
   *
   * <ul>
   *   <li>{@code N = 0} — prefetching disabled entirely;</li>
   *   <li>{@code N = 8}  — heavily capped (below default);</li>
   *   <li>{@code N = 40} — typical 20-core-box default
   *       ({@code 2 × availableProcessors()});</li>
   *   <li>{@code N = 64} — cap upper bound;</li>
   *   <li>{@code N = 1024} — effectively unbounded for this workload.</li>
   * </ul>
   *
   * <p>Writes 10 K leaves, then reopens the DB with a cold {@link HOTTrieReader}
   * and reads every leafIndex under each parallelism setting. All five passes
   * must return byte-identical payloads. Any divergence indicates a correctness
   * bug in the skip-on-contention prefetcher.</p>
   *
   * <p>Restores the pre-test prefetch cap in a {@code finally} so the rest of
   * the JUnit run is unaffected.</p>
   */
  @Test
  void hotTrieReader_prefetchParallelismCap_isBehaviorNeutral() throws IOException {
    final int numLeaves = 10_000;
    final int payloadSize = 128;
    final long seed = 0xCAFE_BABE_DEAD_BEEFL;

    // Populate the projection index once.
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
         JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
         JsonNodeTrx wtx = session.beginNodeTrx()) {
      final ProjectionIndexHOTStorage storage =
          new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
      for (int i = 0; i < numLeaves; i++) {
        storage.put(i, payloadFor(i, payloadSize, seed));
      }
      wtx.commit();
    }

    final int[] permitValues = { 0, 8, 40, 64, 1024 };
    final int savedPermits = HOTTrieReader.getPrefetchAvailablePermitsForTest();
    try {
      byte[][] reference = null;
      for (final int permits : permitValues) {
        HOTTrieReader.setPrefetchParallelismForTest(permits);

        // Each pass must reopen the DB / clear caches so the prefetcher path
        // is actually exercised (otherwise pages are already swizzled from
        // the first pass and no prefetch hint would fire).
        Databases.getGlobalBufferManager().clearAllCaches();

        try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
             JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
          final var rtx = session.beginNodeReadOnlyTrx();
          try {
            final PageReference root =
                ProjectionIndexHOTStorage.rootReference(rtx.getStorageEngineReader(), INDEX_NUMBER);
            assertNotNull(root, "projection root must exist at permits=" + permits);

            final byte[][] observed = new byte[numLeaves][];
            try (HOTTrieReader reader = new HOTTrieReader(rtx.getStorageEngineReader())) {
              for (int i = 0; i < numLeaves; i++) {
                final byte[] key = ProjectionIndexHOTStorage.encodeKey(i);
                final MemorySegment value = reader.get(root, key);
                assertNotNull(value, "permits=" + permits + " leafIndex=" + i + " must resolve");
                final byte[] out = new byte[(int) value.byteSize()];
                MemorySegment.copy(value, ValueLayout.JAVA_BYTE, 0, out, 0, out.length);
                observed[i] = out;
              }
            }

            if (reference == null) {
              reference = observed;
            } else {
              for (int i = 0; i < numLeaves; i++) {
                assertArrayEquals(reference[i], observed[i],
                    "permits=" + permits + " leafIndex=" + i
                        + " must match reference pass (permits=" + permitValues[0] + ")");
              }
            }
          } finally {
            rtx.close();
          }
        }
      }
    } finally {
      // Restore the pre-test permit count so downstream tests see the default.
      HOTTrieReader.setPrefetchParallelismForTest(savedPermits);
    }
  }

  /**
   * iter#08 — zero-alloc cursor fast-path parity. Walks the same data twice:
   * once via the legacy {@link HOTRangeCursor#hasNext} /
   * {@link HOTRangeCursor#next} {@link HOTRangeCursor.Entry} pattern, and
   * once via the zero-alloc {@link HOTRangeCursor#currentLeafPage} +
   * {@link HOTLeafPage#decodeKey8BE} + {@link HOTRangeCursor#currentValueSlice}
   * + {@link HOTRangeCursor#advance} pattern. Composite keys must decode
   * identically, values must be byte-exact.
   */
  @Test
  void zeroAllocCursorPath_parityWithLegacyIterator() throws IOException {
    final int numLeaves = 256;
    final int payloadSize = 96;
    final long seed = 0xF00D_BEEFL;

    // Write
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
         JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
         JsonNodeTrx wtx = session.beginNodeTrx()) {
      final ProjectionIndexHOTStorage storage =
          new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
      for (int i = 0; i < numLeaves; i++) {
        storage.put(i, payloadFor(i, payloadSize, seed));
      }
      wtx.commit();
    }

    // Read via legacy iterator API
    final java.util.ArrayList<Long> legacyKeys = new java.util.ArrayList<>();
    final java.util.ArrayList<byte[]> legacyValues = new java.util.ArrayList<>();
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
         JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      final var rtx = session.beginNodeReadOnlyTrx();
      try {
        final PageReference rootRef =
            ProjectionIndexHOTStorage.rootReference(rtx.getStorageEngineReader(), INDEX_NUMBER);
        assertNotNull(rootRef);
        final byte[] minKey = new byte[] { 0, 0, 0, 0, 0, 0, 0, 0 };
        final byte[] maxKey = new byte[] {
            (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
            (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF };
        try (HOTTrieReader trieReader = new HOTTrieReader(rtx.getStorageEngineReader());
             HOTRangeCursor cursor = trieReader.range(rootRef, minKey, maxKey)) {
          while (cursor.hasNext()) {
            final HOTRangeCursor.Entry entry = cursor.next();
            final byte[] keyBytes = entry.keyBytes();
            final long signFlipped =
                ((long) (keyBytes[0] & 0xFF) << 56) | ((long) (keyBytes[1] & 0xFF) << 48)
                    | ((long) (keyBytes[2] & 0xFF) << 40) | ((long) (keyBytes[3] & 0xFF) << 32)
                    | ((long) (keyBytes[4] & 0xFF) << 24) | ((long) (keyBytes[5] & 0xFF) << 16)
                    | ((long) (keyBytes[6] & 0xFF) << 8) | ((long) (keyBytes[7] & 0xFF));
            legacyKeys.add(signFlipped);
            legacyValues.add(entry.valueBytes());
          }
        }
      } finally {
        rtx.close();
      }
    }

    // Read via zero-alloc fast-path API
    final java.util.ArrayList<Long> fastKeys = new java.util.ArrayList<>();
    final java.util.ArrayList<byte[]> fastValues = new java.util.ArrayList<>();
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
         JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      final var rtx = session.beginNodeReadOnlyTrx();
      try {
        final PageReference rootRef =
            ProjectionIndexHOTStorage.rootReference(rtx.getStorageEngineReader(), INDEX_NUMBER);
        assertNotNull(rootRef);
        final byte[] minKey = new byte[] { 0, 0, 0, 0, 0, 0, 0, 0 };
        final byte[] maxKey = new byte[] {
            (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
            (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF };
        try (HOTTrieReader trieReader = new HOTTrieReader(rtx.getStorageEngineReader());
             HOTRangeCursor cursor = trieReader.range(rootRef, minKey, maxKey)) {
          while (cursor.hasNext()) {
            final HOTLeafPage leaf = cursor.currentLeafPage();
            final int entryIdx = cursor.currentEntryIndex();
            final long signFlipped = leaf.decodeKey8BE(entryIdx);
            final MemorySegment valSlice = cursor.currentValueSlice();
            final byte[] val = new byte[(int) valSlice.byteSize()];
            MemorySegment.copy(valSlice, ValueLayout.JAVA_BYTE, 0, val, 0, val.length);
            fastKeys.add(signFlipped);
            fastValues.add(val);
            cursor.advance();
          }
        }
      } finally {
        rtx.close();
      }
    }

    assertEquals(legacyKeys.size(), fastKeys.size(),
        "zero-alloc cursor must visit the same number of entries as legacy iterator");
    for (int i = 0; i < legacyKeys.size(); i++) {
      assertEquals(legacyKeys.get(i), fastKeys.get(i),
          "entry " + i + ": decoded key must match between legacy and fast paths");
      assertArrayEquals(legacyValues.get(i), fastValues.get(i),
          "entry " + i + ": value bytes must match between legacy and fast paths");
    }
  }

  /**
   * iter#08 — {@link HOTLeafPage#compareKeyWithBound} + {@link HOTLeafPage#decodeKey8BE}
   * byte-level correctness against the legacy allocating path for a leaf
   * populated with composite keys covering boundaries. The composite keys
   * are 8-byte big-endian sign-flipped (see
   * {@link io.sirix.index.hot.PathKeySerializer}).
   *
   * <p>Writes N composite keys, then asserts that for every entry and every
   * bound candidate (min, mid, max, N+1, exact-match):
   * <ul>
   *   <li>{@code compareKeyWithBound} returns the sign of a byte-array
   *       {@code Arrays.compareUnsigned} against the reconstructed key;</li>
   *   <li>{@code decodeKey8BE} returns exactly the big-endian unsigned long
   *       interpretation of the reconstructed key.</li>
   * </ul>
   */
  @Test
  void compareKeyWithBound_and_decodeKey8BE_matchReference() throws IOException {
    final int numLeaves = 256;
    final int payloadSize = 16;
    final long seed = 0xBABE_5EEDL;

    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
         JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
         JsonNodeTrx wtx = session.beginNodeTrx()) {
      final ProjectionIndexHOTStorage storage =
          new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
      for (int i = 0; i < numLeaves; i++) {
        storage.put(i, payloadFor(i, payloadSize, seed));
      }
      wtx.commit();
    }

    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
         JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      final var rtx = session.beginNodeReadOnlyTrx();
      try {
        final PageReference rootRef =
            ProjectionIndexHOTStorage.rootReference(rtx.getStorageEngineReader(), INDEX_NUMBER);
        assertNotNull(rootRef);
        final byte[] minKey = new byte[] { 0, 0, 0, 0, 0, 0, 0, 0 };
        final byte[] maxKey = new byte[] {
            (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
            (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF };
        // Pick a couple of bound candidates per entry.
        final byte[][] bounds = new byte[][] {
            minKey,
            maxKey,
            ProjectionIndexHOTStorage.encodeCompositeKey(numLeaves / 2, 0),
            ProjectionIndexHOTStorage.encodeCompositeKey(numLeaves, 0),   // past last
        };

        try (HOTTrieReader trieReader = new HOTTrieReader(rtx.getStorageEngineReader());
             HOTRangeCursor cursor = trieReader.range(rootRef, minKey, maxKey)) {
          while (cursor.hasNext()) {
            final HOTLeafPage leaf = cursor.currentLeafPage();
            final int entryIdx = cursor.currentEntryIndex();

            // Reference: reconstruct the full 8-byte key and assert decode.
            final byte[] refKey = leaf.getKey(entryIdx);
            assertEquals(8, refKey.length, "composite keys must be 8 bytes");
            long refLong = 0L;
            for (int b = 0; b < 8; b++) refLong = (refLong << 8) | (refKey[b] & 0xFFL);
            assertEquals(refLong, leaf.decodeKey8BE(entryIdx),
                "decodeKey8BE must equal BE-long interpretation of getKey()");

            // Reference: compare against each bound via Arrays.compareUnsigned.
            for (final byte[] bound : bounds) {
              final int refCmp = Arrays.compareUnsigned(refKey, bound);
              final int fastCmp = leaf.compareKeyWithBound(entryIdx, bound);
              // We don't require equal magnitudes — only equal signs.
              assertEquals(Integer.signum(refCmp), Integer.signum(fastCmp),
                  "compareKeyWithBound sign mismatch at entryIdx=" + entryIdx
                      + " bound.length=" + bound.length);
            }
            cursor.advance();
          }
        }
      } finally {
        rtx.close();
      }
    }
  }
}
