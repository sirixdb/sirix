/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.JsonTestHelper;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Reproduction + regression guard for the projection force-rebuild failure:
 * {@code SirixIOException: Projection HOT chunk insert failed after split}.
 *
 * <p>Scenario (mirrors {@code ScaleBenchProjectionSetup.installWildcard} with
 * {@code -Dsirix.projection.forceRebuild=true} over a database that already
 * holds smaller persisted leaves): a 3-column projection build persists small
 * leaves; a later 6-column rebuild overwrites the <em>same leaf ids</em> with
 * payloads 2-4&times; larger. The grown payloads need more chunks per leaf, so
 * the rebuild mixes size-changing value updates with brand-new chunk keys on
 * HOT leaf pages that are already full — exactly the case where a leaf split
 * must make room for the insert.
 */
final class ProjectionIndexHOTStorageGrowingPayloadTest {

  private static final String RESOURCE_NAME = "testResource";
  private static final java.nio.file.Path DATABASE_PATH = JsonTestHelper.PATHS.PATH1.getFile();
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

  /**
   * The core force-rebuild repro: persist small (sub-chunk) leaves, commit,
   * then overwrite every id with a payload ~4x larger (multi-chunk), commit,
   * and read everything back byte-identical.
   */
  @Test
  void overwriteWithLargerLeaves_afterCommit_roundTrips() throws IOException {
    final int numLeaves = 300;
    final int smallSize = 2 * 1024;          // 3-column-ish leaf — one partial chunk
    final int largeSize = 8 * 1024 - 192;    // 6-column-ish leaf — two chunks (full + partial)
    final long smallSeed = 0x3C01_5EEDL;
    final long largeSeed = 0x6C01_5EEDL;

    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
         JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      // Phase 1 — initial (small) projection build, like the 3-column install.
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        for (int i = 0; i < numLeaves; i++) {
          storage.put(i, payloadFor(i, smallSize, smallSeed));
        }
        wtx.commit();
      }

      // Phase 2 — force-rebuild with more columns: same ids, larger payloads.
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        for (int i = 0; i < numLeaves; i++) {
          storage.put(i, payloadFor(i, largeSize, largeSeed));
        }
        // Same-trx readback — catches silent dropped/truncated chunks even
        // before commit.
        for (int i = 0; i < numLeaves; i++) {
          assertArrayEquals(payloadFor(i, largeSize, largeSeed), storage.get(i),
              "leafIndex=" + i + " must return the grown payload in the rebuild trx");
        }
        wtx.commit();
      }

      // Phase 3 — cold read after commit.
      final var rtx = session.beginNodeReadOnlyTrx();
      try {
        final List<byte[]> all =
            ProjectionIndexHOTStorage.readAll(rtx.getStorageEngineReader(), INDEX_NUMBER);
        assertEquals(numLeaves, all.size(), "every grown leaf must survive the rebuild commit");
        for (int i = 0; i < numLeaves; i++) {
          assertArrayEquals(payloadFor(i, largeSize, largeSeed), all.get(i),
              "leafIndex=" + i + " must be byte-identical after the grow-rebuild");
        }
      } finally {
        rtx.close();
      }
    }
  }

  /**
   * Repeated growth across three generations (1 chunk → 2 chunks → 4 chunks)
   * with a commit between each — the multi-revision shape of a database that
   * is migrated twice. Also covers growth where the OLD tail chunk was partial
   * and the new payload extends past it.
   */
  @Test
  void repeatedGrowthAcrossRevisions_roundTrips() throws IOException {
    final int numLeaves = 200;
    final int[] sizes = { 3 * 1024, 7 * 1024 + 123, 15 * 1024 + 77 };
    final long[] seeds = { 0xAAAA_0001L, 0xBBBB_0002L, 0xCCCC_0003L };

    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
         JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      for (int gen = 0; gen < sizes.length; gen++) {
        try (JsonNodeTrx wtx = session.beginNodeTrx()) {
          final ProjectionIndexHOTStorage storage =
              new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
          for (int i = 0; i < numLeaves; i++) {
            storage.put(i, payloadFor(i, sizes[gen], seeds[gen]));
          }
          wtx.commit();
        }

        final var rtx = session.beginNodeReadOnlyTrx();
        try {
          final List<byte[]> all =
              ProjectionIndexHOTStorage.readAll(rtx.getStorageEngineReader(), INDEX_NUMBER);
          assertEquals(numLeaves, all.size(), "generation " + gen + ": leaf count");
          for (int i = 0; i < numLeaves; i++) {
            assertArrayEquals(payloadFor(i, sizes[gen], seeds[gen]), all.get(i),
                "generation " + gen + " leafIndex=" + i + " must round-trip");
          }
        } finally {
          rtx.close();
        }
      }
    }
  }

  /**
   * Same-trx variant: small puts followed by larger overwrites of the same
   * ids WITHOUT an intervening commit. The in-memory (TIL) pages take the
   * same put/update/split paths.
   */
  @Test
  void overwriteWithLargerLeaves_inSameTrx_roundTrips() throws IOException {
    final int numLeaves = 300;
    final int smallSize = 2 * 1024;
    final int largeSize = 6 * 1024 + 511;
    final long smallSeed = 0x1111_2222L;
    final long largeSeed = 0x3333_4444L;

    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
         JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
         JsonNodeTrx wtx = session.beginNodeTrx()) {
      final ProjectionIndexHOTStorage storage =
          new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
      for (int i = 0; i < numLeaves; i++) {
        storage.put(i, payloadFor(i, smallSize, smallSeed));
      }
      for (int i = 0; i < numLeaves; i++) {
        storage.put(i, payloadFor(i, largeSize, largeSeed));
      }
      for (int i = 0; i < numLeaves; i++) {
        assertArrayEquals(payloadFor(i, largeSize, largeSeed), storage.get(i),
            "leafIndex=" + i + " must return the grown payload");
      }
      wtx.commit();
    }
  }

  /**
   * Fresh insert of oversized values: each payload alone (~200 KB = ~50
   * chunks) exceeds a single HOT leaf page's 64 KB slot heap several times
   * over, so the very first leaves already force splits. Everything must
   * still round-trip byte-identical.
   */
  @Test
  void freshInsert_payloadLargerThanOneHOTLeafPage_roundTrips() throws IOException {
    final int numLeaves = 24;
    final int payloadSize = 200 * 1024 + 13;
    final long seed = 0x0B0E_517EDL;

    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
         JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        for (int i = 0; i < numLeaves; i++) {
          storage.put(i, payloadFor(i, payloadSize, seed));
        }
        for (int i = 0; i < numLeaves; i++) {
          assertArrayEquals(payloadFor(i, payloadSize, seed), storage.get(i),
              "oversized leafIndex=" + i + " must round-trip in the same trx");
        }
        wtx.commit();
      }

      final var rtx = session.beginNodeReadOnlyTrx();
      try {
        final List<byte[]> all =
            ProjectionIndexHOTStorage.readAll(rtx.getStorageEngineReader(), INDEX_NUMBER);
        assertEquals(numLeaves, all.size(), "all oversized leaves must persist");
        for (int i = 0; i < numLeaves; i++) {
          assertArrayEquals(payloadFor(i, payloadSize, seed), all.get(i),
              "oversized leafIndex=" + i + " must round-trip after commit");
        }
      } finally {
        rtx.close();
      }
    }
  }

  /**
   * Versioning contract under SLIDING_SNAPSHOT (the strategy the projection
   * chunking is designed for): grow-rebuild in revision 2 must not disturb
   * revision 1 readers — both revisions hydrate their own payloads
   * byte-identically after the grow.
   */
  @Test
  void growUnderSlidingSnapshot_keepsHistoricalRevisionIntact() throws IOException {
    JsonTestHelper.deleteEverything();
    Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH)) {
      db.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME)
          .versioningApproach(VersioningType.SLIDING_SNAPSHOT).build());
    }

    final int numLeaves = 200;
    final int smallSize = 2 * 1024 + 100;
    final int largeSize = 9 * 1024 + 50;
    final long smallSeed = 0x511D_0001L;
    final long largeSeed = 0x511D_0002L;

    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
         JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        for (int i = 0; i < numLeaves; i++) {
          storage.put(i, payloadFor(i, smallSize, smallSeed));
        }
        wtx.commit();
      }
      final int r1 = session.getMostRecentRevisionNumber();

      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        for (int i = 0; i < numLeaves; i++) {
          storage.put(i, payloadFor(i, largeSize, largeSeed));
        }
        wtx.commit();
      }
      final int r2 = session.getMostRecentRevisionNumber();

      // Historical revision r1: still the SMALL payloads, byte-identical.
      final var rtx1 = session.beginNodeReadOnlyTrx(r1);
      try {
        final List<byte[]> atR1 =
            ProjectionIndexHOTStorage.readAll(rtx1.getStorageEngineReader(), INDEX_NUMBER);
        assertEquals(numLeaves, atR1.size(), "r1 must keep every original leaf");
        for (int i = 0; i < numLeaves; i++) {
          assertArrayEquals(payloadFor(i, smallSize, smallSeed), atR1.get(i),
              "r1 leafIndex=" + i + " must be untouched by the r2 grow (CoW isolation)");
        }
      } finally {
        rtx1.close();
      }

      // Current revision r2: the GROWN payloads.
      final var rtx2 = session.beginNodeReadOnlyTrx(r2);
      try {
        final List<byte[]> atR2 =
            ProjectionIndexHOTStorage.readAll(rtx2.getStorageEngineReader(), INDEX_NUMBER);
        assertEquals(numLeaves, atR2.size(), "r2 must contain every grown leaf");
        for (int i = 0; i < numLeaves; i++) {
          assertArrayEquals(payloadFor(i, largeSize, largeSeed), atR2.get(i),
              "r2 leafIndex=" + i + " must hydrate the grown payload");
        }
      } finally {
        rtx2.close();
      }
    }
  }

  /**
   * Shrink direction: a leaf rewritten with FEWER chunks tail-tombstones the
   * stale chunks. {@code readAll} must agree with {@code get}: the leaf
   * survives with the truncated payload (tombstones terminate the payload,
   * they do not erase the whole leaf).
   */
  @Test
  void shrinkMultiChunkLeaf_readAllMatchesGet() throws IOException {
    final int numLeaves = 64;
    final int bigSize = 10 * 1024;       // 3 chunks
    final int smallSize = 2 * 1024 + 17; // 1 chunk
    final long bigSeed = 0xB16_0001L;
    final long smallSeed = 0x5A11_0002L;

    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
         JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        for (int i = 0; i < numLeaves; i++) {
          storage.put(i, payloadFor(i, bigSize, bigSeed));
        }
        wtx.commit();
      }
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        for (int i = 0; i < numLeaves; i++) {
          storage.put(i, payloadFor(i, smallSize, smallSeed));
        }
        for (int i = 0; i < numLeaves; i++) {
          assertArrayEquals(payloadFor(i, smallSize, smallSeed), storage.get(i),
              "get: leafIndex=" + i + " must return only the shrunken payload");
        }
        wtx.commit();
      }

      final var rtx = session.beginNodeReadOnlyTrx();
      try {
        final List<byte[]> all =
            ProjectionIndexHOTStorage.readAll(rtx.getStorageEngineReader(), INDEX_NUMBER);
        assertEquals(numLeaves, all.size(),
            "shrunken leaves must still be visible to readAll (tombstones truncate, not erase)");
        for (int i = 0; i < numLeaves; i++) {
          assertArrayEquals(payloadFor(i, smallSize, smallSeed), all.get(i),
              "readAll: leafIndex=" + i + " must match get() after the shrink");
        }
      } finally {
        rtx.close();
      }
    }
  }

  /**
   * Capacity contract: a payload beyond {@code MAX_CHUNKS_PER_LEAF *
   * CHUNK_SIZE} must be rejected with a clear {@link IllegalArgumentException}
   * up-front — never a deep split failure after partial writes.
   */
  @Test
  void freshInsert_payloadBeyondMaxChunks_throwsCleanContractViolation() throws IOException {
    final int tooBig = ProjectionIndexHOTStorage.MAX_CHUNKS_PER_LEAF
        * ProjectionIndexHOTStorage.CHUNK_SIZE + 1;

    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
         JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
         JsonNodeTrx wtx = session.beginNodeTrx()) {
      final ProjectionIndexHOTStorage storage =
          new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
      assertThrows(IllegalArgumentException.class,
          () -> storage.put(0L, new byte[tooBig]),
          "payload beyond the 1 MB composite-key cap must fail the up-front contract check");
      wtx.commit();
    }
  }

  private static byte[] payloadFor(final long leafIndex, final int size, final long seed) {
    final byte[] out = new byte[size];
    final Random rng = new Random(seed ^ (leafIndex * 0x9E37_79B9_7F4A_7C15L));
    rng.nextBytes(out);
    out[0] = (byte) (leafIndex & 0xFF);
    out[1] = (byte) ((leafIndex >>> 8) & 0xFF);
    return out;
  }
}
