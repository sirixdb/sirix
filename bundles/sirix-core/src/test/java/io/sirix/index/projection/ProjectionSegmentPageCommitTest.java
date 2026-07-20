/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.JsonTestHelper;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * P1 spike + regression suite for the segment-page commit chain
 * (docs/PROJECTION_INDEX_STORAGE_REDESIGN.md §9 P0/P1): a
 * {@code ProjectionSegmentPage} referenced from a {@code HOTLeafPage}'s side
 * map must survive commit → cold reopen → read-back, across deep split
 * cascades, sparse-fragment second commits, and rollbacks — exercising
 * hazards 5.2-a (commit-order key resolution) and 5.2-b (sparse dirty-entry
 * emit + fragment-merge carry) before any projection storage code moves to
 * the new layout.
 *
 * <p>Under the interim chunked layout, the "owner slot" of a segment ref is
 * the leaf's chunk-0 slot ({@code ownerSlotKey = leafIndex << 8}); the P3
 * rewrite switches owner slots to plain descriptor keys with the identical
 * side-map convention {@code (ownerSlotKey << 8) | segmentId}.
 */
final class ProjectionSegmentPageCommitTest {

  private static final String RESOURCE_NAME = "testResource";
  private static final Path DATABASE_PATH = JsonTestHelper.PATHS.PATH1.getFile();
  private static final int INDEX_NUMBER = 0;

  @BeforeEach
  void setUp() throws IOException {
    JsonTestHelper.deleteEverything();
    Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH)) {
      // Default versioning (SLIDING_SNAPSHOT) — the sparse dirty-entry emit and
      // fragment-merge paths only exist under non-FULL versioning.
      db.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME).build());
    }
  }

  @AfterEach
  void tearDown() throws IOException {
    JsonTestHelper.deleteEverything();
    Databases.getGlobalBufferManager().clearAllCaches();
  }

  /** Owner slot of leaf {@code leafIndex}'s chunk-0 slot under the interim layout. */
  private static long ownerSlot(final long leafIndex) {
    return leafIndex << 8;
  }

  private static byte[] segmentBytes(final long ownerSlotKey, final int segmentId, final int size) {
    final byte[] bytes = new byte[size];
    new Random(ownerSlotKey * 31 + segmentId).nextBytes(bytes);
    return bytes;
  }

  private static byte[] chunkPayload(final int leafIndex, final int size, final long seed) {
    final byte[] payload = new byte[size];
    new Random(seed ^ leafIndex).nextBytes(payload);
    return payload;
  }

  @Test
  void segmentRefSurvivesCommitAndColdReopen() {
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
         JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        storage.put(0, chunkPayload(0, 2048, 0xA5A5L));
        storage.putSegmentPage(ownerSlot(0), 1, segmentBytes(ownerSlot(0), 1, 900));
        storage.putSegmentPage(ownerSlot(0), 2, segmentBytes(ownerSlot(0), 2, 3000));

        // Same-transaction readback resolves the in-memory page on the reference.
        assertArrayEquals(segmentBytes(ownerSlot(0), 1, 900), storage.getSegmentPageBytes(ownerSlot(0), 1));
        assertArrayEquals(segmentBytes(ownerSlot(0), 2, 3000), storage.getSegmentPageBytes(ownerSlot(0), 2));
        wtx.commit();
      }

      Databases.getGlobalBufferManager().clearAllCaches();

      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertArrayEquals(segmentBytes(ownerSlot(0), 1, 900),
            ProjectionIndexHOTStorage.readSegmentPageBytes(rtx.getStorageEngineReader(), INDEX_NUMBER,
                ownerSlot(0), 1),
            "segment 1 must survive commit + cold reopen");
        assertArrayEquals(segmentBytes(ownerSlot(0), 2, 3000),
            ProjectionIndexHOTStorage.readSegmentPageBytes(rtx.getStorageEngineReader(), INDEX_NUMBER,
                ownerSlot(0), 2),
            "segment 2 must survive commit + cold reopen");
        assertNull(ProjectionIndexHOTStorage.readSegmentPageBytes(rtx.getStorageEngineReader(), INDEX_NUMBER,
                ownerSlot(0), 3),
            "absent segment id must read as null");
        // The chunk payload sharing the leaf is untouched by the refs section.
        final List<byte[]> all =
            ProjectionIndexHOTStorage.readAll(rtx.getStorageEngineReader(), INDEX_NUMBER);
        assertEquals(1, all.size());
        assertArrayEquals(chunkPayload(0, 2048, 0xA5A5L), all.get(0));
      }
    }
  }

  /**
   * Refs attached EARLY must follow their owner slots through the split
   * cascades caused by hundreds of later multi-chunk puts
   * ({@code HOTLeafPage#moveSegmentRefsAfterSplit} across all split variants).
   */
  @Test
  void segmentRefsSurviveDeepSplitCascades() {
    final int numLeaves = 300;
    final int chunkSize = 8 * 1024 - 192; // two chunks per leaf — forces splits
    final int earlyRefs = 50;
    final long seed = 0xDEE9_5EEDL;

    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
         JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);

        // Early phase: few leaves, attach refs while the trie is still shallow.
        for (int i = 0; i < earlyRefs; i++) {
          storage.put(i, chunkPayload(i, chunkSize, seed));
          storage.putSegmentPage(ownerSlot(i), 1, segmentBytes(ownerSlot(i), 1, 700 + i));
        }
        // Late phase: force deep split cascades AFTER the early refs exist.
        for (int i = earlyRefs; i < numLeaves; i++) {
          storage.put(i, chunkPayload(i, chunkSize, seed));
          storage.putSegmentPage(ownerSlot(i), 1, segmentBytes(ownerSlot(i), 1, 700 + i));
        }

        // Pre-commit readback through writer-side navigation.
        for (int i = 0; i < numLeaves; i++) {
          assertArrayEquals(segmentBytes(ownerSlot(i), 1, 700 + i),
              storage.getSegmentPageBytes(ownerSlot(i), 1),
              "pre-commit segment of leaf " + i + " must resolve after split cascades");
        }
        wtx.commit();
      }

      Databases.getGlobalBufferManager().clearAllCaches();

      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        for (int i = 0; i < numLeaves; i++) {
          assertArrayEquals(segmentBytes(ownerSlot(i), 1, 700 + i),
              ProjectionIndexHOTStorage.readSegmentPageBytes(rtx.getStorageEngineReader(), INDEX_NUMBER,
                  ownerSlot(i), 1),
              "committed segment of leaf " + i + " must survive the split cascade");
        }
        final List<byte[]> all =
            ProjectionIndexHOTStorage.readAll(rtx.getStorageEngineReader(), INDEX_NUMBER);
        assertEquals(numLeaves, all.size(), "chunk payloads must be unaffected by side-map refs");
      }
    }
  }

  /**
   * Second commit touching a subset of leaves: replaced refs serve new bytes,
   * untouched refs carry forward by reference (sparse dirty-entry emit +
   * newest-fragment-authoritative merge), and removed refs stay removed.
   */
  @Test
  void secondCommitFragmentMergeCarriesAndReplacesRefs() {
    final int numLeaves = 120;
    final int chunkSize = 8 * 1024 - 192;
    final long seed = 0xF00D_5EEDL;

    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
         JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        for (int i = 0; i < numLeaves; i++) {
          storage.put(i, chunkPayload(i, chunkSize, seed));
          storage.putSegmentPage(ownerSlot(i), 1, segmentBytes(ownerSlot(i), 1, 500 + i));
          storage.putSegmentPage(ownerSlot(i), 2, segmentBytes(ownerSlot(i), 2, 1200 + i));
        }
        wtx.commit();
      }

      // Second commit: replace segment 1 on every third leaf, remove segment 2
      // on every fifth leaf, leave the rest untouched.
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        for (int i = 0; i < numLeaves; i += 3) {
          storage.putSegmentPage(ownerSlot(i), 1, segmentBytes(ownerSlot(i), 1, 5000 + i));
        }
        for (int i = 0; i < numLeaves; i += 5) {
          storage.removeSegmentPage(ownerSlot(i), 2);
        }
        wtx.commit();
      }

      Databases.getGlobalBufferManager().clearAllCaches();

      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        for (int i = 0; i < numLeaves; i++) {
          final byte[] expected1 = (i % 3 == 0)
              ? segmentBytes(ownerSlot(i), 1, 5000 + i)
              : segmentBytes(ownerSlot(i), 1, 500 + i);
          assertArrayEquals(expected1,
              ProjectionIndexHOTStorage.readSegmentPageBytes(rtx.getStorageEngineReader(), INDEX_NUMBER,
                  ownerSlot(i), 1),
              "segment 1 of leaf " + i + " after second commit");
          final byte[] actual2 =
              ProjectionIndexHOTStorage.readSegmentPageBytes(rtx.getStorageEngineReader(), INDEX_NUMBER,
                  ownerSlot(i), 2);
          if (i % 5 == 0) {
            assertNull(actual2, "removed segment 2 of leaf " + i + " must not resurrect");
          } else {
            assertArrayEquals(segmentBytes(ownerSlot(i), 2, 1200 + i), actual2,
                "untouched segment 2 of leaf " + i + " must carry forward by reference");
          }
        }
      }

      // Time travel: revision 1 still serves the ORIGINAL refs (CoW isolation).
      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(1)) {
        for (int i = 0; i < numLeaves; i++) {
          assertArrayEquals(segmentBytes(ownerSlot(i), 1, 500 + i),
              ProjectionIndexHOTStorage.readSegmentPageBytes(rtx.getStorageEngineReader(), INDEX_NUMBER,
                  ownerSlot(i), 1),
              "revision 1 segment 1 of leaf " + i + " must be unaffected by the later commit");
          assertArrayEquals(segmentBytes(ownerSlot(i), 2, 1200 + i),
              ProjectionIndexHOTStorage.readSegmentPageBytes(rtx.getStorageEngineReader(), INDEX_NUMBER,
                  ownerSlot(i), 2),
              "revision 1 segment 2 of leaf " + i + " must be unaffected by the later removal");
        }
      }
    }
  }

  /** Rolled-back segment pages are never written; nothing survives. */
  @Test
  void rolledBackSegmentRefsAreNeverWritten() {
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
         JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME)) {
      try (JsonNodeTrx wtx = session.beginNodeTrx()) {
        final ProjectionIndexHOTStorage storage =
            new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), INDEX_NUMBER);
        storage.put(0, chunkPayload(0, 2048, 0xBAD5EEDL));
        storage.putSegmentPage(ownerSlot(0), 1, segmentBytes(ownerSlot(0), 1, 800));
        wtx.rollback();
      }

      try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        assertNull(ProjectionIndexHOTStorage.readSegmentPageBytes(rtx.getStorageEngineReader(), INDEX_NUMBER,
                ownerSlot(0), 1),
            "rolled-back segment must not exist in any committed revision");
      }
    }
  }
}
