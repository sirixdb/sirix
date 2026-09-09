/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot;

import io.brackit.query.atomic.QNm;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.exception.SirixIOException;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.IndexType;
import io.sirix.index.SearchMode;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.page.HOTLeafPage;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Regression coverage for the one operation-coded foreground HOT mutation route. */
final class HOTSharedMutationRouteTest {

  private static final String RESOURCE = "shared-hot-mutation-route";
  private static final int PATH_INDEX_NUMBER = 0;
  private static final int NAME_INDEX_NUMBER = IndexDefs.createNameIdxDef(0, IndexDef.DbType.JSON).getID();
  private static final QNm NAME_KEY = new QNm("shared-name");
  private static final QNm GROWTH_NAME_KEY = new QNm("representation-boundary-name");
  private static final long PATH_KEY = 42L;
  private static final long GROWTH_PATH_KEY = 84L;

  @TempDir
  Path temporaryDirectory;

  @ParameterizedTest(name = "{0}")
  @EnumSource(VersioningType.class)
  void bothPostingWritersShareIncrementalAddRemoveAndTombstoneSemantics(final VersioningType versioningType)
      throws IOException {
    final Path databasePath = temporaryDirectory.resolve(versioningType.name().toLowerCase());
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try {
      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
        database.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                                     .versioningApproach(versioningType)
                                                     .maxNumberOfRevisionsToRestore(4)
                                                     .build());
      }

      final int[] revisions = new int[8];
      revisions[0] = commitMutation(databasePath, (wtx, nameWriter, pathWriter) -> {
        nameWriter.indexNodeKey(NAME_KEY, 1L);
        nameWriter.indexNodeKey(NAME_KEY, 2L);
        nameWriter.indexNodeKey(NAME_KEY, (1L << 16) + 3L);
        pathWriter.indexNodeKey(PATH_KEY, 11L);
        pathWriter.indexNodeKey(PATH_KEY, 12L);
        pathWriter.indexNodeKey(PATH_KEY, (2L << 16) + 3L);
        for (long nodeKey = 1_000L; nodeKey <= 1_064L; nodeKey++) {
          nameWriter.indexNodeKey(GROWTH_NAME_KEY, nodeKey);
          pathWriter.indexNodeKey(GROWTH_PATH_KEY, nodeKey);
        }
      });

      final long validationFailuresBefore = AbstractHOTIndexWriter.STRUCTURAL_VALIDATION_FAILURE.get();
      revisions[1] = commitMutation(databasePath, (wtx, nameWriter, pathWriter) -> {
        // An absent posting is a read-only preflight: no index spine or leaf is copied into the TIL.
        final int logEntriesBeforeMisses = wtx.getStorageEngineWriter().getLog().liveEntryCount();
        assertFalse(nameWriter.remove(NAME_KEY, 404L));
        assertFalse(nameWriter.remove(new QNm("missing-name"), 1L));
        assertFalse(pathWriter.remove(PATH_KEY, 404L));
        assertFalse(pathWriter.remove(Long.MIN_VALUE, 1L));
        assertEquals(logEntriesBeforeMisses, wtx.getStorageEngineWriter().getLog().liveEntryCount(),
            "no-op deletes must not copy-on-write HOT pages");

        assertTrue(nameWriter.remove(NAME_KEY, 2L));
        assertEquals("h:remove-posting-bit", nameWriter.lastDispatchHandler,
            "generic writer remove must enter the shared operation-coded route");
        assertPosting(nameWriter.get(NAME_KEY, SearchMode.EQUAL), 1L, (1L << 16) + 3L);

        assertTrue(pathWriter.remove(PATH_KEY, 12L));
        assertEquals("h:remove-posting-bit", pathWriter.lastDispatchHandler,
            "primitive writer remove must enter the shared operation-coded route");
        assertPosting(pathWriter.get(PATH_KEY, SearchMode.EQUAL), 11L, (2L << 16) + 3L);

        // 65 entries use Roaring while 64 use the packed representation. The replacement grows even
        // though a bit was removed; this used to ignore updateValue(false) and report a delete that
        // never happened. Both public writers must take the shared growth-safe replacement arm.
        assertTrue(nameWriter.remove(GROWTH_NAME_KEY, 1_000L));
        assertPostingRange(nameWriter.get(GROWTH_NAME_KEY, SearchMode.EQUAL), 1_001L, 1_064L);
        assertTrue(pathWriter.remove(GROWTH_PATH_KEY, 1_000L));
        assertPostingRange(pathWriter.get(GROWTH_PATH_KEY, SearchMode.EQUAL), 1_001L, 1_064L);
      });

      revisions[2] = commitMutation(databasePath, (wtx, nameWriter, pathWriter) -> {
        // Persist the chunk's final-bit removal as its own revision. The independent sibling chunk
        // must remain visible while version reconstruction suppresses the older chunk value.
        assertTrue(nameWriter.remove(NAME_KEY, 1L));
        assertTrue(pathWriter.remove(PATH_KEY, 11L));
      });

      // Force the re-add to start from a cold, independently committed tombstone revision.
      Databases.getGlobalBufferManager().clearAllCaches();
      assertRevision(databasePath, revisions[2], new long[] {(1L << 16) + 3L}, new long[] {(2L << 16) + 3L}, 1_001L);

      revisions[3] = commitMutation(databasePath, (wtx, nameWriter, pathWriter) -> {
        nameWriter.indexNodeKey(NAME_KEY, 9L);
        pathWriter.indexNodeKey(PATH_KEY, 19L);
      });

      // Cross the four-fragment restore window on the same logical chunk. Historical cold reads
      // below pin every intermediate result, not merely the endpoints.
      revisions[4] = commitMutation(databasePath, (wtx, nameWriter, pathWriter) -> {
        nameWriter.indexNodeKey(NAME_KEY, 20L);
        pathWriter.indexNodeKey(PATH_KEY, 29L);
      });
      revisions[5] = commitMutation(databasePath, (wtx, nameWriter, pathWriter) -> {
        assertTrue(nameWriter.remove(NAME_KEY, 9L));
        assertTrue(pathWriter.remove(PATH_KEY, 19L));
      });
      revisions[6] = commitMutation(databasePath, (wtx, nameWriter, pathWriter) -> {
        nameWriter.indexNodeKey(NAME_KEY, 30L);
        pathWriter.indexNodeKey(PATH_KEY, 39L);
      });
      revisions[7] = commitMutation(databasePath, (wtx, nameWriter, pathWriter) -> {
        assertTrue(nameWriter.remove(NAME_KEY, 20L));
        assertTrue(pathWriter.remove(PATH_KEY, 29L));
      });

      assertEquals(validationFailuresBefore, AbstractHOTIndexWriter.STRUCTURAL_VALIDATION_FAILURE.get(),
          "ordinary posting add/remove operations must publish only invariant-clean frontiers");

      assertRevision(databasePath, revisions[0], new long[] {1L, 2L, (1L << 16) + 3L},
          new long[] {11L, 12L, (2L << 16) + 3L}, 1_000L);
      assertRevision(databasePath, revisions[1], new long[] {1L, (1L << 16) + 3L}, new long[] {11L, (2L << 16) + 3L},
          1_001L);
      assertRevision(databasePath, revisions[2], new long[] {(1L << 16) + 3L}, new long[] {(2L << 16) + 3L}, 1_001L);
      assertRevision(databasePath, revisions[3], new long[] {9L, (1L << 16) + 3L}, new long[] {19L, (2L << 16) + 3L},
          1_001L);
      assertRevision(databasePath, revisions[4], new long[] {9L, 20L, (1L << 16) + 3L},
          new long[] {19L, 29L, (2L << 16) + 3L}, 1_001L);
      assertRevision(databasePath, revisions[5], new long[] {20L, (1L << 16) + 3L}, new long[] {29L, (2L << 16) + 3L},
          1_001L);
      assertRevision(databasePath, revisions[6], new long[] {20L, 30L, (1L << 16) + 3L},
          new long[] {29L, 39L, (2L << 16) + 3L}, 1_001L);
      assertRevision(databasePath, revisions[7], new long[] {30L, (1L << 16) + 3L}, new long[] {39L, (2L << 16) + 3L},
          1_001L);
    } finally {
      Databases.getGlobalBufferManager().clearAllCaches();
      Databases.removeDatabase(databasePath);
    }
  }

  private int commitMutation(final Path databasePath, final PostingMutation mutation) throws IOException {
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        JsonResourceSession session = database.beginResourceSession(RESOURCE);
        JsonNodeTrx wtx = session.beginNodeTrx()) {
      final HOTIndexWriter<QNm> nameWriter = HOTIndexWriter.create(wtx.getStorageEngineWriter(),
          NameKeySerializer.INSTANCE, IndexType.NAME, NAME_INDEX_NUMBER);
      final HOTLongIndexWriter pathWriter =
          HOTLongIndexWriter.create(wtx.getStorageEngineWriter(), IndexType.PATH, PATH_INDEX_NUMBER);
      mutation.apply(wtx, nameWriter, pathWriter);
      wtx.commit();
      return session.getMostRecentRevisionNumber();
    }
  }

  private void assertRevision(final Path databasePath, final int revision, final long[] expectedNamePostings,
      final long[] expectedPathPostings, final long growthFromInclusive) throws IOException {
    Databases.getGlobalBufferManager().clearAllCaches();
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        JsonResourceSession session = database.beginResourceSession(RESOURCE);
        JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision)) {
      final HOTIndexReader<QNm> nameReader = HOTIndexReader.create(rtx.getStorageEngineReader(),
          NameKeySerializer.INSTANCE, IndexType.NAME, NAME_INDEX_NUMBER);
      final HOTLongIndexReader pathReader =
          HOTLongIndexReader.create(rtx.getStorageEngineReader(), IndexType.PATH, PATH_INDEX_NUMBER);
      assertPosting(nameReader.get(NAME_KEY, SearchMode.EQUAL), expectedNamePostings);
      assertPosting(pathReader.get(PATH_KEY, SearchMode.EQUAL), expectedPathPostings);
      assertPostingRange(nameReader.get(GROWTH_NAME_KEY, SearchMode.EQUAL), growthFromInclusive, 1_064L);
      assertPostingRange(pathReader.get(GROWTH_PATH_KEY, SearchMode.EQUAL), growthFromInclusive, 1_064L);
    }
  }

  @FunctionalInterface
  private interface PostingMutation {
    void apply(JsonNodeTrx wtx, HOTIndexWriter<QNm> nameWriter, HOTLongIndexWriter pathWriter);
  }

  @Test
  void malformedPostingFailsClosedAndPoisonsTheTransaction() throws IOException {
    final byte[] unsortedPacked =
        NodeReferencesSerializer.serialize(NodeReferences.ofSortedArray(new long[] {10L, 30L}));
    for (int i = 0; i < Long.BYTES; i++) {
      final byte first = unsortedPacked[2 + i];
      unsortedPacked[2 + i] = unsortedPacked[2 + Long.BYTES + i];
      unsortedPacked[2 + Long.BYTES + i] = first;
    }
    assertMalformedPostingPoisonsTransaction(new byte[0], "zero-length");
    assertMalformedPostingPoisonsTransaction(new byte[] {(byte) 0xFE, 0x55}, "tombstone-with-trailing-bytes");
    assertMalformedPostingPoisonsTransaction(unsortedPacked, "unsorted-packed");
    assertMalformedPostingPoisonsTransaction(
        NodeReferencesSerializer.serialize(NodeReferences.ofSortedArray(new long[] {1L << 16})), "out-of-range-packed");

    final NodeReferences outOfRangeRoaring = new NodeReferences();
    for (long bit16 = 0; bit16 <= 64; bit16++) {
      outOfRangeRoaring.addNodeKey(bit16);
    }
    outOfRangeRoaring.addNodeKey(1L << 16);
    assertMalformedPostingPoisonsTransaction(NodeReferencesSerializer.serialize(outOfRangeRoaring),
        "out-of-range-roaring");
  }

  private void assertMalformedPostingPoisonsTransaction(final byte[] malformedPayload, final String caseName)
      throws IOException {
    final Path databasePath = temporaryDirectory.resolve("corrupt-posting-" + caseName);
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try {
      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
        database.createResource(ResourceConfiguration.newBuilder(RESOURCE).build());
      }

      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
          JsonResourceSession session = database.beginResourceSession(RESOURCE);
          JsonNodeTrx wtx = session.beginNodeTrx()) {
        final HOTIndexWriter<QNm> writer = HOTIndexWriter.create(wtx.getStorageEngineWriter(),
            NameKeySerializer.INSTANCE, IndexType.NAME, NAME_INDEX_NUMBER);
        writer.indexNodeKey(NAME_KEY, 7L);

        final byte[] compositeBuffer = new byte[512];
        final int compositeLength = NameKeySerializer.INSTANCE.serializeWithChunkIdx(NAME_KEY, 0, compositeBuffer, 0);
        final byte[] compositeKey = Arrays.copyOf(compositeBuffer, compositeLength);
        final HOTLeafPage leaf = writer.acquireLeafForRead(compositeKey);
        assertNotNull(leaf);
        try {
          final int slot = leaf.findEntry(compositeKey);
          assertTrue(slot >= 0);
          assertTrue(leaf.updateValue(slot, malformedPayload), "fixture must install the malformed posting payload");
        } finally {
          leaf.releaseGuard();
        }

        final RuntimeException corruption = assertThrows(RuntimeException.class, () -> writer.remove(NAME_KEY, 7L));
        final SirixIOException commitFailure = assertThrows(SirixIOException.class, wtx::commit);
        assertSame(corruption, commitFailure.getCause(),
            "stable posting corruption must remain the rollback-only cause for " + caseName);
        wtx.rollback();
      }
    } finally {
      Databases.getGlobalBufferManager().clearAllCaches();
      Databases.removeDatabase(databasePath);
    }
  }

  private static void assertPosting(final NodeReferences references, final long... expectedNodeKeys) {
    assertNotNull(references);
    assertEquals(expectedNodeKeys.length, references.getNodeKeys().getLongCardinality());
    for (final long expectedNodeKey : expectedNodeKeys) {
      assertTrue(references.getNodeKeys().contains(expectedNodeKey), "missing nodeKey " + expectedNodeKey);
    }
  }

  private static void assertPostingRange(final NodeReferences references, final long fromInclusive,
      final long toInclusive) {
    assertNotNull(references);
    assertEquals(toInclusive - fromInclusive + 1L, references.getNodeKeys().getLongCardinality());
    for (long nodeKey = fromInclusive; nodeKey <= toInclusive; nodeKey++) {
      assertTrue(references.getNodeKeys().contains(nodeKey), "missing nodeKey " + nodeKey);
    }
  }
}
