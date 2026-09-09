/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot;

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
import io.sirix.page.HOTIndirectPage;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.interfaces.Page;
import io.sirix.page.PageReference;
import io.sirix.page.interfaces.PageFragmentKey;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Durable revision metadata for copy-on-written HOT leaf images and their fragment chains. */
final class HOTLeafRevisionMetadataTest {

  private static final String RESOURCE = "hot-leaf-revision-metadata";
  private static final int INDEX_NUMBER = 0;
  private static final int SPARSE_INDEX_NUMBER = 73;
  private static final long PATH_KEY = 42L;

  @TempDir
  Path temporaryDirectory;

  @ParameterizedTest(name = "{0}")
  @EnumSource(VersioningType.class)
  void coldWireImagesAndFragmentKeysCarryTheirPhysicalRevision(final VersioningType versioningType) throws IOException {
    final Path databasePath = temporaryDirectory.resolve(versioningType.name().toLowerCase());
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try {
      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
        database.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                                     .versioningApproach(versioningType)
                                                     .maxNumberOfRevisionsToRestore(4)
                                                     .build());
      }

      final int firstRevision = commitPosting(databasePath, 11L);
      final int secondRevision = commitPosting(databasePath, 12L);
      final int thirdRevision = commitPosting(databasePath, 13L);
      final int fourthRevision = removePosting(databasePath, 11L);
      final int fifthRevision = removePosting(databasePath, 12L);
      final int sixthRevision = removePosting(databasePath, 13L);

      // Clear caches before every probe so both the leaf header and the parent PageReference's
      // fragment metadata are decoded from their durable wire images. The posting assertions then
      // use the normal HOT reader, covering newest-wins updates and the final physical tombstone.
      assertColdRevision(databasePath, firstRevision, new long[] {11L});
      switch (versioningType) {
        case FULL -> {
          assertColdRevision(databasePath, secondRevision, new long[] {11L, 12L});
          assertColdRevision(databasePath, thirdRevision, new long[] {11L, 12L, 13L});
          assertColdRevision(databasePath, fourthRevision, new long[] {12L, 13L});
          assertColdRevision(databasePath, fifthRevision, new long[] {13L});
          assertColdRevision(databasePath, sixthRevision, new long[0]);
        }
        case DIFFERENTIAL -> {
          assertColdRevision(databasePath, secondRevision, new long[] {11L, 12L}, firstRevision);
          assertColdRevision(databasePath, thirdRevision, new long[] {11L, 12L, 13L}, firstRevision);
          assertColdRevision(databasePath, fourthRevision, new long[] {12L, 13L}, firstRevision);
          assertColdRevision(databasePath, fifthRevision, new long[] {13L});
          assertColdRevision(databasePath, sixthRevision, new long[0], fifthRevision);
        }
        case INCREMENTAL -> {
          assertColdRevision(databasePath, secondRevision, new long[] {11L, 12L}, firstRevision);
          assertColdRevision(databasePath, thirdRevision, new long[] {11L, 12L, 13L}, secondRevision, firstRevision);
          assertColdRevision(databasePath, fourthRevision, new long[] {12L, 13L}, thirdRevision, secondRevision,
              firstRevision);
          assertColdRevision(databasePath, fifthRevision, new long[] {13L});
          assertColdRevision(databasePath, sixthRevision, new long[0], fifthRevision);
        }
        case SLIDING_SNAPSHOT -> {
          assertColdRevision(databasePath, secondRevision, new long[] {11L, 12L}, firstRevision);
          assertColdRevision(databasePath, thirdRevision, new long[] {11L, 12L, 13L}, secondRevision, firstRevision);
          assertColdRevision(databasePath, fourthRevision, new long[] {12L, 13L}, thirdRevision, secondRevision,
              firstRevision);
          assertColdRevision(databasePath, fifthRevision, new long[] {13L}, fourthRevision, thirdRevision,
              secondRevision);
          assertColdRevision(databasePath, sixthRevision, new long[0], fifthRevision, fourthRevision, thirdRevision);
        }
      }
    } finally {
      Databases.getGlobalBufferManager().clearAllCaches();
      Databases.removeDatabase(databasePath);
    }
  }

  @Test
  void differentialTruthfulAnchorUsesGlobalRevisionDistanceForALeafLocalFullEmit() throws IOException {
    final Path databasePath = temporaryDirectory.resolve("differential-skipped-global-revisions");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try {
      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
        database.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                                     .versioningApproach(VersioningType.DIFFERENTIAL)
                                                     .maxNumberOfRevisionsToRestore(4)
                                                     .build());
      }

      final int fullDumpRevision = commitPosting(databasePath, 11L);
      final int lastUnrelatedRevision = commitEmptyRevisions(databasePath, 8);
      assertEquals(fullDumpRevision + 8, lastUnrelatedRevision,
          "empty commits must advance the global revision without rewriting this HOT leaf");

      // The first post-gap leaf mutation starts a cumulative delta and truthfully anchors it to the
      // old physical full dump. On the next leaf mutation the global-revision distance is already
      // beyond the restore period, so DIFFERENTIAL emits a new full LEAF image. This is deliberately
      // early in leaf-write count, but it is not a subtree/index rebuild: the logical leaf page key
      // remains stable across all three physical images.
      final int postGapDeltaRevision = commitPosting(databasePath, 12L);
      final int rotatedFullRevision = commitPosting(databasePath, 13L);
      assertEquals(lastUnrelatedRevision + 1, postGapDeltaRevision);
      assertEquals(postGapDeltaRevision + 1, rotatedFullRevision);

      final long originalLeafPageKey = assertColdRevision(databasePath, fullDumpRevision, new long[] {11L});
      final long deltaLeafPageKey =
          assertColdRevision(databasePath, postGapDeltaRevision, new long[] {11L, 12L}, fullDumpRevision);
      final long rotatedLeafPageKey = assertColdRevision(databasePath, rotatedFullRevision, new long[] {11L, 12L, 13L});
      assertEquals(originalLeafPageKey, deltaLeafPageKey,
          "starting the differential chain must preserve the logical leaf identity");
      assertEquals(originalLeafPageKey, rotatedLeafPageKey,
          "a cadence full emit must copy one leaf, never rebuild the subtree");
    } finally {
      Databases.getGlobalBufferManager().clearAllCaches();
      Databases.removeDatabase(databasePath);
    }
  }

  @Test
  void sparseSecondaryRootPersistsPhysicalRevisionMetadataInsideAnIndirectPage() throws IOException {
    final Path databasePath = temporaryDirectory.resolve("sparse-indirect-revision-metadata");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try {
      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
        database.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                                     .versioningApproach(VersioningType.INCREMENTAL)
                                                     .maxNumberOfRevisionsToRestore(4)
                                                     .build());
      }

      final int baseRevision = bulkBuildSparsePathIndex(databasePath);
      final int updateRevision = commitPosting(databasePath, SPARSE_INDEX_NUMBER, 0L, 2_000L);

      Databases.getGlobalBufferManager().clearAllCaches();
      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
          JsonResourceSession session = database.beginResourceSession(RESOURCE);
          JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(updateRevision)) {
        final StorageEngineReader reader = rtx.getStorageEngineReader();
        final PageReference sparseRootReference = pathRootReference(reader, SPARSE_INDEX_NUMBER);
        assertInstanceOf(HOTIndirectPage.class, reader.loadHOTPage(sparseRootReference),
            "more than one leaf at a sparse physical index must persist an indirect HOT root");

        final byte[] targetKey = new byte[HOTLongKeySerializer.CHUNKED_SERIALIZED_SIZE];
        assertEquals(HOTLongKeySerializer.CHUNKED_SERIALIZED_SIZE,
            PathKeySerializer.INSTANCE.serializeWithChunkIdx(0L, 0, targetKey, 0));
        final PageReference leafReference = leafReferenceForKey(reader, sparseRootReference, targetKey);
        assertEquals(1, leafReference.getPageFragments().size(),
            "the updated child must carry its base leaf through the indirect child reference");
        assertEquals(baseRevision, leafReference.getPageFragments().getFirst().revision());

        final List<HOTLeafPage> fragments = reader.loadHOTLeafFragments(leafReference);
        try {
          assertEquals(updateRevision, fragments.getFirst().getRevision());
          assertEquals(baseRevision, fragments.get(1).getRevision());
          assertEquals(fragments.get(1).getRevision(), leafReference.getPageFragments().getFirst().revision());
        } finally {
          reader.releaseHOTLeafFragments(fragments, null);
        }

        final HOTLongIndexReader indexReader = HOTLongIndexReader.create(reader, IndexType.PATH, SPARSE_INDEX_NUMBER);
        final var references = indexReader.get(0L, SearchMode.EQUAL);
        assertNotNull(references);
        assertEquals(2, references.getNodeKeys().getLongCardinality());
        assertTrue(references.getNodeKeys().contains(1_000L));
        assertTrue(references.getNodeKeys().contains(2_000L));
      }
    } finally {
      Databases.getGlobalBufferManager().clearAllCaches();
      Databases.removeDatabase(databasePath);
    }
  }

  private static int commitPosting(final Path databasePath, final long nodeKey) throws IOException {
    return commitPosting(databasePath, INDEX_NUMBER, PATH_KEY, nodeKey);
  }

  private static int commitPosting(final Path databasePath, final int indexNumber, final long pathKey,
      final long nodeKey) throws IOException {
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        JsonResourceSession session = database.beginResourceSession(RESOURCE);
        JsonNodeTrx wtx = session.beginNodeTrx()) {
      final HOTLongIndexWriter writer =
          HOTLongIndexWriter.create(wtx.getStorageEngineWriter(), IndexType.PATH, indexNumber);
      writer.indexNodeKey(pathKey, nodeKey);
      wtx.commit();
      return session.getMostRecentRevisionNumber();
    }
  }

  private static int bulkBuildSparsePathIndex(final Path databasePath) throws IOException {
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        JsonResourceSession session = database.beginResourceSession(RESOURCE);
        JsonNodeTrx wtx = session.beginNodeTrx()) {
      final HOTLongIndexWriter writer =
          HOTLongIndexWriter.create(wtx.getStorageEngineWriter(), IndexType.PATH, SPARSE_INDEX_NUMBER);
      final HOTLongBulkIndexLoader loader = writer.createBulkLoader();
      for (long key = 0; key <= HOTLeafPage.MAX_ENTRIES; key++) {
        loader.add(key, 1_000L + key);
      }
      loader.flush();
      wtx.commit();
      return session.getMostRecentRevisionNumber();
    }
  }

  private static int removePosting(final Path databasePath, final long nodeKey) throws IOException {
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        JsonResourceSession session = database.beginResourceSession(RESOURCE);
        JsonNodeTrx wtx = session.beginNodeTrx()) {
      final HOTLongIndexWriter writer =
          HOTLongIndexWriter.create(wtx.getStorageEngineWriter(), IndexType.PATH, INDEX_NUMBER);
      assertTrue(writer.remove(PATH_KEY, nodeKey), "the posting removed by this revision must exist");
      wtx.commit();
      return session.getMostRecentRevisionNumber();
    }
  }

  private static int commitEmptyRevisions(final Path databasePath, final int count) throws IOException {
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
      for (int i = 0; i < count; i++) {
        try (JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.commit();
        }
      }
      return session.getMostRecentRevisionNumber();
    }
  }

  private static long assertColdRevision(final Path databasePath, final int revision, final long[] expectedNodeKeys,
      final int... expectedFragmentRevisions) throws IOException {
    Databases.getGlobalBufferManager().clearAllCaches();
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        JsonResourceSession session = database.beginResourceSession(RESOURCE);
        JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision)) {
      final StorageEngineReader reader = rtx.getStorageEngineReader();
      final PageReference rootReference = pathRootReference(reader);
      final List<PageFragmentKey> fragmentKeys = rootReference.getPageFragments();
      final List<HOTLeafPage> physicalFragments = reader.loadHOTLeafFragments(rootReference);
      final long logicalPageKey;
      try {
        logicalPageKey = physicalFragments.getFirst().getPageKey();
        assertEquals(revision, physicalFragments.getFirst().getRevision(),
            "the durable head leaf must be stamped by the commit that wrote it");
        assertEquals(expectedFragmentRevisions.length, fragmentKeys.size(),
            "unexpected " + revision + " fragment-chain length");
        assertEquals(1 + fragmentKeys.size(), physicalFragments.size());
        for (int i = 0; i < fragmentKeys.size(); i++) {
          final PageFragmentKey fragmentKey = fragmentKeys.get(i);
          assertEquals(expectedFragmentRevisions[i], fragmentKey.revision(),
              "unexpected physical revision at chain position " + i + " in revision " + revision);
          assertEquals(physicalFragments.get(i + 1).getRevision(), fragmentKey.revision(),
              "fragment metadata must describe the physical image at the corresponding offset");
        }
      } finally {
        reader.releaseHOTLeafFragments(physicalFragments, null);
      }

      final HOTLongIndexReader indexReader = HOTLongIndexReader.create(reader, IndexType.PATH, INDEX_NUMBER);
      final var references = indexReader.get(PATH_KEY, SearchMode.EQUAL);
      if (expectedNodeKeys.length == 0) {
        assertNull(references, "the final posting tombstone must suppress every historical value");
      } else {
        assertNotNull(references);
        assertEquals(expectedNodeKeys.length, references.getNodeKeys().getLongCardinality());
        for (final long expectedNodeKey : expectedNodeKeys) {
          assertTrue(references.getNodeKeys().contains(expectedNodeKey), "missing nodeKey " + expectedNodeKey);
        }
      }
      return logicalPageKey;
    }
  }

  private static PageReference pathRootReference(final StorageEngineReader reader) {
    return pathRootReference(reader, INDEX_NUMBER);
  }

  private static PageReference pathRootReference(final StorageEngineReader reader, final int indexNumber) {
    final PageReference reference =
        reader.getPathPage(reader.getActualRevisionRootPage()).getIndexReference(indexNumber);
    assertNotNull(reference);
    return reference;
  }

  private static PageReference leafReferenceForKey(final StorageEngineReader reader, final PageReference rootReference,
      final byte[] serializedKey) {
    PageReference reference = rootReference;
    while (true) {
      final Page page = reader.loadHOTPage(reference);
      if (page instanceof HOTLeafPage leaf) {
        assertTrue(leaf.findEntry(serializedKey) >= 0, "the routed leaf must contain the target key");
        return reference;
      }

      final HOTIndirectPage indirect = assertInstanceOf(HOTIndirectPage.class, page);
      final int childIndex = indirect.findChildIndex(serializedKey);
      assertTrue(childIndex >= 0, "the indirect page must route the target key");
      reference = indirect.getChildReference(childIndex);
      assertNotNull(reference, "the routed child reference must exist");
    }
  }
}
