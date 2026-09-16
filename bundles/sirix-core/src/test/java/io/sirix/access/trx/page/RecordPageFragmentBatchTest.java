/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access.trx.page;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexType;
import io.sirix.io.StorageType;
import io.sirix.node.interfaces.DataRecord;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.PageReference;
import io.sirix.page.interfaces.KeyValuePage;
import io.sirix.page.interfaces.PageFragmentKey;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The batched fragment-chain load must hand every consumer exactly the fragments the per-fragment
 * loop handed it, in the same (newest-first) order, on cold caches and on a cache-hit/miss mix — and
 * every value of every revision must come out unchanged.
 */
final class RecordPageFragmentBatchTest {
  private static final int VALUES = 2_600;
  private static final int REVISIONS = 7;
  private static final int[] STRIDES = {0, 3, 5, 7, 2, 11, 13};

  @TempDir
  Path directory;

  @ParameterizedTest(name = "{0}")
  @EnumSource(value = VersioningType.class, names = {"SLIDING_SNAPSHOT", "INCREMENTAL", "DIFFERENTIAL"})
  void chainsLoadIdenticallyColdAndOnACacheHitMissMix(final VersioningType versioning) {
    final Path path = directory.resolve("fragments-" + versioning.name().toLowerCase());
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(path)));
    final long[] nodeKeys = new long[VALUES];
    final String[][] expected = new String[REVISIONS + 1][VALUES];
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource")
                                                              .versioningApproach(versioning)
                                                              .maxNumberOfRevisionsToRestore(4)
                                                              .storageType(StorageType.FILE_CHANNEL)
                                                              .build()));
      try (JsonResourceSession session = database.beginResourceSession("resource")) {
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          writer.insertArrayAsFirstChild();
          final long arrayKey = writer.getNodeKey();
          for (int i = 0; i < VALUES; i++) {
            writer.moveTo(arrayKey);
            expected[1][i] = "v1-" + i;
            nodeKeys[i] = writer.insertStringValueAsFirstChild(expected[1][i]).getNodeKey();
          }
          writer.commit();
        }
        // Six more revisions, each rewriting a different scattered subset: every record page ends
        // up with a chain of fragments from several revisions.
        for (int revision = 2; revision <= REVISIONS; revision++) {
          System.arraycopy(expected[revision - 1], 0, expected[revision], 0, VALUES);
          try (JsonNodeTrx writer = session.beginNodeTrx()) {
            for (int i = revision % STRIDES[revision - 1]; i < VALUES; i += STRIDES[revision - 1]) {
              assertTrue(writer.moveTo(nodeKeys[i]));
              expected[revision][i] = "v" + revision + "-" + i;
              writer.setStringValue(expected[revision][i]);
            }
            writer.commit();
          }
        }
      }
    }

    // Cold: every process-local cache dropped, every chain read from file offsets in one batch.
    Databases.clearGlobalCaches();
    final List<List<long[]>> coldChains = new ArrayList<>();
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path);
        JsonResourceSession session = database.beginResourceSession("resource")) {
      for (int revision = 1; revision <= REVISIONS; revision++) {
        try (JsonNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx(revision)) {
          assertValues(trx, nodeKeys, expected[revision]);
          coldChains.add(chains(trx, nodeKeys, revision));
        }
      }
    }
    assertTrue(coldChains.get(REVISIONS - 1).stream().anyMatch(chain -> chain.length > 2),
        "the fixture must produce multi-fragment chains at the latest revision");

    // Mixed: revision 5 first, so its fragments are resident; revision 7's chains then share some of
    // them (hits) and miss the two newer ones — the load must be indistinguishable from the cold one.
    Databases.clearGlobalCaches();
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path);
        JsonResourceSession session = database.beginResourceSession("resource")) {
      try (JsonNodeReadOnlyTrx warm = session.beginNodeReadOnlyTrx(5)) {
        assertValues(warm, nodeKeys, expected[5]);
        assertChainsEqual(coldChains.get(4), chains(warm, nodeKeys, 5));
      }
      try (JsonNodeReadOnlyTrx mixed = session.beginNodeReadOnlyTrx(REVISIONS)) {
        assertChainsEqual(coldChains.get(REVISIONS - 1), chains(mixed, nodeKeys, REVISIONS));
        assertValues(mixed, nodeKeys, expected[REVISIONS]);
      }
      // Reading the same revision again while everything is resident: all hits, same answer.
      try (JsonNodeReadOnlyTrx hot = session.beginNodeReadOnlyTrx(REVISIONS)) {
        assertChainsEqual(coldChains.get(REVISIONS - 1), chains(hot, nodeKeys, REVISIONS));
      }
      // The writer's own read-back of chained pages (copy-on-write) must see the same values.
      try (JsonNodeTrx writer = session.beginNodeTrx()) {
        for (int i = 4; i < VALUES; i += 17) {
          assertTrue(writer.moveTo(nodeKeys[i]));
          assertEquals(expected[REVISIONS][i], writer.getValue());
          writer.setStringValue("v8-" + i);
        }
        writer.commit();
      }
    }
    Databases.clearGlobalCaches();
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path);
        JsonResourceSession session = database.beginResourceSession("resource");
        JsonNodeReadOnlyTrx trx = session.beginNodeReadOnlyTrx()) {
      assertEquals(REVISIONS + 1, trx.getRevisionNumber());
      for (int i = 0; i < VALUES; i++) {
        assertTrue(trx.moveTo(nodeKeys[i]));
        assertEquals((i - 4) % 17 == 0 && i >= 4
            ? "v8-" + i
            : expected[REVISIONS][i], trx.getValue(), "node " + i);
      }
    }
  }

  private static void assertValues(final JsonNodeReadOnlyTrx trx, final long[] nodeKeys, final String[] expected) {
    for (int i = 0; i < nodeKeys.length; i++) {
      assertTrue(trx.moveTo(nodeKeys[i]), "node " + i);
      assertEquals(expected[i], trx.getValue(), "node " + i);
    }
  }

  /**
   * For every record page the values live on: the loaded chain as (revision, storage key) pairs,
   * newest first, exactly as {@code getPageFragments} hands it to the versioning combine.
   */
  private static List<long[]> chains(final JsonNodeReadOnlyTrx trx, final long[] nodeKeys, final int revision) {
    final NodeStorageEngineReader reader = (NodeStorageEngineReader) trx.getStorageEngineReader();
    final List<long[]> chains = new ArrayList<>();
    long previousPageKey = -1;
    for (final long nodeKey : nodeKeys) {
      final long pageKey = reader.pageKey(nodeKey, IndexType.DOCUMENT);
      if (pageKey == previousPageKey) {
        continue;
      }
      previousPageKey = pageKey;
      final PageReference reference = reader.getLeafPageReference(pageKey, 0, IndexType.DOCUMENT);
      assertNotNull(reference);
      final NodeStorageEngineReader.PageFragmentsResult result = reader.getPageFragments(reference);
      try {
        final List<KeyValuePage<DataRecord>> pages = result.pages();
        assertFalse(pages.isEmpty());
        assertEquals(reference.getKey(), result.storageKeyForFirstFragment());
        assertEquals(reference.getPageFragments(), result.originalKeys());
        final long[] chain = new long[pages.size() * 2];
        int previousRevision = Integer.MAX_VALUE;
        for (int i = 0; i < pages.size(); i++) {
          final KeyValueLeafPage page = (KeyValueLeafPage) pages.get(i);
          assertFalse(page.isClosed());
          assertTrue(page.getRevision() <= revision, "a fragment newer than the transaction's revision");
          assertTrue(page.getRevision() < previousRevision, "fragments must be strictly newest-first");
          previousRevision = page.getRevision();
          chain[2 * i] = page.getRevision();
          chain[2 * i + 1] = i == 0
              ? reference.getKey()
              : keyOf(reference.getPageFragments(), page.getRevision());
        }
        chains.add(chain);
      } finally {
        for (final KeyValuePage<DataRecord> page : result.pages()) {
          ((KeyValueLeafPage) page).releaseGuard();
        }
      }
    }
    assertTrue(chains.size() > 1, "the fixture must span several record pages");
    return chains;
  }

  private static long keyOf(final List<PageFragmentKey> fragments, final int revision) {
    for (final PageFragmentKey fragment : fragments) {
      if (fragment.revision() == revision) {
        return fragment.key();
      }
    }
    throw new AssertionError("no fragment key for revision " + revision + " in " + fragments);
  }

  /** {@code assertEquals} on lists of primitive arrays compares by identity; compare contents. */
  private static void assertChainsEqual(final List<long[]> expected, final List<long[]> actual) {
    assertEquals(expected.size(), actual.size(), "page count");
    for (int i = 0; i < expected.size(); i++) {
      assertArrayEquals(expected.get(i), actual.get(i), "chain of page " + i);
    }
  }
}
