package io.sirix.index.interval;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexType;
import io.sirix.index.hot.AbstractHOTIndexWriter;
import io.sirix.index.hot.HOTIndexReader;
import io.sirix.index.hot.HOTIndexWriter;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The selective RI-tree shortcuts must see the same postings as the logical HOT scan. */
final class HotOrderedStorePostingTest {
  private static final String RESOURCE = "posting-shortcuts";
  private static final long FORK = 3;
  private static final long ENDPOINT = 5;
  private static final int BASE_POSTINGS = 350;
  private static final long TAIL_BIT = (BASE_POSTINGS - 1) * 3L;
  private static final long[] CHUNK_BASES = {0, 1L << 16, 0x80000000L << 16};
  private static final ValidTimeKey LEFT = new ValidTimeKey(ValidTimeKey.STORE_LOWER, FORK, ENDPOINT);
  private static final ValidTimeKey RIGHT = new ValidTimeKey(ValidTimeKey.STORE_UPPER, FORK, ENDPOINT);

  @TempDir
  Path directory;

  @AfterEach
  void clearCaches() {
    Databases.clearGlobalCaches();
  }

  private record Snapshot(Set<Long> left, Set<Long> right) {
    private Snapshot {
      left = new TreeSet<>(left);
      right = new TreeSet<>(right);
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void shortcutsIncludePendingAddsBeforeTheFirstFold(final VersioningType versioning) {
    final Path databasePath = directory.resolve("db");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));
    final Set<Long> left = new TreeSet<>();
    final Set<Long> right = Set.of((1L << 16) | 141);
    final long deltasBefore = HOTIndexWriter.postingDeltaWrites();
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      assertTrue(database.createResource(
          ResourceConfiguration.newBuilder(RESOURCE).versioningApproach(versioning).storeDiffs(false).build()));
      try (JsonResourceSession session = database.beginResourceSession(RESOURCE);
          JsonNodeTrx trx = session.beginNodeTrx()) {
        final HOTIndexWriter<ValidTimeKey> writer = HOTIndexWriter.create(trx.getStorageEngineWriter(),
            ValidTimeKeySerializer.INSTANCE, IndexType.VALIDTIME, 0);
        for (int chunk = 0; chunk < 2; chunk++) {
          for (int i = 0; i < 48; i++) {
            final long key = ((long) chunk << 16) | (i * 3L);
            writer.indexNodeKey(LEFT, key);
            left.add(key);
          }
        }
        for (final long key : right) {
          writer.indexNodeKey(RIGHT, key);
        }
        assertTrue(HOTIndexWriter.postingDeltaWrites() > deltasBefore);
        final Snapshot snapshot = new Snapshot(left, right);
        assertStores(trx.getStorageEngineWriter(), snapshot);
        trx.commit();
        assertRevisions(session, List.of(snapshot));
      }
    }
    Databases.clearGlobalCaches();
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
      assertRevisions(session, List.of(new Snapshot(left, right)));
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void shortcutsMergeDeltasAndReferencedChunksAtEveryRevision(final VersioningType versioning) {
    final Path databasePath = directory.resolve("db");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));
    final List<Snapshot> snapshots = new ArrayList<>(5);
    final Set<Long> left = new TreeSet<>();
    final Set<Long> right = new TreeSet<>();
    final long foldsBefore = HOTIndexWriter.postingDeltaFolds();
    final long referencesBefore = AbstractHOTIndexWriter.REFERENCED_CHUNK_WRITES.get();
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                                              .versioningApproach(versioning)
                                                              .maxNumberOfRevisionsToRestore(3)
                                                              .storeDiffs(false)
                                                              .build()));
      try (JsonResourceSession session = database.beginResourceSession(RESOURCE);
          JsonNodeTrx trx = session.beginNodeTrx()) {
        for (int phase = 0; phase < 5; phase++) {
          final HOTIndexWriter<ValidTimeKey> writer = HOTIndexWriter.create(trx.getStorageEngineWriter(),
              ValidTimeKeySerializer.INSTANCE, IndexType.VALIDTIME, 0);
          switch (phase) {
            case 0 -> {
              for (final long base : CHUNK_BASES) {
                for (int i = 0; i < BASE_POSTINGS; i++) {
                  final long key = base | (i * 3L);
                  writer.indexNodeKey(LEFT, key);
                  left.add(key);
                }
              }
              // Only the second chunk intersects, including a posting in its pending delta tail.
              for (final long key : new long[] {(1L << 16) | 3, (1L << 16) | TAIL_BIT}) {
                writer.indexNodeKey(RIGHT, key);
                right.add(key);
              }
              writer.indexNodeKey(new ValidTimeKey(ValidTimeKey.STORE_LOWER, FORK, ENDPOINT + 1), 999);
            }
            case 1 -> {
              for (final long base : CHUNK_BASES) {
                for (int i = 0; i < BASE_POSTINGS; i += 5) {
                  final long key = base | (i * 3L);
                  assertTrue(writer.remove(LEFT, key));
                  left.remove(key);
                }
                for (int i = BASE_POSTINGS; i < BASE_POSTINGS + 10; i++) {
                  final long key = base | (i * 3L);
                  writer.indexNodeKey(LEFT, key);
                  left.add(key);
                }
              }
            }
            case 2 -> {
              for (final long key : new TreeSet<>(left)) {
                if ((key >>> 16) == 1) {
                  assertTrue(writer.remove(LEFT, key));
                  left.remove(key);
                }
              }
            }
            case 3 -> {
              final long key = (1L << 16) | TAIL_BIT;
              writer.indexNodeKey(LEFT, key);
              left.add(key);
            }
            case 4 -> {
              for (final long key : left) {
                assertTrue(writer.remove(LEFT, key));
              }
              left.clear();
            }
            default -> throw new AssertionError(phase);
          }
          final Snapshot snapshot = new Snapshot(left, right);
          assertStores(trx.getStorageEngineWriter(), snapshot);
          trx.commit();
          snapshots.add(snapshot);
          try (JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx(snapshots.size())) {
            assertStores(reader.getStorageEngineReader(), snapshot);
          }
        }
      }
      assertTrue(HOTIndexWriter.postingDeltaFolds() > foldsBefore);
      assertTrue(AbstractHOTIndexWriter.REFERENCED_CHUNK_WRITES.get() > referencesBefore);
      try (JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
        assertRevisions(session, snapshots);
      }
    }
    Databases.clearGlobalCaches();
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
      assertRevisions(session, snapshots);
    }
  }

  private static void assertRevisions(final JsonResourceSession session, final List<Snapshot> snapshots) {
    for (int revision = 1; revision <= snapshots.size(); revision++) {
      try (JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx(revision)) {
        assertStores(reader.getStorageEngineReader(), snapshots.get(revision - 1));
      }
    }
  }

  private static void assertStores(final StorageEngineReader storage, final Snapshot snapshot) {
    final HOTIndexReader<ValidTimeKey> reader =
        HOTIndexReader.create(storage, ValidTimeKeySerializer.INSTANCE, IndexType.VALIDTIME, 0);
    final HotOrderedStore left = new HotOrderedStore(ValidTimeKey.STORE_LOWER, null, reader);
    final HotOrderedStore right = new HotOrderedStore(ValidTimeKey.STORE_UPPER, null, reader);
    assertStore(left, snapshot.left);
    assertStore(right, snapshot.right);
    final boolean expectedIntersection = snapshot.left.stream().anyMatch(snapshot.right::contains);
    assertEquals(expectedIntersection, left.intersects(FORK, ENDPOINT, right, FORK, ENDPOINT));
    assertEquals(expectedIntersection, right.intersects(FORK, ENDPOINT, left, FORK, ENDPOINT));
  }

  private static void assertStore(final HotOrderedStore store, final Set<Long> expected) {
    assertEquals(expected.size(), store.cardinality(FORK, ENDPOINT));
    assertEquals(!expected.isEmpty(), store.hasReferences(FORK, ENDPOINT));
    final Set<Long> scanned = new TreeSet<>();
    store.scan(FORK, ENDPOINT, ENDPOINT, scanned::add);
    assertEquals(expected, scanned);
    for (final long base : CHUNK_BASES) {
      final long[] bits = expected.stream()
                                  .filter(key -> (key >>> 16) == (base >>> 16))
                                  .mapToLong(key -> key & 0xFFFFL)
                                  .sorted()
                                  .toArray();
      final NodeReferences chunk = store.chunk(FORK, ENDPOINT, base);
      if (bits.length == 0) {
        assertNull(chunk);
      } else {
        assertNotNull(chunk);
        assertArrayEquals(bits, chunk.toSortedArray());
      }
    }
    assertNull(store.chunk(FORK, ENDPOINT, 2L << 16));
  }
}
