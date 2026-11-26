package io.sirix.index.hot;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexType;
import io.sirix.index.hot.page.HOTIndirectPage;
import io.sirix.index.hot.page.HOTLeafPage;
import io.sirix.page.PageReference;
import io.sirix.page.interfaces.Page;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Issue 1457: local split integration must retain its outer spine and exact heights (FMT-306). */
@Isolated
final class HOTSubInsertSpineTest {
  private static final HOTKeySerializer<Long> SERIALIZER = new HOTKeySerializer<>() {
    @Override
    public int serialize(final Long value, final byte[] destination, final int offset) {
      for (int i = 0; i < 8; i++) {
        destination[offset + i] = 0;
        destination[offset + 8 + i] = (byte) (value >>> (56 - i * 8));
      }
      return 16;
    }

    @Override
    public int maxSerializedLength(final Long value) {
      return 16;
    }

    @Override
    public Long deserialize(final byte[] bytes, final int offset, final int length) {
      long value = 0;
      for (int i = 8; i < 16; i++) {
        value = (value << 8) | (bytes[offset + i] & 255);
      }
      return value;
    }
  };

  @TempDir
  Path directory;

  @AfterEach
  void clearCaches() {
    Databases.clearGlobalCaches();
  }

  @ParameterizedTest
  @CsvSource({"2048,false", "2048,true", "4096,false", "4096,true"})
  void sequentialLeavesFoldIntoOneBlockAtCommit(final int count, final boolean descending) {
    final List<Long> keys = new ArrayList<>(count);
    for (long i = 0; i < count; i++) {
      keys.add(i);
    }
    if (descending) {
      Collections.reverse(keys);
    }
    exercise(keys, 1, count / HOTLeafPage.MAX_ENTRIES);
  }

  @Test
  void commitNormalizesEachDirtyIndexRootIndependently() {
    final Path databasePath = directory.resolve("multiple-indexes");
    final List<Long> keys = new ArrayList<>(2048);
    for (long key = 2047; key >= 0; key--) {
      keys.add(key);
    }
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("spine").storeDiffs(false).build()));
      try (JsonResourceSession session = database.beginResourceSession("spine");
          JsonNodeTrx transaction = session.beginNodeTrx()) {
        for (int index = 0; index < 2; index++) {
          final HOTIndexWriter<Long> writer =
              HOTIndexWriter.create(transaction.getStorageEngineWriter(), SERIALIZER, IndexType.CAS, index);
          for (final long key : keys) {
            writer.indexNodeKey(key, 1);
          }
          verify(writer.getRootReference(), transaction.getStorageEngineWriter(), keys, 9, 4);
        }
        transaction.commit();
      }
    }
    Databases.clearGlobalCaches();
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        JsonResourceSession session = database.beginResourceSession("spine");
        JsonNodeReadOnlyTrx transaction = session.beginNodeReadOnlyTrx()) {
      for (int index = 0; index < 2; index++) {
        final HOTIndexReader<Long> reader =
            HOTIndexReader.create(transaction.getStorageEngineReader(), SERIALIZER, IndexType.CAS, index);
        verify(reader.getRootReference(), transaction.getStorageEngineReader(), keys, 1, 4);
      }
    }
  }

  @Test
  void localInsertionRetainsNonzeroOuterChildSlots() {
    final List<Long> keys = new ArrayList<>(4608);
    for (long key = 0; key < 512; key++) {
      keys.add(key);
    }
    for (long key = 8191; key >= 4096; key--) {
      keys.add(key);
    }
    // A left sibling exists before the descending subtree grows. Losing the outer slot index
    // folds its replacement into that sibling instead; exhaustive routing and set equality reject it.
    exercise(keys, 7, -1);
  }

  @Tag("heavy")
  @ParameterizedTest
  @ValueSource(longs = {5, 6})
  void skewedDescendingMaintainsExactAncestorHeights(final long seed) {
    final Set<Long> keys = new TreeSet<>();
    final Random random = new Random(seed);
    for (int i = 0; keys.size() < 50_000; i++) {
      keys.add(i < 45_000
          ? (long) i
          : random.nextLong() & Long.MAX_VALUE);
    }
    final List<Long> descending = new ArrayList<>(keys);
    Collections.reverse(descending);
    // Known remaining gap (https://github.com/sirixdb/sirix/issues/1457): these fixed leaf
    // sets admit height 2. Commit normalization improves live 9 to at most 3; retain the routing,
    // metadata,
    // persistence and work bound while the remaining frontier shapes are investigated separately.
    exercise(descending, 3, -1);
  }

  @Test
  @Tag("heavy")
  void structuralInsertionRearmsAnUnchangedHeightCommittedSpine() {
    final Set<Long> initial = new TreeSet<>();
    final Random random = new Random(5);
    for (int i = 0; initial.size() < 50_000; i++) {
      initial.add(i < 45_000
          ? (long) i
          : random.nextLong() & Long.MAX_VALUE);
    }
    final List<Long> keys = new ArrayList<>(initial);
    Collections.reverse(keys);
    final Path path = directory.resolve("rearm-committed-spine");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(path)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      assertTrue(database.createResource(
          ResourceConfiguration.newBuilder("spine").versioningApproach(VersioningType.FULL).storeDiffs(false).build()));
      try (JsonResourceSession session = database.beginResourceSession("spine");
          JsonNodeTrx transaction = session.beginNodeTrx()) {
        HOTIndexWriter<Long> writer =
            HOTIndexWriter.create(transaction.getStorageEngineWriter(), SERIALIZER, IndexType.CAS, 0);
        for (final long key : keys)
          writer.indexNodeKey(key, 1);
        transaction.commit();
        boolean witnessedLocalSplit = false;
        for (long key = 49_095; key >= 45_000 && !witnessedLocalSplit; key--) {
          final StorageEngineWriter engine = transaction.getStorageEngineWriter();
          writer = HOTIndexWriter.create(engine, SERIALIZER, IndexType.CAS, 0);
          final HOTIndirectPage before = (HOTIndirectPage) engine.loadHOTPage(writer.getRootReference());
          assertFalse(before.needsNormalization(), "the previous commit cleared this outer spine");
          final int height = before.getHeight();
          final int leaves = countLeaves(writer.getRootReference(), engine);
          final long localInsertions = AbstractHOTIndexWriter.DIRECTION_ONE_SUBINSERT.get();
          writer.indexNodeKey(key, 1);
          keys.add(key);
          final HOTIndirectPage after = (HOTIndirectPage) engine.loadHOTPage(writer.getRootReference());
          final boolean split = countLeaves(writer.getRootReference(), engine) > leaves;
          if (split && after.getHeight() == height) {
            assertTrue(after.needsNormalization(), "a structural change below an unchanged maximum must re-arm commit");
            witnessedLocalSplit = AbstractHOTIndexWriter.DIRECTION_ONE_SUBINSERT.get() > localInsertions;
          }
          if (after.needsNormalization())
            transaction.commit();
        }
        assertTrue(witnessedLocalSplit, "exercise a subtree-local split below an unchanged-height committed root");
        writer = HOTIndexWriter.create(transaction.getStorageEngineWriter(), SERIALIZER, IndexType.CAS, 0);
        verify(writer.getRootReference(), transaction.getStorageEngineWriter(), keys, 3, -1);
        transaction.commit();
      }
    }
  }

  private static int countLeaves(final PageReference reference, final StorageEngineReader reader) {
    final Page page = reader.loadHOTPage(reference);
    if (page instanceof HOTLeafPage)
      return 1;
    final HOTIndirectPage indirect = (HOTIndirectPage) page;
    int count = 0;
    for (int slot = 0; slot < indirect.getNumChildren(); slot++) {
      count += countLeaves(indirect.getChildReference(slot), reader);
    }
    return count;
  }

  private void exercise(final List<Long> keys, final int maximumHeight, final int expectedLeaves) {
    final Path databasePath = directory.resolve("db");
    final List<Long> nextRevisionKeys = new ArrayList<>(keys);
    long extraKey = keys.size();
    while (nextRevisionKeys.contains(extraKey)) {
      extraKey++;
    }
    nextRevisionKeys.add(extraKey);
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      assertTrue(database.createResource(
          ResourceConfiguration.newBuilder("spine").versioningApproach(VersioningType.FULL).storeDiffs(false).build()));
      try (JsonResourceSession session = database.beginResourceSession("spine");
          JsonNodeTrx transaction = session.beginNodeTrx()) {
        final HOTIndexWriter<Long> writer =
            HOTIndexWriter.create(transaction.getStorageEngineWriter(), SERIALIZER, IndexType.CAS, 0);
        for (final long key : keys) {
          writer.indexNodeKey(key, 1);
        }
        verify(writer.getRootReference(), transaction.getStorageEngineWriter(), keys, Math.max(9, maximumHeight),
            expectedLeaves);
        transaction.commit();
        final HOTIndexWriter<Long> nextWriter =
            HOTIndexWriter.create(transaction.getStorageEngineWriter(), SERIALIZER, IndexType.CAS, 0);
        nextWriter.indexNodeKey(keys.getFirst(), 2); // value-only CoW must not re-arm a normalized spine
        final HOTIndirectPage unchanged =
            (HOTIndirectPage) transaction.getStorageEngineWriter().loadHOTPage(nextWriter.getRootReference());
        assertFalse(unchanged.needsNormalization());
        nextWriter.indexNodeKey(extraKey, 1);
        verify(nextWriter.getRootReference(), transaction.getStorageEngineWriter(), nextRevisionKeys,
            Math.max(9, maximumHeight), -1);
        transaction.commit();
      }
    }
    Databases.clearGlobalCaches();
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath);
        JsonResourceSession session = database.beginResourceSession("spine")) {
      for (int revision = 1; revision <= 2; revision++) {
        try (JsonNodeReadOnlyTrx transaction = session.beginNodeReadOnlyTrx(revision)) {
          final HOTIndexReader<Long> reader =
              HOTIndexReader.create(transaction.getStorageEngineReader(), SERIALIZER, IndexType.CAS, 0);
          verify(reader.getRootReference(), transaction.getStorageEngineReader(), revision == 1
              ? keys
              : nextRevisionKeys, maximumHeight,
              revision == 1
                  ? expectedLeaves
                  : -1);
        }
      }
    }
  }

  private static void verify(final PageReference root, final StorageEngineReader reader, final List<Long> keys,
      final int maximumHeight, final int expectedLeaves) {
    final Set<Long> actual = new TreeSet<>();
    final List<String> heightErrors = new ArrayList<>();
    final int[] leaves = {0};
    final int height = inspect(root, root, reader, actual, heightErrors, leaves, 0);
    assertEquals(new TreeSet<>(keys), actual, "every input key occurs exactly once");
    if (expectedLeaves >= 0) {
      assertEquals(expectedLeaves, leaves[0], "leaf packing remains four/eight full leaves");
    }
    System.out.printf("HOT spine keys=%d leaves=%d height=%d metadataErrors=%s%n", keys.size(), leaves[0], height,
        heightErrors);
    assertEquals(List.of(), heightErrors, "stored heights must equal the walked structure");
    assertTrue(height <= maximumHeight, "structural height exceeds this workload's bound: " + height);
  }

  private static int inspect(final PageReference reference, final PageReference root, final StorageEngineReader reader,
      final Set<Long> keys, final List<String> heightErrors, final int[] leaves, final int depth) {
    assertTrue(depth < 64, "acyclic bounded descent");
    final Page page = reader.loadHOTPage(reference);
    assertNotNull(page);
    assertFalse(page.isClosed());
    if (page instanceof HOTLeafPage leaf) {
      leaves[0]++;
      for (int i = 0; i < leaf.getEntryCount(); i++) {
        final byte[] storedKey = leaf.getKey(i);
        assertNotNull(storedKey);
        assertEquals(20, storedKey.length);
        final long key = SERIALIZER.deserialize(storedKey, 0, 16);
        assertTrue(keys.add(key), "duplicate stored key");
        Page routed = reader.loadHOTPage(root);
        int visits = 0;
        while (routed instanceof HOTIndirectPage indirect) {
          assertTrue(++visits < 64, "routing cycle");
          final int slot = indirect.findChildIndex(storedKey);
          assertTrue(slot >= 0);
          routed = reader.loadHOTPage(indirect.getChildReference(slot));
        }
        assertTrue(routed instanceof HOTLeafPage);
        assertEquals(leaf.getPageKey(), ((HOTLeafPage) routed).getPageKey(), "full-key routing");
      }
      return 0;
    }
    assertTrue(page instanceof HOTIndirectPage);
    final HOTIndirectPage indirect = (HOTIndirectPage) page;
    if (!(reader instanceof StorageEngineWriter)) {
      assertFalse(indirect.needsNormalization(), "durable decode must not schedule historical normalization");
    }
    assertTrue(indirect.getNumChildren() > 0 && indirect.getNumChildren() <= 32);
    int max = 0;
    for (int i = 0; i < indirect.getNumChildren(); i++) {
      max = Math.max(max, inspect(indirect.getChildReference(i), root, reader, keys, heightErrors, leaves, depth + 1));
    }
    final int height = max + 1;
    if (height != indirect.getHeight()) {
      heightErrors.add("stored=" + indirect.getHeight() + ",structural=" + height);
    }
    return height;
  }
}
