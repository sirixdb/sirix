/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;

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

/**
 * FMT-306 — HOT stored height and spine continuity, read from the walked structure rather than from
 * the fields the writer maintains.
 *
 * <p>
 * Every case walks actual child references, recomputes each node's height, and re-routes every
 * stored key from the root, so a lost outer spine shows up as a stored height that disagrees with
 * the structure or as a key that no longer reaches the leaf that holds it. The descending workload
 * is the shape issue <a href="https://github.com/sirixdb/sirix/issues/1457">1457</a> reported:
 * before the repair its committed trie carried two excess levels over the identical ascending leaf
 * set, and its ancestors kept height 1 above taller children.
 */
@Isolated
final class HOTStoredHeightSpineTest {
  /** Sixteen-byte keys: eight zero bytes, then the big-endian value. */
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
        value = (value << 8) | (bytes[offset + i] & 0xFFL);
      }
      return value;
    }
  };

  private static final int KEYS = 4096;

  @TempDir
  Path directory;

  @AfterEach
  void clearCaches() {
    Databases.clearGlobalCaches();
  }

  /**
   * Clause 1: a stored height equals the structure below it live in the writing transaction, in the
   * published revision and after cold decode. Descending and ascending insertion of the same key set
   * are compared, because the defect was order-dependent.
   */
  @Test
  void storedHeightsStayExactLiveCommittedAndCold() {
    for (final boolean descending : new boolean[] {false, true}) {
      final List<Long> keys = sequential(descending);
      final Path path = directory.resolve(descending
          ? "descending"
          : "ascending");
      assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(path)));
      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
        assertTrue(database.createResource(ResourceConfiguration.newBuilder("spine")
                                                                .versioningApproach(VersioningType.FULL)
                                                                .storeDiffs(false)
                                                                .build()));
        try (JsonResourceSession session = database.beginResourceSession("spine");
            JsonNodeTrx transaction = session.beginNodeTrx()) {
          StorageEngineWriter engine = transaction.getStorageEngineWriter();
          final HOTIndexWriter<Long> writer = HOTIndexWriter.create(engine, SERIALIZER, IndexType.CAS, 0);
          for (final long key : keys) {
            writer.indexNodeKey(key, 1);
          }
          assertShapeIsSound(writer.getRootReference(), engine, keys, "live");
          transaction.commit();
          engine = transaction.getStorageEngineWriter();
          final HOTIndexWriter<Long> committed = HOTIndexWriter.create(engine, SERIALIZER, IndexType.CAS, 0);
          final Shape afterCommit = assertShapeIsSound(committed.getRootReference(), engine, keys, "committed");
          // Clause 2, the shrink direction: folding the spine away must leave no larger height behind.
          assertEquals(1, afterCommit.height(),
              "the committed trie over eight full leaves is one indirect level above them");
        }
      }
      Databases.clearGlobalCaches();
      try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path);
          JsonResourceSession session = database.beginResourceSession("spine");
          JsonNodeReadOnlyTrx transaction = session.beginNodeReadOnlyTrx()) {
        final StorageEngineReader reader = transaction.getStorageEngineReader();
        final HOTIndexReader<Long> indexReader = HOTIndexReader.create(reader, SERIALIZER, IndexType.CAS, 0);
        assertShapeIsSound(indexReader.getRootReference(), reader, keys, "cold");
      }
      Databases.clearGlobalCaches();
    }
  }

  /**
   * Clause 3: a subtree-local split below a committed ancestor carries back to the whole parent
   * chain. The second transaction's keys are spread over the whole key space, so they land inside the
   * committed subtrees instead of appending past them; a sequential continuation reaches no
   * subtree-local insertion at all. The witness is
   * {@link AbstractHOTIndexWriter#DIRECTION_ONE_SUBINSERT}: without it the case could stop reaching
   * that path and keep passing for the wrong reason. Measured: 384 subtree-local insertions.
   */
  @Test
  void aSubtreeLocalSplitLeavesNoAncestorDescribingTheReplacedSubtree() {
    final List<Long> keys = sequential(true);
    final Path path = directory.resolve("local-split");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(path)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      assertTrue(database.createResource(
          ResourceConfiguration.newBuilder("spine").versioningApproach(VersioningType.FULL).storeDiffs(false).build()));
      try (JsonResourceSession session = database.beginResourceSession("spine");
          JsonNodeTrx transaction = session.beginNodeTrx()) {
        StorageEngineWriter engine = transaction.getStorageEngineWriter();
        HOTIndexWriter<Long> writer = HOTIndexWriter.create(engine, SERIALIZER, IndexType.CAS, 0);
        for (final long key : keys) {
          writer.indexNodeKey(key, 1);
        }
        transaction.commit();
        engine = transaction.getStorageEngineWriter();
        final long before = AbstractHOTIndexWriter.DIRECTION_ONE_SUBINSERT.get();
        writer = HOTIndexWriter.create(engine, SERIALIZER, IndexType.CAS, 0);
        final HOTIndirectPage committedRoot = (HOTIndirectPage) engine.loadHOTPage(writer.getRootReference());
        assertFalse(committedRoot.needsNormalization(), "the previous commit cleared this outer spine");
        for (final long key : spread()) {
          writer.indexNodeKey(key, 1);
          keys.add(key);
        }
        assertTrue(AbstractHOTIndexWriter.DIRECTION_ONE_SUBINSERT.get() > before,
            "this fixture must split inside a subtree below an already committed ancestor");
        assertShapeIsSound(writer.getRootReference(), engine, keys, "after a subtree-local split");
        transaction.commit();
        engine = transaction.getStorageEngineWriter();
        final HOTIndexWriter<Long> committed = HOTIndexWriter.create(engine, SERIALIZER, IndexType.CAS, 0);
        assertShapeIsSound(committed.getRootReference(), engine, keys, "committed after a subtree-local split");
      }
    }
    Databases.clearGlobalCaches();
  }

  /**
   * Clause 4: locating a key does not depend on a stored height. A wrong height on a spine this
   * transaction owns still routes every key to its own leaf — and is still rejected, because the walk
   * establishes the height from the structure rather than from the field.
   */
  @Test
  void aWrongStoredHeightMisroutesNothingAndIsStillRejected() {
    final List<Long> keys = sequential(true);
    final Path path = directory.resolve("wrong-height");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(path)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      assertTrue(database.createResource(
          ResourceConfiguration.newBuilder("spine").versioningApproach(VersioningType.FULL).storeDiffs(false).build()));
      try (JsonResourceSession session = database.beginResourceSession("spine");
          JsonNodeTrx transaction = session.beginNodeTrx()) {
        StorageEngineWriter engine = transaction.getStorageEngineWriter();
        final HOTIndexWriter<Long> writer = HOTIndexWriter.create(engine, SERIALIZER, IndexType.CAS, 0);
        for (final long key : keys) {
          writer.indexNodeKey(key, 1);
        }
        transaction.commit();
        engine = transaction.getStorageEngineWriter();
        // One further key gives this transaction its own copy of the spine; a committed, cache-shared
        // page is never a legal target for a height change.
        final HOTIndexWriter<Long> owner = HOTIndexWriter.create(engine, SERIALIZER, IndexType.CAS, 0);
        owner.indexNodeKey((long) KEYS, 1);
        keys.add((long) KEYS);
        final PageReference root = owner.getRootReference();
        final Shape sound = assertShapeIsSound(root, engine, keys, "owned");
        final HOTIndirectPage owned = (HOTIndirectPage) engine.loadHOTPage(root);
        owned.setHeightForWrite(sound.height() + 1);

        final Shape wrong = walk(root, engine);
        assertEquals(1, wrong.inexactHeights(), "the walk reports exactly the node whose height was changed");
        assertEquals(0, wrong.routingErrors(), "no key may be misrouted by a stored height");
        assertEquals(new TreeSet<>(keys), wrong.keys(), "every key stays readable through its own leaf");
        assertEquals(List.of("I13-stored-height-exact"),
            HOTInvariantValidator.validate(root, engine)
                                 .violations()
                                 .stream()
                                 .map(HOTInvariantValidator.Violation::invariant)
                                 .distinct()
                                 .toList(),
            "the invariant validator must name the stored-height invariant and nothing else");
        owned.setHeightForWrite(sound.height());
        transaction.rollback();
      }
    }
  }

  /** Descending keys spread over the whole key space, each landing inside a committed subtree. */
  private static List<Long> spread() {
    final Set<Long> keys = new TreeSet<>();
    final Random random = new Random(5);
    while (keys.size() < 512) {
      final long key = random.nextLong() & Long.MAX_VALUE;
      if (key >= KEYS) {
        keys.add(key);
      }
    }
    final List<Long> descending = new ArrayList<>(keys);
    Collections.reverse(descending);
    return descending;
  }

  private static List<Long> sequential(final boolean descending) {
    final List<Long> keys = new ArrayList<>(KEYS);
    for (long key = 0; key < KEYS; key++) {
      keys.add(key);
    }
    if (descending) {
      Collections.reverse(keys);
    }
    return keys;
  }

  /** The structural facts one walk establishes, independently of every stored height. */
  private record Shape(int height, int inexactHeights, int routingErrors, Set<Long> keys) {
  }

  private static Shape assertShapeIsSound(final PageReference root, final StorageEngineReader reader,
      final List<Long> keys, final String state) {
    assertNotNull(root, state + ": the index must have a root");
    final Shape shape = walk(root, reader);
    assertEquals(0, shape.inexactHeights(), state + ": every stored height must equal the walked structure");
    assertEquals(0, shape.routingErrors(), state + ": every stored key must route to the leaf that holds it");
    assertEquals(new TreeSet<>(keys), shape.keys(), state + ": every key occurs exactly once");
    HOTInvariantValidator.validate(root, reader).assertOk();
    return shape;
  }

  private static Shape walk(final PageReference root, final StorageEngineReader reader) {
    final Set<Long> keys = new TreeSet<>();
    final int[] counters = new int[2]; // 0: inexact stored heights, 1: routing errors
    final int height = descend(root, root, reader, keys, counters, 64);
    return new Shape(height, counters[0], counters[1], keys);
  }

  private static int descend(final PageReference reference, final PageReference root, final StorageEngineReader reader,
      final Set<Long> keys, final int[] counters, final int remainingDepth) {
    assertTrue(remainingDepth > 0, "acyclic bounded descent");
    final Page page = reader.loadHOTPage(reference);
    assertNotNull(page, "every reachable reference must resolve");
    assertFalse(page.isClosed(), "a reachable page must not be closed");
    if (page instanceof HOTLeafPage leaf) {
      for (int slot = 0; slot < leaf.getEntryCount(); slot++) {
        final byte[] storedKey = leaf.getKey(slot);
        assertTrue(keys.add(SERIALIZER.deserialize(storedKey, 0, 16)), "duplicate stored key");
        Page routed = reader.loadHOTPage(root);
        int steps = 0;
        while (routed instanceof HOTIndirectPage indirect) {
          assertTrue(++steps < 64, "routing cycle");
          final int child = indirect.findChildIndex(storedKey);
          if (child < 0) {
            counters[1]++;
            routed = null;
            break;
          }
          routed = reader.loadHOTPage(indirect.getChildReference(child));
        }
        if (!(routed instanceof HOTLeafPage target) || target.getPageKey() != leaf.getPageKey()) {
          counters[1]++;
        }
      }
      return 0;
    }
    final HOTIndirectPage indirect = (HOTIndirectPage) page;
    int tallest = 0;
    for (int slot = 0; slot < indirect.getNumChildren(); slot++) {
      tallest = Math.max(tallest,
          descend(indirect.getChildReference(slot), root, reader, keys, counters, remainingDepth - 1));
    }
    final int height = tallest + 1;
    if (height != indirect.getHeight()) {
      counters[0]++;
    }
    return height;
  }
}
