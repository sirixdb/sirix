/*
 * [New BSD License]
 * Copyright (c) 2026, SirixDB Contributors
 * All rights reserved.
 */
package io.sirix.index.hot;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexType;
import io.sirix.index.SearchMode;
import io.sirix.index.interval.ValidTimeKey;
import io.sirix.index.interval.ValidTimeKeySerializer;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A leaf overflow whose fold into its parent has to be declined still has to place its key.
 *
 * <p>
 * A multi-value leaf can hold keys on both sides of a bit its parent's mask already carries but
 * which is off-path for the leaf itself. Splitting the leaf at that bit gives its 1-side half the
 * slot's partial with that column set, and folding it in means inserting it at that partial's
 * ascending position. When a sibling's partial sorts in between, that position is past the sibling,
 * so the fold is refused ({@link HOTIncrementalInsert#canMergeBiNodeAtExistingDiscBit}). At a
 * parent that would fold rather than nest, the standard capacity cascade reaches the very same
 * fold, so there is no second placement: without a fallback the insert — and with it the whole load
 * — stops. The overflow is therefore routed through the complete structural frontier, which splits
 * the parent's subtree immediately before the key and gives the key its own leaf.
 * </p>
 *
 * <p>
 * Both scenarios build the same shape through the public writer and then reach the decline in the
 * two ways a leaf overflows: with a key the leaf does not hold yet, and with one it does, whose
 * posting outgrew the page. Each asserts that the decline and the routing were really reached, so a
 * change in leaf geometry cannot let them pass without exercising anything.
 * </p>
 */
final class HOTDeclinedOverflowFrontierRouteTest {

  private static final String RESOURCE = "declined-overflow-frontier-route";
  private static final int INDEX_NUMBER = 0;

  /** Every key of the shape phase indexes this one node. */
  private static final long SHAPE_NODE_KEY = 7L;

  /** Safety net so a geometry change ends the scenario instead of running forever. */
  private static final int MAX_SHAPE_PUTS = 4_000;
  private static final int MAX_POSTING_PUTS = 60_000;

  /** The keys whose postings scenario B grows: the first twelve puts, all of pattern {@code 0x20}. */
  private static final int GROWN_KEYS = 12;

  @TempDir
  Path temporaryDirectory;

  @AfterEach
  void clearCaches() {
    Databases.clearGlobalCaches();
  }

  @Test
  @DisplayName("a declined fold on a new key is routed through the complete frontier")
  void declinedFoldOnANewKeyIsRouted() {
    loadAndVerify(load -> {
      load.buildStraddlingLeaf();
      final long routedBefore = AbstractHOTIndexWriter.MERGE_OVERFLOW_ROUTED_TO_FRONTIER.get();
      final long declinedBefore = HOTIncrementalInsert.EXISTING_BIT_FOLD_NOT_ADJACENT.get();
      final long fallbacksBefore = AbstractHOTIndexWriter.OFF_PATH_OVERFLOW_FALLBACK.get();

      // The straddling leaf fills up; the put that overflows it splits at the off-path bit and the
      // fold that would follow is the one no position accepts.
      int puts = 0;
      while (AbstractHOTIndexWriter.MERGE_OVERFLOW_ROUTED_TO_FRONTIER.get() == routedBefore && puts < MAX_SHAPE_PUTS) {
        load.put(0x20, SHAPE_NODE_KEY);
        puts++;
      }
      assertRouted(routedBefore, declinedBefore, fallbacksBefore);

      // Keep loading through the spliced frontier, on both sides of it and across it.
      load.repeat(200, 0x00);
      load.repeat(200, 0x30);
      load.repeat(400, 0x20);
      load.repeat(300, 0x10);
    });
  }

  @Test
  @DisplayName("a declined fold on a key the leaf already holds is routed, and the stale entry replaced")
  void declinedFoldOnAResidentKeyIsRouted() {
    loadAndVerify(load -> {
      load.buildStraddlingLeaf();
      final long routedBefore = AbstractHOTIndexWriter.MERGE_OVERFLOW_ROUTED_TO_FRONTIER.get();
      final long declinedBefore = HOTIncrementalInsert.EXISTING_BIT_FOLD_NOT_ADJACENT.get();
      final long fallbacksBefore = AbstractHOTIndexWriter.OFF_PATH_OVERFLOW_FALLBACK.get();

      // No new keys: the twelve resident postings grow until the leaf overflows by bytes on a key
      // it already holds. Node keys of stride two keep the bitmaps free of runs, so they really grow.
      long nodeKey = 9L;
      int puts = 0;
      while (AbstractHOTIndexWriter.MERGE_OVERFLOW_ROUTED_TO_FRONTIER.get() == routedBefore
          && puts < MAX_POSTING_PUTS) {
        load.putResident(0x20, 1 + puts % GROWN_KEYS, nodeKey);
        nodeKey += 2L;
        puts++;
      }
      assertRouted(routedBefore, declinedBefore, fallbacksBefore);

      for (int further = 0; further < 4_000; further++) {
        load.putResident(0x20, 1 + further % GROWN_KEYS, nodeKey);
        nodeKey += 2L;
      }
    });
  }

  private static void assertRouted(final long routedBefore, final long declinedBefore, final long fallbacksBefore) {
    assertTrue(AbstractHOTIndexWriter.MERGE_OVERFLOW_ROUTED_TO_FRONTIER.get() > routedBefore,
        "the scenario must reach a leaf overflow whose fold is declined at a parent the cascade would fold "
            + "into; without one it covers no routing and its shape must be re-tuned");
    assertTrue(HOTIncrementalInsert.EXISTING_BIT_FOLD_NOT_ADJACENT.get() > declinedBefore,
        "the routing must be the placement refusal's, not another decline's");
    assertTrue(AbstractHOTIndexWriter.OFF_PATH_OVERFLOW_FALLBACK.get() > fallbacksBefore,
        "the refusal must be reported by the off-path overflow handler");
  }

  // ===== Harness =====

  /**
   * The put stream of one scenario, recording what every key must hold so the committed index can be
   * checked entry by entry.
   */
  private static final class Load {
    private final HOTIndexWriter<ValidTimeKey> writer;
    private final Map<ValidTimeKey, Set<Long>> expected = new LinkedHashMap<>();
    private long counter;

    private Load(final HOTIndexWriter<ValidTimeKey> writer) {
      this.writer = writer;
    }

    /** One put of a fresh key: {@code pattern} is key byte 1, the running counter its endpoint. */
    private void put(final int pattern, final long nodeKey) {
      putResident(pattern, ++counter, nodeKey);
    }

    /** One put of an already-issued key — {@code endpoint} names which. */
    private void putResident(final int pattern, final long endpoint, final long nodeKey) {
      final ValidTimeKey key = key(pattern, endpoint);
      writer.indexNodeKey(key, nodeKey);
      expected.computeIfAbsent(key, ignored -> new LinkedHashSet<>()).add(nodeKey);
    }

    private void repeat(final int times, final int pattern) {
      for (int i = 0; i < times; i++) {
        put(pattern, SHAPE_NODE_KEY);
      }
    }

    /**
     * A height-1 root over four children whose first child is a leaf straddling an off-path mask bit
     * with a sibling's partial sorting between its two sides.
     *
     * <p>
     * The 513th entry splits the root leaf at key bit 8; the halves split again at bits 10 and 11, so
     * the root's mask is {@code {8, 10, 11}} over the partials {@code 000, 001, 100, 110}. The
     * {@code 0x00} keys then sub-insert into the {@code 0x20} leaf, whose own combination partial is
     * taken — and that leaf now holds keys on both sides of bit 10, which is off-path for it, while the
     * sibling at {@code 001} sorts strictly between {@code 000} and {@code 010}.
     * </p>
     */
    private void buildStraddlingLeaf() {
      repeat(150, 0x20);
      repeat(150, 0x30);
      repeat(150, 0x80);
      repeat(63, 0xa0);
      repeat(300, 0xa0);
      repeat(213, 0x30);
      repeat(3, 0x00);
    }
  }

  /** Run a scenario, commit it, and verify the committed index in full. */
  private void loadAndVerify(final Consumer<Load> scenario) {
    final Path databasePath = temporaryDirectory.resolve("db");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder(RESOURCE).build()));
      final Load load;
      try (JsonResourceSession session = database.beginResourceSession(RESOURCE);
          JsonNodeTrx wtx = session.beginNodeTrx()) {
        load = new Load(HOTIndexWriter.create(wtx.getStorageEngineWriter(), ValidTimeKeySerializer.INSTANCE,
            IndexType.VALIDTIME, INDEX_NUMBER));
        scenario.accept(load);
        wtx.commit();
      }

      try (JsonResourceSession session = database.beginResourceSession(RESOURCE);
          JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        HOTInvariantValidator.validateIndex(rtx.getStorageEngineReader(), IndexType.VALIDTIME, INDEX_NUMBER).assertOk();
        final HOTIndexReader<ValidTimeKey> reader = HOTIndexReader.create(rtx.getStorageEngineReader(),
            ValidTimeKeySerializer.INSTANCE, IndexType.VALIDTIME, INDEX_NUMBER);
        for (final Map.Entry<ValidTimeKey, Set<Long>> entry : load.expected.entrySet()) {
          final NodeReferences postings = reader.get(entry.getKey(), SearchMode.EQUAL);
          assertNotNull(postings, entry.getKey() + " must be readable");
          assertEquals(entry.getValue().size(), postings.cardinality(), "postings of " + entry.getKey());
          for (final long nodeKey : entry.getValue()) {
            assertTrue(postings.isPresent(nodeKey), entry.getKey() + " must hold node " + nodeKey);
          }
        }
      }
    }
  }

  /** {@code pattern} occupies key byte 1 — the serializer sign-flips the fork node. */
  private static ValidTimeKey key(final int pattern, final long endpoint) {
    return new ValidTimeKey(ValidTimeKey.STORE_UPPER, ((long) pattern << 56) ^ Long.MIN_VALUE, endpoint);
  }
}
