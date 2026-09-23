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
import java.util.concurrent.atomic.AtomicLong;
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
 * — stops. The cascade pre-check of the integrate arm asks that placement question of the parent as
 * its first level and routes the overflow through the complete structural frontier, which splits
 * the parent's subtree immediately before the key and gives the key its own leaf.
 * </p>
 *
 * <p>
 * The first two scenarios build that shape through the public writer and reach the decline in the
 * two ways a leaf overflows: with a key the leaf does not hold yet, and with one it does, whose
 * posting outgrew the page. The other two carry the same question one level up. Once the node under
 * the straddling slot is itself a full node of leaves, an overflow inside it starts the capacity
 * cascade, and the fold of its own split into the root is the refused one — reached from the
 * integrate arm when the split bit is fresh to the node's mask, and from the full-parent handler
 * when it is not. Both entries pre-check the cascade and route it the same way; without their
 * pre-check each scenario ends in the refusal the fold primitives raise. Each asserts that the
 * decline and the routing were really reached, on the counter of the entry it claims, so a change
 * in leaf geometry cannot let them pass without exercising anything.
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

  /**
   * The resident keys whose postings the cascade scenarios grow. Both are of pattern {@code 0x20} —
   * the first of the shape phase's opening run, the second of the run that reaches the first routing.
   */
  private static final long FILLER_ENDPOINT = 100L;
  private static final long SECOND_FILLER_ENDPOINT = 1_100L;

  /**
   * Chunk keys of one posting, most significant chunk bits first: 511 keys {@code v << 20}, then 1023
   * keys {@code v << 10}, then 1023 keys {@code v}. A later tier always differs from its neighbours
   * below every bit a split has used so far, so every put merges and every overflow is a merge-path
   * leaf split that adds exactly one leaf child to that node.
   */
  private static final int FIRST_TIER = 511;
  private static final int SECOND_TIER = 1_023;
  private static final int THIRD_TIER = 1_023;
  private static final int FILLER_FAMILY = FIRST_TIER + SECOND_TIER + THIRD_TIER;

  /** Leaves the node one leaf short of full, with that leaf short of full in turn. */
  private static final int FIRST_TIER_PUTS = 363;

  /** Fills the node up through a second resident key without touching the first one's short leaf. */
  private static final int SECOND_FILLER_PUTS = 1_100;

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
      final long routedBefore = AbstractHOTIndexWriter.MERGE_OVERFLOW_ROUTED_FROM_INTEGRATE_ARM.get();
      final long declinedBefore = HOTIncrementalInsert.EXISTING_BIT_FOLD_NOT_ADJACENT.get();
      final long fallbacksBefore = AbstractHOTIndexWriter.OFF_PATH_OVERFLOW_FALLBACK.get();

      // The straddling leaf fills up; the put that overflows it splits at the off-path bit and the
      // fold that would follow is the one no position accepts.
      int puts = 0;
      while (AbstractHOTIndexWriter.MERGE_OVERFLOW_ROUTED_FROM_INTEGRATE_ARM.get() == routedBefore
          && puts < MAX_SHAPE_PUTS) {
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
      final long routedBefore = AbstractHOTIndexWriter.MERGE_OVERFLOW_ROUTED_FROM_INTEGRATE_ARM.get();
      final long declinedBefore = HOTIncrementalInsert.EXISTING_BIT_FOLD_NOT_ADJACENT.get();
      final long fallbacksBefore = AbstractHOTIndexWriter.OFF_PATH_OVERFLOW_FALLBACK.get();

      // No new keys: the twelve resident postings grow until the leaf overflows by bytes on a key
      // it already holds. Node keys of stride two keep the bitmaps free of runs, so they really grow.
      long nodeKey = 9L;
      int puts = 0;
      while (AbstractHOTIndexWriter.MERGE_OVERFLOW_ROUTED_FROM_INTEGRATE_ARM.get() == routedBefore
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

  @Test
  @DisplayName("a cascade the integrate arm would refuse above L's parent is routed instead of throwing")
  void cascadeRefusedAboveTheParentIsRoutedFromTheIntegrateArm() {
    loadAndVerify(load -> {
      load.buildRoutedStraddlingNode();
      final AtomicLong routed = AbstractHOTIndexWriter.MERGE_OVERFLOW_ROUTED_FROM_INTEGRATE_ARM;
      final long cascadesBefore = routed.get();

      // One resident key's posting spreads over fresh chunk keys until its node is full; the next
      // overflow splits that node at a fresh bit and the fold of its half into the root is the one
      // no position accepts. Without the pre-check integrate reaches that fold and throws.
      load.growPostingUntilCascadeRouted(FILLER_ENDPOINT, 0, routed, cascadesBefore);
      assertCascadeRouted(routed, cascadesBefore, "the integrate arm");

      load.repeat(300, 0x20);
      load.repeat(100, 0x00);
      load.repeat(100, 0x30);
    });
  }

  @Test
  @DisplayName("a cascade the full-parent handler would refuse is routed instead of throwing")
  void cascadeRefusedAboveTheParentIsRoutedFromTheFullParentHandler() {
    loadAndVerify(load -> {
      load.buildRoutedStraddlingNode();
      final AtomicLong routed = AbstractHOTIndexWriter.MERGE_OVERFLOW_ROUTED_FROM_FULL_PARENT;
      final long cascadesBefore = routed.get();

      // Leave one leaf of the node short of full, fill the node up through a second resident key,
      // then overflow that leaf at a bit the node's mask already holds with a free position beside
      // its slot: the fold is accepted, the node is full, and the full-parent handler splits it with
      // the insertion and integrates at the root — the same refused fold, one frame further down.
      load.growPosting(FILLER_ENDPOINT, 0, FIRST_TIER_PUTS);
      load.growPosting(SECOND_FILLER_ENDPOINT, 0, SECOND_FILLER_PUTS);
      load.growPostingUntilCascadeRouted(FILLER_ENDPOINT, FIRST_TIER + SECOND_TIER, routed, cascadesBefore);
      assertCascadeRouted(routed, cascadesBefore, "the full-parent handler");

      load.repeat(300, 0x20);
      load.repeat(100, 0x00);
      load.repeat(100, 0x30);
    });
  }

  private static void assertCascadeRouted(final AtomicLong routed, final long cascadesBefore, final String entry) {
    assertTrue(routed.get() > cascadesBefore,
        "the scenario must reach an integrate cascade the pre-check of " + entry + " refuses above L's parent; "
            + "without one it covers no cascade routing there and its shape must be re-tuned");
  }

  private static void assertRouted(final long routedBefore, final long declinedBefore, final long fallbacksBefore) {
    assertTrue(AbstractHOTIndexWriter.MERGE_OVERFLOW_ROUTED_FROM_INTEGRATE_ARM.get() > routedBefore,
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
     * {@code count} chunk keys of one resident key's posting, starting at family index {@code from}.
     */
    private void growPosting(final long endpoint, final int from, final int count) {
      for (int index = from; index < from + count && index < FILLER_FAMILY; index++) {
        putResident(0x20, endpoint, fillerNodeKey(index));
      }
    }

    /**
     * Grow one posting until the cascade pre-check of one entry routes an overflow, or the family runs
     * out.
     */
    private void growPostingUntilCascadeRouted(final long endpoint, final int from, final AtomicLong routed,
        final long cascadesBefore) {
      for (int index = from; index < FILLER_FAMILY && routed.get() == cascadesBefore; index++) {
        putResident(0x20, endpoint, fillerNodeKey(index));
      }
    }

    /**
     * Load until the first declined fold is routed — the height-2 root over a height-1 node of leaves
     * whose sibling partial still sorts between the node's own and its straddle partial, which is the
     * node-level analogue of the leaf shape {@link #buildStraddlingLeaf} builds.
     */
    private void buildRoutedStraddlingNode() {
      buildStraddlingLeaf();
      final long routedBefore = AbstractHOTIndexWriter.MERGE_OVERFLOW_ROUTED_FROM_INTEGRATE_ARM.get();
      int puts = 0;
      while (AbstractHOTIndexWriter.MERGE_OVERFLOW_ROUTED_FROM_INTEGRATE_ARM.get() == routedBefore
          && puts < MAX_SHAPE_PUTS) {
        put(0x20, SHAPE_NODE_KEY);
        puts++;
      }
      assertTrue(AbstractHOTIndexWriter.MERGE_OVERFLOW_ROUTED_FROM_INTEGRATE_ARM.get() > routedBefore,
          "the cascade scenarios start from a routed declined fold; without one their shape has drifted");
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

  /**
   * The node key whose posting chunk is family member {@code index}: the HOT key for a posting is the
   * key's prefix with {@code nodeKey >>> 16} appended, so a fresh chunk is a fresh key of that leaf.
   */
  private static long fillerNodeKey(final int index) {
    final long chunk;
    if (index < FIRST_TIER) {
      chunk = (long) (index + 1) << 20;
    } else if (index < FIRST_TIER + SECOND_TIER) {
      chunk = (long) (index - FIRST_TIER + 1) << 10;
    } else {
      chunk = index - FIRST_TIER - SECOND_TIER + 1;
    }
    return (chunk << 16) + 7L;
  }

  /** {@code pattern} occupies key byte 1 — the serializer sign-flips the fork node. */
  private static ValidTimeKey key(final int pattern, final long endpoint) {
    return new ValidTimeKey(ValidTimeKey.STORE_UPPER, ((long) pattern << 56) ^ Long.MIN_VALUE, endpoint);
  }
}
