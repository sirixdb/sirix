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
import io.sirix.index.interval.IntervalDomain;
import io.sirix.index.interval.RelationalIntervalTree;
import io.sirix.index.interval.ValidTimeKey;
import io.sirix.index.interval.ValidTimeKeySerializer;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Structural inserts next to a leaf that holds keys on both sides of a bit its parent's mask
 * already carries, driven through the index writer exactly as the valid-time interval index drives
 * it.
 *
 * <p>
 * The index is keyed {@code [store][fork][endpoint][chunk]}. Registering many records under one
 * interval yields a single endpoint per store whose postings spread over the trailing chunk index —
 * a handful of keys, each with a posting of several KiB, so a leaf holds only a few of them and the
 * trie over them is built from byte-driven leaf splits. Registering further intervals then inserts
 * keys that differ from those in the endpoint bytes: a bit <em>more</em> significant than the chunk
 * bits the node already discriminates on, and one another branch of the same node may have put into
 * the mask already. That is where a multi-value leaf comes to straddle a mask bit, and where the
 * writer's structural handlers have to place, or decline to place, what they split off it.
 * </p>
 *
 * <p>
 * Each scenario replays the shortest operation sequence found to reach one handler, and asserts
 * that the handler was reached, through the counter incremented at its source. Without that a
 * change in leaf geometry would let the scenario pass without exercising anything. A node key
 * written out as {@code 900_xxx} belongs to a record appended after the shared ones: past every
 * loaded record, so its posting opens a chunk of its own.
 * </p>
 *
 * <p>
 * All three scenarios together run in about 2 to 4 seconds on a development machine, so they belong
 * in the default lane; a suite that grows past 60 seconds belongs behind {@code @Tag("heavy")},
 * which the advisory cross-platform CI lanes exclude ({@code bundles/sirix-core/build.gradle}).
 * </p>
 */
final class HOTStraddlingLeafSpliceTest {

  private static final String RESOURCE = "straddling-leaf-splice";
  private static final int INDEX_NUMBER = 0;

  /** One record is nine nodes apart from the next, as in an array of eight-field objects. */
  private static final long FIRST_RECORD_KEY = 2L;
  private static final long RECORD_KEY_STRIDE = 9L;

  /**
   * Records registered under the one shared interval. Their postings have to spread over enough chunk
   * keys of several KiB each for the trie below them to come from byte-driven leaf splits: every
   * scenario here needs 60,000 and none is reached with 40,000.
   */
  private static final int SHARED_INTERVAL_RECORDS = 60_000;

  private static final Instant FIRST_DAY = Instant.parse("2024-01-01T00:00:00Z");
  private static final int HORIZON_DAYS = 366;

  private static final IntervalDomain DOMAIN = new IntervalDomain();

  @TempDir
  Path temporaryDirectory;

  @AfterEach
  void clearCaches() {
    Databases.clearGlobalCaches();
  }

  @Test
  @DisplayName("a split half that would land past a sibling is not folded into the node")
  void halfThatWouldLandPastASiblingIsNotFolded() {
    final long declinedBefore = HOTIncrementalInsert.EXISTING_BIT_FOLD_NOT_ADJACENT.get();

    loadAndVerify(SHARED_INTERVAL_RECORDS, scenario -> {
      // Intervals that all start on the first day: their upper-store keys share a fork node per
      // magnitude of the end day and differ in the endpoint bytes only.
      scenario.register(ValidTimeKey.STORE_LOWER, 7, recordKey(2));
      scenario.register(ValidTimeKey.STORE_UPPER, 7, recordKey(2));
      scenario.register(ValidTimeKey.STORE_UPPER, 37, recordKey(75));
      scenario.register(ValidTimeKey.STORE_UPPER, 88, 900_029L);
      scenario.register(ValidTimeKey.STORE_UPPER, 64, 900_290L);
      scenario.register(ValidTimeKey.STORE_UPPER, 8, recordKey(555));
      scenario.register(ValidTimeKey.STORE_UPPER, 55, 900_308L);
      scenario.register(ValidTimeKey.STORE_UPPER, 23, recordKey(562));
      scenario.register(ValidTimeKey.STORE_UPPER, 22, recordKey(570));
    });

    assertTrue(HOTIncrementalInsert.EXISTING_BIT_FOLD_NOT_ADJACENT.get() > declinedBefore,
        "the sequence must reach a fold at a bit the parent's mask already holds whose other half would "
            + "not land beside its slot; without one it no longer covers the placement that was published "
            + "out of order, and its record count must be re-tuned");
  }

  @Test
  @DisplayName("a frontier side the block's nested bit cuts through is split before it joins the block")
  void frontierSideCutByTheNestedBitIsSplit() {
    final long splitsBefore = AbstractHOTIndexWriter.FRONTIER_JOIN_STRADDLE_SPLIT.get();

    // The last key sorts just below a side that holds keys on both sides of the bit that tells the
    // key from that side's maximum. Joined as one child, the side's keys with the bit clear match the
    // new key's partial: the block was published with a child whose partial its own keys contradict.
    loadAndVerify(SHARED_INTERVAL_RECORDS, scenario -> {
      scenario.register(ValidTimeKey.STORE_LOWER, 39, recordKey(558));
      scenario.register(ValidTimeKey.STORE_UPPER, 39, recordKey(558));
      scenario.register(ValidTimeKey.STORE_UPPER, 22, recordKey(570));
    });

    assertTrue(AbstractHOTIndexWriter.FRONTIER_JOIN_STRADDLE_SPLIT.get() > splitsBefore,
        "the sequence must reach a complete-frontier join that has to split a side a bit of its block "
            + "cuts through; without one it no longer covers that join and must be re-tuned");
  }

  @Test
  @DisplayName("a frontier side the block's root bit cuts through is split instead of refusing the insert")
  void frontierSideCutByTheRootBitIsSplit() {
    final long splitsBefore = AbstractHOTIndexWriter.FRONTIER_JOIN_STRADDLE_SPLIT.get();

    // Here the side below the new key already straddles the most significant bit of the whole
    // frontier. The join used to give up on the insert with "frontier child straddles Patricia bit".
    loadAndVerify(SHARED_INTERVAL_RECORDS, scenario -> {
      scenario.register(ValidTimeKey.STORE_LOWER, 86, recordKey(1));
      scenario.register(ValidTimeKey.STORE_UPPER, 86, recordKey(1));
      scenario.register(ValidTimeKey.STORE_LOWER, 7, recordKey(2));
      scenario.register(ValidTimeKey.STORE_UPPER, 7, recordKey(2));
      scenario.register(ValidTimeKey.STORE_UPPER, 21, recordKey(148));
      scenario.register(ValidTimeKey.STORE_UPPER, 14, recordKey(155));
      scenario.register(ValidTimeKey.STORE_UPPER, 65, 900_074L);
      scenario.register(ValidTimeKey.STORE_UPPER, 82, recordKey(162));
      scenario.register(ValidTimeKey.STORE_UPPER, 31, recordKey(182));
      scenario.register(ValidTimeKey.STORE_UPPER, 35, 900_272L);
      scenario.register(ValidTimeKey.STORE_UPPER, 25, recordKey(1_292));
      scenario.register(ValidTimeKey.STORE_UPPER, 29, recordKey(1_298));
    });

    assertTrue(AbstractHOTIndexWriter.FRONTIER_JOIN_STRADDLE_SPLIT.get() > splitsBefore,
        "the sequence must reach a complete-frontier join that has to split a side a bit of its block "
            + "cuts through; without one it no longer covers that join and must be re-tuned");
  }

  // ===== Harness =====

  /**
   * One posting registered on top of the shared interval: a store, the interval's end day, a node.
   */
  private record Registration(byte store, int toDay, long nodeKey) {
  }

  /** Collects the registrations a scenario makes so that every one of them is read back. */
  private static final class Scenario {
    private final HOTIndexWriter<ValidTimeKey> writer;
    private final List<Registration> registrations = new ArrayList<>();

    private Scenario(final HOTIndexWriter<ValidTimeKey> writer) {
      this.writer = writer;
    }

    /** Register {@code nodeKey} in one store of the interval from the first day to {@code toDay}. */
    private void register(final byte store, final int toDay, final long nodeKey) {
      writer.indexNodeKey(key(store, toDay), nodeKey);
      registrations.add(new Registration(store, toDay, nodeKey));
    }
  }

  /**
   * Register {@code records} records under the one interval spanning the whole horizon, apply the
   * scenario's {@code steps}, commit, and verify the committed trie: every structural invariant,
   * including that each stored key still routes to the leaf that holds it, and every posting readable
   * — the shared interval's exactly, since a misrouted chunk loses or duplicates postings.
   */
  private void loadAndVerify(final int records, final Consumer<Scenario> steps) {
    final Path databasePath = temporaryDirectory.resolve("db");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder(RESOURCE).build()));
      final Scenario scenario;
      try (JsonResourceSession session = database.beginResourceSession(RESOURCE);
          JsonNodeTrx wtx = session.beginNodeTrx()) {
        final HOTIndexWriter<ValidTimeKey> writer = HOTIndexWriter.create(wtx.getStorageEngineWriter(),
            ValidTimeKeySerializer.INSTANCE, IndexType.VALIDTIME, INDEX_NUMBER);
        for (int record = 0; record < records; record++) {
          writer.indexNodeKey(key(ValidTimeKey.STORE_LOWER, HORIZON_DAYS), recordKey(record));
          writer.indexNodeKey(key(ValidTimeKey.STORE_UPPER, HORIZON_DAYS), recordKey(record));
        }
        scenario = new Scenario(writer);
        steps.accept(scenario);
        wtx.commit();
      }

      try (JsonResourceSession session = database.beginResourceSession(RESOURCE);
          JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        HOTInvariantValidator.validateIndex(rtx.getStorageEngineReader(), IndexType.VALIDTIME, INDEX_NUMBER).assertOk();
        final HOTIndexReader<ValidTimeKey> reader = HOTIndexReader.create(rtx.getStorageEngineReader(),
            ValidTimeKeySerializer.INSTANCE, IndexType.VALIDTIME, INDEX_NUMBER);
        for (final byte store : new byte[] {ValidTimeKey.STORE_LOWER, ValidTimeKey.STORE_UPPER}) {
          final NodeReferences horizon = reader.get(key(store, HORIZON_DAYS), SearchMode.EQUAL);
          assertNotNull(horizon, "the shared interval must stay readable in store " + store);
          assertEquals(records, horizon.cardinality(), "postings of the shared interval in store " + store);
          assertTrue(horizon.isPresent(recordKey(0)) && horizon.isPresent(recordKey(records - 1)));
        }
        for (final Registration registration : scenario.registrations) {
          final NodeReferences postings = reader.get(key(registration.store, registration.toDay), SearchMode.EQUAL);
          assertNotNull(postings, registration + " must be readable");
          assertTrue(postings.isPresent(registration.nodeKey), registration + " must hold its node");
        }
      }
    }
  }

  private static long recordKey(final int record) {
    return FIRST_RECORD_KEY + RECORD_KEY_STRIDE * record;
  }

  /** The relational-interval-tree key of the interval from the first day to {@code toDay}. */
  private static ValidTimeKey key(final byte store, final int toDay) {
    final long lo = DOMAIN.lowerBound(FIRST_DAY);
    final long hi = DOMAIN.upperBound(FIRST_DAY.plus(toDay, ChronoUnit.DAYS));
    return new ValidTimeKey(store, RelationalIntervalTree.forkNode(lo, hi), store == ValidTimeKey.STORE_LOWER
        ? lo
        : hi);
  }
}
