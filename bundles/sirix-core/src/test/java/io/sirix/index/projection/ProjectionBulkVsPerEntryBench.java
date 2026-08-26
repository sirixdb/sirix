/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.JsonTestHelper;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.hot.BulkSpliceTestBridge;
import io.sirix.index.hot.HOTBulkBuilder;
import io.sirix.index.hot.PathKeySerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.LongUnaryOperator;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * PHASE-2 PERF GATE (campaign #76): per-entry {@code writeSlotValue} vs the bulk
 * build-and-splice path for the two projection write shapes the campaign targets.
 *
 * <ul>
 * <li><b>label</b> — order-directory shape: strictly ascending {@code 2^50 + k}, ~30 B
 * payloads (one slot per record);</li>
 * <li><b>segslot</b> — column-segment shape: {@code (rowGroupId << 16) | slotKind} with ~200
 * slots per row group, ascending overall.</li>
 * </ul>
 *
 * <p>Both arms run the PRODUCTION paths inside a real transaction: per-entry =
 * {@link ProjectionIndexHOTStorage#writeSlotValue} (descent + dispatch + put + split cascade);
 * bulk = key serialization + entry-list assembly + {@code spliceBulkBuiltRoot} (via
 * {@link BulkSpliceTestBridge}), i.e. everything a slot loader would do after accumulation.
 *
 * <p>Protocol: per shape and scale, a JIT warmup round of both arms (100 k), then interleaved
 * measured repetitions (per-entry, bulk, per-entry, bulk at 1 M; one repetition each at 10 M,
 * per-entry FIRST so any thermal drift penalizes the bulk arm — conservative for the claim under
 * test). Reported number = best repetition. Correctness witnesses run OUTSIDE the timed
 * regions: sampled read-back equality across arms and a zero-malformed-subtree check on the
 * bulk tree.
 */
final class ProjectionBulkVsPerEntryBench {

  private static final String RESOURCE_NAME = "bulk-vs-perentry";
  private static final Path DATABASE_PATH = JsonTestHelper.PATHS.PATH1.getFile();

  private static final long LABEL_BASE = 1L << 50;
  private static final int SLOTS_PER_ROW_GROUP = 200;
  private static final int PAYLOAD_BYTES = 30;
  private static final int WARMUP_ENTRIES = 100_000;
  private static final int READBACK_SAMPLES = 1_000;

  private static final LongUnaryOperator LABEL_KEYS = k -> LABEL_BASE + k;
  private static final LongUnaryOperator SEGSLOT_KEYS =
      k -> ((k / SLOTS_PER_ROW_GROUP) << 16) | (k % SLOTS_PER_ROW_GROUP);

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
    Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH)) {
      db.createResource(ResourceConfiguration.newBuilder(RESOURCE_NAME).build());
    }
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
    Databases.getGlobalBufferManager().clearAllCaches();
  }

  @Test
  void perfGate() {
    System.out.println("shape    scale     arm        best-ns/entry   total-ms   ratio(perEntry/bulk)");
    runShape("label", LABEL_KEYS, 1_000_000, 2);
    runShape("label", LABEL_KEYS, 10_000_000, 1);
    runShape("segslot", SEGSLOT_KEYS, 1_000_000, 2);
    runShape("segslot", SEGSLOT_KEYS, 10_000_000, 1);
  }

  private void runShape(final String shape, final LongUnaryOperator keyOf, final int entries, final int reps) {
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH);
        JsonResourceSession session = db.beginResourceSession(RESOURCE_NAME);
        JsonNodeTrx wtx = session.beginNodeTrx()) {
      final StorageEngineWriter sew = wtx.getStorageEngineWriter();
      int nextIndexNumber = 0;

      // JIT warmup — both arms, small scale, throwaway index numbers.
      runPerEntry(sew, nextIndexNumber++, keyOf, WARMUP_ENTRIES);
      runBulk(sew, nextIndexNumber++, keyOf, WARMUP_ENTRIES);

      long bestPerEntryNanos = Long.MAX_VALUE;
      long bestBulkNanos = Long.MAX_VALUE;
      ProjectionIndexHOTStorage lastPerEntry = null;
      ProjectionIndexHOTStorage lastBulk = null;
      for (int rep = 0; rep < reps; rep++) {
        final TimedStorage perEntry = timePerEntry(sew, nextIndexNumber++, keyOf, entries);
        final TimedStorage bulk = timeBulk(sew, nextIndexNumber++, keyOf, entries);
        bestPerEntryNanos = Math.min(bestPerEntryNanos, perEntry.nanos);
        bestBulkNanos = Math.min(bestBulkNanos, bulk.nanos);
        lastPerEntry = perEntry.storage;
        lastBulk = bulk.storage;
      }

      // Witnesses (untimed): sampled read-back equality across arms; bulk tree detector-clean.
      verifySampledEquality(lastPerEntry, lastBulk, keyOf, entries);
      assertEquals(0, BulkSpliceTestBridge.malformedSubtreeCount(lastBulk),
          shape + "/" + entries + ": bulk tree must be detector-clean");

      final double ratio = (double) bestPerEntryNanos / (double) bestBulkNanos;
      report(shape, entries, "perEntry", bestPerEntryNanos, Double.NaN);
      report(shape, entries, "bulk", bestBulkNanos, ratio);
    }
  }

  private record TimedStorage(long nanos, ProjectionIndexHOTStorage storage) {}

  private TimedStorage timePerEntry(final StorageEngineWriter sew, final int indexNumber,
      final LongUnaryOperator keyOf, final int entries) {
    final long start = System.nanoTime();
    final ProjectionIndexHOTStorage storage = runPerEntry(sew, indexNumber, keyOf, entries);
    return new TimedStorage(System.nanoTime() - start, storage);
  }

  private TimedStorage timeBulk(final StorageEngineWriter sew, final int indexNumber, final LongUnaryOperator keyOf,
      final int entries) {
    final long start = System.nanoTime();
    final ProjectionIndexHOTStorage storage = runBulk(sew, indexNumber, keyOf, entries);
    return new TimedStorage(System.nanoTime() - start, storage);
  }

  /** Per-entry arm: one production {@code writeSlotValue} per slot. */
  private ProjectionIndexHOTStorage runPerEntry(final StorageEngineWriter sew, final int indexNumber,
      final LongUnaryOperator keyOf, final int entries) {
    final ProjectionIndexHOTStorage storage = new ProjectionIndexHOTStorage(sew, indexNumber);
    final byte[] payload = new byte[PAYLOAD_BYTES];
    for (long k = 0; k < entries; k++) {
      fillPayload(payload, k);
      storage.writeSlotValue(keyOf.applyAsLong(k), payload);
    }
    return storage;
  }

  /**
   * Bulk arm: everything a slot loader does after accumulation — serialize each key
   * (sign-flipped 8-byte BE), copy the payload, assemble the exact-size entry list, build and
   * splice through the production {@code spliceBulkBuiltRoot}.
   */
  private ProjectionIndexHOTStorage runBulk(final StorageEngineWriter sew, final int indexNumber,
      final LongUnaryOperator keyOf, final int entries) {
    final ProjectionIndexHOTStorage storage = new ProjectionIndexHOTStorage(sew, indexNumber);
    final List<HOTBulkBuilder.Entry> list = new ArrayList<>(entries);
    for (long k = 0; k < entries; k++) {
      final byte[] key = new byte[8];
      PathKeySerializer.INSTANCE.serialize(keyOf.applyAsLong(k), key, 0);
      final byte[] payload = new byte[PAYLOAD_BYTES];
      fillPayload(payload, k);
      list.add(new HOTBulkBuilder.Entry(key, payload));
    }
    BulkSpliceTestBridge.spliceBulkBuiltRoot(storage, list);
    return storage;
  }

  /** Deterministic ~30 B payload derived from the slot ordinal. */
  private static void fillPayload(final byte[] payload, final long k) {
    for (int i = 0; i < payload.length; i++) {
      payload[i] = (byte) (k + i * 31);
    }
  }

  private static void verifySampledEquality(final ProjectionIndexHOTStorage perEntry,
      final ProjectionIndexHOTStorage bulk, final LongUnaryOperator keyOf, final int entries) {
    assertNotNull(perEntry, "per-entry arm missing");
    assertNotNull(bulk, "bulk arm missing");
    final byte[] expected = new byte[PAYLOAD_BYTES];
    final long step = Math.max(1L, (long) entries / READBACK_SAMPLES);
    for (long k = 0; k < entries; k += step) {
      final long slotKey = keyOf.applyAsLong(k);
      fillPayload(expected, k);
      final byte[] fromPerEntry = perEntry.getRawSlot(slotKey);
      final byte[] fromBulk = bulk.getRawSlot(slotKey);
      assertNotNull(fromPerEntry, "per-entry arm missing slot " + slotKey);
      assertNotNull(fromBulk, "bulk arm missing slot " + slotKey);
      assertArrayEquals(expected, fromPerEntry, "per-entry payload mismatch at slot " + slotKey);
      assertArrayEquals(expected, fromBulk, "bulk payload mismatch at slot " + slotKey);
    }
    // The extremes are always checked.
    final long last = entries - 1L;
    fillPayload(expected, last);
    assertArrayEquals(expected, perEntry.getRawSlot(keyOf.applyAsLong(last)), "per-entry last slot");
    assertArrayEquals(expected, bulk.getRawSlot(keyOf.applyAsLong(last)), "bulk last slot");
  }

  private static void report(final String shape, final int entries, final String arm, final long nanos,
      final double ratio) {
    final double nsPerEntry = (double) nanos / (double) entries;
    final String ratioText = Double.isNaN(ratio)
        ? "-"
        : String.format("%.2fx", ratio);
    System.out.printf("%-8s %-9d %-10s %13.1f %10.1f   %s%n", shape, entries, arm, nsPerEntry,
        nanos / 1_000_000.0, ratioText);
  }
}
