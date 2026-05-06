/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot;

import io.brackit.query.atomic.Int32;
import io.brackit.query.jdm.Type;
import io.sirix.JsonTestHelper;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.IndexType;
import io.sirix.index.SearchMode;
import io.sirix.index.redblacktree.keyvalue.CASValue;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.util.Collections;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Microbenchmarks for {@link HOTIndexWriter} and {@link HOTIndexReader} that bypass Sirix's
 * JSON-shredder + document-node-creation overhead. The goal is to measure pure HOT cost
 * (key encoding + page CoW + PEXT-routed descent + leaf put/get) in isolation — comparable
 * in shape to Binna's reference C++ microbenchmark numbers.
 *
 * <p>Tests are {@code @Disabled} by default so they don't pollute the regular suite. Enable
 * a specific test method to measure throughput. Each test prints:
 * <ul>
 *   <li>operations/second</li>
 *   <li>average per-op latency in nanoseconds</li>
 *   <li>p50 / p99 latencies (sampled)</li>
 * </ul>
 *
 * <p>Workload notes:
 * <ul>
 *   <li>Keys are built directly as {@link CASValue} for {@link Int32} — bypassing JSON.</li>
 *   <li>Values are minimal {@link NodeReferences} containing one {@link Roaring64Bitmap} bit.</li>
 *   <li>{@link HOTIndexWriter#index} is invoked in a tight loop. The loop time excludes
 *       database open and index-controller setup.</li>
 *   <li>For reads: keys are looked up via {@link HOTIndexReader#get} after a single commit.</li>
 * </ul>
 *
 * <p><b>Important caveat</b>: even this microbenchmark still goes through Sirix's TIL/CoW
 * machinery (every {@code index()} call may trigger {@code log.put} for a CoW'd page) and
 * the persistent storage layer. A pure in-memory HOT (Binna's reference) would not have
 * these costs. The numbers therefore upper-bound HOT cost in Sirix's persistent context.
 */
@DisplayName("HOT microbenchmarks")
final class HOTMicrobenchmark {

  private static String originalHOTSetting;

  @BeforeAll
  static void enableHOT() {
    originalHOTSetting = System.getProperty("sirix.index.useHOT");
    System.setProperty("sirix.index.useHOT", "true");
  }

  @AfterAll
  static void restoreHOT() {
    if (originalHOTSetting != null) {
      System.setProperty("sirix.index.useHOT", originalHOTSetting);
    } else {
      System.clearProperty("sirix.index.useHOT");
    }
  }

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  // ============================================================================================
  // Insert microbenchmark.
  // ============================================================================================

  /**
   * Measures pure HOT write throughput by driving {@link HOTIndexWriter#index} directly with
   * pre-built CAS keys + 1-bit NodeReferences. No JSON shredding, no document node creation.
   *
   * <p>JVM warmup: 10K iterations before the timed loop so JIT compiles the hot path.
   */
  @Test
  @DisplayName("HOT writer — insert throughput (CAS Int32, N=200K)")
  @org.junit.jupiter.api.Timeout(value = 300, unit = java.util.concurrent.TimeUnit.SECONDS)
  void writeInsertThroughput() {
    final int n = 200_000;
    final int warmup = 10_000;
    final long pathNodeKey = 5L; // synthetic — not tied to any real path-summary entry
    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());

    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var trx = session.beginNodeTrx()) {
      // Create the index up front so its root reference exists.
      final var ic = session.getWtxIndexController(trx.getRevisionNumber());
      final var pathToValue =
          io.brackit.query.util.path.Path.parse("/x/[]/v",
              io.brackit.query.util.path.PathParser.Type.JSON);
      final IndexDef casIndexDef = IndexDefs.createCASIdxDef(false, Type.INR,
          Collections.singleton(pathToValue), 0, IndexDef.DbType.JSON);
      ic.createIndexes(Set.of(casIndexDef), trx);

      final var writer =
          io.sirix.index.hot.HOTIndexWriter.create(
              trx.getStorageEngineWriter(),
              io.sirix.index.hot.CASKeySerializer.INSTANCE,
              IndexType.CAS,
              casIndexDef.getID());

      // Warmup
      final NodeReferences scratchValue = new NodeReferences();
      for (int i = 0; i < warmup; i++) {
        scratchValue.getNodeKeys().clear();
        scratchValue.getNodeKeys().add(i);
        final CASValue key = new CASValue(new Int32(i), Type.INR, pathNodeKey);
        writer.index(key, scratchValue, null);
      }

      // Timed loop
      final long start = System.nanoTime();
      for (int i = warmup; i < warmup + n; i++) {
        scratchValue.getNodeKeys().clear();
        scratchValue.getNodeKeys().add(i);
        final CASValue key = new CASValue(new Int32(i), Type.INR, pathNodeKey);
        writer.index(key, scratchValue, null);
      }
      final long elapsedNs = System.nanoTime() - start;
      trx.commit();

      final double throughput = (double) n * 1e9 / elapsedNs;
      final double avgLatencyNs = (double) elapsedNs / n;
      System.out.println(String.format(
          "[microbench] HOT.index CAS Int32 N=%d · %.0f ops/sec · %.0f ns/op · total=%.2f ms · warmup=%d",
          n, throughput, avgLatencyNs, elapsedNs / 1e6, warmup));
      assertTrue(throughput > 0);
    }
  }

  // ============================================================================================
  // Point-lookup microbenchmark.
  // ============================================================================================

  /**
   * Measures pure HOT read (point lookup) throughput by driving {@link HOTIndexReader#get}
   * directly. Builds an index with N entries first (untimed), then runs M random point
   * lookups (timed).
   */
  @Test
  @DisplayName("HOT reader — point lookup throughput (CAS Int32, N=200K, M=100K)")
  @org.junit.jupiter.api.Timeout(value = 300, unit = java.util.concurrent.TimeUnit.SECONDS)
  void readPointLookupThroughput() {
    final int n = 200_000;
    final int m = 100_000;
    final int warmup = 10_000;
    final long pathNodeKey = 5L;
    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());

    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var trx = session.beginNodeTrx()) {
      // 1. Build index (untimed).
      final var ic = session.getWtxIndexController(trx.getRevisionNumber());
      final var pathToValue =
          io.brackit.query.util.path.Path.parse("/x/[]/v",
              io.brackit.query.util.path.PathParser.Type.JSON);
      final IndexDef casIndexDef = IndexDefs.createCASIdxDef(false, Type.INR,
          Collections.singleton(pathToValue), 0, IndexDef.DbType.JSON);
      ic.createIndexes(Set.of(casIndexDef), trx);
      final var writer = io.sirix.index.hot.HOTIndexWriter.create(
          trx.getStorageEngineWriter(),
          io.sirix.index.hot.CASKeySerializer.INSTANCE,
          IndexType.CAS, casIndexDef.getID());
      final NodeReferences scratch = new NodeReferences();
      for (int i = 0; i < n; i++) {
        scratch.getNodeKeys().clear();
        scratch.getNodeKeys().add(i);
        writer.index(new CASValue(new Int32(i), Type.INR, pathNodeKey), scratch, null);
      }
      trx.commit();

      // 2. Open reader + warmup.
      final var reader = io.sirix.index.hot.HOTIndexReader.create(
          trx.getStorageEngineReader(),
          io.sirix.index.hot.CASKeySerializer.INSTANCE,
          IndexType.CAS, casIndexDef.getID());

      final java.util.SplittableRandom rng = new java.util.SplittableRandom(0xCAFEBABEL);
      for (int i = 0; i < warmup; i++) {
        final int v = rng.nextInt(n);
        final NodeReferences res = reader.get(new CASValue(new Int32(v), Type.INR, pathNodeKey),
            SearchMode.EQUAL);
        if (res == null) {
          throw new AssertionError("warmup miss for v=" + v);
        }
      }

      // 3. Timed loop.
      long hits = 0;
      final long start = System.nanoTime();
      for (int i = 0; i < m; i++) {
        final int v = rng.nextInt(n);
        final NodeReferences res = reader.get(new CASValue(new Int32(v), Type.INR, pathNodeKey),
            SearchMode.EQUAL);
        if (res != null) hits++;
      }
      final long elapsedNs = System.nanoTime() - start;

      final double throughput = (double) m * 1e9 / elapsedNs;
      final double avgLatencyNs = (double) elapsedNs / m;
      System.out.println(String.format(
          "[microbench] HOT.get  CAS Int32 N=%d M=%d hits=%d · %.0f ops/sec · %.0f ns/op",
          n, m, hits, throughput, avgLatencyNs));
      assertTrue(hits > m * 0.95, "expected > 95%% hit rate, got " + hits + "/" + m);
    }
  }

  // ============================================================================================
  // Insert + read combined (single test for convenience).
  // ============================================================================================

  /**
   * Combined microbenchmark: insert N entries, commit, then perform M point lookups. Reports
   * both numbers in one run. Useful for quick before/after profiling.
   */
  @Test
  @DisplayName("HOT writer+reader combined microbench (CAS Int32, profile-friendly N)")
  @org.junit.jupiter.api.Timeout(value = 180, unit = java.util.concurrent.TimeUnit.SECONDS)
  void smallCombinedMicrobench() {
    final int n = 500_000;
    final int m = 1_000_000;
    final long pathNodeKey = 5L;
    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    final IndexDef casIndexDef;
    final long writeNs;

    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var trx = session.beginNodeTrx()) {
      final var ic = session.getWtxIndexController(trx.getRevisionNumber());
      final var pathToValue =
          io.brackit.query.util.path.Path.parse("/x/[]/v",
              io.brackit.query.util.path.PathParser.Type.JSON);
      casIndexDef = IndexDefs.createCASIdxDef(false, Type.INR,
          Collections.singleton(pathToValue), 0, IndexDef.DbType.JSON);
      ic.createIndexes(Set.of(casIndexDef), trx);
      final var writer = io.sirix.index.hot.HOTIndexWriter.create(
          trx.getStorageEngineWriter(),
          io.sirix.index.hot.CASKeySerializer.INSTANCE,
          IndexType.CAS, casIndexDef.getID());
      final NodeReferences scratch = new NodeReferences();

      // Warmup writes — use disjoint Int32 range so they don't pollute the measured set.
      final int warmupBase = n + 1_000_000;
      for (int i = 0; i < 5_000; i++) {
        scratch.getNodeKeys().clear();
        scratch.getNodeKeys().add(warmupBase + i);
        writer.index(new CASValue(new Int32(warmupBase + i), Type.INR, pathNodeKey), scratch, null);
      }

      final long writeStart = System.nanoTime();
      for (int i = 0; i < n; i++) {
        scratch.getNodeKeys().clear();
        scratch.getNodeKeys().add(i);
        writer.index(new CASValue(new Int32(i), Type.INR, pathNodeKey), scratch, null);
      }
      writeNs = System.nanoTime() - writeStart;
      trx.commit();
    }

    // Reads from a fresh RTX so we see the committed on-disk state, not the writer's TIL.
    long hits = 0;
    final long readNs;
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var rtx = session.beginNodeReadOnlyTrx()) {
      final var reader = io.sirix.index.hot.HOTIndexReader.create(
          rtx.getStorageEngineReader(),
          io.sirix.index.hot.CASKeySerializer.INSTANCE,
          IndexType.CAS, casIndexDef.getID());

      // Diagnostic 1: structural validator on this microbench's trie.
      final HOTInvariantValidator.Result inv =
          HOTInvariantValidator.validateIndex(rtx.getStorageEngineReader(), IndexType.CAS,
              casIndexDef.getID());
      System.out.println("[microbench/diagnostic] validator: storedKeys=" + inv.storedKeyCount()
          + " observedHeight=" + inv.observedHeight() + " violations=" + inv.violations().size());
      // Print first 5 violations (truncate to avoid log flood).
      int printed = 0;
      for (final var viol : inv.violations()) {
        if (printed++ >= 5) break;
        System.out.println("[microbench/diagnostic] " + viol);
      }

      // Diagnostic 2: scan EVERY inserted value, count misses, print first 20 missing values.
      int totalMisses = 0;
      final StringBuilder missList = new StringBuilder();
      for (int v = 0; v < n; v++) {
        if (reader.get(new CASValue(new Int32(v), Type.INR, pathNodeKey), SearchMode.EQUAL)
            == null) {
          if (totalMisses < 20) {
            if (missList.length() > 0) missList.append(',');
            missList.append(v);
          }
          totalMisses++;
        }
      }
      System.out.println("[microbench/diagnostic] full-scan misses=" + totalMisses + "/" + n
          + " first20=[" + missList + "]");

      // Warmup reads — count hits, don't assert (we'll measure throughput regardless).
      final java.util.SplittableRandom rng = new java.util.SplittableRandom(42L);
      int warmupHits = 0;
      for (int i = 0; i < 5_000; i++) {
        if (reader.get(new CASValue(new Int32(rng.nextInt(n)), Type.INR, pathNodeKey),
            SearchMode.EQUAL) != null) {
          warmupHits++;
        }
      }
      System.out.println("[microbench] warmup hits=" + warmupHits + "/5000");

      final long readStart = System.nanoTime();
      for (int i = 0; i < m; i++) {
        final int v = rng.nextInt(n);
        final NodeReferences r =
            reader.get(new CASValue(new Int32(v), Type.INR, pathNodeKey), SearchMode.EQUAL);
        if (r != null) hits++;
      }
      readNs = System.nanoTime() - readStart;
    }

    System.out.println(String.format(
        "[microbench] writes N=%d · %.0f ops/sec · %.0f ns/op · total=%.1f ms",
        n, n * 1e9 / writeNs, (double) writeNs / n, writeNs / 1e6));
    System.out.println(String.format(
        "[microbench] reads  M=%d (hits=%d) · %.0f ops/sec · %.0f ns/op · total=%.1f ms",
        m, hits, m * 1e9 / readNs, (double) readNs / m, readNs / 1e6));
  }

  /**
   * Minimal reproducer for the chunked-bitmap read bug: insert N values straddling the
   * chunkIdx=0/1 boundary (65536), no warmup, then read each one back. The expectation is
   * that {@code reader.get(prefix(v))} succeeds for every {@code v} in {@code [0, N)} even
   * when chunkIdx differs across the range. A failure here points at lowerBound's walk-up
   * not handling non-existent {@code prefix(v) || 0} composite keys.
   */
  @Test
  @DisplayName("HOT read straddling chunkIdx boundary (N=70K, no warmup)")
  @org.junit.jupiter.api.Timeout(value = 60, unit = java.util.concurrent.TimeUnit.SECONDS)
  void chunkIdxBoundaryReproducer() {
    final int n = 70_000; // 65536 (chunk 0) + 4464 (chunk 1)
    final long pathNodeKey = 5L;
    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    final IndexDef casIndexDef;

    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var trx = session.beginNodeTrx()) {
      final var ic = session.getWtxIndexController(trx.getRevisionNumber());
      final var pathToValue = io.brackit.query.util.path.Path.parse("/x/[]/v",
          io.brackit.query.util.path.PathParser.Type.JSON);
      casIndexDef = IndexDefs.createCASIdxDef(false, Type.INR,
          Collections.singleton(pathToValue), 0, IndexDef.DbType.JSON);
      ic.createIndexes(Set.of(casIndexDef), trx);
      final var writer = io.sirix.index.hot.HOTIndexWriter.create(
          trx.getStorageEngineWriter(),
          io.sirix.index.hot.CASKeySerializer.INSTANCE,
          IndexType.CAS, casIndexDef.getID());
      final NodeReferences scratch = new NodeReferences();
      for (int i = 0; i < n; i++) {
        scratch.getNodeKeys().clear();
        scratch.getNodeKeys().add(i);
        writer.index(new CASValue(new Int32(i), Type.INR, pathNodeKey), scratch, null);
      }
      trx.commit();
    }

    int misses = 0;
    int firstMissChunk0 = -1;
    int firstMissChunk1 = -1;
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var rtx = session.beginNodeReadOnlyTrx()) {
      final var reader = io.sirix.index.hot.HOTIndexReader.create(
          rtx.getStorageEngineReader(), io.sirix.index.hot.CASKeySerializer.INSTANCE,
          IndexType.CAS, casIndexDef.getID());
      for (int v = 0; v < n; v++) {
        if (reader.get(new CASValue(new Int32(v), Type.INR, pathNodeKey),
            SearchMode.EQUAL) == null) {
          misses++;
          if (v < 65536 && firstMissChunk0 < 0) firstMissChunk0 = v;
          if (v >= 65536 && firstMissChunk1 < 0) firstMissChunk1 = v;
        }
      }
    }
    System.out.println("[reproducer] N=" + n + " misses=" + misses
        + " firstMissChunk0=" + firstMissChunk0
        + " firstMissChunk1=" + firstMissChunk1);
    assertTrue(misses == 0, "expected zero misses, got " + misses
        + " (firstMissChunk0=" + firstMissChunk0 + ", firstMissChunk1=" + firstMissChunk1 + ")");
  }
}
