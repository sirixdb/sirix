/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.index.projection.ProjectionColumnStore.ColumnSegmentFetcher;
import io.sirix.index.projection.ProjectionColumnStore.ColumnSlice;
import io.sirix.index.projection.ProjectionColumnStore.LeafColumnAccess;
import io.sirix.index.projection.ProjectionIndexHOTStorage.RowGroupDirectory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The hashed dictionary union behind an ungrouped {@code COUNT(DISTINCT dictColumn)}:
 * {@link ProjectionColumnScan#distinctDictUnion} over {@link DistinctHash128Set} and
 * {@link SharedDistinctHash128Set}.
 *
 * <p>
 * The truth every count is held against is a {@code HashSet<String>} of the PRESENT rows' values,
 * built while the leaves are — and every fixture carries more distinct values than the
 * content-based kernel's cardinality limit admits, so the tests exercise the regime the hashed
 * union exists for, with the bounded kernel's refusal witnessed beside it.
 * </p>
 */
final class DistinctDictUnionKernelTest {

  /** Column 0 the dict column under test, column 1 a long so the leaf has a second lane. */
  private static final byte[] KINDS =
      {ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT, ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG};

  /** Above the content-based kernel's default cardinality limit of 1024. */
  private static final int VOCABULARY = 3_000;

  private record Fixture(ProjectionColumnStore store, ColumnSegmentFetcher fetcher, Set<String> truth) {
  }

  private long previousHeadroom;
  private long previousBudget;
  private boolean previousResidency;

  @BeforeEach
  void setUp() {
    previousHeadroom = HeapHeadroom.setHeadroomForTesting(-1L);
    previousBudget = ProjectionColumnStore.setColumnFillBudgetBytesForTesting(Long.MAX_VALUE >> 1);
    previousResidency = ProjectionColumnStore.setResidencyHeadroomForTesting(false);
  }

  @AfterEach
  void tearDown() {
    ProjectionColumnStore.setResidencyHeadroomForTesting(previousResidency);
    ProjectionColumnStore.setColumnFillBudgetBytesForTesting(previousBudget);
    HeapHeadroom.setHeadroomForTesting(previousHeadroom);
    ProjectionColumnStore.sampleHeadroomShare();
  }

  // ---------------------------------------------------------------- the set

  @Test
  @DisplayName("the set answers add and contains exactly, the all-zero key included, across growths")
  void theSetHoldsKeysExactlyAcrossGrowths() {
    final DistinctHash128Set set = new DistinctHash128Set(0, null);
    final int initialCapacity = set.capacity();
    assertTrue(set.add(0L, 0L), "the all-zero key is a real key");
    assertFalse(set.add(0L, 0L), "…added once");
    assertTrue(set.contains(0L, 0L));
    assertEquals(1, set.size());

    final Random rnd = new Random(7L);
    final long[] keys = new long[2 * 20_000];
    for (int i = 0; i < keys.length; i += 2) {
      keys[i] = rnd.nextLong();
      keys[i + 1] = rnd.nextLong();
      assertTrue(set.add(keys[i], keys[i + 1]), "a fresh random key is new");
    }
    for (int i = 0; i < keys.length; i += 2) {
      assertFalse(set.add(keys[i], keys[i + 1]), "a repeated key is not new");
      assertTrue(set.contains(keys[i], keys[i + 1]));
    }
    assertEquals(1 + keys.length / 2, set.size());
    assertTrue(set.capacity() > initialCapacity, "20K keys must have grown the initial table");
    assertTrue(set.capacity() >= 2 * (keys.length / 2), "the load stays at or below one half");
    assertFalse(set.contains(1L, 2L), "an absent key is absent");

    // Keys that share the LOW half — the probe's index — but differ in the high half are distinct.
    assertTrue(set.add(5L, 6L));
    assertTrue(set.add(5L, 7L));
    assertFalse(set.add(5L, 6L));
    assertTrue(set.contains(5L, 7L));
    assertFalse(set.contains(5L, 8L));
  }

  @Test
  @DisplayName("capacityFor sizes at half load and refuses what the table cannot hold")
  void capacityForSizesAtHalfLoad() {
    assertEquals(256, DistinctHash128Set.capacityFor(0L));
    assertEquals(256, DistinctHash128Set.capacityFor(128L));
    assertEquals(512, DistinctHash128Set.capacityFor(129L));
    assertEquals(1 << 20, DistinctHash128Set.capacityFor(1L << 19));
    assertEquals(1 << 21, DistinctHash128Set.capacityFor((1L << 19) + 1));
    assertThrows(IllegalArgumentException.class, () -> DistinctHash128Set.capacityFor(1L << 40));
  }

  @Test
  @DisplayName("addAll unions two sets and grows the target at most once")
  void addAllUnionsTwoSets() {
    final DistinctHash128Set a = new DistinctHash128Set(0, null);
    final DistinctHash128Set b = new DistinctHash128Set(0, null);
    for (long i = 1; i <= 1_000; i++) {
      a.add(i, -i);
    }
    for (long i = 500; i <= 1_500; i++) {
      b.add(i, -i);
    }
    b.add(0L, 0L);
    a.addAll(b);
    assertEquals(1_501, a.size(), "1..1500 plus the zero key");
    for (long i = 1; i <= 1_500; i++) {
      assertTrue(a.contains(i, -i));
    }
    assertTrue(a.contains(0L, 0L), "the zero flag travels with addAll");
    a.addAll(a);
    assertEquals(1_501, a.size(), "a self-merge is a no-op");
  }

  @Test
  @DisplayName("every array is charged to the budget, a refused growth throws and refunds")
  void arraysAreChargedAndARefusedGrowthRefunds() {
    final long initialBytes = 256L * DistinctHash128Set.BYTES_PER_SLOT;
    // Room for the initial table and ONE doubling, not two.
    final AtomicLong budget = new AtomicLong(initialBytes + 2 * initialBytes);
    final DistinctHash128Set set = new DistinctHash128Set(0, budget);
    assertEquals(initialBytes, set.chargedBytes());
    assertEquals(2 * initialBytes, budget.get(), "the initial table is charged on construction");

    int added = 0;
    while (set.size() < 128) {
      set.add(added + 1, 1L);
      added++;
    }
    assertEquals(256, set.capacity(), "128 keys sit exactly at half load");
    set.add(added + 1, 1L);
    added++;
    assertEquals(512, set.capacity(), "the 129th key doubles the table");
    assertEquals(2 * initialBytes, set.chargedBytes(), "the old table's bytes were released");
    assertEquals(initialBytes, budget.get(), "…back to the budget");

    while (set.size() < 256) {
      set.add(added + 1, 1L);
      added++;
    }
    final int before = added;
    final DistinctHash128Set.ByteBudgetExceededException refused =
        assertThrows(DistinctHash128Set.ByteBudgetExceededException.class, () -> set.add(before + 1, 1L),
            "the second doubling needs 4 * initial bytes with 1 * initial left");
    assertTrue(refused.getMessage().contains("refused"), refused.getMessage());
    assertTrue(refused instanceof IllegalStateException, "the executor rethrows IllegalStateException as-is");
    assertEquals(initialBytes, budget.get(), "a refused charge is refunded in full");
    assertEquals(2 * initialBytes, set.chargedBytes(), "the set still holds exactly its table");
    assertEquals(512, set.capacity());

    assertThrows(DistinctHash128Set.ByteBudgetExceededException.class,
        () -> new DistinctHash128Set(1 << 16, new AtomicLong(1L)), "a refused initial table throws too");
  }

  // ------------------------------------------------------------- the kernel

  @Test
  @DisplayName("the union over resident slices counts exactly the present distinct values, above the card limit")
  void theUnionCountsThePresentDistinctValues() {
    final Fixture f = build(12, 512, 0.15, false, 20260901L);
    assertTrue(f.truth().size() > 1024,
        "the fixture must exceed the content-based kernel's limit: " + f.truth().size());
    assertFalse(f.truth().contains(""), "no present row carries \"\" in this fixture");

    final ColumnSlice[] slices = f.store().column(0, f.fetcher());
    assertNotNull(slices);
    final int leaves = f.store().rowGroupCount();
    assertNull(ProjectionColumnScan.distinctPresentStrings(slices, 0, leaves, 1024),
        "the content-based kernel bails past its limit — the regime the hashed union exists for");

    final LeafColumnAccess access = f.store().residentLeafAccess(f.fetcher(), null);
    final DistinctHash128Set set = new DistinctHash128Set(0, null);
    assertTrue(ProjectionColumnScan.distinctDictUnion(access, 0, 0, leaves, set, new long[2]));
    assertEquals(f.truth().size(), set.size(), "hashed union == the present rows' distinct values");

    // The union is idempotent: a second pass over the same leaves adds nothing.
    assertTrue(ProjectionColumnScan.distinctDictUnion(access, 0, 0, leaves, set, new long[2]));
    assertEquals(f.truth().size(), set.size());

    // MUTATION: the truth shrinks when a leaf is left out, and so does the count.
    final DistinctHash128Set partial = new DistinctHash128Set(0, null);
    assertTrue(ProjectionColumnScan.distinctDictUnion(access, 0, 1, leaves, partial, new long[2]));
    assertTrue(partial.size() < set.size(), "leaf 0 must carry values no other leaf has");
  }

  @Test
  @DisplayName("a \"\" a MISSING row interned is a phantom; a \"\" a present row carries is counted once")
  void thePhantomEmptyIsExcludedAndTheRealEmptyCountedOnce() {
    final Fixture phantom = build(8, 400, 0.20, false, 11L);
    assertFalse(phantom.truth().contains(""));
    final int leaves = phantom.store().rowGroupCount();
    final LeafColumnAccess access = phantom.store().residentLeafAccess(phantom.fetcher(), null);
    final DistinctHash128Set set = new DistinctHash128Set(0, null);
    assertTrue(ProjectionColumnScan.distinctDictUnion(access, 0, 0, leaves, set, new long[2]));
    assertEquals(phantom.truth().size(), set.size(), "the phantom \"\" of the missing rows is not a value");

    final Fixture real = build(8, 400, 0.20, true, 11L);
    assertTrue(real.truth().contains(""), "some present row carries \"\"");
    final int realLeaves = real.store().rowGroupCount();
    final LeafColumnAccess realAccess = real.store().residentLeafAccess(real.fetcher(), null);
    final DistinctHash128Set realSet = new DistinctHash128Set(0, null);
    assertTrue(ProjectionColumnScan.distinctDictUnion(realAccess, 0, 0, realLeaves, realSet, new long[2]));
    assertEquals(real.truth().size(), realSet.size(), "the real \"\" counts exactly once across every leaf");
    assertEquals(phantom.truth().size() + 1, real.truth().size(),
        "same seed, same vocabulary: the two fixtures differ by the \"\" alone");
  }

  @Test
  @DisplayName("windowed per-worker access over window-aligned chunks into ONE shared set agrees with the resident count")
  void windowedWorkersIntoASharedSetAgreeWithTheResidentCount() throws InterruptedException {
    final Fixture f = build(14, 300, 0.10, true, 5L);
    final int leaves = f.store().rowGroupCount();
    final int window = 4;
    final int windows = (leaves + window - 1) / window;
    final int workers = Math.min(3, windows);
    final int chunk = ((windows + workers - 1) / workers) * window;
    // Window-aligned chunks can leave a trailing worker without a range (14 leaves, windows of 4,
    // three workers: chunks of 8 leaves cover it in two) — exactly what the executor's driver does.
    int active = 0;
    for (int w = 0; w < workers; w++) {
      if (w * chunk < leaves) {
        active++;
      }
    }
    assertEquals(2, active, "the fixture is shaped so one worker sits idle");

    final AtomicLong budget = new AtomicLong(64L << 20);
    final SharedDistinctHash128Set shared = new SharedDistinctHash128Set(8, 16, 3, budget);
    final Thread[] threads = new Thread[workers];
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final boolean[] declined = new boolean[workers];
    final long windowedBefore = ProjectionColumnStore.windowedLeafAccessCount();
    for (int w = 0; w < workers; w++) {
      final int idx = w;
      threads[w] = new Thread(() -> {
        try {
          final int from = idx * chunk;
          final int to = Math.min(from + chunk, leaves);
          if (from >= to) {
            return;
          }
          final LeafColumnAccess access = f.store().windowedLeafAccess(f.fetcher(), null, window, window);
          final SharedDistinctHash128Set.Worker worker = shared.worker();
          if (!ProjectionColumnScan.distinctDictUnion(access, 0, from, to, worker, new long[2])) {
            declined[idx] = true;
            return;
          }
          worker.flush();
        } catch (final Throwable t) {
          failure.set(t);
        }
      }, "dict-union-" + w);
      threads[w].start();
    }
    for (final Thread t : threads) {
      t.join();
    }
    assertNull(failure.get(), () -> "a worker failed: " + failure.get());
    for (final boolean d : declined) {
      assertFalse(d, "no worker may decline on a well-formed store");
    }
    assertEquals(f.truth().size(), shared.size(), "the shared set's size is the exact distinct count");
    assertEquals(windowedBefore + active, ProjectionColumnStore.windowedLeafAccessCount(),
        "every active worker took exactly one windowed access");
    assertEquals(0L, f.store().retainedFillBytes(), "the windowed route retains nothing");
    assertTrue(budget.get() < 64L << 20, "the partitions and buffers were charged to the shared budget");

    // The same count through the resident slices, single-threaded, into a plain set.
    final ColumnSlice[] slices = f.store().column(0, f.fetcher());
    assertNotNull(slices);
    final DistinctHash128Set resident = new DistinctHash128Set(0, null);
    assertTrue(ProjectionColumnScan.distinctDictUnion(f.store().residentLeafAccess(f.fetcher(), null), 0, 0, leaves,
        resident, new long[2]));
    assertEquals(shared.size(), resident.size(), "resident and windowed unions must agree");
  }

  @Test
  @DisplayName("a shared set whose budget cannot hold the answer refuses from inside a worker and refunds")
  void aStarvedSharedSetRefusesFromInsideAWorker() {
    final Fixture f = build(6, 512, 0.0, false, 3L);
    final int leaves = f.store().rowGroupCount();
    // Enough for the 4 initial partitions and one worker's buffers, not for the growths ~3000 keys
    // need.
    final long partitionBytes = 4L * 256 * DistinctHash128Set.BYTES_PER_SLOT;
    final long bufferBytes = 4L * 8 * DistinctHash128Set.BYTES_PER_SLOT;
    final AtomicLong budget = new AtomicLong(partitionBytes + bufferBytes + 1024L);
    final SharedDistinctHash128Set shared = new SharedDistinctHash128Set(4, 0, 8, budget);
    final SharedDistinctHash128Set.Worker worker = shared.worker();
    assertEquals(1024L, budget.get(), "partitions and buffers charged exactly");

    final LeafColumnAccess access = f.store().windowedLeafAccess(f.fetcher(), null, 2, 2);
    assertThrows(DistinctHash128Set.ByteBudgetExceededException.class,
        () -> ProjectionColumnScan.distinctDictUnion(access, 0, 0, leaves, worker, new long[2]),
        "the first refused growth escapes the kernel from inside the worker's drain");
    assertTrue(budget.get() >= 0L, "a refusal never leaves the budget overdrawn");

    assertThrows(DistinctHash128Set.ByteBudgetExceededException.class,
        () -> new SharedDistinctHash128Set(4, 1 << 12, 8, new AtomicLong(1L)), "starved on construction too");
    assertThrows(DistinctHash128Set.ByteBudgetExceededException.class,
        () -> new SharedDistinctHash128Set(4, 0, 8, new AtomicLong(partitionBytes)).worker(),
        "starved on the worker's buffers too");
  }

  @Test
  @DisplayName("the kernel refuses a set of the wrong shape loudly")
  void theKernelRefusesAWrongScratch() {
    final Fixture f = build(2, 8, 0.0, false, 1L);
    final LeafColumnAccess access = f.store().residentLeafAccess(f.fetcher(), null);
    assertThrows(IllegalArgumentException.class,
        () -> ProjectionColumnScan.distinctDictUnion(access, 0, 0, 2, new DistinctHash128Set(0, null), new long[1]));
    assertThrows(IllegalArgumentException.class, () -> new SharedDistinctHash128Set(3, 0, 8, null),
        "partitions must be a power of two");
    assertThrows(IllegalArgumentException.class, () -> new SharedDistinctHash128Set(4, 0, 0, null),
        "a worker must buffer at least one key");
  }

  // ------------------------------------------------------------- fixture

  /**
   * A store of {@code leaves} leaves × {@code rowsPerLeaf} rows whose dict column draws from a
   * vocabulary of {@link #VOCABULARY} phrases; a {@code missingRate} share of the rows is MISSING the
   * field (interning the "" default, a phantom) and, when {@code realEmpty}, one extra present row
   * per leaf carries "" for real. The truth is the present rows' distinct values.
   */
  private static Fixture build(final int leaves, final int rowsPerLeaf, final double missingRate,
      final boolean realEmpty, final long seed) {
    final Random rnd = new Random(seed);
    final Map<Long, byte[]> segmentsByOffset = new HashMap<>();
    final List<RowGroupDirectory> directories = new ArrayList<>(leaves);
    final Set<String> truth = new HashSet<>();
    long nextOffset = 1_000;
    for (int leaf = 0; leaf < leaves; leaf++) {
      final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(KINDS.clone());
      final long[] longs = new long[KINDS.length];
      final boolean[] bools = new boolean[KINDS.length];
      final String[] strings = new String[KINDS.length];
      final boolean[] present = new boolean[KINDS.length];
      final boolean[] unrepresentable = new boolean[KINDS.length];
      long recordKey = leaf * 100_000L + 1;
      for (int row = 0; row < rowsPerLeaf; row++) {
        longs[1] = row;
        present[1] = true;
        final boolean missing = rnd.nextDouble() < missingRate;
        if (missing) {
          present[0] = false;
          strings[0] = "";
        } else {
          present[0] = true;
          // Leaf 0 owns a private slice of the vocabulary so leaving it out shrinks the truth.
          final int id = leaf == 0 && row < 64
              ? row
              : 64 + rnd.nextInt(VOCABULARY - 64);
          strings[0] = "phrase " + id + " " + Integer.toHexString(id * 31);
          truth.add(strings[0]);
        }
        assertTrue(page.appendRow(recordKey++, longs, bools, strings, present, unrepresentable));
      }
      if (realEmpty) {
        // One more PRESENT row carrying "" for real — appended after the loop so the two variants of a
        // seed draw the same random rows and differ by this value alone.
        present[0] = true;
        strings[0] = "";
        longs[1] = rowsPerLeaf;
        truth.add("");
        assertTrue(page.appendRow(recordKey++, longs, bools, strings, present, unrepresentable));
      }
      final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded =
          ProjectionIndexColumnSegmentCodec.encode(page.serialize());
      final int segments = encoded.columnSegmentIds().length;
      final int[] ids = new int[segments];
      final long[] offsets = new long[segments];
      for (int i = 0; i < segments; i++) {
        ids[i] = encoded.columnSegmentIds()[i];
        offsets[i] = nextOffset;
        segmentsByOffset.put(nextOffset, encoded.segments()[i]);
        nextOffset += 1 + encoded.segments()[i].length;
      }
      directories.add(new RowGroupDirectory(leaf + 1, encoded.descriptor(), ids, offsets, new byte[ids.length][]));
    }
    final ColumnSegmentFetcher fetcher = wanted -> {
      final byte[][] out = new byte[wanted.length][];
      for (int i = 0; i < wanted.length; i++) {
        out[i] = segmentsByOffset.get(wanted[i]);
      }
      return out;
    };
    return new Fixture(new ProjectionColumnStore(directories), fetcher, truth);
  }
}
