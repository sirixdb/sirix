package io.sirix.index.projection;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Adversarial gate for the string-identity proof.
 *
 * <p>
 * The failure this guards is the one a fingerprint pair CANNOT report: when two distinct strings
 * share both lanes, the group table's identity comparison succeeds, so it folds the two groups and
 * never sets {@code hasProbeKeyCollision()}. No amount of probing catches it — only comparing the
 * bytes does. Since two strings colliding in both real 64-bit functions cannot be constructed, the
 * collision is INJECTED, which is the only way to execute the byte comparison and the decline.
 */
final class ProjectionStringIdentityRegistryTest {

  /** Collapses every value onto one fingerprint pair — a worst-case adversary. */
  private static final ProjectionStringIdentityRegistry.Fingerprint ALL_COLLIDE =
      new ProjectionStringIdentityRegistry.Fingerprint() {

        @Override
        public long primary(final byte[] utf8, final int off, final int len, final long fnv1a64) {
          return 0xDEADBEEFL;
        }

        @Override
        public long secondary(final byte[] utf8, final int off, final int len) {
          return 0xFEEDFACEL;
        }
      };

  /** Collides only on LENGTH, so equal-length distinct strings share a pair. */
  private static final ProjectionStringIdentityRegistry.Fingerprint LENGTH_ONLY =
      new ProjectionStringIdentityRegistry.Fingerprint() {

        @Override
        public long primary(final byte[] utf8, final int off, final int len, final long fnv1a64) {
          return len;
        }

        @Override
        public long secondary(final byte[] utf8, final int off, final int len) {
          return ~len;
        }
      };

  private static byte[] utf8(final String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }

  private static boolean prove(final ProjectionStringIdentityRegistry registry, final int component,
      final String value) {
    final byte[] bytes = utf8(value);
    final long a = registry.laneA(bytes, 0, bytes.length, 0L);
    final long b = registry.laneB(bytes, 0, bytes.length);
    return registry.prove(component, a, b, bytes, 0, bytes.length);
  }

  @Test
  @DisplayName("the same value recurring under one fingerprint is proven, not flagged")
  void repeatedValueIsProven() {
    final ProjectionStringIdentityRegistry registry = new ProjectionStringIdentityRegistry(1, ALL_COLLIDE, 1 << 20);
    assertTrue(prove(registry, 0, "gold"));
    assertTrue(prove(registry, 0, "gold"), "the same bytes must re-prove, however weak the fingerprint");
    assertTrue(registry.identityProven());
    assertFalse(registry.collisionDetected());
  }

  @Test
  @DisplayName("two distinct strings forced onto one fingerprint pair are caught by byte comparison")
  void injectedFingerprintCollisionIsCaught() {
    final ProjectionStringIdentityRegistry registry = new ProjectionStringIdentityRegistry(1, ALL_COLLIDE, 1 << 20);
    assertTrue(prove(registry, 0, "gold"));
    assertFalse(prove(registry, 0, "silver"), "a different value under the same pair must NOT be proven");
    assertTrue(registry.collisionDetected(), "the collision must latch");
    assertFalse(registry.identityProven(), "the scan must be told it cannot trust its identity lanes");
  }

  @Test
  @DisplayName("equal-length distinct strings collide under a length-only fingerprint and are caught")
  void equalLengthCollisionIsCaught() {
    final ProjectionStringIdentityRegistry registry = new ProjectionStringIdentityRegistry(1, LENGTH_ONLY, 1 << 20);
    assertTrue(prove(registry, 0, "abcd"));
    assertTrue(prove(registry, 0, "abcd"));
    assertFalse(prove(registry, 0, "wxyz"), "same length, different bytes: not the same group");
    assertTrue(registry.collisionDetected());
  }

  @Test
  @DisplayName("components are proven independently")
  void componentsAreIndependent() {
    final ProjectionStringIdentityRegistry registry = new ProjectionStringIdentityRegistry(2, ALL_COLLIDE, 1 << 20);
    assertTrue(prove(registry, 0, "gold"));
    // The SAME forced pair in another component is another domain and must not be a collision.
    assertTrue(prove(registry, 1, "silver"));
    assertTrue(registry.identityProven());
    assertFalse(prove(registry, 0, "bronze"), "component 0 still holds gold under that pair");
    assertTrue(registry.collisionDetected());
  }

  @Test
  @DisplayName("a value differing only past a shared prefix is still caught")
  void prefixSharingValuesAreCaught() {
    final ProjectionStringIdentityRegistry registry = new ProjectionStringIdentityRegistry(1, ALL_COLLIDE, 1 << 20);
    assertTrue(prove(registry, 0, "https://example.com/a"));
    assertFalse(prove(registry, 0, "https://example.com/b"));
    assertTrue(registry.collisionDetected());
  }

  @Test
  @DisplayName("a value that is a strict prefix of the canonical one is not equal to it")
  void prefixIsNotEquality() {
    final ProjectionStringIdentityRegistry registry = new ProjectionStringIdentityRegistry(1, ALL_COLLIDE, 1 << 20);
    assertTrue(prove(registry, 0, "abcdef"));
    assertFalse(prove(registry, 0, "abc"), "a prefix is a different value");
    assertTrue(registry.collisionDetected());
  }

  @Test
  @DisplayName("exhausting the canonical-byte budget declines rather than stopping the proof")
  void budgetExhaustionDeclinesConservatively() {
    // Budget fits exactly one entry — its four value bytes PLUS the per-entry overhead charge, so
    // that the budget bounds table and header footprint and not merely the strings.
    final ProjectionStringIdentityRegistry registry =
        new ProjectionStringIdentityRegistry(1, ProjectionStringIdentityRegistry.DEFAULT_FINGERPRINT,
            4L + ProjectionStringIdentityRegistry.ENTRY_OVERHEAD_BYTES);
    assertTrue(prove(registry, 0, "abcd"));
    assertFalse(prove(registry, 0, "efgh"));
    assertTrue(registry.unproven(), "running out of budget must decline, never silently stop proving");
    assertFalse(registry.collisionDetected(), "budget exhaustion is not a collision");
    assertFalse(registry.identityProven());
  }

  @Test
  @DisplayName("the local proof cache never turns an unproven pair into a proven one")
  void localCacheCannotLaunder() {
    final ProjectionStringIdentityRegistry registry = new ProjectionStringIdentityRegistry(1, ALL_COLLIDE, 1 << 20);
    final ProjectionStringIdentityRegistry.LocalProofCache cache =
        new ProjectionStringIdentityRegistry.LocalProofCache(1);
    final byte[] gold = utf8("gold");
    final byte[] silver = utf8("silver");
    final long a = registry.laneA(gold, 0, gold.length, 0L);
    final long b = registry.laneB(gold, 0, gold.length);
    assertTrue(cache.prove(registry, 0, a, b, gold, 0, gold.length));
    assertTrue(cache.prove(registry, 0, a, b, gold, 0, gold.length), "cache hit on identical bytes");
    assertFalse(cache.prove(registry, 0, a, b, silver, 0, silver.length),
        "the cache compares bytes too, so a colliding value still reaches the registry");
    assertTrue(registry.collisionDetected());
  }

  @Test
  @DisplayName("growth preserves every canonical value")
  void growthPreservesCanonicalValues() {
    final ProjectionStringIdentityRegistry registry =
        new ProjectionStringIdentityRegistry(1, ProjectionStringIdentityRegistry.DEFAULT_FINGERPRINT, 1 << 24);
    for (int i = 0; i < 20_000; i++) {
      assertTrue(prove(registry, 0, "value-" + i), "insert " + i);
    }
    for (int i = 0; i < 20_000; i++) {
      assertTrue(prove(registry, 0, "value-" + i), "re-prove after growth " + i);
    }
    assertTrue(registry.identityProven());
  }

  @Test
  @DisplayName("a thrashing high-cardinality load allocates nothing in the local cache")
  void localCacheIsAllocationFreeUnderThrashing() {
    final com.sun.management.ThreadMXBean threads =
        java.lang.management.ManagementFactory.getPlatformMXBean(com.sun.management.ThreadMXBean.class);
    org.junit.jupiter.api.Assumptions.assumeTrue(threads != null && threads.isThreadAllocatedMemorySupported(),
        "thread allocation accounting unavailable");

    // 40k distinct values against a 1k-slot cache: every lookup evicts. The registry is warmed
    // first so its own canonical copies are NOT attributed to the measured loop.
    final int distinct = 40_000;
    final ProjectionStringIdentityRegistry registry =
        new ProjectionStringIdentityRegistry(1, ProjectionStringIdentityRegistry.DEFAULT_FINGERPRINT, 1 << 24);
    final byte[][] values = new byte[distinct][];
    for (int i = 0; i < distinct; i++) {
      values[i] = utf8("https://example.com/path/" + i);
      assertTrue(prove(registry, 0, "https://example.com/path/" + i));
    }
    final long[] lanesA = new long[distinct];
    final long[] lanesB = new long[distinct];
    for (int i = 0; i < distinct; i++) {
      lanesA[i] = registry.laneA(values[i], 0, values[i].length, 0L);
      lanesB[i] = registry.laneB(values[i], 0, values[i].length);
    }
    final ProjectionStringIdentityRegistry.LocalProofCache cache =
        new ProjectionStringIdentityRegistry.LocalProofCache(1);
    // Warm up so JIT compilation is not attributed to the measured window.
    for (int round = 0; round < 2; round++) {
      for (int i = 0; i < distinct; i++) {
        cache.prove(registry, 0, lanesA[i], lanesB[i], values[i], 0, values[i].length);
      }
    }

    final long before = threads.getCurrentThreadAllocatedBytes();
    for (int round = 0; round < 5; round++) {
      for (int i = 0; i < distinct; i++) {
        assertTrue(cache.prove(registry, 0, lanesA[i], lanesB[i], values[i], 0, values[i].length));
      }
    }
    final long allocated = threads.getCurrentThreadAllocatedBytes() - before;

    // 200k evicting lookups. The arena is fixed and an eviction OVERWRITES its slot, so the only
    // allowance here is JIT/bookkeeping noise — a per-eviction copy would land in the megabytes.
    assertTrue(allocated < 64L * 1024,
        "local proof cache allocated " + allocated + " bytes over 200k evicting lookups");
  }

  @Test
  @DisplayName("the byte budget is compared without overflowing and the range is validated")
  void budgetIsOverflowSafeAndRangeIsChecked() {
    final ProjectionStringIdentityRegistry huge =
        new ProjectionStringIdentityRegistry(1, ProjectionStringIdentityRegistry.DEFAULT_FINGERPRINT, Long.MAX_VALUE);
    assertTrue(prove(huge, 0, "value"), "a Long.MAX_VALUE budget must not overflow into a refusal");
    assertTrue(huge.identityProven());

    final ProjectionStringIdentityRegistry registry = new ProjectionStringIdentityRegistry(1);
    final byte[] bytes = utf8("abcdef");
    final long a = registry.laneA(bytes, 0, bytes.length, 0L);
    final long b = registry.laneB(bytes, 0, bytes.length);
    assertThrows(NullPointerException.class, () -> registry.prove(0, a, b, null, 0, 1));
    assertThrows(IllegalArgumentException.class, () -> registry.prove(0, a, b, bytes, -1, 2));
    assertThrows(IllegalArgumentException.class, () -> registry.prove(0, a, b, bytes, 0, -1));
    assertThrows(IllegalArgumentException.class, () -> registry.prove(0, a, b, bytes, 4, 5));
  }

  @Test
  @DisplayName("a latched collision stops a worker even on a local cache hit")
  void latchedCollisionIsVisibleThroughTheCache() {
    final ProjectionStringIdentityRegistry registry =
        new ProjectionStringIdentityRegistry(1, ProjectionStringIdentityRegistry.DEFAULT_FINGERPRINT, 1 << 20);
    final ProjectionStringIdentityRegistry.LocalProofCache cache =
        new ProjectionStringIdentityRegistry.LocalProofCache(1);
    final byte[] gold = utf8("gold");
    final long a = registry.laneA(gold, 0, gold.length, 0L);
    final long b = registry.laneB(gold, 0, gold.length);
    assertTrue(cache.prove(registry, 0, a, b, gold, 0, gold.length));
    assertTrue(cache.prove(registry, 0, a, b, gold, 0, gold.length), "warm the slot");

    // Another worker disproves identity. This worker's next value is a CACHE HIT, and must still
    // stop rather than run its whole leaf range on lanes known to be untrustworthy.
    final byte[] other = utf8("silver");
    final long a2 = registry.laneA(other, 0, other.length, 0L);
    registry.prove(0, a2, b, other, 0, other.length);
    registry.prove(0, a2, b, utf8("bronze"), 0, 6);
    assertTrue(registry.collisionDetected(), "precondition: the registry latched");
    assertFalse(cache.prove(registry, 0, a, b, gold, 0, gold.length),
        "a cache hit must not hide a latched collision from this worker");
  }

  @Test
  @DisplayName("millions of tiny distinct values decline on ENTRY count, not just on value bytes")
  void entryOverheadIsChargedSoTinyValuesCannotExhaustMemory() {
    // 1 MiB of budget. Counting only value bytes, 1-byte values would admit ~1 000 000 entries —
    // whose lanes, references, liveness flags and per-array headers are two orders of magnitude
    // larger than the strings. Charging ENTRY_OVERHEAD_BYTES makes the budget bound entries too.
    final long budget = 1L << 20;
    final ProjectionStringIdentityRegistry registry =
        new ProjectionStringIdentityRegistry(1, ProjectionStringIdentityRegistry.DEFAULT_FINGERPRINT, budget);
    int admitted = 0;
    for (int i = 0; i < 200_000 && registry.identityProven(); i++) {
      if (prove(registry, 0, Integer.toString(i, Character.MAX_RADIX))) {
        admitted++;
      }
    }
    assertTrue(registry.unproven(), "the registry must run out of budget and DECLINE");
    final long ceiling = budget / ProjectionStringIdentityRegistry.ENTRY_OVERHEAD_BYTES;
    assertTrue(admitted <= ceiling,
        "admitted " + admitted + " entries, above the " + ceiling + " the entry charge allows");
    // Sanity: a byte-only budget would have admitted far more than this.
    assertTrue(admitted < 100_000, "entry overhead was not charged: " + admitted + " entries admitted");
  }

  @Test
  @DisplayName("component counts are bounded before any array is sized")
  void componentCountIsBoundedBeforeSizing() {
    final int tooMany = CompositeGroupIdentity.MAX_KEY_COMPONENTS + 1;
    assertThrows(IllegalArgumentException.class, () -> new ProjectionStringIdentityRegistry(tooMany));
    assertThrows(IllegalArgumentException.class, () -> new ProjectionStringIdentityRegistry.LocalProofCache(tooMany));
    assertThrows(IllegalArgumentException.class,
        () -> new ProjectionStringIdentityRegistry.LocalProofCache(Integer.MAX_VALUE));
  }

  @Test
  @DisplayName("constructor arguments are checked")
  void constructorChecksArguments() {
    assertThrows(IllegalArgumentException.class, () -> new ProjectionStringIdentityRegistry(0));
    assertThrows(IllegalArgumentException.class, () -> new ProjectionStringIdentityRegistry(1, ALL_COLLIDE, 0L));
    assertThrows(NullPointerException.class, () -> new ProjectionStringIdentityRegistry(1, null, 16L));
    final ProjectionStringIdentityRegistry registry = new ProjectionStringIdentityRegistry(1);
    assertThrows(IllegalArgumentException.class, () -> prove(registry, 1, "x"));
    assertEquals(ProjectionStringIdentityRegistry.DEFAULT_MAX_CANONICAL_BYTES,
        ProjectionStringIdentityRegistry.DEFAULT_MAX_CANONICAL_BYTES);
  }

  @Test
  @DisplayName("a value the registry has seen re-proves without the monitor, across a grow")
  void repeatedProofsNeverTakeTheMonitor() {
    final ProjectionStringIdentityRegistry registry =
        new ProjectionStringIdentityRegistry(2, ProjectionStringIdentityRegistry.DEFAULT_FINGERPRINT, 1 << 24);
    final int distinct = 5_000; // several doublings past INITIAL_CAPACITY, so grown tables are probed too
    for (int i = 0; i < distinct; i++) {
      assertTrue(prove(registry, 0, "value-" + i), "insert " + i);
      assertTrue(prove(registry, 1, "other-" + i), "insert " + i);
    }
    assertEquals(2L * distinct, registry.lockedProves(), "every first sighting takes the monitor exactly once");
    for (int round = 0; round < 3; round++) {
      for (int i = 0; i < distinct; i++) {
        assertTrue(prove(registry, 0, "value-" + i), "re-prove " + i);
        assertTrue(prove(registry, 1, "other-" + i), "re-prove " + i);
      }
    }
    assertEquals(2L * distinct, registry.lockedProves(), "a repeated value never takes the monitor");
    assertTrue(registry.identityProven());
  }

  @Test
  @DisplayName("with the fast path switched off every proof takes the monitor")
  void killSwitchSendsEveryProofThroughTheMonitor() {
    final boolean previous = ProjectionStringIdentityRegistry.setLockFreeProbeForTesting(false);
    try {
      final ProjectionStringIdentityRegistry registry =
          new ProjectionStringIdentityRegistry(1, ProjectionStringIdentityRegistry.DEFAULT_FINGERPRINT, 1 << 20);
      for (int i = 0; i < 100; i++) {
        assertTrue(prove(registry, 0, "value-" + i));
      }
      for (int i = 0; i < 100; i++) {
        assertTrue(prove(registry, 0, "value-" + i));
      }
      assertEquals(200L, registry.lockedProves(), "the switch routes repeats through the monitor too");
    } finally {
      ProjectionStringIdentityRegistry.setLockFreeProbeForTesting(previous);
    }
  }

  @Test
  @DisplayName("a mismatch under a shared fingerprint is still caught when the reader found the slot lock-free")
  void lockFreeReaderStillDefersMismatchToTheMonitor() {
    final ProjectionStringIdentityRegistry registry = new ProjectionStringIdentityRegistry(1, ALL_COLLIDE, 1 << 20);
    assertTrue(prove(registry, 0, "gold"));
    assertTrue(prove(registry, 0, "gold"), "byte-equal hit");
    assertEquals(1L, registry.lockedProves(), "the hit was served without the monitor");
    assertFalse(prove(registry, 0, "gilt"), "different bytes, same fingerprint: the monitor latches the collision");
    assertEquals(2L, registry.lockedProves(), "the mismatch went through the monitor");
    assertTrue(registry.collisionDetected());
    assertFalse(registry.identityProven());
  }

  @Test
  @DisplayName("workers proving a shared vocabulary concurrently, while it grows, all agree and never decline")
  void concurrentProofsAgreeWhileTheTableGrows() throws Exception {
    final ProjectionStringIdentityRegistry registry =
        new ProjectionStringIdentityRegistry(1, ProjectionStringIdentityRegistry.DEFAULT_FINGERPRINT, 1 << 26);
    final int distinct = 40_000;
    final int workers = 8;
    final byte[][] vocabulary = new byte[distinct][];
    final long[] lanesA = new long[distinct];
    final long[] lanesB = new long[distinct];
    for (int i = 0; i < distinct; i++) {
      vocabulary[i] = utf8("shared-" + i);
      lanesA[i] = registry.laneA(vocabulary[i], 0, vocabulary[i].length, 0L);
      lanesB[i] = registry.laneB(vocabulary[i], 0, vocabulary[i].length);
    }
    final ExecutorService pool = Executors.newFixedThreadPool(workers);
    try {
      final CountDownLatch start = new CountDownLatch(1);
      final List<Future<Long>> results = new ArrayList<>(workers);
      for (int w = 0; w < workers; w++) {
        final int stride = 7 + w; // every worker walks the vocabulary in its own order, so inserts race
        results.add(pool.submit(() -> {
          start.await();
          long proven = 0;
          for (int round = 0; round < 4; round++) {
            for (int k = 0; k < distinct; k++) {
              final int i = (int) (((long) k * stride) % distinct);
              if (registry.prove(0, lanesA[i], lanesB[i], vocabulary[i], 0, vocabulary[i].length)) {
                proven++;
              }
            }
          }
          return proven;
        }));
      }
      start.countDown();
      long proven = 0;
      for (final Future<Long> f : results) {
        proven += f.get(60, TimeUnit.SECONDS);
      }
      assertEquals((long) workers * 4 * distinct, proven, "every proof of a shared vocabulary succeeds");
    } finally {
      pool.shutdownNow();
    }
    assertTrue(registry.identityProven(), "no false collision, no budget refusal");
    final long locked = registry.lockedProves();
    assertTrue(locked >= distinct, "each distinct value was inserted under the monitor: " + locked);
    assertTrue(locked < 2L * distinct, "only racing first sightings may take the monitor twice; " + locked
        + " locked proofs for " + distinct + " values means repeats are taking it");
    for (int i = 0; i < distinct; i++) {
      assertTrue(registry.prove(0, lanesA[i], lanesB[i], vocabulary[i], 0, vocabulary[i].length),
          "value " + i + " survived the concurrent growth");
    }
    assertEquals(locked, registry.lockedProves(), "the post-race sweep took the monitor for nothing");
  }

  @Test
  @DisplayName("pre-proven and eager flags start clear, publish per component and validate the ordinal")
  void preProvenAndEagerFlags() {
    final ProjectionStringIdentityRegistry registry =
        new ProjectionStringIdentityRegistry(3, ProjectionStringIdentityRegistry.DEFAULT_FINGERPRINT, 1 << 20);
    assertEquals(3, registry.components());
    assertTrue(registry.fingerprint() == ProjectionStringIdentityRegistry.DEFAULT_FINGERPRINT,
        "the memo is keyed by the fingerprint INSTANCE, so the getter must hand back the very one");
    for (int k = 0; k < 3; k++) {
      assertFalse(registry.preProven(k), "a fresh registry has nothing pre-proven");
    }
    assertFalse(registry.proveEveryEntry(), "eager proving is opt-in");
    registry.markPreProven(1);
    assertFalse(registry.preProven(0));
    assertTrue(registry.preProven(1));
    assertFalse(registry.preProven(2));
    registry.markPreProven(1); // idempotent
    assertTrue(registry.preProven(1));
    registry.setProveEveryEntry(true);
    assertTrue(registry.proveEveryEntry());
    registry.setProveEveryEntry(false);
    assertFalse(registry.proveEveryEntry());
    assertThrows(IllegalArgumentException.class, () -> registry.preProven(3));
    assertThrows(IllegalArgumentException.class, () -> registry.markPreProven(-1));
    // Marking is a verdict about the component's DATA; it neither proves nor unproves anything the
    // registry itself has seen, so the terminal state and the counters are untouched.
    assertTrue(registry.identityProven());
    assertEquals(0L, registry.lockedProves());
  }

  @Test
  @DisplayName("a pre-proven mark never overrides a latched collision")
  void preProvenDoesNotUnlatchACollision() {
    final ProjectionStringIdentityRegistry registry = new ProjectionStringIdentityRegistry(1, ALL_COLLIDE, 1 << 20);
    final byte[] one = "one".getBytes(StandardCharsets.UTF_8);
    final byte[] two = "two".getBytes(StandardCharsets.UTF_8);
    assertTrue(registry.prove(0, 1L, 2L, one, 0, one.length));
    assertFalse(registry.prove(0, 1L, 2L, two, 0, two.length), "distinct bytes under one pair: a collision");
    assertTrue(registry.collisionDetected());
    registry.markPreProven(0);
    assertTrue(registry.preProven(0));
    assertFalse(registry.identityProven(),
        "the executor notes a column only from identityProven(); a mark must not launder a refuted scan");
  }
}
