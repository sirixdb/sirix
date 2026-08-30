package io.sirix.index.projection;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

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
    final ProjectionStringIdentityRegistry registry =
        new ProjectionStringIdentityRegistry(1, ALL_COLLIDE, 1 << 20);
    assertTrue(prove(registry, 0, "gold"));
    assertTrue(prove(registry, 0, "gold"), "the same bytes must re-prove, however weak the fingerprint");
    assertTrue(registry.identityProven());
    assertFalse(registry.collisionDetected());
  }

  @Test
  @DisplayName("two distinct strings forced onto one fingerprint pair are caught by byte comparison")
  void injectedFingerprintCollisionIsCaught() {
    final ProjectionStringIdentityRegistry registry =
        new ProjectionStringIdentityRegistry(1, ALL_COLLIDE, 1 << 20);
    assertTrue(prove(registry, 0, "gold"));
    assertFalse(prove(registry, 0, "silver"), "a different value under the same pair must NOT be proven");
    assertTrue(registry.collisionDetected(), "the collision must latch");
    assertFalse(registry.identityProven(), "the scan must be told it cannot trust its identity lanes");
  }

  @Test
  @DisplayName("equal-length distinct strings collide under a length-only fingerprint and are caught")
  void equalLengthCollisionIsCaught() {
    final ProjectionStringIdentityRegistry registry =
        new ProjectionStringIdentityRegistry(1, LENGTH_ONLY, 1 << 20);
    assertTrue(prove(registry, 0, "abcd"));
    assertTrue(prove(registry, 0, "abcd"));
    assertFalse(prove(registry, 0, "wxyz"), "same length, different bytes: not the same group");
    assertTrue(registry.collisionDetected());
  }

  @Test
  @DisplayName("components are proven independently")
  void componentsAreIndependent() {
    final ProjectionStringIdentityRegistry registry =
        new ProjectionStringIdentityRegistry(2, ALL_COLLIDE, 1 << 20);
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
    final ProjectionStringIdentityRegistry registry =
        new ProjectionStringIdentityRegistry(1, ALL_COLLIDE, 1 << 20);
    assertTrue(prove(registry, 0, "https://example.com/a"));
    assertFalse(prove(registry, 0, "https://example.com/b"));
    assertTrue(registry.collisionDetected());
  }

  @Test
  @DisplayName("a value that is a strict prefix of the canonical one is not equal to it")
  void prefixIsNotEquality() {
    final ProjectionStringIdentityRegistry registry =
        new ProjectionStringIdentityRegistry(1, ALL_COLLIDE, 1 << 20);
    assertTrue(prove(registry, 0, "abcdef"));
    assertFalse(prove(registry, 0, "abc"), "a prefix is a different value");
    assertTrue(registry.collisionDetected());
  }

  @Test
  @DisplayName("exhausting the canonical-byte budget declines rather than stopping the proof")
  void budgetExhaustionDeclinesConservatively() {
    // Budget fits exactly one entry — its four value bytes PLUS the per-entry overhead charge, so
    // that the budget bounds table and header footprint and not merely the strings.
    final ProjectionStringIdentityRegistry registry = new ProjectionStringIdentityRegistry(1,
        ProjectionStringIdentityRegistry.DEFAULT_FINGERPRINT,
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
    final ProjectionStringIdentityRegistry registry =
        new ProjectionStringIdentityRegistry(1, ALL_COLLIDE, 1 << 20);
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
    assertThrows(IllegalArgumentException.class,
        () -> new ProjectionStringIdentityRegistry.LocalProofCache(tooMany));
    assertThrows(IllegalArgumentException.class,
        () -> new ProjectionStringIdentityRegistry.LocalProofCache(Integer.MAX_VALUE));
  }

  @Test
  @DisplayName("constructor arguments are checked")
  void constructorChecksArguments() {
    assertThrows(IllegalArgumentException.class, () -> new ProjectionStringIdentityRegistry(0));
    assertThrows(IllegalArgumentException.class,
        () -> new ProjectionStringIdentityRegistry(1, ALL_COLLIDE, 0L));
    assertThrows(NullPointerException.class, () -> new ProjectionStringIdentityRegistry(1, null, 16L));
    final ProjectionStringIdentityRegistry registry = new ProjectionStringIdentityRegistry(1);
    assertThrows(IllegalArgumentException.class, () -> prove(registry, 1, "x"));
    assertEquals(ProjectionStringIdentityRegistry.DEFAULT_MAX_CANONICAL_BYTES,
        ProjectionStringIdentityRegistry.DEFAULT_MAX_CANONICAL_BYTES);
  }
}
