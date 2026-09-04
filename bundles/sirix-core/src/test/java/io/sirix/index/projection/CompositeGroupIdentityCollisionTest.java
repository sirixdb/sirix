package io.sirix.index.projection;

import it.unimi.dsi.fastutil.HashCommon;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Adversarial gate for composite group identity.
 *
 * <p>
 * The composite kernels fold one hash per key component into a single 64-bit group key with
 * {@code h = h * FNV_PRIME ^ mix(component)}. Both halves of that expression are invertible, so a
 * colliding second tuple is not searched for — it is SOLVED for:
 *
 * <pre>
 * c1 = invMix(((FNV_SEED * FNV_PRIME ^ mix(b0)) * FNV_PRIME) ^ hTarget)
 * </pre>
 *
 * Every test here first ASSERTS that its two tuples really do collide under the kernel's own
 * constants, so that a future change to the hash cannot quietly turn these guards vacuous: the
 * precondition fails loudly instead.
 */
final class CompositeGroupIdentityCollisionTest {

  /** First tuple: an ordinary pair of numeric key components. */
  private static final long A0 = 1_234_567L;
  private static final long A1 = 987_654_321L;

  /** Second tuple: first component chosen freely, second SOLVED so the composite key collides. */
  private static final long B0 = 42L;

  private static long compositeKey(final long c0, final long c1) {
    long h = ProjectionIndexByteScan.FNV_SEED;
    h = h * ProjectionIndexByteScan.FNV_PRIME ^ HashCommon.mix(c0);
    h = h * ProjectionIndexByteScan.FNV_PRIME ^ HashCommon.mix(c1);
    return h;
  }

  /** The second component that makes {@code (B0, ?)} collide with {@code (A0, A1)}. */
  private static long solveCollidingSecondComponent() {
    final long target = compositeKey(A0, A1);
    final long partial = ProjectionIndexByteScan.FNV_SEED * ProjectionIndexByteScan.FNV_PRIME ^ HashCommon.mix(B0);
    return HashCommon.invMix(partial * ProjectionIndexByteScan.FNV_PRIME ^ target);
  }

  @Test
  @DisplayName("a colliding composite tuple is constructible in closed form, not merely possible")
  void theCollisionIsConstructible() {
    final long b1 = solveCollidingSecondComponent();
    assertTrue(A0 != B0 || A1 != b1, "the two tuples must be distinct");
    assertEquals(compositeKey(A0, A1), compositeKey(B0, b1),
        "the witness is stale: these tuples no longer collide under the kernel's constants");
  }

  @Test
  @DisplayName("two tuples sharing a composite key stay two groups")
  void collidingTuplesDoNotMerge() {
    final long b1 = solveCollidingSecondComponent();
    final long key = compositeKey(A0, A1);
    assertEquals(key, compositeKey(B0, b1), "precondition: the tuples collide");

    // Identity layout of a two-component NUMERIC_LONG key: one mask lane plus one lane each.
    final byte[] kinds =
        {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG, ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG};
    final int width = CompositeGroupIdentity.width(kinds, null);
    assertEquals(3, width);

    final NumericGroupAggTable table = new NumericGroupAggTable(0, 16, true, -1L, width);
    final long[] idA = {0L, A0, A1};
    final long[] idB = {0L, B0, b1};

    final int handleA = table.acquireExact(key, 100L, idA, 0);
    table.storageAtAccBase(handleA)[table.offsetAtAccBase(handleA)] += 5;

    final int handleB = table.acquireExact(key, 200L, idB, 0);
    table.storageAtAccBase(handleB)[table.offsetAtAccBase(handleB)] += 3;

    assertNotEquals(handleA, handleB, "colliding tuples must occupy two buckets");
    assertEquals(2, table.size(), "both tuples must exist as separate groups");
    assertTrue(table.hasProbeKeyCollision(), "the table must report the shared probe key");

    // Re-acquiring each identity must land back on its OWN accumulator.
    final int againA = table.acquireExact(key, 999L, idA, 0);
    final int againB = table.acquireExact(key, 999L, idB, 0);
    assertEquals(handleA, againA);
    assertEquals(handleB, againB);
    assertEquals(5L, table.storageAtAccBase(againA)[table.offsetAtAccBase(againA)]);
    assertEquals(3L, table.storageAtAccBase(againB)[table.offsetAtAccBase(againB)]);
  }

  @Test
  @DisplayName("growth re-homes colliding groups without folding them together")
  void collidingGroupsSurviveRehash() {
    final long b1 = solveCollidingSecondComponent();
    final long key = compositeKey(A0, A1);
    final byte[] kinds =
        {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG, ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG};
    final int width = CompositeGroupIdentity.width(kinds, null);
    final NumericGroupAggTable table = new NumericGroupAggTable(0, 16, true, -1L, width);

    final long[] idA = {0L, A0, A1};
    final long[] idB = {0L, B0, b1};
    table.storageAtAccBase(table.acquireExact(key, 1L, idA, 0))[table.offsetAtAccBase(
        table.acquireExact(key, 1L, idA, 0))] += 7;
    table.storageAtAccBase(table.acquireExact(key, 2L, idB, 0))[table.offsetAtAccBase(
        table.acquireExact(key, 2L, idB, 0))] += 11;

    // Force several growths around the pair.
    final long[] filler = new long[width];
    for (int i = 1; i <= 4_000; i++) {
      filler[0] = 0L;
      filler[1] = 1_000_000L + i;
      filler[2] = i;
      table.acquireExact(compositeKey(filler[1], filler[2]), i, filler, 0);
    }

    final int a = table.acquireExact(key, 3L, idA, 0);
    final int b = table.acquireExact(key, 4L, idB, 0);
    assertNotEquals(a, b);
    assertEquals(7L, table.storageAtAccBase(a)[table.offsetAtAccBase(a)], "group A's count survived growth intact");
    assertEquals(11L, table.storageAtAccBase(b)[table.offsetAtAccBase(b)], "group B's count survived growth intact");
    assertEquals(4_002, table.size());
  }

  @Test
  @DisplayName("the partition merge keeps colliding groups apart across workers")
  void mergeKeepsCollidingGroupsApart() {
    final long b1 = solveCollidingSecondComponent();
    final long key = compositeKey(A0, A1);
    final byte[] kinds =
        {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG, ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG};
    final int width = CompositeGroupIdentity.width(kinds, null);
    final long[] idA = {0L, A0, A1};
    final long[] idB = {0L, B0, b1};

    // Worker 1 saw only tuple A, worker 2 only tuple B — the merge is where they first meet.
    final NumericGroupAggTable one = new NumericGroupAggTable(0, 16, true, -1L, width);
    final int h1 = one.acquireExact(key, 10L, idA, 0);
    one.storageAtAccBase(h1)[one.offsetAtAccBase(h1)] += 5;

    final NumericGroupAggTable two = new NumericGroupAggTable(0, 16, true, -1L, width);
    final int h2 = two.acquireExact(key, 20L, idB, 0);
    two.storageAtAccBase(h2)[two.offsetAtAccBase(h2)] += 3;

    final NumericGroupAggTable into = new NumericGroupAggTable(0, 16, true, -1L, width);
    NumericGroupAggTable.mergePartition(new NumericGroupAggTable[] {one, two}, 0, 64, into);

    assertEquals(2, into.size(), "the merge must not fold two identities into one group");
    final int mergedA = into.acquireExact(key, 0L, idA, 0);
    final int mergedB = into.acquireExact(key, 0L, idB, 0);
    assertEquals(5L, into.storageAtAccBase(mergedA)[into.offsetAtAccBase(mergedA)]);
    assertEquals(3L, into.storageAtAccBase(mergedB)[into.offsetAtAccBase(mergedB)]);
  }

  @Test
  @DisplayName("a probe hash of zero needs no side slot once identity decides membership")
  void zeroProbeHashIsRemappedNotSideSlotted() {
    final byte[] kinds =
        {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG, ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG};
    final int width = CompositeGroupIdentity.width(kinds, null);
    final NumericGroupAggTable table = new NumericGroupAggTable(0, 16, true, -1L, width);

    final long[] first = {0L, 1L, 2L};
    final long[] second = {0L, 3L, 4L};
    final int a = table.acquireExact(0L, 1L, first, 0);
    final int b = table.acquireExact(0L, 2L, second, 0);
    assertNotEquals(a, b, "two identities under a zero probe hash are still two groups");
    assertEquals(2, table.size());
    assertFalse(table.hasZeroKey(), "identity mode never uses the zero side slot");
    assertThrows(IllegalStateException.class, () -> table.acquireZero(0L));
  }

  @Test
  @DisplayName("an ambiguous probe key refuses to answer a hash-keyed side lookup")
  void ambiguousProbeKeyRefusesLookup() {
    final long b1 = solveCollidingSecondComponent();
    final long key = compositeKey(A0, A1);
    final byte[] kinds =
        {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG, ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG};
    final int width = CompositeGroupIdentity.width(kinds, null);
    final NumericGroupAggTable table = new NumericGroupAggTable(0, 16, true, -1L, width);

    table.acquireExact(key, 1L, new long[] {0L, A0, A1}, 0);
    assertEquals(table.handleOfProbeKey(key), table.acquireExact(key, 1L, new long[] {0L, A0, A1}, 0),
        "a unique probe key resolves");

    table.acquireExact(key, 2L, new long[] {0L, B0, b1}, 0);
    assertThrows(IllegalStateException.class, () -> table.handleOfProbeKey(key),
        "once two groups share the key, a hash-keyed lookup must refuse rather than pick one");
  }

  @Test
  @DisplayName("a missing component is identified by its mask bit, not by a value that encodes to zero")
  void missingComponentIsDistinctFromAZeroValue() {
    final byte[] kinds =
        {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG, ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG};
    final int width = CompositeGroupIdentity.width(kinds, null);
    final NumericGroupAggTable table = new NumericGroupAggTable(0, 16, true, -1L, width);

    final long[] realZero = {0L, 0L, 7L}; // component 0 present, value 0
    final long[] missing = {1L, 0L, 7L}; // component 0 absent (mask bit 0 set)
    final int a = table.acquireExact(123L, 1L, realZero, 0);
    final int b = table.acquireExact(123L, 2L, missing, 0);
    assertNotEquals(a, b, "an absent component must not merge with a stored zero");
    assertEquals(2, table.size());
  }

  @Test
  @DisplayName("identity width follows the component kinds")
  void identityWidthFollowsKinds() {
    final byte numeric = ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG;
    final byte dict = ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT;
    assertEquals(3, CompositeGroupIdentity.width(new byte[] {numeric, numeric}, null));
    // A dictionary string costs two lanes: a 128-bit content identity, worker-independent and
    // therefore mergeable without a shared interner.
    assertEquals(5, CompositeGroupIdentity.width(new byte[] {dict, dict}, null));
    assertEquals(4, CompositeGroupIdentity.width(new byte[] {numeric, dict}, null));
    // A substring cast groups on the cast integer, which is exact in one lane.
    assertEquals(3, CompositeGroupIdentity.width(new byte[] {dict, dict}, new int[] {1, 4, 1, 4}));
    // Cast is per component: only the cast one narrows to a single lane.
    assertEquals(4, CompositeGroupIdentity.width(new byte[] {dict, dict}, new int[] {1, 4, 0, 0}));
  }
}
