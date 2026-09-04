/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access.trx.page;

import io.sirix.index.IndexType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins the memory the flush lane uses to stop re-encoding a leaf whose pre-serialization mints
 * overflow carriers it can never key.
 *
 * <p>
 * Every assertion here is stated as a pair — the identity that must be found and the neighbouring
 * identity that must NOT be — because the table's whole value rests on it distinguishing leaves
 * that differ in one component. A table that answered {@code true} for everything would remove the
 * same encodes and silently stop the async flush from ever writing a leaf again.
 * </p>
 */
final class RefusedOverflowLeafTableTest {

  /** A small table makes a deliberate collision reachable; the production default is 64 Ki. */
  private static final int SMALL_SLOTS = 1 << 8;

  /** A bound wide enough that these tests never trip expiry unless they mean to. */
  private static final int WIDE_BOUND = 1_000_000;

  @Test
  void anIdentityIsRememberedAndNeighbouringIdentitiesAreNot() {
    final RefusedOverflowLeafTable table = new RefusedOverflowLeafTable(RefusedOverflowLeafTable.DEFAULT_SLOTS);
    final long pageKey = 4_242L;
    final int nameId = IndexType.NAME.getID();

    assertFalse(table.shouldSkip(pageKey, nameId, 0L, WIDE_BOUND), "an untouched table must remember nothing");

    table.note(pageKey, nameId, 0L);

    assertTrue(table.shouldSkip(pageKey, nameId, 0L, WIDE_BOUND), "the noted identity must be found");
    // Mutate each component of the identity in turn: both must miss, or the table would decline
    // encodes for leaves that were never refused.
    assertFalse(table.shouldSkip(pageKey, IndexType.DOCUMENT.getID(), 0L, WIDE_BOUND),
        "a different index type at the same page key must miss");
    assertFalse(table.shouldSkip(pageKey + 1L, nameId, 0L, WIDE_BOUND),
        "a different page key of the same index type must miss");
    assertFalse(table.shouldSkip(pageKey - 1L, nameId, 0L, WIDE_BOUND), "the preceding page key must miss too");
  }

  @Test
  void everyIndexTypeRoundTripsThroughTheFourBitPack() {
    final RefusedOverflowLeafTable table = new RefusedOverflowLeafTable(RefusedOverflowLeafTable.DEFAULT_SLOTS);
    // The pack spends four bits on the index type. A twelfth index type is fine; a seventeenth would
    // corrupt the page key, so this loop is the guard that fails the day one is added.
    for (final IndexType type : IndexType.values()) {
      final long pageKey = 7_000L + type.getID();
      table.note(pageKey, type.getID(), 0L);
      assertTrue(table.shouldSkip(pageKey, type.getID(), 0L, WIDE_BOUND), "identity for " + type + " must round-trip");
    }
    for (final IndexType type : IndexType.values()) {
      assertTrue(table.shouldSkip(7_000L + type.getID(), type.getID(), 0L, WIDE_BOUND),
          type + " must survive the other notes");
    }
  }

  @Test
  void aCollidingIdentityEvictsTheOlderMarkWithoutEverImpersonatingIt() {
    final RefusedOverflowLeafTable table = new RefusedOverflowLeafTable(SMALL_SLOTS);
    final int nameId = IndexType.NAME.getID();

    final long first = 1L;
    final int firstSlot = table.indexForTesting(first, nameId);
    long colliding = -1L;
    for (long candidate = 2L; candidate < 100_000L; candidate++) {
      if (table.indexForTesting(candidate, nameId) == firstSlot) {
        colliding = candidate;
        break;
      }
    }
    assertNotEquals(-1L, colliding, "a 256-slot table must produce a collision within 100k keys");

    table.note(first, nameId, 0L);
    assertTrue(table.shouldSkip(first, nameId, 0L, WIDE_BOUND));

    table.note(colliding, nameId, 0L);

    // The eviction is a FALSE NEGATIVE — the evicted leaf simply pays one more encode, and the next
    // refusal re-marks it. What must never happen is the reverse: the surviving entry answering for
    // the evicted identity, which would keep a flushable leaf out of the async flush forever.
    assertTrue(table.shouldSkip(colliding, nameId, 0L, WIDE_BOUND), "the newer identity owns the slot");
    assertFalse(table.shouldSkip(first, nameId, 0L, WIDE_BOUND),
        "the evicted identity must MISS, never be impersonated");

    table.note(first, nameId, 0L);
    assertTrue(table.shouldSkip(first, nameId, 0L, WIDE_BOUND),
        "the table is self-healing: a re-refusal restores the mark");
    assertFalse(table.shouldSkip(colliding, nameId, 0L, WIDE_BOUND));
  }

  @Test
  void theSlotCountMustBeAPositivePowerOfTwo() {
    assertThrows(IllegalArgumentException.class, () -> new RefusedOverflowLeafTable(0));
    assertThrows(IllegalArgumentException.class, () -> new RefusedOverflowLeafTable(-1));
    assertThrows(IllegalArgumentException.class, () -> new RefusedOverflowLeafTable(24));
  }

  @Test
  void thePackRejectsComponentsItCannotRepresent() {
    assertThrows(IllegalArgumentException.class, () -> RefusedOverflowLeafTable.pack(-1L, 0));
    assertThrows(IllegalArgumentException.class, () -> RefusedOverflowLeafTable.pack((1L << 57) + 1L, 0));
    assertThrows(IllegalArgumentException.class, () -> RefusedOverflowLeafTable.pack(0L, -1));
    assertThrows(IllegalArgumentException.class, () -> RefusedOverflowLeafTable.pack(0L, 16));
    // The boundary values themselves are representable, and the pack is never zero — zero is the
    // table's empty sentinel, so a zero pack would make page key 0 of index type 0 invisible.
    assertNotEquals(0L, RefusedOverflowLeafTable.pack(0L, 0));
    assertNotEquals(0L, RefusedOverflowLeafTable.pack(1L << 57, 15));
    assertEquals(RefusedOverflowLeafTable.pack(3L, 7), RefusedOverflowLeafTable.pack(3L, 7));
    assertNotEquals(RefusedOverflowLeafTable.pack(3L, 7), RefusedOverflowLeafTable.pack(3L, 8));
  }

  @Test
  void pageKeyZeroOfIndexTypeZeroIsRepresentable() {
    final RefusedOverflowLeafTable table = new RefusedOverflowLeafTable(SMALL_SLOTS);
    assertFalse(table.shouldSkip(0L, 0, 0L, WIDE_BOUND));
    table.note(0L, 0, 0L);
    assertTrue(table.shouldSkip(0L, 0, 0L, WIDE_BOUND), "the empty sentinel must not swallow the all-zero identity");
  }

  @Test
  void aMarkExpiresAfterTheBoundSoNoLeafIsExcludedForever() {
    final RefusedOverflowLeafTable table = new RefusedOverflowLeafTable(SMALL_SLOTS);
    final int nameId = IndexType.NAME.getID();
    final long markedAt = 100L;
    table.note(7L, nameId, markedAt);

    // Inside the bound the mark holds; on the boundary and past it, it must not — this is the
    // property that turns "the leaf is expected to refuse again" into "the leaf cannot be kept out
    // of the async flush for more than N epochs".
    assertTrue(table.shouldSkip(7L, nameId, markedAt, 8));
    assertTrue(table.shouldSkip(7L, nameId, markedAt + 7L, 8));
    assertFalse(table.shouldSkip(7L, nameId, markedAt + 8L, 8), "the bound is exclusive");
    assertFalse(table.shouldSkip(7L, nameId, markedAt + 9L, 8));
    assertFalse(table.shouldSkip(7L, nameId, markedAt + 100_000L, 8));

    // A refusal at the later epoch re-arms it, so a leaf that keeps refusing keeps being skipped —
    // the safety valve costs one encode per bound, not the whole lever.
    table.note(7L, nameId, markedAt + 8L);
    assertTrue(table.shouldSkip(7L, nameId, markedAt + 8L, 8));
  }

  @Test
  void aBoundOfZeroHonoursNoMarkAtAll() {
    final RefusedOverflowLeafTable table = new RefusedOverflowLeafTable(SMALL_SLOTS);
    final int nameId = IndexType.NAME.getID();
    table.note(11L, nameId, 5L);

    // The kill switch's arithmetic form: with a bound of zero every answer is "encode it", which is
    // exactly unmodified behaviour. Asserted rather than assumed, because a bound that silently
    // meant "unbounded" would disable the safety valve in the one configuration meant to restore it.
    assertFalse(table.shouldSkip(11L, nameId, 5L, 0));
    assertFalse(table.shouldSkip(11L, nameId, 5L, -1));
    assertTrue(table.shouldSkip(11L, nameId, 5L, 1), "a bound of one still honours the marking epoch");
  }

  @Test
  void anAgeThatIsNotPlainlyInsideTheBoundFallsToEncoding() {
    final RefusedOverflowLeafTable table = new RefusedOverflowLeafTable(SMALL_SLOTS);
    final int nameId = IndexType.NAME.getID();
    table.note(13L, nameId, 50L);

    // A worker can ask with an epoch OLDER than the mark, by reading a stale flushEpoch while
    // another worker writes a newer one. Both answers are safe there, so the table takes the
    // conservative one and encodes. Asserting it pins two things at once: that the ambiguous case
    // falls to the safe side, and that the arithmetic is signed — an unsigned reading would make
    // this age astronomically large, which also expires, but would expire a freshly written mark
    // in the ordinary case too and quietly reinstate the re-encode loop.
    assertFalse(table.shouldSkip(13L, nameId, 49L, 8), "an epoch behind the mark must fall to encoding");
    assertTrue(table.shouldSkip(13L, nameId, 50L, 8), "the marking epoch itself is inside the bound");
  }
}
