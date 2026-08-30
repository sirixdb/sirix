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
 * identity that must NOT be — because the table's whole value rests on it distinguishing leaves that
 * differ in one component. A table that answered {@code true} for everything would remove the same
 * encodes and silently stop the async flush from ever writing a leaf again.
 * </p>
 */
final class RefusedOverflowLeafTableTest {

  /** A small table makes a deliberate collision reachable; the production default is 64 Ki. */
  private static final int SMALL_SLOTS = 1 << 8;

  @Test
  void anIdentityIsRememberedAndNeighbouringIdentitiesAreNot() {
    final RefusedOverflowLeafTable table = new RefusedOverflowLeafTable(RefusedOverflowLeafTable.DEFAULT_SLOTS);
    final long pageKey = 4_242L;
    final int nameId = IndexType.NAME.getID();

    assertFalse(table.contains(pageKey, nameId), "an untouched table must remember nothing");

    table.note(pageKey, nameId);

    assertTrue(table.contains(pageKey, nameId), "the noted identity must be found");
    // Mutate each component of the identity in turn: both must miss, or the table would decline
    // encodes for leaves that were never refused.
    assertFalse(table.contains(pageKey, IndexType.DOCUMENT.getID()),
        "a different index type at the same page key must miss");
    assertFalse(table.contains(pageKey + 1L, nameId), "a different page key of the same index type must miss");
    assertFalse(table.contains(pageKey - 1L, nameId), "the preceding page key must miss too");
  }

  @Test
  void everyIndexTypeRoundTripsThroughTheFourBitPack() {
    final RefusedOverflowLeafTable table = new RefusedOverflowLeafTable(RefusedOverflowLeafTable.DEFAULT_SLOTS);
    // The pack spends four bits on the index type. A twelfth index type is fine; a seventeenth would
    // corrupt the page key, so this loop is the guard that fails the day one is added.
    for (final IndexType type : IndexType.values()) {
      final long pageKey = 7_000L + type.getID();
      table.note(pageKey, type.getID());
      assertTrue(table.contains(pageKey, type.getID()), "identity for " + type + " must round-trip");
    }
    for (final IndexType type : IndexType.values()) {
      assertTrue(table.contains(7_000L + type.getID(), type.getID()), type + " must survive the other notes");
    }
  }

  @Test
  void aCollidingIdentityEvictsTheOlderMarkWithoutEverImpersonatingIt() {
    final RefusedOverflowLeafTable table = new RefusedOverflowLeafTable(SMALL_SLOTS);
    final int nameId = IndexType.NAME.getID();

    final long first = 1L;
    final int firstSlot = table.slotForTesting(first, nameId);
    long colliding = -1L;
    for (long candidate = 2L; candidate < 100_000L; candidate++) {
      if (table.slotForTesting(candidate, nameId) == firstSlot) {
        colliding = candidate;
        break;
      }
    }
    assertNotEquals(-1L, colliding, "a 256-slot table must produce a collision within 100k keys");

    table.note(first, nameId);
    assertTrue(table.contains(first, nameId));

    table.note(colliding, nameId);

    // The eviction is a FALSE NEGATIVE — the evicted leaf simply pays one more encode, and the next
    // refusal re-marks it. What must never happen is the reverse: the surviving entry answering for
    // the evicted identity, which would keep a flushable leaf out of the async flush forever.
    assertTrue(table.contains(colliding, nameId), "the newer identity owns the slot");
    assertFalse(table.contains(first, nameId), "the evicted identity must MISS, never be impersonated");

    table.note(first, nameId);
    assertTrue(table.contains(first, nameId), "the table is self-healing: a re-refusal restores the mark");
    assertFalse(table.contains(colliding, nameId));
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
    assertFalse(table.contains(0L, 0));
    table.note(0L, 0);
    assertTrue(table.contains(0L, 0), "the empty sentinel must not swallow the all-zero identity");
  }
}
