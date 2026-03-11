package io.sirix.query.compiler.optimizer.join;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for {@link DpTable} — Fibonacci hashing, put/get, resize.
 */
final class DpTableTest {

  @Test
  void putAndGetSingleEntry() {
    final var table = new DpTable(16);
    final var plan = JoinPlan.baseRelation(0, 100, 1.0);

    table.put(0b001, plan);
    assertEquals(plan, table.get(0b001));
    assertEquals(1, table.size());
  }

  @Test
  void getReturnsNullForMissing() {
    final var table = new DpTable(16);
    assertNull(table.get(0b001));
    assertNull(table.get(0b111));
  }

  @Test
  void putOverwritesExistingEntry() {
    final var table = new DpTable(16);
    final var plan1 = JoinPlan.baseRelation(0, 100, 10.0);
    final var plan2 = JoinPlan.baseRelation(0, 100, 5.0);

    table.put(0b001, plan1);
    table.put(0b001, plan2);

    assertEquals(plan2, table.get(0b001));
    assertEquals(1, table.size());
  }

  @Test
  void multipleEntriesWithDifferentKeys() {
    final var table = new DpTable(16);
    for (int i = 0; i < 8; i++) {
      table.put(1L << i, JoinPlan.baseRelation(i, (i + 1) * 100L, i + 1.0));
    }

    assertEquals(8, table.size());
    for (int i = 0; i < 8; i++) {
      final JoinPlan plan = table.get(1L << i);
      assertNotNull(plan, "Missing plan for relation " + i);
      assertEquals((i + 1) * 100L, plan.cardinality());
    }
  }

  @Test
  void resizePreservesAllEntries() {
    final var table = new DpTable(16); // will resize when >12 entries (75%)
    final int n = 20;

    for (int i = 0; i < n; i++) {
      table.put(i + 1L, JoinPlan.baseRelation(0, (i + 1) * 10L, i + 1.0));
    }

    assertEquals(n, table.size());
    for (int i = 0; i < n; i++) {
      final JoinPlan plan = table.get(i + 1L);
      assertNotNull(plan, "Missing plan for key " + (i + 1));
      assertEquals((i + 1) * 10L, plan.cardinality());
    }
  }

  @Test
  void bitmaskKeysDistributeWell() {
    // Bitmask keys (1, 2, 3, 4, 5, 6, 7) — typical for 3-relation DPhyp
    final var table = new DpTable(16);
    for (long key = 1; key <= 7; key++) {
      table.put(key, JoinPlan.baseRelation(0, key * 100, key));
    }

    assertEquals(7, table.size());
    for (long key = 1; key <= 7; key++) {
      assertNotNull(table.get(key), "Missing for key " + key);
      assertEquals(key * 100, table.get(key).cardinality());
    }
  }

  @Test
  void rejectsZeroKey() {
    final var table = new DpTable(16);
    assertThrows(IllegalArgumentException.class,
        () -> table.put(0, JoinPlan.baseRelation(0, 100, 1.0)));
  }

  @Test
  void getForZeroKeyReturnsNull() {
    final var table = new DpTable(16);
    assertNull(table.get(0));
  }
}
