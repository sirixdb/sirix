package io.sirix.query.scan;

import io.sirix.query.scan.DerivedCacheBudget.Reservation;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("native-image")
final class DerivedCacheBudgetTest {
  @Test
  void defaultsRespectAllThreeInputsAndExplicitCapsRespectTheReserve() {
    final long gib = 1L << 30;
    final DerivedCacheBudget budget = new DerivedCacheBudget(8 * gib, 6 * gib, gib, -1);
    assertEquals(768L << 20, budget.metrics().effectiveBytes());
    assertEquals(576L << 20, budget.metrics().numeric().maximumBytes());
    assertEquals(192L << 20, budget.metrics().packed().maximumBytes());
    assertEquals(768L << 20, new DerivedCacheBudget(16 * gib, 6 * gib, gib, -1).metrics().effectiveBytes());
    assertEquals(123, new DerivedCacheBudget(8 * gib, 6 * gib, 123, -1).metrics().effectiveBytes());
    assertEquals(123, new DerivedCacheBudget(8 * gib, 6 * gib, 123, Long.MAX_VALUE).metrics().effectiveBytes());
    assertEquals(0, new DerivedCacheBudget(8 * gib, 6 * gib, gib, 0).metrics().effectiveBytes());
    assertEquals(0, new DerivedCacheBudget(8 * gib, 6 * gib, 0, -1).metrics().effectiveBytes());
    assertThrows(IllegalArgumentException.class, () -> new DerivedCacheBudget(-1, gib, gib, -1));
    assertThrows(IllegalArgumentException.class, () -> new DerivedCacheBudget(gib, gib, gib, -2));
  }

  @Test
  void simultaneousConstructionAndRetainedEntriesNeverExceedTheCombinedBudget() throws Exception {
    final DerivedCacheBudget budget = new DerivedCacheBudget(800, 800, 100, -1);
    final Object numericKey = new Object();
    final Object packedKey = new Object();
    final CountDownLatch building = new CountDownLatch(2);
    final CountDownLatch publish = new CountDownLatch(1);
    try (var executor = Executors.newFixedThreadPool(2)) {
      final var numeric = executor.submit(() -> construct(budget.numeric(), numericKey, 75, 60, building, publish));
      final var packed = executor.submit(() -> construct(budget.packed(), packedKey, 25, 20, building, publish));
      building.await();
      try {
        assertEquals(100, budget.metrics().inFlightBytes());
        assertEquals(0, budget.metrics().retainedBytes());
        assertNull(budget.numeric().reserve(1));
        assertNull(budget.packed().reserve(1));
      } finally {
        publish.countDown();
      }
      numeric.get();
      packed.get();
    }
    assertEquals(80, budget.metrics().retainedBytes());
    assertEquals(0, budget.metrics().inFlightBytes());
    assertEquals(2, budget.metrics().rejectedBytes());
    assertSame(numericKey, budget.numeric().get(numericKey));
    assertSame(packedKey, budget.packed().get(packedKey));
    budget.numeric().clear();
    budget.packed().clear();
    assertEquals(0, budget.metrics().retainedBytes());
  }

  private static void construct(final DerivedCacheBudget.Partition partition, final Object key, final long bytes,
      final long retained, final CountDownLatch building, final CountDownLatch publish) {
    try (final Reservation reservation = partition.reserve(bytes)) {
      building.countDown();
      assertNotNull(reservation);
      publish.await();
      reservation.publish(key, key, retained);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError(e);
    }
  }

  @Test
  void failureReleasesReservationsAndEvictionPricesLongWeightsWithoutWrapping() {
    final DerivedCacheBudget budget = new DerivedCacheBudget(Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, -1);
    assertEquals(Long.MAX_VALUE, DerivedCacheBudget.saturatedMultiply(Long.MAX_VALUE, 2));
    assertEquals(0, DerivedCacheBudget.saturatedMultiply(Long.MAX_VALUE, 0));
    final long weight = (long) Integer.MAX_VALUE + 100;
    final Object key = new Object();
    try (final Reservation ignored = budget.numeric().reserve(weight)) {
      assertNotNull(ignored);
      assertEquals(weight, budget.metrics().inFlightBytes());
    }
    assertEquals(0, budget.metrics().inFlightBytes());
    try (final Reservation reservation = budget.numeric().reserve(weight)) {
      assertNotNull(reservation);
      reservation.publish(key, "value", weight);
    }
    assertEquals("value", budget.numeric().get(key));
    assertEquals(weight, budget.metrics().retainedBytes());
    assertNull(budget.numeric().reserve(Long.MAX_VALUE));
    assertNull(budget.numeric().reserve(Long.MAX_VALUE));
    assertEquals(Long.MAX_VALUE, budget.metrics().rejectedBytes());
    budget.numeric().clear();
    assertEquals(0, budget.metrics().retainedBytes());
  }

  @Test
  void byteWeightedReplacementMakesRoomAndZeroDisables() {
    final DerivedCacheBudget budget = new DerivedCacheBudget(800, 800, 100, -1);
    final Object first = new Object();
    final Object second = new Object();
    try (final Reservation reservation = budget.numeric().reserve(60)) {
      assertNotNull(reservation);
      reservation.publish(first, "first", 60);
    }
    try (final Reservation reservation = budget.numeric().reserve(60)) {
      assertNotNull(reservation);
      assertNull(budget.numeric().get(first));
      assertEquals(60, budget.metrics().inFlightBytes());
      reservation.publish(second, "second", 60);
    }
    assertEquals("second", budget.numeric().get(second));
    assertTrue(budget.metrics().retainedBytes() <= 75);
    final DerivedCacheBudget disabled = new DerivedCacheBudget(800, 800, 100, 0);
    assertNull(disabled.numeric().reserve(1));
    assertNull(disabled.packed().reserve(1));
    assertEquals(0, disabled.metrics().inFlightBytes());
    assertEquals(0, disabled.metrics().retainedBytes());
  }
}
