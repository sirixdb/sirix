/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.utils;

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The leak registry needs weakness AND identity at the same time; losing either breaks detection.
 *
 * <p>Losing WEAKNESS is the silent one: a strong registry makes every page it tracks immortal, so a
 * Cleaner-based "collected without close" counter can never fire, and the diagnostic reports zero
 * for a category it is structurally incapable of observing. That is exactly what
 * {@code KeyValueLeafPage.ALL_LIVE_PAGES} did while it was an {@code IdentityHashMap}-backed set.</p>
 *
 * <p>Losing IDENTITY is the other trap, and the reason a plain {@code WeakHashMap} cannot be used:
 * {@code KeyValueLeafPage} overrides {@code equals}/{@code hashCode}, so two distinct instances for
 * the same page key and revision compare equal and one would silently replace the other in the
 * census — under-reporting precisely when duplicate instances are the thing worth seeing.</p>
 */
public final class WeakIdentitySetTest {

  /** Equal-but-distinct, mirroring KeyValueLeafPage's overridden equality. */
  private static final class EqualByValue {
    private final int value;

    EqualByValue(final int value) {
      this.value = value;
    }

    @Override
    public boolean equals(final Object other) {
      return other instanceof EqualByValue that && that.value == value;
    }

    @Override
    public int hashCode() {
      return value;
    }
  }

  @Test
  void distinctInstancesThatCompareEqualAreTrackedSeparately() {
    final Set<EqualByValue> set = new WeakIdentitySet<>();

    final EqualByValue first = new EqualByValue(42);
    final EqualByValue second = new EqualByValue(42);
    assertEquals(first, second, "precondition: these compare equal");

    set.add(first);
    set.add(second);

    assertEquals(2, set.size(),
        "identity, not equality — an equality-keyed set would lose one instance from the census");
    assertTrue(set.contains(first));
    assertTrue(set.contains(second));

    set.remove(first);
    assertEquals(1, set.size(), "removing one must not remove its equal twin");
    assertTrue(set.contains(second));
  }

  @Test
  void anElementNothingElseReferencesLeavesTheSet() {
    final Set<Object> set = new WeakIdentitySet<>();

    final Object retained = new Object();
    set.add(retained);
    set.add(new Object()); // no other reference — collectable

    assertEquals(2, set.size(), "precondition: both are registered");

    // The registry must not be what keeps an element alive; if it is, a GC-time leak detector can
    // never observe anything.
    final long deadline = System.currentTimeMillis() + 10_000L;
    while (set.size() > 1 && System.currentTimeMillis() < deadline) {
      System.gc();
      try {
        Thread.sleep(25L);
      } catch (final InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        break;
      }
    }

    assertEquals(1, set.size(),
        "the unreferenced element must be collectable — a strong registry pins it forever and "
            + "silently disables collection-based leak detection");
    assertTrue(set.contains(retained), "and the still-referenced one must survive");
  }

  @Test
  void emptinessAndClearBehaveLikeASet() {
    final Set<Object> set = new WeakIdentitySet<>();
    assertTrue(set.isEmpty());

    final Object element = new Object();
    assertTrue(set.add(element), "first add reports insertion");
    assertFalse(set.add(element), "re-adding the same instance is not a new element");
    assertFalse(set.isEmpty());

    set.clear();
    assertTrue(set.isEmpty());
    assertFalse(set.contains(element));
  }
}
