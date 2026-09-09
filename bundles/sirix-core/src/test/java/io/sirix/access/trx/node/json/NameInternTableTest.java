/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access.trx.node.json;

import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.Collections;
import java.util.IdentityHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins the canonical-name table the bulk scanner uses instead of building a String per key
 * occurrence.
 *
 * <p>
 * Every assertion here is about IDENTITY, not equality. Equality would pass for a table that
 * allocated a fresh instance every time — which is precisely the behaviour being removed — so
 * {@code assertSame} is the only assertion that can witness the property, and the one downstream
 * memos benefit from.
 * </p>
 */
final class NameInternTableTest {

  private static char[] buffer(final String prefix, final String name, final String suffix) {
    return (prefix + name + suffix).toCharArray();
  }

  @Test
  void repeatedOccurrencesShareOneInstanceAndAllocateNothingAfterTheFirst() {
    final NameInternTable table = new NameInternTable();
    final char[] first = "WatchID".toCharArray();
    final String canonical = table.intern(first, 0, first.length);

    // A DIFFERENT backing array holding the same characters must still resolve to the very same
    // instance — the scanner's decode buffer is reused and refilled, so the array is never stable.
    for (int repeat = 0; repeat < 100; repeat++) {
      final char[] other = buffer("xx", "WatchID", "yy");
      assertSame(canonical, table.intern(other, 2, 7), "occurrence " + repeat + " must be the canonical instance");
    }
    assertEquals("WatchID", canonical);
    assertEquals(1, table.size(), "one distinct name means one entry");
  }

  @Test
  void aSliceIsKeyedByItsCharactersNotByTheWholeBuffer() {
    final NameInternTable table = new NameInternTable();
    final char[] shared = "AdvEngineIDAdvEngine".toCharArray();

    final String full = table.intern(shared, 0, 11);
    final String prefix = table.intern(shared, 0, 10);
    final String tail = table.intern(shared, 11, 9);

    assertEquals("AdvEngineID", full);
    assertEquals("AdvEngineI", prefix);
    assertEquals("AdvEngine", tail);
    // Three distinct names out of one buffer: a length or offset that leaked into the key would
    // collapse two of these together and hand the scanner the wrong field name.
    assertNotSame(full, prefix);
    assertNotSame(full, tail);
    assertEquals(3, table.size());
    assertSame(tail, table.intern("AdvEngine".toCharArray(), 0, 9));
  }

  @Test
  void theEmptyNameIsRepresentable() {
    final NameInternTable table = new NameInternTable();
    final char[] chars = "abc".toCharArray();
    final String empty = table.intern(chars, 1, 0);
    assertEquals("", empty);
    assertSame(empty, table.intern("zzz".toCharArray(), 0, 0));
  }

  @Test
  void aSliceOutsideTheBufferIsRejected() {
    final NameInternTable table = new NameInternTable();
    final char[] chars = "abc".toCharArray();
    assertThrows(IndexOutOfBoundsException.class, () -> table.intern(chars, -1, 2));
    assertThrows(IndexOutOfBoundsException.class, () -> table.intern(chars, 0, 4));
    assertThrows(IndexOutOfBoundsException.class, () -> table.intern(chars, 2, 2));
    assertThrows(IndexOutOfBoundsException.class, () -> table.intern(chars, 0, -1));
  }

  @Test
  void theCapacityMustBeAPositivePowerOfTwo() {
    assertThrows(IllegalArgumentException.class, () -> new NameInternTable(0));
    assertThrows(IllegalArgumentException.class, () -> new NameInternTable(-2));
    assertThrows(IllegalArgumentException.class, () -> new NameInternTable(48));
  }

  @Test
  void anOverfullTableStillReturnsCorrectNamesInsteadOfWrongOnes() {
    // Two slots: far smaller than any real key set, so the probe limit is reached almost at once.
    // The contract past that point is degraded, never wrong — the caller gets an equal String and
    // pays what it paid before this table existed.
    final NameInternTable table = new NameInternTable(2);
    for (int i = 0; i < 200; i++) {
      final String name = "field" + i;
      final char[] chars = name.toCharArray();
      assertEquals(name, table.intern(chars, 0, chars.length), "a full table must never return another name");
    }
    assertTrue(table.size() <= 2, "the table does not grow");
  }

  @Test
  void concurrentInternsOfTheSameNameAgreeOnOneInstance() throws Exception {
    // The parallel importer hands one table to every chunk builder, so two threads WILL race on the
    // first occurrence of a name. Losing that race must hand back the winner's instance, not a
    // second canonical one: two instances would silently halve the memo's pointer-equality hit rate
    // and are invisible to any equality-based assertion.
    final NameInternTable table = new NameInternTable();
    final int threads = 8;
    final int names = 64;
    final ExecutorService pool = Executors.newFixedThreadPool(threads);
    try {
      final CountDownLatch start = new CountDownLatch(1);
      final Set<String> identities = Collections.newSetFromMap(new IdentityHashMap<>());
      final Set<String> synchronizedIdentities = Collections.synchronizedSet(identities);
      final ConcurrentHashMap<String, String> byValue = new ConcurrentHashMap<>();
      for (int t = 0; t < threads; t++) {
        pool.execute(() -> {
          try {
            start.await();
          } catch (final InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return;
          }
          for (int round = 0; round < 50; round++) {
            for (int n = 0; n < names; n++) {
              final char[] chars = ("column_" + n).toCharArray();
              final String canonical = table.intern(chars, 0, chars.length);
              assertEquals("column_" + n, canonical);
              synchronizedIdentities.add(canonical);
              byValue.put(canonical, canonical);
            }
          }
        });
      }
      start.countDown();
      pool.shutdown();
      assertTrue(pool.awaitTermination(60, TimeUnit.SECONDS), "workers must finish");
      assertEquals(names, byValue.size(), "every name must be seen");
      assertEquals(names, synchronizedIdentities.size(),
          "exactly one INSTANCE per name must survive the race, not one per thread");
    } finally {
      pool.shutdownNow();
    }
  }
}
