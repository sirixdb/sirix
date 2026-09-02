/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The chunk recycler's contract: a given chunk comes back zeroed and IDENTICAL on the next take, only
 * its own length is accepted, the cap drops rather than grows, and a drain leaves nothing to measure.
 */
final class LongChunkPoolTest {

  @Test
  @DisplayName("a given chunk is zeroed and handed back by the next take; a fresh take allocates")
  void giveThenTakeRecyclesTheSameArrayZeroed() {
    final LongChunkPool pool = new LongChunkPool(64, 8);
    final long[] fresh = pool.take();
    assertEquals(64, fresh.length);
    assertEquals(1L, pool.misses());
    assertEquals(0L, pool.hits());
    Arrays.fill(fresh, 0xDEADL);
    assertTrue(pool.give(fresh), "a chunk of the pool's length is kept");
    assertEquals(1, pool.pooled());
    for (final long lane : fresh) {
      assertEquals(0L, lane, "a pooled chunk is zeroed on give, so a stale reference reads EMPTY buckets");
    }
    final long[] again = pool.take();
    assertSame(fresh, again, "the recycled array itself comes back");
    assertEquals(1L, pool.hits());
    assertEquals(0, pool.pooled());
    final long[] third = pool.take();
    assertNotSame(fresh, third, "an empty pool allocates");
    assertEquals(2L, pool.misses());
  }

  @Test
  @DisplayName("only the pool's own chunk length is accepted")
  void wrongLengthIsRefused() {
    final LongChunkPool pool = new LongChunkPool(64, 8);
    assertFalse(pool.give(new long[63]));
    assertFalse(pool.give(new long[65]));
    assertFalse(pool.give(null));
    assertEquals(0, pool.pooled());
    assertEquals(0L, pool.dropped(), "a refused length is not a capacity drop");
  }

  @Test
  @DisplayName("past the cap a give is dropped to the collector, and the count never exceeds the cap")
  void capDropsInsteadOfGrowing() {
    final LongChunkPool pool = new LongChunkPool(16, 2);
    assertTrue(pool.give(new long[16]));
    assertTrue(pool.give(new long[16]));
    assertFalse(pool.give(new long[16]));
    assertEquals(2, pool.pooled());
    assertEquals(1L, pool.dropped());
    pool.take();
    assertTrue(pool.give(new long[16]), "room again once a chunk was taken");
    assertEquals(2, pool.pooled());
  }

  @Test
  @DisplayName("a drain leaves nothing pooled and the next take allocates")
  void drainEmptiesThePool() {
    final LongChunkPool pool = new LongChunkPool(16, 8);
    for (int i = 0; i < 5; i++) {
      pool.give(new long[16]);
    }
    assertEquals(5, pool.pooled());
    pool.drain();
    assertEquals(0, pool.pooled());
    final long missesBefore = pool.misses();
    pool.take();
    assertEquals(missesBefore + 1L, pool.misses(), "a drained pool has nothing to hand out");
  }

  @Test
  @DisplayName("the process-wide witnesses count hits and gives across pools")
  void processWideWitnesses() {
    final long hitsBefore = LongChunkPool.totalHits();
    final long givesBefore = LongChunkPool.totalGives();
    final LongChunkPool pool = new LongChunkPool(16, 8);
    pool.give(pool.take());
    pool.take();
    assertEquals(hitsBefore + 1L, LongChunkPool.totalHits());
    assertEquals(givesBefore + 1L, LongChunkPool.totalGives());
  }

  @Test
  @DisplayName("constructor arguments are checked")
  void argumentsAreChecked() {
    assertThrows(IllegalArgumentException.class, () -> new LongChunkPool(0, 8));
    assertThrows(IllegalArgumentException.class, () -> new LongChunkPool(16, 0));
  }
}
