package io.sirix.io.memorymapped;

import io.sirix.io.SharedArenas;
import io.sirix.io.memorymapped.MMStorage.ArenaGeneration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

final class ArenaGenerationLifetimeTest {
  @Test
  void oldGenerationClosesOnlyAfterItsLastReaderReturns() {
    assumeTrue(SharedArenas.supportsDeterministicClose() && !Boolean.getBoolean("sirix.mmstorage.skipArenaClose"));
    assertFalse(SharedArenas.skipArenaClose());
    final Arena arena = Arena.ofShared();
    try {
      final MemorySegment data = arena.allocate(ValueLayout.JAVA_LONG);
      final MemorySegment revisions = arena.allocate(ValueLayout.JAVA_LONG);
      data.set(ValueLayout.JAVA_LONG, 0, 23L);
      final ArenaGeneration generation = new ArenaGeneration(arena, data, revisions);
      generation.incrementRefCount();
      generation.incrementRefCount();
      generation.markAsOld();
      assertFalse(generation.decrementRefCount());
      assertTrue(data.scope().isAlive());
      assertEquals(23L, data.get(ValueLayout.JAVA_LONG, 0));
      assertTrue(generation.decrementRefCount());
      assertFalse(data.scope().isAlive());
      assertThrows(IllegalStateException.class, () -> data.get(ValueLayout.JAVA_LONG, 0));
    } finally {
      if (arena.scope().isAlive()) {
        arena.close();
      }
    }
  }

  @Test
  void currentGenerationStaysLiveUntilStorageClose() {
    assumeTrue(SharedArenas.supportsDeterministicClose() && !Boolean.getBoolean("sirix.mmstorage.skipArenaClose"));
    assertFalse(SharedArenas.skipArenaClose());
    final Arena arena = Arena.ofShared();
    try {
      final MemorySegment data = arena.allocate(ValueLayout.JAVA_LONG);
      final ArenaGeneration generation = new ArenaGeneration(arena, data, data);
      generation.incrementRefCount();
      assertFalse(generation.decrementRefCount());
      assertTrue(data.scope().isAlive());
      generation.close();
      assertFalse(data.scope().isAlive());
    } finally {
      if (arena.scope().isAlive()) {
        arena.close();
      }
    }
  }

  @Test
  @EnabledIfSystemProperty(named = "sirix.mmstorage.skipArenaClose", matches = "true")
  void diagnosticSkipCloseKeepsMappedGenerationLive() {
    assertTrue(SharedArenas.skipArenaClose());
    assertEquals(SharedArenas.Strategy.SHARED, SharedArenas.strategy());
    try (final Arena arena = Arena.ofShared()) {
      final MemorySegment data = arena.allocate(ValueLayout.JAVA_LONG);
      data.set(ValueLayout.JAVA_LONG, 0, 41L);
      final ArenaGeneration generation = new ArenaGeneration(arena, data, data);
      generation.close();
      assertTrue(data.scope().isAlive());
      assertEquals(41L, data.get(ValueLayout.JAVA_LONG, 0));
    }
  }
}
