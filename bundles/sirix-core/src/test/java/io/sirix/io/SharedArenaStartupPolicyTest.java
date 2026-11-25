package io.sirix.io;

import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Each launcher-policy case runs in its own JVM, before SharedArenas can be initialized. */
final class SharedArenaStartupPolicyTest {
  @Test
  void launcherPolicyHonorsDiagnosticOptInAndRejectsUnsafeProductionSettings() {
    final String mode = System.getProperty("sirix.test.arenaPolicy");
    if ("shared-strategy".equals(mode)) {
      assertEquals(SharedArenas.Strategy.SHARED, SharedArenas.strategy());
      assertFalse(SharedArenas.skipArenaClose());
      assertTrue(SharedArenas.supportsDeterministicClose());
      final Arena arena = SharedArenas.newSharedArena();
      final MemorySegment data = arena.allocate(ValueLayout.JAVA_LONG);
      SharedArenas.close(arena);
      assertFalse(data.scope().isAlive());
      return;
    }
    if ("diagnostic-global".equals(mode)) {
      assertEquals(SharedArenas.Strategy.GLOBAL, SharedArenas.strategy());
      assertFalse(SharedArenas.skipArenaClose());
      assertFalse(SharedArenas.supportsDeterministicClose());
      final Arena arena = SharedArenas.newSharedArena();
      assertSame(Arena.global(), arena);
      final MemorySegment data = arena.allocate(ValueLayout.JAVA_LONG);
      data.set(ValueLayout.JAVA_LONG, 0, 19L);
      SharedArenas.close(arena);
      assertTrue(data.scope().isAlive());
      assertEquals(19L, data.get(ValueLayout.JAVA_LONG, 0));
      return;
    }
    if ("diagnostic-skip".equals(mode)) {
      assertEquals(SharedArenas.Strategy.SHARED, SharedArenas.strategy());
      assertTrue(SharedArenas.skipArenaClose());
      return;
    }
    if ("auto-strategy".equals(mode)) {
      assertEquals(SharedArenas.Strategy.AUTO, SharedArenas.strategy());
      assertFalse(SharedArenas.skipArenaClose());
      assertFalse(SharedArenas.supportsDeterministicClose());
      final Arena arena = SharedArenas.newSharedArena();
      final MemorySegment data = arena.allocate(ValueLayout.JAVA_LONG);
      data.set(ValueLayout.JAVA_LONG, 0, 23L);
      SharedArenas.close(arena);
      assertTrue(data.scope().isAlive(), "an auto arena is GC-reclaimed, so close(Arena) must release nothing");
      assertEquals(23L, data.get(ValueLayout.JAVA_LONG, 0));
      return;
    }
    assertTrue("reject-global".equals(mode) || "reject-skip".equals(mode),
        "a launcher fixture must select a known policy case");
    final ExceptionInInitializerError failure = assertThrows(ExceptionInInitializerError.class, SharedArenas::strategy);
    final IllegalArgumentException cause = assertInstanceOf(IllegalArgumentException.class, failure.getCause());
    assertTrue(cause.getMessage().contains("sirix.diagnostics.allowUnsafeLifetime"));
    assertTrue(cause.getMessage()
                    .contains("reject-global".equals(mode)
                        ? "sirix.arena.strategy=global"
                        : "sirix.mmstorage.skipArenaClose=true"));
  }
}
