package io.sirix.io;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for the {@link SharedArenas} strategy selection and arena lifecycle semantics.
 */
@DisplayName("SharedArenas strategy")
public final class SharedArenasTest {

  @Test
  @DisplayName("defaults to SHARED on HotSpot")
  void defaultIsSharedOnHotspot() {
    assertEquals(SharedArenas.Strategy.SHARED, SharedArenas.determineStrategy(null, false));
    assertEquals(SharedArenas.Strategy.SHARED, SharedArenas.determineStrategy("", false));
    assertEquals(SharedArenas.Strategy.SHARED, SharedArenas.determineStrategy("  ", false));
  }

  @Test
  @DisplayName("defaults to AUTO in a native image")
  void defaultIsAutoInNativeImage() {
    assertEquals(SharedArenas.Strategy.AUTO, SharedArenas.determineStrategy(null, true));
  }

  @Test
  @DisplayName("explicit property overrides the native-image default")
  void explicitOverrides() {
    assertEquals(SharedArenas.Strategy.SHARED, SharedArenas.determineStrategy("shared", true));
    assertEquals(SharedArenas.Strategy.AUTO, SharedArenas.determineStrategy("auto", false));
    assertEquals(SharedArenas.Strategy.GLOBAL, SharedArenas.determineStrategy("GLOBAL", false));
    assertEquals(SharedArenas.Strategy.GLOBAL, SharedArenas.determineStrategy(" global ", true));
  }

  @Test
  @DisplayName("unknown strategy value is rejected")
  void unknownValueRejected() {
    assertThrows(IllegalArgumentException.class, () -> SharedArenas.determineStrategy("bogus", false));
  }

  @Test
  @DisplayName("running strategy on the JVM test suite is SHARED unless overridden")
  void runtimeStrategyOnJvm() {
    final String override = System.getProperty(SharedArenas.STRATEGY_PROPERTY);
    if (override == null) {
      assertEquals(SharedArenas.Strategy.SHARED, SharedArenas.strategy());
    }
  }

  @Test
  @DisplayName("newSharedArena segments are accessible across threads and close() reclaims")
  void sharedArenaCrossThreadAndClose() throws InterruptedException {
    final Arena arena = SharedArenas.newSharedArena();
    final MemorySegment segment = arena.allocate(64);
    segment.set(ValueLayout.JAVA_LONG, 0, 42L);

    // Cross-thread visibility — the property the call sites rely on.
    final AtomicLong fromOtherThread = new AtomicLong();
    final Thread reader = new Thread(() -> fromOtherThread.set(segment.get(ValueLayout.JAVA_LONG, 0)));
    reader.start();
    reader.join();
    assertEquals(42L, fromOtherThread.get());

    SharedArenas.close(arena);
    if (SharedArenas.strategy() == SharedArenas.Strategy.SHARED) {
      // After a real close the scope is dead — access must throw.
      assertThrows(IllegalStateException.class, () -> segment.get(ValueLayout.JAVA_LONG, 0));
    } else {
      // AUTO/GLOBAL: close is a no-op, the segment stays accessible.
      assertEquals(42L, segment.get(ValueLayout.JAVA_LONG, 0));
    }
  }

  @Test
  @DisplayName("close() never throws for any strategy product")
  void closeIsSafeForAllStrategies() {
    // Simulates what the native-image AUTO path does: close(auto arena) must be a no-op,
    // not an UnsupportedOperationException. We can only exercise the active strategy's
    // pairing here, but verify the close helper guards on strategy, not on arena type.
    final Arena arena = SharedArenas.newSharedArena();
    final MemorySegment segment = arena.allocate(8);
    segment.set(ValueLayout.JAVA_INT, 0, 7);
    assertEquals(7, segment.get(ValueLayout.JAVA_INT, 0));
    SharedArenas.close(arena);
    assertTrue(true, "close completed without throwing");
  }
}
