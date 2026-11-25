/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page.pax;

import io.sirix.io.SharedArenas;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Serializer-local confined arenas under the arena strategy a GraalVM native image runs with.
 *
 * <p>
 * Runs in its own JVM (Gradle task {@code sharedArenaAutoStrategyTest}) because
 * {@link SharedArenas} reads its strategy once at class initialization. Under {@code auto} — the
 * native-image default — {@link SharedArenas#close(Arena)} releases nothing, so a confined arena
 * handed to that path would live for the whole process. The owner that created it must close it
 * itself, and that is what this lane pins; the same assertions under the JVM default strategy pass
 * either way and therefore prove nothing about a native image.
 */
final class ConfinedWriterArenaStrategyTest {

  /** Create-and-release cycles: one close cannot show accumulation, a run of them can. */
  private static final int CYCLES = 256;

  @Test
  @DisplayName("a serializer's confined arena is closed by its creator under the native-image arena strategy")
  void confinedWriterTablesCloseTheirOwnArenaUnderTheAutomaticStrategy() {
    assertEquals(SharedArenas.Strategy.AUTO, SharedArenas.strategy(),
        "this lane must run the arena strategy a native image uses");
    assertFalse(SharedArenas.supportsDeterministicClose());

    // The hazard: under this strategy the shared-arena close path is a no-op by design.
    final Arena shared = SharedArenas.newSharedArena();
    final MemorySegment sharedPayload = shared.allocate(64L);
    SharedArenas.close(shared);
    assertTrue(sharedPayload.scope().isAlive(),
        "the shared-arena close path must stay a no-op here, so a confined arena may never use it");

    final byte[] payloadBytes = {1, 2, 3, 4, 5, 6, 7, 8};
    final MemorySegment[] payloads = new MemorySegment[CYCLES];
    for (int cycle = 0; cycle < CYCLES; cycle++) {
      final RegionTable table = RegionTable.newConfinedWriterTable();
      table.set(RegionTable.KIND_NUMBER, payloadBytes);
      final MemorySegment payload = table.payload(RegionTable.KIND_NUMBER);
      assertNotNull(payload, "cycle " + cycle + ": the writer table must hold a native payload");
      assertTrue(payload.isNative(), "cycle " + cycle + ": a confined writer payload is arena-backed");
      assertTrue(payload.scope().isAlive(), "cycle " + cycle + ": the payload must be live before close");
      payloads[cycle] = payload;

      table.close();

      assertFalse(payload.scope().isAlive(), "cycle " + cycle + ": close must reclaim the confined arena");
      assertNull(table.payload(RegionTable.KIND_NUMBER));
      assertEquals(0L, table.retainedFootprintBytes(), "cycle " + cycle + ": a closed table retains nothing");
    }

    for (int cycle = 0; cycle < CYCLES; cycle++) {
      assertFalse(payloads[cycle].scope().isAlive(),
          "cycle " + cycle + ": no earlier cycle's arena may still be alive after the run");
    }
  }
}
