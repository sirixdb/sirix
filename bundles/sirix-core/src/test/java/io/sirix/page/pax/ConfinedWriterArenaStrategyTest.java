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
 * Serializer-local confined arenas across startup arena policies.
 *
 * <p>
 * Runs in the shared-arena policy forks because {@link SharedArenas} reads its strategy once at
 * class initialization. Under {@code auto} — the native-image default —
 * {@link SharedArenas#close(Arena)} releases nothing, so a confined arena handed to that path would
 * live for the whole process. The owner that created it must close it itself under every strategy.
 */
final class ConfinedWriterArenaStrategyTest {

  /** Create-and-release cycles: one close cannot show accumulation, a run of them can. */
  private static final int CYCLES = 256;

  @Test
  @DisplayName("a serializer's confined arena is closed by its creator under every arena strategy")
  void confinedWriterTablesCloseTheirOwnArenaUnderTheSelectedStrategy() {
    final String mode = System.getProperty("sirix.test.arenaPolicy");
    final SharedArenas.Strategy expected = switch (mode) {
      case "shared-strategy", "diagnostic-skip" -> SharedArenas.Strategy.SHARED;
      case "auto-strategy" -> SharedArenas.Strategy.AUTO;
      case "diagnostic-global" -> SharedArenas.Strategy.GLOBAL;
      default -> throw new AssertionError("unexpected arena-policy fork: " + mode);
    };
    assertEquals(expected, SharedArenas.strategy());
    assertEquals(expected == SharedArenas.Strategy.SHARED, SharedArenas.supportsDeterministicClose());
    assertEquals("diagnostic-skip".equals(mode), SharedArenas.skipArenaClose());

    final Arena shared = SharedArenas.newSharedArena();
    final MemorySegment sharedPayload = shared.allocate(64L);
    SharedArenas.close(shared);
    assertEquals(expected != SharedArenas.Strategy.SHARED, sharedPayload.scope().isAlive());

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
