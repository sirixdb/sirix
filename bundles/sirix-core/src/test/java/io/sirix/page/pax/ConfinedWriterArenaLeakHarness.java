package io.sirix.page.pax;

import io.sirix.io.SharedArenas;
import java.lang.foreign.MemorySegment;
import java.util.Arrays;

public final class ConfinedWriterArenaLeakHarness {
  private static final int CYCLES = 4096;
  private static final int PAYLOAD_BYTES = 64 * 1024;

  private ConfinedWriterArenaLeakHarness() {}

  public static void main(final String[] args) {
    if (SharedArenas.strategy() != SharedArenas.Strategy.AUTO || SharedArenas.supportsDeterministicClose()
        || SharedArenas.skipArenaClose()) {
      throw new AssertionError("the harness requires the native-image AUTO arena policy");
    }
    final byte[] bytes = new byte[PAYLOAD_BYTES];
    Arrays.fill(bytes, (byte) 7);
    final MemorySegment[] payloads = new MemorySegment[CYCLES];
    long allocatedBytes = 0;
    for (int cycle = 0; cycle < CYCLES; cycle++) {
      final RegionTable table = RegionTable.newConfinedWriterTable();
      try (table) {
        table.set(RegionTable.KIND_NUMBER, bytes);
        final MemorySegment payload = table.payload(RegionTable.KIND_NUMBER);
        if (payload == null || !payload.isNative() || !payload.scope().isAlive()
            || payload.mismatch(MemorySegment.ofArray(bytes)) != -1L) {
          throw new AssertionError("cycle " + cycle + ": missing native payload");
        }
        payloads[cycle] = payload;
        allocatedBytes += table.retainedFootprintBytes();
      }
      if (payloads[cycle].scope().isAlive() || table.retainedFootprintBytes() != 0L
          || table.payload(RegionTable.KIND_NUMBER) != null) {
        throw new AssertionError("cycle " + cycle + ": confined arena retained after release");
      }
    }
    long retainedPayloadBytes = 0;
    for (final MemorySegment payload : payloads) {
      if (payload.scope().isAlive()) {
        retainedPayloadBytes += payload.byteSize();
      }
    }
    if (retainedPayloadBytes != 0L) {
      throw new AssertionError("retained payload bytes: " + retainedPayloadBytes);
    }
    System.out.println("strategy=" + SharedArenas.strategy() + " cycles=" + CYCLES + " payloadBytesPerCycle="
        + PAYLOAD_BYTES + " allocatedBytes=" + allocatedBytes + " retainedPayloadBytes=" + retainedPayloadBytes);
  }
}
