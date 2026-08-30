/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class GlobalValueDictionaryProbeFrontTest {

  @Test
  void chunkedFrontCrossesEveryOldGrowthBoundaryWithoutAWholeArrayCopy() {
    final GlobalValueDictionaryProbeFront front = new GlobalValueDictionaryProbeFront(3, 8L << 20);
    try {
      for (int id = 1; id <= 20_000; id++) {
        final byte[] value = value(id);
        final long hash = GlobalValueDictionary.valueHash(value, 0, value.length);
        final long secondaryHash = GlobalValueDictionary.secondaryValueHash(value, 0, value.length);
        assertEquals(0, front.findId(hash, secondaryHash, value, 0, value.length));
        front.put(hash, secondaryHash, value, 0, value.length, id);
      }

      for (int id = 1; id <= 20_000; id++) {
        final byte[] value = value(id);
        final long hash = GlobalValueDictionary.valueHash(value, 0, value.length);
        final long secondaryHash = GlobalValueDictionary.secondaryValueHash(value, 0, value.length);
        assertEquals(id, front.findId(hash, secondaryHash, value, 0, value.length));
      }

      assertEquals(20_000, front.entryCount());
      assertTrue(front.retainedBytes() <= 8L << 20);
      assertTrue(front.tableCapacityForTest() >= 2 * front.entryCount());
      assertTrue(front.largestBackingArrayPayloadBytesForTest() <= 256 << 10,
          "every backing array must stay below the minimum G1 humongous threshold");
    } finally {
      front.release();
    }
  }

  @Test
  void primaryHashCollisionStillComparesTheExactBytes() {
    final GlobalValueDictionaryProbeFront front = new GlobalValueDictionaryProbeFront(1, 1L << 20);
    try {
      final byte[] first = "alpha".getBytes(StandardCharsets.UTF_8);
      final byte[] second = "omega".getBytes(StandardCharsets.UTF_8);
      final long forcedPrimaryHash = 42L;
      front.put(forcedPrimaryHash, 11L, first, 0, first.length, 1);
      front.put(forcedPrimaryHash, 22L, second, 0, second.length, 2);

      assertEquals(1, front.findId(forcedPrimaryHash, 11L, first, 0, first.length));
      assertEquals(2, front.findId(forcedPrimaryHash, 22L, second, 0, second.length));
      assertEquals(0, front.findId(forcedPrimaryHash, 11L, second, 0, second.length));
    } finally {
      front.release();
    }
  }

  @Test
  void writerArenaTransferCrossesChunkBoundariesAndLeavesTheFrontOwningItsBytes() {
    final GlobalValueDictionaryWriter writer = new GlobalValueDictionaryWriter(0, 64L << 20);
    final GlobalValueDictionaryProbeFront front = new GlobalValueDictionaryProbeFront(0, 64L << 20);
    final byte[] prefix = new byte[GlobalValueDictionaryWriter.ARENA_CHUNK_BYTES - 8];
    final byte[] crossing = new byte[32];
    Arrays.fill(prefix, (byte) 0x5a);
    Arrays.fill(crossing, (byte) 0x33);
    try {
      assertEquals(1, writer.intern(prefix, 0, prefix.length));
      assertEquals(2, writer.intern(crossing, 0, crossing.length));
      writer.copyValueToProbeFront(1, front);
      writer.copyValueToProbeFront(2, front);
      writer.release();

      final long prefixHash = GlobalValueDictionary.valueHash(prefix, 0, prefix.length);
      final long prefixSecondaryHash = GlobalValueDictionary.secondaryValueHash(prefix, 0, prefix.length);
      final long crossingHash = GlobalValueDictionary.valueHash(crossing, 0, crossing.length);
      final long crossingSecondaryHash = GlobalValueDictionary.secondaryValueHash(crossing, 0, crossing.length);
      assertEquals(1, front.findId(prefixHash, prefixSecondaryHash, prefix, 0, prefix.length));
      assertEquals(2, front.findId(crossingHash, crossingSecondaryHash, crossing, 0, crossing.length),
          "the front's independently owned copy must survive release of the source arena");
    } finally {
      writer.release();
      front.release();
    }
  }

  @Test
  void budgetRefusalHappensBeforeTheChunkThatWouldCrossIt() {
    final long budget = 512L << 10;
    final GlobalValueDictionaryProbeFront front = new GlobalValueDictionaryProbeFront(2, budget);
    try {
      final GlobalDictionaryBudgetExceededException refusal =
          assertThrows(GlobalDictionaryBudgetExceededException.class, () -> {
            for (int nextId = 1;; nextId++) {
              final byte[] value = ("value-" + nextId + "-" + "x".repeat(96)).getBytes(StandardCharsets.UTF_8);
              final long hash = GlobalValueDictionary.valueHash(value, 0, value.length);
              final long secondaryHash = GlobalValueDictionary.secondaryValueHash(value, 0, value.length);
              front.put(hash, secondaryHash, value, 0, value.length, nextId);
            }
          });
      assertTrue(front.retainedBytes() <= budget);
      assertTrue(refusal.breachingBytes() > budget);
      assertEquals("projected-resident", refusal.breachingTerm());
    } finally {
      front.release();
    }
  }

  @Test
  void releaseMakesTheFrontUnusable() {
    final GlobalValueDictionaryProbeFront front = new GlobalValueDictionaryProbeFront(0, 1L << 20);
    front.release();
    final byte[] value = {1};
    assertThrows(IllegalStateException.class, () -> front.findId(1L, 2L, value, 0, value.length));
    assertThrows(IllegalStateException.class, () -> front.put(1L, 2L, value, 0, value.length, 1));
  }

  private static byte[] value(final int id) {
    return ("value-" + id + "-" + Integer.toUnsignedString(id * 0x9E3779B9, 36)).getBytes(StandardCharsets.UTF_8);
  }
}
