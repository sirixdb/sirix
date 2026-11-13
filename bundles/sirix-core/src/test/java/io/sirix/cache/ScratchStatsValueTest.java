package io.sirix.cache;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/** The metrics snapshot is a value: equal counter contents must not depend on array identity. */
final class ScratchStatsValueTest {
  @Test
  void comparesEveryCounterAndArrayByValue() {
    final ScratchStats expected = snapshot(-1);
    final ScratchStats copy = snapshot(-1);
    assertEquals(expected, copy);
    assertEquals(expected.hashCode(), copy.hashCode());
    for (int component = 0; component < 15; component++) {
      assertNotEquals(expected, snapshot(component), "different metric component " + component);
    }
    assertNotEquals(expected, null);
    final Object unrelated = "metrics";
    assertNotEquals(expected, unrelated);
  }

  @Test
  void printsCounterContents() {
    assertEquals("ScratchStats[capacityBytes=[1], acquisitions=[1], created=[1], reclaimed=[1], "
        + "lentBuffers=[1], idleBuffers=[1], requestedBytes=1, roundedBytes=1, lentBytes=1, idleBytes=1, "
        + "refusals=1, refusedBytes=1, staleReleases=1, acquisitionsByUse=" + useCounters() + ", requestedBytesByUse="
        + useCounters() + "]", snapshot(-1).toString());
  }

  private static String useCounters() {
    // The public per-use contract has one counter per enum value, initially one in this fixture.
    final StringBuilder text = new StringBuilder("[");
    for (int i = 0; i < ScratchUse.COUNT; i++) {
      if (i != 0) {
        text.append(", ");
      }
      text.append(1);
    }
    return text.append(']').toString();
  }

  private static ScratchStats snapshot(final int changed) {
    final long[][] arrays = new long[15][];
    for (int i = 0; i < arrays.length; i++) {
      arrays[i] = new long[i >= 13
          ? ScratchUse.COUNT
          : 1];
      for (int j = 0; j < arrays[i].length; j++) {
        arrays[i][j] = changed == i && j == 0
            ? 2
            : 1;
      }
    }
    return new ScratchStats(arrays[0], arrays[1], arrays[2], arrays[3], arrays[4], arrays[5], arrays[6][0],
        arrays[7][0], arrays[8][0], arrays[9][0], arrays[10][0], arrays[11][0], arrays[12][0], arrays[13], arrays[14]);
  }
}
