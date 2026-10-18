package io.sirix.page;

import java.util.Arrays;

import io.sirix.index.IndexType;

/**
 * Write-side state for partitioned sticky body-codec elections.
 *
 * <p>The reader must not carry the partition table or its configuration branch. Keeping this state
 * in a separate class also delays its class initialization until a body is written.</p>
 */
final class PartitionedBodyCodecElection {
  static boolean enabled = Boolean.getBoolean("sirix.codecBakeoff.partitioned");

  private static final ThreadLocal<int[]> STATE = new ThreadLocal<>();

  private PartitionedBodyCodecElection() {
  }

  static int[] state() {
    int[] state = STATE.get();
    if (state == null) {
      int maxId = 0;
      for (final IndexType type : IndexType.values()) {
        maxId = Math.max(maxId, type.getID());
      }
      state = new int[(maxId + 1) * Integer.SIZE * 3];
      STATE.set(state);
    }
    return state;
  }

  static int offset(final byte indexTypeId, final int totalBytes) {
    final int bucket = Integer.SIZE - 1 - Integer.numberOfLeadingZeros(totalBytes | 1);
    return (indexTypeId * Integer.SIZE + bucket) * 3;
  }

  static void reset() {
    final int[] state = STATE.get();
    if (state != null) {
      Arrays.fill(state, 0);
    }
  }
}
