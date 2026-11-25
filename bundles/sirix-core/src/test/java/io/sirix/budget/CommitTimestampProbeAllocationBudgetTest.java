package io.sirix.budget;

import com.sun.management.ThreadMXBean;
import io.sirix.utils.CommitTimestamps;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import java.lang.management.ManagementFactory;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Converting a probe instant into the stored epoch-microsecond unit of VER-201 happens once per
 * commit and once per open-by-time lookup, so it must not allocate. The reverse conversion is the
 * positive control: it must allocate, because it materializes an {@link Instant}, which keeps the
 * zero bound above from being vacuous.
 *
 * <p>
 * Measured on the fixed branch: 0 bytes over 16,384 forward conversions, against 16,384 allocating
 * reverse conversions in the same loop. Converting through {@code Instant.toEpochMilli()} and back,
 * as the millisecond code path did, allocates on the reverse side in exactly the same way.
 */
@Isolated
final class CommitTimestampProbeAllocationBudgetTest {

  private static final int PROBES = 16_384;

  /**
   * Distinct probes, so the conversion cannot be constant-folded into a compile-time value and the
   * zero bound measures the real arithmetic.
   */
  private static final Instant[] PROBE_ROTATION = {Instant.parse("2026-11-21T12:34:56.123456789Z"),
      Instant.parse("1952-03-04T05:06:07.000123456Z"), Instant.EPOCH, Instant.parse("2026-11-21T12:34:56.000001Z")};

  /** Keeps the conversions from being optimized away without allocating itself. */
  private static volatile long sink;

  /** Forces the reverse conversion's result to escape, so it cannot be scalar-replaced. */
  private static volatile Instant instantSink = Instant.EPOCH;

  @Test
  void probeConversionAllocatesNothingWhileTheReverseConversionProvesTheMeasurementIsLive() {
    final var platformBean = ManagementFactory.getThreadMXBean();
    assumeTrue(platformBean instanceof ThreadMXBean);
    final ThreadMXBean bean = (ThreadMXBean) platformBean;
    assumeTrue(bean.isThreadAllocatedMemorySupported());
    if (!bean.isThreadAllocatedMemoryEnabled()) {
      bean.setThreadAllocatedMemoryEnabled(true);
    }
    final long thread = Thread.currentThread().threadId();

    // Keep warmup and measurement at one callsite, as the other allocation budgets here do.
    long forwardBytes = 0;
    long reverseBytes = 0;
    for (int pass = 0; pass < 9; pass++) {
      forwardBytes = 0;
      reverseBytes = 0;
      for (int probe = 0; probe < PROBES; probe++) {
        forwardBytes += forwardAllocatedBytes(bean, thread, PROBE_ROTATION[probe & 3]);
        reverseBytes += reverseAllocatedBytes(bean, thread);
      }
    }

    assertEquals(0, forwardBytes, "instant to epoch-microsecond conversion must not allocate");
    assertTrue(reverseBytes > 0,
        "the reverse conversion must allocate its Instant, otherwise the zero bound is vacuous");
    final long micros = CommitTimestamps.toEpochMicros(PROBE_ROTATION[0]);
    assertEquals(micros, CommitTimestamps.toEpochMicros(CommitTimestamps.toInstant(micros)),
        "the measured conversions must still be the correct ones");
  }

  private static long forwardAllocatedBytes(final ThreadMXBean bean, final long thread, final Instant probe) {
    final long before = bean.getThreadAllocatedBytes(thread);
    sink ^= CommitTimestamps.toEpochMicros(probe);
    return bean.getThreadAllocatedBytes(thread) - before;
  }

  private static long reverseAllocatedBytes(final ThreadMXBean bean, final long thread) {
    final long before = bean.getThreadAllocatedBytes(thread);
    instantSink = CommitTimestamps.toInstant(sink | 1L);
    return bean.getThreadAllocatedBytes(thread) - before;
  }
}
