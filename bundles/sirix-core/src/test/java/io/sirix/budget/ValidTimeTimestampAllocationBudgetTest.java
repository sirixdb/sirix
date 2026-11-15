package io.sirix.budget;

import com.sun.management.ThreadMXBean;
import io.sirix.index.interval.IntervalDomain;
import io.sirix.index.interval.ValidTimeIntervalIndexWriter;
import io.sirix.index.interval.ValidTimeIntervalIndexWriter.Interval;
import io.sirix.utils.UtcTimestampParser;
import io.sirix.utils.UtcTimestampParser.ParsedInstant;
import java.lang.management.ManagementFactory;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Canonical valid-time bounds allocate only the interval retained by index maintenance. */
@Isolated
final class ValidTimeTimestampAllocationBudgetTest {
  private static final int ITERATIONS = 40_000;
  private static volatile long epochSink;
  private static volatile Instant instantSink;
  private static volatile Interval intervalSink;

  @Test
  void warmedWriterParsingHasNoTemporaryAllocationWithAnAllocatingControl() {
    final var platform = ManagementFactory.getThreadMXBean();
    assumeTrue(platform instanceof ThreadMXBean);
    final ThreadMXBean bean = (ThreadMXBean) platform;
    assumeTrue(bean.isThreadAllocatedMemorySupported());
    bean.setThreadAllocatedMemoryEnabled(true);
    final long thread = Thread.currentThread().threadId();
    final String[] inputs =
        {"2024-02-29T12:34:56Z", "2024-02-29T12:34:56.1Z", "2024-02-29T12:34:56.12Z", "2024-02-29T12:34:56.123Z",
            "2024-02-29T12:34:56.1234Z", "2024-02-29T12:34:56.12345Z", "2024-02-29T12:34:56.123456Z",
            "2024-02-29T12:34:56.1234567Z", "2024-02-29T12:34:56.12345678Z", "2024-02-29T12:34:56.123456789Z"};
    final ParsedInstant from = new ParsedInstant();
    final ParsedInstant to = new ParsedInstant();
    UtcTimestampParser.parseNullable("2025-01-01T00:00:00Z", to);
    final IntervalDomain domain = new IntervalDomain();
    final ValidTimeIntervalIndexWriter writer =
        new ValidTimeIntervalIndexWriter(null, null, null, null, domain, "vf", "vt");
    for (int i = 0; i < ITERATIONS; i++) {
      final String text = inputs[i % inputs.length];
      UtcTimestampParser.parseNullable(text, from);
      epochSink = from.epochSecond() + from.nano();
      intervalSink = writer.toPrimitiveInterval(from, to, 1, 1);
      instantSink = Instant.parse(text);
      intervalSink = new Interval(true, i + 256L, i + 512L, false, 1024);
    }
    final long start = bean.getThreadAllocatedBytes(thread);
    for (int i = 0; i < ITERATIONS; i++) {
      UtcTimestampParser.parseNullable(inputs[i % inputs.length], from);
      epochSink = from.epochSecond() + from.nano();
    }
    final long parserBytes = bean.getThreadAllocatedBytes(thread) - start;
    final long ownedStart = bean.getThreadAllocatedBytes(thread);
    for (int i = 0; i < ITERATIONS; i++) {
      intervalSink = new Interval(true, i + 256L, i + 512L, false, 1024);
    }
    final long ownedBytes = bean.getThreadAllocatedBytes(thread) - ownedStart;
    final long writerStart = bean.getThreadAllocatedBytes(thread);
    for (int i = 0; i < ITERATIONS; i++) {
      UtcTimestampParser.parseNullable(inputs[i % inputs.length], from);
      intervalSink = writer.toPrimitiveInterval(from, to, 1, 1);
    }
    final long writerBytes = bean.getThreadAllocatedBytes(thread) - writerStart;
    final long generalStart = bean.getThreadAllocatedBytes(thread);
    for (int i = 0; i < ITERATIONS; i++) {
      instantSink = Instant.parse(inputs[i % inputs.length]);
    }
    final long generalBytes = bean.getThreadAllocatedBytes(thread) - generalStart;
    assertEquals(Instant.parse(inputs[inputs.length - 1]).getEpochSecond(), from.epochSecond());
    assertEquals(123456789, from.nano());
    assertEquals(domain.point(Instant.parse(inputs[inputs.length - 1])), intervalSink.lo());
    assertTrue(ownedBytes >= ITERATIONS * 32L, "the retained interval result must escape and be counted");
    assertEquals(0, parserBytes, "the exact production parseNullable path must allocate no temporary objects");
    assertEquals(ownedBytes, writerBytes, "primitive writer mapping may allocate only its owned interval result");
    assertTrue(generalBytes >= ITERATIONS * 32L, "Instant.parse positive control must allocate");
    if (Boolean.getBoolean("sirix.workBudget.print")) {
      System.out.printf("valid-time allocation: parser=%d writer=%d owned=%d general=%d iterations=%d%n", parserBytes,
          writerBytes, ownedBytes, generalBytes, ITERATIONS);
    }
  }
}
