package io.sirix.query.budget;

import com.sun.management.ThreadMXBean;
import io.brackit.query.atomic.DateTime;
import io.brackit.query.atomic.QNm;
import io.sirix.query.json.AtomicStrJsonDBItem;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBObject;
import io.sirix.query.json.StoredDateTimeParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Allocation units, never elapsed time: SH1 UTC fields must bypass substring/calendar parsing. */
final class StoredDateTimeAllocationBudgetTest {
  private static volatile long epochSink;
  private static volatile DateTime dateSink;
  private static final int ITERATIONS = 20_000;

  @TempDir
  Path directory;

  @Test
  void fixedUtcParserAllocatesNothingAndFieldMemoReusesTheCast() {
    final var platformBean = ManagementFactory.getThreadMXBean();
    assumeTrue(platformBean instanceof ThreadMXBean);
    final var bean = (ThreadMXBean) platformBean;
    assumeTrue(bean.isThreadAllocatedMemorySupported());
    if (!bean.isThreadAllocatedMemoryEnabled()) {
      bean.setThreadAllocatedMemoryEnabled(true);
    }
    final long thread = Thread.currentThread().threadId();
    final String text = "2024-06-29T00:00:00Z";
    final byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
    // Initialize the codec/calendar classes and let compilation settle before counting work.
    for (int iteration = 0; iteration < ITERATIONS; iteration++) {
      epochSink = StoredDateTimeParser.epochMillis(bytes, 0, bytes.length);
      dateSink = new DateTime(text);
    }
    final long fastStart = bean.getThreadAllocatedBytes(thread);
    for (int iteration = 0; iteration < ITERATIONS; iteration++) {
      epochSink = StoredDateTimeParser.epochMillis(bytes, 0, bytes.length);
    }
    final long fastBytes = bean.getThreadAllocatedBytes(thread) - fastStart;
    final long generalStart = bean.getThreadAllocatedBytes(thread);
    for (int iteration = 0; iteration < ITERATIONS; iteration++) {
      dateSink = new DateTime(text);
    }
    final long generalBytes = bean.getThreadAllocatedBytes(thread) - generalStart;
    assertEquals(0, fastBytes, "fixed-layout primitive parse must allocate no substrings, char arrays or dates");
    assertTrue(generalBytes >= ITERATIONS * 32L, "general parser mutation witness must allocate at least its result");

    try (final var store = BasicJsonDBStore.newBuilder().location(directory).build()) {
      final var collection = store.create("dates", "r", "{\"vf\":\"" + text + "\",\"vt\":\"2025-01-01T00:00:00Z\"}");
      final JsonDBObject object = (JsonDBObject) collection.getDocument("r");
      final QNm field = new QNm("vf");
      final var value = (AtomicStrJsonDBItem) object.get(field);
      final DateTime parsed = value.dateTime();
      assertSame(parsed, value.dateTime());
      assertEquals(epochSink, value.epochMillis());
      for (int iteration = 0; iteration < ITERATIONS; iteration++) {
        dateSink = ((AtomicStrJsonDBItem) object.get(field)).dateTime();
      }
      final long memoStart = bean.getThreadAllocatedBytes(thread);
      for (int iteration = 0; iteration < ITERATIONS; iteration++) {
        dateSink = ((AtomicStrJsonDBItem) object.get(field)).dateTime();
      }
      assertEquals(0, bean.getThreadAllocatedBytes(thread) - memoStart,
          "repeated field casts must reuse the parsed value");
      assertSame(parsed, dateSink);
    }
  }
}
