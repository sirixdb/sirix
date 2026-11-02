package io.sirix.query.bench.bitemporal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.management.ThreadMXBean;
import io.sirix.query.bench.bitemporal.BitemporalIntervalStore.Intervals;
import io.sirix.query.bench.bitemporal.BitemporalSchema.Tier;
import java.io.IOException;
import java.io.StringReader;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Random;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class BitemporalLoaderBudgetTest {
  private static final int REPLACEMENT_OPERATIONS = 40_000;
  private static final int WINDOWS_PER_CONTROL = 6;
  private static volatile Object allocationSink;

  @TempDir
  Path directory;

  @Test
  void parserHasOneRecordWindowAndNoPerEventAllocation() throws Exception {
    final Path path = directory.resolve("events.jsonl");
    BitemporalStorageEpochTest.generateEvents(path);
    final String input = Files.readString(path);
    final ThreadMXBean bean = (ThreadMXBean) ManagementFactory.getThreadMXBean();
    bean.setThreadAllocatedMemoryEnabled(true);
    final long thread = Thread.currentThread().threadId();
    for (int pass = 0; pass < 6; pass++) {
      try (BitemporalEventReader reader = new BitemporalEventReader(new StringReader(input), Tier.DEVELOPMENT)) {
        final int[] payload = reader.payload;
        final long bytes = parsingAllocatedBytes(reader, bean, thread);
        if (pass >= 4) {
          assertEquals(0, bytes, "warmed canonical parsing must not allocate per event");
        }
        assertSame(payload, reader.payload);
        assertSame(BitemporalSchema.SUPPLIERS, reader.table);
        assertEquals(4_776, reader.count());
        reader.requireEnd();
      }
    }
  }

  @Test
  void rejectsMissingDuplicateOutOfOrderAndTruncatedInput() throws Exception {
    final String first = "{\"epoch\":0,\"table\":\"contracts\",\"op\":\"delete\",\"id\":1,\"a\":0,\"b\":366}";
    for (final String input : new String[] {first.replace(",\"a\":0", ""), first.replace("\"a\":0", "\"a\":0,\"a\":0"),
        first.replace("366", "367"), first.replace("\"id\":1", "\"id\":2001"), first.replace("\"id\":1", "\"id\":01"),
        first.substring(0, first.length() - 1), first.replace("contracts", "unknown")}) {
      try (BitemporalEventReader reader = new BitemporalEventReader(new StringReader(input), Tier.DEVELOPMENT)) {
        assertThrows(IllegalArgumentException.class, reader::advance);
      }
    }
    try (BitemporalEventReader reader =
        new BitemporalEventReader(new StringReader(first + '\n' + first), Tier.DEVELOPMENT)) {
      assertTrue(reader.advance());
      assertThrows(IllegalArgumentException.class, reader::advance);
    }
    try (BitemporalEventReader reader = new BitemporalEventReader(new StringReader(first), Tier.DEVELOPMENT)) {
      assertTrue(reader.advance());
      assertThrows(IllegalArgumentException.class, reader::advance, "EOF must check whole-stream cardinality");
    }
  }

  @Test
  void intervalReplacementMatchesIndependentDayModelAcrossSlabs() throws Exception {
    final int identities = BitemporalIntervalStore.SLAB_SIZE + 3;
    final int[][] days = new int[identities + 1][BitemporalSchema.HORIZON_DAYS];
    final Random random = new Random(42);
    final Intervals old = new Intervals();
    final Intervals next = new Intervals();
    try (BitemporalIntervalStore owner = new BitemporalIntervalStore(16L << 20);
        BitemporalEventReader event = new BitemporalEventReader(new StringReader(""), Tier.DEVELOPMENT)) {
      final BitemporalIntervalStore.Relation relation = owner.relation(identities);
      for (int operation = 0; operation < 30_000; operation++) {
        event.id = 1 + random.nextInt(identities);
        event.from = random.nextInt(BitemporalSchema.HORIZON_DAYS);
        event.to = event.from + 1 + random.nextInt(BitemporalSchema.HORIZON_DAYS - event.from);
        event.put = random.nextBoolean();
        event.payload[0] = 1 + random.nextInt(3);
        Arrays.fill(days[event.id], event.from, event.to, event.put
            ? event.payload[0]
            : 0);
        relation.read(event.id, old);
        old.replace(event, next);
        for (int row = 0; row < next.size; row++) {
          next.keys[row] = ((long) event.id << 32) | next.get(row, 0);
        }
        relation.write(event.id, next);
        relation.read(event.id, old);
        assertEquals(next.size, old.size);
        int row = 0;
        for (int day = 0; day < BitemporalSchema.HORIZON_DAYS; day++) {
          while (row < old.size && old.get(row, 1) <= day) {
            row++;
          }
          final int actual = row < old.size && old.get(row, 0) <= day
              ? old.get(row, 2)
              : 0;
          assertEquals(days[event.id][day], actual);
          if (actual != 0) {
            assertEquals(((long) event.id << 32) | old.get(row, 0), old.keys[row]);
          }
        }
        for (int i = 1; i < old.size; i++) {
          assertTrue(old.get(i - 1, 1) <= old.get(i, 0));
          assertFalse(old.get(i - 1, 1) == old.get(i, 0) && old.get(i - 1, 2) == old.get(i, 2));
        }
      }
      assertTrue(owner.reservedBytes() < 4L << 20, "native state stays proportional to live intervals");
    }
  }

  @Test
  void scratchAndNativeSlotsAreReusedAndFailureClosesOwner() throws Exception {
    final ThreadMXBean bean = (ThreadMXBean) ManagementFactory.getThreadMXBean();
    bean.setThreadAllocatedMemoryEnabled(true);
    final long thread = Thread.currentThread().threadId();
    final Intervals old = new Intervals();
    final Intervals next = new Intervals();
    final BitemporalIntervalStore owner = new BitemporalIntervalStore(1L << 20);
    final BitemporalIntervalStore.Relation relation = owner.relation(1);
    try (owner; BitemporalEventReader event = new BitemporalEventReader(new StringReader(""), Tier.DEVELOPMENT)) {
      event.from = 0;
      event.to = 366;
      event.put = true;
      final long[] allocated = new long[2 * WINDOWS_PER_CONTROL];
      final long[] inserted = new long[allocated.length];
      for (int window = 0; window < allocated.length; window++) {
        final int operations = REPLACEMENT_OPERATIONS * (1 + (window & 1));
        final long before = bean.getThreadAllocatedBytes(thread);
        inserted[window] = replaceRepeatedly(relation, old, next, event, operations, window >= WINDOWS_PER_CONTROL);
        allocated[window] = bean.getThreadAllocatedBytes(thread) - before;
      }
      for (int window = 0; window < allocated.length; window++) {
        final int operations = REPLACEMENT_OPERATIONS * (1 + (window & 1));
        System.out.println("BITEMPORAL_ALLOCATION window=" + window + " operations=" + operations + " control="
            + (window >= WINDOWS_PER_CONTROL) + " bytes=" + allocated[window]);
        assertEquals(operations / 2, inserted[window], "each PUT must publish one interval");
      }
      assertReplacementAllocationBudget(allocated[WINDOWS_PER_CONTROL - 2], allocated[WINDOWS_PER_CONTROL - 1]);
      final long controlBytesN = allocated[allocated.length - 2];
      final long controlBytes2N = allocated[allocated.length - 1];
      assertTrue(controlBytes2N - controlBytesN >= REPLACEMENT_OPERATIONS,
          "one escaping allocation per operation must produce a positive allocation slope");
      assertNotNull(allocationSink, "the allocation control must publish its result");
      assertThrows(AssertionError.class, () -> assertReplacementAllocationBudget(controlBytesN, controlBytes2N));
      assertEquals((long) BitemporalIntervalStore.SLAB_SIZE * (40 + 4), owner.reservedBytes());
    }
    assertThrows(IllegalStateException.class, () -> relation.read(1, old));
    final BitemporalIntervalStore exhausted = new BitemporalIntervalStore(200_000);
    final BitemporalIntervalStore.Relation retained = exhausted.relation(1);
    assertThrows(IllegalStateException.class, () -> {
      try (exhausted) {
        exhausted.relation(100_000);
      }
    });
    assertThrows(IllegalStateException.class, () -> retained.read(1, old));
  }

  @Test
  void replacementAllocationBudgetRequiresZeroInBothWindows() {
    assertReplacementAllocationBudget(0, 0);
    assertThrows(AssertionError.class, () -> assertReplacementAllocationBudget(1_072, 1_072));
    assertThrows(AssertionError.class, () -> assertReplacementAllocationBudget(1, 0));
    assertThrows(AssertionError.class, () -> assertReplacementAllocationBudget(0, 1));
  }

  private static long parsingAllocatedBytes(final BitemporalEventReader reader, final ThreadMXBean bean,
      final long thread) throws IOException {
    final long before = bean.getThreadAllocatedBytes(thread);
    while (reader.advance()) {
      // No Event, DOM, field-name, or table-name objects are created in this loop.
    }
    return bean.getThreadAllocatedBytes(thread) - before;
  }

  private static void assertReplacementAllocationBudget(final long bytesN, final long bytes2N) {
    assertEquals(0, bytesN, "warmed interval replacement/read/write must allocate nothing at N operations");
    assertEquals(0, bytes2N, "warmed interval replacement/read/write must allocate nothing at 2N operations");
  }

  private static long replaceRepeatedly(final BitemporalIntervalStore.Relation relation, final Intervals old,
      final Intervals next, final BitemporalEventReader event, final int operations,
      final boolean allocatePerOperation) {
    long inserted = 0;
    for (int operation = 0; operation < operations; operation++) {
      event.put = (operation & 1) == 0;
      relation.read(1, old);
      old.replace(event, next);
      relation.write(1, next);
      inserted += next.size;
      if (allocatePerOperation) {
        allocationSink = new Object();
      }
    }
    return inserted;
  }
}
