package io.sirix.index.hot.page;

import com.sun.management.ThreadMXBean;
import io.sirix.index.IndexType;
import java.lang.foreign.Arena;
import java.lang.management.ManagementFactory;
import java.util.Arrays;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Exact unsigned lookup and zero-allocation contract, also run with 128-bit HotSpot vectors. */
final class HOTLeafLookupAllocationTest {
  private static final int ITERATIONS = 4096;
  private static volatile int checksum;

  @ParameterizedTest
  @ValueSource(ints = {0, 7, 8, 9, 15, 16, 17, 31})
  void cachedLookupsAllocateNothing(final int discriminativeBits) {
    final var platform = ManagementFactory.getThreadMXBean();
    assumeTrue(platform instanceof ThreadMXBean);
    final ThreadMXBean bean = (ThreadMXBean) platform;
    assumeTrue(bean.isThreadAllocatedMemorySupported());
    bean.setThreadAllocatedMemoryEnabled(true);
    final int count = discriminativeBits == 0
        ? 32
        : discriminativeBits + 1;
    final byte[][] keys = new byte[count][];
    final byte[] value = {1};
    try (Arena arena = Arena.ofConfined();
        HOTLeafPage page = new HOTLeafPage(1, 1, IndexType.PROJECTION, arena.allocate(HOTLeafPage.DEFAULT_SIZE), null,
            new char[HOTLeafPage.MAX_ENTRIES], 0, 0)) {
      for (int i = 0; i < count; i++) {
        // Zero plus single-bit words require a distinct discriminative bit per adjacent pair.
        keys[i] = key(discriminativeBits == 0
            ? i
            : i == 0
                ? 0
                : 1 << (i - 1));
        assertTrue(page.put(keys[i], value));
      }
      final byte[][] probes = new byte[count * 2 + 2][];
      final byte[][] padded = new byte[probes.length][];
      final int[] expected = new int[probes.length];
      for (int i = 0; i < count; i++) {
        probes[i * 2] = keys[i];
        probes[i * 2 + 1] = keys[i].clone();
        probes[i * 2 + 1][3] |= 1;
      }
      probes[probes.length - 2] = new byte[0];
      probes[probes.length - 1] = key(-1);
      for (int i = 0; i < probes.length; i++) {
        // JDK unsigned lexicographic ordering is the independent match/insertion-point oracle.
        expected[i] = Arrays.binarySearch(keys, probes[i], Arrays::compareUnsigned);
        padded[i] = Arrays.copyOf(probes[i], probes[i].length + 5);
        Arrays.fill(padded[i], probes[i].length, padded[i].length, (byte) 0xA5);
        assertEquals(expected[i], page.findEntry(probes[i]));
        assertEquals(expected[i], page.findEntry(padded[i], probes[i].length));
      }
      final long thread = Thread.currentThread().threadId();
      long allocated = Long.MAX_VALUE;
      for (int pass = 0; pass < 14; pass++) {
        final long before = bean.getThreadAllocatedBytes(thread);
        int mismatches = 0;
        for (int i = 0; i < ITERATIONS; i++) {
          final int probe = i % probes.length;
          if (page.findEntry(probes[probe]) != expected[probe]) {
            mismatches++;
          }
          if (page.findEntry(padded[probe], probes[probe].length) != expected[probe]) {
            mismatches++;
          }
        }
        checksum = mismatches;
        final long bytes = bean.getThreadAllocatedBytes(thread) - before;
        assertEquals(0, mismatches, "both lookup overloads must match unsigned key order");
        if (pass >= 11) {
          allocated = Math.min(allocated, bytes);
        }
      }
      System.out.printf("lookupAllocation discriminativeBits=%d entries=%d bytes=%d%n", discriminativeBits, count,
          allocated);
      assertEquals(0, allocated, "prepared cached lookups must allocate nothing");
    }
  }

  private static byte[] key(final int word) {
    return new byte[] {(byte) (word >>> 24), (byte) (word >>> 16), (byte) (word >>> 8), (byte) word};
  }
}
