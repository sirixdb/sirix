package io.sirix.page;

import com.sun.management.ThreadMXBean;
import io.sirix.node.LE;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.management.ManagementFactory;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

final class RecordPageSaltAllocationTest {
  private static final int ITERATIONS = 100_000;
  private static volatile Object allocationControl;

  @Test
  void freshPageSaltsDoNotAllocatePerPage() {
    final var bean = ManagementFactory.getThreadMXBean();
    assumeTrue(bean instanceof ThreadMXBean);
    final ThreadMXBean allocations = (ThreadMXBean) bean;
    assumeTrue(allocations.isThreadAllocatedMemorySupported());
    allocations.setThreadAllocatedMemoryEnabled(true);
    final long thread = Thread.currentThread().threadId();
    try (final Arena arena = Arena.ofConfined()) {
      final MemorySegment header = arena.allocate(32);
      initializeFreshPages(header);
      initializeFreshPages(header);
      final long before = allocations.getThreadAllocatedBytes(thread);
      initializeFreshPages(header);
      final long bytes = allocations.getThreadAllocatedBytes(thread) - before;
      assertTrue(bytes <= 4096, "salt initialization allocated " + bytes + " bytes for " + ITERATIONS + " pages");
      final long controlBefore = allocations.getThreadAllocatedBytes(thread);
      for (int i = 0; i < ITERATIONS; i++) {
        allocationControl = new byte[8];
      }
      assertTrue(allocations.getThreadAllocatedBytes(thread) - controlBefore > 4096,
          "the allocation counter must observe a per-page allocation positive control");
    }
  }

  private static void initializeFreshPages(final MemorySegment header) {
    for (int i = 0; i < ITERATIONS; i++) {
      header.set(LE.LONG, 24, 0L);
      PageLayout.initializeSalt(header, true);
    }
  }
}
