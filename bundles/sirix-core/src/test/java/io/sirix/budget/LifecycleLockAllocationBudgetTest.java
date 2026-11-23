package io.sirix.budget;

import com.sun.management.ThreadMXBean;
import io.sirix.access.trx.node.TransactionExecutionLock;
import java.lang.management.ManagementFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** CONV-301 / Firstmate097: the uncontended commit/close admission primitive allocates nothing. */
@Isolated
final class LifecycleLockAllocationBudgetTest {
  @Test
  void uncontendedExecutionAndCloseAdmissionAllocateNothing() {
    final var platform = ManagementFactory.getThreadMXBean();
    assumeTrue(platform instanceof ThreadMXBean, "VM has no thread allocation accounting");
    final ThreadMXBean bean = (ThreadMXBean) platform;
    assumeTrue(bean.isThreadAllocatedMemorySupported(), "VM has no thread allocation accounting");
    if (!bean.isThreadAllocatedMemoryEnabled()) {
      bean.setThreadAllocatedMemoryEnabled(true);
    }
    final TransactionExecutionLock lock = new TransactionExecutionLock();
    final long thread = Thread.currentThread().threadId();
    long allocated = 0;
    for (int pass = 0; pass < 9; pass++) {
      final long before = bean.getThreadAllocatedBytes(thread);
      for (int operation = 0; operation < 16384; operation++) {
        lock.assertCanClose();
        lock.lock();
        lock.unlock();
      }
      allocated = bean.getThreadAllocatedBytes(thread) - before;
    }
    assertEquals(0, allocated, "uncontended lifecycle admission must not add per-operation garbage");
    assertEquals(0, lock.getHoldCount());
    lock.lock();
    try {
      assertTrue(lock.isHeldByCurrentThread());
      assertThrows(IllegalStateException.class, lock::assertCanClose, "TXN-804: reject execution reentry");
    } finally {
      lock.unlock();
    }
  }
}
