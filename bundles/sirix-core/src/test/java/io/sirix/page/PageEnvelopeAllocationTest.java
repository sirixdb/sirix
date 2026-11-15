package io.sirix.page;

import com.sun.management.ThreadMXBean;
import io.sirix.node.Bytes;
import io.sirix.node.BytesIn;
import org.junit.Test;

import java.lang.management.ManagementFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Fixed-work allocation check; no wall-clock thresholds or benchmark timing. */
public final class PageEnvelopeAllocationTest {
  @Test
  public void unusedAndSkippedExtensionsAllocateNoPerPageObjects() {
    final ThreadMXBean bean = (ThreadMXBean) ManagementFactory.getThreadMXBean();
    bean.setThreadAllocatedMemoryEnabled(true);
    check(bean, new byte[] {0, 0}, (byte) 0);
    check(bean, new byte[] {0, 3}, (byte) 3);
    check(bean, new byte[] {0, (byte) 0x83, 2, 1, 0}, (byte) 3);
  }

  private static void check(final ThreadMXBean bean, final byte[] wire, final byte mask) {
    final BytesIn<?> source = Bytes.wrapForRead(wire);
    int checksum = 0;
    for (int i = 0; i < 100_000; i++) {
      source.position(0);
      checksum += PageKind.readVersionAndFlagsAllowing(source, mask);
    }
    final long thread = Thread.currentThread().threadId();
    final long before = bean.getThreadAllocatedBytes(thread);
    for (int i = 0; i < 100_000; i++) {
      source.position(0);
      checksum += PageKind.readVersionAndFlagsAllowing(source, mask);
    }
    final long allocated = bean.getThreadAllocatedBytes(thread) - before;
    assertEquals(200_000 * mask, checksum);
    assertTrue("Per-page allocation: " + allocated, allocated <= 1024);
  }
}
