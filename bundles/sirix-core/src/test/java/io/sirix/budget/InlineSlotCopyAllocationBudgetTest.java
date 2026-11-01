package io.sirix.budget;

import com.sun.management.ThreadMXBean;
import io.sirix.access.ResourceConfiguration;
import io.sirix.index.IndexType;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.OverflowPage;
import io.sirix.page.PageReference;
import io.sirix.settings.Constants;
import java.lang.foreign.ValueLayout;
import java.lang.management.ManagementFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Inline version reconstruction must not box keys just to probe an empty overflow map. */
@Isolated
final class InlineSlotCopyAllocationBudgetTest {
  private static final int SLOT = 7;
  private static final long PAGE_KEY = 1L;
  private static final long RECORD_KEY = (PAGE_KEY << Constants.NDP_NODE_COUNT_EXPONENT) + SLOT;
  private static final int COPIES = 16_384;
  private static final byte[] SLOT_BYTES = {1, 2, 3, 4};
  private static volatile PageReference referenceSink;

  @Test
  void copyingInlineSlotsAndLookingUpEmptyReferencesAllocateNothing() {
    final var platformBean = ManagementFactory.getThreadMXBean();
    assumeTrue(platformBean instanceof ThreadMXBean);
    final ThreadMXBean bean = (ThreadMXBean) platformBean;
    assumeTrue(bean.isThreadAllocatedMemorySupported());
    if (!bean.isThreadAllocatedMemoryEnabled()) {
      bean.setThreadAllocatedMemoryEnabled(true);
    }
    final ResourceConfiguration config =
        ResourceConfiguration.newBuilder("inline-copy-budget").useDeweyIDs(false).build();
    try (final KeyValueLeafPage source = new KeyValueLeafPage(PAGE_KEY, IndexType.DOCUMENT, config, 1, null, null, false);
        final KeyValueLeafPage target = new KeyValueLeafPage(PAGE_KEY, IndexType.DOCUMENT, config, 1, null, null, false)) {
      source.setSlot(SLOT_BYTES, SLOT);
      assertTrue(RECORD_KEY > 127, "exercise keys beyond the Long cache");
      // Warm the exact loop and grow owned page storage before counting temporary work.
      for (int warmup = 0; warmup < 8; warmup++) {
        target.reset();
        copySlots(source, target);
      }
      target.reset();
      final long thread = Thread.currentThread().threadId();
      final long before = bean.getThreadAllocatedBytes(thread);
      copySlots(source, target);
      final long bytes = bean.getThreadAllocatedBytes(thread) - before;
      assertEquals(0, bytes, "inline copy and empty reference lookup must not allocate boxed keys");
      assertNull(referenceSink);
      assertEquals(1, target.size(), "the measured copies must actually publish a slot");
      assertArrayEquals(SLOT_BYTES, target.getSlot(SLOT).toArray(ValueLayout.JAVA_BYTE));
      assertArrayEquals(SLOT_BYTES, source.getSlot(SLOT).toArray(ValueLayout.JAVA_BYTE));
      assertTrue(target.getReferencesMap().isEmpty());
    }
  }

  @Test
  void inlineCopyRemovesStaleOverflowReferenceAndKeepsOtherReferences() {
    final ResourceConfiguration config =
        ResourceConfiguration.newBuilder("inline-copy-overflow-control").useDeweyIDs(false).build();
    try (final KeyValueLeafPage source = new KeyValueLeafPage(PAGE_KEY, IndexType.DOCUMENT, config, 1, null, null, false);
        final KeyValueLeafPage target = new KeyValueLeafPage(PAGE_KEY, IndexType.DOCUMENT, config, 1, null, null, false)) {
      source.setSlot(SLOT_BYTES, SLOT);
      final PageReference stale = new PageReference();
      stale.setPage(new OverflowPage(new byte[] {9, 8, 7}));
      final PageReference unrelated = new PageReference();
      target.setPageReference(RECORD_KEY, stale);
      target.setPageReference(RECORD_KEY + 1, unrelated);
      assertSame(stale, target.getPageReference(RECORD_KEY));
      target.copySlotFromPage(source, SLOT);
      assertNull(target.getPageReference(RECORD_KEY));
      assertSame(unrelated, target.getPageReference(RECORD_KEY + 1));
      assertEquals(1, target.getReferencesMap().size());
      assertArrayEquals(SLOT_BYTES, target.getSlot(SLOT).toArray(ValueLayout.JAVA_BYTE));
    }
  }

  private static void copySlots(final KeyValueLeafPage source, final KeyValueLeafPage target) {
    for (int copy = 0; copy < COPIES; copy++) {
      target.copySlotFromPage(source, SLOT);
      referenceSink = target.getPageReference(RECORD_KEY);
    }
  }
}
