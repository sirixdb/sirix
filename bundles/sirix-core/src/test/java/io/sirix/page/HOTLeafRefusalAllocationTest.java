package io.sirix.page;

import com.sun.management.ThreadMXBean;
import io.sirix.access.ResourceConfiguration;
import io.sirix.cache.Allocators;
import io.sirix.index.IndexType;
import io.sirix.index.hot.page.HOTLeafPage;
import io.sirix.node.ByteArrayBytesIn;
import io.sirix.settings.VersioningType;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Clean-fork allocation check: refusing a truncated HOT leaf frame allocates nothing in proportion
 * to the sizes the frame declares.
 */
final class HOTLeafRefusalAllocationTest {
  /** Refusals per measured pass. */
  private static final int REFUSALS = 256;
  private static volatile Object result;

  /**
   * FMT-211 and the persisted-reference rule: a reader checks a declared size against the bytes
   * present before it allocates from that size. Refusing a frame that declares the largest slot
   * region (or prefix) and is one byte short must therefore allocate no more than refusing a frame
   * that declares a few bytes and is one byte short at the same check.
   */
  @Test
  void refusingATruncatedFrameAllocatesIndependentlyOfItsDeclaredSize() throws IOException {
    final var platform = ManagementFactory.getThreadMXBean();
    assumeTrue(platform instanceof ThreadMXBean);
    final ThreadMXBean bean = (ThreadMXBean) platform;
    assumeTrue(bean.isThreadAllocatedMemorySupported());
    bean.setThreadAllocatedMemoryEnabled(true);
    Allocators.getInstance().init(1L << 30);
    final long thread = Thread.currentThread().threadId();
    final ResourceConfiguration config =
        new ResourceConfiguration.Builder("hot-refusal").versioningApproach(VersioningType.FULL).build();

    final long slotRegion = refusalBytes(config, truncatedSlotRegion(HOTLeafPage.MAX_ENTRIES, HOTLeafPage.DEFAULT_SIZE),
        bean, thread) - refusalBytes(config, truncatedSlotRegion(1, 2 * Short.BYTES), bean, thread);
    final long prefix = refusalBytes(config, truncatedPrefix(0xFFFF), bean, thread)
        - refusalBytes(config, truncatedPrefix(1), bean, thread);
    System.out.printf("refusalAllocation slotRegion=%d prefix=%d bytes per refusal%n", slotRegion, prefix);
    // One declared byte would already be in proportion; a kibibyte leaves room for allocation
    // accounting noise while staying far below the 64 KiB the declared sizes would cost.
    assertTrue(slotRegion < 1024, "slot-region refusal allocated " + slotRegion + " more bytes for the larger frame");
    assertTrue(prefix < 1024, "prefix refusal allocated " + prefix + " more bytes for the larger frame");
  }

  /** Mode-zero kind-12 frame: {@code count} zero slot offsets and a slot region one byte short. */
  private static byte[] truncatedSlotRegion(final int count, final int usedSlotBytes) {
    final ByteBuffer frame = frameHeader(HOTLeafPage.FLAG_NARROW_SLOT_OFFSETS, 0,
        2 * Integer.BYTES + count * Short.BYTES + usedSlotBytes - 1);
    frame.putInt(count).putInt(usedSlotBytes);
    return frame.array();
  }

  /** Kind-12 frame declaring {@code prefixLength} prefix bytes, one byte short of the dimensions. */
  private static byte[] truncatedPrefix(final int prefixLength) {
    return frameHeader((byte) 0, prefixLength, prefixLength + 2 * Integer.BYTES - 1).array();
  }

  /** Kind, version, flags, page key 7, revision 3, projection index and the prefix length. */
  private static ByteBuffer frameHeader(final byte flags, final int prefixLength, final int bodyBytes) {
    final ByteBuffer frame = ByteBuffer.allocate(11 + bodyBytes).order(ByteOrder.LITTLE_ENDIAN);
    frame.put(PageKind.HOT_LEAF_PAGE.getID()).put((byte) 0).put(flags).put((byte) 7).putInt(3)
         .put(IndexType.PROJECTION.getID()).putShort((short) prefixLength);
    return frame;
  }

  /** Minimum allocated bytes per refusal over the measured passes; every read must refuse. */
  private static long refusalBytes(final ResourceConfiguration config, final byte[] frame, final ThreadMXBean bean,
      final long thread) throws IOException {
    final PagePersister persister = new PagePersister();
    final ByteArrayBytesIn input = new ByteArrayBytesIn(frame);
    long least = Long.MAX_VALUE;
    for (int pass = 0; pass < 8; pass++) {
      int refused = 0;
      final long before = bean.getThreadAllocatedBytes(thread);
      for (int i = 0; i < REFUSALS; i++) {
        input.position(0);
        try {
          result = persister.deserializePage(config, input, SerializationType.DATA);
        } catch (final IllegalStateException refusal) {
          refused++;
        }
      }
      least = Math.min(least, bean.getThreadAllocatedBytes(thread) - before);
      assertEquals(REFUSALS, refused, "every read of the truncated frame must be refused");
    }
    return least / REFUSALS;
  }
}
