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

    final long slotRegion =
        refusalBytes(config, truncatedSlotRegion(HOTLeafPage.MAX_ENTRIES, HOTLeafPage.DEFAULT_SIZE), bean, thread)
            - refusalBytes(config, truncatedSlotRegion(1, 2 * Short.BYTES), bean, thread);
    final long prefix = refusalBytes(config, truncatedPrefix(0xFFFF), bean, thread)
        - refusalBytes(config, truncatedPrefix(1), bean, thread);
    System.out.printf("refusalAllocation slotRegion=%d prefix=%d bytes per refusal%n", slotRegion, prefix);
    // One declared byte would already be in proportion; a kibibyte leaves room for allocation
    // accounting noise while staying far below the 64 KiB the declared sizes would cost.
    assertTrue(slotRegion < 1024, "slot-region refusal allocated " + slotRegion + " more bytes for the larger frame");
    assertTrue(prefix < 1024, "prefix refusal allocated " + prefix + " more bytes for the larger frame");
  }

  /**
   * FMT-216: the reader must verify that the stored token stream can produce the declared decoded
   * length "before reserving a destination based on" it. The paired frames below each declare a large
   * and a small size with the SAME token stream, so the only thing the measurement can attribute a
   * difference to is the declared size.
   *
   * <p>
   * The first pair carries a byte-run header and no token byte at all. The second carries five token
   * bytes — a declared size is not producible just because some tokens follow it: five body bytes
   * spend one on the long-run opcode and so leave a four-byte count, which cannot reach the gibibyte
   * either frame declares.
   */
  @Test
  void refusingAnUnproducibleOverflowPayloadAllocatesIndependentlyOfItsDeclaredSize() throws IOException {
    final var platform = ManagementFactory.getThreadMXBean();
    assumeTrue(platform instanceof ThreadMXBean);
    final ThreadMXBean bean = (ThreadMXBean) platform;
    assumeTrue(bean.isThreadAllocatedMemorySupported());
    bean.setThreadAllocatedMemoryEnabled(true);
    Allocators.getInstance().init(1L << 30);
    final long thread = Thread.currentThread().threadId();
    final ResourceConfiguration config =
        new ResourceConfiguration.Builder("overflow-refusal").versioningApproach(VersioningType.FULL).build();

    final byte[] noTokens = new byte[0];
    final long emptyStream =
        refusalBytes(config, unproducibleOverflowPayload(1 << 20, MEBIBYTE_COUNT, noTokens), bean, thread)
            - refusalBytes(config, unproducibleOverflowPayload(64, new byte[] {0x40}, noTokens), bean, thread);
    final byte[] fiveTokens = new byte[5];
    final long tokenStream =
        refusalBytes(config, unproducibleOverflowPayload(1 << 30, GIBIBYTE_COUNT, fiveTokens), bean, thread)
            - refusalBytes(config, unproducibleOverflowPayload(1 << 29, HALF_GIBIBYTE_COUNT, fiveTokens), bean, thread);
    System.out.printf("overflowRefusalAllocation empty=%d tokens=%d bytes per refusal%n", emptyStream, tokenStream);
    assertTrue(emptyStream < 1024,
        "refusing the mebibyte frame allocated " + emptyStream + " more bytes than refusing the 64-byte frame");
    assertTrue(tokenStream < 1024, "refusing the gibibyte frame with five token bytes allocated " + tokenStream
        + " more bytes than refusing the half-gibibyte one");
  }

  /** Stop-bit counts of the declared sizes used above, least significant group first. */
  private static final byte[] MEBIBYTE_COUNT = {(byte) 0x80, (byte) 0x80, 0x40};
  private static final byte[] GIBIBYTE_COUNT = {(byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, 0x04};
  private static final byte[] HALF_GIBIBYTE_COUNT = {(byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, 0x02};

  /**
   * Compressed projection {@link io.sirix.page.OverflowPage} image whose byte-run frame is a marker,
   * {@code sizeVarint} and {@code tokenBytes}: the page and the frame agree on {@code declared}
   * bytes, and that token stream cannot deliver them.
   */
  private static byte[] unproducibleOverflowPayload(final int declared, final byte[] sizeVarint,
      final byte[] tokenBytes) {
    final int storedLength = 1 + sizeVarint.length + tokenBytes.length;
    final ByteBuffer image = ByteBuffer.allocate(12 + storedLength).order(ByteOrder.LITTLE_ENDIAN);
    image.put(PageKind.OVERFLOWPAGE.getID())
         .put((byte) 0)
         .put((byte) (PageKind.FLAG_OVERFLOW_PAYLOAD_COMPRESSED | PageKind.FLAG_OVERFLOW_PROJECTION_PAYLOAD))
         .putInt(declared)
         .putInt(storedLength)
         .put((byte) 2)
         .put(ByteRunCodec.FRAME_MARKER)
         .put(sizeVarint)
         .put(tokenBytes);
    return image.array();
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
    frame.put(PageKind.HOT_LEAF_PAGE.getID())
         .put((byte) 0)
         .put(flags)
         .put((byte) 7)
         .putInt(3)
         .put(IndexType.PROJECTION.getID())
         .putShort((short) prefixLength);
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
