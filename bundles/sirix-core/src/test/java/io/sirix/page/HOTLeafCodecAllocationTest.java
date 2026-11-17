package io.sirix.page;

import com.sun.management.ThreadMXBean;
import io.sirix.access.ResourceConfiguration;
import io.sirix.cache.Allocators;
import io.sirix.index.IndexType;
import io.sirix.index.hot.page.HOTLeafPage;
import io.sirix.index.hot.page.HOTProjectionImage;
import io.sirix.io.PageHasher;
import io.sirix.io.bytepipe.FFILz4Compressor;
import io.sirix.node.ByteArrayBytesIn;
import io.sirix.node.MemorySegmentBytesOut;
import io.sirix.node.MemorySegmentBytesIn;
import io.sirix.node.Utils;
import io.sirix.settings.VersioningType;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Clean-fork allocation gate: reusable codec work adds no objects to the corresponding raw path.
 */
final class HOTLeafCodecAllocationTest {
  private static final int ITERATIONS = 4096;
  private static volatile Object result;
  private static volatile long checksum;

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void encodedPathsAddNoAllocation(final boolean repeated) {
    final var platform = ManagementFactory.getThreadMXBean();
    assumeTrue(platform instanceof ThreadMXBean);
    final ThreadMXBean bean = (ThreadMXBean) platform;
    assumeTrue(bean.isThreadAllocatedMemorySupported());
    bean.setThreadAllocatedMemoryEnabled(true);
    Allocators.getInstance().init(1L << 30);
    assumeTrue(!repeated || FFILz4Compressor.isNativeAvailable());
    try (Fixture fixture = new Fixture(repeated)) {
      final long thread = Thread.currentThread().threadId();
      final StringBuilder failures = new StringBuilder();
      for (int operation = 0; operation < 12; operation++) {
        long raw = Long.MAX_VALUE;
        long encoded = Long.MAX_VALUE;
        // Same callsite and enough invocations to compile both arms before the measured passes.
        for (int pass = 0; pass < 14; pass++) {
          final long rawBytes = measure(fixture, operation, false, bean, thread);
          final long encodedBytes = measure(fixture, operation, true, bean, thread);
          if (pass >= 11) {
            raw = Math.min(raw, rawBytes);
            encoded = Math.min(encoded, encodedBytes);
          }
        }
        System.out.printf("codecAllocation repeated=%s operation=%d raw=%d encoded=%d bytes/%d pages%n", repeated,
            operation, raw, encoded, ITERATIONS);
        if (encoded > raw) {
          failures.append("operation ")
                  .append(operation)
                  .append(": raw=")
                  .append(raw)
                  .append(" encoded=")
                  .append(encoded)
                  .append('\n');
        }
        if (operation == 3 || operation == 4 || operation == 5 || operation == 7 || operation == 9 || operation == 10) {
          assertEquals(0, raw, "raw codec baseline must allocate nothing");
        }
      }
      assertTrue(failures.isEmpty(), failures.toString());
    }
  }

  private static long measure(final Fixture fixture, final int operation, final boolean encoded,
      final ThreadMXBean bean, final long thread) {
    final long before = bean.getThreadAllocatedBytes(thread);
    for (int i = 0; i < ITERATIONS; i++) {
      fixture.run(operation, encoded);
    }
    return bean.getThreadAllocatedBytes(thread) - before;
  }

  private static final class Fixture implements AutoCloseable {
    private final ResourceConfiguration config =
        new ResourceConfiguration.Builder("codec-allocation").versioningApproach(VersioningType.FULL).build();
    private final Arena arena = Arena.ofConfined();
    private final MemorySegment destination = arena.allocate(HOTLeafPage.DEFAULT_SIZE);
    private final HOTLeafPage page = new HOTLeafPage(7, 3, IndexType.PROJECTION);
    private final MemorySegmentBytesOut output = MemorySegmentBytesOut.synchronousScratch(128 * 1024);
    private final byte[] key = {17};
    private final byte[] heap = new byte[HOTLeafPage.DEFAULT_SIZE];
    private final byte[][] bytes = new byte[2][];
    private final ByteArrayBytesIn[] inputs = new ByteArrayBytesIn[2];
    private final MemorySegment[] wires = new MemorySegment[2];
    private final MemorySegmentBytesIn[] nativeInputs = new MemorySegmentBytesIn[2];
    private final HOTLeafPage[] cached = new HOTLeafPage[2];
    private final ByteBuffer[] buffers = new ByteBuffer[2];
    private final PageReference[] references = new PageReference[2];
    private final HOTProjectionImage[] images = new HOTProjectionImage[2];
    private final int payload;
    private final int used;

    private Fixture(final boolean repeated) {
      final byte[] value = new byte[128];
      if (repeated) {
        for (int i = 0; i < value.length; i++) {
          value[i] = (byte) (i % 17 + 1);
        }
      }
      for (int i = 0; i < 32; i++) {
        assertTrue(page.put(new byte[] {(byte) i}, value));
      }
      PageKind.serializeRawHOTLeaf(config, output, page);
      bytes[0] = output.toByteArray();
      output.clear();
      PageKind.HOT_LEAF_PAGE.serializePage(config, output, page, SerializationType.DATA);
      bytes[1] = output.toByteArray();
      assertTrue((bytes[1][2] & HOTLeafPayloadCodec.FLAG_COMPRESSED) != 0);
      final ByteArrayBytesIn source = new ByteArrayBytesIn(bytes[0]);
      source.position(3);
      Utils.getVarLong(source);
      source.readInt();
      source.readByte();
      source.skip(Short.toUnsignedInt(source.readShort()));
      final int count = source.readInt();
      used = source.readInt();
      source.skip((long) count * Integer.BYTES);
      payload = (int) source.position();
      if (!repeated) {
        // Force the legal zero-run arm even when LZ4 would win for this fixture's directory pattern.
        final byte[] frame = new byte[ZeroRunByteCodec.maxEncodedSize(used)];
        final int stored = ZeroRunByteCodec.encode(bytes[0], payload, used, frame, 0);
        output.clear();
        output.write(bytes[0], 0, payload);
        output.writeInt(stored);
        output.writeByte((byte) 0);
        output.write(frame, 0, stored);
        bytes[1] = output.toByteArray();
        bytes[1][2] |= HOTLeafPayloadCodec.FLAG_COMPRESSED;
      }
      for (int i = 0; i < 2; i++) {
        inputs[i] = new ByteArrayBytesIn(bytes[i]);
        buffers[i] = ByteBuffer.wrap(bytes[i]);
        wires[i] = arena.allocate(bytes[i].length);
        MemorySegment.copy(bytes[i], 0, wires[i], ValueLayout.JAVA_BYTE, 0, bytes[i].length);
        nativeInputs[i] = new MemorySegmentBytesIn(wires[i]);
        nativeInputs[i].position(1);
        cached[i] =
            (HOTLeafPage) PageKind.HOT_LEAF_PAGE.deserializePage(config, nativeInputs[i], SerializationType.DATA);
        references[i] = new PageReference().setKey(7);
        references[i].setHash(PageHasher.compute(bytes[i]));
        inputs[i].position(1);
        images[i] = HOTProjectionImage.copyIfFits(inputs[i]);
        images[i].prepareForCache();
      }

    }

    private void run(final int operation, final boolean encoded) {
      final int arm = encoded
          ? 1
          : 0;
      final ByteArrayBytesIn source = inputs[arm];
      switch (operation) {
        case 0 -> {
          source.position(1);
          result = HOTProjectionImage.copyIfFits(source);
        }
        case 1 -> result = HOTProjectionImage.copyVerifiedFrame(buffers[arm], references[arm], config);
        case 2 -> {
          source.position(1);
          result = PageKind.deserializeHOTProjectionEntry(source, key, 0);
        }
        case 3 -> {
          output.clear();
          if (encoded) {
            HOTLeafPayloadCodec.writePage(output, bytes[0], bytes[0].length);
          } else {
            output.write(bytes[0]);
          }
          checksum = output.position();
        }
        case 4 -> {
          source.position(payload);
          if (encoded) {
            HOTLeafPayloadCodec.decode(source, destination, used);
          } else {
            MemorySegment.copy(bytes[0], payload, destination, ValueLayout.JAVA_BYTE, 0, used);
          }
          checksum = destination.get(ValueLayout.JAVA_BYTE, used - 1);
        }
        case 5 -> {
          source.position(payload);
          if (encoded) {
            HOTLeafPayloadCodec.decodeHeap(source, heap, 0, used);
          } else {
            System.arraycopy(bytes[0], payload, heap, 0, used);
          }
          checksum = heap[used - 1];
        }
        case 6 -> result = images[arm].select(key, 0);
        case 7 -> {
          output.clear();
          if (encoded) {
            PageKind.HOT_LEAF_PAGE.serializePage(config, output, page, SerializationType.DATA);
          } else {
            PageKind.serializeRawHOTLeaf(config, output, page);
          }
          checksum = output.position();
        }
        case 8 -> {
          nativeInputs[arm].position(1);
          try (HOTLeafPage decoded = (HOTLeafPage) PageKind.deserializeHOTLeafFragment(nativeInputs[arm])) {
            // Materialized pages escape to their reader/cache in production in both arms.
            result = decoded;
            checksum = decoded.getUsedSlotsSize();
          }
        }
        case 9 -> {
          nativeInputs[arm].position(payload);
          if (encoded) {
            HOTLeafPayloadCodec.decode(nativeInputs[arm], destination, used);
          } else {
            MemorySegment.copy(wires[0], payload, destination, 0, used);
          }
          checksum = destination.get(ValueLayout.JAVA_BYTE, used - 1);
        }
        case 10 -> checksum = cached[arm].findEntry(key);
        case 11 -> {
          nativeInputs[arm].position(1);
          try (HOTLeafPage decoded =
              (HOTLeafPage) PageKind.HOT_LEAF_PAGE.deserializePage(config, nativeInputs[arm], SerializationType.DATA)) {
            // Materialized pages escape to their reader/cache in production in both arms.
            result = decoded;
            checksum = decoded.getUsedSlotsSize();
          }
        }
        default -> throw new AssertionError(operation);
      }
    }

    @Override
    public void close() {
      output.close();
      cached[0].close();
      cached[1].close();
      page.close();
      arena.close();
    }
  }
}
