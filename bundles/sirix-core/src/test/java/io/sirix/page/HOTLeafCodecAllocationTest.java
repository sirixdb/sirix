package io.sirix.page;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.sun.management.ThreadMXBean;
import io.sirix.access.ResourceConfiguration;
import io.sirix.cache.Allocators;
import io.sirix.index.IndexType;
import io.sirix.index.hot.page.HOTLeafPage;
import io.sirix.index.hot.page.HOTProjectionImage;
import io.sirix.io.PageHasher;
import io.sirix.io.bytepipe.ByteHandlerPipeline;
import io.sirix.io.memorymapped.MMFileReader;
import io.sirix.node.ByteArrayBytesIn;
import io.sirix.node.MemorySegmentBytesIn;
import io.sirix.node.MemorySegmentBytesOut;
import io.sirix.settings.VersioningType;
import java.io.IOException;
import java.io.UncheckedIOException;
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
  /** Operation that materializes a compact raw fragment through deserializeHOTLeafFragment. */
  private static final int COMPACT_FRAGMENT = 5;
  /** Entries in the fixture leaf, and so in each decoded slot directory. */
  private static final int ENTRIES = 32;
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
    try (Fixture fixture = new Fixture(repeated)) {
      final long thread = Thread.currentThread().threadId();
      final StringBuilder failures = new StringBuilder();
      for (int operation = 0; operation < 8; operation++) {
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
        // The captain's allocation bar is a median allocation delta <= 0 against main. Operation 5
        // decodes a compact raw fragment, whose directory is char[count] in this change and was
        // int[count] on main. Holding the encoded arm to main's raw cost therefore adds the
        // measured int[count] - char[count] difference; every other operation compares arm to arm.
        final long bound = operation == COMPACT_FRAGMENT
            ? raw + mainDirectoryExtraBytes(bean, thread)
            : raw;
        if (encoded > bound) {
          failures.append("operation ")
                  .append(operation)
                  .append(": raw=")
                  .append(raw)
                  .append(" encoded=")
                  .append(encoded)
                  .append(" bound=")
                  .append(bound)
                  .append('\n');
        }
        if (operation == 3 || operation == 6) {
          assertEquals(0, raw, "raw writer and cached leaf lookup must allocate nothing");
        }
      }
      assertTrue(failures.isEmpty(), failures.toString());
    }
  }

  /**
   * Bytes main's raw compact decoder allocated beyond this change's for {@link #ITERATIONS} decodes:
   * an int[ENTRIES] directory in place of a char[ENTRIES] one, measured on this JVM's layout.
   */
  private static long mainDirectoryExtraBytes(final ThreadMXBean bean, final long thread) {
    long ints = Long.MAX_VALUE;
    long chars = Long.MAX_VALUE;
    for (int pass = 0; pass < 4; pass++) {
      long before = bean.getThreadAllocatedBytes(thread);
      for (int i = 0; i < ITERATIONS; i++) {
        result = new int[ENTRIES];
      }
      ints = Math.min(ints, bean.getThreadAllocatedBytes(thread) - before);
      before = bean.getThreadAllocatedBytes(thread);
      for (int i = 0; i < ITERATIONS; i++) {
        result = new char[ENTRIES];
      }
      chars = Math.min(chars, bean.getThreadAllocatedBytes(thread) - before);
    }
    return ints - chars;
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
    private final HOTLeafPage page = new HOTLeafPage(7, 3, IndexType.PROJECTION);
    private final MemorySegmentBytesOut output = MemorySegmentBytesOut.synchronousScratch(128 * 1024);
    private final byte[] key = {17};
    private final byte[][] bytes = new byte[2][];
    private final ByteArrayBytesIn[] inputs = new ByteArrayBytesIn[2];
    private final MemorySegment[] wires = new MemorySegment[2];
    private final MemorySegmentBytesIn[] nativeInputs = new MemorySegmentBytesIn[2];
    private final HOTLeafPage[] cached = new HOTLeafPage[2];
    private final ByteBuffer[] buffers = new ByteBuffer[2];
    private final PageReference[] references = new PageReference[2];
    private final HOTProjectionImage[] images = new HOTProjectionImage[2];
    private final MMFileReader reader = new MMFileReader(MemorySegment.NULL, MemorySegment.NULL,
        new ByteHandlerPipeline(), SerializationType.DATA, new PagePersister(), Caffeine.newBuilder().build(), null);

    private Fixture(final boolean repeated) {
      final byte[] value = new byte[128];
      if (repeated) {
        for (int i = 0; i < value.length; i++) {
          value[i] = (byte) (i % 17 + 1);
        }
      }
      for (int i = 0; i < ENTRIES; i++) {
        assertTrue(page.put(new byte[] {(byte) i}, value));
      }
      PageKind.serializeHOTLeaf(config, output, page, false);
      bytes[0] = output.toByteArray();
      output.clear();
      PageKind.HOT_LEAF_PAGE.serializePage(config, output, page, SerializationType.DATA);
      bytes[1] = output.toByteArray();
      assertTrue((bytes[1][2] & HOTLeafPage.FLAG_NARROW_SLOT_OFFSETS) != 0);
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
        case 4 -> result = images[arm].select(key, 0);
        case 3 -> {
          output.clear();
          if (encoded) {
            PageKind.HOT_LEAF_PAGE.serializePage(config, output, page, SerializationType.DATA);
          } else {
            PageKind.serializeHOTLeaf(config, output, page, false);
          }
          checksum = output.position();
        }
        case 5 -> {
          nativeInputs[arm].position(1);
          try (HOTLeafPage decoded = (HOTLeafPage) PageKind.deserializeHOTLeafFragment(nativeInputs[arm])) {
            // Materialized pages escape to their reader/cache in production in both arms.
            result = decoded;
            checksum = decoded.getUsedSlotsSize();
          }
        }
        case 6 -> checksum = cached[arm].findEntry(key);
        case 7 -> {
          try (HOTLeafPage decoded = (HOTLeafPage) reader.deserializeFromSegment(config, wires[arm])) {
            result = decoded;
            checksum = decoded.getActualMemorySize();
          } catch (final IOException failure) {
            throw new UncheckedIOException(failure);
          }
        }
        default -> throw new AssertionError(operation);
      }
    }

    @Override
    public void close() {
      reader.close();
      output.close();
      cached[0].close();
      cached[1].close();
      page.close();
      arena.close();
    }
  }
}
