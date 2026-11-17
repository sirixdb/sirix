package io.sirix.page;

import com.github.benmanes.caffeine.cache.Caffeine;
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
import io.sirix.node.Utils;
import io.sirix.settings.VersioningType;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

/**
 * GC-profiled raw/encoded pairs for codec selection, destination reuse and owned projection
 * results.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
public class HOTLeafCodecAllocationBenchmark {
  @Param({"RAW", "ENCODED"})
  public String encoding;
  @Param({"ZERO", "REPEATED"})
  public String content;
  private Fixture fixture;
  private boolean encoded;
  @SuppressWarnings("UnusedVariable") // Volatile publication makes measured results escape.
  private static volatile Object result;
  @SuppressWarnings("UnusedVariable") // Volatile writes keep primitive work observable to the JIT.
  private static volatile long checksum;

  @Setup
  public void setup() {
    encoded = encoding.equals("ENCODED");
    // One encoding per fresh fork; main ignores this additive diagnostic writer property.
    System.setProperty("sirix.page.hotLeaf.compress", Boolean.toString(encoded));
    Allocators.getInstance().init(1L << 30);
    fixture = new Fixture(content.equals("REPEATED"), encoded);
  }

  @Benchmark
  public void coldProjectionCopy() {
    fixture.run(0, encoded);
  }

  @Benchmark
  public void coldVerifiedCopy() {
    fixture.run(1, encoded);
  }

  @Benchmark
  public void uncachedSelection() {
    fixture.run(2, encoded);
  }

  @Benchmark
  public void encoderSelection() {
    fixture.run(3, encoded);
  }

  @Benchmark
  public void nativeDecode() {
    fixture.run(4, encoded);
  }

  @Benchmark
  public void heapDecode() {
    fixture.run(5, encoded);
  }

  @Benchmark
  public void warmImageLookup() {
    fixture.run(6, encoded);
  }

  @Benchmark
  public void serializePage() {
    fixture.run(7, encoded);
  }

  @Benchmark
  public void compactMaterialize() {
    fixture.run(8, encoded);
  }

  @Benchmark
  public void nativeInputDecode() {
    fixture.run(9, encoded);
  }

  @Benchmark
  public void warmLeafLookup() {
    fixture.run(10, encoded);
  }

  @Benchmark
  public void fullMaterialize() {
    fixture.run(11, encoded);
  }

  @TearDown
  public void tearDown() {
    fixture.close();
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
    private final MMFileReader reader = new MMFileReader(MemorySegment.NULL, MemorySegment.NULL,
        new ByteHandlerPipeline(), SerializationType.DATA, new PagePersister(), Caffeine.newBuilder().build(), null);
    private final int payload;
    private final int used;

    private Fixture(final boolean repeated, final boolean encoded) {
      final byte[] value = new byte[128];
      if (repeated) {
        for (int i = 0; i < value.length; i++) {
          value[i] = (byte) (i % 17 + 1);
        }
      }
      for (int i = 0; i < 32; i++) {
        if (!page.put(new byte[] {(byte) i}, value)) {
          throw new IllegalStateException("Fixture overflow");
        }
      }
      // This fixture is dense and has no side references. Keep its raw control independent of
      // new production helpers so this same benchmark class can run over unchanged main classes.
      output.writeByte((byte) 12);
      output.writeByte((byte) 0);
      output.writeByte((byte) 0);
      Utils.putVarLong(output, page.getPageKey());
      output.writeInt(page.getRevision());
      output.writeByte(page.getIndexType().getID());
      output.writeShort((short) page.getCommonPrefixLen());
      output.write(page.getCommonPrefix(), 0, page.getCommonPrefixLen());
      output.writeInt(page.getEntryCount());
      output.writeInt(page.getUsedSlotsSize());
      for (int i = 0; i < page.getEntryCount(); i++) {
        output.writeInt(page.getSlotOffset(i));
      }
      output.writeSegment(page.slots(), 0, page.getUsedSlotsSize());
      bytes[0] = output.toByteArray();
      output.clear();
      PageKind.HOT_LEAF_PAGE.serializePage(config, output, page, SerializationType.DATA);
      bytes[1] = output.toByteArray();
      if (((bytes[1][2] & HOTLeafPayloadCodec.FLAG_COMPRESSED) != 0) != encoded) {
        throw new IllegalStateException("Fixture encoding does not match the requested fork");
      }
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
      if (!repeated && encoded) {
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
          PageKind.HOT_LEAF_PAGE.serializePage(config, output, page, SerializationType.DATA);
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
