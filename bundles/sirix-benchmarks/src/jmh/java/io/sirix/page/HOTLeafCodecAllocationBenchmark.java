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
import java.util.Objects;
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
 * GC-profiled raw/encoded pairs for directory serialization and owned projection results.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
public class HOTLeafCodecAllocationBenchmark {
  // The paired runner requests RAW explicitly on the pre-u16 baseline.
  @Param({"ENCODED"})
  public String encoding;
  @Param({"ZERO", "REPEATED"})
  public String content;
  private Fixture fixture;
  @SuppressWarnings("UnusedVariable") // Volatile publication makes measured results escape.
  private static volatile Object result;
  @SuppressWarnings("UnusedVariable") // Volatile writes keep primitive work observable to the JIT.
  private static volatile long checksum;

  @Setup
  public void setup() {
    Allocators.getInstance().init(1L << 30);
    fixture = new Fixture(content.equals("REPEATED"), encoding.equals("ENCODED"));
  }

  @Benchmark
  public void coldProjectionCopy() {
    fixture.run(0);
  }

  @Benchmark
  public void coldVerifiedCopy() {
    fixture.run(1);
  }

  @Benchmark
  public void uncachedSelection() {
    fixture.run(2);
  }

  @Benchmark
  public void warmImageLookup() {
    fixture.run(4);
  }

  @Benchmark
  public void serializePage() {
    fixture.run(3);
  }

  @Benchmark
  public void compactMaterialize() {
    fixture.run(5);
  }

  @Benchmark
  public void warmLeafLookup() {
    fixture.run(6);
  }

  @Benchmark
  public void fullMaterialize() {
    fixture.run(7);
  }

  @TearDown
  public void tearDown() {
    fixture.close();
  }

  private static final class Fixture implements AutoCloseable {
    private final ResourceConfiguration config =
        new ResourceConfiguration.Builder("codec-allocation").versioningApproach(VersioningType.FULL).build();
    private final Arena arena = Arena.ofConfined();
    private final HOTLeafPage page = new HOTLeafPage(7, 3, IndexType.PROJECTION);
    private final MemorySegmentBytesOut output = MemorySegmentBytesOut.synchronousScratch(128 * 1024);
    private final byte[] key = {17};
    private final ByteArrayBytesIn source;
    private final MemorySegment wire;
    private final MemorySegmentBytesIn nativeInput;
    private final HOTLeafPage cached;
    private final ByteBuffer buffer;
    private final PageReference reference;
    private final HOTProjectionImage image;
    private final MMFileReader reader = new MMFileReader(MemorySegment.NULL, MemorySegment.NULL,
        new ByteHandlerPipeline(), SerializationType.DATA, new PagePersister(), Caffeine.newBuilder().build(), null);

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
      if (encoded) {
        PageKind.HOT_LEAF_PAGE.serializePage(config, output, page, SerializationType.DATA);
      } else {
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
      }

      final byte[] bytes = output.toByteArray();
      if (encoded && (bytes[2] & HOTLeafPage.FLAG_NARROW_SLOT_OFFSETS) == 0) {
        throw new IllegalStateException("Fixture requires a narrow directory");
      }
      source = new ByteArrayBytesIn(bytes);
      buffer = ByteBuffer.wrap(bytes);
      wire = arena.allocate(bytes.length);
      MemorySegment.copy(bytes, 0, wire, ValueLayout.JAVA_BYTE, 0, bytes.length);
      nativeInput = new MemorySegmentBytesIn(wire);
      nativeInput.position(1);
      cached = (HOTLeafPage) PageKind.HOT_LEAF_PAGE.deserializePage(config, nativeInput, SerializationType.DATA);
      reference = new PageReference().setKey(7);
      reference.setHash(PageHasher.compute(bytes));
      source.position(1);
      image = Objects.requireNonNull(HOTProjectionImage.copyIfFits(source));
      image.prepareForCache();
    }

    private void run(final int operation) {
      switch (operation) {
        case 0 -> {
          source.position(1);
          result = HOTProjectionImage.copyIfFits(source);
        }
        case 1 -> result = HOTProjectionImage.copyVerifiedFrame(buffer, reference, config);
        case 2 -> {
          source.position(1);
          result = PageKind.deserializeHOTProjectionEntry(source, key, 0);
        }
        case 4 -> result = image.select(key, 0);
        case 3 -> {
          output.clear();
          PageKind.HOT_LEAF_PAGE.serializePage(config, output, page, SerializationType.DATA);
          checksum = output.position();
        }
        case 5 -> {
          nativeInput.position(1);
          try (HOTLeafPage decoded = (HOTLeafPage) PageKind.deserializeHOTLeafFragment(nativeInput)) {
            // Materialized pages escape to their reader/cache in production in both arms.
            result = decoded;
            checksum = decoded.getUsedSlotsSize();
          }
        }
        case 6 -> checksum = cached.findEntry(key);
        case 7 -> {
          try (HOTLeafPage decoded = (HOTLeafPage) reader.deserializeFromSegment(config, wire)) {
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
      cached.close();
      page.close();
      arena.close();
    }
  }
}
