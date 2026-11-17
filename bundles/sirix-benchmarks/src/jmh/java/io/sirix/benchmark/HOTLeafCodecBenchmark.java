/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.benchmark;

import com.github.benmanes.caffeine.cache.Caffeine;
import io.sirix.access.ResourceConfiguration;
import io.sirix.cache.Allocators;
import io.sirix.index.IndexType;
import io.sirix.index.hot.page.HOTLeafPage;
import io.sirix.index.hot.page.HOTProjectionImage;
import io.sirix.io.bytepipe.ByteHandlerPipeline;
import io.sirix.io.memorymapped.MMFileReader;
import io.sirix.node.ByteArrayBytesIn;
import io.sirix.node.MemorySegmentBytesOut;
import io.sirix.node.Utils;
import io.sirix.page.PageKind;
import io.sirix.page.PagePersister;
import io.sirix.page.SerializationType;
import io.sirix.settings.VersioningType;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

/** Paired raw/encoded cold materialization and warm lookup with identical logical leaf contents. */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
public class HOTLeafCodecBenchmark {
  @Param({"RAW", "ENCODED"})
  public String encoding;
  @Param({"ZERO", "REPEATED"})
  public String content;
  private final ResourceConfiguration config =
      new ResourceConfiguration.Builder("codec-benchmark").versioningApproach(VersioningType.FULL).build();
  private final MMFileReader reader = new MMFileReader(MemorySegment.NULL, MemorySegment.NULL,
      new ByteHandlerPipeline(), SerializationType.DATA, new PagePersister(), Caffeine.newBuilder().build(), null);
  private final byte[][] keys = new byte[128][];
  private Arena arena;
  private MemorySegment wire;
  private HOTLeafPage cached;
  private HOTProjectionImage image;
  private int cursor;

  @Setup(Level.Trial)
  @SuppressWarnings("MustBeClosedChecker") // Trial-owned arena is closed in tearDown.
  public void setup() throws Exception {
    Allocators.getInstance().init(1L << 30);
    final HOTLeafPage original = new HOTLeafPage(7, 3, IndexType.PROJECTION);
    final byte[] value = new byte[128];
    if (content.equals("REPEATED")) {
      for (int i = 0; i < value.length; i++) {
        value[i] = (byte) (i % 17 + 1);
      }
    }
    try (MemorySegmentBytesOut sink = new MemorySegmentBytesOut()) {
      for (int i = 0; i < keys.length; i++) {
        keys[i] = new byte[] {(byte) i};
        if (!original.put(keys[i], value)) {
          throw new IllegalStateException("Benchmark leaf overflow");
        }
      }
      if (encoding.equals("ENCODED")) {
        PageKind.HOT_LEAF_PAGE.serializePage(config, sink, original, SerializationType.DATA);
      } else {
        sink.writeByte((byte) 12);
        sink.writeByte((byte) 0); // V0
        sink.writeByte((byte) 0); // raw payload, no side references
        Utils.putVarLong(sink, original.getPageKey());
        sink.writeInt(original.getRevision());
        sink.writeByte(original.getIndexType().getID());
        sink.writeShort((short) original.getCommonPrefixLen());
        sink.write(original.getCommonPrefix(), 0, original.getCommonPrefixLen());
        sink.writeInt(original.getEntryCount());
        sink.writeInt(original.getUsedSlotsSize());
        for (int i = 0; i < original.getEntryCount(); i++) {
          sink.writeInt(original.getSlotOffset(i));
        }
        sink.writeSegment(original.slots(), 0, original.getUsedSlotsSize());
      }
      final byte[] bytes = sink.toByteArray();
      arena = Arena.ofConfined();
      wire = arena.allocate(bytes.length);
      wire.copyFrom(MemorySegment.ofArray(bytes));
      cached = (HOTLeafPage) reader.deserializeFromSegment(config, wire);
      System.out.printf("codecRetained encoding=%s content=%s bytes=%d%n", encoding, content,
          cached.getActualMemorySize());
      final ByteArrayBytesIn imageInput = new ByteArrayBytesIn(bytes);
      imageInput.readByte();
      image = HOTProjectionImage.copyIfFits(imageInput);
      image.prepareForCache();
    } finally {
      original.close();
    }
  }

  @Benchmark
  public long coldMaterialize() throws Exception {
    try (HOTLeafPage page = (HOTLeafPage) reader.deserializeFromSegment(config, wire)) {
      return page.getActualMemorySize();
    }
  }

  @Benchmark
  public int warmLeafLookup() {
    return cached.findEntry(keys[cursor++ & 127]);
  }

  @Benchmark
  public byte[] warmImageLookup() {
    return image.select(keys[cursor++ & 127], 0).value();
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    cached.close();
    reader.close();
    arena.close();
  }
}
