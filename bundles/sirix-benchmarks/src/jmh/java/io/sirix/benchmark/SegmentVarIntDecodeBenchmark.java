/*
 * Copyright (c) 2026, Sirix Contributors
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the <organization> nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package io.sirix.benchmark;

import io.sirix.node.DeltaVarIntCodec;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
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
import org.openjdk.jmh.infra.Blackhole;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Varint decode out of a {@link MemorySegment}, byte-wise against a bulk 8-byte load, across the
 * width distributions the two callers actually see.
 *
 * <p>This exists because the obvious optimisation did not survive being measured. Reading eight
 * bytes in one unaligned load and shifting the varint out of the register replaces up to eight
 * individually bounds-checked byte reads, which sounds like it should win outright. It does not:
 * on the hardware this was written against the byte-wise decoder was faster at <em>every</em> width
 * tested — about 2.1× on {@code NARROW}, 1.6× on {@code MIXED}, 1.5× on {@code WIDE}.
 *
 * <p>The narrow case is the easiest to explain: record deltas are overwhelmingly one or two bytes,
 * where the byte-wise decoder issues a single load and returns, while the bulk path first reads
 * {@code byteSize()}, compares, issues a wider load that can straddle a cache line, and only then
 * starts branching. That the wide cases lose too is the more interesting result, and the reason
 * this benchmark is checked in rather than discarded — it is the place to re-check the trade on
 * different hardware, and against the {@code int}-offset overload of {@link DeltaVarIntCodec},
 * which does carry a bulk path on the strength of an analytical-scan profile not reproduced here.
 *
 * <p>The {@code long}-offset overload — the one the record accessors reach, since their offsets
 * derive from a {@code long} heap base — stays byte-wise on the strength of these numbers.
 *
 * <p>{@code WIDE} is the scan-shaped distribution; {@code NARROW} is the ingest-shaped one;
 * {@code MIXED} is an even spread across widths.
 *
 * <p>Run with:
 * <pre>
 *   ./gradlew :sirix-benchmarks:jmh -Pjmh.includes="SegmentVarIntDecodeBenchmark"
 * </pre>
 *
 * @author Johannes Lichtenberger
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 8, time = 1)
@Fork(value = 2, jvmArgsAppend = {"--enable-native-access=ALL-UNNAMED"})
public class SegmentVarIntDecodeBenchmark {

  private static final int COUNT = 100_000;

  private static final long SEED = 20260728L;

  /** Varint width distribution. */
  @Param({"NARROW", "MIXED", "WIDE"})
  public Widths widths;

  /** How wide the encoded values are — the single factor that decides which decoder wins. */
  public enum Widths {
    /** One and two byte values: sibling and parent deltas, the ingest path's diet. */
    NARROW {
      @Override
      long next(final Random random) {
        return random.nextInt(4) == 0 ? 128 + random.nextInt(16_256) : random.nextInt(128);
      }
    },
    /** An even spread over every width. */
    MIXED {
      @Override
      long next(final Random random) {
        return switch (random.nextInt(5)) {
          case 0 -> random.nextInt(128);
          case 1 -> 128 + random.nextInt(16_256);
          case 2 -> 16_384 + random.nextInt(1 << 20);
          case 3 -> (1L << 28) + random.nextInt(1 << 16);
          default -> (1L << 45) + random.nextInt(1 << 16);
        };
      }
    },
    /** Wide values read back to back — the analytical-scan shape. */
    WIDE {
      @Override
      long next(final Random random) {
        return (1L << 45) + random.nextInt(1 << 20);
      }
    };

    abstract long next(Random random);
  }

  private Arena arena;
  private MemorySegment segment;
  private long[] offsets;

  @Setup(Level.Trial)
  public void setUp() {
    final Random random = new Random(SEED);
    final byte[] scratch = new byte[16];
    final byte[] staging = new byte[COUNT * 12];
    offsets = new long[COUNT];

    int position = 0;
    for (int i = 0; i < COUNT; i++) {
      offsets[i] = position;
      final int width = writeVarLong(scratch, widths.next(random));
      System.arraycopy(scratch, 0, staging, position, width);
      position += width;
    }

    arena = Arena.ofConfined();
    // Tail slack so the bulk path is taken at every offset; its fallback is covered by unit tests.
    segment = arena.allocate(position + 16);
    MemorySegment.copy(staging, 0, segment, ValueLayout.JAVA_BYTE, 0L, position);

    System.out.printf("[SegmentVarIntDecodeBenchmark] widths=%s %d varints in %d B (%.2f B each)%n",
        widths, COUNT, position, position / (double) COUNT);
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    arena.close();
  }

  /** The decoder the record accessors use today. */
  @Benchmark
  public void byteWise(final Blackhole blackhole) {
    long sink = 0L;
    for (final long offset : offsets) {
      sink += DeltaVarIntCodec.readVarLongFromSegment(segment, offset);
    }
    blackhole.consume(sink);
  }

  /** The bulk 8-byte alternative, kept here rather than in the codec because it loses on NARROW. */
  @Benchmark
  public void bulkEightByte(final Blackhole blackhole) {
    long sink = 0L;
    for (final long offset : offsets) {
      sink += bulkRead(segment, offset);
    }
    blackhole.consume(sink);
  }

  /** One unaligned 8-byte load, then shift the varint out of the register. */
  private static long bulkRead(final MemorySegment segment, final long offset) {
    if (offset + 8 <= segment.byteSize()) {
      final long w = segment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
      final int b0 = (int) (w & 0xFF);
      if ((b0 & 0x80) == 0) {
        return b0;
      }
      final int b1 = (int) ((w >>> 8) & 0xFF);
      if ((b1 & 0x80) == 0) {
        return (b0 & 0x7FL) | ((long) b1 << 7);
      }
      final int b2 = (int) ((w >>> 16) & 0xFF);
      if ((b2 & 0x80) == 0) {
        return (b0 & 0x7FL) | ((b1 & 0x7FL) << 7) | ((long) b2 << 14);
      }
      final int b3 = (int) ((w >>> 24) & 0xFF);
      if ((b3 & 0x80) == 0) {
        return (b0 & 0x7FL) | ((b1 & 0x7FL) << 7) | ((b2 & 0x7FL) << 14) | ((long) b3 << 21);
      }
      final int b4 = (int) ((w >>> 32) & 0xFF);
      if ((b4 & 0x80) == 0) {
        return (b0 & 0x7FL) | ((b1 & 0x7FL) << 7) | ((b2 & 0x7FL) << 14)
            | ((b3 & 0x7FL) << 21) | ((long) b4 << 28);
      }
      long result = (b0 & 0x7FL) | ((b1 & 0x7FL) << 7) | ((b2 & 0x7FL) << 14)
          | ((b3 & 0x7FL) << 21) | ((b4 & 0x7FL) << 28);
      long position = offset + 5;
      int shift = 35;
      byte b;
      do {
        b = segment.get(ValueLayout.JAVA_BYTE, position++);
        result |= (long) (b & 0x7F) << shift;
        shift += 7;
      } while ((b & 0x80) != 0);
      return result;
    }
    return DeltaVarIntCodec.readVarLongFromSegment(segment, offset);
  }

  private static int writeVarLong(final byte[] buffer, final long value) {
    int position = 0;
    long remaining = value;
    while ((remaining & ~0x7FL) != 0) {
      buffer[position++] = (byte) ((remaining & 0x7F) | 0x80);
      remaining >>>= 7;
    }
    buffer[position++] = (byte) remaining;
    return position;
  }
}
