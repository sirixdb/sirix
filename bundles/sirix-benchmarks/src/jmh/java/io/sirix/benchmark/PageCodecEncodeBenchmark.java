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

import io.sirix.page.ByteRunCodec;
import io.sirix.page.SirixLZ77Codec;
import io.sirix.page.ZeroRunByteCodec;
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
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Encode throughput of the three page-body codecs, on the inputs a record heap actually produces.
 *
 * <p>Profiling a warm JSON ingest attributes roughly a tenth of application-thread CPU to
 * {@link SirixLZ77Codec} alone, which makes this the largest single block on the write path and the
 * one most worth guarding against regression. The two run-length codecs sit on the same path for
 * the column regions.
 *
 * <p>Each codec is measured against four payload shapes, because their cost is entirely
 * data-dependent — a page of repeating record headers and a page of incompressible payload exercise
 * completely different branches:
 * <ul>
 *   <li>{@code RECORD_HEAP} — repeating record headers with varying payloads, the common case;</li>
 *   <li>{@code ZERO_RUNS} — long zero runs, what a sparse column region looks like;</li>
 *   <li>{@code INCOMPRESSIBLE} — random bytes, the worst case, where LZ77 finds nothing and falls
 *       back to literal tokens;</li>
 *   <li>{@code SHORT_RUNS} — a few distinct byte values, which is where the run-length codecs earn
 *       their place.</li>
 * </ul>
 *
 * <p>The {@code segment} and {@code array} arms of the run-length codecs are deliberately both
 * present: the segment entry point mirrors its input into a heap array and then runs the array
 * scan, so the pair reports what that mirror costs against what it saves. If the two ever converge,
 * the mirror has stopped paying for itself.
 *
 * <p>Every arm consumes its result through a {@link Blackhole} and encodes from a pre-filled input,
 * so nothing here measures allocation.
 *
 * <p>Run with:
 * <pre>
 *   ./gradlew :sirix-benchmarks:jmh -Pjmh.includes="PageCodecEncodeBenchmark"
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
public class PageCodecEncodeBenchmark {

  /** A typical Sirix page body. Large enough to be representative, below the 64 KiB LZ77 ceiling. */
  private static final int PAGE_SIZE = 32 * 1024;

  private static final long SEED = 20260728L;

  /** Payload shape. The codecs are data-dependent, so a single shape would prove nothing. */
  @Param({"RECORD_HEAP", "ZERO_RUNS", "INCOMPRESSIBLE", "SHORT_RUNS"})
  public Shape shape;

  /** The input payload shapes, each generating one page body. */
  public enum Shape {
    /** Repeating record headers with varying payloads — what a node heap looks like. */
    RECORD_HEAP {
      @Override
      byte[] generate(final Random random) {
        final byte[] data = new byte[PAGE_SIZE];
        final byte[] header = new byte[4 + random.nextInt(6)];
        random.nextBytes(header);
        int i = 0;
        while (i < data.length) {
          for (int h = 0; h < header.length && i < data.length; h++) {
            data[i++] = header[h];
          }
          final int payload = 4 + random.nextInt(28);
          for (int p = 0; p < payload && i < data.length; p++) {
            data[i++] = (byte) (random.nextInt(6) == 0 ? 0 : random.nextInt(256));
          }
        }
        return data;
      }
    },

    /** Long zero runs between sparse values — a sparse column region. */
    ZERO_RUNS {
      @Override
      byte[] generate(final Random random) {
        final byte[] data = new byte[PAGE_SIZE];
        int i = 0;
        while (i < data.length) {
          i += Math.min(data.length - i, 24 + random.nextInt(200));  // zeros, already in place
          final int values = 1 + random.nextInt(8);
          for (int v = 0; v < values && i < data.length; v++) {
            data[i++] = (byte) (1 + random.nextInt(255));
          }
        }
        return data;
      }
    },

    /** Random bytes — the worst case for all three codecs. */
    INCOMPRESSIBLE {
      @Override
      byte[] generate(final Random random) {
        final byte[] data = new byte[PAGE_SIZE];
        random.nextBytes(data);
        return data;
      }
    },

    /** Runs drawn from a handful of byte values — where run-length encoding wins. */
    SHORT_RUNS {
      @Override
      byte[] generate(final Random random) {
        final byte[] data = new byte[PAGE_SIZE];
        int i = 0;
        while (i < data.length) {
          final byte value = (byte) random.nextInt(4);
          final int run = 2 + random.nextInt(30);
          final int end = Math.min(data.length, i + run);
          Arrays.fill(data, i, end, value);
          i = end;
        }
        return data;
      }
    };

    abstract byte[] generate(Random random);
  }

  private Arena arena;
  private MemorySegment input;
  private byte[] inputArray;
  private byte[] output;

  @Setup(Level.Trial)
  public void setUp() {
    inputArray = shape.generate(new Random(SEED));
    arena = Arena.ofConfined();
    input = arena.allocate(inputArray.length);
    MemorySegment.copy(inputArray, 0, input, ValueLayout.JAVA_BYTE, 0L, inputArray.length);
    // Worst case across all three codecs, so no arm ever overruns or reallocates mid-measurement.
    // Taking LZ77's bound alone is not enough: on incompressible input the run-length codecs
    // expand further than it does, and the overrun surfaces as a dropped @Param rather than a
    // visible failure in JMH's summary table.
    final int worstCase = Math.max(SirixLZ77Codec.maxEncodedSize(inputArray.length),
        Math.max(ByteRunCodec.maxEncodedSize(inputArray.length),
            ZeroRunByteCodec.maxEncodedSize(inputArray.length)));
    output = new byte[worstCase + 64];

    System.out.printf("[PageCodecEncodeBenchmark] shape=%s size=%d B -> lz77=%d B, byteRun=%d B, zeroRun=%d B%n",
        shape, inputArray.length,
        SirixLZ77Codec.encode(input, 0L, inputArray.length, output, 0),
        ByteRunCodec.encode(input, 0L, inputArray.length, output, 0),
        ZeroRunByteCodec.encode(input, 0L, inputArray.length, output, 0));
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    arena.close();
  }

  @Benchmark
  public void lz77Encode(final Blackhole blackhole) {
    blackhole.consume(SirixLZ77Codec.encode(input, 0L, inputArray.length, output, 0));
  }

  @Benchmark
  public void byteRunEncodeFromSegment(final Blackhole blackhole) {
    blackhole.consume(ByteRunCodec.encode(input, 0L, inputArray.length, output, 0));
  }

  @Benchmark
  public void byteRunEncodeFromArray(final Blackhole blackhole) {
    blackhole.consume(ByteRunCodec.encode(inputArray, 0, inputArray.length, output, 0));
  }

  @Benchmark
  public void zeroRunEncodeFromSegment(final Blackhole blackhole) {
    blackhole.consume(ZeroRunByteCodec.encode(input, 0L, inputArray.length, output, 0));
  }

  @Benchmark
  public void zeroRunEncodeFromArray(final Blackhole blackhole) {
    blackhole.consume(ZeroRunByteCodec.encode(inputArray, 0, inputArray.length, output, 0));
  }
}
