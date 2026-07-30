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

import io.sirix.page.pax.StringRegion;
import net.openhft.hashing.LongHashFunction;
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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * The per-page string dictionary: what it costs to add a page worth of values, and what the hash
 * that pre-filters its dedup candidates costs on its own.
 *
 * <p>{@link StringRegion.Encoder#addValue} hashes every value it is handed, so on a page dense with
 * strings the hash alone was a measurable share of ingest CPU — a warm profile put the hand-rolled
 * FNV-1a it used to compute at over 2% of application-thread samples. FNV-1a is one multiply per
 * byte on a serial dependency chain, which is why it costs what it does; XXH3 consumes eight bytes
 * a step with instruction-level parallelism.
 *
 * <p>The {@code hashOnly} arms isolate that choice so it can be re-checked rather than assumed:
 * the two hashes over identical corpora, at the string lengths a dictionary actually sees. Swapping
 * them is safe because the hash only decides which dictionary entries get compared — equality is
 * confirmed byte-by-byte, ids are handed out in first-seen order, and the table never leaves the
 * page being encoded — so this is purely a speed question.
 *
 * <p>The answer is not uniform, which is the point of parameterising by length. XXH3's per-call
 * setup has to be amortised before its wider steps pay, so on the hardware this was written
 * against it was roughly 1.6× faster over 12-32 byte values, 2.6× over 32-96, and 4.4× on free
 * text — but about 1.6× <em>slower</em> on 4-12 byte ids, where FNV-1a's single multiply per byte
 * is simply less work. JSON string values sit above that crossover, so the encoder uses XXH3; a
 * workload dominated by very short values would want the opposite, and this is where to find out.
 *
 * <p>{@code addValues} measures the whole path, including the linear dedup scan, so a future change
 * to the dictionary structure has a baseline too. It is worth watching alongside {@code distinct}:
 * the scan is linear in the dictionary size, so the two params together show where hashing stops
 * dominating and the scan takes over.
 *
 * <p>Run with:
 * <pre>
 *   ./gradlew :sirix-benchmarks:jmh -Pjmh.includes="StringRegionDictionaryBenchmark"
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
public class StringRegionDictionaryBenchmark {

  /** Values added per page — one per slot on a string-dense page. */
  private static final int VALUES_PER_PAGE = 1024;

  private static final long SEED = 20260728L;

  private static final LongHashFunction XX3 = LongHashFunction.xx3();

  /** String length bucket. Short keys and free text sit at opposite ends of the hash's cost curve. */
  @Param({"TINY", "SHORT", "MEDIUM", "LONG"})
  public Lengths lengths;

  /** How many distinct values the page holds — the rest are repeats that dedup. */
  @Param({"32", "1024"})
  public int distinct;

  /** The string length buckets a page dictionary sees in practice. */
  public enum Lengths {
    /** Ids and enum-like values. */
    TINY(4, 12),
    /** Names and short labels. */
    SHORT(12, 32),
    /** Titles and one-line descriptions. */
    MEDIUM(32, 96),
    /** Free text. */
    LONG(96, 400);

    private final int min;
    private final int max;

    Lengths(final int min, final int max) {
      this.min = min;
      this.max = max;
    }

    int length(final Random random) {
      return min + random.nextInt(max - min + 1);
    }
  }

  private byte[][] values;
  private StringRegion.Encoder encoder;

  @Setup(Level.Trial)
  public void setUp() {
    final Random random = new Random(SEED);

    final byte[][] pool = new byte[distinct][];
    for (int i = 0; i < distinct; i++) {
      final int length = lengths.length(random);
      final StringBuilder text = new StringBuilder(length);
      for (int c = 0; c < length; c++) {
        text.append((char) ('a' + random.nextInt(26)));
      }
      pool[i] = text.toString().getBytes(StandardCharsets.UTF_8);
    }

    // A page's worth of values drawn from the pool, so repeats exercise the dedup hit path.
    values = new byte[VALUES_PER_PAGE][];
    long totalBytes = 0L;
    for (int i = 0; i < VALUES_PER_PAGE; i++) {
      values[i] = pool[random.nextInt(distinct)];
      totalBytes += values[i].length;
    }

    encoder = new StringRegion.Encoder();

    System.out.printf("[StringRegionDictionaryBenchmark] lengths=%s distinct=%d -> %d values, %d B%n",
        lengths, distinct, VALUES_PER_PAGE, totalBytes);
  }

  /** The whole dictionary path for one page: hash, dedup scan, and insert. */
  @Benchmark
  public void addValues(final Blackhole blackhole) {
    encoder.reset();
    for (int i = 0; i < values.length; i++) {
      // Spread across a few tags, as a real page does — each tag keeps its own dictionary.
      encoder.addValue(i & 3, values[i]);
    }
    blackhole.consume(encoder);
  }

  /** The hash in use today, isolated from the dedup scan. */
  @Benchmark
  public void hashOnlyXxh3(final Blackhole blackhole) {
    long sink = 0L;
    for (final byte[] value : values) {
      sink ^= XX3.hashBytes(value);
    }
    blackhole.consume(sink);
  }

  /** The hand-rolled FNV-1a this replaced, kept as the comparison point. */
  @Benchmark
  public void hashOnlyFnv1a(final Blackhole blackhole) {
    long sink = 0L;
    for (final byte[] value : values) {
      sink ^= fnv1a64(value);
    }
    blackhole.consume(sink);
  }

  private static long fnv1a64(final byte[] data) {
    long hash = 0xcbf29ce484222325L;
    for (final byte b : data) {
      hash ^= b & 0xFF;
      hash *= 0x100000001b3L;
    }
    return hash;
  }
}
