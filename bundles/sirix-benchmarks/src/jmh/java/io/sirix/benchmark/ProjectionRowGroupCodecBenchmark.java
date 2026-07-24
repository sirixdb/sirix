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

import io.sirix.index.projection.ProjectionIndexRowGroupCodec;
import io.sirix.index.projection.ProjectionIndexRowGroupPage;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * JMH benchmark for the projection-leaf storage codec
 * ({@link ProjectionIndexRowGroupCodec}) — the encode/decode boundary that backs the
 * README's persistence claims.
 *
 * <p>Two dimensions the README asserts but never measured:
 * <ul>
 *   <li><b>"bit-packed to roughly 5% of their in-memory size"</b> — the trial
 *       {@link #setUp()} prints the actual {@code compact / raw} ratio for a
 *       representative bench-shaped leaf (age {@code long}, active {@code boolean},
 *       dictionary-encoded {@code dept} string), so the figure is observable
 *       rather than asserted.</li>
 *   <li><b>"decoded once per revision ... sub-second per ~10M rows"</b> — the
 *       {@link #decode()} arm reports {@code AverageTime} per 1024-row leaf;
 *       10M rows ≈ 9766 leaves, so the projected per-revision decode cost is
 *       {@code decodeMicros * 9766} — the value the README's "sub-second" claim
 *       should be checked against on the target host.</li>
 * </ul>
 *
 * <p>Run with:
 * <pre>
 *   ./gradlew :sirix-benchmarks:jmh -Pjmh.includes="ProjectionRowGroupCodecBenchmark"
 * </pre>
 *
 * @author Johannes Lichtenberger
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 1,
    jvmArgs = {"--add-modules=jdk.incubator.vector", "--enable-preview", "--enable-native-access=ALL-UNNAMED"})
public class ProjectionRowGroupCodecBenchmark {

  private static final byte[] KINDS = {
      ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG,
      ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN,
      ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT
  };

  private static final String[] DEPTS = {
      "Eng", "Sales", "Ops", "Finance", "HR", "Marketing", "Legal", "Support",
      "Research", "Platform", "Data", "Security", "Design", "QA", "Partners",
      "Customer", "Mobile", "Cloud", "Compliance", "Analytics"
  };

  private byte[] raw;
  private byte[] compact;

  @Setup(Level.Trial)
  public void setUp() {
    final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(KINDS);
    final Random rng = new Random(7);
    long key = 1_000_000L;
    for (int i = 0; i < ProjectionIndexRowGroupPage.MAX_ROWS; i++) {
      key += 8 + rng.nextInt(9);
      final long[] nums = {18 + rng.nextInt(48), 0L, 0L};
      final boolean[] bools = {false, rng.nextBoolean(), false};
      final String[] strs = {null, null, DEPTS[rng.nextInt(DEPTS.length)]};
      page.appendRow(key, nums, bools, strs);
    }
    raw = page.serialize();
    compact = ProjectionIndexRowGroupCodec.encode(raw);
    if (raw.length != ProjectionIndexRowGroupCodec.decode(compact).length) {
      throw new IllegalStateException("decode(encode(raw)) length mismatch — codec broken");
    }
    System.out.printf(
        "%n[projection-codec] raw(in-memory)=%d B, compact(persisted)=%d B, ratio=%.2f%% (%.1fx), %.1f B/row%n",
        raw.length, compact.length, 100.0 * compact.length / raw.length,
        raw.length / (double) compact.length, compact.length / (double) ProjectionIndexRowGroupPage.MAX_ROWS);
  }

  /** Compact-encode a full raw leaf (the commit-time persistence step). */
  @Benchmark
  public byte[] encode() {
    return ProjectionIndexRowGroupCodec.encode(raw);
  }

  /** Decode a compact leaf back to scan form (the per-revision decode step). */
  @Benchmark
  public byte[] decode() {
    return ProjectionIndexRowGroupCodec.decode(compact);
  }
}
