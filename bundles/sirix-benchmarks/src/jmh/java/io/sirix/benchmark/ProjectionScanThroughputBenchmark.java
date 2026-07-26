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

import io.sirix.index.projection.ProjectionIndexByteScan;
import io.sirix.index.projection.ProjectionIndexRowGroupPage;
import io.sirix.index.projection.ProjectionIndexScan;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * JMH throughput benchmark for the columnar projection scan kernels in
 * {@link ProjectionIndexByteScan} — the vectorised path the README describes
 * ("scans with SIMD kernels"). Operates on synthetic serialised
 * {@link ProjectionIndexRowGroupPage}s (a {@code long} age column, a {@code boolean}
 * active column and a dictionary-encoded {@code dept} string column, 1024 rows
 * per leaf) so the numbers isolate the scan kernel from session/HOT I/O.
 *
 * <p>The numeric-compare arms exercise the two-pass compare-then-pack kernel
 * whose inner compare loop C2 SuperWord auto-vectorises to {@code VPCMPGTQ}
 * (allocation-free); {@code AverageTime} over a known row count converts
 * directly to <b>ns/row</b>, the figure comparable to a columnar engine's
 * per-record filter cost.
 *
 * <p>Run with:
 * <pre>
 *   ./gradlew :sirix-benchmarks:jmh -Pjmh.includes="ProjectionScanThroughputBenchmark"
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
public class ProjectionScanThroughputBenchmark {

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

  /** Total rows across all leaves. Divide the reported µs by this to get ns/row. */
  @Param({"1048576"})
  public int rows;

  private List<byte[]> leaves;
  private ProjectionIndexScan.ColumnPredicate[] numericGt;
  private ProjectionIndexScan.ColumnPredicate[] numericBetween;
  private ProjectionIndexScan.ColumnPredicate[] conjunction;
  private ProjectionIndexScan.ColumnPredicate[] none;
  /** Reused across invocations so the group-by arm times the scan, not map allocation/resize. */
  private Object2LongOpenHashMap<String> deptGroups;

  @Setup(Level.Trial)
  public void setUp() {
    final int rowGroupCount = (rows + ProjectionIndexRowGroupPage.MAX_ROWS - 1) / ProjectionIndexRowGroupPage.MAX_ROWS;
    leaves = new ArrayList<>(rowGroupCount);
    final Random rng = new Random(42);
    long key = 0;
    int remaining = rows;
    for (int l = 0; l < rowGroupCount; l++) {
      final int n = Math.min(ProjectionIndexRowGroupPage.MAX_ROWS, remaining);
      final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(KINDS);
      for (int i = 0; i < n; i++) {
        key += 8 + rng.nextInt(9);
        final long[] nums = {18 + rng.nextInt(48), 0L, 0L};
        final boolean[] bools = {false, rng.nextBoolean(), false};
        final String[] strs = {null, null, DEPTS[rng.nextInt(DEPTS.length)]};
        page.appendRow(key, nums, bools, strs);
      }
      leaves.add(page.serialize());
      remaining -= n;
    }
    numericGt = new ProjectionIndexScan.ColumnPredicate[] {
        ProjectionIndexScan.ColumnPredicate.numeric(0, ProjectionIndexScan.Op.GT, 40L) };
    numericBetween = new ProjectionIndexScan.ColumnPredicate[] {
        ProjectionIndexScan.ColumnPredicate.numericBetween(0, ProjectionIndexScan.Op.GE, 30L,
            ProjectionIndexScan.Op.LT, 55L) };
    conjunction = new ProjectionIndexScan.ColumnPredicate[] {
        ProjectionIndexScan.ColumnPredicate.numeric(0, ProjectionIndexScan.Op.GT, 40L),
        ProjectionIndexScan.ColumnPredicate.booleanEq(1, true) };
    none = new ProjectionIndexScan.ColumnPredicate[0];
    // Pre-sized past the DEPTS cardinality so it never resizes during a run.
    deptGroups = new Object2LongOpenHashMap<>(2 * DEPTS.length);
  }

  /** Clear the reused group-by accumulator between invocations, outside the measured region. */
  @Setup(Level.Invocation)
  public void resetGroups() {
    deptGroups.clear();
  }

  /** Single-bound numeric filter — the SuperWord-vectorised compare kernel. */
  @Benchmark
  public long numericGreaterThan() {
    return ProjectionIndexByteScan.conjunctiveCount(leaves, numericGt);
  }

  /** Fused range filter — one load, two compares AND'd per row. */
  @Benchmark
  public long numericBetween() {
    return ProjectionIndexByteScan.conjunctiveCount(leaves, numericBetween);
  }

  /** Conjunctive predicate over the numeric + boolean columns. */
  @Benchmark
  public long conjunctiveFilteredCount() {
    return ProjectionIndexByteScan.conjunctiveCount(leaves, conjunction);
  }

  /** Full-column aggregate: count/sum/min/max folded over the age column. */
  @Benchmark
  public void numericAggregate(final Blackhole bh) {
    final long[] acc = {0L, 0L, Long.MAX_VALUE, Long.MIN_VALUE};
    ProjectionIndexByteScan.conjunctiveAggregateNumeric(leaves, none, 0, acc);
    bh.consume(acc);
  }

  /** Single-key group-by-count over the dictionary-encoded dept column. */
  @Benchmark
  public int groupByDeptCount() {
    ProjectionIndexByteScan.conjunctiveCountByGroup(leaves, none, 2, deptGroups);
    return deptGroups.size();
  }
}
