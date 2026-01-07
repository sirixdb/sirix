/*
 * Copyright (c) 2023, Sirix
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

import io.sirix.page.PageReference;
import io.sirix.page.delegates.BitmapReferencesPage;
import io.sirix.settings.Constants;
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

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * JMH benchmarks for BitmapReferencesPage index() lookup performance.
 * 
 * <p>Compares the optimized POPCNT-based index() implementation against various
 * access patterns and population densities.</p>
 * 
 * <p>Run with:
 * <pre>
 * ./gradlew :sirix-benchmarks:jmh -Pjmh.includes="BitmapReferencesPageBenchmark"
 * </pre>
 * 
 * @author Johannes Lichtenberger
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 1)
@Fork(value = 1, jvmArgs = {
    "--add-modules=jdk.incubator.vector",
    "--enable-preview",
    "--enable-native-access=ALL-UNNAMED"
})
public class BitmapReferencesPageBenchmark {

  /**
   * Number of references populated in the page.
   * Tests different sparsity levels.
   */
  @Param({"10", "100", "500", "900"})
  public int populationCount;

  /**
   * Whether references are populated densely (consecutive) or sparsely (distributed).
   */
  @Param({"dense", "sparse"})
  public String populationPattern;

  private BitmapReferencesPage page;
  private int[] populatedOffsets;
  private int[] lookupOffsets;
  private int idx;

  @Setup(Level.Trial)
  @SuppressWarnings("resource")
  public void setup() {
    page = new BitmapReferencesPage(Constants.INP_REFERENCE_COUNT);
    Random random = new Random(42);
    
    populatedOffsets = new int[populationCount];
    
    if ("dense".equals(populationPattern)) {
      // Dense: consecutive offsets starting from 0
      for (int i = 0; i < populationCount; i++) {
        populatedOffsets[i] = i;
      }
    } else {
      // Sparse: distributed across the full range
      int step = Constants.INP_REFERENCE_COUNT / populationCount;
      for (int i = 0; i < populationCount; i++) {
        populatedOffsets[i] = Math.min(i * step, Constants.INP_REFERENCE_COUNT - 1);
      }
    }
    
    // Populate the page
    for (int offset : populatedOffsets) {
      PageReference ref = page.getOrCreateReference(offset);
      if (ref != null) {
        ref.setLogKey(offset);
      }
    }
    
    // Pre-generate random lookup offsets (only offsets that exist)
    lookupOffsets = new int[1000];
    for (int i = 0; i < lookupOffsets.length; i++) {
      lookupOffsets[i] = populatedOffsets[random.nextInt(populatedOffsets.length)];
    }
    idx = 0;
  }

  /**
   * Benchmark random lookups on the populated page.
   * This exercises the optimized index() method.
   */
  @Benchmark
  public PageReference randomLookup() {
    int offset = lookupOffsets[idx++ % 1000];
    return page.getOrCreateReference(offset);
  }

  /**
   * Benchmark sequential lookups (simulating a scan).
   */
  @Benchmark
  public void sequentialLookup(Blackhole bh) {
    for (int offset : populatedOffsets) {
      bh.consume(page.getOrCreateReference(offset));
    }
  }

  /**
   * Benchmark lookup at the last (highest) populated offset.
   * This is the worst case for the old implementation.
   */
  @Benchmark
  public PageReference worstCaseLookup() {
    int lastOffset = populatedOffsets[populatedOffsets.length - 1];
    return page.getOrCreateReference(lastOffset);
  }
}

