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

import io.sirix.io.RevisionIndex;
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

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * JMH benchmarks comparing RevisionIndex search strategies.
 * 
 * <p>Tests performance of:
 * <ul>
 *   <li>Optimized RevisionIndex (SIMD + Eytzinger layout)</li>
 *   <li>Standard Arrays.binarySearch (baseline)</li>
 * </ul>
 * 
 * <p>Run with:
 * <pre>
 * ./gradlew :sirix-benchmarks:jmh
 * </pre>
 * 
 * Or for specific benchmark:
 * <pre>
 * ./gradlew :sirix-benchmarks:benchmarkRevisionIndex
 * </pre>
 * 
 * @author Johannes Lichtenberger
 * @since 1.0.0
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 2, jvmArgs = {
    "--add-modules=jdk.incubator.vector",
    "--enable-preview",
    "--enable-native-access=ALL-UNNAMED"
})
public class RevisionIndexBenchmark {

  /**
   * Number of revisions to test.
   * Covers range from tiny (SIMD linear search) to large (Eytzinger layout).
   */
  @Param({"10", "32", "64", "128", "256", "512", "1024", "4096", "10000"})
  public int size;

  /**
   * Type of search pattern.
   */
  @Param({"first", "middle", "last", "random", "notFound"})
  public String searchPattern;

  // Test data
  private RevisionIndex revisionIndex;
  private long[] timestamps;
  private long[] offsets;
  private long searchTarget;
  private long[] randomTargets;
  private int randomIndex;
  private Random random;

  @Setup(Level.Trial)
  public void setupTrial() {
    // Create timestamp and offset arrays
    timestamps = new long[size];
    offsets = new long[size];
    
    long baseTime = System.currentTimeMillis() - (size * 1000L);
    for (int i = 0; i < size; i++) {
      timestamps[i] = baseTime + (i * 1000L);  // 1 second apart
      offsets[i] = i * 4096L;  // 4KB page size
    }
    
    // Build the optimized RevisionIndex
    revisionIndex = RevisionIndex.create(timestamps, offsets);
    
    // Prepare random targets for random search pattern
    random = new Random(42);  // Fixed seed for reproducibility
    randomTargets = new long[1000];
    for (int i = 0; i < randomTargets.length; i++) {
      int idx = random.nextInt(size);
      randomTargets[i] = timestamps[idx];
    }
    randomIndex = 0;
  }

  @Setup(Level.Invocation)
  public void setupInvocation() {
    // Set up search target based on pattern
    switch (searchPattern) {
      case "first":
        searchTarget = timestamps[0];
        break;
      case "middle":
        searchTarget = timestamps[size / 2];
        break;
      case "last":
        searchTarget = timestamps[size - 1];
        break;
      case "random":
        searchTarget = randomTargets[randomIndex++ % randomTargets.length];
        break;
      case "notFound":
        // Search for a value between two timestamps
        int idx = size / 2;
        searchTarget = (timestamps[idx] + timestamps[idx + 1]) / 2;
        break;
      default:
        searchTarget = timestamps[size / 2];
    }
  }

  /**
   * Benchmark the optimized RevisionIndex.findRevision().
   * Uses SIMD for small sizes, Eytzinger layout for large sizes.
   */
  @Benchmark
  public int optimizedRevisionIndex(Blackhole bh) {
    int result = revisionIndex.findRevision(searchTarget);
    bh.consume(result);
    return result;
  }

  /**
   * Benchmark standard Arrays.binarySearch as baseline.
   */
  @Benchmark
  public int baselineArraysBinarySearch(Blackhole bh) {
    int result = Arrays.binarySearch(timestamps, searchTarget);
    bh.consume(result);
    return result;
  }

  /**
   * Benchmark a simple linear search for comparison on small arrays.
   */
  @Benchmark
  public int baselineLinearSearch(Blackhole bh) {
    int result = linearSearch(timestamps, searchTarget);
    bh.consume(result);
    return result;
  }

  /**
   * Simple linear search implementation for baseline comparison.
   */
  private static int linearSearch(long[] array, long target) {
    for (int i = 0; i < array.length; i++) {
      if (array[i] == target) {
        return i;
      }
      if (array[i] > target) {
        return -(i + 1);
      }
    }
    return -(array.length + 1);
  }
}

