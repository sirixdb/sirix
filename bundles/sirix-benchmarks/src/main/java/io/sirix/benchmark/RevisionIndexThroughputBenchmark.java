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
 * Throughput benchmarks for RevisionIndex to measure operations per second.
 * 
 * <p>Focuses on throughput rather than latency to understand 
 * how many revision lookups can be performed per second under load.
 * 
 * @author Johannes Lichtenberger
 * @since 1.0.0
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 2)
@Fork(value = 2, jvmArgs = {
    "--add-modules=jdk.incubator.vector",
    "--enable-preview",
    "--enable-native-access=ALL-UNNAMED"
})
public class RevisionIndexThroughputBenchmark {

  /**
   * Size ranges to test SIMD vs Eytzinger crossover point.
   */
  @Param({"16", "32", "48", "64", "80", "96", "112", "128", "160", "192", "256"})
  public int size;

  private RevisionIndex revisionIndex;
  private long[] timestamps;
  private long[] randomTargets;
  private int targetIndex;

  @Setup(Level.Trial)
  public void setup() {
    timestamps = new long[size];
    long[] offsets = new long[size];
    
    long baseTime = System.currentTimeMillis();
    for (int i = 0; i < size; i++) {
      timestamps[i] = baseTime + (i * 1000L);
      offsets[i] = i * 4096L;
    }
    
    revisionIndex = RevisionIndex.create(timestamps, offsets);
    
    // Pre-generate random lookup targets for reproducibility
    Random random = new Random(12345);
    randomTargets = new long[10000];
    for (int i = 0; i < randomTargets.length; i++) {
      randomTargets[i] = timestamps[random.nextInt(size)];
    }
    targetIndex = 0;
  }

  /**
   * Throughput of optimized RevisionIndex with random access pattern.
   */
  @Benchmark
  public int optimizedRandomAccess(Blackhole bh) {
    long target = randomTargets[targetIndex++ % randomTargets.length];
    int result = revisionIndex.findRevision(target);
    bh.consume(result);
    return result;
  }

  /**
   * Throughput of Arrays.binarySearch with random access pattern.
   */
  @Benchmark
  public int baselineRandomAccess(Blackhole bh) {
    long target = randomTargets[targetIndex++ % randomTargets.length];
    int result = Arrays.binarySearch(timestamps, target);
    bh.consume(result);
    return result;
  }

  /**
   * Throughput of optimized RevisionIndex with sequential access pattern.
   * This tests cache locality effects.
   */
  @Benchmark
  public int optimizedSequentialAccess(Blackhole bh) {
    int idx = targetIndex++ % size;
    int result = revisionIndex.findRevision(timestamps[idx]);
    bh.consume(result);
    return result;
  }

  /**
   * Throughput of Arrays.binarySearch with sequential access pattern.
   */
  @Benchmark
  public int baselineSequentialAccess(Blackhole bh) {
    int idx = targetIndex++ % size;
    int result = Arrays.binarySearch(timestamps, timestamps[idx]);
    bh.consume(result);
    return result;
  }
}

