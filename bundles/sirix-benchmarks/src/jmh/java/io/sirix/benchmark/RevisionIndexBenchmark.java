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

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * JMH benchmarks comparing RevisionIndex search strategies.
 * 
 * <p>Run with:
 * <pre>
 * ./gradlew :sirix-benchmarks:jmh
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
public class RevisionIndexBenchmark {

  /**
   * Key sizes to test SIMD threshold crossover.
   */
  @Param({"32", "64", "128", "256", "1000"})
  public int size;

  private RevisionIndex revisionIndex;
  private long[] timestamps;
  private long[] randomTargets;
  private int idx;

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
    
    // Pre-generate random targets
    Random random = new Random(42);
    randomTargets = new long[1000];
    for (int i = 0; i < randomTargets.length; i++) {
      randomTargets[i] = timestamps[random.nextInt(size)];
    }
    idx = 0;
  }

  @Benchmark
  public int optimizedRevisionIndex() {
    long target = randomTargets[idx++ % 1000];
    return revisionIndex.findRevision(target);
  }

  @Benchmark
  public int baselineArraysBinarySearch() {
    long target = randomTargets[idx++ % 1000];
    return Arrays.binarySearch(timestamps, target);
  }
}
