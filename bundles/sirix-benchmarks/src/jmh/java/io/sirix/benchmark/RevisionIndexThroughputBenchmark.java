/*
 * Copyright (c) 2023, Sirix
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
 * Throughput benchmark for RevisionIndex - measures ops/µs.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 2)
@Fork(value = 1, jvmArgs = {
    "--add-modules=jdk.incubator.vector",
    "--enable-preview",
    "--enable-native-access=ALL-UNNAMED"
})
public class RevisionIndexThroughputBenchmark {

  @Param({"64", "128", "256", "1000"})
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
    
    Random random = new Random(12345);
    randomTargets = new long[1024];
    for (int i = 0; i < randomTargets.length; i++) {
      randomTargets[i] = timestamps[random.nextInt(size)];
    }
    idx = 0;
  }

  @Benchmark
  public int optimizedRandomAccess() {
    return revisionIndex.findRevision(randomTargets[idx++ % 1024]);
  }

  @Benchmark
  public int baselineRandomAccess() {
    return Arrays.binarySearch(timestamps, randomTargets[idx++ % 1024]);
  }
}
