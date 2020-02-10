package io.sirix.benchmark;

import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(value = 2, jvmArgs = {"-Xms2G", "-Xmx2G"})
public class BenchmarkRunner {
  public static void main(String[] args) throws RunnerException {
    final var opt = new OptionsBuilder()
        .include(".*XMarkBench.*")
        .forks(1)
        .build();

    new Runner(opt).run();
  }
}
