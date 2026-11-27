package io.sirix.benchmark;

import com.sun.management.OperatingSystemMXBean;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.IterationParams;
import org.openjdk.jmh.profile.InternalProfiler;
import org.openjdk.jmh.results.AggregationPolicy;
import org.openjdk.jmh.results.IterationResult;
import org.openjdk.jmh.results.Result;
import org.openjdk.jmh.results.ScalarResult;

import java.lang.management.ManagementFactory;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Reports the CPU time the benchmark JVM spent per operation as the secondary metric
 * {@value #METRIC} in nanoseconds.
 *
 * <p>
 * The nightly benchmark tier ({@code bundles/sirix-query/bench/nightly}) runs on shared hosted
 * runners, where wall-clock time moves with the neighbours of the virtual machine and CPU time does
 * not, or moves much less. The figure is the process CPU time (user plus system, every thread: the
 * benchmark threads, the collector and the JIT compilers) consumed between the start and the end of
 * one iteration, divided by the operations JMH counted in that iteration. It is the cost the
 * measured code causes, so a collector that works harder or a compiler that recompiles is part of
 * the answer.
 *
 * <p>
 * Attach it as {@code -prof io.sirix.benchmark.CpuTimeProfiler}. It is an internal profiler: it
 * runs in the forked JVM next to the benchmark and takes two clock reads per iteration, outside the
 * timed section of the iteration.
 */
public final class CpuTimeProfiler implements InternalProfiler {

  /** The secondary metric name the results ledger importer reads. */
  public static final String METRIC = "cpu.time.norm";

  private static final String UNIT = "ns/op";

  private final OperatingSystemMXBean operatingSystem;
  private long cpuAtStartNanos;

  /** Creates the profiler; refuses a JVM that cannot report process CPU time. */
  public CpuTimeProfiler() {
    // Typed as Object: the platform interface shares its simple name with the com.sun one.
    final Object bean = ManagementFactory.getOperatingSystemMXBean();
    if (!(bean instanceof OperatingSystemMXBean sunBean)) {
      throw new IllegalStateException("this JVM does not report process CPU time (com.sun.management)");
    }
    this.operatingSystem = sunBean;
    if (sunBean.getProcessCpuTime() < 0) {
      throw new IllegalStateException("the operating system does not report process CPU time");
    }
  }

  @Override
  public void beforeIteration(final BenchmarkParams benchmarkParams, final IterationParams iterationParams) {
    cpuAtStartNanos = operatingSystem.getProcessCpuTime();
  }

  @Override
  public Collection<? extends Result> afterIteration(final BenchmarkParams benchmarkParams,
      final IterationParams iterationParams, final IterationResult result) {
    final long cpuNanos = operatingSystem.getProcessCpuTime() - cpuAtStartNanos;
    final long operations = result.getMetadata().getAllOps();
    if (cpuNanos < 0 || operations <= 0) {
      // No operation was counted: a per-operation figure would be a division by zero, and a
      // missing figure is honest where a zero is not.
      return Collections.emptyList();
    }
    return List.of(new ScalarResult(METRIC, (double) cpuNanos / operations, UNIT, AggregationPolicy.AVG));
  }

  @Override
  public String getDescription() {
    return "Process CPU time per operation (user + system, all threads), in ns/op";
  }
}
