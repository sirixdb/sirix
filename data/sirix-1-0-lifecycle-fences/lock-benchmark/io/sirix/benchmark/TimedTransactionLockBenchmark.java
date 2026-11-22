package io.sirix.benchmark;

import io.sirix.access.trx.node.BeforeTransactionExecutionLock;
import io.sirix.access.trx.node.TransactionExecutionLock;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class TimedTransactionLockBenchmark {
  @Param({"before", "after"})
  public String implementation;
  private Lock execution;
  private long value;

  @Setup
  public void setup() {
    execution = implementation.equals("before")
        ? new BeforeTransactionExecutionLock()
        : new TransactionExecutionLock();
  }

  @Benchmark
  public long timedMutation() {
    execution.lock();
    try {
      return ++value;
    } finally {
      execution.unlock();
    }
  }

  @Benchmark
  public long countCommitWithinTimedMutation() {
    execution.lock();
    try {
      execution.lock();
      try {
        execution.lock();
        try {
          return ++value;
        } finally {
          execution.unlock();
        }
      } finally {
        execution.unlock();
      }
    } finally {
      execution.unlock();
    }
  }
}
