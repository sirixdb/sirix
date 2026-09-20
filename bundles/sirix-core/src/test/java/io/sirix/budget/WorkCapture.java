/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.budget;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Runs one load or query and captures how much work the engine's own counters say it did.
 *
 * <p>
 * This is the measuring half of a <em>work budget</em> test: the test states how much work an
 * operation may do, in the units the engine already counts (leaves read, routes taken, pages left
 * pinned), and fails when the engine starts doing materially more. It deliberately measures no
 * time. A wall-clock threshold on a shared runner is flaky, and when it fails it cannot say what
 * changed; a work counter is exact on any machine and names the path that grew. See the README
 * beside this class.
 *
 * <pre>{@code
 * final WorkCapture.Captured<String> query = WorkCapture.of(QueryWorkCounters.ROUTES)
 *                                                       .and(EngineWorkCounters.CHUNKED_BODIES)
 *                                                       .call(() -> evaluate(chain, ctx, QUERY));
 * assertEquals(expected, query.result());
 * query.work().assertExactly(QueryWorkCounters.GROUP_SUMMARY, 1, "a count-only group-by scanned the row groups");
 * }</pre>
 *
 * <p>
 * Instances are immutable, so a test class can keep one capture as a constant and reuse it. The
 * counters are process-wide totals: captures are exact only while nothing else in the JVM drives
 * the engine, which holds for the sequential test JVM the build uses. Mark a budget test class
 * {@code @Isolated} so that stays true if parallel execution is ever switched on.
 */
public final class WorkCapture {

  /**
   * Set {@code -Dsirix.workBudget.print=true} on the Gradle command line (the build forwards every
   * {@code sirix.*} property to the test JVM) to print each capture's full table.
   */
  public static final String PRINT_PROPERTY = "sirix.workBudget.print";

  private static final String OWN_CLASS = WorkCapture.class.getName();

  private static final int MAX_PRINTED_CALLERS = 4;

  /** An operation that may throw, returning nothing. */
  @FunctionalInterface
  public interface ThrowingAction {
    void run() throws Exception;
  }

  /** An operation that may throw, returning its answer so the test can check it is still exact. */
  @FunctionalInterface
  public interface ThrowingSupplier<T> {
    T get() throws Exception;
  }

  /**
   * What one captured operation answered and how much work it took.
   *
   * @param result the operation's answer
   * @param work the work it did
   */
  public record Captured<T>(T result, WorkReport work) {
  }

  private final List<WorkCounter> counters;

  private final List<WorkProbe> probes;

  private WorkCapture(final List<WorkCounter> counters, final List<WorkProbe> probes) {
    this.counters = List.copyOf(counters);
    this.probes = List.copyOf(probes);
    requireDistinctNames(this.counters, this.probes);
  }

  /** A capture of the given counters. */
  public static WorkCapture of(final WorkCounter... counters) {
    Objects.requireNonNull(counters, "counters");
    return new WorkCapture(List.of(counters), List.of());
  }

  /** A capture of the given counters. */
  public static WorkCapture of(final Collection<WorkCounter> counters) {
    Objects.requireNonNull(counters, "counters");
    return new WorkCapture(List.copyOf(counters), List.of());
  }

  /** This capture plus {@code more} counters. */
  public WorkCapture and(final Collection<WorkCounter> more) {
    Objects.requireNonNull(more, "more");
    final List<WorkCounter> all = new ArrayList<>(counters.size() + more.size());
    all.addAll(counters);
    all.addAll(more);
    return new WorkCapture(all, probes);
  }

  /** This capture plus {@code more} counters. */
  public WorkCapture and(final WorkCounter... more) {
    Objects.requireNonNull(more, "more");
    return and(List.of(more));
  }

  /** This capture plus a probe, installed for the length of each captured operation. */
  public WorkCapture with(final WorkProbe probe) {
    Objects.requireNonNull(probe, "probe");
    final List<WorkProbe> all = new ArrayList<>(probes.size() + 1);
    all.addAll(probes);
    all.add(probe);
    return new WorkCapture(counters, all);
  }

  /**
   * Runs {@code action} and reports the work it did.
   *
   * @throws AssertionError if a gated counter is switched off, or a counter ran backwards
   * @throws Exception whatever {@code action} throws; the probes are uninstalled first
   */
  public WorkReport run(final ThrowingAction action) throws Exception {
    Objects.requireNonNull(action, "action");
    return call(() -> {
      action.run();
      return null;
    }).work();
  }

  /**
   * Runs {@code action} and reports its answer together with the work it did.
   *
   * @throws AssertionError if a gated counter is switched off, or a counter ran backwards
   * @throws Exception whatever {@code action} throws; the probes are uninstalled first
   */
  public <T> Captured<T> call(final ThrowingSupplier<T> action) throws Exception {
    Objects.requireNonNull(action, "action");
    final List<WorkCounter> captured = allCounters();
    for (final WorkCounter counter : captured) {
      counter.requireLive();
    }

    final int count = captured.size();
    final long[] before = new long[count];
    final long[] after = new long[count];
    final T result;
    int opened = 0;
    try {
      for (final WorkProbe probe : probes) {
        probe.open();
        opened++;
      }
      for (int i = 0; i < count; i++) {
        before[i] = captured.get(i).read();
      }
      result = action.get();
      for (int i = 0; i < count; i++) {
        after[i] = captured.get(i).read();
      }
    } finally {
      // Reverse order, so a probe that displaced another probe's observer hands it back intact.
      for (int i = opened - 1; i >= 0; i--) {
        probes.get(i).close();
      }
    }
    final WorkReport work = WorkReport.between(captured, before, after);
    if (Boolean.getBoolean(PRINT_PROPERTY)) {
      printEvidence(work);
    }
    return new Captured<>(result, work);
  }

  /**
   * Prints one capture under the test frame that took it. Off unless
   * {@code -D}{@value #PRINT_PROPERTY} is set: this is how the figures behind a budget are gathered,
   * before it is set and whenever it is changed.
   */
  private static void printEvidence(final WorkReport work) {
    // The capturing class's own frames, innermost first: a shared helper and then the test calling it.
    final List<StackWalker.StackFrame> callers = StackWalker.getInstance().walk(frames -> {
      final List<StackWalker.StackFrame> own = new ArrayList<>(MAX_PRINTED_CALLERS);
      frames.filter(frame -> !isOwnFrame(frame.getClassName()))
            .takeWhile(frame -> own.isEmpty() || frame.getClassName().equals(own.get(0).getClassName()))
            .limit(MAX_PRINTED_CALLERS)
            .forEach(own::add);
      return own;
    });
    final StringBuilder label = new StringBuilder(128).append("[work-budget] ");
    if (callers.isEmpty()) {
      label.append("unknown caller");
    } else {
      label.append(callers.get(0).getClassName());
      for (int i = callers.size() - 1; i >= 0; i--) {
        label.append(i == callers.size() - 1
            ? " "
            : " -> ").append(callers.get(i).getMethodName()).append(':').append(callers.get(i).getLineNumber());
      }
    }
    System.out.println(label.append(System.lineSeparator()).append(work));
  }

  /** Frames of this class and its nested types; everything else is the code that captured. */
  private static boolean isOwnFrame(final String className) {
    return className.equals(OWN_CLASS) || className.startsWith(OWN_CLASS + '$');
  }

  private List<WorkCounter> allCounters() {
    final List<WorkCounter> all = new ArrayList<>(counters);
    for (final WorkProbe probe : probes) {
      all.addAll(probe.counters());
    }
    return all;
  }

  private static void requireDistinctNames(final List<WorkCounter> counters, final List<WorkProbe> probes) {
    final Set<String> names = new HashSet<>();
    for (final WorkCounter counter : counters) {
      requireNew(names, counter);
    }
    for (final WorkProbe probe : probes) {
      for (final WorkCounter counter : probe.counters()) {
        requireNew(names, counter);
      }
    }
  }

  private static void requireNew(final Set<String> names, final WorkCounter counter) {
    Objects.requireNonNull(counter, "counter");
    if (!names.add(counter.name())) {
      throw new IllegalArgumentException(
          "work counter '" + counter.name() + "' is captured twice; a report names " + "each figure once");
    }
  }
}
