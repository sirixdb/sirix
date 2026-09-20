/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.budget;

import java.util.List;
import java.util.Objects;

/**
 * The work one captured operation did, and the assertions that turn it into a budget.
 *
 * <p>
 * Every assertion takes the regression it guards against, in words. That sentence is the first
 * thing a failure prints, followed by every counter of the capture, so whoever breaks a budget
 * learns which work grew and what else moved with it without opening a profiler. A budget that
 * cannot say what it guards is a magic number; do not add one.
 *
 * <p>
 * Use {@link #assertExactly} only where the path is deterministic (a route is taken or it is not).
 * Where the figure depends on scheduling or on sizes the engine is free to tune, assert a bound,
 * and give the bound a floor as well as a ceiling wherever the operation must do <em>some</em> of
 * that work: a ceiling alone is satisfied by a counter nobody increments any more.
 */
public final class WorkReport {

  private static final String README = "bundles/sirix-core/src/test/java/io/sirix/budget/README.md";

  private final String[] names;

  private final long[] work;

  private WorkReport(final String[] names, final long[] work) {
    this.names = names;
    this.work = work;
  }

  /**
   * The work between two readings of {@code counters}.
   *
   * @throws AssertionError if a counter ran backwards: something reset it mid-capture, and a budget
   *         over a reset counter proves nothing
   */
  static WorkReport between(final List<WorkCounter> counters, final long[] before, final long[] after) {
    final int count = counters.size();
    final String[] names = new String[count];
    final long[] work = new long[count];
    for (int i = 0; i < count; i++) {
      names[i] = counters.get(i).name();
      work[i] = after[i] - before[i];
      if (work[i] < 0) {
        throw new AssertionError("work counter '" + names[i] + "' ran backwards during the capture (" + before[i]
            + " -> " + after[i] + "): something reset it while the operation ran, so the capture proves nothing. "
            + "Reset counters before a capture, never inside one.");
      }
    }
    return new WorkReport(names, work);
  }

  /**
   * The work counted by {@code counter}.
   *
   * @throws IllegalArgumentException if the capture did not include {@code counter}; an uncaptured
   *         counter must not read as zero work
   */
  public long of(final WorkCounter counter) {
    return work[indexOf(counter)];
  }

  /** Requires exactly {@code expected}; for deterministic paths only. */
  public WorkReport assertExactly(final WorkCounter counter, final long expected, final String guardsAgainst) {
    requireExplanation(counter, guardsAgainst);
    final long actual = of(counter);
    if (actual != expected) {
      throw failure(counter, actual, "exactly " + expected, guardsAgainst);
    }
    return this;
  }

  /** Requires none of this work at all. */
  public WorkReport assertZero(final WorkCounter counter, final String guardsAgainst) {
    return assertExactly(counter, 0, guardsAgainst);
  }

  /**
   * Requires at most {@code max}. Prefer {@link #assertBetween} whenever the operation has to do some
   * of this work, so a counter that stopped counting cannot pass.
   */
  public WorkReport assertAtMost(final WorkCounter counter, final long max, final String guardsAgainst) {
    requireExplanation(counter, guardsAgainst);
    requireBound(max, "max");
    final long actual = of(counter);
    if (actual > max) {
      throw failure(counter, actual, "at most " + max, guardsAgainst);
    }
    return this;
  }

  /** Requires at least {@code min}: the operation really took this path, or the counter is live. */
  public WorkReport assertAtLeast(final WorkCounter counter, final long min, final String guardsAgainst) {
    requireExplanation(counter, guardsAgainst);
    requireBound(min, "min");
    final long actual = of(counter);
    if (actual < min) {
      throw failure(counter, actual, "at least " + min, guardsAgainst);
    }
    return this;
  }

  /**
   * Requires {@code min <= work <= max}: a ceiling on the work, and a floor proving it was counted.
   */
  public WorkReport assertBetween(final WorkCounter counter, final long min, final long max,
      final String guardsAgainst) {
    requireExplanation(counter, guardsAgainst);
    requireBound(min, "min");
    requireBound(max, "max");
    if (min > max) {
      throw new IllegalArgumentException("empty budget for '" + counter.name() + "': " + min + " > " + max);
    }
    final long actual = of(counter);
    if (actual < min || actual > max) {
      throw failure(counter, actual, "between " + min + " and " + max, guardsAgainst);
    }
    return this;
  }

  private AssertionError failure(final WorkCounter counter, final long actual, final String budget,
      final String guardsAgainst) {
    final String line = System.lineSeparator();
    final StringBuilder message = new StringBuilder(256);
    message.append("work budget broken: ")
           .append(counter.name())
           .append(" = ")
           .append(actual)
           .append(", budget is ")
           .append(budget)
           .append(line)
           .append("  one unit is: ")
           .append(counter.meaning())
           .append(line)
           .append("  guards against: ")
           .append(guardsAgainst)
           .append(line)
           .append("  captured work:")
           .append(line);
    appendTable(message, counter.name());
    message.append("  A budget changes only deliberately, with evidence: see ").append(README);
    return new AssertionError(message.toString());
  }

  private void appendTable(final StringBuilder out, final String highlighted) {
    int width = 0;
    for (final String name : names) {
      width = Math.max(width, name.length());
    }
    final String line = System.lineSeparator();
    for (int i = 0; i < names.length; i++) {
      out.append("    ").append(names[i]);
      for (int pad = names[i].length(); pad < width + 2; pad++) {
        out.append(' ');
      }
      out.append(work[i]);
      if (names[i].equals(highlighted)) {
        out.append("   <-- broken");
      }
      out.append(line);
    }
  }

  private int indexOf(final WorkCounter counter) {
    Objects.requireNonNull(counter, "counter");
    for (int i = 0; i < names.length; i++) {
      if (names[i].equals(counter.name())) {
        return i;
      }
    }
    throw new IllegalArgumentException("work counter '" + counter.name() + "' was not part of this capture, so "
        + "it has no reading; add it to the WorkCapture instead of treating it as zero");
  }

  private static void requireBound(final long bound, final String what) {
    if (bound < 0) {
      throw new IllegalArgumentException(what + " must not be negative: " + bound);
    }
  }

  /**
   * Every budget says what regression it guards, whether or not it holds today. Checking this only
   * while building a failure message would let a magic number sit green for years and report itself
   * malformed on the one day someone needs it to explain what broke.
   */
  private static void requireExplanation(final WorkCounter counter, final String guardsAgainst) {
    Objects.requireNonNull(counter, "counter");
    Objects.requireNonNull(guardsAgainst, "guardsAgainst");
    if (guardsAgainst.isBlank()) {
      throw new IllegalArgumentException("budget on '" + counter.name() + "' must say what regression it guards");
    }
  }

  /** Every captured counter and the work it counted, one per line. */
  @Override
  public String toString() {
    final StringBuilder out = new StringBuilder(128);
    appendTable(out, "");
    return out.toString();
  }
}
