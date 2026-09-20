/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.budget;

import org.jspecify.annotations.Nullable;

import java.util.Objects;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;

/**
 * One of the engine's own work counters, named so a budget failure can say which kind of work grew.
 *
 * <p>
 * A counter is a name and a way to read a monotonic total; {@link WorkCapture} turns two readings
 * into the work one operation did. The name is the engine's own word for the figure wherever it has
 * one (the benchmark runner's {@code # served:} line says {@code groupSummary}, so does the
 * counter), so a failing budget reads like the campaign output it guards.
 *
 * <p>
 * <b>Gated counters.</b> Counters on a hot path are compiled away in production behind a
 * {@code static final} flag read once at class initialization, so a test cannot switch one on for
 * itself. A gated counter therefore carries its flag and the JVM argument that provides it, and
 * {@link #requireLive()} fails the test rather than letting it read a dead instrument: every such
 * counter reads zero with the gate off, and an upper bound is trivially satisfied by zero. The
 * build provides the argument (see the {@code test} block of
 * {@code bundles/sirix-core/build.gradle}); the test asserts it. A counter that exists only under
 * one configuration, such as the frame-slot allocator's, is gated the same way.
 */
public final class WorkCounter {

  private final String name;

  private final String meaning;

  private final LongSupplier reader;

  private final @Nullable String gateArgument;

  private final @Nullable BooleanSupplier gate;

  private WorkCounter(final String name, final String meaning, final LongSupplier reader,
      final @Nullable String gateArgument, final @Nullable BooleanSupplier gate) {
    this.name = requireName(name);
    this.meaning = Objects.requireNonNull(meaning, "meaning");
    this.reader = Objects.requireNonNull(reader, "reader");
    this.gateArgument = gateArgument;
    this.gate = gate;
  }

  /**
   * A counter the engine maintains unconditionally.
   *
   * @param name the engine's own name for the figure; unique within one capture
   * @param meaning one line on what one unit of this counter is
   * @param reader reads the running total
   */
  public static WorkCounter alwaysOn(final String name, final String meaning, final LongSupplier reader) {
    return new WorkCounter(name, meaning, reader, null, null);
  }

  /**
   * A counter that only counts while a {@code static final} diagnostic flag is on.
   *
   * @param name the engine's own name for the figure; unique within one capture
   * @param meaning one line on what one unit of this counter is
   * @param reader reads the running total
   * @param gateArgument the complete JVM argument that switches the counter on, as the build must
   *        provide it, for example {@code -Dsirix.hot.mergeDiag=true}
   * @param gate reads the flag the engine actually folded in, not the property
   */
  public static WorkCounter gated(final String name, final String meaning, final LongSupplier reader,
      final String gateArgument, final BooleanSupplier gate) {
    Objects.requireNonNull(gateArgument, "gateArgument");
    Objects.requireNonNull(gate, "gate");
    if (!gateArgument.startsWith("-D")) {
      throw new IllegalArgumentException(
          "a gated counter must give the JVM argument that provides its gate, not '" + gateArgument + "'");
    }
    return new WorkCounter(name, meaning, reader, gateArgument, gate);
  }

  public String name() {
    return name;
  }

  public String meaning() {
    return meaning;
  }

  /** The running total right now. */
  public long read() {
    return reader.getAsLong();
  }

  /** Whether this counter only counts behind a diagnostic flag. */
  public boolean isGated() {
    return gate != null;
  }

  /**
   * Fails unless this counter is counting. An always-on counter is live by construction; a gated one
   * is live only when the engine's flag is on.
   *
   * @throws AssertionError if the gate is off, naming the JVM argument the build has to provide
   */
  public void requireLive() {
    final BooleanSupplier flag = gate;
    if (flag != null && !flag.getAsBoolean()) {
      throw new AssertionError("work counter '" + name + "' is switched off, so it reads zero whatever the engine does "
          + "and any budget on it would pass vacuously. Its gate is read once, when the engine class initializes: "
          + "provide " + gateArgument + " to the test JVM (the 'test' block of the module's build.gradle) instead "
          + "of setting it from the test.");
    }
  }

  private static String requireName(final String name) {
    Objects.requireNonNull(name, "name");
    if (name.isBlank()) {
      throw new IllegalArgumentException("a work counter needs a name");
    }
    return name;
  }

  @Override
  public String toString() {
    return name;
  }
}
