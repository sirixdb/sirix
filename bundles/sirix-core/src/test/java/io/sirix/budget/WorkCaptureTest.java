/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.budget;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The capture helper's own contract. The budget tests trust it to fail loudly in exactly the cases
 * where a quiet reading would let a regression through: a counter that is switched off, a counter
 * that was never captured, a counter reset mid-capture, and an observer left installed.
 */
final class WorkCaptureTest {

  private final AtomicLong leaves = new AtomicLong();

  private final AtomicLong runs = new AtomicLong();

  private final WorkCounter leafReads = WorkCounter.alwaysOn("test.leafReads", "one leaf read", leaves::get);

  private final WorkCounter runReads = WorkCounter.alwaysOn("test.runs", "one run", runs::get);

  @Test
  void aCaptureReportsOnlyTheWorkDoneInsideIt() throws Exception {
    leaves.set(40);
    final WorkCapture.Captured<String> captured = WorkCapture.of(leafReads, runReads).call(() -> {
      leaves.addAndGet(7);
      runs.incrementAndGet();
      return "answer";
    });
    leaves.addAndGet(100);

    assertEquals("answer", captured.result(), "the operation's answer must come back, to be checked for exactness");
    assertEquals(7, captured.work().of(leafReads));
    assertEquals(1, captured.work().of(runReads));
  }

  @Test
  void aCaptureIsImmutableAndReusable() throws Exception {
    final WorkCapture base = WorkCapture.of(leafReads);
    final WorkCapture extended = base.and(runReads);

    final WorkReport first = extended.run(() -> runs.addAndGet(2));
    final WorkReport second = extended.run(() -> runs.addAndGet(5));

    assertEquals(2, first.of(runReads));
    assertEquals(5, second.of(runReads), "each capture takes its own readings");
    assertThrows(IllegalArgumentException.class, () -> base.run(() -> {
    }).of(runReads), "extending a capture must not change the one it was built from");
  }

  @Test
  void aCounterThatWasNotCapturedHasNoReading() throws Exception {
    final WorkReport work = WorkCapture.of(leafReads).run(leaves::incrementAndGet);

    final IllegalArgumentException missing = assertThrows(IllegalArgumentException.class, () -> work.of(runReads));
    assertTrue(missing.getMessage().contains("test.runs"), missing.getMessage());
    assertThrows(IllegalArgumentException.class, () -> work.assertZero(runReads, "would pass on a counter nobody read"),
        "asserting zero on an uncaptured counter must not pass");
  }

  @Test
  void twoCountersMayNotShareAName() {
    final WorkCounter impostor = WorkCounter.alwaysOn("test.leafReads", "another reading", runs::get);

    assertThrows(IllegalArgumentException.class, () -> WorkCapture.of(leafReads, impostor));
    assertThrows(IllegalArgumentException.class, () -> WorkCapture.of(leafReads).and(impostor));
    assertThrows(IllegalArgumentException.class,
        () -> WorkCapture.of(leafReads).with(new RecordingProbe("p", new ArrayList<>(), impostor)));
  }

  @Test
  void aSwitchedOffCounterFailsTheCaptureBeforeTheOperationRuns() {
    final AtomicBoolean gate = new AtomicBoolean(false);
    final WorkCounter gated =
        WorkCounter.gated("test.gated", "one gated unit", leaves::get, "-Dsirix.test.gate=true", gate::get);
    final AtomicBoolean ran = new AtomicBoolean();

    final AssertionError dead =
        assertThrows(AssertionError.class, () -> WorkCapture.of(gated).run(() -> ran.set(true)));

    assertFalse(ran.get(), "a capture that cannot count must not run the operation and report zero work");
    assertTrue(dead.getMessage().contains("-Dsirix.test.gate=true"), dead.getMessage());
    assertTrue(gated.isGated());
    assertFalse(leafReads.isGated());

    gate.set(true);
    gated.requireLive();
  }

  @Test
  void aCounterResetDuringTheCaptureFailsIt() {
    leaves.set(10);

    final AssertionError backwards =
        assertThrows(AssertionError.class, () -> WorkCapture.of(leafReads).run(() -> leaves.set(3)));

    assertTrue(backwards.getMessage().contains("ran backwards"), backwards.getMessage());
    assertTrue(backwards.getMessage().contains("10 -> 3"), backwards.getMessage());
  }

  @Test
  void probesBracketTheOperationAndCloseInReverseOrder() throws Exception {
    final List<String> events = new ArrayList<>();
    final RecordingProbe outer = new RecordingProbe("outer", events);
    final RecordingProbe inner = new RecordingProbe("inner", events);

    WorkCapture.of(leafReads).with(outer).with(inner).run(() -> events.add("operation"));

    assertEquals(List.of("open outer", "open inner", "operation", "close inner", "close outer"), events);
  }

  @Test
  void probesAreClosedWhenTheOperationThrows() {
    final List<String> events = new ArrayList<>();
    final RecordingProbe probe = new RecordingProbe("probe", events);

    final IOException thrown = assertThrows(IOException.class, () -> WorkCapture.of(leafReads).with(probe).run(() -> {
      throw new IOException("the load failed");
    }));

    assertEquals("the load failed", thrown.getMessage(), "the operation's own failure must surface unchanged");
    assertEquals(List.of("open probe", "close probe"), events, "a failed operation must not leave an observer behind");
  }

  @Test
  void aProbeThatFailsToOpenClosesTheOnesAlreadyOpen() {
    final List<String> events = new ArrayList<>();
    final RecordingProbe opened = new RecordingProbe("first", events);
    final WorkProbe failing = new RecordingProbe("second", events) {
      @Override
      public void open() {
        throw new IllegalStateException("seam already taken");
      }
    };
    final AtomicBoolean ran = new AtomicBoolean();

    assertThrows(IllegalStateException.class,
        () -> WorkCapture.of(leafReads).with(opened).with(failing).run(() -> ran.set(true)));

    assertFalse(ran.get());
    assertEquals(List.of("open first", "close first"), events);
  }

  @Test
  void aProbesCountersAreCapturedWithTheRest() throws Exception {
    final AtomicLong observed = new AtomicLong();
    final WorkCounter probed = WorkCounter.alwaysOn("test.probed", "one observed event", observed::get);
    final RecordingProbe probe = new RecordingProbe("probe", new ArrayList<>(), probed);

    final WorkReport work = WorkCapture.of(leafReads).with(probe).run(() -> observed.addAndGet(4));

    assertEquals(4, work.of(probed));
  }

  @Test
  void aBrokenBudgetSaysWhatItGuardsAndShowsEveryCounter() throws Exception {
    final WorkReport work = WorkCapture.of(leafReads, runReads).run(() -> {
      leaves.addAndGet(23);
      runs.addAndGet(2);
    });

    final AssertionError broken = assertThrows(AssertionError.class,
        () -> work.assertAtMost(leafReads, 8, "the count-only group-by scans the row groups again"));

    final String message = broken.getMessage();
    assertTrue(message.contains("test.leafReads = 23"), message);
    assertTrue(message.contains("at most 8"), message);
    assertTrue(message.contains("one unit is: one leaf read"), message);
    assertTrue(message.contains("guards against: the count-only group-by scans the row groups again"), message);
    assertTrue(message.contains("<-- broken"), message);
    assertTrue(message.contains("test.runs"), "the neighbours of the broken counter show where the work went");
    assertTrue(message.contains("README.md"), message);
  }

  @Test
  void everyKindOfBoundHoldsAndBreaksWhereItShould() throws Exception {
    final WorkReport work = WorkCapture.of(leafReads, runReads).run(() -> leaves.addAndGet(5));

    assertSame(work,
        work.assertExactly(leafReads, 5, "exact")
            .assertZero(runReads, "zero")
            .assertAtMost(leafReads, 5, "ceiling")
            .assertAtLeast(leafReads, 5, "floor")
            .assertBetween(leafReads, 5, 5, "both"),
        "a bound that holds returns the report, so budgets chain");

    assertThrows(AssertionError.class, () -> work.assertExactly(leafReads, 4, "exact"));
    assertThrows(AssertionError.class, () -> work.assertZero(leafReads, "zero"));
    assertThrows(AssertionError.class, () -> work.assertAtMost(leafReads, 4, "ceiling"));
    assertThrows(AssertionError.class, () -> work.assertAtLeast(leafReads, 6, "floor"));
    assertThrows(AssertionError.class, () -> work.assertBetween(leafReads, 6, 9, "below the floor"));
    assertThrows(AssertionError.class, () -> work.assertBetween(leafReads, 1, 4, "above the ceiling"));
    // The floor is the point of assertBetween: a counter nobody increments any more reads zero,
    // which every ceiling allows.
    assertThrows(AssertionError.class, () -> work.assertBetween(runReads, 1, 8, "a dead counter reads zero"));
  }

  @Test
  void aBudgetMustBeWellFormedAndSayWhatItGuards() throws Exception {
    final WorkReport work = WorkCapture.of(leafReads).run(() -> leaves.addAndGet(5));

    assertThrows(IllegalArgumentException.class, () -> work.assertBetween(leafReads, 9, 1, "empty range"));
    assertThrows(IllegalArgumentException.class, () -> work.assertAtMost(leafReads, -1, "negative"));
    assertThrows(IllegalArgumentException.class, () -> work.assertAtLeast(leafReads, -1, "negative"));
    assertThrows(IllegalArgumentException.class, () -> work.assertAtMost(leafReads, 1, "  "),
        "a budget that breaks without saying what it guards is a magic number");
  }

  @Test
  void aCounterNeedsANameAndAGatedOneItsProperty() {
    assertThrows(IllegalArgumentException.class, () -> WorkCounter.alwaysOn(" ", "unnamed", leaves::get));
    assertThrows(NullPointerException.class, () -> WorkCounter.alwaysOn(null, "unnamed", leaves::get));
    assertThrows(NullPointerException.class, () -> WorkCounter.alwaysOn("test.x", "no reader", null));
    assertThrows(IllegalArgumentException.class,
        () -> WorkCounter.gated("test.x", "not a JVM argument", leaves::get, "sirix.test.gate", () -> true));
    assertEquals("test.leafReads", leafReads.toString());
    assertEquals("one leaf read", leafReads.meaning());
  }

  @Test
  void theReportListsEveryCounterInCaptureOrder() throws Exception {
    final WorkReport work = WorkCapture.of(runReads, leafReads).run(() -> {
      runs.addAndGet(2);
      leaves.addAndGet(31);
    });

    final String[] lines = work.toString().strip().split("\\R");
    assertEquals(2, lines.length, work.toString());
    assertTrue(lines[0].strip().matches("test\\.runs\\s+2"), lines[0]);
    assertTrue(lines[1].strip().matches("test\\.leafReads\\s+31"), lines[1]);
  }

  /** A probe that records when it is opened and closed, feeding the given counters. */
  private static class RecordingProbe implements WorkProbe {

    private final String name;

    private final List<String> events;

    private final List<WorkCounter> counters;

    RecordingProbe(final String name, final List<String> events, final WorkCounter... counters) {
      this.name = name;
      this.events = events;
      this.counters = List.of(counters);
    }

    @Override
    public List<WorkCounter> counters() {
      return counters;
    }

    @Override
    public void open() {
      events.add("open " + name);
    }

    @Override
    public void close() {
      events.add("close " + name);
    }
  }
}
