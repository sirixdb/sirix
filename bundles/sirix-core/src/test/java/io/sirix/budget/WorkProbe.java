/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.budget;

import java.util.List;

/**
 * Work the engine exposes through a test seam rather than a counter: an observer or hook a test
 * installs, which the probe turns into {@link WorkCounter}s for the length of one capture.
 *
 * <p>
 * The engine's seams are single static fields, so a probe owns the field only between
 * {@link #open()} and {@link #close()} and must hand back whatever it displaced. A probe that left
 * its observer installed would count the next test's work into a closed capture, and one that
 * cleared the field instead of restoring it would silently disarm another test's fault injection.
 *
 * <p>
 * A probe lives in the package that owns its seam, because the seams are package-private on
 * purpose; the probe class itself is public so tests in any module can capture through it.
 */
public interface WorkProbe extends AutoCloseable {

  /**
   * The counters this probe feeds. They start from zero at {@link #open()}, so a level the probe
   * tracks as a running maximum reads as that maximum in the capture.
   */
  List<WorkCounter> counters();

  /** Installs the probe's observers, remembering what they displace, and zeroes its counters. */
  void open();

  /** Restores what {@link #open()} displaced. Must not throw, and must be safe to call twice. */
  @Override
  void close();
}
