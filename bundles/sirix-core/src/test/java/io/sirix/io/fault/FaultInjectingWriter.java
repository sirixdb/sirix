/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.io.fault;

import io.sirix.access.ResourceConfiguration;
import io.sirix.api.StorageEngineReader;
import io.sirix.exception.SirixIOException;
import io.sirix.io.RevisionFileData;
import io.sirix.io.Writer;
import io.sirix.node.BytesOut;
import io.sirix.page.PageReference;
import io.sirix.page.RevisionRootPage;
import io.sirix.page.UberPage;
import io.sirix.page.interfaces.Page;

import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A delegating {@link Writer} that throws at a configurable {@link Point} after a
 * configurable number of invocations. Used to simulate process death partway through a
 * commit or other Writer operation, so recovery can be exercised in CI without needing
 * a real {@code kill -9}.
 *
 * <p>Usage:
 *
 * <pre>{@code
 * FaultInjectingWriter w = new FaultInjectingWriter(realWriter)
 *     .injectAt(Point.AFTER_UBER_PAGE_REFERENCE_WRITE, 1);
 *
 * // real writer code path uses w; the second writeUberPageReference call throws.
 * }</pre>
 *
 * <p>The decorator is forward-only — once an injection has fired, subsequent calls
 * delegate normally (no automatic re-arming). To re-fire, call
 * {@link #injectAt(Point, int)} again.
 *
 * <p>This class is test-scope-only; production code never sees it. The full integration
 * pattern (wiring this into the live storage factory so real {@link
 * io.sirix.access.trx.node.AbstractResourceSession} commits run through the injector)
 * is deferred — the present scope verifies the decorator's own contract so future tests
 * can rely on it.
 */
public final class FaultInjectingWriter implements Writer {

  /**
   * Where to throw. Names map onto the {@link Writer} entry points: every method that
   * mutates storage has both a {@code BEFORE_*} and an {@code AFTER_*} point so a test
   * can simulate "crashed before write started" vs. "crashed after write returned but
   * before next step." Read-only Reader methods inherited from {@link Writer}'s
   * {@code extends Reader} are not injected — recovery doesn't depend on read failures.
   */
  public enum Point {
    BEFORE_WRITE,
    AFTER_WRITE,
    BEFORE_UBER_PAGE_REFERENCE_WRITE,
    AFTER_UBER_PAGE_REFERENCE_WRITE,
    BEFORE_TRUNCATE_TO,
    AFTER_TRUNCATE_TO,
    BEFORE_TRUNCATE,
    AFTER_TRUNCATE,
    BEFORE_FORCE_ALL,
    AFTER_FORCE_ALL,
    BEFORE_CLOSE,
    AFTER_CLOSE,
  }

  /** Exception thrown at the injection point — distinct so tests can catch it cleanly. */
  public static final class SimulatedCrashException extends RuntimeException {
    public SimulatedCrashException(final String message) {
      super(message);
    }
  }

  private final Writer delegate;
  private volatile Point activePoint;
  private final AtomicInteger countdown = new AtomicInteger();

  public FaultInjectingWriter(final Writer delegate) {
    this.delegate = Objects.requireNonNull(delegate, "delegate");
  }

  /**
   * Arm the injector. The next {@code afterNCalls}-th invocation of the method
   * corresponding to {@code point} will throw {@link SimulatedCrashException} instead
   * of (or in addition to) calling the delegate. {@code afterNCalls == 1} means "throw
   * on the very next call;" {@code 2} means "let one call through, throw on the next."
   *
   * <p>Calling this again before the previous arming fires replaces it.
   */
  public FaultInjectingWriter injectAt(final Point point, final int afterNCalls) {
    if (afterNCalls < 1) {
      throw new IllegalArgumentException("afterNCalls must be >= 1, got " + afterNCalls);
    }
    this.activePoint = Objects.requireNonNull(point, "point");
    this.countdown.set(afterNCalls);
    return this;
  }

  /** Disarm any pending injection; subsequent calls delegate normally. */
  public FaultInjectingWriter clear() {
    this.activePoint = null;
    this.countdown.set(0);
    return this;
  }

  /** Whether the injector is armed for {@code point}. */
  public boolean isArmedFor(final Point point) {
    return this.activePoint == point;
  }

  /**
   * Internal: fire if the active point matches and the countdown hits zero. Disarms
   * after firing so subsequent calls delegate normally — the decorator simulates a
   * single crash, not a continuously-failing storage device.
   */
  private void maybeFire(final Point point) {
    if (this.activePoint != point) {
      return;
    }
    if (this.countdown.decrementAndGet() != 0) {
      return;
    }
    this.activePoint = null;
    throw new SimulatedCrashException("FaultInjectingWriter: crash at " + point);
  }

  // ─────────────────────── Writer methods ───────────────────────

  @Override
  public Writer write(final ResourceConfiguration resourceConfiguration,
      final PageReference pageReference, final Page page, final BytesOut<?> bufferedBytes) {
    maybeFire(Point.BEFORE_WRITE);
    delegate.write(resourceConfiguration, pageReference, page, bufferedBytes);
    maybeFire(Point.AFTER_WRITE);
    return this;
  }

  @Override
  public Writer writeUberPageReference(final ResourceConfiguration resourceConfiguration,
      final PageReference pageReference, final Page page, final BytesOut<?> bufferedBytes) {
    maybeFire(Point.BEFORE_UBER_PAGE_REFERENCE_WRITE);
    delegate.writeUberPageReference(resourceConfiguration, pageReference, page, bufferedBytes);
    maybeFire(Point.AFTER_UBER_PAGE_REFERENCE_WRITE);
    return this;
  }

  @Override
  public Writer truncateTo(final StorageEngineReader storageEngineReader, final int revision) {
    maybeFire(Point.BEFORE_TRUNCATE_TO);
    delegate.truncateTo(storageEngineReader, revision);
    maybeFire(Point.AFTER_TRUNCATE_TO);
    return this;
  }

  @Override
  public Writer truncate() {
    maybeFire(Point.BEFORE_TRUNCATE);
    delegate.truncate();
    maybeFire(Point.AFTER_TRUNCATE);
    return this;
  }

  @Override
  public void flushBufferedWrites(final BytesOut<?> bufferedBytes) {
    delegate.flushBufferedWrites(bufferedBytes);
  }

  @Override
  public void forceAll() {
    maybeFire(Point.BEFORE_FORCE_ALL);
    delegate.forceAll();
    maybeFire(Point.AFTER_FORCE_ALL);
  }

  @Override
  public void close() {
    maybeFire(Point.BEFORE_CLOSE);
    delegate.close();
    maybeFire(Point.AFTER_CLOSE);
  }

  // ─────────────────────── Reader methods (inherited via Writer extends Reader) ───────────────────────
  // Read-only methods are not injected — recovery contracts don't depend on read failures.

  @Override
  public PageReference readUberPageReference() {
    return delegate.readUberPageReference();
  }

  @Override
  public Page read(final PageReference key, final ResourceConfiguration resourceConfiguration) {
    return delegate.read(key, resourceConfiguration);
  }

  @Override
  public RevisionRootPage readRevisionRootPage(final int revision,
      final ResourceConfiguration resourceConfiguration) {
    return delegate.readRevisionRootPage(revision, resourceConfiguration);
  }

  @Override
  public Instant readRevisionRootPageCommitTimestamp(final int revision) {
    return delegate.readRevisionRootPageCommitTimestamp(revision);
  }

  @Override
  public RevisionFileData getRevisionFileData(final int revision) {
    return delegate.getRevisionFileData(revision);
  }
}
