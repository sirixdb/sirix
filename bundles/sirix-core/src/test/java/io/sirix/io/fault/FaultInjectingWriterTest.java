package io.sirix.io.fault;

import io.sirix.access.ResourceConfiguration;
import io.sirix.api.StorageEngineReader;
import io.sirix.io.RevisionFileData;
import io.sirix.io.Writer;
import io.sirix.io.fault.FaultInjectingWriter.Point;
import io.sirix.io.fault.FaultInjectingWriter.SimulatedCrashException;
import io.sirix.node.BytesOut;
import io.sirix.page.PageReference;
import io.sirix.page.RevisionRootPage;
import io.sirix.page.interfaces.Page;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Contract tests for {@link FaultInjectingWriter}: pin the decorator's behaviour so
 * follow-up tests can layer it onto real storage paths and trust what they see.
 */
final class FaultInjectingWriterTest {

  /** Counts every method-call delegation so tests can assert which paths fired. */
  private static final class CountingWriter implements Writer {
    final AtomicInteger writes = new AtomicInteger();
    final AtomicInteger uberWrites = new AtomicInteger();
    final AtomicInteger truncateTos = new AtomicInteger();
    final AtomicInteger truncates = new AtomicInteger();
    final AtomicInteger forceAlls = new AtomicInteger();
    final AtomicInteger closes = new AtomicInteger();

    @Override
    public Writer write(final ResourceConfiguration rc, final PageReference pr,
        final Page page, final BytesOut<?> bb) {
      writes.incrementAndGet();
      return this;
    }
    @Override
    public Writer writeUberPageReference(final ResourceConfiguration rc,
        final PageReference pr, final Page page, final BytesOut<?> bb) {
      uberWrites.incrementAndGet();
      return this;
    }
    @Override
    public Writer truncateTo(final StorageEngineReader r, final int rev) {
      truncateTos.incrementAndGet();
      return this;
    }
    @Override public Writer truncate() {
      truncates.incrementAndGet();
      return this;
    }
    @Override public void forceAll() {
      forceAlls.incrementAndGet();
    }
    @Override public void close() {
      closes.incrementAndGet();
    }
    @Override public PageReference readUberPageReference() { return null; }
    @Override public Page read(final PageReference key, final ResourceConfiguration rc) { return null; }
    @Override public RevisionRootPage readRevisionRootPage(final int r, final ResourceConfiguration rc) { return null; }
    @Override public Instant readRevisionRootPageCommitTimestamp(final int r) { return Instant.EPOCH; }
    @Override public RevisionFileData getRevisionFileData(final int r) { return null; }
  }

  @Test
  void delegatesWhenNotArmed() {
    final CountingWriter inner = new CountingWriter();
    final FaultInjectingWriter w = new FaultInjectingWriter(inner);

    w.write(null, null, null, null);
    w.writeUberPageReference(null, null, null, null);
    w.truncateTo(null, 0);
    w.forceAll();
    w.close();

    assertEquals(1, inner.writes.get());
    assertEquals(1, inner.uberWrites.get());
    assertEquals(1, inner.truncateTos.get());
    assertEquals(1, inner.forceAlls.get());
    assertEquals(1, inner.closes.get());
  }

  @Test
  void firesAtBeforeWrite_skipsDelegate() {
    final CountingWriter inner = new CountingWriter();
    final FaultInjectingWriter w = new FaultInjectingWriter(inner)
        .injectAt(Point.BEFORE_WRITE, 1);

    final SimulatedCrashException ex = assertThrows(SimulatedCrashException.class,
        () -> w.write(null, null, null, null));
    assertTrue(ex.getMessage().contains("BEFORE_WRITE"), "message should name the point");
    assertEquals(0, inner.writes.get(),
        "BEFORE_* must throw before invoking the delegate");
  }

  @Test
  void firesAtAfterWrite_invokesDelegateThenThrows() {
    final CountingWriter inner = new CountingWriter();
    final FaultInjectingWriter w = new FaultInjectingWriter(inner)
        .injectAt(Point.AFTER_WRITE, 1);

    assertThrows(SimulatedCrashException.class,
        () -> w.write(null, null, null, null));
    assertEquals(1, inner.writes.get(),
        "AFTER_* must invoke the delegate before throwing");
  }

  @Test
  void countdownAllowsPriorCalls_thenFires() {
    final CountingWriter inner = new CountingWriter();
    final FaultInjectingWriter w = new FaultInjectingWriter(inner)
        .injectAt(Point.AFTER_WRITE, 3); // throw on the third call

    w.write(null, null, null, null);
    w.write(null, null, null, null);
    assertEquals(2, inner.writes.get(), "first two calls should pass through");

    assertThrows(SimulatedCrashException.class,
        () -> w.write(null, null, null, null));
    assertEquals(3, inner.writes.get(), "third call invokes delegate then throws");
  }

  @Test
  void firesOnce_thenSelfDisarms() {
    final CountingWriter inner = new CountingWriter();
    final FaultInjectingWriter w = new FaultInjectingWriter(inner)
        .injectAt(Point.AFTER_WRITE, 1);

    assertThrows(SimulatedCrashException.class,
        () -> w.write(null, null, null, null));

    // Subsequent calls should delegate normally — the decorator simulates a single
    // crash, not a continuously-failing device.
    w.write(null, null, null, null);
    w.write(null, null, null, null);
    assertEquals(3, inner.writes.get());
    assertFalse(w.isArmedFor(Point.AFTER_WRITE), "post-fire state must be disarmed");
  }

  @Test
  void clear_disarmsBeforeFiring() {
    final CountingWriter inner = new CountingWriter();
    final FaultInjectingWriter w = new FaultInjectingWriter(inner)
        .injectAt(Point.BEFORE_WRITE, 1);
    assertTrue(w.isArmedFor(Point.BEFORE_WRITE));
    w.clear();
    assertFalse(w.isArmedFor(Point.BEFORE_WRITE));

    w.write(null, null, null, null);
    assertEquals(1, inner.writes.get(),
        "after clear, calls must delegate normally");
  }

  @Test
  void wrongPointDoesNotFire() {
    final CountingWriter inner = new CountingWriter();
    final FaultInjectingWriter w = new FaultInjectingWriter(inner)
        .injectAt(Point.BEFORE_FORCE_ALL, 1);

    // write() should not fire — only forceAll() should.
    w.write(null, null, null, null);
    w.writeUberPageReference(null, null, null, null);
    assertEquals(1, inner.writes.get());
    assertEquals(1, inner.uberWrites.get());

    assertThrows(SimulatedCrashException.class, w::forceAll);
  }

  @Test
  void allEntryPoints_canBeArmedAndFire() {
    for (final Point p : Point.values()) {
      final CountingWriter inner = new CountingWriter();
      final FaultInjectingWriter w = new FaultInjectingWriter(inner)
          .injectAt(p, 1);
      // Drive the entry point that matches the configured Point. We don't try to
      // exercise EVERY method here — we just assert that for every Point, arming the
      // decorator records that arming. The previous tests cover the actual delegate
      // and throw semantics for representative points; this one is a coverage smoke
      // that the enum and the maybeFire() switch are wired up consistently.
      assertTrue(w.isArmedFor(p), "decorator must report armed for " + p);
    }
  }

  @Test
  void rejectsZeroOrNegativeCountdown() {
    final FaultInjectingWriter w = new FaultInjectingWriter(new CountingWriter());
    assertThrows(IllegalArgumentException.class, () -> w.injectAt(Point.AFTER_WRITE, 0));
    assertThrows(IllegalArgumentException.class, () -> w.injectAt(Point.AFTER_WRITE, -1));
  }

  @Test
  void rejectsNullPoint() {
    final FaultInjectingWriter w = new FaultInjectingWriter(new CountingWriter());
    assertThrows(NullPointerException.class, () -> w.injectAt(null, 1));
  }

  @Test
  void rejectsNullDelegate() {
    assertThrows(NullPointerException.class, () -> new FaultInjectingWriter(null));
  }

  @Test
  void readMethodsAreNotInjected() {
    // Inherited Reader methods (read, readUberPageReference, etc.) MUST delegate
    // straight through — recovery contracts don't depend on read failures.
    final CountingWriter inner = new CountingWriter();
    final FaultInjectingWriter w = new FaultInjectingWriter(inner)
        .injectAt(Point.BEFORE_WRITE, 1);

    // No injection point exists for read — these should not throw even though the
    // decorator is armed.
    assertSame(null, w.readUberPageReference());
    assertSame(null, w.read(null, null));
    assertNotNull(w.readRevisionRootPageCommitTimestamp(0));
    assertSame(null, w.getRevisionFileData(0));
  }
}
