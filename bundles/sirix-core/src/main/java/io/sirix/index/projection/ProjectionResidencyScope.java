/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import java.util.Arrays;
import java.util.concurrent.atomic.LongAdder;

/**
 * One query's residency scope (R1, docs/STORAGE_AND_SPEED_PLAN.md §3): while it is open, every
 * column a {@link ProjectionColumnStore} publishes or reports RESIDENT is PINNED, and at its close
 * the stores it touched release what is no longer pinned and no longer fits the headroom share.
 *
 * <p>
 * <b>Why a scope and not an LRU.</b> A published fill is handed to running scan workers as plain
 * arrays. Evicting one on a size trigger, from a maintenance thread or a cache listener, would drop
 * the store's slot under a kernel that is mid-scan: correct in Java (the worker's own reference
 * keeps the arrays alive) but the ledger would then under-count live bytes and admit a second fill
 * beside the first. Tying the release to a query boundary removes both the timer and the guesswork:
 * at the moment the last query that observed a column resident has left, nothing is scanning it.
 * </p>
 *
 * <p>
 * <b>Pinning is deliberately conservative.</b> A publish or a positive residency answer pins the
 * column in EVERY open scope, not only in the one belonging to the query that asked. Attributing a
 * fill to "the" query would need the query identity threaded through every fill call and through
 * the worker pools that run them; over-pinning needs nothing, and its only effect is to postpone a
 * release by one query boundary. Under-pinning, by contrast, would let one query's close drop a
 * column a concurrent query had already routed itself onto — the mid-route decline this design
 * exists to avoid.
 * </p>
 *
 * <p>
 * <b>Cost.</b> The registry is a copy-on-write array read through one volatile load; with no scope
 * open a pin is that load plus a zero-length loop, and with a scope open it is a bit test on a
 * per-store word. Nothing here runs per leaf or per row: the pin sites are column-granular (a fill,
 * a residency predicate), so the per-query cost is bounded by the number of columns a query
 * touches.
 * </p>
 */
public final class ProjectionResidencyScope implements AutoCloseable {

  private static final ProjectionResidencyScope[] NONE = new ProjectionResidencyScope[0];

  /** Guards {@link #active} — taken only when a scope opens or closes, never on a serving path. */
  private static final Object REGISTRY_LOCK = new Object();

  /** Copy-on-write registry of open scopes; read without a lock on every pin. */
  private static volatile ProjectionResidencyScope[] active = NONE;

  private static final LongAdder OPENED = new LongAdder();

  /** Pinned slots per store; {@code null} until this scope pins something in that store. */
  private ProjectionColumnStore[] stores = new ProjectionColumnStore[2];

  private long[][] pins = new long[2][];

  private int storeCount;

  private volatile boolean closed;

  private ProjectionResidencyScope() {}

  /**
   * Open a scope for the query that is starting. The caller MUST close it — the executor does so at
   * the exit of the top-level call that opened it, in a {@code finally}.
   *
   * @return the open scope
   */
  public static ProjectionResidencyScope open() {
    final ProjectionResidencyScope scope = new ProjectionResidencyScope();
    synchronized (REGISTRY_LOCK) {
      final ProjectionResidencyScope[] current = active;
      final ProjectionResidencyScope[] next = Arrays.copyOf(current, current.length + 1);
      next[current.length] = scope;
      active = next;
    }
    OPENED.increment();
    // Sample the headroom ONCE per query boundary, so every planner predicate and every fill door of
    // the query that is starting price against the SAME budget. A budget that moved mid-query would
    // let a route be admitted as resident and then refused at its second fill — the whole-leaf
    // re-entry the combined-fit rule already exists to prevent.
    ProjectionColumnStore.sampleHeadroomShare();
    return scope;
  }

  /** Scopes opened process-wide — test observability. */
  public static long openedCount() {
    return OPENED.sum();
  }

  /** Whether any scope is currently open (test observability; also the pin fast path's predicate). */
  public static boolean anyOpen() {
    return active.length != 0;
  }

  /**
   * Pin {@code slot} of {@code store} in every open scope.
   *
   * @param store the store whose column (or KEYS lane) was published or reported resident
   * @param slot the pin slot: a column index, or {@link ProjectionColumnStore#keysPinSlot()}
   */
  static void pin(final ProjectionColumnStore store, final int slot) {
    final ProjectionResidencyScope[] scopes = active;
    for (int i = 0; i < scopes.length; i++) {
      scopes[i].pinIn(store, slot);
    }
  }

  private void pinIn(final ProjectionColumnStore store, final int slot) {
    if (closed) {
      return; // volatile pre-check: a closed scope never takes its monitor again
    }
    synchronized (this) {
      if (closed) {
        return;
      }
      long[] bits = null;
      int index = -1;
      for (int i = 0; i < storeCount; i++) {
        if (stores[i] == store) {
          bits = pins[i];
          index = i;
          break;
        }
      }
      if (index < 0) {
        if (storeCount == stores.length) {
          stores = Arrays.copyOf(stores, storeCount << 1);
          pins = Arrays.copyOf(pins, storeCount << 1);
        }
        bits = new long[(store.pinSlotCount() + 63) >>> 6];
        stores[storeCount] = store;
        pins[storeCount] = bits;
        storeCount++;
      }
      final int word = slot >>> 6;
      final long mask = 1L << (slot & 63);
      if (word >= bits.length || (bits[word] & mask) != 0) {
        return; // already pinned by this scope — the store's counter must move exactly once
      }
      bits[word] |= mask;
      store.acquirePin(slot);
    }
  }

  /**
   * Drop this scope's pins and let each store it touched release what is now unpinned and over the
   * headroom share. Idempotent.
   */
  @Override
  public void close() {
    // Unregister FIRST: a pin that reads the registry after this point cannot reach a scope that is
    // about to drain, and one that read it before is stopped by the closed flag inside the monitor.
    synchronized (REGISTRY_LOCK) {
      final ProjectionResidencyScope[] current = active;
      int at = -1;
      for (int i = 0; i < current.length; i++) {
        if (current[i] == this) {
          at = i;
          break;
        }
      }
      if (at >= 0) {
        final ProjectionResidencyScope[] next = new ProjectionResidencyScope[current.length - 1];
        System.arraycopy(current, 0, next, 0, at);
        System.arraycopy(current, at + 1, next, at, current.length - at - 1);
        active = next;
      }
    }
    final ProjectionColumnStore[] touched;
    final long[][] held;
    final int count;
    synchronized (this) {
      if (closed) {
        return;
      }
      closed = true;
      count = storeCount;
      touched = stores;
      held = pins;
      stores = new ProjectionColumnStore[0];
      pins = new long[0][];
      storeCount = 0;
    }
    // Re-sample at the query's END too: the release decides against the memory situation the query
    // actually left behind, not the one it started from.
    ProjectionColumnStore.sampleHeadroomShare();
    // Drop every pin BEFORE sweeping any store: a query that filled two columns of one store must
    // see both unpinned when the first sweep runs, or the sweep would keep the second for no reason.
    // The store monitor is taken by the sweep, never while this scope's monitor is held — the pin
    // path takes the scope monitor while a caller may hold the store monitor, so the one direction
    // store -> scope is the only nesting that ever happens.
    for (int i = 0; i < count; i++) {
      touched[i].dropPins(held[i]);
    }
    for (int i = 0; i < count; i++) {
      touched[i].releaseUnpinnedOverBudget();
    }
  }
}
