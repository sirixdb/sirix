/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access.trx.page;

import io.sirix.budget.WorkCounter;
import io.sirix.budget.WorkProbe;
import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiConsumer;

/**
 * Watches a load's transaction intent log, async-flush rotation by rotation: how many pages it left
 * pinned, and how many the pre-commit spill drained.
 *
 * <p>
 * Trie pages a load cannot flush yet sit in the intent log's pinned region, each holding an
 * off-heap frame, and leave it before the final commit only through
 * {@code spillEligiblePinnedTriePages}. The engine counts neither side readably: its own spill
 * figures are private and printed only under {@code -Dsirix.hft.telemetry}. It does expose the
 * async-flush fault hook, whose sites bracket exactly the moments that matter, and
 * {@code TransactionIntentLog.pinnedSize()} is public and always on. This probe reads the one at
 * the other.
 *
 * <ul>
 * <li>{@code "prepare"} fires once per async-flush <em>rotation</em>, <em>after</em> the previous
 * snapshot's cleanup has promoted pages into the pinned region. That is every rotation, not only a
 * full intent-log epoch: {@code startAsyncFlushOwned} runs the spill inside
 * {@code if (includeTransactionLog)} and injects this site outside it, so a side-pages-only
 * rotation — the path a projection bulk load drives most — reaches it with no spill having run. The
 * pinned size sampled there is therefore post-spill on a full epoch and pre-spill on a side-only
 * rotation, which can only make {@link #pinnedPagesPeak()} read high, never low. A spill that runs
 * keeps the region bounded; one refused at its gate lets it grow until the arena is exhausted.</li>
 * <li>{@code "trie-spill-before-publish"} and {@code "trie-spill-after-publish"} bracket the
 * publication of one spill batch, so the drop in pinned size between them is the pages it drained.
 * A batch can only be published inside a full epoch whose spill ran, which is what makes
 * {@link #spillBatches()} — and not {@link #rotations()} — the figure that proves a fixture really
 * exercised the spill.</li>
 * </ul>
 *
 * <p>
 * The hook is one static field that fault-injection tests also use, so the probe chains to whatever
 * hook it displaces and restores it on close. It never throws on its own account.
 */
public final class IntentLogEpochProbe implements WorkProbe {

  private static final String ROTATION_SITE = "prepare";

  private static final String BEFORE_PUBLISH_SITE = "trie-spill-before-publish";

  private static final String AFTER_PUBLISH_SITE = "trie-spill-after-publish";

  private final LongAdder rotations = new LongAdder();

  private final LongAdder spillBatches = new LongAdder();

  private final LongAdder spilledPages = new LongAdder();

  private final AtomicLong pinnedPagesPeak = new AtomicLong();

  /** A spill publishes on the thread that started it, so its before/after samples pair up here. */
  private final ThreadLocal<int[]> pinnedBeforePublish = ThreadLocal.withInitial(() -> new int[1]);

  private final WorkCounter rotationCounter = WorkCounter.alwaysOn("til.rotations",
      "one async-flush rotation of the load's storage engine, with or without the intent log", rotations::sum);

  private final WorkCounter spillBatchCounter = WorkCounter.alwaysOn("til.spillBatches",
      "one batch of pinned trie pages written and published ahead of the final commit", spillBatches::sum);

  private final WorkCounter spilledPageCounter = WorkCounter.alwaysOn("til.spilledPages",
      "one pinned trie page drained from the intent log ahead of the final commit", spilledPages::sum);

  private final WorkCounter pinnedPagesPeakCounter = WorkCounter.alwaysOn("til.pinnedPagesPeak",
      "the most pages any rotation found pinned; each holds an off-heap frame", pinnedPagesPeak::get);

  private final BiConsumer<NodeStorageEngineWriter, String> hook = this::observe;

  private volatile @Nullable BiConsumer<NodeStorageEngineWriter, String> displacedHook;

  private boolean open;

  /**
   * Async-flush rotations the load went through, side-pages-only ones included. Too coarse to prove a
   * fixture exercised the spill; use {@link #spillBatches()} for that.
   */
  public WorkCounter rotations() {
    return rotationCounter;
  }

  /** Spill batches published ahead of the final commit; each one ran inside a full epoch. */
  public WorkCounter spillBatches() {
    return spillBatchCounter;
  }

  /** Pinned pages those batches drained. */
  public WorkCounter spilledPages() {
    return spilledPageCounter;
  }

  /** The largest pinned region any rotation sampled; see the class javadoc on when it is taken. */
  public WorkCounter pinnedPagesPeak() {
    return pinnedPagesPeakCounter;
  }

  @Override
  public List<WorkCounter> counters() {
    return List.of(rotationCounter, spillBatchCounter, spilledPageCounter, pinnedPagesPeakCounter);
  }

  @Override
  public void open() {
    if (open) {
      throw new IllegalStateException("the intent-log probe is already open");
    }
    rotations.reset();
    spillBatches.reset();
    spilledPages.reset();
    pinnedPagesPeak.set(0);
    displacedHook = NodeStorageEngineWriter.asyncFlushFaultHook;
    NodeStorageEngineWriter.asyncFlushFaultHook = hook;
    open = true;
  }

  @Override
  public void close() {
    if (!open) {
      return;
    }
    open = false;
    NodeStorageEngineWriter.asyncFlushFaultHook = displacedHook;
    displacedHook = null;
  }

  private void observe(final NodeStorageEngineWriter writer, final String site) {
    // Only sites inside a running epoch are sampled: the close and rollback sites fire after the
    // intent log has been closed, where getLog() is unusable on purpose.
    if (ROTATION_SITE.equals(site)) {
      rotations.increment();
      final int pinned = writer.getLog().pinnedSize();
      pinnedPagesPeak.accumulateAndGet(pinned, Math::max);
    } else if (BEFORE_PUBLISH_SITE.equals(site)) {
      pinnedBeforePublish.get()[0] = writer.getLog().pinnedSize();
    } else if (AFTER_PUBLISH_SITE.equals(site)) {
      spillBatches.increment();
      spilledPages.add(Math.max(0, pinnedBeforePublish.get()[0] - writer.getLog().pinnedSize()));
    }
    final BiConsumer<NodeStorageEngineWriter, String> displaced = displacedHook;
    if (displaced != null) {
      displaced.accept(writer, site);
    }
  }
}
