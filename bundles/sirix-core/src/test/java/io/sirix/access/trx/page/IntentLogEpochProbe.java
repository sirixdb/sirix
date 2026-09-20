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
 * Watches a load's transaction intent log, epoch by epoch: how many pages it left pinned, and how
 * many the pre-commit spill drained.
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
 * <li>{@code "prepare"} fires once per epoch, <em>after</em> the previous snapshot's cleanup has
 * promoted pages into the pinned region and the spill has drained what it could. The pinned size
 * there is the epoch's residue. A spill that runs keeps it bounded; one refused at its gate lets it
 * grow with every epoch until the arena is exhausted.</li>
 * <li>{@code "trie-spill-before-publish"} and {@code "trie-spill-after-publish"} bracket the
 * publication of one spill batch, so the drop in pinned size between them is the pages it
 * drained.</li>
 * </ul>
 *
 * <p>
 * The hook is one static field that fault-injection tests also use, so the probe chains to whatever
 * hook it displaces and restores it on close. It never throws on its own account.
 */
public final class IntentLogEpochProbe implements WorkProbe {

  private static final String EPOCH_SITE = "prepare";

  private static final String BEFORE_PUBLISH_SITE = "trie-spill-before-publish";

  private static final String AFTER_PUBLISH_SITE = "trie-spill-after-publish";

  private final LongAdder epochs = new LongAdder();

  private final LongAdder spillBatches = new LongAdder();

  private final LongAdder spilledPages = new LongAdder();

  private final AtomicLong pinnedPagesPeak = new AtomicLong();

  /** A spill publishes on the thread that started it, so its before/after samples pair up here. */
  private final ThreadLocal<int[]> pinnedBeforePublish = ThreadLocal.withInitial(() -> new int[1]);

  private final WorkCounter epochCounter =
      WorkCounter.alwaysOn("til.epochs", "one async-flush epoch of the load's transaction intent log", epochs::sum);

  private final WorkCounter spillBatchCounter = WorkCounter.alwaysOn("til.spillBatches",
      "one batch of pinned trie pages written and published ahead of the final commit", spillBatches::sum);

  private final WorkCounter spilledPageCounter = WorkCounter.alwaysOn("til.spilledPages",
      "one pinned trie page drained from the intent log ahead of the final commit", spilledPages::sum);

  private final WorkCounter pinnedPagesPeakCounter = WorkCounter.alwaysOn("til.pinnedPagesPeak",
      "the most pages any epoch left pinned after its spill; each holds an off-heap frame", pinnedPagesPeak::get);

  private final BiConsumer<NodeStorageEngineWriter, String> hook = this::observe;

  private volatile @Nullable BiConsumer<NodeStorageEngineWriter, String> displacedHook;

  private boolean open;

  /** Async-flush epochs the load went through; a budget over too few epochs proves nothing. */
  public WorkCounter epochs() {
    return epochCounter;
  }

  /** Spill batches published ahead of the final commit. */
  public WorkCounter spillBatches() {
    return spillBatchCounter;
  }

  /** Pinned pages those batches drained. */
  public WorkCounter spilledPages() {
    return spilledPageCounter;
  }

  /** The largest post-spill pinned region of any epoch. */
  public WorkCounter pinnedPagesPeak() {
    return pinnedPagesPeakCounter;
  }

  @Override
  public List<WorkCounter> counters() {
    return List.of(epochCounter, spillBatchCounter, spilledPageCounter, pinnedPagesPeakCounter);
  }

  @Override
  public void open() {
    if (open) {
      throw new IllegalStateException("the intent-log probe is already open");
    }
    epochs.reset();
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
    if (EPOCH_SITE.equals(site)) {
      epochs.increment();
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
