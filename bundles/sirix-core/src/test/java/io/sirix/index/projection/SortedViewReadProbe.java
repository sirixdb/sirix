/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.budget.WorkCounter;
import io.sirix.budget.WorkProbe;
import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.IntConsumer;

/**
 * Counts what a query's reader fetches from a sorted view, which is the only way to tell its two
 * walks apart.
 *
 * <p>
 * A grouped top-K over a sorted view first walks the range's <em>group summaries</em>, one per data
 * leaf. Only if that declines does it seek the range again and walk the <em>data leaves</em>
 * themselves. No served-route counter separates "declined after one walk" from "declined after
 * two": both leave {@code sortedGroupBys} unchanged. The engine exposes the two reads through
 * observers instead, and a data-leaf read of zero is exactly "the full-key walk never started".
 * Directory nodes and leaf bounds bypass both observers.
 *
 * <p>
 * The observers fire on whichever thread fetches, including the summary lanes' workers, so the
 * counts are striped.
 */
public final class SortedViewReadProbe implements WorkProbe {

  private final LongAdder summaryReads = new LongAdder();

  private final LongAdder dataLeafReads = new LongAdder();

  private final WorkCounter summaryReadCounter = WorkCounter.alwaysOn("sortedView.summaryReads",
      "one group summary fetched by a query: a leaf of the range visited by the summaries walk", summaryReads::sum);

  private final WorkCounter dataLeafReadCounter = WorkCounter.alwaysOn("sortedView.dataLeafReads",
      "one sorted data leaf fetched by a query: a leaf visited by the full-key walk", dataLeafReads::sum);

  private final IntConsumer onSummaryRead = this::countSummaryRead;

  private final IntConsumer onDataLeafRead = this::countDataLeafRead;

  /** Read by the fetching threads, which may already see this probe's observer installed. */
  private volatile @Nullable IntConsumer displacedSummaryObserver;

  private volatile @Nullable IntConsumer displacedDataLeafObserver;

  private boolean open;

  /** Group summaries fetched: the work of the summaries walk. */
  public WorkCounter summaryReads() {
    return summaryReadCounter;
  }

  /** Sorted data leaves fetched: the work of the full-key walk, zero when it never started. */
  public WorkCounter dataLeafReads() {
    return dataLeafReadCounter;
  }

  @Override
  public List<WorkCounter> counters() {
    return List.of(summaryReadCounter, dataLeafReadCounter);
  }

  @Override
  public void open() {
    if (open) {
      throw new IllegalStateException("the sorted-view probe is already open");
    }
    summaryReads.reset();
    dataLeafReads.reset();
    displacedSummaryObserver = ProjectionSortedGroupSummary.setReadObserverForTesting(onSummaryRead);
    displacedDataLeafObserver = ProjectionSortedLeafStore.setQueryLeafReadObserverForTesting(onDataLeafRead);
    open = true;
  }

  @Override
  public void close() {
    if (!open) {
      return;
    }
    open = false;
    ProjectionSortedLeafStore.setQueryLeafReadObserverForTesting(displacedDataLeafObserver);
    ProjectionSortedGroupSummary.setReadObserverForTesting(displacedSummaryObserver);
    displacedDataLeafObserver = null;
    displacedSummaryObserver = null;
  }

  private void countSummaryRead(final int leafId) {
    summaryReads.increment();
    final IntConsumer displaced = displacedSummaryObserver;
    if (displaced != null) {
      displaced.accept(leafId);
    }
  }

  private void countDataLeafRead(final int leafId) {
    dataLeafReads.increment();
    final IntConsumer displaced = displacedDataLeafObserver;
    if (displaced != null) {
      displaced.accept(leafId);
    }
  }
}
