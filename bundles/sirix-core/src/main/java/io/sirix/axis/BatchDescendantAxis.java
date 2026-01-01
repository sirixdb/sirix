/**
 * Copyright (c) 2025, SirixDB
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met:
 * - Redistributions of source code must retain the above copyright notice
 * - Redistributions in binary form must reproduce the above copyright notice
 *
 * THIS SOFTWARE IS PROVIDED "AS IS" WITHOUT WARRANTY OF ANY KIND.
 */
package io.sirix.axis;

import io.sirix.api.Filter;
import io.sirix.api.NodeCursor;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.settings.Fixed;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.checkerframework.checker.index.qual.NonNegative;

import java.util.function.LongConsumer;

import static java.util.Objects.requireNonNull;

/**
 * A preorder descendant traversal that can produce results in batches to reduce per-element iterator overhead.
 *
 * <p>This is intended for very hot loops where the caller wants to process node keys in chunks, e.g.:
 *
 * <pre>
 *   final var axis = new BatchDescendantAxis(rtx);
 *   final var batch = new LongArrayList(4096);
 *   while (axis.nextBatch(batch, 4096) > 0) {
 *     for (int i = 0; i < batch.size(); i++) {
 *       final long nodeKey = batch.getLong(i);
 *       // process...
 *     }
 *   }
 * </pre>
 *
 * <p>Semantics:
 * <ul>
 *   <li>Traversal order matches {@link DescendantAxis} (preorder DFS).</li>
 *   <li>On each returned key, this axis moves the underlying cursor to that key.</li>
 *   <li>When finished, {@link #nextBatch(LongArrayList, int)} returns {@code 0}.</li>
 * </ul>
 */
public final class BatchDescendantAxis {

  private final NodeCursor cursor;
  private final IncludeSelf includeSelf;
  private LongArrayList rightSiblingKeyStack;
  private boolean first;
  private boolean finished;
  private long startNodeKey;
  private long startNodeRightSiblingKey;

  /**
   * Create an axis starting at the cursor's current node (excluding self).
   *
   * @param cursor the cursor to traverse with
   */
  public BatchDescendantAxis(final NodeCursor cursor) {
    this(cursor, IncludeSelf.NO);
  }

  /**
   * Create an axis starting at the cursor's current node.
   *
   * @param cursor the cursor to traverse with
   * @param includeSelf whether to include the start node
   */
  public BatchDescendantAxis(final NodeCursor cursor, final IncludeSelf includeSelf) {
    this.cursor = requireNonNull(cursor);
    this.includeSelf = requireNonNull(includeSelf);
    reset(cursor.getNodeKey());
  }

  /**
   * Reset the axis to start traversal at {@code nodeKey}.
   *
   * @param nodeKey start node key
   */
  public void reset(@NonNegative final long nodeKey) {
    this.startNodeKey = nodeKey;
    this.first = true;
    this.finished = false;
    this.rightSiblingKeyStack = new LongArrayList();

    // Move to the start node (BatchDescendantAxis directly depends on cursor position).
    cursor.moveTo(nodeKey);
    this.startNodeRightSiblingKey = cursor.getRightSiblingKey();
  }

  /**
   * Fill {@code out} with up to {@code maxItems} next node keys in preorder.
   *
   * <p>{@code out} is cleared at the beginning of the call.
   *
   * @param out the output list to fill (reused by caller to avoid allocations)
   * @param maxItems maximum number of keys to add (must be &gt; 0)
   * @return number of keys added (0 if traversal finished)
   */
  public int nextBatch(final LongArrayList out, final int maxItems) {
    requireNonNull(out);
    if (maxItems <= 0) {
      throw new IllegalArgumentException("maxItems must be > 0");
    }

    out.clear();
    if (finished) {
      return 0;
    }

    int added = 0;
    while (added < maxItems) {
      final long nextKey = nextKey();
      if (nextKey == Fixed.NULL_NODE_KEY.getStandardProperty()) {
        break;
      }

      // Move cursor (keeps traversal state correct).
      if (!cursor.moveTo(nextKey)) {
        throw new IllegalStateException("Failed to move to nodeKey: " + nextKey);
      }

      out.add(nextKey);
      added++;
    }

    return added;
  }

  /**
   * Visit up to {@code maxItems} next nodes and call {@code consumer} for each node key.
   *
   * <p>This is the most allocation-friendly way to consume this axis:
   * the cursor is moved to each node and you can use flyweight getters on the cursor/transaction to
   * check structural properties cheaply, only materializing the node object when really needed.
   *
   * @param maxItems maximum number of nodes to visit (must be &gt; 0)
   * @param consumer callback invoked with each visited node key (cursor is positioned on it)
   * @return number of visited nodes (0 if traversal finished)
   */
  public int forEachNext(final int maxItems, final LongConsumer consumer) {
    if (maxItems <= 0) {
      throw new IllegalArgumentException("maxItems must be > 0");
    }
    requireNonNull(consumer);
    if (finished) {
      return 0;
    }

    int visited = 0;
    while (visited < maxItems) {
      final long nextKey = nextKey();
      if (nextKey == Fixed.NULL_NODE_KEY.getStandardProperty()) {
        break;
      }
      if (!cursor.moveTo(nextKey)) {
        throw new IllegalStateException("Failed to move to nodeKey: " + nextKey);
      }
      consumer.accept(nextKey);
      visited++;
    }
    return visited;
  }

  /**
   * Visit up to {@code maxItems} next nodes that match the given filters and call {@code consumer} for each match.
   *
   * <p>This reuses the existing {@link Filter} implementations (same contract as {@link io.sirix.axis.filter.FilterAxis})
   * without the per-item iterator overhead. Filters are evaluated while the cursor is already positioned on the
   * candidate node, so they can rely on flyweight getters and avoid node materialization.
   *
   * @param maxItems maximum number of matching nodes to visit (must be &gt; 0)
   * @param consumer callback invoked with each visited node key (cursor is positioned on it)
   * @param firstFilter first filter (required)
   * @param moreFilters additional filters (optional)
   * @param <R> the cursor type bound to the filters
   * @return number of matching nodes visited (0 if traversal finished)
   */
  @SafeVarargs
  public final <R extends NodeReadOnlyTrx & NodeCursor> int forEachNextFiltered(
      final int maxItems,
      final LongConsumer consumer,
      final Filter<R> firstFilter,
      final Filter<R>... moreFilters) {
    if (maxItems <= 0) {
      throw new IllegalArgumentException("maxItems must be > 0");
    }
    requireNonNull(consumer);
    requireNonNull(firstFilter);
    if (!cursor.equals(firstFilter.getTrx())) {
      throw new IllegalArgumentException("The filter must be bound to the same transaction as the axis!");
    }
    if (moreFilters != null) {
      for (final Filter<R> filter : moreFilters) {
        requireNonNull(filter);
        if (!cursor.equals(filter.getTrx())) {
          throw new IllegalArgumentException("The filter must be bound to the same transaction as the axis!");
        }
      }
    }
    if (finished) {
      return 0;
    }

    int matched = 0;
    while (matched < maxItems) {
      final long nextKey = nextKey();
      if (nextKey == Fixed.NULL_NODE_KEY.getStandardProperty()) {
        break;
      }
      if (!cursor.moveTo(nextKey)) {
        throw new IllegalStateException("Failed to move to nodeKey: " + nextKey);
      }

      boolean filterResult = firstFilter.filter();
      if (filterResult && moreFilters != null) {
        for (final Filter<R> filter : moreFilters) {
          filterResult = filter.filter();
          if (!filterResult) {
            break;
          }
        }
      }

      if (filterResult) {
        consumer.accept(nextKey);
        matched++;
      }
    }

    return matched;
  }

  /**
   * Compute the next node key (without moving the cursor).
   *
   * @return next node key, or {@link Fixed#NULL_NODE_KEY} when done
   */
  private long nextKey() {
    if (finished) {
      return Fixed.NULL_NODE_KEY.getStandardProperty();
    }

    // First element handling.
    if (first) {
      first = false;
      if (includeSelf == IncludeSelf.YES) {
        return cursor.getNodeKey();
      }
      final long firstChildKey = cursor.getFirstChildKey();
      return firstChildKey == Fixed.NULL_NODE_KEY.getStandardProperty() ? done() : firstChildKey;
    }

    // PERF: Avoid hasFirstChild()/hasRightSibling() to prevent redundant flyweight field reads.
    final long firstChildKey = cursor.getFirstChildKey();
    if (firstChildKey != Fixed.NULL_NODE_KEY.getStandardProperty()) {
      final long rightSiblingKey = cursor.getRightSiblingKey();
      if (rightSiblingKey != Fixed.NULL_NODE_KEY.getStandardProperty()) {
        rightSiblingKeyStack.add(rightSiblingKey);
      }
      return firstChildKey;
    }

    final long rightSiblingKey = cursor.getRightSiblingKey();
    if (rightSiblingKey != Fixed.NULL_NODE_KEY.getStandardProperty()) {
      return rightSiblingKey == startNodeRightSiblingKey ? done() : rightSiblingKey;
    }

    while (!rightSiblingKeyStack.isEmpty()) {
      final long key = rightSiblingKeyStack.popLong();
      return key == startNodeRightSiblingKey ? done() : key;
    }

    return done();
  }

  private long done() {
    // Mark finished and reset cursor to start node, like AbstractAxis does when hasNext() becomes false.
    finished = true;
    cursor.moveTo(startNodeKey);
    return Fixed.NULL_NODE_KEY.getStandardProperty();
  }
}


