/*
 * Copyright 2012 by Thomas Mauch (original brownies-collections GapList)
 * Copyright 2026 by Johannes Lichtenberger (stripped-down, HFT-optimized adaptation for Sirix)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Derived from org.magicwerk.brownies.collections.GapList, adapted for the Sirix
 * database system. This version retains only the subset of operations used by Sirix
 * (construct, add-at-index, get, set, clear, size, iteration) and is optimized for
 * HFT-grade performance: zero unnecessary allocations, cache-friendly linear layout,
 * power-of-2 growth, and minimal branching in hot paths.
 */
package io.sirix.utils;

import java.util.AbstractList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.RandomAccess;

/**
 * A gap-buffer-backed list that amortizes insertions at arbitrary positions.
 *
 * <p>Maintains a single contiguous gap region inside a linear {@code Object[]} array.
 * Insertions near the gap require no data movement; insertions elsewhere relocate
 * the gap first. Random access ({@link #get}, {@link #set}) is O(1) with a single
 * branch. Iteration is sequential over two contiguous array segments.</p>
 *
 * <p><strong>Not synchronized.</strong> Iterators are not fail-fast.</p>
 *
 * @param <E> element type
 */
public final class GapList<E> extends AbstractList<E> implements RandomAccess {

  /** Minimum capacity to avoid frequent tiny re-allocations. */
  private static final int MIN_CAPACITY = 8;

  /** Backing store. Elements occupy [0..gapStart) and [gapEnd..capacity). */
  private Object[] values;

  /** Logical size (number of elements, excludes gap). */
  private int size;

  /**
   * Physical index where the gap begins (first unused slot).
   * Invariant: 0 <= gapStart <= gapEnd <= values.length
   */
  private int gapStart;

  /**
   * Physical index one past the last unused slot of the gap.
   * gapEnd - gapStart == gap size.
   */
  private int gapEnd;

  /**
   * Construct a gap list with the specified initial capacity.
   *
   * @param capacity desired initial capacity (will be rounded up to at least {@link #MIN_CAPACITY})
   */
  public GapList(final int capacity) {
    final int cap = Math.max(tableSizeFor(capacity), MIN_CAPACITY);
    values = new Object[cap];
    size = 0;
    gapStart = 0;
    gapEnd = cap;
  }

  // ---------------------------------------------------------------------------
  // AbstractList contract
  // ---------------------------------------------------------------------------

  @Override
  public int size() {
    return size;
  }

  @SuppressWarnings("unchecked")
  @Override
  public E get(final int index) {
    if (index < 0 || index >= size) {
      throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size);
    }
    return (E) values[toPhysical(index)];
  }

  @SuppressWarnings("unchecked")
  @Override
  public E set(final int index, final E element) {
    if (index < 0 || index >= size) {
      throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size);
    }
    final int phys = toPhysical(index);
    final E old = (E) values[phys];
    values[phys] = element;
    return old;
  }

  @Override
  public void add(final int index, final E element) {
    if (index < 0 || index > size) {
      throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size);
    }
    ensureCapacityInternal(size + 1);
    moveGapTo(index);
    values[gapStart] = element;
    gapStart++;
    size++;
  }

  @Override
  public boolean add(final E element) {
    ensureCapacityInternal(size + 1);
    moveGapTo(size);
    values[gapStart] = element;
    gapStart++;
    size++;
    return true;
  }

  @SuppressWarnings("unchecked")
  @Override
  public E remove(final int index) {
    if (index < 0 || index >= size) {
      throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size);
    }
    moveGapTo(index);
    // Element to remove is now at gapEnd (the first element after the gap).
    final E removed = (E) values[gapEnd];
    values[gapEnd] = null; // allow GC
    gapEnd++;
    size--;
    return removed;
  }

  @Override
  public void clear() {
    // Null out for GC
    Arrays.fill(values, 0, gapStart, null);
    Arrays.fill(values, gapEnd, values.length, null);
    size = 0;
    gapStart = 0;
    gapEnd = values.length;
  }

  // ---------------------------------------------------------------------------
  // Optimized iterator — avoids AbstractList's range-checking ListIterator
  // ---------------------------------------------------------------------------

  @Override
  public Iterator<E> iterator() {
    return new GapListIterator();
  }

  private final class GapListIterator implements Iterator<E> {
    /** Current physical position in the backing array. */
    private int pos;
    /** Number of elements still to deliver. */
    private int remaining;

    GapListIterator() {
      pos = 0;
      remaining = size;
    }

    @Override
    public boolean hasNext() {
      return remaining > 0;
    }

    @SuppressWarnings("unchecked")
    @Override
    public E next() {
      if (remaining <= 0) {
        throw new NoSuchElementException();
      }
      // Skip over the gap
      if (pos == gapStart) {
        pos = gapEnd;
      }
      final E val = (E) values[pos];
      pos++;
      remaining--;
      return val;
    }
  }

  // ---------------------------------------------------------------------------
  // Internal helpers
  // ---------------------------------------------------------------------------

  /**
   * Map a logical index to a physical index in the backing array.
   * Elements before the gap are at the same physical index; elements at or
   * after the gap are shifted right by the gap size.
   */
  private int toPhysical(final int logicalIndex) {
    return logicalIndex < gapStart ? logicalIndex : logicalIndex + (gapEnd - gapStart);
  }

  /**
   * Move the gap so that it starts at the given logical index.
   * Uses {@link System#arraycopy} for bulk moves.
   *
   * <p>When moving left (index < gapStart), elements [index..gapStart) are
   * shifted right by gapSize, vacating [index..index+gapSize). When moving
   * right (index > gapStart), elements [gapEnd..gapEnd+count) are shifted
   * left by gapSize, vacating [newGapStart..newGapEnd).</p>
   */
  private void moveGapTo(final int index) {
    if (index == gapStart) {
      return; // already there
    }
    final int gap = gapEnd - gapStart;
    if (index < gapStart) {
      // Shift elements [index..gapStart) right by gap positions
      final int count = gapStart - index;
      System.arraycopy(values, index, values, index + gap, count);
      // Null the vacated region [index .. index + min(gap, count))
      // Since gap + count == old gapEnd - index and count == gapStart - index,
      // the vacated region is [index .. min(index+gap, gapStart))
      Arrays.fill(values, index, index + Math.min(gap, count), null);
    } else {
      // Shift elements [gapEnd .. gapEnd+count) left by gap positions
      final int count = index - gapStart;
      System.arraycopy(values, gapEnd, values, gapStart, count);
      // Null the vacated region: [max(gapStart+count, gapEnd) .. gapEnd+count)
      final int newGapEnd = gapEnd + count;
      Arrays.fill(values, Math.max(gapStart + count, gapEnd), newGapEnd, null);
    }
    gapEnd = index + gap;
    gapStart = index;
  }

  /**
   * Ensure the backing array can hold at least {@code minCapacity} elements.
   * Growth uses power-of-2 doubling to stay cache-line-friendly and avoid
   * repeated re-allocations.
   */
  private void ensureCapacityInternal(final int minCapacity) {
    final int gapSize = gapEnd - gapStart;
    if (gapSize > 0) {
      return; // gap has room
    }
    // No gap — need to grow
    final int oldCapacity = values.length;
    int newCapacity = oldCapacity << 1;
    if (newCapacity < minCapacity) {
      newCapacity = tableSizeFor(minCapacity);
    }
    if (newCapacity < MIN_CAPACITY) {
      newCapacity = MIN_CAPACITY;
    }
    final Object[] newValues = new Object[newCapacity];
    // Copy elements before gap position (which equals gapStart == gapEnd here)
    if (gapStart > 0) {
      System.arraycopy(values, 0, newValues, 0, gapStart);
    }
    // Copy elements after gap position
    final int tailCount = oldCapacity - gapStart;
    if (tailCount > 0) {
      final int newGapEnd = newCapacity - tailCount;
      System.arraycopy(values, gapStart, newValues, newGapEnd, tailCount);
      gapEnd = newGapEnd;
    } else {
      gapEnd = newCapacity;
    }
    values = newValues;
  }

  /**
   * Round up to the next power of 2 (minimum 1).
   * Identical to {@code HashMap.tableSizeFor}.
   */
  private static int tableSizeFor(final int cap) {
    int n = cap - 1;
    n |= n >>> 1;
    n |= n >>> 2;
    n |= n >>> 4;
    n |= n >>> 8;
    n |= n >>> 16;
    return n < 0 ? 1 : n + 1;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append('[');
    boolean first = true;
    for (int i = 0; i < gapStart; i++) {
      if (!first) {
        sb.append(", ");
      }
      sb.append(values[i]);
      first = false;
    }
    for (int i = gapEnd; i < values.length; i++) {
      if (!first) {
        sb.append(", ");
      }
      sb.append(values[i]);
      first = false;
    }
    sb.append(']');
    return sb.toString();
  }
}
