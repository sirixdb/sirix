package io.sirix.index.vector.hnsw;

import java.util.NoSuchElementException;

/**
 * Zero-allocation min-heap using parallel primitive arrays for (long key, float distance) pairs.
 * Ordered by distance ascending — the smallest distance is at the root.
 *
 * <p>Pre-allocates arrays at construction time. Grows by 2x if capacity is exceeded.
 * Designed for reuse via {@link #clear()}.
 */
public final class PrimitiveLongFloatMinHeap {

  private long[] keys;
  private float[] distances;
  private int size;

  /**
   * Creates a min-heap with the given initial capacity.
   *
   * @param initialCapacity initial capacity, must be positive
   */
  public PrimitiveLongFloatMinHeap(final int initialCapacity) {
    if (initialCapacity <= 0) {
      throw new IllegalArgumentException("Initial capacity must be positive: " + initialCapacity);
    }
    this.keys = new long[initialCapacity];
    this.distances = new float[initialCapacity];
    this.size = 0;
  }

  /**
   * Inserts a key-distance pair into the heap.
   *
   * @param key      the node key
   * @param distance the distance value
   */
  public void insert(final long key, final float distance) {
    if (size == keys.length) {
      grow();
    }
    keys[size] = key;
    distances[size] = distance;
    siftUp(size);
    size++;
  }

  /**
   * Returns the key at the root (minimum distance) without removing it.
   *
   * @return the key with the smallest distance
   * @throws NoSuchElementException if the heap is empty
   */
  public long peekKey() {
    if (size == 0) {
      throw new NoSuchElementException("Heap is empty");
    }
    return keys[0];
  }

  /**
   * Returns the distance at the root (minimum) without removing it.
   *
   * @return the smallest distance
   * @throws NoSuchElementException if the heap is empty
   */
  public float peekDistance() {
    if (size == 0) {
      throw new NoSuchElementException("Heap is empty");
    }
    return distances[0];
  }

  /**
   * Removes and returns the key at the root (minimum distance).
   *
   * @return the key with the smallest distance
   * @throws NoSuchElementException if the heap is empty
   */
  public long poll() {
    if (size == 0) {
      throw new NoSuchElementException("Heap is empty");
    }
    final long result = keys[0];
    size--;
    if (size > 0) {
      keys[0] = keys[size];
      distances[0] = distances[size];
      siftDown(0);
    }
    return result;
  }

  /**
   * Removes and returns the distance at the root (minimum distance).
   * Also removes the corresponding key.
   *
   * @return the smallest distance
   * @throws NoSuchElementException if the heap is empty
   */
  public float pollDistance() {
    if (size == 0) {
      throw new NoSuchElementException("Heap is empty");
    }
    final float result = distances[0];
    size--;
    if (size > 0) {
      keys[0] = keys[size];
      distances[0] = distances[size];
      siftDown(0);
    }
    return result;
  }

  /**
   * Returns the number of elements in the heap.
   */
  public int size() {
    return size;
  }

  /**
   * Returns true if the heap is empty.
   */
  public boolean isEmpty() {
    return size == 0;
  }

  /**
   * Clears the heap for reuse. Does not deallocate arrays.
   */
  public void clear() {
    size = 0;
  }

  /**
   * Returns keys sorted by distance ascending. Drains the heap.
   *
   * @return array of keys sorted from nearest to farthest
   */
  public long[] toSortedKeys() {
    final int n = size;
    final long[] result = new long[n];
    for (int i = 0; i < n; i++) {
      result[i] = poll();
    }
    return result;
  }

  private void siftUp(int index) {
    while (index > 0) {
      final int parent = (index - 1) >>> 1;
      if (distances[index] < distances[parent]) {
        swap(index, parent);
        index = parent;
      } else {
        break;
      }
    }
  }

  private void siftDown(int index) {
    final int halfSize = size >>> 1;
    while (index < halfSize) {
      int child = (index << 1) + 1;
      final int right = child + 1;
      if (right < size && distances[right] < distances[child]) {
        child = right;
      }
      if (distances[child] < distances[index]) {
        swap(index, child);
        index = child;
      } else {
        break;
      }
    }
  }

  private void swap(final int i, final int j) {
    final long tmpKey = keys[i];
    final float tmpDist = distances[i];
    keys[i] = keys[j];
    distances[i] = distances[j];
    keys[j] = tmpKey;
    distances[j] = tmpDist;
  }

  private void grow() {
    final int newCapacity = keys.length << 1;
    final long[] newKeys = new long[newCapacity];
    final float[] newDistances = new float[newCapacity];
    System.arraycopy(keys, 0, newKeys, 0, size);
    System.arraycopy(distances, 0, newDistances, 0, size);
    keys = newKeys;
    distances = newDistances;
  }
}
