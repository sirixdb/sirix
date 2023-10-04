package io.sirix.utils;

import java.util.Arrays;

/**
 * This is the basic structure of an efficient and memory-saving hash set.
 * The first entry of the token set (offset 0) will always be kept empty.
 *
 * @author BaseX Team 2005-22, BSD License
 * @author Christian Gruen
 */
public abstract class ASet {
  /**
   * Hash table buckets.
   */
  protected int[] buckets;
  /**
   * Pointers to the next entry.
   */
  protected int[] next;
  /**
   * Hash entries. The actual number of entries is {@code size - 1}.
   */
  protected int size = 1;

  /**
   * Empty constructor.
   */
  protected ASet() {
  }

  /**
   * Constructor with initial capacity.
   *
   * @param capacity array capacity (will be resized to a power of two)
   */
  protected ASet(final long capacity) {
    long c = 1;
    while (c < capacity)
      c <<= 1;
    final int s = Array.checkCapacity(c);
    buckets = new int[s];
    next = new int[s];
  }

  /**
   * Resets the data structure.
   */
  protected void clear() {
    Arrays.fill(buckets, 0);
    size = 1;
  }

  /**
   * Returns the number of entries.
   * The actual number of keys may be smaller if keys have been deleted.
   *
   * @return number of entries
   */
  public final int size() {
    return size - 1;
  }

  /**
   * Tests is the set is empty.
   *
   * @return result of check
   */
  public final boolean isEmpty() {
    return size == 1;
  }

  /**
   * Resizes the hash table.
   */
  protected final void checkSize() {
    if (size < capacity())
      return;

    final int newSize = size << 1;
    final int[] tmp = new int[newSize];

    for (final int b : buckets) {
      int id = b;
      while (id != 0) {
        final int p = hash(id) & newSize - 1;
        final int nx = next[id];
        next[id] = tmp[p];
        tmp[p] = id;
        id = nx;
      }
    }
    buckets = tmp;
    next = Arrays.copyOf(next, newSize);
    rehash(newSize);
  }

  /**
   * Returns the current array capacity.
   *
   * @return array capacity
   */
  protected final int capacity() {
    return next.length;
  }

  /**
   * Returns the hash value of the element with the specified id.
   *
   * @param id id of the element
   * @return hash value
   */
  protected abstract int hash(int id);

  /**
   * Rehashes all entries.
   *
   * @param newSize new hash size
   */
  protected abstract void rehash(int newSize);

  /**
   * Returns a string representation of the set or map.
   *
   * @param keys hash keys
   * @return string
   */
  public String toString(final Object[] keys) {
    return toString(keys, null);
  }

  /**
   * Returns a string representation of the set or map.
   *
   * @param keys   hash keys
   * @param values hash values or {@code null}
   * @return string
   */
  public String toString(final Object[] keys, final Object[] values) {
    final StringBuilder sb = new StringBuilder().append(this.getClass().toString()).append('[');
    boolean more = false;
    for (int i = 1; i < size; i++) {
      final Object key = keys[i];
      if (key == null)
        continue;
      if (more)
        sb.append(',');
      sb.append(key);
      if (values != null)
        sb.append('=').append(values[i]);
      more = true;
    }
    return sb.append(']').toString();
  }
}