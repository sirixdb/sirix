package org.sirix.utils;

import java.util.Iterator;

/**
 * This class is used to iterate over the elements of an array, or parts of it.
 * {@code null} references are skipped. An iterator cannot be used twice.
 *
 * @param <E> generic object type
 * @author BaseX Team 2005-22, BSD License
 * @author Christian Gruen
 */
public final class ArrayIterator<E> implements Iterator<E>, Iterable<E> {
  /**
   * Array.
   */
  private final Object[] array;
  /**
   * Index + 1 of last object to return.
   */
  private final int end;
  /**
   * Current index.
   */
  private int start;

  /**
   * Constructor.
   *
   * @param array array to iterate through
   */
  public ArrayIterator(final Object[] array) {
    this(array, 0, array.length);
  }

  /**
   * Constructor.
   *
   * @param array array to iterate through
   * @param end   index + 1 of last object to return
   */
  public ArrayIterator(final Object[] array, final int end) {
    this(array, 0, end);
  }

  /**
   * Constructor.
   *
   * @param array array to iterate through
   * @param start index of first object to return
   * @param end   index + 1 of last object to return
   */
  public ArrayIterator(final Object[] array, final int start, final int end) {
    this.array = array;
    this.start = start;
    this.end = end;
  }

  @Override
  public Iterator<E> iterator() {
    return this;
  }

  @Override
  public boolean hasNext() {
    while (start < end) {
      if (array[start] != null)
        return true;
      ++start;
    }
    return false;
  }

  @Override
  @SuppressWarnings("unchecked")
  public E next() {
    return (E) array[start++];
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }
}
