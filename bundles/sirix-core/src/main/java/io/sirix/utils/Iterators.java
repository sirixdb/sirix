package io.sirix.utils;

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Lightweight replacement for selected methods from {@code com.google.common.collect.Iterators}.
 * All operations are lazy and zero-allocation beyond the iterator object itself.
 *
 * <p>Thread-safety: not thread-safe (single-threaded usage only).
 */
public final class Iterators {

  private Iterators() {
    // utility class
  }

  /**
   * Returns an iterator over the given elements. The returned iterator does not support
   * {@link Iterator#remove()}.
   *
   * @param elements the elements to iterate over
   * @param <T>      the element type
   * @return an unmodifiable iterator over the given elements
   */
  @SafeVarargs
  public static <T> Iterator<T> forArray(final T... elements) {
    if (elements.length == 0) {
      return Collections.emptyIterator();
    }
    return new ArrayIterator<>(elements);
  }

  /**
   * Returns an unmodifiable view of the given iterator. The returned iterator throws
   * {@link UnsupportedOperationException} on {@link Iterator#remove()}.
   *
   * @param iterator the iterator to wrap
   * @param <T>      the element type
   * @return an unmodifiable iterator
   */
  public static <T> Iterator<T> unmodifiableIterator(final Iterator<? extends T> iterator) {
    @SuppressWarnings("unchecked")
    final Iterator<T> typedIterator = (Iterator<T>) iterator;
    // If already an unmodifiable type we control, return as-is
    if (iterator instanceof ArrayIterator || iterator instanceof ConcatIterator) {
      return typedIterator;
    }
    return new UnmodifiableIterator<>(typedIterator);
  }

  /**
   * Concatenates two iterators into a single iterator. Elements from {@code a} are returned first,
   * followed by elements from {@code b}. Evaluation is lazy.
   *
   * @param a   the first iterator
   * @param b   the second iterator
   * @param <T> the element type
   * @return a concatenated iterator
   */
  public static <T> Iterator<T> concat(final Iterator<? extends T> a, final Iterator<? extends T> b) {
    return new ConcatTwoIterator<>(a, b);
  }

  /**
   * Concatenates multiple iterators into a single iterator, consuming them lazily in order.
   *
   * @param iterators an iterator of iterators
   * @param <T>       the element type
   * @return a concatenated iterator
   */
  public static <T> Iterator<T> concat(final Iterator<? extends Iterator<? extends T>> iterators) {
    return new ConcatIterator<>(iterators);
  }

  // ---- Internal iterator implementations ----

  private static final class ArrayIterator<T> implements Iterator<T> {
    private final T[] elements;
    private int index;

    ArrayIterator(final T[] elements) {
      this.elements = elements;
      this.index = 0;
    }

    @Override
    public boolean hasNext() {
      return index < elements.length;
    }

    @Override
    public T next() {
      if (index >= elements.length) {
        throw new NoSuchElementException();
      }
      return elements[index++];
    }
  }

  private static final class UnmodifiableIterator<T> implements Iterator<T> {
    private final Iterator<T> delegate;

    UnmodifiableIterator(final Iterator<T> delegate) {
      this.delegate = delegate;
    }

    @Override
    public boolean hasNext() {
      return delegate.hasNext();
    }

    @Override
    public T next() {
      return delegate.next();
    }
  }

  private static final class ConcatTwoIterator<T> implements Iterator<T> {
    private final Iterator<? extends T> a;
    private final Iterator<? extends T> b;

    ConcatTwoIterator(final Iterator<? extends T> a, final Iterator<? extends T> b) {
      this.a = a;
      this.b = b;
    }

    @Override
    public boolean hasNext() {
      return a.hasNext() || b.hasNext();
    }

    @Override
    public T next() {
      if (a.hasNext()) {
        return a.next();
      }
      if (b.hasNext()) {
        return b.next();
      }
      throw new NoSuchElementException();
    }
  }

  private static final class ConcatIterator<T> implements Iterator<T> {
    private final Iterator<? extends Iterator<? extends T>> metaIterator;
    private Iterator<? extends T> current;

    ConcatIterator(final Iterator<? extends Iterator<? extends T>> metaIterator) {
      this.metaIterator = metaIterator;
      this.current = Collections.emptyIterator();
    }

    @Override
    public boolean hasNext() {
      while (!current.hasNext()) {
        if (!metaIterator.hasNext()) {
          return false;
        }
        current = metaIterator.next();
      }
      return true;
    }

    @Override
    public T next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      return current.next();
    }
  }
}
