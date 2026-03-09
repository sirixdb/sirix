package io.sirix.utils;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Lightweight replacement for {@code com.google.common.collect.AbstractIterator}.
 * Subclasses override {@link #computeNext()} and call {@link #endOfData()} to signal exhaustion.
 *
 * <p>State machine uses an enum to avoid object allocation.
 *
 * <p>Thread-safety: not thread-safe (single-threaded usage only).
 *
 * @param <T> the element type
 */
public abstract class AbstractComputingIterator<T> implements Iterator<T> {

  private enum State {
    /** Next element has not been computed yet. */
    NOT_READY,
    /** Next element has been computed and is available via {@code next}. */
    READY,
    /** Iterator is exhausted. */
    DONE,
    /** computeNext() failed; iterator is broken. */
    FAILED
  }

  private State state = State.NOT_READY;
  private T next;

  /** Constructor for use by subclasses. */
  protected AbstractComputingIterator() {
  }

  /**
   * Computes the next element. Implementations must return the next value, or call
   * {@link #endOfData()} and return its result to signal that the iterator is exhausted.
   *
   * @return the next element, or the result of {@link #endOfData()} if no more elements
   */
  protected abstract T computeNext();

  /**
   * Signals that the iterator has no more elements. Must be called from within
   * {@link #computeNext()} to indicate exhaustion.
   *
   * @return always {@code null} (convenience for {@code return endOfData();} pattern)
   */
  protected final T endOfData() {
    state = State.DONE;
    return null;
  }

  @Override
  public final boolean hasNext() {
    switch (state) {
      case READY:
        return true;
      case DONE:
        return false;
      case FAILED:
        // fall through to NOT_READY
      default:
        break;
    }
    return tryToComputeNext();
  }

  @Override
  public final T next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    state = State.NOT_READY;
    final T result = next;
    next = null; // allow GC of the element
    return result;
  }

  /**
   * Peek at the next element without advancing the iterator. Only valid when {@link #hasNext()}
   * has returned {@code true}.
   *
   * @return the next element
   * @throws NoSuchElementException if the iterator is exhausted
   */
  public final T peek() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    return next;
  }

  private boolean tryToComputeNext() {
    state = State.FAILED; // in case computeNext() throws
    next = computeNext();
    if (state != State.DONE) {
      state = State.READY;
      return true;
    }
    return false;
  }
}
