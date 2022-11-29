package org.sirix.axis.pathsummary;

import com.google.common.base.MoreObjects;
import org.sirix.axis.IncludeSelf;
import org.sirix.index.path.summary.PathNode;

import java.util.Iterator;
import java.util.NoSuchElementException;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Provide standard Java iterator capability compatible with the enhanced for loop available since Java 5. Override the
 * "template method" {@code nextNode()} to implement an axis. Return {@code done()} if the axis has no more "elements".
 *
 * @author Johannes Lichtenberger
 */
public abstract class AbstractAxis implements Iterator<PathNode>, Iterable<PathNode> {

  /**
   * Key of next node.
   */
  protected PathNode nextNode;

  /**
   * Node where axis started.
   */
  protected PathNode startPathNode;

  /**
   * Include self?
   */
  private final IncludeSelf includeSelf;

  /**
   * Current state.
   */
  private State state = State.NOT_READY;

  /**
   * State of the iterator.
   */
  private enum State {
    /**
     * We have computed the next element and haven't returned it yet.
     */
    READY,

    /**
     * We haven't yet computed or have already returned the element.
     */
    NOT_READY,

    /**
     * We have reached the end of the data and are finished.
     */
    DONE,

    /**
     * We've suffered an exception and are kaput.
     */
    FAILED,
  }

  /**
   * Constructor.
   *
   * @param pathNode the context node
   * @throws NullPointerException if {@code nodeCursor} is {@code null}
   */
  public AbstractAxis(final PathNode pathNode) {
    this.startPathNode = checkNotNull(pathNode);
    this.nextNode = pathNode;
    includeSelf = IncludeSelf.NO;
    reset(startPathNode);
  }

  /**
   * Constructor.
   *
   * @param pathNode    the context node
   * @param includeSelf determines if self is included
   * @throws NullPointerException if {@code nodeCursor} or {@code includeSelf} is {@code null}
   */
  public AbstractAxis(final PathNode pathNode, final IncludeSelf includeSelf) {
    startPathNode = checkNotNull(pathNode);
    this.includeSelf = checkNotNull(includeSelf);
    reset(startPathNode);
  }

  @Override
  public final Iterator<PathNode> iterator() {
    return this;
  }

  /**
   * Signals that axis traversal is done, that is {@code hasNext()} must return false. Is callable
   * from subclasses which implement {@link #nextNode()} to signal that the axis-traversal is done and
   * {@link #hasNext()} must return false.
   *
   * @return {@code null} to indicate that the travesal is done
   */
  protected PathNode done() {
    return null;
  }

  /**
   * {@inheritDoc}
   *
   * <p>
   * During the last call to {@code hasNext()}, that is {@code hasNext()} returns false, the
   * transaction is reset to the start key.
   * </p>
   *
   * <p>
   * <strong>Implementors must implement {@code nextKey()} instead which is a template method called
   * from this {@code hasNext()} method.</strong>
   * </p>
   */
  @Override
  public final boolean hasNext() {
    // First check the state.
    checkState(state != State.FAILED);
    switch (state) {
      case DONE:
        return false;
      case READY:
        return true;
      case FAILED:
      case NOT_READY:
      default:
    }

    final boolean hasNext = tryToComputeNext();
    if (hasNext) {
      return true;
    } else {
      // Reset to the start key before invoking the axis.
      reset(startPathNode);
      return false;
    }
  }

  /**
   * Try to compute the next node key.
   *
   * @return {@code true} if next node key exists, {@code false} otherwise
   */
  private boolean tryToComputeNext() {
    state = State.FAILED; // temporary pessimism
    // Template method.
    nextNode = nextNode();
    if (nextNode == null) {
      state = State.DONE;
    }
    if (state == State.DONE) {
      return false;
    }
    state = State.READY;
    return true;
  }

  /**
   * Returns the next node key. <strong>Note:</strong> the implementation must either call
   * {@link #done()} when there are no elements left in the iteration or return the node key
   * {@code EFixed.NULL_NODE.getStandardProperty()}.
   *
   * <p>
   * The initial invocation of {@link #hasNext()} or {@link #next()} calls this method, as does the
   * first invocation of {@code hasNext} or {@code next} following each successful call to
   * {@code next}. Once the implementation either invokes {@link #done()}, returns
   * {@code EFixed.NULL_NODE.getStandardProperty()} or throws an exception, {@code nextKey()} is
   * guaranteed to never be called again.
   * </p>
   *
   * <p>
   * If this method throws an exception, it will propagate outward to the {@code hasNext} or
   * {@code next} invocation that invoked this method. Any further attempts to use the iterator will
   * result in an {@link IllegalStateException}.
   * </p>
   *
   * <p>
   * The implementation of this method may not invoke the {@code hasNext}, {@code next}, or
   * {@link #peek()} methods on this instance; if it does, an {@code IllegalStateException} will
   * result.
   * </p>
   *
   * @return the next node
   * @throws RuntimeException if any unrecoverable error happens. This exception will propagate
   *                          outward to the {@code hasNext()}, {@code next()}, or {@code peek()} invocation that
   *                          invoked this method. Any further attempts to use the iterator will result in an
   *                          {@link IllegalStateException}.
   */
  protected abstract PathNode nextNode();

  @Override
  public final PathNode next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    state = State.NOT_READY;

    // Retrieve next node.
    return nextNode;
  }

  /**
   * Remove is not supported.
   */
  @Override
  public final void remove() {
    throw new UnsupportedOperationException();
  }

  public void reset(final PathNode pathNode) {
    startPathNode = pathNode;
    nextNode = pathNode;
    state = State.NOT_READY;
  }

  public final PathNode peek() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    return nextNode;
  }

  public final IncludeSelf includeSelf() {
    return includeSelf;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("pathNode", nextNode).toString();
  }
}
