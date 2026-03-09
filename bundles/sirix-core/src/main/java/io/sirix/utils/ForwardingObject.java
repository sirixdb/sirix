package io.sirix.utils;

/**
 * Lightweight replacement for {@code com.google.common.collect.ForwardingObject}.
 * An abstract base class for implementing the decorator pattern, where the {@link #delegate()}
 * method returns the backing object to which calls are forwarded.
 */
public abstract class ForwardingObject {

  /** Constructor for use by subclasses. */
  protected ForwardingObject() {
  }

  /**
   * Returns the backing delegate instance that methods should be forwarded to.
   *
   * @return the delegate instance
   */
  protected abstract Object delegate();

  @Override
  public String toString() {
    return delegate().toString();
  }
}
