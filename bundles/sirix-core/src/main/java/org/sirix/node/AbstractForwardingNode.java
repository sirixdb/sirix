package org.sirix.node;

import com.google.common.collect.ForwardingObject;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.interfaces.Node;

import java.math.BigInteger;

/**
 * Skeletal implementation of {@link Node} interface.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 *
 */
public abstract class AbstractForwardingNode extends ForwardingObject implements Node {

  /** Constructor for use by subclasses. */
  protected AbstractForwardingNode() {}

  @Override
  protected abstract @NonNull NodeDelegate delegate();

  @Override
  public SirixDeweyID getDeweyID() {
    return delegate().getDeweyID();
  }

  /**
   * Get a snapshot of the node delegate.
   *
   * @return new {@link NodeDelegate} instance (snapshot of the current one)
   */
  @NonNull
  public NodeDelegate getNodeDelegate() {
    return delegate();
  }

  @Override
  public BigInteger computeHash() {
    return delegate().computeHash();
  }

  @Override
  public void setTypeKey(final int typeKey) {
    delegate().setTypeKey(typeKey);
  }

  @Override
  public boolean hasParent() {
    return delegate().hasParent();
  }

  @Override
  public long getNodeKey() {
    return delegate().getNodeKey();
  }

  @Override
  public long getParentKey() {
    return delegate().getParentKey();
  }

  @Override
  public void setParentKey(final long parentKey) {
    delegate().setParentKey(parentKey);
  }

  @Override
  public BigInteger getHash() {
    return delegate().getHash();
  }

  @Override
  public void setHash(final BigInteger hash) {
    delegate().setHash(hash);
  }

  @Override
  public int getPreviousRevisionNumber() {
    return delegate().getPreviousRevisionNumber();
  }

  @Override
  public void setPreviousRevision(int revision) {
    delegate().setPreviousRevision(revision);
  }

  @Override
  public @NonNull String toString() {
    return delegate().toString();
  }

  @Override
  public boolean isSameItem(final @Nullable Node other) {
    return delegate().isSameItem(other);
  }

  @Override
  public void setDeweyID(SirixDeweyID id) {
    delegate().setDeweyID(id);
  }

  @Override
  public byte[] getDeweyIDAsBytes() {
    return delegate().getDeweyIDAsBytes();
  }

  @Override
  public int getLastModifiedRevisionNumber() {
    return delegate().getLastModifiedRevisionNumber();
  }

  @Override
  public void setLastModifiedRevision(int revision) {
    delegate().setLastModifiedRevision(revision);
  }
}
