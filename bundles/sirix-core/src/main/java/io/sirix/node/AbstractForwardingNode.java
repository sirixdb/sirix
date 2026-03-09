package io.sirix.node;

import io.sirix.utils.ForwardingObject;
import io.sirix.node.BytesOut;
import org.jspecify.annotations.Nullable;
import io.sirix.node.delegates.NodeDelegate;
import io.sirix.node.interfaces.Node;


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
  protected abstract NodeDelegate delegate();

  @Override
  public SirixDeweyID getDeweyID() {
    return delegate().getDeweyID();
  }

  /**
   * Get a snapshot of the node delegate.
   *
   * @return new {@link NodeDelegate} instance (snapshot of the current one)
   */
  public NodeDelegate getNodeDelegate() {
    return delegate();
  }

  @Override
  public long computeHash(BytesOut<?> bytes) {
    return delegate().computeHash(bytes);
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
  public void setNodeKey(final long nodeKey) {
    delegate().setNodeKey(nodeKey);
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
  public void setHash(final long hash) {
    delegate().setHash(hash);
  }

  @Override
  public long getHash() {
    return delegate().getHash();
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
  public String toString() {
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
