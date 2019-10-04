package org.sirix.node.immutable.json;

import org.sirix.node.interfaces.Node;
import org.sirix.node.interfaces.StructNode;
import org.sirix.node.interfaces.immutable.ImmutableJsonNode;
import org.sirix.node.interfaces.immutable.ImmutableStructNode;

import javax.annotation.Nullable;
import java.math.BigInteger;

public abstract class AbstractImmutableJsonStructuralNode implements ImmutableStructNode, ImmutableJsonNode {

  public abstract StructNode structDelegate();

  @Override
  public boolean hasFirstChild() {
    return structDelegate().hasFirstChild();
  }

  @Override
  public boolean hasLeftSibling() {
    return structDelegate().hasLeftSibling();
  }

  @Override
  public boolean hasRightSibling() {
    return structDelegate().hasRightSibling();
  }

  @Override
  public long getChildCount() {
    return structDelegate().getChildCount();
  }

  @Override
  public long getDescendantCount() {
    return structDelegate().getDescendantCount();
  }

  @Override
  public long getFirstChildKey() {
    return structDelegate().getFirstChildKey();
  }

  @Override
  public long getLeftSiblingKey() {
    return structDelegate().getLeftSiblingKey();
  }

  @Override
  public long getRightSiblingKey() {
    return structDelegate().getRightSiblingKey();
  }

  @Override
  public boolean isSameItem(final @Nullable Node other) {
    return structDelegate().isSameItem(other);
  }

  @Override
  public BigInteger getHash() {
    return structDelegate().getHash();
  }

  @Override
  public long getParentKey() {
    return structDelegate().getParentKey();
  }

  @Override
  public boolean hasParent() {
    return structDelegate().hasParent();
  }

  @Override
  public long getNodeKey() {
    return structDelegate().getNodeKey();
  }

  @Override
  public long getRevision() {
    return structDelegate().getRevision();
  }

  @Override
  public boolean equals(Object obj) {
    return structDelegate().equals(obj);
  }

  @Override
  public int hashCode() {
    return structDelegate().hashCode();
  }

  @Override
  public String toString() {
    return structDelegate().toString();
  }
}
