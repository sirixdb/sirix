package io.sirix.node.immutable.json;

import io.sirix.node.SirixDeweyID;
import io.sirix.node.interfaces.Node;
import io.sirix.node.interfaces.StructNode;
import io.sirix.node.interfaces.immutable.ImmutableJsonNode;
import io.sirix.node.interfaces.immutable.ImmutableNode;
import io.sirix.node.interfaces.immutable.ImmutableStructNode;
import org.checkerframework.checker.nullness.qual.Nullable;

public abstract class AbstractImmutableJsonStructuralNode implements ImmutableStructNode, ImmutableJsonNode {

  @Override
  public abstract ImmutableNode clone();

  public abstract StructNode structDelegate();

  @Override
  public SirixDeweyID getDeweyID() {
    return structDelegate().getDeweyID();
  }

  @Override
  public byte[] getDeweyIDAsBytes() {
    return structDelegate().getDeweyIDAsBytes();
  }

  @Override
  public boolean hasFirstChild() {
    return structDelegate().hasFirstChild();
  }

  @Override
  public boolean hasLastChild() {
    return structDelegate().hasLastChild();
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
  public long getLastChildKey() {
    return structDelegate().getLastChildKey();
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
  public long getHash() {
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
  public int getPreviousRevisionNumber() {
    return structDelegate().getPreviousRevisionNumber();
  }

  @Override
  public int getLastModifiedRevisionNumber() {
    return structDelegate().getLastModifiedRevisionNumber();
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof AbstractImmutableJsonStructuralNode)) {
      return false;
    }
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
