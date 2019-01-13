package org.sirix.node.immutable.json;

import java.util.Optional;
import javax.annotation.Nullable;
import org.sirix.api.visitor.VisitResult;
import org.sirix.api.visitor.Visitor;
import org.sirix.node.SirixDeweyID;
import org.sirix.node.interfaces.Node;
import org.sirix.node.interfaces.StructNode;
import org.sirix.node.interfaces.immutable.ImmutableStructNode;

public abstract class AbstractImmutableJSONStructuralNode implements ImmutableStructNode {

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
  public int getTypeKey() {
    return structDelegate().getTypeKey();
  }

  @Override
  public boolean isSameItem(final @Nullable Node other) {
    return structDelegate().isSameItem(other);
  }

  @Override
  public VisitResult acceptVisitor(final Visitor visitor) {
    throw new UnsupportedOperationException();
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
  public long getRevision() {
    return structDelegate().getRevision();
  }

  @Override
  public Optional<SirixDeweyID> getDeweyID() {
    return structDelegate().getDeweyID();
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
