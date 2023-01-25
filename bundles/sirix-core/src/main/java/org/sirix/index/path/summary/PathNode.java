package org.sirix.index.path.summary;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.util.path.Path;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.jetbrains.annotations.NotNull;
import org.sirix.node.NodeKind;
import org.sirix.node.delegates.NameNodeDelegate;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.delegates.StructNodeDelegate;
import org.sirix.node.interfaces.NameNode;
import org.sirix.node.xml.AbstractStructForwardingNode;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Path node in the {@link PathSummaryReader}.
 *
 * @author Johannes Lichtenberger
 */
public final class PathNode extends AbstractStructForwardingNode implements NameNode {

  /**
   * {@link NodeDelegate} instance.
   */
  private final NodeDelegate nodeDel;

  /**
   * {@link StructNodeDelegate} instance.
   */
  private final StructNodeDelegate structNodeDel;

  /**
   * {@link NameNodeDelegate} instance.
   */
  private final NameNodeDelegate nameNodeDel;

  /**
   * Kind of node to index.
   */
  private final NodeKind kind;

  /**
   * The node name.
   */
  private final QNm name;

  /**
   * Number of references to this path node.
   */
  private int references;

  /**
   * Level of this path node.
   */
  private final int level;

  private PathNode firstChild;

  private PathNode lastChild;

  private PathNode parent;

  private PathNode leftSibling;

  private PathNode rightSibling;

  /**
   * Constructor.
   *
   * @param name          the full qualified name
   * @param nodeDel       {@link NodeDelegate} instance
   * @param structNodeDel {@link StructNodeDelegate} instance
   * @param nameNodeDel   {@link NameNodeDelegate} instance
   * @param kind          kind of node to index
   * @param references    number of references to this path node
   * @param level         level of this path node
   */
  public PathNode(final QNm name, final NodeDelegate nodeDel, @NonNull final StructNodeDelegate structNodeDel,
      @NonNull final NameNodeDelegate nameNodeDel, @NonNull final NodeKind kind, @NonNegative final int references,
      @NonNegative final int level) {
    this.name = name;
    this.nodeDel = checkNotNull(nodeDel);
    this.structNodeDel = checkNotNull(structNodeDel);
    this.nameNodeDel = checkNotNull(nameNodeDel);
    this.kind = checkNotNull(kind);
    checkArgument(references > 0, "references must be > 0!");
    this.references = references;
    this.level = level;
  }

  /**
   * Get the path up to the root path node.
   *
   * @param reader {@link PathSummaryReader} instance
   * @return path up to the root
   */
  public Path<QNm> getPath(final PathSummaryReader reader) {
    PathNode node = this;
    final long nodeKey = reader.getNodeKey();
    reader.moveTo(node.getNodeKey());
    final PathNode[] pathNodes = new PathNode[level];
    for (int i = level - 1; i >= 0; i--) {
      pathNodes[i] = node;
      reader.moveToParent();
      node = reader.getPathNode();
    }

    final Path<QNm> path = new Path<>();
    for (final PathNode pathNode : pathNodes) {
      reader.moveTo(pathNode.getNodeKey());
      if (pathNode.getPathKind() == NodeKind.ATTRIBUTE) {
        path.attribute(reader.getName());
      } else {
        final QNm name;
        if (reader.getPathKind() == NodeKind.OBJECT_KEY) {
          name = new QNm(null, null, reader.getName().getLocalName());
          path.childObjectField(name);
        } else if (reader.getPathKind() == NodeKind.ARRAY) {
          path.childArray();
        } else {
          name = reader.getName();
          path.child(name);
        }
      }
    }
    reader.moveTo(nodeKey);
    return path;
  }

  /**
   * Level of this path node.
   *
   * @return level of this path node
   */
  public int getLevel() {
    return level;
  }

  /**
   * Get the number of references to this path node.
   *
   * @return number of references
   */
  public int getReferences() {
    return references;
  }

  /**
   * Set the reference count.
   *
   * @param references number of references
   */
  public void setReferenceCount(final @NonNegative int references) {
    checkArgument(references > 0, "pReferences must be > 0!");
    this.references = references;
  }

  /**
   * Increment the reference count.
   */
  public void incrementReferenceCount() {
    references++;
  }

  /**
   * Decrement the reference count.
   */
  public void decrementReferenceCount() {
    if (references <= 1) {
      throw new IllegalStateException();
    }
    references--;
  }

  /**
   * Get the kind of path (element, attribute or namespace).
   *
   * @return path kind
   */
  public NodeKind getPathKind() {
    return kind;
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.PATH;
  }

  @Override
  public int getPrefixKey() {
    return nameNodeDel.getPrefixKey();
  }

  @Override
  public int getLocalNameKey() {
    return nameNodeDel.getLocalNameKey();
  }

  @Override
  public int getURIKey() {
    return nameNodeDel.getURIKey();
  }

  @Override
  public void setLocalNameKey(final int nameKey) {
    nameNodeDel.setLocalNameKey(nameKey);
  }

  @Override
  public void setPrefixKey(final int prefixKey) {
    nameNodeDel.setPrefixKey(prefixKey);
  }

  @Override
  public void setURIKey(final int uriKey) {
    nameNodeDel.setURIKey(uriKey);
  }

  @Override
  protected StructNodeDelegate structDelegate() {
    return structNodeDel;
  }

  @Override
  protected @NotNull NodeDelegate delegate() {
    return nodeDel;
  }

  /**
   * Get the name node delegate.
   *
   * @return name node delegate.
   */
  public NameNodeDelegate getNameNodeDelegate() {
    return nameNodeDel;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(nodeDel, nameNodeDel);
  }

  @Override
  public boolean equals(final @Nullable Object obj) {
    if (obj instanceof PathNode other) {
      return Objects.equal(nodeDel, other.nodeDel) && Objects.equal(nameNodeDel, other.nameNodeDel);
    }
    return false;
  }

  @Override
  public @NotNull String toString() {
    return MoreObjects.toStringHelper(this)
                      .add("node delegate", nodeDel)
                      .add("struct delegate", structNodeDel)
                      .add("name delegate", nameNodeDel)
                      .add("references", references)
                      .add("kind", kind)
                      .add("level", level)
                      .toString();
  }

  @Override
  public void setPathNodeKey(final long nodeKey) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getPathNodeKey() {
    throw new UnsupportedOperationException();
  }

  @Override
  public QNm getName() {
    return name;
  }

  public void setFirstChild(PathNode pathNode) {
    firstChild = pathNode;
  }

  public void setLastChild(PathNode pathNode) {
    lastChild = pathNode;
  }

  public void setParent(PathNode pathNode) {
    parent = pathNode;
  }

  public void setLeftSibling(PathNode pathNode) {
    leftSibling = pathNode;
  }

  public void setRightSibling(PathNode pathNode) {
    rightSibling = pathNode;
  }

  public PathNode getFirstChild() {
    return firstChild;
  }

  public PathNode getLastChild() {
    return lastChild;
  }

  public PathNode getParent() {
    return parent;
  }

  public PathNode getLeftSibling() {
    return leftSibling;
  }

  public PathNode getRightSibling() {
    return rightSibling;
  }
}
