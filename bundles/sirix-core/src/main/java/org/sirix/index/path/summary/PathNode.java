package org.sirix.index.path.summary;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.brackit.xquery.atomic.QNm;
import org.brackit.xquery.util.path.Path;
import org.sirix.node.NodeKind;
import org.sirix.node.delegates.NameNodeDelegate;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.delegates.StructNodeDelegate;
import org.sirix.node.interfaces.NameNode;
import org.sirix.node.xml.AbstractStructForwardingNode;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

/**
 * Path node in the {@link PathSummaryReader}.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class PathNode extends AbstractStructForwardingNode implements NameNode {

  /** {@link NodeDelegate} instance. */
  private final NodeDelegate mNodeDel;

  /** {@link StructNodeDelegate} instance. */
  private final StructNodeDelegate mStructNodeDel;

  /** {@link NameNodeDelegate} instance. */
  private final NameNodeDelegate mNameNodeDel;

  /** Kind of node to index. */
  private final NodeKind mKind;

  /** Number of references to this path node. */
  private int mReferences;

  /** Level of this path node. */
  private int mLevel;

  /**
   * Constructor.
   *
   * @param nodeDel {@link NodeDelegate} instance
   * @param structNodeDel {@link StructNodeDelegate} instance
   * @param nameNodeDel {@link NameNodeDelegate} instance
   * @param kind kind of node to index
   * @param references number of references to this path node
   * @param level level of this path node
   */
  public PathNode(final NodeDelegate nodeDel, @Nonnull final StructNodeDelegate structNodeDel,
      @Nonnull final NameNodeDelegate nameNodeDel, @Nonnull final NodeKind kind, @Nonnegative final int references,
      @Nonnegative final int level) {
    mNodeDel = checkNotNull(nodeDel);
    mStructNodeDel = checkNotNull(structNodeDel);
    mNameNodeDel = checkNotNull(nameNodeDel);
    mKind = checkNotNull(kind);
    checkArgument(references > 0, "references must be > 0!");
    mReferences = references;
    mLevel = level;
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
    final PathNode[] path = new PathNode[mLevel];
    for (int i = mLevel - 1; i >= 0; i--) {
      path[i] = node;
      node = reader.moveToParent().trx().getPathNode();
    }

    final Path<QNm> p = new Path<>();
    for (final PathNode n : path) {
      reader.moveTo(n.getNodeKey());
      if (n.getPathKind() == NodeKind.ATTRIBUTE) {
        p.attribute(reader.getName());
      } else {
        p.child(reader.getName());
      }
    }
    reader.moveTo(nodeKey);
    return p;
  }

  /**
   * Level of this path node.
   *
   * @return level of this path node
   */
  public int getLevel() {
    return mLevel;
  }

  /**
   * Get the number of references to this path node.
   *
   * @return number of references
   */
  public int getReferences() {
    return mReferences;
  }

  /**
   * Set the reference count.
   *
   * @param references number of references
   */
  public void setReferenceCount(final @Nonnegative int references) {
    checkArgument(references > 0, "pReferences must be > 0!");
    mReferences = references;
  }

  /**
   * Increment the reference count.
   */
  public void incrementReferenceCount() {
    mReferences++;
  }

  /**
   * Decrement the reference count.
   */
  public void decrementReferenceCount() {
    if (mReferences <= 1) {
      throw new IllegalStateException();
    }
    mReferences--;
  }

  /**
   * Get the kind of path (element, attribute or namespace).
   *
   * @return path kind
   */
  public NodeKind getPathKind() {
    return mKind;
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.PATH;
  }

  @Override
  public int getPrefixKey() {
    return mNameNodeDel.getPrefixKey();
  }

  @Override
  public int getLocalNameKey() {
    return mNameNodeDel.getLocalNameKey();
  }

  @Override
  public int getURIKey() {
    return mNameNodeDel.getURIKey();
  }

  @Override
  public void setLocalNameKey(final int nameKey) {
    mNameNodeDel.setLocalNameKey(nameKey);
  }

  @Override
  public void setPrefixKey(final int prefixKey) {
    mNameNodeDel.setPrefixKey(prefixKey);
  }

  @Override
  public void setURIKey(final int uriKey) {
    mNameNodeDel.setURIKey(uriKey);
  }

  @Override
  protected StructNodeDelegate structDelegate() {
    return mStructNodeDel;
  }

  @Override
  protected NodeDelegate delegate() {
    return mNodeDel;
  }

  /**
   * Get the name node delegate.
   *
   * @return name node delegate.
   */
  public NameNodeDelegate getNameNodeDelegate() {
    return mNameNodeDel;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mNodeDel, mNameNodeDel);
  }

  @Override
  public boolean equals(final @Nullable Object obj) {
    if (obj instanceof PathNode) {
      final PathNode other = (PathNode) obj;
      return Objects.equal(mNodeDel, other.mNodeDel) && Objects.equal(mNameNodeDel, other.mNameNodeDel);
    }
    return false;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
                      .add("node delegate", mNodeDel)
                      .add("struct delegate", mStructNodeDel)
                      .add("name delegate", mNameNodeDel)
                      .add("references", mReferences)
                      .add("kind", mKind)
                      .add("level", mLevel)
                      .toString();
  }

  @Override
  public void setPathNodeKey(final long pNodeKey) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getPathNodeKey() {
    throw new UnsupportedOperationException();
  }

  @Override
  public QNm getName() {
    // FIXME (should be implemented!)
    throw new UnsupportedOperationException();
  }

}
