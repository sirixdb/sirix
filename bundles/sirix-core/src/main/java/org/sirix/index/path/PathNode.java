package org.sirix.index.path;

import static com.google.common.base.Preconditions.checkNotNull;
import com.google.common.base.Objects;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.sirix.api.visitor.EVisitResult;
import org.sirix.api.visitor.IVisitor;
import org.sirix.node.AbsStructForwardingNode;
import org.sirix.node.EKind;
import org.sirix.node.delegates.NameNodeDelegate;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.delegates.StructNodeDelegate;
import org.sirix.node.interfaces.INameNode;

public class PathNode extends AbsStructForwardingNode implements INameNode {

  private final NodeDelegate mNodeDel;
  private final StructNodeDelegate mStructNodeDel;
  private final NameNodeDelegate mNameNodeDel;
  private final EKind mKind;
  private long mPCR;

  public PathNode(@Nonnull final NodeDelegate pNodeDel,
    @Nonnull final StructNodeDelegate pStructNodeDel,
    @Nonnull final NameNodeDelegate pNameNodeDel, @Nonnull EKind pKind,
    @Nonnegative long pPCR) {
    mNodeDel = checkNotNull(pNodeDel);
    mStructNodeDel = checkNotNull(pStructNodeDel);
    mNameNodeDel = checkNotNull(pNameNodeDel);
    mKind = checkNotNull(pKind);
    mPCR = pPCR;
  }

  public EKind getPathKind() {
    return mKind;
  }

  @Override
  public EKind getKind() {
    return EKind.PATH;
  }

  @Override
  public int getNameKey() {
    return mNameNodeDel.getNameKey();
  }

  @Override
  public int getURIKey() {
    return mNameNodeDel.getURIKey();
  }

  @Override
  public void setNameKey(final int pNameKey) {
    mNameNodeDel.setNameKey(pNameKey);
  }

  @Override
  public void setURIKey(final int pUriKey) {
    mNameNodeDel.setURIKey(pUriKey);
  }

  @Override
  public EVisitResult acceptVisitor(@Nonnull final IVisitor pVisitor) {
    throw new UnsupportedOperationException();
  }

  /**
   * Get path class record.
   * 
   * @return path class record.
   */
  public long getPCR() {
    return mPCR;
  }

  /**
   * Set path class record.
   * 
   * @param pPCR
   *          path class record
   */
  public void setPCR(final long pPCR) {
    mPCR = pPCR;
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
    return Objects.hashCode(mNodeDel, mStructNodeDel, mNameNodeDel);
  }

  @Override
  public boolean equals(@Nullable Object pObj) {
    if (pObj instanceof PathNode) {
      final PathNode other = (PathNode)pObj;
      return Objects.equal(mNodeDel, other.mNodeDel)
        && Objects.equal(mStructNodeDel, other.mStructNodeDel)
        && Objects.equal(mNameNodeDel, other.mNameNodeDel);
    }
    return false;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("node delegate", mNodeDel).add(
      "struct delegate", mStructNodeDel).add("name delegate", mNameNodeDel)
      .toString();
  }

  @Override
  public void setPathNodeKey(long nodeKey) {
  }

  @Override
  public long getPathNodeKey() {
    return 0;
  }

}
