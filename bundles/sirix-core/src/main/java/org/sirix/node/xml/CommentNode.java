package org.sirix.node.xml;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.hash.HashCode;
import org.sirix.api.visitor.VisitResult;
import org.sirix.api.visitor.XmlNodeVisitor;
import org.sirix.node.NodeKind;
import org.sirix.node.SirixDeweyID;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.delegates.StructNodeDelegate;
import org.sirix.node.delegates.ValueNodeDelegate;
import org.sirix.node.immutable.xml.ImmutableComment;
import org.sirix.node.interfaces.Node;
import org.sirix.node.interfaces.StructNode;
import org.sirix.node.interfaces.ValueNode;
import org.sirix.node.interfaces.immutable.ImmutableXmlNode;
import org.sirix.settings.Constants;
import org.sirix.settings.Fixed;

import org.checkerframework.checker.nullness.qual.Nullable;
import java.math.BigInteger;

/**
 * Comment node implementation.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class CommentNode extends AbstractStructForwardingNode implements ValueNode, ImmutableXmlNode {

  /** {@link StructNodeDelegate} reference. */
  private final StructNodeDelegate mStructNodeDel;

  /** {@link ValueNodeDelegate} reference. */
  private final ValueNodeDelegate mValDel;

  /** Value of the node. */
  private byte[] mValue;

  private BigInteger mHash;

  /**
   * Constructor for TextNode.
   *
   * @param valDel delegate for {@link ValueNode} implementation
   * @param structDel delegate for {@link StructNode} implementation
   */
  public CommentNode(final BigInteger hashCode, final ValueNodeDelegate valDel, final StructNodeDelegate structDel) {
    mHash = hashCode;
    assert valDel != null;
    mValDel = valDel;
    assert structDel != null;
    mStructNodeDel = structDel;
  }

  /**
   * Constructor for TextNode.
   *
   * @param valDel delegate for {@link ValueNode} implementation
   * @param structDel delegate for {@link StructNode} implementation
   */
  public CommentNode(final ValueNodeDelegate valDel, final StructNodeDelegate structDel) {
    assert valDel != null;
    mValDel = valDel;
    assert structDel != null;
    mStructNodeDel = structDel;
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.COMMENT;
  }

  @Override
  public BigInteger computeHash() {
    final HashCode valueHashCode = mStructNodeDel.getNodeDelegate().getHashFunction().hashBytes(getRawValue());

    final BigInteger valueBigInteger = new BigInteger(1, valueHashCode.asBytes());

    BigInteger result = BigInteger.ONE;

    result = BigInteger.valueOf(31).multiply(result).add(mStructNodeDel.getNodeDelegate().computeHash());
    result = BigInteger.valueOf(31).multiply(result).add(mStructNodeDel.computeHash());
    result = BigInteger.valueOf(31).multiply(result).add(valueBigInteger);

    return Node.to128BitsAtMaximumBigInteger(result);
  }

  @Override
  public void setHash(final BigInteger hash) {
    mHash = Node.to128BitsAtMaximumBigInteger(hash);
  }

  @Override
  public BigInteger getHash() {
    return mHash;
  }

  @Override
  public byte[] getRawValue() {
    if (mValue == null) {
      mValue = mValDel.getRawValue();
    }
    return mValue;
  }

  @Override
  public void setValue(final byte[] value) {
    mValue = null;
    mValDel.setValue(value);
  }

  @Override
  public long getFirstChildKey() {
    return Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public VisitResult acceptVisitor(final XmlNodeVisitor visitor) {
    return visitor.visit(ImmutableComment.of(this));
  }

  @Override
  public void decrementChildCount() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void incrementChildCount() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getDescendantCount() {
    return 0;
  }

  @Override
  public void decrementDescendantCount() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void incrementDescendantCount() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setDescendantCount(final long descendantCount) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mStructNodeDel.getNodeDelegate(), mValDel);
  }

  @Override
  public boolean equals(final @Nullable Object obj) {
    if (obj instanceof CommentNode) {
      final CommentNode other = (CommentNode) obj;
      return Objects.equal(mStructNodeDel.getNodeDelegate(), other.getNodeDelegate()) && mValDel.equals(other.mValDel);
    }
    return false;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
                      .add("node delegate", mStructNodeDel.getNodeDelegate())
                      .add("value delegate", mValDel)
                      .toString();
  }

  public ValueNodeDelegate getValNodeDelegate() {
    return mValDel;
  }

  @Override
  protected NodeDelegate delegate() {
    return mStructNodeDel.getNodeDelegate();
  }

  @Override
  protected StructNodeDelegate structDelegate() {
    return mStructNodeDel;
  }

  @Override
  public String getValue() {
    return new String(mValDel.getRawValue(), Constants.DEFAULT_ENCODING);
  }

  @Override
  public SirixDeweyID getDeweyID() {
    return mStructNodeDel.getNodeDelegate().getDeweyID();
  }

  @Override
  public int getTypeKey() {
    return mStructNodeDel.getNodeDelegate().getTypeKey();
  }

}
