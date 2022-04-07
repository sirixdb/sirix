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
  private final StructNodeDelegate structNodeDel;

  /** {@link ValueNodeDelegate} reference. */
  private final ValueNodeDelegate valDel;

  /** Value of the node. */
  private byte[] value;

  private BigInteger hash;

  /**
   * Constructor for TextNode.
   *
   * @param valDel delegate for {@link ValueNode} implementation
   * @param structDel delegate for {@link StructNode} implementation
   */
  public CommentNode(final BigInteger hashCode, final ValueNodeDelegate valDel, final StructNodeDelegate structDel) {
    hash = hashCode;
    assert valDel != null;
    this.valDel = valDel;
    assert structDel != null;
    structNodeDel = structDel;
  }

  /**
   * Constructor for TextNode.
   *
   * @param valDel delegate for {@link ValueNode} implementation
   * @param structDel delegate for {@link StructNode} implementation
   */
  public CommentNode(final ValueNodeDelegate valDel, final StructNodeDelegate structDel) {
    assert valDel != null;
    this.valDel = valDel;
    assert structDel != null;
    structNodeDel = structDel;
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.COMMENT;
  }

  @Override
  public BigInteger computeHash() {
    final HashCode valueHashCode = structNodeDel.getNodeDelegate().getHashFunction().hashBytes(getRawValue());

    final BigInteger valueBigInteger = new BigInteger(1, valueHashCode.asBytes());

    BigInteger result = BigInteger.ONE;

    result = BigInteger.valueOf(31).multiply(result).add(structNodeDel.getNodeDelegate().computeHash());
    result = BigInteger.valueOf(31).multiply(result).add(structNodeDel.computeHash());
    result = BigInteger.valueOf(31).multiply(result).add(valueBigInteger);

    return Node.to128BitsAtMaximumBigInteger(result);
  }

  @Override
  public void setHash(final BigInteger hash) {
    this.hash = Node.to128BitsAtMaximumBigInteger(hash);
  }

  @Override
  public BigInteger getHash() {
    return hash;
  }

  @Override
  public byte[] getRawValue() {
    if (value == null) {
      value = valDel.getRawValue();
    }
    return value;
  }

  @Override
  public void setValue(final byte[] value) {
    this.value = null;
    valDel.setValue(value);
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
    return Objects.hashCode(structNodeDel.getNodeDelegate(), valDel);
  }

  @Override
  public boolean equals(final @Nullable Object obj) {
    if (obj instanceof CommentNode) {
      final CommentNode other = (CommentNode) obj;
      return Objects.equal(structNodeDel.getNodeDelegate(), other.getNodeDelegate()) && valDel.equals(other.valDel);
    }
    return false;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
                      .add("node delegate", structNodeDel.getNodeDelegate())
                      .add("value delegate", valDel)
                      .toString();
  }

  public ValueNodeDelegate getValNodeDelegate() {
    return valDel;
  }

  @Override
  protected NodeDelegate delegate() {
    return structNodeDel.getNodeDelegate();
  }

  @Override
  protected StructNodeDelegate structDelegate() {
    return structNodeDel;
  }

  @Override
  public String getValue() {
    return new String(valDel.getRawValue(), Constants.DEFAULT_ENCODING);
  }

  @Override
  public SirixDeweyID getDeweyID() {
    return structNodeDel.getNodeDelegate().getDeweyID();
  }

  @Override
  public int getTypeKey() {
    return structNodeDel.getNodeDelegate().getTypeKey();
  }

  @Override
  public byte[] getDeweyIDAsBytes() {
    return structNodeDel.getDeweyIDAsBytes();
  }
}
