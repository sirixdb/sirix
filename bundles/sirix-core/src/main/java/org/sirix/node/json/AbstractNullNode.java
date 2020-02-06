package org.sirix.node.json;

import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.delegates.StructNodeDelegate;
import org.sirix.node.interfaces.Node;
import org.sirix.node.interfaces.immutable.ImmutableJsonNode;
import org.sirix.node.xml.AbstractStructForwardingNode;

import java.math.BigInteger;

public abstract class AbstractNullNode extends AbstractStructForwardingNode implements ImmutableJsonNode {
  private StructNodeDelegate structNodeDelegate;

  private BigInteger hashCode;

  public AbstractNullNode(StructNodeDelegate mStructNodeDel) {
    this.structNodeDelegate = mStructNodeDel;
  }

  @Override
  public BigInteger computeHash() {
    BigInteger result = BigInteger.ONE;

    result = BigInteger.valueOf(31).multiply(result).add(structNodeDelegate.getNodeDelegate().computeHash());
    if (structNodeDelegate.isNotEmpty()) {
      result = BigInteger.valueOf(31).multiply(result).add(structNodeDelegate.computeHash());
    }
    return Node.to128BitsAtMaximumBigInteger(result);
  }

  @Override
  public void setHash(final BigInteger hash) {
    if (hash != null) {
      hashCode = Node.to128BitsAtMaximumBigInteger(hash);
    } else {
      hashCode = null;
    }
  }

  @Override
  public BigInteger getHash() {
    return hashCode;
  }

  @Override
  protected NodeDelegate delegate() {
    return structNodeDelegate.getNodeDelegate();
  }

  @Override
  protected StructNodeDelegate structDelegate() {
    return structNodeDelegate;
  }
}
