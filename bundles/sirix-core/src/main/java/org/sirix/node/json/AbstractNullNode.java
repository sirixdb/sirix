package org.sirix.node.json;

import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.delegates.StructNodeDelegate;
import org.sirix.node.interfaces.Node;
import org.sirix.node.interfaces.immutable.ImmutableJsonNode;
import org.sirix.node.xml.AbstractStructForwardingNode;

import java.math.BigInteger;

public abstract class AbstractNullNode extends AbstractStructForwardingNode implements ImmutableJsonNode {
  private StructNodeDelegate mStructNodeDel;

  private BigInteger mHash;

  public AbstractNullNode(StructNodeDelegate mStructNodeDel) {
    this.mStructNodeDel = mStructNodeDel;
  }

  @Override
  public BigInteger computeHash() {
    BigInteger result = BigInteger.ONE;

    result = BigInteger.valueOf(31).multiply(result).add(mStructNodeDel.getNodeDelegate().computeHash());
    if (mStructNodeDel.isNotEmpty()) {
      result = BigInteger.valueOf(31).multiply(result).add(mStructNodeDel.computeHash());
    }
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
  protected NodeDelegate delegate() {
    return mStructNodeDel.getNodeDelegate();
  }

  @Override
  protected StructNodeDelegate structDelegate() {
    return mStructNodeDel;
  }
}
