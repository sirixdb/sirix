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
    var result = BIG_INT_31.add(structNodeDelegate.getNodeDelegate().computeHash());
    if (structNodeDelegate.isNotEmpty()) {
      var multiplyBigInt = BIG_INT_31.multiply(result);
      result = multiplyBigInt.add(structNodeDelegate.computeHash());
      multiplyBigInt = null;
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
    if (hashCode == null) {
      hashCode = computeHash();
    }
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
