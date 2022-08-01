package org.sirix.node.json;

import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.delegates.StructNodeDelegate;
import org.sirix.node.interfaces.Node;
import org.sirix.node.interfaces.immutable.ImmutableJsonNode;
import org.sirix.node.xml.AbstractStructForwardingNode;

import java.math.BigInteger;

public abstract class AbstractBooleanNode extends AbstractStructForwardingNode implements ImmutableJsonNode {

  private static final BigInteger BIG_INT_TRUE = BigInteger.valueOf(Boolean.hashCode(true));

  private static final BigInteger BIG_INT_FALSE = BigInteger.valueOf(Boolean.hashCode(false));

  private StructNodeDelegate structNodeDelegate;

  private boolean boolValue;

  private BigInteger hashCode;

  public AbstractBooleanNode(StructNodeDelegate structNodeDelegate, final boolean boolValue) {
    this.structNodeDelegate = structNodeDelegate;
    this.boolValue = boolValue;
  }

  @Override
  public BigInteger computeHash() {
    BigInteger result = BigInteger.ONE;

    result = BIG_INT_31.add(structNodeDelegate.getNodeDelegate().computeHash());
    if (structNodeDelegate.isNotEmpty()) {
      var multiplyBigInt = BIG_INT_31.multiply(result);
      result = multiplyBigInt.add(structNodeDelegate.computeHash());
      multiplyBigInt = null;
    }
    var multiplyBigInt = BIG_INT_31.multiply(result);
    result = multiplyBigInt.add(boolValue ? BIG_INT_TRUE : BIG_INT_FALSE);
    multiplyBigInt = null;

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

  public void setValue(final boolean value) {
    hashCode = null;
    boolValue = value;
  }

  public boolean getValue() {
    return boolValue;
  }

  @Override
  public StructNodeDelegate getStructNodeDelegate() {
    return structNodeDelegate;
  }

  @Override
  protected StructNodeDelegate structDelegate() {
    return structNodeDelegate;
  }

  @Override
  protected NodeDelegate delegate() {
    return structNodeDelegate.getNodeDelegate();
  }
}
