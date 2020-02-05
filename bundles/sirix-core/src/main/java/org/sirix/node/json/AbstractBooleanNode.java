package org.sirix.node.json;

import org.sirix.api.visitor.JsonNodeVisitor;
import org.sirix.api.visitor.VisitResult;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.delegates.StructNodeDelegate;
import org.sirix.node.immutable.json.ImmutableBooleanNode;
import org.sirix.node.interfaces.Node;
import org.sirix.node.interfaces.immutable.ImmutableJsonNode;
import org.sirix.node.xml.AbstractStructForwardingNode;

import java.math.BigInteger;

public abstract class AbstractBooleanNode extends AbstractStructForwardingNode implements ImmutableJsonNode {

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

    result = BigInteger.valueOf(31).multiply(result).add(structNodeDelegate.getNodeDelegate().computeHash());
    if (structNodeDelegate.isNotEmpty()) {
      result = BigInteger.valueOf(31).multiply(result).add(structNodeDelegate.computeHash());
    }
    result = BigInteger.valueOf(31).multiply(result).add(BigInteger.valueOf(Boolean.hashCode(boolValue)));

    return Node.to128BitsAtMaximumBigInteger(result);
  }

  @Override
  public void setHash(final BigInteger hash) {
    this.hashCode = Node.to128BitsAtMaximumBigInteger(hash);
  }

  @Override
  public BigInteger getHash() {
    return hashCode;
  }

  public void setValue(final boolean value) {
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
