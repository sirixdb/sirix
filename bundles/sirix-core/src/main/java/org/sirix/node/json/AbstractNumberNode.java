package org.sirix.node.json;

import com.google.common.hash.HashCode;
import org.sirix.api.visitor.JsonNodeVisitor;
import org.sirix.api.visitor.VisitResult;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.delegates.StructNodeDelegate;
import org.sirix.node.immutable.json.ImmutableNumberNode;
import org.sirix.node.interfaces.Node;
import org.sirix.node.interfaces.immutable.ImmutableJsonNode;
import org.sirix.node.xml.AbstractStructForwardingNode;

import java.math.BigInteger;

public abstract class AbstractNumberNode extends AbstractStructForwardingNode implements ImmutableJsonNode {

  private StructNodeDelegate structNodeDelegate;
  private Number number;

  private BigInteger hashCode;

  public AbstractNumberNode(StructNodeDelegate structNodeDel, Number number) {
    this.structNodeDelegate = structNodeDel;
    this.number = number;
  }

  @Override
  public BigInteger computeHash() {
    final HashCode valueHashCode = structNodeDelegate.getNodeDelegate().getHashFunction().hashInt(number.hashCode());

    BigInteger result = BigInteger.ONE;

    result = BigInteger.valueOf(31).multiply(result).add(structNodeDelegate.getNodeDelegate().computeHash());

    if (structNodeDelegate.isNotEmpty()) {
      result = BigInteger.valueOf(31).multiply(result).add(structNodeDelegate.computeHash());
    }

    result = BigInteger.valueOf(31).multiply(result).add(new BigInteger(1, valueHashCode.asBytes()));

    return Node.to128BitsAtMaximumBigInteger(result);
  }

  @Override
  public void setHash(final BigInteger hash) {
    hashCode = Node.to128BitsAtMaximumBigInteger(hash);
  }

  @Override
  public BigInteger getHash() {
    return hashCode;
  }

  public void setValue(final Number number) {
    this.number = number;
  }

  public Number getValue() {
    return number;
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
