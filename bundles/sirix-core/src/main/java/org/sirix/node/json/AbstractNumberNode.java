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

    var result = BIG_INT_31.add(structNodeDelegate.getNodeDelegate().computeHash());

    if (structNodeDelegate.isNotEmpty()) {
      var multiplyBigInt = BIG_INT_31.multiply(result);
      result = multiplyBigInt.add(structNodeDelegate.computeHash());
      multiplyBigInt = null;
    }

    var multiplyBigInt = BIG_INT_31.multiply(result);
    result = multiplyBigInt.add(new BigInteger(1, valueHashCode.asBytes()));
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

  public void setValue(final Number number) {
    hashCode = null;
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
