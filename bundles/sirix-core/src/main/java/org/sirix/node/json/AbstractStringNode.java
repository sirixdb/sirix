package org.sirix.node.json;

import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.delegates.StructNodeDelegate;
import org.sirix.node.delegates.ValueNodeDelegate;
import org.sirix.node.interfaces.Node;
import org.sirix.node.interfaces.ValueNode;
import org.sirix.node.interfaces.immutable.ImmutableJsonNode;
import org.sirix.node.xml.AbstractStructForwardingNode;
import org.sirix.settings.Constants;

import java.math.BigInteger;

public abstract class AbstractStringNode extends AbstractStructForwardingNode implements ValueNode, ImmutableJsonNode {

  private final ValueNodeDelegate valueNodeDelegate;

  private final StructNodeDelegate structNodeDelegate;

  private BigInteger hashCode;

  public AbstractStringNode(ValueNodeDelegate valueNodeDelegate, StructNodeDelegate structNodeDelegate) {
    this.valueNodeDelegate = valueNodeDelegate;
    this.structNodeDelegate = structNodeDelegate;
  }

  @Override
  public BigInteger computeHash() {
    BigInteger result = BigInteger.ONE;

    result = BigInteger.valueOf(31).multiply(result).add(structNodeDelegate.getNodeDelegate().computeHash());
    if (structNodeDelegate.isNotEmpty()) {
      result = BigInteger.valueOf(31).multiply(result).add(structNodeDelegate.computeHash());
    }
    result = BigInteger.valueOf(31).multiply(result).add(valueNodeDelegate.computeHash());

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
  public byte[] getRawValue() {
    return valueNodeDelegate.getRawValue();
  }

  @Override
  public void setValue(final byte[] value) {
    valueNodeDelegate.setValue(value);
  }

  @Override
  public String getValue() {
    return new String(valueNodeDelegate.getRawValue(), Constants.DEFAULT_ENCODING);
  }

  @Override
  public StructNodeDelegate getStructNodeDelegate() {
    return structNodeDelegate;
  }

  public ValueNodeDelegate getValNodeDelegate() {
    return valueNodeDelegate;
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
