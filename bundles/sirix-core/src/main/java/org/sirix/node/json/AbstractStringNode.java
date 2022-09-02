package org.sirix.node.json;

import com.google.common.hash.Funnel;
import com.google.common.hash.HashFunction;
import com.google.common.hash.PrimitiveSink;
import org.jetbrains.annotations.NotNull;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.delegates.StructNodeDelegate;
import org.sirix.node.delegates.ValueNodeDelegate;
import org.sirix.node.interfaces.Node;
import org.sirix.node.interfaces.StructNode;
import org.sirix.node.interfaces.ValueNode;
import org.sirix.node.interfaces.immutable.ImmutableJsonNode;
import org.sirix.node.xml.AbstractStructForwardingNode;
import org.sirix.settings.Constants;
import org.sirix.settings.Fixed;

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
    final var nodeDelegate = structNodeDelegate.getNodeDelegate();
    final HashFunction hashFunction = nodeDelegate.getHashFunction();

    final Funnel<StructNode> nodeFunnel = (StructNode node, PrimitiveSink into) -> {
      into = into.putLong(node.getNodeKey()).putLong(node.getParentKey()).putByte(node.getKind().getId());

      if (node.getLastChildKey() != Fixed.INVALID_KEY_FOR_TYPE_CHECK.getStandardProperty()) {
        into.putLong(node.getChildCount())
            .putLong(node.getDescendantCount())
            .putLong(node.getLeftSiblingKey())
            .putLong(node.getRightSiblingKey())
            .putLong(node.getFirstChildKey())
            .putLong(node.getLastChildKey());
      } else {
        into.putLong(node.getChildCount())
            .putLong(node.getDescendantCount())
            .putLong(node.getLeftSiblingKey())
            .putLong(node.getRightSiblingKey())
            .putLong(node.getFirstChildKey());
      }

      into.putString(new String(valueNodeDelegate.getRawValue(), Constants.DEFAULT_ENCODING),
                     Constants.DEFAULT_ENCODING);
    };

    return Node.to128BitsAtMaximumBigInteger(new BigInteger(1,
                                                            nodeDelegate.getHashFunction()
                                                                        .hashObject(this, nodeFunnel)
                                                                        .asBytes()));
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
    hashCode = null;
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
  protected @NotNull NodeDelegate delegate() {
    return structNodeDelegate.getNodeDelegate();
  }
}
