package org.sirix.node.json;

import com.google.common.hash.Funnel;
import com.google.common.hash.HashFunction;
import com.google.common.hash.PrimitiveSink;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.delegates.StructNodeDelegate;
import org.sirix.node.interfaces.Node;
import org.sirix.node.interfaces.StructNode;
import org.sirix.node.interfaces.immutable.ImmutableJsonNode;
import org.sirix.node.xml.AbstractStructForwardingNode;
import org.sirix.settings.Fixed;

import java.math.BigInteger;

public abstract class AbstractNumberNode extends AbstractStructForwardingNode implements ImmutableJsonNode {

  private final StructNodeDelegate structNodeDelegate;
  private Number number;

  private BigInteger hashCode;

  public AbstractNumberNode(StructNodeDelegate structNodeDel, Number number) {
    this.structNodeDelegate = structNodeDel;
    this.number = number;
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

      into.putInt(number.hashCode());
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
