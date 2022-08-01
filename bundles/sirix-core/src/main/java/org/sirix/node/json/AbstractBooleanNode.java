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
import org.sirix.settings.Constants;
import org.sirix.settings.Fixed;

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

      into.putBoolean(boolValue);
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
