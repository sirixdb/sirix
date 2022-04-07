package org.sirix.node.xml;

import java.math.BigInteger;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.brackit.xquery.atomic.QNm;
import org.sirix.api.PageReadOnlyTrx;
import org.sirix.api.visitor.VisitResult;
import org.sirix.api.visitor.XmlNodeVisitor;
import org.sirix.node.NodeKind;
import org.sirix.node.SirixDeweyID;
import org.sirix.node.delegates.NameNodeDelegate;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.delegates.StructNodeDelegate;
import org.sirix.node.delegates.ValueNodeDelegate;
import org.sirix.node.immutable.xml.ImmutablePI;
import org.sirix.node.interfaces.NameNode;
import org.sirix.node.interfaces.Node;
import org.sirix.node.interfaces.ValueNode;
import org.sirix.node.interfaces.immutable.ImmutableXmlNode;
import org.sirix.settings.Constants;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

/**
 * <p>
 * Node representing a processing instruction.
 * </p>
 */
public final class PINode extends AbstractStructForwardingNode implements ValueNode, NameNode, ImmutableXmlNode {

  /** Delegate for name node information. */
  private final NameNodeDelegate nameDel;

  /** Delegate for val node information. */
  private final ValueNodeDelegate valDel;

  /** Delegate for structural node information. */
  private final StructNodeDelegate structNodeDel;

  /** {@link PageReadOnlyTrx} reference. */
  private final PageReadOnlyTrx pageReadTrx;

  private BigInteger hash;

  /**
   * Creating a processing instruction.
   *
   * @param structDel {@link StructNodeDelegate} to be set
   * @param nameDel {@link NameNodeDelegate} to be set
   * @param valDel {@link ValueNodeDelegate} to be set
   */
  public PINode(final BigInteger hashCode, final StructNodeDelegate structDel, final NameNodeDelegate nameDel,
      final ValueNodeDelegate valDel, final PageReadOnlyTrx pageReadTrx) {
    hash = hashCode;
    assert structDel != null : "structDel must not be null!";
    structNodeDel = structDel;
    assert nameDel != null : "nameDel must not be null!";
    this.nameDel = nameDel;
    assert valDel != null : "valDel must not be null!";
    this.valDel = valDel;
    assert pageReadTrx != null : "pageReadTrx must not be null!";
    this.pageReadTrx = pageReadTrx;
  }

  /**
   * Creating a processing instruction.
   *
   * @param structDel {@link StructNodeDelegate} to be set
   * @param nameDel {@link NameNodeDelegate} to be set
   * @param valDel {@link ValueNodeDelegate} to be set
   *
   */
  public PINode(final StructNodeDelegate structDel, final NameNodeDelegate nameDel, final ValueNodeDelegate valDel,
      final PageReadOnlyTrx pageReadTrx) {
    assert structDel != null : "structDel must not be null!";
    structNodeDel = structDel;
    assert nameDel != null : "nameDel must not be null!";
    this.nameDel = nameDel;
    assert valDel != null : "valDel must not be null!";
    this.valDel = valDel;
    assert pageReadTrx != null : "pageReadTrx must not be null!";
    this.pageReadTrx = pageReadTrx;
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.PROCESSING_INSTRUCTION;
  }

  @Override
  public BigInteger computeHash() {
    BigInteger result = BigInteger.ONE;

    result = BigInteger.valueOf(31).multiply(result).add(structNodeDel.getNodeDelegate().computeHash());
    result = BigInteger.valueOf(31).multiply(result).add(structNodeDel.computeHash());
    result = BigInteger.valueOf(31).multiply(result).add(nameDel.computeHash());
    result = BigInteger.valueOf(31).multiply(result).add(valDel.computeHash());

    return Node.to128BitsAtMaximumBigInteger(result);
  }

  @Override
  public void setHash(final BigInteger hash) {
    this.hash = Node.to128BitsAtMaximumBigInteger(hash);
  }

  @Override
  public BigInteger getHash() {
    return hash;
  }

  @Override
  public VisitResult acceptVisitor(final XmlNodeVisitor visitor) {
    return visitor.visit(ImmutablePI.of(this));
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
                      .add("structDel", structNodeDel)
                      .add("nameDel", nameDel)
                      .add("valDel", valDel)
                      .toString();
  }

  @Override
  public int getPrefixKey() {
    return nameDel.getPrefixKey();
  }

  @Override
  public int getLocalNameKey() {
    return nameDel.getLocalNameKey();
  }

  @Override
  public int getURIKey() {
    return nameDel.getURIKey();
  }

  @Override
  public void setPrefixKey(final int prefixKey) {
    hash = null;
    nameDel.setPrefixKey(prefixKey);
  }

  @Override
  public void setLocalNameKey(final int localNameKey) {
    hash = null;
    nameDel.setLocalNameKey(localNameKey);
  }

  @Override
  public void setURIKey(final int uriKey) {
    hash = null;
    nameDel.setURIKey(uriKey);
  }

  @Override
  public byte[] getRawValue() {
    return valDel.getRawValue();
  }

  @Override
  public void setValue(final byte[] value) {
    hash = null;
    valDel.setValue(value);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(nameDel, valDel);
  }

  @Override
  public boolean equals(final @Nullable Object obj) {
    if (obj instanceof PINode) {
      final PINode other = (PINode) obj;
      return Objects.equal(nameDel, other.nameDel) && Objects.equal(valDel, other.valDel);
    }
    return false;
  }

  @Override
  public void setPathNodeKey(final @NonNegative long pathNodeKey) {
    hash = null;
    nameDel.setPathNodeKey(pathNodeKey);
  }

  @Override
  public long getPathNodeKey() {
    return nameDel.getPathNodeKey();
  }

  /**
   * Getting the inlying {@link NameNodeDelegate}.
   *
   * @return the {@link NameNodeDelegate} instance
   */
  public NameNodeDelegate getNameNodeDelegate() {
    return nameDel;
  }

  /**
   * Getting the inlying {@link ValueNodeDelegate}.
   *
   * @return the {@link ValueNodeDelegate} instance
   */
  public ValueNodeDelegate getValNodeDelegate() {
    return valDel;
  }

  @Override
  protected NodeDelegate delegate() {
    return structNodeDel.getNodeDelegate();
  }

  @Override
  protected StructNodeDelegate structDelegate() {
    return structNodeDel;
  }

  @Override
  public QNm getName() {
    final String uri = pageReadTrx.getName(nameDel.getURIKey(), NodeKind.NAMESPACE);
    final int prefixKey = nameDel.getPrefixKey();
    final String prefix = prefixKey == -1
        ? ""
        : pageReadTrx.getName(prefixKey, NodeKind.PROCESSING_INSTRUCTION);
    final int localNameKey = nameDel.getLocalNameKey();
    final String localName = localNameKey == -1
        ? ""
        : pageReadTrx.getName(localNameKey, NodeKind.PROCESSING_INSTRUCTION);
    return new QNm(uri, prefix, localName);
  }

  @Override
  public String getValue() {
    return new String(valDel.getRawValue(), Constants.DEFAULT_ENCODING);
  }

  @Override
  public SirixDeweyID getDeweyID() {
    return structNodeDel.getNodeDelegate().getDeweyID();
  }

  @Override
  public int getTypeKey() {
    return structNodeDel.getNodeDelegate().getTypeKey();
  }

  @Override
  public byte[] getDeweyIDAsBytes() {
    return structNodeDel.getDeweyIDAsBytes();
  }
}
