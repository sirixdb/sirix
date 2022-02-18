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
  private final NameNodeDelegate mNameDel;

  /** Delegate for val node information. */
  private final ValueNodeDelegate mValDel;

  /** Delegate for structural node information. */
  private final StructNodeDelegate mStructNodeDel;

  /** {@link PageReadOnlyTrx} reference. */
  private final PageReadOnlyTrx mPageReadTrx;

  private BigInteger mHash;

  /**
   * Creating a processing instruction.
   *
   * @param structDel {@link StructNodeDelegate} to be set
   * @param nameDel {@link NameNodeDelegate} to be set
   * @param valDel {@link ValueNodeDelegate} to be set
   */
  public PINode(final BigInteger hashCode, final StructNodeDelegate structDel, final NameNodeDelegate nameDel,
      final ValueNodeDelegate valDel, final PageReadOnlyTrx pageReadTrx) {
    mHash = hashCode;
    assert structDel != null : "structDel must not be null!";
    mStructNodeDel = structDel;
    assert nameDel != null : "nameDel must not be null!";
    mNameDel = nameDel;
    assert valDel != null : "valDel must not be null!";
    mValDel = valDel;
    assert pageReadTrx != null : "pageReadTrx must not be null!";
    mPageReadTrx = pageReadTrx;
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
    mStructNodeDel = structDel;
    assert nameDel != null : "nameDel must not be null!";
    mNameDel = nameDel;
    assert valDel != null : "valDel must not be null!";
    mValDel = valDel;
    assert pageReadTrx != null : "pageReadTrx must not be null!";
    mPageReadTrx = pageReadTrx;
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.PROCESSING_INSTRUCTION;
  }

  @Override
  public BigInteger computeHash() {
    BigInteger result = BigInteger.ONE;

    result = BigInteger.valueOf(31).multiply(result).add(mStructNodeDel.getNodeDelegate().computeHash());
    result = BigInteger.valueOf(31).multiply(result).add(mStructNodeDel.computeHash());
    result = BigInteger.valueOf(31).multiply(result).add(mNameDel.computeHash());
    result = BigInteger.valueOf(31).multiply(result).add(mValDel.computeHash());

    return Node.to128BitsAtMaximumBigInteger(result);
  }

  @Override
  public void setHash(final BigInteger hash) {
    mHash = Node.to128BitsAtMaximumBigInteger(hash);
  }

  @Override
  public BigInteger getHash() {
    return mHash;
  }

  @Override
  public VisitResult acceptVisitor(final XmlNodeVisitor visitor) {
    return visitor.visit(ImmutablePI.of(this));
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
                      .add("structDel", mStructNodeDel)
                      .add("nameDel", mNameDel)
                      .add("valDel", mValDel)
                      .toString();
  }

  @Override
  public int getPrefixKey() {
    return mNameDel.getPrefixKey();
  }

  @Override
  public int getLocalNameKey() {
    return mNameDel.getLocalNameKey();
  }

  @Override
  public int getURIKey() {
    return mNameDel.getURIKey();
  }

  @Override
  public void setPrefixKey(final int prefixKey) {
    mHash = null;
    mNameDel.setPrefixKey(prefixKey);
  }

  @Override
  public void setLocalNameKey(final int localNameKey) {
    mHash = null;
    mNameDel.setLocalNameKey(localNameKey);
  }

  @Override
  public void setURIKey(final int uriKey) {
    mHash = null;
    mNameDel.setURIKey(uriKey);
  }

  @Override
  public byte[] getRawValue() {
    return mValDel.getRawValue();
  }

  @Override
  public void setValue(final byte[] value) {
    mHash = null;
    mValDel.setValue(value);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mNameDel, mValDel);
  }

  @Override
  public boolean equals(final @Nullable Object obj) {
    if (obj instanceof PINode) {
      final PINode other = (PINode) obj;
      return Objects.equal(mNameDel, other.mNameDel) && Objects.equal(mValDel, other.mValDel);
    }
    return false;
  }

  @Override
  public void setPathNodeKey(final @NonNegative long pathNodeKey) {
    mHash = null;
    mNameDel.setPathNodeKey(pathNodeKey);
  }

  @Override
  public long getPathNodeKey() {
    return mNameDel.getPathNodeKey();
  }

  /**
   * Getting the inlying {@link NameNodeDelegate}.
   *
   * @return the {@link NameNodeDelegate} instance
   */
  public NameNodeDelegate getNameNodeDelegate() {
    return mNameDel;
  }

  /**
   * Getting the inlying {@link ValueNodeDelegate}.
   *
   * @return the {@link ValueNodeDelegate} instance
   */
  public ValueNodeDelegate getValNodeDelegate() {
    return mValDel;
  }

  @Override
  protected NodeDelegate delegate() {
    return mStructNodeDel.getNodeDelegate();
  }

  @Override
  protected StructNodeDelegate structDelegate() {
    return mStructNodeDel;
  }

  @Override
  public QNm getName() {
    final String uri = mPageReadTrx.getName(mNameDel.getURIKey(), NodeKind.NAMESPACE);
    final int prefixKey = mNameDel.getPrefixKey();
    final String prefix = prefixKey == -1
        ? ""
        : mPageReadTrx.getName(prefixKey, NodeKind.PROCESSING_INSTRUCTION);
    final int localNameKey = mNameDel.getLocalNameKey();
    final String localName = localNameKey == -1
        ? ""
        : mPageReadTrx.getName(localNameKey, NodeKind.PROCESSING_INSTRUCTION);
    return new QNm(uri, prefix, localName);
  }

  @Override
  public String getValue() {
    return new String(mValDel.getRawValue(), Constants.DEFAULT_ENCODING);
  }

  @Override
  public SirixDeweyID getDeweyID() {
    return mStructNodeDel.getNodeDelegate().getDeweyID();
  }

  @Override
  public int getTypeKey() {
    return mStructNodeDel.getNodeDelegate().getTypeKey();
  }
}
