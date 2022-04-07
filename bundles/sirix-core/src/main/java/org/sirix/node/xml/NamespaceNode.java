/**
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met: * Redistributions of source code must retain the
 * above copyright notice, this list of conditions and the following disclaimer. * Redistributions
 * in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.sirix.node.xml;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import org.brackit.xquery.atomic.QNm;
import org.sirix.api.visitor.VisitResult;
import org.sirix.api.visitor.XmlNodeVisitor;
import org.sirix.node.AbstractForwardingNode;
import org.sirix.node.NodeKind;
import org.sirix.node.SirixDeweyID;
import org.sirix.node.delegates.NameNodeDelegate;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.immutable.xml.ImmutableNamespace;
import org.sirix.node.interfaces.NameNode;
import org.sirix.node.interfaces.Node;
import org.sirix.node.interfaces.immutable.ImmutableXmlNode;

import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.Nullable;
import java.math.BigInteger;

/**
 * <p>
 * Node representing a namespace.
 * </p>
 */
public final class NamespaceNode extends AbstractForwardingNode implements NameNode, ImmutableXmlNode {

  /** Delegate for name node information. */
  private final NameNodeDelegate nameDel;

  /** {@link NodeDelegate} reference. */
  private final NodeDelegate nodeDel;

  /** The qualified name. */
  private final QNm qNm;

  private BigInteger hash;

  /**
   * Constructor.
   *
   * @param nodeDel {@link NodeDelegate} reference
   * @param nameDel {@link NameNodeDelegate} reference
   * @param qNm The qualified name.
   */
  public NamespaceNode(final NodeDelegate nodeDel, final NameNodeDelegate nameDel, final QNm qNm) {
    assert nodeDel != null;
    assert nameDel != null;
    assert qNm != null;
    this.nodeDel = nodeDel;
    this.nameDel = nameDel;
    this.qNm = qNm;
  }

  /**
   * Constructor.
   *
   * @param hashCode hash code
   * @param nodeDel {@link NodeDelegate} reference
   * @param nameDel {@link NameNodeDelegate} reference
   * @param qNm The qualified name.
   */
  public NamespaceNode(final BigInteger hashCode, final NodeDelegate nodeDel, final NameNodeDelegate nameDel,
      final QNm qNm) {
    assert nodeDel != null;
    assert nameDel != null;
    assert qNm != null;
    hash = hashCode;
    this.nodeDel = nodeDel;
    this.nameDel = nameDel;
    this.qNm = qNm;
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.NAMESPACE;
  }

  @Override
  public BigInteger computeHash() {
    BigInteger result = BigInteger.ONE;

    result = BigInteger.valueOf(31).multiply(result).add(nodeDel.computeHash());
    result = BigInteger.valueOf(31).multiply(result).add(nameDel.computeHash());

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
  public VisitResult acceptVisitor(final XmlNodeVisitor visitor) {
    return visitor.visit(ImmutableNamespace.of(this));
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(nodeDel, nameDel);
  }

  @Override
  public boolean equals(final @Nullable Object obj) {
    if (obj instanceof NamespaceNode) {
      final NamespaceNode other = (NamespaceNode) obj;
      return Objects.equal(nodeDel, other.nodeDel) && Objects.equal(nameDel, other.nameDel);
    }
    return false;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("nodeDel", nodeDel).add("nameDel", nameDel).toString();
  }

  @Override
  public void setPathNodeKey(final @NonNegative long pathNodeKey) {
    nameDel.setPathNodeKey(pathNodeKey);
  }

  @Override
  public long getPathNodeKey() {
    return nameDel.getPathNodeKey();
  }

  /**
   * Getting the inlying {@link NameNodeDelegate}.
   *
   * @return {@link NameNodeDelegate} instance
   */
  public NameNodeDelegate getNameNodeDelegate() {
    return nameDel;
  }

  @Override
  protected NodeDelegate delegate() {
    return nodeDel;
  }

  @Override
  public QNm getName() {
    return qNm;
  }

  @Override
  public SirixDeweyID getDeweyID() {
    return nodeDel.getDeweyID();
  }

  @Override
  public int getTypeKey() {
    return nodeDel.getTypeKey();
  }

  @Override
  public byte[] getDeweyIDAsBytes() {
    return nodeDel.getDeweyIDAsBytes();
  }
}
