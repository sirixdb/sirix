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

package org.sirix.node.xdm;

import java.util.Optional;
import javax.annotation.Nonnegative;
import javax.annotation.Nullable;
import org.brackit.xquery.atomic.QNm;
import org.sirix.api.visitor.VisitResult;
import org.sirix.api.visitor.XmlNodeVisitor;
import org.sirix.node.AbstractForwardingNode;
import org.sirix.node.Kind;
import org.sirix.node.SirixDeweyID;
import org.sirix.node.delegates.NameNodeDelegate;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.delegates.StructNodeDelegate;
import org.sirix.node.delegates.ValNodeDelegate;
import org.sirix.node.immutable.xdm.ImmutableAttributeNode;
import org.sirix.node.interfaces.NameNode;
import org.sirix.node.interfaces.ValueNode;
import org.sirix.node.interfaces.immutable.ImmutableXmlNode;
import org.sirix.settings.Constants;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

/**
 * <h1>AttributeNode</h1>
 *
 * <p>
 * Node representing an attribute.
 * </p>
 */
public final class AttributeNode extends AbstractForwardingNode implements ValueNode, NameNode, ImmutableXmlNode {

  /** Delegate for name node information. */
  private final NameNodeDelegate mNameDel;

  /** Delegate for val node information. */
  private final ValNodeDelegate mValDel;

  /** Node delegate. */
  private final NodeDelegate mNodeDel;

  /** The qualified name. */
  private final QNm mQNm;

  /**
   * Creating an attribute.
   *
   * @param nodeDel {@link NodeDelegate} to be set
   * @param nodeDel {@link StructNodeDelegate} to be set
   * @param valDel {@link ValNodeDelegate} to be set
   *
   */
  public AttributeNode(final NodeDelegate nodeDel, final NameNodeDelegate nameDel, final ValNodeDelegate valDel,
      final QNm qNm) {
    assert nodeDel != null : "nodeDel must not be null!";
    mNodeDel = nodeDel;
    assert nameDel != null : "nameDel must not be null!";
    mNameDel = nameDel;
    assert valDel != null : "valDel must not be null!";
    mValDel = valDel;
    assert qNm != null : "qNm must not be null!";
    mQNm = qNm;
  }

  @Override
  public Kind getKind() {
    return Kind.ATTRIBUTE;
  }

  @Override
  public VisitResult acceptVisitor(final XmlNodeVisitor visitor) {
    return visitor.visit(ImmutableAttributeNode.of(this));
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("nameDel", mNameDel).add("valDel", mValDel).toString();
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
    mNameDel.setPrefixKey(prefixKey);
  }

  @Override
  public void setLocalNameKey(final int localNameKey) {
    mNameDel.setLocalNameKey(localNameKey);
  }

  @Override
  public void setURIKey(final int uriKey) {
    mNameDel.setURIKey(uriKey);
  }

  @Override
  public byte[] getRawValue() {
    return mValDel.getRawValue();
  }

  @Override
  public void setValue(final byte[] pVal) {
    mValDel.setValue(pVal);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mNameDel, mValDel);
  }

  @Override
  public boolean equals(final @Nullable Object obj) {
    if (obj instanceof AttributeNode) {
      final AttributeNode other = (AttributeNode) obj;
      return Objects.equal(mNameDel, other.mNameDel) && Objects.equal(mValDel, other.mValDel);
    }
    return false;
  }

  @Override
  public void setPathNodeKey(final @Nonnegative long pathNodeKey) {
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
   * Getting the inlying {@link ValNodeDelegate}.
   *
   * @return the {@link ValNodeDelegate} instance
   */
  public ValNodeDelegate getValNodeDelegate() {
    return mValDel;
  }

  @Override
  protected NodeDelegate delegate() {
    return mNodeDel;
  }

  @Override
  public QNm getName() {
    return mQNm;
  }

  @Override
  public String getValue() {
    return new String(mValDel.getRawValue(), Constants.DEFAULT_ENCODING);
  }

  @Override
  public Optional<SirixDeweyID> getDeweyID() {
    return mNodeDel.getDeweyID();
  }

  @Override
  public int getTypeKey() {
    return mNodeDel.getTypeKey();
  }
}
