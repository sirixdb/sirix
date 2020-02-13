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

import static com.google.common.base.Preconditions.checkNotNull;
import java.math.BigInteger;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.sirix.api.visitor.VisitResult;
import org.sirix.api.visitor.XmlNodeVisitor;
import org.sirix.node.NodeKind;
import org.sirix.node.SirixDeweyID;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.delegates.StructNodeDelegate;
import org.sirix.node.immutable.xdm.ImmutableDocumentNode;
import org.sirix.node.interfaces.Node;
import org.sirix.node.interfaces.StructNode;
import org.sirix.node.interfaces.immutable.ImmutableXmlNode;
import com.google.common.base.Objects;

/**
 * <h1>DocumentNode</h1>
 *
 * <p>
 * Node representing the root of a document. This node is guaranteed to exist in revision 0 and can
 * not be removed.
 * </p>
 */
public final class XmlDocumentRootNode extends AbstractStructForwardingNode implements StructNode, ImmutableXmlNode {

  /** {@link NodeDelegate} reference. */
  private final NodeDelegate mNodeDel;

  /** {@link StructNodeDelegate} reference. */
  private final StructNodeDelegate mStructNodeDel;

  private BigInteger mHash;

  /**
   * Constructor.
   *
   * @param nodeDel {@link NodeDelegate} reference
   * @param structDel {@link StructNodeDelegate} reference
   */
  public XmlDocumentRootNode(final @Nonnull NodeDelegate nodeDel, final @Nonnull StructNodeDelegate structDel) {
    mNodeDel = checkNotNull(nodeDel);
    mStructNodeDel = checkNotNull(structDel);
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.XML_DOCUMENT;
  }

  @Override
  public BigInteger computeHash() {
    BigInteger result = BigInteger.ONE;

    result = BigInteger.valueOf(31).multiply(result).add(mStructNodeDel.getNodeDelegate().computeHash());
    result = BigInteger.valueOf(31).multiply(result).add(mStructNodeDel.computeHash());

    return Node.to128BitsAtMaximumBigInteger(result);
  }

  @Override
  public void setHash(final BigInteger hash) {
    mHash = Node.to128BitsAtMaximumBigInteger(hash);
  }

  @Override
  public BigInteger getHash() {
    if (mHash == null)
      mHash = Node.to128BitsAtMaximumBigInteger(computeHash());
    return mHash;
  }

  @Override
  public VisitResult acceptVisitor(final XmlNodeVisitor visitor) {
    return visitor.visit(ImmutableDocumentNode.of(this));
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mNodeDel);
  }

  @Override
  public boolean equals(@Nullable final Object obj) {
    if (obj instanceof XmlDocumentRootNode) {
      final XmlDocumentRootNode other = (XmlDocumentRootNode) obj;
      return Objects.equal(mNodeDel, other.mNodeDel);
    }
    return false;
  }

  @Override
  public String toString() {
    return super.toString();
  }

  @Override
  protected NodeDelegate delegate() {
    return mNodeDel;
  }

  @Override
  protected StructNodeDelegate structDelegate() {
    return mStructNodeDel;
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
