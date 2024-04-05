/*
 * Copyright (c) 2023, Sirix Contributors
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the <organization> nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.sirix.node.xml;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import io.brackit.query.atomic.QNm;
import io.sirix.api.visitor.VisitResult;
import io.sirix.api.visitor.XmlNodeVisitor;
import io.sirix.node.AbstractForwardingNode;
import io.sirix.node.NodeKind;
import io.sirix.node.SirixDeweyID;
import io.sirix.node.delegates.NameNodeDelegate;
import io.sirix.node.delegates.NodeDelegate;
import io.sirix.node.immutable.xml.ImmutableNamespace;
import io.sirix.node.interfaces.NameNode;
import io.sirix.node.interfaces.immutable.ImmutableXmlNode;
import net.openhft.chronicle.bytes.Bytes;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.nio.ByteBuffer;

/**
 * Node representing a namespace.
 */
public final class NamespaceNode extends AbstractForwardingNode implements NameNode, ImmutableXmlNode {

  /** Delegate for name node information. */
  private final NameNodeDelegate nameNodeDelegate;

  /** {@link NodeDelegate} reference. */
  private final NodeDelegate nodeDelegate;

  /** The qualified name. */
  private final QNm qNm;

  private long hash;

  /**
   * Constructor.
   *
   * @param nodeDel {@link NodeDelegate} reference
   * @param nameNodeDelegate {@link NameNodeDelegate} reference
   * @param qNm The qualified name.
   */
  public NamespaceNode(final NodeDelegate nodeDel, final NameNodeDelegate nameNodeDelegate, final QNm qNm) {
    assert nodeDel != null;
    assert nameNodeDelegate != null;
    this.nodeDelegate = nodeDel;
    this.nameNodeDelegate = nameNodeDelegate;
    this.qNm = qNm;
  }

  /**
   * Constructor.
   *
   * @param hashCode hash code
   * @param nodeDel {@link NodeDelegate} reference
   * @param nameNodeDelegate {@link NameNodeDelegate} reference
   * @param qNm The qualified name.
   */
  public NamespaceNode(final long hashCode, final NodeDelegate nodeDel, final NameNodeDelegate nameNodeDelegate,
      final QNm qNm) {
    assert nodeDel != null;
    assert nameNodeDelegate != null;
    hash = hashCode;
    this.nodeDelegate = nodeDel;
    this.nameNodeDelegate = nameNodeDelegate;
    this.qNm = qNm;
  }

  @Override
  public NamespaceNode clone() {
    return new NamespaceNode(nodeDelegate.clone(), nameNodeDelegate.clone(), qNm);
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.NAMESPACE;
  }

  @Override
  public long computeHash(Bytes<ByteBuffer> bytes) {
    bytes.clear();
    bytes.writeLong(nodeDelegate.getNodeKey())
         .writeLong(nodeDelegate.getParentKey())
         .writeByte(nodeDelegate.getKind().getId());

    bytes.writeLong(nameNodeDelegate.getPrefixKey())
         .writeLong(nameNodeDelegate.getLocalNameKey())
         .writeLong(nameNodeDelegate.getURIKey());

    final var buffer = bytes.underlyingObject().rewind();
    buffer.limit((int) bytes.readLimit());

    return nodeDelegate.getHashFunction().hashBytes(buffer);
  }

  @Override
  public void setHash(final long hash) {
    this.hash = hash;
  }

  @Override
  public long getHash() {
    return hash;
  }

  @Override
  public int getPrefixKey() {
    return nameNodeDelegate.getPrefixKey();
  }

  @Override
  public int getLocalNameKey() {
    return nameNodeDelegate.getLocalNameKey();
  }

  @Override
  public int getURIKey() {
    return nameNodeDelegate.getURIKey();
  }

  @Override
  public void setPrefixKey(final int prefixKey) {
    hash = 0L;
    nameNodeDelegate.setPrefixKey(prefixKey);
  }

  @Override
  public void setLocalNameKey(final int localNameKey) {
    hash = 0L;
    nameNodeDelegate.setLocalNameKey(localNameKey);
  }

  @Override
  public void setURIKey(final int uriKey) {
    hash = 0L;
    nameNodeDelegate.setURIKey(uriKey);
  }

  @Override
  public VisitResult acceptVisitor(final XmlNodeVisitor visitor) {
    return visitor.visit(ImmutableNamespace.of(this));
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(nodeDelegate, nameNodeDelegate);
  }

  @Override
  public boolean equals(final @Nullable Object obj) {
    if (obj instanceof final NamespaceNode other) {
      return Objects.equal(nodeDelegate, other.nodeDelegate) && Objects.equal(nameNodeDelegate, other.nameNodeDelegate);
    }
    return false;
  }

  @Override
  public @NonNull String toString() {
    return MoreObjects.toStringHelper(this).add("nodeDel", nodeDelegate).add("nameDel", nameNodeDelegate).toString();
  }

  @Override
  public void setPathNodeKey(final @NonNegative long pathNodeKey) {
    nameNodeDelegate.setPathNodeKey(pathNodeKey);
  }

  @Override
  public long getPathNodeKey() {
    return nameNodeDelegate.getPathNodeKey();
  }

  /**
   * Getting the inlying {@link NameNodeDelegate}.
   *
   * @return {@link NameNodeDelegate} instance
   */
  public NameNodeDelegate getNameNodeDelegate() {
    return nameNodeDelegate;
  }

  @Override
  protected @NonNull NodeDelegate delegate() {
    return nodeDelegate;
  }

  @Override
  public QNm getName() {
    return qNm;
  }

  @Override
  public SirixDeweyID getDeweyID() {
    return nodeDelegate.getDeweyID();
  }

  @Override
  public int getTypeKey() {
    return nodeDelegate.getTypeKey();
  }

  @Override
  public byte[] getDeweyIDAsBytes() {
    return nodeDelegate.getDeweyIDAsBytes();
  }
}
