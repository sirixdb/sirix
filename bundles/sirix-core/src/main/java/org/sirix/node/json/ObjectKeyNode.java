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

package org.sirix.node.json;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import net.openhft.chronicle.bytes.Bytes;
import org.brackit.xquery.atomic.QNm;
import org.checkerframework.checker.index.qual.NonNegative;
import org.jetbrains.annotations.NotNull;
import org.sirix.api.visitor.JsonNodeVisitor;
import org.sirix.api.visitor.VisitResult;
import org.sirix.node.NodeKind;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.delegates.StructNodeDelegate;
import org.sirix.node.immutable.json.ImmutableObjectKeyNode;
import org.sirix.node.interfaces.immutable.ImmutableJsonNode;
import org.sirix.node.interfaces.immutable.ImmutableNameNode;
import org.sirix.node.xml.AbstractStructForwardingNode;
import org.sirix.settings.Fixed;

import java.nio.ByteBuffer;

/**
 * Node representing an object key/field.
 *
 * <strong>This class is not part of the public API and might change.</strong>
 */
public final class ObjectKeyNode extends AbstractStructForwardingNode implements ImmutableJsonNode, ImmutableNameNode {

  /**
   * {@link StructNodeDelegate} reference.
   */
  private final StructNodeDelegate structNodeDelegate;

  private int nameKey;

  private String name;

  private long pathNodeKey;

  private long hash;

  /**
   * Constructor
   *
   * @param structDel {@link StructNodeDelegate} to be set
   * @param name      the key name
   */
  public ObjectKeyNode(final StructNodeDelegate structDel, final int nameKey, final String name,
      final long pathNodeKey) {
    assert structDel != null;
    assert name != null;
    structNodeDelegate = structDel;
    this.nameKey = nameKey;
    this.name = name;
    this.pathNodeKey = pathNodeKey;
  }

  /**
   * Constructor
   *
   * @param hashCode    the hash code
   * @param structDel   {@link StructNodeDelegate} to be set
   * @param nameKey     the key of the name
   * @param name        the String name
   * @param pathNodeKey the path node key
   */
  public ObjectKeyNode(final long hashCode, final StructNodeDelegate structDel, final int nameKey, final String name,
      final long pathNodeKey) {
    hash = hashCode;
    assert structDel != null;
    structNodeDelegate = structDel;
    this.nameKey = nameKey;
    this.name = name;
    this.pathNodeKey = pathNodeKey;
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.OBJECT_KEY;
  }

  @Override
  public long computeHash(Bytes<ByteBuffer> bytes) {
    final var nodeDelegate = structNodeDelegate.getNodeDelegate();

    bytes.clear();
    bytes.writeLong(nodeDelegate.getNodeKey())
         .writeLong(nodeDelegate.getParentKey())
         .writeByte(nodeDelegate.getKind().getId());

    bytes.writeLong(structNodeDelegate.getDescendantCount())
         .writeLong(structNodeDelegate.getLeftSiblingKey())
         .writeLong(structNodeDelegate.getRightSiblingKey())
         .writeLong(structNodeDelegate.getFirstChildKey());

    if (structNodeDelegate.getLastChildKey() != Fixed.INVALID_KEY_FOR_TYPE_CHECK.getStandardProperty()) {
      bytes.writeLong(structNodeDelegate.getLastChildKey());
    }

    bytes.writeInt(nameKey);

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

  public int getNameKey() {
    return nameKey;
  }

  public void setNameKey(final int nameKey) {
    this.nameKey = nameKey;
  }

  public void setName(final String name) {
    this.name = name;
  }

  @Override
  public VisitResult acceptVisitor(final JsonNodeVisitor visitor) {
    return visitor.visit(ImmutableObjectKeyNode.of(this));
  }

  @Override
  public @NotNull String toString() {
    return MoreObjects.toStringHelper(this)
                      .add("name", name)
                      .add("nameKey", nameKey)
                      .add("structDelegate", structNodeDelegate)
                      .toString();
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(name, nameKey, delegate());
  }

  @Override
  public boolean equals(final Object obj) {
    if (!(obj instanceof final ObjectKeyNode other))
      return false;

    return Objects.equal(name, other.name) && nameKey == other.nameKey && Objects.equal(delegate(), other.delegate());
  }

  @Override
  protected @NotNull NodeDelegate delegate() {
    return structNodeDelegate.getNodeDelegate();
  }

  @Override
  protected StructNodeDelegate structDelegate() {
    return structNodeDelegate;
  }

  public ObjectKeyNode setPathNodeKey(final @NonNegative long pathNodeKey) {
    this.pathNodeKey = pathNodeKey;
    return this;
  }

  @Override
  public int getLocalNameKey() {
    return nameKey;
  }

  @Override
  public int getPrefixKey() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getURIKey() {
    throw new UnsupportedOperationException();
  }

  public long getPathNodeKey() {
    return pathNodeKey;
  }

  public QNm getName() {
    return new QNm(name);
  }
}
