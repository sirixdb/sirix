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
import net.openhft.chronicle.bytes.Bytes;
import org.brackit.xquery.atomic.QNm;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.jetbrains.annotations.NotNull;
import io.sirix.api.PageReadOnlyTrx;
import io.sirix.api.visitor.VisitResult;
import io.sirix.api.visitor.XmlNodeVisitor;
import io.sirix.node.NodeKind;
import io.sirix.node.SirixDeweyID;
import io.sirix.node.delegates.NameNodeDelegate;
import io.sirix.node.delegates.NodeDelegate;
import io.sirix.node.delegates.StructNodeDelegate;
import io.sirix.node.delegates.ValueNodeDelegate;
import io.sirix.node.immutable.xml.ImmutablePI;
import io.sirix.node.interfaces.NameNode;
import io.sirix.node.interfaces.ValueNode;
import io.sirix.node.interfaces.immutable.ImmutableXmlNode;
import io.sirix.settings.Constants;
import io.sirix.settings.Fixed;

import java.nio.ByteBuffer;

/**
 * <p>
 * Node representing a processing instruction.
 * </p>
 */
public final class PINode extends AbstractStructForwardingNode implements ValueNode, NameNode, ImmutableXmlNode {

  /** Delegate for name node information. */
  private final NameNodeDelegate nameNodeDelegate;

  /** Delegate for val node information. */
  private final ValueNodeDelegate valueNodeDelegate;

  /** Delegate for structural node information. */
  private final StructNodeDelegate structNodeDelegate;

  /** {@link PageReadOnlyTrx} reference. */
  private final PageReadOnlyTrx pageReadTrx;

  private long hash;

  /**
   * Creating a processing instruction.
   *
   * @param structNodeDelegate {@link StructNodeDelegate} to be set
   * @param nameNodeDelegate {@link NameNodeDelegate} to be set
   * @param valueNodeDelegate {@link ValueNodeDelegate} to be set
   */
  public PINode(final long hashCode, final StructNodeDelegate structNodeDelegate, final NameNodeDelegate nameNodeDelegate,
      final ValueNodeDelegate valueNodeDelegate, final PageReadOnlyTrx pageReadTrx) {
    hash = hashCode;
    assert structNodeDelegate != null : "structNodeDelegate must not be null!";
    this.structNodeDelegate = structNodeDelegate;
    assert nameNodeDelegate != null : "nameDel must not be null!";
    this.nameNodeDelegate = nameNodeDelegate;
    assert valueNodeDelegate != null : "valDel must not be null!";
    this.valueNodeDelegate = valueNodeDelegate;
    assert pageReadTrx != null : "pageReadTrx must not be null!";
    this.pageReadTrx = pageReadTrx;
  }

  /**
   * Creating a processing instruction.
   *
   * @param structNodeDelegate {@link StructNodeDelegate} to be set
   * @param nameNodeDelegate {@link NameNodeDelegate} to be set
   * @param valueNodeDelegate {@link ValueNodeDelegate} to be set
   *
   */
  public PINode(final StructNodeDelegate structNodeDelegate, final NameNodeDelegate nameNodeDelegate, final ValueNodeDelegate valueNodeDelegate,
      final PageReadOnlyTrx pageReadTrx) {
    assert structNodeDelegate != null : "structNodeDelegate must not be null!";
    this.structNodeDelegate = structNodeDelegate;
    assert nameNodeDelegate != null : "nameDel must not be null!";
    this.nameNodeDelegate = nameNodeDelegate;
    assert valueNodeDelegate != null : "valDel must not be null!";
    this.valueNodeDelegate = valueNodeDelegate;
    assert pageReadTrx != null : "pageReadTrx must not be null!";
    this.pageReadTrx = pageReadTrx;
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.PROCESSING_INSTRUCTION;
  }

  @Override
  public long computeHash(Bytes<ByteBuffer> bytes) {
    final var nodeDelegate = structNodeDelegate.getNodeDelegate();

    bytes.clear();

    bytes.writeLong(nodeDelegate.getNodeKey())
         .writeLong(nodeDelegate.getParentKey())
         .writeByte(nodeDelegate.getKind().getId());

    bytes.writeLong(structNodeDelegate.getChildCount())
         .writeLong(structNodeDelegate.getDescendantCount())
         .writeLong(structNodeDelegate.getLeftSiblingKey())
         .writeLong(structNodeDelegate.getRightSiblingKey())
         .writeLong(structNodeDelegate.getFirstChildKey());

    if (structNodeDelegate.getLastChildKey() != Fixed.INVALID_KEY_FOR_TYPE_CHECK.getStandardProperty()) {
      bytes.writeLong(structNodeDelegate.getLastChildKey());
    }

    bytes.writeLong(nameNodeDelegate.getPrefixKey())
         .writeLong(nameNodeDelegate.getLocalNameKey())
         .writeLong(nameNodeDelegate.getURIKey());

    bytes.writeUtf8(new String(valueNodeDelegate.getRawValue(), Constants.DEFAULT_ENCODING));

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
    if (hash == 0L) {
      hash = computeHash(Bytes.elasticHeapByteBuffer());
    }
    return hash;
  }

  @Override
  public VisitResult acceptVisitor(final XmlNodeVisitor visitor) {
    return visitor.visit(ImmutablePI.of(this));
  }

  @Override
  public @NotNull String toString() {
    return MoreObjects.toStringHelper(this)
                      .add("structDel", structNodeDelegate)
                      .add("nameDel", nameNodeDelegate)
                      .add("valDel", valueNodeDelegate)
                      .toString();
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
  public byte[] getRawValue() {
    return valueNodeDelegate.getRawValue();
  }

  @Override
  public void setRawValue(final byte[] value) {
    hash = 0L;
    valueNodeDelegate.setRawValue(value);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(nameNodeDelegate, valueNodeDelegate);
  }

  @Override
  public boolean equals(final @Nullable Object obj) {
    if (obj instanceof final PINode other) {
      return Objects.equal(nameNodeDelegate, other.nameNodeDelegate) && Objects.equal(valueNodeDelegate, other.valueNodeDelegate);
    }
    return false;
  }

  @Override
  public void setPathNodeKey(final @NonNegative long pathNodeKey) {
    hash = 0L;
    nameNodeDelegate.setPathNodeKey(pathNodeKey);
  }

  @Override
  public long getPathNodeKey() {
    return nameNodeDelegate.getPathNodeKey();
  }

  /**
   * Getting the inlying {@link NameNodeDelegate}.
   *
   * @return the {@link NameNodeDelegate} instance
   */
  public NameNodeDelegate getNameNodeDelegate() {
    return nameNodeDelegate;
  }

  /**
   * Getting the inlying {@link ValueNodeDelegate}.
   *
   * @return the {@link ValueNodeDelegate} instance
   */
  public ValueNodeDelegate getValNodeDelegate() {
    return valueNodeDelegate;
  }

  @Override
  protected @NotNull NodeDelegate delegate() {
    return structNodeDelegate.getNodeDelegate();
  }

  @Override
  protected StructNodeDelegate structDelegate() {
    return structNodeDelegate;
  }

  @Override
  public QNm getName() {
    final String uri = pageReadTrx.getName(nameNodeDelegate.getURIKey(), NodeKind.NAMESPACE);
    final int prefixKey = nameNodeDelegate.getPrefixKey();
    final String prefix = prefixKey == -1
        ? ""
        : pageReadTrx.getName(prefixKey, NodeKind.PROCESSING_INSTRUCTION);
    final int localNameKey = nameNodeDelegate.getLocalNameKey();
    final String localName = localNameKey == -1
        ? ""
        : pageReadTrx.getName(localNameKey, NodeKind.PROCESSING_INSTRUCTION);
    return new QNm(uri, prefix, localName);
  }

  @Override
  public String getValue() {
    return new String(valueNodeDelegate.getRawValue(), Constants.DEFAULT_ENCODING);
  }

  @Override
  public SirixDeweyID getDeweyID() {
    return structNodeDelegate.getNodeDelegate().getDeweyID();
  }

  @Override
  public int getTypeKey() {
    return structNodeDelegate.getNodeDelegate().getTypeKey();
  }

  @Override
  public byte[] getDeweyIDAsBytes() {
    return structNodeDelegate.getDeweyIDAsBytes();
  }
}
