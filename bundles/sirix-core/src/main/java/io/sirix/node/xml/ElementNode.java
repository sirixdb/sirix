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
import io.sirix.node.NodeKind;
import io.sirix.node.SirixDeweyID;
import io.sirix.node.interfaces.NameNode;
import it.unimi.dsi.fastutil.longs.LongList;
import net.openhft.chronicle.bytes.Bytes;
import org.brackit.xquery.atomic.QNm;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.jetbrains.annotations.NotNull;
import io.sirix.api.visitor.VisitResult;
import io.sirix.api.visitor.XmlNodeVisitor;
import io.sirix.node.delegates.NameNodeDelegate;
import io.sirix.node.delegates.NodeDelegate;
import io.sirix.node.delegates.StructNodeDelegate;
import io.sirix.node.immutable.xml.ImmutableElement;
import io.sirix.node.interfaces.immutable.ImmutableXmlNode;
import io.sirix.settings.Fixed;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

/**
 * <p>
 * Node representing an XML element.
 * </p>
 *
 * <strong>This class is not part of the public API and might change.</strong>
 */
public final class ElementNode extends AbstractStructForwardingNode implements NameNode, ImmutableXmlNode {

  /**
   * Delegate for name node information.
   */
  private final NameNodeDelegate nameNodeDelegate;

  /**
   * Keys of attributes.
   */
  private final LongList attributeKeys;

  /**
   * Keys of namespace declarations.
   */
  private final LongList namespaceKeys;

  /**
   * {@link StructNodeDelegate} reference.
   */
  private final StructNodeDelegate structNodeDelegate;

  /**
   * The qualified name.
   */
  private final QNm qNm;

  private long hash;

  /**
   * Constructor
   *
   * @param structDel        {@link StructNodeDelegate} to be set
   * @param nameNodeDelegate {@link NameNodeDelegate} to be set
   * @param attributeKeys    list of attribute keys
   * @param namespaceKeys    keys of namespaces to be set
   */
  public ElementNode(final long hashCode, final StructNodeDelegate structDel, final NameNodeDelegate nameNodeDelegate,
      final LongList attributeKeys, final LongList namespaceKeys, final QNm qNm) {
    hash = hashCode;
    assert structDel != null;
    structNodeDelegate = structDel;
    assert nameNodeDelegate != null;
    this.nameNodeDelegate = nameNodeDelegate;
    assert attributeKeys != null;
    this.attributeKeys = attributeKeys;
    assert namespaceKeys != null;
    this.namespaceKeys = namespaceKeys;
    assert qNm != null;
    this.qNm = qNm;
  }

  /**
   * Constructor
   *
   * @param structDel        {@link StructNodeDelegate} to be set
   * @param nameNodeDelegate {@link NameNodeDelegate} to be set
   * @param attributeKeys    list of attribute keys
   * @param namespaceKeys    keys of namespaces to be set
   */
  public ElementNode(final StructNodeDelegate structDel, final NameNodeDelegate nameNodeDelegate,
      final LongList attributeKeys, final LongList namespaceKeys, final QNm qNm) {
    assert structDel != null;
    structNodeDelegate = structDel;
    assert nameNodeDelegate != null;
    this.nameNodeDelegate = nameNodeDelegate;
    assert attributeKeys != null;
    this.attributeKeys = attributeKeys;
    assert namespaceKeys != null;
    this.namespaceKeys = namespaceKeys;
    assert qNm != null;
    this.qNm = qNm;
  }

  /**
   * Getting the count of attributes.
   *
   * @return the count of attributes
   */
  public int getAttributeCount() {
    return attributeKeys.size();
  }

  /**
   * Getting the attribute key for an given index.
   *
   * @param index index of the attribute
   * @return the attribute key
   */
  public long getAttributeKey(final @NonNegative int index) {
    if (attributeKeys.size() <= index) {
      return Fixed.NULL_NODE_KEY.getStandardProperty();
    }
    return attributeKeys.getLong(index);
  }

  /**
   * Inserting an attribute.
   *
   * @param attrKey the new attribute key
   */
  public void insertAttribute(final @NonNegative long attrKey) {
    attributeKeys.add(attrKey);
  }

  /**
   * Removing an attribute.
   *
   * @param attrNodeKey the key of the attribute to be removed
   */
  public void removeAttribute(final @NonNegative long attrNodeKey) {
    attributeKeys.removeIf(key -> key == attrNodeKey);
  }

  /**
   * Getting the count of namespaces.
   *
   * @return the count of namespaces
   */
  public int getNamespaceCount() {
    return namespaceKeys.size();
  }

  /**
   * Getting the namespace key for a given index.
   *
   * @param namespaceKey index of the namespace
   * @return the namespace key
   */
  public long getNamespaceKey(final @NonNegative int namespaceKey) {
    if (namespaceKeys.size() <= namespaceKey) {
      return Fixed.NULL_NODE_KEY.getStandardProperty();
    }
    return namespaceKeys.getLong(namespaceKey);
  }

  /**
   * Inserting a namespace.
   *
   * @param namespaceKey new namespace key
   */
  public void insertNamespace(final long namespaceKey) {
    namespaceKeys.add(namespaceKey);
  }

  /**
   * Removing a namepsace.
   *
   * @param namespaceKey the key of the namespace to be removed
   */
  public void removeNamespace(final long namespaceKey) {
    namespaceKeys.removeIf(key -> key == namespaceKey);
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
    nameNodeDelegate.setPrefixKey(prefixKey);
  }

  @Override
  public void setLocalNameKey(final int localNameKey) {
    nameNodeDelegate.setLocalNameKey(localNameKey);
  }

  @Override
  public void setURIKey(final int uriKey) {
    nameNodeDelegate.setURIKey(uriKey);
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.ELEMENT;
  }

  @Override
  public @NotNull String toString() {
    return MoreObjects.toStringHelper(this)
                      .add("nameDelegate", nameNodeDelegate)
                      .add("nameSpaceKeys", namespaceKeys)
                      .add("attributeKeys", attributeKeys)
                      .add("structDelegate", structNodeDelegate)
                      .toString();
  }

  @Override
  public VisitResult acceptVisitor(final XmlNodeVisitor visitor) {
    return visitor.visit(ImmutableElement.of(this));
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

    bytes.writeInt(nameNodeDelegate.getPrefixKey())
         .writeInt(nameNodeDelegate.getLocalNameKey())
         .writeInt(nameNodeDelegate.getURIKey());

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
  public int hashCode() {
    return Objects.hashCode(delegate(), nameNodeDelegate);
  }

  @Override
  public boolean equals(final Object obj) {
    return obj instanceof final ElementNode other && Objects.equal(delegate(), other.delegate()) && Objects.equal(
        nameNodeDelegate,
        other.nameNodeDelegate);
  }

  /**
   * Get a {@link List} with all attribute keys.
   *
   * @return unmodifiable view of {@link List} with all attribute keys
   */
  public List<Long> getAttributeKeys() {
    return Collections.unmodifiableList(attributeKeys);
  }

  /**
   * Get a {@link List} with all namespace keys.
   *
   * @return unmodifiable view of {@link List} with all namespace keys
   */
  public List<Long> getNamespaceKeys() {
    return Collections.unmodifiableList(namespaceKeys);
  }

  @Override
  protected @NotNull NodeDelegate delegate() {
    return structNodeDelegate.getNodeDelegate();
  }

  @Override
  protected StructNodeDelegate structDelegate() {
    return structNodeDelegate;
  }

  /**
   * Get name node delegate.
   *
   * @return snapshot of the name node delegate (new instance)
   */
  @NonNull
  public NameNodeDelegate getNameNodeDelegate() {
    return new NameNodeDelegate(nameNodeDelegate);
  }

  @Override
  public void setPathNodeKey(final @NonNegative long pathNodeKey) {
    nameNodeDelegate.setPathNodeKey(pathNodeKey);
  }

  @Override
  public long getPathNodeKey() {
    return nameNodeDelegate.getPathNodeKey();
  }

  @Override
  public QNm getName() {
    return qNm;
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
