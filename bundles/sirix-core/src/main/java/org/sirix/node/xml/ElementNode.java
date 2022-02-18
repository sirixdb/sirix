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
import com.google.common.collect.BiMap;
import org.brackit.xquery.atomic.QNm;
import org.sirix.api.visitor.VisitResult;
import org.sirix.api.visitor.XmlNodeVisitor;
import org.sirix.node.NodeKind;
import org.sirix.node.SirixDeweyID;
import org.sirix.node.delegates.NameNodeDelegate;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.delegates.StructNodeDelegate;
import org.sirix.node.immutable.xml.ImmutableElement;
import org.sirix.node.interfaces.NameNode;
import org.sirix.node.interfaces.Node;
import org.sirix.node.interfaces.immutable.ImmutableXmlNode;
import org.sirix.settings.Fixed;
import org.sirix.utils.NamePageHash;

import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import java.math.BigInteger;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * <p>
 * Node representing an XML element.
 * </p>
 *
 * <strong>This class is not part of the public API and might change.</strong>
 */
public final class ElementNode extends AbstractStructForwardingNode implements NameNode, ImmutableXmlNode {

  /** Delegate for name node information. */
  private final NameNodeDelegate mNameDel;

  /** Mapping names/keys. */
  private final BiMap<Long, Long> mAttributes;

  /** Keys of attributes. */
  private final List<Long> mAttributeKeys;

  /** Keys of namespace declarations. */
  private final List<Long> mNamespaceKeys;

  /** {@link StructNodeDelegate} reference. */
  private final StructNodeDelegate structNodeDel;

  /** The qualified name. */
  private final QNm mQNm;

  private BigInteger mHash;

  /**
   * Constructor
   *
   * @param structDel {@link StructNodeDelegate} to be set
   * @param nameDel {@link NameNodeDelegate} to be set
   * @param attributeKeys list of attribute keys
   * @param attributes attribute nameKey / nodeKey mapping in both directions
   * @param namespaceKeys keys of namespaces to be set
   */
  public ElementNode(final BigInteger hashCode, final StructNodeDelegate structDel, final NameNodeDelegate nameDel, final List<Long> attributeKeys,
      final BiMap<Long, Long> attributes, final List<Long> namespaceKeys, final QNm qNm) {
    mHash = hashCode;
    assert structDel != null;
    structNodeDel = structDel;
    assert nameDel != null;
    mNameDel = nameDel;
    assert attributeKeys != null;
    mAttributeKeys = attributeKeys;
    assert attributes != null;
    mAttributes = attributes;
    assert namespaceKeys != null;
    mNamespaceKeys = namespaceKeys;
    assert qNm != null;
    mQNm = qNm;
  }

  /**
   * Constructor
   *
   * @param structDel {@link StructNodeDelegate} to be set
   * @param nameDel {@link NameNodeDelegate} to be set
   * @param attributeKeys list of attribute keys
   * @param attributes attribute nameKey / nodeKey mapping in both directions
   * @param namespaceKeys keys of namespaces to be set
   */
  public ElementNode(final StructNodeDelegate structDel, final NameNodeDelegate nameDel, final List<Long> attributeKeys,
      final BiMap<Long, Long> attributes, final List<Long> namespaceKeys, final QNm qNm) {
    assert structDel != null;
    structNodeDel = structDel;
    assert nameDel != null;
    mNameDel = nameDel;
    assert attributeKeys != null;
    mAttributeKeys = attributeKeys;
    assert attributes != null;
    mAttributes = attributes;
    assert namespaceKeys != null;
    mNamespaceKeys = namespaceKeys;
    assert qNm != null;
    mQNm = qNm;
  }

  /**
   * Getting the count of attributes.
   *
   * @return the count of attributes
   */
  public int getAttributeCount() {
    return mAttributeKeys.size();
  }

  /**
   * Getting the attribute key for an given index.
   *
   * @param index index of the attribute
   * @return the attribute key
   */
  public long getAttributeKey(final @NonNegative int index) {
    if (mAttributeKeys.size() <= index) {
      return Fixed.NULL_NODE_KEY.getStandardProperty();
    }
    return mAttributeKeys.get(index);
  }

  /**
   * Getting the attribute key by name (from the dictionary).
   *
   * @param name the attribute-name to lookup
   * @return the attribute key associated with the name
   */
  public Optional<Long> getAttributeKeyByName(final QNm name) {
    final int prefixIndex = name.getPrefix() != null && !name.getPrefix().isEmpty()
        ? NamePageHash.generateHashForString(name.getPrefix())
        : -1;
    final int localNameIndex = NamePageHash.generateHashForString(name.getLocalName());
    return Optional.ofNullable(mAttributes.get((long) (prefixIndex + localNameIndex)));
  }

  /**
   * Get name key (prefixKey+localNameKey) by node key.
   *
   * @param key node key
   * @return optional name key
   */
  public Optional<Long> getAttributeNameKey(final @NonNegative long key) {
    return Optional.ofNullable(mAttributes.inverse().get(key));
  }

  /**
   * Inserting an attribute.
   *
   * @param attrKey the new attribute key
   * @param nameIndex index mapping to name string
   */
  public void insertAttribute(final @NonNegative long attrKey, final long nameIndex) {
    mAttributeKeys.add(attrKey);
    mAttributes.put(nameIndex, attrKey);
  }

  /**
   * Removing an attribute.
   *
   * @param attrKey the key of the attribute to be removed@NonNegative@NonNegative
   */
  public void removeAttribute(final @NonNegative long attrKey) {
    mAttributeKeys.remove(attrKey);
    mAttributes.inverse().remove(attrKey);
  }

  /**
   * Getting the count of namespaces.
   *
   * @return the count of namespaces
   */
  public int getNamespaceCount() {
    return mNamespaceKeys.size();
  }

  /**
   * Getting the namespace key for a given index.
   *
   * @param namespaceKey index of the namespace
   * @return the namespace key
   */
  public long getNamespaceKey(final @NonNegative int namespaceKey) {
    if (mNamespaceKeys.size() <= namespaceKey) {
      return Fixed.NULL_NODE_KEY.getStandardProperty();
    }
    return mNamespaceKeys.get(namespaceKey);
  }

  /**
   * Inserting a namespace.
   *
   * @param namespaceKey new namespace key
   */
  public void insertNamespace(final long namespaceKey) {
    mNamespaceKeys.add(namespaceKey);
  }

  /**
   * Removing a namepsace.
   *
   * @param namespaceKey the key of the namespace to be removed
   */
  public void removeNamespace(final long namespaceKey) {
    mNamespaceKeys.remove(namespaceKey);
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
  public NodeKind getKind() {
    return NodeKind.ELEMENT;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
                      .add("nameDelegate", mNameDel)
                      .add("nameSpaceKeys", mNamespaceKeys)
                      .add("attributeKeys", mAttributeKeys)
                      .add("structDelegate", structNodeDel)
                      .toString();
  }

  @Override
  public VisitResult acceptVisitor(final XmlNodeVisitor visitor) {
    return visitor.visit(ImmutableElement.of(this));
  }

  @Override
  public BigInteger computeHash() {
    BigInteger result = BigInteger.ONE;

    result = BigInteger.valueOf(31).multiply(result).add(structNodeDel.getNodeDelegate().computeHash());
    result = BigInteger.valueOf(31).multiply(result).add(structNodeDel.computeHash());
    result = BigInteger.valueOf(31).multiply(result).add(mNameDel.computeHash());

    return Node.to128BitsAtMaximumBigInteger(result);
  }

  @Override
  public void setHash(final BigInteger hash) {
    mHash = Node.to128BitsAtMaximumBigInteger(hash);

    assert mHash.toByteArray().length <= 17;
  }

  @Override
  public BigInteger getHash() {
    return mHash;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(delegate(), mNameDel);
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj instanceof ElementNode) {
      final ElementNode other = (ElementNode) obj;
      return Objects.equal(delegate(), other.delegate()) && Objects.equal(mNameDel, other.mNameDel);
    }
    return false;
  }

  /**
   * Get a {@link List} with all attribute keys.
   *
   * @return unmodifiable view of {@link List} with all attribute keys
   */
  public List<Long> getAttributeKeys() {
    return Collections.unmodifiableList(mAttributeKeys);
  }

  /**
   * Get a {@link List} with all namespace keys.
   *
   * @return unmodifiable view of {@link List} with all namespace keys
   */
  public List<Long> getNamespaceKeys() {
    return Collections.unmodifiableList(mNamespaceKeys);
  }

  @Override
  protected NodeDelegate delegate() {
    return structNodeDel.getNodeDelegate();
  }

  @Override
  protected StructNodeDelegate structDelegate() {
    return structNodeDel;
  }

  /**
   * Get name node delegate.
   *
   * @return snapshot of the name node delegate (new instance)
   */
  @NonNull
  public NameNodeDelegate getNameNodeDelegate() {
    return new NameNodeDelegate(mNameDel);
  }

  @Override
  public void setPathNodeKey(final @NonNegative long pathNodeKey) {
    mNameDel.setPathNodeKey(pathNodeKey);
  }

  @Override
  public long getPathNodeKey() {
    return mNameDel.getPathNodeKey();
  }

  @Override
  public QNm getName() {
    return mQNm;
  }

  @Override
  public SirixDeweyID getDeweyID() {
    return structNodeDel.getNodeDelegate().getDeweyID();
  }

  @Override
  public int getTypeKey() {
    return structNodeDel.getNodeDelegate().getTypeKey();
  }
}
