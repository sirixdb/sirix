/**
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 * * Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the
 * names of its contributors may be used to endorse or promote products
 * derived from this software without specific prior written permission.
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

package org.sirix.node;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.xml.namespace.QName;

import org.sirix.access.Utils;
import org.sirix.api.visitor.VisitResult;
import org.sirix.api.visitor.Visitor;
import org.sirix.node.delegates.NameNodeDelegate;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.delegates.StructNodeDelegate;
import org.sirix.node.immutable.ImmutableElement;
import org.sirix.node.interfaces.NameNode;
import org.sirix.settings.Fixed;
import org.sirix.utils.NamePageHash;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.collect.BiMap;

/**
 * <h1>ElementNode</h1>
 * 
 * <p>
 * Node representing an XML element.
 * </p>
 */
public final class ElementNode extends AbsStructForwardingNode implements
		NameNode {

	/** Delegate for name node information. */
	private final NameNodeDelegate mNameDel;

	/** Mapping names/keys. */
	private final BiMap<Integer, Long> mAttributes;

	/** Keys of attributes. */
	private final List<Long> mAttributeKeys;

	/** Keys of namespace declarations. */
	private final List<Long> mNamespaceKeys;

	/** {@link StructNodeDelegate} reference. */
	private final StructNodeDelegate mStructNodeDel;

	/**
	 * Constructor
	 * 
	 * @param structDel
	 *          {@link StructNodeDelegate} to be set
	 * @param nameDel
	 *          {@link NameNodeDelegate} to be set
	 * @param attributeKeys
	 *          list of attribute keys
	 * @param attributes
	 *          attribute nameKey / nodeKey mapping in both directions
	 * @param pNamespaceKeys
	 *          keys of namespaces to be set
	 */
	public ElementNode(@Nonnull final StructNodeDelegate structDel,
			@Nonnull final NameNodeDelegate nameDel,
			@Nonnull final List<Long> attributeKeys,
			@Nonnull final BiMap<Integer, Long> attributes,
			@Nonnull final List<Long> pNamespaceKeys) {
		mStructNodeDel = checkNotNull(structDel);
		mNameDel = checkNotNull(nameDel);
		mAttributeKeys = checkNotNull(attributeKeys);
		mAttributes = checkNotNull(attributes);
		mNamespaceKeys = checkNotNull(pNamespaceKeys);
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
	 * @param index
	 *          index of the attribute
	 * @return the attribute key
	 */
	public long getAttributeKey(final @Nonnegative int index) {
		if (mAttributeKeys.size() <= index) {
			return Fixed.NULL_NODE_KEY.getStandardProperty();
		}
		return mAttributeKeys.get(index);
	}

	/**
	 * Getting the attribute key by name (from the dictionary).
	 * 
	 * @param name
	 *          the attribute-name to lookup
	 * @return the attribute key associated with the name
	 */
	public Optional<Long> getAttributeKeyByName(final @Nonnull QName name) {
		final int nameIndex = NamePageHash.generateHashForString(Utils
				.buildName(name));
		return Optional.fromNullable(mAttributes.get(nameIndex));
	}

	/**
	 * Get name key by node key.
	 * 
	 * @param key
	 *          node key
	 * @return optional name key
	 */
	public Optional<Integer> getAttributeNameKey(final @Nonnegative long key) {
		return Optional.fromNullable(mAttributes.inverse().get(key));
	}

	/**
	 * Inserting an attribute.
	 * 
	 * @param attrKey
	 *          the new attribute key
	 * @param nameIndex
	 *          index mapping to name string
	 */
	public void insertAttribute(final long attrKey, final int nameIndex) {
		mAttributeKeys.add(attrKey);
		mAttributes.put(nameIndex, attrKey);
	}

	/**
	 * Removing an attribute.
	 * 
	 * @param attrKey
	 *          the key of the attribute to be removed
	 */
	public void removeAttribute(final long attrKey) {
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
	 * @param namespaceKey
	 *          index of the namespace
	 * @return the namespace key
	 */
	public long getNamespaceKey(final @Nonnegative int namespaceKey) {
		if (mNamespaceKeys.size() <= namespaceKey) {
			return Fixed.NULL_NODE_KEY.getStandardProperty();
		}
		return mNamespaceKeys.get(namespaceKey);
	}

	/**
	 * Inserting a namespace.
	 * 
	 * @param namespaceKey
	 *          new namespace key
	 */
	public void insertNamespace(final long namespaceKey) {
		mNamespaceKeys.add(namespaceKey);
	}

	/**
	 * Removing a namepsace.
	 * 
	 * @param namespaceKey
	 *          the key of the namespace to be removed
	 */
	public void removeNamespace(final long namespaceKey) {
		mNamespaceKeys.remove(namespaceKey);
	}

	@Override
	public int getNameKey() {
		return mNameDel.getNameKey();
	}

	@Override
	public int getURIKey() {
		return mNameDel.getURIKey();
	}

	@Override
	public void setNameKey(final int nameKey) {
		mNameDel.setNameKey(nameKey);
	}

	@Override
	public void setURIKey(final int uriKey) {
		mNameDel.setURIKey(uriKey);
	}

	@Override
	public Kind getKind() {
		return Kind.ELEMENT;
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this).add("nameDelegate", mNameDel)
				.add("nameSpaceKeys", mNamespaceKeys)
				.add("attributeKeys", mAttributeKeys)
				.add("structDelegate", mStructNodeDel).toString();
	}

	@Override
	public VisitResult acceptVisitor(final @Nonnull Visitor visitor) {
		return visitor.visit(ImmutableElement.of(this));
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(delegate(), mNameDel);
	}

	@Override
	public boolean equals(final Object obj) {
		if (obj instanceof ElementNode) {
			final ElementNode other = (ElementNode) obj;
			return Objects.equal(delegate(), other.delegate())
					&& Objects.equal(mNameDel, other.mNameDel);
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
		return mStructNodeDel.getNodeDelegate();
	}

	@Override
	protected StructNodeDelegate structDelegate() {
		return mStructNodeDel;
	}

	/**
	 * Get name node delegate.
	 * 
	 * @return snapshot of the name node delegate (new instance)
	 */
	@Nonnull
	public NameNodeDelegate getNameNodeDelegate() {
		return new NameNodeDelegate(mNameDel);
	}

	@Override
	public void setPathNodeKey(final @Nonnegative long pathNodeKey) {
		mNameDel.setPathNodeKey(pathNodeKey);
	}

	@Override
	public long getPathNodeKey() {
		return mNameDel.getPathNodeKey();
	}
}
