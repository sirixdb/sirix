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
import org.sirix.api.visitor.IVisitResult;
import org.sirix.api.visitor.IVisitor;
import org.sirix.node.delegates.NameNodeDelegate;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.delegates.StructNodeDelegate;
import org.sirix.node.immutable.ImmutableElement;
import org.sirix.node.interfaces.INameNode;
import org.sirix.settings.EFixed;
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
		INameNode {

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
	 * @param pNodeDel
	 *          {@link NodeDelegate} to be set
	 * @param pStructDel
	 *          {@link StructNodeDelegate} to be set
	 * @param pNameDel
	 *          {@link NameNodeDelegate} to be set
	 * @param pAttributeKeys
	 *          keys of attributes to be set
	 * @param pNamespaceKeys
	 *          keys of namespaces to be set
	 */
	public ElementNode(@Nonnull final StructNodeDelegate pStructDel,
			@Nonnull final NameNodeDelegate pNameDel,
			@Nonnull final List<Long> pAttributeKeys,
			@Nonnull final BiMap<Integer, Long> pAttributes,
			@Nonnull final List<Long> pNamespaceKeys) {
		mStructNodeDel = checkNotNull(pStructDel);
		mNameDel = checkNotNull(pNameDel);
		mAttributeKeys = checkNotNull(pAttributeKeys);
		mAttributes = checkNotNull(pAttributes);
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
	 * @param pIndex
	 *          index of the attribute
	 * @return the attribute key
	 */
	public long getAttributeKey(final int pIndex) {
		if (mAttributeKeys.size() <= pIndex) {
			return EFixed.NULL_NODE_KEY.getStandardProperty();
		}
		return mAttributeKeys.get(pIndex);
	}

	/**
	 * Getting the attribute key by name (from the dictionary).
	 * 
	 * @param pNameIndex
	 *          name index
	 * @return the attribute key
	 */
	public Optional<Long> getAttributeKeyByName(@Nonnull final QName pQName) {
		final int nameIndex = NamePageHash.generateHashForString(Utils
				.buildName(pQName));
		return Optional.fromNullable(mAttributes.get(nameIndex));
	}

	/**
	 * Inserting an attribute.
	 * 
	 * @param pAttrKey
	 *          the new attribute key
	 * @param pNameIndex
	 *          index mapping to name string
	 */
	public void insertAttribute(final long pAttrKey, final int pNameIndex) {
		mAttributeKeys.add(pAttrKey);
		mAttributes.put(pNameIndex, pAttrKey);
	}

	/**
	 * Removing an attribute.
	 * 
	 * @param pAttrKey
	 *          the key of the attribute to be removed
	 */
	public void removeAttribute(final long pAttrKey) {
		mAttributeKeys.remove(pAttrKey);
		mAttributes.inverse().remove(pAttrKey);
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
	 * @param pNamespaceKey
	 *          index of the namespace
	 * @return the namespace key
	 */
	public long getNamespaceKey(final int pNamespaceKey) {
		if (mNamespaceKeys.size() <= pNamespaceKey) {
			return EFixed.NULL_NODE_KEY.getStandardProperty();
		}
		return mNamespaceKeys.get(pNamespaceKey);
	}

	/**
	 * Inserting a namespace.
	 * 
	 * @param pNamespaceKey
	 *          new namespace key
	 */
	public void insertNamespace(final long pNamespaceKey) {
		mNamespaceKeys.add(pNamespaceKey);
	}

	/**
	 * Removing a namepsace.
	 * 
	 * @param pNamespaceKey
	 *          the key of the namespace to be removed
	 */
	public void removeNamespace(final long pNamespaceKey) {
		mNamespaceKeys.remove(pNamespaceKey);
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
	public void setNameKey(final int pNameKey) {
		mNameDel.setNameKey(pNameKey);
	}

	@Override
	public void setURIKey(final int pUriKey) {
		mNameDel.setURIKey(pUriKey);
	}

	@Override
	public EKind getKind() {
		return EKind.ELEMENT;
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this).add("nameDelegate", mNameDel)
				.add("nameSpaceKeys", mNamespaceKeys)
				.add("attributeKeys", mAttributeKeys)
				.add("structDelegate", mStructNodeDel).toString();
	}

	@Override
	public IVisitResult acceptVisitor(@Nonnull final IVisitor pVisitor) {
		return pVisitor.visit(ImmutableElement.of(this));
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(delegate(), mNameDel);
	}

	@Override
	public boolean equals(final Object pObj) {
		if (pObj instanceof ElementNode) {
			final ElementNode other = (ElementNode) pObj;
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
	public void setPathNodeKey(@Nonnegative final long pPathNodeKey) {
		mNameDel.setPathNodeKey(pPathNodeKey);
	}

	@Override
	public long getPathNodeKey() {
		return mNameDel.getPathNodeKey();
	}
}
