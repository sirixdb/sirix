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

import javax.annotation.Nonnegative;
import javax.annotation.Nullable;

import org.brackit.xquery.atomic.QNm;
import org.sirix.api.visitor.VisitResult;
import org.sirix.api.visitor.Visitor;
import org.sirix.node.delegates.NameNodeDelegate;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.immutable.ImmutableNamespace;
import org.sirix.node.interfaces.NameNode;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

/**
 * <h1>NamespaceNode</h1>
 *
 * <p>
 * Node representing a namespace.
 * </p>
 */
public final class NamespaceNode extends AbstractForwardingNode implements
		NameNode {

	/** Delegate for name node information. */
	private final NameNodeDelegate mNameDel;

	/** {@link NodeDelegate} reference. */
	private final NodeDelegate mNodeDel;

	/** The qualified name. */
	private final QNm mQNm;

	/**
	 * Constructor.
	 *
	 * @param nodeDel
	 *          {@link NodeDelegate} reference
	 * @param nameDel
	 *          {@link NameNodeDelegate} reference
	 * @param qNm
	 *          The qualified name.
	 */
	public NamespaceNode(final NodeDelegate nodeDel,
			final NameNodeDelegate nameDel, final QNm qNm) {
		assert nodeDel != null;
		assert nameDel != null;
		assert qNm != null;
		mNodeDel = nodeDel;
		mNameDel = nameDel;
		mQNm = qNm;
	}

	@Override
	public Kind getKind() {
		return Kind.NAMESPACE;
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
	public VisitResult acceptVisitor(final Visitor visitor) {
		return visitor.visit(ImmutableNamespace.of(this));
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(mNodeDel, mNameDel);
	}

	@Override
	public boolean equals(final @Nullable Object obj) {
		if (obj instanceof NamespaceNode) {
			final NamespaceNode other = (NamespaceNode) obj;
			return Objects.equal(mNodeDel, other.mNodeDel)
					&& Objects.equal(mNameDel, other.mNameDel);
		}
		return false;
	}

	@Override
	public String toString() {
		return MoreObjects.toStringHelper(this).add("nodeDel", mNodeDel)
				.add("nameDel", mNameDel).toString();
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
	 * @return {@link NameNodeDelegate} instance
	 */
	NameNodeDelegate getNameNodeDelegate() {
		return mNameDel;
	}

	@Override
	protected NodeDelegate delegate() {
		return mNodeDel;
	}

	@Override
	public QNm getName() {
		return mQNm;
	}
}
