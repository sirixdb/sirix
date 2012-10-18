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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.sirix.api.visitor.VisitResult;
import org.sirix.api.visitor.Visitor;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.delegates.StructNodeDelegate;
import org.sirix.node.immutable.ImmutableDocument;
import org.sirix.node.interfaces.StructNode;

import com.google.common.base.Objects;

/**
 * <h1>DocumentNode</h1>
 * 
 * <p>
 * Node representing the root of a document. This node is guaranteed to exist in
 * revision 0 and can not be removed.
 * </p>
 */
public final class DocumentRootNode extends AbstructStructForwardingNode
		implements StructNode {

	/** {@link NodeDelegate} reference. */
	private final NodeDelegate mNodeDel;

	/** {@link StructNodeDelegate} reference. */
	private final StructNodeDelegate mStructNodeDel;

	/**
	 * Constructor.
	 * 
	 * @param nodeDel
	 *          {@link NodeDelegate} reference
	 * @param structDel
	 *          {@link StructNodeDelegate} reference
	 */
	public DocumentRootNode(final @Nonnull NodeDelegate nodeDel,
			@Nonnull final StructNodeDelegate structDel) {
		mNodeDel = checkNotNull(nodeDel);
		mStructNodeDel = checkNotNull(structDel);
	}

	@Override
	public Kind getKind() {
		return Kind.DOCUMENT_ROOT;
	}

	@Override
	public VisitResult acceptVisitor(final @Nonnull Visitor visitor) {
		return visitor.visit(ImmutableDocument.of(this));
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(mNodeDel);
	}

	@Override
	public boolean equals(@Nullable final Object obj) {
		if (obj instanceof DocumentRootNode) {
			final DocumentRootNode other = (DocumentRootNode) obj;
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
}
