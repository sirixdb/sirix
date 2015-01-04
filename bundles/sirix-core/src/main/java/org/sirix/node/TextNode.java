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

import javax.annotation.Nullable;

import org.sirix.api.visitor.VisitResult;
import org.sirix.api.visitor.Visitor;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.delegates.StructNodeDelegate;
import org.sirix.node.delegates.ValNodeDelegate;
import org.sirix.node.immutable.ImmutableText;
import org.sirix.node.interfaces.Node;
import org.sirix.node.interfaces.StructNode;
import org.sirix.node.interfaces.ValueNode;
import org.sirix.settings.Constants;
import org.sirix.settings.Fixed;

import com.google.common.base.Objects;

/**
 * <h1>TextNode</h1>
 * 
 * <p>
 * Node representing a text node.
 * </p>
 */
public final class TextNode extends AbstractStructForwardingNode implements
		ValueNode {

	/** Delegate for common value node information. */
	private final ValNodeDelegate mValDel;

	/** {@link StructNodeDelegate} reference. */
	private final StructNodeDelegate mStructNodeDel;

	/** Value of the node. */
	private byte[] mValue;

	/**
	 * Constructor for TextNode.
	 * 
	 * @param pDel
	 *          delegate for {@link Node} implementation
	 * @param valDel
	 *          delegate for {@link ValueNode} implementation
	 * @param structDel
	 *          delegate for {@link StructNode} implementation
	 */
	public TextNode(final ValNodeDelegate valDel,
			final StructNodeDelegate structDel) {
		mStructNodeDel = checkNotNull(structDel);
		mValDel = checkNotNull(valDel);
	}

	@Override
	public Kind getKind() {
		return Kind.TEXT;
	}

	@Override
	public byte[] getRawValue() {
		if (mValue == null) {
			mValue = mValDel.getRawValue();
		}
		return mValue;
	}

	@Override
	public void setValue(final byte[] value) {
		mValue = null;
		mValDel.setValue(value);
	}

	@Override
	public long getFirstChildKey() {
		return Fixed.NULL_NODE_KEY.getStandardProperty();
	}

	@Override
	public VisitResult acceptVisitor(final Visitor visitor) {
		return visitor.visit(ImmutableText.of(this));
	}

	@Override
	public void decrementChildCount() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void incrementChildCount() {
		throw new UnsupportedOperationException();
	}

	@Override
	public long getDescendantCount() {
		return 0;
	}

	@Override
	public void decrementDescendantCount() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void incrementDescendantCount() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setDescendantCount(long descendantCount) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(mStructNodeDel.getNodeDelegate(), mValDel);
	}

	@Override
	public boolean equals(final @Nullable Object obj) {
		if (obj instanceof TextNode) {
			final TextNode other = (TextNode) obj;
			return Objects.equal(mStructNodeDel.getNodeDelegate(),
					other.getNodeDelegate())
					&& mValDel.equals(other.mValDel);
		}
		return false;
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this)
				.add("node delegate", mStructNodeDel.getNodeDelegate())
				.add("struct delegate", mStructNodeDel).add("value delegate", mValDel)
				.toString();
	}

	public ValNodeDelegate getValNodeDelegate() {
		return mValDel;
	}

	@Override
	protected NodeDelegate delegate() {
		return mStructNodeDel.getNodeDelegate();
	}

	@Override
	protected StructNodeDelegate structDelegate() {
		return mStructNodeDel;
	}

	@Override
	public String getValue() {
		return new String(mValDel.getRawValue(), Constants.DEFAULT_ENCODING);
	}
}
