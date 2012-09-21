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
package org.sirix.page;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import com.google.common.base.Objects;
import com.google.common.base.Objects.ToStringHelper;
import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import org.sirix.api.IPageWriteTrx;
import org.sirix.exception.SirixException;
import org.sirix.node.EKind;
import org.sirix.node.interfaces.INodeBase;
import org.sirix.page.delegates.PageDelegate;
import org.sirix.page.interfaces.IPage;

/**
 * <h1>NodePage</h1>
 * 
 * <p>
 * A node page stores a set of nodes.
 * </p>
 */
public class NodePage implements IPage {

	/** Key of node page. This is the base key of all contained nodes. */
	private final long mNodePageKey;

	/** Nodes. */
	private final Map<Long, INodeBase> mNodes;

	/** {@link PageDelegate} reference. */
	private final long mRevision;

	/**
	 * Create node page.
	 * 
	 * @param pNodePageKey
	 *          base key assigned to this node page
	 * @param pRevision
	 *          revision the page belongs to
	 */
	public NodePage(@Nonnegative final long pNodePageKey,
			@Nonnegative final long pRevision) {
		checkArgument(pNodePageKey >= 0, "pNodePageKey must not be negative!");
		checkArgument(pRevision >= 0, "pRevision must not be negative!");
		mRevision = pRevision;
		mNodePageKey = pNodePageKey;
		mNodes = new HashMap<>();
	}

	/**
	 * Read node page.
	 * 
	 * @param pIn
	 *          input bytes to read page from
	 */
	protected NodePage(final @Nonnull ByteArrayDataInput pIn) {
		mRevision = pIn.readLong();
		mNodePageKey = pIn.readLong();
		final int size = pIn.readInt();
		mNodes = new HashMap<>(size);
		for (int offset = 0; offset < size; offset++) {
			final byte id = pIn.readByte();
			final EKind enumKind = EKind.getKind(id);
			final INodeBase node = enumKind.deserialize(pIn);
			mNodes.put(node.getNodeKey(), node);
		}
	}

	/**
	 * Get key of node page.
	 * 
	 * @return node page key
	 */
	public final long getNodePageKey() {
		return mNodePageKey;
	}

	/**
	 * Get node with the specified node key.
	 * 
	 * @param pKey
	 *          node key
	 * @return node with given node key, or {@code null} if not present
	 */
	public INodeBase getNode(final @Nonnegative long pKey) {
		checkArgument(pKey >= 0, "pKey must not be negative!");
		return mNodes.get(pKey);
	}

	/**
	 * Overwrite a single node at a given offset.
	 * 
	 * @param pKey
	 *          key of node to overwrite in this node page
	 * @param pNode
	 *          node to store at given nodeOffset
	 */
	public void setNode(final @Nonnull INodeBase pNode) {
		mNodes.put(pNode.getNodeKey(), checkNotNull(pNode));
	}

	@Override
	public void serialize(final @Nonnull ByteArrayDataOutput pOut) {
		pOut.writeLong(mRevision);
		pOut.writeLong(mNodePageKey);
		pOut.writeInt(mNodes.size());
		for (final INodeBase node : mNodes.values()) {
			final byte id = node.getKind().getId();
			pOut.writeByte(id);
			EKind.getKind(node.getClass()).serialize(pOut, node);
		}
	}

	@Override
	public final String toString() {
		final ToStringHelper helper = Objects.toStringHelper(this)
				.add("revision", mRevision).add("pagekey", mNodePageKey)
				.add("nodes", mNodes.toString());
		for (final INodeBase node : mNodes.values()) {
			helper.add("node", node);
		}
		return helper.toString();
	}

	/**
	 * Entry set of all nodes in the page.
	 * 
	 * @return an entry set
	 */
	public final Set<Entry<Long, INodeBase>> entrySet() {
		return Collections.unmodifiableSet(mNodes.entrySet());
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(mNodePageKey, mNodes);
	}

	@Override
	public boolean equals(final Object pObj) {
		if (pObj instanceof NodePage) {
			final NodePage other = (NodePage) pObj;
			return Objects.equal(mNodePageKey, other.mNodePageKey)
					&& Objects.equal(mNodes, other.mNodes);
		}
		return false;
	}

	@Override
	public long getRevision() {
		return mRevision;
	}

	@Override
	public PageReference[] getReferences() {
		return null;
	}

	@Override
	public void commit(@Nonnull final IPageWriteTrx pPageWriteTrx)
			throws SirixException {
	}

	/**
	 * All available nodes.
	 * 
	 * @return a collection view of all nodes
	 */
	public Collection<INodeBase> values() {
		return Collections.unmodifiableCollection(mNodes.values());
	}

}
