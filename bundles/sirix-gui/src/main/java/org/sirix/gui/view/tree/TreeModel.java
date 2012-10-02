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

package org.sirix.gui.view.tree;

import org.sirix.api.INodeReadTrx;
import org.sirix.exception.SirixException;
import org.sirix.gui.ReadDB;
import org.sirix.node.EKind;
import org.sirix.node.interfaces.INode;
import org.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;

/**
 * <h1>TreeModel</h1>
 * 
 * <p>
 * Extends an AbstractTreeModel and implements main methods, used to construct
 * the Tree representation with sirix items.
 * </p>
 * 
 * @author Johannes Lichtenberger, University of Konstanz.
 * 
 */
public final class TreeModel extends AbsTreeModel {

	/** Logger. */
	private static final LogWrapper LOGWRAPPER = new LogWrapper(
			LoggerFactory.getLogger(TreeModel.class));

	/** sirix {@link INodeReadTrx}. */
	private transient INodeReadTrx mRtx;

	/**
	 * Constructor.
	 * 
	 * @param pDB
	 *          {@link ReadDB} instance
	 */
	TreeModel(final ReadDB pDB) {
		try {
			mRtx = pDB.getSession().beginNodeReadTrx(pDB.getRevisionNumber());
			mRtx.moveTo(pDB.getNodeKey());
		} catch (final SirixException e) {
			LOGWRAPPER.error(e.getMessage(), e);
		}
	}

	@Override
	public Object getChild(final Object pParent, final int pIndex) {
		final INode parentNode = (INode) pParent;
		final long parentNodeKey = parentNode.getNodeKey();
		mRtx.moveTo(parentNodeKey);

		switch (parentNode.getKind()) {
		case DOCUMENT_ROOT:
			assert pIndex == 0;
			mRtx.moveToFirstChild();
			return mRtx.getNode();
		case ELEMENT:
			// Namespaces.
			final int namespCount = mRtx.getNamespaceCount();
			if (pIndex < namespCount) {
				if (!mRtx.moveToNamespace(pIndex).hasMoved()) {
					throw new IllegalStateException("No namespace with index " + pIndex
							+ " found!");
				}
				return mRtx.getNode();
			}

			// Attributes.
			final int attCount = mRtx.getAttributeCount();
			if (pIndex < (namespCount + attCount)) {
				if (!mRtx.moveToAttribute(pIndex - namespCount).hasMoved()) {
					throw new IllegalStateException("No attribute with index " + pIndex
							+ " found!");
				}
				return mRtx.getNode();
			}

			// Children.
			final long childCount = mRtx.getChildCount();
			if (pIndex < (namespCount + attCount + childCount)) {
				if (!mRtx.moveToFirstChild().hasMoved()) {
					throw new IllegalStateException("No node with index " + pIndex
							+ " found!");
				}
				final long upper = pIndex - namespCount - attCount;
				for (long i = 0; i < upper; i++) {
					if (!mRtx.moveToRightSibling().hasMoved()) {
						throw new IllegalStateException("No node with index " + pIndex
								+ " found!");
					}
				}

				return mRtx.getNode();
			} else {
				throw new IllegalStateException("May not happen: node with " + pIndex
						+ " not found!");
			}
		default:
			return null;
		}
	}

	@Override
	public int getChildCount(final Object parent) {
		mRtx.moveTo(((INode) parent).getNodeKey());

		switch (mRtx.getKind()) {
		case DOCUMENT_ROOT:
			assert mRtx.hasFirstChild();
			return 1;
		case ELEMENT:
			final int namespaces = mRtx.getNamespaceCount();
			final int attributes = mRtx.getAttributeCount();
			final long children = mRtx.getChildCount();

			// TODO: possibly unsafe cast.
			return (int) (namespaces + attributes + children);
		default:
			return 0;
		}
	}

	@Override
	public int getIndexOfChild(final Object pParent, final Object pChild) {
		if (pParent == null || pChild == null) {
			return -1;
		}

		// Parent node.
		mRtx.moveTo(((INode) pParent).getNodeKey());
		final INode parentNode = (INode) pParent;

		// Child node.
		final INode childNode = (INode) pChild;

		// Return value.
		int index = -1;

		// Values needed.
		final long nodeKey = parentNode.getNodeKey();
		int namespCount = 0;
		int attCount = 0;

		switch (childNode.getKind()) {
		case NAMESPACE:
			namespCount = mRtx.getNamespaceCount();
			for (int i = 0; i < namespCount; i++) {
				mRtx.moveToNamespace(i);
				if (mRtx.getNodeKey() == childNode.getNodeKey()) {
					index = i;
					break;
				}
				mRtx.moveTo(nodeKey);
			}
			break;
		case ATTRIBUTE:
			namespCount = mRtx.getNamespaceCount();
			attCount = mRtx.getAttributeCount();
			for (int i = 0; i < attCount; i++) {
				mRtx.moveToAttribute(i);
				if (mRtx.getNodeKey() == childNode.getNodeKey()) {
					index = namespCount + i;
					break;
				}
				mRtx.moveTo(nodeKey);
			}
			break;
		case WHITESPACE:
			break;
		case ELEMENT:
		case COMMENT:
		case PROCESSING:
		case TEXT:
			if (parentNode.getKind() == EKind.ELEMENT) {
				namespCount = mRtx.getNamespaceCount();
				attCount = mRtx.getAttributeCount();
			}
			final long childCount = mRtx.getChildCount();

			if (childCount == 0) {
				throw new IllegalStateException("May not happen!");
			}

			for (int i = 0; i < childCount; i++) {
				if (i == 0) {
					mRtx.moveToFirstChild();
				} else {
					mRtx.moveToRightSibling();
				}
				if (mRtx.getNodeKey() == childNode.getNodeKey()) {
					index = namespCount + attCount + i;
					break;
				}
			}

			break;
		default:
			throw new IllegalStateException("Child node kind not known! ");
		}

		return index;
	}

	@Override
	public Object getRoot() {
		mRtx.moveToDocumentRoot();
		return mRtx.getNode();
	}

	@Override
	public boolean isLeaf(final Object pNode) {
		mRtx.moveTo(((INode) pNode).getNodeKey());

		switch (mRtx.getKind()) {
		case DOCUMENT_ROOT:
			return false;
		case ELEMENT:
			if (mRtx.getNamespaceCount() > 0) {
				return false;
			}
			if (mRtx.getAttributeCount() > 0) {
				return false;
			}
			if (mRtx.getChildCount() > 0) {
				return false;
			}
		default:
			// If it's not document root or element node it must be a leaf node.
			return true;
		}
	}
}
