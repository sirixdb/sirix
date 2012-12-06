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
package org.sirix.access;

import javax.annotation.Nonnull;
import javax.xml.namespace.QName;

import org.sirix.api.NodeReadTrx;
import org.sirix.api.NodeWriteTrx;
import org.sirix.exception.SirixException;
import org.sirix.node.Kind;
import org.sirix.node.TextNode;
import org.sirix.node.interfaces.StructNode;
import org.sirix.page.PageKind;
import org.sirix.page.UnorderedKeyValuePage;
import org.sirix.settings.Fixed;

import com.google.common.base.Optional;

/**
 * Determines the position of the insertion of nodes and appropriate methods for
 * movement and the copy of whole subtrees.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
enum InsertPos {
	/** Insert as first child. */
	ASFIRSTCHILD {
		@Override
		void processMove(final @Nonnull StructNode fromNode,
				final @Nonnull StructNode toNode, final @Nonnull NodeWriteTrxImpl wtx)
				throws SirixException {
			assert fromNode != null;
			assert toNode != null;
			assert wtx != null;

			// Adapt childCount of parent where the subtree has to be inserted.
			StructNode newParent = (StructNode) wtx.getPageTransaction()
					.prepareEntryForModification(toNode.getNodeKey(), PageKind.NODEPAGE,
							Optional.<UnorderedKeyValuePage> absent());
			if (fromNode.getParentKey() != toNode.getNodeKey()) {
				newParent.incrementChildCount();
			}
			wtx.getPageTransaction().finishEntryModification(newParent.getNodeKey(),
					PageKind.NODEPAGE);

			if (toNode.hasFirstChild()) {
				wtx.moveTo(toNode.getFirstChildKey());

				if (wtx.getKind() == Kind.TEXT && fromNode.getKind() == Kind.TEXT) {
					final StringBuilder builder = new StringBuilder(wtx.getValue());

					// Adapt right sibling key of moved node.
					wtx.moveTo(wtx.getRightSiblingKey());
					final TextNode moved = (TextNode) wtx.getPageTransaction()
							.prepareEntryForModification(fromNode.getNodeKey(),
									PageKind.NODEPAGE, Optional.<UnorderedKeyValuePage> absent());
					moved.setRightSiblingKey(wtx.getNodeKey());
					wtx.getPageTransaction().finishEntryModification(moved.getNodeKey(),
							PageKind.NODEPAGE);

					// Merge text nodes.
					wtx.moveTo(moved.getNodeKey());
					builder.insert(0, wtx.getValue());
					wtx.setValue(builder.toString());

					// Remove first child.
					wtx.moveTo(toNode.getFirstChildKey());
					wtx.remove();

					// Adapt left sibling key of former right sibling of first child.
					wtx.moveTo(moved.getRightSiblingKey());
					final StructNode rightSibling = (StructNode) wtx.getPageTransaction()
							.prepareEntryForModification(wtx.getNodeKey(), PageKind.NODEPAGE,
									Optional.<UnorderedKeyValuePage> absent());
					rightSibling.setLeftSiblingKey(fromNode.getNodeKey());
					wtx.getPageTransaction().finishEntryModification(
							rightSibling.getNodeKey(), PageKind.NODEPAGE);
				} else {
					// Adapt left sibling key of former first child.
					final StructNode oldFirstChild = (StructNode) wtx
							.getPageTransaction().prepareEntryForModification(
									toNode.getFirstChildKey(), PageKind.NODEPAGE,
									Optional.<UnorderedKeyValuePage> absent());
					oldFirstChild.setLeftSiblingKey(fromNode.getNodeKey());
					wtx.getPageTransaction().finishEntryModification(
							oldFirstChild.getNodeKey(), PageKind.NODEPAGE);

					// Adapt right sibling key of moved node.
					final StructNode moved = (StructNode) wtx.getPageTransaction()
							.prepareEntryForModification(fromNode.getNodeKey(),
									PageKind.NODEPAGE, Optional.<UnorderedKeyValuePage> absent());
					moved.setRightSiblingKey(oldFirstChild.getNodeKey());
					wtx.getPageTransaction().finishEntryModification(moved.getNodeKey(),
							PageKind.NODEPAGE);
				}
			} else {
				// Adapt right sibling key of moved node.
				final StructNode moved = (StructNode) wtx.getPageTransaction()
						.prepareEntryForModification(fromNode.getNodeKey(),
								PageKind.NODEPAGE, Optional.<UnorderedKeyValuePage> absent());
				moved.setRightSiblingKey(Fixed.NULL_NODE_KEY.getStandardProperty());
				wtx.getPageTransaction().finishEntryModification(moved.getNodeKey(),
						PageKind.NODEPAGE);
			}

			// Adapt first child key of parent where the subtree has to be inserted.
			newParent = (StructNode) wtx.getPageTransaction()
					.prepareEntryForModification(toNode.getNodeKey(), PageKind.NODEPAGE,
							Optional.<UnorderedKeyValuePage> absent());
			newParent.setFirstChildKey(fromNode.getNodeKey());
			wtx.getPageTransaction().finishEntryModification(newParent.getNodeKey(),
					PageKind.NODEPAGE);

			// Adapt left sibling key and parent key of moved node.
			final StructNode moved = (StructNode) wtx.getPageTransaction()
					.prepareEntryForModification(fromNode.getNodeKey(),
							PageKind.NODEPAGE, Optional.<UnorderedKeyValuePage> absent());
			moved.setLeftSiblingKey(Fixed.NULL_NODE_KEY.getStandardProperty());
			moved.setParentKey(toNode.getNodeKey());
			wtx.getPageTransaction().finishEntryModification(moved.getNodeKey(),
					PageKind.NODEPAGE);
		}

		@Override
		void insertNode(final @Nonnull NodeWriteTrx wtx,
				final @Nonnull NodeReadTrx rtx) throws SirixException {
			assert wtx != null;
			assert rtx != null;
			assert wtx.getKind() == Kind.ELEMENT || wtx.getKind() == Kind.DOCUMENT;
			switch (rtx.getKind()) {
			case ELEMENT:
				wtx.insertElementAsFirstChild(rtx.getName());
				break;
			case TEXT:
				assert wtx.getKind() == Kind.ELEMENT;
				wtx.insertTextAsFirstChild(rtx.getValue());
				break;
			default:
				throw new IllegalStateException("Node type not known!");
			}

		}
	},
	/** Insert as right sibling. */
	ASRIGHTSIBLING {
		@Override
		void processMove(final @Nonnull StructNode fromNode,
				final @Nonnull StructNode toNode, final @Nonnull NodeWriteTrxImpl wtx)
				throws SirixException {
			assert fromNode != null;
			assert toNode != null;
			assert wtx != null;

			// Increment child count of parent node if moved node was not a child
			// before.
			if (fromNode.getParentKey() != toNode.getParentKey()) {
				final StructNode parentNode = (StructNode) wtx.getPageTransaction()
						.prepareEntryForModification(toNode.getParentKey(),
								PageKind.NODEPAGE, Optional.<UnorderedKeyValuePage> absent());
				parentNode.incrementChildCount();
				wtx.getPageTransaction().finishEntryModification(
						parentNode.getNodeKey(), PageKind.NODEPAGE);
			}

			final boolean hasMoved = wtx.moveTo(toNode.getRightSiblingKey())
					.hasMoved();

			if (fromNode.getKind() == Kind.TEXT && toNode.getKind() == Kind.TEXT) {
				// Merge text: FROM and TO are of TEXT_KIND.
				wtx.moveTo(toNode.getNodeKey());
				final StringBuilder builder = new StringBuilder(wtx.getValue());

				// Adapt left sibling key of former right sibling of first child.
				if (toNode.hasRightSibling()) {
					final StructNode rightSibling = (StructNode) wtx.getPageTransaction()
							.prepareEntryForModification(wtx.getRightSiblingKey(),
									PageKind.NODEPAGE, Optional.<UnorderedKeyValuePage> absent());
					rightSibling.setLeftSiblingKey(fromNode.getNodeKey());
					wtx.getPageTransaction().finishEntryModification(
							rightSibling.getNodeKey(), PageKind.NODEPAGE);
				}

				// Adapt sibling keys of moved node.
				final TextNode movedNode = (TextNode) wtx.getPageTransaction()
						.prepareEntryForModification(fromNode.getNodeKey(),
								PageKind.NODEPAGE, Optional.<UnorderedKeyValuePage> absent());
				movedNode.setRightSiblingKey(toNode.getRightSiblingKey());
				// Adapt left sibling key of moved node.
				movedNode.setLeftSiblingKey(wtx.getLeftSiblingKey());
				wtx.getPageTransaction().finishEntryModification(
						movedNode.getNodeKey(), PageKind.NODEPAGE);

				// Merge text nodes.
				wtx.moveTo(movedNode.getNodeKey());
				builder.append(wtx.getValue());
				wtx.setValue(builder.toString());

				final StructNode insertAnchor = (StructNode) wtx.getPageTransaction()
						.prepareEntryForModification(toNode.getNodeKey(),
								PageKind.NODEPAGE, Optional.<UnorderedKeyValuePage> absent());
				// Adapt right sibling key of node where the subtree has to be inserted.
				insertAnchor.setRightSiblingKey(fromNode.getNodeKey());
				wtx.getPageTransaction().finishEntryModification(
						insertAnchor.getNodeKey(), PageKind.NODEPAGE);

				// Remove first child.
				wtx.moveTo(toNode.getNodeKey());
				wtx.remove();
			} else if (hasMoved && fromNode.getKind() == Kind.TEXT
					&& wtx.getKind() == Kind.TEXT) {
				// Merge text: RIGHT and FROM are of TEXT_KIND.
				final StringBuilder builder = new StringBuilder(wtx.getValue());

				// Adapt left sibling key of former right sibling of first child.
				final StructNode rightSibling = (StructNode) wtx.getPageTransaction()
						.prepareEntryForModification(wtx.getNodeKey(), PageKind.NODEPAGE,
								Optional.<UnorderedKeyValuePage> absent());
				rightSibling.setLeftSiblingKey(fromNode.getNodeKey());
				wtx.getPageTransaction().finishEntryModification(
						rightSibling.getNodeKey(), PageKind.NODEPAGE);

				// Adapt sibling keys of moved node.
				final TextNode movedNode = (TextNode) wtx.getPageTransaction()
						.prepareEntryForModification(fromNode.getNodeKey(),
								PageKind.NODEPAGE, Optional.<UnorderedKeyValuePage> absent());
				movedNode.setRightSiblingKey(rightSibling.getNodeKey());
				movedNode.setLeftSiblingKey(toNode.getNodeKey());
				wtx.getPageTransaction().finishEntryModification(
						movedNode.getNodeKey(), PageKind.NODEPAGE);

				// Merge text nodes.
				wtx.moveTo(movedNode.getNodeKey());
				builder.insert(0, wtx.getValue());
				wtx.setValue(builder.toString());

				// Remove right sibling.
				wtx.moveTo(toNode.getRightSiblingKey());
				wtx.remove();

				final StructNode insertAnchor = (StructNode) wtx.getPageTransaction()
						.prepareEntryForModification(toNode.getNodeKey(),
								PageKind.NODEPAGE, Optional.<UnorderedKeyValuePage> absent());
				// Adapt right sibling key of node where the subtree has to be inserted.
				insertAnchor.setRightSiblingKey(fromNode.getNodeKey());
				wtx.getPageTransaction().finishEntryModification(
						insertAnchor.getNodeKey(), PageKind.NODEPAGE);
			} else {
				// No text merging involved.
				final StructNode insertAnchor = (StructNode) wtx.getPageTransaction()
						.prepareEntryForModification(toNode.getNodeKey(),
								PageKind.NODEPAGE, Optional.<UnorderedKeyValuePage> absent());
				final long rightSiblKey = insertAnchor.getRightSiblingKey();
				// Adapt right sibling key of node where the subtree has to be inserted.
				insertAnchor.setRightSiblingKey(fromNode.getNodeKey());
				wtx.getPageTransaction().finishEntryModification(
						insertAnchor.getNodeKey(), PageKind.NODEPAGE);

				if (rightSiblKey > -1) {
					// Adapt left sibling key of former right sibling.
					final StructNode oldRightSibling = (StructNode) wtx
							.getPageTransaction().prepareEntryForModification(rightSiblKey,
									PageKind.NODEPAGE, Optional.<UnorderedKeyValuePage> absent());
					oldRightSibling.setLeftSiblingKey(fromNode.getNodeKey());
					wtx.getPageTransaction().finishEntryModification(
							oldRightSibling.getNodeKey(), PageKind.NODEPAGE);
				}
				// Adapt right- and left-sibling key of moved node.
				final StructNode movedNode = (StructNode) wtx.getPageTransaction()
						.prepareEntryForModification(fromNode.getNodeKey(),
								PageKind.NODEPAGE, Optional.<UnorderedKeyValuePage> absent());
				movedNode.setRightSiblingKey(rightSiblKey);
				movedNode.setLeftSiblingKey(insertAnchor.getNodeKey());
				wtx.getPageTransaction().finishEntryModification(
						movedNode.getNodeKey(), PageKind.NODEPAGE);
			}

			// Adapt parent key of moved node.
			final StructNode movedNode = (StructNode) wtx.getPageTransaction()
					.prepareEntryForModification(fromNode.getNodeKey(),
							PageKind.NODEPAGE, Optional.<UnorderedKeyValuePage> absent());
			movedNode.setParentKey(toNode.getParentKey());
			wtx.getPageTransaction().finishEntryModification(movedNode.getNodeKey(),
					PageKind.NODEPAGE);
		}

		@Override
		void insertNode(final @Nonnull NodeWriteTrx wtx,
				final @Nonnull NodeReadTrx rtx) throws SirixException {
			assert wtx != null;
			assert rtx != null;
			assert wtx.getKind() == Kind.ELEMENT || wtx.getKind() == Kind.TEXT;
			switch (rtx.getKind()) {
			case ELEMENT:
				wtx.insertElementAsRightSibling(rtx.getName());
				break;
			case TEXT:
				wtx.insertTextAsRightSibling(rtx.getValue());
				break;
			default:
				throw new IllegalStateException("Node type not known!");
			}
		}
	},
	/** Insert as a non structural node. */
	ASNONSTRUCTURAL {
		@Override
		void processMove(final @Nonnull StructNode fromNode,
				final @Nonnull StructNode toNode, final @Nonnull NodeWriteTrxImpl wtx)
				throws SirixException {
			// Not allowed.
			throw new AssertionError("May never be invoked!");
		}

		@Override
		void insertNode(final @Nonnull NodeWriteTrx wtx,
				final @Nonnull NodeReadTrx rtx) throws SirixException {
			assert wtx != null;
			assert rtx != null;
			assert wtx.getKind() == Kind.ELEMENT;
			switch (rtx.getKind()) {
			case NAMESPACE:
				final QName name = rtx.getName();
				wtx.insertNamespace(new QName(name.getNamespaceURI(), "", name
						.getLocalPart()));
				wtx.moveToParent();
				break;
			case ATTRIBUTE:
				wtx.insertAttribute(rtx.getName(), rtx.getValue());
				wtx.moveToParent();
				break;
			default:
				throw new IllegalStateException(
						"Only namespace- and attribute-nodes are permitted!");
			}
		}
	},

	ASLEFTSIBLING {
		@Override
		void processMove(final @Nonnull StructNode pFromNode,
				final @Nonnull StructNode pToNode, final @Nonnull NodeWriteTrxImpl pWtx)
				throws SirixException {
			throw new UnsupportedOperationException();
		}

		@Override
		void insertNode(final @Nonnull NodeWriteTrx pWtx,
				final @Nonnull NodeReadTrx pRtx) throws SirixException {
			assert pWtx != null;
			assert pRtx != null;
			assert pWtx.getKind() == Kind.ELEMENT || pWtx.getKind() == Kind.TEXT;
			switch (pRtx.getKind()) {
			case ELEMENT:
				pWtx.insertElementAsLeftSibling(pRtx.getName());
				break;
			case TEXT:
				pWtx.insertTextAsLeftSibling(pRtx.getValue());
				break;
			default:
				throw new IllegalStateException("Node type not known!");
			}
		}
	};

	/**
	 * Process movement of a subtree.
	 * 
	 * @param fromNode
	 *          root of subtree to move
	 * @param toNode
	 *          determines where the subtree has to be inserted
	 * @param wtx
	 *          write-transaction which implements the {@link NodeWriteTrx}
	 *          interface
	 * @throws SirixException
	 *           if an I/O error occurs
	 */
	abstract void processMove(final @Nonnull StructNode fromNode,
			final @Nonnull StructNode toNode, final @Nonnull NodeWriteTrxImpl wtx)
			throws SirixException;

	/**
	 * Insert a node (copy operation).
	 * 
	 * @param rtx
	 *          read-transaction which implements the {@link NodeReadTrx}
	 *          interface
	 * @param wtx
	 *          write-transaction which implements the {@link NodeWriteTrx}
	 *          interface
	 * @throws SirixException
	 *           if insertion of node fails
	 */
	abstract void insertNode(final @Nonnull NodeWriteTrx wtx,
			final @Nonnull NodeReadTrx rtx) throws SirixException;
}
