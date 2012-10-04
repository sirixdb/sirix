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
package org.sirix.diff.algorithm.fmse;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.namespace.QName;

import org.sirix.access.Utils;
import org.sirix.api.IAxis;
import org.sirix.api.INodeReadTrx;
import org.sirix.api.INodeWriteTrx;
import org.sirix.api.visitor.IVisitor;
import org.sirix.axis.AbsAxis;
import org.sirix.axis.ChildAxis;
import org.sirix.axis.DescendantAxis;
import org.sirix.axis.EIncludeSelf;
import org.sirix.axis.LevelOrderAxis;
import org.sirix.axis.LevelOrderAxis.EIncludeNodes;
import org.sirix.axis.PostOrderAxis;
import org.sirix.axis.visitor.DeleteFMSEVisitor;
import org.sirix.axis.visitor.VisitorDescendantAxis;
import org.sirix.diff.algorithm.IImportDiff;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixUsageException;
import org.sirix.node.EKind;
import org.sirix.node.TextNode;
import org.sirix.node.interfaces.INode;
import org.sirix.utils.LogWrapper;
import org.sirix.utils.Pair;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;

/**
 * Provides the fast match / edit script (fmes) tree to tree correction
 * algorithm as described in "Change detection in hierarchically structured
 * information" by S. Chawathe, A. Rajaraman, H. Garcia-Molina and J. Widom
 * Stanford University, 1996 ([CRGMW95]) <br>
 * FMES is used by the <a href="http://www.logilab.org/projects/xmldiff">python
 * script</a> xmldiff from Logilab. <br>
 * 
 * Based on the FMES version of Daniel Hottinger and Franziska Meyer.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public final class FMSE implements IImportDiff, AutoCloseable {

	/** {@link LogWrapper} reference. */
	private static final LogWrapper LOGWRAPPER = new LogWrapper(
			LoggerFactory.getLogger(FMSE.class));

	/** Determines if a reverse lookup has to be made. */
	enum EReverseMap {
		/** Yes, reverse lookup. */
		TRUE,

		/** No, normal lookup. */
		FALSE
	}

	/** Algorithm name. */
	private static final String NAME = "Fast Matching / Edit Script";

	/**
	 * Matching Criterion 1. For the "good matching problem", the following
	 * conditions must hold for leafs x and y:
	 * <ul>
	 * <li>label(x) == label(y)</li>
	 * <li>compare(value(x), value(y)) <= FMESF</li>
	 * </ul>
	 * where FMESF is in the range [0,1] and compare() computes the cost of
	 * updating a leaf node.
	 */
	private static final double FMESF = 0.5;

	/**
	 * Matching Criterion 2. For the "good matching problem", the following
	 * conditions must hold inner nodes x and y:
	 * <ul>
	 * <li>label(x) == label(y)</li>
	 * <li>|common(x,y)| / max(|x|, |y|) > FMESTHRESHOLD</li>
	 * </ul>
	 * where FMESTHRESHOLD is in the range [0.5, 1] and common(x,y) computes the
	 * number of leafs that can be matched between x and y.
	 */
	private static final double FMESTHRESHOLD = 0.5;

	/**
	 * Used by emitInsert: when inserting a whole subtree - keep track that nodes
	 * are not inserted multiple times.
	 */
	private transient Map<Long, Boolean> mAlreadyInserted;

	/**
	 * This is the matching M between nodes as described in the paper.
	 */
	private transient Matching mFastMatching;

	/**
	 * This is the total matching M' between nodes as described in the paper.
	 */
	private transient Matching mTotalMatching;

	/**
	 * Stores the in-order property for each node for old revision.
	 */
	private transient Map<Long, Boolean> mInOrderOldRev;

	/**
	 * Stores the in-order property for each node for new revision.
	 */
	private transient Map<Long, Boolean> mInOrderNewRev;

	/**
	 * Number of descendants in subtree of node on old revision.
	 */
	private transient Map<Long, Long> mDescendantsOldRev;

	/**
	 * Number of descendants in subtree of node on new revision.
	 */
	private transient Map<Long, Long> mDescendantsNewRev;

	/** {@link IVisitor} implementation on old revision. */
	private transient IVisitor mOldRevVisitor;

	/** {@link IVisitor} implementation on new revision. */
	private transient IVisitor mNewRevVisitor;

	/** {@link IVisitor} implementation to collect label/nodes on old revision. */
	private transient LabelFMSEVisitor mLabelOldRevVisitor;

	/** {@link IVisitor} implementation to collect label/nodes on new revision. */
	private transient LabelFMSEVisitor mLabelNewRevVisitor;

	/** sirix {@link INodeWriteTrx}. */
	private transient INodeWriteTrx mWtx;

	/** sirix {@link INodeReadTrx}. */
	private transient INodeReadTrx mRtx;

	/** Start key of old revision. */
	private transient long mOldStartKey;

	/** Start key of new revision. */
	private transient long mNewStartKey;

	/** Max length for Levenshtein comparsion. */
	private static final int MAX_LENGTH = 50;

	/** {@code Levenshtein} reference, can be reused among instances. */
	private static final Levenshtein mLevenshtein = new Levenshtein();

	@Override
	public void diff(final INodeWriteTrx pWtx, final INodeReadTrx pRtx)
			throws SirixException {
		mWtx = checkNotNull(pWtx);
		mRtx = checkNotNull(pRtx);
		mOldStartKey = mWtx.getNodeKey();
		mNewStartKey = mRtx.getNodeKey();
		mDescendantsOldRev = new HashMap<>();
		mDescendantsNewRev = new HashMap<>();
		mInOrderOldRev = new HashMap<>();
		mInOrderNewRev = new HashMap<>();
		mAlreadyInserted = new HashMap<>();

		mOldRevVisitor = new FMSEVisitor(mWtx, mInOrderOldRev, mDescendantsOldRev);
		mNewRevVisitor = new FMSEVisitor(mRtx, mInOrderNewRev, mDescendantsNewRev);

		mLabelOldRevVisitor = new LabelFMSEVisitor(mWtx);
		mLabelNewRevVisitor = new LabelFMSEVisitor(mRtx);
		init(mWtx, mOldRevVisitor);
		init(mRtx, mNewRevVisitor);
		mFastMatching = fastMatch(mWtx, mRtx);
		mTotalMatching = new Matching(mFastMatching);
		firstFMESStep(mWtx, mRtx);
		try {
			secondFMESStep(mWtx, mRtx);
		} catch (final SirixException e) {
			LOGWRAPPER.error(e.getMessage(), e);
		}
	}

	/**
	 * First step of the edit script algorithm. Combines the update, insert, align
	 * and move phases.
	 * 
	 * @param pWtx
	 *          {@link INodeWriteTrx} implementation reference on old revisionso
	 * @param pRtxn
	 *          {@link INodeReadTrx} implementation reference o new revision
	 */
	private void firstFMESStep(final INodeWriteTrx pWtx, final INodeReadTrx pRtx) {
		assert pWtx != null;
		assert pRtx != null;

		pWtx.moveTo(mOldStartKey);
		pRtx.moveTo(mNewStartKey);

		// 2. Iterate over new shreddered file
		for (final IAxis axis = new LevelOrderAxis.Builder(pRtx)
				.includeSelf(EIncludeSelf.YES)
				.includeNodes(EIncludeNodes.NONSTRUCTURAL).build(); axis.hasNext();) {
			axis.next();
			final long nodeKey = axis.getTrx().getNodeKey();
			doFirstFSMEStep(pWtx, pRtx);
			axis.getTrx().moveTo(nodeKey);
		}
	}

	/**
	 * Do the actual first step of FSME.
	 * 
	 * @param pWtx
	 *          {@link INodeWriteTrx} implementation reference on old revision
	 * @param pRtx
	 *          {@link INodeReadTrx} implementation reference on new revision
	 * @throws SirixException
	 *           if anything in sirix fails
	 */
	private void doFirstFSMEStep(final INodeWriteTrx pWtx, final INodeReadTrx pRtx) {
		assert pWtx != null;
		assert pRtx != null;
		// 2(a) - Parent of x.
		final long key = pRtx.getNodeKey();
		final long x = pRtx.getNodeKey();
		pRtx.moveToParent();
		final long y = pRtx.getNodeKey();

		final Long z = mTotalMatching.reversePartner(y);
		Long w = mTotalMatching.reversePartner(x);

		pWtx.moveTo(mOldStartKey);
		// 2(b) - insert
		if (w == null) {
			// x has no partner.
			assert z != null;
			mInOrderNewRev.put(x, true);
			final int k = findPos(x, pWtx, pRtx);
			assert k > -1;
			w = emitInsert(x, z, k, pWtx, pRtx);
		} else if (x != pWtx.getNodeKey()) {
			// 2(c) not the root (x has a partner in M').
			if (pWtx.moveTo(w).hasMoved()
					&& pRtx.moveTo(x).hasMoved()
					&& pWtx.getKind() == pRtx.getKind()
					&& (!nodeValuesEqual(w, x, pWtx, pRtx) || (pRtx.getKind() == EKind.ATTRIBUTE && !pRtx
							.getValue().equals(pWtx.getValue())))) {
				// Either QNames differ or the values in case of attribute nodes.
				emitUpdate(w, x, pWtx, pRtx);
			}
			pWtx.moveTo(w);
			pWtx.moveToParent();
			final long v = pWtx.getNodeKey();
			if (!mTotalMatching.contains(v, y) && pWtx.moveTo(w).hasMoved()
					&& pRtx.moveTo(x).hasMoved()) {
				assert z != null;
				mInOrderNewRev.put(x, true);
				pRtx.moveTo(x);
				if (pRtx.getKind() == EKind.NAMESPACE
						|| pRtx.getKind() == EKind.ATTRIBUTE) {
					pWtx.moveTo(w);
					try {
						mTotalMatching.remove(w);
						pWtx.remove();
					} catch (final SirixException e) {
						LOGWRAPPER.error(e.getMessage(), e);
					}
					w = emitInsert(x, z, -1, pWtx, pRtx);
				} else {
					final int k = findPos(x, pWtx, pRtx);
					assert k > -1;
					emitMove(w, z, k, pWtx, pRtx);
				}
			}
		}

		alignChildren(w, x, pWtx, pRtx);
		pRtx.moveTo(key);
	}

	/**
	 * Second step of the edit script algorithm. This is the delete phase.
	 * 
	 * @param pWtx
	 *          {@link INodeWriteTrx} implementation reference on old revision
	 * @param pRtx
	 *          {@link INodeReadTrx} implementation reference on new revision
	 */
	private void secondFMESStep(final INodeWriteTrx pWtx, final INodeReadTrx pRtx)
			throws SirixException {
		assert pWtx != null;
		assert pRtx != null;
		pWtx.moveTo(mOldStartKey);
		for (final IAxis axis = new VisitorDescendantAxis.Builder(pWtx)
				.includeSelf()
				.visitor(
						Optional.<DeleteFMSEVisitor> of(new DeleteFMSEVisitor(pWtx,
								mTotalMatching, mOldStartKey))).build(); axis.hasNext();) {
			axis.next();
		}
	}

	/**
	 * Alignes the children of a node x according the the children of node w.
	 * 
	 * @param pW
	 *          node in the first document
	 * @param pX
	 *          node in the second document
	 * @param pWtx
	 *          {@link INodeWriteTrx} implementation reference on old revision
	 * @param pRtx
	 *          {@link INodeReadTrx} implementation reference on new revision
	 */
	private void alignChildren(final long pW, final long pX,
			final INodeWriteTrx pWtx, final INodeReadTrx pRtx) {
		assert pW >= 0;
		assert pX >= 0;
		assert pWtx != null;
		assert pRtx != null;

		pWtx.moveTo(pW);
		pRtx.moveTo(pX);

		// Mark all children of w and all children of x "out of order".
		markOutOfOrder(pWtx, mInOrderOldRev);
		markOutOfOrder(pRtx, mInOrderNewRev);

		// 2
		final List<Long> first = commonChildren(pW, pX, pWtx, pRtx,
				EReverseMap.FALSE);
		final List<Long> second = commonChildren(pX, pW, pRtx, pWtx,
				EReverseMap.TRUE);
		// 3 && 4
		final List<Pair<Long, Long>> s = Util.longestCommonSubsequence(first,
				second, new IComparator<Long>() {
					@Override
					public boolean isEqual(final Long pX, final Long pY) {
						return mTotalMatching.contains(pX, pY);
					}
				});
		// 5
		final Map<Long, Long> seen = new HashMap<>();
		for (final Pair<Long, Long> p : s) {
			mInOrderOldRev.put(p.getFirst(), true);
			mInOrderNewRev.put(p.getSecond(), true);
			seen.put(p.getFirst(), p.getSecond());
		}
		// 6
		for (final long a : first) {
			pWtx.moveTo(a);
			final Long b = mTotalMatching.partner(a);
			// assert b != null;
			if (seen.get(a) == null && pWtx.moveTo(a).hasMoved() && b != null
					&& pRtx.moveTo(b).hasMoved()) { // (a,
				// b)
				// \notIn
				// S
				// if (seen.get(a) == null || (seen.get(a) != null &&
				// !seen.get(a).equals(b))
				// && mInOrderOldRev.get(a) != null && !mInOrderOldRev.get(a)) { // (a,
				// b) \notIn S
				mInOrderOldRev.put(a, true);
				mInOrderNewRev.put(b, true);
				final int k = findPos(b, pWtx, pRtx);
				LOGWRAPPER.debug("Move in align children: " + k);
				emitMove(a, pW, k, pWtx, pRtx);
			}
		}
	}

	/**
	 * Mark children out of order.
	 * 
	 * @param pRtx
	 *          {@link INodeReadTrx} reference
	 * @param pInOrder
	 *          {@link Map} to put all children out of order
	 */
	private void markOutOfOrder(final INodeReadTrx pRtx,
			final Map<Long, Boolean> pInOrder) {
		for (final AbsAxis axis = new ChildAxis(pRtx); axis.hasNext();) {
			axis.next();
			pInOrder.put(axis.getTrx().getNodeKey(), false);
		}
	}

	/**
	 * The sequence of children of n whose partners are children of o. This is
	 * used by alignChildren().
	 * 
	 * @param pN
	 *          parent node in a document tree
	 * @param pO
	 *          corresponding parent node in the other tree
	 * @param pFirstRtx
	 *          {@link INodeReadTrx} on pN node
	 * @param pSecondRtx
	 *          {@link INodeReadTrx} on pO node
	 * @param pReverse
	 *          determines if reverse partners need to be found
	 * @return {@link List} of common child nodes
	 */
	private List<Long> commonChildren(final long pN, final long pO,
			final INodeReadTrx pFirstRtx, final INodeReadTrx pSecondRtx,
			final EReverseMap pReverse) {
		assert pN >= 0;
		assert pO >= 0;
		assert pFirstRtx != null;
		assert pSecondRtx != null;
		assert pReverse != null;
		final List<Long> retVal = new LinkedList<>();
		pFirstRtx.moveTo(pN);
		if (pFirstRtx.hasFirstChild()) {
			pFirstRtx.moveToFirstChild();

			do {
				Long partner;
				if (pReverse == EReverseMap.TRUE) {
					partner = mTotalMatching.reversePartner(pFirstRtx.getNodeKey());
				} else {
					partner = mTotalMatching.partner(pFirstRtx.getNodeKey());
				}

				if (partner != null) {
					pSecondRtx.moveTo(partner);
					if (pSecondRtx.getParentKey() == pO) {
						retVal.add(pFirstRtx.getNodeKey());
					}
				}
			} while (pFirstRtx.hasRightSibling()
					&& pFirstRtx.moveToRightSibling().hasMoved());
		}
		return retVal;
	}

	/**
	 * Emits the move of node "child" to the pPos-th child of node "parent".
	 * 
	 * @param pChild
	 *          child node to move
	 * @param pParent
	 *          node where to insert the moved subtree
	 * @param pPos
	 *          position among the childs to move to
	 * @param pWtx
	 *          {@link INodeWriteTrx} implementation reference on old revision
	 * @param pRtx
	 *          {@link INodeReadTrx} implementation reference on new revision
	 */
	private long emitMove(final long pChild, final long pParent, int pPos,
			final INodeWriteTrx pWtx, final INodeReadTrx pRtx) {
		assert pChild >= 0;
		assert pParent >= 0;
		assert pWtx != null;
		assert pRtx != null;

		boolean moved = pWtx.moveTo(pChild).hasMoved();
		assert moved;

		if (pWtx.getKind() == EKind.ATTRIBUTE || pWtx.getKind() == EKind.NAMESPACE) {
			// Attribute- and namespace-nodes can't be moved.
			return -1;
		}

		assert pPos >= 0;
		moved = pWtx.moveTo(pParent).hasMoved();
		assert moved;

		try {
			if (pPos == 0) {
				assert pWtx.getKind() == EKind.ELEMENT
						|| pWtx.getKind() == EKind.DOCUMENT_ROOT;
				if (pWtx.getFirstChildKey() == pChild) {
					LOGWRAPPER
							.error("Something went wrong: First child and child may never be the same!");
				} else {
					if (pWtx.moveTo(pChild).hasMoved()) {
						boolean isTextKind = false;
						if (pWtx.getKind() == EKind.TEXT) {
							isTextKind = true;
						}

						checkFromNodeForTextRemoval(pWtx, pChild);
						pWtx.moveTo(pParent);
						if (isTextKind && pWtx.getFirstChildKey() != pChild) {
							if (pWtx.hasFirstChild()) {
								pWtx.moveToFirstChild();
								if (pWtx.getKind() == EKind.TEXT) {
									mTotalMatching.remove(pWtx.getNodeKey());
									pWtx.remove();
								}
								pWtx.moveTo(pParent);
							}
						}

						if (pWtx.getKind() == EKind.DOCUMENT_ROOT) {
							pRtx.moveTo(pChild);
							pWtx.moveTo(pWtx.copySubtreeAsFirstChild(pRtx).getNodeKey());
						} else {
							pWtx.moveTo(pWtx.moveSubtreeToFirstChild(pChild).getNodeKey());
						}
					}
				}
			} else {
				assert pWtx.hasFirstChild();
				pWtx.moveToFirstChild();

				for (int i = 1; i < pPos; i++) {
					assert pWtx.hasRightSibling();
					pWtx.moveToRightSibling();
				}

				// Check if text nodes are getting collapsed and remove mappings
				// accordingly.
				// ===========================================================================
				final long nodeKey = pWtx.getNodeKey();
				checkFromNodeForTextRemoval(pWtx, pChild);
				pWtx.moveTo(nodeKey);
				if (pWtx.getKind() == EKind.TEXT && pWtx.moveTo(pChild).hasMoved()
						&& pWtx.getKind() == EKind.TEXT) {
					pWtx.moveTo(nodeKey);
					mTotalMatching.remove(pWtx.getNodeKey());
				}
				pWtx.moveTo(nodeKey);
				if (pWtx.moveToRightSibling().hasMoved()) {
					final long rightNodeKey = pWtx.getNodeKey();
					if (pWtx.getKind() == EKind.TEXT && pWtx.moveTo(pChild).hasMoved()
							&& pWtx.getKind() == EKind.TEXT) {
						pWtx.moveTo(rightNodeKey);
						mTotalMatching.remove(pWtx.getNodeKey());
					}
					pWtx.moveToLeftSibling();
				}

				// Move.
				moved = pWtx.moveTo(nodeKey).hasMoved();
				assert moved;
				assert pWtx.getNodeKey() != pChild;

				pWtx.moveTo(pWtx.moveSubtreeToRightSibling(pChild).getNodeKey());
			}
		} catch (final SirixException e) {
			LOGWRAPPER.error(e.getMessage(), e);
		}

		return pWtx.getNodeKey();
	}

	private void checkFromNodeForTextRemoval(final INodeWriteTrx pWtx,
			final long pChild) {
		boolean maybeRemoveLeftSibling = pWtx.getLeftSiblingKey() == pChild ? true
				: false;
		if (pWtx.moveTo(pChild).hasMoved()) {
			boolean isText = false;
			if (pWtx.hasLeftSibling()) {
				pWtx.moveToLeftSibling();
				if (pWtx.getKind() == EKind.TEXT) {
					isText = true;
				}
				pWtx.moveToRightSibling();
			}
			if (isText && pWtx.hasRightSibling()) {
				pWtx.moveToRightSibling();
				if (pWtx.getKind() == EKind.TEXT && isText) {
					if (maybeRemoveLeftSibling) {
						boolean moved = pWtx.moveToLeftSibling().hasMoved();
						assert moved;
						moved = pWtx.moveToLeftSibling().hasMoved();
						assert moved;
						mTotalMatching.remove(pWtx.getNodeKey());
					} else {
						mTotalMatching.remove(pWtx.getNodeKey());
					}
				}
				pWtx.moveToLeftSibling();
			}
		}
	}

	/**
	 * Emit an update.
	 * 
	 * @param pFromNode
	 *          the node to update
	 * @param pToNode
	 *          the new node
	 * @param pWtxnull
	 *          {@link INodeWriteTrx} implementation reference on old revision
	 * @param pRtx
	 *          {@link INodeReadTrx} implementation reference on new revision
	 * @return updated {@link INode}
	 */
	private long emitUpdate(final long pFromNode, final long pToNode,
			final INodeWriteTrx pWtx, final INodeReadTrx pRtx) {
		assert pFromNode >= 0;
		assert pToNode >= 0;
		assert pWtx != null;
		assert pRtx != null;

		pWtx.moveTo(pFromNode);
		pRtx.moveTo(pToNode);

		try {
			switch (pRtx.getKind()) {
			case ELEMENT:
			case ATTRIBUTE:
			case NAMESPACE:
				assert pRtx.getKind() == EKind.ELEMENT
						|| pRtx.getKind() == EKind.ATTRIBUTE
						|| pRtx.getKind() == EKind.NAMESPACE;
				pWtx.setQName(pRtx.getName());

				if (pWtx.getKind() == EKind.ATTRIBUTE) {
					pWtx.setValue(pRtx.getValue());
				}
				break;
			case TEXT:
				assert pWtx.getKind() == EKind.TEXT;
				pWtx.setValue(pRtx.getValue());
				break;
			default:
			}
		} catch (final SirixException e) {
			LOGWRAPPER.error(e.getMessage(), e);
		}

		return pWtx.getNodeKey();
	}

	/**
	 * Emit an insert operation.
	 * 
	 * @param pParent
	 *          parent of the current {@link INode} implementation reference to
	 *          insert
	 * @param pChild
	 *          the current node to insert
	 * @param pPos
	 *          position of the insert
	 * @param pWtx
	 *          {@link INodeWriteTrx} implementation reference on old revision
	 * @param pRtx
	 *          {@link INodeReadTrx} implementation reference on new revision
	 * @return inserted {@link INode} implementation reference
	 * @throws SirixException
	 *           if anything in sirix fails
	 */
	private long emitInsert(final long pChild, final long pParent,
			final int pPos, final INodeWriteTrx pWtx, final INodeReadTrx pRtx) {
		assert pChild >= 0;
		assert pParent >= 0;
		assert pWtx != null;
		assert pRtx != null;

		// Determines if node has been already inserted (for subtrees).
		if (mAlreadyInserted.get(pChild) != null) {
			return pChild; // actually child'
		}

		pWtx.moveTo(pParent);
		pRtx.moveTo(pChild);

		try {
			switch (pRtx.getKind()) {
			case ATTRIBUTE:
				try {
					pWtx.insertAttribute(pRtx.getName(), pRtx.getValue());
				} catch (final SirixUsageException e) {
					mTotalMatching.remove(pWtx.getNodeKey());
					pWtx.setValue(pRtx.getValue());
				}
				process(pWtx.getNodeKey(), pRtx.getNodeKey());
				break;
			case NAMESPACE:
				// Note that the insertion is right (localPart as prefix).
				try {
					pWtx.insertNamespace(new QName(pRtx.getName().getNamespaceURI(), "",
							pRtx.getName().getLocalPart()));
				} catch (final SirixUsageException e) {
					mTotalMatching.remove(pWtx.getNodeKey());
				}
				process(pWtx.getNodeKey(), pRtx.getNodeKey());
				break;
			default:
				// In case of other node types.
				long oldKey = 0;
				if (pPos == 0) {
					switch (pRtx.getKind()) {
					case ELEMENT:
						oldKey = pWtx.copySubtreeAsFirstChild(pRtx).getNodeKey();
						break;
					case TEXT:
						// Remove first child text node if there is one and a new text node
						// is inserted.
						if (pWtx.hasFirstChild()) {
							pWtx.moveToFirstChild();
							if (pWtx.getKind() == EKind.TEXT) {
								mTotalMatching.remove(pWtx.getNodeKey());
								pWtx.remove();
							}
							pWtx.moveTo(pParent);
						}

						oldKey = pWtx.insertTextAsFirstChild(pRtx.getValue()).getNodeKey();
						break;
					default:
						// Already inserted.
					}
				} else {
					assert pWtx.hasFirstChild();
					pWtx.moveToFirstChild();
					for (int i = 0; i < pPos - 1; i++) {
						assert pWtx.hasRightSibling();
						pWtx.moveToRightSibling();
					}

					// Remove right sibl. text node if a text node already exists.
					removeRightSiblingTextNode(pWtx);
					switch (pRtx.getKind()) {
					case ELEMENT:
						oldKey = pWtx.copySubtreeAsRightSibling(pRtx).getNodeKey();
						break;
					case TEXT:
						oldKey = pWtx.insertTextAsRightSibling(pRtx.getValue())
								.getNodeKey();
						break;
					default:
						// Already inserted.
						throw new IllegalStateException("Child should be already inserted!");
					}
				}

				// Mark all nodes in subtree as inserted.
				pWtx.moveTo(oldKey);
				pRtx.moveTo(pChild);
				for (final IAxis oldAxis = new DescendantAxis(pWtx, EIncludeSelf.YES), newAxis = new DescendantAxis(
						pRtx, EIncludeSelf.YES); oldAxis.hasNext() && newAxis.hasNext();) {
					oldAxis.next();
					newAxis.next();
					final INodeReadTrx oldRtx = oldAxis.getTrx();
					final INodeReadTrx newRtx = newAxis.getTrx();
					process(oldRtx.getNodeKey(), newRtx.getNodeKey());
					final long newNodeKey = newRtx.getNodeKey();
					final long oldNodeKey = oldRtx.getNodeKey();
					if (newRtx.getKind() == EKind.ELEMENT) {
						assert newRtx.getKind() == oldRtx.getKind();
						if (newRtx.getAttributeCount() > 0) {
							for (int i = 0, attCount = newRtx.getAttributeCount(); i < attCount; i++) {
								pRtx.moveToAttribute(i);
								for (int j = 0, oldAttCount = oldRtx.getAttributeCount(); i < oldAttCount; j++) {
									pWtx.moveToAttribute(j);
									if (pWtx.getName().equals(pRtx.getName())) {
										process(oldAxis.getTrx().getNodeKey(), newAxis.getTrx()
												.getNodeKey());
										break;
									}
									oldAxis.getTrx().moveTo(oldNodeKey);
								}
								newAxis.getTrx().moveTo(newNodeKey);
							}
						}
						if (newRtx.getNamespaceCount() > 0) {
							for (int i = 0, nspCount = newRtx.getNamespaceCount(); i < nspCount; i++) {
								pRtx.moveToNamespace(i);
								for (int j = 0, oldNspCount = oldRtx.getNamespaceCount(); j < oldNspCount; j++) {
									pWtx.moveToNamespace(j);
									if (pWtx.getName().getNamespaceURI()
											.equals(pRtx.getName().getNamespaceURI())
											&& pWtx.getName().getPrefix()
													.equals(pWtx.getName().getPrefix())) {
										process(pWtx.getNodeKey(), pRtx.getNodeKey());
										break;
									}
									oldAxis.getTrx().moveTo(oldNodeKey);
								}
								newAxis.getTrx().moveTo(newNodeKey);
							}
						}
					}

					newAxis.getTrx().moveTo(newNodeKey);
				}
			}
		} catch (final SirixException e) {
			LOGWRAPPER.error(e.getMessage(), e);
		}

		return pWtx.getNodeKey();
	}

	/**
	 * Remove right sibling text node from the storage as well as from the
	 * matching.
	 * 
	 * @param pWtx
	 *          sirix {@link INodeWriteTrx}
	 * @throws SirixException
	 *           if removing of node in the storage fails
	 */
	private void removeRightSiblingTextNode(final INodeWriteTrx pWtx)
			throws SirixException {
		assert pWtx != null;
		if (pWtx.hasRightSibling()) {
			final long nodeKey = pWtx.getNodeKey();
			pWtx.moveToRightSibling();
			if (pWtx.getKind() == EKind.TEXT) {
				mTotalMatching.remove(pWtx.getNodeKey());
				pWtx.remove();
			}
			pWtx.moveTo(nodeKey);
		}
	}

	/**
	 * Process nodes and add update data structures.
	 * 
	 * @param pOldKey
	 *          {@link INode} in old revision
	 * @param pNewKey
	 *          {@link INode} in new revision
	 */
	private void process(final long pOldKey, final long pNewKey) {
		mAlreadyInserted.put(pNewKey, true);
		final Long partner = mTotalMatching.partner(pOldKey);
		if (partner != null) {
			mTotalMatching.remove(pOldKey);
		}
		final Long reversePartner = mTotalMatching.reversePartner(pNewKey);
		if (reversePartner != null) {
			mTotalMatching.remove(reversePartner);
		}
		assert !mTotalMatching.contains(pOldKey, pNewKey);
		mTotalMatching.add(pOldKey, pNewKey);
		mInOrderOldRev.put(pOldKey, true);
		mInOrderNewRev.put(pNewKey, true);
	}

	/**
	 * The position of node x in the destination tree (tree2).
	 * 
	 * @param pX
	 *          a node in the second (new) document
	 * @param pWtx
	 *          {@link INodeWriteTrx} implementation reference on old revision
	 * @param pRtx
	 *          {@link INodeReadTrx} implementation reference on new revision
	 * @return it's position, with respect to already inserted/deleted nodes
	 */
	private int findPos(final long pX, final INodeWriteTrx pWtx,
			final INodeReadTrx pRtx) {
		assert pX > 0;
		assert pWtx != null;
		assert pRtx != null;
		pRtx.moveTo(pX);

		if (pRtx.getKind() == EKind.ATTRIBUTE || pRtx.getKind() == EKind.NAMESPACE) {
			return 0;
		} else {
			final long nodeKey = pRtx.getNodeKey();
			// 1 - Let y = p(x) in T2.
			// if (pRtx.getItem().getKind() == ENode.ATTRIBUTE_KIND
			// || pRtx.getItem().getKind() == ENode.NAMESPACE_KIND) {
			// pRtx.moveToParent();
			// }
			pRtx.moveToParent();

			// 2 - If x is the leftmost child of y that is marked "in order", return
			// 0.
			if (pRtx.hasFirstChild()) {
				pRtx.moveToFirstChild();

				final long v = pRtx.getNodeKey();
				if (mInOrderNewRev.get(v) != null && mInOrderNewRev.get(v) && v == pX) {
					return 0;
				}
			}

			// 3 - Find v \in T2 where v is the rightmost sibling of x
			// that is to the left of x and is marked "in order".
			pRtx.moveTo(nodeKey);
			// if (pRtx.getItem().getKind() == ENode.ATTRIBUTE_KIND
			// || pRtx.getItem().getKind() == ENode.NAMESPACE_KIND) {
			// pRtx.moveToParent();
			// }
			pRtx.moveToLeftSibling();
			long v = pRtx.getNodeKey();
			while (pRtx.hasLeftSibling()
					&& (mInOrderNewRev.get(v) == null || (mInOrderNewRev.get(v) != null && !mInOrderNewRev
							.get(v)))) {
				pRtx.moveToLeftSibling();
				v = pRtx.getNodeKey();
			}

			// Step 2 states that in ``in order'' node exists, but this is not
			// true.
			if (mInOrderNewRev.get(v) == null) {
				// Assume it is the first node (undefined in the paper).
				return 0;
			}

			// 4 - Let u be the partner of v in T1
			Long u = mTotalMatching.reversePartner(v);
			int i = -1;
			if (u != null) {
				boolean moved = pWtx.moveTo(u).hasMoved();
				assert moved;

				// Suppose u is the i-th child of its parent (counting from left to
				// right) that is marked "in order". Return i+1.
				final long toNodeKey = u;
				pWtx.moveToParent();
				pWtx.moveToFirstChild();
				do {
					u = pWtx.getNodeKey();
					i++;
				} while (u != toNodeKey && pWtx.hasRightSibling()
						&& pWtx.moveToRightSibling().hasMoved());
			}

			return i + 1;
		}
	}

	/**
	 * The fast match algorithm. Try to resolve the "good matching problem".
	 * 
	 * @param pWtx
	 *          {@link INodeWriteTrx} implementation reference on old revision
	 * @param pRtx
	 *          {@link INodeReadTrx} implementation reference on new revision
	 * @return {@link Matching} reference with matched nodes
	 * @throws SirixException
	 *           if anything in sirix fails
	 */
	private Matching fastMatch(final INodeWriteTrx pWtx, final INodeReadTrx pRtx) {
		assert pWtx != null;
		assert pRtx != null;

		// Chain all nodes with a given label l in tree T together.
		getLabels(pWtx, mLabelOldRevVisitor);
		getLabels(pRtx, mLabelNewRevVisitor);

		// Do the matching job on the leaf nodes.
		final Matching matching = new Matching(pWtx, pRtx);
		matching.reset();
		match(mLabelOldRevVisitor.getLeafLabels(),
				mLabelNewRevVisitor.getLeafLabels(), matching, new LeafEqual());

		// Remove roots ('/') from labels and append them to mapping.
		final Map<EKind, List<Long>> oldLabels = mLabelOldRevVisitor.getLabels();
		final Map<EKind, List<Long>> newLabels = mLabelNewRevVisitor.getLabels();
		oldLabels.remove(EKind.DOCUMENT_ROOT);
		newLabels.remove(EKind.DOCUMENT_ROOT);

		pWtx.moveTo(mOldStartKey);
		pRtx.moveTo(mNewStartKey);
		pWtx.moveToParent();
		pRtx.moveToParent();
		matching.add(pWtx.getNodeKey(), pRtx.getNodeKey());

		match(oldLabels, newLabels, matching, new InnerNodeEqual(matching));

		return matching;
	}

	/**
	 * Actual matching.
	 * 
	 * @param pOldLabels
	 *          nodes in tree1, sorted by node type (element, attribute, text,
	 *          comment, ...)
	 * @param pNewLabels
	 *          nodes in tree2, sorted by node type (element, attribute, text,
	 *          comment, ...)
	 * @param pMatching
	 *          {@link Matching} reference
	 * @param pCmp
	 *          functional class
	 */
	private void match(final Map<EKind, List<Long>> pOldLabels,
			final Map<EKind, List<Long>> pNewLabels, final Matching pMatching,
			final IComparator<Long> pCmp) {
		final Set<EKind> labels = pOldLabels.keySet();
		labels.retainAll(pNewLabels.keySet()); // intersection

		// 2 - for each label do
		for (final EKind label : labels) {
			final List<Long> first = pOldLabels.get(label); // 2(a)
			final List<Long> second = pNewLabels.get(label); // 2(b)

			// 2(c)
			final List<Pair<Long, Long>> common = Util.longestCommonSubsequence(
					first, second, pCmp);
			// Used to remove the nodes in common from s1 and s2 in step 2(e).
			final Map<Long, Boolean> seen = new HashMap<>();

			// 2(d) - for each pair of nodes in the lcs: add to matching.
			for (final Pair<Long, Long> p : common) {
				pMatching.add(p.getFirst(), p.getSecond());
				seen.put(p.getFirst(), true);
				seen.put(p.getSecond(), true);
			}

			// 2(e) (prepare) - remove nodes in common from s1, s2.
			removeCommonNodes(first, seen);
			removeCommonNodes(second, seen);

			// 2(e) - For each unmatched node x \in s1.
			final Iterator<Long> firstIterator = first.iterator();
			while (firstIterator.hasNext()) {
				final Long firstItem = firstIterator.next();
				boolean firstIter = true;
				// If there is an unmatched node y \in s2.
				final Iterator<Long> secondIterator = second.iterator();
				while (secondIterator.hasNext()) {
					final Long secondItem = secondIterator.next();
					// Such that equal.
					if (pCmp.isEqual(firstItem, secondItem)) {
						// 2(e)A
						pMatching.add(firstItem, secondItem);

						// 2(e)B
						if (firstIter) {
							firstIter = false;
							firstIterator.remove();
						}
						secondIterator.remove();
						break;
					}
				}
			}
		}
	}

	/**
	 * Remove nodes in common.
	 * 
	 * @param pList
	 *          {@link List} of {@link INode}s
	 * @param pSeen
	 *          {@link Map} of {@link INode}s
	 */
	private void removeCommonNodes(final List<Long> pList,
			final Map<Long, Boolean> pSeen) {
		assert pList != null;
		assert pSeen != null;

		final Iterator<Long> iterator = pList.iterator();
		while (iterator.hasNext()) {
			final Long item = iterator.next();
			if (pSeen.containsKey(item)) {
				iterator.remove();
			}
		}
	}

	/**
	 * Initialize data structures.
	 * 
	 * @param pRtx
	 *          {@link IRriteTransaction} implementation reference on old revision
	 * @param pVisitor
	 *          {@link IVisitor} reference
	 * @throws SirixException
	 *           if anything in sirix fails
	 */
	private void init(final INodeReadTrx pRtx, final IVisitor pVisitor) {
		assert pVisitor != null;

		final long nodeKey = pRtx.getNodeKey();
		for (final IAxis axis = new PostOrderAxis(pRtx); axis.hasNext();) {
			axis.next();
			if (axis.getTrx().getNodeKey() == nodeKey) {
				break;
			}
			axis.getTrx().acceptVisitor(pVisitor);
		}
		pRtx.acceptVisitor(pVisitor);
	}

	/**
	 * Creates a flat list of all nodes by doing an in-order-traversal. NOTE:
	 * Since this is not a binary tree, we use post-order-traversal (wrong in
	 * paper). For each node type (element, attribute, text, comment, ...) there
	 * is a separate list.
	 * 
	 * @param pRtx
	 *          {@link INodeReadTrx} reference
	 * @param pVisitor
	 *          {@link LabelFMSEVisitor} used to save node type/list
	 */
	private void getLabels(final INodeReadTrx pRtx,
			final LabelFMSEVisitor pVisitor) {
		assert pRtx != null;
		assert pVisitor != null;

		final long nodeKey = pRtx.getNodeKey();
		for (final AbsAxis axis = new PostOrderAxis(pRtx); axis.hasNext();) {
			axis.next();
			if (axis.getTrx().getNodeKey() == nodeKey) {
				break;
			}
			axis.getTrx().acceptVisitor(pVisitor);
		}
		pRtx.acceptVisitor(pVisitor);
	}

	/**
	 * Compares the values of two nodes. Values are the text content, if the nodes
	 * do have child nodes or the name for inner nodes such as element or
	 * attribute (an attribute has one child: the value).
	 * 
	 * @param pX
	 *          first node
	 * @param pY
	 *          second node
	 * @param pRtx
	 *          {@link INodeReadTrx} implementation reference
	 * @param pWtx
	 *          {@link INodeWriteTrx} implementation reference
	 * @return true iff the values of the nodes are equal
	 */
	private boolean nodeValuesEqual(final long pX, final long pY,
			final INodeReadTrx pRtxOld, final INodeReadTrx pRtxNew) {
		assert pX >= 0;
		assert pY >= 0;
		assert pRtxOld != null;
		assert pRtxNew != null;

		final String a = getNodeValue(pX, pRtxOld);
		final String b = getNodeValue(pY, pRtxNew);

		return a == null ? b == null : a.equals(b);
	}

	/**
	 * Get node value of current node which is the string representation of
	 * {@link QName}s in the form {@code prefix:localName} or the value of
	 * {@link TextNode}s.
	 * 
	 * @param pNodeKey
	 *          node from which to get the value
	 * @param pRtx
	 *          {@link INodeReadTrx} implementation reference
	 * @return string value of current node
	 */
	private String getNodeValue(final long pNodeKey, final INodeReadTrx pRtx) {
		assert pNodeKey >= 0;
		assert pRtx != null;
		pRtx.moveTo(pNodeKey);
		final StringBuilder retVal = new StringBuilder();
		switch (pRtx.getKind()) {
		case ELEMENT:
		case NAMESPACE:
		case ATTRIBUTE:
			retVal.append(Utils.buildName(pRtx.getName()));
			break;
		case TEXT:
		case COMMENT:
			retVal.append(pRtx.getValue());
			break;
		case PROCESSING:
			retVal.append(pRtx.getName().getLocalPart()).append(" ")
					.append(pRtx.getValue());
			break;
		default:
			// Do nothing.
		}
		return retVal.toString();
	}

	/**
	 * This functional class is used to compare leaf nodes. The comparison is done
	 * by comparing the (characteristic) string for two nodes. If the strings are
	 * sufficient similar, the nodes are considered to be equal.
	 */
	private class LeafEqual implements IComparator<Long> {

		@Override
		public boolean isEqual(final Long pFirstNode, final Long pSecondNode) {
			assert pFirstNode != null;
			assert pSecondNode != null;

			// Old.
			mWtx.moveTo(pFirstNode);

			// New.
			mRtx.moveTo(pSecondNode);

			assert mWtx.getKind() == mRtx.getKind();
			double ratio = 0;

			if (mWtx.getKind() == EKind.ATTRIBUTE
					|| mWtx.getKind() == EKind.NAMESPACE
					|| mWtx.getKind() == EKind.PROCESSING) {
				if (mWtx.getName().equals(mRtx.getName())) {
					ratio = 1;
					if (mWtx.getKind() == EKind.ATTRIBUTE
							|| mWtx.getKind() == EKind.PROCESSING) {
						ratio = calculateRatio(mWtx.getValue(), mRtx.getValue());
					}

					// Also check QNames of the parents.
					if (ratio > FMESF) {
						mWtx.moveToParent();
						mRtx.moveToParent();
						// final QName oldQName = mWtx.getQNameOfCurrentNode();
						// final QName newQName = mRtx.getQNameOfCurrentNode();
						// if (oldQName.equals(newQName) &&
						// oldQName.getPrefix().equals(newQName.getPrefix())) {
						// ratio = checkAncestors(mWtx.getItem().getKey(),
						// mRtx.getItem().getKey()) ? 1 : 0;
						// } else {
						ratio = calculateRatio(getNodeValue(pFirstNode, mWtx),
								getNodeValue(pSecondNode, mRtx));
						// if (ratio > FMESF) {
						// ratio = checkAncestors(mWtx.getItem().getKey(),
						// mRtx.getItem().getKey()) ? 1 : 0;
						// }
						// }
					}
				}
			} else {
				if (nodeValuesEqual(pFirstNode, pSecondNode, mWtx, mRtx)) {
					ratio = 1;
				} else {
					ratio = calculateRatio(getNodeValue(pFirstNode, mWtx),
							getNodeValue(pSecondNode, mRtx));
				}

				if (ratio <= FMESF
						&& checkAncestors(mWtx.getNodeKey(), mRtx.getNodeKey())) {
					ratio = 1;
				}
			}

			return ratio > FMESF;
		}
	}

	/**
	 * This functional class is used to compare inner nodes. FMES uses different
	 * comparison criteria for leaf nodes and inner nodes. This class compares two
	 * nodes by calculating the number of common children (i.e. children contained
	 * in the matching) in relation to the total number of children.
	 */
	private class InnerNodeEqual implements IComparator<Long> {

		/** {@link Matching} reference. */
		private final Matching mMatching;

		/**
		 * Constructor.
		 * 
		 * @param pMatching
		 *          {@link Matching} reference
		 * @param pWtx
		 *          {@link INodeWriteTrx} implementation reference on old revision
		 * @param pRtx
		 *          {@link INodeReadTrx} implementation reference on new revision
		 */
		public InnerNodeEqual(final Matching pMatching) {
			assert pMatching != null;
			mMatching = pMatching;
		}

		@Override
		public boolean isEqual(final Long pFirstNode, final Long pSecondNode) {
			assert pFirstNode != null;
			assert pSecondNode != null;

			mWtx.moveTo(pFirstNode);
			mRtx.moveTo(pSecondNode);

			assert mWtx.getKind() == mRtx.getKind();

			boolean retVal = false;

			if ((mWtx.hasFirstChild() || mWtx.getAttributeCount() > 0 || mWtx
					.getNamespaceCount() > 0)
					&& (mRtx.hasFirstChild() || mRtx.getAttributeCount() > 0 || mRtx
							.getNamespaceCount() > 0)) {
				final long common = mMatching.containedDescendants(pFirstNode,
						pSecondNode);
				final long maxFamilySize = Math.max(mDescendantsOldRev.get(pFirstNode),
						mDescendantsNewRev.get(pSecondNode));
				if (common == 0 && maxFamilySize == 1) {
					retVal = mWtx.getName().equals(mRtx.getName());
				} else {
					retVal = ((double) common / (double) maxFamilySize) >= FMESTHRESHOLD;
				}
			} else {
				final QName oldName = mWtx.getName();
				final QName newName = mRtx.getName();
				if (oldName.getNamespaceURI().equals(newName.getNamespaceURI())
						&& calculateRatio(oldName.getLocalPart(), newName.getLocalPart()) > 0.7) {
					retVal = checkAncestors(mWtx.getNodeKey(), mRtx.getNodeKey());
				}
			}

			return retVal;
		}
	}

	@Override
	public String getName() {
		return NAME;
	}

	/**
	 * Calculate ratio between 0 and 1 between two String values for
	 * {@code text-nodes} denoted.
	 * 
	 * @param pFirstNode
	 *          node key of first node
	 * @param pSecondNode
	 *          node key of second node
	 * @return ratio between 0 and 1, whereas 1 is a complete match and 0 denotes
	 *         that the Strings are completely different
	 */
	private float calculateRatio(final String pOldValue, final String pNewValue) {
		assert pOldValue != null;
		assert pNewValue != null;
		float ratio;

		if (pOldValue.length() > MAX_LENGTH || pNewValue.length() > MAX_LENGTH) {
			ratio = Util.quickRatio(pOldValue, pNewValue);
		} else {
			ratio = mLevenshtein.getSimilarity(pOldValue, pNewValue);
		}

		return ratio;
	}

	/**
	 * Check if ancestors are equal.
	 * 
	 * @param pOldKey
	 *          start key in old revision
	 * @param pNewKey
	 *          start key in new revision
	 * @return {@code true} if all ancestors up to the start keys of the
	 *         FMSE-algorithm, {@code false} otherwise
	 */
	private boolean checkAncestors(final long pOldKey, final long pNewKey) {
		assert pOldKey >= 0;
		assert pNewKey >= 0;
		mWtx.moveTo(pOldKey);
		mRtx.moveTo(pNewKey);
		boolean retVal = true;
		if (mWtx.hasParent() && mRtx.hasParent()) {
			do {
				mWtx.moveToParent();
				mRtx.moveToParent();
			} while (mWtx.getNodeKey() != mOldStartKey
					&& mRtx.getNodeKey() != mNewStartKey
					&& mWtx.hasParent()
					&& mRtx.hasParent()
					&& calculateRatio(getNodeValue(mWtx.getNodeKey(), mWtx),
							getNodeValue(mRtx.getNodeKey(), mRtx)) >= 0.7f);
			if ((mWtx.hasParent() && mWtx.getNodeKey() != mOldStartKey)
					|| (mRtx.hasParent() && mRtx.getNodeKey() != mNewStartKey)) {
				retVal = false;
			} else {
				retVal = true;
			}
		} else {
			retVal = false;
		}
		return retVal;
	}

	@Override
	public void close() throws SirixException {
		mWtx.commit();
	}
}
