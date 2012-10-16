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

package org.sirix.diff;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.Nonnull;
import javax.xml.namespace.QName;

import org.sirix.access.HashKind;
import org.sirix.api.Axis;
import org.sirix.api.NodeReadTrx;
import org.sirix.axis.DescendantAxis;
import org.sirix.axis.IncludeSelf;
import org.sirix.diff.DiffFactory.Builder;
import org.sirix.diff.DiffFactory.EDiff;
import org.sirix.diff.DiffFactory.EDiffOptimized;
import org.sirix.exception.SirixException;
import org.sirix.node.Kind;
import org.sirix.node.ElementNode;
import org.sirix.node.interfaces.StructNode;

/**
 * Abstract diff class which implements common functionality.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
@Nonnull
abstract class AbsDiff extends AbsDiffObservable {

	/** Determines transaction movement. */
	private enum EMove {
		/** To the following node (next node in the following-axis). */
		FOLLOWING,

		/** Next node in document order. */
		DOCUMENT_ORDER
	}

	/** Determines the current revision. */
	private enum ERevision {
		/** Old revision. */
		OLD,

		/** New revision. */
		NEW;
	}

	/**
	 * Kind of hash method.
	 * 
	 * @see HashKind
	 */
	private HashKind mHashKind;

	/**
	 * Kind of difference.
	 * 
	 * @see EDiff
	 */
	private EDiff mDiff;

	/** Diff kind. */
	private EDiffOptimized mDiffKind;

	/** {@link DepthCounter} instance. */
	private final DepthCounter mDepth;

	/** Key of "root" node in new revision. */
	private long mRootKey;

	/** Root key of old revision. */
	private final long mOldRootKey;

	/**
	 * Determines if {@link NodeReadTrx} on newer revision moved to the node
	 * denoted by {@code mNewStartKey}.
	 */
	private boolean mNewRtxMoved;

	/**
	 * Determines if {@link NodeReadTrx} on older revision moved to the node
	 * denoted by {@code mOldStartKey}.
	 */
	private boolean mOldRtxMoved;

	/** Determines if the GUI uses the algorithm or not. */
	private boolean mIsGUI;

	/** Determines if it's the first diff-comparison. */
	private boolean mIsFirst;

	/** {@link NodeReadTrx} on new revision. */
	private final NodeReadTrx mNewRtx;

	/** {@link NodeReadTrx} on old revision. */
	private final NodeReadTrx mOldRtx;

	/**
	 * Constructor.
	 * 
	 * @param pBuilder
	 *          {@link Builder} reference
	 * @throws SirixException
	 *           if setting up transactions failes
	 */
	AbsDiff(final Builder pBuilder) throws SirixException {
		mDiffKind = checkNotNull(pBuilder).mKind;
		synchronized (pBuilder.mSession) {
			mNewRtx = pBuilder.mSession.beginNodeReadTrx(pBuilder.mNewRev);
			mOldRtx = pBuilder.mSession.beginNodeReadTrx(pBuilder.mOldRev);
			mHashKind = pBuilder.mHashKind;
		}
		mNewRtxMoved = mNewRtx.moveTo(pBuilder.mNewStartKey).hasMoved();
		mOldRtxMoved = mOldRtx.moveTo(pBuilder.mOldStartKey).hasMoved();
		if (mNewRtx.getKind() == Kind.DOCUMENT_ROOT) {
			mNewRtx.moveToFirstChild();
		}
		if (mOldRtx.getKind() == Kind.DOCUMENT_ROOT) {
			mOldRtx.moveToFirstChild();
		}
		mRootKey = pBuilder.mNewStartKey;
		mOldRootKey = pBuilder.mOldStartKey;

		synchronized (pBuilder.mObservers) {
			for (final DiffObserver observer : pBuilder.mObservers) {
				addObserver(observer);
			}
		}
		mDiff = EDiff.SAME;
		mDiffKind = pBuilder.mKind;
		mDepth = new DepthCounter(pBuilder.mNewDepth, pBuilder.mOldDepth);
		mIsGUI = pBuilder.mIsGUI;
		mIsFirst = true;
	}

	/**
	 * Do the diff.
	 * 
	 * @throws SirixException
	 *           if setting up transactions failes
	 */
	void diffMovement() throws SirixException {
		assert mHashKind != null;
		assert mNewRtx != null;
		assert mOldRtx != null;
		assert mDiff != null;
		assert mDiffKind != null;

		if (!mNewRtxMoved) {
			fireDeletes();
			if (!mIsGUI || mDepth.getNewDepth() == 0) {
				fireInserts();
			}
			done();
			return;
		}
		if (!mOldRtxMoved) {
			fireInserts();
			if (!mIsGUI || mDepth.getOldDepth() == 0) {
				fireDeletes();
			}
			done();
			return;
		}

		// Check first node.
		if (mHashKind == HashKind.None || mDiffKind == EDiffOptimized.NO) {
			mDiff = diff(mNewRtx, mOldRtx, mDepth);
		} else {
			mDiff = optimizedDiff(mNewRtx, mOldRtx, mDepth);
		}
		mIsFirst = false;

		// Iterate over new revision (order of operators significant -- regarding
		// the OR).
		if (mDiff != EDiff.SAMEHASH) {
			while ((mOldRtx.getKind() != Kind.DOCUMENT_ROOT && mDiff == EDiff.DELETED)
					|| moveCursor(mNewRtx, ERevision.NEW, EMove.FOLLOWING)) {
				if (mDiff != EDiff.INSERTED) {
					moveCursor(mOldRtx, ERevision.OLD, EMove.FOLLOWING);
				}

				if (mNewRtx.getKind() != Kind.DOCUMENT_ROOT
						|| mOldRtx.getKind() != Kind.DOCUMENT_ROOT) {
					if (mHashKind == HashKind.None || mDiffKind == EDiffOptimized.NO) {
						mDiff = diff(mNewRtx, mOldRtx, mDepth);
					} else {
						mDiff = optimizedDiff(mNewRtx, mOldRtx, mDepth);
					}
				}
			}

			// Nodes deleted in old rev at the end of the tree.
			if (mOldRtx.getKind() != Kind.DOCUMENT_ROOT) {
				mRootKey = mOldRootKey;
				// First time it might be EDiff.INSERTED where the cursor doesn't move.
				if (mDiff == EDiff.INSERTED) {
					mDiff = EDiff.DELETED;
					fireDiff(mDiff, mNewRtx.getNodeKey(), mOldRtx.getNodeKey(),
							new DiffDepth(mDepth.getNewDepth(), mDepth.getOldDepth()));
				}
				boolean moved = true;
				if (mDiffKind == EDiffOptimized.HASHED && mDiff == EDiff.SAMEHASH) {
					moved = moveToFollowingNode(mOldRtx, ERevision.OLD);
					if (moved) {
						mDiff = EDiff.DELETED;
						fireDiff(mDiff, mNewRtx.getNodeKey(), mOldRtx.getNodeKey(),
								new DiffDepth(mDepth.getNewDepth(), mDepth.getOldDepth()));
					}
				}
				if (moved) {
					while (moveCursor(mOldRtx, ERevision.OLD, EMove.DOCUMENT_ORDER)) {
						mDiff = EDiff.DELETED;
						fireDiff(mDiff, mNewRtx.getNodeKey(), mOldRtx.getNodeKey(),
								new DiffDepth(mDepth.getNewDepth(), mDepth.getOldDepth()));
					}
				}
			}
		}

		diffDone();
	}

	/**
	 * Done processing diffs. Fire remaining diffs and signal that the algorithm
	 * is done.
	 * 
	 * @throws SirixException
	 *           if sirix fails to close the transactions
	 */
	private void diffDone() throws SirixException {
		mNewRtx.close();
		mOldRtx.close();
		done();
	}

	/**
	 * Fire {@code EDiff.DELETEs} for the whole subtree.
	 */
	private void fireDeletes() {
		fireDiff(EDiff.DELETED, mNewRtx.getNodeKey(), mOldRtx.getNodeKey(),
				new DiffDepth(mDepth.getNewDepth(), mDepth.getOldDepth()));
		mIsFirst = false;
		while (moveCursor(mOldRtx, ERevision.OLD, EMove.DOCUMENT_ORDER)) {
			fireDiff(EDiff.DELETED, mNewRtx.getNodeKey(), mOldRtx.getNodeKey(),
					new DiffDepth(mDepth.getNewDepth(), mDepth.getOldDepth()));
		}
	}

	/**
	 * Fire {@code EDiff.INSERTs} for the whole subtree.
	 */
	private void fireInserts() {
		fireDiff(EDiff.INSERTED, mNewRtx.getNodeKey(), mOldRtx.getNodeKey(),
				new DiffDepth(mDepth.getNewDepth(), mDepth.getOldDepth()));
		mIsFirst = false;
		while (moveCursor(mNewRtx, ERevision.NEW, EMove.DOCUMENT_ORDER)) {
			fireDiff(EDiff.INSERTED, mNewRtx.getNodeKey(), mOldRtx.getNodeKey(),
					new DiffDepth(mDepth.getNewDepth(), mDepth.getOldDepth()));
		}
	}

	/**
	 * Move cursor one node forward in pre order.
	 * 
	 * @param pRtx
	 *          the {@link NodeReadTrx} to use
	 * @param pRevision
	 *          the {@link ERevision} constant
	 * @return {@code true}, if cursor moved, {@code false} otherwise, if no nodes
	 *         follow in document order
	 */
	private boolean moveCursor(@Nonnull final NodeReadTrx pRtx,
			@Nonnull final ERevision pRevision, @Nonnull final EMove pMove) {
		assert pRtx != null;
		assert pRevision != null;

		boolean moved = false;

		if (pRtx.getKind() != Kind.DOCUMENT_ROOT) {
			switch (mDiff) {
			case SAME:
			case SAMEHASH:
			case UPDATED:
				moved = moveToNext(pRtx, pRevision);
				break;
			case REPLACED:
				moved = moveToFollowingNode(pRtx, pRevision);
				break;
			case INSERTED:
			case DELETED:
				if (pMove == EMove.FOLLOWING
						&& (mDiff == EDiff.INSERTED || mDiff == EDiff.DELETED)) {
					if (pRtx.getKind() == Kind.DOCUMENT_ROOT) {
						moved = false;
					} else {
						moved = true;
					}
				} else {
					moved = moveToNext(pRtx, pRevision);
				}
				break;
			default:
			}
		}

		return moved;
	}

	private boolean moveToNext(@Nonnull final NodeReadTrx pRtx,
			@Nonnull final ERevision pRevision) {
		boolean moved = false;
		if (pRtx.hasFirstChild()) {
			if (pRtx.getKind() != Kind.DOCUMENT_ROOT
					&& mDiffKind == EDiffOptimized.HASHED && mDiff == EDiff.SAMEHASH) {
				moved = pRtx.moveToRightSibling().hasMoved();

				if (!moved) {
					moved = moveToFollowingNode(pRtx, pRevision);
				}
			} else {
				moved = pRtx.moveToFirstChild().hasMoved();

				if (moved) {
					switch (pRevision) {
					case NEW:
						mDepth.incrementNewDepth();
						break;
					case OLD:
						mDepth.incrementOldDepth();
						break;
					}
				}
			}
		} else if (pRtx.hasRightSibling()) {
			if (pRtx.getNodeKey() == mRootKey) {
				pRtx.moveToDocumentRoot();
			} else {
				moved = pRtx.moveToRightSibling().hasMoved();
			}
		} else {
			moved = moveToFollowingNode(pRtx, pRevision);
		}
		return moved;
	}

	/**
	 * Move to next following node.
	 * 
	 * @param pRtx
	 *          the {@link NodeReadTrx} to use
	 * @param pRevision
	 *          the {@link ERevision} constant
	 * @return true, if cursor moved, false otherwise
	 */
	private boolean moveToFollowingNode(final NodeReadTrx pRtx,
			final ERevision pRevision) {
		boolean moved = false;
		while (!pRtx.hasRightSibling() && pRtx.hasParent()
				&& pRtx.getNodeKey() != mRootKey) {
			moved = pRtx.moveToParent().hasMoved();
			if (moved) {
				switch (pRevision) {
				case NEW:
					mDepth.decrementNewDepth();
					break;
				case OLD:
					mDepth.decrementOldDepth();
					break;
				}
			}
		}

		if (pRtx.getNodeKey() == mRootKey) {
			pRtx.moveToDocumentRoot();
		}

		moved = pRtx.moveToRightSibling().hasMoved();
		return moved;
	}

	/**
	 * Diff of nodes.
	 * 
	 * @param pNewRtx
	 *          {@link NodeReadTrx} on new revision
	 * @param pOldRtx
	 *          {@link NodeReadTrx} on old revision
	 * @param pDepth
	 *          {@link DepthCounter} container for current depths of both
	 *          transaction cursors
	 * @param paramFireDiff
	 *          determines if a diff should be fired
	 * @return kind of difference
	 */
	EDiff diff(@Nonnull final NodeReadTrx pNewRtx,
			@Nonnull final NodeReadTrx pOldRtx, @Nonnull final DepthCounter pDepth) {
		assert pNewRtx != null;
		assert pOldRtx != null;
		assert pDepth != null;

		EDiff diff = EDiff.SAME;

		// Check for modifications.
		switch (pNewRtx.getKind()) {
		case DOCUMENT_ROOT:
		case TEXT:
		case ELEMENT:
			if (checkNodes(pNewRtx, pOldRtx)) {
				fireDiff(diff, pNewRtx.getNodeKey(), pOldRtx.getNodeKey(),
						new DiffDepth(pDepth.getNewDepth(), pDepth.getOldDepth()));
			} else {
				diff = diffAlgorithm(pNewRtx, pOldRtx, pDepth);
			}
			break;
		default:
			// Do nothing.
		}

		return diff;
	}

	/**
	 * Optimized diff, which skips unnecessary comparsions.
	 * 
	 * @param pNewRtx
	 *          {@link NodeReadTrx} on new revision
	 * @param pOldRtx
	 *          {@link NodeReadTrx} on old revision
	 * @param pDepth
	 *          {@link DepthCounter} container for current depths of both
	 *          transaction cursors
	 * @param paramFireDiff
	 *          determines if a diff should be fired
	 * @return kind of difference
	 */
	EDiff optimizedDiff(@Nonnull final NodeReadTrx pNewRtx,
			@Nonnull final NodeReadTrx pOldRtx, @Nonnull final DepthCounter pDepth) {
		assert pNewRtx != null;
		assert pOldRtx != null;
		assert pDepth != null;

		EDiff diff = EDiff.SAMEHASH;

		// Check for modifications.
		switch (pNewRtx.getKind()) {
		case DOCUMENT_ROOT:
		case TEXT:
		case ELEMENT:
			if (pNewRtx.getNodeKey() != pOldRtx.getNodeKey()
					|| pNewRtx.getHash() != pOldRtx.getHash()) {
				// Check if nodes are the same (even if subtrees may vary).
				if (checkNodes(pNewRtx, pOldRtx)) {
					diff = EDiff.SAME;
					fireDiff(diff, pNewRtx.getNodeKey(), pOldRtx.getNodeKey(),
							new DiffDepth(pDepth.getNewDepth(), pDepth.getOldDepth()));
				} else {
					diff = diffAlgorithm(pNewRtx, pOldRtx, pDepth);
				}
			} else {
				fireDiff(diff, pNewRtx.getNodeKey(), pOldRtx.getNodeKey(),
						new DiffDepth(pDepth.getNewDepth(), pDepth.getOldDepth()));
			}
			break;
		default:
			// Do nothing.
		}

		return diff;
	}

	/**
	 * Main algorithm to compute diffs between two nodes.
	 * 
	 * @param pNewRtx
	 *          {@link NodeReadTrx} on new revision
	 * @param pOldRtx
	 *          {@link NodeReadTrx} on old revision
	 * @param pDepth
	 *          {@link DepthCounter} container for current depths of both
	 *          transaction cursors
	 * @return kind of diff
	 */
	private EDiff diffAlgorithm(@Nonnull final NodeReadTrx pNewRtx,
			@Nonnull final NodeReadTrx pOldRtx, @Nonnull final DepthCounter pDepth) {
		assert pNewRtx != null;
		assert pOldRtx != null;
		assert pDepth != null;
		EDiff diff = null;

		if (pDepth.getOldDepth() > pDepth.getNewDepth()) { // Check if node has been
																												// deleted.
			diff = EDiff.DELETED;
			emitDiffs(diff);
		} else if (checkUpdate(pNewRtx, pOldRtx)) { // Check if node has been
																								// updated.
			diff = EDiff.UPDATED;
			fireDiff(diff, pNewRtx.getNodeKey(), pOldRtx.getNodeKey(), new DiffDepth(
					pDepth.getNewDepth(), pDepth.getOldDepth()));
		} else if (checkReplace(pNewRtx, pOldRtx)) { // Check if node has been
																									// replaced.
			diff = EDiff.REPLACED;
		} else {
			final long oldKey = pOldRtx.getNodeKey();
			final boolean movedOld = pOldRtx.moveTo(pNewRtx.getNodeKey()).hasMoved();
			pOldRtx.moveTo(oldKey);

			final long newKey = pNewRtx.getNodeKey();
			final boolean movedNew = pNewRtx.moveTo(pOldRtx.getNodeKey()).hasMoved();
			pNewRtx.moveTo(newKey);

			if (!movedOld) {
				diff = EDiff.INSERTED;
			} else if (!movedNew) {
				diff = EDiff.DELETED;
			} else {
				// Determine if one of the right sibling matches.
				EFoundEqualNode found = EFoundEqualNode.FALSE;

				while (pOldRtx.hasRightSibling()
						&& pOldRtx.moveToRightSibling().hasMoved()
						&& found == EFoundEqualNode.FALSE) {
					if (checkNodes(pNewRtx, pOldRtx)) {
						found = EFoundEqualNode.TRUE;
						break;
					}
				}

				pOldRtx.moveTo(oldKey);
				diff = found.kindOfDiff();
			}

			mDiff = diff;
			emitDiffs(diff);
		}

		assert diff != null;
		return diff;
	}

	/**
	 * Emit diffs for {@code INSERTED} or {@code DELETED} nodes and traverse
	 * accordingly.
	 * 
	 * @param pDiff
	 *          kind of diff
	 */
	private void emitDiffs(@Nonnull final EDiff pDiff) {
		final ERevision revision = pDiff == EDiff.DELETED ? ERevision.OLD
				: ERevision.NEW;
		final int depth = pDiff == EDiff.DELETED ? mDepth.getOldDepth() : mDepth
				.getNewDepth();
		final NodeReadTrx rtx = pDiff == EDiff.DELETED ? mOldRtx : mNewRtx;
		do {
			fireDiff(pDiff, mNewRtx.getNodeKey(), mOldRtx.getNodeKey(),
					new DiffDepth(mDepth.getNewDepth(), mDepth.getOldDepth()));
		} while (moveCursor(rtx, revision, EMove.DOCUMENT_ORDER)
				&& ((pDiff == EDiff.INSERTED && mDepth.getNewDepth() > depth) || (pDiff == EDiff.DELETED && mDepth
						.getOldDepth() > depth)));
	}

	/**
	 * Check {@link QName} of nodes.
	 * 
	 * @param pNewRtx
	 *          {@link NodeReadTrx} on new revision
	 * @param pOldRtx
	 *          {@link NodeReadTrx} on old revision
	 * @return {@code true} if nodes are "equal" according to their {@link QName}
	 *         s, {@code false

	 */
	boolean checkName(@Nonnull final NodeReadTrx pNewRtx,
			@Nonnull final NodeReadTrx pOldRtx) {
		boolean found = false;
		if (pNewRtx.getKind() == pOldRtx.getKind()) {
			switch (pNewRtx.getKind()) {
			case ELEMENT:
				if (pNewRtx.getNameKey() == pOldRtx.getNameKey()) {
					found = true;
				}
				break;
			case TEXT:
				if (pNewRtx.getValue().equals(pOldRtx.getValue())) {
					found = true;
				}
				break;
			default:
			}
		}
		return found;
	}

	/**
	 * Check if nodes are equal excluding subtrees.
	 * 
	 * @param pNewRtx
	 *          {@link NodeReadTrx} on new revision
	 * @param pOldRtx
	 *          {@link NodeReadTrx} on old revision
	 * @return true if nodes are "equal", otherwise false
	 */
	abstract boolean checkNodes(@Nonnull final NodeReadTrx pNewRtx,
			@Nonnull final NodeReadTrx pOldRtx);

	/**
	 * Check for a replace of a node.
	 * 
	 * @param pNewRtx
	 *          first {@link NodeReadTrx} instance
	 * @param pOldRtx
	 *          second {@link NodeReadTrx} instance
	 * @return true if node has been replaced, false otherwise
	 */
	boolean checkReplace(@Nonnull final NodeReadTrx pNewRtx,
			@Nonnull final NodeReadTrx pOldRtx) {
		boolean replaced = false;
		if (pNewRtx.getNodeKey() != pOldRtx.getNodeKey()) {
			final long newKey = pNewRtx.getNodeKey();
			boolean movedNewRtx = pNewRtx.moveToRightSibling().hasMoved();
			final long oldKey = pOldRtx.getNodeKey();
			boolean movedOldRtx = pOldRtx.moveToRightSibling().hasMoved();
			if (movedNewRtx && movedOldRtx) {
				if (pNewRtx.getNodeKey() == pOldRtx.getNodeKey()) {
					replaced = true;
				} else {
					while (pNewRtx.hasRightSibling() && pOldRtx.hasRightSibling()) {
						pNewRtx.moveToRightSibling();
						pOldRtx.moveToRightSibling();
						if (pNewRtx.getNodeKey() == pOldRtx.getNodeKey()) {
							replaced = true;
							break;
						}
					}
				}
			} else if (!movedNewRtx && !movedOldRtx
					&& (mDiff == EDiff.SAME || mDiff == EDiff.SAMEHASH)) {
				movedNewRtx = pNewRtx.moveToParent().hasMoved();
				movedOldRtx = pOldRtx.moveToParent().hasMoved();

				if (movedNewRtx && movedOldRtx
						&& pNewRtx.getNodeKey() == pOldRtx.getNodeKey()) {
					replaced = true;
				}
			}
			pNewRtx.moveTo(newKey);
			pOldRtx.moveTo(oldKey);

			if (replaced) {
				final long newNodeKey = pNewRtx.getNodeKey();
				final long oldNodeKey = pOldRtx.getNodeKey();
				final Axis oldAxis = new DescendantAxis(pOldRtx, IncludeSelf.YES);
				final Axis newAxis = new DescendantAxis(pNewRtx, IncludeSelf.YES);
				while (oldAxis.hasNext()) {
					oldAxis.next();
					fireDiff(EDiff.REPLACEDOLD, pNewRtx.getNodeKey(), pOldRtx.getNodeKey(),
							new DiffDepth(mDepth.getNewDepth(), mDepth.getOldDepth()));
					adjustDepth(pOldRtx, oldNodeKey, ERevision.OLD);
				}

				while (newAxis.hasNext()) {
					newAxis.next();
					fireDiff(EDiff.REPLACEDNEW, pNewRtx.getNodeKey(), pOldRtx.getNodeKey(),
							new DiffDepth(mDepth.getNewDepth(), mDepth.getOldDepth()));
					adjustDepth(pNewRtx, newNodeKey, ERevision.NEW);
				}
				pNewRtx.moveTo(newNodeKey);
				pOldRtx.moveTo(oldNodeKey);
				mDiff = EDiff.REPLACED;
				// final IAxis oldAxis = new DescendantAxis(pOldRtx, EIncludeSelf.YES);
				// final IAxis newAxis = new DescendantAxis(pNewRtx, EIncludeSelf.YES);
				// if (pNewRtx.getDescendantCount() >= pOldRtx
				// .getDescendantCount()) {
				// while (newAxis.hasNext()) {
				// newAxis.next();
				// boolean moved = false;
				// if (oldAxis.hasNext()) {
				// oldAxis.next();
				// moved = true;
				// }
				// fireDiff(EDiff.REPLACEDNEW, pNewRtx, pOldRtx,
				// new DiffDepth(mDepth.getNewDepth(), mDepth.getOldDepth()));
				// adjustDepth(newAxis.getTransaction(), newNodeKey, ERevision.NEW);
				// if (moved) {
				// adjustDepth(oldAxis.getTransaction(), oldNodeKey, ERevision.OLD);
				// }
				// }
				//
				// } else {
				// while (oldAxis.hasNext()) {
				// oldAxis.next();
				// boolean moved = false;
				// if (newAxis.hasNext()) {
				// newAxis.next();
				// moved = true;
				// }
				// fireDiff(EDiff.REPLACEDOLD, pNewRtx, pOldRtx,
				// new DiffDepth(mDepth.getNewDepth(), mDepth.getOldDepth()));
				// adjustDepth(oldAxis.getTransaction(), oldNodeKey, ERevision.OLD);
				// if (moved) {
				// adjustDepth(newAxis.getTransaction(), newNodeKey, ERevision.NEW);
				// }
				// }
				// }
				pNewRtx.moveTo(newNodeKey);
				pOldRtx.moveTo(oldNodeKey);
			}
		}
		return replaced;
	}

	/**
	 * Adjust the depth.
	 * 
	 * @param pRtx
	 *          the transaction to simulate moves
	 * @param pStartNodeKey
	 *          the start node key
	 */
	private void adjustDepth(final NodeReadTrx pRtx, final long pStartNodeKey,
			final ERevision pRevision) {
		assert pRtx != null;
		assert pStartNodeKey >= 0;
		assert pRevision != null;
		final long nodeKey = pRtx.getNodeKey();
		if (pRtx.hasFirstChild()) {
			switch (pRevision) {
			case NEW:
				mDepth.incrementNewDepth();
				break;
			case OLD:
				mDepth.incrementOldDepth();
				break;
			}
		} else {
			while (!pRtx.hasRightSibling() && pRtx.hasParent()
					&& pRtx.getNodeKey() != pStartNodeKey) {
				pRtx.moveToParent();
				switch (pRevision) {
				case NEW:
					mDepth.decrementNewDepth();
					break;
				case OLD:
					mDepth.decrementOldDepth();
					break;
				}
			}
		}
		pRtx.moveTo(nodeKey);
	}

	/**
	 * Check for an update of a node.
	 * 
	 * @param pNewRtx
	 *          first {@link NodeReadTrx} instance
	 * @param pOldRtx
	 *          second {@link NodeReadTrx} instance
	 * @return kind of diff
	 */
	boolean checkUpdate(final NodeReadTrx pNewRtx, final NodeReadTrx pOldRtx) {
		if (mIsFirst) {
			return pNewRtx.getNodeKey() == pOldRtx.getNodeKey();
		}
		return pNewRtx.getNodeKey() == pOldRtx.getNodeKey()
				&& pNewRtx.getParentKey() == pOldRtx.getParentKey()
				&& mDepth.getNewDepth() == mDepth.getOldDepth();
	}
}
