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

package org.sirix.service.xml.serialize;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.Callable;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import org.sirix.api.IAxis;
import org.sirix.api.INodeReadTrx;
import org.sirix.api.ISession;
import org.sirix.axis.DescendantAxis;
import org.sirix.axis.EIncludeSelf;
import org.sirix.exception.SirixException;
import org.sirix.node.EKind;

/**
 * Class implements main serialization algorithm. Other classes can extend it.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public abstract class AbsSerializer implements Callable<Void> {

	/** Sirix session {@link ISession}. */
	protected final ISession mSession;

	/** Stack for reading end element. */
	protected final Deque<Long> mStack;

	/** Array with versions to print. */
	protected final int[] mRevisions;

	/** Root node key of subtree to shredder. */
	protected final long mNodeKey;

	/**
	 * Constructor.
	 * 
	 * @param pSession
	 *          Sirix {@link ISession}
	 * @param pRevision
	 *          first revision to serialize
	 * @param pRevisions
	 *          revisions to serialize
	 */
	public AbsSerializer(@Nonnull final ISession pSession, final int pRevision,
			final int... pRevisions) {
		mStack = new ArrayDeque<>();
		mRevisions = pRevisions == null ? new int[1]
				: new int[pRevisions.length + 1];
		initialize(pRevision, pRevisions);
		mSession = checkNotNull(pSession);
		mNodeKey = 0;
	}

	/**
	 * Constructor.
	 * 
	 * @param pSession
	 *          Sirix {@link ISession}
	 * @param pKey
	 *          key of root node from which to shredder the subtree
	 * @param pRevision
	 *          first revision to serialize
	 * @param pRevisions
	 *          revisions to serialize
	 */
	public AbsSerializer(final @Nonnull ISession pSession,
			final @Nonnegative long pKey, final @Nonnegative int pRevision,
			final int... pRevisions) {
		mStack = new ArrayDeque<>();
		mRevisions = pRevisions == null ? new int[1]
				: new int[pRevisions.length + 1];
		initialize(pRevision, pRevisions);
		mSession = checkNotNull(pSession);
		mNodeKey = pKey;
	}

	/**
	 * Initialize.
	 * 
	 * @param pRevision
	 *          first revision to serialize
	 * @param pRevisions
	 *          revisions to serialize
	 */
	private void initialize(@Nonnegative final int pRevision,
			final int... pRevisions) {
		mRevisions[0] = pRevision;
		if (pRevisions != null) {
			for (int i = 0; i < pRevisions.length; i++) {
				mRevisions[i + 1] = pRevisions[i];
			}
		}
	}

	/**
	 * Serialize the storage.
	 * 
	 * @return null.
	 * @throws SirixException
	 *           if can't call serailzer
	 */
	@Override
	public Void call() throws SirixException {
		emitStartDocument();

		final int length = (mRevisions.length == 1 && mRevisions[0] < 0) ? (int) mSession
				.getLastRevisionNumber() : mRevisions.length;
		for (int i = 0; i < length; i++) {
			try (final INodeReadTrx rtx = mSession
					.beginNodeReadTrx((mRevisions.length == 1 && mRevisions[0] < 0) ? i
							: mRevisions[i])) {
				if (length > 1) {
					emitStartManualElement(i);
				}

				rtx.moveTo(mNodeKey);

				final IAxis descAxis = new DescendantAxis(rtx, EIncludeSelf.YES);

				// Setup primitives.
				boolean closeElements = false;
				long key = rtx.getNode().getNodeKey();

				// Iterate over all nodes of the subtree including self.
				while (descAxis.hasNext()) {
					key = descAxis.next();

					// Emit all pending end elements.
					if (closeElements) {
						while (!mStack.isEmpty()
								&& mStack.peek() != rtx.getStructuralNode().getLeftSiblingKey()) {
							rtx.moveTo(mStack.pop());
							emitEndElement(rtx);
							rtx.moveTo(key);
						}
						if (!mStack.isEmpty()) {
							rtx.moveTo(mStack.pop());
							emitEndElement(rtx);
						}
						rtx.moveTo(key);
						closeElements = false;
					}

					// Emit node.
					emitStartElement(rtx);

					// Push end element to stack if we are a start element with
					// children.
					if (rtx.getNode().getKind() == EKind.ELEMENT
							&& rtx.getStructuralNode().hasFirstChild()) {
						mStack.push(rtx.getNode().getNodeKey());
					}

					// Remember to emit all pending end elements from stack if
					// required.
					if (!rtx.getStructuralNode().hasFirstChild()
							&& !rtx.getStructuralNode().hasRightSibling()) {
						closeElements = true;
					}

				}

				// Finally emit all pending end elements.
				while (!mStack.isEmpty()) {
					rtx.moveTo(mStack.pop());
					emitEndElement(rtx);
				}

				if (length > 1) {
					emitEndManualElement(i);
				}
			}
		}
		emitEndDocument();

		return null;
	}

	/** Emit start document. */
	protected abstract void emitStartDocument();

	/**
	 * Emit start tag.
	 * 
	 * @param pRtx
	 *          Sirix {@link INodeReadTrx}
	 */
	protected abstract void emitStartElement(@Nonnull final INodeReadTrx pRtx);

	/**
	 * Emit end tag.
	 * 
	 * @param pRtx
	 *          Sirix {@link INodeReadTrx}
	 */
	protected abstract void emitEndElement(@Nonnull final INodeReadTrx pRtx);

	/**
	 * Emit a start tag, which specifies a revision.
	 * 
	 * @param pRevision
	 *          the revision to serialize
	 */
	protected abstract void emitStartManualElement(
			@Nonnegative final long pRevision);

	/**
	 * Emit an end tag, which specifies a revision.
	 * 
	 * @param pRevision
	 *          the revision to serialize
	 */
	protected abstract void emitEndManualElement(@Nonnegative final long pRevision);

	/** Emit end document. */
	protected abstract void emitEndDocument();
}
