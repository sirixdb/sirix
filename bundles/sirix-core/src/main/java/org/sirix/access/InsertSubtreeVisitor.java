///**
// * Copyright (c) 2011, University of Konstanz, Distributed Systems Group
// * All rights reserved.
// * 
// * Redistribution and use in source and binary forms, with or without
// * modification, are permitted provided that the following conditions are met:
// * * Redistributions of source code must retain the above copyright
// * notice, this list of conditions and the following disclaimer.
// * * Redistributions in binary form must reproduce the above copyright
// * notice, this list of conditions and the following disclaimer in the
// * documentation and/or other materials provided with the distribution.
// * * Neither the name of the University of Konstanz nor the
// * names of its contributors may be used to endorse or promote products
// * derived from this software without specific prior written permission.
// * 
// * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
// * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
// * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// * DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
// * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
// * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
// * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
// * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
// * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
// */
//package org.sirix.access;
//
//import static com.google.common.base.Preconditions.checkNotNull;
//
//import javax.annotation.Nonnull;
//
//import org.sirix.api.NodeReadTrx;
//import org.sirix.api.NodeWriteTrx;
//
///**
// * Allows insertion of subtrees.
// * 
// * @author Johannes Lichtenberger, University of Konstanz
// * 
// *         Currently not used because of tail recursion which isn't optimized in
// *         Java.
// */
//class InsertSubtreeVisitor extends AbstractVisitor {
//
//	/** Read-transaction which implements the {@link NodeReadTrx} interface. */
//	private final NodeReadTrx mRtx;
//
//	/** Write-transaction which implements the {@link NodeWriteTrx} interface. */
//	private final NodeWriteTrx mWtx;
//
//	/** Determines how to insert a node. */
//	private InsertPos mInsert;
//
//	/** First visitor step. */
//	private boolean mFirst;
//
//	/** Depth starting at 0. */
//	private int mDepth;
//
//	/**
//	 * Constructor.
//	 * 
//	 * @param pRtx
//	 *          read-transaction which implements the {@link NodeReadTrx}
//	 *          interface
//	 * @param pWtx
//	 *          write-transaction which implements the {@link NodeWriteTrx}
//	 *          interface
//	 * @param pInsert
//	 *          determines how to insert a node
//	 */
//	InsertSubtreeVisitor(final NodeReadTrx pRtx,
//			final NodeWriteTrx pWtx, final InsertPos pInsert) {
//		mRtx = checkNotNull(pRtx);
//		mWtx = checkNotNull(pWtx);
//		mInsert = checkNotNull(pInsert);
//		mFirst = true;
//	}

	// @Override
	// public EVisitResult visit(final ElementNode pNode) {
	// mRtx.moveTo(pNode.getNodeKey());
	// try {
	// mInsert.insertNode(mWtx, mRtx);
	// mInsert = EInsertPos.ASNONSTRUCTURAL;
	//
	// for (int i = 0, nspCount = pNode.getNamespaceCount(); i < nspCount; i++) {
	// mRtx.moveToNamespace(i);
	// mInsert.insertNode(mWtx, mRtx);
	// mRtx.moveToParent();
	// }
	//
	// for (int i = 0, attrCount = pNode.getAttributeCount(); i < attrCount; i++)
	// {
	// mRtx.moveToAttribute(i);
	// mInsert.insertNode(mWtx, mRtx);
	// mRtx.moveToParent();
	// }
	//
	// if (pNode.hasFirstChild()) {
	// mFirst = false;
	// mInsert = EInsertPos.ASFIRSTCHILD;
	// mRtx.moveToFirstChild();
	// mDepth++;
	// } else if (!mFirst && pNode.hasRightSibling()) {
	// mInsert = EInsertPos.ASRIGHTSIBLING;
	// mRtx.moveToRightSibling();
	// } else if (!mFirst) {
	// if (!moveToNextNode()) {
	// return EVisitResult.TERMINATE;
	// }
	// }
	// } catch (final SirixException e) {
	// throw new IllegalStateException(e);
	// }
	// if (mFirst) {
	// return EVisitResult.TERMINATE;
	// }
	// return mRtx.acceptVisitor(this);
	// }
	//
	// @Override
	// public EVisitResult visit(final TextNode pNode) {
	// mRtx.moveTo(pNode.getNodeKey());
	// try {
	// mInsert.insertNode(mWtx, mRtx);
	//
	// if (!mFirst && mRtx.hasRightSibling()) {
	// mRtx.moveToRightSibling();
	// mInsert = EInsertPos.ASRIGHTSIBLING;
	// } else if (!mFirst) {
	// if (!moveToNextNode()) {
	// return EVisitResult.TERMINATE;
	// }
	// }
	// } catch (final SirixException e) {
	// throw new IllegalStateException(e);
	// }
	// if (mFirst) {
	// return EVisitResult.TERMINATE;
	// }
	// return mRtx.acceptVisitor(this);
	// }
	//
	// /** Insert next node in document order/preorder. */
	// private boolean moveToNextNode() {
	// boolean retVal = false;
	// while (!mRtx.hasRightSibling() && mDepth > 0) {
	// mRtx.moveToParent();
	// mWtx.moveToParent();
	// mDepth--;
	// }
	//
	// if (mDepth > 0) {
	// mInsert = EInsertPos.ASRIGHTSIBLING;
	// if (mRtx.hasRightSibling()) {
	// mRtx.moveToRightSibling();
	// retVal = true;
	// }
	// }
	// return retVal;
	// }
	//
	// @Override
	// public EVisitResult visit(CommentNode pNode) {
	// // FIXME.
	// return null;
	// }
//
//}
