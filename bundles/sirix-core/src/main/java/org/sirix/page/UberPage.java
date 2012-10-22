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

import static com.google.common.base.Preconditions.checkNotNull;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.node.DocumentRootNode;
import org.sirix.node.SirixDeweyID;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.delegates.StructNodeDelegate;
import org.sirix.page.delegates.PageDelegate;
import org.sirix.page.interfaces.Page;
import org.sirix.settings.Fixed;
import org.sirix.settings.Constants;

/**
 * <h1>UberPage</h1>
 * 
 * <p>
 * Uber page holds a reference to the static revision root page tree.
 * </p>
 */
public final class UberPage extends AbstractForwardingPage {

	/** Offset of indirect page reference. */
	private static final int INDIRECT_REFERENCE_OFFSET = 0;

	/** Number of revisions. */
	private final int mRevisionCount;

	/**
	 * {@code true} if this uber page is the uber page of a fresh sirix file,
	 * {@code false} otherwise.
	 */
	private boolean mBootstrap;

	/** {@link PageDelegate} reference. */
	private final PageDelegate mDelegate;

	/** Determines if the first revision has been bulk inserted. */
	private boolean mBulkInserted;

	private final RevisionRootPage mRootPage;

	private final ResourceConfiguration mResourceConfiguration;

	/**
	 * Create uber page.
	 * 
	 * @param resourceConfig
	 *          {@link ResourceConfiguration} reference
	 */
	public UberPage(final @Nonnull ResourceConfiguration resourceConfig) {
		mDelegate = new PageDelegate(1, Constants.UBP_ROOT_REVISION_NUMBER);
		mRevisionCount = Constants.UBP_ROOT_REVISION_COUNT;
		mBootstrap = true;
		mBulkInserted = true;
		assert resourceConfig != null : "resourceConfig must not be null!";
		mResourceConfiguration = resourceConfig;

		// --- Create revision tree
		// ------------------------------------------------

		// Initialize revision tree to guarantee that there is a revision root
		// page.
		Page page = null;
		PageReference reference = getReference(INDIRECT_REFERENCE_OFFSET);

		// Remaining levels.
		for (int i = 0, l = Constants.UBPINP_LEVEL_PAGE_COUNT_EXPONENT.length; i < l; i++) {
			page = new IndirectPage(Constants.UBP_ROOT_REVISION_NUMBER);
			reference.setPage(page);
			reference.setPageKind(PageKind.INDIRECTPAGE);
			reference = page.getReference(0);
		}

		mRootPage = new RevisionRootPage();
		reference.setPage(mRootPage);
		reference.setPageKind(PageKind.REVISIONROOTPAGE);

		// --- Create node tree
		// ----------------------------------------------------

		// Initialize revision tree to guarantee that there is a revision root
		// page.
		reference = mRootPage.getIndirectPageReference();
		createTree(reference, PageKind.NODEPAGE);
		mRootPage.incrementMaxNodeKey();
	}

	/**
	 * Initialize value tree.
	 */
	public void createValueTree() {
		final PageReference reference = mRootPage.getValuePageReference().getPage()
				.getReference(INDIRECT_REFERENCE_OFFSET);
		createTree(reference, PageKind.VALUEPAGE);
		mRootPage.incrementMaxValueNodeKey();
	}

	/**
	 * Initialize path summary tree.
	 */
	public void createPathSummaryTree() {
		final PageReference reference = mRootPage.getPathSummaryPageReference()
				.getPage().getReference(INDIRECT_REFERENCE_OFFSET);
		createTree(reference, PageKind.PATHSUMMARYPAGE);
		mRootPage.incrementMaxPathNodeKey();
	}

	/**
	 * Determines if first (revision 0) has been solely bulk inserted.
	 */
	public boolean isBulkInserted() {
		return mBulkInserted;
	}

	/**
	 * Set if first revision has been bulk inserted.
	 * 
	 * @param isBulkInserted
	 *          bulk inserted or not
	 */
	public void setIsBulkInserted(final boolean isBulkInserted) {
		mBulkInserted = isBulkInserted;
	}

	/**
	 * Create the initial tree structure.
	 * 
	 * @param reference
	 *          reference from revision root
	 * @param pageKind
	 *          the page kind
	 */
	private void createTree(@Nonnull PageReference reference,
			final @Nonnull PageKind pageKind) {
		Page page = null;

		// Level page count exponent from the configuration.
		final int[] levelPageCountExp = getPageCountExp(pageKind);

		// Remaining levels.
		for (int i = 0, l = levelPageCountExp.length; i < l; i++) {
			page = new IndirectPage(Constants.UBP_ROOT_REVISION_NUMBER);
			reference.setPage(page);
			reference.setPageKind(PageKind.INDIRECTPAGE);
			reference = page.getReference(0);
		}

		final NodePage ndp = new NodePage(
				Fixed.ROOT_PAGE_KEY.getStandardProperty(),
				Constants.UBP_ROOT_REVISION_NUMBER, mResourceConfiguration);
		reference.setPage(ndp);
		reference.setPageKind(pageKind);

		final NodeDelegate nodeDel = new NodeDelegate(
				Fixed.DOCUMENT_NODE_KEY.getStandardProperty(),
				Fixed.NULL_NODE_KEY.getStandardProperty(),
				Fixed.NULL_NODE_KEY.getStandardProperty(), 0, Optional.of(SirixDeweyID
						.newRootID()));
		final StructNodeDelegate strucDel = new StructNodeDelegate(nodeDel,
				Fixed.NULL_NODE_KEY.getStandardProperty(),
				Fixed.NULL_NODE_KEY.getStandardProperty(),
				Fixed.NULL_NODE_KEY.getStandardProperty(), 0, 0);
		ndp.setNode(new DocumentRootNode(nodeDel, strucDel));
	}

	/**
	 * Read uber page.
	 * 
	 * @param pIn
	 *          input bytes
	 * @param resourceConfig
	 *          {@link ResourceConfiguration} reference
	 */
	protected UberPage(final @Nonnull ByteArrayDataInput pIn,
			final @Nonnull ResourceConfiguration resourceConfig) {
		mDelegate = new PageDelegate(1, pIn);
		mRevisionCount = pIn.readInt();
		mBulkInserted = pIn.readBoolean();
		mBootstrap = false;
		mRootPage = null;
		assert resourceConfig != null : "resourceConfig must not be null!";
		mResourceConfiguration = resourceConfig;
	}

	/**
	 * Clone uber page.
	 * 
	 * @param committedUberPage
	 *          page to clone
	 * @param revisionToUse
	 *          revision number to use
	 * @param resourceConfig
	 *          {@link ResourceConfiguration} reference
	 */
	public UberPage(final @Nonnull UberPage committedUberPage,
			final @Nonnegative int revisionToUse) {
		mDelegate = new PageDelegate(committedUberPage, revisionToUse);
		if (committedUberPage.isBootstrap()) {
			mRevisionCount = committedUberPage.mRevisionCount;
			mBootstrap = committedUberPage.mBootstrap;
			mRootPage = committedUberPage.mRootPage;
		} else {
			mRevisionCount = committedUberPage.mRevisionCount + 1;
			mBootstrap = false;
			mRootPage = null;
		}
		mResourceConfiguration = committedUberPage.mResourceConfiguration;
	}

	/**
	 * Get indirect page reference.
	 * 
	 * @return indirect page reference
	 */
	public PageReference getIndirectPageReference() {
		return getReference(INDIRECT_REFERENCE_OFFSET);
	}

	/**
	 * Get number of revisions.
	 * 
	 * @return number of revisions
	 */
	public int getRevisionCount() {
		return mRevisionCount;
	}

	/**
	 * Get key of last committed revision.
	 * 
	 * @return key of last committed revision
	 */
	public int getLastCommitedRevisionNumber() {
		return mRevisionCount - 2;
	}

	/**
	 * Get revision key of current in-memory state.
	 * 
	 * @return revision key
	 */
	public int getRevisionNumber() {
		return mRevisionCount - 1;
	}

	/**
	 * Flag to indicate whether this uber page is the first ever.
	 * 
	 * @return {@code true} if this uber page is the first one of sirix,
	 *         {@code false} otherwise
	 */
	public boolean isBootstrap() {
		return mBootstrap;
	}

	@Override
	public void serialize(final @Nonnull ByteArrayDataOutput pOut) {
		mBootstrap = false;
		mDelegate.serialize(checkNotNull(pOut));
		pOut.writeInt(mRevisionCount);
		pOut.writeBoolean(mBulkInserted);
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this)
				.add("forwarding page", super.toString())
				.add("revisionCount", mRevisionCount)
				.add("indirectPage", getReferences()[INDIRECT_REFERENCE_OFFSET])
				.add("isBootstrap", mBootstrap).add("isBulkInserted", mBulkInserted)
				.toString();
	}

	@Override
	protected Page delegate() {
		return mDelegate;
	}

	@Override
	public Page setDirty(final boolean pDirty) {
		mDelegate.setDirty(pDirty);
		return this;
	}

	/**
	 * Get the page count exponent for the given page.
	 * 
	 * @param pageKind
	 *          page to lookup the exponent in the constant definition
	 * @return page count exponent
	 */
	public int[] getPageCountExp(final @Nonnull PageKind pageKind) {
		int[] inpLevelPageCountExp = new int[0];
		switch (pageKind) {
		case PATHSUMMARYPAGE:
			inpLevelPageCountExp = Constants.PATHINP_LEVEL_PAGE_COUNT_EXPONENT;
			break;
		case VALUEPAGE:
		case NODEPAGE:
			inpLevelPageCountExp = Constants.INP_LEVEL_PAGE_COUNT_EXPONENT;
			break;
		case UBERPAGE:
			inpLevelPageCountExp = Constants.UBPINP_LEVEL_PAGE_COUNT_EXPONENT;
			break;
		default:
			throw new IllegalStateException("page kind not known!");
		}
		return inpLevelPageCountExp;
	}
}