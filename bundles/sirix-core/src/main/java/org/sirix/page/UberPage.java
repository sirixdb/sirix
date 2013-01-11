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

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.page.delegates.PageDelegate;
import org.sirix.page.interfaces.Page;
import org.sirix.settings.Constants;

import com.google.common.base.Objects;
import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;

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

	/** {@link PageDelegate} instance. */
	private final PageDelegate mDelegate;

	/** {@link RevisionRootPage} instance. */
	private final RevisionRootPage mRootPage;

	/**
	 * Create uber page.
	 * 
	 * @param resourceConfig
	 *          {@link ResourceConfiguration} reference
	 */
	public UberPage() {
		mDelegate = new PageDelegate(1, Constants.UBP_ROOT_REVISION_NUMBER);
		mRevisionCount = Constants.UBP_ROOT_REVISION_COUNT;
		mBootstrap = true;

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
			reference.setPageKind(PageKind.UBERPAGE);
			reference = page.getReference(0);
		}

		mRootPage = new RevisionRootPage();
		reference.setPage(mRootPage);
		reference.setPageKind(PageKind.REVISIONROOTPAGE);
	}

	/**
	 * Read uber page.
	 * 
	 * @param pIn
	 *          input bytes
	 * @param resourceConfig
	 *          {@link ResourceConfiguration} reference
	 */
	protected UberPage(final @Nonnull ByteArrayDataInput pIn) {
		mDelegate = new PageDelegate(1, pIn);
		mRevisionCount = pIn.readInt();
		mBootstrap = false;
		mRootPage = null;
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
		mDelegate = new PageDelegate(committedUberPage,
				committedUberPage.isBootstrap() ? 0 : revisionToUse);
		if (committedUberPage.isBootstrap()) {
			mRevisionCount = committedUberPage.mRevisionCount;
			mBootstrap = committedUberPage.mBootstrap;
			mRootPage = committedUberPage.mRootPage;
		} else {
			mRevisionCount = committedUberPage.mRevisionCount + 1;
			mBootstrap = false;
			mRootPage = null;
		}
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
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this)
				.add("forwarding page", super.toString())
				.add("revisionCount", mRevisionCount)
				.add("indirectPage", getReferences()[INDIRECT_REFERENCE_OFFSET])
				.add("isBootstrap", mBootstrap).toString();
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
		case ATTRIBUTEVALUEPAGE:
		case TEXTVALUEPAGE:
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