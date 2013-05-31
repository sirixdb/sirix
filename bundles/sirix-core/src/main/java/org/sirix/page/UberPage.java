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

import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.api.PageReadTrx;
import org.sirix.api.PageWriteTrx;
import org.sirix.cache.IndirectPageLogKey;
import org.sirix.node.interfaces.Record;
import org.sirix.page.delegates.PageDelegate;
import org.sirix.page.interfaces.KeyValuePage;
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
	private RevisionRootPage mRootPage;

	/** The current most recent revision */
	private final int mRevision;

	/** The number of indexes. */
	private int mIndexes;

	/**
	 * Create uber page.
	 * 
	 * @param resourceConfig
	 *          {@link ResourceConfiguration} reference
	 */
	public UberPage() {
		mDelegate = new PageDelegate(1);
		mRevision = Constants.UBP_ROOT_REVISION_NUMBER;
		mRevisionCount = Constants.UBP_ROOT_REVISION_COUNT;
		mBootstrap = true;
	}

	/**
	 * Read uber page.
	 * 
	 * @param pIn
	 *          input bytes
	 * @param resourceConfig
	 *          {@link ResourceConfiguration} reference
	 */
	protected UberPage(final ByteArrayDataInput in) {
		mDelegate = new PageDelegate(1, in);
		mRevisionCount = in.readInt();
		mRevision = mRevisionCount == 0 ? 0 : mRevisionCount - 1;
		mBootstrap = false;
		mRootPage = null;
	}

	/**
	 * Clone constructor.
	 * 
	 * @param commitedUberPage
	 *          page to clone
	 * @param resourceConfig
	 *          {@link ResourceConfiguration} reference
	 */
	public UberPage(final UberPage commitedUberPage) {
		mDelegate = new PageDelegate(commitedUberPage);
		if (commitedUberPage.isBootstrap()) {
			mRevision = commitedUberPage.mRevision;
			mRevisionCount = commitedUberPage.mRevisionCount;
			mBootstrap = commitedUberPage.mBootstrap;
			mRootPage = commitedUberPage.mRootPage;
			mIndexes = commitedUberPage.mIndexes;
		} else {
			mRevision = commitedUberPage.mRevision + 1;
			mRevisionCount = commitedUberPage.mRevisionCount + 1;
			mBootstrap = false;
			mRootPage = null;
			mIndexes = commitedUberPage.mIndexes;
		}
	}

	/**
	 * Increment the number of stored indexes by one.
	 * 
	 * @return this {@code UberPage} reference
	 */
	public UberPage incrementIndexCount() {
		mIndexes++;
		return this;
	}

	/**
	 * Get the number of stored indexes.
	 * 
	 * @return number of stored indexes
	 */
	public int getIndexCount() {
		return mIndexes;
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
	public void serialize(final ByteArrayDataOutput out) {
		mBootstrap = false;
		mDelegate.serialize(checkNotNull(out));
		out.writeInt(mRevisionCount);
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
	 * Create revision tree.
	 * 
	 * @param pageReadTrx
	 *          {@link PageReadTrx} instance
	 * @param revisionRoot
	 *          {@link RevisionRootPage} instance
	 */
	public <K extends Comparable<? super K>, V extends Record, S extends KeyValuePage<K, V>> void createRevisionTree(
			final PageWriteTrx<K, V, S> pageWriteTrx) {
		// Initialize revision tree to guarantee that there is a revision root page.
		Page page = null;

		// Remaining levels.
		for (int i = 0, l = Constants.UBPINP_LEVEL_PAGE_COUNT_EXPONENT.length; i < l; i++) {
			page = new IndirectPage();
			pageWriteTrx.putPageIntoCache(new IndirectPageLogKey(PageKind.UBERPAGE, -1,
					i, 0), page);
		}

		mRootPage = new RevisionRootPage();
		pageWriteTrx.putPageIntoCache(new IndirectPageLogKey(PageKind.UBERPAGE, -1,
				Constants.UBPINP_LEVEL_PAGE_COUNT_EXPONENT.length, 0), mRootPage);
	}

	/**
	 * Get the page count exponent for the given page.
	 * 
	 * @param pageKind
	 *          page to lookup the exponent in the constant definition
	 * @return page count exponent
	 */
	public int[] getPageCountExp(final PageKind pageKind) {
		int[] inpLevelPageCountExp = new int[0];
		switch (pageKind) {
		case PATHSUMMARYPAGE:
			inpLevelPageCountExp = Constants.PATHINP_LEVEL_PAGE_COUNT_EXPONENT;
			break;
		case PATHPAGE:
		case CASPAGE:
		case NAMEPAGE:
		case RECORDPAGE:
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

	/**
	 * Get the revision number.
	 * 
	 * @return revision number
	 */
	public int getRevision() {
		return mRevision;
	}
}