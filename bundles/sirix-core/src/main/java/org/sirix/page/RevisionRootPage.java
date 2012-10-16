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

import org.sirix.page.delegates.PageDelegate;
import org.sirix.page.interfaces.Page;
import org.sirix.settings.Constants;

import com.google.common.base.Objects;
import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;

/**
 * <h1>RevisionRootPage</h1>
 * 
 * <p>
 * Revision root page holds a reference to the name page as well as the static
 * node page tree.
 * </p>
 */
public final class RevisionRootPage extends AbsForwardingPage {

	/** Offset of name page reference. */
	private static final int NAME_REFERENCE_OFFSET = 0;

	/** Offset of path summary page reference. */
	private static final int PATH_SUMMARY_REFERENCE_OFFSET = 1;

	/** Offset of value page reference. */
	private static final int VALUE_REFERENCE_OFFSET = 2;

	/** Offset of indirect page reference. */
	private static final int INDIRECT_REFERENCE_OFFSET = 3;

	/** Last allocated node key. */
	private long mMaxNodeKey;

	/** Last allocated path node key. */
	private long mMaxPathNodeKey;

	/** Last allocated value node key. */
	private long mMaxValueNodeKey;

	/** Timestamp of revision. */
	private long mRevisionTimestamp;

	/** {@link PageDelegate} instance. */
	private final PageDelegate mDelegate;

	/**
	 * Create revision root page.
	 */
	public RevisionRootPage() {
		mDelegate = new PageDelegate(4, Constants.UBP_ROOT_REVISION_NUMBER);
		getReference(NAME_REFERENCE_OFFSET).setPage(
				new NamePage(Constants.UBP_ROOT_REVISION_NUMBER));
		getReference(PATH_SUMMARY_REFERENCE_OFFSET).setPage(
				new PathSummaryPage(Constants.UBP_ROOT_REVISION_NUMBER));
		getReference(VALUE_REFERENCE_OFFSET).setPage(
				new ValuePage(Constants.UBP_ROOT_REVISION_NUMBER));
		mMaxNodeKey = -1L;
		mMaxPathNodeKey = -1L;
		mMaxValueNodeKey = -1L;
	}

	/**
	 * Read revision root page.
	 * 
	 * @param in
	 *          input stream
	 */
	protected RevisionRootPage(final @Nonnull ByteArrayDataInput in) {
		mDelegate = new PageDelegate(4, in);
		mMaxNodeKey = in.readLong();
		mMaxPathNodeKey = in.readLong();
		mMaxValueNodeKey = in.readLong();
		mRevisionTimestamp = in.readLong();
	}

	/**
	 * Clone revision root page.
	 * 
	 * @param committedRevisionRootPage
	 *          page to clone
	 * @param revisionToUse
	 *          revision number to use
	 */
	public RevisionRootPage(
			@Nonnull final RevisionRootPage committedRevisionRootPage,
			final int revisionToUse) {
		mDelegate = new PageDelegate(committedRevisionRootPage, revisionToUse);
		mMaxNodeKey = committedRevisionRootPage.mMaxNodeKey;
		mMaxPathNodeKey = committedRevisionRootPage.mMaxPathNodeKey;
		mMaxValueNodeKey = committedRevisionRootPage.mMaxValueNodeKey;
		mRevisionTimestamp = committedRevisionRootPage.mRevisionTimestamp;
	}

	/**
	 * Get path summary page reference.
	 * 
	 * @return path summary page reference
	 */
	public PageReference getPathSummaryPageReference() {
		return getReference(PATH_SUMMARY_REFERENCE_OFFSET);
	}

	/**
	 * Get value page reference.
	 * 
	 * @return value page reference
	 */
	public PageReference getValuePageReference() {
		return getReference(VALUE_REFERENCE_OFFSET);
	}

	/**
	 * Get name page reference.
	 * 
	 * @return name page reference
	 */
	public PageReference getNamePageReference() {
		return getReference(NAME_REFERENCE_OFFSET);
	}

	/**
	 * Get indirect page reference.
	 * 
	 * @return Indirect page reference.
	 */
	public PageReference getIndirectPageReference() {
		return getReference(INDIRECT_REFERENCE_OFFSET);
	}

	/**
	 * Get timestamp of revision.
	 * 
	 * @return Revision timestamp.
	 */
	public long getRevisionTimestamp() {
		return mRevisionTimestamp;
	}

	/**
	 * Get last allocated node key.
	 * 
	 * @return Last allocated node key
	 */
	public long getMaxNodeKey() {
		return mMaxNodeKey;
	}

	/**
	 * Get last allocated path node key.
	 * 
	 * @return last allocated path node key
	 */
	public long getMaxPathNodeKey() {
		return mMaxPathNodeKey;
	}

	/**
	 * Get last allocated value node key.
	 * 
	 * @return last allocated value node key
	 */
	public long getMaxValueNodeKey() {
		return mMaxValueNodeKey;
	}

	/**
	 * Increment number of nodes by one while allocating another key.
	 */
	public void incrementMaxNodeKey() {
		mMaxNodeKey += 1;
	}

	/**
	 * Increment number of path nodes by one while allocating another key.
	 */
	public void incrementMaxPathNodeKey() {
		mMaxPathNodeKey += 1;
	}

	/**
	 * Increment number of value nodes by one while allocating another key.
	 */
	public void incrementMaxValueNodeKey() {
		mMaxValueNodeKey += 1;
	}

	/**
	 * Set the maximum node key in the revision.
	 * 
	 * @param maxNodeKey
	 *          new maximum node key
	 */
	public void setMaxNodeKey(final @Nonnegative long maxNodeKey) {
		mMaxNodeKey = maxNodeKey;
	}

	/**
	 * Set the maximum path node key in the revision.
	 * 
	 * @param maxNodeKey
	 *          new maximum node key
	 */
	public void setMaxPathNodeKey(final @Nonnegative long maxNodeKey) {
		mMaxPathNodeKey = maxNodeKey;
	}

	/**
	 * Set the maximum value node key in the revision.
	 * 
	 * @param maxNodeKey
	 *          new maximum node key
	 */
	public void setMaxValueNodeKey(final @Nonnegative long maxNodeKey) {
		mMaxValueNodeKey = maxNodeKey;
	}

	@Override
	public void serialize(final @Nonnull ByteArrayDataOutput pOut) {
		mRevisionTimestamp = System.currentTimeMillis();
		mDelegate.serialize(checkNotNull(pOut));
		pOut.writeLong(mMaxNodeKey);
		pOut.writeLong(mMaxPathNodeKey);
		pOut.writeLong(mMaxValueNodeKey);
		pOut.writeLong(mRevisionTimestamp);
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this)
				.add("revisionTimestamp", mRevisionTimestamp)
				.add("maxNodeKey", mMaxNodeKey).add("delegate", mDelegate)
				.add("namePage", getReference(NAME_REFERENCE_OFFSET))
				.add("pathSummaryPage", getReference(PATH_SUMMARY_REFERENCE_OFFSET))
				.add("valuePage", getReference(VALUE_REFERENCE_OFFSET))
				.add("nodePage", getReference(INDIRECT_REFERENCE_OFFSET)).toString();
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
}
