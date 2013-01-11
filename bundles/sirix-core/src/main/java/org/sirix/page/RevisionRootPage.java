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

import org.sirix.api.PageReadTrx;
import org.sirix.api.PageWriteTrx;
import org.sirix.exception.SirixException;
import org.sirix.node.DocumentRootNode;
import org.sirix.node.SirixDeweyID;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.delegates.StructNodeDelegate;
import org.sirix.node.interfaces.Record;
import org.sirix.page.delegates.PageDelegate;
import org.sirix.page.interfaces.KeyValuePage;
import org.sirix.page.interfaces.Page;
import org.sirix.settings.Constants;
import org.sirix.settings.Fixed;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
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
public final class RevisionRootPage extends AbstractForwardingPage {

	/** Offset of indirect page reference. */
	private static final int INDIRECT_REFERENCE_OFFSET = 0;

	/** Offset of name page reference. */
	private static final int NAME_REFERENCE_OFFSET = 1;

	/** Offset of path summary page reference. */
	private static final int PATH_SUMMARY_REFERENCE_OFFSET = 2;

	/** Offset of text value page reference. */
	private static final int TEXT_VALUE_REFERENCE_OFFSET = 3;

	/** Offset of text value page reference. */
	private static final int ATTRIBUTE_VALUE_REFERENCE_OFFSET = 4;

	/** Last allocated node key. */
	private long mMaxNodeKey;

	/** Last allocated path node key. */
	private long mMaxPathNodeKey;

	/** Last allocated text value node key. */
	private long mMaxTextValueNodeKey;

	/** Last allocated attribute value node key. */
	private long mMaxAttributeValueNodeKey;

	/** Timestamp of revision. */
	private long mRevisionTimestamp;

	/** {@link PageDelegate} instance. */
	private final PageDelegate mDelegate;

	/**
	 * Create revision root page.
	 */
	public RevisionRootPage() {
		mDelegate = new PageDelegate(5, Constants.UBP_ROOT_REVISION_NUMBER);
		getReference(NAME_REFERENCE_OFFSET).setPage(
				new NamePage(Constants.UBP_ROOT_REVISION_NUMBER));
		getReference(PATH_SUMMARY_REFERENCE_OFFSET).setPage(
				new PathSummaryPage(Constants.UBP_ROOT_REVISION_NUMBER));
		getReference(TEXT_VALUE_REFERENCE_OFFSET).setPage(
				new TextValuePage(Constants.UBP_ROOT_REVISION_NUMBER));
		getReference(ATTRIBUTE_VALUE_REFERENCE_OFFSET).setPage(
				new AttributeValuePage(Constants.UBP_ROOT_REVISION_NUMBER));
		mMaxNodeKey = -1L;
		mMaxPathNodeKey = -1L;
		mMaxTextValueNodeKey = -1L;
		mMaxAttributeValueNodeKey = -1L;
	}

	/**
	 * Read revision root page.
	 * 
	 * @param in
	 *          input stream
	 */
	protected RevisionRootPage(final @Nonnull ByteArrayDataInput in) {
		mDelegate = new PageDelegate(5, in);
		mMaxNodeKey = in.readLong();
		mMaxPathNodeKey = in.readLong();
		mMaxTextValueNodeKey = in.readLong();
		mMaxAttributeValueNodeKey = in.readLong();
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
		mMaxTextValueNodeKey = committedRevisionRootPage.mMaxTextValueNodeKey;
		mMaxAttributeValueNodeKey = committedRevisionRootPage.mMaxAttributeValueNodeKey;
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
	 * Get text value page reference.
	 * 
	 * @return value page reference
	 */
	public PageReference getTextValuePageReference() {
		return getReference(TEXT_VALUE_REFERENCE_OFFSET);
	}

	/**
	 * Get attribute value page reference.
	 * 
	 * @return value page reference
	 */
	public PageReference getAttributeValuePageReference() {
		return getReference(ATTRIBUTE_VALUE_REFERENCE_OFFSET);
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
	 * Get last allocated text value node key.
	 * 
	 * @return last allocated value node key
	 */
	public long getMaxTextValueNodeKey() {
		return mMaxTextValueNodeKey;
	}

	/**
	 * Get last allocated attribute value node key.
	 * 
	 * @return last allocated value node key
	 */
	public long getMaxAttributeValueNodeKey() {
		return mMaxAttributeValueNodeKey;
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
	 * Increment number of text value nodes by one while allocating another key.
	 */
	public void incrementMaxTextValueNodeKey() {
		mMaxTextValueNodeKey += 1;
	}

	/**
	 * Increment number of attribute value nodes by one while allocating another
	 * key.
	 */
	public void incrementMaxAttributeValueNodeKey() {
		mMaxAttributeValueNodeKey += 1;
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
	public void setMaxTextValueNodeKey(final @Nonnegative long maxNodeKey) {
		mMaxTextValueNodeKey = maxNodeKey;
	}

	/**
	 * Set the maximum value node key in the revision.
	 * 
	 * @param maxNodeKey
	 *          new maximum node key
	 */
	public void setMaxAttributeValueNodeKey(final @Nonnegative long maxNodeKey) {
		mMaxAttributeValueNodeKey = maxNodeKey;
	}

	/**
	 * Only commit whole subtree if it's the currently added revision.
	 * 
	 * {@inheritDoc}
	 */
	@Override
	public <K extends Comparable<? super K>, V extends Record, S extends KeyValuePage<K, V>> void commit(
			@Nonnull PageWriteTrx<K, V, S> pageWriteTrx) throws SirixException {
		if (mDelegate.getRevision() == pageWriteTrx.getUberPage().getRevision()) {
			super.commit(pageWriteTrx);
		}
	}

	@Override
	public void serialize(final @Nonnull ByteArrayDataOutput pOut) {
		mRevisionTimestamp = System.currentTimeMillis();
		mDelegate.serialize(checkNotNull(pOut));
		pOut.writeLong(mMaxNodeKey);
		pOut.writeLong(mMaxPathNodeKey);
		pOut.writeLong(mMaxTextValueNodeKey);
		pOut.writeLong(mMaxAttributeValueNodeKey);
		pOut.writeLong(mRevisionTimestamp);
	}

	@Override
	public String toString() {
		return Objects
				.toStringHelper(this)
				.add("revisionTimestamp", mRevisionTimestamp)
				.add("maxNodeKey", mMaxNodeKey)
				.add("delegate", mDelegate)
				.add("namePage", getReference(NAME_REFERENCE_OFFSET))
				.add("pathSummaryPage", getReference(PATH_SUMMARY_REFERENCE_OFFSET))
				.add("textValuePage", getReference(TEXT_VALUE_REFERENCE_OFFSET))
				.add("attributeValuePage",
						getReference(ATTRIBUTE_VALUE_REFERENCE_OFFSET))
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

	/**
	 * Initialize value tree.
	 * 
	 * @param pageReadTrx
	 *          {@link PageReadTrx} instance
	 * @param revisionRoot
	 *          {@link RevisionRootPage} instance
	 */
	public void createNodeTree(final @Nonnull PageReadTrx pageReadTrx) {
		final PageReference reference = getIndirectPageReference();
		if (reference.getPage() == null && reference.getKey() == Constants.NULL_ID) {
			createTree(reference, PageKind.NODEPAGE, pageReadTrx);
			incrementMaxNodeKey();
		}
	}

	/**
	 * Initialize text value tree.
	 * 
	 * @param pageReadTrx
	 *          {@link PageReadTrx} instance
	 * @param revisionRoot
	 *          {@link RevisionRootPage} instance
	 */
	public void createTextValueTree(final @Nonnull PageReadTrx pageReadTrx) {
		final PageReference reference = getTextValuePageReference().getPage()
				.getReference(INDIRECT_REFERENCE_OFFSET);
		if (reference.getPage() == null && reference.getKey() == Constants.NULL_ID) {
			createTree(reference, PageKind.TEXTVALUEPAGE, pageReadTrx);
			incrementMaxTextValueNodeKey();
		}
	}

	/**
	 * Initialize attribute value tree.
	 * 
	 * @param pageReadTrx
	 *          {@link PageReadTrx} instance
	 * @param revisionRoot
	 *          {@link RevisionRootPage} instance
	 */
	public void createAttributeValueTree(final @Nonnull PageReadTrx pageReadTrx) {
		final PageReference reference = getAttributeValuePageReference().getPage()
				.getReference(INDIRECT_REFERENCE_OFFSET);
		if (reference.getPage() == null && reference.getKey() == Constants.NULL_ID) {
			createTree(reference, PageKind.ATTRIBUTEVALUEPAGE, pageReadTrx);
			incrementMaxAttributeValueNodeKey();
		}
	}

	/**
	 * Initialize path summary tree.
	 * 
	 * @param pageReadTrx
	 *          {@link PageReadTrx} instance
	 * @param revisionRoot
	 *          {@link RevisionRootPage} instance
	 */
	public void createPathSummaryTree(final @Nonnull PageReadTrx pageReadTrx) {
		final PageReference reference = getPathSummaryPageReference().getPage()
				.getReference(INDIRECT_REFERENCE_OFFSET);
		if (reference.getPage() == null && reference.getKey() == Constants.NULL_ID) {
			createTree(reference, PageKind.PATHSUMMARYPAGE, pageReadTrx);
			incrementMaxPathNodeKey();
		}
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
			final @Nonnull PageKind pageKind, final @Nonnull PageReadTrx pageReadTrx) {
		Page page = null;

		// Level page count exponent from the configuration.
		final int[] levelPageCountExp = pageReadTrx.getUberPage().getPageCountExp(
				pageKind);

		// Remaining levels.
		for (int i = 0, l = levelPageCountExp.length; i < l; i++) {
			page = new IndirectPage(Constants.UBP_ROOT_REVISION_NUMBER);
			reference.setPage(page);
			reference.setPageKind(PageKind.INDIRECTPAGE);
			reference = page.getReference(0);
		}

		final UnorderedKeyValuePage ndp = new UnorderedKeyValuePage(
				Fixed.ROOT_PAGE_KEY.getStandardProperty(),
				Constants.UBP_ROOT_REVISION_NUMBER, pageReadTrx);
		ndp.setDirty(true);
		reference.setPage(ndp);
		reference.setKeyValuePageKey(0);
		reference.setPageKind(pageKind);

		final Optional<SirixDeweyID> id = pageReadTrx.getSession()
				.getResourceConfig().mDeweyIDsStored ? Optional.of(SirixDeweyID
				.newRootID()) : Optional.<SirixDeweyID> absent();
		final NodeDelegate nodeDel = new NodeDelegate(
				Fixed.DOCUMENT_NODE_KEY.getStandardProperty(),
				Fixed.NULL_NODE_KEY.getStandardProperty(),
				Fixed.NULL_NODE_KEY.getStandardProperty(), 0, id);
		final StructNodeDelegate strucDel = new StructNodeDelegate(nodeDel,
				Fixed.NULL_NODE_KEY.getStandardProperty(),
				Fixed.NULL_NODE_KEY.getStandardProperty(),
				Fixed.NULL_NODE_KEY.getStandardProperty(), 0, 0);
		ndp.setEntry(0L, new DocumentRootNode(nodeDel, strucDel));
	}
}
