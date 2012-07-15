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
import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;

import javax.annotation.Nonnull;

import org.sirix.page.delegates.PageDelegate;
import org.sirix.page.interfaces.IPage;
import org.sirix.utils.IConstants;

/**
 * <h1>RevisionRootPage</h1>
 * 
 * <p>
 * Revision root page holds a reference to the name page as well as the static node page tree.
 * </p>
 */
public final class RevisionRootPage extends AbsForwardingPage {

  /** Offset of name page reference. */
  private static final int NAME_REFERENCE_OFFSET = 0;

  /** Offset of path summary page reference. */
  private static final int PATH_SUMMARY_REFERENCE_OFFSET = 1;

  /** Offset of indirect page reference. */
  private static final int INDIRECT_REFERENCE_OFFSET = 2;

  /** Number of nodes of this revision. */
  private long mRevisionSize;

  /** Last allocated node key. */
  private long mMaxNodeKey;

  /** Last allocated path node key. */
  private long mMaxPathNodeKey;

  /** Timestamp of revision. */
  private long mRevisionTimestamp;

  /** {@link PageDelegate} instance. */
  private final PageDelegate mDelegate;

  /**
   * Create revision root page.
   */
  public RevisionRootPage() {
    mDelegate = new PageDelegate(3, IConstants.UBP_ROOT_REVISION_NUMBER);
    mRevisionSize = 0L;
    getReferences()[NAME_REFERENCE_OFFSET].setPage(new NamePage(
      IConstants.UBP_ROOT_REVISION_NUMBER));
    getReferences()[PATH_SUMMARY_REFERENCE_OFFSET].setPage(new PathSummaryPage(
      IConstants.UBP_ROOT_REVISION_NUMBER));
    mMaxNodeKey = -1L;
    mMaxPathNodeKey = -1L;
  }

  /**
   * Read revision root page.
   * 
   * @param pIn
   *          input stream
   */
  protected RevisionRootPage(@Nonnull final ByteArrayDataInput pIn) {
    mDelegate = new PageDelegate(3, pIn);
    mRevisionSize = pIn.readLong();
    mMaxNodeKey = pIn.readLong();
    mMaxPathNodeKey = pIn.readLong();
    mRevisionTimestamp = pIn.readLong();
  }

  /**
   * Clone revision root page.
   * 
   * @param pCommittedRevisionRootPage
   *          page to clone
   * @param pRevisionToUse
   *          revision number to use
   */
  public RevisionRootPage(
    @Nonnull final RevisionRootPage pCommittedRevisionRootPage,
    final long pRevisionToUse) {
    mDelegate = new PageDelegate(pCommittedRevisionRootPage, pRevisionToUse);
    mRevisionSize = pCommittedRevisionRootPage.mRevisionSize;
    mMaxNodeKey = pCommittedRevisionRootPage.mMaxNodeKey;
    mMaxPathNodeKey = pCommittedRevisionRootPage.mMaxPathNodeKey;
    mRevisionTimestamp = pCommittedRevisionRootPage.mRevisionTimestamp;
  }

  /**
   * Get path summary page reference.
   * 
   * @return path summary page reference
   */
  public PageReference getPathSummaryPageReference() {
    return getReferences()[PATH_SUMMARY_REFERENCE_OFFSET];
  }

  /**
   * Get name page reference.
   * 
   * @return name page reference
   */
  public PageReference getNamePageReference() {
    return getReferences()[NAME_REFERENCE_OFFSET];
  }

  /**
   * Get indirect page reference.
   * 
   * @return Indirect page reference.
   */
  public PageReference getIndirectPageReference() {
    return getReferences()[INDIRECT_REFERENCE_OFFSET];
  }

  /**
   * Get size of revision, i.e., the node count visible in this revision.
   * 
   * @return Revision size.
   */
  public long getRevisionSize() {
    return mRevisionSize;
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
   * Set the maximum node key in the revision.
   * 
   * @param pMaxNodeKey
   *          new maximum node key
   */
  public void setMaxNodeKey(final long pMaxNodeKey) {
    mMaxNodeKey = pMaxNodeKey;
  }

  /**
   * Set the maximum path node key in the revision.
   * 
   * @param pMaxNodeKey
   *          new maximum node key
   */
  public void setMaxPathNodeKey(final long pMaxNodeKey) {
    mMaxPathNodeKey = pMaxNodeKey;
  }

  @Override
  public void serialize(@Nonnull final ByteArrayDataOutput pOut) {
    mRevisionTimestamp = System.currentTimeMillis();
    mDelegate.serialize(checkNotNull(pOut));
    pOut.writeLong(mRevisionSize);
    pOut.writeLong(mMaxNodeKey);
    pOut.writeLong(mMaxPathNodeKey);
    pOut.writeLong(mRevisionTimestamp);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("revisionSize", mRevisionSize).add(
      "revisionTimestamp", mRevisionTimestamp).add("maxNodeKey", mMaxNodeKey)
      .add("delegate", mDelegate).add("namePage",
        getReferences()[NAME_REFERENCE_OFFSET]).add("pathSummaryPage",
        getReferences()[PATH_SUMMARY_REFERENCE_OFFSET]).add("indirectPage",
        getReferences()[INDIRECT_REFERENCE_OFFSET]).toString();
  }

  @Override
  protected IPage delegate() {
    return mDelegate;
  }
}
