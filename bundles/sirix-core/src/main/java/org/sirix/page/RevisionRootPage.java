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

import javax.annotation.Nonnull;

import com.google.common.base.Objects;
import org.sirix.io.ITTSink;
import org.sirix.io.ITTSource;
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

  /** Offset of indirect page reference. */
  private static final int INDIRECT_REFERENCE_OFFSET = 1;

  /** Number of nodes of this revision. */
  private long mRevisionSize;

  /** Last allocated node key. */
  private long mMaxNodeKey;

  /** Timestamp of revision. */
  private long mRevisionTimestamp;

  /** {@link PageDelegate} instance. */
  private final PageDelegate mDelegate;

  /**
   * Create revision root page.
   */
  public RevisionRootPage() {
    mDelegate = new PageDelegate(2, IConstants.UBP_ROOT_REVISION_NUMBER);
    mRevisionSize = 0L;
    final PageReference ref = getReferences()[NAME_REFERENCE_OFFSET];
    ref.setPage(new NamePage(IConstants.UBP_ROOT_REVISION_NUMBER));
    mMaxNodeKey = -1L;
  }

  /**
   * Read revision root page.
   * 
   * @param pIn
   *          input stream
   */
  protected RevisionRootPage(@Nonnull final ITTSource pIn) {
    mDelegate = new PageDelegate(2, pIn);
    mRevisionSize = pIn.readLong();
    mMaxNodeKey = pIn.readLong();
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
   * @return Last allocated node key.
   */
  public long getMaxNodeKey() {
    return mMaxNodeKey;
  }

  /**
   * Increment number of nodes by one while allocating another key.
   */
  public void incrementMaxNodeKey() {
    mMaxNodeKey += 1;
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

  @Override
  public void serialize(@Nonnull final ITTSink pOut) {
    mRevisionTimestamp = System.currentTimeMillis();
    mDelegate.serialize(checkNotNull(pOut));
    pOut.writeLong(mRevisionSize);
    pOut.writeLong(mMaxNodeKey);
    pOut.writeLong(mRevisionTimestamp);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("revisionSize", mRevisionSize).add(
      "revisionTimestamp", mRevisionTimestamp).add("namePage",
      getReferences()[NAME_REFERENCE_OFFSET]).add("indirectPage",
      getReferences()[INDIRECT_REFERENCE_OFFSET]).toString();
  }

  @Override
  protected IPage delegate() {
    return mDelegate;
  }
}
