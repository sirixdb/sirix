/**
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met: * Redistributions of source code must retain the
 * above copyright notice, this list of conditions and the following disclaimer. * Redistributions
 * in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.sirix.page;

import static com.google.common.base.Preconditions.checkNotNull;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import org.sirix.api.PageReadTrx;
import org.sirix.api.PageWriteTrx;
import org.sirix.cache.TransactionIntentLog;
import org.sirix.node.interfaces.Record;
import org.sirix.page.delegates.PageDelegate;
import org.sirix.page.interfaces.KeyValuePage;
import org.sirix.page.interfaces.Page;
import org.sirix.settings.Constants;
import com.google.common.base.MoreObjects;

/**
 * <h1>RevisionRootPage</h1>
 *
 * <p>
 * Revision root page holds a reference to the name page as well as the static node page tree.
 * </p>
 */
public final class RevisionRootPage extends AbstractForwardingPage {

  /** Offset of indirect page reference. */
  private static final int INDIRECT_REFERENCE_OFFSET = 0;

  /** Offset of path summary page reference. */
  private static final int PATH_SUMMARY_REFERENCE_OFFSET = 1;

  /** Offset of name page reference. */
  private static final int NAME_REFERENCE_OFFSET = 2;

  /** Offset of CAS page reference. */
  private static final int CAS_REFERENCE_OFFSET = 3;

  /** Offset of path page reference. */
  private static final int PATH_REFERENCE_OFFSET = 4;

  /** Last allocated node key. */
  private long mMaxNodeKey;

  /** Timestamp of revision. */
  private long mRevisionTimestamp;

  /** {@link PageDelegate} instance. */
  private final PageDelegate mDelegate;

  /** Revision number. */
  private final int mRevision;

  /**
   * Create revision root page.
   */
  public RevisionRootPage() {
    mDelegate = new PageDelegate(5);
    getReference(PATH_SUMMARY_REFERENCE_OFFSET).setPage(new PathSummaryPage());
    getReference(NAME_REFERENCE_OFFSET).setPage(new NamePage());
    getReference(CAS_REFERENCE_OFFSET).setPage(new CASPage());
    getReference(PATH_REFERENCE_OFFSET).setPage(new PathPage());
    mRevision = Constants.UBP_ROOT_REVISION_NUMBER;
    mMaxNodeKey = -1L;
  }

  /**
   * Read revision root page.
   *
   * @param in input stream
   */
  protected RevisionRootPage(final DataInput in, final SerializationType type) throws IOException {
    mDelegate = new PageDelegate(5, in, type);
    mRevision = in.readInt();
    mMaxNodeKey = in.readLong();
    mRevisionTimestamp = in.readLong();
  }

  /**
   * Clone revision root page.
   *
   * @param committedRevisionRootPage page to clone
   * @param representRev revision number to use
   */
  public RevisionRootPage(final RevisionRootPage committedRevisionRootPage,
      final @Nonnegative int representRev) {
    mDelegate = new PageDelegate(committedRevisionRootPage,
        committedRevisionRootPage.mDelegate.getBitmap());
    mRevision = representRev;
    mMaxNodeKey = committedRevisionRootPage.mMaxNodeKey;
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
   * Get CAS page reference.
   *
   * @return CAS page reference
   */
  public PageReference getCASPageReference() {
    return getReference(CAS_REFERENCE_OFFSET);
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
   * Get path page reference.
   *
   * @return path page reference
   */
  public PageReference getPathPageReference() {
    return getReference(PATH_REFERENCE_OFFSET);
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
   * Increment number of nodes by one while allocating another key.
   */
  public long incrementAndGetMaxNodeKey() {
    return ++mMaxNodeKey;
  }

  /**
   * Set the maximum node key in the revision.
   *
   * @param maxNodeKey new maximum node key
   */
  public void setMaxNodeKey(final @Nonnegative long maxNodeKey) {
    mMaxNodeKey = maxNodeKey;
  }

  /**
   * Only commit whole subtree if it's the currently added revision.
   *
   * {@inheritDoc}
   */
  @Override
  public <K extends Comparable<? super K>, V extends Record, S extends KeyValuePage<K, V>> void commit(
      @Nonnull PageWriteTrx<K, V, S> pageWriteTrx) {
    if (mRevision == pageWriteTrx.getUberPage().getRevision()) {
      super.commit(pageWriteTrx);
    }
  }

  @Override
  public void serialize(final DataOutput out, final SerializationType type) throws IOException {
    mRevisionTimestamp = System.currentTimeMillis();
    mDelegate.serialize(checkNotNull(out), checkNotNull(type));
    out.writeInt(mRevision);
    out.writeLong(mMaxNodeKey);
    out.writeLong(mRevisionTimestamp);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
                      .add("revisionTimestamp", mRevisionTimestamp)
                      .add("maxNodeKey", mMaxNodeKey)
                      .add("delegate", mDelegate)
                      .add("namePage", getReference(NAME_REFERENCE_OFFSET))
                      .add("pathSummaryPage", getReference(PATH_SUMMARY_REFERENCE_OFFSET))
                      .add("pathPage", getReference(PATH_REFERENCE_OFFSET))
                      .add("CASPage", getReference(CAS_REFERENCE_OFFSET))
                      .add("nodePage", getReference(INDIRECT_REFERENCE_OFFSET))
                      .toString();
  }

  @Override
  protected Page delegate() {
    return mDelegate;
  }

  /**
   * Initialize node tree.
   *
   * @param pageReadTrx {@link PageReadTrx} instance
   * @param log the transaction intent log
   */
  public void createNodeTree(final PageReadTrx pageReadTrx, final TransactionIntentLog log) {
    final PageReference reference = getIndirectPageReference();
    if (reference.getPage() == null && reference.getKey() == Constants.NULL_ID_LONG
        && reference.getLogKey() == Constants.NULL_ID_INT
        && reference.getPersistentLogKey() == Constants.NULL_ID_LONG) {
      PageUtils.createTree(reference, PageKind.RECORDPAGE, -1, pageReadTrx, log);
      incrementAndGetMaxNodeKey();
    }
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
