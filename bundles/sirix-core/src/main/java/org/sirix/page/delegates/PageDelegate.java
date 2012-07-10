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

package org.sirix.page.delegates;

import static com.google.common.base.Preconditions.checkArgument;
import com.google.common.base.Objects;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import org.sirix.api.IPageWriteTrx;
import org.sirix.exception.AbsTTException;
import org.sirix.io.ITTSink;
import org.sirix.io.ITTSource;
import org.sirix.page.PageReference;
import org.sirix.page.interfaces.IPage;

/**
 * <h1>Page</h1>
 * 
 * <p>
 * Class to provide basic reference handling functionality.
 * </p>
 */
public class PageDelegate implements IPage {

  /** Page references. */
  private PageReference[] mReferences;

  /** Revision of this page. */
  private final long mRevision;

  /**
   * Constructor to initialize instance.
   * 
   * @param pReferenceCount
   *          number of references of page
   * @param pRevision
   *          revision number
   */
  public PageDelegate(@Nonnegative final int pReferenceCount,
    @Nonnegative final long pRevision) {
    checkArgument(pReferenceCount >= 0);
    checkArgument(pRevision >= 0);
    mReferences = new PageReference[pReferenceCount];
    mRevision = pRevision;
    for (int i = 0; i < pReferenceCount; i++) {
      mReferences[i] = new PageReference();
    }
  }

  /**
   * Constructor to initialize instance.
   * 
   * @param pReferenceCount
   *          number of references of page
   * @param pIn
   *          input stream to read from
   */
  public PageDelegate(@Nonnegative final int pReferenceCount,
    @Nonnull final ITTSource pIn) {
    checkArgument(pReferenceCount >= 0);
    mReferences = new PageReference[pReferenceCount];
    mRevision = pIn.readLong();
    for (int offset = 0; offset < mReferences.length; offset++) {
      mReferences[offset] = new PageReference();
      mReferences[offset].setKey(pIn.readLong());
    }
  }

  /**
   * Constructor to initialize instance.
   * 
   * @param pCommitedPage
   *          commited page
   * @param pRevision
   *          revision number
   */
  public PageDelegate(@Nonnull final IPage pCommitedPage,
    @Nonnegative final long pRevision) {
    checkArgument(pRevision >= 0);
    mReferences = pCommitedPage.getReferences();
    mRevision = pRevision;
  }

  /**
   * Get page reference of given offset.
   * 
   * @param pOffset
   *          offset of page reference
   * @return {@link PageReference} at given offset
   */
  public final PageReference getChildren(@Nonnegative final int pOffset) {
    if (mReferences[pOffset] == null) {
      mReferences[pOffset] = new PageReference();
    }
    return mReferences[pOffset];
  }

  /**
   * Recursively call commit on all referenced pages.
   * 
   * @param pState
   *          IWriteTransaction state
   * @throws AbsTTException
   *           if a write-error occured
   */
  @Override
  public final void commit(@Nonnull final IPageWriteTrx pPageWriteTrx)
    throws AbsTTException {
    for (final PageReference reference : mReferences) {
      pPageWriteTrx.commit(reference);
    }
  }

  /**
   * Serialize page references into output.
   * 
   * @param pOut
   *          output stream
   */
  @Override
  public void serialize(@Nonnull final ITTSink pOut) {
    for (final PageReference reference : mReferences) {
      pOut.writeLong(reference.getKey());
    }
  }

  /**
   * Get all references.
   * 
   * @return copied references
   */
  @Override
  public final PageReference[] getReferences() {
    final PageReference[] copiedRefs = new PageReference[mReferences.length];
    System.arraycopy(mReferences, 0, copiedRefs, 0, mReferences.length);
    return copiedRefs;
  }

  /**
   * Get the revision.
   * 
   * @return the revision
   */
  @Override
  public final long getRevision() {
    return mRevision;
  }

  @Override
  public String toString() {
    final Objects.ToStringHelper helper =  Objects.toStringHelper(this);
    for (final PageReference ref : mReferences) {
      helper.add("reference", ref);
    }
    return helper.toString();
  }

}
