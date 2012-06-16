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

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import org.sirix.api.IPageWriteTrx;
import org.sirix.exception.AbsTTException;
import org.sirix.io.EStorage;
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
  public PageDelegate(@Nonnegative final int pReferenceCount, @Nonnegative final long pRevision) {
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
  public PageDelegate(@Nonnegative final int pReferenceCount, @Nonnull final ITTSource pIn) {
    mReferences = new PageReference[pReferenceCount];
    mRevision = pIn.readLong();
    for (int offset = 0; offset < mReferences.length; offset++) {
      mReferences[offset] = new PageReference();
      mReferences[offset].setKey(pIn.readLong());
    }
  }

  public void initialize(@Nonnull final IPage pCommittedPage) {
    mReferences = pCommittedPage.getReferences();
  }

  /**
   * Get page reference of given offset.
   * 
   * @param pOffset
   *          offset of page reference
   * @return {@link PageReference} at given offset
   */
  public final PageReference getChildren(@Nonnegative final int pOffset) {
    if (getReferences()[pOffset] == null) {
      getReferences()[pOffset] = new PageReference();
    }
    return getReferences()[pOffset];
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
  public final void commit(@Nonnull final IPageWriteTrx pPageWriteTrx) throws AbsTTException {
    for (final PageReference reference : getReferences()) {
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
    for (final PageReference reference : getReferences()) {
      pOut.writeLong(reference.getKey());
    }
  }

  /**
   * @return the mReferences
   */
  @Override
  public final PageReference[] getReferences() {
    return mReferences;
  }

  /**
   * @return the mRevision
   */
  @Override
  public final long getRevision() {
    return mRevision;
  }

  @Override
  public String toString() {
    final StringBuilder builder = new StringBuilder();
    if (getReferences().length > 0) {
      builder.append("References: ");
      for (final PageReference ref : getReferences()) {
        if (ref != null) {
          builder.append(ref.getKey()).append(",");
        }
      }
    } else {
      builder.append("No references");
    }
    return builder.toString();
  }

}
