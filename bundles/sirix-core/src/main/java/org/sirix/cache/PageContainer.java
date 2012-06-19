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

package org.sirix.cache;

import static com.google.common.base.Preconditions.checkNotNull;
import com.google.common.base.Objects;
import com.sleepycat.bind.tuple.TupleOutput;

import javax.annotation.Nonnull;

import org.sirix.io.berkeley.TupleOutputSink;
import org.sirix.page.EPage;
import org.sirix.page.NodePage;
import org.sirix.page.PagePersistenter;
import org.sirix.page.interfaces.IPage;

/**
 * <h1>PageContainer</h1>
 * 
 * <p>
 * This class acts as a container for revisioned {@link IPage}s. Each {@link IPage} is stored in a
 * versioned manner. If modifications occur, the versioned {@link IPage}s are dereferenced and
 * reconstructed. Afterwards, this container is used to store a complete {@link IPage} as well as one for
 * upcoming modifications.
 * </p>
 * 
 * <p>
 * Both {@link IPage}s can differ since the complete one is mainly used for read access and the modifying
 * one for write access (and therefore mostly lazy dereferenced).
 * </p>
 * 
 * @author Sebastian Graf, University of Konstanz
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public final class PageContainer {

  /** {@link NodePage} reference, which references the complete node page. */
  private final IPage mComplete;

  /** {@link NodePage} reference, which references the modified node page. */
  private final IPage mModified;

  /** Empty instance. */
  public static final PageContainer EMPTY_INSTANCE = new PageContainer();

  /** Private constructor for empty instance. */
  private PageContainer() {
    mComplete = null;
    mModified = null;
  }

  /**
   * Constructor with complete page and lazy instantiated modifying page.
   * 
   * @param pComplete
   *          to be used as a base for this container
   */
  public PageContainer(@Nonnull final IPage pComplete) {
    this(checkNotNull(pComplete), EPage.getKind(pComplete.getClass()).getInstance(pComplete));
  }

  /**
   * Constructor with both, complete and modifying page.
   * 
   * @param pComplete
   *          to be used as a base for this container
   * @param pModifying
   *          to be used as a base for this container
   */
  public PageContainer(@Nonnull final IPage pComplete, @Nonnull final IPage pModifying) {
    mComplete = checkNotNull(pComplete);
    mModified = checkNotNull(pModifying);
  }

  /**
   * Getting the complete page.
   * 
   * @return the complete page
   */
  public IPage getComplete() {
    return mComplete;
  }

  /**
   * Getting the modified page.
   * 
   * @return the modified page
   */
  public IPage getModified() {
    return mModified;
  }

  /**
   * Serializing the container to the cache.
   * 
   * @param paramOut
   *          for serialization
   */
  public void serialize(@Nonnull final TupleOutput pOut) {
    final TupleOutputSink sink = new TupleOutputSink(checkNotNull(pOut));
    PagePersistenter.serializePage(sink, mComplete);
    PagePersistenter.serializePage(sink, mModified);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((mComplete == null) ? 0 : mComplete.hashCode());
    result = prime * result + ((mModified == null) ? 0 : mModified.hashCode());
    return result;
  }

  @Override
  public boolean equals(final Object pObj) {
    if (this == pObj) {
      return true;
    }

    if (pObj == null) {
      return false;
    }

    if (getClass() != pObj.getClass()) {
      return false;
    }

    final PageContainer other = (PageContainer)pObj;
    if (mComplete == null && other.mComplete != null) {
      return false;
    } else if (!mComplete.equals(other.mComplete)) {
      return false;
    } else if (!mModified.equals(other.mModified)) {
      return false;
    }

    return true;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("Complete page: ",
      mComplete).add("Modified page: ", mModified).toString();
  }
}
