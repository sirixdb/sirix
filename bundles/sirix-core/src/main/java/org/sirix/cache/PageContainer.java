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
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import com.sleepycat.bind.tuple.TupleOutput;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.sirix.page.NodePage;
import org.sirix.page.PagePersistenter;

/**
 * <h1>PageContainer</h1>
 * 
 * <p>
 * This class acts as a container for revisioned {@link NodePage}s. Each {@link NodePage} is stored in a
 * versioned manner. If modifications occur, the versioned {@link NodePage}s are dereferenced and
 * reconstructed. Afterwards, this container is used to store a complete {@link NodePage} as well as one for
 * upcoming modifications.
 * </p>
 * 
 * <p>
 * Both {@link NodePage}s can differ since the complete one is mainly used for read access and the modifying one
 * for write access (and therefore mostly lazy dereferenced).
 * </p>
 * 
 * @author Sebastian Graf, University of Konstanz
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public final class PageContainer {

  /** {@link NodePage} reference, which references the complete node page. */
  private final NodePage mComplete;

  /** {@link NodePage} reference, which references the modified node page. */
  private final NodePage mModified;

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
  public PageContainer(@Nonnull final NodePage pComplete) {
    this(pComplete, new NodePage(pComplete.getNodePageKey(), pComplete.getRevision()));
  }

  /**
   * Constructor with both, complete and modifying page.
   * 
   * @param pComplete
   *          to be used as a base for this container
   * @param pModifying
   *          to be used as a base for this container
   */
  public PageContainer(@Nonnull final NodePage pComplete, @Nonnull final NodePage pModifying) {
    mComplete = checkNotNull(pComplete);
    mModified = checkNotNull(pModifying);
  }

  /**
   * Getting the complete page.
   * 
   * @return the complete page
   */
  public NodePage getComplete() {
    return mComplete;
  }

  /**
   * Getting the modified page.
   * 
   * @return the modified page
   */
  public NodePage getModified() {
    return mModified;
  }

  /**
   * Serializing the container to the cache.
   * 
   * @param pOut
   *          for serialization
   */
  public void serialize(@Nonnull final TupleOutput pOut) {
    final ByteArrayDataOutput sink = ByteStreams.newDataOutput();
    PagePersistenter.serializePage(sink, mComplete);
    PagePersistenter.serializePage(sink, mModified);
    pOut.write(sink.toByteArray());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mComplete, mModified);
  }

  @Override
  public boolean equals(@Nullable final Object pObj) {
    if (pObj instanceof PageContainer) {
      final PageContainer other = (PageContainer) pObj;
      return Objects.equal(mComplete, other.mComplete) && Objects.equal(mModified, other.mModified);
    }
    return false;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("complete page",
      mComplete).add("modified page", mModified).toString();
  }
}
