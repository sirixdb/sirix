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

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import org.sirix.node.DocumentRootNode;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.delegates.StructNodeDelegate;
import org.sirix.page.delegates.PageDelegate;
import org.sirix.page.interfaces.IPage;
import org.sirix.settings.EFixed;
import org.sirix.utils.IConstants;

/**
 * <h1>UberPage</h1>
 * 
 * <p>
 * Uber page holds a reference to the static revision root page tree.
 * </p>
 */
public final class UberPage extends AbsForwardingPage {

  /** Offset of indirect page reference. */
  private static final int INDIRECT_REFERENCE_OFFSET = 0;

  /** Number of revisions. */
  private final long mRevisionCount;

  /** {@code true} if this uber page is the uber page of a fresh sirix file, {@code false} otherwise. */
  private boolean mBootstrap;

  /** {@link PageDelegate} reference. */
  private final PageDelegate mDelegate;

  /**
   * Create uber page.
   */
  public UberPage() {
    mDelegate = new PageDelegate(1, IConstants.UBP_ROOT_REVISION_NUMBER);
    mRevisionCount = IConstants.UBP_ROOT_REVISION_COUNT;
    mBootstrap = true;

    // --- Create revision tree
    // ------------------------------------------------

    // Initialize revision tree to guarantee that there is a revision root
    // page.
    IPage page = null;
    PageReference reference = getReferences()[INDIRECT_REFERENCE_OFFSET];

    // Remaining levels.
    for (int i = 0, l = IConstants.INP_LEVEL_PAGE_COUNT_EXPONENT.length; i < l; i++) {
      page = new IndirectPage(IConstants.UBP_ROOT_REVISION_NUMBER);
      reference.setPage(page);
      reference = page.getReferences()[0];
    }

    final RevisionRootPage rrp = new RevisionRootPage();
    reference.setPage(rrp);

    // --- Create node tree
    // ----------------------------------------------------

    // Initialize revision tree to guarantee that there is a revision root
    // page.
    reference = rrp.getIndirectPageReference();
    createTree(reference);
    rrp.incrementMaxNodeKey();

    // Initialize path tree to guarantee that there is a revision root
    // page.
    reference =
      rrp.getPathSummaryPageReference().getPage().getReferences()[INDIRECT_REFERENCE_OFFSET];
    createTree(reference);
    rrp.incrementMaxPathNodeKey();
    
    // Initialize value tree to guarantee that there is a revision root
    // page.
    reference =
      rrp.getValuePageReference().getPage().getReferences()[INDIRECT_REFERENCE_OFFSET];
    createTree(reference);
    rrp.incrementMaxValueNodeKey();
  }

  /**
   * Create the initial tree structure.
   * 
   * @param pReference
   *          reference from revision root
   */
  private void createTree(@Nonnull PageReference pReference) {
    IPage page = null;

    // Remaining levels.
    for (int i = 0, l = IConstants.INP_LEVEL_PAGE_COUNT_EXPONENT.length; i < l; i++) {
      page = new IndirectPage(IConstants.UBP_ROOT_REVISION_NUMBER);
      pReference.setPage(page);
      pReference = page.getReferences()[0];
    }

    final NodePage ndp =
      new NodePage(EFixed.ROOT_PAGE_KEY.getStandardProperty(),
        IConstants.UBP_ROOT_REVISION_NUMBER);
    pReference.setPage(ndp);

    final NodeDelegate nodeDel =
      new NodeDelegate(EFixed.DOCUMENT_NODE_KEY.getStandardProperty(),
        EFixed.NULL_NODE_KEY.getStandardProperty(), EFixed.NULL_NODE_KEY
          .getStandardProperty());
    final StructNodeDelegate strucDel =
      new StructNodeDelegate(nodeDel, EFixed.NULL_NODE_KEY
        .getStandardProperty(), EFixed.NULL_NODE_KEY.getStandardProperty(),
        EFixed.NULL_NODE_KEY.getStandardProperty(), 0, 0);
    ndp.setNode(0, new DocumentRootNode(nodeDel, strucDel));
  }

  /**
   * Read uber page.
   * 
   * @param pIn
   *          input bytes
   */
  protected UberPage(final @Nonnull ByteArrayDataInput pIn) {
    mDelegate = new PageDelegate(1, pIn);
    mRevisionCount = pIn.readLong();
    mBootstrap = false;
  }

  /**
   * Clone uber page.
   * 
   * @param pCommittedUberPage
   *          Page to clone.
   * @param pRevisionToUse
   *          Revision number to use.
   */
  public UberPage(final @Nonnull UberPage pCommittedUberPage,
    @Nonnegative final long pRevisionToUse) {
    mDelegate = new PageDelegate(pCommittedUberPage, pRevisionToUse);
    if (pCommittedUberPage.isBootstrap()) {
      mRevisionCount = pCommittedUberPage.mRevisionCount;
      mBootstrap = pCommittedUberPage.mBootstrap;
    } else {
      mRevisionCount = pCommittedUberPage.mRevisionCount + 1;
      mBootstrap = false;
    }
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
   * Get number of revisions.
   * 
   * @return Number of revisions.
   */
  public long getRevisionCount() {
    return mRevisionCount;
  }

  /**
   * Get key of last committed revision.
   * 
   * @return Key of last committed revision.
   */
  public long getLastCommitedRevisionNumber() {
    return mRevisionCount - 2;
  }

  /**
   * Get revision key of current in-memory state.
   * 
   * @return Revision key.
   */
  public long getRevisionNumber() {
    return mRevisionCount - 1;
  }

  /**
   * Flag to indicate whether this uber page is the first ever.
   * 
   * @return {@code true} if this uber page is the first oINDIRECT_REFERENCE_OFFSETne of sirix, {@code false}
   *         otherwise.
   */
  public boolean isBootstrap() {
    return mBootstrap;
  }

  @Override
  public void serialize(final @Nonnull ByteArrayDataOutput pOut) {
    mBootstrap = false;
    mDelegate.serialize(checkNotNull(pOut));
    pOut.writeLong(mRevisionCount);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("forwarding page", super.toString()).add("revisionCount",
        mRevisionCount).add("indirectPage",
        getReferences()[INDIRECT_REFERENCE_OFFSET]).add("isBootstrap",
        mBootstrap).toString();
  }

  @Override
  protected IPage delegate() {
    return mDelegate;
  }
}
