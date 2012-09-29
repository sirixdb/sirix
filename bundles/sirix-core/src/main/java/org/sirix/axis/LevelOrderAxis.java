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
package org.sirix.axis;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayDeque;
import java.util.Deque;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import org.sirix.api.INodeReadTrx;
import org.sirix.node.EKind;
import org.sirix.node.ElementNode;
import org.sirix.node.interfaces.IStructNode;

/**
 * Iterates over {@link IStructuralNode}s in a breath first traversal.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public final class LevelOrderAxis extends AbsAxis {

  /** Determines if structural or structural and non structural nodes should be included. */
  public enum EIncludeNodes {
    /** Only structural nodes. */
    STRUCTURAL,

    /** Structural and non-structural nodes. */
    NONSTRUCTURAL
  }

  /** {@link Deque} for remembering next nodeKey in document order. */
  private Deque<Long> mFirstChilds;

  /** Determines if {@code attribute-} and {@code namespace-} nodes should be included or not. */
  private EIncludeNodes mIncludeNodes;

  /** Determines if {@code hasNext()} is called for the first time. */
  private boolean mFirst;

  /** Filter by level. */
  private int mFilterLevel = Integer.MAX_VALUE;
  
  /** Current level. */
  private int mLevel;

  /** Builder. */
  public static class Builder {

    /** Determines if {@code attribute-} and {@code namespace-} nodes should be included or not. */
    private EIncludeNodes mIncludeNodes = EIncludeNodes.STRUCTURAL;

    /** Filter by level. */
    private int mFilterLevel = Integer.MAX_VALUE;

    /** Sirix {@link INodeReadTrx}. */
    private INodeReadTrx mRtx;

    /** Determines if current start node to traversal should be included or not. */
    private EIncludeSelf mIncludeSelf = EIncludeSelf.NO;

    /**
     * Constructor.
     * 
     * @param pRtx
     *          Sirix {@link INodeReadTrx}
     */
    public Builder(final @Nonnull INodeReadTrx pRtx) {
      mRtx = checkNotNull(pRtx);
    }

    /**
     * Determines the types of nodes to include in the traversal (structural or structural and
     * non-structural).
     * 
     * @param pIncludeNodes
     *          type of nodes to include
     * @return this builder instance
     */
    public Builder includeNodes(final @Nonnull EIncludeNodes pIncludeNodes) {
      mIncludeNodes = checkNotNull(pIncludeNodes);
      return this;
    }

    /**
     * Determines the types of nodes to include in the traversal (structural or structural and
     * non-structural).
     * 
     * @param pIncludeSelf
     *          include current node or not
     * @return this builder instance
     */
    public Builder includeSelf(final @Nonnull EIncludeSelf pIncludeSelf) {
      mIncludeSelf = checkNotNull(pIncludeSelf);
      return this;
    }

    /**
     * Determines the maximum level to filter.
     * 
     * @param pFilterLevel
     *          maximum level to filter nodes
     * @return this builder instance
     */
    public Builder filterLevel(final @Nonnegative int pFilterLevel) {
      checkArgument(pFilterLevel >= 0, "pFilterLevel must be >= 0!");
      mFilterLevel = pFilterLevel;
      return this;
    }

    /**
     * Build a new instance.
     * 
     * @return new instance
     */
    public LevelOrderAxis build() {
      return new LevelOrderAxis(this);
    }
  }

  /**
   * Constructor initializing internal state.
   * 
   * @param pRtx
   *          exclusive (immutable) trx to iterate with
   * @param pIncludeNodes
   *          determines if only structural or also non-structural nodes should be included
   * @param pIncludeSelf
   *          determines if self included
   * @param pFilterLevel
   *          filter level
   */
  public LevelOrderAxis(final @Nonnull Builder pBuilder) {
    super(pBuilder.mRtx, pBuilder.mIncludeSelf);
    mIncludeNodes = pBuilder.mIncludeNodes;
    mFilterLevel = pBuilder.mFilterLevel;
  }

  /**
   * Constructor initializing internal state.
   * 
   * @param pRtx
   *          exclusive (immutable) trx to iterate with
   * @param pIncludeNodes
   *          determines if only structural or also non-structural nodes should be included
   * @param pIncludeSelf
   *          determines if self included
   */
  @Deprecated
  public LevelOrderAxis(@Nonnull final INodeReadTrx pRtx,
    @Nonnull final EIncludeNodes pIncludeNodes, final EIncludeSelf pIncludeSelf) {
    super(pRtx, pIncludeSelf);
    mIncludeNodes = checkNotNull(pIncludeNodes);
  }

  @Override
  public void reset(final long pNodeKey) {
    super.reset(pNodeKey);
    mFirst = true;
    mFirstChilds = new ArrayDeque<>();
  }

  @Override
  public boolean hasNext() {
    if (!isHasNext()) {
      return false;
    }
    if (isNext()) {
      return true;
    }
    resetToLastKey();

    final INodeReadTrx rtx = getTransaction();
    // Determines if it's the first call to hasNext().
    if (mFirst) {
      mFirst = false;

      if (rtx.getKind() == EKind.ATTRIBUTE || rtx.getKind() == EKind.NAMESPACE) {
        return false;
      }
      if (isSelfIncluded() == EIncludeSelf.YES) {
        mKey = rtx.getNodeKey();
      } else {
        if (rtx.hasRightSibling()) {
          mKey = rtx.getRightSiblingKey();
        } else if (rtx.hasFirstChild()) {
          mKey = rtx.getFirstChildKey();
        } else {
          resetToStartKey();
          return false;
        }
      }

      return true;
    } else {
      // Follow right sibling if there is one.
      if (rtx.hasRightSibling()) {
        processElement();
        // Add first child to queue.
        if (rtx.hasFirstChild()) {
          mFirstChilds.add(rtx.getFirstChildKey());
        }
        mKey = rtx.getRightSiblingKey();
        return true;
      }

      // Add first child to queue.
      processElement();
      if (rtx.hasFirstChild()) {
        mFirstChilds.add(rtx.getFirstChildKey());
      }

      // Then follow first child on stack.
      if (!mFirstChilds.isEmpty()) {
        mLevel++;
        
        // End traversal if level is reached.
        if (mLevel > mFilterLevel) {
          resetToStartKey();
          return false;
        }
        
        mKey = mFirstChilds.pollFirst();
        return true;
      }

      // Then follow first child if there is one.
      if (getTransaction().hasFirstChild()) {
        mLevel++;
        
        // End traversal if level is reached.
        if (mLevel > mFilterLevel) {
          resetToStartKey();
          return false;
        }
        
        mKey = getTransaction().getFirstChildKey();
        return true;
      }

      // Then end.
      resetToStartKey();
      return false;
    }
  }

  /** Process an element node. */
  private void processElement() {
    final INodeReadTrx rtx = (INodeReadTrx)getTransaction();
    if (rtx.getKind() == EKind.ELEMENT
      && mIncludeNodes == EIncludeNodes.NONSTRUCTURAL) {
      for (int i = 0, nspCount = rtx.getNamespaceCount(); i < nspCount; i++) {
        rtx.moveToNamespace(i);
        mFirstChilds.add(rtx.getNodeKey());
        rtx.moveToParent();
      }
      for (int i = 0, attCount = rtx.getAttributeCount(); i < attCount; i++) {
        rtx.moveToAttribute(i);
        mFirstChilds.add(rtx.getNodeKey());
        rtx.moveToParent();
      }
    }
  }
}
