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
package org.sirix.diff.algorithm.fmse;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import org.sirix.api.Axis;
import org.sirix.api.NodeReadTrx;
import org.sirix.axis.DescendantAxis;
import org.sirix.axis.IncludeSelf;
import org.sirix.node.Kind;

/**
 * Keeps track of nodes in a matching.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public final class Matching {

  /** Forward matching. */
  private final Map<Long, Long> mMapping;

  /** Backward machting. */
  private final Map<Long, Long> mReverseMapping;

  /**
   * Tracks the (grand-)parent-child relation of nodes. We use this to speed
   * up the calculation of the number of nodes in the subtree of two nodes
   * that are in the matching.
   */
  private final ConnectionMap<Long> mIsInSubtree;

  /** {@link NodeReadTrx} reference on old revision. */
  private final NodeReadTrx mRtxOld;

  /** {@link NodeReadTrx} reference on new revision. */
  private final NodeReadTrx mRtxNew;

  /**
   * Creates a new matching.
   * 
   * @param pRtxOld
   *          {@link NodeReadTrx} reference on old revision
   * @param pRtxNew
   *          {@link NodeReadTrx} reference on new revision.
   */
  public Matching(final NodeReadTrx pRtxOld, final NodeReadTrx pRtxNew) {
    mMapping = new HashMap<>();
    mReverseMapping = new HashMap<>();
    mIsInSubtree = new ConnectionMap<>();
    mRtxOld = checkNotNull(pRtxOld);
    mRtxNew = checkNotNull(pRtxNew);
  }

  /**
   * Copy constructor. Creates a new matching with the same state as the
   * matching pMatch.
   * 
   * @param pMatch
   *          the original {@link Matching} reference
   */
  public Matching(final Matching pMatch) {
    mMapping = new HashMap<>(pMatch.mMapping);
    mReverseMapping = new HashMap<>(pMatch.mReverseMapping);
    mIsInSubtree = new ConnectionMap<>(pMatch.mIsInSubtree);
    mRtxOld = pMatch.mRtxOld;
    mRtxNew = pMatch.mRtxNew;
  }

  /**
   * Adds the matching x -> y.
   * 
   * @param pNodeX
   *          source node (in old revision)
   * @param pNodeY
   *          partner of pNodeX (in new revision)
   */
  public void add(@Nonnegative final long pNodeX, @Nonnegative final long pNodeY) {
    mRtxOld.moveTo(pNodeX);
    mRtxNew.moveTo(pNodeY);
    if (mRtxOld.getKind() != mRtxNew.getKind()) {
      throw new AssertionError();
    }
    mMapping.put(pNodeX, pNodeY);
    mReverseMapping.put(pNodeY, pNodeX);
    updateSubtreeMap(pNodeX, mRtxOld);
    updateSubtreeMap(pNodeY, mRtxNew);
  }

  /**
   * Remove matching.
   * 
   * @param pNodeX
   *          source node for which to remove the connection
   */
  public boolean remove(@Nonnegative final long pNodeX) {
    mReverseMapping.remove(mMapping.get(pNodeX));
    return mMapping.remove(pNodeX) == null ? false : true;
  }

  /**
   * For each anchestor of n: n is in it's subtree.
   * 
   * @param pKey
   *          key of node in subtree
   * @param pRtx
   *          {@link NodeReadTrx} reference
   */
  private void updateSubtreeMap(@Nonnegative final long pKey, @Nonnull final NodeReadTrx pRtx) {
    assert pKey >= 0;
    assert pRtx != null;

    mIsInSubtree.set(pKey, pKey, true);
    pRtx.moveTo(pKey);
    if (pRtx.hasParent()) {
      while (pRtx.hasParent()) {
        pRtx.moveToParent();
        mIsInSubtree.set(pRtx.getNodeKey(), pKey, true);
      }
      pRtx.moveTo(pKey);
    }
  }

  /**
   * Checks if the matching contains the pair (x, y).
   * 
   * @param pNodeX
   *          source node
   * @param pNodeY
   *          partner of x
   * @return true iff add(x, y) was invoked first
   */
  public boolean contains(@Nonnegative final long pNodeX, @Nonnegative final long pNodeY) {
    return mMapping.get(pNodeX) == null ? false : mMapping.get(pNodeX).equals(pNodeY);
  }

  /**
   * Counts the number of descendant nodes in the subtrees of x and y that are also
   * in the matching.
   * 
   * @param pNodeX
   *          first subtree root node
   * @param pNodeY
   *          second subtree root node
   * @return number of descendant which have been matched
   */
  public long containedDescendants(@Nonnegative final long pNodeX, @Nonnegative final long pNodeY) {
    long retVal = 0;

    mRtxOld.moveTo(pNodeX);
    for (final Axis axis = new DescendantAxis(mRtxOld, IncludeSelf.YES); axis.hasNext();) {
      axis.next();
      retVal += mIsInSubtree.get(pNodeY, partner(mRtxOld.getNodeKey())) ? 1 : 0;
      if (mRtxOld.getKind() == Kind.ELEMENT) {
        for (int i = 0, nspCount = mRtxOld.getNamespaceCount(); i < nspCount; i++) {
          mRtxOld.moveToNamespace(i);
          retVal +=
            mIsInSubtree.get(pNodeY, partner(axis.getTrx().getNodeKey())) ? 1 : 0;
          mRtxOld.moveToParent();
        }
        for (int i = 0, attCount = mRtxOld.getAttributeCount(); i < attCount; i++) {
          mRtxOld.moveToAttribute(i);
          retVal +=
            mIsInSubtree.get(pNodeY, partner(axis.getTrx().getNodeKey())) ? 1 : 0;
          mRtxOld.moveToParent();
        }
      }
    }

    return retVal;
  }

  /**
   * Returns the partner node of {@code pNode} according to mapping.
   * 
   * @param pNode
   *          node for which a partner has to be found
   * @return the {@code nodeKey} of the other node or {@code null}
   */
  public Long partner(@Nonnegative final long pNode) {
    return mMapping.get(pNode);
  }

  /**
   * Returns the node for which "node" is the partner (normally used for retrieving partners of nodes in new
   * revision).
   * 
   * @param pNode
   *          node for which a reverse partner has to be found
   * @return x iff add(x, node) was called before
   */
  public Long reversePartner(@Nonnegative final long pNode) {
    return mReverseMapping.get(pNode);
  }

  /** Reset internal datastructures. */
  public void reset() {
    mMapping.clear();
    mReverseMapping.clear();
    mIsInSubtree.reset();
  }
}
