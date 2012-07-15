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

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayDeque;
import java.util.Deque;

import javax.annotation.Nonnull;

import org.sirix.api.INodeReadTrx;
import org.sirix.node.EKind;
import org.sirix.node.ElementNode;
import org.sirix.node.interfaces.IStructNode;

/**
 * Iterates over {@link AbsStructuralNode}s in a breath first traversal.
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

    // First move to next key.
    final INodeReadTrx rtx = (INodeReadTrx)getTransaction();
    final IStructNode node = rtx.getStructuralNode();

    // Determines if it's the first call to hasNext().
    if (mFirst) {
      mFirst = false;

      final EKind kind = node.getKind();
      if (kind == EKind.ATTRIBUTE || kind == EKind.NAMESPACE) {
        return false;
      }
      if (isSelfIncluded() == EIncludeSelf.YES) {
        mKey = node.getNodeKey();
      } else {
        if (node.hasRightSibling()) {
          mKey = node.getRightSiblingKey();
        } else if (node.hasFirstChild()) {
          mKey = node.getFirstChildKey();
        } else {
          resetToStartKey();
          return false;
        }
      }

      return true;
    } else {
      // Follow right sibling if there is one.
      if (node.hasRightSibling()) {
        processElement();
        // Add first child to queue.
        if (node.hasFirstChild()) {
          mFirstChilds.add(node.getFirstChildKey());
        }
        mKey = node.getRightSiblingKey();
        return true;
      }

      // Add first child to queue.
      processElement();
      if (node.hasFirstChild()) {
        mFirstChilds.add(node.getFirstChildKey());
      }

      // Then follow first child on stack.
      if (!mFirstChilds.isEmpty()) {
        mKey = mFirstChilds.pollFirst();
        return true;
      }

      // Then follow first child if there is one.
      if (node.hasFirstChild()) {
        mKey = node.getFirstChildKey();
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
    if (rtx.getStructuralNode().getKind() == EKind.ELEMENT
      && mIncludeNodes == EIncludeNodes.NONSTRUCTURAL) {
      final ElementNode element = (ElementNode)rtx.getNode();
      for (int i = 0, nspCount = element.getNamespaceCount(); i < nspCount; i++) {
        rtx.moveToNamespace(i);
        mFirstChilds.add(rtx.getNode().getNodeKey());
        rtx.moveToParent();
      }
      for (int i = 0, attCount = element.getAttributeCount(); i < attCount; i++) {
        rtx.moveToAttribute(i);
        mFirstChilds.add(rtx.getNode().getNodeKey());
        rtx.moveToParent();
      }
    }
  }
}
