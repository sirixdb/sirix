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
package org.sirix.axis;

import org.sirix.api.NodeCursor;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.node.NodeKind;

import javax.annotation.Nonnegative;
import java.util.ArrayDeque;
import java.util.Deque;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Iterates over a subtree in levelorder / in a breath first traversal.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 *
 */
public final class LevelOrderAxis extends AbstractAxis {

  /**
   * Determines if structural or structural and non structural nodes should be included.
   */
  private enum IncludeNodes {
    /** Only structural nodes. */
    STRUCTURAL,

    /** Structural and non-structural nodes. */
    NONSTRUCTURAL
  }

  /** {@link Deque} for remembering next nodeKey in document order. */
  private Deque<Long> mFirstChilds;

  /**
   * Determines if {@code attribute-} and {@code namespace-} nodes should be included or not.
   */
  private final IncludeNodes mIncludeNodes;

  /** Determines if {@code hasNext()} is called for the first time. */
  private boolean mFirst;

  /** Filter by level. */
  private int mFilterLevel = Integer.MAX_VALUE;

  /** Current level. */
  private int mLevel;

  /**
   * Get a new builder instance.
   *
   * @param rtx the {@link NodeCursor} to iterate with
   * @return {@link Builder} instance
   */
  public static Builder newBuilder(final NodeCursor rtx) {
    return new Builder(rtx);
  }

  /** Builder. */
  public static class Builder {
    /**
     * Determines if {@code attribute-} and {@code namespace-} nodes should be included or not.
     */
    private IncludeNodes mIncludeNodes = IncludeNodes.STRUCTURAL;

    /** Filter by level. */
    private int mFilterLevel = Integer.MAX_VALUE;

    /** Sirix {@link NodeCursor}. */
    private final NodeCursor mRtx;

    /** Determines if current start node to traversal should be included or not. */
    private IncludeSelf mIncludeSelf = IncludeSelf.NO;

    /**
     * Constructor.
     *
     * @param rtx Sirix {@link NodeCursor}
     */
    public Builder(final NodeCursor rtx) {
      mRtx = checkNotNull(rtx);
    }

    /**
     * Determines that non-structural nodes (attributes, namespaces) should be taken into account.
     *
     * @return this builder instance
     */
    public Builder includeNonStructuralNodes() {
      mIncludeNodes = IncludeNodes.NONSTRUCTURAL;
      return this;
    }

    /**
     * Determines that the current node should also be considered.
     *
     * @return this builder instance
     */
    public Builder includeSelf() {
      mIncludeSelf = IncludeSelf.YES;
      return this;
    }

    /**
     * Determines the maximum level to filter.
     *
     * @param filterLevel maximum level to filter nodes
     * @return this builder instance
     */
    public Builder filterLevel(final @Nonnegative int filterLevel) {
      checkArgument(filterLevel >= 0, "filterLevel must be >= 0!");
      mFilterLevel = filterLevel;
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
   * @param builder the builder reference
   */
  private LevelOrderAxis(final Builder builder) {
    super(builder.mRtx, builder.mIncludeSelf);
    mIncludeNodes = builder.mIncludeNodes;
    mFilterLevel = builder.mFilterLevel;
  }

  @Override
  public void reset(final long pNodeKey) {
    super.reset(pNodeKey);
    mFirst = true;
    mFirstChilds = new ArrayDeque<>();
  }

  @Override
  protected long nextKey() {
    final NodeCursor cursor = getCursor();
    // Determines if it's the first call to hasNext().
    if (mFirst) {
      mFirst = false;

      if (cursor.getKind() == NodeKind.ATTRIBUTE || cursor.getKind() == NodeKind.NAMESPACE) {
        return done();
      }

      if (isSelfIncluded() == IncludeSelf.YES) {
        return cursor.getNodeKey();
      } else {
        if (cursor.hasRightSibling()) {
          return cursor.getRightSiblingKey();
        } else if (cursor.hasFirstChild()) {
          return cursor.getFirstChildKey();
        } else {
          return done();
        }
      }
    }
    // Follow right sibling if there is one.
    if (cursor.hasRightSibling()) {
      processElement();
      // Add first child to queue.
      if (cursor.hasFirstChild()) {
        mFirstChilds.add(cursor.getFirstChildKey());
      }
      return cursor.getRightSiblingKey();
    }

    // Add first child to queue.
    processElement();
    if (cursor.hasFirstChild()) {
      mFirstChilds.add(cursor.getFirstChildKey());
    }

    // Then follow first child on stack.
    if (!mFirstChilds.isEmpty()) {
      mLevel++;

      // End traversal if level is reached.
      if (mLevel > mFilterLevel) {
        return done();
      }

      return mFirstChilds.pollFirst();
    }

    // Then follow first child if there is one.
    if (cursor.hasFirstChild()) {
      mLevel++;

      // End traversal if level is reached.
      if (mLevel > mFilterLevel) {
        return done();
      }

      return cursor.getFirstChildKey();
    }

    return done();
  }

  /**
   * Get the current level.
   *
   * @return the current level
   */
  public int getCurrentLevel() {
    return mLevel;
  }

  /** Process an element node. */
  private void processElement() {
    if (getCursor() instanceof XmlNodeReadOnlyTrx) {
      final XmlNodeReadOnlyTrx rtx = asXdmNodeReadTrx();
      if (rtx.getKind() == NodeKind.ELEMENT && mIncludeNodes == IncludeNodes.NONSTRUCTURAL) {
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
}
