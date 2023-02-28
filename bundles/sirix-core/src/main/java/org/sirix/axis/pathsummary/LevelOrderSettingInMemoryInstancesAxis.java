/**
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group All rights reserved.
 * <p>
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met: * Redistributions of source code must retain the
 * above copyright notice, this list of conditions and the following disclaimer. * Redistributions
 * in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 * <p>
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.sirix.axis.pathsummary;

import org.checkerframework.checker.index.qual.NonNegative;
import org.sirix.axis.IncludeSelf;
import org.sirix.index.path.summary.PathNode;
import org.sirix.index.path.summary.PathSummaryReader;
import org.sirix.settings.Fixed;

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
public final class LevelOrderSettingInMemoryInstancesAxis extends AbstractAxis {

  private final PathSummaryReader reader;

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
  private Deque<PathNode> firstChildren;

  /** Determines if {@code hasNext()} is called for the first time. */
  private boolean isFirst;

  /** Filter by level. */
  private int filterLevel = Integer.MAX_VALUE;

  /** Current level. */
  private int level;

  /**
   * Get a new builder instance.
   *
   * @param rtx the {@link PathSummaryReader} to iterate with
   * @return {@link Builder} instance
   */
  public static Builder newBuilder(final PathSummaryReader rtx) {
    return new Builder(rtx);
  }

  /** Builder. */
  public static class Builder {
    /** Filter by level. */
    private int filterLevel = Integer.MAX_VALUE;

    /** Sirix {@link PathSummaryReader}. */
    private final PathSummaryReader reader;

    /** Determines if current start node to traversal should be included or not. */
    private IncludeSelf includeSelf = IncludeSelf.NO;

    /**
     * Constructor.
     *
     * @param pathSummaryReader Sirix {@link PathSummaryReader}
     */
    public Builder(final PathSummaryReader pathSummaryReader) {
      this.reader = checkNotNull(pathSummaryReader);
    }

    /**
     * Determines that the current node should also be considered.
     *
     * @return this builder instance
     */
    public Builder includeSelf() {
      includeSelf = IncludeSelf.YES;
      return this;
    }

    /**
     * Determines the maximum level to filter.
     *
     * @param filterLevel maximum level to filter nodes
     * @return this builder instance
     */
    public Builder filterLevel(final @NonNegative int filterLevel) {
      checkArgument(filterLevel >= 0, "filterLevel must be >= 0!");
      this.filterLevel = filterLevel;
      return this;
    }

    /**
     * Build a new instance.
     *
     * @return new instance
     */
    public LevelOrderSettingInMemoryInstancesAxis build() {
      return new LevelOrderSettingInMemoryInstancesAxis(this);
    }
  }

  /**
   * Constructor initializing internal state.
   *
   * @param builder the builder reference
   */
  private LevelOrderSettingInMemoryInstancesAxis(final Builder builder) {
    super(builder.reader.getPathNode(), builder.includeSelf);
    filterLevel = builder.filterLevel;
    reader = builder.reader;
  }

  @Override
  public void reset(final PathNode pathNode) {
    super.reset(pathNode);
    level = 0;
    isFirst = true;
    firstChildren = new ArrayDeque<>();
    if (reader != null) {
      reader.moveTo(startPathNode.getNodeKey());
    }
  }

  @Override
  protected PathNode nextNode() {
    // Determines if it's the first call to hasNext().
    final long nodeKey = nextNode.getNodeKey();
    reader.moveTo(nodeKey);

    if (isFirst) {
      isFirst = false;

      if (includeSelf() == IncludeSelf.YES) {
        if (nextNode.getParent() == null && reader.hasParent()
            && reader.getParentKey() != Fixed.DOCUMENT_NODE_KEY.getStandardProperty()) {
          reader.moveToParent();
          final PathNode parentNode = reader.getPathNode();
          nextNode.setParent(parentNode);
          reader.moveTo(nodeKey);
        }
        return nextNode;
      } else {
        if (nextNode.hasRightSibling()) {
          return getRightSibling(nodeKey);
        } else if (nextNode.hasFirstChild()) {
          return getFirstChild(nodeKey);
        } else {
          return done();
        }
      }
    }
    // Follow right sibling if there is one.
    if (nextNode.hasRightSibling()) {
      // Add first child to queue.
      if (nextNode.hasFirstChild()) {
        firstChildren.add(getFirstChild(nodeKey));
      }

      return getRightSibling(nodeKey);
    }

    // Add first child to queue.
    if (nextNode.hasFirstChild()) {
      firstChildren.add(getFirstChild(nodeKey));
    }

    // Then follow first child on stack.
    if (!firstChildren.isEmpty()) {
      level++;

      // End traversal if level is reached.
      if (level > filterLevel) {
        return done();
      }

      final var pathNode = firstChildren.pop();
      reader.moveTo(pathNode.getNodeKey());
      return pathNode;
    }

    // Then follow first child if there is one.
    if (nextNode.hasFirstChild()) {
      level++;

      // End traversal if level is reached.
      if (level > filterLevel) {
        return done();
      }

      return getFirstChild(nodeKey);
    }

    return done();
  }

  private PathNode getFirstChild(long nodeKey) {
    PathNode firstChild = nextNode.getFirstChild();
    if (firstChild == null) {
      reader.moveToFirstChild();
      firstChild = reader.getPathNode();
      nextNode.setFirstChild(firstChild);
      reader.moveTo(nodeKey);
    }
    if (firstChild.getParent() == null) {
      firstChild.setParent(nextNode);
    }
    //reader.moveTo(firstChild.getNodeKey());
    return firstChild;
  }

  private PathNode getRightSibling(long nodeKey) {
    PathNode rightSibling = nextNode.getRightSibling();
    if (rightSibling == null) {
      reader.moveToRightSibling();
      rightSibling = reader.getPathNode();
      nextNode.setRightSibling(rightSibling);
      reader.moveTo(nodeKey);
    }
    if (rightSibling.getLeftSibling() == null) {
      var parentNode = nextNode.getParent();
      rightSibling.setParent(parentNode);
      rightSibling.setLeftSibling(nextNode);
    }
    //reader.moveTo(rightSibling.getNodeKey());
    return rightSibling;
  }

  /**
   * Get the current level.
   *
   * @return the current level
   */
  public int getCurrentLevel() {
    return level;
  }
}
