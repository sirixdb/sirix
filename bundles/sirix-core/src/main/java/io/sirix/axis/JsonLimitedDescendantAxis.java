/*
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

package io.sirix.axis;

import io.sirix.api.NodeCursor;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.settings.Fixed;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;

import static java.util.Objects.requireNonNull;

/**
 * <p>
 * Iterate over all descendants of a JSON subtree with configurable limits on depth (maxLevel) and
 * children per parent (maxChildren). Handles OBJECT_KEY semantics where key and value are at the
 * same level.
 * </p>
 * 
 * <p>
 * This axis is optimized for high performance with O(1) per-node overhead and O(height) space.
 * </p>
 *
 * @author Johannes Lichtenberger
 */
public final class JsonLimitedDescendantAxis extends AbstractAxis {

  /** Maximum depth to traverse (0 = start node only) */
  private final int maxLevel;

  /** Maximum children per parent node */
  private final int maxChildren;

  /** Stack of right sibling keys for backtracking */
  private LongArrayList siblingStack;

  /** Parallel stack of levels when siblings were pushed */
  private IntArrayList levelStack;

  /** Child count at each level (dynamic sizing) */
  private IntArrayList childCount;

  /** Current depth relative to start node */
  private int level;

  /** First call flag */
  private boolean first;

  /** Boundary marker: right sibling of start node */
  private long startNodeRightSiblingKey;

  /**
   * Builder for JsonLimitedDescendantAxis.
   */
  public static final class Builder {
    private final NodeCursor cursor;
    private IncludeSelf includeSelf = IncludeSelf.NO;
    private int maxLevel = Integer.MAX_VALUE;
    private int maxChildren = Integer.MAX_VALUE;

    public Builder(NodeCursor cursor) {
      this.cursor = requireNonNull(cursor);
    }

    public Builder includeSelf() {
      this.includeSelf = IncludeSelf.YES;
      return this;
    }

    public Builder maxLevel(int maxLevel) {
      this.maxLevel = maxLevel;
      return this;
    }

    public Builder maxChildren(int maxChildren) {
      this.maxChildren = maxChildren;
      return this;
    }

    public JsonLimitedDescendantAxis build() {
      return new JsonLimitedDescendantAxis(this);
    }
  }

  public static Builder newBuilder(NodeCursor cursor) {
    return new Builder(cursor);
  }

  private JsonLimitedDescendantAxis(Builder builder) {
    super(builder.cursor, builder.includeSelf);
    this.maxLevel = builder.maxLevel;
    this.maxChildren = builder.maxChildren;
    // Note: siblingStack, levelStack, childCount are initialized in reset()
    // which is called by super() constructor
  }

  @Override
  public void reset(long nodeKey) {
    super.reset(nodeKey);
    first = true;

    // Initialize or clear stacks
    if (siblingStack == null) {
      siblingStack = new LongArrayList();
    } else {
      siblingStack.clear();
    }
    if (levelStack == null) {
      levelStack = new IntArrayList();
    } else {
      levelStack.clear();
    }
    if (childCount == null) {
      childCount = new IntArrayList();
    } else {
      childCount.clear();
    }

    level = 0;

    // Cache boundary marker: start node's right sibling
    final NodeCursor cursor = getCursor();
    final long currentKey = cursor.getNodeKey();
    cursor.moveTo(nodeKey);
    startNodeRightSiblingKey = cursor.getRightSiblingKey();
    cursor.moveTo(currentKey);
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // Child count helpers (dynamic sizing)
  // ═══════════════════════════════════════════════════════════════════════════

  private int getChildCount(int lvl) {
    return lvl < childCount.size()
        ? childCount.getInt(lvl)
        : 0;
  }

  private void setChildCount(int lvl, int value) {
    while (childCount.size() <= lvl) {
      childCount.add(0);
    }
    childCount.set(lvl, value);
  }

  private void incrementChildCount(int lvl) {
    setChildCount(lvl, getChildCount(lvl) + 1);
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // Main traversal logic
  // ═══════════════════════════════════════════════════════════════════════════

  @Override
  protected long nextKey() {
    final NodeCursor cursor = getCursor();

    // Cast to JSON transaction if possible (for isObjectKey check)
    final boolean isJsonTrx = cursor instanceof JsonNodeReadOnlyTrx;
    final JsonNodeReadOnlyTrx jsonRtx = isJsonTrx
        ? (JsonNodeReadOnlyTrx) cursor
        : null;

    // ═══════════════════════════════════════════════════════════════════════
    // CASE 1: First call
    // Note: We start at level 1 (not 0) to match the old visitor semantics.
    // maxLevel=1 means "show only the start node", maxLevel=2 means "start + children", etc.
    // ═══════════════════════════════════════════════════════════════════════
    if (first) {
      first = false;

      if (includeSelf() == IncludeSelf.YES) {
        level = 1; // Start at level 1 (matches old algorithm)
        setChildCount(1, 1);
        return cursor.getNodeKey();
      } else {
        // Skip start node, go to first child
        long firstChildKey = cursor.getFirstChildKey();

        if (firstChildKey == Fixed.NULL_NODE_KEY.getStandardProperty()) {
          return done();
        }

        // Check constraints for level 2 (first child when includeSelf=NO)
        if (2 > maxLevel) {
          return done();
        }
        if (maxChildren < 1) {
          return done();
        }

        level = 2;
        setChildCount(2, 1);
        return firstChildKey;
      }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // CASE 2: Try to descend to first child
    // The level check happens at the CURRENT node, not the child.
    // If current level > maxLevel, we don't descend further (we've already visited this node).
    // ═══════════════════════════════════════════════════════════════════════
    long firstChildKey = cursor.getFirstChildKey();
    if (firstChildKey != Fixed.NULL_NODE_KEY.getStandardProperty()) {

      // Check if current node is an ObjectKey
      boolean isObjectKey = isJsonTrx && jsonRtx.isObjectKey();

      // Level check: if we're PAST maxLevel, don't descend further
      // (ObjectKey always descends to value regardless of level)
      // At level=maxLevel, we can still visit children (they'll be at maxLevel+1)
      // but those children won't be able to descend (they'll be past maxLevel)
      if (!isObjectKey && level > maxLevel) {
        // We're beyond maxLevel - don't visit children
        return trySiblingOrPop(cursor, jsonRtx);
      }

      // Check maxChildren (skip for ObjectKey - value is part of key-value pair)
      if (!isObjectKey && maxChildren < 1) {
        return trySiblingOrPop(cursor, jsonRtx);
      }

      // ObjectKey → Value: NO level increment (same level)
      // Other → Child: level increment
      int nextLevel = isObjectKey
          ? level
          : level + 1;

      // Push right sibling for later backtracking (at CURRENT level)
      long rightSibKey = cursor.getRightSiblingKey();
      if (rightSibKey != Fixed.NULL_NODE_KEY.getStandardProperty()) {
        siblingStack.add(rightSibKey);
        levelStack.add(level);
      }

      // Update level and child count (only if NOT ObjectKey)
      if (!isObjectKey) {
        level = nextLevel;
        setChildCount(level, 1);
      }
      // For ObjectKey: level stays same, childCount not changed
      // (the ObjectKey was already counted when we moved to it)

      return firstChildKey;
    }

    // ═══════════════════════════════════════════════════════════════════════
    // CASE 3 & 4: Try sibling or pop
    // ═══════════════════════════════════════════════════════════════════════
    return trySiblingOrPop(cursor, jsonRtx);
  }

  /**
   * Try to move to right sibling, or pop from stack if no sibling available.
   */
  private long trySiblingOrPop(NodeCursor cursor, JsonNodeReadOnlyTrx jsonRtx) {
    // ═══════════════════════════════════════════════════════════════════════
    // CASE 3: Try right sibling
    // ═══════════════════════════════════════════════════════════════════════
    long rightSibKey = cursor.getRightSiblingKey();
    if (rightSibKey != Fixed.NULL_NODE_KEY.getStandardProperty()) {
      if (getChildCount(level) < maxChildren) {
        incrementChildCount(level);
        return checkBoundary(rightSibKey);
      }
      // maxChildren exceeded - fall through to pop
    }

    // ═══════════════════════════════════════════════════════════════════════
    // CASE 4: Pop from stack
    // ═══════════════════════════════════════════════════════════════════════
    while (!siblingStack.isEmpty()) {
      long sibKey = siblingStack.popLong();
      level = levelStack.popInt();

      if (getChildCount(level) < maxChildren) {
        incrementChildCount(level);
        return checkBoundary(sibKey);
      }
      // maxChildren exceeded at this level - continue popping
    }

    return done();
  }

  /**
   * Boundary check: don't traverse past start node's subtree.
   */
  private long checkBoundary(long key) {
    if (key == startNodeRightSiblingKey) {
      return done();
    }
    return key;
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // Getters for serializer integration
  // ═══════════════════════════════════════════════════════════════════════════

  public int getCurrentLevel() {
    return level;
  }

  public int getMaxLevel() {
    return maxLevel;
  }

  public int getMaxChildren() {
    return maxChildren;
  }

  public int getChildCountAtCurrentLevel() {
    return getChildCount(level);
  }
}
