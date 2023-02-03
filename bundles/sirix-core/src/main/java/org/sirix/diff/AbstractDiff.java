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

package org.sirix.diff;

import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.sirix.access.trx.node.HashType;
import org.sirix.api.Axis;
import org.sirix.api.NodeCursor;
import org.sirix.api.NodeReadOnlyTrx;
import org.sirix.api.NodeTrx;
import org.sirix.axis.DescendantAxis;
import org.sirix.axis.IncludeSelf;
import org.sirix.diff.DiffFactory.Builder;
import org.sirix.diff.DiffFactory.DiffOptimized;
import org.sirix.diff.DiffFactory.DiffType;
import org.sirix.exception.SirixException;
import org.sirix.node.NodeKind;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Abstract diff class which implements common functionality.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 */
@NonNull
abstract class AbstractDiff<R extends NodeReadOnlyTrx & NodeCursor, W extends NodeTrx & NodeCursor>
    extends AbstractDiffObservable {

  /**
   * The old maximum depth.
   */
  private final long oldMaxDepth;

  /**
   * Determines transaction movement.
   */
  private enum Move {
    /**
     * To the following node (next node in the following-axis).
     */
    FOLLOWING,

    /**
     * Next node in document order.
     */
    DOCUMENT_ORDER
  }

  /**
   * Determines the current revision.
   */
  private enum Revision {
    /**
     * Old revision.
     */
    OLD,

    /**
     * New revision.
     */
    NEW
  }

  /**
   * Kind of hash method.
   *
   * @see HashType
   */
  private final HashType hashKind;

  /**
   * Kind of difference.
   *
   * @see DiffType
   */
  private DiffType diff;

  /**
   * Diff kind.
   */
  private DiffOptimized diffKind;

  /**
   * {@link DepthCounter} instance.
   */
  private final DepthCounter depth;

  /**
   * Key of "root" node in new revision.
   */
  private long rootKey;

  /**
   * Root key of old revision.
   */
  private final long oldRootKey;

  /**
   * Determines if the read only transaction on the newer revision moved to the node denoted by
   * {@code mNewStartKey}.
   */
  private final boolean newRtxMoved;

  /**
   * Determines if the read only transaction on the older revision moved to the node denoted by
   * {@code mOldStartKey}.
   */
  private final boolean oldRtxMoved;

  /**
   * Determines if the GUI uses the algorithm or not.
   */
  private final boolean isGUI;

  /**
   * Determines if it's the first diff-comparison.
   */
  private boolean isFirst;

  /**
   * Read only transaction on new revision.
   */
  private final R newRtx;

  /**
   * Read only transaction on old revision.
   */
  private final R oldRtx;

  /**
   * Determines if subtrees should be skipped or not.
   */
  private final boolean skipSubtrees;

  /**
   * Constructor.
   *
   * @param builder {@link Builder} reference
   * @throws SirixException if setting up transactions failes
   */
  AbstractDiff(final Builder<R, W> builder) throws SirixException {
    skipSubtrees = builder.skipSubtrees;
    diffKind = checkNotNull(builder).kind;
    oldMaxDepth = builder.oldMaxDepth;
    synchronized (builder.resMgr) {
      newRtx = builder.resMgr.beginNodeReadOnlyTrx(builder.newRev);
      oldRtx = builder.resMgr.beginNodeReadOnlyTrx(builder.oldRev);
      hashKind = builder.hashKind;
    }
    newRtxMoved = newRtx.moveTo(builder.newStartKey);
    oldRtxMoved = oldRtx.moveTo(builder.oldStartKey);
    if (newRtx.getKind() == documentNode()) {
      newRtx.moveToFirstChild();
    }
    if (oldRtx.getKind() == documentNode()) {
      oldRtx.moveToFirstChild();
    }
    rootKey = builder.newStartKey;
    oldRootKey = builder.oldStartKey;

    synchronized (builder.observers) {
      for (final DiffObserver observer : builder.observers) {
        addObserver(observer);
      }
    }

    diff = DiffType.SAME;
    diffKind = builder.kind;
    depth = new DepthCounter(builder.newDepth, builder.oldDepth);
    isGUI = builder.isGUI;
    isFirst = true;
  }

  /**
   * Do the diff.
   */
  void diffMovement() {
    assert hashKind != null;
    assert newRtx != null;
    assert oldRtx != null;
    assert diff != null;
    assert diffKind != null;

    if (!newRtxMoved) {
      fireDeletes();
      if (!isGUI || depth.getNewDepth() == 0) {
        fireInserts();
      }
      done();
      return;
    }
    if (!oldRtxMoved) {
      fireInserts();
      if (!isGUI || depth.getOldDepth() == 0) {
        fireDeletes();
      }
      done();
      return;
    }

    // Check first node.
    if (hashKind == HashType.NONE || diffKind == DiffOptimized.NO) {
      diff = diff(newRtx, oldRtx, depth);
    } else {
      diff = optimizedDiff(newRtx, oldRtx, depth);
    }

    isFirst = false;

    // Iterate over new revision (order of operators significant -- regarding
    // the OR).
    if (diff != DiffType.SAMEHASH) {
      while ((oldRtx.getKind() != documentNode() && diff == DiffType.DELETED) || moveCursor(newRtx, Revision.NEW,
                                                                                            Move.FOLLOWING)) {
        if (diff != DiffType.INSERTED) {
          moveCursor(oldRtx, Revision.OLD, Move.FOLLOWING);
        }

        if (newRtx.getKind() != documentNode() || oldRtx.getKind() != documentNode()) {
          if (hashKind == HashType.NONE || diffKind == DiffOptimized.NO) {
            diff = diff(newRtx, oldRtx, depth);
          } else {
            diff = optimizedDiff(newRtx, oldRtx, depth);
          }
        }
      }

      // Nodes deleted in old rev at the end of the tree.
      if (oldRtx.getKind() != documentNode()) {
        rootKey = oldRootKey;
        // First time it might be DiffType.INSERTED where the cursor doesn't move.
        if (diff == DiffType.INSERTED) {
          emitDeleteDiff();
        }
        boolean moved = true;
        if (diffKind == DiffOptimized.HASHED && diff == DiffType.SAMEHASH) {
          moved = moveToFollowingNode(oldRtx, Revision.OLD);
          if (moved) {
            emitDeleteDiff();
          }
        }
        if (moved) {
          if (skipSubtrees) {
            while (moveToFollowingNode(oldRtx, Revision.OLD)) {
              emitDeleteDiff();
            }
          } else {
            while (moveCursor(oldRtx, Revision.OLD, Move.DOCUMENT_ORDER)) {
              emitDeleteDiff();
            }
          }
        }
      }
    }

    diffDone();
  }

  private void emitDeleteDiff() {
    diff = DiffType.DELETED;
    final DiffDepth diffDepth = new DiffDepth(this.depth.getNewDepth(), this.depth.getOldDepth());
    fireDiff(diff, newRtx.getNodeKey(), oldRtx.getNodeKey(), diffDepth);
    emitNonStructuralDiff(newRtx, oldRtx, diffDepth, diff);
  }

  /**
   * Done processing diffs. Fire remaining diffs and signal that the algorithm is done.
   *
   * @throws SirixException if sirix fails to close the transactions
   */
  private void diffDone() throws SirixException {
    newRtx.close();
    oldRtx.close();
    done();
  }

  /**
   * Fire {@code EDiff.DELETEs} for the whole subtree.
   */
  private void fireDeletes() {
    fireDiff(DiffType.DELETED, newRtx.getNodeKey(), oldRtx.getNodeKey(),
             new DiffDepth(depth.getNewDepth(), depth.getOldDepth()));

    isFirst = false;

    if (skipSubtrees) {
      moveToFollowingNode(oldRtx, Revision.OLD);
    } else {
      while (moveCursor(oldRtx, Revision.OLD, Move.DOCUMENT_ORDER)) {
        final DiffDepth depth = new DiffDepth(this.depth.getNewDepth(), this.depth.getOldDepth());
        fireDiff(DiffType.DELETED, newRtx.getNodeKey(), oldRtx.getNodeKey(), depth);
        emitNonStructuralDiff(newRtx, oldRtx, depth, DiffType.DELETED);
      }
    }
  }

  /**
   * Fire {@code Diff.INSERT}s for the whole subtree.
   */
  private void fireInserts() {
    fireDiff(DiffType.INSERTED, newRtx.getNodeKey(), oldRtx.getNodeKey(),
             new DiffDepth(depth.getNewDepth(), depth.getOldDepth()));

    isFirst = false;

    if (skipSubtrees) {
      moveToFollowingNode(newRtx, Revision.NEW);
    } else {
      while (moveCursor(newRtx, Revision.NEW, Move.DOCUMENT_ORDER)) {
        final DiffDepth depth = new DiffDepth(this.depth.getNewDepth(), this.depth.getOldDepth());
        fireDiff(DiffType.INSERTED, newRtx.getNodeKey(), oldRtx.getNodeKey(), depth);
        emitNonStructuralDiff(newRtx, oldRtx, depth, DiffType.DELETED);
      }
    }
  }

  /**
   * Move cursor one node forward in pre order.
   *
   * @param rtx      the transactional cursor to use
   * @param revision the {@link Revision} constant
   * @return {@code true}, if cursor moved, {@code false} otherwise, if no nodes follow in document
   * order
   */
  private boolean moveCursor(final R rtx, final Revision revision, final Move move) {
    assert rtx != null;
    assert revision != null;

    boolean moved = false;

    if (rtx.getKind() != documentNode()) {
      switch (diff) {
        case SAME, SAMEHASH, UPDATED -> moved = moveToNext(rtx, revision);
        case REPLACED -> moved = moveToFollowingNode(rtx, revision);
        case INSERTED, DELETED -> {
          if (move == Move.FOLLOWING) {
            moved = rtx.getKind() != documentNode();
          } else {
            moved = moveToNext(rtx, revision);
          }
        }
        case MOVEDFROM, MOVEDTO, REPLACEDNEW, REPLACEDOLD, default -> {
        }
      }
    }
    return moved;
  }

  private boolean moveToNext(final R rtx, final Revision revision) {
    boolean moved = false;
    if (rtx.hasFirstChild()) {
      if (rtx.getKind() != documentNode() && ((diffKind == DiffOptimized.HASHED && diff == DiffType.SAMEHASH) || (
          oldMaxDepth > 0 && rtx.getKind() != NodeKind.OBJECT_KEY && depth.getOldDepth() + 1 >= oldMaxDepth))) {
        moved = rtx.moveToRightSibling();

        if (!moved) {
          moved = moveToFollowingNode(rtx, revision);
        }
      } else {
        final NodeKind nodeKind = rtx.getKind();
        moved = rtx.moveToFirstChild();

        if (moved && nodeKind != NodeKind.OBJECT_KEY) {
          switch (revision) {
            case NEW -> depth.incrementNewDepth();
            case OLD -> depth.incrementOldDepth();
            default -> {
              // Must not happen.
            }
          }
        }
      }
    } else if (rtx.hasRightSibling()) {
      if (rtx.getNodeKey() == rootKey) {
        rtx.moveToDocumentRoot();
      } else {
        moved = rtx.moveToRightSibling();
      }
    } else {
      moved = moveToFollowingNode(rtx, revision);
    }
    return moved;
  }

  /**
   * Move to next following node.
   *
   * @param rtx      the transactional cursor to use
   * @param revision the {@link Revision} constant
   * @return true, if cursor moved, false otherwise
   */
  private boolean moveToFollowingNode(final R rtx, final Revision revision) {
    boolean moved;
    while (!rtx.hasRightSibling() && rtx.hasParent() && rtx.getNodeKey() != rootKey) {
      moved = rtx.moveToParent();
      if (moved && rtx.getKind() != NodeKind.OBJECT_KEY) {
        switch (revision) {
          case NEW -> depth.decrementNewDepth();
          case OLD -> depth.decrementOldDepth();
          default -> {
            // Must not happen.
          }
        }
      }
    }

    if (rtx.getNodeKey() == rootKey) {
      rtx.moveToDocumentRoot();
    }

    moved = rtx.moveToRightSibling();
    return moved;
  }

  /**
   * Diff of nodes.
   *
   * @param newRtx read-only transaction on new revision
   * @param oldRtx read-only transaction on old revision
   * @param depth  {@link DepthCounter} container for current depths of both transaction cursors
   * @return kind of difference
   */
  DiffType diff(final R newRtx, final R oldRtx, final DepthCounter depth) {
    assert newRtx != null;
    assert oldRtx != null;
    assert depth != null;

    DiffType diff = DiffType.SAME;

    // Check for modifications.
    if (checkNodes(newRtx, oldRtx)) {
      final DiffDepth diffDepth = new DiffDepth(depth.getNewDepth(), depth.getOldDepth());
      fireDiff(diff, newRtx.getNodeKey(), oldRtx.getNodeKey(), diffDepth);
      emitNonStructuralDiff(newRtx, oldRtx, diffDepth, diff);
    } else {
      diff = diffAlgorithm(newRtx, oldRtx, depth);
    }

    return diff;
  }

  /**
   * Optimized diff, which skips unnecessary comparsions.
   *
   * @param newRtx read-only transaction on new revision
   * @param oldRtx read-only transaction on old revision
   * @param depth  {@link DepthCounter} container for current depths of both transaction cursors
   * @return kind of difference
   */
  DiffType optimizedDiff(final R newRtx, final R oldRtx, final DepthCounter depth) {
    assert newRtx != null;
    assert oldRtx != null;
    assert depth != null;

    DiffType diff = DiffType.SAMEHASH;

    // Check for modifications.
    if (newRtx.getNodeKey() != oldRtx.getNodeKey() || newRtx.getHash() != oldRtx.getHash()) {
      // Check if nodes are the same (even if subtrees may vary).
      if (checkNodes(newRtx, oldRtx)) {
        diff = DiffType.SAME;
        final DiffDepth diffDepth = new DiffDepth(depth.getNewDepth(), depth.getOldDepth());
        fireDiff(diff, newRtx.getNodeKey(), oldRtx.getNodeKey(), diffDepth);
        emitNonStructuralDiff(newRtx, oldRtx, diffDepth, diff);
      } else {
        diff = diffAlgorithm(newRtx, oldRtx, depth);
      }
    } else {
      final DiffDepth diffDepth = new DiffDepth(depth.getNewDepth(), depth.getOldDepth());
      fireDiff(diff, newRtx.getNodeKey(), oldRtx.getNodeKey(), diffDepth);
      emitNonStructuralDiff(newRtx, oldRtx, diffDepth, diff);
    }

    return diff;
  }

  /**
   * Main algorithm to compute diffs between two nodes.
   *
   * @param newRtx read-only transaction on new revision
   * @param oldRtx read-only transaction on old revision
   * @param depth  {@link DepthCounter} container for current depths of both transaction cursors
   * @return kind of diff
   */
  private DiffType diffAlgorithm(final R newRtx, final R oldRtx, final DepthCounter depth) {
    assert newRtx != null;
    assert oldRtx != null;
    assert depth != null;
    DiffType diff;

    if (depth.getOldDepth() > depth.getNewDepth()) { // Check if node has been deleted.
      diff = DiffType.DELETED;
      emitDiffs(diff);
    } else if (checkUpdate(newRtx, oldRtx)) { // Check if node has been updated.
      diff = DiffType.UPDATED;
      final DiffDepth diffDepth = new DiffDepth(depth.getNewDepth(), depth.getOldDepth());
      if (checkNodeNamesOrValues(newRtx, oldRtx)) {
        fireDiff(DiffType.SAME, newRtx.getNodeKey(), oldRtx.getNodeKey(), diffDepth);
      } else {
        fireDiff(diff, newRtx.getNodeKey(), oldRtx.getNodeKey(), diffDepth);
      }
      emitNonStructuralDiff(newRtx, oldRtx, diffDepth, diff);
    } else if (checkReplace(newRtx, oldRtx)) { // Check if node has been replaced.
      diff = DiffType.REPLACED;
    } else {
      final long oldKey = oldRtx.getNodeKey();
      final boolean movedOld = oldRtx.moveTo(newRtx.getNodeKey());
      oldRtx.moveTo(oldKey);

      final long newKey = newRtx.getNodeKey();
      final boolean movedNew = newRtx.moveTo(oldRtx.getNodeKey());
      newRtx.moveTo(newKey);

      if (!movedOld) {
        diff = DiffType.INSERTED;
      } else if (!movedNew) {
        diff = DiffType.DELETED;
      } else {
        // Determine if one of the right sibling matches.
        FoundMatchingNode found = FoundMatchingNode.FALSE;

        while (oldRtx.hasRightSibling() && oldRtx.moveToRightSibling() && found == FoundMatchingNode.FALSE) {
          if (checkNodeNamesOrValuesAndNodeKeys(newRtx, oldRtx)) {
            found = FoundMatchingNode.TRUE;
          }
        }

        oldRtx.moveTo(oldKey);
        diff = found.kindOfDiff();
      }

      this.diff = diff;
      emitDiffs(diff);
    }

    assert diff != null;
    return diff;
  }

  /**
   * Emit diffs for {@code INSERTED} or {@code DELETED} nodes and traverse accordingly.
   *
   * @param diff kind of diff
   */
  private void emitDiffs(final DiffType diff) {
    final Revision revision = diff == DiffType.DELETED ? Revision.OLD : Revision.NEW;
    final int depth = diff == DiffType.DELETED ? this.depth.getOldDepth() : this.depth.getNewDepth();
    final R rtx = diff == DiffType.DELETED ? oldRtx : newRtx;

    if (skipSubtrees) {
      final DiffDepth diffDepth = new DiffDepth(this.depth.getNewDepth(), this.depth.getOldDepth());
      fireDiff(diff, newRtx.getNodeKey(), oldRtx.getNodeKey(), diffDepth);
      emitNonStructuralDiff(newRtx, oldRtx, diffDepth, diff);
      moveToFollowingNode(rtx, revision);
    } else {
      do {
        final DiffDepth diffDepth = new DiffDepth(this.depth.getNewDepth(), this.depth.getOldDepth());
        fireDiff(diff, newRtx.getNodeKey(), oldRtx.getNodeKey(), diffDepth);
        emitNonStructuralDiff(newRtx, oldRtx, diffDepth, diff);
      } while (moveCursor(rtx, revision, Move.DOCUMENT_ORDER) && (
          (diff == DiffType.INSERTED && this.depth.getNewDepth() > depth) || (diff == DiffType.DELETED
              && this.depth.getOldDepth() > depth)));
    }
  }

  /**
   * Check if nodes are equal excluding subtrees, including namespaces and attributes if full diffing
   * is done.
   *
   * @param newRtx transactional cursor on new revision
   * @param oldRtx transactional cursor on old revision
   * @return true if nodes are "equal", otherwise false
   */
  abstract boolean checkNodes(final R newRtx, final R oldRtx);

  /**
   * Check if nodes are equal excluding subtrees, excluding namespaces and attributes if full diffing
   * is done.
   *
   * @param newRtx transactional cursor on new revision
   * @param oldRtx transactional cursor on old revision
   * @return true if nodes are "equal", otherwise false
   */
  private boolean checkNodeNamesOrValuesAndNodeKeys(R newRtx, R oldRtx) {
    return newRtx.getNodeKey() == oldRtx.getNodeKey() && checkNodeNamesOrValues(newRtx, oldRtx);
  }

  /**
   * Emit non structural diffs, that is for XML attribute- and namespace- nodes.
   *
   * @param newRtx transactional cursor on new revision
   * @param oldRtx transactional cursor on old revision
   * @param depth  the current depth
   * @param diff   the diff type
   */
  abstract void emitNonStructuralDiff(final R newRtx, final R oldRtx, final DiffDepth depth, final DiffType diff);

  /**
   * Check for a replace of a node.
   *
   * @param newRtx read-only transaction on new revision
   * @param oldRtx read-only transaction on old revision
   * @return {@code true}, if node has been replaced, {@code false} otherwise
   */
  boolean checkReplace(final R newRtx, final R oldRtx) {
    boolean replaced = false;
    if (newRtx.getNodeKey() != oldRtx.getNodeKey()) {
      final long newKey = newRtx.getNodeKey();
      boolean movedNewRtx = newRtx.moveToRightSibling();
      final long oldKey = oldRtx.getNodeKey();
      boolean movedOldRtx = oldRtx.moveToRightSibling();
      if (movedNewRtx && movedOldRtx && newRtx.getNodeKey() == oldRtx.getNodeKey()) {
        replaced = true;
      } else if (!movedNewRtx && !movedOldRtx && (diff == DiffType.SAME || diff == DiffType.SAMEHASH)) {
        movedNewRtx = newRtx.moveToParent();
        movedOldRtx = oldRtx.moveToParent();

        if (movedNewRtx && movedOldRtx && newRtx.getNodeKey() == oldRtx.getNodeKey()) {
          replaced = true;
        }
      }
      newRtx.moveTo(newKey);
      oldRtx.moveTo(oldKey);

      if (replaced) {
        if (skipSubtrees) {
          final DiffDepth diffDepth = new DiffDepth(depth.getNewDepth(), depth.getOldDepth());

          fireDiff(DiffType.REPLACEDOLD, newRtx.getNodeKey(), oldRtx.getNodeKey(), diffDepth);
          fireDiff(DiffType.REPLACEDNEW, newRtx.getNodeKey(), oldRtx.getNodeKey(), diffDepth);
          emitNonStructuralDiff(newRtx, oldRtx, diffDepth, DiffType.REPLACEDOLD);
        } else {
          final long newNodeKey = newRtx.getNodeKey();
          final long oldNodeKey = oldRtx.getNodeKey();
          final Axis oldAxis = new DescendantAxis(oldRtx, IncludeSelf.YES);
          final Axis newAxis = new DescendantAxis(newRtx, IncludeSelf.YES);
          while (oldAxis.hasNext()) {
            oldAxis.nextLong();

            final DiffDepth diffDepth = new DiffDepth(depth.getNewDepth(), depth.getOldDepth());

            fireDiff(DiffType.REPLACEDOLD, newRtx.getNodeKey(), oldRtx.getNodeKey(), diffDepth);
            emitNonStructuralDiff(newRtx, oldRtx, diffDepth, DiffType.REPLACEDOLD);

            adjustDepth(oldRtx, oldNodeKey, Revision.OLD);
          }

          while (newAxis.hasNext()) {
            newAxis.nextLong();

            final DiffDepth diffDepth = new DiffDepth(depth.getNewDepth(), depth.getOldDepth());

            fireDiff(DiffType.REPLACEDNEW, newRtx.getNodeKey(), oldRtx.getNodeKey(), diffDepth);
            emitNonStructuralDiff(newRtx, oldRtx, diffDepth, DiffType.REPLACEDNEW);

            adjustDepth(newRtx, newNodeKey, Revision.NEW);
          }

          newRtx.moveTo(newNodeKey);
          oldRtx.moveTo(oldNodeKey);

          diff = DiffType.REPLACED;
        }
      }
    }

    return replaced;
  }

  /**
   * Adjust the depth.
   *
   * @param rtx          the transaction to simulate moves
   * @param startNodeKey the start node key
   * @param revision     revision to iterate over
   */
  private void adjustDepth(final R rtx, final @NonNegative long startNodeKey, final Revision revision) {
    assert rtx != null;
    assert revision != null;
    final long nodeKey = rtx.getNodeKey();
    if (rtx.hasFirstChild()) {
      switch (revision) {
        case NEW -> depth.incrementNewDepth();
        case OLD -> depth.incrementOldDepth();
        default -> {
          // Must not happen.
        }
      }
    } else {
      while (!rtx.hasRightSibling() && rtx.hasParent() && rtx.getNodeKey() != startNodeKey) {
        rtx.moveToParent();
        switch (revision) {
          case NEW -> depth.decrementNewDepth();
          case OLD -> depth.decrementOldDepth();
          default -> {
            // Must not happen.
          }
        }
      }
    }
    rtx.moveTo(nodeKey);
  }

  /**
   * Check for an update of a node.
   *
   * @param newRtx read-only transaction on new revision
   * @param oldRtx read-only transaction on old revision
   * @return kind of diff
   */
  boolean checkUpdate(final R newRtx, final R oldRtx) {
    if (isFirst) {
      return newRtx.getNodeKey() == oldRtx.getNodeKey();
    }
    return newRtx.getNodeKey() == oldRtx.getNodeKey() && newRtx.getParentKey() == oldRtx.getParentKey()
        && depth.getNewDepth() == depth.getOldDepth();
  }

  /**
   * Check names or values of nodes.
   *
   * @param newRtx read-only transaction on new revision
   * @param oldRtx read-only transaction on old revision
   * @return {@code true} if nodes are "equal" according to their names or values, depending on the type, {@code false}
   * otherwise
   */
  abstract boolean checkNodeNamesOrValues(R newRtx, R oldRtx);

  /**
   * Get the document node kind.
   *
   * @return the document node kind
   */
  abstract NodeKind documentNode();
}
