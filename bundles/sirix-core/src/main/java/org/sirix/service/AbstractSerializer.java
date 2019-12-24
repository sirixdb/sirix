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

package org.sirix.service;

import static com.google.common.base.Preconditions.checkNotNull;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.Callable;
import javax.annotation.Nonnegative;
import org.sirix.api.Axis;
import org.sirix.api.NodeCursor;
import org.sirix.api.NodeReadOnlyTrx;
import org.sirix.api.NodeTrx;
import org.sirix.api.ResourceManager;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.api.visitor.NodeVisitor;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.axis.visitor.VisitorDescendantAxis;
import org.sirix.exception.SirixException;
import org.sirix.service.json.serialize.JsonMaxLevelVisitor;
import org.sirix.service.xml.serialize.XmlMaxLevelVisitor;
import org.sirix.settings.Constants;

/**
 * Class implements main serialization algorithm. Other classes can extend it.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 *
 */
public abstract class AbstractSerializer<R extends NodeReadOnlyTrx & NodeCursor, W extends NodeTrx & NodeCursor>
    implements Callable<Void> {

  /** Sirix {@link ResourceManager}. */
  protected final ResourceManager<R, W> mResMgr;

  /** Stack for reading end element. */
  protected final Deque<Long> mStack;

  /** Array with versions to print. */
  protected final int[] mRevisions;

  /** Root node key of subtree to shredder. */
  protected final long mNodeKey;

  /** Optional visitor. */
  protected final NodeVisitor mVisitor;

  /**
   * Constructor.
   *
   * @param resMgr Sirix {@link ResourceManager}
   * @param revision first revision to serialize
   * @param revisions revisions to serialize
   */
  public AbstractSerializer(final ResourceManager<R, W> resMgr, final NodeVisitor visitor,
      final @Nonnegative int revision, final int... revisions) {
    mVisitor = visitor;
    mStack = new ArrayDeque<>();
    mRevisions = revisions == null
        ? new int[1]
        : new int[revisions.length + 1];
    initialize(revision, revisions);
    mResMgr = checkNotNull(resMgr);
    mNodeKey = 0;
  }

  /**
   * Constructor.
   *
   * @param resMgr Sirix {@link ResourceManager}
   * @param key key of root node from which to shredder the subtree
   * @param revision first revision to serialize
   * @param revisions revisions to serialize
   */
  public AbstractSerializer(final ResourceManager<R, W> resMgr, final NodeVisitor visitor, final @Nonnegative long key,
      final @Nonnegative int revision, final int... revisions) {
    mVisitor = visitor;
    mStack = new ArrayDeque<>();
    mRevisions = revisions == null
        ? new int[1]
        : new int[revisions.length + 1];
    initialize(revision, revisions);
    mResMgr = checkNotNull(resMgr);
    mNodeKey = key;
  }

  protected long maxLevel() {
    if (mVisitor == null)
      throw new UnsupportedOperationException();

    if (mVisitor instanceof XmlMaxLevelVisitor) {
      final XmlMaxLevelVisitor visitor = (XmlMaxLevelVisitor) mVisitor;
      return visitor.getMaxLevel();
    } else if (mVisitor instanceof JsonMaxLevelVisitor) {
      final JsonMaxLevelVisitor visitor = (JsonMaxLevelVisitor) mVisitor;
      return visitor.getMaxLevel();
    }

    throw new UnsupportedOperationException();
  }

  protected long currentLevel() {
    if (mVisitor == null)
      throw new UnsupportedOperationException();

    if (mVisitor instanceof XmlMaxLevelVisitor) {
      final XmlMaxLevelVisitor visitor = (XmlMaxLevelVisitor) mVisitor;
      return visitor.getCurrentLevel();
    } else if (mVisitor instanceof JsonMaxLevelVisitor) {
      final JsonMaxLevelVisitor visitor = (JsonMaxLevelVisitor) mVisitor;
      return visitor.getCurrentLevel();
    }

    throw new UnsupportedOperationException();
  }

  /**
   * Initialize.
   *
   * @param revision first revision to serialize
   * @param revisions revisions to serialize
   */
  private void initialize(final @Nonnegative int revision, final int... revisions) {
    mRevisions[0] = revision;
    if (revisions != null) {
      for (int i = 0; i < revisions.length; i++) {
        mRevisions[i + 1] = revisions[i];
      }
    }
  }

  /**
   * Serialize the storage.
   *
   * @return null.
   * @throws SirixException if can't call serailzer
   */
  @Override
  public Void call() throws SirixException {
    emitStartDocument();

    final int nrOfRevisions = mRevisions.length;
    final int length = (nrOfRevisions == 1 && mRevisions[0] < 0)
        ? (int) mResMgr.getMostRecentRevisionNumber()
        : nrOfRevisions;

    for (int i = 1; i <= length; i++) {
      try (final R rtx = mResMgr.beginNodeReadOnlyTrx((nrOfRevisions == 1 && mRevisions[0] < 0)
          ? i
          : mRevisions[i - 1])) {
        emitRevisionStartNode(rtx);

        rtx.moveTo(mNodeKey);

        final VisitorDescendantAxis.Builder builder = VisitorDescendantAxis.newBuilder(rtx).includeSelf();

        if (mVisitor != null) {
          builder.visitor(mVisitor);

          if (mVisitor instanceof XmlMaxLevelVisitor) {
            final XmlMaxLevelVisitor visitor = (XmlMaxLevelVisitor) mVisitor;
            visitor.setTrx((XmlNodeReadOnlyTrx) rtx);
          } else if (mVisitor instanceof JsonMaxLevelVisitor) {
            final JsonMaxLevelVisitor visitor = (JsonMaxLevelVisitor) mVisitor;
            visitor.setTrx((JsonNodeReadOnlyTrx) rtx);
          }
        }

        final Axis descAxis = builder.build();

        // Setup primitives.
        boolean closeElements = false;
        long key = rtx.getNodeKey();

        // Iterate over all nodes of the subtree including s.
        while (descAxis.hasNext()) {
          key = descAxis.next();

          // Emit all pending end elements.
          if (closeElements) {
            while (!mStack.isEmpty() && mStack.peek() != rtx.getLeftSiblingKey()) {
              rtx.moveTo(mStack.pop());
              emitEndNode(rtx);
              rtx.moveTo(key);
            }
            if (!mStack.isEmpty()) {
              rtx.moveTo(mStack.pop());
              emitEndNode(rtx);
            }
            rtx.moveTo(key);
            closeElements = false;
          }

          // Emit node.
          final long nodeKey = rtx.getNodeKey();
          emitNode(rtx);
          rtx.moveTo(nodeKey);

          // Push end element to stack if we are a start element with
          // children.
          if (!rtx.isDocumentRoot() && (rtx.hasFirstChild() && isSubtreeGoingToBeVisited(rtx))) {
            mStack.push(rtx.getNodeKey());
          }

          // Remember to emit all pending end elements from stack if
          // required.
          if ((!rtx.hasFirstChild() || isSubtreeGoingToBePruned(rtx)) && !rtx.hasRightSibling()) {
            closeElements = true;
          }
        }

        // Finally emit all pending end elements.
        while (!mStack.isEmpty() && mStack.peek() != Constants.NULL_ID_LONG) {
          rtx.moveTo(mStack.pop());
          emitEndNode(rtx);
        }

        emitRevisionEndNode(rtx);
      }
    }

    emitEndDocument();

    return null;
  }

  protected abstract boolean isSubtreeGoingToBePruned(R rtx);

  protected abstract boolean isSubtreeGoingToBeVisited(R rtx);

  /**
   * Emit start document.
   */
  protected abstract void emitStartDocument();

  /**
   * Emit start tag.
   *
   * @param rtx Sirix {@link XmlNodeReadOnlyTrx}
   */
  protected abstract void emitNode(R rtx);

  /**
   * Emit end tag.
   *
   * @param rtx Sirix {@link XmlNodeReadOnlyTrx}
   */
  protected abstract void emitEndNode(R rtx);

  /**
   * Emit a start tag, which specifies a revision.
   *
   * @param rtx Sirix {@link XmlNodeReadOnlyTrx}
   */
  protected abstract void emitRevisionStartNode(R rtx);

  /**
   * Emit an end tag, which specifies a revision.
   *
   * @param rtx Sirix {@link XmlNodeReadOnlyTrx}
   */
  protected abstract void emitRevisionEndNode(R rtx);

  /** Emit end document. */
  protected abstract void emitEndDocument();
}
