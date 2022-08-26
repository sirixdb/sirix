package org.sirix.service;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.checkerframework.checker.index.qual.NonNegative;
import org.sirix.api.*;
import org.sirix.api.visitor.NodeVisitor;
import org.sirix.axis.visitor.VisitorDescendantAxis;
import org.sirix.exception.SirixException;
import org.sirix.settings.Constants;

import java.util.concurrent.Callable;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Class implements main serialization algorithm. Other classes can extend it.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 */
public abstract class AbstractSerializer<R extends NodeReadOnlyTrx & NodeCursor, W extends NodeTrx & NodeCursor>
    implements Callable<Void> {

  /**
   * Sirix {@link ResourceSession}.
   */
  protected final ResourceSession<R, W> resMgr;

  /**
   * Stack for reading end element.
   */
  protected final LongArrayList stack;

  /**
   * Array with versions to print.
   */
  protected final int[] revisions;

  /**
   * Root node key of subtree to shredder.
   */
  protected final long startNodeKey;

  /**
   * Optional visitor.
   */
  protected final NodeVisitor visitor;

  protected boolean hasToSkipSiblings;

  /**
   * Constructor.
   *
   * @param resMgr    Sirix {@link ResourceSession}
   * @param revision  first revision to serialize
   * @param revisions revisions to serialize
   */
  public AbstractSerializer(final ResourceSession<R, W> resMgr, final NodeVisitor visitor,
      final @NonNegative int revision, final int... revisions) {
    this.visitor = visitor;
    stack = new LongArrayList();
    this.revisions = revisions == null ? new int[1] : new int[revisions.length + 1];
    initialize(revision, revisions);
    this.resMgr = checkNotNull(resMgr);
    startNodeKey = 0;
  }

  /**
   * Constructor.
   *
   * @param resMgr    Sirix {@link ResourceSession}
   * @param key       key of root node from which to serialize the subtree
   * @param revision  first revision to serialize
   * @param revisions revisions to serialize
   */
  public AbstractSerializer(final ResourceSession<R, W> resMgr, final NodeVisitor visitor, final @NonNegative long key,
      final @NonNegative int revision, final int... revisions) {
    this.visitor = visitor;
    stack = new LongArrayList();
    this.revisions = revisions == null ? new int[1] : new int[revisions.length + 1];
    initialize(revision, revisions);
    this.resMgr = checkNotNull(resMgr);
    startNodeKey = key;
  }

  /**
   * Initialize.
   *
   * @param revision  first revision to serialize
   * @param revisions revisions to serialize
   */
  private void initialize(final @NonNegative int revision, final int... revisions) {
    this.revisions[0] = revision;
    if (revisions != null) {
      System.arraycopy(revisions, 0, this.revisions, 1, revisions.length);
    }
  }

  /**
   * Serialize the storage.
   *
   * @return null.
   * @throws SirixException if anything went wrong
   */
  @Override
  public Void call() {
    emitStartDocument();

    final int nrOfRevisions = revisions.length;
    final int length = (nrOfRevisions == 1 && revisions[0] < 0) ? resMgr.getMostRecentRevisionNumber() : nrOfRevisions;

    for (int i = 1; i <= length; i++) {
      try (final R rtx = resMgr.beginNodeReadOnlyTrx((nrOfRevisions == 1 && revisions[0] < 0) ? i : revisions[i - 1])) {
        emitRevisionStartNode(rtx);

        rtx.moveTo(startNodeKey);

        final VisitorDescendantAxis.Builder builder = VisitorDescendantAxis.newBuilder(rtx).includeSelf();

        if (visitor != null) {
          builder.visitor(visitor);
          setTrxForVisitor(rtx);
        }

        final Axis descAxis = builder.build();

        // Setup primitives.
        boolean closeElements = false;
        long key;

        // Iterate over all nodes of the subtree including self.
        while (descAxis.hasNext()) {
          key = descAxis.nextLong();

          // Emit all pending end elements.
          if (closeElements) {
            while (!stack.isEmpty() && stack.peekLong(0) != rtx.getLeftSiblingKey()) {
              rtx.moveTo(stack.popLong());
              emitEndNode(rtx, false);
              rtx.moveTo(key);
            }
            if (!stack.isEmpty()) {
              rtx.moveTo(stack.popLong());
              emitEndNode(rtx, true);
            }
            rtx.moveTo(key);
            closeElements = false;
          }

          // Emit node.
          final long nodeKey = rtx.getNodeKey();
          emitNode(rtx);
          rtx.moveTo(nodeKey);

          // Push end element to stack if we are a start element with children.
          boolean withChildren = false;
          if (!rtx.isDocumentRoot() && (rtx.hasFirstChild() && isSubtreeGoingToBeVisited(rtx))) {
            stack.push(rtx.getNodeKey());
            withChildren = true;
          }

          hasToSkipSiblings = areSiblingNodesGoingToBeSkipped(rtx);

          // Remember to emit all pending end elements from stack if required.
          if (!withChildren && !rtx.isDocumentRoot() && (!rtx.hasRightSibling() || hasToSkipSiblings)) {
            closeElements = true;
          }
        }

        // Finally emit all pending end elements.
        while (!stack.isEmpty() && stack.peekLong(0) != Constants.NULL_ID_LONG) {
          rtx.moveTo(stack.popLong());
          emitEndNode(rtx, false);
        }

        emitRevisionEndNode(rtx);
      }
    }

    emitEndDocument();

    return null;
  }

  protected abstract void setTrxForVisitor(R rtx);

  protected abstract boolean areSiblingNodesGoingToBeSkipped(R rtx);

  protected abstract boolean isSubtreeGoingToBeVisited(R rtx);

  /**
   * Emit start document.
   */
  protected abstract void emitStartDocument();

  /**
   * Emit start tag.
   *
   * @param rtx read-only transaction
   */
  protected abstract void emitNode(R rtx);

  /**
   * Emit end tag.
   *
   * @param rtx read-only transaction
   */
  protected abstract void emitEndNode(R rtx, boolean lastEndNode);

  /**
   * Emit a start tag, which specifies a revision.
   *
   * @param rtx read-only transaction
   */
  protected abstract void emitRevisionStartNode(R rtx);

  /**
   * Emit an end tag, which specifies a revision.
   *
   * @param rtx read-only transaction
   */
  protected abstract void emitRevisionEndNode(R rtx);

  /**
   * Emit end document.
   */
  protected abstract void emitEndDocument();
}
