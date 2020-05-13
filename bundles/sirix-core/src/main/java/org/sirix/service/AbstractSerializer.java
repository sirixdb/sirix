package org.sirix.service;

import org.sirix.api.*;
import org.sirix.api.visitor.NodeVisitor;
import org.sirix.axis.visitor.VisitorDescendantAxis;
import org.sirix.exception.SirixException;
import org.sirix.settings.Constants;

import javax.annotation.Nonnegative;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.Callable;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Class implements main serialization algorithm. Other classes can extend it.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 *
 */
public abstract class AbstractSerializer<R extends NodeReadOnlyTrx & NodeCursor, W extends NodeTrx & NodeCursor>
    implements Callable<Void> {

  /** Sirix {@link ResourceManager}. */
  protected final ResourceManager<R, W> resMgr;

  /** Stack for reading end element. */
  protected final Deque<Long> stack;

  /** Array with versions to print. */
  protected final int[] revisions;

  /** Root node key of subtree to shredder. */
  protected final long startNodeKey;

  /** Optional visitor. */
  protected final NodeVisitor visitor;

  /**
   * Constructor.
   *
   * @param resMgr Sirix {@link ResourceManager}
   * @param revision first revision to serialize
   * @param revisions revisions to serialize
   */
  public AbstractSerializer(final ResourceManager<R, W> resMgr, final NodeVisitor visitor,
      final @Nonnegative int revision, final int... revisions) {
    this.visitor = visitor;
    stack = new ArrayDeque<>();
    this.revisions = revisions == null
        ? new int[1]
        : new int[revisions.length + 1];
    initialize(revision, revisions);
    this.resMgr = checkNotNull(resMgr);
    startNodeKey = 0;
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
    this.visitor = visitor;
    stack = new ArrayDeque<>();
    this.revisions = revisions == null
        ? new int[1]
        : new int[revisions.length + 1];
    initialize(revision, revisions);
    this.resMgr = checkNotNull(resMgr);
    startNodeKey = key;
  }

  /**
   * Initialize.
   *
   * @param revision first revision to serialize
   * @param revisions revisions to serialize
   */
  private void initialize(final @Nonnegative int revision, final int... revisions) {
    this.revisions[0] = revision;
    if (revisions != null) {
      for (int i = 0; i < revisions.length; i++) {
        this.revisions[i + 1] = revisions[i];
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

    final int nrOfRevisions = revisions.length;
    final int length = (nrOfRevisions == 1 && revisions[0] < 0)
        ? resMgr.getMostRecentRevisionNumber()
        : nrOfRevisions;

    for (int i = 1; i <= length; i++) {
      try (final R rtx = resMgr.beginNodeReadOnlyTrx((nrOfRevisions == 1 && revisions[0] < 0)
          ? i
          : revisions[i - 1])) {
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
        long key = rtx.getNodeKey();

        // Iterate over all nodes of the subtree including s.
        while (descAxis.hasNext()) {
          key = descAxis.next();

          // Emit all pending end elements.
          if (closeElements) {
            while (!stack.isEmpty() && stack.peek() != rtx.getLeftSiblingKey()) {
              rtx.moveTo(stack.pop());
              emitEndNode(rtx);
              rtx.moveTo(key);
            }
            if (!stack.isEmpty()) {
              rtx.moveTo(stack.pop());
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
            stack.push(rtx.getNodeKey());
          }

          // Remember to emit all pending end elements from stack if
          // required.
          if ((!rtx.hasFirstChild() || isSubtreeGoingToBePruned(rtx)) && !rtx.hasRightSibling()) {
            closeElements = true;
          }
        }

        // Finally emit all pending end elements.
        while (!stack.isEmpty() && stack.peek() != Constants.NULL_ID_LONG) {
          rtx.moveTo(stack.pop());
          emitEndNode(rtx);
        }

        emitRevisionEndNode(rtx);
      }
    }

    emitEndDocument();

    return null;
  }

  protected abstract void setTrxForVisitor(R rtx);

  protected abstract boolean isSubtreeGoingToBePruned(R rtx);

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
  protected abstract void emitEndNode(R rtx);

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

  /** Emit end document. */
  protected abstract void emitEndDocument();
}
