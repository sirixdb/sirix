package io.sirix.service;

import io.sirix.api.Axis;
import io.sirix.api.NodeCursor;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.api.NodeTrx;
import io.sirix.api.ResourceSession;
import io.sirix.api.visitor.NodeVisitor;
import io.sirix.axis.DescendantAxis;
import io.sirix.axis.IncludeSelf;
import io.sirix.axis.visitor.VisitorDescendantAxis;
import io.sirix.exception.SirixException;
import io.sirix.settings.Constants;
import org.jspecify.annotations.Nullable;
import it.unimi.dsi.fastutil.longs.LongArrayList;

import java.util.concurrent.Callable;

import static java.util.Objects.requireNonNull;

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
  protected final ResourceSession<R, W> session;

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
   * A CLIENT-OWNED read cursor to serialize through, or {@code null} to open (and close) one per
   * revision as before.
   *
   * <p>Callers that already hold an open transaction — a query engine emitting a result, a service
   * layer that just navigated to the node it wants to write out — otherwise paid a full transaction
   * open and close per serialize call purely to re-reach state they already had. When set, exactly
   * ONE revision is serialized (the cursor's own): a borrowed cursor cannot time-travel, so the
   * {@link #revisions} array is not consulted. The serializer never closes it and restores its
   * position on the way out; the client keeps ownership of the lifecycle.
   */
  protected final @Nullable R clientTrx;

  /**
   * Constructor.
   *
   * @param resMgr Sirix {@link ResourceSession}
   * @param revision first revision to serialize
   * @param revisions revisions to serialize
   */
  public AbstractSerializer(final ResourceSession<R, W> resMgr, final NodeVisitor visitor,
      final int revision, final int... revisions) {
    this.visitor = visitor;
    stack = new LongArrayList();
    this.revisions = revisions == null
        ? new int[1]
        : new int[revisions.length + 1];
    initialize(revision, revisions);
    this.session = requireNonNull(resMgr);
    startNodeKey = 0;
    this.clientTrx = null;
  }

  /**
   * Constructor.
   *
   * @param resMgr Sirix {@link ResourceSession}
   * @param key key of root node from which to serialize the subtree
   * @param revision first revision to serialize
   * @param revisions revisions to serialize
   */
  public AbstractSerializer(final ResourceSession<R, W> resMgr, final NodeVisitor visitor, final long key,
      final int revision, final int... revisions) {
    this.visitor = visitor;
    stack = new LongArrayList();
    this.revisions = revisions == null
        ? new int[1]
        : new int[revisions.length + 1];
    initialize(revision, revisions);
    this.session = requireNonNull(resMgr);
    startNodeKey = key;
    this.clientTrx = null;
  }

  /**
   * Constructor that OPTIONALLY borrows a client-owned read cursor; see {@link #clientTrx}. A
   * {@code null} cursor behaves exactly like the constructors above (a transaction is opened per
   * serialized revision); a non-null one serializes that cursor's revision alone, so {@code
   * revision}/{@code revisions} are ignored.
   *
   * @param resMgr Sirix {@link ResourceSession}
   * @param clientTrx the client's open read cursor, or {@code null} to open transactions here
   * @param visitor optional visitor
   * @param key key of root node from which to serialize the subtree
   * @param revision first revision to serialize (ignored when {@code clientTrx} is given)
   * @param revisions further revisions to serialize (ignored when {@code clientTrx} is given)
   */
  public AbstractSerializer(final ResourceSession<R, W> resMgr, final @Nullable R clientTrx,
      final NodeVisitor visitor, final long key, final int revision, final int... revisions) {
    this.visitor = visitor;
    stack = new LongArrayList();
    this.clientTrx = clientTrx;
    if (clientTrx == null) {
      this.revisions = revisions == null
          ? new int[1]
          : new int[revisions.length + 1];
      initialize(revision, revisions);
    } else {
      this.revisions = new int[] {clientTrx.getRevisionNumber()};
    }
    this.session = requireNonNull(resMgr);
    startNodeKey = key;
  }

  /**
   * Initialize.
   *
   * @param revision first revision to serialize
   * @param revisions revisions to serialize
   */
  private void initialize(final int revision, final int... revisions) {
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

    if (clientTrx != null) {
      // Borrowed cursor: serialize the client's revision through the client's own transaction and
      // give the cursor back exactly where it was. Opening a transaction of our own here is what
      // made a serialize call cost a transaction open + close (revision-root load, buffer-pool
      // borrow, epoch registration) on top of the traversal it actually needs.
      final long borrowedNodeKey = clientTrx.getNodeKey();
      try {
        serializeRevision(clientTrx, true);
      } finally {
        clientTrx.moveTo(borrowedNodeKey);
      }
      emitEndDocument();
      return null;
    }

    final int nrOfRevisions = revisions.length;
    final int length = (nrOfRevisions == 1 && revisions[0] < 0)
        ? session.getMostRecentRevisionNumber()
        : nrOfRevisions;

    for (int i = 1; i <= length; i++) {
      try (final R rtx = session.beginNodeReadOnlyTrx((nrOfRevisions == 1 && revisions[0] < 0)
          ? i
          : revisions[i - 1])) {
        serializeRevision(rtx, false);
      }
    }

    emitEndDocument();

    return null;
  }

  /**
   * Serialize one revision through the given cursor.
   *
   * @param rtx the cursor to traverse with — either a transaction this serializer opened, or a
   *        client-owned one ({@code borrowed})
   * @param borrowed whether the cursor belongs to a client and must not be assumed to be at its
   *        initial position
   */
  private void serializeRevision(final R rtx, final boolean borrowed) {
    if (borrowed) {
      // A freshly opened transaction sits on the document root, and emitRevisionStartNode reads
      // the cursor to decide how to open the revision (single-named-member wrapping, indentation
      // depth). A borrowed cursor can be anywhere, so reproduce the fresh-transaction state
      // before emitting — otherwise the same document serializes differently depending on where
      // the client happened to leave its cursor.
      rtx.moveToDocumentRoot();
    }
    emitRevisionStartNode(rtx);

    rtx.moveTo(startNodeKey);

    final Axis descAxis;
    if (visitor != null) {
      final VisitorDescendantAxis.Builder builder = VisitorDescendantAxis.newBuilder(rtx).includeSelf();
      builder.visitor(visitor);
      setTrxForVisitor(rtx);
      descAxis = builder.build();
    } else {
      // No visitor: the plain descendant axis skips the per-node visitor-protocol checks.
      descAxis = new DescendantAxis(rtx, IncludeSelf.YES);
    }

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
      // Re-position only if the emitter actually moved (XML attribute/namespace iteration
      // does; the JSON emitters don't) — an unconditional moveTo re-bound the singleton
      // cursor once per node for nothing.
      if (rtx.getNodeKey() != nodeKey) {
        rtx.moveTo(nodeKey);
      }

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
