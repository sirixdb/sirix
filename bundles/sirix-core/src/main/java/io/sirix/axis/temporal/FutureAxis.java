package io.sirix.axis.temporal;

import io.sirix.api.NodeCursor;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.api.NodeTrx;
import io.sirix.api.ResourceSession;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.axis.AbstractTemporalAxis;
import io.sirix.axis.IncludeSelf;

import static java.util.Objects.requireNonNull;

/**
 * Retrieve a node by node key in all future revisions. In each revision a
 * {@link XmlNodeReadOnlyTrx} is opened which is moved to the node with the given node key if it
 * exists. Otherwise the iterator has no more elements (the {@link XmlNodeReadOnlyTrx} moved to the
 * node by it's node key).
 *
 * @author Johannes Lichtenberger
 *
 */
public final class FutureAxis<R extends NodeReadOnlyTrx & NodeCursor, W extends NodeTrx & NodeCursor>
    extends AbstractTemporalAxis<R, W> {

  /** The revision number. */
  private int revision;

  /** Sirix {@link ResourceSession}. */
  private final ResourceSession<R, W> resourceSession;

  /** Node key to lookup and retrieve. */
  private final long nodeKey;

  /**
   * Constructor.
   *
   * @param rtx Sirix {@link XmlNodeReadOnlyTrx}
   */
  public FutureAxis(final ResourceSession<R, W> resourceSession, final R rtx) {
    // Using telescope pattern instead of builder (only one optional parameter).
    this(resourceSession, rtx, IncludeSelf.NO);
  }

  /**
   * Constructor.
   *
   * @param resourceSession the resource manager
   * @param rtx the transactional read only cursor
   * @param includeSelf determines if current revision must be included or not
   */
  public FutureAxis(final ResourceSession<R, W> resourceSession, final R rtx, final IncludeSelf includeSelf) {
    this.resourceSession = requireNonNull(resourceSession);
    nodeKey = rtx.getNodeKey();
    revision = requireNonNull(includeSelf) == IncludeSelf.YES
        ? rtx.getRevisionNumber()
        : rtx.getRevisionNumber() + 1;
  }

  @Override
  protected R computeNext() {
    // != a little bit faster?
    if (revision <= resourceSession.getMostRecentRevisionNumber()) {
      final R rtx = resourceSession.beginNodeReadOnlyTrx(revision);
      revision++;
      if (rtx.moveTo(nodeKey)) {
        return rtx;
      } else {
        rtx.close();
        return endOfData();
      }
    } else {
      return endOfData();
    }
  }

  @Override
  public ResourceSession<R, W> getResourceSession() {
    return resourceSession;
  }
}
