package org.sirix.xquery.stream;

import static com.google.common.base.Preconditions.checkNotNull;
import org.brackit.xquery.xdm.AbstractTemporalNode;
import org.brackit.xquery.xdm.DocumentException;
import org.brackit.xquery.xdm.Stream;
import org.sirix.api.Axis;
import org.sirix.api.xdm.XdmNodeReadOnlyTrx;
import org.sirix.axis.AbstractTemporalAxis;
import org.sirix.xquery.node.DBCollection;
import org.sirix.xquery.node.DBNode;
import com.google.common.base.MoreObjects;

/**
 * {@link Stream}, wrapping a temporal axis.
 *
 * @author Johannes Lichtenberger
 *
 */
public class TemporalSirixStream implements Stream<AbstractTemporalNode<DBNode>> {

  /** Temporal axis. */
  private final AbstractTemporalAxis<XdmNodeReadOnlyTrx> mAxis;

  /** The {@link DBCollection} reference. */
  private DBCollection mCollection;

  /**
   * Constructor.
   *
   * @param axis Sirix {@link Axis}
   * @param collection {@link DBCollection} the nodes belong to
   */
  public TemporalSirixStream(final AbstractTemporalAxis<XdmNodeReadOnlyTrx> axis, final DBCollection collection) {
    mAxis = checkNotNull(axis);
    mCollection = checkNotNull(collection);
  }

  @Override
  public AbstractTemporalNode<DBNode> next() throws DocumentException {
    if (mAxis.hasNext()) {
      mAxis.next();
      return new DBNode(mAxis.getTrx(), mCollection);
    }
    return null;
  }

  @Override
  public void close() {}

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("axis", mAxis).toString();
  }
}
