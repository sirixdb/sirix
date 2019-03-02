package org.sirix.xquery.stream;

import static com.google.common.base.Preconditions.checkNotNull;
import org.brackit.xquery.xdm.AbstractTemporalNode;
import org.brackit.xquery.xdm.DocumentException;
import org.brackit.xquery.xdm.Stream;
import org.sirix.api.Axis;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.axis.AbstractTemporalAxis;
import org.sirix.xquery.node.XmlDBCollection;
import org.sirix.xquery.node.XmlDBNode;
import com.google.common.base.MoreObjects;

/**
 * {@link Stream}, wrapping a temporal axis.
 *
 * @author Johannes Lichtenberger
 *
 */
public class TemporalSirixStream implements Stream<AbstractTemporalNode<XmlDBNode>> {

  /** Temporal axis. */
  private final AbstractTemporalAxis<XmlNodeReadOnlyTrx> mAxis;

  /** The {@link XmlDBCollection} reference. */
  private XmlDBCollection mCollection;

  /**
   * Constructor.
   *
   * @param axis Sirix {@link Axis}
   * @param collection {@link XmlDBCollection} the nodes belong to
   */
  public TemporalSirixStream(final AbstractTemporalAxis<XmlNodeReadOnlyTrx> axis, final XmlDBCollection collection) {
    mAxis = checkNotNull(axis);
    mCollection = checkNotNull(collection);
  }

  @Override
  public AbstractTemporalNode<XmlDBNode> next() throws DocumentException {
    if (mAxis.hasNext()) {
      mAxis.next();
      return new XmlDBNode(mAxis.getTrx(), mCollection);
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
