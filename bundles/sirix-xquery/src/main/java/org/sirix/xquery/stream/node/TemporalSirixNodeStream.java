package org.sirix.xquery.stream.node;

import com.google.common.base.MoreObjects;
import org.brackit.xquery.xdm.DocumentException;
import org.brackit.xquery.xdm.Stream;
import org.brackit.xquery.xdm.node.AbstractTemporalNode;
import org.sirix.api.Axis;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.api.xml.XmlNodeTrx;
import org.sirix.axis.AbstractTemporalAxis;
import org.sirix.xquery.node.XmlDBCollection;
import org.sirix.xquery.node.XmlDBNode;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * {@link Stream}, wrapping a temporal axis.
 *
 * @author Johannes Lichtenberger
 *
 */
public class TemporalSirixNodeStream implements Stream<AbstractTemporalNode<XmlDBNode>> {

  /** Temporal axis. */
  private final AbstractTemporalAxis<XmlNodeReadOnlyTrx, XmlNodeTrx> mAxis;

  /** The {@link XmlDBCollection} reference. */
  private final XmlDBCollection mCollection;

  /**
   * Constructor.
   *
   * @param axis Sirix {@link Axis}
   * @param collection {@link XmlDBCollection} the nodes belong to
   */
  public TemporalSirixNodeStream(final AbstractTemporalAxis<XmlNodeReadOnlyTrx, XmlNodeTrx> axis,
      final XmlDBCollection collection) {
    mAxis = checkNotNull(axis);
    mCollection = checkNotNull(collection);
  }

  @Override
  public AbstractTemporalNode<XmlDBNode> next() throws DocumentException {
    if (mAxis.hasNext()) {
      final var rtx = mAxis.next();
      return new XmlDBNode(rtx, mCollection);
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
