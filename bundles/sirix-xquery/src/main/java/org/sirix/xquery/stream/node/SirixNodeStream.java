package org.sirix.xquery.stream.node;

import com.google.common.base.MoreObjects;
import org.brackit.xquery.xdm.DocumentException;
import org.brackit.xquery.xdm.Stream;
import org.sirix.api.Axis;
import org.sirix.api.SirixAxis;
import org.sirix.xquery.node.XmlDBCollection;
import org.sirix.xquery.node.XmlDBNode;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * {@link Stream}, wrapping a Sirix {@link Axis}.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class SirixNodeStream implements Stream<XmlDBNode> {
  /** Sirix {@link Axis}. */
  private final Axis mAxis;

  /** {@link XmlDBCollection} the nodes belong to. */
  private final XmlDBCollection mCollection;

  /**
   * Constructor.
   *
   * @param axis Sirix {@link SirixAxis}
   * @param collection {@link XmlDBCollection} the nodes belong to
   */
  public SirixNodeStream(final Axis axis, final XmlDBCollection collection) {
    mAxis = checkNotNull(axis);
    mCollection = checkNotNull(collection);
  }

  @Override
  public XmlDBNode next() throws DocumentException {
    if (mAxis.hasNext()) {
      mAxis.next();
      return new XmlDBNode(mAxis.asXdmNodeReadTrx(), mCollection);
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
