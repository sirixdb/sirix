package org.sirix.xquery.stream.node;

import static com.google.common.base.Preconditions.checkNotNull;
import org.brackit.xquery.xdm.DocumentException;
import org.brackit.xquery.xdm.Stream;
import org.sirix.api.Axis;
import org.sirix.api.SirixAxis;
import org.sirix.xquery.node.XmlDBCollection;
import org.sirix.xquery.node.XmlDBNode;
import com.google.common.base.MoreObjects;

/**
 * {@link Stream}, wrapping a Sirix {@link Axis}.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class SirixNodeStream implements Stream<XmlDBNode> {
  /** Sirix {@link Axis}. */
  private final Axis axis;

  /** {@link XmlDBCollection} the nodes belong to. */
  private final XmlDBCollection collection;

  /**
   * Constructor.
   *
   * @param axis Sirix {@link SirixAxis}
   * @param collection {@link XmlDBCollection} the nodes belong to
   */
  public SirixNodeStream(final Axis axis, final XmlDBCollection collection) {
    this.axis = checkNotNull(axis);
    this.collection = checkNotNull(collection);
  }

  @Override
  public XmlDBNode next() throws DocumentException {
    if (axis.hasNext()) {
      axis.next();
      return new XmlDBNode(axis.asXdmNodeReadTrx(), collection);
    }
    return null;
  }

  @Override
  public void close() {}

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("axis", axis).toString();
  }
}
