package io.sirix.query.stream.node;

import com.google.common.base.MoreObjects;
import io.brackit.query.jdm.DocumentException;
import io.brackit.query.jdm.Stream;
import io.sirix.api.Axis;
import io.sirix.api.SirixAxis;
import io.sirix.query.node.XmlDBCollection;
import io.sirix.query.node.XmlDBNode;

import static java.util.Objects.requireNonNull;

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
    this.axis = requireNonNull(axis);
    this.collection = requireNonNull(collection);
  }

  @Override
  public XmlDBNode next() throws DocumentException {
    if (axis.hasNext()) {
      axis.nextLong();
      return new XmlDBNode(axis.asXmlNodeReadTrx(), collection);
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
